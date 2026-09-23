use crate::observe::snapshot::{AgentSummary, HealthView, PeerView, WorkerView, WorkView};
use crate::observe::{Cursor, Event, Observer, Plane, Snapshot};
use axum::extract::ws::{close_code, CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

const STREAM_POLL_INTERVAL: Duration = Duration::from_secs(1);
const STREAM_PING_INTERVAL: Duration = Duration::from_secs(30);
const STREAM_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const STREAM_BUFFER_CAPACITY: usize = 64;

#[derive(Clone, Debug)]
pub struct AppState {
    pub home: PathBuf,
    pub token: Option<String>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/snapshot", get(snapshot))
        .route("/agents", get(agents))
        .route("/workers", get(workers))
        .route("/workers/{id}", get(worker_detail))
        .route("/work", get(work))
        .route("/work/{id}", get(work_detail))
        .route("/feed", get(feed))
        .route("/stream", get(stream))
        .with_state(state)
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn snapshot(State(state): State<AppState>) -> Json<SnapshotResponse> {
    let snapshot = Observer::open(&state.home).snapshot();
    Json(snapshot.into())
}

async fn agents(State(state): State<AppState>) -> Json<Vec<AgentSummary>> {
    let snapshot = Observer::open(&state.home).snapshot();
    Json(vec![snapshot.agent])
}

async fn workers(State(state): State<AppState>) -> Json<Vec<WorkerView>> {
    Json(Observer::open(&state.home).snapshot().workers)
}

async fn worker_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<WorkerView>, StatusCode> {
    Observer::open(&state.home)
        .snapshot()
        .workers
        .into_iter()
        .find(|worker| worker.worker_id == id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn work(State(state): State<AppState>) -> Json<Vec<WorkView>> {
    Json(Observer::open(&state.home).snapshot().work)
}

async fn work_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<WorkView>, StatusCode> {
    Observer::open(&state.home)
        .snapshot()
        .work
        .into_iter()
        .find(|work| work.work_id == id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

#[derive(Debug, Deserialize)]
struct FeedQuery {
    from: Option<String>,
    limit: Option<usize>,
}

async fn feed(
    State(state): State<AppState>,
    Query(query): Query<FeedQuery>,
) -> Result<Json<Vec<Event>>, StatusCode> {
    let cursor = query
        .from
        .as_deref()
        .map(Cursor::decode)
        .transpose()
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .unwrap_or_default();
    let (events, _) = Observer::open(&state.home).events_since(&cursor);
    let mut events = events
        .into_iter()
        .filter(|event| event.kind == "feed_entry" && event.src.artifact == "mesh/feed.jsonl")
        .collect::<Vec<_>>();
    if let Some(limit) = query.limit {
        let first = events.len().saturating_sub(limit);
        events.drain(..first);
    }
    Ok(Json(events))
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    from: Option<String>,
    planes: Option<String>,
}

async fn stream(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_stream(socket, state, query))
}

fn stream_frames(events: &[Event], planes: Option<&[Plane]>) -> Vec<String> {
    events
        .iter()
        .filter(|event| planes.is_none_or(|planes| planes.contains(&event.plane)))
        .map(|event| {
            serde_json::to_string(event)
                .expect("Event envelopes contain only JSON-serializable values")
        })
        .collect()
}

fn parse_planes(value: Option<&str>) -> Option<Vec<Plane>> {
    value.map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| match value {
                "mesh" => Plane::Mesh,
                "work" => Plane::Work,
                "worker" => Plane::Worker,
                "presence" => Plane::Presence,
                "health" => Plane::Health,
                _ => Plane::Unknown,
            })
            .collect()
    })
}

#[derive(Debug)]
enum StreamMessage {
    Text(String),
    Ping,
}

#[derive(Debug, PartialEq)]
enum TailExit {
    SlowConsumer,
    ReceiverClosed,
}

async fn handle_stream(mut socket: WebSocket, state: AppState, query: StreamQuery) {
    let cursor = match query.from.as_deref().map(Cursor::decode).transpose() {
        Ok(Some(cursor)) => cursor,
        Ok(None) => Observer::open(&state.home).snapshot().cursor,
        Err(_) => {
            close_policy(&mut socket, "invalid stream cursor").await;
            return;
        }
    };
    let planes = parse_planes(query.planes.as_deref());
    let observer = Observer::open(&state.home);
    let (events, cursor) = observer.events_since(&cursor);

    for frame in stream_frames(&events, planes.as_deref()) {
        if !send_message(&mut socket, Message::Text(frame.into())).await {
            return;
        }
    }

    let (outbound, mut incoming) = mpsc::channel(STREAM_BUFFER_CAPACITY);
    let mut tailer = tokio::spawn(live_tail(
        state.home,
        cursor,
        planes,
        outbound,
    ));

    loop {
        tokio::select! {
            result = &mut tailer => {
                if matches!(result, Ok(TailExit::SlowConsumer)) {
                    close_policy(&mut socket, "stream consumer too slow").await;
                }
                break;
            }
            message = incoming.recv() => {
                let Some(message) = message else {
                    break;
                };
                let message = match message {
                    StreamMessage::Text(frame) => Message::Text(frame.into()),
                    StreamMessage::Ping => Message::Ping(Vec::new().into()),
                };
                if !send_message(&mut socket, message).await {
                    break;
                }
            }
            message = socket.recv() => {
                match message {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(Message::Ping(payload))) => {
                        if !send_message(&mut socket, Message::Pong(payload)).await {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                }
            }
        }
    }

    tailer.abort();
}

async fn live_tail(
    home: PathBuf,
    mut cursor: Cursor,
    planes: Option<Vec<Plane>>,
    outbound: mpsc::Sender<StreamMessage>,
) -> TailExit {
    let observer = Observer::open(home);
    let now = Instant::now();
    let mut poll = time::interval_at(now + STREAM_POLL_INTERVAL, STREAM_POLL_INTERVAL);
    let mut ping = time::interval_at(now + STREAM_PING_INTERVAL, STREAM_PING_INTERVAL);

    loop {
        tokio::select! {
            _ = poll.tick() => {
                let (events, advanced) = observer.events_since(&cursor);
                for frame in stream_frames(&events, planes.as_deref()) {
                    if let Err(exit) = queue_message(&outbound, StreamMessage::Text(frame)) {
                        return exit;
                    }
                }
                cursor = advanced;
            }
            _ = ping.tick() => {
                if let Err(exit) = queue_message(&outbound, StreamMessage::Ping) {
                    return exit;
                }
            }
        }
    }
}

fn queue_message(
    outbound: &mpsc::Sender<StreamMessage>,
    message: StreamMessage,
) -> Result<(), TailExit> {
    match outbound.try_send(message) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => Err(TailExit::SlowConsumer),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(TailExit::ReceiverClosed),
    }
}

async fn send_message(socket: &mut WebSocket, message: Message) -> bool {
    matches!(
        time::timeout(STREAM_SEND_TIMEOUT, socket.send(message)).await,
        Ok(Ok(()))
    )
}

async fn close_policy(socket: &mut WebSocket, reason: &'static str) {
    let close = Message::Close(Some(CloseFrame {
        code: close_code::POLICY,
        reason: reason.into(),
    }));
    let _ = time::timeout(STREAM_SEND_TIMEOUT, socket.send(close)).await;
}

#[derive(Serialize)]
struct SnapshotResponse {
    agent: AgentSummary,
    workers: Vec<WorkerView>,
    work: Vec<WorkView>,
    peers: Vec<PeerView>,
    health: Option<HealthView>,
    cursor: String,
}

impl From<Snapshot> for SnapshotResponse {
    fn from(snapshot: Snapshot) -> Self {
        let Snapshot {
            agent,
            workers,
            work,
            peers,
            health,
            cursor,
        } = snapshot;
        Self {
            agent,
            workers,
            work,
            peers,
            health,
            cursor: cursor.encode(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{router, stream_frames, AppState};
    use crate::observe::{Cursor, Event, Plane, Src, Subject};
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use serde_json::Value;
    use std::fs;
    use tower::ServiceExt;

    fn stream_event(plane: Plane, kind: &str, reference: &str) -> Event {
        Event {
            v: 1,
            ts: format!("2026-09-23T07:00:0{reference}Z"),
            agent_id: "gw-local".into(),
            plane,
            kind: kind.into(),
            subject: Subject {
                worker_id: None,
                work_id: None,
                peer: None,
            },
            payload: serde_json::json!({"reference": reference}),
            src: Src {
                artifact: "fixture.jsonl".into(),
                reference: reference.into(),
            },
        }
    }

    #[test]
    fn stream_frames_preserve_order_and_filter_planes() {
        let events = vec![
            stream_event(Plane::Mesh, "mesh-first", "1"),
            stream_event(Plane::Worker, "worker-middle", "2"),
            stream_event(Plane::Work, "work-last", "3"),
        ];

        let filtered = stream_frames(&events, Some(&[Plane::Mesh, Plane::Work]));
        assert_eq!(
            filtered,
            vec![
                serde_json::to_string(&events[0]).unwrap(),
                serde_json::to_string(&events[2]).unwrap(),
            ]
        );

        assert_eq!(
            stream_frames(&events, None),
            events
                .iter()
                .map(|event| serde_json::to_string(event).unwrap())
                .collect::<Vec<_>>()
        );
    }

    fn seed_home_with_one_worker() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let worker = tmp.path().join("workers/impl-0923t0700-serve");
        fs::create_dir_all(worker.join("outbox")).unwrap();
        fs::write(
            worker.join("state.json"),
            br#"{"state":"awaiting-input","lane":"impl","worktree":{"path":"/tmp/worktree","branch":"a2a/impl/serve"},"max_turns":8,"turns_this_epoch":3,"last_delivered_seq":0}"#,
        )
        .unwrap();
        fs::write(
            worker.join("result.json"),
            br#"{"status":"fail","summary":"waiting for input"}"#,
        )
        .unwrap();
        fs::write(
            worker.join("outbox/1.question.json"),
            br#"{"seq":1,"question":"choose A or B","blocking":true}"#,
        )
        .unwrap();
        fs::create_dir_all(tmp.path().join("mesh/gateway")).unwrap();
        fs::write(
            tmp.path().join("mesh/gateway/gw.key"),
            br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
        )
        .unwrap();
        fs::write(
            tmp.path().join("mesh/events.jsonl"),
            b"{\"seq\":1,\"ts\":\"2026-09-23T07:00:00Z\",\"kind\":\"work_transition\",\"work_id\":\"work-1\",\"to\":\"offered\"}\n",
        )
        .unwrap();
        fs::write(
            tmp.path().join("mesh/feed.jsonl"),
            b"{\"seq\":2,\"origin\":\"gw-local\",\"origin_seq\":2,\"ts\":\"2026-09-23T07:00:01Z\",\"text\":\"hello from feed\"}\n",
        )
        .unwrap();
        tmp
    }

    async fn body_json(response: Response) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn snapshot_healthz_and_agents_render() {
        let tmp = seed_home_with_one_worker();
        let app = router(AppState {
            home: tmp.path().into(),
            token: None,
        });

        let healthz = app
            .clone()
            .oneshot(
                Request::get("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(healthz.status(), StatusCode::OK);
        assert_eq!(body_json(healthz).await, serde_json::json!({"ok": true}));

        let snapshot = app
            .clone()
            .oneshot(
                Request::get("/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshot.status(), StatusCode::OK);
        let snapshot = body_json(snapshot).await;
        assert!(snapshot["workers"].is_array());
        assert_eq!(snapshot["workers"].as_array().unwrap().len(), 1);
        assert!(snapshot["cursor"].is_string());

        let agents = app
            .oneshot(
                Request::get("/agents")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(agents.status(), StatusCode::OK);
        let agents = body_json(agents).await;
        assert_eq!(agents.as_array().unwrap().len(), 1);
        assert_eq!(agents[0]["agent_id"], "gw-local");
        assert_eq!(agents[0]["worker_count"], 1);
    }

    #[tokio::test]
    async fn workers_work_and_feed_render() {
        let tmp = seed_home_with_one_worker();
        let app = router(AppState {
            home: tmp.path().into(),
            token: None,
        });

        let workers = app
            .clone()
            .oneshot(Request::get("/workers").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(workers.status(), StatusCode::OK);
        let workers = body_json(workers).await;
        assert_eq!(workers.as_array().unwrap().len(), 1);
        assert_eq!(workers[0]["worker_id"], "impl-0923t0700-serve");

        let worker = app
            .clone()
            .oneshot(
                Request::get("/workers/impl-0923t0700-serve")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(worker.status(), StatusCode::OK);
        let worker = body_json(worker).await;
        assert_eq!(worker["state"], "awaiting-input");
        assert_eq!(worker["lane"], "impl");
        assert_eq!(worker["worktree"]["branch"], "a2a/impl/serve");
        assert_eq!(worker["max_turns"], 8);
        assert_eq!(worker["turns_this_epoch"], 3);
        assert_eq!(worker["last_result"]["status"], "fail");
        assert_eq!(worker["awaiting_input"]["question"], "choose A or B");

        let missing_worker = app
            .clone()
            .oneshot(
                Request::get("/workers/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_worker.status(), StatusCode::NOT_FOUND);

        let work = app
            .clone()
            .oneshot(Request::get("/work").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(work.status(), StatusCode::OK);
        let work = body_json(work).await;
        assert_eq!(work.as_array().unwrap().len(), 1);
        assert_eq!(work[0]["work_id"], "work-1");

        let work_item = app
            .clone()
            .oneshot(Request::get("/work/work-1").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(work_item.status(), StatusCode::OK);
        let work_item = body_json(work_item).await;
        assert_eq!(work_item["state"], "offered");

        let missing_work = app
            .clone()
            .oneshot(
                Request::get("/work/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_work.status(), StatusCode::NOT_FOUND);

        let feed = app
            .oneshot(
                Request::get("/feed?limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(feed.status(), StatusCode::OK);
        let feed = body_json(feed).await;
        assert_eq!(feed.as_array().unwrap().len(), 1);
        assert_eq!(feed[0]["kind"], "feed_entry");
        assert_eq!(feed[0]["payload"]["text"], "hello from feed");
    }

    #[tokio::test]
    async fn feed_decodes_opaque_cursor_and_rejects_invalid_tokens() {
        let tmp = seed_home_with_one_worker();
        let app = router(AppState {
            home: tmp.path().into(),
            token: None,
        });
        let mut cursor = Cursor::default();
        cursor.set("mesh/feed.jsonl", 1);

        let after_cursor = app
            .clone()
            .oneshot(
                Request::get(format!("/feed?from={}", cursor.encode()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(after_cursor.status(), StatusCode::OK);
        let after_cursor = body_json(after_cursor).await;
        assert_eq!(after_cursor.as_array().unwrap().len(), 1);
        assert_eq!(after_cursor[0]["payload"]["origin_seq"], 2);

        cursor.set("mesh/feed.jsonl", 2);
        let at_tip = app
            .clone()
            .oneshot(
                Request::get(format!("/feed?from={}", cursor.encode()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(at_tip.status(), StatusCode::OK);
        assert!(body_json(at_tip).await.as_array().unwrap().is_empty());

        let invalid = app
            .oneshot(
                Request::get("/feed?from=not-a-cursor")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    }
}
