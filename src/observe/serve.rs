use crate::observe::snapshot::{AgentSummary, HealthView, PeerView, WorkerView, WorkView};
use crate::observe::{Cursor, Event, Observer, Snapshot};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
    use super::{router, AppState};
    use crate::observe::Cursor;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use serde_json::Value;
    use std::fs;
    use tower::ServiceExt;

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
