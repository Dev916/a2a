use crate::observe::snapshot::{AgentSummary, HealthView, PeerView, WorkerView, WorkView};
use crate::observe::{Observer, Snapshot};
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
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
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use serde_json::Value;
    use std::fs;
    use tower::ServiceExt;

    fn seed_home_with_one_worker() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let worker = tmp.path().join("workers/impl-0923t0700-serve");
        fs::create_dir_all(&worker).unwrap();
        fs::write(
            worker.join("state.json"),
            br#"{"state":"running","lane":"impl","last_delivered_seq":0}"#,
        )
        .unwrap();
        fs::create_dir_all(tmp.path().join("mesh/gateway")).unwrap();
        fs::write(
            tmp.path().join("mesh/gateway/gw.key"),
            br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
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
}
