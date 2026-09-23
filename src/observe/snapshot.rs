//! Folded, read-only views returned by the observability plane.

use super::cursor::Cursor;
use serde::{Deserialize, Serialize};

/// Current observable state paired with the cursor at which it was folded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Snapshot {
    pub agent: AgentSummary,
    pub workers: Vec<WorkerView>,
    pub work: Vec<WorkView>,
    pub peers: Vec<PeerView>,
    pub health: Option<HealthView>,
    pub cursor: Cursor,
}

/// Identity of the home represented by this snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentSummary {
    pub agent_id: String,
    #[serde(default)]
    pub online: bool,
    #[serde(default)]
    pub worker_count: usize,
    #[serde(default)]
    pub work_count: usize,
    #[serde(default)]
    pub peer_count: usize,
}

/// Minimal worker projection; later folds can add fields without rejecting old readers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerView {
    pub worker_id: String,
    pub state: String,
    #[serde(default)]
    pub lane: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turns_this_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub awaiting_input: Option<serde_json::Value>,
}

/// Minimal distributed-work projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkView {
    pub work_id: String,
    pub state: String,
}

/// Minimal mesh-peer projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerView {
    pub peer: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Aggregate health state without committing the contract to one subsystem's schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthView {
    pub status: String,
    #[serde(default)]
    pub details: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_shape_round_trips() {
        let snapshot = Snapshot {
            agent: AgentSummary {
                agent_id: "gw-local".into(),
                online: true,
                worker_count: 1,
                work_count: 1,
                peer_count: 1,
            },
            workers: vec![WorkerView {
                worker_id: "impl-1".into(),
                state: "running".into(),
                lane: "impl".into(),
                worktree: None,
                max_turns: Some(8),
                turns_this_epoch: Some(1),
                last_result: None,
                awaiting_input: None,
            }],
            work: vec![WorkView {
                work_id: "work-1".into(),
                state: "offered".into(),
            }],
            peers: vec![PeerView {
                peer: "gw-peer".into(),
                status: "up".into(),
                version: Some("1.2.3".into()),
            }],
            health: Some(HealthView {
                status: "ok".into(),
                details: serde_json::json!({"state": "ok"}),
            }),
            cursor: Cursor::default(),
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        let back: Snapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back, snapshot);
    }
}
