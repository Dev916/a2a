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
}

/// Minimal worker projection; later folds can add fields without rejecting old readers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerView {
    pub worker_id: String,
    pub state: String,
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
}

/// Aggregate health state without committing the contract to one subsystem's schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthView {
    pub status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_shape_round_trips() {
        let snapshot = Snapshot {
            agent: AgentSummary {
                agent_id: "gw-local".into(),
            },
            workers: vec![WorkerView {
                worker_id: "impl-1".into(),
                state: "running".into(),
            }],
            work: vec![WorkView {
                work_id: "work-1".into(),
                state: "offered".into(),
            }],
            peers: vec![PeerView {
                peer: "gw-peer".into(),
                status: "up".into(),
            }],
            health: Some(HealthView {
                status: "ok".into(),
            }),
            cursor: Cursor::default(),
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        let back: Snapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back, snapshot);
    }
}
