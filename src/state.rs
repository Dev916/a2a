//! The versioned `state.json` snapshot (spec §4.5).
//!
//! The snapshot is a *derived checkpoint*, never the source of truth —
//! `events.jsonl` is (spec §4.6). Losing or corrupting `state.json` costs a
//! replay, never data, which is why [`load_snapshot`] distinguishes
//! [`SnapshotError::Missing`]/[`SnapshotError::Corrupt`] (recoverable by
//! folding from zero) from [`SnapshotError::TooNew`] (NOT recoverable —
//! a newer binary owns this worker and this one must not mutate it).
//!
//! Forward compatibility, same discipline as `events.rs`: open enums plus a
//! `#[serde(flatten)] extra` bag so a round trip through an older binary never
//! drops a newer binary's fields.

use crate::config::{Lane, SandboxMode};
use crate::events::Transport;
use crate::fsm::{FailureClass, StateReason, WorkerState};
use crate::output::{AppError, ExitCode};
use crate::store;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Snapshot schema version. An older binary meeting a higher number refuses to
/// mutate the worker (spec §4.5).
pub const SNAPSHOT_SCHEMA_VERSION: u32 = 1;

pub const STATE_FILE: &str = "state.json";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerSnapshot {
    pub schema_version: u32,
    pub id: String,
    pub epoch: u32,
    pub lane: Lane,
    pub owner: Option<String>,
    pub cwd: PathBuf,
    pub transport: Transport,
    pub requested_transport: Transport,
    pub sandbox: SandboxMode,
    pub network: bool,
    pub writable_roots: Vec<PathBuf>,
    pub elevation_reason: Option<String>,
    pub session_id: Option<String>,
    pub rollout_path: Option<PathBuf>,
    pub pid: Option<u32>,
    pub pid_start_witness: Option<String>,
    pub model: String,
    pub effort: String,
    pub brief_sha: String,
    pub schema_path: Option<PathBuf>,
    pub state: WorkerState,
    pub state_reason: Option<StateReason>,
    pub failure_class: Option<FailureClass>,
    pub entered_state_at: DateTime<Utc>,
    pub deadline_at: Option<DateTime<Utc>>,
    pub last_event_seq: u64,
    pub last_applied_seq: u64,
    pub last_delivered_seq: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Fields written by a newer binary (and the fold's own
    /// `fold_rejections` audit trail), preserved verbatim.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl WorkerSnapshot {
    /// The zero snapshot: lane-defaulted identity with nothing observed yet.
    ///
    /// Used in two places — test seeding, and the fold's rebuild-from-zero
    /// base, where the genesis event immediately overwrites every identity
    /// field it carries.
    pub(crate) fn blank(id: &str, cwd: &Path) -> Self {
        let now = Utc::now();
        let lane = id
            .split('-')
            .next()
            .and_then(|p| Lane::from_str(p).ok())
            .unwrap_or(Lane::General);
        Self {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            id: id.to_string(),
            epoch: 1,
            lane,
            owner: None,
            cwd: cwd.to_path_buf(),
            transport: Transport::Exec,
            requested_transport: Transport::Exec,
            sandbox: SandboxMode::WorkspaceWrite,
            network: false,
            writable_roots: Vec::new(),
            elevation_reason: None,
            session_id: None,
            rollout_path: None,
            pid: None,
            pid_start_witness: None,
            model: "gpt-5.5".to_string(),
            effort: "medium".to_string(),
            brief_sha: String::new(),
            schema_path: None,
            state: WorkerState::Spawned,
            state_reason: None,
            failure_class: None,
            entered_state_at: now,
            deadline_at: None,
            last_event_seq: 0,
            last_applied_seq: 0,
            last_delivered_seq: 0,
            created_at: now,
            updated_at: now,
            extra: serde_json::Map::new(),
        }
    }

    /// Plain `pub fn` on purpose — integration tests are external crates, so a
    /// `#[cfg(test)]` constructor would be invisible to them.
    ///
    /// Seeds: schema_version 1, epoch 1, state `spawned`, lane parsed from the
    /// id prefix (fallback `general`), cwd as given, transport/requested
    /// `exec`, sandbox `workspace-write`, network false, model `gpt-5.5`,
    /// effort `medium`, all seqs 0, timestamps now.
    pub fn new_for_test(id: &str, cwd: &Path) -> Self {
        Self::blank(id, cwd)
    }
}

/// Why a snapshot could not be loaded. Only `TooNew` is fatal: the other three
/// are answered by replaying the WAL.
#[derive(Debug)]
pub enum SnapshotError {
    Missing,
    Corrupt,
    TooNew(u32),
    Io(std::io::Error),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::Missing => write!(f, "no state.json for this worker"),
            SnapshotError::Corrupt => write!(f, "state.json is unparseable"),
            SnapshotError::TooNew(v) => write!(
                f,
                "state.json is schema_version {v}, this a2a understands {SNAPSHOT_SCHEMA_VERSION}"
            ),
            SnapshotError::Io(e) => write!(f, "reading state.json: {e}"),
        }
    }
}

impl std::error::Error for SnapshotError {}

/// Every mutating path funnels a snapshot error through here, so a newer
/// worker is always refused with the same contract: `SCHEMA_TOO_NEW` +
/// precondition exit (spec §4.5).
impl From<SnapshotError> for AppError {
    fn from(e: SnapshotError) -> AppError {
        match e {
            SnapshotError::TooNew(v) => AppError::new(
                "SCHEMA_TOO_NEW",
                ExitCode::Precondition,
                format!(
                    "worker state.json is schema_version {v}; this a2a understands {SNAPSHOT_SCHEMA_VERSION}"
                ),
            )
            .remedy("upgrade a2a — an older binary must never mutate a newer worker"),
            SnapshotError::Missing => AppError::new(
                "NO_SUCH_WORKER",
                ExitCode::NoSuchWorker,
                "worker has no state.json",
            ),
            SnapshotError::Corrupt => AppError::new(
                "STATE_CORRUPT",
                ExitCode::Internal,
                "worker state.json is unparseable",
            )
            .remedy("rebuild it by folding events.jsonl (`a2a status` reconciles)"),
            SnapshotError::Io(e) => {
                AppError::new("IO", ExitCode::Internal, format!("reading state.json: {e}"))
            }
        }
    }
}

/// Read `<worker_dir>/state.json`.
///
/// The version gate runs BEFORE strict deserialization so that a newer schema
/// is reported as [`SnapshotError::TooNew`] (refuse to mutate) rather than
/// misdiagnosed as [`SnapshotError::Corrupt`] (rebuild and overwrite).
pub fn load_snapshot(worker_dir: &Path) -> Result<WorkerSnapshot, SnapshotError> {
    let path = worker_dir.join(STATE_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(SnapshotError::Missing),
        Err(e) => return Err(SnapshotError::Io(e)),
    };
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| SnapshotError::Corrupt)?;
    match value.get("schema_version").and_then(|v| v.as_u64()) {
        Some(v) if v as u32 > SNAPSHOT_SCHEMA_VERSION => return Err(SnapshotError::TooNew(v as u32)),
        Some(_) => {}
        None => return Err(SnapshotError::Corrupt),
    }
    serde_json::from_value(value).map_err(|_| SnapshotError::Corrupt)
}

/// Atomically replace `<worker_dir>/state.json` (temp → fsync → rename →
/// fsync(dir)), so a reader never sees a half-written checkpoint.
pub fn save_snapshot(worker_dir: &Path, snap: &WorkerSnapshot) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(snap)?;
    store::write_atomic(&worker_dir.join(STATE_FILE), &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_for_test_seeds_the_documented_values() {
        let tmp = tempfile::tempdir().unwrap();
        let s = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        assert_eq!(s.schema_version, 1);
        assert_eq!(s.epoch, 1);
        assert_eq!(s.state, WorkerState::Spawned);
        assert_eq!(s.lane, Lane::Impl);
        assert_eq!(s.cwd, tmp.path());
        assert_eq!(s.transport, Transport::Exec);
        assert_eq!(s.requested_transport, Transport::Exec);
        assert_eq!(s.sandbox, SandboxMode::WorkspaceWrite);
        assert!(!s.network);
        assert_eq!(s.model, "gpt-5.5");
        assert_eq!(s.effort, "medium");
        assert_eq!(
            (s.last_event_seq, s.last_applied_seq, s.last_delivered_seq),
            (0, 0, 0)
        );
    }

    #[test]
    fn unknown_id_prefix_falls_back_to_the_general_lane() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(
            WorkerSnapshot::new_for_test("weird", tmp.path()).lane,
            Lane::General
        );
        assert_eq!(
            WorkerSnapshot::new_for_test("nope-0814t1300-aaaa", tmp.path()).lane,
            Lane::General
        );
    }

    #[test]
    fn unknown_fields_survive_a_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = WorkerSnapshot::new_for_test("review-0814t1300-aaaa", tmp.path());
        s.extra
            .insert("from_the_future".into(), serde_json::json!({"a": 1}));
        save_snapshot(tmp.path(), &s).unwrap();
        let back = load_snapshot(tmp.path()).unwrap();
        assert_eq!(back.extra.get("from_the_future"), Some(&serde_json::json!({"a": 1})));
        assert_eq!(back.lane, Lane::Review);
    }

    #[test]
    fn missing_and_corrupt_are_distinguished_from_too_new() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(matches!(
            load_snapshot(tmp.path()),
            Err(SnapshotError::Missing)
        ));
        std::fs::write(tmp.path().join(STATE_FILE), b"{not json").unwrap();
        assert!(matches!(
            load_snapshot(tmp.path()),
            Err(SnapshotError::Corrupt)
        ));
        // parseable JSON, unparseable snapshot
        std::fs::write(tmp.path().join(STATE_FILE), br#"{"schema_version":1}"#).unwrap();
        assert!(matches!(
            load_snapshot(tmp.path()),
            Err(SnapshotError::Corrupt)
        ));
        std::fs::write(tmp.path().join(STATE_FILE), br#"{"schema_version":9}"#).unwrap();
        assert!(matches!(
            load_snapshot(tmp.path()),
            Err(SnapshotError::TooNew(9))
        ));
    }

    #[test]
    fn too_new_maps_to_the_schema_too_new_precondition_error() {
        let err: AppError = SnapshotError::TooNew(2).into();
        assert_eq!(err.code, "SCHEMA_TOO_NEW");
        assert_eq!(err.exit, ExitCode::Precondition);
    }

    #[test]
    fn state_json_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let s = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        save_snapshot(tmp.path(), &s).unwrap();
        let mode = std::fs::metadata(tmp.path().join(STATE_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
