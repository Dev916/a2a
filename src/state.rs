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

/// The git worktree a worker was dispatched into (spec §4.5, §4.8).
///
/// Spec §4.5 names `path`, `branch` and `base_ref`; **`repo` is a deliberate
/// addition**, and a required one rather than a convenience. Every git call
/// `gc` makes about a finished worker — `worktree remove`, `worktree prune`,
/// `rev-list --count <base>..<branch>` — has to run with `-C <parent repo>`,
/// and once the worktree directory is gone there is nothing left to derive the
/// parent from: `<wt>/.git` is the gitdir *pointer* and it disappears with the
/// directory, and `rev-parse --show-toplevel` inside a linked worktree answers
/// with that worktree's own toplevel, never the parent's.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WorktreeRef {
    pub path: PathBuf,
    pub branch: String,
    pub base_ref: String,
    /// Canonical parent-repo toplevel. Every `gc` git call needs `-C <repo>`,
    /// and it is underivable once the worktree directory is gone.
    pub repo: PathBuf,
}

/// What the framework commit on `done` did for one worker (see
/// `src/auto_commit.rs`). Fold-derived from the worker's own
/// `result_committed` / `commit_skipped` / `commit_failed` row, so a snapshot
/// rebuilt from the WAL reports the same outcome.
///
/// `outcome` is an open string — `committed`, `clean`, `skipped` or `failed`
/// today — so a newer binary's verdict round-trips through an older one.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AutoCommitRecord {
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

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
    /// The worker's own git worktree, when it has one.
    ///
    /// `#[serde(default)]` like every field added after schema_version 1
    /// shipped: a Phase-1/2 snapshot has no such key and must still load rather
    /// than read as `Corrupt`.
    #[serde(default)]
    pub worktree: Option<WorktreeRef>,
    /// The target repo's `HEAD` at spawn — recorded **whether or not** there is
    /// a worktree, because it is the anchor `a2a verify` diffs against in both
    /// cases. Invariant: when [`Self::worktree`] is `Some`, this is
    /// `Some(worktree.base_ref)`.
    #[serde(default)]
    pub base_ref: Option<String>,
    pub session_id: Option<String>,
    /// The app-server thread of record. The same UUID as `session_id` on
    /// 0.140.0 — both are kept because they are two different *claims*: the
    /// thread id is what `turn/start` and `turn/steer` address, and the session
    /// id is what `codex exec resume` accepts (the cross-transport resume path).
    ///
    /// `#[serde(default)]`, like every field added after schema_version 1
    /// shipped: a snapshot written by the Phase-1 binary has no such key and
    /// must still load rather than read as corrupt.
    #[serde(default)]
    pub thread_id: Option<String>,
    /// Position in the admission queue while `state == queued` (Task 14 fills
    /// it; nothing reads it before then).
    #[serde(default)]
    pub queue_position: Option<u64>,
    /// The turn budget this worker was born with (spec §4.9) — the lane's
    /// `max_turns`, already clamped by anything the brief asked for.
    ///
    /// Recorded on the worker rather than read from config at enforcement time
    /// for the same reason `model` and `sandbox` are: an operator editing the
    /// lane table must not silently re-judge a worker that is already running.
    /// `None` is a worker spawned before budgets existed, and the enforcement
    /// path falls back to the lane's current value for it.
    #[serde(default)]
    pub max_turns: Option<u64>,
    /// How many turns THIS epoch has opened. Fold-derived, never
    /// writer-derived: a `turn_started` envelope increments it and an epoch
    /// bump resets it, so a snapshot rebuilt from the WAL counts the same.
    #[serde(default)]
    pub turns_this_epoch: Option<u64>,
    /// The worker this one was respawned from (spec §4.9), when it is a retry.
    ///
    /// Genesis-borne like every other identity field, so `a2a respawn`'s chain
    /// survives losing `state.json`. `None` is the ordinary case: a worker
    /// somebody spawned directly has no parent, and inventing one would make
    /// every fleet look like a retry storm.
    #[serde(default)]
    pub parent_id: Option<String>,
    /// Which attempt at this brief this worker is. **Absent ≡ 1**: every worker
    /// that existed before lineage did is the first attempt at its own brief,
    /// and nothing about it changes on disk when this binary reads it.
    #[serde(default)]
    pub attempt: Option<u32>,
    /// Will the framework commit this worker's change when it reaches `done`?
    ///
    /// The EFFECTIVE decision (lane `auto_commit` under `spawn --commit` /
    /// `--no-commit`), taken at spawn and carried by genesis like every other
    /// per-worker override — so an operator flipping the lane key later never
    /// re-decides a worker already in flight, and a worker born before the
    /// feature existed (no key at all) is never committed by an upgrade.
    #[serde(default)]
    pub auto_commit_enabled: bool,
    /// The commit the framework made on `done`, when it made one.
    #[serde(default)]
    pub committed_sha: Option<String>,
    /// What the framework commit did — `None` until it has run (or for a
    /// worker it never runs for).
    #[serde(default)]
    pub auto_commit: Option<AutoCommitRecord>,
    pub rollout_path: Option<PathBuf>,
    pub pid: Option<u32>,
    pub pid_start_witness: Option<String>,
    /// The named routing profile selected at spawn, if one was requested.
    #[serde(default)]
    pub profile: Option<String>,
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
            worktree: None,
            base_ref: None,
            session_id: None,
            thread_id: None,
            queue_position: None,
            max_turns: None,
            turns_this_epoch: None,
            parent_id: None,
            attempt: None,
            auto_commit_enabled: false,
            committed_sha: None,
            auto_commit: None,
            rollout_path: None,
            pid: None,
            pid_start_witness: None,
            profile: None,
            model: crate::config::DEFAULT_MODEL.to_string(),
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
    /// `exec`, sandbox `workspace-write`, network false, model `DEFAULT_MODEL`,
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
        Some(v) if v as u32 > SNAPSHOT_SCHEMA_VERSION => {
            return Err(SnapshotError::TooNew(v as u32))
        }
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
        assert_eq!(s.thread_id, None);
        assert_eq!(s.queue_position, None);
        assert_eq!(s.worktree, None);
        assert_eq!(s.base_ref, None);
        assert_eq!(s.sandbox, SandboxMode::WorkspaceWrite);
        assert!(!s.network);
        assert_eq!(s.model, crate::config::DEFAULT_MODEL);
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
        assert_eq!(
            back.extra.get("from_the_future"),
            Some(&serde_json::json!({"a": 1}))
        );
        assert_eq!(back.lane, Lane::Review);
    }

    /// A snapshot written by the Phase-1 binary has no `thread_id` and no
    /// `queue_position`. It must load as a snapshot with neither — never as
    /// `Corrupt`, which would silently rebuild every worker on a mixed machine.
    #[test]
    fn a_snapshot_without_the_phase_two_fields_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let s = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        let mut value = serde_json::to_value(&s).unwrap();
        let obj = value.as_object_mut().unwrap();
        obj.remove("thread_id");
        obj.remove("queue_position");
        std::fs::write(tmp.path().join(STATE_FILE), value.to_string()).unwrap();

        let back = load_snapshot(tmp.path()).expect("an older snapshot is not corrupt");
        assert_eq!(back.thread_id, None);
        assert_eq!(back.queue_position, None);
        assert!(
            back.extra.get("thread_id").is_none(),
            "the new fields are typed, not swept into the forward-compat bag"
        );
    }

    /// Same discipline one phase later: a Phase-1/2 snapshot has neither
    /// `worktree` nor `base_ref`, and must load as a worker with neither rather
    /// than as one this binary refuses to read.
    #[test]
    fn a_snapshot_without_the_phase_four_fields_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let s = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        let mut value = serde_json::to_value(&s).unwrap();
        let obj = value.as_object_mut().unwrap();
        obj.remove("worktree");
        obj.remove("base_ref");
        std::fs::write(tmp.path().join(STATE_FILE), value.to_string()).unwrap();

        let back = load_snapshot(tmp.path()).expect("an older snapshot is not corrupt");
        assert_eq!(back.worktree, None);
        assert_eq!(back.base_ref, None);
        assert!(
            back.extra.get("worktree").is_none() && back.extra.get("base_ref").is_none(),
            "the new fields are typed, not swept into the forward-compat bag"
        );
    }

    /// One phase further on: a snapshot written before turn budgets existed has
    /// neither `max_turns` nor `turns_this_epoch`, and must load as a worker
    /// with neither — never as one whose budget reads `0` (which the broker
    /// would trip on its very next turn).
    #[test]
    fn a_snapshot_without_the_phase_five_fields_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let s = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        let mut value = serde_json::to_value(&s).unwrap();
        let obj = value.as_object_mut().unwrap();
        obj.remove("max_turns");
        obj.remove("turns_this_epoch");
        // ...and neither `parent_id` nor `attempt`, which arrived with
        // `a2a respawn` in the same phase. An ABSENT `attempt` is attempt 1 —
        // it must not read as `0`, and it must not make the record corrupt.
        obj.remove("parent_id");
        obj.remove("attempt");
        std::fs::write(tmp.path().join(STATE_FILE), value.to_string()).unwrap();

        let back = load_snapshot(tmp.path()).expect("an older snapshot is not corrupt");
        assert_eq!(back.max_turns, None);
        assert_eq!(back.turns_this_epoch, None);
        assert_eq!(back.parent_id, None);
        assert_eq!(back.attempt, None);
        assert!(
            back.extra.get("max_turns").is_none()
                && back.extra.get("turns_this_epoch").is_none()
                && back.extra.get("parent_id").is_none()
                && back.extra.get("attempt").is_none(),
            "the new fields are typed, not swept into the forward-compat bag"
        );
    }

    /// A snapshot written before the framework commit existed has none of its
    /// three keys, and must load as a worker that is never auto-committed —
    /// never as `Corrupt`, and never as one an upgrade would suddenly commit.
    #[test]
    fn a_snapshot_without_the_auto_commit_fields_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let s = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        let mut value = serde_json::to_value(&s).unwrap();
        let obj = value.as_object_mut().unwrap();
        obj.remove("auto_commit_enabled");
        obj.remove("committed_sha");
        obj.remove("auto_commit");
        std::fs::write(tmp.path().join(STATE_FILE), value.to_string()).unwrap();

        let back = load_snapshot(tmp.path()).expect("an older snapshot is not corrupt");
        assert!(!back.auto_commit_enabled);
        assert_eq!(back.committed_sha, None);
        assert_eq!(back.auto_commit, None);
        assert!(back.extra.is_empty(), "typed, not swept into the bag");
    }

    /// The worktree round trips as a typed object, and the diff anchor agrees
    /// with it — the invariant every writer of `worktree` has to maintain.
    #[test]
    fn a_worktree_round_trips_and_agrees_with_the_base_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        let wt = WorktreeRef {
            path: PathBuf::from("/home/worktrees/impl-0814t1300-aaaa"),
            branch: "a2a/impl/impl-0814t1300-aaaa".into(),
            base_ref: "0123456789abcdef0123456789abcdef01234567".into(),
            repo: PathBuf::from("/repo"),
        };
        s.base_ref = Some(wt.base_ref.clone());
        s.worktree = Some(wt.clone());
        save_snapshot(tmp.path(), &s).unwrap();

        let back = load_snapshot(tmp.path()).unwrap();
        assert_eq!(back.worktree.as_ref(), Some(&wt));
        assert_eq!(
            back.base_ref,
            back.worktree.map(|w| w.base_ref),
            "a worktree's base ref IS the snapshot's diff anchor"
        );
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
