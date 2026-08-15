//! Reconcile-on-read: the Phase-1 ownership rule (spec §4.6).
//!
//! There is no broker in Phase 1, so **every CLI read path owns the machine**.
//! `status`/`ls`/`result`/`wait` all funnel through [`reconcile`], which is the
//! only place where "what the kernel and the filesystem say" becomes "what the
//! WAL says". `fold::recover` deliberately stops short of that: it *observes*,
//! this module *commits*, and it commits through exactly the same
//! append→fold→checkpoint path a broker would use.
//!
//! Everything below runs under an exclusive `flock` on `<worker>/state.lock`,
//! so N concurrent `a2a status` invocations serialize instead of interleaving
//! two folds over one log. The lock is nonblocking-with-retry rather than
//! blocking: a wedged holder must surface as a typed `LOCK_TIMEOUT` (exit 8),
//! never as a CLI that hangs forever.
//!
//! The pass is ordered, and the order is the contract:
//!
//! 1. **Ingest** `exec.<epoch>.jsonl` from a persisted byte watermark —
//!    redacting at ingest, so a credential a worker printed never lands in
//!    `events.jsonl` in the clear (spec §5), and never re-reading a byte.
//! 2. **Turn completion** first, because a worker that ended its turn with a
//!    result finished; its process exiting afterwards is a consequence, not a
//!    failure.
//! 3. **Process death** second, and only when the kernel actually says so:
//!    a free/absent `run.lock` is treated as death only once `exit_code` exists
//!    or a 5s grace has elapsed — otherwise a worker reconciled milliseconds
//!    after spawn would be declared dead before its shim reached the lock.
//! 4. **Deadline expiry** last, evaluated against the persisted absolute
//!    `deadline_at` (spec §4.6: timeouts are events, fired immediately if past).
//!    A task timeout also group-kills, via the recorded pid + start witness.

use crate::config::Config;
use crate::events::{map_exec_line, Envelope, EventClass, EventKind};
use crate::exec::{self, Liveness};
use crate::fold::{self, append_and_fold, LivenessInput};
use crate::fsm::{self, Action, FsmEvent, TimeoutKind, WorkerState};
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode};
use crate::redact;
use crate::state::{load_snapshot, save_snapshot, SnapshotError, WorkerSnapshot};
use crate::store::EventLog;
use chrono::{DateTime, Duration as Delta, Utc};
use nix::fcntl::{Flock, FlockArg};
use serde_json::{json, Value};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::Duration;

/// Per-worker serialization point for every reconcile.
pub const STATE_LOCK: &str = "state.lock";

/// Nonblocking flock retried 50ms × 200 = 10s, then `LOCK_TIMEOUT` (exit 8).
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const LOCK_RETRY_TRIES: u32 = 200;

/// A freshly spawned worker whose shim has not yet reached `run.lock` must not
/// be declared dead. Death needs the `exit_code` sentinel, or this much time in
/// the current state.
const DEATH_GRACE_SECS: i64 = 5;

/// Observe the worker, commit whatever the observation implies, and return the
/// resulting snapshot. See the module docs for the ordering contract.
pub fn reconcile(
    home: &A2aHome,
    cfg: &Config,
    id: &str,
    now: DateTime<Utc>,
) -> Result<WorkerSnapshot, AppError> {
    let dir = home.worker_dir(id);
    if !dir.is_dir() {
        return Err(AppError::new(
            "NO_SUCH_WORKER",
            ExitCode::NoSuchWorker,
            format!("no worker `{id}`"),
        ));
    }
    let _lock = lock_state(&dir)?;
    reconcile_locked(&dir, cfg, now)
}

/// Everything from here down assumes `state.lock` is held.
fn reconcile_locked(
    dir: &Path,
    cfg: &Config,
    now: DateTime<Utc>,
) -> Result<WorkerSnapshot, AppError> {
    let live = exec::liveness(dir);

    // 1. Load. A newer schema is refused outright (an older binary must never
    //    mutate a newer worker); missing/corrupt is answered by the log.
    let snap = match load_snapshot(dir) {
        Ok(s) => s,
        Err(SnapshotError::Missing) | Err(SnapshotError::Corrupt) => fold::recover(
            dir,
            LivenessInput {
                process_alive: live == Liveness::Alive,
            },
            now,
        )?,
        Err(e) => return Err(e.into()),
    };

    let mut log = EventLog::open(dir).map_err(|e| io_err("opening events.jsonl", e))?;

    // 2. Ingest this epoch's exec stream from the byte watermark.
    let (mut snap, ingested) = ingest(&mut log, dir, snap, now)?;

    // Session capture: the codex session id is what makes a worker resumable,
    // so it is promoted out of the transport stream into the snapshot.
    if let Some(session) = &ingested.session {
        if snap.state == WorkerState::Running && snap.session_id.as_deref() != Some(&session.id) {
            snap = append_and_fold(
                &mut log,
                dir,
                snap,
                EventKind::SessionIdentified,
                json!({
                    "fsm": "SessionIdentified",
                    "session_id": session.id,
                    "rollout_path": session.rollout_path,
                }),
                now,
            )?;
        }
    }

    // 3. Detect and commit, in order.
    let exit_code = exec::read_exit_code(dir);
    let grace_elapsed = now - snap.entered_state_at >= Delta::seconds(DEATH_GRACE_SECS);
    let death_observed = live != Liveness::Alive && (exit_code.is_some() || grace_elapsed);

    // 3a. Turn completion. A turn that produced a result (or a question) is a
    //     conclusion in its own right; only a turn that ended empty AND whose
    //     process is confirmed gone falls through to the death branch, where it
    //     becomes `failed{no_result}` rather than a worker parked in
    //     awaiting-input with nobody home.
    let mut turn_concluded = false;
    if ingested.turn_completed && snap.state == WorkerState::Running {
        let has_result = has_result(dir);
        let has_question = has_unanswered_question(dir, snap.last_delivered_seq);
        if has_result || has_question || !death_observed {
            snap = commit(
                &mut log,
                dir,
                snap,
                EventKind::StateChange,
                json!({"fsm": "TurnEnded", "has_result": has_result, "has_question": has_question}),
                &FsmEvent::TurnEnded {
                    has_result,
                    has_question,
                },
                now,
            )?;
            turn_concluded = true;
        }
    }

    // 3b. Process death. Only from the two states that own a live process —
    //     in exec transport the child legitimately exits at a turn boundary, so
    //     awaiting-input/escalated must never read as a death.
    if !turn_concluded
        && death_observed
        && matches!(snap.state, WorkerState::Running | WorkerState::Spawned)
    {
        snap = commit(
            &mut log,
            dir,
            snap,
            EventKind::StateChange,
            json!({"fsm": "ProcessExited", "code": exit_code}),
            &FsmEvent::ProcessExited { code: exit_code },
            now,
        )?;
    }

    // 3c. Deadline expiry, against the persisted absolute deadline.
    if snap.deadline_at.is_some_and(|dl| dl <= now) {
        if let Some(kind) = timeout_kind(snap.state) {
            snap = commit(
                &mut log,
                dir,
                snap,
                EventKind::Timeout,
                json!({"fsm": "Timeout", "kind": timeout_name(kind)}),
                &FsmEvent::Timeout { kind },
                now,
            )?;
        }
        snap.deadline_at = None;
    }

    // 4. Re-arm from the state we ended in, so the next reader fires the RIGHT
    //    deadline (a worker that moved running → awaiting-input must be guarded
    //    by the blocked timeout, not by the task timeout it inherited).
    arm_deadline(&mut snap, cfg);
    save_snapshot(dir, &snap).map_err(|e| io_err("writing state.json", e))?;
    Ok(snap)
}

// ------------------------------------------------------------------ the lock

/// Exclusive, nonblocking, retried. Held for the whole reconcile; released by
/// the kernel when the returned guard drops (or when this process dies).
fn lock_state(dir: &Path) -> Result<Flock<File>, AppError> {
    let path = dir.join(STATE_LOCK);
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .map_err(|e| io_err("opening state.lock", e))?;
    for _ in 0..LOCK_RETRY_TRIES {
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => return Ok(lock),
            Err((back, _errno)) => {
                file = back;
                std::thread::sleep(LOCK_RETRY_INTERVAL);
            }
        }
    }
    Err(AppError::new(
        "LOCK_TIMEOUT",
        ExitCode::CmdTimeout,
        format!(
            "could not acquire {} within {}s — another a2a process is holding it",
            path.display(),
            (LOCK_RETRY_TRIES as u128 * LOCK_RETRY_INTERVAL.as_millis()) / 1000
        ),
    )
    .remedy("re-run once the other command finishes; `a2a doctor` lists stale locks"))
}

// ---------------------------------------------------------------- the ingest

/// What one ingest pass learned that the caller has to act on.
struct Ingested {
    session: Option<SessionIds>,
    turn_completed: bool,
}

struct SessionIds {
    id: String,
    rollout_path: Option<String>,
}

/// Append every new, mappable exec line to the WAL as an envelope, advancing a
/// persisted byte watermark so no line is ever ingested twice.
///
/// Only whole lines are consumed: the shim may be mid-write, and half a JSON
/// object is not an event. Redaction happens *before* the parse, so the
/// credential never reaches `events.jsonl` even as an unparsed fragment.
fn ingest(
    log: &mut EventLog,
    dir: &Path,
    mut snap: WorkerSnapshot,
    now: DateTime<Utc>,
) -> Result<(WorkerSnapshot, Ingested), AppError> {
    let mut found = Ingested {
        session: None,
        turn_completed: false,
    };
    let path = exec::exec_stream_path(dir, snap.epoch);
    let key = format!("exec_offset_{}", snap.epoch);
    let mut offset = snap.extra.get(&key).and_then(Value::as_u64).unwrap_or(0);
    let len = match std::fs::metadata(&path) {
        Ok(m) => m.len(),
        Err(_) => return Ok((snap, found)), // no stream yet: nothing to ingest
    };

    let mut pending: Vec<Envelope> = Vec::new();
    if offset > len {
        // The stream shrank under us (truncated or replaced). We cannot know
        // what was lost, so we say so and resume from the current end rather
        // than replaying bytes that no longer mean what the watermark thought.
        let mut env = Envelope::new(
            snap.epoch,
            snap.transport,
            EventClass::Lifecycle,
            EventKind::EventsDropped,
            json!({"stream": path, "watermark": offset, "len": len,
                   "reason": "exec stream shrank below the ingest watermark"}),
            now,
        );
        log.append(&mut env)
            .map_err(|e| io_err("appending events_dropped", e))?;
        pending.push(env);
        offset = len;
    }

    let mut file = File::open(&path).map_err(|e| io_err("opening the exec stream", e))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| io_err("seeking the exec stream", e))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|e| io_err("reading the exec stream", e))?;
    let consumed = match buf.iter().rposition(|&b| b == b'\n') {
        Some(i) => i + 1,
        None => 0, // no complete line yet
    };

    for line in buf[..consumed].split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let text = redact::redact_egress(&String::from_utf8_lossy(line));
        let Ok(raw) = serde_json::from_str::<Value>(&text) else {
            continue; // not JSON (stray transport noise): consumed, not logged
        };
        let Some((class, kind)) = map_exec_line(&raw) else {
            continue; // token-level delta: dropped by contract
        };
        match kind {
            EventKind::SessionIdentified => {
                if let Some(ids) = extract_session(&raw) {
                    found.session = Some(ids);
                }
            }
            EventKind::TurnCompleted => found.turn_completed = true,
            _ => {}
        }
        let mut env = Envelope::new(snap.epoch, snap.transport, class, kind, raw, now);
        log.append(&mut env)
            .map_err(|e| io_err("appending an ingested event", e))?;
        pending.push(env);
    }

    offset += consumed as u64;
    snap.extra.insert(key, json!(offset));
    Ok((fold::fold(snap, &pending, now), found))
}

/// Session identification across the spellings the exec transport has used
/// (`session.created`, `session_configured`, `thread.started`).
fn extract_session(raw: &Value) -> Option<SessionIds> {
    let nested = ["session", "msg", "thread"];
    let id = pick_str(raw, &["session_id", "sessionId", "thread_id", "threadId", "id"]).or_else(
        || {
            nested
                .iter()
                .filter_map(|k| raw.get(k))
                .find_map(|v| pick_str(v, &["session_id", "thread_id", "id"]))
        },
    )?;
    let rollout_path = pick_str(raw, &["rollout_path", "rollout"]).or_else(|| {
        nested
            .iter()
            .filter_map(|k| raw.get(k))
            .find_map(|v| pick_str(v, &["rollout_path", "rollout"]))
    });
    Some(SessionIds { id, rollout_path })
}

fn pick_str(v: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|k| v.get(*k).and_then(Value::as_str))
        .map(str::to_string)
}

// ------------------------------------------------------------- observations

/// A result exists only if it has content — codex creates the
/// `--output-last-message` file before it has anything to put in it.
fn has_result(dir: &Path) -> bool {
    std::fs::metadata(dir.join("result.md"))
        .map(|m| m.len() > 0)
        .unwrap_or(false)
}

/// An outbox question is *unanswered* while its seq is beyond the last answer
/// we delivered. A question whose seq cannot be determined counts as
/// unanswered: parking a worker for a human is always recoverable, silently
/// failing one that asked a question is not.
fn has_unanswered_question(dir: &Path, last_answer_seq: u64) -> bool {
    let Ok(entries) = std::fs::read_dir(dir.join("outbox")) else {
        return false;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".question.json") {
            continue;
        }
        let seq = name
            .split('.')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                std::fs::read_to_string(entry.path())
                    .ok()
                    .and_then(|t| serde_json::from_str::<Value>(&t).ok())
                    .and_then(|v| v.get("seq").and_then(Value::as_u64))
            });
        match seq {
            Some(s) if s <= last_answer_seq => continue,
            _ => return true,
        }
    }
    false
}

// ------------------------------------------------------------- the WAL write

/// Append the event, fold it, checkpoint, then perform the transition's
/// actions — the spec §4.6 ordering (durable first, side effect second), so a
/// crash can only ever cost a replay, never an unrecorded kill.
fn commit(
    log: &mut EventLog,
    dir: &Path,
    snap: WorkerSnapshot,
    kind: EventKind,
    payload: Value,
    ev: &FsmEvent,
    now: DateTime<Utc>,
) -> Result<WorkerSnapshot, AppError> {
    // Preview the pure transition purely for its actions; the fold re-runs it
    // and is the authority on the resulting state (and records any rejection).
    let actions = match fsm::step(snap.state, snap.state_reason, ev) {
        Ok(t) => t.actions,
        Err(_) => Vec::new(),
    };
    let snap = append_and_fold(log, dir, snap, kind, payload, now)?;
    for action in actions {
        perform(&snap, action);
    }
    Ok(snap)
}

fn perform(snap: &WorkerSnapshot, action: Action) {
    match action {
        Action::KillProcess => {
            if let (Some(pid), Some(witness)) = (snap.pid, snap.pid_start_witness.as_deref()) {
                // Witness-checked inside: a recycled pid is never signalled.
                let _ = exec::kill_worker(pid, witness);
            }
        }
        // Inbox delivery is `send`'s job (Task 16); the escalation hook lands
        // with the broker. Both are listed so this match stays total.
        Action::DeliverInbox | Action::RecordEscalation => {}
    }
}

// --------------------------------------------------------------- deadlines

/// Which deadline the current state is guarded by; `None` = no deadline is
/// armed in this state, so an expiry is not ours to fire.
fn timeout_kind(state: WorkerState) -> Option<TimeoutKind> {
    match state {
        WorkerState::Spawned => Some(TimeoutKind::Start),
        WorkerState::Running => Some(TimeoutKind::Task),
        WorkerState::AwaitingInput => Some(TimeoutKind::Blocked),
        _ => None,
    }
}

fn timeout_name(kind: TimeoutKind) -> &'static str {
    match kind {
        TimeoutKind::Start => "start",
        TimeoutKind::Task => "task",
        TimeoutKind::Blocked => "blocked",
    }
}

/// Derive `deadline_at` from the state we are in and when we entered it.
///
/// Deriving (rather than incrementing) is what stops a deadline from sliding
/// forward one poll at a time: N reconciles of an unchanged worker compute the
/// same absolute instant. Terminal and escalated states carry no deadline.
fn arm_deadline(snap: &mut WorkerSnapshot, cfg: &Config) {
    let Some(lane) = cfg.lanes.get(&snap.lane) else {
        return;
    };
    let from = snap.entered_state_at;
    snap.deadline_at = match snap.state {
        WorkerState::Spawned => Some(from + Delta::seconds(lane.start_timeout_secs as i64)),
        WorkerState::Running => Some(from + Delta::seconds(lane.task_timeout_secs as i64)),
        WorkerState::AwaitingInput => {
            Some(from + Delta::seconds(lane.blocked_timeout_secs as i64))
        }
        _ => None,
    };
}

fn io_err(ctx: &str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Lane;

    #[test]
    fn timeout_kind_is_armed_per_state() {
        assert_eq!(timeout_kind(WorkerState::Spawned), Some(TimeoutKind::Start));
        assert_eq!(timeout_kind(WorkerState::Running), Some(TimeoutKind::Task));
        assert_eq!(
            timeout_kind(WorkerState::AwaitingInput),
            Some(TimeoutKind::Blocked)
        );
        for terminal in [
            WorkerState::Done,
            WorkerState::Failed,
            WorkerState::Cancelled,
            WorkerState::Escalated,
            WorkerState::Orphaned,
        ] {
            assert_eq!(timeout_kind(terminal), None);
        }
    }

    #[test]
    fn unanswered_question_is_seq_relative_to_the_last_answer() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("outbox")).unwrap();
        assert!(!has_unanswered_question(tmp.path(), 0));
        std::fs::write(
            tmp.path().join("outbox/2.question.json"),
            br#"{"seq":2,"question":"A or B?"}"#,
        )
        .unwrap();
        assert!(has_unanswered_question(tmp.path(), 1));
        assert!(!has_unanswered_question(tmp.path(), 2), "already answered");
        // a name we cannot read a seq out of is treated as unanswered
        std::fs::write(tmp.path().join("outbox/x.question.json"), b"{}").unwrap();
        assert!(has_unanswered_question(tmp.path(), 99));
    }

    #[test]
    fn result_must_have_content() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!has_result(tmp.path()));
        std::fs::write(tmp.path().join("result.md"), b"").unwrap();
        assert!(!has_result(tmp.path()), "an empty result is not a result");
        std::fs::write(tmp.path().join("result.md"), b"done\n").unwrap();
        assert!(has_result(tmp.path()));
    }

    #[test]
    fn session_ids_are_extracted_from_every_known_spelling() {
        let flat = extract_session(&json!({"type":"session.created","session_id":"s1",
                                           "rollout_path":"/r.jsonl"}))
        .unwrap();
        assert_eq!(flat.id, "s1");
        assert_eq!(flat.rollout_path.as_deref(), Some("/r.jsonl"));
        assert_eq!(
            extract_session(&json!({"type":"thread.started","thread_id":"t9"}))
                .unwrap()
                .id,
            "t9"
        );
        assert_eq!(
            extract_session(&json!({"type":"session_configured","session":{"id":"n1"}}))
                .unwrap()
                .id,
            "n1"
        );
        assert!(extract_session(&json!({"type":"session.created"})).is_none());
    }

    #[test]
    fn deadlines_are_derived_not_incremented() {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        let (cfg, _, _) = crate::config::load(&home).unwrap();
        let mut snap = WorkerSnapshot::new_for_test("general-0814t1300-aaaa", tmp.path());
        snap.lane = Lane::General;
        snap.state = WorkerState::Running;
        arm_deadline(&mut snap, &cfg);
        let first = snap.deadline_at.unwrap();
        arm_deadline(&mut snap, &cfg);
        assert_eq!(snap.deadline_at.unwrap(), first, "deadline must not slide");
        snap.state = WorkerState::Done;
        arm_deadline(&mut snap, &cfg);
        assert!(snap.deadline_at.is_none(), "terminal carries no deadline");
    }
}
