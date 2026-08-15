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
//!    A turn that produced a result under a `--schema` passes through the
//!    **schema gate** (spec §4.3) *before* the transition is committed, because
//!    a result that does not satisfy the schema must land the worker in
//!    `failed{schema_violation}` rather than `done` — and `done` is absorbing,
//!    so there is no second chance to demote it afterwards.
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
use crate::inbox;
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
use std::path::{Path, PathBuf};
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
        // Turn-boundary delivery FIRST (spec §4.3): if the operator queued
        // anything, this instant is when the transport can accept it, so the
        // batch goes out and the worker keeps running. No `TurnEnded` is
        // computed — a queued steer must not have to be followed by a manual
        // `a2a send` to un-park the worker it was meant to steer.
        if let Some(next) = deliver_at_turn_boundary(&mut log, dir, &snap, cfg, now)? {
            snap = next;
            // A worker we just restarted is emphatically not dead.
            turn_concluded = true;
        } else {
            let has_result = has_result(dir);
            let has_question = has_unanswered_question(dir, snap.last_delivered_seq);
            if has_result || has_question || !death_observed {
                // The gate runs only when this turn actually produced a result
                // AND the worker was spawned with a schema; everything else
                // keeps the plain `result.md`-is-the-result rule (spec §4.3).
                let gate = match (has_result, snap.schema_path.clone()) {
                    (true, Some(schema)) => Some(schema_gate(dir, &schema)?),
                    _ => None,
                };
                snap = match gate {
                    // Failed the gate: the raw text is preserved, the worker
                    // fails *resumably*, and `done` is never reached.
                    Some(Gate::Violation { detail }) => {
                        let detail = redact::redact_egress(&detail);
                        commit(
                            &mut log,
                            dir,
                            snap,
                            EventKind::StateChange,
                            json!({"fsm": "ResultInvalid", "detail": detail}),
                            &FsmEvent::ResultInvalid {
                                detail: detail.clone(),
                            },
                            now,
                        )?
                    }
                    passed => {
                        if let Some(Gate::Pass { path, rotated }) = passed {
                            // Bookkeeping, not a transition: no `fsm` key, so
                            // the fold consumes the seq and changes nothing.
                            snap = append_and_fold(
                                &mut log,
                                dir,
                                snap,
                                EventKind::ResultWritten,
                                json!({"path": path, "rotated_prior_to": rotated}),
                                now,
                            )?;
                        }
                        commit(
                            &mut log,
                            dir,
                            snap,
                            EventKind::StateChange,
                            json!({"fsm": "TurnEnded", "has_result": has_result,
                                   "has_question": has_question}),
                            &FsmEvent::TurnEnded {
                                has_result,
                                has_question,
                            },
                            now,
                        )?
                    }
                };
                turn_concluded = true;
            }
        }
    }

    // 3b. Process death. Only from the two states that own a live process —
    //     in exec transport the child legitimately exits at a turn boundary, so
    //     awaiting-input/escalated must never read as a death.
    if !turn_concluded
        && death_observed
        && matches!(snap.state, WorkerState::Running | WorkerState::Spawned)
    {
        // A RESUMED epoch whose process died without ever starting a turn did
        // not "run and produce nothing" — codex refused the session (deleted or
        // archived out from under us), and spec §7 gives that its own
        // `failure_class` so an orchestrator reports it distinctly instead of
        // retrying a conversation that no longer exists.
        snap = if snap.state == WorkerState::Running && session_missing(dir, snap.epoch) {
            let session = snap.session_id.clone();
            commit(
                &mut log,
                dir,
                snap,
                EventKind::StateChange,
                json!({"fsm": "SessionMissing", "session_id": session, "code": exit_code}),
                &FsmEvent::SessionMissing,
                now,
            )?
        } else {
            commit(
                &mut log,
                dir,
                snap,
                EventKind::StateChange,
                json!({"fsm": "ProcessExited", "code": exit_code}),
                &FsmEvent::ProcessExited { code: exit_code },
                now,
            )?
        };
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
///
/// Public because every mutating command (`send` today, `kill`/`resume` next)
/// serializes on the SAME lock a reconcile takes — one worker, one writer.
pub fn lock_state(dir: &Path) -> Result<Flock<File>, AppError> {
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
///
/// Known gap (§4.5 says every worker file is 0600): `result.md` is created by
/// the codex *child*, so it lands under that child's umask — 0644 in practice.
/// The schema gate tightens it because the gate legitimately owns the file at
/// that moment; without `--schema` nothing rewrites it, and reconcile is a read
/// path that must not silently mutate modes. `doctor` (Task 19) is where a
/// world-readable `result.md` gets reported.
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

/// Did `epoch` begin with an `a2a resume` that never saw a turn start?
///
/// Both halves are read from the WAL rather than remembered in the snapshot, so
/// the answer survives a rebuild-from-zero:
///
/// * the `Resumed` event that bumped INTO this epoch was appended while the
///   snapshot still carried the previous one, so it is stamped `epoch - 1`;
/// * `TurnStarted` is the first thing a healthy resumed session emits, and
///   ingest stamps every exec line with the epoch it was read under.
///
/// A resumed process that exits with neither is the `session_missing` shape
/// (spec §7): codex could not open the conversation, so nothing ever started.
fn session_missing(dir: &Path, epoch: u32) -> bool {
    let Ok(events) = crate::store::read_events(dir, 0) else {
        return false; // unreadable log: fall back to the ordinary death path
    };
    let mut resumed = false;
    for env in &events {
        if env.kind == EventKind::TurnStarted && env.epoch == epoch {
            return false; // this epoch did start a turn — a real run died
        }
        if env.kind == EventKind::Resumed
            && env.epoch + 1 == epoch
            && env.raw.get("fsm").and_then(Value::as_str) == Some("Resume")
        {
            resumed = true;
        }
    }
    resumed
}

// -------------------------------------------------------- inbox at turn end

/// Hand any queued `inbox/` messages to the worker at the turn boundary and
/// return the resulting snapshot; `None` means there was nothing to deliver and
/// the caller should conclude the turn normally.
///
/// Two deliberate non-failures:
///
/// * **No session id** — nothing to resume, so the messages stay queued and the
///   worker parks in `awaiting-input`, where an operator can see them. A read
///   path must not turn "cannot deliver yet" into "cannot read this worker".
/// * **Unreadable inbox** — same reasoning: park the worker, let `doctor`
///   report the directory.
///
/// A delivery that *starts* and then fails (no prompt file, no fork) IS an
/// error: the worker's transport is broken, and silently reporting `done` on a
/// turn we tried to extend would be a lie.
fn deliver_at_turn_boundary(
    log: &mut EventLog,
    dir: &Path,
    snap: &WorkerSnapshot,
    cfg: &Config,
    now: DateTime<Utc>,
) -> Result<Option<WorkerSnapshot>, AppError> {
    if snap.session_id.is_none() {
        return Ok(None);
    }
    match inbox::undelivered(dir, snap.last_delivered_seq) {
        Ok(pending) if pending.is_empty() => return Ok(None),
        Err(_) => return Ok(None),
        Ok(_) => {}
    }
    let delivery = inbox::deliver(dir, cfg, snap)?;
    Ok(Some(append_and_fold(
        log,
        dir,
        snap.clone(),
        EventKind::InboxDelivered,
        json!({
            "watermark": delivery.watermark,
            "seqs": delivery.seqs,
            "pid": delivery.pid,
            "start_witness": delivery.start_witness,
        }),
        now,
    )?))
}

// ------------------------------------------------------------ the schema gate

/// The name every schema-conforming result lands under.
pub const RESULT_JSON: &str = "result.json";
/// Where a schema-violating result's raw text is preserved.
pub const RESULT_RAW: &str = "result.raw.md";
/// What codex's `--output-last-message` writes.
pub const RESULT_MD: &str = "result.md";

/// What the §4.3 gate decided about a completed turn's `result.md`.
enum Gate {
    /// Conforming: parsed JSON is now `result.json`, any prior one rotated.
    Pass {
        path: PathBuf,
        rotated: Option<PathBuf>,
    },
    /// Non-conforming: `result.md` is preserved as `result.raw.md` and the
    /// worker fails `schema_violation` — resumable, since the session survives.
    Violation { detail: String },
}

/// Spec §4.3, Phase 1 subset: parse `result.md` as JSON and require the
/// schema's **top-level `required` keys**. Full JSON-Schema validation is a
/// Phase 2+ upgrade (and a `doctor` note) — the point of the Phase 1 gate is
/// that a worker can never silently report `done` with a result the
/// orchestrator cannot parse.
///
/// `Err` is reserved for *our* IO failures. Everything the worker (or a human
/// editing the schema) can get wrong — unparseable result, missing keys, a
/// schema file that has since become unreadable — is a `Violation`, so the
/// worker ends in a state a human can read and repair with `a2a resume`,
/// never in a reconcile that errors forever.
fn schema_gate(dir: &Path, schema_path: &Path) -> Result<Gate, AppError> {
    let schema = match std::fs::read_to_string(schema_path)
        .ok()
        .and_then(|t| serde_json::from_str::<Value>(&t).ok())
    {
        Some(v) => v,
        None => {
            return violation(
                dir,
                format!(
                    "--schema {} is missing or is not valid JSON",
                    schema_path.display()
                ),
            )
        }
    };
    let text = std::fs::read_to_string(dir.join(RESULT_MD))
        .map_err(|e| io_err("reading result.md", e))?;
    let parsed = match serde_json::from_str::<Value>(&text) {
        Ok(v) => v,
        // The serde message names a position, never content — the result text
        // itself must not travel into events.jsonl through an error string.
        Err(e) => return violation(dir, format!("result.md is not JSON: {e}")),
    };
    let required: Vec<&str> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if !required.is_empty() {
        let Some(obj) = parsed.as_object() else {
            return violation(
                dir,
                format!(
                    "result.md is a JSON {}, but the schema requires an object with key(s): {}",
                    type_name(&parsed),
                    required.join(", ")
                ),
            );
        };
        let missing: Vec<&str> = required
            .iter()
            .copied()
            .filter(|k| !obj.contains_key(*k))
            .collect();
        if !missing.is_empty() {
            return violation(
                dir,
                format!("result.md is missing required key(s): {}", missing.join(", ")),
            );
        }
    }

    let rotated = rotate_result_json(dir)?;
    let mut bytes = serde_json::to_vec_pretty(&parsed).map_err(|e| {
        AppError::new(
            "IO",
            ExitCode::Internal,
            format!("re-serializing the result: {e}"),
        )
    })?;
    bytes.push(b'\n');
    let path = dir.join(RESULT_JSON);
    // write_atomic creates the temp at 0600, so result.json is 0600 by
    // construction — unlike result.md, which codex creates under the child's
    // umask; the gate is the one place that owns it, so it tightens it here.
    crate::store::write_atomic(&path, &bytes).map_err(|e| io_err("writing result.json", e))?;
    chmod_0600(&dir.join(RESULT_MD));
    Ok(Gate::Pass { path, rotated })
}

/// Preserve the raw text at `result.raw.md` (0600) and report the violation.
fn violation(dir: &Path, detail: String) -> Result<Gate, AppError> {
    let raw = dir.join(RESULT_RAW);
    let md = dir.join(RESULT_MD);
    if md.exists() {
        std::fs::rename(&md, &raw).map_err(|e| io_err("preserving result.raw.md", e))?;
        chmod_0600(&raw);
    }
    Ok(Gate::Violation { detail })
}

/// `result.json` → `result.<n>.json` for the smallest free `n` (spec §4.3:
/// priors are kept, never overwritten). `None` = there was nothing to rotate.
fn rotate_result_json(dir: &Path) -> Result<Option<PathBuf>, AppError> {
    let current = dir.join(RESULT_JSON);
    if !current.exists() {
        return Ok(None);
    }
    for n in 1..u32::MAX {
        let candidate = dir.join(format!("result.{n}.json"));
        if candidate.exists() {
            continue;
        }
        std::fs::rename(&current, &candidate)
            .map_err(|e| io_err("rotating the prior result.json", e))?;
        return Ok(Some(candidate));
    }
    unreachable!("u32::MAX prior results is not a reachable state")
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Best effort by design: a mode we could not tighten is a `doctor` finding,
/// never a reason to fail the reconcile that just recorded the result.
fn chmod_0600(p: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600));
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
        // `DeliverInbox` is performed by `send` itself (it owns the queue write
        // that precedes it) and by `deliver_at_turn_boundary` above, both of
        // which record the resulting watermark. Reconcile never reaches this
        // arm with a Send event, and the escalation hook lands with the broker.
        // Both are listed so this match stays total.
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
///
/// Public for the same reason as [`lock_state`]: a command that commits a
/// transition outside `reconcile` must re-arm from the state it landed in, or
/// it leaves the previous state's deadline pointed at the new one.
pub fn arm_deadline(snap: &mut WorkerSnapshot, cfg: &Config) {
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
    use std::os::unix::fs::PermissionsExt;

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

    /// Helper: a worker dir with `result.md` and a schema file beside it.
    fn gate_fixture(result: &str, schema: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(RESULT_MD), result).unwrap();
        let schema_path = tmp.path().join("schema.json");
        std::fs::write(&schema_path, schema).unwrap();
        (tmp, schema_path)
    }

    fn mode_of(p: &Path) -> u32 {
        std::fs::metadata(p).unwrap().permissions().mode() & 0o777
    }

    fn chmod(p: &Path, mode: u32) {
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    fn a_conforming_result_becomes_result_json_at_0600() {
        let (tmp, schema) = gate_fixture(
            r#"{"status":"pass","iterations":3}"#,
            r#"{"type":"object","required":["status"]}"#,
        );
        // codex writes result.md under the child's umask; the gate owns it now
        chmod(&tmp.path().join(RESULT_MD), 0o644);
        let gate = schema_gate(tmp.path(), &schema).unwrap();
        let Gate::Pass { path, rotated } = gate else {
            panic!("a conforming result must pass the gate");
        };
        assert_eq!(path, tmp.path().join(RESULT_JSON));
        assert!(rotated.is_none(), "nothing to rotate on the first result");
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written["status"], json!("pass"));
        assert_eq!(mode_of(&path), 0o600);
        assert_eq!(mode_of(&tmp.path().join(RESULT_MD)), 0o600, "tightened");
        assert!(!tmp.path().join(RESULT_RAW).exists());
    }

    #[test]
    fn priors_rotate_to_result_n_json_and_are_never_overwritten() {
        let (tmp, schema) = gate_fixture(
            r#"{"status":"pass","n":2}"#,
            r#"{"type":"object","required":["status"]}"#,
        );
        std::fs::write(tmp.path().join(RESULT_JSON), br#"{"status":"first"}"#).unwrap();
        let Gate::Pass { rotated, .. } = schema_gate(tmp.path(), &schema).unwrap() else {
            panic!("must pass");
        };
        assert_eq!(rotated, Some(tmp.path().join("result.1.json")));
        let prior: Value =
            serde_json::from_str(&std::fs::read_to_string(rotated.unwrap()).unwrap()).unwrap();
        assert_eq!(prior["status"], json!("first"), "prior kept verbatim");
        // a third turn rotates to .2, leaving .1 alone
        std::fs::write(tmp.path().join(RESULT_MD), br#"{"status":"pass","n":3}"#).unwrap();
        let Gate::Pass { rotated, .. } = schema_gate(tmp.path(), &schema).unwrap() else {
            panic!("must pass");
        };
        assert_eq!(rotated, Some(tmp.path().join("result.2.json")));
        assert_eq!(
            serde_json::from_str::<Value>(
                &std::fs::read_to_string(tmp.path().join("result.1.json")).unwrap()
            )
            .unwrap()["status"],
            json!("first")
        );
    }

    #[test]
    fn every_violation_shape_preserves_the_raw_text_at_0600() {
        let required = r#"{"type":"object","required":["status"]}"#;
        for (result, expect) in [
            ("not json at all", "not JSON"),
            (r#"{"iterations":3}"#, "missing required key"),
            (r#"["status"]"#, "requires an object"),
        ] {
            let (tmp, schema) = gate_fixture(result, required);
            let Gate::Violation { detail } = schema_gate(tmp.path(), &schema).unwrap() else {
                panic!("`{result}` must not pass the gate");
            };
            assert!(detail.contains(expect), "{detail}");
            assert!(!detail.contains(result), "detail must not quote the result");
            assert!(!tmp.path().join(RESULT_MD).exists(), "moved, not copied");
            assert_eq!(
                std::fs::read_to_string(tmp.path().join(RESULT_RAW)).unwrap(),
                result
            );
            assert_eq!(mode_of(&tmp.path().join(RESULT_RAW)), 0o600);
            assert!(!tmp.path().join(RESULT_JSON).exists());
        }
    }

    #[test]
    fn a_schema_with_no_required_keys_only_demands_parseable_json() {
        let (tmp, schema) = gate_fixture("[1, 2, 3]", r#"{"type":"array"}"#);
        assert!(matches!(
            schema_gate(tmp.path(), &schema).unwrap(),
            Gate::Pass { .. }
        ));
    }

    #[test]
    fn an_unreadable_schema_fails_the_worker_rather_than_passing_it_unvalidated() {
        let (tmp, _) = gate_fixture(r#"{"status":"pass"}"#, "{}");
        let missing = tmp.path().join("gone.json");
        let Gate::Violation { detail } = schema_gate(tmp.path(), &missing).unwrap() else {
            panic!("an unvalidatable result must never read as done");
        };
        assert!(detail.contains("gone.json"), "{detail}");
        assert!(tmp.path().join(RESULT_RAW).exists());
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
