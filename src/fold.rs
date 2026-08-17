//! WAL fold and crash recovery (spec §4.6).
//!
//! `events.jsonl` is the source of truth; `state.json` is a derived
//! checkpoint. [`fold`] is the one function that turns the log into state, and
//! it is **pure** (the clock is injected) so `fold(events[..k])` for every
//! prefix `k` is testable against incremental application.
//!
//! Three invariants make replay safe on any crash boundary:
//!
//! * **Genesis is special.** The `spawn` event creates a worker, so it cannot
//!   go through [`fsm::step`] (there is no prior state to step from). It seeds
//!   identity directly and sets `spawned`.
//! * **Seq dedup.** Events carry a monotonic `seq`; anything at or below
//!   `last_applied_seq` is already reflected in the snapshot and is skipped.
//!   Re-applying is a no-op, never an error — recovery must be idempotent
//!   because it runs on every read path.
//! * **Epoch fencing.** Every spawn/resume bumps `epoch`. A buffer flushed by
//!   a dead prior run must not resurrect (or complete!) a resumed worker, so
//!   stale-epoch events advance the seq watermark — they ARE consumed — but
//!   never touch state.
//!
//! And one rule that outranks all of them: **the fold never errors.** A
//! rejected transition inside a replay is recorded into
//! `extra["fold_rejections"]` and the fold continues, because recovery must
//! always terminate in a state a human can read and act on.

use crate::events::{self, Envelope, EventClass, Transport};
use crate::fsm::{self, FsmEvent, TimeoutKind, WorkerState};
use crate::output::{AppError, ExitCode};
use crate::state::{load_snapshot, save_snapshot, SnapshotError, WorkerSnapshot};
use crate::store;
use chrono::{DateTime, Utc};
use std::path::Path;
use std::str::FromStr;

/// Bounded audit trail: a crash-looping worker must not grow `state.json`
/// without limit, but the most recent rejections are diagnostic gold.
const MAX_RECORDED_REJECTIONS: usize = 32;

/// Lifecycle envelopes carry their [`FsmEvent`] payload in `raw` as
/// `{"fsm":"<Variant>", ...fields}`. Trace events and unrecognized names
/// yield `None` — observability never drives the machine.
pub fn envelope_to_fsm_event(env: &Envelope) -> Option<FsmEvent> {
    if env.class != EventClass::Lifecycle {
        return None;
    }
    let raw = &env.raw;
    let s = |k: &str| raw.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let b = |k: &str| raw.get(k).and_then(|v| v.as_bool()).unwrap_or(false);
    Some(match raw.get("fsm").and_then(|v| v.as_str())? {
        "Spawn" => FsmEvent::Spawn {
            id: s("id").unwrap_or_default(),
        },
        "Admit" => FsmEvent::Admit,
        "ProcessStarted" => FsmEvent::ProcessStarted {
            pid: raw.get("pid").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
            start_witness: s("start_witness").unwrap_or_default(),
        },
        "SessionIdentified" => FsmEvent::SessionIdentified {
            session_id: s("session_id").unwrap_or_default(),
            rollout_path: s("rollout_path"),
        },
        "TurnEnded" => FsmEvent::TurnEnded {
            has_result: b("has_result"),
            has_question: b("has_question"),
        },
        "ResultInvalid" => FsmEvent::ResultInvalid {
            detail: s("detail").unwrap_or_default(),
        },
        "ProcessExited" => FsmEvent::ProcessExited {
            code: raw.get("code").and_then(|v| v.as_i64()).map(|c| c as i32),
        },
        "SessionMissing" => FsmEvent::SessionMissing,
        "Timeout" => FsmEvent::Timeout {
            kind: match raw.get("kind").and_then(|v| v.as_str()) {
                Some("start") => TimeoutKind::Start,
                Some("blocked") => TimeoutKind::Blocked,
                _ => TimeoutKind::Task,
            },
        },
        "Interrupt" => FsmEvent::Interrupt,
        "Kill" => FsmEvent::Kill,
        "OrphanDetected" => FsmEvent::OrphanDetected,
        "Resume" => FsmEvent::Resume {
            answer_queued: b("answer_queued"),
        },
        "Send" => FsmEvent::Send,
        _ => return None, // written by a newer a2a: consume the seq, change nothing
    })
}

/// Replay `events` onto `snap`. Pure, total, and idempotent.
pub fn fold(mut snap: WorkerSnapshot, events: &[Envelope], now: DateTime<Utc>) -> WorkerSnapshot {
    for env in events {
        // seq 0 = never stamped by `EventLog::append` (a synthetic event folded
        // before it was logged): apply it, but leave the watermark alone.
        let stamped = env.seq != 0;
        if stamped && env.seq <= snap.last_applied_seq {
            continue; // already reflected in the snapshot — advance nothing
        }

        if is_genesis(env) {
            seed_from_genesis(&mut snap, env, now);
        } else if env.epoch >= snap.epoch {
            if let Some(ev) = envelope_to_fsm_event(env) {
                apply_event(&mut snap, env, &ev, now);
            } else if matches!(
                env.kind,
                events::EventKind::InboxDelivered | events::EventKind::TransportRestarted
            ) {
                apply_transport_identity(&mut snap, env);
            }
        }
        // else: stale epoch — fenced. The seq is still consumed below.

        if stamped {
            snap.last_applied_seq = env.seq;
            snap.last_event_seq = snap.last_event_seq.max(env.seq);
        }
        snap.updated_at = now;
    }
    snap
}

fn is_genesis(env: &Envelope) -> bool {
    env.class == EventClass::Lifecycle
        && env.raw.get("fsm").and_then(|v| v.as_str()) == Some("Spawn")
}

/// The genesis event is the ONLY writer of identity. Everything a worker is —
/// which lane, which cwd, which sandbox, which model — comes from here, so a
/// snapshot rebuilt from zero is identical to the original.
fn seed_from_genesis(snap: &mut WorkerSnapshot, env: &Envelope, now: DateTime<Utc>) {
    let raw = &env.raw;
    let s = |k: &str| raw.get(k).and_then(|v| v.as_str());

    if let Some(id) = s("id") {
        snap.id = id.to_string();
    }
    if let Some(lane) = s("lane").and_then(|l| crate::config::Lane::from_str(l).ok()) {
        snap.lane = lane;
    }
    if let Some(cwd) = s("cwd") {
        snap.cwd = cwd.into();
    }
    if let Some(sha) = s("brief_sha") {
        snap.brief_sha = sha.to_string();
    }
    if let Some(sandbox) = s("sandbox").and_then(|m| crate::config::SandboxMode::from_str(m).ok()) {
        snap.sandbox = sandbox;
    }
    if let Some(net) = raw.get("network").and_then(|v| v.as_bool()) {
        snap.network = net;
    }
    if let Some(model) = s("model") {
        snap.model = model.to_string();
    }
    if let Some(effort) = s("effort") {
        snap.effort = effort.to_string();
    }
    if let Some(t) = raw.get("transport") {
        if let Ok(t) = serde_json::from_value::<Transport>(t.clone()) {
            snap.transport = t;
            snap.requested_transport = t;
        }
    }
    snap.created_at = s("created_at")
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&Utc))
        .unwrap_or(env.ts);
    snap.state = WorkerState::Spawned;
    snap.state_reason = None;
    snap.failure_class = None;
    snap.entered_state_at = snap.created_at;
    snap.updated_at = now;
}

/// Transport bookkeeping: events that move no state but replace the worker's
/// process, and therefore must still reach the snapshot.
///
/// Two events land here, for the same reason. An **inbox delivery** resumes the
/// worker inside the current epoch; a **resume** starts a new epoch on the same
/// session. Neither has an FSM row that could carry a pid (the machine is
/// already in `running`), and neither may be lost: the snapshot has to follow
/// the pid/witness pair or a later `a2a kill` would aim at the previous,
/// already-dead process. Losing a delivery's watermark would additionally
/// re-deliver messages the worker already read after any snapshot rebuild.
///
/// The watermark only ever moves FORWARD (`max`), so replaying an older
/// delivery after a newer one cannot un-acknowledge a message.
fn apply_transport_identity(snap: &mut WorkerSnapshot, env: &Envelope) {
    if let Some(w) = env.raw.get("watermark").and_then(|v| v.as_u64()) {
        snap.last_delivered_seq = snap.last_delivered_seq.max(w);
    }
    if let Some(pid) = env.raw.get("pid").and_then(|v| v.as_u64()) {
        snap.pid = Some(pid as u32);
        snap.pid_start_witness = env
            .raw
            .get("start_witness")
            .and_then(|v| v.as_str())
            .map(str::to_string);
    }
}

fn apply_event(snap: &mut WorkerSnapshot, env: &Envelope, ev: &FsmEvent, now: DateTime<Utc>) {
    match fsm::step(snap.state, snap.state_reason, ev) {
        Ok(t) => {
            // Identity the event carries, recorded regardless of the state edge.
            match ev {
                FsmEvent::ProcessStarted { pid, start_witness } => {
                    snap.pid = Some(*pid);
                    snap.pid_start_witness = Some(start_witness.clone());
                }
                FsmEvent::SessionIdentified {
                    session_id,
                    rollout_path,
                } => {
                    snap.session_id = Some(session_id.clone());
                    snap.rollout_path = rollout_path.as_ref().map(Into::into);
                }
                _ => {}
            }
            if t.state != snap.state {
                snap.entered_state_at = now;
            }
            snap.state = t.state;
            snap.state_reason = t.reason;
            snap.failure_class = t.failure.map(|class| refine_failure_class(env, class));
            if t.epoch_bump {
                snap.epoch += 1;
            }
        }
        Err(rejected) => record_rejection(snap, env, &rejected),
    }
}

/// The FSM decides the STATE; the row may refine the DIAGNOSIS.
///
/// `fsm::step` cannot know *why* a process exited — from `running` it can only
/// say `no_result` — but the writer often does: the broker knows a child died
/// because its credentials expired, or because that worker's actor panicked.
/// A lifecycle row may therefore carry `"failure_class": "<snake_case>"`, which
/// is honoured **only** on a transition that already lands in `failed`.
///
/// Deliberately in the fold rather than as a post-hoc snapshot patch: the class
/// lives in the WAL, so a snapshot rebuilt from zero reports the same failure a
/// month later. An unparseable or unknown value keeps the machine's own answer.
fn refine_failure_class(env: &Envelope, class: fsm::FailureClass) -> fsm::FailureClass {
    env.raw
        .get("failure_class")
        .cloned()
        .and_then(|v| serde_json::from_value::<fsm::FailureClass>(v).ok())
        .filter(|refined| *refined != fsm::FailureClass::Unknown)
        .unwrap_or(class)
}

/// A rejection during replay is data, not an error: the log said something the
/// machine could not accept, and the human needs to see exactly what.
fn record_rejection(snap: &mut WorkerSnapshot, env: &Envelope, rejected: &fsm::Rejected) {
    let entry = serde_json::json!({
        "seq": env.seq,
        "epoch": env.epoch,
        "from": rejected.from.as_str(),
        "event": rejected.event,
        "reason": rejected.reason,
    });
    let list = snap
        .extra
        .entry("fold_rejections".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    if let Some(arr) = list.as_array_mut() {
        arr.push(entry);
        if arr.len() > MAX_RECORDED_REJECTIONS {
            let drop = arr.len() - MAX_RECORDED_REJECTIONS;
            arr.drain(..drop);
        }
    }
}

/// The WAL helper every mutating command goes through: append + fsync FIRST,
/// then fold, then checkpoint (spec §4.6 ordering — the log leads, the
/// snapshot follows, so a crash between them costs only a replay).
///
/// A convenience over [`append_envelope_and_fold`] for the overwhelmingly
/// common case: a **lifecycle** envelope built from `snap`'s own epoch and
/// transport, carrying an `{"fsm": …}` payload.
pub fn append_and_fold(
    log: &mut store::EventLog,
    worker_dir: &Path,
    snap: WorkerSnapshot,
    kind: events::EventKind,
    fsm_payload: serde_json::Value,
    now: DateTime<Utc>,
) -> Result<WorkerSnapshot, AppError> {
    let mut env = Envelope::new(
        snap.epoch,
        snap.transport,
        EventClass::Lifecycle,
        kind,
        fsm_payload,
        now,
    );
    append_envelope_and_fold(log, worker_dir, snap, &mut env, now)
}

/// The same append → fold → checkpoint sequence for an envelope the **caller**
/// built, of any [`EventClass`].
///
/// The broker's `WorkerActor` needs this: an app-server notification arrives as
/// a whole envelope (its `raw` is the notification's own params, which is what
/// [`events::synthesize_result`] later reads), so there is nothing for
/// [`append_and_fold`] to construct. `env` is taken by `&mut` because
/// [`store::EventLog::append`] stamps the seq into it, and the caller needs the
/// stamped envelope back (a trace-tail subscriber forwards exactly what was
/// written).
///
/// Folding a non-lifecycle envelope is safe by construction:
/// [`envelope_to_fsm_event`] returns `None` for anything that is not
/// `lifecycle`, so a trace envelope routed here consumes its seq and changes no
/// state. Pure trace envelopes should still go through
/// [`store::EventLog::append`] directly — observability must never pay for a
/// snapshot write, let alone drive the machine.
pub fn append_envelope_and_fold(
    log: &mut store::EventLog,
    worker_dir: &Path,
    snap: WorkerSnapshot,
    env: &mut Envelope,
    now: DateTime<Utc>,
) -> Result<WorkerSnapshot, AppError> {
    log.append(env)
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("appending event: {e}")))?;
    let snap = fold(snap, std::slice::from_ref(env), now);
    save_snapshot(worker_dir, &snap)
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("writing state.json: {e}")))?;
    Ok(snap)
}

/// What the caller observed about the worker's process. Injected so [`recover`]
/// stays pure of `/proc`, `flock`, and signals.
pub struct LivenessInput {
    pub process_alive: bool,
}

/// Rebuild a worker's state after any crash (spec §4.6 recovery table).
///
/// * snapshot present → fold only the events it has not seen;
/// * snapshot missing or corrupt → fold the whole log from zero;
/// * neither snapshot nor events → the process never got far enough to log
///   anything: `failed{never_started}` (an exec fork failure must be
///   distinguishable from a healthy worker);
/// * `running` with a dead process → `orphaned`.
///
/// Read-only by design: committing what it observed is `reconcile`'s job,
/// under `state.lock`, through the same WAL path.
pub fn recover(
    worker_dir: &Path,
    live: LivenessInput,
    now: DateTime<Utc>,
) -> Result<WorkerSnapshot, AppError> {
    let id = worker_dir
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();

    let (mut snap, since, had_snapshot) = match load_snapshot(worker_dir) {
        Ok(s) => {
            let since = s.last_applied_seq;
            (s, since, true)
        }
        // Recoverable: the log outranks the checkpoint.
        Err(SnapshotError::Missing) | Err(SnapshotError::Corrupt) => {
            (WorkerSnapshot::blank(&id, worker_dir), 0, false)
        }
        Err(e) => return Err(e.into()),
    };

    let events = store::read_events(worker_dir, since)
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading events: {e}")))?;

    if !had_snapshot && events.is_empty() {
        // Nothing was ever durably recorded for this worker.
        snap.state = WorkerState::Failed;
        snap.state_reason = None;
        snap.failure_class = Some(fsm::FailureClass::NeverStarted);
        snap.entered_state_at = now;
        snap.updated_at = now;
        return Ok(snap);
    }

    snap = fold(snap, &events, now);

    if snap.state == WorkerState::Running && !live.process_alive {
        let mut env = Envelope::new(
            snap.epoch,
            snap.transport,
            EventClass::Lifecycle,
            events::EventKind::Orphaned,
            serde_json::json!({"fsm": "OrphanDetected"}),
            now,
        );
        env.seq = snap.last_applied_seq; // observed, not yet logged: watermark unchanged
        apply_event(&mut snap, &env, &FsmEvent::OrphanDetected, now);
        snap.updated_at = now;
    }

    Ok(snap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventKind;
    use serde_json::json;

    fn ev(payload: serde_json::Value, epoch: u32, seq: u64) -> Envelope {
        let mut e = Envelope::new(
            epoch,
            Transport::Exec,
            EventClass::Lifecycle,
            EventKind::StateChange,
            payload,
            Utc::now(),
        );
        e.seq = seq;
        e
    }

    #[test]
    fn trace_and_unknown_payloads_map_to_none() {
        let mut trace = ev(json!({"fsm":"Kill"}), 1, 1);
        trace.class = EventClass::Trace;
        assert!(envelope_to_fsm_event(&trace).is_none());
        assert!(envelope_to_fsm_event(&ev(json!({"fsm":"FromTheFuture"}), 1, 1)).is_none());
        assert!(envelope_to_fsm_event(&ev(json!({"no":"fsm"}), 1, 1)).is_none());
    }

    #[test]
    fn every_fsm_variant_round_trips_through_the_payload_mapping() {
        for (payload, name) in [
            (json!({"fsm":"Spawn","id":"x"}), "spawn"),
            (json!({"fsm":"Admit"}), "admit"),
            (
                json!({"fsm":"ProcessStarted","pid":7,"start_witness":"w"}),
                "process_started",
            ),
            (
                json!({"fsm":"SessionIdentified","session_id":"s"}),
                "session_identified",
            ),
            (json!({"fsm":"TurnEnded"}), "turn_ended"),
            (json!({"fsm":"ResultInvalid","detail":"d"}), "result_invalid"),
            (json!({"fsm":"ProcessExited","code":1}), "process_exited"),
            (json!({"fsm":"SessionMissing"}), "session_missing"),
            (json!({"fsm":"Timeout","kind":"blocked"}), "timeout"),
            (json!({"fsm":"Interrupt"}), "interrupt"),
            (json!({"fsm":"Kill"}), "kill"),
            (json!({"fsm":"OrphanDetected"}), "orphan_detected"),
            (json!({"fsm":"Resume","answer_queued":true}), "resume"),
            (json!({"fsm":"Send"}), "send"),
        ] {
            let mapped = envelope_to_fsm_event(&ev(payload.clone(), 1, 1))
                .unwrap_or_else(|| panic!("{payload} did not map"));
            assert_eq!(fsm::event_name(&mapped), name);
        }
    }

    #[test]
    fn rejected_transitions_are_recorded_and_never_error() {
        let tmp = tempfile::tempdir().unwrap();
        let snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        // `send` to a spawned worker is a rejection, not a panic and not an error
        let after = fold(snap, &[ev(json!({"fsm":"Send"}), 1, 1)], Utc::now());
        assert_eq!(after.state, WorkerState::Spawned);
        assert_eq!(after.last_applied_seq, 1, "a rejected event still consumes its seq");
        let rejections = after.extra["fold_rejections"].as_array().unwrap();
        assert_eq!(rejections.len(), 1);
        assert_eq!(rejections[0]["event"], json!("send"));
        assert_eq!(rejections[0]["from"], json!("spawned"));
    }

    #[test]
    fn resume_bumps_the_epoch_so_the_prior_run_is_fenced() {
        let tmp = tempfile::tempdir().unwrap();
        let mut snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        snap.state = WorkerState::Done;
        let after = fold(snap, &[ev(json!({"fsm":"Resume"}), 1, 1)], Utc::now());
        assert_eq!(after.state, WorkerState::Running);
        assert_eq!(after.epoch, 2);
        // an in-flight event from the dead epoch-1 run cannot complete it
        let stale = fold(
            after.clone(),
            &[ev(json!({"fsm":"TurnEnded","has_result":true}), 1, 2)],
            Utc::now(),
        );
        assert_eq!(stale.state, WorkerState::Running);
        assert_eq!(stale.last_applied_seq, 2);
    }

    #[test]
    fn session_identified_copies_ids_into_the_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let mut snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        snap.state = WorkerState::Running;
        let after = fold(
            snap,
            &[ev(
                json!({"fsm":"SessionIdentified","session_id":"sess-1","rollout_path":"/r.jsonl"}),
                1,
                1,
            )],
            Utc::now(),
        );
        assert_eq!(after.session_id.as_deref(), Some("sess-1"));
        assert_eq!(
            after.rollout_path.as_deref(),
            Some(std::path::Path::new("/r.jsonl"))
        );
        assert_eq!(after.state, WorkerState::Running);
    }

    #[test]
    fn process_started_records_the_pid_and_witness() {
        let tmp = tempfile::tempdir().unwrap();
        let snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        let after = fold(
            snap,
            &[ev(
                json!({"fsm":"ProcessStarted","pid":4242,"start_witness":"boot-1"}),
                1,
                1,
            )],
            Utc::now(),
        );
        assert_eq!(after.pid, Some(4242));
        assert_eq!(after.pid_start_witness.as_deref(), Some("boot-1"));
    }

    #[test]
    fn append_and_fold_writes_the_log_then_the_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", dir);
        let mut log = store::EventLog::open(dir).unwrap();
        let snap = append_and_fold(
            &mut log,
            dir,
            snap,
            EventKind::StateChange,
            json!({"fsm":"ProcessStarted","pid":9,"start_witness":"w"}),
            Utc::now(),
        )
        .unwrap();
        assert_eq!(snap.state, WorkerState::Running);
        assert_eq!(snap.last_applied_seq, 1);
        // both sides of the WAL are on disk and agree
        assert_eq!(store::read_events(dir, 0).unwrap().len(), 1);
        let on_disk = load_snapshot(dir).unwrap();
        assert_eq!(on_disk.state, WorkerState::Running);
        assert_eq!(on_disk.pid, Some(9));
    }

    /// The broker path: a caller-built envelope of ANY class goes through the
    /// same append → fold → checkpoint sequence, and comes back stamped.
    #[test]
    fn append_envelope_and_fold_takes_a_caller_built_envelope_of_any_class() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", dir);
        let mut log = store::EventLog::open(dir).unwrap();

        // An app-server notification envelope: `raw` is the notification's own
        // params, and the fsm payload is merged into it.
        let mut lifecycle = Envelope::new(
            1,
            Transport::AppServer,
            EventClass::Lifecycle,
            EventKind::StateChange,
            json!({"fsm":"ProcessStarted","pid":31337,"start_witness":"w",
                   "threadId":"t1"}),
            Utc::now(),
        );
        let snap =
            append_envelope_and_fold(&mut log, dir, snap, &mut lifecycle, Utc::now()).unwrap();
        assert_eq!(lifecycle.seq, 1, "the caller's envelope is stamped in place");
        assert_eq!(snap.state, WorkerState::Running);
        assert_eq!(snap.pid, Some(31337));
        assert_eq!(snap.last_applied_seq, 1);

        // A trace envelope routed here consumes its seq and moves nothing:
        // only `lifecycle` envelopes can ever reach the machine.
        let mut trace = Envelope::new(
            1,
            Transport::AppServer,
            EventClass::Trace,
            EventKind::CommandExec,
            json!({"fsm":"Kill","item":{"type":"commandExecution"}}),
            Utc::now(),
        );
        let snap = append_envelope_and_fold(&mut log, dir, snap, &mut trace, Utc::now()).unwrap();
        assert_eq!(trace.seq, 2);
        assert_eq!(snap.state, WorkerState::Running, "trace never drives the FSM");
        assert_eq!(snap.last_applied_seq, 2);
        assert!(snap.extra.get("fold_rejections").is_none());

        // Both halves of the WAL are on disk and agree.
        assert_eq!(store::read_events(dir, 0).unwrap().len(), 2);
        assert_eq!(load_snapshot(dir).unwrap().pid, Some(31337));
    }

    /// `append_and_fold` is now a thin wrapper: same log, same snapshot.
    #[test]
    fn append_and_fold_delegates_to_append_envelope_and_fold() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", dir);
        let mut log = store::EventLog::open(dir).unwrap();
        let snap = append_and_fold(
            &mut log,
            dir,
            snap,
            EventKind::StateChange,
            json!({"fsm":"ProcessStarted","pid":9,"start_witness":"w"}),
            Utc::now(),
        )
        .unwrap();
        let written = &store::read_events(dir, 0).unwrap()[0];
        assert_eq!(written.class, EventClass::Lifecycle);
        assert_eq!(written.epoch, snap.epoch);
        assert_eq!(written.transport, snap.transport);
        assert_eq!(written.seq, 1);
    }

    #[test]
    fn recover_refuses_a_newer_schema() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(crate::state::STATE_FILE),
            br#"{"schema_version":99}"#,
        )
        .unwrap();
        let err = recover(tmp.path(), LivenessInput { process_alive: true }, Utc::now())
            .expect_err("a newer snapshot must not be mutated");
        assert_eq!(err.code, "SCHEMA_TOO_NEW");
        assert_eq!(err.exit, ExitCode::Precondition);
    }

    #[test]
    fn recover_folds_only_events_the_snapshot_has_not_seen() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", dir);
        let mut log = store::EventLog::open(dir).unwrap();
        snap = append_and_fold(
            &mut log,
            dir,
            snap,
            EventKind::StateChange,
            json!({"fsm":"ProcessStarted","pid":9,"start_witness":"w"}),
            Utc::now(),
        )
        .unwrap();
        // crash: the event is logged but the snapshot never caught up
        snap.state = WorkerState::Spawned;
        snap.last_applied_seq = 0;
        save_snapshot(dir, &snap).unwrap();
        let r = recover(dir, LivenessInput { process_alive: true }, Utc::now()).unwrap();
        assert_eq!(r.state, WorkerState::Running);
        assert_eq!(r.last_applied_seq, 1);
    }
}
