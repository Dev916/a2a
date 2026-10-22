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
            absorb_app_server_identity(&mut snap, env);
            // The turn budget's counter is FOLD-derived (spec §4.9), and it is
            // incremented here — inside the epoch-current branch, so a buffer
            // flushed by a dead prior run cannot spend a resumed worker's
            // budget, and BEFORE the FSM dispatch, so the count a `turn_started`
            // trips on is the count that same envelope produced.
            if env.kind == events::EventKind::TurnStarted {
                snap.turns_this_epoch = Some(snap.turns_this_epoch.unwrap_or(0) + 1);
            }
            if let Some(ev) = envelope_to_fsm_event(env) {
                apply_event(&mut snap, env, &ev, now);
            } else if matches!(
                env.kind,
                events::EventKind::InboxDelivered | events::EventKind::TransportRestarted
            ) {
                apply_transport_identity(&mut snap, env);
            } else if matches!(
                env.kind,
                events::EventKind::ResultCommitted
                    | events::EventKind::CommitSkipped
                    | events::EventKind::CommitFailed
            ) {
                absorb_auto_commit(&mut snap, env);
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
    if let Some(profile) = s("profile") {
        snap.profile = Some(profile.to_string());
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
            // The default when genesis names no separate request: the two agree
            // (Phase-1 genesis rows have only `transport`).
            snap.requested_transport = t;
        }
    }
    // ... and the request is its OWN key when the spawn path recorded one, so a
    // rebuild can still say what was asked for rather than only what happened.
    if let Some(t) = raw.get("requested_transport") {
        if let Ok(t) = serde_json::from_value::<Transport>(t.clone()) {
            snap.requested_transport = t;
        }
    }
    if let Some(owner) = s("owner") {
        snap.owner = Some(owner.to_string());
    }
    // The thread of record, when the spawn already knew one (a re-attach); a
    // fresh spawn learns it later, from `SessionIdentified`.
    if let Some(thread_id) = s("thread_id") {
        snap.thread_id = Some(thread_id.to_string());
    }
    // Bookkeeping the snapshot carries verbatim: HOW the transport was chosen
    // (`auto` | `explicit`), which is not derivable from the transport itself.
    if let Some(selection) = raw.get("transport_selection") {
        snap.extra
            .insert("transport_selection".to_string(), selection.clone());
    }
    // Why this worker was allowed `danger-full-access` (spec §5). Genesis is
    // the only writer of identity, and "this worker ran with full access, for
    // this stated reason" is identity — a rebuild that dropped it would leave
    // `audit.jsonl` as the only surviving copy of a fact the worker itself is
    // supposed to carry.
    if let Some(reason) = s("elevation_reason") {
        snap.elevation_reason = Some(reason.to_string());
    }
    // The result schema. Genesis has recorded this key since Phase 1 and the
    // fold never read it — harmless while nothing depended on it, and a silent
    // data loss the moment the impl-lane acceptance gate does. Fixed here,
    // where lossless rebuild lives.
    if let Some(schema) = s("schema_path") {
        snap.schema_path = Some(schema.into());
    }
    // WHICH rung of the precedence table chose it (`flag` | `brief` |
    // `lane-default`). Bookkeeping the snapshot carries verbatim, like
    // `transport_selection`: a worker judged against the impl lane's shipped
    // default and one judged against an operator's own file are not the same
    // worker, and only genesis can still say which this was after a rebuild.
    if let Some(source) = raw.get("schema_source") {
        snap.extra
            .insert("schema_source".to_string(), source.clone());
    }
    // The worker's own tree (spec §4.8). ALL FOUR keys or nothing: a half-built
    // `WorktreeRef` is a worktree no `gc` git call can address (each needs
    // `-C <repo>`), so a partial genesis row must read as "no worktree" rather
    // than as one a2a will later try and fail to operate on.
    if let (Some(path), Some(branch), Some(base_ref), Some(repo)) = (
        s("worktree_path"),
        s("worktree_branch"),
        s("worktree_base_ref"),
        s("worktree_repo"),
    ) {
        snap.worktree = Some(crate::state::WorktreeRef {
            path: path.into(),
            branch: branch.to_string(),
            base_ref: base_ref.to_string(),
            repo: repo.into(),
        });
    }
    // Why a worker whose lane defaults to a worktree has none. Bookkeeping the
    // snapshot carries verbatim, like `transport_selection`: it explains an
    // absence, and an explanation only `state.json` knew would not survive the
    // rebuild that makes the absence visible again.
    if let Some(reason) = raw.get("worktree_skipped") {
        snap.extra
            .insert("worktree_skipped".to_string(), reason.clone());
    }
    // The diff anchor, recorded whether or not there is a worktree. When only
    // the worktree keys are present it is derived from them, so the "a worktree
    // implies its base ref" invariant holds after a fold from zero too.
    if let Some(base) = s("base_ref") {
        snap.base_ref = Some(base.to_string());
    } else if let Some(derived) = snap.worktree.as_ref().map(|w| w.base_ref.clone()) {
        snap.base_ref = Some(derived);
    }
    // The EFFECTIVE turn budget (spec §4.9), already clamped by anything the
    // brief asked for. In genesis rather than read from config at enforcement
    // time, for the reason every other identity field is: an operator editing
    // the lane table must not silently re-judge a worker already in flight, and
    // a rebuild from zero has to reach the same verdict.
    if let Some(max) = raw.get("max_turns").and_then(|v| v.as_u64()) {
        snap.max_turns = Some(max);
    }
    // A new worker has taken no turns. Seeded unconditionally so a rebuild from
    // zero starts counting from the same place the original did.
    snap.turns_this_epoch = Some(0);
    // Retry lineage (spec §4.9), present ONLY on a worker `a2a respawn` made.
    // Genesis is the only writer of identity, and "this is the third attempt at
    // that brief" is identity: it is what the attempt guard reads, and a
    // snapshot rebuilt from zero has to reach the same verdict. An ABSENT
    // `attempt` is attempt 1, so a plain spawn's genesis keeps its exact
    // Phase-1/2 shape and every worker born before lineage existed stays the
    // first attempt at its own brief.
    if let Some(parent) = s("parent_id") {
        snap.parent_id = Some(parent.to_string());
    }
    if let Some(attempt) = raw.get("attempt").and_then(|v| v.as_u64()) {
        snap.attempt = Some(attempt as u32);
    }
    // The framework-commit decision (`src/auto_commit.rs`), present ONLY when
    // it is "yes": an ABSENT key is "no", so every worker born before the
    // feature existed is never committed by the binary that introduced it.
    snap.auto_commit_enabled = raw.get("auto_commit_enabled").and_then(|v| v.as_bool()) == Some(true);
    // The auto-review marker (spec §4.8): which impl worker's diff this worker
    // was made to review. Bookkeeping the snapshot carries verbatim, like
    // `schema_source` — and deliberately NOT `parent_id`, which is the retry
    // chain `max_attempts` counts. A reviewer is not a retry of anything, and
    // sharing the key would quarantine an impl worker for being reviewed twice.
    // It reaches `snap.extra`, which is where `ls --json` reads it and where a
    // rebuild from zero puts it back.
    if let Some(reviewed) = raw.get("auto_review_of") {
        snap.extra
            .insert("auto_review_of".to_string(), reviewed.clone());
    }
    snap.created_at = s("created_at")
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&Utc))
        .unwrap_or(env.ts);
    // Admission control (spec §4.9): a worker born at the cap starts `queued`
    // and waits for `Admit`. The MARKER is in genesis rather than in a second
    // event because genesis is the only writer of identity — a snapshot rebuilt
    // from zero must land in the same state, and a queued worker that replayed
    // as `spawned` would look like one whose process merely never came up.
    //
    // This is a fold change, not an FSM change: `queued --admit--> spawned` is
    // already a row (and `docs/machines/worker.md` already draws the arrow).
    snap.state = if raw.get("queued").and_then(|v| v.as_bool()) == Some(true) {
        WorkerState::Queued
    } else {
        WorkerState::Spawned
    };
    snap.state_reason = None;
    snap.failure_class = None;
    snap.entered_state_at = snap.created_at;
    snap.updated_at = now;
}

/// The framework commit's verdict on a `done` worker. Bookkeeping, not a
/// transition: these rows carry no `fsm` key and only ever fill in the
/// snapshot's `auto_commit` record (and `committed_sha` when there is one).
fn absorb_auto_commit(snap: &mut WorkerSnapshot, env: &Envelope) {
    let s = |k: &str| env.raw.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let record = match env.kind {
        events::EventKind::ResultCommitted => {
            snap.committed_sha = s("sha");
            crate::state::AutoCommitRecord {
                outcome: "committed".to_string(),
                sha: s("sha"),
                reason: None,
            }
        }
        events::EventKind::CommitSkipped => crate::state::AutoCommitRecord {
            outcome: s("outcome").unwrap_or_else(|| "skipped".to_string()),
            sha: None,
            reason: s("reason"),
        },
        _ => crate::state::AutoCommitRecord {
            outcome: "failed".to_string(),
            sha: None,
            reason: s("reason"),
        },
    };
    snap.auto_commit = Some(record);
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
    // A restart may move the worker to a DIFFERENT transport, and that is not a
    // detail: `reconcile` routes on `snap.transport`, so an app-server worker
    // resumed by `a2a resume` (which always forks a `codex exec` child) would
    // otherwise still be looked up in a broker that no longer owns it — and be
    // committed `orphaned` seconds after a perfectly healthy resume. Only a row
    // that says so moves it, so the broker's own restart rows (which carry no
    // `transport` key) leave it alone.
    if let Some(t) = env.raw.get("transport") {
        if let Ok(t) = serde_json::from_value::<crate::events::Transport>(t.clone()) {
            snap.transport = t;
        }
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

/// App-server identity a row may carry regardless of which FSM edge (if any) it
/// drives: the **thread of record** and the sandbox the server actually
/// resolved.
///
/// In the fold rather than patched onto the snapshot afterwards, for the same
/// reason `failure_class` is: both facts live in the WAL, so a snapshot rebuilt
/// from zero a month later reports the same thread and the same sandbox. The
/// actor writes them onto `ProcessStarted`/`SessionIdentified`; a Phase-1 exec
/// row carries neither key and is untouched.
fn absorb_app_server_identity(snap: &mut WorkerSnapshot, env: &Envelope) {
    if env.class != EventClass::Lifecycle {
        return;
    }
    if let Some(thread_id) = env.raw.get("thread_id").and_then(|v| v.as_str()) {
        snap.thread_id = Some(thread_id.to_string());
    }
    if let Some(sandbox) = env.raw.get("resolved_sandbox") {
        if !sandbox.is_null() {
            snap.extra
                .insert("resolved_sandbox".to_string(), sandbox.clone());
        }
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
                // A new epoch is a fresh budget (spec §4.9): the whole point of
                // `a2a resume` on a `failed{budget_exhausted}` worker is that
                // the operator has looked and decided to grant more turns. The
                // reset rides the epoch bump rather than the resume command so
                // every future edge that bumps gets it for free.
                snap.turns_this_epoch = Some(0);
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
            (
                json!({"fsm":"ResultInvalid","detail":"d"}),
                "result_invalid",
            ),
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
        assert_eq!(
            after.last_applied_seq, 1,
            "a rejected event still consumes its seq"
        );
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

    /// Genesis is the only writer of identity, so everything Phase 2 records at
    /// spawn has to be readable back out of it — otherwise `state.json` becomes
    /// the source of truth for owner and thread, which is exactly the thing the
    /// WAL exists to prevent.
    #[test]
    fn genesis_seeds_the_owner_the_thread_of_record_and_a_separate_request() {
        let tmp = tempfile::tempdir().unwrap();
        let snap = WorkerSnapshot::new_for_test("general-0817t1200-aaaa", tmp.path());
        let after = fold(
            snap,
            &[ev(
                json!({"fsm":"Spawn","id":"general-0817t1200-aaaa","lane":"general",
                       "cwd":"/repo","transport":"exec","requested_transport":"app-server",
                       "transport_selection":"auto","owner":"mike","thread_id":"thread-1"}),
                1,
                1,
            )],
            Utc::now(),
        );
        assert_eq!(after.owner.as_deref(), Some("mike"));
        assert_eq!(after.thread_id.as_deref(), Some("thread-1"));
        assert_eq!(after.transport, Transport::Exec);
        assert_eq!(after.requested_transport, Transport::AppServer);
        assert_eq!(after.extra["transport_selection"], json!("auto"));

        // A Phase-1 genesis names no request: the two agree, as they always did.
        let snap = WorkerSnapshot::new_for_test("general-0817t1200-bbbb", tmp.path());
        let after = fold(
            snap,
            &[ev(
                json!({"fsm":"Spawn","id":"general-0817t1200-bbbb","lane":"general",
                       "cwd":"/repo","transport":"app-server"}),
                1,
                1,
            )],
            Utc::now(),
        );
        assert_eq!(after.transport, Transport::AppServer);
        assert_eq!(after.requested_transport, Transport::AppServer);
        assert_eq!(after.owner, None);
    }

    /// One `turn_started` envelope per turn, counted by the fold — plus the
    /// genesis seed that starts the count at zero and records the budget the
    /// count is judged against.
    #[test]
    fn the_fold_counts_turn_started_and_genesis_seeds_the_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let genesis = ev(
            json!({"fsm":"Spawn","id":"general-0818t1200-aaaa","lane":"general",
                   "cwd":"/repo","transport":"exec","max_turns":3}),
            1,
            1,
        );
        let mut log = vec![genesis];
        for seq in 2..=4u64 {
            let mut turn = ev(json!({}), 1, seq);
            turn.kind = EventKind::TurnStarted;
            log.push(turn);
        }
        let after = fold(
            WorkerSnapshot::new_for_test("general-0818t1200-aaaa", tmp.path()),
            &log,
            Utc::now(),
        );
        assert_eq!(after.max_turns, Some(3), "the budget comes out of genesis");
        assert_eq!(after.turns_this_epoch, Some(3), "three turns, three counts");

        // (d) Replay is idempotent: seq dedup means folding the same log over
        // the result changes nothing. Recovery runs on every read path, so a
        // counter that double-counted would trip a worker for being read twice.
        let again = fold(after.clone(), &log, Utc::now());
        assert_eq!(again.turns_this_epoch, Some(3), "seq dedup, not re-counted");
        assert_eq!(again.epoch, after.epoch);

        // ...and a rebuild from zero reaches the same number, which is what
        // makes the counter fold-derived rather than writer-derived.
        let rebuilt = fold(
            WorkerSnapshot::new_for_test("general-0818t1200-aaaa", tmp.path()),
            &log,
            Utc::now(),
        );
        assert_eq!(rebuilt.turns_this_epoch, after.turns_this_epoch);
        assert_eq!(rebuilt.max_turns, after.max_turns);
    }

    /// (b) The epoch bump is the reset, and (c) a stale-epoch turn is fenced —
    /// the two halves of "one epoch, one budget".
    #[test]
    fn a_resume_resets_the_counter_and_a_stale_turn_never_increments_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut turn1 = ev(json!({}), 1, 2);
        turn1.kind = EventKind::TurnStarted;
        let mut turn2 = ev(json!({}), 1, 3);
        turn2.kind = EventKind::TurnStarted;
        let after = fold(
            WorkerSnapshot::new_for_test("general-0818t1200-bbbb", tmp.path()),
            &[
                ev(
                    json!({"fsm":"Spawn","id":"general-0818t1200-bbbb","lane":"general",
                           "cwd":"/repo","transport":"exec","max_turns":1}),
                    1,
                    1,
                ),
                turn1,
                turn2,
                // The worker has to be terminal for `Resume` to be legal.
                ev(
                    json!({"fsm":"ProcessStarted","pid":1,"start_witness":"w"}),
                    1,
                    4,
                ),
                ev(json!({"fsm":"ProcessExited","code":0}), 1, 5),
                ev(json!({"fsm":"Resume","answer_queued":false}), 1, 6),
            ],
            Utc::now(),
        );
        assert_eq!(after.epoch, 2, "resume bumps the epoch");
        assert_eq!(
            after.turns_this_epoch,
            Some(0),
            "the resumed worker gets a fresh budget"
        );
        assert!(
            after.extra.get("fold_rejections").is_none(),
            "no rejections on the way there: {:?}",
            after.extra.get("fold_rejections")
        );

        // (c) Epoch fencing: a `turn_started` from the dead epoch-1 run is
        // consumed (its seq advances the watermark) and counts for nothing.
        let mut stale = ev(json!({}), 1, 7);
        stale.kind = EventKind::TurnStarted;
        let fenced = fold(after, &[stale], Utc::now());
        assert_eq!(fenced.turns_this_epoch, Some(0), "stale epoch, no count");
        assert_eq!(fenced.last_applied_seq, 7, "the seq is still consumed");
    }

    /// The worktree, its diff anchor and the result schema all come out of
    /// genesis, because genesis is the only writer of identity — a snapshot
    /// rebuilt from zero must be able to name the branch it produced.
    #[test]
    fn genesis_seeds_the_worktree_the_base_ref_and_the_schema_path() {
        let tmp = tempfile::tempdir().unwrap();
        let base = "0123456789abcdef0123456789abcdef01234567";
        let after = fold(
            WorkerSnapshot::new_for_test("impl-0817t1200-aaaa", tmp.path()),
            &[ev(
                json!({"fsm":"Spawn","id":"impl-0817t1200-aaaa","lane":"impl",
                       "cwd":"/home/worktrees/impl-0817t1200-aaaa",
                       "schema_path":"/tpl/impl-result.schema.json",
                       "worktree_path":"/home/worktrees/impl-0817t1200-aaaa",
                       "worktree_branch":"a2a/impl/impl-0817t1200-aaaa",
                       "worktree_base_ref":base,
                       "worktree_repo":"/repo",
                       "base_ref":base}),
                1,
                1,
            )],
            Utc::now(),
        );
        let wt = after.worktree.clone().expect("all four keys present");
        assert_eq!(
            wt.path,
            std::path::Path::new("/home/worktrees/impl-0817t1200-aaaa")
        );
        assert_eq!(wt.branch, "a2a/impl/impl-0817t1200-aaaa");
        assert_eq!(wt.base_ref, base);
        assert_eq!(wt.repo, std::path::Path::new("/repo"));
        assert_eq!(after.base_ref.as_deref(), Some(base));
        assert_eq!(
            after.schema_path.as_deref(),
            Some(std::path::Path::new("/tpl/impl-result.schema.json"))
        );
    }

    /// Two halves of the same rule. A worktree-less worker still records the
    /// anchor `a2a verify` needs; a genesis missing ANY of the four worktree
    /// keys yields no worktree at all rather than a partial one.
    #[test]
    fn the_base_ref_stands_alone_and_a_partial_worktree_is_no_worktree() {
        let tmp = tempfile::tempdir().unwrap();
        let base = "0123456789abcdef0123456789abcdef01234567";
        let seeded = |extra: serde_json::Value| {
            let mut payload = json!({"fsm":"Spawn","id":"impl-0817t1200-aaaa",
                                     "lane":"impl","cwd":"/repo"});
            for (k, v) in extra.as_object().unwrap() {
                payload[k] = v.clone();
            }
            fold(
                WorkerSnapshot::new_for_test("impl-0817t1200-aaaa", tmp.path()),
                &[ev(payload, 1, 1)],
                Utc::now(),
            )
        };

        // No worktree, but the diff anchor is still recorded.
        let plain = seeded(json!({"base_ref": base, "worktree_skipped": "not-a-git-repo"}));
        assert_eq!(plain.worktree, None);
        assert_eq!(plain.base_ref.as_deref(), Some(base));

        // Every one-key-short row: no partial object, ever.
        for missing in [
            "worktree_path",
            "worktree_branch",
            "worktree_base_ref",
            "worktree_repo",
        ] {
            let mut keys = serde_json::Map::new();
            for k in [
                "worktree_path",
                "worktree_branch",
                "worktree_base_ref",
                "worktree_repo",
            ] {
                if k != missing {
                    keys.insert(
                        k.to_string(),
                        json!(if k == "worktree_base_ref" { base } else { "x" }),
                    );
                }
            }
            let partial = seeded(serde_json::Value::Object(keys));
            assert_eq!(
                partial.worktree, None,
                "missing {missing} must not build a worktree"
            );
        }

        // ...and when the worktree keys are complete but `base_ref` is absent,
        // the anchor is DERIVED, so the invariant survives a fold from zero.
        let derived = seeded(json!({"worktree_path":"/home/worktrees/w",
                                    "worktree_branch":"a2a/impl/w",
                                    "worktree_base_ref":base,
                                    "worktree_repo":"/repo"}));
        assert_eq!(
            derived.base_ref,
            derived.worktree.map(|w| w.base_ref),
            "a worktree always implies its base ref"
        );
    }

    /// A worker born at the admission cap starts `queued`, from the genesis
    /// marker alone — and gets to `spawned` the one way the machine allows.
    #[test]
    fn a_queued_genesis_seeds_queued_and_admit_is_what_leaves_it() {
        let tmp = tempfile::tempdir().unwrap();
        let genesis = |queued: Option<bool>| {
            let mut payload = json!({"fsm":"Spawn","id":"general-0817t1200-aaaa",
                                     "lane":"general","cwd":"/repo","transport":"app-server"});
            if let Some(q) = queued {
                payload["queued"] = json!(q);
            }
            payload
        };
        let folded = |payload, events: &[serde_json::Value]| {
            let mut all = vec![ev(payload, 1, 1)];
            all.extend(
                events
                    .iter()
                    .enumerate()
                    .map(|(n, e)| ev(e.clone(), 1, n as u64 + 2)),
            );
            fold(
                WorkerSnapshot::new_for_test("general-0817t1200-aaaa", tmp.path()),
                &all,
                Utc::now(),
            )
        };

        assert_eq!(folded(genesis(Some(true)), &[]).state, WorkerState::Queued);
        // Absent and explicitly-false are the same worker: the Phase-1 shape.
        assert_eq!(folded(genesis(None), &[]).state, WorkerState::Spawned);
        assert_eq!(
            folded(genesis(Some(false)), &[]).state,
            WorkerState::Spawned
        );

        // Admission, then the ordinary start flow — no rejections anywhere.
        let admitted = folded(
            genesis(Some(true)),
            &[
                json!({"fsm":"Admit"}),
                json!({"fsm":"ProcessStarted","pid":0,"start_witness":"w"}),
            ],
        );
        assert_eq!(admitted.state, WorkerState::Running);
        assert!(
            admitted.extra.get("fold_rejections").is_none(),
            "queued → spawned → running is a legal path: {:?}",
            admitted.extra
        );

        // ... and `kill` is the other door out of the queue.
        let cancelled = folded(genesis(Some(true)), &[json!({"fsm":"Kill"})]);
        assert_eq!(cancelled.state, WorkerState::Cancelled);
        assert_eq!(cancelled.failure_class, None, "a human stop never fails");
    }

    /// The thread of record and the resolved sandbox arrive on later rows (the
    /// actor opens the thread after genesis), and they must survive a rebuild.
    #[test]
    fn the_thread_of_record_and_resolved_sandbox_are_absorbed_from_any_lifecycle_row() {
        let tmp = tempfile::tempdir().unwrap();
        let snap = WorkerSnapshot::new_for_test("general-0817t1200-aaaa", tmp.path());
        let after = fold(
            snap,
            &[
                ev(
                    json!({"fsm":"ProcessStarted","pid":0,"start_witness":"w",
                           "thread_id":"thread-7",
                           "resolved_sandbox":{"type":"workspaceWrite","writableRoots":[]}}),
                    1,
                    1,
                ),
                ev(
                    json!({"fsm":"SessionIdentified","session_id":"thread-7",
                           "thread_id":"thread-7"}),
                    1,
                    2,
                ),
            ],
            Utc::now(),
        );
        assert_eq!(after.thread_id.as_deref(), Some("thread-7"));
        assert_eq!(after.session_id.as_deref(), Some("thread-7"));
        assert_eq!(
            after.extra["resolved_sandbox"]["type"],
            json!("workspaceWrite")
        );
        assert!(after.extra.get("fold_rejections").is_none());
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

    /// A resume can move a worker between transports, and only a row that says
    /// so moves it. Without the first half, `reconcile` asks a broker about a
    /// worker that is now a plain `codex exec` child and orphans it seconds
    /// after a healthy `a2a resume` (caught by Task 18's cross-transport
    /// proof); without the second half, the broker's own supervision restarts
    /// would silently demote a live app-server worker to exec.
    #[test]
    fn a_transport_restart_moves_the_transport_only_when_the_row_names_one() {
        let tmp = tempfile::tempdir().unwrap();
        let mut snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        snap.transport = Transport::AppServer;

        let restarted = |payload: serde_json::Value, seq: u64| {
            let mut e = Envelope::new(
                1,
                Transport::Exec,
                EventClass::Lifecycle,
                EventKind::TransportRestarted,
                payload,
                Utc::now(),
            );
            e.seq = seq;
            e
        };

        // The broker's own restart row: pid + thread, no transport key.
        let after = fold(
            snap.clone(),
            &[restarted(
                json!({"pid": 11, "start_witness": "w", "thread_id": "t"}),
                1,
            )],
            Utc::now(),
        );
        assert_eq!(after.transport, Transport::AppServer);
        assert_eq!(after.pid, Some(11));

        // `a2a resume`'s row: the new epoch runs on `codex exec`.
        let after = fold(
            snap,
            &[restarted(
                json!({"pid": 12, "start_witness": "w", "transport": "exec"}),
                1,
            )],
            Utc::now(),
        );
        assert_eq!(after.transport, Transport::Exec);
    }

    /// The auto-commit rows: genesis carries the decision, and the verdict row
    /// fills in the record (and the sha) without moving the state.
    #[test]
    fn genesis_carries_the_auto_commit_decision_and_the_verdict_folds_in() {
        let tmp = tempfile::tempdir().unwrap();
        let base = WorkerSnapshot::blank("impl-0814t1300-aaaa", tmp.path());
        let mut genesis = ev(json!({"fsm": "Spawn", "id": "impl-0814t1300-aaaa",
                                    "auto_commit_enabled": true}), 1, 1);
        genesis.kind = EventKind::Spawned;
        let snap = fold(base.clone(), &[genesis.clone()], Utc::now());
        assert!(snap.auto_commit_enabled);
        let mut plain = ev(json!({"fsm": "Spawn", "id": "impl-0814t1300-aaaa"}), 1, 1);
        plain.kind = EventKind::Spawned;
        assert!(!fold(base.clone(), &[plain], Utc::now()).auto_commit_enabled);

        let mut committed = ev(json!({"sha": "abc123", "files": 1, "branch": "b"}), 1, 2);
        committed.kind = EventKind::ResultCommitted;
        let after = fold(snap.clone(), &[committed], Utc::now());
        assert_eq!(after.committed_sha.as_deref(), Some("abc123"));
        assert_eq!(after.auto_commit.as_ref().unwrap().outcome, "committed");
        assert_eq!(after.state, snap.state, "bookkeeping moves no state");
        assert!(after.extra.get("fold_rejections").is_none());

        let mut skipped = ev(json!({"outcome": "clean", "reason": "nothing to commit"}), 1, 2);
        skipped.kind = EventKind::CommitSkipped;
        let after = fold(snap.clone(), &[skipped], Utc::now());
        assert_eq!(after.committed_sha, None);
        assert_eq!(after.auto_commit.as_ref().unwrap().outcome, "clean");

        let mut failed = ev(json!({"reason": "git said no"}), 1, 2);
        failed.kind = EventKind::CommitFailed;
        let after = fold(snap, &[failed], Utc::now());
        let rec = after.auto_commit.unwrap();
        assert_eq!((rec.outcome.as_str(), rec.reason.as_deref()), ("failed", Some("git said no")));
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
        assert_eq!(
            lifecycle.seq, 1,
            "the caller's envelope is stamped in place"
        );
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
        assert_eq!(
            snap.state,
            WorkerState::Running,
            "trace never drives the FSM"
        );
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
        let err = recover(
            tmp.path(),
            LivenessInput {
                process_alive: true,
            },
            Utc::now(),
        )
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
        let r = recover(
            dir,
            LivenessInput {
                process_alive: true,
            },
            Utc::now(),
        )
        .unwrap();
        assert_eq!(r.state, WorkerState::Running);
        assert_eq!(r.last_applied_seq, 1);
    }
}
