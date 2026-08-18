//! `a2a resume <id> [message]` — the only door out of a terminal state
//! (spec §4.6, §7).
//!
//! A resume is a **new epoch on an old conversation**: the codex session id
//! survives, everything that fences a run does not. That split is the whole
//! design:
//!
//! ```text
//! epoch 1   exec.1.jsonl   ingest watermark exec_offset_1   ← frozen, evidence
//! epoch 2   exec.2.jsonl   ingest watermark exec_offset_2   ← the live run
//! ```
//!
//! The fold discards events whose epoch is below the snapshot's, so a buffer
//! flushed by the dead epoch-1 run can never complete (or resurrect) the
//! resumed worker — and because each epoch has its own stream file, reconcile
//! ingests only the current one. The epoch-1 `turn.completed` line that made
//! the worker `done` is never re-applied: it lives in a file this epoch does
//! not read.
//!
//! Order of operations, and why:
//!
//! 1. **`session_id` gate first.** No session means no conversation to resume;
//!    spawning without `exec resume <id>` would silently start a *new* one
//!    against the same worker dir. Typed `SESSION_UNKNOWN`, exit 9.
//! 2. **`step(Resume)` second.** A `running` worker is rejected verbatim by the
//!    machine (`ILLEGAL_TRANSITION`, exit 9) — resume is not a way to
//!    double-drive a live session.
//! 3. **WAL before process.** The `Resumed` event (which is what applies the
//!    epoch bump) is durable before any child exists, so a crash in the gap
//!    leaves a worker that reconcile can see and fail, never an invisible
//!    process writing into a stream nobody reads.
//! 4. **Identity after.** The new pid + start witness land in their own
//!    bookkeeping event, exactly like an inbox delivery — a later `a2a kill`
//!    must signal the process this resume started, not the corpse of the last
//!    one. That row also names the **transport** the new epoch runs on, because
//!    a resume is the one place a worker can change transports: this command
//!    always forks a `codex exec resume <session>` child, so an app-server
//!    worker becomes an exec worker here. It is the same conversation on a
//!    different pipe — which is exactly what makes the broker optional
//!    (spec §4.4) — and until the snapshot says so, `reconcile` keeps asking a
//!    broker about a worker it no longer owns and orphans it.
//!
//! The message travels through the 0600 `prompt.md` on the child's stdin, never
//! argv (spec §5). Queued `inbox/` messages are deliberately NOT consumed here:
//! delivery happens at a turn boundary (spec §4.3), so whatever is queued goes
//! out when this resumed turn ends. `answer_queued` in the WAL records that a
//! backlog was waiting when the resume happened.

use crate::commands::owner;
use crate::config;
use crate::events::{EventKind, Transport};
use crate::exec::{self, SpawnSpec};
use crate::fold::append_and_fold;
use crate::fsm::{self, FsmEvent};
use crate::home::A2aHome;
use crate::ids;
use crate::inbox;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::{self, reconcile};
use crate::state::{load_snapshot, save_snapshot, WorkerSnapshot};
use crate::store::{write_atomic, EventLog};
use chrono::Utc;
use serde_json::{json, Value};
use std::path::PathBuf;

/// What a resume with no message says to the worker. Empty stdin would be a
/// turn with no instruction at all; this points the worker back at the brief,
/// which is the only always-valid instruction source (spec §4.7).
const CONTINUE_PROMPT: &str = "Continue the task. Re-read brief.md if you need the definition of \
                               done, and finish by writing the result in the required format.";

pub fn run(
    home: &A2aHome,
    id_arg: &str,
    message: Option<&str>,
    force: bool,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let (cfg, _origins, warnings) = config::load(home)?;
    if !quiet {
        for w in &warnings {
            eprintln!("a2a: warning: {w}");
        }
    }
    let id = ids::resolve(home, id_arg)?;
    let dir = home.worker_dir(&id);

    let snap = reconcile(home, &cfg, &id, Utc::now())?;
    // Ahead of the session gate: whether this worker COULD be resumed is not
    // the caller's business until it is established that they may (spec §4.8).
    owner::guard(&id, snap.owner.as_deref(), force)?;
    session_of(&snap, &id)?;
    let answer_queued = inbox::undelivered(&dir, snap.last_delivered_seq)
        .map(|pending| !pending.is_empty())
        .unwrap_or(false);
    // The machine is the authority on whether this worker can be resumed at
    // all; its rejection carries the remedy, so it is surfaced verbatim.
    fsm::step(snap.state, snap.state_reason, &FsmEvent::Resume { answer_queued }).map_err(
        |rejected| {
            AppError::new(
                "ILLEGAL_TRANSITION",
                ExitCode::Precondition,
                rejected.to_string(),
            )
            .remedy(rejected.reason)
        },
    )?;

    // Everything below mutates the worker, so it runs under `state.lock`.
    let _lock = reconcile::lock_state(&dir)?;
    let mut snap = load_snapshot(&dir)?;
    let session = session_of(&snap, &id)?;
    let from = snap.state;
    let mut log = EventLog::open(&dir).map_err(|e| io_err("opening events.jsonl", e))?;

    // The transition FIRST: this event is what bumps the epoch (the fold
    // applies it), so the new stream file and every event below it are already
    // fenced against the run that just ended.
    snap = append_and_fold(
        &mut log,
        &dir,
        snap,
        EventKind::Resumed,
        json!({
            "fsm": "Resume",
            "from": from.as_str(),
            "answer_queued": answer_queued,
            "session_id": session,
            "has_message": message.is_some(),
        }),
        Utc::now(),
    )?;

    // The instruction travels as a FILE wired to the child's stdin — argv ends
    // with the `-` positional and never carries operator text (spec §5).
    let prompt_file = dir.join("prompt.md");
    let body = message.map(str::trim).filter(|m| !m.is_empty());
    let prompt = match body {
        Some(m) => format!("{m}\n"),
        None => format!("{CONTINUE_PROMPT}\n"),
    };
    write_atomic(&prompt_file, prompt.as_bytes()).map_err(|e| io_err("writing prompt.md", e))?;

    let codex_bin = cfg
        .codex_bin
        .clone()
        .unwrap_or_else(|| PathBuf::from("codex"));
    let spawned = exec::spawn_detached(&SpawnSpec {
        codex_bin: &codex_bin,
        cwd: &snap.cwd,
        worker_dir: &dir,
        // The NEW epoch: the shim opens `exec.<epoch>.jsonl`, so the previous
        // epoch's stream (and its ingest watermark) are untouched.
        epoch: snap.epoch,
        sandbox: snap.sandbox,
        network: snap.network,
        model: &snap.model,
        effort: &snap.effort,
        schema_path: snap.schema_path.as_deref(),
        prompt_file: &prompt_file,
        resume_session: Some(&session),
        env: &[],
    });
    let spawned = match spawned {
        Ok(p) => p,
        Err(e) => {
            // The epoch bump is already durable, so record WHY it has no
            // process rather than leaving a worker parked in `running` with
            // nobody home: `running` + ProcessExited ⇒ failed{no_result}.
            let _ = append_and_fold(
                &mut log,
                &dir,
                snap,
                EventKind::StateChange,
                json!({"fsm": "ProcessExited", "code": null}),
                Utc::now(),
            );
            return Err(e);
        }
    };

    // Process identity, recorded the same way a turn-boundary delivery records
    // it: the FSM has no row for "a new child in an epoch that is already
    // running", but the snapshot must follow the pid or `a2a kill` would aim
    // at the previous process.
    let identity = json!({
        "pid": spawned.pid,
        "start_witness": spawned.start_witness,
        "epoch": snap.epoch,
        "resume_session": session,
        // **The transport this epoch actually runs on.** `resume` always forks
        // a `codex exec resume <session>` child, so a worker that ran on the
        // app-server transport CHANGES TRANSPORT here — the broker does not own
        // the new epoch and must not be consulted about it. Recording it on the
        // row (rather than patching the snapshot) is what makes a rebuilt
        // snapshot agree: without it `reconcile` still routes the worker to a
        // broker, finds none, and commits `orphaned` on a healthy child. That
        // is the exact failure Task 18's cross-transport proof caught.
        "transport": Transport::Exec,
    });
    snap = append_and_fold(
        &mut log,
        &dir,
        snap,
        EventKind::TransportRestarted,
        identity,
        Utc::now(),
    )?;

    // `running` is guarded by the task deadline; derive it from the state we
    // just entered rather than inheriting the terminal state's (absent) one.
    reconcile::arm_deadline(&mut snap, &cfg);
    save_snapshot(&dir, &snap).map_err(|e| io_err("writing state.json", e))?;

    if !quiet {
        eprintln!(
            "a2a: {id}: {} → running (epoch {}, session {session}){}",
            from.as_str(),
            snap.epoch,
            if answer_queued {
                ", queued inbox messages follow at the next turn boundary"
            } else {
                ""
            }
        );
    }
    Ok(Outcome::from(payload(&id, &snap, message.is_some(), json_mode)))
}

/// A worker with no recorded codex session id cannot be resumed: there is no
/// conversation to continue, and spawning without `exec resume <session>` would
/// quietly start a different one (spec §7).
fn session_of(snap: &WorkerSnapshot, id: &str) -> Result<String, AppError> {
    snap.session_id.clone().ok_or_else(|| {
        AppError::new(
            "SESSION_UNKNOWN",
            ExitCode::Precondition,
            format!(
                "worker {id} has no recorded codex session id, so there is no conversation to resume"
            ),
        )
        .remedy("`a2a tail <id>` shows whether a session was ever identified; re-spawn if not")
    })
}

/// stdout stays machine-readable: the full record under `--json`, the worker id
/// otherwise (so `a2a resume <id> && a2a wait <id>` composes).
fn payload(id: &str, snap: &WorkerSnapshot, had_message: bool, json_mode: bool) -> Value {
    if json_mode {
        json!({
            "id": id,
            "state": snap.state.as_str(),
            "epoch": snap.epoch,
            "session_id": snap.session_id,
            "pid": snap.pid,
            "message_sent": had_message,
            "deadline_at": snap.deadline_at,
        })
    } else {
        Value::String(id.to_string())
    }
}

fn io_err(ctx: &str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}
