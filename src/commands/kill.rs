//! `a2a kill <id>` — stop a worker on purpose (spec §4.6).
//!
//! Two properties make this command safe to hand an operator (or a retry loop):
//!
//! * **A human stop is `cancelled`, never `failed`.** The FSM row is what
//!   guarantees it; retry policy keys off `failure_class`, and a killed worker
//!   has none, so nothing auto-restarts what a human deliberately stopped.
//! * **It is idempotent.** A worker that is already terminal short-circuits
//!   BEFORE `step()` and exits 0 with `data.already_terminal == true`. An
//!   operator re-running `kill` (or a script that kills a fleet twice) is not
//!   an error, and pretending otherwise would train people to ignore exit
//!   codes.
//!
//! The signal itself is witnessed: [`exec::kill_worker`] compares the recorded
//! process start-time witness before it signals, so a recycled PID is never hit.
//! The WAL leads the side effect (append + fsync → kill → snapshot), so a crash
//! mid-kill costs a replay, never an unrecorded stop.
//!
//! **On the app-server transport there is no signal at all.** The pid recorded
//! against such a worker is the broker's shared `codex app-server` child — every
//! worker's — so stopping one worker is `turn/interrupt` + `Cancelled`, done by
//! the broker. With the broker gone there is nothing to interrupt either: the
//! worker is marked `cancelled` and no process is touched. A worker-scoped
//! `kill -pgid` there would take the whole fleet's transport with it.

use crate::brokerclient::{self, Handshake};
use crate::commands::owner;
use crate::config;
use crate::events::{EventKind, Transport};
use crate::exec::{self, KillOutcome, Liveness};
use crate::fold::append_and_fold;
use crate::fsm::{self, Action, FsmEvent};
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::{self, reconcile};
use crate::state::{load_snapshot, save_snapshot, WorkerSnapshot};
use crate::store::EventLog;
use chrono::Utc;
use serde_json::{json, Value};

pub fn run(
    home: &A2aHome,
    id_arg: &str,
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

    // Reconcile first: killing a worker that finished a millisecond ago must
    // report the state it actually reached, not overwrite it with `cancelled`.
    let snap = reconcile(home, &cfg, &id, Utc::now())?;
    // The owner guard comes BEFORE the idempotent short-circuit: a caller with
    // no business here gets the same answer whatever state the worker is in,
    // rather than an exit 0 that reads as "you may kill this, it was just
    // already over" (spec §4.8).
    owner::guard(&id, snap.owner.as_deref(), force)?;
    if snap.state.is_terminal() {
        return Ok(Outcome::from(already_terminal(
            &id, &snap, json_mode, quiet,
        )));
    }

    // An app-server worker is the broker's to stop, because only the broker can
    // interrupt its turn. A refusal here is real; a broker that has never heard
    // of the worker (or is not there at all) falls through to the local path,
    // which records `cancelled` WITHOUT signalling anything.
    if snap.transport == Transport::AppServer {
        if let Some(outcome) = kill_via_broker(home, &id, json_mode, quiet)? {
            return Ok(outcome);
        }
    }

    // Everything below mutates the worker, so it runs under `state.lock` — the
    // same serialization point every reconcile takes.
    let _lock = reconcile::lock_state(&dir)?;
    let mut snap = load_snapshot(&dir)?;
    // The worker may have finished in the window between the reconcile above
    // and this lock. Re-checking here keeps the short-circuit BEFORE `step()`
    // in that race too, so a kill never logs a rejected transition.
    if snap.state.is_terminal() {
        return Ok(Outcome::from(already_terminal(
            &id, &snap, json_mode, quiet,
        )));
    }

    let transition =
        fsm::step(snap.state, snap.state_reason, &FsmEvent::Kill).map_err(|rejected| {
            AppError::new(
                "ILLEGAL_TRANSITION",
                ExitCode::Precondition,
                rejected.to_string(),
            )
            .remedy(rejected.reason)
        })?;

    let mut log = EventLog::open(&dir).map_err(|e| io_err("opening events.jsonl", e))?;
    let payload = json!({
        "fsm": "Kill",
        "from": snap.state.as_str(),
        "pid": snap.pid,
    });
    let from = snap.state;
    // WAL first, side effect second (spec §4.6): a crash between them replays
    // into `cancelled` and kills again, which is harmless. The reverse order
    // could leave a dead process recorded as running forever.
    snap = append_and_fold(&mut log, &dir, snap, EventKind::Killed, payload, Utc::now())?;

    // The FSM lists `KillProcess` only for the states that normally own a live
    // child; `a2a kill` additionally refuses to leave one behind, so a worker
    // the kernel still says is alive is signalled whatever the row said (the
    // witness check is what makes that safe).
    //
    // Except on the app-server transport, where the recorded pid is not this
    // worker's process at all (see the module docs).
    let outcome = if snap.transport == Transport::AppServer {
        "no_signal"
    } else if transition.actions.contains(&Action::KillProcess)
        || exec::liveness(&dir) == Liveness::Alive
    {
        signal(&snap)?
    } else {
        "no_process"
    };

    // `cancelled` carries no deadline; re-derive rather than inherit the one
    // the previous state was guarded by.
    reconcile::arm_deadline(&mut snap, &cfg);
    save_snapshot(&dir, &snap).map_err(|e| io_err("writing state.json", e))?;

    if !quiet {
        eprintln!(
            "a2a: {id}: {} → {} ({outcome})",
            from.as_str(),
            snap.state.as_str()
        );
    }
    Ok(Outcome::from(payload_for(
        &id, &snap, false, outcome, json_mode,
    )))
}

/// Ask the broker to stop one of ITS workers.
///
/// `Ok(None)` = "not the broker's problem after all": the socket is switched
/// off, no daemon is listening, or the daemon is not tracking this worker. Each
/// of those is a fall-through to the local path, which records the stop without
/// touching a process. Any other error is the broker's answer and is returned
/// as-is — an unreachable-but-present broker must not be quietly worked around.
fn kill_via_broker(
    home: &A2aHome,
    id: &str,
    json_mode: bool,
    quiet: bool,
) -> Result<Option<Outcome>, AppError> {
    if brokerclient::disabled() {
        return Ok(None);
    }
    let Ok(Some(handshake)) = brokerclient::try_handshake(home.root()) else {
        return Ok(None); // no broker: nothing to interrupt, nothing to signal
    };
    let Handshake::Ready(mut client) = handshake else {
        return Ok(None); // a broker we cannot speak to: record the stop locally
    };
    match client.request("kill", json!({"id": id})) {
        Ok(_) => {}
        // The daemon is up but does not own this worker (it was spawned against
        // a previous instance): its files are ours to finish.
        Err(e) if e.code == "NO_SUCH_WORKER" => return Ok(None),
        Err(e) => return Err(e),
    }
    // The broker's actor has already committed `Cancelled` under the same
    // `state.lock` this process would have taken, so the file is the answer.
    let snap = load_snapshot(&home.worker_dir(id))?;
    if !quiet {
        eprintln!("a2a: {id}: {} (via the broker)", snap.state.as_str());
    }
    Ok(Some(Outcome::from(payload_for(
        id, &snap, false, "broker", json_mode,
    ))))
}

/// Witness-checked group kill of the recorded process. A worker with no pid on
/// record (killed before its first `ProcessStarted`) is not an error — there is
/// simply nothing to signal.
fn signal(snap: &WorkerSnapshot) -> Result<&'static str, AppError> {
    let (Some(pid), Some(witness)) = (snap.pid, snap.pid_start_witness.as_deref()) else {
        return Ok("no_process");
    };
    Ok(match exec::kill_worker(pid, witness)? {
        KillOutcome::Killed => "killed",
        KillOutcome::AlreadyDead => "already_dead",
        // The pid was recycled onto someone else's process: signalling it would
        // be worse than not killing at all. The state is still `cancelled`.
        KillOutcome::WitnessMismatch => "witness_mismatch",
    })
}

fn already_terminal(id: &str, snap: &WorkerSnapshot, json_mode: bool, quiet: bool) -> Value {
    if !quiet {
        eprintln!(
            "a2a: {id}: already {} — nothing to kill",
            snap.state.as_str()
        );
    }
    payload_for(id, snap, true, "already_terminal", json_mode)
}

/// stdout stays machine-readable: the full record under `--json`, the resulting
/// state otherwise (the same spelling `a2a status` prints).
fn payload_for(
    id: &str,
    snap: &WorkerSnapshot,
    already_terminal: bool,
    outcome: &str,
    json_mode: bool,
) -> Value {
    if json_mode {
        json!({
            "id": id,
            "state": snap.state.as_str(),
            "already_terminal": already_terminal,
            "signal": outcome,
            "epoch": snap.epoch,
            "pid": snap.pid,
        })
    } else {
        Value::String(snap.state.as_str().to_string())
    }
}

fn io_err(ctx: &str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}
