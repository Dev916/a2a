//! `a2a send <id> <message>` — queue a message, deliver it at the turn
//! boundary (spec §4.3, §4.7).
//!
//! Sending is two separable things, and the state machine decides which one
//! happens:
//!
//! * **Queue** always. The message lands in `inbox/` before anything else can
//!   fail, so `send` never loses an operator's words.
//! * **Deliver** only from a turn boundary. `awaiting-input`/`escalated` carry
//!   `Action::DeliverInbox`, so the message goes out immediately as a resumed
//!   turn. A `running` worker is mid-turn and the exec transport has no way in:
//!   the FSM row is `Running + Send → Running` with no action, and the queued
//!   message is picked up by whoever observes the next turn end (`reconcile`).
//!
//! `--queue-only` is the batching escape hatch: a pure enqueue with no
//! reconcile and no `step()` at all, so an orchestrator can stage several
//! messages and let the final plain `send` carry the whole backlog in one turn.
//!
//! A terminal worker rejects `Send` and the rejection is surfaced verbatim as
//! `ILLEGAL_TRANSITION` (exit 9) — including the FSM's own remedy, which names
//! `a2a resume` as the command that WOULD work.

use crate::config;
use crate::events::EventKind;
use crate::fold::append_and_fold;
use crate::fsm::{self, Action, FsmEvent};
use crate::home::A2aHome;
use crate::ids;
use crate::inbox::{self, MsgKind};
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::{self, reconcile};
use crate::state::{load_snapshot, save_snapshot};
use crate::store::EventLog;
use chrono::Utc;
use serde_json::{json, Value};

pub fn run(
    home: &A2aHome,
    id_arg: &str,
    message: &str,
    from: &str,
    kind: MsgKind,
    queue_only: bool,
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

    // --queue-only: a pure enqueue. No reconcile, no step(), no delivery — so
    // it is legal from ANY state, including one that would reject `Send`. The
    // orchestrator uses it to batch; nothing about the worker changes.
    if queue_only {
        let seq = inbox::queue_message(&dir, from, kind, message)
            .map_err(|e| io_err("queueing the message", e))?;
        if !quiet {
            eprintln!("a2a: {id}: queued message {seq} (not delivered: --queue-only)");
        }
        return Ok(Outcome::from(payload(&id, seq, false, json_mode)));
    }

    let snap = reconcile(home, &cfg, &id, Utc::now())?;
    let transition = fsm::step(snap.state, snap.state_reason, &FsmEvent::Send)
        .map_err(|rejected| {
            AppError::new(
                "ILLEGAL_TRANSITION",
                ExitCode::Precondition,
                rejected.to_string(),
            )
            .remedy(rejected.reason)
        })?;
    let deliver_now = transition.actions.contains(&Action::DeliverInbox);

    // Preconditions BEFORE the queue and the WAL: a `send` that cannot possibly
    // be delivered must not first move the worker to `running`.
    if deliver_now && snap.session_id.is_none() {
        return Err(AppError::new(
            "SESSION_UNKNOWN",
            ExitCode::Precondition,
            format!("worker {id} has no recorded codex session id, so it cannot be resumed"),
        )
        .remedy("`a2a tail <id>` shows whether a session was ever identified; re-spawn if not"));
    }

    let seq = inbox::queue_message(&dir, from, kind, message)
        .map_err(|e| io_err("queueing the message", e))?;

    // Everything below mutates the worker, so it runs under `state.lock` —
    // the same serialization point every reconcile takes.
    let _lock = reconcile::lock_state(&dir)?;
    let now = Utc::now();
    let mut snap = load_snapshot(&dir)?;
    let mut log = EventLog::open(&dir).map_err(|e| io_err("opening events.jsonl", e))?;
    // WAL first, side effect second (spec §4.6). The Send row is recorded even
    // when it is a self-loop on `running`: the operator acted, and `a2a tail`
    // is where that becomes visible.
    snap = append_and_fold(
        &mut log,
        &dir,
        snap,
        EventKind::StateChange,
        json!({"fsm": "Send", "seq": seq, "from": from, "kind": kind.as_str()}),
        now,
    )?;

    if deliver_now {
        let delivery = inbox::deliver(&dir, &cfg, &snap)?;
        // Recorded AFTER the child exists: the queue is at-least-once, and this
        // watermark is what makes the re-delivery a no-op (see inbox.rs).
        snap = append_and_fold(
            &mut log,
            &dir,
            snap,
            EventKind::InboxDelivered,
            json!({
                "watermark": delivery.watermark,
                "seqs": delivery.seqs,
                "pid": delivery.pid,
                "start_witness": delivery.start_witness,
            }),
            now,
        )?;
    }
    // The blocked deadline this worker was carrying belongs to a state it just
    // left; re-derive from the state it is in now so the next reader fires the
    // right one (and never the wrong one).
    reconcile::arm_deadline(&mut snap, &cfg);
    save_snapshot(&dir, &snap).map_err(|e| io_err("writing state.json", e))?;

    if !quiet {
        eprintln!(
            "a2a: {id}: queued message {seq}{}",
            if deliver_now {
                " and delivered it (resumed turn)"
            } else {
                " (delivery at the next turn boundary)"
            }
        );
    }
    Ok(Outcome::from(payload(&id, seq, deliver_now, json_mode)))
}

/// stdout stays machine-readable: the full record under `--json`, the queued
/// seq otherwise (an orchestrator's cursor into its own messages).
fn payload(id: &str, seq: u64, delivered: bool, json_mode: bool) -> Value {
    if json_mode {
        json!({"id": id, "seq": seq, "queued": true, "delivered": delivered})
    } else {
        Value::String(seq.to_string())
    }
}

fn io_err(ctx: &str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}
