//! `a2a send <id> <message>` — queue a message, deliver it as soon as the
//! transport allows (spec §4.3, §4.7).
//!
//! Sending is two separable things, and the state machine plus the transport
//! decide which one happens:
//!
//! * **Queue** always. The message lands in `inbox/` before anything else can
//!   fail, so `send` never loses an operator's words.
//! * **Deliver** at a turn boundary — or, on the live transport, *now*.
//!   `awaiting-input`/`escalated` carry `Action::DeliverInbox`, so the message
//!   goes out immediately as a resumed turn on either transport. A `running`
//!   worker is mid-turn, and there the two transports genuinely differ: the
//!   exec transport has no way in (the FSM row is `Running + Send → Running`
//!   with no action, and the queued message is picked up by whoever observes
//!   the next turn end), while the app-server transport steers the live turn
//!   through the broker (`turn/steer`, spec §4.3).
//!
//! **Who delivers** follows from ownership, not from taste: an app-server
//! worker's directory belongs to the broker's `WorkerActor`, and the CLI's
//! reconcile is read-only over it. So this command queues and the *broker*
//! delivers. With no broker to route through, it falls back to the Phase-1
//! path, which queues without delivering — the worker's messages are still
//! durable, and `a2a resume` picks them up.
//!
//! `--queue-only` is the batching escape hatch: a pure enqueue with no
//! reconcile, no `step()` and no broker at all, so an orchestrator can stage
//! several messages and let the final plain `send` carry the whole backlog in
//! one turn. That holds on both transports — the broker is handed the rendered
//! backlog, not just the newest message.
//!
//! A terminal worker rejects `Send` and the rejection is surfaced verbatim as
//! `ILLEGAL_TRANSITION` (exit 9) — including the FSM's own remedy, which names
//! `a2a resume` as the command that WOULD work. That check runs BEFORE anything
//! is queued, on both paths.

use crate::brokerclient::{self, Handshake};
use crate::commands::owner;
use crate::config;
use crate::events::{EventKind, Transport};
use crate::fold::append_and_fold;
use crate::fsm::{self, Action, FsmEvent};
use crate::home::A2aHome;
use crate::ids;
use crate::inbox::{self, MsgKind};
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::{self, reconcile};
use crate::state::{load_snapshot, save_snapshot, WorkerSnapshot};
use crate::store::EventLog;
use chrono::Utc;
use serde_json::{json, Value};
use std::path::Path;

// Every argument here is a distinct CLI flag; bundling them into a struct would
// move the same list one file to the left (the same call the two helpers below
// already make).
#[allow(clippy::too_many_arguments)]
pub fn run(
    home: &A2aHome,
    id_arg: &str,
    message: &str,
    from: &str,
    kind: MsgKind,
    queue_only: bool,
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

    // --queue-only: a pure enqueue. No reconcile, no step(), no delivery — so
    // it is legal from ANY state, including one that would reject `Send`. The
    // orchestrator uses it to batch; nothing about the worker changes.
    if queue_only {
        // The one guard on this path, and it deliberately does NOT reconcile:
        // `--queue-only` is legal from any state, so the snapshot is read only
        // for its owner. A worker with no readable snapshot is treated as
        // unowned — the same way the fold treats a missing owner key — because
        // an unreadable checkpoint must not become an accidental lock.
        owner::guard(
            &id,
            load_snapshot(&dir).ok().and_then(|s| s.owner).as_deref(),
            force,
        )?;
        let seq = inbox::queue_message(&dir, from, kind, message)
            .map_err(|e| io_err("queueing the message", e))?;
        if !quiet {
            eprintln!("a2a: {id}: queued message {seq} (not delivered: --queue-only)");
        }
        return Ok(Outcome::from(payload(&id, seq, false, None, json_mode)));
    }

    let snap = reconcile(home, &cfg, &id, Utc::now())?;
    // Before the machine is consulted: a caller who does not own this worker
    // must not learn its state from an `ILLEGAL_TRANSITION` remedy, and must
    // certainly not have their message queued on the way to being refused.
    owner::guard(&id, snap.owner.as_deref(), force)?;
    let transition =
        fsm::step(snap.state, snap.state_reason, &FsmEvent::Send).map_err(|rejected| {
            AppError::new(
                "ILLEGAL_TRANSITION",
                ExitCode::Precondition,
                rejected.to_string(),
            )
            .remedy(rejected.reason)
        })?;
    let deliver_now = transition.actions.contains(&Action::DeliverInbox);

    // The live transport: this worker's files AND its open turn belong to the
    // broker, so the message is queued here and delivered there — mid-turn when
    // a turn is open, as a fresh turn when one is not. `None` means there was no
    // broker to route through, and the Phase-1 path below takes over.
    if snap.transport == Transport::AppServer {
        if let Some(outcome) = send_via_broker(
            home, &id, &dir, &snap, message, from, kind, json_mode, quiet,
        )? {
            return Ok(outcome);
        }
    }

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
    Ok(Outcome::from(payload(
        &id,
        seq,
        deliver_now,
        None,
        json_mode,
    )))
}

/// Hand the message to the broker that owns this worker.
///
/// `Ok(None)` = "not the broker's problem after all": the socket is switched
/// off, no daemon is listening, or the one that is speaks another protocol.
/// Each of those falls through to the Phase-1 path, and each is decided BEFORE
/// anything is queued — so a fall-through cannot queue the message twice.
///
/// The queue leads the request on purpose: delivering, broker-side, also
/// *acknowledges* this worker's inbox, so the operator's words must already be
/// in it. Handing over the rendered backlog rather than the single new message
/// is what keeps `--queue-only` staging meaningful on this transport.
#[allow(clippy::too_many_arguments)]
fn send_via_broker(
    home: &A2aHome,
    id: &str,
    dir: &Path,
    snap: &WorkerSnapshot,
    message: &str,
    from: &str,
    kind: MsgKind,
    json_mode: bool,
    quiet: bool,
) -> Result<Option<Outcome>, AppError> {
    if brokerclient::disabled() {
        return Ok(None);
    }
    let Ok(Some(handshake)) = brokerclient::try_handshake(home.root()) else {
        return Ok(None); // no broker: the message waits in the queue
    };
    let Handshake::Ready(mut client) = handshake else {
        return Ok(None); // a broker we cannot speak to: queue and say so
    };

    let seq = inbox::queue_message(dir, from, kind, message)
        .map_err(|e| io_err("queueing the message", e))?;
    let pending = inbox::undelivered(dir, snap.last_delivered_seq)
        .map_err(|e| io_err("reading the inbox", e))?;
    let text = inbox::render(&pending);

    let answer = match client.request("send", json!({"id": id, "text": text})) {
        Ok(v) => v,
        // The daemon is up but does not own this worker (it was spawned against
        // a previous instance). The message is already durable; whoever adopts
        // the worker next delivers it.
        Err(e) if e.code == "NO_SUCH_WORKER" => {
            if !quiet {
                eprintln!(
                    "a2a: {id}: queued message {seq} (the broker is not tracking this worker yet)"
                );
            }
            return Ok(Some(Outcome::from(payload(
                id, seq, false, None, json_mode,
            ))));
        }
        Err(e) => return Err(e),
    };

    // Which door it went through is the broker's answer, and it is the same
    // string the `inbox_delivered` envelope records.
    let send_path = answer
        .get("send_path")
        .and_then(Value::as_str)
        .map(str::to_string);
    if !quiet {
        eprintln!(
            "a2a: {id}: queued message {seq} and delivered it via the broker ({})",
            send_path.as_deref().unwrap_or("delivered")
        );
    }
    Ok(Some(Outcome::from(payload(
        id,
        seq,
        true,
        send_path.as_deref(),
        json_mode,
    ))))
}

/// stdout stays machine-readable: the full record under `--json`, the queued
/// seq otherwise (an orchestrator's cursor into its own messages).
///
/// `send_path` appears only when something was actually delivered over the live
/// transport — an absent key means "nothing was steered", which is exactly what
/// the exec transport and `--queue-only` mean.
fn payload(id: &str, seq: u64, delivered: bool, send_path: Option<&str>, json_mode: bool) -> Value {
    if !json_mode {
        return Value::String(seq.to_string());
    }
    let mut v = json!({"id": id, "seq": seq, "queued": true, "delivered": delivered});
    if let (Some(obj), Some(path)) = (v.as_object_mut(), send_path) {
        obj.insert("send_path".to_string(), json!(path));
    }
    v
}

fn io_err(ctx: &str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}
