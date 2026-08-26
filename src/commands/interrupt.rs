//! `a2a interrupt <id>` — end the turn a worker is running right now
//! (spec §4.1, §4.3).
//!
//! This is the one command that has no exec-transport spelling at all. `codex
//! exec` owns its stdin for the whole turn and there is no side channel into
//! it; the only worker-scoped thing a signal could do there is *end the
//! worker*, which is `a2a kill` and a different answer. So an exec worker is
//! refused with a **static** precondition (exit 9): the verdict comes from the
//! worker's own record, and it does not change if a broker appears.
//!
//! On the app-server transport there is a real answer. The broker knows the
//! open `turnId`, sends `turn/interrupt`, and commits `awaiting-input
//! {interrupted}` — the steering pause the FSM has always had a row for. The
//! CLI never touches the worker's files here: the broker owns them, and the
//! snapshot this command reports is the one the broker has already written.
//!
//! Two deliberate choices, both recorded in the Phase 2 plan:
//!
//! * **Interrupting a turn that is already over is exit 0 with `data.noop`**,
//!   not an error. An operator racing a turn's last millisecond has not made a
//!   mistake, and a retry loop that treats "too late" as a failure would fight
//!   it. (The spec wanted a dedicated exit code; the pinned table has no free
//!   slot worth burning.)
//! * **A missing broker is exit 7, never a quiet success.** The turn is still
//!   running somewhere as far as anyone here can tell; saying "interrupted"
//!   would be a lie the caller would act on. There is no autostart either — a
//!   fresh daemon has never heard of this worker's turn.

use crate::brokerclient;
use crate::commands::owner;
use crate::config;
use crate::events::Transport;
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use crate::state::load_snapshot;
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

    // Reconcile first, for the same reason `kill` does: the transport (and the
    // state this command reports) must come from a worker that has been read
    // rather than remembered.
    let snap = reconcile(home, &cfg, &id, Utc::now())?;
    // Who is asking, before what they asked for: "not yours" is the truer
    // answer than "your transport cannot do that" (spec §4.8).
    owner::guard(&id, snap.owner.as_deref(), force)?;
    if snap.transport != Transport::AppServer {
        return Err(AppError::new(
            "TRANSPORT_UNSUPPORTED",
            ExitCode::Precondition,
            format!(
                "worker {id} runs on the `{}` transport, which has no way into a live turn",
                transport_name(snap.transport)
            ),
        )
        .remedy(
            "`a2a kill <id>` stops it; `a2a send <id> <message>` reaches it at the next turn \
             boundary. Spawn with `--transport app-server` for mid-turn control",
        ));
    }

    // Reachability, not a static precondition: exit 7 (Global Constraints).
    if brokerclient::disabled() {
        return Err(brokerclient::disabled_error());
    }
    // `connect`, never `connect_or_start`: the turn belongs to the daemon that
    // opened it, and a brand-new one could only answer NO_SUCH_WORKER.
    let mut client = brokerclient::connect(home.root())?;
    let answer = client.request("interrupt", json!({"id": &id}))?;
    let noop = answer.get("noop").and_then(Value::as_bool).unwrap_or(false);

    // The broker's actor committed under the same `state.lock` this process
    // would have taken, so the file — not our pre-request copy — is the answer.
    let snap = load_snapshot(&home.worker_dir(&id))?;
    if !quiet {
        eprintln!(
            "a2a: {id}: {}",
            if noop {
                format!("already {} — no turn to interrupt", snap.state.as_str())
            } else {
                format!("turn interrupted → {}", snap.state.as_str())
            }
        );
    }
    Ok(Outcome::from(payload(&id, &snap, noop, json_mode)))
}

/// The wire spelling, taken from serde so the message can never drift from the
/// on-disk vocabulary.
fn transport_name(t: Transport) -> String {
    serde_json::to_value(t)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// stdout stays machine-readable: the full record under `--json`, the resulting
/// state otherwise (the same spelling `a2a status` prints).
fn payload(id: &str, snap: &crate::state::WorkerSnapshot, noop: bool, json_mode: bool) -> Value {
    if json_mode {
        json!({
            "id": id,
            "state": snap.state.as_str(),
            "state_reason": snap.state_reason,
            "noop": noop,
            "epoch": snap.epoch,
        })
    } else {
        Value::String(snap.state.as_str().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_names_match_the_on_disk_spelling() {
        assert_eq!(transport_name(Transport::AppServer), "app-server");
        assert_eq!(transport_name(Transport::Exec), "exec");
        assert_eq!(transport_name(Transport::Unknown), "unknown");
    }
}
