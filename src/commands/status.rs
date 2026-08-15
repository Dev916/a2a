//! `a2a status <id>` — the canonical read path.
//!
//! In Phase 1 there is no broker, so reading a worker is what advances it
//! (spec §4.6): this command resolves the id, runs [`reconcile`], and reports
//! the state it committed. The exit code IS the answer — an orchestrator can
//! branch on it without parsing anything — so a failed worker exits 4 while
//! still emitting a complete `data` payload.

use crate::config;
use crate::fsm::WorkerState;
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use crate::state::WorkerSnapshot;
use chrono::Utc;
use serde_json::{json, Value};

/// The state → exit code contract (plan Task 13). Non-terminal states are 0:
/// "still working" is not an error, it is the answer. `wait` is the command
/// that turns "not terminal yet" into exit 6.
fn exit_for(state: WorkerState) -> ExitCode {
    match state {
        WorkerState::Done | WorkerState::Cancelled => ExitCode::Success,
        WorkerState::Spawned | WorkerState::Queued | WorkerState::Running => ExitCode::Success,
        WorkerState::Failed => ExitCode::WorkerFailed,
        WorkerState::AwaitingInput | WorkerState::Escalated => ExitCode::Blocked,
        WorkerState::Orphaned => ExitCode::Orphaned,
        // Written by a newer a2a: reported, never silently normalized.
        WorkerState::Unknown => ExitCode::Internal,
    }
}

pub fn run(
    home: &A2aHome,
    id_arg: &str,
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
    let snap = reconcile(home, &cfg, &id, Utc::now())?;

    if !quiet {
        // stderr carries the human decoration; stdout stays machine-readable.
        eprintln!(
            "a2a: {id} {}{} (epoch {}, {} lane)",
            snap.state.as_str(),
            snap.failure_class
                .and_then(|c| serde_json::to_value(c).ok())
                .and_then(|v| v.as_str().map(str::to_string))
                .map(|c| format!("{{{c}}}"))
                .unwrap_or_default(),
            snap.epoch,
            snap.lane
        );
    }
    let data = if json_mode {
        payload(&id, &snap)
    } else {
        Value::String(snap.state.as_str().to_string())
    };
    Ok(Outcome::with(data, exit_for(snap.state)))
}

fn payload(id: &str, snap: &WorkerSnapshot) -> Value {
    json!({
        "id": id,
        "lane": snap.lane.as_str(),
        "state": snap.state.as_str(),
        "state_reason": snap.state_reason,
        "failure_class": snap.failure_class,
        "epoch": snap.epoch,
        "transport": snap.transport,
        "sandbox": snap.sandbox.as_str(),
        "network": snap.network,
        "model": snap.model,
        "effort": snap.effort,
        "cwd": snap.cwd,
        "session_id": snap.session_id,
        "pid": snap.pid,
        "created_at": snap.created_at,
        "entered_state_at": snap.entered_state_at,
        "updated_at": snap.updated_at,
        "deadline_at": snap.deadline_at,
        "last_event_seq": snap.last_event_seq,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_follow_the_state_contract() {
        for (state, code) in [
            (WorkerState::Done, ExitCode::Success),
            (WorkerState::Cancelled, ExitCode::Success),
            (WorkerState::Spawned, ExitCode::Success),
            (WorkerState::Queued, ExitCode::Success),
            (WorkerState::Running, ExitCode::Success),
            (WorkerState::Failed, ExitCode::WorkerFailed),
            (WorkerState::AwaitingInput, ExitCode::Blocked),
            (WorkerState::Escalated, ExitCode::Blocked),
            (WorkerState::Orphaned, ExitCode::Orphaned),
        ] {
            assert_eq!(exit_for(state), code, "{}", state.as_str());
        }
    }
}
