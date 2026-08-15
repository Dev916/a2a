//! `a2a wait <id>… [--until …] [--timeout <dur>]` — the monitor primitive
//! (spec §4.1).
//!
//! An orchestrator that polls `a2a status` in a loop burns its context window
//! on answers it already knows. `wait` is the fix: **one call per state
//! change**. It blocks, reconciling every watched worker every 500 ms (Phase 1
//! has no broker to push an event, so the poll lives here where it costs the
//! orchestrator nothing), and returns the moment a worker reaches the stop set.
//!
//! **The exit code encodes the state reached** — 0 done/cancelled · 4 failed ·
//! 5 awaiting-input/escalated · 10 orphaned — so a caller branches on `$?`
//! without parsing anything. **Timeout expiry is exit 6** ("not terminal yet",
//! spec §4.1); 8 is reserved for a2a-internal timeouts like a wedged
//! `state.lock`, and conflating the two would tell an orchestrator to give up
//! on a worker that is merely slow.
//!
//! `--until` picks the stop set:
//!
//! | value      | stops on                                               |
//! |------------|--------------------------------------------------------|
//! | `any`      | anything that is not spawned/queued/running (default)  |
//! | `blocked`  | awaiting-input / escalated                             |
//! | `terminal` | done / failed / cancelled                              |
//! | `done`     | done                                                   |
//!
//! Every stop set additionally stops on a **terminal** state, because terminal
//! states are absorbing (only `a2a resume` leaves one): `--until done` on a
//! worker that just failed would otherwise block until its timeout for a
//! transition that can never come. The exit code still encodes the state
//! actually reached, so `--until done` returning 4 is unambiguous.
//!
//! With several ids, the **first** one to reach the stop set wins and its id is
//! printed — the "which of my workers needs me next?" primitive. Ties are
//! broken by argument order, so the result is deterministic.

use crate::config;
use crate::fsm::WorkerState;
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use crate::state::WorkerSnapshot;
use chrono::Utc;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

/// Reconcile cadence. Fast enough that a human never notices, slow enough that
/// a fleet-wide wait is not a spin loop over N `state.lock` acquisitions.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Safety net when `--timeout` is omitted: one hour, matching the default lane
/// `task_timeout_secs`. A wait must always terminate — an escalated or orphaned
/// worker can sit still forever, and a CLI that hangs forever is a CLI an
/// orchestrator cannot supervise.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3600);

#[derive(clap::ValueEnum, Clone, Copy, PartialEq, Debug)]
#[clap(rename_all = "kebab-case")]
pub enum Until {
    /// Anything that is not spawned/queued/running.
    Any,
    /// Only awaiting-input / escalated.
    Blocked,
    /// done / failed / cancelled.
    Terminal,
    /// Only done.
    Done,
}

impl Until {
    fn as_str(self) -> &'static str {
        match self {
            Until::Any => "any",
            Until::Blocked => "blocked",
            Until::Terminal => "terminal",
            Until::Done => "done",
        }
    }
}

/// `15s`, `5m`, `2h` — and a bare integer means seconds. Deliberately tiny:
/// this is a CLI flag, not a duration DSL, and an unparseable value is a usage
/// error (exit 2 via clap) rather than something to guess at.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty duration (expected e.g. 30s, 5m, 2h)".to_string());
    }
    let (digits, mult) = match t.chars().last().expect("non-empty") {
        's' | 'S' => (&t[..t.len() - 1], 1),
        'm' | 'M' => (&t[..t.len() - 1], 60),
        'h' | 'H' => (&t[..t.len() - 1], 3600),
        '0'..='9' => (t, 1),
        other => {
            return Err(format!(
                "unknown duration unit `{other}` in `{s}` (expected s, m or h)"
            ))
        }
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "`{s}` is not a duration (expected <number><s|m|h>, e.g. 30s)"
        ));
    }
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("`{s}` is too large to be a duration"))?;
    n.checked_mul(mult)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("`{s}` is too large to be a duration"))
}

pub fn run(
    home: &A2aHome,
    id_args: &[String],
    until: Until,
    timeout: Duration,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let (cfg, _origins, warnings) = config::load(home)?;
    if !quiet {
        for w in &warnings {
            eprintln!("a2a: warning: {w}");
        }
    }
    // Resolve every id BEFORE blocking: a typo must fail in milliseconds with
    // exit 3, not after a fifteen-second wait on a worker that never existed.
    let ids: Vec<String> = id_args
        .iter()
        .map(|arg| ids::resolve(home, arg))
        .collect::<Result<_, _>>()?;

    let started = Instant::now();
    loop {
        // One clock per sweep, so two workers' observations are comparable.
        let now = Utc::now();
        for id in &ids {
            let snap = reconcile(home, &cfg, id, now)?;
            if reached(until, snap.state) {
                if !quiet {
                    eprintln!(
                        "a2a: {id} {} after {}s",
                        snap.state.as_str(),
                        started.elapsed().as_secs()
                    );
                }
                let exit = exit_for(snap.state);
                let data = if json_mode {
                    payload(id, &snap, started.elapsed(), &ids)
                } else {
                    // stdout carries the id — with several watched workers that
                    // is the answer ("this one changed"), and with one it is
                    // still the identifier a caller pipes onward.
                    Value::String(id.clone())
                };
                return Ok(Outcome::with(data, exit));
            }
        }
        // Checked after a full sweep, so a worker that finishes exactly at the
        // deadline is reported as finished rather than as a timeout.
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            if !quiet {
                eprintln!(
                    "a2a: timed out after {}s waiting for {} (--until {})",
                    timeout.as_secs(),
                    ids.join(", "),
                    until.as_str()
                );
            }
            let data = if json_mode {
                timeout_payload(home, &cfg, &ids, elapsed, until)
            } else {
                Value::Null
            };
            return Ok(Outcome::with(data, ExitCode::NotTerminal));
        }
        std::thread::sleep(POLL_INTERVAL.min(timeout - elapsed));
    }
}

/// Has this worker reached a state `wait` should return on?
///
/// Terminal is always a stop: it is absorbing, so continuing to poll could only
/// ever end in a timeout that misreports a finished worker as "not terminal
/// yet". `unknown` stops too — a snapshot this binary cannot interpret will not
/// become interpretable by waiting.
fn reached(until: Until, state: WorkerState) -> bool {
    if state.is_terminal() || state == WorkerState::Unknown {
        return true;
    }
    match until {
        Until::Any => !matches!(
            state,
            WorkerState::Spawned | WorkerState::Queued | WorkerState::Running
        ),
        Until::Blocked => matches!(state, WorkerState::AwaitingInput | WorkerState::Escalated),
        // Both are subsets of terminal, already handled above.
        Until::Terminal | Until::Done => false,
    }
}

/// The state → exit code contract for `wait`. Identical to `result`'s: a
/// spawned/queued/running worker means "not terminal yet" (exit 6), which is
/// also exactly what a timeout means — the two paths agree by construction.
fn exit_for(state: WorkerState) -> ExitCode {
    match state {
        WorkerState::Done | WorkerState::Cancelled => ExitCode::Success,
        WorkerState::Failed => ExitCode::WorkerFailed,
        WorkerState::AwaitingInput | WorkerState::Escalated => ExitCode::Blocked,
        WorkerState::Orphaned => ExitCode::Orphaned,
        WorkerState::Spawned | WorkerState::Queued | WorkerState::Running => {
            ExitCode::NotTerminal
        }
        WorkerState::Unknown => ExitCode::Internal,
    }
}

fn payload(id: &str, snap: &WorkerSnapshot, waited: Duration, watched: &[String]) -> Value {
    json!({
        "id": id,
        "state": snap.state.as_str(),
        "state_reason": snap.state_reason,
        "failure_class": snap.failure_class,
        "epoch": snap.epoch,
        "timed_out": false,
        "waited_ms": waited.as_millis() as u64,
        "watched": watched,
    })
}

/// On expiry the caller still gets every watched worker's last known state —
/// "which of them is holding me up" is the next question, and a second round of
/// `status` calls to answer it would defeat the purpose of `wait`.
fn timeout_payload(
    home: &A2aHome,
    cfg: &config::Config,
    ids: &[String],
    waited: Duration,
    until: Until,
) -> Value {
    let now = Utc::now();
    let workers: Vec<Value> = ids
        .iter()
        .map(|id| {
            let state = reconcile(home, cfg, id, now)
                .map(|s| s.state.as_str().to_string())
                .unwrap_or_else(|_| "unknown".to_string());
            json!({"id": id, "state": state})
        })
        .collect();
    json!({
        "id": Value::Null,
        "timed_out": true,
        "until": until.as_str(),
        "waited_ms": waited.as_millis() as u64,
        "watched": ids,
        "workers": workers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse_seconds_minutes_and_hours() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("15S").unwrap(), Duration::from_secs(15));
        assert_eq!(parse_duration(" 45 ").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_duration("0s").unwrap(), Duration::from_secs(0));
    }

    #[test]
    fn a_bad_duration_names_what_was_expected() {
        for bad in ["", "s", "abc", "5d", "5 s", "-1s", "1.5h", "m5"] {
            let err = parse_duration(bad).unwrap_err();
            assert!(!err.is_empty(), "{bad}");
        }
        assert!(parse_duration("99999999999999999999s").is_err());
        assert!(parse_duration("9999999999999999h").is_err(), "overflow");
    }

    #[test]
    fn until_any_stops_on_everything_that_left_running() {
        for state in [
            WorkerState::Spawned,
            WorkerState::Queued,
            WorkerState::Running,
        ] {
            assert!(!reached(Until::Any, state), "{}", state.as_str());
        }
        for state in [
            WorkerState::AwaitingInput,
            WorkerState::Escalated,
            WorkerState::Done,
            WorkerState::Failed,
            WorkerState::Cancelled,
            WorkerState::Orphaned,
        ] {
            assert!(reached(Until::Any, state), "{}", state.as_str());
        }
    }

    #[test]
    fn until_blocked_ignores_running_but_never_waits_on_an_absorbing_state() {
        assert!(!reached(Until::Blocked, WorkerState::Running));
        assert!(!reached(Until::Blocked, WorkerState::Orphaned));
        assert!(reached(Until::Blocked, WorkerState::AwaitingInput));
        assert!(reached(Until::Blocked, WorkerState::Escalated));
        // terminal is absorbing: stop, and let the exit code say what happened
        assert!(reached(Until::Blocked, WorkerState::Done));
        assert!(reached(Until::Blocked, WorkerState::Failed));
    }

    #[test]
    fn until_terminal_and_done_wait_through_blocked() {
        for until in [Until::Terminal, Until::Done] {
            assert!(!reached(until, WorkerState::AwaitingInput));
            assert!(!reached(until, WorkerState::Escalated));
            assert!(!reached(until, WorkerState::Running));
            assert!(reached(until, WorkerState::Done));
            assert!(reached(until, WorkerState::Failed));
            assert!(reached(until, WorkerState::Cancelled));
        }
    }

    #[test]
    fn exit_codes_encode_the_state_reached() {
        for (state, code) in [
            (WorkerState::Done, ExitCode::Success),
            (WorkerState::Cancelled, ExitCode::Success),
            (WorkerState::Failed, ExitCode::WorkerFailed),
            (WorkerState::AwaitingInput, ExitCode::Blocked),
            (WorkerState::Escalated, ExitCode::Blocked),
            (WorkerState::Orphaned, ExitCode::Orphaned),
            // only reachable on expiry, and expiry is exit 6 by contract
            (WorkerState::Running, ExitCode::NotTerminal),
            (WorkerState::Unknown, ExitCode::Internal),
        ] {
            assert_eq!(exit_for(state), code, "{}", state.as_str());
        }
    }
}
