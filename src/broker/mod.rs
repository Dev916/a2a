//! The broker daemon's actor tree (spec §4.2).
//!
//! Everything under this module is async, and everything under this module
//! obeys the actor discipline the rest of `a2a` is built to allow:
//!
//! * **One owner per piece of state.** `Arc<Mutex<…>>`/`Arc<RwLock<…>>` are
//!   forbidden here (source-scan-tested): state that two tasks need is reached
//!   by sending a message to the task that owns it, never by sharing a lock.
//! * **Bounded channels only.** Per-worker mailboxes are bounded at
//!   [`worker_actor::MAILBOX_CAPACITY`] (spec §4.5); a producer that outruns a
//!   consumer gets backpressure, and the one stream that cannot apply
//!   backpressure — the trace tail — drops the OLDEST and says so in the log.
//! * **Cancellation safety.** No `write_all` inside a `select!`, no
//!   `std::thread::sleep`: writes go through dedicated writer tasks, and
//!   blocking file work goes through `spawn_blocking`.
//!
//! [`worker_actor::WorkerActor`] is the sole writer to one worker's directory —
//! but only *alongside* Phase 1's CLI paths, which are still perfectly entitled
//! to reconcile the same worker. What makes "sole writer" true across both is
//! that every actor write takes the same `state.lock` a reconcile takes and
//! re-opens the `EventLog` inside it.
//!
//! The one *synchronous* thing that lives here is startup reconciliation
//! ([`reap_previous_instance`]): it runs before the runtime exists, because
//! reaping the previous instance's child is a blocking, signal-and-wait job that
//! must be finished before this instance owns anything.

pub mod child;
pub mod core;
pub mod sockserv;
pub mod wire;
pub mod worker_actor;

use crate::events::Transport;
use crate::exec::{self, KillOutcome};
use crate::fsm::WorkerState;
use crate::state::{self, WorkerSnapshot};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;
use std::io::Write;
use std::path::{Path, PathBuf};

/// The CLI's front door: 0600, inside the 0700 home, peer-uid checked.
pub const BROKER_SOCK: &str = "broker.sock";

/// The singleton. Acquired with `flock` BEFORE the daemon forks and held for the
/// life of the process — the lock is what makes "one broker per home" true, and
/// the socket is only ever its consequence.
pub const BROKER_LOCK: &str = "broker.lock";

/// The daemon's own pid, 0600, written after daemonization. Ops needs a pid it
/// can signal, and the crash tests need one they can `SIGKILL`.
pub const BROKER_PID: &str = "broker.pid";

/// The daemon's stderr, and the child's: one file, because a broker log that
/// omits what the app-server said about its own death is not a log.
pub const BROKER_LOG: &str = "broker.log";

/// Frames that belong to no worker: broker-scoped notifications, unroutable
/// frames, degradation events. Append-only NDJSON, 0600.
pub const BROKER_JSONL: &str = "broker.jsonl";

/// What the daemon knows about itself, rewritten atomically on every change —
/// the file `a2a doctor` and the spawn path read to decide whether the
/// app-server transport is healthy.
pub const BROKER_HEALTH: &str = "broker-health.json";

pub fn lock_path(home: &Path) -> PathBuf {
    home.join(BROKER_LOCK)
}

pub fn pid_path(home: &Path) -> PathBuf {
    home.join(BROKER_PID)
}

// ---------------------------------------------------------------------------
// startup reconciliation
// ---------------------------------------------------------------------------

/// One process a previous instance left behind, and what became of it.
#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct ReapedChild {
    pub pid: u32,
    /// `killed` | `already-dead` | `witness-mismatch` | `kill-failed`
    pub outcome: &'static str,
    /// The workers that remembered this pid — one app-server child is every
    /// worker's, so this is usually more than one.
    pub workers: Vec<String>,
}

/// What a broker start did to the tree it inherited.
///
/// Reported through `broker status` (and `a2a status --broker`) because the
/// operator's first question after a restart is "what happened to my workers",
/// and the answer must not require reading a log.
#[derive(Serialize, Clone, Debug, Default)]
pub struct StartupReport {
    /// Worker directories with a readable snapshot.
    pub scanned: usize,
    pub reaped: Vec<ReapedChild>,
    pub reaped_at: Option<DateTime<Utc>>,
    /// Re-attached to this instance's child, in `created_at` order.
    pub adopted: Vec<String>,
    /// Live app-server workers that could NOT be re-attached (no thread to
    /// resume, or a server that refused).
    pub adoption_failed: Vec<String>,
    /// Queued workers, oldest first — the order admission control replays them
    /// in (Task 14 consumes this; nothing here admits anything).
    pub requeued: Vec<String>,
    /// Live exec workers, recorded and then left entirely alone: their
    /// processes are not this daemon's to signal and their directories are not
    /// this daemon's to write.
    pub exec_untouched: Vec<String>,
    pub adopted_at: Option<DateTime<Utc>>,
}

/// **Phase one: reap.** Kill whatever app-server child the previous instance
/// left running, identity-checked (spec §4.2).
///
/// Called BEFORE this instance launches its own child and before anything is
/// adopted, and that order is the whole point: a thread resumed while the old
/// server still holds it has two servers, and the second one to write wins
/// silently. The witness check is what keeps a recycled pid safe — a bare pid
/// from a snapshot written hours ago is not evidence of anything.
///
/// Synchronous on purpose: it runs before the runtime is built, so the
/// signal-and-wait grace loop cannot block a reactor that does not exist yet.
pub fn reap_previous_instance(home: &Path) -> StartupReport {
    let workers = scan(home);
    let mut report = StartupReport {
        scanned: workers.len(),
        ..StartupReport::default()
    };
    let ours = std::process::id();
    for (_, snap) in &workers {
        if snap.transport != Transport::AppServer || snap.state.is_terminal() {
            continue;
        }
        let (Some(pid), Some(witness)) = (snap.pid, snap.pid_start_witness.as_deref()) else {
            continue;
        };
        // A pid of 0 is "attached, not owned" (the re-attach path records one);
        // our own pid is not a corpse.
        if pid == 0 || pid == ours {
            continue;
        }
        if let Some(seen) = report.reaped.iter_mut().find(|r| r.pid == pid) {
            seen.workers.push(snap.id.clone());
            continue;
        }
        let outcome = match exec::kill_worker(pid, witness) {
            Ok(KillOutcome::Killed) => "killed",
            Ok(KillOutcome::AlreadyDead) => "already-dead",
            Ok(KillOutcome::WitnessMismatch) => "witness-mismatch",
            Err(e) => {
                eprintln!("a2a broker: reaping pid {pid}: {}: {}", e.code, e.message);
                "kill-failed"
            }
        };
        report.reaped.push(ReapedChild {
            pid,
            outcome,
            workers: vec![snap.id.clone()],
        });
    }
    report.reaped_at = Some(Utc::now());
    record_phase(home, "reap", &report);
    report
}

/// **Phase two: adopt.** Re-attach the workers that survived the previous
/// instance, and record the ones this daemon deliberately does not touch.
///
/// * `running` / `awaiting-input` / `orphaned` app-server workers are resumed
///   through the live child (`thread/resume`). Orphaned is in the set on
///   purpose: it is the state a failed re-attach lands in, and a new broker is
///   exactly the event that can undo it.
/// * `queued` workers are RE-ENQUEUED, not admitted: this records the order
///   admission control must replay them in (`created_at`, oldest first) and
///   gives them no actor.
/// * exec workers are recorded and skipped. Their processes belong to their own
///   shims, and their directories belong to the CLI's reconcile path.
pub async fn adopt_previous_workers(
    home: &Path,
    core: &core::CoreHandle,
    report: &mut StartupReport,
) {
    for (dir, snap) in scan(home) {
        if snap.transport == Transport::Exec {
            if !snap.state.is_terminal() {
                report.exec_untouched.push(snap.id.clone());
            }
            continue;
        }
        if snap.transport != Transport::AppServer {
            continue;
        }
        match snap.state {
            WorkerState::Running | WorkerState::AwaitingInput | WorkerState::Orphaned => {
                if snap.session_id.is_none() {
                    report.adoption_failed.push(snap.id.clone());
                    continue;
                }
                match core.resume_worker(dir).await {
                    Ok(_) => report.adopted.push(snap.id.clone()),
                    Err(e) => {
                        eprintln!(
                            "a2a broker: adopting {}: {}: {}",
                            snap.id, e.code, e.message
                        );
                        report.adoption_failed.push(snap.id.clone());
                    }
                }
            }
            WorkerState::Queued => report.requeued.push(snap.id.clone()),
            _ => {}
        }
    }
    report.adopted_at = Some(Utc::now());
    let home = home.to_path_buf();
    let snapshot = report.clone();
    // Through `spawn_blocking` because this is the async half: the file append
    // is small, but "small" is not a scheduling guarantee.
    let _ = tokio::task::spawn_blocking(move || record_phase(&home, "adopt", &snapshot)).await;
}

/// Every worker directory with a readable snapshot, oldest first.
///
/// `created_at` order is not cosmetic — it IS the re-enqueue order — and the id
/// tiebreak is what keeps two workers created in the same millisecond from
/// swapping places between two reads of the same tree.
fn scan(home: &Path) -> Vec<(PathBuf, WorkerSnapshot)> {
    let Ok(entries) = std::fs::read_dir(home.join("workers")) else {
        return Vec::new();
    };
    let mut out: Vec<(PathBuf, WorkerSnapshot)> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|dir| dir.is_dir())
        .filter_map(|dir| state::load_snapshot(&dir).ok().map(|snap| (dir, snap)))
        .collect();
    out.sort_by(|a, b| {
        a.1.created_at
            .cmp(&b.1.created_at)
            .then_with(|| a.1.id.cmp(&b.1.id))
    });
    out
}

/// One line per phase in `broker.jsonl`, so the ORDER of the two phases is a
/// fact on disk rather than a claim in a report.
fn record_phase(home: &Path, phase: &str, report: &StartupReport) {
    let Ok(mut f) = exec::open_append_0600(&home.join(BROKER_JSONL)) else {
        return;
    };
    let line = json!({"ts": Utc::now(), "broker": "startup", "phase": phase,
                      "report": report});
    let _ = writeln!(f, "{line}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventKind;
    use crate::fold;
    use crate::store::EventLog;

    fn seed(home: &Path, id: &str, patch: serde_json::Value) {
        let dir = home.join("workers").join(id);
        std::fs::create_dir_all(&dir).unwrap();
        let mut log = EventLog::open(&dir).unwrap();
        fold::append_and_fold(
            &mut log,
            &dir,
            WorkerSnapshot::new_for_test(id, home),
            EventKind::Spawned,
            json!({"fsm": "Spawn", "id": id, "lane": "general", "cwd": home,
                   "transport": "app-server"}),
            Utc::now(),
        )
        .unwrap();
        let path = dir.join("state.json");
        let mut value: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        for (k, v) in patch.as_object().unwrap() {
            value.as_object_mut().unwrap().insert(k.clone(), v.clone());
        }
        crate::store::write_atomic(&path, value.to_string().as_bytes()).unwrap();
    }

    /// The scan is the re-enqueue order, and it is `created_at` — never
    /// readdir order, which is whatever the filesystem feels like today.
    #[test]
    fn the_scan_is_ordered_by_created_at() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        let now = Utc::now();
        seed(
            home,
            "general-0817t1200-zzzz",
            json!({"created_at": now, "state": "queued"}),
        );
        seed(
            home,
            "general-0817t1200-aaaa",
            json!({"created_at": now - chrono::Duration::minutes(5), "state": "queued"}),
        );
        let ids: Vec<String> = scan(home).into_iter().map(|(_, s)| s.id).collect();
        assert_eq!(
            ids,
            vec![
                "general-0817t1200-aaaa".to_string(),
                "general-0817t1200-zzzz".to_string()
            ]
        );
    }

    /// A recorded pid is not evidence: without a matching start-time witness
    /// nothing is signalled, and a terminal worker's memory of a child is not a
    /// reason to signal anything either.
    #[test]
    fn reaping_is_witness_checked_and_skips_terminal_workers() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        let mut victim = std::process::Command::new("sleep")
            .arg("30")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let pid = victim.id();

        seed(
            home,
            "general-0817t1200-aaaa",
            json!({"state": "running", "pid": pid,
                   "pid_start_witness": "not the witness this pid has"}),
        );
        seed(
            home,
            "general-0817t1200-bbbb",
            json!({"state": "done", "pid": pid, "pid_start_witness": "whatever"}),
        );

        let report = reap_previous_instance(home);
        assert_eq!(report.scanned, 2);
        assert_eq!(
            report.reaped,
            vec![ReapedChild {
                pid,
                outcome: "witness-mismatch",
                workers: vec!["general-0817t1200-aaaa".to_string()],
            }],
            "the mismatch is recorded, and the terminal worker contributes nothing"
        );
        assert!(report.reaped_at.is_some());
        assert!(
            victim.try_wait().unwrap().is_none(),
            "a pid whose witness does not match is never signalled"
        );
        let _ = victim.kill();
        let _ = victim.wait();
    }
}
