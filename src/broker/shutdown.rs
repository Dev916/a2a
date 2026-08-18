//! Shutdown, in three phases: **detect**, **tell**, **wait**.
//!
//! ```text
//!   DETECT   one select of cancel-safe branches
//!            ctrl_c │ SIGTERM │ idle timer │ internal cause channel
//!                              │
//!   TELL     ───────────────────┴──────────────────────────────────
//!            accepting.cancel()          ← the front door closes first
//!            core.drain(grace)           ← every live worker is flushed to
//!                                          `orphaned`, then the child is
//!                                          SIGTERM'd and (if it insists) SIGKILL'd
//!            root.cancel()               ← the root token every worker holds a
//!                                          child_token of
//!                              │
//!   WAIT     ─────────────────┴───────────────────────────────────
//!            TaskTracker::close + wait, bounded by [`GRACE`]
//!            → forced exit if the wait runs out
//! ```
//!
//! Three decisions are load-bearing:
//!
//! * **One root token, child tokens below it.** [`Shutdown::rooted`] adopts the
//!   core's own token as the daemon's root, so the accept loop and every
//!   `WorkerActor` are cancelled by the same `cancel()` — there is no second
//!   tree that can be half-cancelled. The order is what makes the phases
//!   meaningful: the *child* token that stops the front door is cancelled
//!   first, and the root last, because a worker cancelled before it has
//!   flushed is a worker whose state died with the daemon.
//! * **The teardown order is observable.** Each phase writes one line to
//!   `broker.jsonl` ([`PHASES`]), so "the socket went before the lock" is a
//!   fact on disk rather than a claim in a report. An operator debugging a
//!   restart that raced itself needs the order, not the outcome.
//! * **The idle timer is a safety net, not a scheduler.** A broker autostarted
//!   by one `a2a spawn` in a directory nobody returns to would otherwise hold a
//!   socket, a lock and an app-server child forever. "Live" is deliberately
//!   generous — any non-terminal worker (including one waiting for a human),
//!   any exec worker on disk, any subscriber, any connected client — because
//!   the cost of retiring too early is an operator's work, and the cost of
//!   retiring too late is one idle process.

use crate::broker::core::CoreHandle;
use crate::exec;
use serde::Serialize;
use serde_json::{json, Value};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// How long the daemon waits for its own tasks after everything has been told
/// to stop. Past this it stops waiting and exits: a shutdown that can hang is
/// not a shutdown.
pub const GRACE: Duration = Duration::from_secs(10);

/// How long the workers get to flush their snapshots before the child is
/// signalled. Inside [`GRACE`] on purpose — the two budgets are sequential.
pub const FLUSH_GRACE: Duration = Duration::from_secs(5);

/// SIGTERM → SIGKILL for the app-server child.
pub const CHILD_GRACE: Duration = Duration::from_secs(3);

/// Bound on the internal cause channel. Causes are single-shot by nature; the
/// slack is only so a `try_send` from a connection task can never fail for
/// want of room.
pub const CAUSE_CAPACITY: usize = 8;

/// The idle watcher never sleeps longer than this, however long the timeout is:
/// a 24-hour timeout must still notice a shutdown promptly.
pub const IDLE_POLL_CAP: Duration = Duration::from_secs(15);
pub const IDLE_POLL_FLOOR: Duration = Duration::from_millis(100);

// -- phases -----------------------------------------------------------------

pub const PHASE_STOP_ACCEPTING: &str = "stop_accepting";
pub const PHASE_WORKERS_FLUSHED: &str = "workers_flushed";
pub const PHASE_CHILD_STOPPED: &str = "child_stopped";
pub const PHASE_SOCKET_UNLINKED: &str = "socket_unlinked";
pub const PHASE_LOCK_RELEASED: &str = "lock_released";

/// The teardown order, in the order it must happen. Pinned as a value because
/// every step's *absence* is meaningful to whoever comes next: a socket that
/// outlives the child hands a live address to a CLI whose worker is already
/// gone, and a lock released before the socket lets a successor start into a
/// home this daemon has not finished leaving.
pub const PHASES: [&str; 5] = [
    PHASE_STOP_ACCEPTING,
    PHASE_WORKERS_FLUSHED,
    PHASE_CHILD_STOPPED,
    PHASE_SOCKET_UNLINKED,
    PHASE_LOCK_RELEASED,
];

// ---------------------------------------------------------------------------
// causes
// ---------------------------------------------------------------------------

/// Why the daemon is going down. Recorded with every phase line, because "the
/// broker exited" and "the broker was told to exit" are different incidents.
#[derive(Clone, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    /// `a2a broker shutdown` (or a skewed client draining us).
    Op,
    Sigterm,
    /// Ctrl-C on a `--foreground` run.
    Interrupt,
    /// Nothing live for `broker.idle_timeout_secs`.
    Idle,
    /// Something inside the daemon decided it cannot go on.
    Internal(String),
}

impl Cause {
    pub fn as_str(&self) -> &str {
        match self {
            Cause::Op => "op",
            Cause::Sigterm => "sigterm",
            Cause::Interrupt => "interrupt",
            Cause::Idle => "idle",
            Cause::Internal(_) => "internal",
        }
    }
}

// ---------------------------------------------------------------------------
// the token tree
// ---------------------------------------------------------------------------

/// The daemon's cancellation tree and the channel its own tasks report through.
pub struct Shutdown {
    root: CancellationToken,
    accepting: CancellationToken,
    tx: mpsc::Sender<Cause>,
    rx: mpsc::Receiver<Cause>,
}

impl Shutdown {
    /// Adopt `root` as the daemon's root token — in practice the core's own, so
    /// that one `cancel()` reaches every `WorkerActor` child token.
    pub fn rooted(root: CancellationToken) -> Shutdown {
        let accepting = root.child_token();
        let (tx, rx) = mpsc::channel(CAUSE_CAPACITY);
        Shutdown {
            root,
            accepting,
            tx,
            rx,
        }
    }

    /// The token the socket server stops on. A CHILD of the root: cancelling it
    /// closes the front door and nothing else.
    pub fn accepting(&self) -> CancellationToken {
        self.accepting.clone()
    }

    pub fn root(&self) -> CancellationToken {
        self.root.clone()
    }

    /// The far end of the internal cause channel, for tasks that decide the
    /// daemon should stop (the `shutdown` op, today).
    pub fn causes(&self) -> mpsc::Sender<Cause> {
        self.tx.clone()
    }

    /// **Phase one — detect.** One `select!`, every branch cancel-safe.
    ///
    /// `idle` is `None` when the timer is disabled (`idle_timeout_secs = 0`).
    /// The signal streams are built here rather than passed in because a stream
    /// registered and then dropped is a handler uninstalled, and the window
    /// between "the daemon is up" and "the daemon can be stopped" must be zero.
    pub async fn detect(&mut self, idle: Option<IdleWatch>) -> Cause {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).ok();
        let accepting = self.accepting.clone();
        tokio::select! {
            biased;
            // An explicit cause always wins: it says WHY, and the token below
            // only says THAT.
            Some(cause) = self.rx.recv() => cause,
            // Someone cancelled the front door directly (the `shutdown` op
            // cancels it whether or not anybody is listening for causes).
            _ = accepting.cancelled() => self.rx.try_recv().unwrap_or(Cause::Op),
            _ = terminated(&mut term) => Cause::Sigterm,
            _ = tokio::signal::ctrl_c() => Cause::Interrupt,
            _ = idle_expired(idle) => Cause::Idle,
        }
    }
}

/// `SIGTERM`, or never if the handler could not be installed — a daemon that
/// cannot register one still has SIGKILL, and pretending otherwise by returning
/// immediately would shut it down at startup.
async fn terminated(stream: &mut Option<tokio::signal::unix::Signal>) {
    match stream {
        Some(s) => {
            s.recv().await;
        }
        None => std::future::pending().await,
    }
}

async fn idle_expired(watch: Option<IdleWatch>) {
    match watch {
        Some(watch) => watch.expired().await,
        None => std::future::pending().await,
    }
}

// ---------------------------------------------------------------------------
// the idle timer
// ---------------------------------------------------------------------------

/// The counters the socket server keeps, shared by pointer rather than by
/// message because they are *counters* — nothing decides anything from them
/// except this timer, and a stale read costs one more poll interval.
#[derive(Clone)]
pub struct Liveness {
    pub clients: Arc<AtomicUsize>,
    pub subscribers: Arc<AtomicUsize>,
}

/// Fires once the broker has had nothing live for `timeout`.
pub struct IdleWatch {
    core: CoreHandle,
    home: PathBuf,
    timeout: Duration,
    liveness: Liveness,
}

impl IdleWatch {
    pub fn new(core: CoreHandle, home: impl Into<PathBuf>, timeout: Duration, liveness: Liveness) -> IdleWatch {
        IdleWatch {
            core,
            home: home.into(),
            timeout,
            liveness,
        }
    }

    /// Poll until the broker has been idle for the whole timeout.
    ///
    /// The clock starts at construction, not at the first idle observation: a
    /// daemon autostarted for a job that then failed has been idle since the
    /// moment it came up, and waiting for a first non-idle sample would leave
    /// exactly that daemon running forever.
    pub async fn expired(self) {
        if self.timeout.is_zero() {
            std::future::pending::<()>().await;
        }
        let poll = (self.timeout / 4).clamp(IDLE_POLL_FLOOR, IDLE_POLL_CAP);
        let mut idle_since = Some(Instant::now());
        loop {
            tokio::time::sleep(poll).await;
            if self.live().await {
                idle_since = None;
                continue;
            }
            let since = *idle_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= self.timeout {
                return;
            }
        }
    }

    /// Is anything going on? Four questions, cheapest first.
    async fn live(&self) -> bool {
        if self.liveness.subscribers.load(Ordering::Relaxed) > 0
            || self.liveness.clients.load(Ordering::Relaxed) > 0
        {
            return true;
        }
        match self.core.idle().await {
            // Non-terminal, un-escalated workers: `awaiting-input` is in this
            // set on purpose. A worker waiting for a human is the LEAST
            // disposable thing the broker holds.
            Ok(report) => {
                // Queued workers count too, and they have no actor to be
                // counted BY: a daemon that retired itself while workers were
                // waiting for its own cap would leave them waiting forever.
                if report.live > 0 || report.queued > 0 {
                    return true;
                }
            }
            // The core is gone; there is nothing left to keep alive.
            Err(_) => return false,
        }
        // Exec workers are not this daemon's to touch, but they ARE work in
        // flight — and the CLI paths that serve them expect a broker to be
        // there when they finish.
        let home = self.home.clone();
        tokio::task::spawn_blocking(move || super::live_exec_workers(&home))
            .await
            .unwrap_or(0)
            > 0
    }
}

// ---------------------------------------------------------------------------
// phases two and three
// ---------------------------------------------------------------------------

/// What the teardown did, for the daemon's final envelope and for the tests.
#[derive(Serialize, Clone, Debug, Default)]
pub struct TeardownReport {
    pub cause: String,
    /// Workers flushed to `orphaned` on the way out.
    pub orphaned: Vec<String>,
    /// `terminated` | `killed` | `already-gone` | `none`
    pub child: String,
    /// False when [`GRACE`] ran out before every task had finished — the
    /// daemon then leaves anyway, and says so.
    pub graceful: bool,
}

/// **Phases two and three — tell, then wait.**
///
/// Returns once the daemon owns nothing but its files; unlinking those is the
/// caller's job, because the lock must be released *after* them and the lock
/// lives outside the runtime.
pub async fn teardown(home: &Path, core: CoreHandle, sd: &Shutdown, cause: &Cause) -> TeardownReport {
    let mut report = TeardownReport {
        cause: cause.as_str().to_string(),
        ..TeardownReport::default()
    };

    // 1. Stop accepting. The front door closes before anything else moves, so
    //    no client can be handed a worker that is already being torn down.
    sd.accepting().cancel();
    record_phase(home, PHASE_STOP_ACCEPTING, cause, json!({}));

    // 2. Flush the workers, then stop the child. Both happen inside the core,
    //    which is the only thing that may touch either.
    let drained = tokio::time::timeout(GRACE, core.drain(FLUSH_GRACE, CHILD_GRACE))
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
    report.orphaned = drained.orphaned.clone();
    report.child = drained.child.to_string();
    record_phase(
        home,
        PHASE_WORKERS_FLUSHED,
        cause,
        json!({"orphaned": drained.orphaned, "stopped": drained.stopped,
               "flushed_in_time": drained.actors_finished}),
    );
    record_phase(home, PHASE_CHILD_STOPPED, cause, json!({"child": drained.child}));

    // 3. Cancel the root — every worker holds a child token of it — and wait
    //    for the tracker, bounded.
    report.graceful = tokio::time::timeout(GRACE, core.shutdown()).await.is_ok();
    report
}

/// One line per phase in `broker.jsonl`.
///
/// Synchronous, and deliberately: it is a small append under a lock nobody else
/// holds, and a phase line that is written *after* the phase it describes is
/// worse than useless. Callers in async context are fine — this is the same
/// blocking append the startup phases use.
pub fn record_phase(home: &Path, phase: &str, cause: &Cause, detail: Value) {
    let Ok(mut f) = exec::open_append_0600(&home.join(super::BROKER_JSONL)) else {
        return;
    };
    let line = json!({"ts": chrono::Utc::now(), "broker": "shutdown", "phase": phase,
                      "cause": cause.as_str(), "detail": detail});
    // One whole line per syscall — `broker.jsonl` has more than one appender
    // (the core's log task is still running when the first phases are written),
    // and `writeln!` would hand it a line at a time in several pieces.
    let _ = f.write_all(&crate::broker::core::line_bytes(&line));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_teardown_order_is_pinned() {
        assert_eq!(
            PHASES,
            [
                "stop_accepting",
                "workers_flushed",
                "child_stopped",
                "socket_unlinked",
                "lock_released"
            ]
        );
        assert_eq!(GRACE, Duration::from_secs(10));
    }

    /// The front door is a CHILD of the root: closing it must not stop a single
    /// worker, and cancelling the root must stop everything.
    #[test]
    fn the_accepting_token_is_a_child_of_the_root() {
        let root = CancellationToken::new();
        let sd = Shutdown::rooted(root.clone());
        sd.accepting().cancel();
        assert!(sd.accepting().is_cancelled());
        assert!(!root.is_cancelled(), "closing the door is not stopping the house");

        let sd2 = Shutdown::rooted(CancellationToken::new());
        let door = sd2.accepting();
        sd2.root().cancel();
        assert!(door.is_cancelled(), "the root reaches every child");
    }

    /// A cause sent before anything else wins over the token that carries no
    /// explanation.
    #[tokio::test(flavor = "current_thread")]
    async fn an_explicit_cause_beats_a_bare_cancellation() {
        let mut sd = Shutdown::rooted(CancellationToken::new());
        sd.causes().send(Cause::Idle).await.unwrap();
        sd.accepting().cancel();
        assert_eq!(sd.detect(None).await, Cause::Idle);
    }

    /// ... and a bare cancellation is still a shutdown: the `shutdown` op is
    /// allowed to cancel and say nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn a_bare_cancellation_reads_as_the_op() {
        let mut sd = Shutdown::rooted(CancellationToken::new());
        sd.accepting().cancel();
        assert_eq!(sd.detect(None).await, Cause::Op);
    }

    #[test]
    fn a_cause_names_itself_for_the_log() {
        assert_eq!(Cause::Sigterm.as_str(), "sigterm");
        assert_eq!(Cause::Idle.as_str(), "idle");
        assert_eq!(Cause::Internal("boom".into()).as_str(), "internal");
    }
}
