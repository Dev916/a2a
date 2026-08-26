//! `a2a broker …` — the daemon and the commands that reach it.
//!
//! **The pinned order is flock, THEN fork.** Every other arrangement has a
//! window in it:
//!
//! ```text
//!   flock(broker.lock, LOCK_EX|LOCK_NB)      ← the singleton, decided here
//!     │  EWOULDBLOCK ─▶ "a2a: broker already running", exit 0
//!     ▼
//!   fork ─▶ parent _exit(0)                  ← the launching process leaves
//!   setsid                                   ← no controlling terminal
//!   SIGHUP ignored
//!   fork ─▶ parent _exit(0)                  ← never a session leader again
//!   stdin ◀ /dev/null, stdout/stderr ▶ broker.log
//!   broker.pid (0600)                        ← the daemon's OWN pid
//!     │
//!     ▼
//!   reap the previous instance's child ─▶ launch ours ─▶ adopt its workers
//! ```
//!
//! The lock is acquired before the first `fork` and the descriptor is carried
//! through both of them, so there is no instant at which the home is unowned:
//! `flock` locks the open file *description*, which `fork` shares rather than
//! copies, and the intermediate parents leave through `_exit` so no `Drop` can
//! unlock it on the way out. A socket-existence probe cannot do this job —
//! two daemons cold-starting in the same millisecond both see no socket — which
//! is why [`sockserv::bind`]'s stale-socket check is now a *consequence* of
//! holding the lock rather than the thing that decides ownership.
//!
//! Teardown runs the same order backwards, and [`broker::shutdown`] owns it:
//!
//! ```text
//!   stop accepting ─▶ workers flushed to orphaned ─▶ child SIGTERM→SIGKILL
//!                  ─▶ socket unlinked ─▶ pidfile removed ─▶ flock released LAST
//! ```
//!
//! Every step's absence is meaningful to whoever comes next, which is why the
//! order is pinned and each step writes a line to `broker.jsonl`: a socket that
//! outlives the child hands a live address to a CLI whose transport is already
//! gone, and a lock released early lets a successor start into a home this
//! daemon has not finished leaving. So a `broker restart` that sees the lock
//! free knows everything else is already gone.

use crate::broker::child::{self, ChildSpec, Supervision};
use crate::broker::shutdown::{self, Cause, IdleWatch, Shutdown};
use crate::broker::sockserv::{self, ServeCtx};
use crate::broker::{self, core, StartupReport};
use crate::brokerclient;
use crate::config::{self, Config};
use crate::exec;
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode, Outcome};
use nix::fcntl::{Flock, FlockArg};
use nix::sys::signal::{SigHandler, Signal};
use nix::unistd::ForkResult;
use serde_json::{json, Value};
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long `broker restart` waits for the retired daemon to release the lock
/// before it gives up rather than racing it.
pub const LOCK_WAIT: Duration = Duration::from_secs(10);
const LOCK_POLL: Duration = Duration::from_millis(100);

/// The one place in `a2a` that builds a runtime for a whole command.
pub fn run(home: &A2aHome, foreground: bool, quiet: bool) -> Result<Outcome, AppError> {
    let (cfg, _, _) = config::load(home)?;
    let root = home.root().to_path_buf();

    // 1. The singleton, before anything observable exists.
    let Some(lock) = acquire_lock(&root)? else {
        if !quiet {
            eprintln!("a2a: broker already running");
        }
        return Ok(Outcome::from(
            json!({"started": false, "already_running": true}),
        ));
    };

    // 2. Detach, keeping the lock descriptor through both forks.
    if !foreground {
        daemonize(&root)?;
    }
    // 3. ... and only now is there a pid worth writing down.
    write_pidfile(&root)?;

    // 4. Reap before the runtime exists: the grace loop is a blocking wait, and
    //    the previous instance's child must be gone before ours is launched.
    let startup = broker::reap_previous_instance(&root);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            AppError::new(
                "INTERNAL",
                ExitCode::Internal,
                format!("could not build the broker runtime: {e}"),
            )
        })?;
    let outcome = runtime.block_on(daemon(root.clone(), cfg, quiet, startup));
    // Bounded: a blocking task that will not finish must not become a daemon
    // that will not exit.
    runtime.shutdown_timeout(Duration::from_secs(2));

    // The last two phases, out here because the lock is not the runtime's to
    // hold and must be released LAST of all.
    let (data, cause) = match outcome {
        Ok((data, cause)) => (Ok(data), cause),
        Err(e) => (Err(e), Cause::Internal("the daemon could not start".into())),
    };
    if data
        .as_ref()
        .map(|d| d["started"] == json!(true))
        .unwrap_or(true)
    {
        let _ = std::fs::remove_file(sockserv::socket_path(&root));
        shutdown::record_phase(&root, shutdown::PHASE_SOCKET_UNLINKED, &cause, json!({}));
    }
    let _ = std::fs::remove_file(broker::pid_path(&root));
    shutdown::record_phase(&root, shutdown::PHASE_LOCK_RELEASED, &cause, json!({}));
    // Unlinked by the HOLDER, and only ever by the holder: nobody else can have
    // replaced the file while this process held the flock on it, because every
    // other acquirer is refused (EWOULDBLOCK) until the `drop` below.
    let _ = std::fs::remove_file(broker::lock_path(&root));
    drop(lock);

    // The forced exit. Everything the daemon owned is released above, so a task
    // that outlived [`shutdown::GRACE`] is holding nothing an operator can see —
    // and a daemon that will not exit is worse than one that exits loudly. The
    // final envelope is the price, and it is only ever paid here.
    if data
        .as_ref()
        .map(|d| d["shutdown"]["report"]["graceful"] == json!(false))
        .unwrap_or(false)
    {
        eprintln!(
            "a2a broker: forcing exit after {}s — some tasks did not stop",
            shutdown::GRACE.as_secs()
        );
        std::process::exit(0);
    }
    data.map(Outcome::from)
}

async fn daemon(
    home: PathBuf,
    cfg: Config,
    quiet: bool,
    mut startup: StartupReport,
) -> Result<(Value, Cause), AppError> {
    let Some(listener) = sockserv::bind(&home)? else {
        // Belt and braces: with the lock held this is close to unreachable, but
        // a live socket we did not bind is still someone else's, and taking it
        // would be the one mistake this whole file exists to prevent.
        if !quiet {
            eprintln!("a2a: broker already running");
        }
        return Ok((
            json!({"started": false, "already_running": true}),
            Cause::Op,
        ));
    };

    let codex_bin = cfg
        .codex_bin
        .clone()
        .unwrap_or_else(|| PathBuf::from("codex"));
    let spec = ChildSpec::app_server(codex_bin.clone(), child::child_stderr_log(&home));
    let core = match core::start(home.clone(), cfg.clone(), spec, Supervision::from_env()).await {
        Ok(core) => core,
        Err(e) => {
            // A daemon that never started its child owns nothing: take the
            // address back down so the next CLI does not connect to a corpse.
            let _ = std::fs::remove_file(sockserv::socket_path(&home));
            return Err(e);
        }
    };

    // Phase two of startup reconciliation, now that there is a child to attach
    // the survivors to.
    broker::adopt_previous_workers(&home, &core, &mut startup).await;

    // ONE root token — the core's own, which every `WorkerActor` already holds
    // a `child_token` of. The front door hangs off it as a child, so closing
    // the door and stopping the house are separate acts with one root.
    let mut sd = Shutdown::rooted(core.cancel_token());
    let ctx = ServeCtx::new(&home, core.clone(), sd.accepting())
        .with_config(Some(codex_bin), cfg.allowed_roots.clone())
        .with_startup(&startup)
        .with_causes(sd.causes())
        .with_deferred_unlink();
    let pid = ctx.pid;
    let idle_timeout = Duration::from_secs(cfg.broker.idle_timeout_secs);
    let watch = (!idle_timeout.is_zero())
        .then(|| IdleWatch::new(core.clone(), &home, idle_timeout, ctx.liveness()));
    let serving = tokio::spawn(sockserv::serve(listener, ctx));

    // 1. DETECT.
    let cause = sd.detect(watch).await;
    if !quiet {
        eprintln!("a2a broker: shutting down ({})", cause.as_str());
    }
    // 2/3. TELL, then WAIT — the accept loop first, the root token last.
    let report = shutdown::teardown(&home, core, &sd, &cause).await;
    let _ = tokio::time::timeout(sockserv::SHUTDOWN_GRACE, serving).await;
    Ok((
        json!({"started": true, "pid": pid,
               "shutdown": {"cause": cause.as_str(), "report": report}}),
        cause,
    ))
}

// ---------------------------------------------------------------------------
// the singleton
// ---------------------------------------------------------------------------

/// `Ok(None)` = somebody else holds it, which is not an error: a second
/// `broker run` is a no-op by design (twenty concurrent cold starts must
/// converge on one daemon and nineteen quiet exits).
fn acquire_lock(home: &Path) -> Result<Option<Flock<File>>, AppError> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = broker::lock_path(home);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .map_err(|e| io_error(&path, e))?;
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(lock) => Ok(Some(lock)),
        Err((_file, nix::errno::Errno::EWOULDBLOCK)) => Ok(None),
        Err((_file, errno)) => Err(AppError::new(
            "BROKER_LOCK",
            ExitCode::Internal,
            format!("could not lock {}: {errno}", path.display()),
        )),
    }
}

/// True when nobody holds `broker.lock` — the fact `broker restart` waits on,
/// because the socket disappearing is not the same event as the daemon exiting.
fn lock_is_free(home: &Path) -> bool {
    let Ok(file) = std::fs::OpenOptions::new()
        .write(true)
        .open(broker::lock_path(home))
    else {
        return true; // no lock file at all
    };
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(lock) => {
            drop(lock); // we were only probing
            true
        }
        Err(_) => false,
    }
}

fn wait_for_lock_release(home: &Path, patience: Duration) -> Result<(), AppError> {
    let deadline = Instant::now() + patience;
    while !lock_is_free(home) {
        if Instant::now() >= deadline {
            return Err(AppError::new(
                "BROKER_LOCK",
                ExitCode::BrokerUnavailable,
                format!(
                    "the previous broker still holds {} after {}s",
                    broker::lock_path(home).display(),
                    patience.as_secs()
                ),
            )
            .remedy("check broker.log, then kill the pid in broker.pid"));
        }
        std::thread::sleep(LOCK_POLL);
    }
    Ok(())
}

/// Double-fork + setsid, with the lock descriptor carried through.
///
/// The second fork is not superstition: after `setsid` this process is a
/// session leader, and a session leader that opens a terminal acquires it as a
/// controlling terminal. Forking once more makes that impossible for the rest
/// of the daemon's life.
fn daemonize(home: &Path) -> Result<(), AppError> {
    let log_path = home.join(broker::BROKER_LOG);
    let log = exec::open_append_0600(&log_path).map_err(|e| io_error(&log_path, e))?;
    let devnull = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")
        .map_err(|e| io_error(Path::new("/dev/null"), e))?;

    leave()?;
    nix::unistd::setsid().map_err(|e| daemon_error(format!("setsid: {e}")))?;
    // A daemon must survive the hangup of the terminal that started it.
    unsafe { nix::sys::signal::signal(Signal::SIGHUP, SigHandler::SigIgn) }
        .map_err(|e| daemon_error(format!("ignoring SIGHUP: {e}")))?;
    leave()?;

    for (fd, source) in [
        (0, devnull.as_raw_fd()),
        (1, log.as_raw_fd()),
        (2, log.as_raw_fd()),
    ] {
        nix::unistd::dup2(source, fd)
            .map_err(|e| daemon_error(format!("dup2 onto fd {fd}: {e}")))?;
    }
    Ok(())
}

/// Fork, and leave through the parent — `_exit`, never `exit`, so nothing this
/// process was holding (the lock above all) is released on the way out.
fn leave() -> Result<(), AppError> {
    match unsafe { nix::unistd::fork() } {
        Ok(ForkResult::Parent { .. }) => unsafe { nix::libc::_exit(0) },
        Ok(ForkResult::Child) => Ok(()),
        Err(e) => Err(daemon_error(format!("fork: {e}"))),
    }
}

fn write_pidfile(home: &Path) -> Result<(), AppError> {
    use std::io::Write;
    let path = broker::pid_path(home);
    let mut f = crate::home::write_new_0600(&path).map_err(|e| io_error(&path, e))?;
    writeln!(f, "{}", std::process::id()).map_err(|e| io_error(&path, e))
}

// ---------------------------------------------------------------------------
// the commands that reach a running daemon
// ---------------------------------------------------------------------------

/// `a2a broker status` (and `a2a status --broker`) — exit 0 up, exit 7 down.
///
/// Never autostarts. "Is there a broker" is a question, and a question that
/// changes the answer by being asked is not one.
pub fn status(home: &A2aHome, quiet: bool) -> Result<Outcome, AppError> {
    let mut client = brokerclient::connect(home.root())?;
    let info = client.request("info", json!({}))?;
    let data = json!({
        "running": true,
        "pid": info["pid"],
        "started_at": info["started_at"],
        "uptime_secs": info["uptime_secs"],
        "transport": info["transport"],
        "child_pid": info["child_pid"],
        "restarts": info["restarts"],
        "generation": info["generation"],
        "workers": info["workers"],
        "live": info["live"],
        // Workers with no actor because they are still waiting for a slot
        // (spec §4.9). Reported next to `live` because "8 live, 3 waiting" is
        // one fact about a fleet, and an operator asking why a spawn has not
        // started is asking exactly this.
        "queued": info["queued"],
        // Why a spawn this daemon has room for may still be refused (spec §4.9).
        "breaker": info["breaker"],
        "clients": info["clients"],
        "socket": info["socket"],
        "protocol_version": info["protocol_version"],
        "binary_version": info["binary_version"],
        "codex_bin": info["codex_bin"],
        "allowed_roots": info["allowed_roots"],
        "reconciled": info["reconciled"],
    });
    if !quiet {
        // An open breaker is the loudest thing a broker can be (spec §4.9): a
        // daemon that is up, idle and refusing every spawn looks healthy on
        // every other number on this line.
        let breaker = if info["breaker"]["open"] == json!(true) {
            format!(
                ", breaker OPEN ({} failures in {}s)",
                info["breaker"]["failures"], info["breaker"]["window_secs"]
            )
        } else {
            String::new()
        };
        eprintln!(
            "a2a: broker up (pid {}, {}, {} worker(s), {} queued, {} client(s), up {}s){}",
            info["pid"],
            info["transport"].as_str().unwrap_or("unknown"),
            info["workers"],
            info["queued"],
            info["clients"],
            info["uptime_secs"],
            breaker
        );
    }
    Ok(Outcome::from(data))
}

/// `a2a broker reset-breaker` — close the circuit breaker again (spec §4.9).
///
/// [`brokerclient::connect`], never `connect_or_start`: autostarting a daemon in
/// order to reset a breaker it never had is a lie about what happened, so a home
/// with no broker is exit 7 rather than a fresh daemon and a cheerful `true`.
pub fn reset_breaker(home: &A2aHome, quiet: bool) -> Result<Outcome, AppError> {
    let mut client = brokerclient::connect(home.root())?;
    let data = client.request("reset-breaker", json!({}))?;
    let was_open = data["was_open"].as_bool().unwrap_or(false);
    if !quiet {
        eprintln!(
            "a2a: breaker reset ({})",
            if was_open {
                "it was open"
            } else {
                "it was already closed"
            }
        );
    }
    Ok(Outcome::from(json!({"reset": true, "was_open": was_open})))
}

/// `a2a broker shutdown` — graceful, bounded, and idempotent.
pub fn shutdown(home: &A2aHome) -> Result<Outcome, AppError> {
    let stopped = brokerclient::shutdown(home.root())?;
    Ok(Outcome::from(json!({"shutdown": stopped})))
}

/// `a2a broker restart` — shutdown, then run.
///
/// The wait between them is on the LOCK, not on the socket: a daemon that has
/// unlinked its socket is still holding its home until the lock goes, and a
/// successor that starts inside that window loses the race and exits 0, leaving
/// the operator with a "restart" that quietly restarted nothing.
pub fn restart(home: &A2aHome, quiet: bool) -> Result<Outcome, AppError> {
    let stopped = brokerclient::shutdown(home.root())?;
    wait_for_lock_release(home.root(), LOCK_WAIT)?;
    let mut client = brokerclient::connect_or_start(home.root())?;
    let info = client.request("info", json!({}))?;
    if !quiet {
        eprintln!("a2a: broker restarted (pid {})", info["pid"]);
    }
    Ok(Outcome::from(
        json!({"restarted": true, "stopped": stopped, "pid": info["pid"]}),
    ))
}

/// `a2a __broker-call <op> [params]` — one request over the socket.
///
/// The hidden ops seam Task 8 needs to exercise autostart, the skew policy and
/// the op table from the command line; `broker status` is the user-facing shape
/// of the `info` op, and this stays as the generic escape hatch.
pub fn call(home: &A2aHome, op: &str, params: Option<&str>) -> Result<Outcome, AppError> {
    let params: Value = match params {
        None => json!({}),
        Some(text) => serde_json::from_str(text).map_err(|e| {
            AppError::new(
                "USAGE",
                ExitCode::Usage,
                format!("params must be a JSON object: {e}"),
            )
        })?,
    };
    let mut client = brokerclient::connect_or_start(home.root())?;
    let data = client.request(op, params)?;
    Ok(Outcome::from(data))
}

fn io_error(path: &Path, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{}: {e}", path.display()))
}

fn daemon_error(detail: String) -> AppError {
    AppError::new(
        "BROKER_DAEMONIZE",
        ExitCode::Internal,
        format!("the broker could not detach: {detail}"),
    )
    .remedy("`a2a broker run --foreground` runs it attached")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The lock is the singleton: the second acquirer is told to go away, and
    /// releasing the first hands ownership straight over.
    #[test]
    fn the_lock_admits_exactly_one_holder() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        assert!(lock_is_free(home), "an empty home is unowned");

        let first = acquire_lock(home)
            .unwrap()
            .expect("the first acquirer wins");
        assert!(
            acquire_lock(home).unwrap().is_none(),
            "the second acquirer is refused rather than failed"
        );
        assert!(!lock_is_free(home));

        drop(first);
        assert!(lock_is_free(home), "release hands ownership over");
        assert!(acquire_lock(home).unwrap().is_some());
    }

    #[test]
    fn the_pidfile_is_this_process_at_0600() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        write_pidfile(tmp.path()).unwrap();
        let path = broker::pid_path(tmp.path());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            std::process::id().to_string()
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    /// A `restart` that cannot see the lock released refuses to start a
    /// successor rather than racing one that will lose.
    #[test]
    fn waiting_for_the_lock_is_bounded() {
        let tmp = tempfile::tempdir().unwrap();
        let held = acquire_lock(tmp.path()).unwrap().unwrap();
        let err = wait_for_lock_release(tmp.path(), Duration::from_millis(200)).unwrap_err();
        assert_eq!(err.exit, ExitCode::BrokerUnavailable);
        drop(held);
        wait_for_lock_release(tmp.path(), Duration::from_millis(200)).unwrap();
    }
}
