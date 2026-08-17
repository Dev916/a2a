//! `a2a broker …` — the daemon and the two commands that reach it.
//!
//! **Scope note for Task 9.** This is Task 8's minimum: `broker run` builds the
//! one sanctioned tokio runtime, starts [`core::start`], serves the socket, and
//! stops on SIGTERM/SIGINT or a `shutdown` op — in the FOREGROUND. The
//! singleton (`broker.lock`, flock-before-fork), the double-fork/setsid, the
//! pidfile, `broker status`, `broker restart` and startup reconciliation are
//! Task 9's, and belong in this file next to [`run`]. `--foreground` is already
//! accepted so the flag's meaning does not change under callers when
//! daemonization lands.
//!
//! The autostart path in [`crate::brokerclient`] detaches the daemon into its
//! own session and redirects its stdio to `broker.log`, so the absence of
//! daemonization is not observable to a CLI that autostarts one.

use crate::broker::child::{self, ChildSpec, Supervision};
use crate::broker::core;
use crate::broker::sockserv::{self, ServeCtx};
use crate::brokerclient;
use crate::config::{self, Config};
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode, Outcome};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// How long the daemon waits for the core's tasks after the socket is down.
pub const CORE_SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

/// The one place in `a2a` that builds a runtime for a whole command.
pub fn run(home: &A2aHome, _foreground: bool, quiet: bool) -> Result<Outcome, AppError> {
    let (cfg, _, _) = config::load(home)?;
    let root = home.root().to_path_buf();
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
    let data = runtime.block_on(daemon(root, cfg, quiet))?;
    Ok(Outcome::from(data))
}

async fn daemon(home: PathBuf, cfg: Config, quiet: bool) -> Result<Value, AppError> {
    let Some(listener) = sockserv::bind(&home)? else {
        // Observable, and exit 0: a second `broker run` is a no-op, not a
        // failure — twenty concurrent cold starts must converge on one daemon
        // and nineteen quiet exits (Task 9 pins the count).
        if !quiet {
            eprintln!("a2a: broker already running");
        }
        return Ok(json!({"started": false, "already_running": true}));
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

    let shutdown = CancellationToken::new();
    let ctx = ServeCtx::new(&home, core.clone(), shutdown.clone())
        .with_config(Some(codex_bin), cfg.allowed_roots.clone());
    let pid = ctx.pid;
    let serving = tokio::spawn(sockserv::serve(listener, ctx));

    wait_for_stop(&shutdown).await;
    shutdown.cancel();
    let _ = serving.await;
    let _ = tokio::time::timeout(CORE_SHUTDOWN_GRACE, core.shutdown()).await;
    Ok(json!({"started": true, "pid": pid}))
}

/// A `shutdown` op, a SIGTERM or a SIGINT — whichever comes first.
async fn wait_for_stop(shutdown: &CancellationToken) {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => {
            shutdown.cancelled().await;
            return;
        }
    };
    let mut int = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(_) => {
            shutdown.cancelled().await;
            return;
        }
    };
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

/// `a2a broker shutdown` — graceful, bounded, and idempotent.
pub fn shutdown(home: &A2aHome) -> Result<Outcome, AppError> {
    let stopped = brokerclient::shutdown(home.root())?;
    Ok(Outcome::from(json!({"shutdown": stopped})))
}

/// `a2a __broker-call <op> [params]` — one request over the socket.
///
/// The hidden ops seam Task 8 needs to exercise autostart, the skew policy and
/// the op table from the command line. Task 9 turns the `info` op into the
/// user-facing `a2a broker status`; this stays as the generic escape hatch.
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
