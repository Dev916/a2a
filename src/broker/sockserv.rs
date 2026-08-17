//! The broker's front door: a 0600 unix socket inside the 0700 home, one
//! reader/writer task pair per connection, and the ops that reach
//! [`BrokerCore`](super::core::BrokerCore).
//!
//! ```text
//!   a2a CLI ──connect──▶ broker.sock (0600, peer-uid checked)
//!                          │
//!            ┌─────────────┴──────────────┐
//!            │  reader task   writer task │   one pair per connection
//!            └─────────────┬──────────────┘
//!                          ▼
//!                   CoreHandle (mpsc)  ──▶ registry / actors
//! ```
//!
//! The security properties are not incidental — the socket must not
//! out-privilege the CLI (spec §5):
//!
//! * **0600 inside 0700.** Two layers, because either one alone has a way to be
//!   wrong: a permissive home makes the mode moot, and a permissive socket makes
//!   the home moot.
//! * **Peer uid == our uid.** File modes are advisory on some platforms and
//!   nonexistent on others; `LOCAL_PEERCRED`/`SO_PEERCRED` asks the kernel who
//!   is actually on the other end.
//! * **A capped frame.** The sender picks the length, so the receiver picks the
//!   limit ([`wire::MAX_FRAME_BYTES`]).
//!
//! And one liveness property: **writes never happen inside a `select!`**. Every
//! frame this module emits goes through the connection's writer task over a
//! bounded mpsc, so a cancelled future can never leave half a line on the wire.

use crate::broker::core::{CoreHandle, TailFrame};
use crate::broker::shutdown::{self, Cause, Liveness};
use crate::broker::wire::{self, Frame, FrameType, Incoming, WireError};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Bound on one connection's outbound queue. A subscriber that stops reading
/// backs up here first, and the tail's own [`broadcast`] lag is what catches it.
pub const WRITER_CAPACITY: usize = 256;

/// How long `serve` waits for in-flight connections after cancellation before
/// it removes the socket and returns.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// How long a connection waits for its subscription's farewell frame to reach
/// the writer. Short: the frame is one line into a bounded channel that is
/// being drained, and a connection that will not take it is already gone.
pub const FAREWELL_GRACE: Duration = Duration::from_secs(1);

pub fn socket_path(home: &Path) -> PathBuf {
    home.join(super::BROKER_SOCK)
}

/// Bind the listener, 0600.
///
/// `Ok(None)` means **someone else is already serving**: a socket that answers
/// a connect is a live daemon, and the caller's job is then to exit quietly, not
/// to steal the address. A socket that does NOT answer is stale — and only then
/// is it unlinked, because unlinking first is how two daemons end up believing
/// they own the same home.
///
/// That probe is still a TOCTOU: two daemons cold-starting in the same
/// millisecond can both see no socket. Task 9 closes it properly with
/// `broker.lock` acquired BEFORE anything else; until then the client side
/// keeps the window shut by autostarting at most once
/// ([`crate::brokerclient::connect_or_start`]).
pub fn bind(home: &Path) -> Result<Option<UnixListener>, AppError> {
    let path = socket_path(home);
    if path.exists() {
        if std::os::unix::net::UnixStream::connect(&path).is_ok() {
            return Ok(None);
        }
        std::fs::remove_file(&path).map_err(|e| io_error(&path, e))?;
    }
    let listener = UnixListener::bind(&path).map_err(|e| io_error(&path, e))?;
    // Bind honours the umask, so the mode is corrected immediately afterwards.
    // The window is real but harmless: the 0700 home is the outer gate, and the
    // peer-uid check is the inner one.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| io_error(&path, e))?;
    Ok(Some(listener))
}

fn io_error(path: &Path, e: std::io::Error) -> AppError {
    AppError::new(
        "IO",
        ExitCode::Internal,
        format!("{}: {e}", path.display()),
    )
}

/// Everything a connection needs that is not the connection itself.
///
/// Cloned per connection rather than shared behind a lock: the only mutable
/// piece is the client count, which is an atomic because it is a *counter*, not
/// state anyone makes decisions from.
#[derive(Clone)]
pub struct ServeCtx {
    pub home: PathBuf,
    pub core: CoreHandle,
    /// Cancelled by SIGTERM/SIGINT or by a `shutdown` op. Stops the accept
    /// loop, every connection, and the daemon.
    pub shutdown: CancellationToken,
    pub started_at: DateTime<Utc>,
    pub pid: u32,
    pub codex_bin: Option<PathBuf>,
    pub allowed_roots: Vec<PathBuf>,
    /// What startup reconciliation did to the tree this daemon inherited
    /// (`Value::Null` for a core with no daemon around it). Carried here rather
    /// than in the core because it is a fact about this PROCESS's start, not
    /// about the registry.
    pub reconciled: Value,
    clients: Arc<AtomicUsize>,
    /// Connections with a live tail. Counted separately from `clients` because
    /// it is the pinned half of the idle timer's liveness question: a `status`
    /// call comes and goes, a tail is somebody *watching*.
    subscribers: Arc<AtomicUsize>,
    /// Where a connection reports WHY the daemon is stopping. `None` when
    /// nothing is listening (an in-process core with no daemon around it), in
    /// which case cancelling the token is the whole message.
    causes: Option<mpsc::Sender<Cause>>,
    /// Whether `serve` unlinks the socket when it returns.
    ///
    /// True for a bare server (nothing else is going to do it). The DAEMON sets
    /// it false: its teardown order puts the socket *after* the child, and a
    /// socket unlinked here would jump the queue.
    unlink_on_exit: bool,
}

impl ServeCtx {
    pub fn new(home: impl Into<PathBuf>, core: CoreHandle, shutdown: CancellationToken) -> ServeCtx {
        ServeCtx {
            home: home.into(),
            core,
            shutdown,
            started_at: Utc::now(),
            pid: std::process::id(),
            codex_bin: None,
            allowed_roots: Vec::new(),
            reconciled: Value::Null,
            clients: Arc::new(AtomicUsize::new(0)),
            subscribers: Arc::new(AtomicUsize::new(0)),
            causes: None,
            unlink_on_exit: true,
        }
    }

    /// Report shutdown causes through `tx` as well as through the token.
    pub fn with_causes(mut self, tx: mpsc::Sender<Cause>) -> ServeCtx {
        self.causes = Some(tx);
        self
    }

    /// Hand the socket's removal to the caller (see [`ServeCtx::unlink_on_exit`]).
    pub fn with_deferred_unlink(mut self) -> ServeCtx {
        self.unlink_on_exit = false;
        self
    }

    /// The counters the idle timer reads.
    pub fn liveness(&self) -> Liveness {
        Liveness {
            clients: self.clients.clone(),
            subscribers: self.subscribers.clone(),
        }
    }

    pub fn subscribers(&self) -> usize {
        self.subscribers.load(Ordering::Relaxed)
    }

    /// Say why, then stop. The token is cancelled either way: a cause nobody is
    /// listening for must never be the difference between stopping and not.
    async fn stop(&self, cause: Cause) {
        if let Some(tx) = &self.causes {
            let _ = tx.try_send(cause);
        }
        self.shutdown.cancel();
    }

    pub fn with_config(mut self, codex_bin: Option<PathBuf>, allowed_roots: Vec<PathBuf>) -> ServeCtx {
        self.codex_bin = codex_bin;
        self.allowed_roots = allowed_roots;
        self
    }

    pub fn with_startup(mut self, report: &super::StartupReport) -> ServeCtx {
        self.reconciled = serde_json::to_value(report).unwrap_or(Value::Null);
        self
    }

    pub fn clients(&self) -> usize {
        self.clients.load(Ordering::Relaxed)
    }
}

/// Accept until cancelled, then drain and unlink.
pub async fn serve(listener: UnixListener, ctx: ServeCtx) {
    let tracker = TaskTracker::new();
    let mut next_conn: u64 = 1;
    loop {
        tokio::select! {
            biased;
            _ = ctx.shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let conn = next_conn;
                    next_conn += 1;
                    tracker.spawn(connection(stream, conn, ctx.clone()));
                }
                Err(e) => {
                    eprintln!("a2a broker: accept: {e}");
                    // A listener that cannot accept is not going to recover by
                    // spinning on it.
                    if e.kind() == std::io::ErrorKind::InvalidInput {
                        break;
                    }
                }
            },
        }
    }
    tracker.close();
    let _ = tokio::time::timeout(SHUTDOWN_GRACE, tracker.wait()).await;
    // The address is ours to remove: we bound it, and nothing else may have
    // taken it while we held it. The daemon defers this so the socket outlives
    // the workers and the child (see [`ServeCtx::unlink_on_exit`]).
    if ctx.unlink_on_exit {
        let _ = std::fs::remove_file(socket_path(&ctx.home));
    }
}

/// The uid on the other end, straight from the kernel.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn peer_uid<F: std::os::fd::AsFd>(stream: &F) -> std::io::Result<u32> {
    nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::PeerCredentials)
        .map(|creds| creds.uid())
        .map_err(std::io::Error::from)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn peer_uid<F: std::os::fd::AsFd>(stream: &F) -> std::io::Result<u32> {
    nix::sys::socket::getsockopt(stream, nix::sys::socket::sockopt::LocalPeerCred)
        .map(|cred| cred.uid())
        .map_err(std::io::Error::from)
}

/// One connection, start to finish.
async fn connection(stream: UnixStream, conn: u64, ctx: ServeCtx) {
    let ours = nix::unistd::getuid().as_raw();
    // Asked BEFORE the stream is split: the halves are not file descriptors, and
    // the question is about the descriptor.
    let peer = peer_uid(&stream);
    let (reader, writer) = stream.into_split();
    let (out, out_rx) = mpsc::channel::<Frame>(WRITER_CAPACITY);
    let writer_task = tokio::spawn(writer_task(writer, out_rx));

    match peer {
        Ok(uid) if uid == ours => {}
        other => {
            let detail = match other {
                Ok(uid) => format!("uid {uid} is not uid {ours}"),
                Err(e) => format!("the peer's credentials could not be read: {e}"),
            };
            let _ = out
                .send(Frame::err(
                    wire::HELLO_ID,
                    WireError::new(
                        wire::ERR_PEER_DENIED,
                        ExitCode::Precondition,
                        format!("the broker serves its own uid only: {detail}"),
                    ),
                ))
                .await;
            drop(out);
            let _ = writer_task.await;
            return;
        }
    }

    ctx.clients.fetch_add(1, Ordering::Relaxed);
    let mut reader = BufReader::new(reader);
    serve_connection(&mut reader, &out, conn, &ctx).await;
    ctx.clients.fetch_sub(1, Ordering::Relaxed);

    drop(out);
    let _ = writer_task.await;
    ctx.core.conn_closed(conn).await;
}

/// The sole writer of this connection's socket. Every frame goes through it, so
/// no cancelled future can interleave two half-written lines.
async fn writer_task(mut writer: tokio::net::unix::OwnedWriteHalf, mut rx: mpsc::Receiver<Frame>) {
    while let Some(frame) = rx.recv().await {
        if writer.write_all(&frame.encode()).await.is_err() {
            return;
        }
        if writer.flush().await.is_err() {
            return;
        }
    }
    let _ = writer.shutdown().await;
}

/// Handshake, then requests.
async fn serve_connection(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    out: &mpsc::Sender<Frame>,
    conn: u64,
    ctx: &ServeCtx,
) {
    let hello = match next_frame(reader, out, ctx).await {
        Some(frame) => frame,
        None => return,
    };
    if hello.ty != FrameType::Hello {
        let _ = out
            .send(Frame::err(
                hello.id,
                WireError::new(
                    wire::ERR_BAD_FRAME,
                    ExitCode::Precondition,
                    "the first frame on a broker connection is a hello",
                ),
            ))
            .await;
        return;
    }
    let peer: wire::Hello = match serde_json::from_value(hello.body.clone()) {
        Ok(peer) => peer,
        Err(e) => {
            let _ = out
                .send(Frame::err(
                    hello.id,
                    WireError::new(
                        wire::ERR_BAD_FRAME,
                        ExitCode::Precondition,
                        format!("unreadable hello body: {e}"),
                    ),
                ))
                .await;
            return;
        }
    };

    if peer.protocol_version != wire::protocol_version() {
        // The skew answer carries the LIVE-WORKER COUNT, because that is the
        // fact the client's policy turns on: with nothing in flight this daemon
        // is disposable, and with work in flight it is not.
        let live = ctx.core.idle().await.map(|idle| idle.live).unwrap_or(0);
        let _ = out
            .send(Frame::err(
                hello.id,
                skew_error(&peer, live).remedy("`a2a broker restart`"),
            ))
            .await;
        drain_only(reader, out, ctx).await;
        return;
    }

    if out.send(Frame::hello()).await.is_err() {
        return;
    }
    request_loop(reader, out, conn, ctx).await;
}

pub fn skew_error(peer: &wire::Hello, live_workers: usize) -> WireError {
    WireError::new(
        wire::ERR_VERSION_SKEW,
        ExitCode::BrokerUnavailable,
        format!(
            "the broker speaks wire protocol {} (a2a {}); this client speaks {} (a2a {})",
            wire::protocol_version(),
            wire::BINARY_VERSION,
            peer.protocol_version,
            peer.binary_version
        ),
    )
    .with("broker_protocol_version", json!(wire::protocol_version()))
    .with("client_protocol_version", json!(peer.protocol_version))
    .with("broker_binary_version", json!(wire::BINARY_VERSION))
    .with("client_binary_version", json!(peer.binary_version))
    .with("live_workers", json!(live_workers))
}

/// What a peer we cannot understand is still allowed to do: retire us.
///
/// The carve-out is deliberate. A client that finds a daemon from another
/// generation must be able to replace it, and it cannot ask through a protocol
/// neither end trusts — so exactly one op survives the skew, and it is the one
/// that ends the conversation.
async fn drain_only(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    out: &mpsc::Sender<Frame>,
    ctx: &ServeCtx,
) {
    let Some(frame) = next_frame(reader, out, ctx).await else {
        return;
    };
    if frame.ty == FrameType::Req && frame.op() == Some("shutdown") {
        let _ = out
            .send(Frame::resp(frame.id, json!({"shutting_down": true})))
            .await;
        ctx.stop(Cause::Op).await;
        return;
    }
    let _ = out
        .send(Frame::err(
            frame.id,
            WireError::new(
                wire::ERR_DRAIN_ONLY,
                ExitCode::BrokerUnavailable,
                "this broker and this client disagree about the protocol: \
                 the only op it will accept from you is `shutdown`",
            )
            .remedy("`a2a broker restart`"),
        ))
        .await;
}

async fn request_loop(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    out: &mpsc::Sender<Frame>,
    conn: u64,
    ctx: &ServeCtx,
) {
    let mut subscription: Option<tokio::task::JoinHandle<()>> = None;
    while let Some(frame) = next_frame(reader, out, ctx).await {
        if frame.ty != FrameType::Req {
            let _ = out
                .send(Frame::err(
                    frame.id,
                    WireError::new(
                        wire::ERR_BAD_FRAME,
                        ExitCode::Precondition,
                        format!("expected a req frame, got {:?}", frame.ty),
                    ),
                ))
                .await;
            break;
        }
        let op = frame.op().unwrap_or_default().to_string();
        if op == "subscribe" {
            // ONE subscription per connection: a second one would need a second
            // stream of ids on a wire that has exactly one.
            if subscription.is_some() {
                let _ = out
                    .send(Frame::err(
                        frame.id,
                        WireError::new(
                            wire::ERR_ALREADY_SUBSCRIBED,
                            ExitCode::Precondition,
                            "this connection already has a subscription",
                        )
                        .remedy("open a second connection for a second tail"),
                    ))
                    .await;
                continue;
            }
            match ctx.core.subscribe().await {
                Ok(rx) => {
                    let _ = out.send(Frame::resp(frame.id, json!({"subscribed": true}))).await;
                    ctx.subscribers.fetch_add(1, Ordering::Relaxed);
                    subscription = Some(tokio::spawn(pump_subscription(
                        rx,
                        out.clone(),
                        frame.id,
                        ctx.shutdown.clone(),
                    )));
                }
                Err(e) => {
                    let _ = out.send(Frame::err(frame.id, WireError::from_app(&e))).await;
                }
            }
            continue;
        }

        let answer = dispatch(&op, &frame.body, conn, ctx).await;
        let reply = match answer {
            Ok(body) => Frame::resp(frame.id, body),
            Err(e) => Frame::err(frame.id, e),
        };
        if out.send(reply).await.is_err() {
            break;
        }
        if op == "shutdown" {
            ctx.stop(Cause::Op).await;
            break;
        }
    }
    if let Some(mut handle) = subscription {
        // Awaited, not aborted, when the daemon is going down: the pump's LAST
        // act is the farewell frame, and aborting it here would deliver exactly
        // the silent EOF that frame exists to prevent.
        if ctx.shutdown.is_cancelled() {
            let _ = tokio::time::timeout(FAREWELL_GRACE, &mut handle).await;
        }
        handle.abort();
        ctx.subscribers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Read the next frame, answering the two framing failures in place.
///
/// `None` means "this connection is over" — EOF, a write failure, a cancelled
/// daemon, or a framing error that has already been reported.
async fn next_frame(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    out: &mpsc::Sender<Frame>,
    ctx: &ServeCtx,
) -> Option<Frame> {
    let incoming = tokio::select! {
        biased;
        // Cancellation here is terminal — the reader is never resumed — so a
        // partial line is dropped on purpose rather than half-parsed.
        _ = ctx.shutdown.cancelled() => return None,
        read = wire::read_frame(reader) => read,
    };
    match incoming {
        Ok(Incoming::Frame(frame)) => Some(*frame),
        Ok(Incoming::TooLarge) => {
            let _ = out
                .send(Frame::err(
                    wire::HELLO_ID,
                    WireError::new(
                        wire::ERR_FRAME_TOO_LARGE,
                        ExitCode::Precondition,
                        format!("a frame may not exceed {} bytes", wire::MAX_FRAME_BYTES),
                    )
                    .with("max_bytes", json!(wire::MAX_FRAME_BYTES)),
                ))
                .await;
            None
        }
        Ok(Incoming::Malformed(detail)) => {
            let _ = out
                .send(Frame::err(
                    wire::HELLO_ID,
                    WireError::new(
                        wire::ERR_BAD_FRAME,
                        ExitCode::Precondition,
                        format!("unreadable frame: {detail}"),
                    ),
                ))
                .await;
            None
        }
        Ok(Incoming::Eof) | Err(_) => None,
    }
}

/// Forward one subscription's envelopes until the connection, the tail, or the
/// daemon ends.
///
/// A lagged subscriber is TOLD, with the last seq it actually received: a tail
/// that silently skips is worse than one that admits a gap, because the reader
/// cannot tell the difference between "nothing happened" and "I missed it".
/// The same argument is why a shutdown ends with a `broker_shutdown` frame
/// rather than with a closed socket.
pub async fn pump_subscription(
    mut rx: broadcast::Receiver<TailFrame>,
    out: mpsc::Sender<Frame>,
    id: u64,
    shutdown: CancellationToken,
) {
    let mut last_seq: u64 = 0;
    loop {
        // Both branches are cancel-safe: `broadcast::Receiver::recv` may be
        // dropped mid-poll without losing a message it has not yet returned,
        // and the frame below is handed to the writer task, never written here.
        let received = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                let _ = out
                    .send(Frame::broker_shutdown(id, shutdown::GRACE.as_secs()))
                    .await;
                return;
            }
            received = rx.recv() => received,
        };
        match received {
            Ok(tail) => {
                last_seq = tail.envelope.seq;
                let body = json!({"worker": tail.worker_id, "envelope": tail.envelope});
                if out.send(Frame::event(id, body)).await.is_err() {
                    return;
                }
            }
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                let _ = out
                    .send(Frame::err(
                        id,
                        WireError::new(
                            wire::ERR_LAGGED,
                            ExitCode::Internal,
                            format!("this tail fell {missed} events behind and was cut"),
                        )
                        .with("last_seq", json!(last_seq))
                        .with("missed", json!(missed))
                        .remedy("re-read the worker's events.jsonl from `last_seq`"),
                    ))
                    .await;
                return;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// The op table. Every entry is a [`CoreHandle`] call or a fact about the
/// daemon; nothing here touches a worker directory directly.
async fn dispatch(
    op: &str,
    body: &Value,
    _conn: u64,
    ctx: &ServeCtx,
) -> Result<Value, WireError> {
    match op {
        "info" => Ok(info(ctx).await),
        "list" => {
            let workers = ctx.core.list().await.map_err(|e| WireError::from_app(&e))?;
            Ok(json!({"workers": workers}))
        }
        "status" => {
            let snap = ctx
                .core
                .status(&string_arg(body, "id")?)
                .await
                .map_err(|e| WireError::from_app(&e))?;
            Ok(serde_json::to_value(snap).unwrap_or(Value::Null))
        }
        // The one op that carries CLAIMS rather than just a target: the cwd and
        // the lane the caller says it resolved. The core re-checks both against
        // what THIS daemon was started with (spec §5).
        "spawn" => {
            let req = crate::broker::core::SpawnRequest {
                dir: PathBuf::from(string_arg(body, "dir")?),
                cwd: body.get("cwd").and_then(Value::as_str).map(PathBuf::from),
                lane: body
                    .get("lane")
                    .and_then(Value::as_str)
                    .and_then(|l| l.parse::<crate::config::Lane>().ok()),
            };
            let snap = ctx
                .core
                .spawn_checked(req)
                .await
                .map_err(|e| WireError::from_app(&e))?;
            Ok(serde_json::to_value(snap).unwrap_or(Value::Null))
        }
        "resume" => {
            let snap = ctx
                .core
                .resume_worker(string_arg(body, "dir")?)
                .await
                .map_err(|e| WireError::from_app(&e))?;
            Ok(serde_json::to_value(snap).unwrap_or(Value::Null))
        }
        "send" => {
            let path = ctx
                .core
                .send(&string_arg(body, "id")?, string_arg(body, "text")?)
                .await
                .map_err(|e| WireError::from_app(&e))?;
            Ok(json!({"send_path": path.as_str()}))
        }
        "interrupt" => {
            let outcome = ctx
                .core
                .interrupt(&string_arg(body, "id")?)
                .await
                .map_err(|e| WireError::from_app(&e))?;
            Ok(json!({"noop": outcome
                == crate::broker::worker_actor::InterruptOutcome::NoopTurnAlreadyOver}))
        }
        "kill" => {
            ctx.core
                .kill(&string_arg(body, "id")?)
                .await
                .map_err(|e| WireError::from_app(&e))?;
            Ok(json!({}))
        }
        "shutdown" => Ok(json!({"shutting_down": true})),
        other => Err(WireError::new(
            wire::ERR_UNKNOWN_OP,
            ExitCode::Precondition,
            format!("this broker does not know the op `{other}`"),
        )),
    }
}

async fn info(ctx: &ServeCtx) -> Value {
    let idle = ctx.core.idle().await.ok();
    json!({
        "protocol_version": wire::protocol_version(),
        "binary_version": wire::BINARY_VERSION,
        "pid": ctx.pid,
        "started_at": ctx.started_at,
        "uptime_secs": (Utc::now() - ctx.started_at).num_seconds().max(0),
        "socket": socket_path(&ctx.home),
        "clients": ctx.clients(),
        "workers": idle.as_ref().map(|i| i.workers),
        "live": idle.as_ref().map(|i| i.live),
        "generation": idle.as_ref().map(|i| i.generation),
        "child_pid": idle.as_ref().and_then(|i| i.child_pid),
        "restarts": idle.as_ref().map(|i| i.restarts),
        "transport": match idle.as_ref().map(|i| i.degraded) {
            Some(true) => "degraded-exec",
            _ => "app-server",
        },
        "codex_bin": ctx.codex_bin,
        "allowed_roots": ctx.allowed_roots,
        "reconciled": ctx.reconciled,
    })
}

fn string_arg(body: &Value, key: &str) -> Result<String, WireError> {
    body.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            WireError::new(
                wire::ERR_BAD_FRAME,
                ExitCode::Usage,
                format!("this op needs a string `{key}`"),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Envelope, EventClass, EventKind, Transport};

    fn tail(seq: u64) -> TailFrame {
        let mut envelope = Envelope::new(
            1,
            Transport::AppServer,
            EventClass::Lifecycle,
            EventKind::TurnStarted,
            json!({}),
            Utc::now(),
        );
        envelope.seq = seq;
        TailFrame {
            worker_id: "general-0817t1200-aaaa".to_string(),
            envelope,
        }
    }

    /// The kernel's answer, not the file mode's: a real socket, a real connect,
    /// a real `getsockopt`.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_peer_uid_comes_from_the_kernel() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("probe.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let connect = tokio::task::spawn_blocking(move || {
            std::os::unix::net::UnixStream::connect(&path).unwrap()
        });
        let (server, _) = listener.accept().await.unwrap();
        let client = connect.await.unwrap();

        assert_eq!(peer_uid(&server).unwrap(), nix::unistd::getuid().as_raw());
        drop(client);
    }

    /// A tail that falls behind is cut with the seq it last saw — the cursor a
    /// reader needs to go back to the file and catch up.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_lagged_subscriber_is_told_where_it_lost_the_thread() {
        let (tx, rx) = broadcast::channel::<TailFrame>(2);
        let (out, mut frames) = mpsc::channel::<Frame>(16);

        // One frame is delivered, then the channel overruns while nobody pumps.
        tx.send(tail(1)).unwrap();
        let pump = tokio::spawn(pump_subscription(rx, out, 9, CancellationToken::new()));
        let first = frames.recv().await.expect("the first event went out");
        assert_eq!(first.ty, FrameType::Event);
        assert_eq!(first.body["envelope"]["seq"], json!(1));

        // Now overrun it: the receiver is parked inside `recv`, so the frames
        // pile up behind a capacity of two.
        for seq in 2..=8 {
            let _ = tx.send(tail(seq));
        }
        let mut lag = None;
        while let Some(frame) = frames.recv().await {
            if frame.ty == FrameType::Err {
                lag = Some(frame);
                break;
            }
        }
        let lag = lag.expect("the lagged subscriber was told");
        let err = WireError::from_value(&lag.body).unwrap();
        assert_eq!(err.code, wire::ERR_LAGGED);
        assert!(
            err.get("last_seq").and_then(Value::as_u64).unwrap() >= 1,
            "the cut names the last seq that actually arrived: {:?}",
            err.extra
        );
        let _ = pump.await;
    }

    #[test]
    fn a_missing_argument_is_a_usage_error_not_a_panic() {
        let err = string_arg(&json!({"op": "status"}), "id").unwrap_err();
        assert_eq!(err.code, wire::ERR_BAD_FRAME);
        assert_eq!(err.exit, ExitCode::Usage as i32);
    }
}
