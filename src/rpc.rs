//! NDJSON JSON-RPC client for the private `codex app-server` child (spec §4.2).
//!
//! One [`Client`] owns one child process. Three tasks sit behind it:
//!
//! ```text
//!   call()/respond() ──mpsc──▶ writer task ──▶ child stdin
//!   child stdout ──▶ reader task ──mpsc──▶ dispatcher ──┬─▶ oneshot per pending call
//!                                                       ├─▶ notifications channel
//!                                                       └─▶ server_requests channel
//! ```
//!
//! Cancellation safety is the whole point of that shape
//! (`docs/development/rust-async-cancellation-graceful-shutdown.md` §2):
//!
//! * **`write_all` is never inside a `select!`.** All writes are funnelled
//!   through a dedicated writer task that owns the child's stdin and does
//!   nothing else, so a dropped `call()` future can never truncate a frame
//!   mid-line and desynchronize the stream.
//! * The dispatcher's `select!` arms are `mpsc::Receiver::recv` only — every
//!   one cancel-safe — so the pending-call map can live *inside* that task.
//!   That is also why there is no `Arc<Mutex<…>>` here: the map is owned, not
//!   shared (the actor discipline the broker is built on).
//! * Channels are bounded. A consumer that stops draining notifications
//!   applies backpressure to the dispatcher; nothing is silently dropped.
//!
//! Framing is the verified live one (see [`crate::protocol`]): requests carry
//! `jsonrpc`/`id`/`method`/`params`, responses omit `jsonrpc`, notifications
//! have no `id`, and a frame with **both** `id` and `method` is a server→client
//! request that must be answered — [`Client::respond`] is that answer. A line
//! that does not parse is never dropped: it is surfaced as a synthetic
//! [`UNPARSEABLE_METHOD`] notification, because silent loss on this stream is
//! indistinguishable from a hang.

use crate::protocol::{RpcErrorObject, RpcNotification, RpcRequest, RpcResponse};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

/// Method of the synthetic notification carrying a line that did not parse.
/// The raw text is in `params.line`.
pub const UNPARSEABLE_METHOD: &str = "__unparseable";

/// Bound on the notification channel (spec §4.5 uses the same 1024 for worker
/// mailboxes). A slow consumer backs the dispatcher up; it never loses frames.
pub const NOTIFICATION_CAPACITY: usize = 1024;
/// Bound on the server→client request channel. These are rare and each owes an
/// answer within 100ms, so the queue should never be deep.
pub const SERVER_REQUEST_CAPACITY: usize = 256;
/// Bound on the outbound frame queue feeding the writer task.
pub const WRITER_CAPACITY: usize = 256;
/// Bound on the pending-call registration queue.
const REGISTRY_CAPACITY: usize = 256;

/// The env allowlist for the app-server child (spec §4.2). It applies to the
/// **child and to workers**, never to the broker daemon itself, which needs its
/// own `A2A_*` configuration to function.
pub const CHILD_ENV_ALLOWLIST: [&str; 7] = [
    "PATH",
    "HOME",
    "USER",
    "TERM",
    "LANG",
    "TMPDIR",
    "CODEX_HOME",
];

/// Names that are stripped from the child environment even when a caller passes
/// them explicitly — the scrub always wins over the allowlist and over
/// `extra_env`. `AWS_` is a prefix match.
pub fn is_scrubbed_env(key: &str) -> bool {
    key == "OPENAI_API_KEY" || key == "DEPOT_TOKEN" || key.starts_with("AWS_")
}

/// The complete environment for a process spawned from the codex binary:
/// [`CHILD_ENV_ALLOWLIST`] read out of this process's environment, then the
/// caller's explicit pairs, with [`is_scrubbed_env`] winning over both.
///
/// Shared rather than inlined into [`Client::spawn`] because the app-server
/// child is not the only thing `a2a` runs that binary for — the broker also asks
/// it `--version` for the drift gate, and a version probe that inherited the
/// daemon's environment would hand a credential to a process the allowlist
/// exists to keep it away from.
pub fn child_env(extra: &[(String, String)]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = CHILD_ENV_ALLOWLIST
        .iter()
        .filter(|key| !is_scrubbed_env(key))
        .filter_map(|key| std::env::var(key).ok().map(|v| ((*key).to_string(), v)))
        .collect();
    out.extend(
        extra
            .iter()
            .filter(|(key, _)| !is_scrubbed_env(key))
            .cloned(),
    );
    out
}

/// Everything a `call` can fail with.
///
/// The distinction that matters operationally: [`RpcError::ChildDied`] means the
/// transport is gone (the broker must restart the child and the caller should
/// fall back), while [`RpcError::Remote`] means the server answered and said no.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    /// The per-request deadline elapsed with no answer.
    Timeout,
    /// The child's stdout hit EOF, or its stdin is already closed.
    ChildDied,
    /// A JSON-RPC error frame came back.
    Remote { code: i64, message: String },
    /// The frame arrived but did not fit the expected type.
    Decode(String),
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::Timeout => write!(f, "app-server request timed out"),
            RpcError::ChildDied => write!(f, "app-server child died"),
            RpcError::Remote { code, message } => {
                write!(f, "app-server error {code}: {message}")
            }
            RpcError::Decode(e) => write!(f, "app-server response did not decode: {e}"),
        }
    }
}

impl std::error::Error for RpcError {}

/// A server→client request: it carries an `id` and therefore **owes an answer**
/// (see `crate::protocol::SERVER_REQUEST_METHODS`). Dropping one hangs the turn
/// on the far side forever.
#[derive(Debug, Clone)]
pub struct ServerRequestFrame {
    pub id: serde_json::Value,
    pub method: String,
    pub params: serde_json::Value,
}

/// The two inbound streams a caller must drain.
pub struct Incoming {
    pub notifications: mpsc::Receiver<RpcNotification>,
    pub server_requests: mpsc::Receiver<ServerRequestFrame>,
}

/// One classified inbound line, as the reader task hands it to the dispatcher.
enum Frame {
    Response(RpcResponse),
    Notification(RpcNotification),
    ServerRequest(ServerRequestFrame),
    /// Child stdout reached EOF (or errored): the transport is finished.
    Eof,
}

/// Pending-call bookkeeping, sent to the dispatcher that owns the map.
enum Registry {
    Register(u64, oneshot::Sender<RpcResponse>),
    /// The caller gave up (timeout): forget the id so a late answer is ignored
    /// rather than accumulating.
    Cancel(u64),
}

/// A live NDJSON JSON-RPC connection to one app-server child.
///
/// Dropping the `Client` closes the writer channel, which closes the child's
/// stdin, which is how a well-behaved app-server is asked to exit.
pub struct Client {
    writer: mpsc::Sender<Vec<u8>>,
    registry: mpsc::Sender<Registry>,
    next_id: AtomicU64,
}

impl Client {
    /// Spawns `bin` with a **scrubbed, allowlisted** environment and wires up
    /// the three tasks.
    ///
    /// `extra_env` is the explicit-pairs channel: the broker's test seam
    /// forwards `FAKE_*` / `A2A_TEST_*` through it, and production spawns pass
    /// none. The scrub list ([`is_scrubbed_env`]) always wins over it.
    ///
    /// The child's stderr is appended to `stderr_log` (its parent directory is
    /// created if needed) — never inherited, so a chatty child cannot interleave
    /// with `a2a`'s own output.
    ///
    /// The child is spawned `kill_on_drop`: a graceful shutdown should still
    /// close stdin and reap it, but dropping the handle must never leak a live
    /// app-server.
    pub async fn spawn(
        bin: &Path,
        args: &[&str],
        extra_env: &[(String, String)],
        stderr_log: &Path,
    ) -> io::Result<(Client, Incoming, Child)> {
        if let Some(parent) = stderr_log.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let errfile = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(stderr_log)?;

        let mut cmd = Command::new(bin);
        cmd.args(args);
        cmd.env_clear();
        cmd.envs(child_env(extra_env));
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(errfile))
            .kill_on_drop(true);

        let mut child = cmd.spawn()?;
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");

        let (writer_tx, writer_rx) = mpsc::channel::<Vec<u8>>(WRITER_CAPACITY);
        let (registry_tx, registry_rx) = mpsc::channel::<Registry>(REGISTRY_CAPACITY);
        let (frames_tx, frames_rx) = mpsc::channel::<Frame>(NOTIFICATION_CAPACITY);
        let (notif_tx, notif_rx) = mpsc::channel::<RpcNotification>(NOTIFICATION_CAPACITY);
        let (sreq_tx, sreq_rx) = mpsc::channel::<ServerRequestFrame>(SERVER_REQUEST_CAPACITY);

        tokio::spawn(writer_task(stdin, writer_rx));
        tokio::spawn(reader_task(stdout, frames_tx));
        tokio::spawn(dispatch_task(frames_rx, registry_rx, notif_tx, sreq_tx));

        let client = Client {
            writer: writer_tx,
            registry: registry_tx,
            next_id: AtomicU64::new(1),
        };
        let incoming = Incoming {
            notifications: notif_rx,
            server_requests: sreq_rx,
        };
        Ok((client, incoming, child))
    }

    /// Sends a request and awaits its correlated answer.
    ///
    /// `timeout` is per-request and mandatory: a dropped server→client answer,
    /// a wedged model call or a stopped child would otherwise wait forever.
    pub async fn call<P: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: &P,
        timeout: Duration,
    ) -> Result<R, RpcError> {
        let params = serde_json::to_value(params).map_err(|e| RpcError::Decode(e.to_string()))?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        if self
            .registry
            .send(Registry::Register(id, tx))
            .await
            .is_err()
        {
            return Err(RpcError::ChildDied);
        }
        let mut line = serde_json::to_vec(&RpcRequest::new(id, method, params))
            .map_err(|e| RpcError::Decode(e.to_string()))?;
        line.push(b'\n');
        if self.writer.send(line).await.is_err() {
            let _ = self.registry.send(Registry::Cancel(id)).await;
            return Err(RpcError::ChildDied);
        }
        match tokio::time::timeout(timeout, rx).await {
            Err(_) => {
                let _ = self.registry.send(Registry::Cancel(id)).await;
                Err(RpcError::Timeout)
            }
            // The dispatcher dropped the sender without answering: EOF.
            Ok(Err(_)) => Err(RpcError::ChildDied),
            Ok(Ok(response)) => {
                if let Some(e) = response.error {
                    return Err(RpcError::Remote {
                        code: e.code,
                        message: e.message,
                    });
                }
                serde_json::from_value(response.result.unwrap_or(serde_json::Value::Null))
                    .map_err(|e| RpcError::Decode(e.to_string()))
            }
        }
    }

    /// Answers a [`ServerRequestFrame`]: `Ok(value)` becomes a result frame,
    /// `Err((code, message))` a JSON-RPC error frame. Neither carries a
    /// `jsonrpc` field — responses on this wire do not have one.
    pub async fn respond(
        &self,
        id: serde_json::Value,
        result: Result<serde_json::Value, (i64, String)>,
    ) -> io::Result<()> {
        let frame = match result {
            Ok(value) => RpcResponse {
                id,
                result: Some(value),
                error: None,
            },
            Err((code, message)) => RpcResponse {
                id,
                result: None,
                error: Some(RpcErrorObject {
                    code,
                    message,
                    data: None,
                }),
            },
        };
        let mut line = serde_json::to_vec(&frame)?;
        line.push(b'\n');
        self.writer
            .send(line)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "app-server stdin is closed"))
    }
}

// ---------------------------------------------------------------------------
// tasks
// ---------------------------------------------------------------------------

/// Sole owner of the child's stdin. `write_all` lives here and only here —
/// never in a `select!` — so no cancelled future can leave a half-written line
/// on the wire.
async fn writer_task(mut stdin: tokio::process::ChildStdin, mut rx: mpsc::Receiver<Vec<u8>>) {
    while let Some(buf) = rx.recv().await {
        if stdin.write_all(&buf).await.is_err() {
            break;
        }
        if stdin.flush().await.is_err() {
            break;
        }
    }
    // Dropping stdin closes it, which is the app-server's exit signal.
}

/// Reads NDJSON lines and classifies them. Nothing else: keeping the read in a
/// task of its own means the dispatcher's `select!` only ever polls cancel-safe
/// `recv()`s.
async fn reader_task(stdout: tokio::process::ChildStdout, tx: mpsc::Sender<Frame>) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                if tx.send(classify(&line)).await.is_err() {
                    return; // dispatcher gone
                }
            }
            _ => {
                let _ = tx.send(Frame::Eof).await;
                return;
            }
        }
    }
}

fn unparseable(line: &str) -> Frame {
    Frame::Notification(RpcNotification {
        method: UNPARSEABLE_METHOD.to_string(),
        params: serde_json::json!({ "line": line }),
    })
}

fn classify(line: &str) -> Frame {
    let value: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => return unparseable(line),
    };
    let has_method = value.get("method").and_then(|m| m.as_str()).is_some();
    let id = value.get("id").filter(|v| !v.is_null()).cloned();
    match (has_method, id) {
        // id + method: a server→client request.
        (true, Some(id)) => Frame::ServerRequest(ServerRequestFrame {
            id,
            method: value["method"].as_str().unwrap_or_default().to_string(),
            params: value
                .get("params")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        }),
        (true, None) => match serde_json::from_value::<RpcNotification>(value) {
            Ok(n) => Frame::Notification(n),
            Err(_) => unparseable(line),
        },
        (false, Some(_)) => match serde_json::from_value::<RpcResponse>(value) {
            Ok(r) => Frame::Response(r),
            Err(_) => unparseable(line),
        },
        (false, None) => unparseable(line),
    }
}

/// Owns the pending-call map — no lock, no sharing, one owner (the actor
/// discipline `src/broker/` is built on). Both `select!` arms are `recv()`s,
/// which are cancel-safe, so nothing can be lost when one arm wins.
async fn dispatch_task(
    mut frames: mpsc::Receiver<Frame>,
    mut registry: mpsc::Receiver<Registry>,
    notifications: mpsc::Sender<RpcNotification>,
    server_requests: mpsc::Sender<ServerRequestFrame>,
) {
    let mut pending: HashMap<u64, oneshot::Sender<RpcResponse>> = HashMap::new();
    let mut clients_gone = false;
    loop {
        tokio::select! {
            msg = registry.recv(), if !clients_gone => match msg {
                Some(Registry::Register(id, tx)) => { pending.insert(id, tx); }
                Some(Registry::Cancel(id)) => { pending.remove(&id); }
                // Every Client handle was dropped; keep serving in-flight work
                // but stop polling this arm (recv would return None forever).
                None => clients_gone = true,
            },
            frame = frames.recv() => match frame {
                Some(Frame::Response(response)) => {
                    if let Some(id) = response.id.as_u64() {
                        if let Some(tx) = pending.remove(&id) {
                            let _ = tx.send(response);
                        }
                        // Otherwise it is a late answer to a call that already
                        // timed out: absorbed, never fatal.
                    }
                }
                Some(Frame::Notification(n)) => {
                    if notifications.send(n).await.is_err() && clients_gone {
                        break;
                    }
                }
                Some(Frame::ServerRequest(r)) => {
                    if server_requests.send(r).await.is_err() && clients_gone {
                        break;
                    }
                }
                // EOF, or the reader task is gone: the transport is finished.
                Some(Frame::Eof) | None => break,
            },
        }
    }
    // Dropping the map resolves every waiter as ChildDied; dropping the two
    // senders closes the streams so `recv()` yields None instead of hanging.
    drop(pending);
}
