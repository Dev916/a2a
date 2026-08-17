//! The CLI's half of the broker socket — **synchronous**, because every `a2a`
//! command except the daemon itself is (Global Constraints).
//!
//! Three things live here, and only one of them is "send bytes":
//!
//! * **Autostart.** A CLI that finds no broker starts one. The binary it starts
//!   is resolved `A2A_BROKER_BIN` → `A2A_SHIM_BIN` → [`exec::shim_bin`] and
//!   NEVER from a bare `current_exe`: under `cargo test` that is the test
//!   harness, and a harness told to `broker run` is a hang with a confusing
//!   name. The daemon is then handed the environment it needs — it is not under
//!   the child env allowlist, which governs the app-server child and workers.
//! * **The skew policy (spec §4.2), both branches.** A broker from another
//!   generation with nothing in flight is *disposable*: drain it and start one
//!   from this process's own environment. A broker holding live workers is not:
//!   exit 7, name both builds, and hand the operator `a2a broker restart` rather
//!   than killing work it cannot see.
//! * **Buffered events.** A subscription and request/response share one
//!   connection, so a request that meets an `event` frame keeps it instead of
//!   discarding it — `next_event` drains that buffer first. A tail that loses
//!   frames to an unrelated `status` call would be a bug nobody could reproduce.

use crate::broker::sockserv;
use crate::broker::wire::{self, Frame, FrameType, Incoming, WireError};
use crate::events::Envelope;
use crate::exec;
use crate::output::{AppError, ExitCode};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::io::{BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// First choice for the daemon binary: an explicit override for ops.
pub const BROKER_BIN_ENV: &str = "A2A_BROKER_BIN";

/// The autostart retry budget: 20 × 100ms. Bounded because a broker that has
/// not answered in two seconds is not slow, it is broken — and a CLI that waits
/// forever is worse than one that says so.
pub const CONNECT_TRIES: u32 = 20;
pub const CONNECT_INTERVAL: Duration = Duration::from_millis(100);

/// How long a drain waits for the retired daemon's socket to disappear.
pub const DRAIN_WAIT: Duration = Duration::from_secs(10);

/// Environment the DAEMON needs, by exact name. The daemon is not a worker: it
/// resolves the codex binary, the allowed roots and the lane configuration
/// itself, so the allowlist that governs the app-server child does not apply.
pub const DAEMON_ENV_KEYS: [&str; 4] = [
    "A2A_CODEX_BIN",
    "A2A_CODEX_VERSION_RANGE",
    "A2A_ALLOWED_ROOTS",
    "A2A_SHIM_BIN",
];

/// ... and by prefix: lane overrides, admission caps, and the two test seams
/// (`A2A_TEST_*` carries the protocol-skew override, `FAKE_*` drives the
/// scripted app-server through the daemon into its child).
pub const DAEMON_ENV_PREFIXES: [&str; 4] = ["A2A_LANES_", "A2A_ADMISSION_", "A2A_TEST_", "FAKE_"];

/// A connected, handshaken broker client.
pub struct BrokerClient {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
    next_id: u64,
    /// What the far end said it is.
    pub peer: wire::Hello,
    /// True when the handshake failed on version skew: this connection may do
    /// exactly one thing, and it is `shutdown`.
    skewed: bool,
    subscription: Option<u64>,
    events: VecDeque<TailEvent>,
}

/// One envelope, as a subscriber sees it.
#[derive(Deserialize, Clone, Debug)]
pub struct TailEvent {
    pub worker: String,
    pub envelope: Envelope,
}

/// What a handshake produced.
pub enum Handshake {
    Ready(BrokerClient),
    /// The far end speaks another protocol. The client is kept: it is the only
    /// thing that can retire the daemon.
    Skewed {
        client: BrokerClient,
        error: WireError,
    },
}

impl Handshake {
    pub fn live_workers(&self) -> u64 {
        match self {
            Handshake::Ready(_) => 0,
            Handshake::Skewed { error, .. } => error
                .get("live_workers")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX),
        }
    }
}

// ---------------------------------------------------------------------------
// connecting
// ---------------------------------------------------------------------------

/// Connect to a broker that is already running. Never starts one.
pub fn connect(home: &Path) -> Result<BrokerClient, AppError> {
    match try_handshake(home)? {
        Some(Handshake::Ready(client)) => Ok(client),
        Some(Handshake::Skewed { error, .. }) => Err(error.to_app()),
        None => Err(unavailable(home, "no broker is listening")),
    }
}

/// Connect, starting a broker if there is none — and applying the skew policy
/// if the one that is there is from another generation.
pub fn connect_or_start(home: &Path) -> Result<BrokerClient, AppError> {
    let mut respawned = false;
    // EXACTLY ONE autostart per path through this function, which is why the
    // loop never re-enters the "nothing is listening" branch: a second `broker
    // run` racing the first is how one of them ends up holding a listener on an
    // unlinked socket — running, unreachable, and nobody's to clean up.
    let mut outcome = match try_handshake(home)? {
        Some(outcome) => outcome,
        None => {
            autostart(home)?;
            retry_handshake(home)?
        }
    };
    loop {
        match outcome {
            Handshake::Ready(client) => return Ok(client),
            Handshake::Skewed { mut client, error } => {
                // Nothing in flight: this daemon is disposable, and the one that
                // replaces it comes up from THIS process's environment — which
                // is what makes the respawn a fix rather than a coin flip.
                if !respawned && error.get("live_workers").and_then(Value::as_u64) == Some(0) {
                    client.drain()?;
                    wait_for_socket_removal(home, DRAIN_WAIT)?;
                    autostart(home)?;
                    respawned = true;
                    outcome = retry_handshake(home)?;
                    continue;
                }
                return Err(error.to_app());
            }
        }
    }
}

/// Ask a running broker to stop, and wait for its socket to go with it.
///
/// `Ok(false)` when there was nothing to stop — shutting down an absent daemon
/// is a success, not an error, or every teardown path needs a special case.
pub fn shutdown(home: &Path) -> Result<bool, AppError> {
    let Some(outcome) = try_handshake(home)? else {
        return Ok(false);
    };
    match outcome {
        Handshake::Ready(mut client) => {
            client.request("shutdown", json!({}))?;
        }
        // A daemon we cannot talk to is still a daemon we may retire.
        Handshake::Skewed { mut client, .. } => client.drain()?,
    }
    wait_for_socket_removal(home, DRAIN_WAIT)?;
    Ok(true)
}

/// `Ok(None)` = nothing is listening (no socket, or a stale one).
pub fn try_handshake(home: &Path) -> Result<Option<Handshake>, AppError> {
    let path = sockserv::socket_path(home);
    match UnixStream::connect(&path) {
        Ok(stream) => handshake(stream, home).map(Some),
        Err(e) if is_absent(&e) => Ok(None),
        Err(e) => Err(unavailable(home, format!("connecting: {e}"))),
    }
}

/// The bounded retry a freshly autostarted daemon is given to come up.
pub fn retry_handshake(home: &Path) -> Result<Handshake, AppError> {
    for _ in 0..CONNECT_TRIES {
        if let Some(outcome) = try_handshake(home)? {
            return Ok(outcome);
        }
        // Blocking on purpose: this is the synchronous CLI, and the one async
        // process in this system is the daemon we are waiting for.
        std::thread::sleep(CONNECT_INTERVAL);
    }
    Err(unavailable(
        home,
        format!(
            "the broker did not answer within {}ms of being started",
            CONNECT_TRIES as u128 * CONNECT_INTERVAL.as_millis()
        ),
    ))
}

fn handshake(stream: UnixStream, home: &Path) -> Result<Handshake, AppError> {
    let reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| unavailable(home, format!("cloning the socket: {e}")))?,
    );
    let mut client = BrokerClient {
        stream,
        reader,
        next_id: 1,
        peer: wire::Hello {
            protocol_version: 0,
            binary_version: String::new(),
        },
        skewed: false,
        subscription: None,
        events: VecDeque::new(),
    };
    client.write(&Frame::hello())?;
    let frame = client.read(home)?;
    match frame.ty {
        FrameType::Hello => {
            client.peer = serde_json::from_value(frame.body).map_err(|e| {
                unavailable(home, format!("the broker's hello is unreadable: {e}"))
            })?;
            Ok(Handshake::Ready(client))
        }
        FrameType::Err => {
            let error = WireError::from_value(&frame.body)
                .unwrap_or_else(|| WireError::from_app(&unavailable(home, "the broker refused us")));
            if error.code == wire::ERR_VERSION_SKEW {
                client.skewed = true;
                client.peer = wire::Hello {
                    protocol_version: error
                        .get("broker_protocol_version")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as u32,
                    binary_version: error
                        .get("broker_binary_version")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                };
                return Ok(Handshake::Skewed { client, error });
            }
            Err(error.to_app())
        }
        other => Err(unavailable(
            home,
            format!("the broker answered a hello with {other:?}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// the conversation
// ---------------------------------------------------------------------------

impl BrokerClient {
    /// One request, one answer. Event frames that arrive in between are kept.
    pub fn request(&mut self, op: &str, params: Value) -> Result<Value, AppError> {
        if self.skewed && op != "shutdown" {
            return Err(AppError::new(
                wire::ERR_VERSION_SKEW,
                ExitCode::BrokerUnavailable,
                format!("this broker speaks protocol {}", self.peer.protocol_version),
            )
            .remedy("`a2a broker restart`"));
        }
        let id = self.next_id;
        self.next_id += 1;
        self.write(&Frame::req(id, op, params))?;
        loop {
            let frame = self.read(Path::new(""))?;
            if frame.ty == FrameType::Event {
                self.buffer_event(frame);
                continue;
            }
            if frame.id != id {
                // An error stamped with someone else's id is a framing failure,
                // not an answer: report it rather than waiting forever.
                if frame.ty == FrameType::Err {
                    return Err(self.wire_error(&frame));
                }
                continue;
            }
            return match frame.ty {
                FrameType::Resp => Ok(frame.body),
                FrameType::Err => Err(self.wire_error(&frame)),
                other => Err(AppError::new(
                    wire::ERR_BAD_FRAME,
                    ExitCode::Internal,
                    format!("the broker answered a request with {other:?}"),
                )),
            };
        }
    }

    /// Start this connection's one subscription. Returns its frame id — the id
    /// every `event` frame on this connection will carry, which is the id
    /// `request` is about to spend.
    pub fn subscribe(&mut self) -> Result<u64, AppError> {
        let id = self.next_id;
        self.request("subscribe", json!({}))?;
        self.subscription = Some(id);
        Ok(id)
    }

    /// The subscription's frame id, once there is one.
    pub fn subscription(&self) -> Option<u64> {
        self.subscription
    }

    /// The next envelope, or `None` if none arrived inside `timeout`.
    pub fn next_event(&mut self, timeout: Duration) -> Result<Option<TailEvent>, AppError> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        self.stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| internal(format!("setting a read timeout: {e}")))?;
        let outcome = wire::read_frame_sync(&mut self.reader);
        let _ = self.stream.set_read_timeout(None);
        match outcome {
            Ok(Incoming::Frame(frame)) => match frame.ty {
                FrameType::Event => {
                    self.buffer_event(*frame);
                    Ok(self.events.pop_front())
                }
                FrameType::Err => Err(self.wire_error(&frame)),
                _ => Ok(None),
            },
            Ok(Incoming::Eof) => Err(AppError::new(
                wire::ERR_BROKER_UNAVAILABLE,
                ExitCode::BrokerUnavailable,
                "the broker closed the tail",
            )),
            Ok(Incoming::TooLarge) => Err(internal("the broker sent an oversized frame")),
            Ok(Incoming::Malformed(detail)) => {
                Err(internal(format!("unreadable frame from the broker: {detail}")))
            }
            Err(e) if would_block(&e) => Ok(None),
            Err(e) => Err(AppError::new(
                wire::ERR_BROKER_UNAVAILABLE,
                ExitCode::BrokerUnavailable,
                format!("reading from the broker: {e}"),
            )),
        }
    }

    /// The one op a skewed connection may perform.
    pub fn drain(&mut self) -> Result<(), AppError> {
        let id = self.next_id;
        self.next_id += 1;
        self.write(&Frame::req(id, "shutdown", json!({})))?;
        // The answer is a courtesy; the daemon may close first, and a broker
        // that hung up mid-shutdown has still shut down.
        match self.read(Path::new("")) {
            Ok(_) | Err(_) => Ok(()),
        }
    }

    fn buffer_event(&mut self, frame: Frame) {
        if let Ok(event) = serde_json::from_value::<TailEvent>(frame.body) {
            self.events.push_back(event);
        }
    }

    fn wire_error(&self, frame: &Frame) -> AppError {
        WireError::from_value(&frame.body)
            .map(|e| e.to_app())
            .unwrap_or_else(|| internal(format!("the broker sent an unreadable error: {}", frame.body)))
    }

    fn write(&mut self, frame: &Frame) -> Result<(), AppError> {
        self.stream
            .write_all(&frame.encode())
            .and_then(|()| self.stream.flush())
            .map_err(|e| {
                AppError::new(
                    wire::ERR_BROKER_UNAVAILABLE,
                    ExitCode::BrokerUnavailable,
                    format!("writing to the broker: {e}"),
                )
                .remedy("`a2a broker restart`")
            })
    }

    fn read(&mut self, home: &Path) -> Result<Frame, AppError> {
        match wire::read_frame_sync(&mut self.reader) {
            Ok(Incoming::Frame(frame)) => Ok(*frame),
            Ok(Incoming::Eof) => Err(unavailable(home, "the broker closed the connection")),
            Ok(Incoming::TooLarge) => Err(internal("the broker sent an oversized frame")),
            Ok(Incoming::Malformed(detail)) => {
                Err(internal(format!("unreadable frame from the broker: {detail}")))
            }
            Err(e) => Err(unavailable(home, format!("reading: {e}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// autostart
// ---------------------------------------------------------------------------

/// The `a2a` binary that will host the daemon.
///
/// `A2A_BROKER_BIN` first (an ops override), then [`exec::shim_bin`], which is
/// `A2A_SHIM_BIN` and then the binary NEXT TO the running executable — the
/// Phase-1 resolution, and the reason a `cargo test` run does not try to
/// daemonize its own test harness.
pub fn broker_bin() -> Result<PathBuf, AppError> {
    if let Some(path) = std::env::var_os(BROKER_BIN_ENV).map(PathBuf::from) {
        if path.is_file() {
            return Ok(path);
        }
    }
    exec::shim_bin()
}

/// The environment an autostarted daemon is handed explicitly.
///
/// A spawned child inherits its parent's environment anyway; this is what makes
/// the set *stated* rather than incidental, so the day someone adds an
/// `env_clear()` here the daemon still gets the four things it cannot resolve
/// without (and the test seams keep working).
pub fn daemon_env() -> Vec<(String, String)> {
    daemon_env_from(std::env::vars())
}

/// The pure half — the process environment is an input, not a global.
pub fn daemon_env_from<I>(vars: I) -> Vec<(String, String)>
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut out: Vec<(String, String)> = vars
        .into_iter()
        .filter(|(key, _)| {
            DAEMON_ENV_KEYS.contains(&key.as_str())
                || DAEMON_ENV_PREFIXES.iter().any(|p| key.starts_with(p))
        })
        .collect();
    out.sort();
    out
}

/// Fork `<a2a> --home <home> broker run`.
///
/// Returns as soon as the process exists; the caller's bounded retry-connect is
/// what decides whether it came up. Its stdio goes to `broker.log`, and it gets
/// its own session so a `^C` in the terminal that started it does not take the
/// daemon with it.
pub fn autostart(home: &Path) -> Result<u32, AppError> {
    let bin = broker_bin()?;
    let log = exec::open_append_0600(&home.join(crate::broker::BROKER_LOG)).map_err(|e| {
        internal(format!(
            "opening {}: {e}",
            home.join(crate::broker::BROKER_LOG).display()
        ))
    })?;
    let errlog = log
        .try_clone()
        .map_err(|e| internal(format!("cloning the broker log handle: {e}")))?;

    let mut cmd = std::process::Command::new(&bin);
    cmd.arg("--home")
        .arg(home)
        .args(["broker", "run"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(log))
        .stderr(std::process::Stdio::from(errlog));
    for (key, value) in daemon_env() {
        cmd.env(key, value);
    }
    unsafe {
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(|| match nix::unistd::setsid() {
            Ok(_) => Ok(()),
            Err(nix::errno::Errno::EPERM) => Ok(()), // already a session leader
            Err(e) => Err(std::io::Error::from(e)),
        });
    }
    let child = cmd.spawn().map_err(|e| {
        AppError::new(
            "BROKER_START_FAILED",
            ExitCode::BrokerUnavailable,
            format!("could not start `{} broker run`: {e}", bin.display()),
        )
        .remedy("check broker.log in the a2a home")
    })?;
    Ok(child.id())
}

/// Wait for a retired daemon to take its socket down.
pub fn wait_for_socket_removal(home: &Path, patience: Duration) -> Result<(), AppError> {
    let path = sockserv::socket_path(home);
    let deadline = Instant::now() + patience;
    while path.exists() {
        if Instant::now() >= deadline {
            return Err(AppError::new(
                wire::ERR_BROKER_UNAVAILABLE,
                ExitCode::BrokerUnavailable,
                format!(
                    "the broker was asked to stop but {} is still there after {}s",
                    path.display(),
                    patience.as_secs()
                ),
            )
            .remedy("`a2a broker restart`"));
        }
        std::thread::sleep(CONNECT_INTERVAL);
    }
    Ok(())
}

fn is_absent(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
    )
}

fn would_block(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

fn unavailable(home: &Path, detail: impl Into<String>) -> AppError {
    let where_ = if home.as_os_str().is_empty() {
        String::new()
    } else {
        format!(" at {}", sockserv::socket_path(home).display())
    };
    AppError::new(
        wire::ERR_BROKER_UNAVAILABLE,
        ExitCode::BrokerUnavailable,
        format!("the broker{where_} is not reachable: {}", detail.into()),
    )
    .remedy("`a2a broker run` starts one; `a2a doctor` says why it will not start")
}

fn internal(message: impl Into<String>) -> AppError {
    AppError::new("INTERNAL", ExitCode::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The daemon is NOT under the child env allowlist: it resolves the codex
    /// binary, the allowed roots and the lane config itself, so those have to
    /// reach it — and the two test seams have to reach it or the scripted
    /// app-server can never be driven through a real broker.
    #[test]
    fn the_daemon_environment_is_by_name_and_by_prefix() {
        let env: Vec<(String, String)> = [
            ("A2A_CODEX_BIN", "/opt/codex"),
            ("A2A_CODEX_VERSION_RANGE", ">=0.140, <0.150"),
            ("A2A_ALLOWED_ROOTS", "/Users/x/dev"),
            ("A2A_SHIM_BIN", "/usr/local/bin/a2a"),
            ("A2A_LANES_IMPL_TASK_TIMEOUT_SECS", "60"),
            ("A2A_ADMISSION_MAX_RUNNING", "4"),
            ("A2A_TEST_PROTO_VERSION", "42"),
            ("FAKE_SCENARIO", "happy"),
            // ... and the ones that must NOT travel:
            ("A2A_HOME", "/tmp/somewhere-else"),
            ("A2A_JSON", "1"),
            ("OPENAI_API_KEY", "sk-secret"),
            ("PATH", "/usr/bin"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let selected = daemon_env_from(env);
        let keys: Vec<&str> = selected.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "A2A_ADMISSION_MAX_RUNNING",
                "A2A_ALLOWED_ROOTS",
                "A2A_CODEX_BIN",
                "A2A_CODEX_VERSION_RANGE",
                "A2A_LANES_IMPL_TASK_TIMEOUT_SECS",
                "A2A_SHIM_BIN",
                "A2A_TEST_PROTO_VERSION",
                "FAKE_SCENARIO",
            ],
            "A2A_HOME travels as `--home` (not as inherited state), and nothing \
             else is smuggled in because it happens to start with `A2A_`"
        );
    }

    #[test]
    fn an_absent_socket_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(try_handshake(tmp.path()).unwrap().is_none());
        assert!(!shutdown(tmp.path()).unwrap());
    }

    #[test]
    fn a_connect_with_no_broker_is_exit_7() {
        let tmp = tempfile::tempdir().unwrap();
        let Err(err) = connect(tmp.path()) else {
            panic!("connecting to an empty home cannot succeed");
        };
        assert_eq!(err.exit, ExitCode::BrokerUnavailable);
        assert_eq!(err.code, wire::ERR_BROKER_UNAVAILABLE);
        assert!(err.remedy.is_some(), "an unreachable broker names a way out");
    }
}
