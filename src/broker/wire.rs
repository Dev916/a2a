//! The CLI↔broker wire: **one frame shape**, NDJSON, and one integer that
//! decides whether two binaries may talk to each other at all.
//!
//! ```text
//! {"v":1,"id":7,"type":"req","body":{"op":"status","id":"impl-…"}}
//! {"v":1,"id":7,"type":"resp","body":{…}}
//! {"v":1,"id":0,"type":"hello","body":{"protocol_version":1,"binary_version":"0.1.0"}}
//! {"v":1,"id":7,"type":"err","body":{"code":"NO_SUCH_WORKER","exit":3,…}}
//! ```
//!
//! Three decisions are load-bearing here, and each is the answer to a way this
//! could rot:
//!
//! * **The handshake is a frame like any other.** A protocol whose first
//!   message has a different shape needs two parsers, and the second one is
//!   always the one nobody tests. `hello` is a [`Frame`] with `type:"hello"`;
//!   the reader that handles it is the reader that handles everything else.
//! * **One version integer decides skew.** [`Frame::v`] is the shape of the
//!   envelope and has never changed; `protocol_version` inside the hello body is
//!   what the two ends compare. Two knobs would eventually disagree about which
//!   one meant "incompatible", and a wrong answer there is a daemon and a CLI
//!   silently misreading each other's payloads.
//! * **Frames are capped at [`MAX_FRAME_BYTES`].** The sender picks the length,
//!   so the receiver must pick the limit: past the cap the reader stops
//!   *accumulating* (it keeps draining, so the socket does not wedge) and the
//!   connection ends with a typed error rather than an allocation.

use crate::output::{AppError, ExitCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::io;

/// The envelope shape. Every frame carries it; it changes only if `{v,id,type,
/// body}` itself changes, which is not the same event as a payload change.
pub const WIRE_VERSION: u32 = 1;

/// THE skew key: bumped whenever a daemon and a CLI of different builds can no
/// longer safely exchange payloads.
pub const PROTOCOL_VERSION: u32 = 1;

/// Test-only override of *this process's* protocol version — the seam both
/// branches of the skew policy are exercised through. Read by both ends (a
/// daemon autostarted by a skewed client inherits it, which is exactly how the
/// respawn branch is meant to converge).
pub const PROTO_VERSION_ENV: &str = "A2A_TEST_PROTO_VERSION";

/// The binary version each end announces. Never compared — it is what the skew
/// error NAMES, so an operator learns which two builds disagree.
pub const BINARY_VERSION: &str = env!("CARGO_PKG_VERSION");

/// 1 MiB. Generous for a `send` (the message travels in the frame) and far
/// below anything that would matter to the daemon's footprint.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// The id every hello carries: a hello answers no request.
pub const HELLO_ID: u64 = 0;

/// The last frame a subscription ever gets: the broker is going down.
///
/// A tail that ends in a bare EOF is indistinguishable from a broker that
/// crashed, from a socket that was unlinked underneath it, and from a worker
/// that simply stopped saying anything — three incidents with three different
/// remedies. One frame, on the subscription's own id, removes the ambiguity.
pub const EVENT_BROKER_SHUTDOWN: &str = "broker_shutdown";

// -- error codes ------------------------------------------------------------

pub const ERR_VERSION_SKEW: &str = "BROKER_VERSION_SKEW";
pub const ERR_FRAME_TOO_LARGE: &str = "FRAME_TOO_LARGE";
pub const ERR_BAD_FRAME: &str = "BAD_FRAME";
pub const ERR_LAGGED: &str = "LAGGED";
pub const ERR_UNKNOWN_OP: &str = "UNKNOWN_OP";
pub const ERR_ALREADY_SUBSCRIBED: &str = "ALREADY_SUBSCRIBED";
pub const ERR_PEER_DENIED: &str = "PEER_DENIED";
pub const ERR_DRAIN_ONLY: &str = "DRAIN_ONLY";
pub const ERR_BROKER_UNAVAILABLE: &str = "BROKER_UNAVAILABLE";

/// Codes that survive the wire round trip as `&'static str`.
///
/// [`AppError::code`] is `&'static str` (it is a *taxonomy*, not free text), so
/// a code arriving over the socket has to be interned. Everything the broker
/// can emit is listed here; anything else — a code from a NEWER daemon — is
/// leaked once, which is bounded by the number of distinct unknown codes a
/// short-lived CLI process can meet, and is strictly better than discarding the
/// one field an orchestrator branches on.
pub const KNOWN_CODES: [&str; 76] = [
    ERR_VERSION_SKEW,
    ERR_FRAME_TOO_LARGE,
    ERR_BAD_FRAME,
    ERR_LAGGED,
    ERR_UNKNOWN_OP,
    ERR_ALREADY_SUBSCRIBED,
    ERR_PEER_DENIED,
    ERR_DRAIN_ONLY,
    ERR_BROKER_UNAVAILABLE,
    "NO_SUCH_WORKER",
    // The two refusals a spawn can meet on the far side of the socket: the
    // broker's own allowlist re-check, and the codex drift gate (spec §4.2/§5).
    "CWD_NOT_ALLOWED",
    "CODEX_DRIFT",
    // ... and the two admission refusals (spec §4.9): the queue is full, or the
    // caller said `--no-queue` and the broker is at its cap.
    "QUEUE_FULL",
    "AT_CAPACITY",
    // ... and the third, which is not about room at all: the fleet has failed
    // K times in a window and the door is shut until somebody re-opens it
    // (spec §4.9). `ATTEMPTS_EXHAUSTED`/`PARENT_ACTIVE` are deliberately NOT
    // here — `respawn` decides both CLI-side and neither crosses this socket.
    "BREAKER_OPEN",
    "BROKER_GONE",
    "WORK_REPO_UNREACHABLE",
    "WORK_DAEMON_DOWN",
    "WORK_RECONCILE_FAILED",
    "WORK_PUSH_FAILED",
    "APP_SERVER",
    "APP_SERVER_START",
    "SNAPSHOT_UNREADABLE",
    "SESSION_UNKNOWN",
    "WORK_SIGNATURE_INVALID",
    "WORK_SENDER_UNKNOWN",
    "WORK_TOKEN_MISMATCH",
    "WORK_EXPIRED",
    "WORK_TOO_LARGE",
    "WORK_BAD_REPO_URL",
    "WORK_WRONG_FLEET",
    "WORK_NOT_FOUND",
    "WORK_SCHEMA_MISSING",
    "REGISTRY_SCHEMA_STALE",
    "WORK_CONTROL_CHARS",
    "WORK_PEER_TOO_OLD",
    "WORK_BRIEF_INVALID",
    "WORK_STATE_CONFLICT",
    "WORK_RESULT_NOT_LINEAR",
    "WORK_BINARY_IN_RESULT",
    "WORK_ATTRS_IN_RESULT",
    "WORK_HEAD_MOVED",
    "WORK_BAD_BASE_REF",
    "HANDSHAKE_BAD_NONCE",
    "CONFIG",
    "IO",
    "INTERNAL",
    "WORK_PEER_ROW_INVALID",
    "ROOTS_UNREADABLE",
    "ENROLLMENT_REBIND_REQUIRED",
    "REVOCATION_INVALID",
    "WORK_SENDER_ROW_INVALID",
    "WORK_SECRET_IN_RESULT",
    "WORK_SCAN_OVERSIZE",
    "WORK_HELD",
    "WORK_ID_INVALID",
    "WORK_TERMINAL_OBSERVED",
    "WORK_NOT_HELD",
    "WORK_ALLOW_SECRETS_REQUIRED",
    "WORK_ALLOW_SECRETS_STALE",
    "ROOT_ROTATION_STAGED",
    "ROOT_ROTATION_ABORT_REFUSED",
    "ROOT_NEXT_MISSING",
    "ROOT_PREV_MISSING",
    "ROOT_NEXT_FLEET_MISMATCH",
    "ROOT_ANNOUNCEMENT_INVALID",
    "ROOT_ANNOUNCEMENT_FLEET_MISMATCH",
    "ROTATION_PHASE_UNMET",
    "ROTATION_SOAK_UNMET",
    "ROTATION_STATUS_UNAVAILABLE",
    "REGISTRY_SET_REFUSED",
    "RATE_LIMITED",
    "SESSION_LIMIT",
    "PREAUTH_LIMIT",
    "WORK_POOL_FULL",
    "FEED_ORIGIN_UNBOUND",
];

/// This process's protocol version.
pub fn protocol_version() -> u32 {
    std::env::var(PROTO_VERSION_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(PROTOCOL_VERSION)
}

// ---------------------------------------------------------------------------
// the frame
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum FrameType {
    Hello,
    Req,
    Resp,
    Event,
    Err,
    /// A type this build has never heard of. Kept as a value rather than a
    /// parse failure so a newer peer's frame can be *reported* instead of
    /// crashing the reader.
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Frame {
    pub v: u32,
    /// Correlates a `resp`/`err` with its `req`, and every `event` with the
    /// subscription that asked for it. `0` on a hello.
    pub id: u64,
    #[serde(rename = "type")]
    pub ty: FrameType,
    pub body: Value,
}

impl Frame {
    pub fn new(id: u64, ty: FrameType, body: Value) -> Frame {
        Frame {
            v: WIRE_VERSION,
            id,
            ty,
            body,
        }
    }

    /// This end's hello — identical in both directions.
    pub fn hello() -> Frame {
        Frame::new(
            HELLO_ID,
            FrameType::Hello,
            json!({"protocol_version": protocol_version(),
                   "binary_version": BINARY_VERSION}),
        )
    }

    pub fn req(id: u64, op: &str, params: Value) -> Frame {
        let mut body = json!({"op": op});
        if let (Some(target), Some(extra)) = (body.as_object_mut(), params.as_object()) {
            for (k, v) in extra {
                target.insert(k.clone(), v.clone());
            }
        }
        Frame::new(id, FrameType::Req, body)
    }

    pub fn resp(id: u64, body: Value) -> Frame {
        Frame::new(id, FrameType::Resp, body)
    }

    pub fn event(id: u64, body: Value) -> Frame {
        Frame::new(id, FrameType::Event, body)
    }

    /// The farewell every live subscription gets, on its own id.
    pub fn broker_shutdown(id: u64, grace_secs: u64) -> Frame {
        Frame::event(
            id,
            json!({"event": EVENT_BROKER_SHUTDOWN, "grace_secs": grace_secs}),
        )
    }

    pub fn err(id: u64, err: WireError) -> Frame {
        Frame::new(id, FrameType::Err, err.to_value())
    }

    /// The `op` of a request frame.
    pub fn op(&self) -> Option<&str> {
        self.body.get("op").and_then(Value::as_str)
    }

    /// One line, newline-terminated: the whole framing.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(self).unwrap_or_else(|_| b"{}".to_vec());
        bytes.push(b'\n');
        bytes
    }
}

/// The hello body, both ways.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Hello {
    pub protocol_version: u32,
    #[serde(default)]
    pub binary_version: String,
}

impl Hello {
    pub fn here() -> Hello {
        Hello {
            protocol_version: protocol_version(),
            binary_version: BINARY_VERSION.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

/// An [`AppError`] that survives a socket: same fields, plus whatever structured
/// detail the code needs (`live_workers` on a skew, `last_seq` on a lag).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WireError {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub remedy: Option<String>,
    pub exit: i32,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl WireError {
    pub fn new(code: &str, exit: ExitCode, message: impl Into<String>) -> WireError {
        WireError {
            code: code.to_string(),
            message: message.into(),
            retryable: false,
            remedy: None,
            exit: exit as i32,
            extra: Map::new(),
        }
    }

    pub fn remedy(mut self, remedy: impl Into<String>) -> WireError {
        self.remedy = Some(remedy.into());
        self
    }

    /// Attach one structured field. The client branches on these, so they are
    /// data, not prose in `message`.
    pub fn with(mut self, key: &str, value: Value) -> WireError {
        self.extra.insert(key.to_string(), value);
        self
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.extra.get(key)
    }

    /// The structured half of an error, under one key.
    ///
    /// Nested rather than flattened alongside `live_workers`/`last_seq`: those
    /// are the WIRE's own fields, minted here and read here, while `data` is the
    /// CLI's — it travels through unchanged and comes out of `a2a --json` as
    /// `error.data`. Keeping the two namespaces apart is what stops a future
    /// wire field from silently renaming something an orchestrator branches on.
    pub const DATA_KEY: &'static str = "data";

    pub fn from_app(err: &AppError) -> WireError {
        let mut extra = Map::new();
        if let Some(data) = err.data.clone() {
            extra.insert(WireError::DATA_KEY.to_string(), data);
        }
        WireError {
            code: err.code.to_string(),
            message: err.message.clone(),
            retryable: err.retryable,
            remedy: err.remedy.clone(),
            exit: err.exit as i32,
            extra,
        }
    }

    pub fn to_app(&self) -> AppError {
        let mut err = AppError::new(
            static_code(&self.code),
            exit_from_i32(self.exit),
            self.message.clone(),
        );
        err.retryable = self.retryable;
        err.remedy = self.remedy.clone();
        err.data = self.get(WireError::DATA_KEY).cloned();
        err
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| json!({"code": self.code}))
    }

    pub fn from_value(value: &Value) -> Option<WireError> {
        serde_json::from_value(value.clone()).ok()
    }
}

/// Intern a wire code as `&'static str` — see [`KNOWN_CODES`].
pub fn static_code(code: &str) -> &'static str {
    for known in KNOWN_CODES {
        if known == code {
            return known;
        }
    }
    Box::leak(code.to_string().into_boxed_str())
}

/// The inverse of `ExitCode as i32`, total by construction: a code this build
/// does not know maps to `Internal` rather than to a panic.
pub fn exit_from_i32(n: i32) -> ExitCode {
    match n {
        0 => ExitCode::Success,
        2 => ExitCode::Usage,
        3 => ExitCode::NoSuchWorker,
        4 => ExitCode::WorkerFailed,
        5 => ExitCode::Blocked,
        6 => ExitCode::NotTerminal,
        7 => ExitCode::BrokerUnavailable,
        8 => ExitCode::CmdTimeout,
        9 => ExitCode::Precondition,
        10 => ExitCode::Orphaned,
        _ => ExitCode::Internal,
    }
}

// ---------------------------------------------------------------------------
// the codec
// ---------------------------------------------------------------------------

/// What one read produced.
#[derive(Debug)]
pub enum Incoming {
    Frame(Box<Frame>),
    /// The line ran past [`MAX_FRAME_BYTES`]. It was drained (so the stream is
    /// still framed) but never accumulated.
    TooLarge,
    /// A line that is not a frame. Reported rather than skipped: silently
    /// ignoring garbage is how a protocol bug becomes a hang.
    Malformed(String),
    Eof,
}

/// Accumulates one line, refusing to grow past the cap.
struct LineAcc {
    buf: Vec<u8>,
    overflow: bool,
}

impl LineAcc {
    fn new() -> LineAcc {
        LineAcc {
            buf: Vec::new(),
            overflow: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        if self.overflow || self.buf.len() + bytes.len() > MAX_FRAME_BYTES {
            self.overflow = true;
            return;
        }
        self.buf.extend_from_slice(bytes);
    }

    fn finish(self) -> Incoming {
        if self.overflow {
            return Incoming::TooLarge;
        }
        decode(&self.buf)
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty() && !self.overflow
    }
}

fn decode(line: &[u8]) -> Incoming {
    let text = String::from_utf8_lossy(line);
    if text.trim().is_empty() {
        return Incoming::Malformed(String::new());
    }
    match serde_json::from_str::<Frame>(&text) {
        Ok(frame) => Incoming::Frame(Box::new(frame)),
        Err(e) => Incoming::Malformed(e.to_string()),
    }
}

/// Read one frame from an async stream.
///
/// Never called inside a `select!` arm that could resume it: cancellation here
/// drops a partial line, which is fine only because every cancellation of this
/// reader is terminal (the connection is going away).
pub async fn read_frame<R>(reader: &mut R) -> io::Result<Incoming>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;
    let mut acc = LineAcc::new();
    loop {
        let (done, consumed) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return Ok(if acc.is_empty() {
                    Incoming::Eof
                } else {
                    acc.finish()
                });
            }
            match available.iter().position(|b| *b == b'\n') {
                Some(i) => {
                    acc.push(&available[..i]);
                    (true, i + 1)
                }
                None => {
                    let n = available.len();
                    acc.push(available);
                    (false, n)
                }
            }
        };
        reader.consume(consumed);
        if done {
            return Ok(acc.finish());
        }
    }
}

/// Read one frame from a blocking stream — the CLI's half, which is
/// synchronous on purpose (the daemon is the only async process).
pub fn read_frame_sync<R: io::BufRead>(reader: &mut R) -> io::Result<Incoming> {
    let mut acc = LineAcc::new();
    loop {
        let (done, consumed) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                return Ok(if acc.is_empty() {
                    Incoming::Eof
                } else {
                    acc.finish()
                });
            }
            match available.iter().position(|b| *b == b'\n') {
                Some(i) => {
                    acc.push(&available[..i]);
                    (true, i + 1)
                }
                None => {
                    let n = available.len();
                    acc.push(available);
                    (false, n)
                }
            }
        };
        reader.consume(consumed);
        if done {
            return Ok(acc.finish());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_round_trips_through_exactly_one_line() {
        let frame = Frame::req(7, "status", json!({"id": "impl-0817t1200-aaaa"}));
        let bytes = frame.encode();
        assert_eq!(bytes.iter().filter(|b| **b == b'\n').count(), 1);
        assert_eq!(*bytes.last().unwrap(), b'\n');

        let value: Value = serde_json::from_slice(&bytes[..bytes.len() - 1]).unwrap();
        assert_eq!(value["v"], json!(WIRE_VERSION));
        assert_eq!(value["type"], json!("req"));
        assert_eq!(value["body"]["op"], json!("status"));
        assert_eq!(value["body"]["id"], json!("impl-0817t1200-aaaa"));
    }

    #[test]
    fn the_hello_body_is_the_one_the_skew_check_reads() {
        let hello = Frame::hello();
        assert_eq!(hello.ty, FrameType::Hello);
        assert_eq!(hello.id, HELLO_ID);
        let parsed: Hello = serde_json::from_value(hello.body).unwrap();
        assert_eq!(parsed.protocol_version, protocol_version());
        assert_eq!(parsed.binary_version, BINARY_VERSION);
    }

    #[test]
    fn a_line_past_the_cap_is_too_large_and_the_stream_stays_framed() {
        let mut line = vec![b'x'; MAX_FRAME_BYTES + 10];
        line.push(b'\n');
        line.extend_from_slice(&Frame::resp(1, json!({"ok": true})).encode());
        let mut reader = io::BufReader::with_capacity(64, io::Cursor::new(line));

        assert!(matches!(
            read_frame_sync(&mut reader).unwrap(),
            Incoming::TooLarge
        ));
        // The oversized line was DRAINED, not left in the buffer: the next
        // frame is still readable, which is what makes the error typed rather
        // than the start of a desync.
        match read_frame_sync(&mut reader).unwrap() {
            Incoming::Frame(f) => assert_eq!(f.id, 1),
            other => panic!("expected the following frame, got {other:?}"),
        }
    }

    #[test]
    fn garbage_is_reported_not_skipped() {
        let mut reader = io::BufReader::new(io::Cursor::new(b"not json\n".to_vec()));
        assert!(matches!(
            read_frame_sync(&mut reader).unwrap(),
            Incoming::Malformed(_)
        ));
    }

    #[test]
    fn an_unknown_frame_type_parses_rather_than_killing_the_reader() {
        let mut reader = io::BufReader::new(io::Cursor::new(
            br#"{"v":1,"id":3,"type":"telepathy","body":{}}"#.to_vec(),
        ));
        match read_frame_sync(&mut reader).unwrap() {
            Incoming::Frame(f) => assert_eq!(f.ty, FrameType::Unknown),
            other => panic!("expected a frame, got {other:?}"),
        }
    }

    #[test]
    fn an_error_survives_the_round_trip_with_its_exit_code_and_extras() {
        let wire = WireError::new(ERR_VERSION_SKEW, ExitCode::BrokerUnavailable, "mismatch")
            .remedy("`a2a broker restart`")
            .with("live_workers", json!(3));
        let back = WireError::from_value(&wire.to_value()).unwrap();
        assert_eq!(back.get("live_workers"), Some(&json!(3)));

        let app = back.to_app();
        assert_eq!(app.code, ERR_VERSION_SKEW);
        assert_eq!(app.exit, ExitCode::BrokerUnavailable);
        assert_eq!(app.remedy.as_deref(), Some("`a2a broker restart`"));
    }

    /// `error.data` is the CLI's field, not the wire's: it goes over the socket
    /// untouched and comes back out of `to_app` as the same value, because an
    /// orchestrator meeting a full admission queue branches on
    /// `retry_after_secs` rather than on the sentence around it.
    #[test]
    fn structured_error_data_crosses_the_socket_unchanged() {
        let app = AppError::new("QUEUE_FULL", ExitCode::Precondition, "the queue is full")
            .data(json!({"retry_after_secs": 30, "queue_cap": 32}));
        let back = WireError::from_value(&WireError::from_app(&app).to_value())
            .unwrap()
            .to_app();
        assert_eq!(back.code, "QUEUE_FULL");
        assert_eq!(back.exit, ExitCode::Precondition);
        assert_eq!(
            back.data,
            Some(json!({"retry_after_secs": 30, "queue_cap": 32}))
        );
        // An error with nothing structured to say carries no key at all.
        let plain = AppError::new("NO_SUCH_WORKER", ExitCode::NoSuchWorker, "who?");
        assert_eq!(WireError::from_app(&plain).get(WireError::DATA_KEY), None);
        assert_eq!(WireError::from_app(&plain).to_app().data, None);
    }

    #[test]
    fn every_exit_code_survives_the_round_trip() {
        for code in [
            ExitCode::Success,
            ExitCode::Internal,
            ExitCode::Usage,
            ExitCode::NoSuchWorker,
            ExitCode::WorkerFailed,
            ExitCode::Blocked,
            ExitCode::NotTerminal,
            ExitCode::BrokerUnavailable,
            ExitCode::CmdTimeout,
            ExitCode::Precondition,
            ExitCode::Orphaned,
        ] {
            assert_eq!(exit_from_i32(code as i32), code);
        }
    }
}
