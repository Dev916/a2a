use serde_json::{json, Value};

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(i32)]
pub enum ExitCode {
    Success = 0,
    Internal = 1,
    Usage = 2,
    NoSuchWorker = 3,
    WorkerFailed = 4,
    Blocked = 5,
    NotTerminal = 6,
    BrokerUnavailable = 7,
    CmdTimeout = 8,
    Precondition = 9,
    Orphaned = 10,
}

#[derive(Debug)]
pub struct AppError {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
    pub remedy: Option<String>,
    pub exit: ExitCode,
    /// Structured detail a caller BRANCHES on, as opposed to prose it prints.
    ///
    /// `QUEUE_FULL` carries `{"retry_after_secs": …}` here: an orchestrator that
    /// meets a full admission queue has to decide when to try again, and parsing
    /// that number out of a human sentence is the kind of thing that works until
    /// the sentence is improved. Serialized as `error.data` in the envelope, and
    /// carried across the broker socket by [`crate::broker::wire::WireError`].
    pub data: Option<Value>,
}

impl AppError {
    pub fn new(code: &'static str, exit: ExitCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            remedy: None,
            exit,
            data: None,
        }
    }
    pub fn remedy(mut self, r: impl Into<String>) -> Self {
        self.remedy = Some(r.into());
        self
    }
    /// Attach the machine-readable half of this error.
    pub fn data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
}

/// A successful command's machine payload plus the exit code it maps to.
///
/// Success and exit-zero are NOT the same thing: `a2a status` on a failed
/// worker succeeded (it answered the question) and must still exit 4 with a
/// full `data` payload, not an error envelope. Commands that only ever exit 0
/// convert from `Value` and never mention this type.
#[derive(Debug)]
pub struct Outcome {
    pub data: Value,
    pub exit: ExitCode,
}

impl Outcome {
    pub fn with(data: Value, exit: ExitCode) -> Self {
        Self { data, exit }
    }
}

impl From<Value> for Outcome {
    fn from(data: Value) -> Self {
        Self {
            data,
            exit: ExitCode::Success,
        }
    }
}

/// stdout = machine payload only; stderr = diagnostics/decoration.
pub fn emit_ok(json_mode: bool, command: &str, data: Value) {
    if json_mode {
        println!(
            "{}",
            json!({"schema_version":1,"ok":true,"command":command,"data":data})
        );
    } else if let Some(s) = data.as_str() {
        println!("{s}");
    } else if !data.is_null() {
        println!("{}", serde_json::to_string_pretty(&data).unwrap());
    }
}

pub fn emit_err(json_mode: bool, command: &str, err: &AppError) -> i32 {
    if json_mode {
        println!(
            "{}",
            json!({"schema_version":1,"ok":false,"command":command,
            "error":{"code":err.code,"message":err.message,"retryable":err.retryable,
                     "remedy":err.remedy,"data":err.data}})
        );
    } else {
        eprintln!(
            "a2a: {}: {}{}",
            err.code,
            err.message,
            err.remedy
                .as_deref()
                .map(|r| format!(" (remedy: {r})"))
                .unwrap_or_default()
        );
    }
    err.exit as i32
}
