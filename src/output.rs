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
}

impl AppError {
    pub fn new(code: &'static str, exit: ExitCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            remedy: None,
            exit,
        }
    }
    pub fn remedy(mut self, r: impl Into<String>) -> Self {
        self.remedy = Some(r.into());
        self
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
            "error":{"code":err.code,"message":err.message,"retryable":err.retryable,"remedy":err.remedy}})
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
