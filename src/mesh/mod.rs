use crate::home::{create_dir_0700, A2aHome};
use crate::output::{AppError, ExitCode};
use crate::redact;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

pub const NOTE_MAX_BYTES: usize = 1_048_576;
pub const POST_MAX_BYTES: usize = 262_144;
pub const STATUS_MAX_BYTES: usize = 4_096;
pub const INJECT_BUDGET_BYTES: usize = 8_192;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provider {
    Claude,
    Codex,
}

impl Provider {
    fn as_str(self) -> &'static str {
        match self {
            Provider::Claude => "claude",
            Provider::Codex => "codex",
        }
    }
}

impl fmt::Display for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerId {
    pub provider: Provider,
    pub uuid: String,
}

impl PeerId {
    pub fn parse(s: &str) -> Result<Self, AppError> {
        let Some((provider, uuid)) = s.split_once(':') else {
            return Err(invalid_peer_id(
                "expected peer id `<provider>:<uuid>` with provider claude|codex",
            ));
        };
        if uuid.is_empty() || uuid.contains(':') || uuid.contains('/') {
            return Err(invalid_peer_id(
                "expected peer id `<provider>:<uuid>` with a non-empty uuid",
            ));
        }
        let provider = match provider {
            "claude" => Provider::Claude,
            "codex" => Provider::Codex,
            _ => return Err(invalid_peer_id("unknown provider (expected claude|codex)")),
        };
        Ok(Self {
            provider,
            uuid: uuid.to_string(),
        })
    }

    pub fn dir_name(&self) -> String {
        format!("{}-{}", self.provider, self.uuid)
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.provider, self.uuid)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Presence {
    pub peer: String,
    pub provider: String,
    pub worker_id: Option<String>,
    pub status: Option<String>,
    pub working_on: Option<String>,
    pub cwd: Option<String>,
    pub registered_at: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub retired: bool,
    pub cursors: Cursors,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursors {
    pub feed_seq: u64,
    pub inbox: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Note {
    pub from: String,
    pub to: String,
    pub ts: DateTime<Utc>,
    pub kind: String,
    pub text: String,
    pub refs: serde_json::Value,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FeedEntry {
    pub seq: u64,
    pub origin: Option<String>,
    pub origin_seq: Option<u64>,
    pub from: String,
    pub ts: DateTime<Utc>,
    pub text: String,
    pub refs: serde_json::Value,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

pub fn mesh_root(home: &A2aHome) -> PathBuf {
    let root = home.root().join("mesh");
    create_dir_0700(&root).expect("creating mesh root");
    root
}

pub fn peer_dir(home: &A2aHome, id: &PeerId) -> PathBuf {
    mesh_root(home).join("peers").join(id.dir_name())
}

pub fn scan_or_refuse(text: &str, code: &'static str) -> Result<(), AppError> {
    if let Some(cap) = cap_for(code) {
        let len = text.len();
        if len > cap {
            return Err(AppError::new(
                code,
                ExitCode::Precondition,
                format!("text is {len} bytes, exceeding cap {cap} bytes"),
            ));
        }
    }

    let hits = redact::scan_text(text);
    if let Some(hit) = hits.first() {
        return Err(AppError::new(
            code,
            ExitCode::Precondition,
            format!("text refused by secret scan: line {} ({})", hit.line, hit.rule),
        ));
    }
    Ok(())
}

pub mod keys;

fn cap_for(code: &str) -> Option<usize> {
    match code {
        "NOTE_TOO_LARGE" => Some(NOTE_MAX_BYTES),
        "POST_TOO_LARGE" => Some(POST_MAX_BYTES),
        "STATUS_TOO_LARGE" => Some(STATUS_MAX_BYTES),
        _ => None,
    }
}

fn invalid_peer_id(message: impl Into<String>) -> AppError {
    AppError::new("PEER_ID_INVALID", ExitCode::Precondition, message)
}

pub mod presence;
pub mod feed;
pub mod notes;
pub mod routing;
pub mod registry;
pub mod wire;
pub mod gateway;
