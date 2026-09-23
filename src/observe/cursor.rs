//! Opaque replay cursors built from source-native watermarks.

use crate::mesh::keys::{b64_decode, b64_encode};
use crate::output::{AppError, ExitCode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A resumable position across every source participating in an observation.
///
/// A single scalar cannot survive independent log rotation and compaction, so
/// each source advances its own watermark. `gap` tells a consumer that replay
/// resumed at the oldest retained record rather than the exact requested one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Cursor {
    #[serde(default)]
    pub watermarks: BTreeMap<String, Watermark>,
    #[serde(default)]
    pub gap: bool,
}

/// The two source position styles used by the observability plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Watermark {
    Sequence { seq: u64 },
    Line { generation: u64, position: u64 },
}

impl Cursor {
    /// Record a source-native numeric high-water mark.
    pub fn set(&mut self, source: impl Into<String>, seq: u64) {
        self.watermarks
            .insert(source.into(), Watermark::Sequence { seq });
    }

    /// Record a line position whose generation changes when the source rotates.
    pub fn set_line(&mut self, source: impl Into<String>, generation: u64, position: u64) {
        self.watermarks.insert(
            source.into(),
            Watermark::Line {
                generation,
                position,
            },
        );
    }

    /// Serialize this cursor as URL-safe, unpadded base64 over compact JSON.
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self)
            .expect("Cursor serialization is infallible for string and integer fields");
        b64_encode(&json)
            .trim_end_matches('=')
            .replace('+', "-")
            .replace('/', "_")
    }

    /// Decode an opaque cursor token without exposing parser panics to callers.
    pub fn decode(token: &str) -> Result<Self, AppError> {
        let json = decode_base64url(token).map_err(|()| {
            AppError::new(
                "CURSOR_INVALID",
                ExitCode::Precondition,
                "cursor is not valid base64url",
            )
        })?;
        serde_json::from_slice(&json).map_err(|error| {
            AppError::new(
                "CURSOR_INVALID",
                ExitCode::Precondition,
                format!("cursor JSON is invalid: {error}"),
            )
        })
    }
}

fn decode_base64url(token: &str) -> Result<Vec<u8>, ()> {
    if token
        .bytes()
        .any(|byte| !matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_'))
    {
        return Err(());
    }

    let mut standard = token.replace('-', "+").replace('_', "/");
    match standard.len() % 4 {
        0 => {}
        2 => standard.push_str("=="),
        3 => standard.push('='),
        _ => return Err(()),
    }
    b64_decode(&standard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_encodes_per_source_watermarks_and_round_trips() {
        let mut c = Cursor::default();
        c.set("feed", 42);
        c.set_line("broker.jsonl", 1, 900);

        let token = c.encode();
        assert!(token.bytes().all(|byte| byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_')));
        let back = Cursor::decode(&token).unwrap();
        assert_eq!(back, c);
        assert!(Cursor::decode("!!not-base64!!").is_err());
        assert!(Cursor::decode("bm90LWpzb24").is_err());
    }
}
