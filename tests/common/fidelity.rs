// tests/common/fidelity.rs
//
// The **fidelity contract** for `tests/fixtures/fake-app-server` (spec §8.3):
// every frame the fake emits and every request the client sends is validated
// against the checked-in schemas in `schema/v2-snapshot/` in the same test run,
// so a drifted fake is a test failure rather than a silent pass.
//
// Included per-test rather than from `common/mod.rs` so that only the test
// crates that actually need schema validation link `jsonschema`:
//
//     #[path = "common/fidelity.rs"]
//     mod fidelity;
#![allow(dead_code)]

use jsonschema::Validator;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Notification method → the snapshot schema for its `params`.
///
/// Only the four lifecycle notifications are snapshotted (Task 1's subset), so
/// only they can be validated. Anything else the fake emits — the deliberately
/// unknown method of the `bad-vocabulary` scenario, for instance — is reported
/// as unvalidated instead of silently passing.
pub const NOTIFICATION_SCHEMAS: [(&str, &str); 4] = [
    ("item/completed", "v2/ItemCompletedNotification.json"),
    ("thread/started", "v2/ThreadStartedNotification.json"),
    ("turn/completed", "v2/TurnCompletedNotification.json"),
    ("turn/started", "v2/TurnStartedNotification.json"),
];

/// Client→server request method → the snapshot schema for its `params`.
pub const REQUEST_PARAM_SCHEMAS: [(&str, &str); 6] = [
    ("initialize", "v1/InitializeParams.json"),
    ("thread/resume", "v2/ThreadResumeParams.json"),
    ("thread/start", "v2/ThreadStartParams.json"),
    ("turn/interrupt", "v2/TurnInterruptParams.json"),
    ("turn/start", "v2/TurnStartParams.json"),
    ("turn/steer", "v2/TurnSteerParams.json"),
];

pub fn snapshot_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("schema/v2-snapshot")
}

pub fn notification_schema(method: &str) -> Option<&'static str> {
    NOTIFICATION_SCHEMAS
        .iter()
        .find(|(m, _)| *m == method)
        .map(|(_, s)| *s)
}

pub fn request_params_schema(method: &str) -> Option<&'static str> {
    REQUEST_PARAM_SCHEMAS
        .iter()
        .find(|(m, _)| *m == method)
        .map(|(_, s)| *s)
}

/// Compiles a snapshot schema. Panics with the offending path — a missing or
/// malformed snapshot is a gate failure, not something to skip past.
pub fn validator(rel: &str) -> Validator {
    let path = snapshot_dir().join(rel);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("snapshot schema {} unreadable: {e}", path.display()));
    let schema: Value = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("snapshot schema {} is not JSON: {e}", path.display()));
    jsonschema::validator_for(&schema)
        .unwrap_or_else(|e| panic!("snapshot schema {} does not compile: {e}", path.display()))
}

/// Validates one instance, reporting every violation at once.
pub fn assert_valid(instance: &Value, rel: &str, context: &str) {
    let v = validator(rel);
    let errors: Vec<String> = v
        .iter_errors(instance)
        .map(|e| format!("  at {}: {e}", e.instance_path()))
        .collect();
    assert!(
        errors.is_empty(),
        "{context} does not match {rel}:\n{}\ninstance: {}",
        errors.join("\n"),
        serde_json::to_string(instance).unwrap()
    );
}

/// What a transcript audit found. Construction itself panics on any violation;
/// the sets exist so a test can additionally assert *coverage* (the fake really
/// did emit the frames the scenario promises).
#[derive(Debug, Default)]
pub struct Audit {
    /// Notification methods the fake emitted, validated against a snapshot.
    pub notifications: BTreeSet<String>,
    /// Request methods the client sent, validated against a Params snapshot.
    pub requests: BTreeSet<String>,
    /// Methods seen on the wire that no snapshot covers (reported, not failed).
    pub unvalidated: BTreeSet<String>,
}

impl Audit {
    pub fn assert_covers(&self, notifications: &[&str], requests: &[&str]) {
        for m in notifications {
            assert!(
                self.notifications.contains(*m),
                "no schema-validated {m} notification in transcript (saw {:?})",
                self.notifications
            );
        }
        for m in requests {
            assert!(
                self.requests.contains(*m),
                "no schema-validated {m} request in transcript (saw {:?})",
                self.requests
            );
        }
    }
}

/// Replays a `$FAKE_TRANSCRIPT` and validates it against the snapshot schemas.
///
/// Transcript lines are `{"dir":"in"|"out","frame":{…}}`, or `{"raw":"…"}` for
/// a line that was not JSON. `out` frames with a `method` and no `id` are the
/// fake's notifications; `in` frames with both are the client's requests. The
/// fake's own server→client requests and the client's answers to them carry no
/// snapshot Params schema and are counted as unvalidated.
pub fn audit(transcript: &Path) -> Audit {
    let raw = std::fs::read_to_string(transcript).unwrap_or_else(|e| {
        panic!(
            "fake transcript {} unreadable: {e} — did the fixture run with FAKE_TRANSCRIPT set?",
            transcript.display()
        )
    });
    let mut audit = Audit::default();
    for (n, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let rec: Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("transcript line {} is not JSON: {e}", n + 1));
        let dir = rec["dir"].as_str().unwrap_or("");
        let frame = match rec.get("frame") {
            Some(f) => f,
            None => continue, // a raw, deliberately unparseable line
        };
        let method = match frame.get("method").and_then(|m| m.as_str()) {
            Some(m) => m.to_string(),
            None => continue, // a response frame
        };
        let has_id = frame.get("id").is_some();
        let params = frame.get("params").cloned().unwrap_or(Value::Null);
        match (dir, has_id) {
            ("out", false) => match notification_schema(&method) {
                Some(rel) => {
                    assert_valid(&params, rel, &format!("fake notification {method}"));
                    audit.notifications.insert(method);
                }
                None => {
                    audit.unvalidated.insert(method);
                }
            },
            ("in", true) => match request_params_schema(&method) {
                Some(rel) => {
                    assert_valid(&params, rel, &format!("client request {method}"));
                    audit.requests.insert(method);
                }
                None => {
                    audit.unvalidated.insert(method);
                }
            },
            _ => {
                audit.unvalidated.insert(method);
            }
        }
    }
    audit
}
