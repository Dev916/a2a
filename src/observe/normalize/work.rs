//! Normalization for distributed work-item transitions.

use crate::observe::cursor::Watermark;
use crate::observe::source::{read_json, read_jsonl, read_jsonl_records_from};
use crate::observe::{Cursor, Event, Plane, Src, Subject};
use chrono::{SecondsFormat, Utc};
use serde_json::Value;
use std::path::Path;

const BROKER_ARTIFACT: &str = "broker.jsonl";
const MESH_EVENTS_ARTIFACT: &str = "mesh/events.jsonl";

/// Normalize work transitions newer than the cursor's source-specific watermarks.
pub fn normalize(home: &Path, cursor: &Cursor) -> Vec<Event> {
    let agent_id = read_agent_id(home);
    let mut events = Vec::new();

    normalize_mesh_events(home, cursor, &agent_id, &mut events);
    normalize_broker(home, cursor, &agent_id, &mut events);

    events.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.src.artifact.cmp(&right.src.artifact))
            .then_with(|| left.src.reference.cmp(&right.src.reference))
    });
    events
}

fn normalize_mesh_events(home: &Path, cursor: &Cursor, agent_id: &str, events: &mut Vec<Event>) {
    let watermark = sequence_watermark(cursor, MESH_EVENTS_ARTIFACT);
    for row in read_jsonl(&home.join(MESH_EVENTS_ARTIFACT)) {
        let Some(sequence) = row.get("seq").and_then(Value::as_u64) else {
            continue;
        };
        if sequence <= watermark {
            continue;
        }
        push_transition(
            events,
            agent_id,
            MESH_EVENTS_ARTIFACT,
            format!("seq={sequence}"),
            row,
        );
    }
}

fn normalize_broker(home: &Path, cursor: &Cursor, agent_id: &str, events: &mut Vec<Event>) {
    let offset = line_watermark(cursor, BROKER_ARTIFACT);
    let (rows, _advanced) = read_jsonl_records_from(&home.join(BROKER_ARTIFACT), offset);
    for (row, end_offset) in rows {
        push_transition(
            events,
            agent_id,
            BROKER_ARTIFACT,
            format!("offset={end_offset}"),
            row,
        );
    }
}

fn push_transition(
    events: &mut Vec<Event>,
    agent_id: &str,
    artifact: &str,
    reference: String,
    row: Value,
) {
    let Some(work_id) = work_id(&row).map(str::to_string) else {
        return;
    };
    let Some(kind) = work_kind(&row) else {
        return;
    };
    events.push(Event {
        v: 1,
        ts: source_timestamp(&row),
        agent_id: agent_id.to_string(),
        plane: Plane::Work,
        kind: kind.to_string(),
        subject: Subject {
            worker_id: None,
            work_id: Some(work_id),
            peer: None,
        },
        payload: row,
        src: Src {
            artifact: artifact.to_string(),
            reference,
        },
    });
}

fn work_kind(value: &Value) -> Option<&'static str> {
    if let Some(kind) = work_error_kind(value) {
        return Some(kind);
    }

    if is_release_transition(value) {
        return Some("work_released");
    }

    mapped_work_kind(
        value,
        &[
            "kind",
            "event",
            "verb",
            "action",
            "transition",
            "status",
            "result_status",
            "to",
            "to_status",
        ],
    )
}

fn map_work_token(value: &str) -> Option<&'static str> {
    let token = normalized_token(value);
    let token = token.strip_prefix("mesh_").unwrap_or(&token);
    let token = token.strip_prefix("work_").unwrap_or(token);
    match token {
        "offer" | "offered" | "sent" => Some("work_offered"),
        "approve" | "approved" => Some("work_approved"),
        "release" | "released" => Some("work_released"),
        "done" | "completed" => Some("work_done"),
        "fail" | "failed" => Some("work_failed"),
        "result_empty" => Some("work_result_empty"),
        _ => None,
    }
}

fn work_id(value: &Value) -> Option<&str> {
    string_for_keys(value, &["work_id", "workId"])
}

fn work_error_kind(value: &Value) -> Option<&'static str> {
    for key in ["code", "error_code", "reason", "detail", "result_summary"] {
        if let Some(candidate) = value.get(key).and_then(Value::as_str) {
            let token = normalized_token(candidate);
            if token.starts_with("work_result_empty") {
                return Some("work_result_empty");
            }
            if token.starts_with("work_reconcile_failed")
                || token.starts_with("work_push_failed")
            {
                return Some("work_failed");
            }
        }
    }
    for child in child_objects(value) {
        if let Some(kind) = work_error_kind(child) {
            return Some(kind);
        }
    }
    None
}

fn is_release_transition(value: &Value) -> bool {
    let from = value
        .get("from")
        .or_else(|| value.get("from_status"))
        .and_then(Value::as_str);
    let to = value
        .get("to")
        .or_else(|| value.get("to_status"))
        .and_then(Value::as_str);
    if from.is_some_and(|status| normalized_token(status) == "awaiting_release")
        && to.is_some_and(|status| normalized_token(status) == "running")
    {
        return true;
    }
    child_objects(value).into_iter().any(is_release_transition)
}

fn mapped_work_kind(value: &Value, keys: &[&str]) -> Option<&'static str> {
    for key in keys {
        if let Some(candidate) = value.get(*key).and_then(Value::as_str) {
            if let Some(kind) = map_work_token(candidate) {
                return Some(kind);
            }
        }
    }
    for child in child_objects(value) {
        if let Some(kind) = mapped_work_kind(child, keys) {
            return Some(kind);
        }
    }
    None
}

fn string_for_keys<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(found) = value.get(*key).and_then(Value::as_str) {
            return Some(found);
        }
    }
    for child in child_objects(value) {
        if let Some(found) = string_for_keys(child, keys) {
            return Some(found);
        }
    }
    None
}

fn child_objects(value: &Value) -> Vec<&Value> {
    ["raw", "payload", "data", "body", "error", "refs", "work"]
        .into_iter()
        .filter_map(|key| value.get(key))
        .collect()
}

fn normalized_token(value: &str) -> String {
    let mut token = String::with_capacity(value.len());
    let mut previous_was_lowercase = false;
    for character in value.trim().chars() {
        if character.is_ascii_alphanumeric() {
            if character.is_ascii_uppercase() && previous_was_lowercase {
                token.push('_');
            }
            token.push(character.to_ascii_lowercase());
            previous_was_lowercase = character.is_ascii_lowercase() || character.is_ascii_digit();
        } else if !token.ends_with('_') {
            token.push('_');
            previous_was_lowercase = false;
        }
    }
    token.trim_matches('_').to_string()
}

fn read_agent_id(home: &Path) -> String {
    read_json(&home.join("mesh/gateway/gw.key"))
        .and_then(|value| {
            value
                .get("gateway_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default()
}

fn source_timestamp(value: &Value) -> String {
    value
        .get("ts")
        .or_else(|| value.get("updated_at"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn sequence_watermark(cursor: &Cursor, artifact: &str) -> u64 {
    match cursor.watermarks.get(artifact) {
        Some(Watermark::Sequence { seq }) => *seq,
        _ => 0,
    }
}

fn line_watermark(cursor: &Cursor, artifact: &str) -> u64 {
    match cursor.watermarks.get(artifact) {
        Some(Watermark::Line { position, .. }) => *position,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::Plane;
    use serde_json::json;
    use std::fs;

    fn seed_identity(home: &Path) {
        let gateway = home.join("mesh/gateway");
        fs::create_dir_all(&gateway).unwrap();
        fs::write(
            gateway.join("gw.key"),
            br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
        )
        .unwrap();
    }

    #[test]
    fn offered_transition_is_normalized_with_work_subject() {
        let tmp = tempfile::tempdir().unwrap();
        seed_identity(tmp.path());
        fs::create_dir_all(tmp.path().join("mesh")).unwrap();
        let offered = json!({
            "seq": 7,
            "ts": "2026-09-23T02:00:00Z",
            "kind": "work_transition",
            "work_id": "work-1",
            "from": null,
            "to": "offered"
        });
        fs::write(
            tmp.path().join("mesh/events.jsonl"),
            format!("{}\n", serde_json::to_string(&offered).unwrap()),
        )
        .unwrap();

        let events = normalize(tmp.path(), &Cursor::default());

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].plane, Plane::Work);
        assert_eq!(events[0].kind, "work_offered");
        assert_eq!(events[0].subject.work_id.as_deref(), Some("work-1"));
        assert_eq!(events[0].payload, offered);
        assert_eq!(events[0].src.artifact, "mesh/events.jsonl");
        assert_eq!(events[0].src.reference, "seq=7");
    }

    #[test]
    fn transition_vocabulary_maps_to_the_work_taxonomy() {
        let cases = [
            (json!({"work_id": "w", "kind": "work_offered"}), "work_offered"),
            (json!({"work_id": "w", "status": "approved"}), "work_approved"),
            (json!({"work_id": "w", "verb": "release"}), "work_released"),
            (json!({"work_id": "w", "to": "done"}), "work_done"),
            (json!({"work_id": "w", "to": "failed"}), "work_failed"),
            (
                json!({"work_id": "w", "kind": "error", "code": "WORK_RESULT_EMPTY"}),
                "work_result_empty",
            ),
        ];

        for (source, expected) in cases {
            assert_eq!(work_kind(&source), Some(expected));
        }
    }

    #[test]
    fn broker_transitions_use_byte_cursor_and_known_result_empty_code() {
        let tmp = tempfile::tempdir().unwrap();
        seed_identity(tmp.path());
        let offered = json!({
            "ts": "2026-09-23T03:00:00Z",
            "kind": "work_transition",
            "work_id": "work-old",
            "to": "offered"
        });
        let empty = json!({
            "ts": "2026-09-23T03:00:01Z",
            "kind": "error",
            "work_id": "work-empty",
            "code": "WORK_RESULT_EMPTY"
        });
        let first = format!("{}\n", serde_json::to_string(&offered).unwrap());
        let second = format!("{}\n", serde_json::to_string(&empty).unwrap());
        fs::write(tmp.path().join("broker.jsonl"), format!("{first}{second}"))
            .unwrap();
        let mut cursor = Cursor::default();
        cursor.set_line("broker.jsonl", 0, first.len() as u64);

        let events = normalize(tmp.path(), &cursor);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "work_result_empty");
        assert_eq!(events[0].subject.work_id.as_deref(), Some("work-empty"));
        assert_eq!(events[0].src.artifact, "broker.jsonl");
        assert_eq!(
            events[0].src.reference,
            format!("offset={}", first.len() + second.len())
        );
    }
}
