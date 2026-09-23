//! Normalization for per-worker lifecycle and result artifacts.

use crate::observe::cursor::Watermark;
use crate::observe::source::read_json;
use crate::observe::tail::read_retained_jsonl;
use crate::observe::{Cursor, Event, Plane, Src, Subject};
use chrono::{SecondsFormat, Utc};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

const WORKERS_DIR: &str = "workers";
const EVENTS_FILE: &str = "events.jsonl";
const RESULT_FILE: &str = "result.json";

/// Normalize worker records newer than the cursor's source-specific watermarks.
pub fn normalize(home: &Path, cursor: &Cursor) -> Vec<Event> {
    let agent_id = read_agent_id(home);
    let mut events = Vec::new();
    let mut workers = worker_dirs(home);
    workers.sort_by(|left, right| left.0.cmp(&right.0));

    for (worker_id, worker_dir) in workers {
        normalize_worker(
            cursor,
            &agent_id,
            &worker_id,
            &worker_dir,
            &mut events,
        );
    }

    events.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.src.artifact.cmp(&right.src.artifact))
            .then_with(|| left.src.reference.cmp(&right.src.reference))
    });
    events
}

fn worker_dirs(home: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = fs::read_dir(home.join(WORKERS_DIR)) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            if !entry.file_type().ok()?.is_dir() {
                return None;
            }
            Some((entry.file_name().to_string_lossy().into_owned(), entry.path()))
        })
        .collect()
}

fn normalize_worker(
    cursor: &Cursor,
    agent_id: &str,
    worker_id: &str,
    worker_dir: &Path,
    events: &mut Vec<Event>,
) {
    let event_artifact = format!("{WORKERS_DIR}/{worker_id}/{EVENTS_FILE}");
    let watermark = sequence_watermark(cursor, &event_artifact);
    let mut result_written_ts = None;

    for row in read_retained_jsonl(&worker_dir.join(EVENTS_FILE)) {
        if row.get("kind").and_then(Value::as_str) == Some("result_written") {
            result_written_ts = explicit_timestamp(&row).map(str::to_string);
        }
        let Some(sequence) = row.get("seq").and_then(Value::as_u64) else {
            continue;
        };
        if sequence <= watermark {
            continue;
        }
        let Some(kind) = worker_kind(&row) else {
            continue;
        };
        events.push(event(
            agent_id,
            worker_id,
            kind,
            source_timestamp(&row),
            event_artifact.clone(),
            format!("seq={sequence}"),
            row,
        ));
    }

    let result_artifact = format!("{WORKERS_DIR}/{worker_id}/{RESULT_FILE}");
    if !artifact_is_new(cursor, &result_artifact) {
        return;
    }
    let Some(result) = read_json(&worker_dir.join(RESULT_FILE)) else {
        return;
    };
    let state_ts = read_json(&worker_dir.join("state.json"))
        .and_then(|state| explicit_timestamp_field(&state, "updated_at").map(str::to_string));
    let ts = explicit_timestamp(&result)
        .map(str::to_string)
        .or(result_written_ts)
        .or(state_ts)
        .unwrap_or_else(now_timestamp);
    events.push(event(
        agent_id,
        worker_id,
        "worker_result",
        ts,
        result_artifact,
        "current".to_string(),
        result,
    ));
}

fn worker_kind(row: &Value) -> Option<&'static str> {
    let envelope_kind = row.get("kind").and_then(Value::as_str)?;
    let raw = row.get("raw").unwrap_or(row);
    match envelope_kind {
        "spawned" => Some(if has_respawn_lineage(raw) {
            "worker_respawned"
        } else {
            "worker_spawned"
        }),
        "respawned" => Some("worker_respawned"),
        "interrupted" => Some("worker_interrupted"),
        "killed" => Some("worker_killed"),
        "budget_exhausted" => Some("worker_budget_exhausted"),
        "blocked" | "awaiting_input" | "awaiting-input" => Some("worker_awaiting_input"),
        "answered" | "inbox_delivered" => Some("worker_answered"),
        "running" | "done" | "failed" | "state_change" | "resumed" | "error" => {
            state_change_kind(row, raw)
        }
        _ => None,
    }
}

fn state_change_kind(row: &Value, raw: &Value) -> Option<&'static str> {
    match raw.get("fsm").and_then(Value::as_str) {
        Some("Spawn") => Some(if has_respawn_lineage(raw) {
            "worker_respawned"
        } else {
            "worker_spawned"
        }),
        Some("TurnEnded") if raw.get("has_result").and_then(Value::as_bool) != Some(true) => {
            Some("worker_awaiting_input")
        }
        Some("Send") => Some("worker_answered"),
        Some("Resume") if raw.get("answer_queued").and_then(Value::as_bool) == Some(true) => {
            Some("worker_answered")
        }
        Some("Interrupt") => Some("worker_interrupted"),
        Some("Kill") => Some("worker_killed"),
        Some("Timeout") if failure_class(row, raw) == Some("budget_exhausted") => {
            Some("worker_budget_exhausted")
        }
        Some(
            "Admit" | "ProcessStarted" | "SessionIdentified" | "TurnEnded" | "ResultInvalid"
            | "ProcessExited" | "SessionMissing" | "Timeout" | "OrphanDetected" | "Resume",
        ) => Some("worker_status"),
        _ => status_kind(row, raw),
    }
}

fn status_kind(row: &Value, raw: &Value) -> Option<&'static str> {
    let state = raw
        .get("to")
        .or_else(|| row.get("to"))
        .or_else(|| raw.get("state"))
        .or_else(|| row.get("state"))
        .and_then(Value::as_str);
    match state {
        Some("awaiting-input" | "awaiting_input") => Some("worker_awaiting_input"),
        Some(
            "spawned" | "queued" | "running" | "escalated" | "done" | "failed"
            | "cancelled" | "orphaned" | "unknown",
        ) => Some("worker_status"),
        _ => match row.get("kind").and_then(Value::as_str) {
            Some("state_change" | "running" | "done" | "failed" | "resumed" | "error") => {
                Some("worker_status")
            }
            _ => None,
        },
    }
}

fn failure_class<'a>(row: &'a Value, raw: &'a Value) -> Option<&'a str> {
    raw.get("failure_class")
        .or_else(|| row.get("failure_class"))
        .and_then(Value::as_str)
}

fn has_respawn_lineage(value: &Value) -> bool {
    value.get("parent_id").is_some_and(|parent| !parent.is_null())
        || value
            .get("attempt")
            .and_then(Value::as_u64)
            .is_some_and(|attempt| attempt > 1)
}

fn event(
    agent_id: &str,
    worker_id: &str,
    kind: &str,
    ts: String,
    artifact: String,
    reference: String,
    payload: Value,
) -> Event {
    Event {
        v: 1,
        ts,
        agent_id: agent_id.to_string(),
        plane: Plane::Worker,
        kind: kind.to_string(),
        subject: Subject {
            worker_id: Some(worker_id.to_string()),
            work_id: None,
            peer: None,
        },
        payload,
        src: Src {
            artifact,
            reference,
        },
    }
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
    explicit_timestamp(value)
        .map(str::to_string)
        .unwrap_or_else(now_timestamp)
}

fn explicit_timestamp(value: &Value) -> Option<&str> {
    explicit_timestamp_field(value, "ts")
}

fn explicit_timestamp_field<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn sequence_watermark(cursor: &Cursor, artifact: &str) -> u64 {
    match cursor.watermarks.get(artifact) {
        Some(Watermark::Sequence { seq }) => *seq,
        _ => 0,
    }
}

fn artifact_is_new(cursor: &Cursor, artifact: &str) -> bool {
    match cursor.watermarks.get(artifact) {
        None => true,
        Some(Watermark::Sequence { seq }) => *seq < 1,
        Some(Watermark::Line { position, .. }) => *position < 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::Plane;
    use serde_json::json;
    use std::fs;

    fn envelope(seq: u64, ts: &str, kind: &str, raw: serde_json::Value) -> serde_json::Value {
        json!({
            "schema_version": 1,
            "seq": seq,
            "ts": ts,
            "epoch": 1,
            "class": "lifecycle",
            "kind": kind,
            "transport": "exec",
            "raw": raw
        })
    }

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
    fn worker_events_and_result_are_normalized_with_worker_subject() {
        let tmp = tempfile::tempdir().unwrap();
        seed_identity(tmp.path());
        let worker_id = "impl-0923t0100-test";
        let worker = tmp.path().join("workers").join(worker_id);
        fs::create_dir_all(&worker).unwrap();

        let spawned = envelope(
            1,
            "2026-09-23T01:00:00Z",
            "spawned",
            json!({"fsm": "Spawn", "id": worker_id}),
        );
        let running = envelope(
            2,
            "2026-09-23T01:00:01Z",
            "state_change",
            json!({"fsm": "ProcessStarted", "pid": 42, "start_witness": "boot"}),
        );
        fs::write(
            worker.join("events.jsonl"),
            format!(
                "{}\n{}\n",
                serde_json::to_string(&spawned).unwrap(),
                serde_json::to_string(&running).unwrap()
            ),
        )
        .unwrap();
        let result = json!({
            "ts": "2026-09-23T01:00:02Z",
            "status": "pass",
            "summary": "implemented"
        });
        fs::write(
            worker.join("result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();

        let events = normalize(tmp.path(), &Cursor::default());

        assert_eq!(events.len(), 3);
        assert_eq!(
            events.iter().map(|event| event.kind.as_str()).collect::<Vec<_>>(),
            vec!["worker_spawned", "worker_status", "worker_result"]
        );
        assert!(events.iter().all(|event| {
            event.plane == Plane::Worker
                && event.subject.worker_id.as_deref() == Some(worker_id)
        }));
        assert_eq!(events[0].payload, spawned);
        assert_eq!(events[1].payload, running);
        assert_eq!(events[2].payload, result);
    }

    #[test]
    fn lifecycle_vocabulary_maps_to_the_worker_taxonomy() {
        let cases = [
            (
                envelope(
                    1,
                    "2026-09-23T01:00:00Z",
                    "spawned",
                    json!({"fsm": "Spawn", "id": "impl-child", "parent_id": "impl-parent", "attempt": 2}),
                ),
                "worker_respawned",
            ),
            (
                envelope(
                    2,
                    "2026-09-23T01:00:01Z",
                    "state_change",
                    json!({"fsm": "TurnEnded", "has_result": false, "has_question": true}),
                ),
                "worker_awaiting_input",
            ),
            (
                envelope(
                    3,
                    "2026-09-23T01:00:02Z",
                    "state_change",
                    json!({"fsm": "Send"}),
                ),
                "worker_answered",
            ),
            (
                envelope(
                    4,
                    "2026-09-23T01:00:03Z",
                    "interrupted",
                    json!({"fsm": "Interrupt"}),
                ),
                "worker_interrupted",
            ),
            (
                envelope(
                    5,
                    "2026-09-23T01:00:04Z",
                    "killed",
                    json!({"fsm": "Kill"}),
                ),
                "worker_killed",
            ),
            (
                envelope(
                    6,
                    "2026-09-23T01:00:05Z",
                    "budget_exhausted",
                    json!({"fsm": "Timeout", "kind": "task", "failure_class": "budget_exhausted"}),
                ),
                "worker_budget_exhausted",
            ),
        ];

        for (source, expected) in cases {
            assert_eq!(worker_kind(&source), Some(expected));
        }
    }

    #[test]
    fn worker_sequence_cursor_filters_committed_envelopes() {
        let tmp = tempfile::tempdir().unwrap();
        let worker_id = "impl-0923t0100-cursor";
        let worker = tmp.path().join("workers").join(worker_id);
        fs::create_dir_all(&worker).unwrap();
        let spawned = envelope(
            1,
            "2026-09-23T04:00:00Z",
            "spawned",
            json!({"fsm": "Spawn", "id": worker_id}),
        );
        let interrupted = envelope(
            2,
            "2026-09-23T04:00:01Z",
            "interrupted",
            json!({"fsm": "Interrupt"}),
        );
        fs::write(
            worker.join("events.jsonl"),
            format!(
                "{}\n{}\n",
                serde_json::to_string(&spawned).unwrap(),
                serde_json::to_string(&interrupted).unwrap()
            ),
        )
        .unwrap();
        let mut cursor = Cursor::default();
        cursor.set(format!("workers/{worker_id}/events.jsonl"), 1);

        let events = normalize(tmp.path(), &cursor);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "worker_interrupted");
        assert_eq!(events[0].src.reference, "seq=2");
    }
}
