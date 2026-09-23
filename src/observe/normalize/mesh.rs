//! Normalization for mesh feed, note, and gateway-limit artifacts.

use crate::observe::cursor::Watermark;
use crate::observe::source::{read_json, read_jsonl, read_jsonl_records_from};
use crate::observe::{Cursor, Event, Plane, Src, Subject};
use chrono::{SecondsFormat, Utc};
use serde_json::Value;
use std::path::Path;

const FEED_ARTIFACT: &str = "mesh/feed.jsonl";
const EVENTS_ARTIFACT: &str = "mesh/events.jsonl";
const GATEWAY_ARTIFACT: &str = "mesh/gateway/gateway.jsonl";

/// Normalize mesh records newer than the cursor's source-specific watermarks.
///
/// This function only uses the read-only source helpers. In particular, it
/// takes a path instead of resolving an `A2aHome`, because resolving a home can
/// create layout files and directories.
pub fn normalize(home: &Path, cursor: &Cursor) -> Vec<Event> {
    let agent_id = read_agent_id(home);
    let mut events = Vec::new();

    normalize_feed(home, cursor, &agent_id, &mut events);
    normalize_notes(home, cursor, &agent_id, &mut events);
    normalize_gateway(home, cursor, &agent_id, &mut events);

    events.sort_by(|left, right| left.ts.cmp(&right.ts));
    events
}

fn normalize_feed(home: &Path, cursor: &Cursor, agent_id: &str, events: &mut Vec<Event>) {
    let watermark = sequence_watermark(cursor, FEED_ARTIFACT);
    for row in read_jsonl(&home.join(FEED_ARTIFACT)) {
        if is_feed_bookkeeping(&row) {
            continue;
        }
        let Some(sequence) = feed_sequence(&row) else {
            continue;
        };
        if sequence <= watermark {
            continue;
        }
        let reference = match row.get("origin_seq").and_then(Value::as_u64) {
            Some(origin_seq) => format!("origin_seq={origin_seq}"),
            None => format!("seq={sequence}"),
        };
        events.push(event(
            agent_id,
            "feed_entry",
            FEED_ARTIFACT,
            reference,
            row,
        ));
    }
}

fn normalize_notes(home: &Path, cursor: &Cursor, agent_id: &str, events: &mut Vec<Event>) {
    let watermark = sequence_watermark(cursor, EVENTS_ARTIFACT);
    for row in read_jsonl(&home.join(EVENTS_ARTIFACT)) {
        if row.get("kind").and_then(Value::as_str) == Some("events_rotated") {
            continue;
        }
        let Some(sequence) = row.get("seq").and_then(Value::as_u64) else {
            continue;
        };
        if sequence <= watermark {
            continue;
        }
        events.push(event(
            agent_id,
            "note",
            EVENTS_ARTIFACT,
            format!("seq={sequence}"),
            row,
        ));
    }
}

fn normalize_gateway(home: &Path, cursor: &Cursor, agent_id: &str, events: &mut Vec<Event>) {
    let offset = line_watermark(cursor, GATEWAY_ARTIFACT);
    let (rows, _advanced) = read_jsonl_records_from(&home.join(GATEWAY_ARTIFACT), offset);
    for (row, end_offset) in rows {
        let Some(kind) = gateway_kind(&row) else {
            continue;
        };
        events.push(event(
            agent_id,
            kind,
            GATEWAY_ARTIFACT,
            format!("offset={end_offset}"),
            row,
        ));
    }
}

fn event(
    agent_id: &str,
    kind: &str,
    artifact: &str,
    reference: String,
    payload: Value,
) -> Event {
    Event {
        v: 1,
        ts: source_timestamp(&payload),
        agent_id: agent_id.to_string(),
        plane: Plane::Mesh,
        kind: kind.to_string(),
        subject: Subject {
            worker_id: None,
            work_id: None,
            peer: None,
        },
        payload,
        src: Src {
            artifact: artifact.to_string(),
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
    value
        .get("ts")
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

fn feed_sequence(value: &Value) -> Option<u64> {
    value
        .get("origin_seq")
        .and_then(Value::as_u64)
        .or_else(|| value.get("seq").and_then(Value::as_u64))
}

fn is_feed_bookkeeping(value: &Value) -> bool {
    matches!(
        value.get("kind").and_then(Value::as_str),
        Some("events_rotated" | "feed_compacted")
    )
}

fn gateway_kind(value: &Value) -> Option<&'static str> {
    let kind = value.get("kind").and_then(Value::as_str);
    let action = value.get("action").and_then(Value::as_str);
    if kind == Some("deferral") || action == Some("deferral_received") {
        Some("deferral")
    } else if matches!(kind, Some("link_limit" | "limit_state_change")) {
        Some("link_limit")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::cursor::Watermark;
    use crate::observe::{Cursor, Plane};
    use serde_json::json;
    use std::fs;

    fn seed_identity(home: &std::path::Path) {
        let gateway = home.join("mesh/gateway");
        fs::create_dir_all(&gateway).unwrap();
        fs::write(
            gateway.join("gw.key"),
            br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
        )
        .unwrap();
    }

    #[test]
    fn feed_entry_mirrors_the_source_and_skips_a_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        seed_identity(tmp.path());
        let entry = json!({
            "seq": 7,
            "origin": "gw-local",
            "origin_seq": 42,
            "from": "codex:abc",
            "ts": "2026-09-17T18:22:01.412Z",
            "text": "hello",
            "refs": {"work_id": "work-1"}
        });
        let mut bytes = serde_json::to_vec(&entry).unwrap();
        bytes.extend_from_slice(b"\n{\"seq\":8");
        fs::create_dir_all(tmp.path().join("mesh")).unwrap();
        fs::write(tmp.path().join("mesh/feed.jsonl"), bytes).unwrap();

        let events = normalize(tmp.path(), &Cursor::default());

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.v, 1);
        assert_eq!(event.ts, "2026-09-17T18:22:01.412Z");
        assert_eq!(event.agent_id, "gw-local");
        assert_eq!(event.plane, Plane::Mesh);
        assert_eq!(event.kind, "feed_entry");
        assert_eq!(event.payload, entry);
        assert_eq!(event.src.artifact, "mesh/feed.jsonl");
        assert_eq!(event.src.reference, "origin_seq=42");
    }

    #[test]
    fn sequence_cursor_filters_feed_and_mesh_event_rows() {
        let tmp = tempfile::tempdir().unwrap();
        seed_identity(tmp.path());
        fs::create_dir_all(tmp.path().join("mesh")).unwrap();
        fs::write(
            tmp.path().join("mesh/feed.jsonl"),
            concat!(
                "{\"seq\":1,\"origin_seq\":1,\"ts\":\"2026-09-17T00:00:00Z\"}\n",
                "{\"seq\":2,\"origin_seq\":2,\"ts\":\"2026-09-17T00:00:02Z\"}\n"
            ),
        )
        .unwrap();
        fs::write(
            tmp.path().join("mesh/events.jsonl"),
            concat!(
                "{\"seq\":3,\"ts\":\"2026-09-17T00:00:01Z\",\"kind\":\"outbox_notice\",\"raw\":{\"text\":\"note\"}}\n",
                "{\"seq\":4,\"ts\":\"2026-09-17T00:00:03Z\",\"kind\":\"outbox_notice\",\"raw\":{\"text\":\"new\"}}\n"
            ),
        )
        .unwrap();
        let mut cursor = Cursor::default();
        cursor.set("mesh/feed.jsonl", 1);
        cursor.set("mesh/events.jsonl", 3);

        let events = normalize(tmp.path(), &cursor);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "feed_entry");
        assert_eq!(events[0].src.reference, "origin_seq=2");
        assert_eq!(events[1].kind, "note");
        assert_eq!(events[1].src.reference, "seq=4");
    }

    #[test]
    fn gateway_rows_map_to_deferral_and_link_limit_after_line_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        seed_identity(tmp.path());
        let first = concat!(
            "{\"ts\":\"2026-09-17T00:00:00Z\",\"kind\":\"link_limit\",",
            "\"peer_or_source\":\"gw-peer\",\"action\":\"closed\"}\n"
        );
        let second = concat!(
            "{\"ts\":\"2026-09-17T00:00:01Z\",\"kind\":\"link\",",
            "\"peer_gateway_id\":\"gw-peer\",\"action\":\"deferral_received\"}\n"
        );
        fs::write(
            tmp.path().join("mesh/gateway/gateway.jsonl"),
            format!("{first}{second}"),
        )
        .unwrap();
        let mut cursor = Cursor::default();
        cursor.watermarks.insert(
            "mesh/gateway/gateway.jsonl".into(),
            Watermark::Line {
                generation: 0,
                position: first.len() as u64,
            },
        );

        let events = normalize(tmp.path(), &cursor);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "deferral");
        assert_eq!(events[0].payload["action"], "deferral_received");
    }
}
