//! Normalization for the gateway health snapshot.

use crate::observe::cursor::Watermark;
use crate::observe::source::read_json;
use crate::observe::{Cursor, Event, Plane, Src, Subject};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use std::fs;
use std::path::Path;

const HEALTH_ARTIFACT: &str = "mesh/gateway-health.json";

/// Normalize the current gateway health snapshot.
///
/// The gateway writes this file atomically, but an interrupted or externally
/// damaged file is still possible. Invalid and missing snapshots are therefore
/// soft-skipped instead of escaping an error from the observation plane.
pub fn normalize(home: &Path, cursor: &Cursor) -> Vec<Event> {
    let path = home.join(HEALTH_ARTIFACT);
    let Some(health) = read_json(&path) else {
        if path.exists() {
            eprintln!(
                "a2a observe: ignoring unreadable or corrupt {}",
                path.display()
            );
        }
        return Vec::new();
    };
    if !health.is_object() {
        eprintln!(
            "a2a observe: ignoring malformed non-object {}",
            path.display()
        );
        return Vec::new();
    }

    let sequence = health.get("health_seq").and_then(Value::as_u64);
    if !snapshot_is_new(cursor, HEALTH_ARTIFACT, sequence) {
        return Vec::new();
    }

    let timestamp = health_timestamp(&path, &health);
    let reference = sequence
        .map(|sequence| format!("health_seq={sequence}"))
        .unwrap_or_else(|| "current".to_string());
    let agent_id = read_agent_id(home);
    let mut events = Vec::new();

    if let Some(payload) = health.get("limits").cloned() {
        events.push(event(
            &agent_id,
            "limits",
            &timestamp,
            &reference,
            payload,
        ));
    }
    if let Some(payload) = health.get("replication").cloned() {
        events.push(event(
            &agent_id,
            "replication",
            &timestamp,
            &reference,
            payload,
        ));
    }
    if let Some(payload) = direct_or_projected_block(
        &health,
        "exposure",
        &[
            "expose",
            "endpoint_url",
            "exposure_error",
            "exposure_error_since",
            "registry_error",
            "registry_error_since",
        ],
    ) {
        events.push(event(
            &agent_id,
            "exposure",
            &timestamp,
            &reference,
            payload,
        ));
    }
    if let Some(payload) = direct_or_projected_block(
        &health,
        "rotation",
        &[
            "token_expires_at",
            "days_remaining",
            "token_expiry_warning",
            "root_mismatch_suspected",
        ],
    ) {
        events.push(event(
            &agent_id,
            "rotation",
            &timestamp,
            &reference,
            payload,
        ));
    }

    events
}

fn direct_or_projected_block(value: &Value, direct: &str, fields: &[&str]) -> Option<Value> {
    if let Some(block) = value.get(direct) {
        return Some(block.clone());
    }
    let object = value.as_object()?;
    let mut projected = Map::new();
    for field in fields {
        if let Some(value) = object.get(*field) {
            projected.insert((*field).to_string(), value.clone());
        }
    }
    (!projected.is_empty()).then_some(Value::Object(projected))
}

fn event(
    agent_id: &str,
    kind: &str,
    timestamp: &str,
    reference: &str,
    payload: Value,
) -> Event {
    Event {
        v: 1,
        ts: timestamp.to_string(),
        agent_id: agent_id.to_string(),
        plane: Plane::Health,
        kind: kind.to_string(),
        subject: Subject {
            worker_id: None,
            work_id: None,
            peer: None,
        },
        payload,
        src: Src {
            artifact: HEALTH_ARTIFACT.to_string(),
            reference: reference.to_string(),
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

fn health_timestamp(path: &Path, value: &Value) -> String {
    value
        .get("ts")
        .or_else(|| value.get("written_at"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| modified_timestamp(path))
        .unwrap_or_else(now_timestamp)
}

fn modified_timestamp(path: &Path) -> Option<String> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    Some(DateTime::<Utc>::from(modified).to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn snapshot_is_new(cursor: &Cursor, artifact: &str, sequence: Option<u64>) -> bool {
    match cursor.watermarks.get(artifact) {
        None => true,
        Some(Watermark::Sequence { seq }) => sequence.map_or(*seq < 1, |current| current > *seq),
        Some(Watermark::Line { position, .. }) => *position < 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::Plane;
    use serde_json::json;
    use std::collections::BTreeSet;
    use std::fs;

    #[test]
    fn limits_block_becomes_a_health_event() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("mesh")).unwrap();
        let limits = json!({
            "gw-peer": {
                "defers": {"notes": 2},
                "drops": {"feed_entries": 1}
            }
        });
        fs::write(
            tmp.path().join("mesh/gateway-health.json"),
            serde_json::to_vec(&json!({
                "health_seq": 7,
                "written_at": "2026-09-23T04:00:00Z",
                "limits": limits,
                "replication": {
                    "per_origin_lag": {},
                    "last_anti_entropy": null
                },
                "expose": "none",
                "endpoint_url": null,
                "token_expires_at": null
            }))
            .unwrap(),
        )
        .unwrap();

        let events = normalize(tmp.path(), &Cursor::default());
        let kinds = events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            kinds,
            BTreeSet::from(["exposure", "limits", "replication", "rotation"])
        );
        let event = events
            .iter()
            .find(|event| event.kind == "limits")
            .expect("limits event");

        assert_eq!(event.plane, Plane::Health);
        assert_eq!(event.ts, "2026-09-23T04:00:00Z");
        assert_eq!(event.payload, limits);
        assert_eq!(event.src.artifact, "mesh/gateway-health.json");
        assert_eq!(event.src.reference, "health_seq=7");
    }

    #[test]
    fn corrupt_health_is_a_soft_skip() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("mesh")).unwrap();
        fs::write(
            tmp.path().join("mesh/gateway-health.json"),
            b"this is not json",
        )
        .unwrap();

        assert!(normalize(tmp.path(), &Cursor::default()).is_empty());
    }
}
