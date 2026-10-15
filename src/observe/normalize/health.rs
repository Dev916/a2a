//! Normalization for the gateway health snapshot.

use crate::observe::cursor::Watermark;
use crate::observe::source::read_json;
use crate::observe::{Cursor, Event, Plane, Src, Subject};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

const HEALTH_ARTIFACT: &str = "mesh/gateway-health.json";

/// Normalize the current gateway health snapshot.
///
/// The gateway rewrites this file every second with fresh heartbeat counters,
/// so a new `health_seq` alone means nothing. Each kind is emitted only when
/// its meaningful projection differs from the fingerprint the cursor recorded
/// for it (`mesh/gateway-health.json#<kind>`).
///
/// The gateway writes this file atomically, but an interrupted or externally
/// damaged file is still possible. Invalid and missing snapshots are therefore
/// soft-skipped instead of escaping an error from the observation plane.
pub fn normalize(home: &Path, cursor: &Cursor) -> Vec<Event> {
    normalize_with_watermarks(home, cursor).0
}

/// Like [`normalize`], also returning the per-kind fingerprint watermarks of
/// the exact snapshot that was read, so the advanced cursor cannot skip a
/// change written between this read and a later re-read.
pub fn normalize_with_watermarks(home: &Path, cursor: &Cursor) -> (Vec<Event>, Vec<(String, u64)>) {
    let path = home.join(HEALTH_ARTIFACT);
    let Some(health) = read_json(&path) else {
        if path.exists() {
            eprintln!(
                "a2a observe: ignoring unreadable or corrupt {}",
                path.display()
            );
        }
        return (Vec::new(), Vec::new());
    };
    if !health.is_object() {
        eprintln!(
            "a2a observe: ignoring malformed non-object {}",
            path.display()
        );
        return (Vec::new(), Vec::new());
    }

    let sequence = health.get("health_seq").and_then(Value::as_u64);
    let timestamp = health_timestamp(&path, &health);
    let reference = sequence
        .map(|sequence| format!("health_seq={sequence}"))
        .unwrap_or_else(|| "current".to_string());
    let agent_id = read_agent_id(home);
    let mut events = Vec::new();
    let mut watermarks = Vec::new();

    for (kind, payload, meaningful) in meaningful_blocks(&health) {
        let key = fingerprint_key(kind);
        let current = fingerprint(&meaningful);
        let changed = match cursor.watermarks.get(&key) {
            Some(Watermark::Sequence { seq }) => *seq != current,
            // Cursors minted before per-kind fingerprints resume on health_seq.
            _ => snapshot_is_new(cursor, HEALTH_ARTIFACT, sequence),
        };
        if changed {
            events.push(event(&agent_id, kind, &timestamp, &reference, payload));
        }
        watermarks.push((key, current));
    }

    (events, watermarks)
}

/// The per-kind fingerprint watermarks for the health snapshot currently on disk.
pub fn watermarks(home: &Path) -> Vec<(String, u64)> {
    let Some(health) = read_json(&home.join(HEALTH_ARTIFACT)).filter(Value::is_object) else {
        return Vec::new();
    };
    meaningful_blocks(&health)
        .into_iter()
        .map(|(kind, _, meaningful)| (fingerprint_key(kind), fingerprint(&meaningful)))
        .collect()
}

/// Cursor key holding the fingerprint of one health kind's meaningful subset.
pub(crate) fn fingerprint_key(kind: &str) -> String {
    format!("{HEALTH_ARTIFACT}#{kind}")
}

/// Stable 64-bit fingerprint of a JSON value (object keys serialize sorted).
pub(crate) fn fingerprint(value: &Value) -> u64 {
    let digest = Sha256::digest(value.to_string().as_bytes());
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(prefix)
}

/// Every health kind present in `health` as (kind, event payload, meaningful
/// subset). Heartbeat counters, `last_heartbeat`, `health_seq`, `written_at`
/// and per-link traffic timestamps are excluded from the meaningful subset.
fn meaningful_blocks(health: &Value) -> Vec<(&'static str, Value, Value)> {
    let mut blocks = Vec::new();
    let same = |kind, value: Value| (kind, value.clone(), value);

    if let Some(gateway) = projected(health, &["state", "pid", "listen_port", "escalation", "shutdown"]) {
        blocks.push(same("gateway", gateway));
    }
    if let Some(loops) = health.get("loops").and_then(Value::as_object) {
        let mut projection = Map::new();
        for (name, row) in loops {
            let row = projected(row, &["state", "restart_policy", "restarts", "last_exit"])
                .unwrap_or(Value::Null);
            projection.insert(name.clone(), row);
        }
        blocks.push(same("loops", Value::Object(projection)));
    }
    if let Some(payload) = health.get("limits").cloned() {
        blocks.push(same("limits", payload));
    }
    if let Some(payload) = health.get("replication").cloned() {
        blocks.push(same("replication", payload));
    }
    if let Some(links) = health.get("links").and_then(Value::as_array) {
        let mut peers = links
            .iter()
            .filter_map(|link| {
                ["peer", "peer_id", "peer_gateway_id", "gateway_id", "id"]
                    .into_iter()
                    .find_map(|field| link.get(field).and_then(Value::as_str))
            })
            .map(str::to_string)
            .collect::<Vec<_>>();
        peers.sort();
        let meaningful = json!({"count": links.len(), "peers": peers});
        let mut payload = meaningful.clone();
        payload["links"] = Value::Array(links.clone());
        blocks.push(("links", payload, meaningful));
    }
    if let Some(payload) = direct_or_projected_block(
        health,
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
        blocks.push(same("exposure", payload));
    }
    if let Some(payload) = direct_or_projected_block(
        health,
        "rotation",
        &[
            "token_expires_at",
            "days_remaining",
            "token_expiry_warning",
            "root_mismatch_suspected",
        ],
    ) {
        blocks.push(same("rotation", payload));
    }
    blocks
}

fn projected(value: &Value, fields: &[&str]) -> Option<Value> {
    let object = value.as_object()?;
    let mut projection = Map::new();
    for field in fields {
        if let Some(value) = object.get(*field) {
            projection.insert((*field).to_string(), value.clone());
        }
    }
    (!projection.is_empty()).then_some(Value::Object(projection))
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

    fn gateway_health(seq: u64, beat: u64, dialer_state: &str, links: Value) -> Value {
        let ts = format!("2026-10-15T16:00:{:02}Z", seq % 60);
        let loop_row = |state: &str| {
            json!({
                "state": state,
                "restart_policy": "one-for-one",
                "restarts": 0,
                "heartbeats": beat,
                "last_heartbeat": ts,
                "last_exit": null
            })
        };
        json!({
            "schema_version": 2,
            "pid": 844,
            "health_seq": seq,
            "written_at": ts,
            "state": "running",
            "expose": "ngrok",
            "endpoint_url": "wss://4.tcp.ngrok.io:16537",
            "token_expires_at": "2026-11-24T14:54:25Z",
            "days_remaining": 40,
            "loops": {
                "registry": loop_row("running"),
                "dialer": loop_row(dialer_state)
            },
            "replication": {"per_origin_lag": {}, "last_anti_entropy": "2026-10-15T16:21:52Z"},
            "limits": {},
            "links": links,
            "escalation": null,
            "shutdown": null
        })
    }

    fn seed_health(home: &Path, value: &Value) {
        fs::create_dir_all(home.join("mesh/gateway")).unwrap();
        fs::write(
            home.join("mesh/gateway/gw.key"),
            br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
        )
        .unwrap();
        fs::write(
            home.join("mesh/gateway-health.json"),
            serde_json::to_vec(value).unwrap(),
        )
        .unwrap();
    }

    fn kinds_on(events: &[Event], plane: Plane) -> Vec<String> {
        events
            .iter()
            .filter(|event| event.plane == plane)
            .map(|event| event.kind.clone())
            .collect()
    }

    #[test]
    fn heartbeat_only_rewrites_emit_no_health_events() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        let link = json!([{"peer": "gw-a", "last_rx": "2026-10-15T16:00:00Z"}]);
        seed_health(home, &gateway_health(10, 100, "running", link));
        let observer = crate::observe::Observer::open(home);
        let c0 = observer.snapshot().cursor;

        let mut cursor = c0;
        for beat in 1..=5 {
            let link = json!([{"peer": "gw-a", "last_rx": format!("2026-10-15T16:00:0{beat}Z")}]);
            seed_health(home, &gateway_health(10 + beat, 100 + beat, "running", link));
            let (events, advanced) = observer.events_since(&cursor);
            assert!(
                events.is_empty(),
                "heartbeat churn must not emit events: {:?}",
                events.iter().map(|event| &event.kind).collect::<Vec<_>>()
            );
            cursor = advanced;
        }
    }

    #[test]
    fn loop_state_change_emits_one_loops_event() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_health(home, &gateway_health(10, 100, "running", json!([])));
        let observer = crate::observe::Observer::open(home);
        let c0 = observer.snapshot().cursor;

        seed_health(home, &gateway_health(11, 101, "restarting", json!([])));
        let (events, c1) = observer.events_since(&c0);
        assert_eq!(kinds_on(&events, Plane::Health), vec!["loops".to_string()]);
        assert_eq!(events[0].payload["dialer"]["state"], "restarting");
        assert!(events[0].payload["dialer"].get("heartbeats").is_none());

        seed_health(home, &gateway_health(12, 102, "restarting", json!([])));
        assert!(observer.events_since(&c1).0.is_empty());
    }

    #[test]
    fn link_change_emits_one_links_event() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_health(home, &gateway_health(10, 100, "running", json!([])));
        let observer = crate::observe::Observer::open(home);
        let c0 = observer.snapshot().cursor;

        let link = json!([{"peer": "gw-a", "last_rx": "2026-10-15T16:00:11Z"}]);
        seed_health(home, &gateway_health(11, 101, "running", link));
        let (events, c1) = observer.events_since(&c0);
        assert_eq!(kinds_on(&events, Plane::Health), vec!["links".to_string()]);
        let links = events.iter().find(|event| event.kind == "links").unwrap();
        assert_eq!(links.payload["count"], 1);
        assert_eq!(links.payload["peers"], json!(["gw-a"]));
        assert_eq!(kinds_on(&events, Plane::Presence), vec!["peer_up".to_string()]);

        let link = json!([{"peer": "gw-a", "last_rx": "2026-10-15T16:00:12Z"}]);
        seed_health(home, &gateway_health(12, 102, "running", link));
        assert!(observer.events_since(&c1).0.is_empty());
    }

    #[test]
    fn genesis_emits_every_present_health_kind() {
        let tmp = tempfile::tempdir().unwrap();
        seed_health(tmp.path(), &gateway_health(10, 100, "running", json!([])));
        let kinds = normalize(tmp.path(), &Cursor::default())
            .into_iter()
            .map(|event| event.kind)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            kinds,
            ["exposure", "gateway", "limits", "links", "loops", "replication", "rotation"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
    }

    #[test]
    fn legacy_health_seq_cursor_still_resumes() {
        let tmp = tempfile::tempdir().unwrap();
        seed_health(tmp.path(), &gateway_health(10, 100, "running", json!([])));
        let mut legacy = Cursor::default();
        legacy.set(HEALTH_ARTIFACT, 10);
        assert!(normalize(tmp.path(), &legacy).is_empty());
        legacy.set(HEALTH_ARTIFACT, 9);
        assert!(!normalize(tmp.path(), &legacy).is_empty());
    }
}
