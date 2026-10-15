//! Normalization for mesh peer presence and version snapshots.

use crate::observe::cursor::Watermark;
use crate::observe::source::{modified_nanos, read_json};
use crate::observe::{Cursor, Event, Plane, Src, Subject};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

const HEALTH_ARTIFACT: &str = "mesh/gateway-health.json";
const REGISTRY_ARTIFACT: &str = "mesh/gateway/registry-cache.json";
const PEERS_DIR: &str = "mesh/peers";
/// The single cursor watermark for every `mesh/peers/<peer>/presence.json`:
/// the newest presence file modification time, in nanoseconds since the epoch.
pub(crate) const PEERS_WATERMARK: &str = "mesh/peers";
/// Fingerprint of the peer rows derived from `gateway-health.json` (links and
/// peers), so heartbeat-only rewrites do not re-announce every linked peer.
pub(crate) const HEALTH_PEERS_WATERMARK: &str = "mesh/gateway-health.json#peers";

#[derive(Clone)]
struct PeerRow {
    peer: String,
    payload: Value,
    status: Option<&'static str>,
    version: Option<String>,
    timestamp: String,
}

/// Normalize current peer presence and version snapshots.
pub fn normalize(home: &Path, cursor: &Cursor) -> Vec<Event> {
    normalize_with_watermarks(home, cursor).0
}

/// Like [`normalize`], also returning the health-peer fingerprint of the exact
/// `gateway-health.json` that was read.
pub fn normalize_with_watermarks(home: &Path, cursor: &Cursor) -> (Vec<Event>, Vec<(String, u64)>) {
    let agent_id = read_agent_id(home);
    let health_path = home.join(HEALTH_ARTIFACT);
    let health = read_json(&health_path).filter(Value::is_object);
    let health_sequence = health
        .as_ref()
        .and_then(|value| value.get("health_seq"))
        .and_then(Value::as_u64);
    let health_reference = health_sequence
        .map(|sequence| format!("health_seq={sequence}"))
        .unwrap_or_else(|| "current".to_string());
    let health_fallback_ts = health
        .as_ref()
        .map(|value| source_timestamp(&health_path, value, &["ts", "written_at"]))
        .unwrap_or_else(now_timestamp);
    let health_rows = health
        .as_ref()
        .map(|value| health_peer_rows(value, &health_fallback_ts))
        .unwrap_or_default();
    let live_peers = health_rows
        .iter()
        .filter(|row| row.status == Some("peer_up"))
        .map(|row| row.peer.clone())
        .collect::<BTreeSet<_>>();

    let registry_path = home.join(REGISTRY_ARTIFACT);
    let registry = read_json(&registry_path);
    let registry_rows = registry
        .as_ref()
        .map(|value| registry_peer_rows(value, &registry_path))
        .unwrap_or_default();

    let mut events = Vec::new();
    let mut emitted = BTreeSet::new();

    if snapshot_is_new(cursor, REGISTRY_ARTIFACT, None) {
        // The cached registry row is the authoritative source for a gateway's
        // advertised binary version, so consume these before fallbacks.
        for row in &registry_rows {
            if row.peer == agent_id {
                continue;
            }
            if row.version.is_some() {
                push_event(
                    &mut events,
                    &mut emitted,
                    &agent_id,
                    row,
                    "peer_version",
                    REGISTRY_ARTIFACT,
                    format!("peer={}", row.peer),
                );
            }
        }
    }

    let health_fingerprint = health.as_ref().map(|_| health_peers_fingerprint(&health_rows));
    let health_rows_changed = match (cursor.watermarks.get(HEALTH_PEERS_WATERMARK), health_fingerprint) {
        (Some(Watermark::Sequence { seq }), Some(current)) => *seq != current,
        // Cursors minted before the fingerprint resume on health_seq.
        _ => snapshot_is_new(cursor, HEALTH_ARTIFACT, health_sequence),
    };
    if health.is_some() && health_rows_changed {
        for row in &health_rows {
            if row.peer == agent_id {
                continue;
            }
            if let Some(status) = row.status {
                push_event(
                    &mut events,
                    &mut emitted,
                    &agent_id,
                    row,
                    status,
                    HEALTH_ARTIFACT,
                    format!("{health_reference};peer={}", row.peer),
                );
            }
            if row.version.is_some() {
                push_event(
                    &mut events,
                    &mut emitted,
                    &agent_id,
                    row,
                    "peer_version",
                    HEALTH_ARTIFACT,
                    format!("{health_reference};peer={}", row.peer),
                );
            }
        }
    }

    if snapshot_is_new(cursor, REGISTRY_ARTIFACT, None) {
        for row in &registry_rows {
            if row.peer == agent_id {
                continue;
            }
            let inferred = if live_peers.contains(&row.peer) {
                Some("peer_up")
            } else if health.is_some() {
                Some("peer_down")
            } else {
                None
            };
            if let Some(status) = row.status.or(inferred) {
                push_event(
                    &mut events,
                    &mut emitted,
                    &agent_id,
                    row,
                    status,
                    REGISTRY_ARTIFACT,
                    format!("peer={}", row.peer),
                );
            }
        }
    }

    for (artifact, path, value) in presence_artifacts(home) {
        let sequence = value.get("presence_seq").and_then(Value::as_u64);
        if !presence_is_new(cursor, &artifact, &path, sequence) {
            continue;
        }
        let Some(peer) = peer_id(&value).map(str::to_string).or_else(|| {
            path.parent()
                .and_then(Path::file_name)
                .and_then(|name| peer_from_dir_name(&name.to_string_lossy()))
        }) else {
            continue;
        };
        if peer == agent_id {
            continue;
        }
        let row = PeerRow {
            peer,
            status: explicit_presence_kind(&value).or(Some("peer_up")),
            version: peer_version(&value),
            timestamp: source_timestamp(&path, &value, &["ts", "last_seen", "registered_at"]),
            payload: value,
        };
        if let Some(status) = row.status {
            push_event(
                &mut events,
                &mut emitted,
                &agent_id,
                &row,
                status,
                &artifact,
                presence_reference(&row, sequence),
            );
        }
        if row.version.is_some() {
            push_event(
                &mut events,
                &mut emitted,
                &agent_id,
                &row,
                "peer_version",
                &artifact,
                presence_reference(&row, sequence),
            );
        }
    }

    events.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.subject.peer.cmp(&right.subject.peer))
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.src.artifact.cmp(&right.src.artifact))
    });
    let watermarks = health_fingerprint
        .map(|fingerprint| vec![(HEALTH_PEERS_WATERMARK.to_string(), fingerprint)])
        .unwrap_or_default();
    (events, watermarks)
}

/// The health-peer fingerprint for the `gateway-health.json` on disk.
pub(crate) fn health_peers_watermark(home: &Path) -> Option<u64> {
    let health_path = home.join(HEALTH_ARTIFACT);
    let health = read_json(&health_path).filter(Value::is_object)?;
    Some(health_peers_fingerprint(&health_peer_rows(&health, "")))
}

/// Identity, status and version of each health-derived peer row; traffic
/// timestamps and heartbeats are deliberately left out.
fn health_peers_fingerprint(rows: &[PeerRow]) -> u64 {
    let mut identity = rows
        .iter()
        .map(|row| {
            serde_json::json!([row.peer, row.status, row.version])
        })
        .collect::<Vec<_>>();
    identity.sort_by_key(Value::to_string);
    super::health::fingerprint(&Value::Array(identity))
}

fn health_peer_rows(value: &Value, fallback_timestamp: &str) -> Vec<PeerRow> {
    let mut rows = Vec::new();
    if let Some(links) = value.get("links").and_then(Value::as_array) {
        for link in links {
            let Some(peer) = peer_id(link) else {
                continue;
            };
            rows.push(PeerRow {
                peer: peer.to_string(),
                payload: link.clone(),
                status: Some("peer_up"),
                version: peer_version(link),
                timestamp: source_timestamp_or(
                    link,
                    &["ts", "established_at", "last_rx", "last_tx"],
                    fallback_timestamp,
                ),
            });
        }
    }

    if let Some(peers) = value.get("peers") {
        match peers {
            Value::Array(peers) => {
                for peer_row in peers {
                    let Some(peer) = peer_id(peer_row) else {
                        continue;
                    };
                    rows.push(PeerRow {
                        peer: peer.to_string(),
                        payload: peer_row.clone(),
                        status: explicit_presence_kind(peer_row).or(Some("peer_up")),
                        version: peer_version(peer_row),
                        timestamp: source_timestamp_or(
                            peer_row,
                            &["ts", "last_seen", "last_heartbeat"],
                            fallback_timestamp,
                        ),
                    });
                }
            }
            Value::Object(peers) => {
                for (peer, peer_row) in peers {
                    let payload = peer_payload(peer_row);
                    rows.push(PeerRow {
                        peer: peer.clone(),
                        status: explicit_presence_kind(&payload).or(Some("peer_up")),
                        version: peer_version(&payload),
                        timestamp: source_timestamp_or(
                            &payload,
                            &["ts", "last_seen", "last_heartbeat"],
                            fallback_timestamp,
                        ),
                        payload,
                    });
                }
            }
            _ => {}
        }
    }
    rows
}

fn registry_peer_rows(value: &Value, path: &Path) -> Vec<PeerRow> {
    let peers = value
        .get("gateways")
        .or_else(|| value.get("peers"))
        .and_then(Value::as_object);
    let Some(peers) = peers else {
        return Vec::new();
    };
    peers
        .iter()
        .map(|(peer, source)| {
            let payload = peer_payload(source);
            PeerRow {
                peer: peer.clone(),
                status: explicit_presence_kind(&payload),
                version: peer_version(&payload),
                timestamp: source_timestamp(
                    path,
                    &payload,
                    &["ts", "last_heartbeat", "last_seen"],
                ),
                payload,
            }
        })
        .collect()
}

fn peer_payload(value: &Value) -> Value {
    if value.is_object() {
        return value.clone();
    }
    let mut payload = Map::new();
    if let Some(version) = value.as_str() {
        payload.insert("version".to_string(), Value::String(version.to_string()));
    } else {
        payload.insert("value".to_string(), value.clone());
    }
    Value::Object(payload)
}

fn peer_id(value: &Value) -> Option<&str> {
    ["peer", "peer_id", "peer_gateway_id", "gateway_id", "id"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .filter(|peer| !peer.is_empty())
}

fn peer_version(value: &Value) -> Option<String> {
    value
        .get("version")
        .or_else(|| value.get("peer_version"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|version| !version.is_empty())
        .map(str::to_string)
}

fn explicit_presence_kind(value: &Value) -> Option<&'static str> {
    if value.get("retired").and_then(Value::as_bool) == Some(true)
        || ["revoked_at", "invalid_since", "missing_since", "retired_at"]
            .into_iter()
            .any(|field| value.get(field).is_some_and(|value| !value.is_null()))
    {
        return Some("peer_down");
    }

    for field in ["up", "online", "connected"] {
        if let Some(state) = value.get(field).and_then(Value::as_bool) {
            return Some(if state { "peer_up" } else { "peer_down" });
        }
    }

    let status = ["status", "state", "presence"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))?
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_");
    match status.as_str() {
        "up" | "online" | "active" | "connected" | "running" => Some("peer_up"),
        "down" | "offline" | "disconnected" | "retired" | "stale" | "stopped" => {
            Some("peer_down")
        }
        _ => None,
    }
}

fn presence_artifacts(home: &Path) -> Vec<(String, std::path::PathBuf, Value)> {
    let Ok(entries) = fs::read_dir(home.join(PEERS_DIR)) else {
        return Vec::new();
    };
    let mut paths = entries
        .flatten()
        .filter_map(|entry| {
            if !entry.file_type().ok()?.is_dir() {
                return None;
            }
            let path = entry.path().join("presence.json");
            let value = read_json(&path)?;
            let artifact = format!(
                "{PEERS_DIR}/{}/presence.json",
                entry.file_name().to_string_lossy()
            );
            Some((artifact, path, value))
        })
        .collect::<Vec<_>>();
    paths.sort_by(|left, right| left.0.cmp(&right.0));
    paths
}

fn peer_from_dir_name(name: &str) -> Option<String> {
    let (provider, id) = name.split_once('-')?;
    (!provider.is_empty() && !id.is_empty()).then(|| format!("{provider}:{id}"))
}

fn presence_reference(row: &PeerRow, sequence: Option<u64>) -> String {
    sequence
        .map(|sequence| format!("presence_seq={sequence}"))
        .unwrap_or_else(|| format!("peer={}", row.peer))
}

fn push_event(
    events: &mut Vec<Event>,
    emitted: &mut BTreeSet<(String, String)>,
    agent_id: &str,
    row: &PeerRow,
    kind: &str,
    artifact: &str,
    reference: String,
) {
    let category = if matches!(kind, "peer_up" | "peer_down") {
        "peer_status"
    } else {
        kind
    };
    if !emitted.insert((row.peer.clone(), category.to_string())) {
        return;
    }
    events.push(Event {
        v: 1,
        ts: row.timestamp.clone(),
        agent_id: agent_id.to_string(),
        plane: Plane::Presence,
        kind: kind.to_string(),
        subject: Subject {
            worker_id: None,
            work_id: None,
            peer: Some(row.peer.clone()),
        },
        payload: row.payload.clone(),
        src: Src {
            artifact: artifact.to_string(),
            reference,
        },
    });
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

fn source_timestamp(path: &Path, value: &Value, fields: &[&str]) -> String {
    explicit_timestamp(value, fields)
        .map(str::to_string)
        .or_else(|| modified_timestamp(path))
        .unwrap_or_else(now_timestamp)
}

fn source_timestamp_or(value: &Value, fields: &[&str], fallback: &str) -> String {
    explicit_timestamp(value, fields)
        .map(str::to_string)
        .unwrap_or_else(|| fallback.to_string())
}

fn explicit_timestamp<'a>(value: &'a Value, fields: &[&str]) -> Option<&'a str> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_str))
}

fn modified_timestamp(path: &Path) -> Option<String> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    Some(DateTime::<Utc>::from(modified).to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Newest presence.json modification time across every peer dir (0 if none).
pub(crate) fn peers_tip(home: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(home.join(PEERS_DIR)) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|entry| modified_nanos(&entry.path().join("presence.json")))
        .max()
        .unwrap_or(0)
}

/// Whether one peer's presence changed after `cursor`.
///
/// Current cursors carry one `mesh/peers` mtime watermark. Cursors minted
/// before it carried one sequence watermark per presence file; those still
/// resume with their original per-file semantics for one pass, after which the
/// advanced cursor holds only the bounded watermark.
fn presence_is_new(cursor: &Cursor, artifact: &str, path: &Path, sequence: Option<u64>) -> bool {
    match cursor.watermarks.get(PEERS_WATERMARK) {
        Some(Watermark::Sequence { seq }) => {
            modified_nanos(path).is_none_or(|modified| modified > *seq)
        }
        Some(Watermark::Line { .. }) | None => snapshot_is_new(cursor, artifact, sequence),
    }
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
    use std::fs;

    #[test]
    fn registry_versions_become_one_peer_version_event_per_peer() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("mesh/gateway")).unwrap();
        fs::write(
            tmp.path().join("mesh/gateway-health.json"),
            serde_json::to_vec(&json!({
                "health_seq": 8,
                "written_at": "2026-09-23T04:01:00Z",
                "links": [
                    {"peer": "gw-a", "established_at": "2026-09-23T03:59:00Z"},
                    {"peer": "gw-b", "established_at": "2026-09-23T03:59:30Z"}
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            tmp.path().join("mesh/gateway/registry-cache.json"),
            serde_json::to_vec(&json!({
                "gateways": {
                    "gw-a": {"version": "1.2.3"},
                    "gw-b": {"version": "2.0.0"}
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let versions = normalize(tmp.path(), &Cursor::default())
            .into_iter()
            .filter(|event| event.kind == "peer_version")
            .collect::<Vec<_>>();

        assert_eq!(versions.len(), 2);
        assert!(versions.iter().all(|event| event.plane == Plane::Presence));
        assert_eq!(versions[0].subject.peer.as_deref(), Some("gw-a"));
        assert_eq!(versions[0].payload["version"], "1.2.3");
        assert_eq!(versions[1].subject.peer.as_deref(), Some("gw-b"));
        assert_eq!(versions[1].payload["version"], "2.0.0");
    }

    #[test]
    fn health_peer_rows_map_explicit_up_and_down_states() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("mesh")).unwrap();
        fs::write(
            tmp.path().join("mesh/gateway-health.json"),
            serde_json::to_vec(&json!({
                "health_seq": 9,
                "written_at": "2026-09-23T04:02:00Z",
                "peers": [
                    {"peer": "gw-up", "online": true, "version": "1.0.0"},
                    {"peer": "gw-down", "online": false, "version": "0.9.0"}
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let events = normalize(tmp.path(), &Cursor::default());

        assert!(events.iter().any(|event| {
            event.kind == "peer_up" && event.subject.peer.as_deref() == Some("gw-up")
        }));
        assert!(events.iter().any(|event| {
            event.kind == "peer_down" && event.subject.peer.as_deref() == Some("gw-down")
        }));
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == "peer_version")
                .count(),
            2
        );
    }
}
