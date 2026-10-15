//! Cursor-based replay across every observable source.

use crate::observe::cursor::Watermark;
use crate::observe::normalize::{health, mesh, presence, work, worker};
use crate::observe::source::{read_jsonl, read_jsonl_records_from};
use crate::observe::{Cursor, Event};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Read and normalize every record after `cursor`, then return a cursor at the
/// complete-line tips observed by this pass.
pub(super) fn events_since(home: &Path, cursor: &Cursor) -> (Vec<Event>, Cursor) {
    let (read_cursor, sequence_gap) = replay_cursor(home, cursor);
    let line_gap = line_artifacts(&read_cursor)
        .into_iter()
        .any(|artifact| line_window(home, &artifact, &read_cursor).gap);

    let mut events = Vec::new();
    events.extend(mesh::normalize(home, &read_cursor));
    events.extend(work::normalize(home, &read_cursor));
    events.extend(worker::normalize(home, &read_cursor));
    events.extend(presence::normalize(home, &read_cursor));
    events.extend(health::normalize(home, &read_cursor));
    events.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.src.artifact.cmp(&right.src.artifact))
            .then_with(|| left.src.reference.cmp(&right.src.reference))
            .then_with(|| left.kind.cmp(&right.kind))
    });

    let mut advanced = advanced_cursor(home, cursor);
    advanced.gap = cursor.gap || sequence_gap || line_gap;
    (events, advanced)
}

/// Replace fold's legacy generation zero with a stable identity for each
/// byte-position source. A rename preserves the old file's identity in `.1`,
/// which lets the next read finish that generation before entering the new one.
pub(super) fn stamp_line_generations(home: &Path, cursor: &mut Cursor) {
    let lines = line_artifacts(cursor);
    for artifact in lines {
        let Some(Watermark::Line { position, .. }) = cursor.watermarks.get(&artifact) else {
            continue;
        };
        let position = *position;
        if let Some(generation) = file_generation(&home.join(&artifact)) {
            cursor.set_line(artifact, generation, position);
        }
    }
}

/// Sequence logs retain one renamed generation. Normalizers use this helper so
/// a poll after rotation sees the unread suffix of `.1` as well as the fresh
/// current file; their existing sequence filters suppress already-seen rows.
pub(crate) fn read_retained_jsonl(path: &Path) -> Vec<Value> {
    let mut rows = read_jsonl(&rotated_path(path));
    rows.extend(read_jsonl(path));
    rows
}

/// Read a byte-position source from the generation named by its cursor.
pub(crate) fn read_line_records(
    home: &Path,
    cursor: &Cursor,
    artifact: &str,
) -> Vec<(Value, u64)> {
    line_window(home, artifact, cursor).rows
}

struct LineWindow {
    rows: Vec<(Value, u64)>,
    gap: bool,
}

fn line_window(home: &Path, artifact: &str, cursor: &Cursor) -> LineWindow {
    let current = home.join(artifact);
    let rotated = rotated_path(&current);
    let (current_rows, current_end) = read_jsonl_records_from(&current, 0);
    let (rotated_rows, rotated_end) = read_jsonl_records_from(&rotated, 0);
    let current_generation = file_generation(&current);
    let rotated_generation = file_generation(&rotated);

    let Some(Watermark::Line {
        generation,
        position,
    }) = cursor.watermarks.get(artifact)
    else {
        return LineWindow {
            rows: joined(rotated_rows, current_rows),
            gap: false,
        };
    };

    if *generation != 0 && current_generation == Some(*generation) {
        if *position <= current_end {
            return LineWindow {
                rows: read_jsonl_records_from(&current, *position).0,
                gap: false,
            };
        }
        return LineWindow {
            rows: current_rows,
            gap: true,
        };
    }

    if *generation != 0 && rotated_generation == Some(*generation) {
        if *position <= rotated_end {
            return LineWindow {
                rows: joined(
                    read_jsonl_records_from(&rotated, *position).0,
                    current_rows,
                ),
                gap: false,
            };
        }
        return LineWindow {
            rows: joined(rotated_rows, current_rows),
            gap: true,
        };
    }

    // Generation zero is the cursor shape emitted before the tailer stamps a
    // snapshot. Preserve its byte-watermark behavior for compatibility.
    if *generation == 0 {
        if *position <= current_end {
            return LineWindow {
                rows: read_jsonl_records_from(&current, *position).0,
                gap: false,
            };
        }
        if *position <= rotated_end && rotated_generation.is_some() {
            return LineWindow {
                rows: joined(
                    read_jsonl_records_from(&rotated, *position).0,
                    current_rows,
                ),
                gap: false,
            };
        }
    }

    LineWindow {
        rows: joined(rotated_rows, current_rows),
        gap: current_generation.is_some() || rotated_generation.is_some(),
    }
}

fn joined(
    mut first: Vec<(Value, u64)>,
    second: Vec<(Value, u64)>,
) -> Vec<(Value, u64)> {
    first.extend(second);
    first
}

fn replay_cursor(home: &Path, cursor: &Cursor) -> (Cursor, bool) {
    let mut replay = cursor.clone();
    let mut gap = false;
    for artifact in sequence_artifacts(home, cursor) {
        let path = home.join(&artifact);
        if !path.exists() && !rotated_path(&path).exists() {
            continue;
        }
        let rows = read_retained_jsonl(&path);
        let requested = match cursor.watermarks.get(&artifact) {
            Some(Watermark::Sequence { seq }) => *seq,
            _ => 0,
        };
        let sequences = rows
            .iter()
            .filter_map(|row| source_sequence(&artifact, row))
            .collect::<Vec<_>>();
        let earliest = sequences.iter().copied().min();
        let latest = sequences.iter().copied().max();
        let compacted = artifact == "mesh/feed.jsonl"
            && rows
                .iter()
                .any(|row| feed_compacted_past(row, requested));
        let missing_prefix = earliest
            .is_some_and(|earliest| requested.saturating_add(1) < earliest);
        let reset = latest.is_some_and(|latest| requested > latest);

        if compacted || missing_prefix || reset {
            gap = true;
            let resume = if compacted {
                0
            } else {
                earliest.unwrap_or(1).saturating_sub(1)
            };
            replay.set(artifact, resume);
        }
    }
    (replay, gap)
}

fn advanced_cursor(home: &Path, cursor: &Cursor) -> Cursor {
    let mut observed = super::fold::snapshot(home).cursor;
    stamp_line_generations(home, &mut observed);
    let mut advanced = cursor.clone();
    // Legacy per-peer presence watermarks are superseded by `mesh/peers`.
    advanced
        .watermarks
        .retain(|artifact, _| !is_legacy_presence_watermark(artifact));
    for (artifact, watermark) in observed.watermarks {
        let path = home.join(&artifact);
        let retained_exists = path.exists() || rotated_path(&path).exists();
        if retained_exists || !advanced.watermarks.contains_key(&artifact) {
            advanced.watermarks.insert(artifact, watermark);
        }
    }
    advanced
}

fn is_legacy_presence_watermark(artifact: &str) -> bool {
    artifact.starts_with("mesh/peers/") && artifact.ends_with("/presence.json")
}

fn sequence_artifacts(home: &Path, cursor: &Cursor) -> BTreeSet<String> {
    let mut artifacts = BTreeSet::from([
        "mesh/feed.jsonl".to_string(),
        "mesh/events.jsonl".to_string(),
    ]);
    if let Ok(entries) = fs::read_dir(home.join("workers")) {
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                artifacts.insert(format!(
                    "workers/{}/events.jsonl",
                    entry.file_name().to_string_lossy()
                ));
            }
        }
    }
    for artifact in cursor.watermarks.keys() {
        if artifact.starts_with("workers/") && artifact.ends_with("/events.jsonl") {
            artifacts.insert(artifact.clone());
        }
    }
    artifacts
}

fn line_artifacts(cursor: &Cursor) -> Vec<String> {
    cursor
        .watermarks
        .iter()
        .filter_map(|(artifact, watermark)| match watermark {
            Watermark::Line { .. } => Some(artifact.clone()),
            Watermark::Sequence { .. } => None,
        })
        .collect()
}

fn source_sequence(artifact: &str, row: &Value) -> Option<u64> {
    if artifact == "mesh/feed.jsonl" {
        row.get("origin_seq")
            .and_then(Value::as_u64)
            .or_else(|| row.get("seq").and_then(Value::as_u64))
    } else {
        row.get("seq").and_then(Value::as_u64)
    }
}

fn feed_compacted_past(row: &Value, requested: u64) -> bool {
    if row.get("kind").and_then(Value::as_str) != Some("feed_compacted") {
        return false;
    }
    row.get("compacted_before")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(_, sequence)| sequence.as_u64())
        .any(|sequence| requested.saturating_add(1) < sequence)
}

fn rotated_path(path: &Path) -> PathBuf {
    let mut rotated = path.as_os_str().to_os_string();
    rotated.push(".1");
    PathBuf::from(rotated)
}

#[cfg(unix)]
fn file_generation(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::metadata(path).ok()?;
    let generation = metadata
        .dev()
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(metadata.ino());
    Some(generation.max(1))
}

#[cfg(not(unix))]
fn file_generation(path: &Path) -> Option<u64> {
    use std::time::UNIX_EPOCH;

    let metadata = fs::metadata(path).ok()?;
    let created = metadata.created().ok()?.duration_since(UNIX_EPOCH).ok()?;
    Some((created.as_secs() ^ u64::from(created.subsec_nanos())).max(1))
}

#[cfg(test)]
mod tests {
    use crate::observe::Observer;
    use serde_json::{json, Value};
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::Path;

    fn seed_identity(home: &Path) {
        fs::create_dir_all(home.join("mesh/gateway")).unwrap();
        fs::write(
            home.join("mesh/gateway/gw.key"),
            br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
        )
        .unwrap();
    }

    fn worker_event(seq: u64, ts: &str, kind: &str, raw: Value) -> String {
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "schema_version": 1,
                "seq": seq,
                "ts": ts,
                "epoch": 1,
                "class": "lifecycle",
                "kind": kind,
                "transport": "exec",
                "raw": raw
            }))
            .unwrap()
        )
    }

    fn append(path: &Path, row: &str) {
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(row.as_bytes()).unwrap();
    }

    #[test]
    fn events_since_merges_new_sources_and_advances_the_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_identity(home);
        fs::create_dir_all(home.join("mesh")).unwrap();
        fs::write(
            home.join("mesh/feed.jsonl"),
            concat!(
                "{\"seq\":1,\"origin\":\"gw-local\",\"origin_seq\":1,",
                "\"ts\":\"2026-09-23T05:00:00Z\",\"text\":\"old\"}\n"
            ),
        )
        .unwrap();
        let worker_id = "impl-0923t0500-tail";
        let worker = home.join("workers").join(worker_id);
        fs::create_dir_all(&worker).unwrap();
        fs::write(
            worker.join("events.jsonl"),
            worker_event(
                1,
                "2026-09-23T05:00:00Z",
                "spawned",
                json!({"fsm": "Spawn", "id": worker_id}),
            ),
        )
        .unwrap();

        let observer = Observer::open(home);
        let c0 = observer.snapshot().cursor;
        append(
            &home.join("mesh/feed.jsonl"),
            concat!(
                "{\"seq\":2,\"origin\":\"gw-local\",\"origin_seq\":2,",
                "\"ts\":\"2026-09-23T05:00:02Z\",\"text\":\"new\"}\n"
            ),
        );
        append(
            &worker.join("events.jsonl"),
            &worker_event(
                2,
                "2026-09-23T05:00:01Z",
                "state_change",
                json!({"fsm": "ProcessStarted", "pid": 42}),
            ),
        );
        let feed_before = fs::read(home.join("mesh/feed.jsonl")).unwrap();
        let worker_before = fs::read(worker.join("events.jsonl")).unwrap();

        let (events, c1) = observer.events_since(&c0);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "worker_status");
        assert_eq!(events[0].subject.worker_id.as_deref(), Some(worker_id));
        assert_eq!(events[1].kind, "feed_entry");
        assert!(events[0].ts < events[1].ts);
        let (replayed, c2) = observer.events_since(&c1);
        assert!(replayed.is_empty());
        assert_eq!(c2, c1);
        assert_eq!(fs::read(home.join("mesh/feed.jsonl")).unwrap(), feed_before);
        assert_eq!(
            fs::read(worker.join("events.jsonl")).unwrap(),
            worker_before
        );
    }

    #[test]
    fn worker_rotation_replays_the_retained_tail_once() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_identity(home);
        let worker_id = "impl-0923t0501-rotate";
        let worker = home.join("workers").join(worker_id);
        fs::create_dir_all(&worker).unwrap();
        let events_path = worker.join("events.jsonl");
        fs::write(
            &events_path,
            worker_event(
                1,
                "2026-09-23T05:01:00Z",
                "spawned",
                json!({"fsm": "Spawn", "id": worker_id}),
            ),
        )
        .unwrap();
        let observer = Observer::open(home);
        let c0 = observer.snapshot().cursor;

        append(
            &events_path,
            &worker_event(
                2,
                "2026-09-23T05:01:01Z",
                "state_change",
                json!({"fsm": "ProcessStarted", "pid": 7}),
            ),
        );
        fs::rename(&events_path, worker.join("events.jsonl.1")).unwrap();
        fs::write(
            &events_path,
            format!(
                "{}{}",
                worker_event(
                    3,
                    "2026-09-23T05:01:02Z",
                    "events_rotated",
                    json!({
                        "last_rotated_seq": 2,
                        "discarded_prior_generation": false
                    }),
                ),
                worker_event(
                    4,
                    "2026-09-23T05:01:03Z",
                    "interrupted",
                    json!({"fsm": "Interrupt"}),
                )
            ),
        )
        .unwrap();

        let (events, c1) = observer.events_since(&c0);

        assert_eq!(
            events
                .iter()
                .map(|event| event.src.reference.as_str())
                .collect::<Vec<_>>(),
            vec!["seq=2", "seq=4"]
        );
        assert!(!c1.gap);
        assert!(observer.events_since(&c1).0.is_empty());
    }

    #[test]
    fn cursor_older_than_retained_worker_history_sets_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_identity(home);
        let worker_id = "impl-0923t0502-gap";
        let worker = home.join("workers").join(worker_id);
        fs::create_dir_all(&worker).unwrap();
        fs::write(
            worker.join("events.jsonl.1"),
            worker_event(
                5,
                "2026-09-23T05:02:00Z",
                "spawned",
                json!({"fsm": "Spawn", "id": worker_id}),
            ),
        )
        .unwrap();
        fs::write(
            worker.join("events.jsonl"),
            format!(
                "{}{}",
                worker_event(
                    6,
                    "2026-09-23T05:02:01Z",
                    "events_rotated",
                    json!({
                        "last_rotated_seq": 5,
                        "discarded_prior_generation": true
                    }),
                ),
                worker_event(
                    7,
                    "2026-09-23T05:02:02Z",
                    "state_change",
                    json!({"fsm": "ProcessStarted"}),
                )
            ),
        )
        .unwrap();
        let artifact = format!("workers/{worker_id}/events.jsonl");
        let mut old = crate::observe::Cursor::default();
        old.set(&artifact, 1);

        let (events, advanced) = Observer::open(home).events_since(&old);

        assert_eq!(
            events
                .iter()
                .map(|event| event.src.reference.as_str())
                .collect::<Vec<_>>(),
            vec!["seq=5", "seq=7"]
        );
        assert!(advanced.gap);
    }

    #[test]
    fn line_generation_finishes_rotated_broker_before_current_file() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_identity(home);
        let broker = home.join("broker.jsonl");
        let first = concat!(
            "{\"ts\":\"2026-09-23T05:03:00Z\",\"kind\":\"work_transition\",",
            "\"work_id\":\"work-old\",\"to\":\"offered\"}\n"
        );
        fs::write(&broker, first).unwrap();
        let observer = Observer::open(home);
        let c0 = observer.snapshot().cursor;
        append(
            &broker,
            concat!(
                "{\"ts\":\"2026-09-23T05:03:01Z\",\"kind\":\"work_transition\",",
                "\"work_id\":\"work-middle\",\"to\":\"approved\"}\n"
            ),
        );
        fs::rename(&broker, home.join("broker.jsonl.1")).unwrap();
        fs::write(
            &broker,
            concat!(
                "{\"ts\":\"2026-09-23T05:03:02Z\",\"kind\":\"work_transition\",",
                "\"work_id\":\"work-new\",\"to\":\"done\"}\n"
            ),
        )
        .unwrap();

        let (events, c1) = observer.events_since(&c0);

        assert_eq!(
            events
                .iter()
                .map(|event| event.subject.work_id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            vec!["work-middle", "work-new"]
        );
        assert!(!c1.gap);
        let crate::observe::cursor::Watermark::Line {
            generation: old_generation,
            ..
        } = c0.watermarks.get("broker.jsonl").unwrap()
        else {
            panic!("broker cursor must be a line watermark");
        };
        let crate::observe::cursor::Watermark::Line {
            generation: new_generation,
            ..
        } = c1.watermarks.get("broker.jsonl").unwrap()
        else {
            panic!("broker cursor must remain a line watermark");
        };
        assert_ne!(old_generation, new_generation);
        assert!(observer.events_since(&c1).0.is_empty());
    }

    #[test]
    fn compacted_feed_replays_earliest_retained_entry_and_sets_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_identity(home);
        let feed = home.join("mesh/feed.jsonl");
        fs::write(
            &feed,
            concat!(
                "{\"seq\":1,\"origin\":\"gw-local\",\"origin_seq\":1,",
                "\"ts\":\"2026-09-23T05:04:00Z\",\"text\":\"old\"}\n"
            ),
        )
        .unwrap();
        let observer = Observer::open(home);
        let c0 = observer.snapshot().cursor;
        fs::write(
            &feed,
            concat!(
                "{\"seq\":5,\"origin\":\"gw-local\",\"origin_seq\":5,",
                "\"ts\":\"2026-09-23T05:04:05Z\",\"text\":\"earliest\"}\n",
                "{\"seq\":6,\"kind\":\"feed_compacted\",",
                "\"ts\":\"2026-09-23T05:04:06Z\",",
                "\"compacted_before\":{\"gw-local\":5}}\n"
            ),
        )
        .unwrap();

        let (events, advanced) = observer.events_since(&c0);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].src.reference, "origin_seq=5");
        assert_eq!(events[0].payload["text"], "earliest");
        assert!(advanced.gap);
        assert!(observer.events_since(&advanced).0.is_empty());
    }

    fn write_presence(home: &Path, dir: &str, body: &str) -> std::path::PathBuf {
        let peer_dir = home.join("mesh/peers").join(dir);
        fs::create_dir_all(&peer_dir).unwrap();
        let path = peer_dir.join("presence.json");
        fs::write(&path, body).unwrap();
        path
    }

    fn set_mtime(path: &Path, when: std::time::SystemTime) {
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(when)
            .unwrap();
    }

    fn presence_peers(events: &[crate::observe::Event]) -> Vec<String> {
        let mut peers = events
            .iter()
            .filter(|event| event.src.artifact.starts_with("mesh/peers/"))
            .filter_map(|event| event.subject.peer.clone())
            .collect::<Vec<_>>();
        peers.dedup();
        peers
    }

    #[test]
    fn cursor_does_not_scale_with_peer_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_identity(home);
        for index in 0..3000 {
            write_presence(
                home,
                &format!("claude-{index:05}"),
                &format!(r#"{{"peer":"claude:{index:05}","presence_seq":{index},"retired":true}}"#),
            );
        }
        let workers = 2;
        for index in 0..workers {
            let worker = home.join("workers").join(format!("impl-{index}"));
            fs::create_dir_all(&worker).unwrap();
            fs::write(worker.join("state.json"), br#"{"state":"done"}"#).unwrap();
        }

        let snapshot = Observer::open(home).snapshot();
        let watermarks = &snapshot.cursor.watermarks;
        assert!(
            watermarks.len() <= 10 + 2 * workers,
            "cursor must be bounded, got {} watermarks",
            watermarks.len()
        );
        assert!(watermarks.contains_key("mesh/peers"));
        assert!(!watermarks.keys().any(|key| key.starts_with("mesh/peers/")));
        assert!(snapshot.cursor.encode().len() < 2_000);

        let (_, advanced) = Observer::open(home).events_since(&snapshot.cursor);
        assert!(advanced.watermarks.len() <= 10 + 2 * workers);
    }

    #[test]
    fn presence_events_follow_the_single_peers_watermark() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_identity(home);
        let past = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        let a = write_presence(home, "claude-a", r#"{"peer":"claude:a","presence_seq":1}"#);
        let b = write_presence(home, "claude-b", r#"{"peer":"claude:b","presence_seq":2}"#);
        set_mtime(&a, past);
        set_mtime(&b, past);
        let observer = Observer::open(home);
        let c0 = observer.snapshot().cursor;
        assert!(observer.events_since(&c0).0.is_empty());

        fs::write(&a, r#"{"peer":"claude:a","presence_seq":3,"retired":true}"#).unwrap();
        set_mtime(&a, std::time::SystemTime::now() + std::time::Duration::from_secs(1));
        let (events, c1) = observer.events_since(&c0);

        assert_eq!(presence_peers(&events), vec!["claude:a".to_string()]);
        let down = events.iter().find(|event| event.kind == "peer_down").unwrap();
        assert_eq!(down.subject.peer.as_deref(), Some("claude:a"));
        assert!(observer.events_since(&c1).0.is_empty());
    }

    #[test]
    fn legacy_per_peer_cursor_still_decodes_and_is_folded_away() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        seed_identity(home);
        write_presence(home, "claude-a", r#"{"peer":"claude:a","presence_seq":3}"#);
        write_presence(home, "claude-b", r#"{"peer":"claude:b","presence_seq":5}"#);
        let mut legacy = crate::observe::Cursor::default();
        legacy.set("mesh/peers/claude-a/presence.json", 3);
        legacy.set("mesh/peers/claude-b/presence.json", 4);
        let token = legacy.encode();

        let decoded = crate::observe::Cursor::decode(&token).expect("legacy cursor decodes");
        let observer = Observer::open(home);
        let (events, advanced) = observer.events_since(&decoded);

        assert_eq!(presence_peers(&events), vec!["claude:b".to_string()]);
        assert!(advanced.watermarks.contains_key("mesh/peers"));
        assert!(
            !advanced.watermarks.keys().any(|key| key.starts_with("mesh/peers/")),
            "legacy per-peer keys are dropped once folded: {:?}",
            advanced.watermarks.keys().collect::<Vec<_>>()
        );
        assert!(!advanced.gap);
        assert!(observer.events_since(&advanced).0.is_empty());
    }
}
