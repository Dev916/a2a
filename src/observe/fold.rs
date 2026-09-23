//! Read-only folding of one a2a home into its current observable state.

use crate::observe::normalize::{presence, work};
use crate::observe::snapshot::{
    AgentSummary, HealthView, PeerView, Snapshot, WorkerView, WorkView,
};
use crate::observe::{read_json, read_jsonl, read_jsonl_from, Cursor};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Fold the artifacts currently present under `home` without mutating them.
pub fn snapshot(home: &Path) -> Snapshot {
    // Capture a lower-bound cursor first. If a writer advances while the fold
    // is reading, a later stream may replay an already-reflected event, but it
    // cannot skip an event that was absent from the returned views.
    let cursor = current_cursor(home);
    let workers = worker_views(home);
    let work = work_views(home);
    let peers = peer_views(home);
    let health = health_view(home);

    Snapshot {
        agent: AgentSummary {
            agent_id: agent_id(home),
            online: health
                .as_ref()
                .is_some_and(|health| health.status == "running"),
            worker_count: workers.len(),
            work_count: work.len(),
            peer_count: peers.len(),
        },
        workers,
        work,
        peers,
        health,
        cursor,
    }
}

fn agent_id(home: &Path) -> String {
    read_json(&home.join("mesh/gateway/gw.key"))
        .and_then(|value| {
            value
                .get("gateway_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default()
}

fn worker_views(home: &Path) -> Vec<WorkerView> {
    let mut workers = worker_dirs(home);
    workers.sort_by(|left, right| left.0.cmp(&right.0));
    workers
        .into_iter()
        .map(|(worker_id, worker_dir)| worker_view(&worker_id, &worker_dir))
        .collect()
}

fn worker_dirs(home: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = fs::read_dir(home.join("workers")) else {
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

fn worker_view(worker_id: &str, worker_dir: &Path) -> WorkerView {
    let state = read_json(&worker_dir.join("state.json"));
    let state_name = state
        .as_ref()
        .and_then(|value| value.get("state"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let lane = state
        .as_ref()
        .and_then(|value| value.get("lane"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| {
            worker_id
                .split('-')
                .next()
                .unwrap_or("unknown")
                .to_string()
        });
    let last_delivered_seq = state
        .as_ref()
        .and_then(|value| value.get("last_delivered_seq"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let awaiting_input = matches!(
        state_name.as_str(),
        "awaiting-input" | "awaiting_input" | "escalated"
    )
    .then(|| latest_unanswered_question(worker_dir, last_delivered_seq))
    .flatten();

    WorkerView {
        worker_id: worker_id.to_string(),
        state: state_name,
        lane,
        worktree: state
            .as_ref()
            .and_then(|value| value.get("worktree"))
            .filter(|value| !value.is_null())
            .cloned(),
        max_turns: state
            .as_ref()
            .and_then(|value| value.get("max_turns"))
            .and_then(Value::as_u64),
        turns_this_epoch: state
            .as_ref()
            .and_then(|value| value.get("turns_this_epoch"))
            .and_then(Value::as_u64),
        last_result: read_json(&worker_dir.join("result.json")),
        awaiting_input,
    }
}

fn latest_unanswered_question(worker_dir: &Path, last_delivered_seq: u64) -> Option<Value> {
    let entries = fs::read_dir(worker_dir.join("outbox")).ok()?;
    let mut questions = entries
        .flatten()
        .filter_map(|entry| {
            if !entry.file_type().ok()?.is_file() {
                return None;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".question.json") {
                return None;
            }
            let question = read_json(&entry.path())?;
            let sequence = name
                .split('.')
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| question.get("seq").and_then(Value::as_u64));
            if sequence.is_some_and(|sequence| sequence <= last_delivered_seq) {
                return None;
            }
            Some((sequence.unwrap_or(u64::MAX), name, question))
        })
        .collect::<Vec<_>>();
    questions.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    questions.pop().map(|(_, _, question)| question)
}

fn work_views(home: &Path) -> Vec<WorkView> {
    let mut current = BTreeMap::new();
    for event in work::normalize(home, &Cursor::default()) {
        let Some(work_id) = event.subject.work_id else {
            continue;
        };
        let state = work_state(&event.payload).unwrap_or_else(|| match event.kind.as_str() {
            "work_offered" => "offered".to_string(),
            "work_approved" => "approved".to_string(),
            "work_released" => "running".to_string(),
            "work_done" => "done".to_string(),
            "work_failed" | "work_result_empty" => "failed".to_string(),
            _ => "unknown".to_string(),
        });
        current.insert(work_id.clone(), WorkView { work_id, state });
    }
    current.into_values().collect()
}

fn work_state(value: &Value) -> Option<String> {
    for field in ["to", "to_status", "status", "result_status"] {
        if let Some(state) = value.get(field).and_then(Value::as_str) {
            let normalized = state.trim().to_ascii_lowercase().replace('-', "_");
            return Some(
                normalized
                    .strip_prefix("work_")
                    .unwrap_or(&normalized)
                    .to_string(),
            );
        }
    }
    for field in ["raw", "payload", "data", "body", "work"] {
        if let Some(state) = value.get(field).and_then(work_state) {
            return Some(state);
        }
    }
    None
}

fn peer_views(home: &Path) -> Vec<PeerView> {
    let mut peers: BTreeMap<String, PeerView> = BTreeMap::new();
    for event in presence::normalize(home, &Cursor::default()) {
        let Some(peer) = event.subject.peer else {
            continue;
        };
        let view = peers.entry(peer.clone()).or_insert_with(|| PeerView {
            peer,
            status: "unknown".to_string(),
            version: None,
        });
        match event.kind.as_str() {
            "peer_up" => view.status = "up".to_string(),
            "peer_down" => view.status = "down".to_string(),
            "peer_version" => view.version = peer_version(&event.payload),
            _ => {}
        }
    }
    peers.into_values().collect()
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

fn health_view(home: &Path) -> Option<HealthView> {
    let details = read_json(&home.join("mesh/gateway-health.json"))?;
    if !details.is_object() {
        return None;
    }
    let status = details
        .get("state")
        .or_else(|| details.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    Some(HealthView { status, details })
}

fn current_cursor(home: &Path) -> Cursor {
    let mut cursor = Cursor::default();
    cursor.set(
        "mesh/feed.jsonl",
        sequence_tip(&home.join("mesh/feed.jsonl"), &["origin_seq", "seq"]),
    );
    cursor.set(
        "mesh/events.jsonl",
        sequence_tip(&home.join("mesh/events.jsonl"), &["seq"]),
    );
    set_line_tip(
        &mut cursor,
        home,
        "mesh/gateway/gateway.jsonl",
    );
    set_line_tip(&mut cursor, home, "broker.jsonl");
    set_snapshot_tip(
        &mut cursor,
        home,
        "mesh/gateway-health.json",
        "health_seq",
    );
    set_snapshot_tip(
        &mut cursor,
        home,
        "mesh/gateway/registry-cache.json",
        "registry_seq",
    );

    for (worker_id, worker_dir) in worker_dirs(home) {
        let events_artifact = format!("workers/{worker_id}/events.jsonl");
        cursor.set(
            events_artifact,
            sequence_tip(&worker_dir.join("events.jsonl"), &["seq"]),
        );
        let result_artifact = format!("workers/{worker_id}/result.json");
        cursor.set(
            result_artifact,
            u64::from(read_json(&worker_dir.join("result.json")).is_some()),
        );
    }

    let peers_dir = home.join("mesh/peers");
    if let Ok(entries) = fs::read_dir(peers_dir) {
        for entry in entries.flatten() {
            if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            set_snapshot_tip(
                &mut cursor,
                home,
                &format!("mesh/peers/{name}/presence.json"),
                "presence_seq",
            );
        }
    }
    cursor
}

fn sequence_tip(path: &Path, fields: &[&str]) -> u64 {
    let mut tip = 0;
    for path in [rotated_path(path), path.to_path_buf()] {
        for row in read_jsonl(&path) {
            for field in fields {
                if let Some(sequence) = row.get(*field).and_then(Value::as_u64) {
                    tip = tip.max(sequence);
                    break;
                }
            }
        }
    }
    tip
}

fn rotated_path(path: &Path) -> PathBuf {
    let mut path = path.as_os_str().to_os_string();
    path.push(".1");
    PathBuf::from(path)
}

fn set_line_tip(cursor: &mut Cursor, home: &Path, artifact: &str) {
    let (_, position) = read_jsonl_from(&home.join(artifact), 0);
    cursor.set_line(artifact, 0, position);
}

fn set_snapshot_tip(
    cursor: &mut Cursor,
    home: &Path,
    artifact: &str,
    sequence_field: &str,
) {
    let value = read_json(&home.join(artifact)).filter(Value::is_object);
    let sequence = value
        .as_ref()
        .and_then(|value| value.get(sequence_field))
        .and_then(Value::as_u64)
        .or_else(|| value.as_ref().map(|_| 1))
        .unwrap_or(0);
    cursor.set(artifact, sequence);
}

#[cfg(test)]
mod tests {
    use crate::observe::Observer;
    use serde_json::json;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::SystemTime;

    fn file_stamp(path: &Path) -> (PathBuf, u64, SystemTime) {
        let metadata = fs::metadata(path).unwrap();
        (path.to_path_buf(), metadata.len(), metadata.modified().unwrap())
    }

    #[test]
    fn observer_folds_seeded_home_without_modifying_sources() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path();
        let worker_id = "impl-0923t0100-fold";
        let worker = home.join("workers").join(worker_id);
        fs::create_dir_all(worker.join("outbox")).unwrap();
        fs::create_dir_all(home.join("mesh/gateway")).unwrap();

        let state_path = worker.join("state.json");
        fs::write(
            &state_path,
            serde_json::to_vec(&json!({
                "id": worker_id,
                "state": "done",
                "lane": "impl",
                "worktree": {
                    "path": "/tmp/worktree",
                    "branch": "a2a/impl/test",
                    "base_ref": "main",
                    "repo": "/tmp/repo"
                },
                "max_turns": 8,
                "turns_this_epoch": 2,
                "last_delivered_seq": 0
            }))
            .unwrap(),
        )
        .unwrap();
        let result_path = worker.join("result.json");
        fs::write(
            &result_path,
            serde_json::to_vec(&json!({
                "status": "pass",
                "summary": "folded"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            worker.join("events.jsonl"),
            concat!(
                "{\"seq\":1,\"ts\":\"2026-09-23T01:00:00Z\",\"kind\":\"spawned\",\"raw\":{\"fsm\":\"Spawn\"}}\n",
                "{\"seq\":2,\"ts\":\"2026-09-23T01:00:01Z\",\"kind\":\"state_change\",\"raw\":{\"fsm\":\"ProcessStarted\"}}\n",
                "{\"seq\":3,\"ts\":\"2026-09-23T01:00:02Z\",\"kind\":\"state_change\",\"raw\":{\"fsm\":\"TurnEnded\",\"has_result\":true}}\n"
            ),
        )
        .unwrap();

        let identity_path = home.join("mesh/gateway/gw.key");
        fs::write(
            &identity_path,
            br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
        )
        .unwrap();
        let health_path = home.join("mesh/gateway-health.json");
        fs::write(
            &health_path,
            serde_json::to_vec(&json!({
                "health_seq": 4,
                "written_at": "2026-09-23T01:00:03Z",
                "state": "running",
                "links": [{"peer": "gw-peer", "version": "1.2.3"}],
                "limits": {}
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            home.join("mesh/events.jsonl"),
            b"{\"seq\":5,\"ts\":\"2026-09-23T01:00:04Z\",\"kind\":\"work_transition\",\"work_id\":\"work-1\",\"to\":\"offered\"}\n",
        )
        .unwrap();

        let tracked = [&state_path, &result_path, &identity_path, &health_path];
        let before = tracked.map(|path| file_stamp(path));

        let snapshot = Observer::open(home).snapshot();

        assert_eq!(snapshot.agent.agent_id, "gw-local");
        assert!(snapshot.agent.online);
        assert_eq!(snapshot.agent.worker_count, 1);
        assert_eq!(snapshot.agent.work_count, 1);
        assert_eq!(snapshot.agent.peer_count, 1);
        assert_eq!(snapshot.workers.len(), 1);
        assert_eq!(snapshot.workers[0].worker_id, worker_id);
        assert_eq!(snapshot.workers[0].state, "done");
        assert_eq!(snapshot.workers[0].lane, "impl");
        assert_eq!(snapshot.workers[0].max_turns, Some(8));
        assert_eq!(snapshot.workers[0].turns_this_epoch, Some(2));
        assert_eq!(
            snapshot.workers[0].last_result.as_ref().unwrap()["status"],
            "pass"
        );
        assert!(snapshot.workers[0].awaiting_input.is_none());
        assert_eq!(snapshot.work.len(), 1);
        assert_eq!(snapshot.work[0].state, "offered");
        assert_eq!(snapshot.peers.len(), 1);
        assert_eq!(snapshot.peers[0].peer, "gw-peer");
        assert_eq!(snapshot.peers[0].status, "up");
        assert_eq!(snapshot.peers[0].version.as_deref(), Some("1.2.3"));
        assert_eq!(snapshot.health.as_ref().unwrap().status, "running");
        assert!(!snapshot.cursor.watermarks.is_empty());

        let after = tracked.map(|path| file_stamp(path));
        assert_eq!(before, after);
    }

    #[test]
    fn parked_worker_exposes_only_the_latest_unanswered_question() {
        let tmp = tempfile::tempdir().unwrap();
        let worker = tmp.path().join("workers/impl-parked");
        fs::create_dir_all(worker.join("outbox")).unwrap();
        fs::write(
            worker.join("state.json"),
            br#"{"state":"awaiting-input","lane":"impl","last_delivered_seq":1}"#,
        )
        .unwrap();
        fs::write(
            worker.join("outbox/1.question.json"),
            br#"{"seq":1,"question":"already answered"}"#,
        )
        .unwrap();
        fs::write(
            worker.join("outbox/2.question.json"),
            br#"{"seq":2,"question":"choose A or B"}"#,
        )
        .unwrap();
        fs::create_dir_all(tmp.path().join("mesh")).unwrap();
        fs::write(
            tmp.path().join("mesh/gateway-health.json"),
            b"not json",
        )
        .unwrap();

        let snapshot = Observer::open(tmp.path()).snapshot();

        assert_eq!(snapshot.workers.len(), 1);
        assert_eq!(
            snapshot.workers[0].awaiting_input.as_ref().unwrap()["seq"],
            2
        );
        assert_eq!(
            snapshot.workers[0].awaiting_input.as_ref().unwrap()["question"],
            "choose A or B"
        );
        assert!(snapshot.health.is_none());
    }
}
