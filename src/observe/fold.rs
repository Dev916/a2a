//! Read-only folding of one a2a home into its current observable state.

use crate::observe::normalize::{presence, work};
use crate::observe::snapshot::{
    AgentSummary, HealthView, PeerView, SessionView, Snapshot, WorkerView, WorkView,
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
    let agent_id = agent_id(home);
    let sessions = session_views(home, &agent_id, &workers);

    Snapshot {
        agent: AgentSummary {
            agent_id,
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
        sessions,
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
    let text = |key: &str| {
        state
            .as_ref()
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .map(str::to_string)
    };

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
        session_id: text("session_id").or_else(|| text("thread_id")),
        pid: state
            .as_ref()
            .and_then(|value| value.get("pid"))
            .and_then(Value::as_u64),
        model: text("model"),
        effort: text("effort"),
        transport: text("transport"),
        cwd: text("cwd"),
    }
}

/// Sessions seen within this window are "active"; older ones are "stale".
/// Matches the default mesh `stale_after_secs`.
const SESSION_STALE_AFTER_SECS: i64 = 900;
/// Sessions not seen for a day are noise and are left out entirely.
const SESSION_HIDE_AFTER_SECS: i64 = 24 * 60 * 60;

fn pid_alive(pid: u64) -> Option<bool> {
    let pid = i32::try_from(pid).ok().filter(|pid| *pid > 0)?;
    // Signal 0: existence check only, nothing is delivered. EPERM means the
    // process exists but belongs to someone else.
    Some(matches!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    ))
}

fn session_views(home: &Path, agent_id: &str, workers: &[WorkerView]) -> Vec<SessionView> {
    let Ok(entries) = fs::read_dir(home.join("mesh/peers")) else {
        return Vec::new();
    };
    let now = chrono::Utc::now();
    let mut sessions = entries
        .flatten()
        .filter_map(|entry| {
            let p = read_json(&entry.path().join("presence.json"))?;
            let text = |key: &str| p.get(key).and_then(Value::as_str).map(str::to_string);
            if p.get("retired").and_then(Value::as_bool) == Some(true) {
                return None;
            }
            // Presence replicated from other gateways is not a session of THIS agent.
            if text("home_gateway").is_some_and(|gateway| gateway != agent_id) {
                return None;
            }
            let peer = text("peer")?;
            let (provider, session_id) = peer.split_once(':')?;
            let last_seen = text("last_seen")?;
            let age = chrono::DateTime::parse_from_rfc3339(&last_seen)
                .map(|seen| (now - seen.with_timezone(&chrono::Utc)).num_seconds())
                .unwrap_or(i64::MAX);
            if age >= SESSION_HIDE_AFTER_SECS {
                return None;
            }
            let worker_id = text("worker_id");
            // A codex worker's pid lives in its worker state, not in presence.
            let pid = p.get("pid").and_then(Value::as_u64).or_else(|| {
                let id = worker_id.as_deref()?;
                workers.iter().find(|w| w.worker_id == id)?.pid
            });
            Some(SessionView {
                provider: text("provider").unwrap_or_else(|| provider.to_string()),
                session_id: session_id.to_string(),
                peer: peer.clone(),
                pid,
                pid_alive: pid.and_then(pid_alive),
                cwd: text("cwd"),
                worker_id,
                status: text("status"),
                working_on: text("working_on"),
                label: if age >= SESSION_STALE_AFTER_SECS { "stale" } else { "active" }
                    .to_string(),
                last_seen,
            })
        })
        .collect::<Vec<_>>();
    sessions.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
    sessions
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

    #[test]
    fn workers_expose_session_id_pid_and_run_parameters() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("workers/impl-1");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("state.json"),
            r#"{"state":"running","lane":"impl","session_id":"01a10203-869c-7ff1-8036-92650cc0c4da",
                "pid":13127,"model":"gpt-5.6-sol","effort":"max","transport":"app-server","cwd":"/w"}"#,
        )
        .unwrap();
        let snapshot = super::snapshot(tmp.path());
        let w = &snapshot.workers[0];
        assert_eq!(w.session_id.as_deref(), Some("01a10203-869c-7ff1-8036-92650cc0c4da"));
        assert_eq!(w.pid, Some(13127));
        assert_eq!(w.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(w.effort.as_deref(), Some("max"));
        assert_eq!(w.transport.as_deref(), Some("app-server"));
        assert_eq!(w.cwd.as_deref(), Some("/w"));
    }

    #[test]
    fn worker_session_id_falls_back_to_thread_id_and_absent_fields_stay_none() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("workers/impl-2");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("state.json"), r#"{"state":"done","thread_id":"t-123"}"#).unwrap();
        let snapshot = super::snapshot(tmp.path());
        let w = &snapshot.workers[0];
        assert_eq!(w.session_id.as_deref(), Some("t-123"));
        assert_eq!(w.pid, None);
        assert_eq!(w.model, None);
        let json = serde_json::to_value(w).unwrap();
        assert!(json.get("pid").is_none(), "absent optional fields are not serialized: {json}");
    }

    #[test]
    fn sessions_list_local_live_peers_with_session_id_and_pid() {
        let tmp = tempfile::tempdir().unwrap();
        let now = chrono::Utc::now().to_rfc3339();
        let peer = |name: &str, body: String| {
            let dir = tmp.path().join("mesh/peers").join(name);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("presence.json"), body).unwrap();
        };
        let me = std::process::id();
        peer("claude-aaa", format!(r#"{{"peer":"claude:aaa","provider":"claude","retired":false,"last_seen":"{now}","pid":{me},"cwd":"/proj","working_on":"plan"}}"#));
        peer("claude-old", r#"{"peer":"claude:old","provider":"claude","retired":false,"last_seen":"2026-01-01T00:00:00Z"}"#.to_string());
        peer("claude-gone", format!(r#"{{"peer":"claude:gone","provider":"claude","retired":true,"last_seen":"{now}"}}"#));
        peer("claude-far", format!(r#"{{"peer":"claude:far","provider":"claude","retired":false,"last_seen":"{now}","home_gateway":"gw-somewhere-else"}}"#));
        peer("broken", "{not json".to_string());

        let sessions = super::snapshot(tmp.path()).sessions;
        assert_eq!(sessions.len(), 1, "retired, remote-homed, long-stale and corrupt peers are excluded: {sessions:?}");
        let s = &sessions[0];
        assert_eq!(s.session_id, "aaa");
        assert_eq!(s.provider, "claude");
        assert_eq!(s.pid, Some(u64::from(me)));
        assert_eq!(s.pid_alive, Some(true));
        assert_eq!(s.cwd.as_deref(), Some("/proj"));
        assert_eq!(s.working_on.as_deref(), Some("plan"));
        assert_eq!(s.label, "active");
    }

    #[test]
    fn sessions_keep_locally_homed_peers_and_borrow_codex_worker_pids() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("mesh/gateway")).unwrap();
        fs::write(
            tmp.path().join("mesh/gateway/gw.key"),
            br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
        )
        .unwrap();
        let worker = tmp.path().join("workers/impl-9");
        fs::create_dir_all(&worker).unwrap();
        fs::write(worker.join("state.json"), r#"{"state":"running","pid":4242}"#).unwrap();
        let stale = (chrono::Utc::now() - chrono::Duration::seconds(3600)).to_rfc3339();
        let dir = tmp.path().join("mesh/peers/codex-ccc");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("presence.json"),
            format!(r#"{{"peer":"codex:ccc","provider":"codex","retired":false,"last_seen":"{stale}","home_gateway":"gw-local","worker_id":"impl-9"}}"#),
        )
        .unwrap();

        let sessions = super::snapshot(tmp.path()).sessions;
        assert_eq!(sessions.len(), 1, "{sessions:?}");
        assert_eq!(sessions[0].provider, "codex");
        assert_eq!(sessions[0].worker_id.as_deref(), Some("impl-9"));
        assert_eq!(sessions[0].pid, Some(4242));
        assert_eq!(sessions[0].label, "stale");
    }
}
