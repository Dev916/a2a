mod common;

use a2a::home::A2aHome;
use a2a::mesh::gateway::presence_wire;
use a2a::mesh::gateway::replicate::CursorStore;
use a2a::mesh::registry::{GatewayRow, REGISTRY_SCHEMA_VERSION};
use a2a::mesh::{PeerId, Presence};
use chrono::{Duration, TimeZone, Utc};
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn write_gateway_key(home: &Path, gateway_id: &str) {
    let dir = home.join("mesh").join("gateway");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("gw.key"),
        serde_json::to_vec(&json!({
            "gateway_id": gateway_id,
            "secret_b64": "test-only-not-a-real-key"
        }))
        .unwrap(),
    )
    .unwrap();
    fs::set_permissions(dir.join("gw.key"), fs::Permissions::from_mode(0o600)).unwrap();
}

fn write_presence(home: &Path, peer: &str, days_old: i64, home_gateway: Option<&str>) {
    let id = PeerId::parse(peer).unwrap();
    let dir = home.join("mesh").join("peers").join(id.dir_name());
    fs::create_dir_all(&dir).unwrap();
    let now = Utc::now();
    let mut value = json!({
        "peer": peer,
        "provider": id.provider.to_string(),
        "worker_id": null,
        "status": null,
        "working_on": null,
        "cwd": null,
        "registered_at": now - Duration::days(days_old),
        "last_seen": now - Duration::days(days_old),
        "retired": false,
        "presence_seq": 1,
        "cursors": {"feed_seq": 0, "inbox": null}
    });
    if let Some(gateway) = home_gateway {
        value["home_gateway"] = json!(gateway);
    }
    fs::write(
        dir.join("presence.json"),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
}

fn registry_row(gateway_id: &str, last_heartbeat: chrono::DateTime<Utc>) -> GatewayRow {
    GatewayRow {
        fleet_id: "fleet-a".to_string(),
        gateway_id: gateway_id.to_string(),
        owner: gateway_id.to_string(),
        endpoint_url: Some(format!("wss://127.0.0.1/{gateway_id}")),
        pubkey_fpr: format!("{gateway_id}-pub"),
        cert_fpr: Some(format!("{gateway_id}-cert")),
        version: "1".to_string(),
        created_at: last_heartbeat,
        last_heartbeat,
        revoked_at: None,
        schema_version: REGISTRY_SCHEMA_VERSION,
    }
}

fn spooled_note(home: &Path, gateway: &str, name: &str) {
    let dir = home.join("mesh").join("outbound").join(gateway);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(name), b"{\"trace_id\":\"test\"}\n").unwrap();
}

#[test]
fn presence_is_layer1_compatible_and_tuple_lww_counter_beats_clock_skew() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    let peer = PeerId::parse("codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let old_seen = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let older_clock = old_seen - Duration::hours(1);

    let legacy: Presence = serde_json::from_value(json!({
        "peer": peer.to_string(),
        "provider": "codex",
        "worker_id": null,
        "status": "legacy",
        "working_on": null,
        "cwd": null,
        "registered_at": old_seen,
        "last_seen": old_seen,
        "retired": false,
        "cursors": {"feed_seq": 0, "inbox": null},
        "future": true
    }))
    .unwrap();
    assert_eq!(legacy.presence_seq, 0);
    assert_eq!(legacy.home_gateway, None);
    assert_eq!(legacy.extra["future"], true);

    let mut cached = legacy.clone();
    cached.presence_seq = 7;
    cached.home_gateway = Some("gw-a".to_string());
    a2a::mesh::presence::save(&home, &cached).unwrap();

    let mut incoming = cached.clone();
    incoming.presence_seq = 8;
    incoming.last_seen = older_clock;
    incoming.status = Some("newer despite backward clock".to_string());
    assert!(presence_wire::merge_presence(&home, incoming, "gw-a").unwrap());
    let merged = a2a::mesh::presence::load(&home, &peer).unwrap().unwrap();
    assert_eq!(
        merged.status.as_deref(),
        Some("newer despite backward clock")
    );
    assert_eq!(merged.presence_seq, 8);

    let mut stale_by_counter = merged.clone();
    stale_by_counter.presence_seq = 7;
    stale_by_counter.last_seen = old_seen + Duration::hours(2);
    stale_by_counter.status = Some("wall clock alone must not win".to_string());
    assert!(!presence_wire::merge_presence(&home, stale_by_counter, "gw-a").unwrap());
    let kept = a2a::mesh::presence::load(&home, &peer).unwrap().unwrap();
    assert_eq!(kept.status.as_deref(), Some("newer despite backward clock"));

    let mut tie_breaker = kept.clone();
    tie_breaker.home_gateway = Some("gw-b".to_string());
    tie_breaker.status = Some("gateway tie breaker".to_string());
    assert!(presence_wire::merge_presence(&home, tie_breaker, "gw-b").unwrap());
    let winner = a2a::mesh::presence::load(&home, &peer).unwrap().unwrap();
    assert_eq!(winner.home_gateway.as_deref(), Some("gw-b"));
    assert_eq!(winner.status.as_deref(), Some("gateway tie breaker"));
}

#[test]
fn local_presence_changes_persist_counter_and_gossip_frame_sets_home_gateway() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    let peer = PeerId::parse("claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    write_gateway_key(tmp.path(), "gw-a");
    write_presence(tmp.path(), &peer.to_string(), 0, None);

    let first = presence_wire::record_local_change(&home, &peer, now, "gw-a").unwrap();
    let second =
        presence_wire::record_local_change(&home, &peer, now + Duration::seconds(1), "gw-a")
            .unwrap();
    assert_eq!(first.presence_seq, 2);
    assert_eq!(second.presence_seq, 3);
    assert_eq!(second.home_gateway.as_deref(), Some("gw-a"));

    let frame = presence_wire::gossip_frame(&home, "gw-a").unwrap();
    let a2a::mesh::wire::Frame::Presence { peers } = frame else {
        panic!("gossip frame is presence");
    };
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].home_gateway.as_deref(), Some("gw-a"));
    assert_eq!(peers[0].presence_seq, 3);
}

#[test]
fn roster_renders_remote_peers_via_gateway_and_json_carries_home_gateway() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    write_gateway_key(&home, "gw-a");
    write_presence(
        &home,
        "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        0,
        Some("gw-b"),
    );

    let human = common::a2a(&home, &work)
        .args(["mesh", "roster"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let human = String::from_utf8(human).unwrap();
    assert!(human.contains(" via gw-b "), "{human}");

    let json = common::a2a(&home, &work)
        .args(["mesh", "roster", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let value: Value = serde_json::from_slice(&json).unwrap();
    let row = &value["data"][0];
    assert_eq!(row["home_gateway"], "gw-b");
}

#[test]
fn gc_keeps_remote_homed_peer_until_home_gateway_is_registry_retired_then_archives_origin() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    write_gateway_key(&home, "gw-a");
    write_presence(
        &home,
        "claude:11111111-1111-4111-8111-111111111111",
        8,
        None,
    );
    write_presence(
        &home,
        "codex:22222222-2222-4222-8222-222222222222",
        8,
        Some("gw-b"),
    );
    write_presence(
        &home,
        "codex:33333333-3333-4333-8333-333333333333",
        8,
        Some("gw-c"),
    );
    let a2a_home = home_at(&home);
    let now = Utc::now();
    presence_wire::refresh_registry_cache(
        &a2a_home,
        &[
            registry_row("gw-b", now - Duration::days(8)),
            registry_row("gw-c", now),
        ],
        now - Duration::minutes(1),
    )
    .unwrap();
    presence_wire::refresh_registry_cache(&a2a_home, &[registry_row("gw-c", now)], now).unwrap();

    let mut cursors = CursorStore::load_or_rebuild(&a2a_home).unwrap();
    cursors.apply_advance(a2a::mesh::gateway::replicate::CursorAdvance::new("gw-b", 4));
    cursors.apply_advance(a2a::mesh::gateway::replicate::CursorAdvance::new("gw-c", 9));
    cursors.persist(&a2a_home).unwrap();
    spooled_note(&home, "gw-b", "000001-one.note.json");
    spooled_note(&home, "gw-b", "000002-two.note.json");

    let out = common::a2a(&home, &work)
        .args(["--json", "gc"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let value: Value = serde_json::from_slice(&out).unwrap();
    let removed = value["data"]["mesh"]["removed"].as_array().unwrap();
    assert!(
        removed
            .iter()
            .any(|peer| peer == "claude:11111111-1111-4111-8111-111111111111"),
        "{value}"
    );
    assert!(
        removed
            .iter()
            .any(|peer| peer == "codex:22222222-2222-4222-8222-222222222222"),
        "{value}"
    );
    assert!(
        !removed
            .iter()
            .any(|peer| peer == "codex:33333333-3333-4333-8333-333333333333"),
        "{value}"
    );
    assert!(!home.join("mesh").join("outbound").join("gw-b").exists());
    let retired_dir = home
        .join("mesh")
        .join("outbound")
        .join("retired")
        .join("gw-b");
    assert_eq!(fs::read_dir(retired_dir).unwrap().count(), 2);

    let cursor_text =
        fs::read_to_string(home.join("mesh").join("gateway").join("cursors.json")).unwrap();
    let cursor_json: Value = serde_json::from_str(&cursor_text).unwrap();
    assert!(
        cursor_json["origins"].get("gw-b").is_none(),
        "{cursor_json}"
    );
    assert_eq!(cursor_json["origins"]["gw-c"]["last_seq"], 9);

    let log = fs::read_to_string(home.join("mesh").join("gateway").join("gateway.jsonl")).unwrap();
    assert!(log.contains(r#""kind":"origin_retired""#), "{log}");
    assert!(log.contains(r#""gateway_id":"gw-b""#), "{log}");
    assert!(log.contains(r#""archived":2"#), "{log}");
}
