mod common;

use a2a::home::A2aHome;
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_init, gateway_identity, gateway_public_key_b64,
    install_gateway_token, SystemClock,
};
use a2a::mesh::gateway::replicate::{
    self, AntiEntropyApply, AntiEntropyCycle, CursorAdvance, CursorStore, FeedReorderBuffers,
    IngestDecision, PeerRevocationTracker, WantReply,
};
use a2a::mesh::registry::{FileRegistry, GatewayRow, Registry, REGISTRY_SCHEMA_VERSION};
use a2a::mesh::work::{
    sign_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkEnvelope, WorkRepo,
};
use a2a::mesh::work_store;
use a2a::mesh::wire::{Frame, OriginWant};
use a2a::mesh::FeedEntry;
use chrono::{TimeZone, Utc};
use predicates::prelude::*;
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn cfg_for(home: &A2aHome) -> a2a::config::Config {
    a2a::config::load(home).unwrap().0
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

fn copy_root_pub(root: &Path, joiner: &Path) {
    fs::create_dir_all(joiner.join("mesh/fleet")).unwrap();
    fs::copy(
        root.join("mesh/fleet/root.pub"),
        joiner.join("mesh/fleet/root.pub"),
    )
    .unwrap();
}

fn enroll_joiner(root: &A2aHome, joiner: &A2aHome, clock: &SystemClock) -> String {
    let identity = gateway_identity(joiner, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(joiner).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey_b64, clock).unwrap();
    install_gateway_token(joiner, &token, clock).unwrap();
    identity.gateway_id
}

fn pool_work_fixture(work_id: &str) -> (tempfile::TempDir, A2aHome, A2aHome, WorkEnvelope) {
    let tmp = tempfile::tempdir().unwrap();
    let root = home_at(&tmp.path().join("root"));
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let clock = SystemClock;
    fleet_init(&root).unwrap();
    copy_root_pub(root.root(), sender.root());
    copy_root_pub(root.root(), receiver.root());
    enroll_joiner(&root, &sender, &clock);
    enroll_joiner(&root, &receiver, &clock);
    let env = sign_envelope(
        &sender,
        UnsignedWorkEnvelope {
            work_id: work_id.to_string(),
            session: "codex:sender-session".to_string(),
            addressing: WorkAddressing::Pool,
            repo: WorkRepo {
                remote_url: "https://github.com/example/project.git".to_string(),
                base_ref: "main".to_string(),
            },
            title: "Implement mesh handoff".to_string(),
            brief_text: "---\nlane: impl\n---\nDo the task.".to_string(),
            acceptance: "focused test passes".to_string(),
            ttl_days: 7,
            max_bytes: 262_144,
        },
        &clock,
    )
    .unwrap();
    (tmp, sender, receiver, env)
}

fn registry_row(gateway_id: &str, endpoint_url: Option<&str>) -> GatewayRow {
    GatewayRow {
        fleet_id: "fleet-a".to_string(),
        gateway_id: gateway_id.to_string(),
        owner: "test-owner".to_string(),
        endpoint_url: endpoint_url.map(str::to_string),
        pubkey_fpr: format!("{gateway_id}-pub"),
        cert_fpr: Some(format!("{gateway_id}-cert")),
        version: "1".to_string(),
        created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        last_heartbeat: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        revoked_at: None,
        schema_version: REGISTRY_SCHEMA_VERSION,
        row_msg: None,
        row_token: None,
        row_sig: None,
        signed_at: None,
        revocation: None,
    }
}

fn feed_json(home: &Path, work: &Path) -> Vec<Value> {
    let out = common::a2a(home, work)
        .args(["mesh", "feed", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: Value = serde_json::from_slice(&out).unwrap();
    serde_json::from_value(v["data"].clone()).unwrap()
}

fn entry(origin: &str, origin_seq: u64, text: &str) -> FeedEntry {
    FeedEntry {
        seq: origin_seq,
        origin: Some(origin.to_string()),
        origin_seq: Some(origin_seq),
        from: "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
        ts: Utc
            .timestamp_opt(1_700_000_000 + origin_seq as i64, 0)
            .unwrap(),
        text: text.to_string(),
        refs: json!({}),
        extra: serde_json::Map::new(),
    }
}

fn local_entry(text: &str) -> FeedEntry {
    FeedEntry {
        seq: 0,
        origin: None,
        origin_seq: None,
        from: "codex:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
        ts: Utc::now(),
        text: text.to_string(),
        refs: json!({}),
        extra: serde_json::Map::new(),
    }
}

#[test]
fn feed_entries_are_layer1_compatible_and_render_canonical_origin_citations() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();

    let legacy: FeedEntry = serde_json::from_value(json!({
        "seq": 12,
        "from": "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "ts": Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "text": "legacy",
        "refs": {},
        "future": true
    }))
    .unwrap();
    assert_eq!(legacy.origin, None);
    assert_eq!(legacy.origin_seq, None);
    assert_eq!(legacy.extra["future"], true);

    common::a2a(&home, &work)
        .args([
            "mesh",
            "post",
            "--session",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "before gateway",
        ])
        .assert()
        .success();
    let human = common::a2a(&home, &work)
        .args(["mesh", "feed"])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("1 "));
    assert!(
        !String::from_utf8(human.get_output().stdout.clone())
            .unwrap()
            .starts_with("gw-local-test:1"),
        "plain local seq is rendered before a gateway identity exists"
    );
    let rows = feed_json(&home, &work);
    assert_eq!(rows[0]["origin"], Value::Null);
    assert_eq!(rows[0]["origin_seq"], Value::Null);

    write_gateway_key(&home, "gw-local-test");
    let out = common::a2a(&home, &work)
        .args(["mesh", "feed"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let rendered = String::from_utf8(out).unwrap();
    assert!(rendered.starts_with("gw-local-test:1 "), "{rendered}");
    let rows = feed_json(&home, &work);
    assert_eq!(rows[0]["origin"], "gw-local-test");
    assert_eq!(rows[0]["origin_seq"], 1);
}

#[tokio::test]
async fn watcher_deltas_push_feed_through_live_session_and_peer_applies() {
    let tmp = tempfile::tempdir().unwrap();
    let home_a = home_at(&tmp.path().join("a"));
    let home_b = home_at(&tmp.path().join("b"));
    write_gateway_key(home_a.root(), "gw-a");
    write_gateway_key(home_b.root(), "gw-b");
    let cfg_a = cfg_for(&home_a);
    let cfg_b = cfg_for(&home_b);
    a2a::mesh::feed::append_entry(&home_a, &cfg_a, local_entry("watcher delta")).unwrap();

    let link = common::start_mesh_link(
        home_a.clone(),
        cfg_a.clone(),
        "gw-a",
        home_b.clone(),
        cfg_b,
        "gw-b",
    )
    .await;
    let sent =
        a2a::mesh::gateway::link::push_watcher_deltas(&home_a, &cfg_a, "gw-a", &link.a_state)
            .await
            .unwrap();
    assert!(sent >= 1);
    common::wait_until(|| {
        a2a::mesh::feed::read(&home_b, 0)
            .unwrap()
            .iter()
            .any(|entry| entry.origin.as_deref() == Some("gw-a") && entry.text == "watcher delta")
    })
    .await;
    link.stop().await;
}

#[tokio::test]
async fn anti_entropy_digest_over_session_repairs_frame_gap() {
    let tmp = tempfile::tempdir().unwrap();
    let home_a = home_at(&tmp.path().join("a"));
    let home_b = home_at(&tmp.path().join("b"));
    write_gateway_key(home_a.root(), "gw-a");
    write_gateway_key(home_b.root(), "gw-b");
    let cfg_a = cfg_for(&home_a);
    let cfg_b = cfg_for(&home_b);
    for text in ["one", "two", "three"] {
        a2a::mesh::feed::append_entry(&home_a, &cfg_a, local_entry(text)).unwrap();
    }

    let link =
        common::start_mesh_link(home_a.clone(), cfg_a, "gw-a", home_b.clone(), cfg_b, "gw-b").await;
    a2a::mesh::gateway::link::push_anti_entropy_digest(&home_a, &link.a_state)
        .await
        .unwrap();
    common::wait_until(|| {
        let rows = a2a::mesh::feed::read(&home_b, 0).unwrap();
        rows.iter()
            .filter(|entry| entry.origin.as_deref() == Some("gw-a"))
            .count()
            == 3
    })
    .await;
    link.stop().await;
}

#[test]
fn remote_ingress_appends_before_advancing_and_rebuilds_or_repairs_cursors_by_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    let cfg = cfg_for(&home);
    let mut cursors = CursorStore::load_or_rebuild(&home).unwrap();

    let accepted = replicate::append_remote_entry(
        &home,
        &cfg,
        &mut cursors,
        "gw-remote",
        entry("gw-remote", 1, "one"),
    )
    .unwrap();
    assert_eq!(accepted, IngestDecision::Accepted { local_seq: 1 });
    assert_eq!(cursors.last_seq("gw-remote"), 1);
    cursors.persist(&home).unwrap();
    let feed = fs::read_to_string(tmp.path().join("mesh").join("feed.jsonl")).unwrap();
    assert!(feed.contains(r#""origin":"gw-remote""#), "{feed}");

    let replay = replicate::append_remote_entry(
        &home,
        &cfg,
        &mut cursors,
        "gw-remote",
        entry("gw-remote", 1, "again"),
    )
    .unwrap();
    assert_eq!(replay, IngestDecision::Duplicate);
    assert_eq!(a2a::mesh::feed::read(&home, 0).unwrap().len(), 1);

    fs::remove_file(tmp.path().join("mesh").join("gateway").join("cursors.json")).unwrap();
    let rebuilt = CursorStore::load_or_rebuild(&home).unwrap();
    assert_eq!(rebuilt.last_seq("gw-remote"), 1);

    let cursor_path = tmp.path().join("mesh").join("gateway").join("cursors.json");
    fs::write(
        &cursor_path,
        br#"{"origins":{"gw-remote":{"last_seq":42,"compacted_before_seq":0},"gw-gone":{"last_seq":7,"compacted_before_seq":0}}}"#,
    )
    .unwrap();
    let repaired = CursorStore::load_or_rebuild(&home).unwrap();
    assert_eq!(repaired.last_seq("gw-remote"), 1);
    assert_eq!(repaired.last_seq("gw-gone"), 0);

    fs::write(&cursor_path, b"not json").unwrap();
    let rebuilt = CursorStore::load_or_rebuild(&home).unwrap();
    assert_eq!(rebuilt.last_seq("gw-remote"), 1);
}

#[test]
fn feed_work_refs_materialize_pool_work_and_rejected_refs_advance_cursor() {
    let (_tmp, _sender, receiver, env) = pool_work_fixture("01HZ7P7Q0V0V7G01J4J8R8M2Q4");
    let mut cfg = cfg_for(&receiver);
    let mut cursors = CursorStore::load_or_rebuild(&receiver).unwrap();
    let mut work_entry = entry("gw-remote", 1, "work offered");
    work_entry.refs = json!({ "work": env });

    assert_eq!(
        replicate::append_remote_entry(&receiver, &cfg, &mut cursors, "gw-remote", work_entry)
            .unwrap(),
        IngestDecision::Accepted { local_seq: 1 }
    );
    let stored = work_store::load(&receiver, "01HZ7P7Q0V0V7G01J4J8R8M2Q4")
        .unwrap()
        .expect("pool work is materialized");
    assert_eq!(stored.source, work_store::WorkSource::Pool);
    assert_eq!(cursors.last_seq("gw-remote"), 1);

    cfg.mesh.work.max_pool_bytes = 2;
    let mut rejected = entry("gw-remote", 2, "oversized refs");
    rejected.refs = json!({ "work": { "too_large": "x".repeat(32) } });
    assert_eq!(
        replicate::append_remote_entry(&receiver, &cfg, &mut cursors, "gw-remote", rejected)
            .unwrap(),
        IngestDecision::Rejected {
            reason: "WORK_TOO_LARGE".to_string()
        }
    );
    assert_eq!(
        cursors.last_seq("gw-remote"),
        2,
        "rejected work refs still skip and advance the origin cursor"
    );

    cfg.mesh.work.max_pool_bytes = 32_768;
    let mut after = entry("gw-remote", 3, "after skipped work");
    after.refs = json!({});
    assert_eq!(
        replicate::append_remote_entry(&receiver, &cfg, &mut cursors, "gw-remote", after).unwrap(),
        IngestDecision::Accepted { local_seq: 2 }
    );
    assert_eq!(cursors.last_seq("gw-remote"), 3);
}

#[test]
fn supervisor_cursor_advances_never_regress_and_compaction_watermarks_reject_old_replays() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    let cfg = cfg_for(&home);
    let mut cursors = CursorStore::load_or_rebuild(&home).unwrap();
    let old = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let recent = Utc.timestamp_opt(1_700_200_000, 0).unwrap();

    let mut one = entry("gw-origin", 1, "old one");
    one.ts = old;
    let mut two = entry("gw-origin", 2, "old two");
    two.ts = old;
    let mut three = entry("gw-origin", 3, "new three");
    three.ts = recent;
    for item in [one, two, three] {
        replicate::append_remote_entry(&home, &cfg, &mut cursors, "gw-origin", item).unwrap();
    }
    cursors.apply_advance(CursorAdvance::new("gw-origin", 2));
    assert_eq!(cursors.last_seq("gw-origin"), 3);
    cursors.apply_advance(CursorAdvance::new("gw-origin", 1));
    assert_eq!(cursors.last_seq("gw-origin"), 3);

    let sweep = replicate::compact_feed(
        &home,
        &cfg,
        recent,
        chrono::Duration::seconds(1),
        &mut cursors,
    )
    .unwrap();
    assert_eq!(sweep.removed, 2);
    assert_eq!(cursors.compacted_before_seq("gw-origin"), 3);
    assert_eq!(a2a::mesh::feed::read(&home, 0).unwrap().len(), 1);

    let mut stale = CursorStore::load_or_rebuild(&home).unwrap();
    stale.set_compacted_before_seq("gw-origin", 3);
    assert_eq!(
        replicate::append_remote_entry(
            &home,
            &cfg,
            &mut stale,
            "gw-origin",
            entry("gw-origin", 2, "trimmed")
        )
        .unwrap(),
        IngestDecision::Compacted { from_seq: 3 }
    );
    assert_eq!(
        replicate::entries_for_want(&home, "gw-origin", 2, &stale).unwrap(),
        WantReply::Compacted { from_seq: 3 }
    );
}

#[test]
fn loopback_feed_replication_pushes_local_deltas_with_attribution() {
    let tmp = tempfile::tempdir().unwrap();
    let home_a = tmp.path().join("a");
    let home_b = tmp.path().join("b");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    write_gateway_key(&home_a, "gw-a");
    write_gateway_key(&home_b, "gw-b");

    common::a2a(&home_a, &work)
        .args([
            "mesh",
            "post",
            "--session",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "hello from a",
        ])
        .assert()
        .success();

    let cfg = cfg_for(&home_at(&home_a));
    let mut b_cursors = CursorStore::load_or_rebuild(&home_at(&home_b)).unwrap();
    let pushed = replicate::replicate_feed_once(
        &cfg,
        &home_at(&home_a),
        "gw-a",
        &home_at(&home_b),
        &mut b_cursors,
    )
    .unwrap();
    assert_eq!(pushed, 1);
    assert_eq!(b_cursors.last_seq("gw-a"), 1);

    let rows = feed_json(&home_b, &work);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["origin"], "gw-a");
    assert_eq!(rows[0]["origin_seq"], 1);

    let out = common::a2a(&home_b, &work)
        .args(["mesh", "feed"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert!(
        String::from_utf8(out).unwrap().starts_with("gw-a:1 "),
        "remote canonical citation is stable"
    );

    let replay = replicate::replicate_feed_once(
        &cfg,
        &home_at(&home_a),
        "gw-a",
        &home_at(&home_b),
        &mut b_cursors,
    )
    .unwrap();
    assert_eq!(replay, 0);
    assert_eq!(
        a2a::mesh::feed::read(&home_at(&home_b), 0).unwrap().len(),
        1
    );
}

#[test]
fn want_catchup_buffers_gaps_and_streams_contiguous_ranges_oldest_first() {
    let tmp = tempfile::tempdir().unwrap();
    let home_sender = home_at(&tmp.path().join("sender"));
    let home_receiver = home_at(&tmp.path().join("receiver"));
    let cfg = cfg_for(&home_sender);
    let mut sender_cursors = CursorStore::load_or_rebuild(&home_sender).unwrap();
    let mut receiver_cursors = CursorStore::load_or_rebuild(&home_receiver).unwrap();

    for seq in [1, 2, 3, 4] {
        replicate::append_remote_entry(
            &home_sender,
            &cfg,
            &mut sender_cursors,
            "gw-origin",
            entry("gw-origin", seq, &format!("entry {seq}")),
        )
        .unwrap();
    }

    assert_eq!(
        replicate::want_for_origins(["gw-origin"], &receiver_cursors),
        Frame::Want {
            origins: vec![OriginWant {
                origin: "gw-origin".to_string(),
                from_seq: 1,
            }],
        }
    );
    assert_eq!(
        replicate::entries_for_want_capped(&home_sender, "gw-origin", 1, &sender_cursors, 2)
            .unwrap()
            .entries()
            .len(),
        2
    );

    let mut reorder = FeedReorderBuffers::new(4);
    let out_of_order = replicate::receive_feed_entries(
        &home_receiver,
        &cfg,
        &mut receiver_cursors,
        &mut reorder,
        "gw-origin",
        vec![entry("gw-origin", 3, "entry 3")],
    )
    .unwrap();
    assert_eq!(out_of_order.accepted, 0);
    assert_eq!(out_of_order.buffered, 1);
    assert_eq!(receiver_cursors.last_seq("gw-origin"), 0);

    let filled_gap = replicate::receive_feed_entries(
        &home_receiver,
        &cfg,
        &mut receiver_cursors,
        &mut reorder,
        "gw-origin",
        vec![
            entry("gw-origin", 1, "entry 1"),
            entry("gw-origin", 2, "entry 2"),
        ],
    )
    .unwrap();
    assert_eq!(filled_gap.accepted, 3);
    assert_eq!(receiver_cursors.last_seq("gw-origin"), 3);

    let rows = feed_json(home_receiver.root(), tmp.path());
    let seqs: Vec<u64> = rows
        .iter()
        .map(|row| row["origin_seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3]);
}

#[test]
fn reorder_buffer_cap_eviction_is_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    let cfg = cfg_for(&home);
    let mut cursors = CursorStore::load_or_rebuild(&home).unwrap();
    let mut reorder = FeedReorderBuffers::new(1);

    let report = replicate::receive_feed_entries(
        &home,
        &cfg,
        &mut cursors,
        &mut reorder,
        "gw-origin",
        vec![
            entry("gw-origin", 3, "entry 3"),
            entry("gw-origin", 4, "entry 4"),
        ],
    )
    .unwrap();
    assert_eq!(report.accepted, 0);
    assert_eq!(report.buffered, 2);
    assert_eq!(report.evictions.len(), 1);
    assert_eq!(report.evictions[0].origin, "gw-origin");
    assert_eq!(report.evictions[0].origin_seq, 3);
}

#[test]
fn transitive_relay_pushes_every_origin_verbatim_in_both_directions() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = cfg_for(&home_at(tmp.path()));
    let home_a = home_at(&tmp.path().join("a"));
    let home_b = home_at(&tmp.path().join("b"));
    let home_c = home_at(&tmp.path().join("c"));
    let mut a_cursors = CursorStore::load_or_rebuild(&home_a).unwrap();
    let mut b_cursors = CursorStore::load_or_rebuild(&home_b).unwrap();
    let mut c_cursors = CursorStore::load_or_rebuild(&home_c).unwrap();

    replicate::append_remote_entry(
        &home_a,
        &cfg,
        &mut a_cursors,
        "gw-a",
        entry("gw-a", 1, "from a"),
    )
    .unwrap();
    replicate::append_remote_entry(
        &home_c,
        &cfg,
        &mut c_cursors,
        "gw-c",
        entry("gw-c", 1, "from c"),
    )
    .unwrap();

    assert_eq!(
        replicate::replicate_feed_once(&cfg, &home_a, "gw-a", &home_b, &mut b_cursors).unwrap(),
        1
    );
    assert_eq!(
        replicate::replicate_feed_once(&cfg, &home_c, "gw-c", &home_b, &mut b_cursors).unwrap(),
        1
    );
    assert_eq!(
        replicate::replicate_feed_once(&cfg, &home_b, "gw-b", &home_c, &mut c_cursors).unwrap(),
        1
    );
    assert_eq!(
        replicate::replicate_feed_once(&cfg, &home_b, "gw-b", &home_a, &mut a_cursors).unwrap(),
        1
    );

    let a_rows = feed_json(home_a.root(), tmp.path());
    let c_rows = feed_json(home_c.root(), tmp.path());
    assert!(a_rows
        .iter()
        .any(|row| row["origin"] == "gw-c" && row["origin_seq"] == 1));
    assert!(c_rows
        .iter()
        .any(|row| row["origin"] == "gw-a" && row["origin_seq"] == 1));
}

#[test]
fn anti_entropy_digest_repairs_mid_log_delete_and_updates_health_shape() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = cfg_for(&home_at(tmp.path()));
    let home_a = home_at(&tmp.path().join("a"));
    let home_b = home_at(&tmp.path().join("b"));
    let mut a_cursors = CursorStore::load_or_rebuild(&home_a).unwrap();
    let mut b_cursors = CursorStore::load_or_rebuild(&home_b).unwrap();

    for seq in [1, 2, 3] {
        replicate::append_remote_entry(
            &home_a,
            &cfg,
            &mut a_cursors,
            "gw-a",
            entry("gw-a", seq, &format!("entry {seq}")),
        )
        .unwrap();
    }
    replicate::replicate_feed_once(&cfg, &home_a, "gw-a", &home_b, &mut b_cursors).unwrap();
    silently_delete_origin_seq(&home_b, "gw-a", 2);
    b_cursors = CursorStore::load_or_rebuild(&home_b).unwrap();

    let digest = replicate::digest_for_home(&home_a).unwrap();
    assert_eq!(
        replicate::wants_from_digest(&home_b, &b_cursors, &digest).unwrap(),
        vec![OriginWant {
            origin: "gw-a".to_string(),
            from_seq: 2,
        }]
    );

    let cycle = AntiEntropyCycle::new(chrono::Duration::seconds(10), 2);
    let report = cycle
        .apply(AntiEntropyApply {
            home: &home_b,
            cfg: &cfg,
            cursors: &mut b_cursors,
            peer_origin: "gw-a",
            remote_digest: digest,
            repair_entries: replicate::entries_for_want_capped(&home_a, "gw-a", 2, &a_cursors, 16)
                .unwrap()
                .entries()
                .to_vec(),
            now: Utc.timestamp_opt(1_700_000_010, 0).unwrap(),
        })
        .unwrap();
    assert_eq!(report.repaired_entries, 1);
    assert_eq!(b_cursors.last_seq("gw-a"), 3);
    assert!(report.health.last_anti_entropy.is_some());
    assert_eq!(report.health.per_origin_lag.get("gw-a"), Some(&0));
}

#[tokio::test]
async fn legacy_revoked_at_does_not_make_a_present_peer_look_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    registry.init_schema().unwrap();
    registry
        .upsert_self(registry_row("gw-a", Some("wss://127.0.0.1:1")))
        .await
        .unwrap();
    registry
        .upsert_self(registry_row("gw-b", Some("wss://127.0.0.1:2")))
        .await
        .unwrap();
    let mut tracker = PeerRevocationTracker::default();
    assert!(tracker
        .closed_after_discovery(
            "gw-a",
            ["gw-b"],
            &registry.discover_unverified("fleet-a").await.unwrap()
        )
        .is_empty());

    registry
        .revoke(
            "fleet-a",
            "gw-b",
            "test-revocation",
            Utc.timestamp_opt(1_700_000_020, 0).unwrap(),
        )
        .await
        .unwrap();
    assert!(tracker
        .closed_after_discovery(
            "gw-a",
            ["gw-b"],
            &registry.discover_unverified("fleet-a").await.unwrap()
        )
        .is_empty());
    assert!(tracker
        .closed_after_discovery(
            "gw-a",
            ["gw-b"],
            &registry.discover_unverified("fleet-a").await.unwrap()
        )
        .is_empty());
}

fn silently_delete_origin_seq(home: &A2aHome, origin: &str, origin_seq: u64) {
    let feed = home.root().join("mesh").join("feed.jsonl");
    let kept: Vec<String> = fs::read_to_string(&feed)
        .unwrap()
        .lines()
        .filter(|line| {
            let value: Value = serde_json::from_str(line).unwrap();
            !(value["origin"] == origin && value["origin_seq"] == origin_seq)
        })
        .map(str::to_string)
        .collect();
    fs::write(&feed, format!("{}\n", kept.join("\n"))).unwrap();
}

#[test]
fn local_posts_interleaved_with_replicated_entries_advertise_dense_origin_seqs() {
    // Live-fleet regression (a2a-ch7): a replicated entry at feed seq 1 pushed
    // the first LOCAL post to feed seq 2; seq-based claiming advertised the
    // local origin at origin_seq 2, peers wanted from 1, and the contiguity
    // walk returned nothing — a permanent replication deadlock.
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(&tmp.path().join("home"));
    let cfg = cfg_for(&home);
    write_gateway_key(home.root(), "gw-b");

    // A replicated remote entry occupies feed seq 1.
    a2a::mesh::feed::append_entry(&home, &cfg, entry("gw-a", 1, "from gw-a")).unwrap();
    // The first local post lands at feed seq 2.
    a2a::mesh::feed::append_entry(&home, &cfg, local_entry("first local")).unwrap();

    let digest = replicate::digest_for_home(&home).unwrap();
    let local = digest.iter().find(|d| d.origin == "gw-b").expect("local origin advertised");
    assert_eq!(local.max_seq, 1, "dense numbering: first local entry is origin_seq 1");

    let cursors = CursorStore::default();
    match replicate::entries_for_want(&home, "gw-b", 1, &cursors).unwrap() {
        WantReply::Entries(entries) => {
            assert_eq!(entries.len(), 1, "want from 1 serves the local entry");
            assert_eq!(entries[0].origin_seq, Some(1));
            assert_eq!(entries[0].text, "first local");
        }
        other => panic!("expected entries, got {other:?}"),
    }

    // A second local post stays contiguous.
    a2a::mesh::feed::append_entry(&home, &cfg, local_entry("second local")).unwrap();
    let digest = replicate::digest_for_home(&home).unwrap();
    let local = digest.iter().find(|d| d.origin == "gw-b").unwrap();
    assert_eq!(local.max_seq, 2);
}

#[test]
fn compaction_persists_dense_claims_so_numbering_survives_generation_drops() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(&tmp.path().join("home"));
    let cfg = cfg_for(&home);
    write_gateway_key(home.root(), "gw-b");

    // Old removable remote entry (ts far in the past), then a fresh local one.
    a2a::mesh::feed::append_entry(&home, &cfg, entry("gw-a", 1, "ancient remote")).unwrap();
    a2a::mesh::feed::append_entry(&home, &cfg, local_entry("fresh local")).unwrap();

    let mut cursors = CursorStore::default();
    let compaction = replicate::compact_feed(
        &home,
        &cfg,
        Utc::now(),
        chrono::Duration::days(30),
        &mut cursors,
    )
    .unwrap();
    assert_eq!(compaction.removed, 1, "the ancient remote entry is compacted");

    // The surviving local entry must now be claimed ON DISK: read the raw file
    // and assert the origin fields were persisted by the rewrite.
    let raw = fs::read_to_string(home.root().join("mesh/feed.jsonl")).unwrap();
    let claimed = raw
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|v| v["text"] == "fresh local")
        .expect("local entry survives compaction");
    assert_eq!(claimed["origin"], "gw-b", "claim persisted on disk");
    assert_eq!(claimed["origin_seq"], 1, "dense origin_seq persisted on disk");
}

#[tokio::test]
async fn rejected_work_refs_over_live_link_advance_cursor_and_session_survives() {
    let (_tmp, _sender, receiver, _env) = pool_work_fixture("01HZ7P7Q0V0V7G01J4J8R8M2Q5");
    let home_a = home_at(&receiver.root().join("peer-a"));
    let home_b = receiver;
    write_gateway_key(home_a.root(), "gw-a");
    let mut cfg_a = cfg_for(&home_a);
    let mut cfg_b = cfg_for(&home_b);
    cfg_a.mesh.work.max_pool_bytes = 2;
    cfg_b.mesh.work.max_pool_bytes = 2;

    let link = common::start_mesh_link(
        home_a.clone(),
        cfg_a.clone(),
        "gw-a",
        home_b.clone(),
        cfg_b,
        "gw-b",
    )
    .await;
    let mut rejected = entry("gw-a", 1, "oversized work");
    rejected.refs = json!({ "work": { "too_large": "x".repeat(32) } });
    assert!(
        link.a_state
            .send(
                "gw-b",
                Frame::Feed {
                    origin: "gw-a".to_string(),
                    entries: vec![rejected],
                },
            )
            .await
    );
    common::wait_until(|| {
        CursorStore::load_cached(&home_b)
            .unwrap()
            .last_seq("gw-a")
            == 1
    })
    .await;

    let mut accepted = entry("gw-a", 2, "after rejected refs");
    accepted.refs = json!({});
    assert!(
        link.a_state
            .send(
                "gw-b",
                Frame::Feed {
                    origin: "gw-a".to_string(),
                    entries: vec![accepted],
                },
            )
            .await
    );
    common::wait_until(|| {
        a2a::mesh::feed::read(&home_b, 0)
            .unwrap()
            .iter()
            .any(|entry| {
                entry.origin.as_deref() == Some("gw-a")
                    && entry.origin_seq == Some(2)
                    && entry.text == "after rejected refs"
            })
    })
    .await;
    link.stop().await;
}
