mod common;

use a2a::home::A2aHome;
use a2a::mesh::gateway::replicate::{
    self, CursorAdvance, CursorStore, IngestDecision, WantReply,
};
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
        ts: Utc.timestamp_opt(1_700_000_000 + origin_seq as i64, 0).unwrap(),
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

#[test]
fn remote_ingress_appends_before_advancing_and_rebuilds_or_repairs_cursors_by_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    let cfg = cfg_for(&home);
    let mut cursors = CursorStore::load_or_rebuild(&home).unwrap();

    let accepted =
        replicate::append_remote_entry(&home, &cfg, &mut cursors, "gw-remote", entry("gw-remote", 1, "one"))
            .unwrap();
    assert_eq!(accepted, IngestDecision::Accepted { local_seq: 1 });
    assert_eq!(cursors.last_seq("gw-remote"), 1);
    cursors.persist(&home).unwrap();
    let feed = fs::read_to_string(tmp.path().join("mesh").join("feed.jsonl")).unwrap();
    assert!(feed.contains(r#""origin":"gw-remote""#), "{feed}");

    let replay =
        replicate::append_remote_entry(&home, &cfg, &mut cursors, "gw-remote", entry("gw-remote", 1, "again"))
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
        replicate::append_remote_entry(&home, &cfg, &mut stale, "gw-origin", entry("gw-origin", 2, "trimmed"))
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
    assert_eq!(a2a::mesh::feed::read(&home_at(&home_b), 0).unwrap().len(), 1);
}
