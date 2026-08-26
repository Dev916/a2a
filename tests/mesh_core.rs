use a2a::home::A2aHome;
use a2a::mesh::{
    mesh_root, peer_dir, scan_or_refuse, Cursors, FeedEntry, Note, PeerId, Presence, Provider,
    STATUS_MAX_BYTES,
};
use a2a::output::ExitCode;
use chrono::Utc;
use serde_json::json;

#[test]
fn peer_ids_parse_display_and_round_trip_to_dir_names() {
    for (raw, provider, dir) in [
        (
            "claude:0a1ec845-4394-449e-a098-e0e01cb19887",
            Provider::Claude,
            "claude-0a1ec845-4394-449e-a098-e0e01cb19887",
        ),
        (
            "codex:01a024c0-a14e-7b93-a380-9382233fe37e",
            Provider::Codex,
            "codex-01a024c0-a14e-7b93-a380-9382233fe37e",
        ),
    ] {
        let id = PeerId::parse(raw).unwrap();
        assert_eq!(id.provider, provider);
        assert_eq!(id.to_string(), raw);
        assert_eq!(id.dir_name(), dir);
    }

    let err = PeerId::parse("gemini:0a1ec845-4394-449e-a098-e0e01cb19887").unwrap_err();
    assert_eq!(err.exit, ExitCode::Precondition);
    assert_eq!(err.code, "PEER_ID_INVALID");
    assert!(err.message.contains("unknown provider"), "{}", err.message);
}

#[test]
fn mesh_paths_are_rooted_under_the_home() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let root = mesh_root(&home);
    assert_eq!(root, tmp.path().join("mesh"));
    assert!(root.is_dir());

    let id = PeerId::parse("codex:01a024c0-a14e-7b93-a380-9382233fe37e").unwrap();
    assert_eq!(
        peer_dir(&home, &id),
        tmp.path()
            .join("mesh")
            .join("peers")
            .join("codex-01a024c0-a14e-7b93-a380-9382233fe37e")
    );
}

#[test]
fn serde_shapes_tolerate_unknown_keys() {
    let now = Utc::now();
    let presence = Presence {
        peer: "claude:0a1ec845-4394-449e-a098-e0e01cb19887".to_string(),
        provider: "claude".to_string(),
        worker_id: None,
        status: Some("reviewing".to_string()),
        working_on: Some("~/dev/a2a".to_string()),
        cwd: Some("/Users/webmech/dev/a2a".to_string()),
        registered_at: now,
        last_seen: now,
        retired: false,
        presence_seq: 0,
        home_gateway: None,
        cursors: Cursors {
            feed_seq: 41,
            inbox: Some("last.note.json".to_string()),
        },
        extra: serde_json::Map::new(),
    };
    let mut v = serde_json::to_value(&presence).unwrap();
    v["layer2"] = json!({"endpoint": "local"});
    let decoded: Presence = serde_json::from_value(v).unwrap();
    assert_eq!(decoded.extra["layer2"]["endpoint"], "local");

    let note: Note = serde_json::from_value(json!({
        "from": "claude:0a1ec845-4394-449e-a098-e0e01cb19887",
        "to": "codex:01a024c0-a14e-7b93-a380-9382233fe37e",
        "ts": now,
        "kind": "note",
        "text": "handoff",
        "refs": {"beads": ["a2a-c9g"]},
        "future": true
    }))
    .unwrap();
    assert_eq!(note.note_id, None);
    assert_eq!(note.extra["future"], true);

    let feed: FeedEntry = serde_json::from_value(json!({
        "seq": 42,
        "from": "claude:0a1ec845-4394-449e-a098-e0e01cb19887",
        "ts": now,
        "text": "shipped",
        "refs": {"worker": null},
        "future": {"ok": true}
    }))
    .unwrap();
    assert_eq!(feed.seq, 42);
    assert_eq!(feed.extra["future"]["ok"], true);
}

#[test]
fn scan_or_refuse_enforces_caps_and_secret_scan() {
    let cap_sized = "x".repeat(STATUS_MAX_BYTES);
    scan_or_refuse(&cap_sized, "STATUS_TOO_LARGE").unwrap();

    let err = scan_or_refuse(&(cap_sized + "x"), "STATUS_TOO_LARGE").unwrap_err();
    assert_eq!(err.exit, ExitCode::Precondition);
    assert_eq!(err.code, "STATUS_TOO_LARGE");
    assert!(
        err.message.contains(&STATUS_MAX_BYTES.to_string()),
        "{}",
        err.message
    );

    let err = scan_or_refuse(
        "OPENAI_API_KEY=sk-abc123456789012345678901",
        "SECRET_IN_STATUS",
    )
    .unwrap_err();
    assert_eq!(err.exit, ExitCode::Precondition);
    assert_eq!(err.code, "SECRET_IN_STATUS");
    assert!(err.message.contains("openai_key"), "{}", err.message);
    assert!(
        !err.message.contains("sk-abc123456789012345678901"),
        "secret value leaked: {}",
        err.message
    );
}
