mod common;

use a2a::home::A2aHome;
use a2a::mesh::gateway::limits::{
    Bucket, BucketKind, LimitOutcome, PreAuthAdmission, PreAuthGate,
    MAX_ORIGINS_PER_REQUEST_FRAME,
};
use a2a::mesh::gateway::link::{
    accept_and_handshake, LinkDirection, LinkSessionParams, LinkStateHandle,
};
use a2a::mesh::gateway::notes_wire;
use a2a::mesh::keys::{
    enroll_gateway_token, ensure_gateway_token, fleet_init, gateway_public_key_b64,
    gateway_signing_key, gateway_token, install_gateway_token, RootSet, SystemClock, TestClock,
};
use a2a::mesh::work::{
    sign_envelope, verify_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkRepo,
};
use a2a::mesh::work_store::{self, WorkSource, MAX_POOL_OFFERS, WORK_OFFER_KIND};
use a2a::mesh::wire::{Frame, HandshakeIdentity, OriginWant, TlsIdentity};
use a2a::mesh::{FeedEntry, Note, PeerId};
use chrono::{TimeZone, Utc};
use rand::{rngs::StdRng, SeedableRng};
use serde_json::{json, Value};
use std::fs;
use std::net::{IpAddr, Ipv4Addr};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn home_at(path: &std::path::Path) -> A2aHome {
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

fn note(to: &str, text: &str, note_id: &str) -> Note {
    Note {
        note_id: Some(note_id.to_string()),
        from: "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
        to: to.to_string(),
        ts: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        kind: "note".to_string(),
        text: text.to_string(),
        refs: json!({}),
        extra: serde_json::Map::new(),
    }
}

fn note_files(home: &Path, peer: &str) -> Vec<PathBuf> {
    let id = PeerId::parse(peer).unwrap();
    collect_note_files(&home.join("mesh/peers").join(id.dir_name()).join("inbox"))
}

fn collect_note_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return files;
    };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            files.extend(collect_note_files(&entry.path()));
        } else if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".note.json")
        {
            files.push(entry.path());
        }
    }
    files.sort();
    files
}

fn local_feed_entry(text: &str) -> FeedEntry {
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

fn copy_root_pub(root: &A2aHome, joiner: &A2aHome) {
    let fleet = joiner.root().join("mesh/fleet");
    fs::create_dir_all(&fleet).unwrap();
    fs::copy(
        root.root().join("mesh/fleet/root.pub"),
        fleet.join("root.pub"),
    )
    .unwrap();
}

fn enroll_joiner(root: &A2aHome, joiner: &A2aHome) {
    a2a::mesh::keys::gateway_identity(joiner, false).unwrap();
    let token = enroll_gateway_token(
        root,
        &a2a::mesh::keys::gateway_identity(joiner, false)
            .unwrap()
            .gateway_id,
        &gateway_public_key_b64(joiner).unwrap(),
        &SystemClock,
    )
    .unwrap();
    install_gateway_token(joiner, &token, &SystemClock).unwrap();
}

fn verified_pool_offer(home_root: &Path) -> (A2aHome, a2a::mesh::work::VerifiedEnvelope) {
    let root = home_at(&home_root.join("root"));
    let sender = home_at(&home_root.join("offer-sender"));
    let receiver = home_at(&home_root.join("offer-receiver"));
    fleet_init(&root).unwrap();
    copy_root_pub(&root, &sender);
    copy_root_pub(&root, &receiver);
    enroll_joiner(&root, &sender);
    let envelope = sign_envelope(
        &sender,
        UnsignedWorkEnvelope {
            work_id: "01HZ7P7Q0V0V7G01J4J8R8M2Q4".to_string(),
            session: "codex:sender-session".to_string(),
            addressing: WorkAddressing::Pool,
            repo: WorkRepo {
                remote_url: "https://github.com/example/project.git".to_string(),
                base_ref: "main".to_string(),
            },
            title: "Bound the pool".to_string(),
            brief_text: "---\nlane: impl\n---\nDo the bounded task.".to_string(),
            acceptance: "focused test passes".to_string(),
            ttl_days: 7,
            max_bytes: 262_144,
        },
        &SystemClock,
    )
    .unwrap();
    let verified = verify_envelope(&receiver, &envelope, &SystemClock).unwrap();
    (receiver, verified)
}

#[test]
fn metered_buckets_use_per_denomination_depth_and_log_one_transition() {
    let now = Utc::now();
    let mut off = Bucket::new(0, BucketKind::Notes);
    assert!(matches!(off.take(1, now), LimitOutcome::Off));

    let mut per_min = Bucket::new(60, BucketKind::Notes);
    for _ in 0..60 {
        assert!(matches!(
            per_min.take(1, now),
            LimitOutcome::Allowed
        ));
    }
    assert!(matches!(
        per_min.take(1, now),
        LimitOutcome::Deferred {
            transitioned: true,
            ..
        }
    ));
    assert!(matches!(
        per_min.take(1, now),
        LimitOutcome::Deferred {
            transitioned: false,
            ..
        }
    ));
    assert!(matches!(
        per_min.take(1, now + chrono::Duration::seconds(60)),
        LimitOutcome::Allowed
    ));

    let mut hourly = Bucket::new(4, BucketKind::WorkOffers);
    for _ in 0..4 {
        assert!(matches!(hourly.take(1, now), LimitOutcome::Allowed));
    }
    assert!(matches!(
        hourly.take(1, now),
        LimitOutcome::Deferred { .. }
    ));
}

#[test]
fn pre_auth_bounds_close_sockets_without_an_ack() {
    let cfg = a2a::config::MeshLimitsConfig::default();
    let mut gate = PreAuthGate::new(&cfg);
    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let now = Utc::now();
    for _ in 0..32 {
        assert!(matches!(
            gate.admit(ip, now),
            PreAuthAdmission::Admitted
        ));
    }
    assert!(matches!(
        gate.admit(ip, now),
        PreAuthAdmission::TooManyUnauthenticated
    ));
    gate.release(ip);
    assert!(matches!(
        gate.admit(ip, now),
        PreAuthAdmission::Admitted
    ));
    for _ in 0..60 {
        gate.release(ip);
        let _ = gate.admit(ip, now);
    }
    assert!(matches!(
        gate.admit(ip, now),
        PreAuthAdmission::SourceRateLimited
    ));
    assert!(gate.health()["refusals"]["source_rate_limited"]
        .as_u64()
        .unwrap()
        > 0);
}

#[tokio::test]
async fn handshake_deadline_closes_a_silent_socket() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    let clock = TestClock::new(Utc::now());
    fleet_init(&home).unwrap();
    let identity = a2a::mesh::keys::gateway_identity(&home, false).unwrap();
    ensure_gateway_token(&home, &clock).unwrap();
    let handshake_identity = HandshakeIdentity::new(
        gateway_token(&home).unwrap(),
        gateway_signing_key(&home).unwrap(),
    );
    let roots = RootSet::load(&home).unwrap();
    let tls = TlsIdentity::self_signed(vec!["localhost".to_string()]).unwrap();
    let (_silent_client, server) = tokio::io::duplex(4096);
    let result = accept_and_handshake(
        server,
        tls,
        handshake_identity,
        roots,
        &clock,
        Duration::from_millis(50),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("silent client unexpectedly completed a handshake"),
        Err(error) => error,
    };
    assert_eq!(error.code, "PREAUTH_LIMIT");
    let state = LinkStateHandle::new(&home, Duration::from_secs(60)).unwrap();
    assert!(!state.is_live(&identity.gateway_id).await);
}

#[tokio::test]
async fn a_surplus_session_is_refused_with_session_limit_and_keeps_the_first() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    let state = LinkStateHandle::new(&home, Duration::from_secs(60)).unwrap();
    let (tx1, _rx1) = mpsc::channel(4);
    let (tx2, _rx2) = mpsc::channel(4);
    let (tx3, _rx3) = mpsc::channel(4);

    state
        .register("gw-b".into(), LinkDirection::Inbound, tx1.clone())
        .await
        .unwrap();
    let err = state
        .register("gw-b".into(), LinkDirection::Inbound, tx2)
        .await
        .unwrap_err();
    assert_eq!(err.code, "SESSION_LIMIT");
    state
        .register("gw-b".into(), LinkDirection::Outbound, tx3)
        .await
        .unwrap();
    assert!(state.sender("gw-b").await.is_some());
    assert!(!tx1.is_closed(), "the incumbent registration is not overwritten");
}

#[tokio::test]
async fn notes_over_the_bucket_are_deferred_not_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");
    let cfg_a = cfg_for(&sender);
    let mut cfg_b = cfg_for(&receiver);
    cfg_b.mesh.limits.notes_per_min = 2;
    let target = "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    for (text, note_id) in [
        ("one", "01K3KV00000000000000000011"),
        ("two", "01K3KV00000000000000000012"),
        ("three", "01K3KV00000000000000000013"),
    ] {
        notes_wire::spool_note(
            &sender,
            &cfg_a,
            "gw-a",
            "gw-b",
            note(target, text, note_id),
        )
        .unwrap();
    }

    let link = common::start_mesh_link(
        sender.clone(),
        cfg_a.clone(),
        "gw-a",
        receiver.clone(),
        cfg_b,
        "gw-b",
    )
    .await;
    let now = Utc::now();
    let mut rng = StdRng::seed_from_u64(22);
    assert_eq!(
        a2a::mesh::gateway::link::push_spooled_notes(
            &sender,
            &cfg_a,
            &link.a_state,
            now,
            &mut rng,
        )
        .await
        .unwrap(),
        3
    );
    common::wait_until(|| {
        note_files(receiver.root(), target).len() == 2
            && collect_note_files(&sender.root().join("mesh/outbound/gw-b")).len() == 1
    })
    .await;

    let queued = collect_note_files(&sender.root().join("mesh/outbound/gw-b"));
    let held = notes_wire::read_spooled(&queued[0]).unwrap();
    assert!(held.next_attempt_after.is_some_and(|retry| retry > now));
    assert!(collect_note_files(&sender.root().join("mesh/outbound/rejected")).is_empty());
    let log = fs::read_to_string(receiver.root().join("mesh/gateway/gateway.jsonl")).unwrap();
    let transitions = log
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| line["kind"] == "limit_state_change" && line["bucket"] == "notes")
        .count();
    assert_eq!(transitions, 1, "{log}");
    assert_eq!(
        link.b_state.limits_health().await["gw-a"].defers["notes"],
        1
    );
    link.stop().await;
}

#[tokio::test]
async fn feed_over_the_bucket_applies_backpressure_and_never_advances_the_cursor() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");
    let cfg_a = cfg_for(&sender);
    let mut limited_cfg_b = cfg_for(&receiver);
    limited_cfg_b.mesh.limits.feed_entries_per_min = 2;
    for index in 1..=5 {
        a2a::mesh::feed::append_entry(
            &sender,
            &cfg_a,
            local_feed_entry(&format!("entry {index}")),
        )
        .unwrap();
    }

    let link = common::start_mesh_link(
        sender.clone(),
        cfg_a.clone(),
        "gw-a",
        receiver.clone(),
        limited_cfg_b,
        "gw-b",
    )
    .await;
    common::wait_until(|| a2a::mesh::feed::read(&receiver, 0).unwrap().len() == 2).await;
    let cursor_handle = link.b_state.peer_cursors("gw-a").await;
    let before = cursor_handle.lock().await.clone();
    assert_eq!(before.last_seq("gw-a"), 2);
    assert_eq!(before.compacted_before_seq("gw-a"), 0);

    let remaining = a2a::mesh::feed::read(&sender, 0)
        .unwrap()
        .into_iter()
        .filter(|entry| entry.origin_seq.is_some_and(|seq| seq >= 3))
        .collect();
    assert!(link
        .a_state
        .send(
            "gw-b",
            a2a::mesh::wire::Frame::Feed {
                origin: "gw-a".to_string(),
                entries: remaining,
            },
        )
        .await);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = cursor_handle.lock().await.clone();
    assert_eq!(after.last_seq("gw-a"), before.last_seq("gw-a"));
    assert_eq!(
        after.compacted_before_seq("gw-a"),
        before.compacted_before_seq("gw-a")
    );
    link.stop().await;

    let unlimited_cfg_b = cfg_for(&receiver);
    let repaired = common::start_mesh_link(
        sender,
        cfg_a,
        "gw-a",
        receiver.clone(),
        unlimited_cfg_b,
        "gw-b",
    )
    .await;
    common::wait_until(|| a2a::mesh::feed::read(&receiver, 0).unwrap().len() == 5).await;
    let repaired_cursors = repaired.b_state.peer_cursors("gw-a").await;
    assert_eq!(repaired_cursors.lock().await.last_seq("gw-a"), 5);
    repaired.stop().await;
}

#[tokio::test]
async fn only_appended_entries_burn_the_feed_bucket() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");
    let cfg_a = cfg_for(&sender);
    let mut cfg_b = cfg_for(&receiver);
    cfg_b.mesh.limits.feed_entries_per_min = 2;
    for index in 1..=2 {
        a2a::mesh::feed::append_entry(
            &sender,
            &cfg_a,
            local_feed_entry(&format!("duplicate candidate {index}")),
        )
        .unwrap();
    }
    let link = common::start_mesh_link(
        sender.clone(),
        cfg_a,
        "gw-a",
        receiver.clone(),
        cfg_b,
        "gw-b",
    )
    .await;
    common::wait_until(|| a2a::mesh::feed::read(&receiver, 0).unwrap().len() == 2).await;
    let duplicates = a2a::mesh::feed::read(&sender, 0).unwrap();
    assert!(link
        .a_state
        .send(
            "gw-b",
            a2a::mesh::wire::Frame::Feed {
                origin: "gw-a".to_string(),
                entries: duplicates,
            },
        )
        .await);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(a2a::mesh::feed::read(&receiver, 0).unwrap().len(), 2);
    let health = link.b_state.limits_health().await;
    assert_eq!(health["gw-a"].defers.get("feed_entries"), None);
    link.stop().await;
}

#[tokio::test]
async fn rejected_entries_are_exempt_even_when_the_feed_bucket_is_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");
    let cfg_a = cfg_for(&sender);
    let mut cfg_b = cfg_for(&receiver);
    cfg_b.mesh.limits.feed_entries_per_min = 1;
    let link = common::start_mesh_link(
        sender,
        cfg_a,
        "gw-a",
        receiver.clone(),
        cfg_b,
        "gw-b",
    )
    .await;

    let mut accepted = local_feed_entry("the one accepted entry");
    accepted.seq = 1;
    accepted.origin = Some("gw-a".to_string());
    accepted.origin_seq = Some(1);
    assert!(link
        .a_state
        .send(
            "gw-b",
            Frame::Feed {
                origin: "gw-a".to_string(),
                entries: vec![accepted],
            },
        )
        .await);
    common::wait_until(|| a2a::mesh::feed::read(&receiver, 0).unwrap().len() == 1).await;

    let mut rejected = local_feed_entry("invalid work is rejected without a feed token");
    rejected.seq = 2;
    rejected.origin = Some("gw-a".to_string());
    rejected.origin_seq = Some(2);
    rejected.refs = json!({"work": {"invalid": true}});
    assert!(link
        .a_state
        .send(
            "gw-b",
            Frame::Feed {
                origin: "gw-a".to_string(),
                entries: vec![rejected],
            },
        )
        .await);
    let cursors = link.b_state.peer_cursors("gw-a").await;
    common::wait_until(|| {
        cursors
            .try_lock()
            .is_ok_and(|cursor| cursor.last_seq("gw-a") == 2)
    })
    .await;

    assert_eq!(a2a::mesh::feed::read(&receiver, 0).unwrap().len(), 1);
    let health = link.b_state.limits_health().await;
    assert_eq!(health["gw-a"].defers.get("feed_entries"), None);
    link.stop().await;
}

#[tokio::test]
async fn full_pool_rejections_are_exempt_when_the_feed_bucket_is_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("link-sender"));
    let (receiver, verified) = verified_pool_offer(tmp.path());
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");
    let pool = receiver.root().join("work/pool");
    fs::create_dir_all(&pool).unwrap();
    for index in 0..MAX_POOL_OFFERS {
        fs::write(pool.join(format!("occupied-{index}.json")), b"{}").unwrap();
    }
    let cfg_a = cfg_for(&sender);
    let mut cfg_b = cfg_for(&receiver);
    cfg_b.mesh.limits.feed_entries_per_min = 1;
    let link = common::start_mesh_link(
        sender,
        cfg_a,
        "gw-a",
        receiver.clone(),
        cfg_b,
        "gw-b",
    )
    .await;

    let mut accepted = local_feed_entry("the one accepted entry");
    accepted.seq = 1;
    accepted.origin = Some("gw-a".to_string());
    accepted.origin_seq = Some(1);
    assert!(link
        .a_state
        .send(
            "gw-b",
            Frame::Feed {
                origin: "gw-a".to_string(),
                entries: vec![accepted],
            },
        )
        .await);
    common::wait_until(|| a2a::mesh::feed::read(&receiver, 0).unwrap().len() == 1).await;

    let mut rejected = local_feed_entry("a full pool rejects without a feed token");
    rejected.seq = 2;
    rejected.origin = Some("gw-a".to_string());
    rejected.origin_seq = Some(2);
    rejected.refs = json!({"work": verified.envelope});
    assert!(link
        .a_state
        .send(
            "gw-b",
            Frame::Feed {
                origin: "gw-a".to_string(),
                entries: vec![rejected],
            },
        )
        .await);
    let cursors = link.b_state.peer_cursors("gw-a").await;
    common::wait_until(|| {
        cursors
            .try_lock()
            .is_ok_and(|cursor| cursor.last_seq("gw-a") == 2)
    })
    .await;

    let health = link.b_state.limits_health().await;
    assert_eq!(health["gw-a"].defers.get("feed_entries"), None);
    let log = fs::read_to_string(receiver.root().join("mesh/gateway/gateway.jsonl")).unwrap();
    assert!(log.contains("WORK_POOL_FULL"), "{log}");
    link.stop().await;
}

#[tokio::test]
async fn pool_offers_are_charged_before_verify_and_the_pool_store_is_capped() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");
    let cfg_a = cfg_for(&sender);
    let mut cfg_b = cfg_for(&receiver);
    cfg_b.mesh.limits.work_offers_per_hour = 1;
    let link = common::start_mesh_link(
        sender,
        cfg_a,
        "gw-a",
        receiver,
        cfg_b,
        "gw-b",
    )
    .await;

    let mut first = local_feed_entry("invalid offer still burns admission");
    first.seq = 1;
    first.origin = Some("gw-a".to_string());
    first.origin_seq = Some(1);
    first.refs = json!({"work": {"invalid": true}});
    assert!(link
        .a_state
        .send(
            "gw-b",
            a2a::mesh::wire::Frame::Feed {
                origin: "gw-a".to_string(),
                entries: vec![first],
            },
        )
        .await);
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut second = local_feed_entry("second offer is deferred before verify");
    second.seq = 2;
    second.origin = Some("gw-a".to_string());
    second.origin_seq = Some(2);
    second.refs = json!({"work": {"invalid": true}});
    assert!(link
        .a_state
        .send(
            "gw-b",
            a2a::mesh::wire::Frame::Feed {
                origin: "gw-a".to_string(),
                entries: vec![second],
            },
        )
        .await);
    assert!(link
        .a_state
        .send(
            "gw-b",
            a2a::mesh::wire::Frame::Note {
                note: Note {
                    note_id: Some("01K3KV00000000000000000014".to_string()),
                    from: "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
                    to: "gateway:gw-b".to_string(),
                    ts: Utc::now(),
                    kind: WORK_OFFER_KIND.to_string(),
                    text: "third offer is also deferred".to_string(),
                    refs: json!({"work": {"invalid": true}}),
                    extra: serde_json::Map::new(),
                },
            },
        )
        .await);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        link.b_state.limits_health().await["gw-a"].defers["work_offers"],
        2
    );
    assert_eq!(
        link.b_state
            .peer_cursors("gw-a")
            .await
            .lock()
            .await
            .last_seq("gw-a"),
        1
    );
    link.stop().await;

    let pool_tmp = tempfile::tempdir().unwrap();
    let (pool_home, verified) = verified_pool_offer(pool_tmp.path());
    let pool = pool_home.root().join("work/pool");
    fs::create_dir_all(&pool).unwrap();
    for index in 0..MAX_POOL_OFFERS {
        fs::write(pool.join(format!("occupied-{index}.json")), b"{}").unwrap();
    }
    let error = work_store::store_offer(&pool_home, verified, WorkSource::Pool).unwrap_err();
    assert_eq!(error.code, "WORK_POOL_FULL");
}

#[tokio::test]
async fn want_and_digest_frames_are_metered_and_cap_origins_per_frame() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(&tmp.path().join("cap"));
    write_gateway_key(home.root(), "gw-b");
    let cfg = cfg_for(&home);
    a2a::mesh::feed::append_entry(&home, &cfg, local_feed_entry("known origin entry")).unwrap();
    let state = LinkStateHandle::new(&home, Duration::from_secs(60)).unwrap();
    let clock = TestClock::new(Utc::now());
    let (mut peer, side) = a2a::mesh::wire::test_link(&clock).await;
    let token = CancellationToken::new();
    let task = tokio::spawn(a2a::mesh::gateway::link::run_link_session(
        side,
        LinkSessionParams::new(
            home.clone(),
            cfg,
            "gw-b",
            "gw-a",
            LinkDirection::Inbound,
            state,
            token.child_token(),
        ),
    ));
    assert!(matches!(peer.recv().await.unwrap(), Some(Frame::Digest { .. })));
    let mut wants = (0..MAX_ORIGINS_PER_REQUEST_FRAME)
        .map(|_| OriginWant {
            origin: "gw-b".to_string(),
            from_seq: 1,
        })
        .collect::<Vec<_>>();
    wants.extend((MAX_ORIGINS_PER_REQUEST_FRAME..200).map(|index| OriginWant {
        origin: format!("gw-unknown-{index}"),
        from_seq: 1,
    }));
    peer.send(&Frame::Want { origins: wants }).await.unwrap();
    for _ in 0..MAX_ORIGINS_PER_REQUEST_FRAME {
        assert!(matches!(
            peer.recv().await.unwrap(),
            Some(Frame::Feed { ref origin, .. }) if origin == "gw-b"
        ));
    }
    assert!(tokio::time::timeout(Duration::from_millis(50), peer.recv())
        .await
        .is_err());
    peer.send(&Frame::Want {
        origins: vec![OriginWant {
            origin: "gw-never-known".to_string(),
            from_seq: 1,
        }],
    })
    .await
    .unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(50), peer.recv())
        .await
        .is_err());
    token.cancel();
    task.await.unwrap().unwrap();

    let metered_home = home_at(&tmp.path().join("metered"));
    write_gateway_key(metered_home.root(), "gw-b");
    let mut metered_cfg = cfg_for(&metered_home);
    metered_cfg.mesh.limits.wants_per_min = 1;
    metered_cfg.mesh.limits.digests_per_min = 1;
    let metered_state = LinkStateHandle::new(&metered_home, Duration::from_secs(60)).unwrap();
    let (mut metered_peer, metered_side) = a2a::mesh::wire::test_link(&clock).await;
    let metered_token = CancellationToken::new();
    let metered_task = tokio::spawn(a2a::mesh::gateway::link::run_link_session(
        metered_side,
        LinkSessionParams::new(
            metered_home,
            metered_cfg,
            "gw-b",
            "gw-a",
            LinkDirection::Inbound,
            metered_state.clone(),
            metered_token.child_token(),
        ),
    ));
    assert!(matches!(
        metered_peer.recv().await.unwrap(),
        Some(Frame::Digest { .. })
    ));
    for frame in [
        Frame::Digest { origins: vec![] },
        Frame::Digest { origins: vec![] },
        Frame::Want { origins: vec![] },
        Frame::Want { origins: vec![] },
    ] {
        metered_peer.send(&frame).await.unwrap();
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let health = metered_state.limits_health().await;
        if health
            .get("gw-a")
            .is_some_and(|peer| peer.defers.get("digests") == Some(&1)
                && peer.defers.get("wants") == Some(&1))
        {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    metered_token.cancel();
    metered_task.await.unwrap().unwrap();
}
