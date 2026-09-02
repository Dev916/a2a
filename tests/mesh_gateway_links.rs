mod common;

use a2a::mesh::gateway::link::{
    accept_once_io, dial_gateway_io, discover_outbound_peers, jittered_redial_backoff,
    LinkDirection, LinkHealthEvent, LinkLiveness, LinkObserver, LinkRuntimeEvent,
    LinkSessionParams, LinkStateHandle, LinkTaskSet, LinkWriteOp, OutboundQueue, PeerDamping,
    PRESENCE_MAILBOX_CAPACITY,
};
use a2a::mesh::keys::{fleet_init, sign_token, RootSet, TestClock, TokenClaims};
use a2a::mesh::registry::{FileRegistry, GatewayRow, Registry, REGISTRY_SCHEMA_VERSION};
use a2a::mesh::wire::{
    handshake_acceptor, handshake_dialer, test_link, Frame, HandshakeIdentity, TlsIdentity,
};
use a2a::mesh::FeedEntry;
use a2a::{config, mesh};
use chrono::{TimeZone, Utc};
use ed25519_dalek::SigningKey;
use nix::fcntl::{Flock, FlockArg};
use predicates::prelude::*;
use rand::RngCore;
use rand::{rngs::StdRng, SeedableRng};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn row(gateway_id: &str, endpoint: Option<&str>) -> GatewayRow {
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    GatewayRow {
        fleet_id: "fleet-a".to_string(),
        gateway_id: gateway_id.to_string(),
        owner: gateway_id.to_string(),
        endpoint_url: endpoint.map(str::to_string),
        pubkey_fpr: format!("{gateway_id}-pub"),
        cert_fpr: Some(format!("{gateway_id}-cert")),
        version: "test".to_string(),
        created_at: now,
        last_heartbeat: now,
        revoked_at: None,
        schema_version: REGISTRY_SCHEMA_VERSION,
    }
}

fn home_at(path: &Path) -> a2a::home::A2aHome {
    a2a::home::A2aHome::resolve(Some(path)).unwrap()
}

fn cfg_for(home: &a2a::home::A2aHome) -> config::Config {
    config::load(home).unwrap().0
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

fn signing_key() -> SigningKey {
    let mut seed = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut seed);
    SigningKey::from_bytes(&seed)
}

fn b64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(b2 & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

fn handshake_identity(
    home: &a2a::home::A2aHome,
    key: SigningKey,
    gateway_id: &str,
) -> HandshakeIdentity {
    let now = Utc::now().timestamp();
    let claims = TokenClaims {
        fleet_id: "filled-by-signer".to_string(),
        gateway_id: gateway_id.to_string(),
        pubkey_b64: b64_encode(&key.verifying_key().to_bytes()),
        issued_at: now - 10,
        exp: now + 1_000,
    };
    HandshakeIdentity::new(sign_token(&claims, home).unwrap(), key)
}

fn feed_entry(text: &str) -> FeedEntry {
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

async fn wait_for(mut pred: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if pred() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition was not met before timeout"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn recv_link_event(
    events: &mut mpsc::Receiver<LinkHealthEvent>,
    mut pred: impl FnMut(&LinkHealthEvent) -> bool,
) -> LinkHealthEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let event = tokio::time::timeout(Duration::from_millis(250), events.recv())
            .await
            .unwrap_or(None)
            .expect("link event channel stayed open");
        if pred(&event) {
            return event;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected link event was not emitted; last event was {event:?}"
        );
    }
}

fn lock_gateway(home: &Path) -> Flock<std::fs::File> {
    let mesh = home.join("mesh");
    fs::create_dir_all(&mesh).unwrap();
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(mesh.join("gateway.lock"))
        .unwrap();
    Flock::lock(file, FlockArg::LockExclusive).unwrap()
}

fn write_health_with_link(home: &Path) {
    let mesh = home.join("mesh");
    fs::create_dir_all(&mesh).unwrap();
    fs::write(
        mesh.join("gateway-health.json"),
        r#"{
          "schema_version": 2,
          "pid": 4242,
          "health_seq": 7,
          "written_at": "2026-09-02T00:00:00Z",
          "state": "running",
          "listen_port": 7787,
          "expose": "none",
          "endpoint_url": null,
          "exposure_error": null,
          "exposure_error_since": null,
          "registry_error": null,
          "registry_error_since": null,
          "loops": {},
          "replication": {"per_origin_lag": {}, "last_anti_entropy": null},
          "links": [{
            "peer": "gw-b",
            "direction": "outbound",
            "established_at": "2026-09-02T00:00:01Z",
            "last_rx": "2026-09-02T00:00:02Z",
            "last_tx": "2026-09-02T00:00:03Z",
            "frames_rx": 2,
            "frames_tx": 3
          }],
          "escalation": null,
          "shutdown": null
        }"#,
    )
    .unwrap();
}

#[test]
fn mesh_links_cli_renders_health_and_empty_down_daemon_with_hint() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();

    common::a2a(&home, &work)
        .args(["mesh", "links"])
        .assert()
        .success()
        .stdout(predicate::eq("[]\n"))
        .stderr(predicate::str::contains("mesh gateway is not running"));

    write_health_with_link(&home);
    let _lock = lock_gateway(&home);
    let out = common::a2a(&home, &work)
        .args(["--json", "mesh", "links"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["data"][0]["peer"], "gw-b");
    assert_eq!(v["data"][0]["direction"], "outbound");
    assert_eq!(v["data"][0]["frames_rx"], 2);
    assert_eq!(v["data"][0]["frames_tx"], 3);

    common::a2a(&home, &work)
        .args(["mesh", "links"])
        .assert()
        .success()
        .stdout(predicate::str::contains("gw-b"))
        .stdout(predicate::str::contains("outbound"))
        .stdout(predicate::str::contains("frames_tx=3"));
}

#[tokio::test]
async fn discovery_orders_deduped_links_and_damped_side_still_accepts_inbound() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    registry.init_schema().unwrap();
    registry
        .upsert_self(row("gw-a", Some("wss://127.0.0.1:1")))
        .await
        .unwrap();
    registry
        .upsert_self(row("gw-b", Some("wss://127.0.0.1:2")))
        .await
        .unwrap();
    registry.upsert_self(row("gw-c", None)).await.unwrap();
    let rows = registry.discover("fleet-a").await.unwrap();

    assert_eq!(
        discover_outbound_peers("gw-a", &rows)
            .into_iter()
            .map(|r| r.gateway_id.clone())
            .collect::<Vec<_>>(),
        vec!["gw-b"]
    );
    assert!(discover_outbound_peers("gw-b", &rows).is_empty());

    let mut damping = PeerDamping::new(Duration::from_secs(300));
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    assert!(damping.admit("gw-a", "gw-b", LinkDirection::Outbound, now));
    assert!(!damping.admit("gw-a", "gw-b", LinkDirection::Inbound, now));
    assert!(damping.admit("gw-b", "gw-a", LinkDirection::Inbound, now));
    assert!(!damping.admit("gw-b", "gw-a", LinkDirection::Outbound, now));

    assert!(damping
        .record_connect_drop("gw-b", now, Duration::from_secs(1_920))
        .is_none());
    assert!(damping
        .record_connect_drop(
            "gw-b",
            now + chrono::Duration::seconds(60),
            Duration::from_secs(1_920)
        )
        .is_none());
    let event = damping
        .record_connect_drop(
            "gw-b",
            now + chrono::Duration::seconds(120),
            Duration::from_secs(1_920),
        )
        .expect("third drop logs link_flapping");
    assert_eq!(event.kind, "link_flapping");
    assert!(!damping.can_dial("gw-b", now + chrono::Duration::seconds(121)));
    assert!(damping.admit(
        "gw-a",
        "gw-b",
        LinkDirection::Inbound,
        now + chrono::Duration::seconds(121)
    ));
    assert_eq!(
        damping.health(now + chrono::Duration::seconds(121))["gw-b"]["state"],
        "cooldown"
    );
}

#[tokio::test]
async fn link_session_emits_up_stat_and_down_events_for_gateway_health() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    write_gateway_key(home.root(), "gw-a");
    let cfg = cfg_for(&home);
    let state = LinkStateHandle::new(&home, Duration::from_secs(60)).unwrap();
    let clock = TestClock::new(Utc::now());
    let (mut peer, side) = test_link(&clock).await;
    let token = CancellationToken::new();
    let (observer, mut events) = LinkObserver::channel(16);
    let task = tokio::spawn(a2a::mesh::gateway::link::run_link_session(
        side,
        LinkSessionParams::new(
            home,
            cfg,
            "gw-a",
            "gw-b",
            LinkDirection::Outbound,
            state,
            token.child_token(),
        )
        .with_observer(observer),
    ));

    assert!(matches!(
        events.recv().await.unwrap(),
        LinkHealthEvent::Up {
            peer,
            direction: LinkDirection::Outbound,
            ..
        } if peer == "gw-b"
    ));
    peer.recv().await.unwrap().expect("initial digest frame");
    recv_link_event(&mut events, |event| {
        matches!(
            event,
            LinkHealthEvent::Stat {
                peer,
                direction: LinkDirection::Outbound,
                frames_tx: 1,
                ..
            } if peer == "gw-b"
        )
    })
    .await;

    peer.send(&Frame::Presence { peers: Vec::new() }).await.unwrap();
    recv_link_event(&mut events, |event| {
        matches!(
            event,
            LinkHealthEvent::Stat {
                peer,
                direction: LinkDirection::Outbound,
                frames_rx: 1,
                ..
            } if peer == "gw-b"
        )
    })
    .await;

    token.cancel();
    task.await.unwrap().unwrap();
    recv_link_event(&mut events, |event| {
        matches!(
            event,
            LinkHealthEvent::Down {
                peer,
                direction: LinkDirection::Outbound
            } if peer == "gw-b"
        )
    })
    .await;
}

#[test]
fn peer_damping_reset_peer_drops_cooldown_and_flap_history() {
    let mut damping = PeerDamping::new(Duration::from_secs(300));
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    assert!(damping
        .record_connect_drop("gw-b", now, Duration::from_secs(1_920))
        .is_none());
    assert!(damping
        .record_connect_drop(
            "gw-b",
            now + chrono::Duration::seconds(60),
            Duration::from_secs(1_920)
        )
        .is_none());
    assert!(damping
        .record_connect_drop(
            "gw-b",
            now + chrono::Duration::seconds(120),
            Duration::from_secs(1_920)
        )
        .is_some());
    assert!(!damping.can_dial("gw-b", now + chrono::Duration::seconds(121)));

    damping.reset_peer("gw-b");

    assert!(damping.can_dial("gw-b", now + chrono::Duration::seconds(121)));
    assert!(damping.health(now + chrono::Duration::seconds(121))["gw-b"].is_null());
}

#[test]
fn refresh_dial_state_resets_changed_rows_preserves_unchanged_and_forgets_vanished_peers() {
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let mut last_seen = BTreeMap::from([
        (
            "gw-b".to_string(),
            (Some("wss://127.0.0.1:1".to_string()), Some("old-cert".to_string())),
        ),
        (
            "gw-c".to_string(),
            (Some("wss://127.0.0.1:3".to_string()), Some("gw-c-cert".to_string())),
        ),
        (
            "gw-d".to_string(),
            (Some("wss://127.0.0.1:4".to_string()), Some("gw-d-cert".to_string())),
        ),
    ]);
    let mut attempts = BTreeMap::from([
        ("gw-b".to_string(), 4),
        ("gw-c".to_string(), 2),
        ("gw-d".to_string(), 7),
    ]);
    let mut next_dial_after = BTreeMap::from([
        ("gw-b".to_string(), Instant::now() + Duration::from_secs(600)),
        ("gw-c".to_string(), Instant::now() + Duration::from_secs(600)),
        ("gw-d".to_string(), Instant::now() + Duration::from_secs(600)),
    ]);
    let mut damping = PeerDamping::new(Duration::from_secs(300));
    for offset in [0, 60, 120] {
        let _ = damping.record_connect_drop(
            "gw-b",
            now + chrono::Duration::seconds(offset),
            Duration::from_secs(1_920),
        );
    }
    assert!(!damping.can_dial("gw-b", now + chrono::Duration::seconds(121)));

    let mut changed = row("gw-b", Some("wss://127.0.0.1:2"));
    changed.cert_fpr = Some("old-cert".to_string());
    let unchanged = row("gw-c", Some("wss://127.0.0.1:3"));
    let reset = a2a::mesh::gateway::refresh_dial_state(
        &mut last_seen,
        &[changed, unchanged],
        &mut attempts,
        &mut next_dial_after,
        &mut damping,
    );

    assert_eq!(reset, vec!["gw-b".to_string()]);
    assert!(!attempts.contains_key("gw-b"));
    assert!(!next_dial_after.contains_key("gw-b"));
    assert!(damping.can_dial("gw-b", now + chrono::Duration::seconds(121)));
    assert_eq!(attempts.get("gw-c"), Some(&2));
    assert!(next_dial_after.contains_key("gw-c"));
    assert!(!last_seen.contains_key("gw-d"));
    assert!(!attempts.contains_key("gw-d"));
    assert!(!next_dial_after.contains_key("gw-d"));
    assert_eq!(
        last_seen.get("gw-b"),
        Some(&(Some("wss://127.0.0.1:2".to_string()), Some("old-cert".to_string())))
    );
}

#[tokio::test]
async fn acceptor_and_dialer_connect_over_discovered_injected_io_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    registry.init_schema().unwrap();
    registry
        .upsert_self(row("gw-a", Some("wss://127.0.0.1:1")))
        .await
        .unwrap();

    let tls = TlsIdentity::self_signed(vec!["localhost".to_string()]).unwrap();
    registry
        .upsert_self(row("gw-b", Some("wss://127.0.0.1:7787")))
        .await
        .unwrap();
    registry
        .revoke(
            "fleet-a",
            "gw-a",
            Utc.timestamp_opt(1_700_000_001, 0).unwrap(),
        )
        .await
        .unwrap();
    let mut rows = registry.discover("fleet-a").await.unwrap();
    rows[0].cert_fpr = Some(tls.spki_fpr().to_string());

    let (client, server) = tokio::io::duplex(a2a::mesh::wire::MAX_FRAME_BYTES * 2 + 4096);
    let accept = tokio::spawn(accept_once_io(server, tls));
    let mut side = dial_gateway_io(client, discover_outbound_peers("gw-a", &rows)[0])
        .await
        .unwrap();

    let mut accepted = accept.await.unwrap().unwrap();
    side.send(&Frame::Ack {
        ref_id: "probe".to_string(),
        outcome: "ok".to_string(),
    })
    .await
    .unwrap();
    assert_eq!(
        accepted.recv().await.unwrap(),
        Some(Frame::Ack {
            ref_id: "probe".to_string(),
            outcome: "ok".to_string()
        })
    );
    side.close().await.unwrap();
}

#[tokio::test]
async fn handshaked_link_sessions_exchange_digest_want_feed_until_homes_converge() {
    let tmp = tempfile::tempdir().unwrap();
    let trust_home = home_at(&tmp.path().join("trust"));
    fleet_init(&trust_home).unwrap();
    let home_a = home_at(&tmp.path().join("a"));
    let home_b = home_at(&tmp.path().join("b"));
    write_gateway_key(home_a.root(), "gw-a");
    write_gateway_key(home_b.root(), "gw-b");
    let cfg_a = cfg_for(&home_a);
    let cfg_b = cfg_for(&home_b);
    mesh::feed::append_entry(&home_a, &cfg_a, feed_entry("from a")).unwrap();

    let clock = TestClock::new(Utc::now());
    let (mut accept, mut dial) = test_link(&clock).await;
    let roots = RootSet::load(&trust_home).unwrap();
    let accept_id = handshake_identity(&trust_home, signing_key(), "gw-a");
    let accept_task = tokio::spawn(async move {
        let peer = handshake_acceptor(&mut accept, &accept_id, &roots, &clock)
            .await
            .unwrap();
        (accept, peer.gateway_id)
    });
    let roots = RootSet::load(&trust_home).unwrap();
    let clock = TestClock::new(Utc::now());
    let dial_id = handshake_identity(&trust_home, signing_key(), "gw-b");
    let peer = handshake_dialer(&mut dial, &dial_id, &roots, &clock)
        .await
        .unwrap();
    let (accept, accept_peer) = accept_task.await.unwrap();

    let state_a = LinkStateHandle::new(&home_a, Duration::from_secs(60)).unwrap();
    let state_b = LinkStateHandle::new(&home_b, Duration::from_secs(60)).unwrap();
    let token = CancellationToken::new();
    let task_a = tokio::spawn(a2a::mesh::gateway::link::run_link_session(
        accept,
        LinkSessionParams::new(
            home_a.clone(),
            cfg_a,
            "gw-a",
            accept_peer,
            LinkDirection::Inbound,
            state_a,
            token.child_token(),
        ),
    ));
    let task_b = tokio::spawn(a2a::mesh::gateway::link::run_link_session(
        dial,
        LinkSessionParams::new(
            home_b.clone(),
            cfg_b,
            "gw-b",
            peer.gateway_id,
            LinkDirection::Outbound,
            state_b,
            token.child_token(),
        ),
    ));

    wait_for(|| {
        mesh::feed::read(&home_b, 0)
            .unwrap()
            .iter()
            .any(|entry| entry.origin.as_deref() == Some("gw-a") && entry.text == "from a")
    })
    .await;
    token.cancel();
    task_a.await.unwrap().unwrap();
    task_b.await.unwrap().unwrap();
}

#[tokio::test]
async fn session_busy_admission_sends_busy_err_and_closes() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    write_gateway_key(home.root(), "gw-b");
    let cfg = cfg_for(&home);
    let state = LinkStateHandle::new(&home, Duration::from_secs(60)).unwrap();
    let clock = TestClock::new(Utc::now());
    let (mut peer, side) = test_link(&clock).await;
    let token = CancellationToken::new();
    let mut params = LinkSessionParams::new(
        home,
        cfg,
        "gw-b",
        "gw-a",
        LinkDirection::Inbound,
        state,
        token.child_token(),
    );
    params.mailbox_payload_cap = 0;
    params.mailbox_backpressure_window = Duration::ZERO;
    let task = tokio::spawn(a2a::mesh::gateway::link::run_link_session(side, params));

    peer.recv().await.unwrap();
    peer.send(&Frame::Feed {
        origin: "gw-a".to_string(),
        entries: vec![FeedEntry {
            seq: 1,
            origin: Some("gw-a".to_string()),
            origin_seq: Some(1),
            from: "codex:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
            ts: Utc::now(),
            text: "blocked".to_string(),
            refs: json!({}),
            extra: serde_json::Map::new(),
        }],
    })
    .await
    .unwrap();
    assert_eq!(
        peer.recv().await.unwrap(),
        Some(Frame::Err {
            code: "BUSY".to_string(),
            detail: "inbound payload mailbox remained full for 0s".to_string()
        })
    );
    assert_eq!(peer.recv().await.unwrap(), None);
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn liveness_pings_and_closes_after_two_missed_read_windows() {
    let token = CancellationToken::new();
    let (activity_tx, activity_rx) = mpsc::channel(4);
    let (writer_tx, mut writer_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let task = tokio::spawn(LinkLiveness::new(Duration::from_secs(5)).run(
        activity_rx,
        writer_tx,
        event_tx,
        token.clone(),
    ));

    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(writer_rx.recv().await.unwrap(), LinkWriteOp::Ping(vec![]));

    activity_tx.send(()).await.unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(writer_rx.recv().await.unwrap(), LinkWriteOp::Ping(vec![]));

    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(writer_rx.recv().await.unwrap(), LinkWriteOp::Ping(vec![]));
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        writer_rx.recv().await.unwrap(),
        LinkWriteOp::Close(Frame::Err {
            code: "LIVENESS_TIMEOUT".to_string(),
            detail: "no read activity for 10s".to_string(),
        })
    );
    assert_eq!(
        event_rx.recv().await.unwrap(),
        LinkRuntimeEvent::ClosedForLiveness
    );
    task.await.unwrap();
}

#[test]
fn inbound_mailboxes_apply_class_specific_capacity_backpressure_and_busy() {
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let mut inbox = a2a::mesh::gateway::link::InboundMailboxes::new(2, Duration::from_secs(3));

    assert!(inbox.offer_feed(1, now).is_accepted());
    assert!(inbox.offer_note(2, now).is_accepted());
    assert!(inbox
        .offer_feed(3, now + chrono::Duration::seconds(1))
        .is_backpressure());
    assert_eq!(inbox.lossy_payload_drops(), 0);
    assert_eq!(inbox.drain_payloads(), vec![1, 2]);
    assert!(inbox
        .offer_feed(3, now + chrono::Duration::seconds(2))
        .is_accepted());
    assert!(inbox
        .offer_note(4, now + chrono::Duration::seconds(2))
        .is_accepted());
    assert_eq!(
        inbox
            .offer_feed(5, now + chrono::Duration::seconds(6))
            .err_frame(),
        Some(Frame::Err {
            code: "BUSY".to_string(),
            detail: "inbound payload mailbox remained full for 3s".to_string()
        })
    );
    assert_eq!(inbox.drain_payloads(), vec![3, 4]);

    for seq in 0..100 {
        inbox.offer_presence(seq, now);
    }
    let present = inbox.drain_presence();
    assert_eq!(present.len(), PRESENCE_MAILBOX_CAPACITY);
    assert_eq!(present.first(), Some(&36));
    assert_eq!(present.last(), Some(&99));
}

#[test]
fn outbound_queue_sheds_feed_and_presence_newest_with_rate_limited_visibility() {
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let mut outbound = OutboundQueue::with_capacity(3);

    assert!(outbound.enqueue_feed(json!({"seq": 1}), now).queued);
    assert!(outbound.enqueue_presence(json!({"seq": 2}), now).queued);
    assert!(outbound.enqueue_feed(json!({"seq": 3}), now).queued);

    let shed = outbound.enqueue_presence(json!({"seq": 4}), now);
    assert!(!shed.queued);
    assert!(shed.log_outbound_shed);
    assert_eq!(outbound.outbound_shed_count(), 1);
    assert_eq!(outbound.len(), 3);

    let suppressed = outbound.enqueue_feed(json!({"seq": 5}), now + chrono::Duration::seconds(10));
    assert!(!suppressed.queued);
    assert!(!suppressed.log_outbound_shed);
    assert_eq!(outbound.outbound_shed_count(), 2);

    let visible = outbound.enqueue_feed(json!({"seq": 6}), now + chrono::Duration::seconds(61));
    assert!(!visible.queued);
    assert!(visible.log_outbound_shed);
    assert_eq!(
        outbound
            .drain()
            .into_iter()
            .map(|v| v["seq"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/mesh/gateway/link.rs"),
    )
    .unwrap_or_default();
    assert!(
        !src.contains("enqueue_note"),
        "notes must not have an in-memory link enqueue API"
    );
}

#[tokio::test]
async fn link_tasks_are_owned_by_join_set_and_spawned_with_cursor_state() {
    let token = CancellationToken::new();
    let mut tasks = LinkTaskSet::new();
    tasks.spawn_pending("gw-b", json!({"feed": 7}), token.child_token());
    tasks.spawn_pending("gw-c", json!({"feed": 11}), token.child_token());

    assert_eq!(tasks.len(), 2);
    assert_eq!(
        tasks
            .spawned_peer_states()
            .into_iter()
            .map(|s| (s.peer_gateway_id, s.cursors["feed"].as_u64().unwrap()))
            .collect::<Vec<_>>(),
        vec![("gw-b".to_string(), 7), ("gw-c".to_string(), 11)]
    );

    token.cancel();
    let mut exits = Vec::new();
    while let Some(exit) = tasks.join_next().await {
        exits.push(exit.peer_gateway_id);
    }
    exits.sort();
    assert_eq!(exits, vec!["gw-b", "gw-c"]);
}

#[test]
fn redial_backoff_uses_full_jitter_and_distinguishes_recovering_links() {
    let mut first = StdRng::seed_from_u64(7);
    let mut second = StdRng::seed_from_u64(99);
    let a = jittered_redial_backoff(5, Duration::from_secs(60), &mut first);
    let b = jittered_redial_backoff(5, Duration::from_secs(60), &mut second);

    assert!(a <= Duration::from_secs(60));
    assert!(b <= Duration::from_secs(60));
    assert!(a.abs_diff(b) > Duration::from_millis(1), "{a:?} vs {b:?}");
}
