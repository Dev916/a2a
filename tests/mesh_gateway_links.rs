use a2a::mesh::gateway::link::{
    accept_once_io, dial_gateway_io, discover_outbound_peers, jittered_redial_backoff,
    LinkDirection, LinkLiveness, LinkRuntimeEvent, LinkTaskSet, LinkWriteOp, OutboundQueue,
    PeerDamping, PRESENCE_MAILBOX_CAPACITY,
};
use a2a::mesh::registry::{FileRegistry, GatewayRow, Registry, REGISTRY_SCHEMA_VERSION};
use a2a::mesh::wire::{Frame, TlsIdentity};
use chrono::{TimeZone, Utc};
use rand::{rngs::StdRng, SeedableRng};
use serde_json::json;
use std::time::Duration;
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

#[tokio::test]
async fn discovery_orders_deduped_links_and_damped_side_still_accepts_inbound() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    registry.init_schema().unwrap();
    registry.upsert_self(row("gw-a", Some("wss://127.0.0.1:1"))).await.unwrap();
    registry.upsert_self(row("gw-b", Some("wss://127.0.0.1:2"))).await.unwrap();
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

    assert!(damping.record_connect_drop("gw-b", now, Duration::from_secs(1_920)).is_none());
    assert!(damping
        .record_connect_drop("gw-b", now + chrono::Duration::seconds(60), Duration::from_secs(1_920))
        .is_none());
    let event = damping
        .record_connect_drop("gw-b", now + chrono::Duration::seconds(120), Duration::from_secs(1_920))
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
async fn acceptor_and_dialer_connect_over_discovered_injected_io_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    registry.init_schema().unwrap();
    registry.upsert_self(row("gw-a", Some("wss://127.0.0.1:1"))).await.unwrap();

    let tls = TlsIdentity::self_signed(vec!["localhost".to_string()]).unwrap();
    registry
        .upsert_self(row(
            "gw-b",
            Some("wss://127.0.0.1:7787"),
        ))
        .await
        .unwrap();
    registry
        .revoke("fleet-a", "gw-a", Utc.timestamp_opt(1_700_000_001, 0).unwrap())
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
    assert_eq!(writer_rx.recv().await.unwrap(), LinkWriteOp::Close(Frame::Err {
        code: "LIVENESS_TIMEOUT".to_string(),
        detail: "no read activity for 10s".to_string(),
    }));
    assert_eq!(
        event_rx.recv().await.unwrap(),
        LinkRuntimeEvent::ClosedForLiveness
    );
    task.await.unwrap();
}

#[test]
fn inbound_mailboxes_apply_class_specific_capacity_backpressure_and_busy() {
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let mut inbox = a2a::mesh::gateway::link::InboundMailboxes::new(
        2,
        Duration::from_secs(3),
    );

    assert!(inbox.offer_feed(1, now).is_accepted());
    assert!(inbox.offer_note(2, now).is_accepted());
    assert!(inbox.offer_feed(3, now + chrono::Duration::seconds(1)).is_backpressure());
    assert_eq!(inbox.lossy_payload_drops(), 0);
    assert_eq!(inbox.drain_payloads(), vec![1, 2]);
    assert!(inbox.offer_feed(3, now + chrono::Duration::seconds(2)).is_accepted());
    assert!(inbox.offer_note(4, now + chrono::Duration::seconds(2)).is_accepted());
    assert_eq!(
        inbox.offer_feed(5, now + chrono::Duration::seconds(6)).err_frame(),
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
        outbound.drain().into_iter().map(|v| v["seq"].as_u64().unwrap()).collect::<Vec<_>>(),
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
        tasks.spawned_peer_states()
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
