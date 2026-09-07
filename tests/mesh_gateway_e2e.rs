mod common;

use a2a::home::A2aHome;
use a2a::mesh::gateway::{
    notes_wire::{self, deliver_spooled_once, NoteAck, NoteDeliveryTarget},
    presence_wire,
    replicate::{self, AntiEntropyApply, AntiEntropyCycle, CursorStore},
};
use a2a::mesh::keys::{fleet_init, sign_token, RootSet, TestClock, TokenClaims};
use a2a::mesh::registry::{FileRegistry, GatewayRow, Registry, REGISTRY_SCHEMA_VERSION};
use a2a::mesh::wire::{
    accept_pinned, connect_pinned, handshake_acceptor, handshake_dialer, test_link, Frame,
    HandshakeIdentity, OriginWant, TlsIdentity, MAX_FRAME_BYTES,
};
use a2a::mesh::{Note, PeerId, Presence};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use ed25519_dalek::SigningKey;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde_json::{json, Value};
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

// Spec §11 e2e list, pinned verbatim for the layer-2 mesh fleet gate:
// fleet init → enroll ×3 → loopback gateways → feed replicates with `origin:seq` attribution and dedup on reconnect (incl. a `want{}` catch-up after a dropped link); three-gateway TRANSITIVE relay (A—B—C with C outbound-only, A's entry reaches C); directed note to a remote peer via the full ingress path + `inject`; **lost-ack redelivery dedup'd by `note_id`** (exactly one inbox file); offline-queue then deliver on restart; secret-note refusal acked+archived with the SAME `trace_id` in both gateways' logs; token-expiry and bad-PoP handshake refusals; mid-burst cancellation leaves no torn frame; two recovering gateways' redial timestamps differ (jitter proof); presence merge with a deliberately skewed clock (counter wins).

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

fn registry_row(fleet_id: &str, gateway_id: &str, endpoint_url: Option<&str>) -> GatewayRow {
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    GatewayRow {
        fleet_id: fleet_id.to_string(),
        gateway_id: gateway_id.to_string(),
        owner: gateway_id.to_string(),
        endpoint_url: endpoint_url.map(str::to_string),
        pubkey_fpr: format!("{gateway_id}-pub"),
        cert_fpr: Some(format!("{gateway_id}-cert")),
        version: "e2e".to_string(),
        created_at: now,
        last_heartbeat: now,
        revoked_at: None,
        schema_version: REGISTRY_SCHEMA_VERSION,
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

fn note(from: &str, to: &str, text: &str, note_id: &str) -> Note {
    Note {
        note_id: Some(note_id.to_string()),
        from: from.to_string(),
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
    let inbox = home
        .join("mesh")
        .join("peers")
        .join(id.dir_name())
        .join("inbox");
    let mut files = match fs::read_dir(inbox) {
        Ok(entries) => entries.flatten().map(|e| e.path()).collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    files.retain(|p| {
        p.file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".note.json")
    });
    files.sort();
    files
}

fn json_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = match fs::read_dir(dir) {
        Ok(entries) => entries.flatten().map(|e| e.path()).collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    files.retain(|p| {
        p.file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".note.json")
    });
    files.sort();
    files
}

fn write_presence(home: &Path, peer: &str, home_gateway: Option<&str>, seq: u64, last_seen: i64) {
    let id = PeerId::parse(peer).unwrap();
    let dir = home.join("mesh").join("peers").join(id.dir_name());
    fs::create_dir_all(&dir).unwrap();
    let mut value = json!({
        "peer": peer,
        "provider": id.provider.to_string(),
        "worker_id": null,
        "status": null,
        "working_on": null,
        "cwd": null,
        "registered_at": Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "last_seen": Utc.timestamp_opt(last_seen, 0).unwrap(),
        "retired": false,
        "presence_seq": seq,
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

#[tokio::test]
async fn fleet_e2e_replicates_feeds_routes_notes_and_merges_presence() {
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let registry = FileRegistry::new(tmp.path().join("registry"));
    registry.init_schema().unwrap();

    let home_a = home_at(&tmp.path().join("a"));
    let home_b = home_at(&tmp.path().join("b"));
    let home_c = home_at(&tmp.path().join("c"));
    let fleet = fleet_init(&home_a).unwrap();
    for (home, gateway_id, endpoint) in [
        (home_a.root(), "gw-a", Some("wss://127.0.0.1:7101")),
        (home_b.root(), "gw-b", Some("wss://127.0.0.1:7102")),
        (home_c.root(), "gw-c", None),
    ] {
        write_gateway_key(home, gateway_id);
        registry
            .upsert_self(registry_row(&fleet.fleet_id, gateway_id, endpoint))
            .await
            .unwrap();
    }
    let rows = registry.discover(&fleet.fleet_id).await.unwrap();
    assert_eq!(rows.len(), 3, "fleet init plus three enrolled gateway rows");
    assert_eq!(
        rows.iter()
            .find(|row| row.gateway_id == "gw-c")
            .unwrap()
            .endpoint_url,
        None,
        "C is outbound-only in the registry"
    );

    common::a2a(home_a.root(), &work)
        .args([
            "mesh",
            "post",
            "--session",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "a first",
        ])
        .assert()
        .success();
    let cfg = cfg_for(&home_a);
    let mut b_cursors = CursorStore::load_or_rebuild(&home_b).unwrap();
    assert_eq!(
        replicate::replicate_feed_once(&cfg, &home_a, "gw-a", &home_b, &mut b_cursors).unwrap(),
        1
    );
    let rendered = common::a2a(home_b.root(), &work)
        .args(["mesh", "feed"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert!(String::from_utf8(rendered).unwrap().starts_with("gw-a:1 "));
    assert_eq!(
        replicate::replicate_feed_once(&cfg, &home_a, "gw-a", &home_b, &mut b_cursors).unwrap(),
        0,
        "reconnect replay is deduped"
    );

    common::a2a(home_a.root(), &work)
        .args([
            "mesh",
            "post",
            "--session",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "a second after reconnect",
        ])
        .assert()
        .success();
    let digest = replicate::digest_for_home(&home_a).unwrap();
    assert_eq!(
        replicate::wants_from_digest(&home_b, &b_cursors, &digest).unwrap(),
        vec![OriginWant {
            origin: "gw-a".to_string(),
            from_seq: 2,
        }]
    );
    let repair = replicate::entries_for_want_capped(&home_a, "gw-a", 2, &b_cursors, 16)
        .unwrap()
        .entries()
        .to_vec();
    let report = AntiEntropyCycle::new(ChronoDuration::seconds(0), 1)
        .apply(AntiEntropyApply {
            home: &home_b,
            cfg: &cfg,
            cursors: &mut b_cursors,
            peer_origin: "gw-a",
            remote_digest: digest,
            repair_entries: repair,
            now: Utc.timestamp_opt(1_700_000_020, 0).unwrap(),
        })
        .unwrap();
    assert_eq!(report.repaired_entries, 1);
    assert!(report.drift.is_empty(), "catch-up cleared anti-entropy lag");

    let mut c_cursors = CursorStore::load_or_rebuild(&home_c).unwrap();
    assert_eq!(
        replicate::replicate_feed_once(&cfg, &home_b, "gw-b", &home_c, &mut c_cursors).unwrap(),
        2,
        "A's origin relays transitively through B to outbound-only C"
    );
    let c_rows = feed_json(home_c.root(), &work);
    assert_eq!(
        c_rows
            .iter()
            .filter(|row| row["origin"] == "gw-a")
            .map(|row| row["origin_seq"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let target = "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    notes_wire::spool_note(
        &home_a,
        &cfg,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            target,
            "remote ingress note",
            "01K3KV00000000000000001001",
        ),
    )
    .unwrap();
    let delivered = deliver_spooled_once(
        &cfg,
        &home_a,
        NoteDeliveryTarget::Gateway {
            to_gateway: "gw-b",
            home: Some(&home_b),
        },
        Utc.timestamp_opt(1_700_000_030, 0).unwrap(),
        &mut StdRng::seed_from_u64(1),
    )
    .unwrap();
    assert_eq!(delivered.delivered, 1);
    let injected = common::a2a(home_b.root(), &work)
        .args([
            "mesh",
            "inject",
            "--provider",
            "codex",
            "--session",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert!(String::from_utf8(injected)
        .unwrap()
        .contains("remote ingress note"));

    let lost_ack = note(
        "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        target,
        "redeliver once",
        "01K3KV00000000000000001002",
    );
    notes_wire::spool_note(&home_a, &cfg, "gw-a", "gw-b", lost_ack.clone()).unwrap();
    let queued = json_files(&home_a.root().join("mesh").join("outbound").join("gw-b"));
    let envelope = notes_wire::read_spooled(queued.last().unwrap()).unwrap();
    assert_eq!(
        notes_wire::receive_note(&home_b, &cfg, &envelope).unwrap(),
        NoteAck::Delivered
    );
    let before = note_files(home_b.root(), target).len();
    let redelivery = deliver_spooled_once(
        &cfg,
        &home_a,
        NoteDeliveryTarget::Gateway {
            to_gateway: "gw-b",
            home: Some(&home_b),
        },
        Utc.timestamp_opt(1_700_000_040, 0).unwrap(),
        &mut StdRng::seed_from_u64(2),
    )
    .unwrap();
    assert_eq!(redelivery.duplicates, 1);
    assert_eq!(
        note_files(home_b.root(), target).len(),
        before,
        "lost ack leaves exactly one inbox file"
    );

    notes_wire::spool_note(
        &home_a,
        &cfg,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            target,
            "offline then restart",
            "01K3KV00000000000000001003",
        ),
    )
    .unwrap();
    let offline = deliver_spooled_once(
        &cfg,
        &home_a,
        NoteDeliveryTarget::Gateway {
            to_gateway: "gw-b",
            home: None,
        },
        Utc.timestamp_opt(1_700_000_050, 0).unwrap(),
        &mut StdRng::seed_from_u64(3),
    )
    .unwrap();
    assert_eq!(offline.retrying, 1);
    let held = notes_wire::read_spooled(
        &json_files(&home_a.root().join("mesh").join("outbound").join("gw-b"))[0],
    )
    .unwrap();
    let restarted = deliver_spooled_once(
        &cfg,
        &home_a,
        NoteDeliveryTarget::Gateway {
            to_gateway: "gw-b",
            home: Some(&home_b),
        },
        held.next_attempt_after.unwrap(),
        &mut StdRng::seed_from_u64(4),
    )
    .unwrap();
    assert_eq!(restarted.delivered, 1);

    let trace_id = "01K3KV00000000000000001004";
    notes_wire::spool_note(
        &home_a,
        &cfg,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            target,
            "OPENAI_API_KEY=sk-abc123456789012345678901",
            trace_id,
        ),
    )
    .unwrap();
    let rejected = deliver_spooled_once(
        &cfg,
        &home_a,
        NoteDeliveryTarget::Gateway {
            to_gateway: "gw-b",
            home: Some(&home_b),
        },
        Utc.timestamp_opt(1_700_000_060, 0).unwrap(),
        &mut StdRng::seed_from_u64(5),
    )
    .unwrap();
    assert_eq!(rejected.rejected, 1);
    for home in [&home_a, &home_b] {
        let log = fs::read_to_string(
            home.root()
                .join("mesh")
                .join("gateway")
                .join("gateway.jsonl"),
        )
        .unwrap();
        assert!(log.contains(trace_id), "{log}");
    }

    write_presence(home_b.root(), target, Some("gw-b"), 7, 1_700_000_000);
    let mut incoming: Presence = serde_json::from_slice(
        &fs::read(
            home_b
                .root()
                .join("mesh")
                .join("peers")
                .join(PeerId::parse(target).unwrap().dir_name())
                .join("presence.json"),
        )
        .unwrap(),
    )
    .unwrap();
    incoming.presence_seq = 8;
    incoming.last_seen = Utc.timestamp_opt(1_699_999_000, 0).unwrap();
    incoming.status = Some("counter wins over skewed clock".to_string());
    assert!(presence_wire::merge_presence(&home_c, incoming, "gw-b").unwrap());
    let cached = a2a::mesh::presence::load(&home_c, &PeerId::parse(target).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        cached.status.as_deref(),
        Some("counter wins over skewed clock")
    );

    let mut first = StdRng::seed_from_u64(7);
    let mut second = StdRng::seed_from_u64(99);
    let a =
        a2a::mesh::gateway::link::jittered_redial_backoff(5, Duration::from_secs(60), &mut first);
    let b =
        a2a::mesh::gateway::link::jittered_redial_backoff(5, Duration::from_secs(60), &mut second);
    assert!(a.abs_diff(b) > Duration::from_millis(1), "{a:?} vs {b:?}");
}

#[tokio::test]
async fn loopback_tls_frames_run_when_loopback_bind_is_available() {
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(e) if e.kind() == ErrorKind::PermissionDenied => {
            eprintln!("SKIP: sandbox denied loopback bind for mesh_gateway_e2e");
            return;
        }
        Err(e) => panic!("binding loopback listener: {e}"),
    };
    let addr = listener.local_addr().unwrap();
    let tls = TlsIdentity::self_signed(vec!["localhost".to_string()]).unwrap();
    let fpr = tls.spki_fpr().to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut side = accept_pinned(stream, tls).await.unwrap();
        let frame = side.recv().await.unwrap().unwrap();
        side.send(&frame).await.unwrap();
    });
    let mut client = connect_pinned(&format!("wss://{addr}/"), &fpr)
        .await
        .unwrap();
    let frame = Frame::Ack {
        ref_id: "loopback".to_string(),
        outcome: "ok".to_string(),
    };
    client.send(&frame).await.unwrap();
    assert_eq!(client.recv().await.unwrap(), Some(frame));
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn handshake_refusals_and_mid_burst_cancellation_are_e2e_pinned() {
    let now = 1_700_000_000;
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    fleet_init(&home).unwrap();
    let accept_key = signing_key();
    let accept_id = identity(&home, accept_key, "gw-accept", now);

    let expired_key = signing_key();
    let mut expired_claims = claims_for(&expired_key, "gw-expired", now);
    expired_claims.exp = now - 301;
    let expired = HandshakeIdentity::new(sign_token(&expired_claims, &home).unwrap(), expired_key);
    assert_refused_by_acceptor(&home, &accept_id, expired, "TOKEN_EXPIRED", now).await;

    let token_key = signing_key();
    let wrong_pop_key = signing_key();
    let token = sign_token(&claims_for(&token_key, "gw-bad-pop", now), &home).unwrap();
    let bad_pop = HandshakeIdentity::new(token, wrong_pop_key);
    assert_refused_by_acceptor(&home, &accept_id, bad_pop, "NONCE_SIG_INVALID", now).await;

    let clock = TestClock::new(Utc.timestamp_opt(now, 0).unwrap());
    let (a, b) = test_link(&clock).await;
    let (mut writer, reader_half) = a.split();
    drop(reader_half);
    let (_peer_writer, mut peer_reader) = b.split();
    let text = "x".repeat(MAX_FRAME_BYTES - 1024);
    let task = tokio::spawn(async move {
        writer
            .send(&Frame::Note {
                note: note(
                    "codex:from",
                    "claude:to",
                    &text,
                    "01K3KV00000000000000001005",
                ),
            })
            .await
    });
    task.abort();
    let seen = tokio::time::timeout(Duration::from_secs(1), peer_reader.recv())
        .await
        .unwrap()
        .unwrap();
    if let Some(Frame::Note { note }) = seen {
        assert_eq!(note.text.len(), MAX_FRAME_BYTES - 1024);
    } else {
        assert_eq!(seen, None);
    }
}

async fn assert_refused_by_acceptor(
    home: &A2aHome,
    accept_id: &HandshakeIdentity,
    dial_id: HandshakeIdentity,
    code: &str,
    now: i64,
) {
    let roots = RootSet::load(home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(now, 0).unwrap());
    let (mut accept, mut dial) = test_link(&clock).await;
    let accept_id = accept_id.clone();
    let accept_task =
        tokio::spawn(
            async move { handshake_acceptor(&mut accept, &accept_id, &roots, &clock).await },
        );

    let roots = RootSet::load(home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(now, 0).unwrap());
    let dial_err = handshake_dialer(&mut dial, &dial_id, &roots, &clock)
        .await
        .unwrap_err();
    let accept_err = accept_task.await.unwrap().unwrap_err();
    assert_eq!(accept_err.code, code);
    assert_eq!(dial_err.code, "WIRE_PEER_ERR");
}

fn signing_key() -> SigningKey {
    let mut seed = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut seed);
    SigningKey::from_bytes(&seed)
}

fn claims_for(key: &SigningKey, gateway_id: &str, now: i64) -> TokenClaims {
    TokenClaims {
        fleet_id: "filled-by-signer".to_string(),
        gateway_id: gateway_id.to_string(),
        pubkey_b64: b64_encode(&key.verifying_key().to_bytes()),
        key_epoch: 0,
        issued_at: now - 10,
        exp: now + 1_000,
    }
}

fn identity(home: &A2aHome, key: SigningKey, gateway_id: &str, now: i64) -> HandshakeIdentity {
    let token = sign_token(&claims_for(&key, gateway_id, now), home).unwrap();
    HandshakeIdentity::new(token, key)
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
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}
