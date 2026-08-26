use a2a::home::A2aHome;
use a2a::mesh::keys::{fleet_init, sign_token, RootSet, TestClock, TokenClaims};
use a2a::mesh::{FeedEntry, Note, Presence};
use a2a::mesh::wire::{
    accept_pinned_io, connect_pinned_io, handshake_acceptor, handshake_dialer, test_link, Frame,
    HandshakeIdentity, OriginDigest, OriginWant, TlsIdentity, MAX_FRAME_BYTES,
};
use chrono::{TimeZone, Utc};
use ed25519_dalek::{Signer, SigningKey};
use rand::RngCore;
use serde_json::json;
use std::fs;
use std::time::Duration;

fn home_in(tmp: &tempfile::TempDir) -> A2aHome {
    A2aHome::resolve(Some(tmp.path())).unwrap()
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

fn b64_val(b: u8) -> Option<u8> {
    match b {
        b'A'..=b'Z' => Some(b - b'A'),
        b'a'..=b'z' => Some(b - b'a' + 26),
        b'0'..=b'9' => Some(b - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn b64_decode(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let a = b64_val(chunk[0]).unwrap();
        let b = b64_val(chunk[1]).unwrap();
        let c_pad = chunk[2] == b'=';
        let d_pad = chunk[3] == b'=';
        let c = if c_pad { 0 } else { b64_val(chunk[2]).unwrap() };
        let d = if d_pad { 0 } else { b64_val(chunk[3]).unwrap() };
        out.push((a << 2) | (b >> 4));
        if !c_pad {
            out.push((b << 4) | (c >> 2));
        }
        if !d_pad {
            out.push((c << 6) | d);
        }
    }
    out
}

fn claims_for(key: &SigningKey, gateway_id: &str, now: i64) -> TokenClaims {
    TokenClaims {
        fleet_id: "filled-by-signer".to_string(),
        gateway_id: gateway_id.to_string(),
        pubkey_b64: b64_encode(&key.verifying_key().to_bytes()),
        issued_at: now - 10,
        exp: now + 1_000,
    }
}

fn identity(home: &A2aHome, key: SigningKey, gateway_id: &str, now: i64) -> HandshakeIdentity {
    let token = sign_token(&claims_for(&key, gateway_id, now), home).unwrap();
    HandshakeIdentity::new(token, key)
}

fn note() -> Note {
    Note {
        from: "codex:from".to_string(),
        to: "claude:to".to_string(),
        ts: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        kind: "note".to_string(),
        text: "hello".to_string(),
        refs: json!({}),
        extra: serde_json::Map::new(),
    }
}

#[tokio::test]
async fn frame_codec_is_tagged_forgiving_and_continues_after_unknown_kind() {
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let (mut a, mut b) = test_link(&clock).await;

    a.send_raw_text("{\"kind\":\"future_kind\",\"ignored\":true}\n")
        .await
        .unwrap();
    a.send(&Frame::Note { note: note() }).await.unwrap();

    assert_eq!(b.recv().await.unwrap(), Some(Frame::Note { note: note() }));
}

#[tokio::test]
async fn frame_enum_roundtrips_all_spec_kinds() {
    let frames = vec![
        Frame::Challenge { nonce: "n".to_string() },
        Frame::Hello { token: "t".to_string(), nonce_sig: "s".to_string() },
        Frame::Feed {
            origin: "gw-a".to_string(),
            entries: vec![FeedEntry {
                seq: 1,
                from: "codex:1".to_string(),
                ts: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                text: "post".to_string(),
                refs: json!({}),
                extra: serde_json::Map::new(),
            }],
        },
        Frame::Note { note: note() },
        Frame::Presence {
            peers: vec![Presence {
                peer: "codex:1".to_string(),
                provider: "codex".to_string(),
                worker_id: None,
                status: None,
                working_on: None,
                cwd: None,
                registered_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                last_seen: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                retired: false,
                cursors: a2a::mesh::Cursors { feed_seq: 0, inbox: None },
                extra: serde_json::Map::new(),
            }],
        },
        Frame::Digest {
            origins: vec![OriginDigest { origin: "gw-a".to_string(), max_seq: 7, count: 3 }],
        },
        Frame::Want {
            origins: vec![OriginWant { origin: "gw-a".to_string(), from_seq: 4 }],
        },
        Frame::Ack { ref_id: "r".to_string(), outcome: "ok".to_string() },
        Frame::Err { code: "NOPE".to_string(), detail: "detail".to_string() },
    ];

    for frame in frames {
        let line = frame.to_ndjson().unwrap();
        assert!(line.ends_with('\n'), "{line:?}");
        assert_eq!(Frame::from_ndjson(&line).unwrap(), Some(frame));
    }
}

#[tokio::test]
async fn oversized_frame_returns_wire_frame_too_large_and_closes_cleanly() {
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let (mut a, mut b) = test_link(&clock).await;
    let oversized = format!("{{\"kind\":\"note\",\"pad\":\"{}\"}}\n", "x".repeat(MAX_FRAME_BYTES));

    a.send_raw_text(&oversized).await.unwrap();
    let err = b.recv().await.unwrap_err();
    assert_eq!(err.code, "WIRE_FRAME_TOO_LARGE");

    let seen = tokio::time::timeout(Duration::from_secs(1), a.recv()).await.unwrap().unwrap();
    assert_eq!(seen, None);
}

#[tokio::test]
async fn mutual_handshake_accepts_tokens_and_proves_nonce_ownership_both_directions() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    fleet_init(&home).unwrap();
    let roots = RootSet::load(&home).unwrap();
    let now = 1_700_000_000;
    let clock = TestClock::new(Utc.timestamp_opt(now, 0).unwrap());
    let accept_id = identity(&home, signing_key(), "gw-accept-111111", now);
    let dial_id = identity(&home, signing_key(), "gw-dial-222222", now);
    let (mut accept, mut dial) = test_link(&clock).await;

    let accept_task = tokio::spawn(async move {
        handshake_acceptor(&mut accept, &accept_id, &roots, &clock).await
    });
    let roots = RootSet::load(&home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(now, 0).unwrap());
    let dial_claims = handshake_dialer(&mut dial, &dial_id, &roots, &clock).await.unwrap();
    let accept_claims = accept_task.await.unwrap().unwrap();

    assert_eq!(accept_claims.gateway_id, "gw-dial-222222");
    assert_eq!(dial_claims.gateway_id, "gw-accept-111111");
}

#[tokio::test]
async fn handshake_refuses_expired_stuffed_wrong_key_and_unknown_root_tokens() {
    let now = 1_700_000_000;
    let clock = TestClock::new(Utc.timestamp_opt(now, 0).unwrap());
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);
    fleet_init(&home).unwrap();

    let accept_key = signing_key();
    let accept_id = identity(&home, accept_key, "gw-accept-111111", now);

    let expired_key = signing_key();
    let mut expired_claims = claims_for(&expired_key, "gw-expired-333333", now);
    expired_claims.exp = now - 301;
    let expired = HandshakeIdentity::new(sign_token(&expired_claims, &home).unwrap(), expired_key);
    assert_refused_by_acceptor(&home, &accept_id, expired, "TOKEN_EXPIRED", now).await;

    let stuffed_key = signing_key();
    let stuffed = HandshakeIdentity::new(stuffed_token(&home, &stuffed_key, now), stuffed_key);
    assert_refused_by_acceptor(&home, &accept_id, stuffed, "TOKEN_CLAIMS_INVALID", now).await;

    let stolen_token_key = signing_key();
    let wrong_key = signing_key();
    let token = sign_token(&claims_for(&stolen_token_key, "gw-stolen-444444", now), &home).unwrap();
    let stolen = HandshakeIdentity::new(token, wrong_key);
    assert_refused_by_acceptor(&home, &accept_id, stolen, "NONCE_SIG_INVALID", now).await;

    let other = tempfile::tempdir().unwrap();
    let other_home = home_in(&other);
    fleet_init(&other_home).unwrap();
    let unknown_key = signing_key();
    let unknown = identity(&other_home, unknown_key, "gw-unknown-555555", now);
    assert_refused_by_acceptor(&home, &accept_id, unknown, "TOKEN_INVALID", now).await;

    drop(clock);
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
    let accept_task = tokio::spawn(async move {
        handshake_acceptor(&mut accept, &accept_id, &roots, &clock).await
    });

    let roots = RootSet::load(home).unwrap();
    let clock = TestClock::new(Utc.timestamp_opt(now, 0).unwrap());
    let dial_err = handshake_dialer(&mut dial, &dial_id, &roots, &clock).await.unwrap_err();
    let accept_err = accept_task.await.unwrap().unwrap_err();
    assert_eq!(accept_err.code, code);
    assert_eq!(dial_err.code, "WIRE_PEER_ERR");
}

fn stuffed_token(home: &A2aHome, key: &SigningKey, now: i64) -> String {
    let root_file = fs::read(home.root().join("mesh/fleet/root.key")).unwrap();
    let root: serde_json::Value = serde_json::from_slice(&root_file).unwrap();
    let root_key_bytes = b64_decode(root["secret_b64"].as_str().unwrap());
    let root_key = SigningKey::from_bytes(&root_key_bytes.try_into().unwrap());
    let claims = json!({
        "exp": now + 1_000,
        "fleet_id": "filled-by-signer",
        "gateway_id": "gw-stuffed-666666",
        "issued_at": now - 10,
        "pubkey_b64": b64_encode(&key.verifying_key().to_bytes()),
        "stuffed": true
    });
    let claims_bytes = serde_json::to_vec(&claims).unwrap();
    let sig = root_key.sign(&claims_bytes);
    let envelope = json!({
        "v": 1,
        "claims_b64": b64_encode(&claims_bytes),
        "sig_b64": b64_encode(&sig.to_bytes())
    });
    b64_encode(&serde_json::to_vec(&envelope).unwrap())
}

#[tokio::test]
async fn pinned_tls_uses_spki_fingerprint_not_hostname_and_rejects_wrong_pin() {
    let tls = TlsIdentity::self_signed(vec!["random-cn.invalid".to_string()]).unwrap();
    let (client_io, server_io) = tokio::io::duplex(256 * 1024);
    let url = "wss://127.0.0.1:443/";
    let server_tls = tls.clone();
    let server = tokio::spawn(async move {
        accept_pinned_io(server_io, server_tls).await
    });

    let mut client = connect_pinned_io(client_io, url, tls.spki_fpr()).await.unwrap();
    client.close().await.unwrap();
    let mut server = server.await.unwrap().unwrap();
    assert_eq!(server.recv().await.unwrap(), None);

    let tls = TlsIdentity::self_signed(vec!["another-random-cn.invalid".to_string()]).unwrap();
    let (client_io, server_io) = tokio::io::duplex(256 * 1024);
    let server = tokio::spawn(async move {
        let _ = accept_pinned_io(server_io, tls).await;
    });
    let err = connect_pinned_io(client_io, url, &"0".repeat(64)).await.unwrap_err();
    assert_eq!(err.code, "TLS_CERT_PIN_MISMATCH");
    server.await.unwrap();
}

#[tokio::test]
async fn cancelling_writer_mid_large_frame_never_delivers_a_torn_line() {
    let clock = TestClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let (a, b) = test_link(&clock).await;
    let (mut writer, reader_half) = a.split();
    drop(reader_half);
    let (_peer_writer, mut peer_reader) = b.split();
    let text = "x".repeat(MAX_FRAME_BYTES - 1024);
    let frame = Frame::Note {
        note: Note { text: text.clone(), ..note() },
    };

    let task = tokio::spawn(async move {
        writer.send(&frame).await
    });
    task.abort();

    let seen = tokio::time::timeout(Duration::from_secs(1), peer_reader.recv())
        .await
        .unwrap()
        .unwrap();
    if let Some(Frame::Note { note }) = seen {
        assert_eq!(note.text.len(), text.len());
    } else {
        assert_eq!(seen, None);
    }
}
