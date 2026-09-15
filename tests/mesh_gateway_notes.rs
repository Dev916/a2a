mod common;

use a2a::home::A2aHome;
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_init, gateway_identity, gateway_public_key_b64,
    install_gateway_token, SystemClock,
};
use a2a::mesh::gateway::link::{self, LinkDirection, LinkStateHandle};
use a2a::mesh::gateway::{limits, presence_wire};
use a2a::mesh::gateway::notes_wire::{
    self, deliver_spooled_once, receive_note, NoteAck, NoteDeliveryTarget,
};
use a2a::mesh::registry::{GatewayRow, REGISTRY_SCHEMA_VERSION};
use a2a::mesh::row_sig::RowVerdict;
use a2a::mesh::wire::Frame;
use a2a::mesh::work::{
    sign_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkEnvelope, WorkRepo,
};
use a2a::mesh::work_store::{self, WORK_OFFER_KIND};
use a2a::mesh::{notes, Note, PeerId};
use chrono::{TimeZone, Utc};
use rand::{rngs::StdRng, SeedableRng};
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn cfg_for(home: &A2aHome) -> a2a::config::Config {
    a2a::config::load(home).unwrap().0
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

fn write_presence(home: &Path, peer: &str, home_gateway: Option<&str>) {
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
        "last_seen": Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "retired": false,
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

fn signed_work_fixture(
    work_id: &str,
) -> (tempfile::TempDir, A2aHome, A2aHome, String, WorkEnvelope) {
    let tmp = tempfile::tempdir().unwrap();
    let root = home_at(&tmp.path().join("root"));
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let clock = SystemClock;
    fleet_init(&root).unwrap();
    copy_root_pub(root.root(), sender.root());
    copy_root_pub(root.root(), receiver.root());
    enroll_joiner(&root, &sender, &clock);
    let receiver_gateway = enroll_joiner(&root, &receiver, &clock);
    let env = sign_envelope(
        &sender,
        UnsignedWorkEnvelope {
            work_id: work_id.to_string(),
            session: "codex:sender-session".to_string(),
            addressing: WorkAddressing::Directed(receiver_gateway.clone()),
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
    (tmp, sender, receiver, receiver_gateway, env)
}

fn work_note(to_gateway: &str, env: &WorkEnvelope, note_id: &str) -> Note {
    Note {
        note_id: Some(note_id.to_string()),
        from: "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
        to: format!("gateway:{to_gateway}"),
        ts: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        kind: WORK_OFFER_KIND.to_string(),
        text: format!("work offer: {} ({})", env.title, env.work_id),
        refs: json!({ "work": env }),
        extra: serde_json::Map::new(),
    }
}

fn row_with_version(gateway_id: &str, version: &str) -> (GatewayRow, RowVerdict) {
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    (
        GatewayRow {
            fleet_id: "fleet-a".to_string(),
            gateway_id: gateway_id.to_string(),
            owner: gateway_id.to_string(),
            endpoint_url: None,
            pubkey_fpr: format!("{gateway_id}-pubkey"),
            cert_fpr: None,
            version: version.to_string(),
            created_at: now,
            last_heartbeat: now,
            revoked_at: None,
            schema_version: REGISTRY_SCHEMA_VERSION,
            row_msg: None,
            row_token: None,
            row_sig: None,
            signed_at: None,
            revocation: None,
        },
        RowVerdict::Unsigned { stripped: false },
    )
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

#[tokio::test]
async fn spooled_note_delivers_over_session_acks_and_dedupes_duplicate_note_id() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");
    let cfg_a = cfg_for(&sender);
    let cfg_b = cfg_for(&receiver);
    let target = "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    let note_id = "01K3KV0000000000000000000A";
    notes_wire::spool_note(
        &sender,
        &cfg_a,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            target,
            "through session",
            note_id,
        ),
    )
    .unwrap();

    let link = common::start_mesh_link(
        sender.clone(),
        cfg_a.clone(),
        "gw-a",
        receiver.clone(),
        cfg_b,
        "gw-b",
    )
    .await;
    let mut rng = StdRng::seed_from_u64(7);
    a2a::mesh::gateway::link::push_spooled_notes(
        &sender,
        &cfg_a,
        &link.a_state,
        Utc::now(),
        &mut rng,
    )
    .await
    .unwrap();
    common::wait_until(|| {
        note_files(receiver.root(), target).len() == 1
            && json_files(
                &sender
                    .root()
                    .join("mesh")
                    .join("outbound")
                    .join("sent")
                    .join("gw-b"),
            )
            .len()
                == 1
    })
    .await;

    link.a_state
        .send(
            "gw-b",
            a2a::mesh::wire::Frame::Note {
                note: note(
                    "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    target,
                    "through session",
                    note_id,
                ),
            },
        )
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(note_files(receiver.root(), target).len(), 1);
    link.stop().await;
}

#[test]
fn note_id_is_flatten_tolerant_and_local_notes_mint_one() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let legacy: Note = serde_json::from_value(json!({
        "from": "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "to": "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "ts": Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        "kind": "note",
        "text": "legacy",
        "refs": {},
        "future": true
    }))
    .unwrap();
    assert_eq!(legacy.note_id, None);
    assert_eq!(legacy.extra["future"], true);

    common::a2a(&home, &work)
        .args([
            "mesh",
            "note",
            "--session",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "-",
        ])
        .write_stdin("hello local")
        .assert()
        .success();

    let files = note_files(&home, "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    assert_eq!(files.len(), 1);
    let saved: Note = serde_json::from_slice(&fs::read(&files[0]).unwrap()).unwrap();
    let note_id = saved.note_id.as_deref().expect("local note has note_id");
    assert_eq!(note_id.len(), 26);
    ulid::Ulid::from_string(note_id).expect("note_id is a ULID");
    assert!(
        files[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains(note_id),
        "note_id is embedded in inbox filename"
    );
}

#[test]
fn remote_cli_note_spools_instead_of_touching_remote_homed_inbox() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    write_gateway_key(&home, "gw-a");
    write_presence(
        &home,
        "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        Some("gw-b"),
    );

    common::a2a(&home, &work)
        .args([
            "mesh",
            "note",
            "--session",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "-",
        ])
        .write_stdin("hello remote")
        .assert()
        .success();

    assert!(note_files(&home, "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").is_empty());
    let queued = json_files(&home.join("mesh").join("outbound").join("gw-b"));
    assert_eq!(queued.len(), 1);
    let value: Value = serde_json::from_slice(&fs::read(&queued[0]).unwrap()).unwrap();
    assert_eq!(value["from_gateway"], "gw-a");
    assert_eq!(value["to_gateway"], "gw-b");
    assert_eq!(
        value["to_peer"],
        "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    );
    assert_eq!(value["trace_id"], value["note"]["note_id"]);
}

#[test]
fn redelivery_after_swallowed_ack_writes_exactly_one_inbox_file() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let cfg = cfg_for(&sender);
    let mut rng = StdRng::seed_from_u64(9);
    let target = "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");

    notes_wire::spool_note(
        &sender,
        &cfg,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            target,
            "hello once",
            "01K3KV00000000000000000001",
        ),
    )
    .unwrap();
    let queued = json_files(&sender.root().join("mesh").join("outbound").join("gw-b"));
    let envelope = notes_wire::read_spooled(&queued[0]).unwrap();

    assert_eq!(
        receive_note(&receiver, &cfg, &envelope).unwrap(),
        NoteAck::Delivered
    );
    assert_eq!(note_files(receiver.root(), target).len(), 1);

    let report = deliver_spooled_once(
        &cfg,
        &sender,
        NoteDeliveryTarget::Gateway {
            to_gateway: "gw-b",
            home: Some(&receiver),
        },
        Utc.timestamp_opt(1_700_000_010, 0).unwrap(),
        &mut rng,
    )
    .unwrap();
    assert_eq!(report.delivered, 1);
    assert_eq!(report.duplicates, 1);
    assert_eq!(note_files(receiver.root(), target).len(), 1);
    assert!(
        json_files(
            &sender
                .root()
                .join("mesh")
                .join("outbound")
                .join("sent")
                .join("gw-b")
        )
        .len()
            == 1
    );
}

#[test]
fn ingress_refusal_archives_immediately_and_logs_same_trace_id_on_both_gateways() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let cfg = cfg_for(&sender);
    let mut rng = StdRng::seed_from_u64(11);
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");
    let trace_id = "01K3KV00000000000000000002";

    notes_wire::spool_note(
        &sender,
        &cfg,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "OPENAI_API_KEY=sk-abc123456789012345678901",
            trace_id,
        ),
    )
    .unwrap();
    let report = deliver_spooled_once(
        &cfg,
        &sender,
        NoteDeliveryTarget::Gateway {
            to_gateway: "gw-b",
            home: Some(&receiver),
        },
        Utc.timestamp_opt(1_700_000_020, 0).unwrap(),
        &mut rng,
    )
    .unwrap();
    assert_eq!(report.rejected, 1);
    assert!(
        json_files(
            &sender
                .root()
                .join("mesh")
                .join("outbound")
                .join("rejected")
                .join("gw-b")
        )
        .len()
            == 1
    );

    let sender_log = fs::read_to_string(
        sender
            .root()
            .join("mesh")
            .join("gateway")
            .join("gateway.jsonl"),
    )
    .unwrap();
    let receiver_log = fs::read_to_string(
        receiver
            .root()
            .join("mesh")
            .join("gateway")
            .join("gateway.jsonl"),
    )
    .unwrap();
    for log in [&sender_log, &receiver_log] {
        assert!(log.contains(trace_id), "{log}");
        assert!(log.contains(r#""from_gateway":"gw-a""#), "{log}");
        assert!(log.contains(r#""to_gateway":"gw-b""#), "{log}");
        assert!(
            log.contains(r#""to_peer":"codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb""#),
            "{log}"
        );
    }
    assert!(
        sender_log.contains(r#""action":"rejected""#),
        "{sender_log}"
    );
    assert!(
        receiver_log.contains(r#""action":"ingress_refused""#),
        "{receiver_log}"
    );
}

#[test]
fn offline_target_holds_and_restart_pass_resumes_with_capped_full_jitter_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let cfg = cfg_for(&sender);
    let mut rng = StdRng::seed_from_u64(13);
    write_gateway_key(sender.root(), "gw-a");
    write_gateway_key(receiver.root(), "gw-b");

    notes_wire::spool_note(
        &sender,
        &cfg,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "wait for restart",
            "01K3KV00000000000000000003",
        ),
    )
    .unwrap();

    let offline = deliver_spooled_once(
        &cfg,
        &sender,
        NoteDeliveryTarget::Gateway {
            to_gateway: "gw-b",
            home: None,
        },
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        &mut rng,
    )
    .unwrap();
    assert_eq!(offline.retrying, 1);
    let held_path =
        json_files(&sender.root().join("mesh").join("outbound").join("gw-b"))[0].clone();
    let held = notes_wire::read_spooled(&held_path).unwrap();
    assert_eq!(held.attempts, 1);
    let retry_at = held
        .next_attempt_after
        .expect("offline pass schedules retry");
    assert!(retry_at <= Utc.timestamp_opt(1_700_000_010, 0).unwrap());

    let resumed = deliver_spooled_once(
        &cfg,
        &sender,
        NoteDeliveryTarget::Gateway {
            to_gateway: "gw-b",
            home: Some(&receiver),
        },
        retry_at,
        &mut rng,
    )
    .unwrap();
    assert_eq!(resumed.delivered, 1);
    assert_eq!(
        note_files(
            receiver.root(),
            "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        )
        .len(),
        1
    );
}

#[tokio::test]
async fn push_spooled_notes_marks_backoff_before_sending_and_skips_until_due() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let cfg = cfg_for(&sender);
    let state = LinkStateHandle::new(&sender, Duration::from_secs(60)).unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    state
        .register("gw-b".into(), LinkDirection::Outbound, tx)
        .await
        .unwrap();
    let path = notes_wire::spool_note(
        &sender,
        &cfg,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "back off between watcher polls",
            "01K3KV00000000000000000007",
        ),
    )
    .unwrap();
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let mut rng = StdRng::seed_from_u64(17);

    assert_eq!(
        link::push_spooled_notes(&sender, &cfg, &state, now, &mut rng)
            .await
            .unwrap(),
        1
    );
    let held = notes_wire::read_spooled(&path).unwrap();
    assert_eq!(held.attempts, 1);
    let due = held
        .next_attempt_after
        .expect("first push schedules the next attempt");
    assert!(due > now);
    assert!(due <= now + chrono::Duration::seconds(1_920));

    let before_due = due - chrono::Duration::nanoseconds(1);
    assert_eq!(
        link::push_spooled_notes(&sender, &cfg, &state, before_due, &mut rng)
            .await
            .unwrap(),
        0
    );
    assert!(matches!(rx.try_recv(), Ok(Frame::Note { .. })));
    assert!(rx.try_recv().is_err(), "only the first poll reached the link");

    assert_eq!(
        link::push_spooled_notes(&sender, &cfg, &state, due, &mut rng)
            .await
            .unwrap(),
        1
    );
    assert!(matches!(rx.try_recv(), Ok(Frame::Note { .. })));
    assert!(rx.try_recv().is_err(), "the due poll sent exactly once");
    let retried = notes_wire::read_spooled(&path).unwrap();
    assert_eq!(retried.attempts, 2);
    assert!(retried.next_attempt_after.unwrap() <= due + chrono::Duration::seconds(1_920));
}

#[tokio::test]
async fn a_deferred_frame_defers_the_whole_peer_spool_and_archives_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let cfg = cfg_for(&sender);
    let state = LinkStateHandle::new(&sender, Duration::from_secs(60)).unwrap();
    let (tx, _rx) = mpsc::channel(16);
    state
        .register("gw-b".into(), LinkDirection::Outbound, tx)
        .await
        .unwrap();
    for (text, note_id) in [
        ("first deferred note", "01K3KV00000000000000000008"),
        ("second deferred note", "01K3KV00000000000000000009"),
    ] {
        notes_wire::spool_note(
            &sender,
            &cfg,
            "gw-a",
            "gw-b",
            note(
                "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                text,
                note_id,
            ),
        )
        .unwrap();
    }
    let outbound_root = sender.root().join("mesh").join("outbound");
    let outbound_gw_b = outbound_root.join("gw-b");
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let mut rng = StdRng::seed_from_u64(19);

    assert_eq!(
        link::push_spooled_notes(&sender, &cfg, &state, now, &mut rng)
            .await
            .unwrap(),
        2
    );
    let deferred =
        notes_wire::defer_spool_for_peer(&sender, &cfg, "gw-b", Some(45), now, &mut rng)
            .unwrap();
    assert_eq!(deferred, 2);
    for path in json_files(&outbound_gw_b) {
        let held = notes_wire::read_spooled(&path).unwrap();
        let due = held.next_attempt_after.unwrap();
        assert!(due >= now + chrono::Duration::seconds(45));
        assert!(due <= now + chrono::Duration::seconds(1_920));
    }
    assert!(json_files(&outbound_root.join("sent").join("gw-b")).is_empty());
    assert!(json_files(&outbound_root.join("rejected").join("gw-b")).is_empty());

    let capped_at = now + chrono::Duration::seconds(1);
    assert_eq!(
        notes_wire::defer_spool_for_peer(
            &sender,
            &cfg,
            "gw-b",
            Some(10_000),
            capped_at,
            &mut rng,
        )
        .unwrap(),
        2
    );
    for path in json_files(&outbound_gw_b) {
        let held = notes_wire::read_spooled(&path).unwrap();
        assert!(
            held.next_attempt_after.unwrap()
                <= capped_at + chrono::Duration::seconds(1_920)
        );
    }
}

#[tokio::test]
async fn deferred_frame_reschedules_the_peer_spool_without_ending_the_link() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let cfg_a = cfg_for(&sender);
    let cfg_b = cfg_for(&receiver);
    let path = notes_wire::spool_note(
        &sender,
        &cfg_a,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "defer this peer spool",
            "01K3KV0000000000000000000C",
        ),
    )
    .unwrap();
    let link = common::start_mesh_link(
        sender.clone(),
        cfg_a,
        "gw-a",
        receiver,
        cfg_b,
        "gw-b",
    )
    .await;
    let before_send = Utc::now();

    assert!(
        link.b_state
            .send(
                "gw-a",
                Frame::Deferred {
                    code: "RATE_LIMITED".to_string(),
                    retry_after_secs: Some(45),
                    origin: None,
                    applied_through_seq: None,
                },
            )
            .await
    );
    common::wait_until(|| {
        notes_wire::read_spooled(&path)
            .ok()
            .is_some_and(|held| held.next_attempt_after.is_some())
    })
    .await;

    let held = notes_wire::read_spooled(&path).unwrap();
    assert_eq!(held.attempts, 1);
    let due = held.next_attempt_after.unwrap();
    assert!(due >= before_send + chrono::Duration::seconds(45));
    assert!(due <= Utc::now() + chrono::Duration::seconds(1_920));
    assert!(link.a_state.is_live("gw-b").await);
    let log = fs::read_to_string(
        sender
            .root()
            .join("mesh")
            .join("gateway")
            .join("gateway.jsonl"),
    )
    .unwrap();
    assert!(log.contains(r#""action":"deferral_received""#), "{log}");
    assert!(log.contains(r#""code":"RATE_LIMITED""#), "{log}");

    link.stop().await;
}

#[test]
fn handle_ack_on_rate_limited_reschedules_and_never_archives() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let cfg = cfg_for(&sender);
    let trace_id = "01K3KV0000000000000000000B";
    notes_wire::spool_note(
        &sender,
        &cfg,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "rate limited",
            trace_id,
        ),
    )
    .unwrap();
    let outbound_root = sender.root().join("mesh").join("outbound");
    let outbound_gw_b = outbound_root.join("gw-b");

    assert!(!notes_wire::handle_ack(&sender, "gw-b", trace_id, "RATE_LIMITED").unwrap());
    assert_eq!(json_files(&outbound_gw_b).len(), 1);
    assert!(json_files(&outbound_root.join("rejected").join("gw-b")).is_empty());
}

#[test]
fn deferred_is_never_emitted_to_a_peer_that_predates_it() {
    assert!(!limits::peer_supports_deferred(None));
    assert!(!limits::peer_supports_deferred(Some("0.3.0")));
    assert!(limits::peer_supports_deferred(Some("0.4.0")));
    assert!(limits::peer_supports_deferred(Some("0.4.1-dirty")));
    assert_eq!(limits::MIN_DEFERRED_PEER_VERSION, "0.4.0");
    assert_eq!(
        limits::deferred_frame("RATE_LIMITED", Some(30)),
        Frame::Deferred {
            code: "RATE_LIMITED".to_string(),
            retry_after_secs: Some(30),
            origin: None,
            applied_through_seq: None,
        }
    );

    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(tmp.path());
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    presence_wire::refresh_registry_cache(
        &home,
        &[row_with_version("gw-b", "0.4.0")],
        now,
    )
    .unwrap();
    assert_eq!(
        presence_wire::cached_peer_version(&home, "gw-b").as_deref(),
        Some("0.4.0")
    );
}

#[test]
fn token_expired_ack_archives_immediately_without_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let sender = home_at(&tmp.path().join("sender"));
    let cfg = cfg_for(&sender);
    write_gateway_key(sender.root(), "gw-a");
    let trace_id = "01K3KV00000000000000000004";

    notes_wire::spool_note(
        &sender,
        &cfg,
        "gw-a",
        "gw-b",
        note(
            "claude:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "token expired",
            trace_id,
        ),
    )
    .unwrap();

    assert!(notes_wire::handle_ack(&sender, "gw-b", trace_id, "TOKEN_EXPIRED").unwrap());
    assert!(json_files(&sender.root().join("mesh").join("outbound").join("gw-b")).is_empty());
    assert_eq!(
        json_files(
            &sender
                .root()
                .join("mesh")
                .join("outbound")
                .join("rejected")
                .join("gw-b")
        )
        .len(),
        1
    );
}

#[test]
fn gateway_addressed_work_offer_is_verified_stored_and_not_session_injected() {
    let (tmp, _sender, receiver, receiver_gateway, env) =
        signed_work_fixture("01HZ7P7Q0V0V7G01J4J8R8M2Q2");
    let cfg = cfg_for(&receiver);
    let peer = "codex:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    let note = work_note(&receiver_gateway, &env, "01K3KV00000000000000000005");
    let spooled = notes_wire::SpooledNote {
        schema_version: 1,
        trace_id: "01K3KV00000000000000000005".to_string(),
        from_gateway: env.sender.gateway_id.clone(),
        to_gateway: receiver_gateway.clone(),
        to_peer: note.to.clone(),
        attempts: 0,
        next_attempt_after: None,
        note,
    };

    assert_eq!(
        receive_note(&receiver, &cfg, &spooled).unwrap(),
        NoteAck::Delivered
    );
    let stored = receiver
        .root()
        .join("work")
        .join("inbox")
        .join(format!("{}.json", env.work_id));
    assert!(stored.exists(), "directed work is stored");
    assert_eq!(
        fs::metadata(&stored).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(note_files(receiver.root(), peer).is_empty());
    assert!(notes::inject(
        &receiver,
        &cfg,
        &PeerId::parse(peer).unwrap(),
        Utc.timestamp_opt(1_700_000_010, 0).unwrap()
    )
    .unwrap()
    .is_none());

    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    common::a2a(receiver.root(), &work)
        .args([
            "mesh",
            "inbox",
            "--provider",
            "codex",
            "--session",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        ])
        .assert()
        .success()
        .stdout("");
    common::a2a(receiver.root(), &work)
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
        .stdout("");
}

#[test]
fn bad_work_offer_is_rejected_logged_and_not_stored() {
    let (_tmp, _sender, receiver, receiver_gateway, mut env) =
        signed_work_fixture("01HZ7P7Q0V0V7G01J4J8R8M2Q3");
    env.title.push_str(" tampered");
    let cfg = cfg_for(&receiver);
    let note = work_note(&receiver_gateway, &env, "01K3KV00000000000000000006");
    let spooled = notes_wire::SpooledNote {
        schema_version: 1,
        trace_id: "01K3KV00000000000000000006".to_string(),
        from_gateway: env.sender.gateway_id.clone(),
        to_gateway: receiver_gateway,
        to_peer: note.to.clone(),
        attempts: 0,
        next_attempt_after: None,
        note,
    };

    assert_eq!(
        receive_note(&receiver, &cfg, &spooled).unwrap(),
        NoteAck::Rejected {
            code: "WORK_SIGNATURE_INVALID".to_string()
        }
    );
    assert!(work_store::list(&receiver).unwrap().is_empty());
    let log =
        fs::read_to_string(receiver.root().join("mesh").join("gateway").join("gateway.jsonl"))
            .unwrap();
    assert!(log.contains(r#""action":"work_ingest_refused""#), "{log}");
    assert!(log.contains(r#""code":"WORK_SIGNATURE_INVALID""#), "{log}");
}
