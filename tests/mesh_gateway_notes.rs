mod common;

use a2a::home::A2aHome;
use a2a::mesh::gateway::notes_wire::{
    self, deliver_spooled_once, receive_note, NoteAck, NoteDeliveryTarget,
};
use a2a::mesh::{Note, PeerId};
use chrono::{TimeZone, Utc};
use rand::{rngs::StdRng, SeedableRng};
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

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
        cfg_a,
        "gw-a",
        receiver.clone(),
        cfg_b,
        "gw-b",
    )
    .await;
    a2a::mesh::gateway::link::push_spooled_notes(&sender, &link.a_state)
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
