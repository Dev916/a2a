use a2a::events::Transport;
use a2a::fsm::WorkerState;
use a2a::state::WorkerSnapshot;
use assert_cmd::Command;
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn cli(home: &Path) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home)
        .env_remove("CLAUDE_SESSION_ID")
        .env_remove("BEADS_ACTOR")
        .env_remove("A2A_JSON")
        .env_remove("A2A_BROKER_BIN")
        .env_remove("A2A_SHIM_BIN")
        .env_remove("A2A_BROKER_DISABLE");
    c
}

fn feed_json(home: &Path, since: Option<u64>) -> Vec<Value> {
    let mut c = cli(home);
    c.args(["mesh", "feed", "--json"]);
    if let Some(since) = since {
        c.args(["--since", &since.to_string()]);
    }
    let out = c.assert().success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    if v.is_array() {
        serde_json::from_value(v).unwrap()
    } else {
        serde_json::from_value(v["data"].clone()).unwrap()
    }
}

fn notes_json(home: &Path, session: &str) -> Vec<Value> {
    let out = cli(home)
        .args(["mesh", "inbox", "--session", session, "--json"])
        .assert()
        .success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    if v.is_array() {
        serde_json::from_value(v).unwrap()
    } else {
        serde_json::from_value(v["data"].clone()).unwrap()
    }
}

fn data_array(out: &[u8]) -> Vec<Value> {
    let v: Value = serde_json::from_slice(out).unwrap();
    if v.is_array() {
        serde_json::from_value(v).unwrap()
    } else {
        serde_json::from_value(v["data"].clone()).unwrap()
    }
}

fn note_files(home: &Path, provider: &str, session: &str) -> Vec<std::path::PathBuf> {
    let inbox = home
        .join("mesh")
        .join("peers")
        .join(format!("{provider}-{session}"))
        .join("inbox");
    let mut out: Vec<_> = fs::read_dir(inbox)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    out.sort();
    out
}

fn seed_worker(home: &Path, id: &str, session: Option<&str>, state: WorkerState) {
    let a2a_home = a2a::home::A2aHome::resolve(Some(home)).unwrap();
    let dir = a2a_home.worker_dir(id);
    fs::create_dir_all(&dir).unwrap();
    let mut snap = WorkerSnapshot::new_for_test(id, home);
    snap.state = state;
    snap.transport = Transport::Exec;
    snap.requested_transport = Transport::Exec;
    snap.session_id = session.map(str::to_string);
    a2a::state::save_snapshot(&dir, &snap).unwrap();
}

#[test]
fn post_auto_registers_scans_before_cap_and_records_refs() {
    let tmp = tempfile::tempdir().unwrap();
    let session = "11111111-1111-1111-1111-111111111111";
    let secret = format!("ghp_{}", "a".repeat(a2a::mesh::POST_MAX_BYTES + 1));

    let out = cli(tmp.path())
        .args([
            "mesh",
            "post",
            "--session",
            session,
            "--ref-beads",
            "a2a-c9g",
            "--ref-worker",
            "impl-0824t2039-6e7p",
            "posted from argv",
        ])
        .assert()
        .success();
    assert_eq!(
        String::from_utf8(out.get_output().stdout.clone())
            .unwrap()
            .trim(),
        "1"
    );

    let entries = feed_json(tmp.path(), None);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["seq"], 1);
    assert_eq!(entries[0]["from"], format!("claude:{session}"));
    assert_eq!(entries[0]["text"], "posted from argv");
    assert_eq!(entries[0]["refs"]["beads"], json!(["a2a-c9g"]));
    assert_eq!(entries[0]["refs"]["worker"], json!(["impl-0824t2039-6e7p"]));

    let presence = tmp
        .path()
        .join("mesh")
        .join("peers")
        .join(format!("claude-{session}"))
        .join("presence.json");
    assert!(presence.exists(), "posting auto-registers the caller");
    assert_eq!(
        fs::metadata(&presence).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let out = cli(tmp.path())
        .args(["--json", "mesh", "post", "--session", session, &secret])
        .assert()
        .code(9);
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "SECRET_IN_POST");
    assert!(
        !v["error"]["message"].as_str().unwrap().contains("ghp_"),
        "secret value leaked: {v}"
    );

    let out = cli(tmp.path())
        .args([
            "--json",
            "mesh",
            "post",
            "--session",
            session,
            &"x".repeat(a2a::mesh::POST_MAX_BYTES + 1),
        ])
        .assert()
        .code(9);
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "POST_TOO_LARGE");
}

#[test]
fn post_reads_stdin_or_message_file_and_feed_since_is_strictly_after() {
    let tmp = tempfile::tempdir().unwrap();
    let session = "22222222-2222-2222-2222-222222222222";
    let message_file = tmp.path().join("message.txt");
    fs::write(&message_file, "from file\nsecond line").unwrap();

    cli(tmp.path())
        .args(["mesh", "post", "--session", session, "-"])
        .write_stdin("from stdin")
        .assert()
        .success();
    cli(tmp.path())
        .args(["mesh", "post", "--session", session, "--message-file"])
        .arg(&message_file)
        .assert()
        .success();

    let entries = feed_json(tmp.path(), Some(1));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["seq"], 2);
    assert_eq!(entries[0]["text"], "from file\nsecond line");

    let out = cli(tmp.path())
        .args(["mesh", "feed", "--since", "0"])
        .assert()
        .success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    assert!(stdout.contains("1 "));
    assert!(stdout.contains("claude:22222222"));
    assert!(stdout.contains("from stdin"));
    assert!(stdout.contains("from file"));
    assert!(!stdout.contains("second line"));
}

#[test]
fn feed_rotation_keeps_post_seqs_gapless_and_since_returns_the_new_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let session = "33333333-3333-3333-3333-333333333333";

    for i in 0..12 {
        cli(tmp.path())
            .env("A2A_MESH_FEED_MAX_BYTES", "2048")
            .args([
                "mesh",
                "post",
                "--session",
                session,
                &format!("post {i} {}", "x".repeat(110)),
            ])
            .assert()
            .success();
    }

    let root = tmp.path().join("mesh");
    assert!(root.join("feed.jsonl.1").exists(), "feed rotated");
    let live = fs::read_to_string(root.join("feed.jsonl")).unwrap();
    assert_eq!(
        live.lines()
            .filter(|line| line.contains("events_rotated"))
            .count(),
        1,
        "the fresh generation has exactly one rotation marker"
    );

    let all = feed_json(tmp.path(), None);
    let seqs: Vec<u64> = all.iter().map(|v| v["seq"].as_u64().unwrap()).collect();
    assert_eq!(seqs, (1..=12).collect::<Vec<_>>());

    let split = fs::read_to_string(root.join("feed.jsonl.1"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|v| v["seq"].as_u64())
        .max()
        .unwrap();
    let post_split = feed_json(tmp.path(), Some(split));
    assert_eq!(
        post_split
            .iter()
            .map(|v| v["seq"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        ((split + 1)..=12).collect::<Vec<_>>()
    );
}

#[test]
fn daemonless_two_peers_post_and_feed_redacts_human_output() {
    let tmp = tempfile::tempdir().unwrap();
    let alice = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    let bob = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    cli(tmp.path())
        .args(["mesh", "post", "--session", alice, "alice says hello"])
        .assert()
        .success();
    cli(tmp.path())
        .args([
            "mesh",
            "post",
            "--provider",
            "codex",
            "--session",
            bob,
            "bob says hello",
        ])
        .assert()
        .success();

    let entries = feed_json(tmp.path(), None);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["from"], format!("claude:{alice}"));
    assert_eq!(entries[1]["from"], format!("codex:{bob}"));
}

#[test]
fn bare_terminal_posts_use_one_persisted_user_identity_with_precedence() {
    let tmp = tempfile::tempdir().unwrap();

    cli(tmp.path())
        .args(["mesh", "post", "first bare post"])
        .assert()
        .success();
    cli(tmp.path())
        .args(["mesh", "post", "second bare post"])
        .assert()
        .success();

    let entries = feed_json(tmp.path(), None);
    assert_eq!(entries.len(), 2);
    let from = entries[0]["from"].as_str().unwrap();
    assert!(
        from.starts_with("user:"),
        "bare terminal identity should use user provider: {from}"
    );
    assert_eq!(entries[1]["from"], from);

    let identity_file = tmp.path().join("mesh").join("user-identity.json");
    let identity: Value =
        serde_json::from_str(&fs::read_to_string(&identity_file).unwrap()).unwrap();
    assert_eq!(
        identity["session_uuid"],
        from.strip_prefix("user:").unwrap()
    );
    assert!(identity["created_at"].as_str().is_some());
    assert_eq!(
        fs::metadata(&identity_file).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let target = "66666666-6666-6666-6666-666666666666";
    cli(tmp.path())
        .args(["mesh", "register", "--session", target])
        .assert()
        .success();
    cli(tmp.path())
        .args(["mesh", "note", &format!("claude:{target}"), "-"])
        .write_stdin("bare note")
        .assert()
        .success();
    let files = note_files(tmp.path(), "claude", target);
    let note: Value = serde_json::from_str(&fs::read_to_string(&files[0]).unwrap()).unwrap();
    assert_eq!(note["from"], from);

    let explicit = "44444444-4444-4444-4444-444444444444";
    cli(tmp.path())
        .args(["mesh", "post", "--session", explicit, "explicit post"])
        .assert()
        .success();
    let claude_env = "55555555-5555-5555-5555-555555555555";
    cli(tmp.path())
        .env("CLAUDE_SESSION_ID", claude_env)
        .args(["mesh", "post", "env post"])
        .assert()
        .success();

    let entries = feed_json(tmp.path(), None);
    assert_eq!(entries[2]["from"], format!("claude:{explicit}"));
    assert_eq!(entries[3]["from"], format!("claude:{claude_env}"));
}

#[test]
fn note_to_peer_lands_private_fifo_and_inbox_consume_archives_it() {
    let tmp = tempfile::tempdir().unwrap();
    let alice = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    let bob = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    cli(tmp.path())
        .args(["mesh", "register", "--session", bob])
        .assert()
        .success();
    cli(tmp.path())
        .args([
            "mesh",
            "note",
            "--session",
            alice,
            &format!("claude:{bob}"),
            "-",
        ])
        .write_stdin("hello bob\nsecond line")
        .assert()
        .success();

    let files = note_files(tmp.path(), "claude", bob);
    assert_eq!(files.len(), 1);
    assert!(files[0]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("000001-"));
    assert_eq!(
        fs::metadata(&files[0]).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let note: Value = serde_json::from_str(&fs::read_to_string(&files[0]).unwrap()).unwrap();
    assert_eq!(note["from"], format!("claude:{alice}"));
    assert_eq!(note["to"], format!("claude:{bob}"));
    assert_eq!(note["kind"], "note");
    assert_eq!(note["text"], "hello bob\nsecond line");

    let notes = notes_json(tmp.path(), bob);
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0]["text"], "hello bob\nsecond line");

    cli(tmp.path())
        .args(["mesh", "inbox", "--session", bob, "--consume"])
        .assert()
        .success();
    assert!(note_files(tmp.path(), "claude", bob).is_empty());
    assert!(tmp
        .path()
        .join("mesh")
        .join("peers")
        .join(format!("claude-{bob}"))
        .join("inbox")
        .join("read")
        .read_dir()
        .unwrap()
        .next()
        .is_some());
    let presence: Value = serde_json::from_str(
        &fs::read_to_string(
            tmp.path()
                .join("mesh")
                .join("peers")
                .join(format!("claude-{bob}"))
                .join("presence.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(presence["cursors"]["inbox"]
        .as_str()
        .unwrap()
        .starts_with("000001-"));
}

#[test]
fn note_target_prefix_ambiguity_is_exit_three_with_candidates() {
    let tmp = tempfile::tempdir().unwrap();
    for session in [
        "12345678-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "12345678-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    ] {
        cli(tmp.path())
            .args(["mesh", "register", "--session", session])
            .assert()
            .success();
    }

    let out = cli(tmp.path())
        .args([
            "--json",
            "mesh",
            "note",
            "--session",
            "99999999-9999-9999-9999-999999999999",
            "12345678",
            "-",
        ])
        .write_stdin("ambiguous")
        .assert()
        .code(3);
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "AMBIGUOUS_PEER");
    let message = v["error"]["message"].as_str().unwrap();
    assert!(message.contains("claude:12345678-aaaa"), "{message}");
    assert!(message.contains("claude:12345678-bbbb"), "{message}");
}

#[test]
fn note_gates_scan_before_forgiving_cap_and_name_the_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let alice = "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa";
    let bob = "bbbbbbbb-1111-1111-1111-bbbbbbbbbbbb";
    cli(tmp.path())
        .args(["mesh", "register", "--session", bob])
        .assert()
        .success();

    let ok = tmp.path().join("ok.txt");
    fs::write(&ok, "x".repeat(a2a::mesh::NOTE_MAX_BYTES - 1)).unwrap();
    cli(tmp.path())
        .args([
            "mesh",
            "note",
            "--session",
            alice,
            &format!("claude:{bob}"),
            "--message-file",
        ])
        .arg(&ok)
        .assert()
        .success();

    let too_big_secret = tmp.path().join("secret.txt");
    fs::write(
        &too_big_secret,
        format!(
            "ghp_{}\n{}",
            "a".repeat(40),
            "x".repeat(a2a::mesh::NOTE_MAX_BYTES + 1)
        ),
    )
    .unwrap();
    let out = cli(tmp.path())
        .args([
            "--json",
            "mesh",
            "note",
            "--session",
            alice,
            &format!("claude:{bob}"),
            "--message-file",
        ])
        .arg(&too_big_secret)
        .assert()
        .code(9);
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "SECRET_IN_NOTE");

    let too_big = tmp.path().join("too-big.txt");
    fs::write(&too_big, "x".repeat(a2a::mesh::NOTE_MAX_BYTES + 1)).unwrap();
    let out = cli(tmp.path())
        .args([
            "--json",
            "mesh",
            "note",
            "--session",
            alice,
            &format!("claude:{bob}"),
            "--message-file",
        ])
        .arg(&too_big)
        .assert()
        .code(9);
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "NOTE_TOO_LARGE");
    assert!(v["error"]["message"]
        .as_str()
        .unwrap()
        .contains(&a2a::mesh::NOTE_MAX_BYTES.to_string()));
}

#[test]
fn note_to_worker_id_queues_worker_inbox_and_records_mesh_copy() {
    let tmp = tempfile::tempdir().unwrap();
    let worker_id = "general-0824t2052-abcd";
    let session = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    seed_worker(tmp.path(), worker_id, Some(session), WorkerState::Running);

    cli(tmp.path())
        .args([
            "mesh",
            "note",
            "--session",
            "aaaaaaaa-2222-2222-2222-aaaaaaaaaaaa",
            worker_id,
            "-",
        ])
        .write_stdin("worker note")
        .assert()
        .success();

    assert_eq!(note_files(tmp.path(), "codex", session).len(), 1);
    let worker_inbox = tmp.path().join("workers").join(worker_id).join("inbox");
    let queued: Vec<_> = fs::read_dir(worker_inbox)
        .unwrap()
        .flatten()
        .filter(|e| e.file_type().unwrap().is_file())
        .collect();
    assert_eq!(queued.len(), 1);
    let queued_text = fs::read_to_string(queued[0].path()).unwrap();
    assert!(queued_text.contains("worker note"), "{queued_text}");
}

#[test]
fn worker_without_session_refuses_before_a_mesh_copy_is_written() {
    let tmp = tempfile::tempdir().unwrap();
    let worker_id = "general-0824t2052-bbbb";
    seed_worker(tmp.path(), worker_id, None, WorkerState::Running);

    let out = cli(tmp.path())
        .args([
            "--json",
            "mesh",
            "note",
            "--session",
            "aaaaaaaa-3333-3333-3333-aaaaaaaaaaaa",
            worker_id,
            "-",
        ])
        .write_stdin("worker note")
        .assert()
        .code(9);
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "MESH_WORKER_NO_SESSION");
    assert!(!tmp.path().join("mesh").join("peers").exists());
}

#[test]
fn note_to_retired_unknown_or_terminal_peer_warns_but_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    let alice = "aaaaaaaa-4444-4444-4444-aaaaaaaaaaaa";
    let retired = "dddddddd-dddd-dddd-dddd-dddddddddddd";
    cli(tmp.path())
        .args(["mesh", "register", "--session", retired])
        .assert()
        .success();
    cli(tmp.path())
        .args(["mesh", "retire", "--session", retired])
        .assert()
        .success();

    let out = cli(tmp.path())
        .args([
            "mesh",
            "note",
            "--session",
            alice,
            &format!("claude:{retired}"),
            "-",
        ])
        .write_stdin("retired note")
        .assert()
        .success();
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(stderr.contains("retired"), "{stderr}");

    let unknown = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
    let out = cli(tmp.path())
        .args([
            "mesh",
            "note",
            "--session",
            alice,
            &format!("claude:{unknown}"),
            "-",
        ])
        .write_stdin("unknown note")
        .assert()
        .success();
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(stderr.contains("unknown"), "{stderr}");

    let worker_id = "general-0824t2052-cccc";
    let session = "ffffffff-ffff-ffff-ffff-ffffffffffff";
    seed_worker(tmp.path(), worker_id, Some(session), WorkerState::Done);
    let out = cli(tmp.path())
        .args(["mesh", "note", "--session", alice, worker_id, "-"])
        .write_stdin("terminal note")
        .assert()
        .success();
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(stderr.contains("terminal"), "{stderr}");
    assert_eq!(note_files(tmp.path(), "codex", session).len(), 1);
}

#[test]
fn roster_unions_presence_with_live_workers_and_unread_counts() {
    let tmp = tempfile::tempdir().unwrap();
    let claude = "aaaaaaaa-5555-5555-5555-aaaaaaaaaaaa";
    cli(tmp.path())
        .args(["mesh", "register", "--session", claude])
        .assert()
        .success();
    cli(tmp.path())
        .args(["mesh", "status", "--session", claude, "reviewing"])
        .assert()
        .success();
    cli(tmp.path())
        .args([
            "mesh",
            "note",
            "--session",
            claude,
            &format!("claude:{claude}"),
            "-",
        ])
        .write_stdin("self note")
        .assert()
        .success();

    let worker_id = "general-0824t2052-dddd";
    let codex_session = "99999999-5555-5555-5555-999999999999";
    seed_worker(
        tmp.path(),
        worker_id,
        Some(codex_session),
        WorkerState::Running,
    );

    let pending_id = "impl-0824t2052-eeee";
    seed_worker(tmp.path(), pending_id, None, WorkerState::Queued);

    let out = cli(tmp.path())
        .args(["mesh", "roster", "--json"])
        .assert()
        .success();
    let rows = data_array(&out.get_output().stdout);
    let claude_row = rows
        .iter()
        .find(|r| r["peer"] == format!("claude:{claude}"))
        .unwrap();
    assert_eq!(claude_row["provider"], "claude");
    assert_eq!(claude_row["status"], "reviewing");
    assert_eq!(claude_row["label"], "active");
    assert_eq!(claude_row["unread_notes"], 1);

    let codex_row = rows
        .iter()
        .find(|r| r["peer"] == format!("codex:{codex_session}"))
        .unwrap();
    assert_eq!(codex_row["provider"], "codex");
    assert_eq!(codex_row["worker_id"], worker_id);
    assert!(
        codex_row["working_on"]
            .as_str()
            .unwrap()
            .contains("general:"),
        "{codex_row}"
    );
    assert_eq!(codex_row["label"], "active");

    let pending = rows
        .iter()
        .find(|r| r["peer"] == format!("codex:pending-{pending_id}"))
        .unwrap();
    assert_eq!(pending["worker_id"], pending_id);
}
