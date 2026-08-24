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
    assert_eq!(String::from_utf8(out.get_output().stdout.clone()).unwrap().trim(), "1");

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
    assert_eq!(fs::metadata(&presence).unwrap().permissions().mode() & 0o777, 0o600);

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
        live.lines().filter(|line| line.contains("events_rotated")).count(),
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
