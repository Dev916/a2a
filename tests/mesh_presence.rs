use assert_cmd::Command;
use chrono::{Duration, TimeZone, Utc};
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn cli(home: &Path) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home)
        .env_remove("CLAUDE_SESSION_ID")
        .env_remove("BEADS_ACTOR")
        .env_remove("A2A_JSON");
    c
}

fn read_presence(home: &Path, provider: &str, session: &str) -> Value {
    let path = home
        .join("mesh")
        .join("peers")
        .join(format!("{provider}-{session}"))
        .join("presence.json");
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn append_feed(home: &Path, seq: u64) {
    let root = home.join("mesh");
    fs::create_dir_all(&root).unwrap();
    let line = json!({
        "seq": seq,
        "from": "claude:feed-source",
        "ts": Utc::now(),
        "text": format!("entry {seq}"),
        "refs": {}
    });
    use std::io::Write;
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("feed.jsonl"))
        .unwrap();
    writeln!(f, "{line}").unwrap();
}

#[test]
fn register_creates_private_presence_at_the_current_feed_tip() {
    let tmp = tempfile::tempdir().unwrap();
    let session = "0a1ec845-4394-449e-a098-e0e01cb19887";
    append_feed(tmp.path(), 1);
    append_feed(tmp.path(), 2);
    append_feed(tmp.path(), 3);

    cli(tmp.path())
        .args(["mesh", "register", "--session", session])
        .assert()
        .success()
        .stdout("");

    let peer_dir = tmp
        .path()
        .join("mesh")
        .join("peers")
        .join(format!("claude-{session}"));
    let presence_file = peer_dir.join("presence.json");
    assert_eq!(
        fs::metadata(&peer_dir).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(&presence_file).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let p = read_presence(tmp.path(), "claude", session);
    assert_eq!(p["peer"], format!("claude:{session}"));
    assert_eq!(p["provider"], "claude");
    assert_eq!(p["worker_id"], Value::Null);
    assert_eq!(p["status"], Value::Null);
    assert_eq!(p["working_on"], Value::Null);
    assert_eq!(p["cwd"], Value::Null);
    assert_eq!(p["registered_at"], p["last_seen"]);
    assert_eq!(p["retired"], false);
    assert_eq!(p["cursors"]["feed_seq"], 3);
    assert_eq!(p["cursors"]["inbox"], Value::Null);
}

#[test]
fn register_resolves_session_from_the_pinned_chain() {
    let tmp = tempfile::tempdir().unwrap();
    let session = "11111111-1111-1111-1111-111111111111";
    cli(tmp.path())
        .env("CLAUDE_SESSION_ID", session)
        .args(["mesh", "register"])
        .assert()
        .success();
    assert!(tmp
        .path()
        .join("mesh/peers")
        .join(format!("claude-{session}/presence.json"))
        .exists());

    let beads_session = "22222222-2222-2222-2222-222222222222";
    cli(tmp.path())
        .env("BEADS_ACTOR", format!("worker-cc-{beads_session}"))
        .args(["mesh", "register", "--provider", "codex"])
        .assert()
        .success();
    assert!(tmp
        .path()
        .join("mesh/peers")
        .join(format!("codex-{beads_session}/presence.json"))
        .exists());

    let explicit = "33333333-3333-3333-3333-333333333333";
    cli(tmp.path())
        .env("CLAUDE_SESSION_ID", session)
        .args([
            "mesh",
            "register",
            "--provider",
            "codex",
            "--session",
            explicit,
        ])
        .assert()
        .success();
    assert!(tmp
        .path()
        .join("mesh/peers")
        .join(format!("codex-{explicit}/presence.json"))
        .exists());
}

#[test]
fn register_without_session_uses_the_persisted_user_identity() {
    let tmp = tempfile::tempdir().unwrap();
    cli(tmp.path())
        .args(["mesh", "register"])
        .assert()
        .success();

    let identity_file = tmp.path().join("mesh").join("user-identity.json");
    let identity: Value =
        serde_json::from_str(&fs::read_to_string(&identity_file).unwrap()).unwrap();
    let session = identity["session_uuid"].as_str().unwrap();
    let presence = read_presence(tmp.path(), "user", session);
    assert_eq!(presence["peer"], format!("user:{session}"));
    assert_eq!(presence["provider"], "user");
    assert_eq!(
        fs::metadata(&identity_file).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn re_register_refreshes_liveness_but_preserves_status_and_cursors() {
    let tmp = tempfile::tempdir().unwrap();
    let session = "44444444-4444-4444-4444-444444444444";

    cli(tmp.path())
        .args(["mesh", "register", "--session", session])
        .assert()
        .success();
    cli(tmp.path())
        .args(["mesh", "status", "--session", session, "busy"])
        .assert()
        .success();

    let file = tmp
        .path()
        .join("mesh/peers")
        .join(format!("claude-{session}/presence.json"));
    let mut p = read_presence(tmp.path(), "claude", session);
    p["cursors"]["feed_seq"] = json!(99);
    p["retired"] = json!(true);
    a2a::store::write_atomic(&file, p.to_string().as_bytes()).unwrap();
    let before = p["last_seen"].clone();

    std::thread::sleep(std::time::Duration::from_millis(2));
    cli(tmp.path())
        .args(["mesh", "register", "--session", session])
        .assert()
        .success();

    let p = read_presence(tmp.path(), "claude", session);
    assert_eq!(p["status"], "busy");
    assert_eq!(p["cursors"]["feed_seq"], 99);
    assert_eq!(p["retired"], false);
    assert_ne!(p["last_seen"], before);
}

#[test]
fn heartbeat_auto_registers_and_refreshes_work_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let session = "55555555-5555-5555-5555-555555555555";
    let cwd = tmp.path().join("project");

    cli(tmp.path())
        .args([
            "mesh",
            "heartbeat",
            "--session",
            session,
            "--working-on",
            "writing tests",
            "--cwd",
        ])
        .arg(&cwd)
        .assert()
        .success();

    let p = read_presence(tmp.path(), "claude", session);
    assert_eq!(p["working_on"], "writing tests");
    assert_eq!(p["cwd"], cwd.to_string_lossy().as_ref());
    assert_eq!(p["retired"], false);
}

#[test]
fn status_sets_clears_caps_and_refuses_secret_values() {
    let tmp = tempfile::tempdir().unwrap();
    let session = "66666666-6666-6666-6666-666666666666";
    cli(tmp.path())
        .args(["mesh", "status", "--session", session, "ship it"])
        .assert()
        .success();
    assert_eq!(
        read_presence(tmp.path(), "claude", session)["status"],
        "ship it"
    );

    cli(tmp.path())
        .args(["mesh", "status", "--session", session, ""])
        .assert()
        .success();
    assert_eq!(
        read_presence(tmp.path(), "claude", session)["status"],
        Value::Null
    );

    let out = cli(tmp.path())
        .args([
            "--json",
            "mesh",
            "status",
            "--session",
            session,
            &"x".repeat(a2a::mesh::STATUS_MAX_BYTES + 1),
        ])
        .assert()
        .code(9);
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "STATUS_TOO_LARGE");

    let out = cli(tmp.path())
        .args([
            "mesh",
            "status",
            "--session",
            session,
            "OPENAI_API_KEY=sk-abc123456789012345678901",
        ])
        .assert()
        .code(9);
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(stderr.contains("SECRET_IN_STATUS"), "{stderr}");
    assert!(
        !stderr.contains("sk-abc123456789012345678901"),
        "secret value leaked: {stderr}"
    );
}

#[test]
fn status_can_read_from_stdin_and_retire_leaves_the_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let session = "77777777-7777-7777-7777-777777777777";
    cli(tmp.path())
        .args(["mesh", "status", "--session", session, "-"])
        .write_stdin("from stdin")
        .assert()
        .success();
    assert_eq!(
        read_presence(tmp.path(), "claude", session)["status"],
        "from stdin"
    );

    cli(tmp.path())
        .args(["mesh", "retire", "--session", session])
        .assert()
        .success();
    let p = read_presence(tmp.path(), "claude", session);
    assert_eq!(p["retired"], true);
    assert!(tmp
        .path()
        .join("mesh/peers")
        .join(format!("claude-{session}"))
        .is_dir());
}

#[test]
fn staleness_boundaries_are_pure() {
    let now = Utc.with_ymd_and_hms(2026, 8, 24, 12, 0, 0).unwrap();
    let active = now - Duration::seconds(899);
    let stale = now - Duration::seconds(900);
    assert_eq!(
        a2a::mesh::presence::staleness(active, false, now, 900),
        a2a::mesh::presence::Label::Active
    );
    assert_eq!(
        a2a::mesh::presence::staleness(stale, false, now, 900),
        a2a::mesh::presence::Label::Stale
    );
    assert_eq!(
        a2a::mesh::presence::staleness(now, true, now, 900),
        a2a::mesh::presence::Label::Retired
    );
}
