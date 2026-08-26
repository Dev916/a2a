// tests/cmd_kill_resume.rs
mod common;
use common::*;

#[test]
fn kill_running_is_cancelled_second_kill_short_circuits() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "30")]);
    a2a(&home, tmp.path()).args(["kill", &id]).assert().code(0);
    wait_dead(&home, &id, 10);
    assert_eq!(read_snapshot(&home, &id)["state"], "cancelled");
    let out = a2a(&home, tmp.path())
        .args(["--json", "kill", &id])
        .assert()
        .code(0);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["already_terminal"], true);
}

#[test]
fn resume_done_worker_bumps_epoch_streams_separately_and_converges() {
    let rec = tempfile::tempdir().unwrap();
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path())
        .args(["status", &id])
        .assert()
        .code(0);
    let epoch1_stream = home.join("workers").join(&id).join("exec.1.jsonl");
    let before = std::fs::read_to_string(&epoch1_stream).unwrap();
    a2a(&home, tmp.path())
        .env("STUB_RECORD", rec.path())
        .args(["resume", &id, "one more thing"])
        .assert()
        .success();
    a2a(&home, tmp.path())
        .args(["wait", &id, "--timeout", "15s"])
        .assert()
        .code(0);
    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["epoch"], 2);
    assert!(home.join("workers").join(&id).join("exec.2.jsonl").exists());
    assert_eq!(
        std::fs::read_to_string(&epoch1_stream).unwrap(),
        before,
        "epoch-1 stream untouched by resume"
    );
    let stdin = std::fs::read_to_string(rec.path().join("resume-stdin.txt")).unwrap();
    assert!(stdin.contains("one more thing"));
}

#[test]
fn resume_running_rejected_and_missing_session_gated() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "10")]);
    a2a(&home, tmp.path())
        .args(["--json", "resume", &id, "x"])
        .assert()
        .code(9);
    // strip the session id → SESSION_UNKNOWN
    let (home2, tmp2, id2) = spawn_with(&[]);
    wait_dead(&home2, &id2, 10);
    a2a(&home2, tmp2.path())
        .args(["status", &id2])
        .assert()
        .code(0);
    let dir2 = home2.join("workers").join(&id2);
    let mut v = read_snapshot(&home2, &id2);
    v["session_id"] = serde_json::Value::Null;
    a2a::store::write_atomic(&dir2.join("state.json"), v.to_string().as_bytes()).unwrap();
    let out = a2a(&home2, tmp2.path())
        .args(["--json", "resume", &id2, "x"])
        .assert()
        .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "SESSION_UNKNOWN");
}

#[test]
fn resume_that_dies_pre_turn_is_session_missing() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path())
        .args(["status", &id])
        .assert()
        .code(0);
    a2a(&home, tmp.path())
        .env("STUB_RESUME_FAIL", "64")
        .args(["resume", &id, "x"])
        .assert()
        .success();
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path())
        .args(["status", &id])
        .assert()
        .code(4);
    assert_eq!(
        read_snapshot(&home, &id)["failure_class"],
        "session_missing"
    );
}
