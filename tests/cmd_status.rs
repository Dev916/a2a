// tests/cmd_status.rs
mod common;
use common::*;

#[test]
fn finished_worker_reconciles_to_done_with_session_id() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let out = a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .assert()
        .code(0);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["state"], "done");
    assert_eq!(read_snapshot(&home, &id)["session_id"], "stub-sess-1");
}

#[test]
fn failed_stub_is_failed_no_result_exit_4() {
    let (home, tmp, id) = spawn_with(&[("STUB_EXIT", "1"), ("STUB_NO_RESULT", "1")]);
    wait_dead(&home, &id, 10);
    let out = a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .assert()
        .code(4);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["failure_class"], "no_result");
}

#[test]
fn turn_stalled_is_reported_as_the_failed_reason() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path())
        .args(["status", &id])
        .assert()
        .code(0);

    let dir = home.join("workers").join(&id);
    let mut snap = read_snapshot(&home, &id);
    snap["state"] = serde_json::json!("failed");
    snap["state_reason"] = serde_json::Value::Null;
    snap["failure_class"] = serde_json::json!("turn_stalled");
    a2a::store::write_atomic(&dir.join("state.json"), snap.to_string().as_bytes()).unwrap();

    let out = a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .assert()
        .code(4);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["state"], "failed");
    assert_eq!(v["data"]["failure_class"], "turn_stalled");
    assert!(
        String::from_utf8_lossy(&out.get_output().stderr).contains("failed{turn_stalled}"),
        "human status names the refined failure: {}",
        String::from_utf8_lossy(&out.get_output().stderr)
    );
}

#[test]
fn expired_deadline_fires_task_timeout_and_kills() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "60")]);
    rewrite_deadline_to_past(&home, &id);
    a2a(&home, tmp.path())
        .args(["status", &id])
        .assert()
        .code(4);
    assert_eq!(read_snapshot(&home, &id)["failure_class"], "timeout");
    wait_dead(&home, &id, 10);
}

#[test]
fn unknown_worker_exits_3() {
    let tmp = tempfile::tempdir().unwrap();
    let out = a2a(&tmp.path().join("home"), tmp.path())
        .args(["--json", "status", "nope"])
        .assert()
        .code(3);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "NO_SUCH_WORKER");
}

#[test]
fn concurrent_status_calls_do_not_corrupt() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let root = tmp.path().to_path_buf();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (h, r, i) = (home.clone(), root.clone(), id.clone());
            // `let _` only silences #[must_use]; the corruption check is the
            // snapshot assertion below, not any single invocation's exit code.
            std::thread::spawn(move || {
                let _ = a2a(&h, &r).args(["status", &i]).assert();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(read_snapshot(&home, &id)["state"], "done");
}

#[test]
fn just_spawned_worker_is_not_declared_dead_by_grace_rule() {
    // STUB_SLEEP keeps the shim alive; immediately reconciling must keep state=running
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "3")]);
    let out = a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .assert()
        .code(0);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["state"], "running");
}
