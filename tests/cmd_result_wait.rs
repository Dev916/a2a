// tests/cmd_result_wait.rs
mod common;
use common::*;

#[test]
fn result_prints_when_done_exit_6_while_running() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "3"), ("STUB_LAST_MSG", "the answer")]);
    a2a(&home, tmp.path()).args(["result", &id]).assert().code(6);
    wait_dead(&home, &id, 10);
    let out = a2a(&home, tmp.path()).args(["result", &id]).assert().code(0);
    assert!(String::from_utf8_lossy(&out.get_output().stdout).contains("the answer"));
}

#[test]
fn schema_gate_rotates_or_fails() {
    // schema requires {"status": ...}; stub writes non-JSON → schema_violation
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj"); std::fs::create_dir_all(&work).unwrap();
    let schema = tmp.path().join("schema.json");
    std::fs::write(&schema, r#"{"type":"object","required":["status"]}"#).unwrap();
    let brief = write_brief(tmp.path(), "x\n");
    let out = a2a(&home, tmp.path())
        .env("STUB_LAST_MSG", "not json at all")
        .args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(&work)
        .arg("--brief").arg(&brief).arg("--schema").arg(&schema)
        .assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(4);
    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["failure_class"], "schema_violation");
    assert!(home.join("workers").join(&id).join("result.raw.md").exists());
    // valid JSON case → result.json written
    let out = a2a(&home, tmp.path())
        .env("STUB_LAST_MSG", r#"{"status":"pass"}"#)
        .args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(&work)
        .arg("--brief").arg(&brief).arg("--schema").arg(&schema)
        .assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id2 = v["data"]["id"].as_str().unwrap().to_string();
    wait_dead(&home, &id2, 10);
    a2a(&home, tmp.path()).args(["status", &id2]).assert().code(0);
    assert!(home.join("workers").join(&id2).join("result.json").exists());
}

#[test]
fn wait_blocks_until_done_and_times_out_with_6() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "2")]);
    let t0 = std::time::Instant::now();
    a2a(&home, tmp.path()).args(["wait", &id, "--timeout", "15s"]).assert().code(0);
    assert!(t0.elapsed() > std::time::Duration::from_secs(1));
    let (home2, tmp2, id2) = spawn_with(&[("STUB_SLEEP", "30")]);
    a2a(&home2, tmp2.path()).args(["wait", &id2, "--timeout", "1s"]).assert().code(6);
}

#[test]
fn wait_any_returns_first_changed_id() {
    let (home, tmp, fast) = spawn_with(&[("STUB_SLEEP", "1")]);
    // second worker in the SAME home so one wait sees both
    let brief = write_brief(tmp.path(), "slow\n");
    let work = tmp.path().join("proj");
    let out = a2a(&home, tmp.path()).env("STUB_SLEEP", "30")
        .args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(&work)
        .arg("--brief").arg(&brief).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let slow = v["data"]["id"].as_str().unwrap().to_string();
    let out = a2a(&home, tmp.path()).args(["wait", &fast, &slow, "--timeout", "20s"])
        .assert().code(0);
    let printed = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(printed.contains(&fast) && !printed.contains(&slow));
}
