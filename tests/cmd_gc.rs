// tests/cmd_gc.rs
mod common;
use common::*;

#[test]
fn dry_run_lists_but_deletes_nothing_and_age_zero_reaps_with_tombstone() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(0);
    let out = a2a(&home, tmp.path()).args(["--json", "gc", "--dry-run"]).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["reclaimable"][0]["id"], id.as_str());
    assert!(home.join("workers").join(&id).exists(), "dry-run must not delete");
    a2a(&home, tmp.path()).args(["gc", "--age", "0s"]).assert().success();
    assert!(!home.join("workers").join(&id).exists());
    let index = std::fs::read_to_string(home.join("index.jsonl")).unwrap();
    let t: serde_json::Value = serde_json::from_str(index.lines().next().unwrap()).unwrap();
    assert_eq!(t["id"], id.as_str());
    assert_eq!(t["session_id"], "stub-sess-1");
}

#[test]
fn non_terminal_workers_are_never_removed() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "20")]);
    a2a(&home, tmp.path()).args(["gc", "--age", "0s"]).assert().success();
    assert!(home.join("workers").join(&id).exists(), "running worker must survive gc");
}

#[test]
fn invalid_names_and_symlinks_are_skipped_victims_survive() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let workers = home.join("workers");
    std::fs::create_dir_all(workers.join("..evil")).unwrap();
    std::fs::create_dir_all(workers.join("not-an-id")).unwrap();
    let victim = tmp.path().join("victim"); std::fs::create_dir_all(&victim).unwrap();
    std::fs::write(victim.join("precious.txt"), "keep me").unwrap();
    std::os::unix::fs::symlink(&victim, workers.join("evil-link")).unwrap();
    let out = a2a(&home, tmp.path()).args(["gc", "--age", "0s"]).assert().success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(stderr.contains("..evil") && stderr.contains("not-an-id") && stderr.contains("evil-link"));
    assert!(workers.join("..evil").exists() && workers.join("not-an-id").exists());
    assert!(victim.join("precious.txt").exists(), "symlink target must survive");
}

#[test]
fn codex_home_is_never_touched() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let codex_home = tmp.path().join("codex-home");
    std::fs::create_dir_all(codex_home.join("sessions")).unwrap();
    std::fs::write(codex_home.join("sessions/sentinel"), "x").unwrap();
    a2a(&home, tmp.path()).env("CODEX_HOME", &codex_home)
        .args(["gc", "--age", "0s"]).assert().success();
    assert!(codex_home.join("sessions/sentinel").exists());
}
