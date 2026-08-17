// tests/cmd_doctor.rs
mod common;
use common::*;
use std::os::unix::fs::PermissionsExt;

fn checks(
    home: &std::path::Path,
    root: &std::path::Path,
    f: impl FnOnce(&mut assert_cmd::Command),
) -> (serde_json::Value, i32) {
    let mut c = a2a(home, root);
    c.args(["--json", "doctor"]);
    f(&mut c);
    let out = c.assert();
    let code = out.get_output().status.code().unwrap();
    (
        serde_json::from_slice(&out.get_output().stdout).unwrap(),
        code,
    )
}
fn check<'a>(v: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    v["data"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == id)
        .unwrap()
}

#[test]
fn all_ok_with_stub_exits_zero_and_lists_six_checks() {
    let tmp = tempfile::tempdir().unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |_| {});
    assert_eq!(code, 0);
    assert_eq!(v["data"]["checks"].as_array().unwrap().len(), 6);
    assert_eq!(check(&v, "codex_present")["status"], "ok");
    assert_eq!(check(&v, "env_inheritance")["status"], "warn");
}

#[test]
fn missing_codex_fails_exit_9() {
    let tmp = tempfile::tempdir().unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.env("A2A_CODEX_BIN", "/nonexistent/codex");
    });
    assert_eq!(code, 9);
    assert_eq!(check(&v, "codex_present")["status"], "fail");
}

#[test]
fn out_of_range_version_warns_but_exits_zero() {
    // stub-codex has no --version handling; add a fake: a script printing "codex-cli 0.99.0"
    let tmp = tempfile::tempdir().unwrap();
    let fake = tmp.path().join("old-codex");
    std::fs::write(&fake, "#!/bin/sh\necho 'codex-cli 0.99.0'\n").unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.env("A2A_CODEX_BIN", &fake);
    });
    assert_eq!(code, 0);
    assert_eq!(check(&v, "codex_version")["status"], "warn");
}

#[test]
fn bad_home_perms_fail() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    checks(&home, tmp.path(), |_| {}); // create the tree
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (v, code) = checks(&home, tmp.path(), |_| {});
    assert_eq!(code, 9);
    assert_eq!(check(&v, "home_perms")["status"], "fail");
}

#[test]
fn orphaned_worker_surfaces_in_workers_health() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "1")]);
    wait_dead(&home, &id, 10);
    // fake an orphan: rewrite state to running with a dead pid, older than grace
    let dir = home.join("workers").join(&id);
    let mut v = read_snapshot(&home, &id);
    v["state"] = "running".into();
    v["entered_state_at"] = serde_json::json!(chrono::Utc::now() - chrono::Duration::hours(1));
    a2a::store::write_atomic(&dir.join("state.json"), v.to_string().as_bytes()).unwrap();
    // events already show turn completion; doctor's reconcile pass will surface terminal or orphan —
    // either way workers_health reports a non-empty detail
    let (v, _) = checks(&home, tmp.path(), |_| {});
    assert!(
        check(&v, "workers_health")["detail"]
            .as_str()
            .unwrap()
            .len()
            > 0
    );
}
