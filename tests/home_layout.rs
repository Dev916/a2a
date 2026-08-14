use assert_cmd::Command;
use std::os::unix::fs::PermissionsExt;

fn cmd(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home);
    c
}

#[test]
fn home_tree_created_0700_with_layout_version() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("a2a-home");
    cmd(&home).args(["config", "show"]).assert().success();
    let mode = std::fs::metadata(&home).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    assert!(home.join("workers").is_dir());
    assert!(home.join("worktrees").is_dir());
    assert_eq!(
        std::fs::read_to_string(home.join("LAYOUT_VERSION"))
            .unwrap()
            .trim(),
        "1"
    );
}

#[test]
fn home_flag_beats_env() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, b) = (tmp.path().join("env-home"), tmp.path().join("flag-home"));
    let out = cmd(&a)
        .arg("--home")
        .arg(&b)
        .args(["--json", "config", "show"])
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["home"], b.to_str().unwrap());
    assert!(b.exists() && !a.exists());
}

#[test]
fn layout_version_mismatch_exits_9() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    cmd(&home).args(["config", "show"]).assert().success();
    std::fs::write(home.join("LAYOUT_VERSION"), "999\n").unwrap();
    let out = cmd(&home).args(["--json", "config", "show"]).assert().code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "LAYOUT_VERSION_MISMATCH");
}
