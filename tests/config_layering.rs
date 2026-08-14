use assert_cmd::Command;

fn show(home: &std::path::Path, extra_env: &[(&str, &str)], origin: bool) -> serde_json::Value {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home).args(["--json", "config", "show"]);
    if origin {
        c.arg("--origin");
    }
    for (k, v) in extra_env {
        c.env(k, v);
    }
    serde_json::from_slice(&c.assert().success().get_output().stdout).unwrap()
}

#[test]
fn defaults_match_spec_lane_table() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], false);
    assert_eq!(v["data"]["lanes"]["impl"]["sandbox"], "workspace-write");
    assert_eq!(v["data"]["lanes"]["impl"]["model"], "gpt-5.5");
    assert_eq!(v["data"]["lanes"]["impl"]["effort"], "high");
    assert_eq!(v["data"]["lanes"]["impl"]["worktree_default"], false);
    assert_eq!(v["data"]["lanes"]["review"]["sandbox"], "outbox-only");
    assert_eq!(v["data"]["lanes"]["review"]["effort"], "medium");
    assert_eq!(v["data"]["lanes"]["research"]["network"], true);
}

#[test]
fn env_beats_file_beats_default() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path()).unwrap();
    std::fs::write(
        tmp.path().join("config.toml"),
        "[lanes.impl]\ntask_timeout_secs = 60\n",
    )
    .unwrap();
    let v = show(tmp.path(), &[], false);
    assert_eq!(v["data"]["lanes"]["impl"]["task_timeout_secs"], 60);
    let v = show(
        tmp.path(),
        &[("A2A_LANES_IMPL_TASK_TIMEOUT_SECS", "90")],
        false,
    );
    assert_eq!(v["data"]["lanes"]["impl"]["task_timeout_secs"], 90);
    let v = show(
        tmp.path(),
        &[("A2A_LANES_IMPL_TASK_TIMEOUT_SECS", "90")],
        true,
    );
    assert_eq!(v["data"]["origins"]["lanes.impl.task_timeout_secs"], "env");
    assert_eq!(v["data"]["origins"]["lanes.review.sandbox"], "default");
}

#[test]
fn file_only_value_is_labelled_file() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("config.toml"),
        "[lanes.impl]\ntask_timeout_secs = 60\n",
    )
    .unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["origins"]["lanes.impl.task_timeout_secs"], "file");
    assert_eq!(v["data"]["origins"]["lanes.impl.model"], "default");
}

#[test]
fn unknown_key_warns_on_stderr_but_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("config.toml"), "sandbx = \"oops\"\n").unwrap();
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .args(["config", "show"])
        .assert()
        .success();
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(
        stderr.contains("sandbx"),
        "stderr must name the unknown key: {stderr}"
    );
}

#[test]
fn allowed_roots_canonicalized_and_missing_dropped_with_warning() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("no-such-root");
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .env(
            "A2A_ALLOWED_ROOTS",
            format!("/tmp:{}", missing.to_str().unwrap()),
        )
        .args(["--json", "config", "show"])
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let roots = v["data"]["allowed_roots"].as_array().unwrap();
    assert_eq!(roots.len(), 1, "missing root must be dropped: {roots:?}");
    assert_eq!(
        roots[0],
        serde_json::json!(std::fs::canonicalize("/tmp").unwrap().to_str().unwrap())
    );
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(
        stderr.contains("no-such-root"),
        "stderr must name the dropped root: {stderr}"
    );
    assert_eq!(v["data"]["origins"], serde_json::Value::Null);
}
