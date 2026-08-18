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

/// The broker's own section: one key, a default of 300 (the leaked-daemon
/// safety net), and the same defaults < file < env precedence every other key
/// obeys — named in `--origin` as `broker.idle_timeout_secs`.
#[test]
fn broker_idle_timeout_defaults_to_300_and_names_its_layer() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["broker"]["idle_timeout_secs"], 300);
    assert_eq!(
        v["data"]["origins"]["broker.idle_timeout_secs"],
        "default",
        "a value nobody set still says where it came from"
    );

    std::fs::write(
        tmp.path().join("config.toml"),
        "[broker]\nidle_timeout_secs = 60\n",
    )
    .unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["broker"]["idle_timeout_secs"], 60);
    assert_eq!(v["data"]["origins"]["broker.idle_timeout_secs"], "file");

    let v = show(tmp.path(), &[("A2A_BROKER_IDLE_TIMEOUT_SECS", "5")], true);
    assert_eq!(v["data"]["broker"]["idle_timeout_secs"], 5);
    assert_eq!(v["data"]["origins"]["broker.idle_timeout_secs"], "env");
}

/// The admission section (spec §4.9): three numbers with the same
/// defaults < file < env precedence every other key obeys, named in `--origin`
/// as `admission.global_cap`, `admission.queue_cap` and
/// `admission.lanes.<lane>.cap`.
#[test]
fn admission_caps_default_to_8_32_4_and_name_their_layer() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["admission"]["global_cap"], 8);
    assert_eq!(v["data"]["admission"]["queue_cap"], 32);
    assert_eq!(v["data"]["admission"]["lanes"]["impl"]["cap"], 4);
    assert_eq!(v["data"]["admission"]["lanes"]["research"]["cap"], 4);
    for key in [
        "admission.global_cap",
        "admission.queue_cap",
        "admission.lanes.impl.cap",
    ] {
        assert_eq!(
            v["data"]["origins"][key], "default",
            "a value nobody set still says where it came from: {key}"
        );
    }
    // The lane table is its own section: `lanes.*` is untouched by any of this.
    assert_eq!(v["data"]["lanes"]["impl"]["model"], "gpt-5.5");

    std::fs::write(
        tmp.path().join("config.toml"),
        "[admission]\nglobal_cap = 3\nqueue_cap = 5\n\n[admission.lanes.impl]\ncap = 2\n",
    )
    .unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["admission"]["global_cap"], 3);
    assert_eq!(v["data"]["admission"]["queue_cap"], 5);
    assert_eq!(v["data"]["admission"]["lanes"]["impl"]["cap"], 2);
    assert_eq!(
        v["data"]["admission"]["lanes"]["review"]["cap"], 4,
        "one lane's override leaves the others on the default"
    );
    assert_eq!(v["data"]["origins"]["admission.global_cap"], "file");
    assert_eq!(v["data"]["origins"]["admission.lanes.impl.cap"], "file");
    assert_eq!(v["data"]["origins"]["admission.lanes.review.cap"], "default");

    let v = show(
        tmp.path(),
        &[
            ("A2A_ADMISSION_GLOBAL_CAP", "1"),
            ("A2A_ADMISSION_QUEUE_CAP", "2"),
            ("A2A_ADMISSION_LANES_IMPL_CAP", "1"),
        ],
        true,
    );
    assert_eq!(v["data"]["admission"]["global_cap"], 1);
    assert_eq!(v["data"]["admission"]["queue_cap"], 2);
    assert_eq!(v["data"]["admission"]["lanes"]["impl"]["cap"], 1);
    assert_eq!(v["data"]["origins"]["admission.global_cap"], "env");
    assert_eq!(v["data"]["origins"]["admission.queue_cap"], "env");
    assert_eq!(v["data"]["origins"]["admission.lanes.impl.cap"], "env");
}

/// An unknown key under `[admission]` warns like every other unknown key, and
/// the caps around it still apply.
#[test]
fn unknown_admission_key_warns_but_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("config.toml"),
        "[admission]\nglobal_cap = 6\ncap = 9\n\n[admission.lanes.impl]\nlimit = 1\n",
    )
    .unwrap();
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .args(["--json", "config", "show"])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).into_owned();
    assert!(stderr.contains("admission.cap"), "{stderr}");
    assert!(stderr.contains("admission.lanes.impl.limit"), "{stderr}");
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["admission"]["global_cap"], 6);
    assert_eq!(v["data"]["admission"]["lanes"]["impl"]["cap"], 4);
}

/// An unknown key under `[broker]` warns like every other unknown key — a
/// typo in a safety net must not take the CLI down with it.
#[test]
fn unknown_broker_key_warns_but_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("config.toml"),
        "[broker]\nidle_timeout_secs = 7\nidle_timeout = 9\n",
    )
    .unwrap();
    let mut c = Command::cargo_bin("a2a").unwrap();
    let out = c
        .env("A2A_HOME", tmp.path())
        .args(["--json", "config", "show"])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).into_owned();
    assert!(stderr.contains("broker.idle_timeout"), "{stderr}");
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["broker"]["idle_timeout_secs"], 7);
}

/// The `[sync]` section (spec §6): two allowlists and a path, with the same
/// defaults < file < env precedence every other key obeys, named in `--origin`
/// as `sync.allow_skills`, `sync.allow_http_header_servers` and
/// `sync.claude_config_dir`.
#[test]
fn sync_allowlists_default_to_empty_and_name_their_layer() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["sync"]["allow_skills"], serde_json::json!([]));
    assert_eq!(
        v["data"]["sync"]["allow_http_header_servers"],
        serde_json::json!([]),
        "nothing is allowlisted until an operator says so"
    );
    assert_eq!(v["data"]["sync"]["claude_config_dir"], serde_json::Value::Null);
    for key in [
        "sync.allow_skills",
        "sync.allow_http_header_servers",
        "sync.claude_config_dir",
    ] {
        assert_eq!(
            v["data"]["origins"][key], "default",
            "a value nobody set still says where it came from: {key}"
        );
    }

    std::fs::write(
        tmp.path().join("config.toml"),
        "[sync]\nallow_skills = [\"slack-cli\"]\nallow_http_header_servers = [\"ghl\"]\n\
         claude_config_dir = \"/tmp/claude-from-file\"\n",
    )
    .unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(
        v["data"]["sync"]["allow_skills"],
        serde_json::json!(["slack-cli"])
    );
    assert_eq!(
        v["data"]["sync"]["allow_http_header_servers"],
        serde_json::json!(["ghl"])
    );
    assert_eq!(v["data"]["sync"]["claude_config_dir"], "/tmp/claude-from-file");
    assert_eq!(v["data"]["origins"]["sync.allow_skills"], "file");
    assert_eq!(v["data"]["origins"]["sync.claude_config_dir"], "file");

    // Env wins, and a list is comma-separated with blanks trimmed and empties
    // dropped — a trailing comma is a typo, not an empty allowlist entry.
    let v = show(
        tmp.path(),
        &[
            ("A2A_SYNC_ALLOW_SKILLS", " x-cli , docusign ,"),
            ("A2A_SYNC_ALLOW_HTTP_HEADER_SERVERS", "ghl,linear"),
            ("A2A_SYNC_CLAUDE_CONFIG_DIR", "/tmp/claude-from-env"),
        ],
        true,
    );
    assert_eq!(
        v["data"]["sync"]["allow_skills"],
        serde_json::json!(["x-cli", "docusign"])
    );
    assert_eq!(
        v["data"]["sync"]["allow_http_header_servers"],
        serde_json::json!(["ghl", "linear"])
    );
    assert_eq!(v["data"]["sync"]["claude_config_dir"], "/tmp/claude-from-env");
    for key in [
        "sync.allow_skills",
        "sync.allow_http_header_servers",
        "sync.claude_config_dir",
    ] {
        assert_eq!(v["data"]["origins"][key], "env", "{key}");
    }
}

/// An unknown key under `[sync]` warns like every other unknown key, and the
/// allowlist beside it still applies.
#[test]
fn unknown_sync_key_warns_but_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("config.toml"),
        "[sync]\nallow_skills = [\"pr\"]\nallow_http_headers = [\"ghl\"]\n",
    )
    .unwrap();
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .args(["--json", "config", "show"])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).into_owned();
    assert!(stderr.contains("sync.allow_http_headers"), "{stderr}");
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["sync"]["allow_skills"], serde_json::json!(["pr"]));
    assert_eq!(
        v["data"]["sync"]["allow_http_header_servers"],
        serde_json::json!([]),
        "the near-miss spelling sets nothing"
    );
}

/// A wrong-typed `[sync]` value fails loudly and names its dotted key — the
/// same contract every other section carries.
#[test]
fn a_malformed_sync_allowlist_fails_naming_its_dotted_key() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("config.toml"), "[sync]\nallow_skills = 3\n").unwrap();
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .args(["config", "show"])
        .assert()
        .code(9);
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).into_owned();
    assert!(stderr.contains("CONFIG_INVALID"), "{stderr}");
    assert!(
        stderr.contains("sync.allow_skills: expected an array of strings, got integer"),
        "{stderr}"
    );
}
