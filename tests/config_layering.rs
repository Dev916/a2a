use assert_cmd::Command;
mod common;

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
    // The impl lane — and only it — defaults to its own worktree now that
    // `src/worktree.rs` can create one.
    assert_eq!(v["data"]["lanes"]["impl"]["worktree_default"], true);
    assert_eq!(v["data"]["lanes"]["general"]["worktree_default"], false);
    assert_eq!(v["data"]["lanes"]["review"]["sandbox"], "outbox-only");
    assert_eq!(v["data"]["lanes"]["review"]["effort"], "medium");
    assert_eq!(v["data"]["lanes"]["research"]["network"], true);
}

#[test]
fn profiles_pin_defaults_and_layer_file_then_env_with_custom_names() {
    use a2a::config::PROFILE_FIELDS;

    assert_eq!(PROFILE_FIELDS, ["model", "effort"]);

    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["profiles"]["quick"]["model"], "gpt-5.5");
    assert_eq!(v["data"]["profiles"]["quick"]["effort"], "low");
    assert_eq!(v["data"]["profiles"]["standard"]["model"], "gpt-5.5");
    assert_eq!(v["data"]["profiles"]["standard"]["effort"], "high");
    assert_eq!(v["data"]["profiles"]["max"]["model"], "gpt-5.6-sol");
    assert_eq!(v["data"]["profiles"]["max"]["effort"], "max");
    assert_eq!(v["data"]["origins"]["profiles.max.model"], "default");

    std::fs::write(
        tmp.path().join("config.toml"),
        "[profiles.max]\nmodel = \"file-max\"\n\n[profiles.precise]\nmodel = \"custom-model\"\neffort = \"xhigh\"\n",
    )
    .unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["profiles"]["max"]["model"], "file-max");
    assert_eq!(v["data"]["profiles"]["max"]["effort"], "max");
    assert_eq!(v["data"]["origins"]["profiles.max.model"], "file");
    assert_eq!(v["data"]["origins"]["profiles.max.effort"], "default");
    assert_eq!(v["data"]["profiles"]["precise"]["model"], "custom-model");
    assert_eq!(v["data"]["profiles"]["precise"]["effort"], "xhigh");
    assert_eq!(v["data"]["origins"]["profiles.precise.model"], "file");
    assert_eq!(v["data"]["origins"]["profiles.precise.effort"], "file");

    let v = show(
        tmp.path(),
        &[
            ("A2A_PROFILES_MAX_MODEL", "env-max"),
            ("A2A_PROFILES_PRECISE_EFFORT", "medium"),
        ],
        true,
    );
    assert_eq!(v["data"]["profiles"]["max"]["model"], "env-max");
    assert_eq!(v["data"]["profiles"]["precise"]["effort"], "medium");
    assert_eq!(v["data"]["origins"]["profiles.max.model"], "env");
    assert_eq!(v["data"]["origins"]["profiles.precise.effort"], "env");
}

#[test]
fn a_new_profile_requires_both_model_and_effort() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("config.toml"),
        "[profiles.incomplete]\nmodel = \"gpt-5.5\"\n",
    )
    .unwrap();
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .args(["--json", "config", "show"])
        .assert()
        .code(9);
    let value: serde_json::Value =
        serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(value["error"]["code"], "CONFIG_INVALID");
    assert!(
        value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("profiles.incomplete.effort"),
        "{value}"
    );
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

/// Canonicalization and the dropped-root warning — plus the IMPLICIT worktrees
/// root, which this home is the one existing case for: `A2A_HOME` is the
/// tempdir itself (canonically `/private/var/folders/…` on macOS) while the one
/// surviving allowed root is `/tmp` → `/private/tmp`, so `<home>/worktrees` is
/// under no configured root and is appended LAST.
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
    assert_eq!(
        roots.len(),
        3,
        "missing root dropped, implicit worktrees+work added: {roots:?}"
    );
    assert_eq!(
        roots[0],
        serde_json::json!(std::fs::canonicalize("/tmp").unwrap().to_str().unwrap())
    );
    assert_eq!(
        roots[1],
        serde_json::json!(std::fs::canonicalize(tmp.path().join("worktrees"))
            .unwrap()
            .to_str()
            .unwrap()),
        "$A2A_HOME/worktrees is an allowed root by construction, and comes last"
    );
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(
        stderr.contains("no-such-root"),
        "stderr must name the dropped root: {stderr}"
    );
    assert_eq!(v["data"]["origins"], serde_json::Value::Null);
}

#[test]
fn default_allowed_roots_probe_conventional_directories_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let home_env = tmp.path().join("user-home");
    std::fs::create_dir_all(home_env.join("Development")).unwrap();
    let a2a_home = tmp.path().join("a2a-home");

    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", &a2a_home)
        .env("HOME", &home_env)
        .env_remove("A2A_ALLOWED_ROOTS")
        .args(["--json", "config", "show", "--origin"])
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let roots = v["data"]["allowed_roots"].as_array().unwrap();
    assert_eq!(
        roots[0],
        serde_json::json!(std::fs::canonicalize(home_env.join("Development"))
            .unwrap()
            .to_str()
            .unwrap()),
        "the conventional Development directory is a default root"
    );
    assert_eq!(v["data"]["origins"]["allowed_roots"], "default");
}

#[test]
fn missing_default_roots_warn_with_add_root_remedy_when_no_non_implicit_roots_survive() {
    let tmp = tempfile::tempdir().unwrap();
    let home_env = tmp.path().join("user-home");
    std::fs::create_dir_all(&home_env).unwrap();
    let brief = tmp.path().join("brief.md");
    std::fs::write(&brief, "x\n").unwrap();
    let cwd = tmp.path().join("real-project");
    std::fs::create_dir_all(&cwd).unwrap();

    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path().join("a2a-home"))
        .env("HOME", &home_env)
        .env_remove("A2A_ALLOWED_ROOTS")
        .env("A2A_CODEX_BIN", common::stub_path())
        .env("A2A_BROKER_DISABLE", "1")
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(&cwd)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("none are configured on this machine"),
        "spawn should report that no project roots are configured: {v}"
    );
    assert!(
        v["error"]["remedy"]
            .as_str()
            .unwrap()
            .contains("a2a config add-root <dir>"),
        "spawn should tell first-run users how to configure roots: {v}"
    );
}

/// The other half of the implicit root: a home INSIDE an allowed root leaves
/// the list exactly as Phase 1 left it — one entry, no implicit anything.
/// (This is the shape `tests/broker_lifecycle.rs` and `tests/cmd_broker.rs`
/// pin, which is why neither of them needed an edit.)
#[test]
fn a_home_inside_an_allowed_root_adds_no_implicit_root() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", &home)
        .env("A2A_ALLOWED_ROOTS", tmp.path())
        .args(["--json", "config", "show"])
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let roots = v["data"]["allowed_roots"].as_array().unwrap();
    assert_eq!(
        roots,
        &vec![serde_json::json!(std::fs::canonicalize(tmp.path())
            .unwrap()
            .to_str()
            .unwrap())],
        "the configured root already covers the worktrees dir: {roots:?}"
    );
    // The origins key set is unchanged: the implicit root rides under
    // `allowed_roots` rather than earning a name of its own.
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", &home)
        .env("A2A_ALLOWED_ROOTS", tmp.path())
        .args(["--json", "config", "show", "--origin"])
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let origins = v["data"]["origins"].as_object().unwrap();
    assert_eq!(origins["allowed_roots"], "env");
    assert!(
        !origins.keys().any(|k| k.contains("worktrees")),
        "no new origin key: {origins:?}"
    );
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
        v["data"]["origins"]["broker.idle_timeout_secs"], "default",
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

/// The `[broker]` section's second key (spec §4.5): the `events.jsonl` cap,
/// 10 MiB by default, layered like every other key — and `0` is a value an
/// operator sets on purpose (no rotation), not an unset one.
#[test]
fn broker_events_max_bytes_defaults_to_ten_mebibytes_and_names_its_layer() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["broker"]["events_max_bytes"], 10_485_760);
    assert_eq!(v["data"]["origins"]["broker.events_max_bytes"], "default");

    std::fs::write(
        tmp.path().join("config.toml"),
        "[broker]\nevents_max_bytes = 65536\n",
    )
    .unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["broker"]["events_max_bytes"], 65536);
    assert_eq!(v["data"]["origins"]["broker.events_max_bytes"], "file");

    let v = show(tmp.path(), &[("A2A_BROKER_EVENTS_MAX_BYTES", "0")], true);
    assert_eq!(v["data"]["broker"]["events_max_bytes"], 0);
    assert_eq!(v["data"]["origins"]["broker.events_max_bytes"], "env");
}

#[test]
fn broker_turn_stall_defaults_to_600_and_zero_is_a_layered_opt_out() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["broker"]["turn_stall_secs"], 600);
    assert_eq!(v["data"]["origins"]["broker.turn_stall_secs"], "default");

    std::fs::write(
        tmp.path().join("config.toml"),
        "[broker]\nturn_stall_secs = 45\n",
    )
    .unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["broker"]["turn_stall_secs"], 45);
    assert_eq!(v["data"]["origins"]["broker.turn_stall_secs"], "file");

    let v = show(tmp.path(), &[("A2A_BROKER_TURN_STALL_SECS", "0")], true);
    assert_eq!(v["data"]["broker"]["turn_stall_secs"], 0);
    assert_eq!(v["data"]["origins"]["broker.turn_stall_secs"], "env");
}

/// The mesh section (spec §10): the feed cap, staleness window and gc horizon
/// report default origins, layer through file/env, reject malformed values with
/// the dotted key, and warn on unknown siblings.
#[test]
fn mesh_defaults_layer_and_name_bad_values() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["mesh"]["feed_max_bytes"], 10_485_760);
    assert_eq!(v["data"]["mesh"]["stale_after_secs"], 900);
    assert_eq!(v["data"]["mesh"]["gc_after_days"], 7);
    for key in [
        "mesh.feed_max_bytes",
        "mesh.stale_after_secs",
        "mesh.gc_after_days",
    ] {
        assert_eq!(v["data"]["origins"][key], "default", "{key}");
    }

    std::fs::write(
        tmp.path().join("config.toml"),
        "[mesh]\nfeed_max_bytes = 65536\nstale_after_secs = 30\ngc_after_days = 2\nsurprise = 1\n",
    )
    .unwrap();
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .args(["--json", "config", "show", "--origin"])
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["mesh"]["feed_max_bytes"], 65536);
    assert_eq!(v["data"]["mesh"]["stale_after_secs"], 30);
    assert_eq!(v["data"]["mesh"]["gc_after_days"], 2);
    assert_eq!(v["data"]["origins"]["mesh.feed_max_bytes"], "file");
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(stderr.contains("mesh.surprise"), "{stderr}");

    let v = show(
        tmp.path(),
        &[
            ("A2A_MESH_FEED_MAX_BYTES", "0"),
            ("A2A_MESH_STALE_AFTER_SECS", "45"),
            ("A2A_MESH_GC_AFTER_DAYS", "10"),
        ],
        true,
    );
    assert_eq!(v["data"]["mesh"]["feed_max_bytes"], 0);
    assert_eq!(v["data"]["mesh"]["stale_after_secs"], 45);
    assert_eq!(v["data"]["mesh"]["gc_after_days"], 10);
    assert_eq!(v["data"]["origins"]["mesh.gc_after_days"], "env");

    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .env("A2A_MESH_STALE_AFTER_SECS", "soon")
        .args(["--json", "config", "show"])
        .assert()
        .failure();
    let e: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(e["error"]["code"], "CONFIG_INVALID");
    assert!(
        e["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mesh.stale_after_secs"),
        "{e}"
    );

    std::fs::write(
        tmp.path().join("config.toml"),
        "[mesh]\ngc_after_days = \"later\"\n",
    )
    .unwrap();
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .args(["--json", "config", "show"])
        .assert()
        .failure();
    let e: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(e["error"]["code"], "CONFIG_INVALID");
    assert!(
        e["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mesh.gc_after_days"),
        "{e}"
    );
}

/// The `[mesh.work]` section (work handoff spec §8): every key, including the
/// envelope TTL, layers through defaults < file < env and reports its origin
/// under the same dotted path as every other section.
#[test]
fn mesh_work_defaults_layer_and_name_bad_values() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(v["data"]["mesh"]["work"]["ttl_days"], 7);
    assert_eq!(v["data"]["mesh"]["work"]["max_directed_bytes"], 262_144);
    assert_eq!(v["data"]["mesh"]["work"]["max_pool_bytes"], 32_768);
    assert_eq!(
        v["data"]["mesh"]["work"]["result_note_max_bytes"],
        65_536
    );
    assert_eq!(v["data"]["mesh"]["work"]["diff_scan_max_bytes"], 5_242_880);
    assert_eq!(v["data"]["mesh"]["work"]["work_poll_secs"], 15);
    assert_eq!(v["data"]["mesh"]["work"]["claim_stall_secs"], 900);
    assert_eq!(v["data"]["mesh"]["work"]["checkout_retain_days"], 30);
    assert_eq!(v["data"]["origins"]["mesh.work.ttl_days"], "default");
    assert_eq!(
        v["data"]["origins"]["mesh.work.diff_scan_max_bytes"],
        "default"
    );

    std::fs::write(
        tmp.path().join("config.toml"),
        "[mesh.work]\nttl_days = 3\nmax_directed_bytes = 8192\nmax_pool_bytes = 4096\n\
         result_note_max_bytes = 2048\ndiff_scan_max_bytes = 1024\nwork_poll_secs = 2\nclaim_stall_secs = 4\n\
         checkout_retain_days = 5\nsurprise = 1\n",
    )
    .unwrap();
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .args(["--json", "config", "show", "--origin"])
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["mesh"]["work"]["ttl_days"], 3);
    assert_eq!(v["data"]["mesh"]["work"]["max_directed_bytes"], 8192);
    assert_eq!(v["data"]["mesh"]["work"]["diff_scan_max_bytes"], 1024);
    assert_eq!(v["data"]["origins"]["mesh.work.ttl_days"], "file");
    assert_eq!(
        v["data"]["origins"]["mesh.work.diff_scan_max_bytes"],
        "file"
    );
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).into_owned();
    assert!(stderr.contains("mesh.work.surprise"), "{stderr}");

    let v = show(
        tmp.path(),
        &[
            ("A2A_MESH_WORK_TTL_DAYS", "9"),
            ("A2A_MESH_WORK_MAX_DIRECTED_BYTES", "16384"),
            ("A2A_MESH_WORK_DIFF_SCAN_MAX_BYTES", "1024"),
        ],
        true,
    );
    assert_eq!(v["data"]["mesh"]["work"]["ttl_days"], 9);
    assert_eq!(v["data"]["mesh"]["work"]["max_directed_bytes"], 16_384);
    assert_eq!(v["data"]["mesh"]["work"]["diff_scan_max_bytes"], 1024);
    assert_eq!(v["data"]["origins"]["mesh.work.ttl_days"], "env");
    assert_eq!(
        v["data"]["origins"]["mesh.work.diff_scan_max_bytes"],
        "env"
    );

    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .env("A2A_MESH_WORK_TTL_DAYS", "soon")
        .args(["--json", "config", "show"])
        .assert()
        .failure();
    let e: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(e["error"]["code"], "CONFIG_INVALID");
    assert!(
        e["error"]["message"]
            .as_str()
            .unwrap()
            .contains("A2A_MESH_WORK_TTL_DAYS"),
        "{e}"
    );
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
    assert_eq!(
        v["data"]["origins"]["admission.lanes.review.cap"],
        "default"
    );

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
    assert_eq!(
        v["data"]["sync"]["claude_config_dir"],
        serde_json::Value::Null
    );
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
    assert_eq!(
        v["data"]["sync"]["claude_config_dir"],
        "/tmp/claude-from-file"
    );
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
    assert_eq!(
        v["data"]["sync"]["claude_config_dir"],
        "/tmp/claude-from-env"
    );
    for key in [
        "sync.allow_skills",
        "sync.allow_http_header_servers",
        "sync.claude_config_dir",
    ] {
        assert_eq!(v["data"]["origins"][key], "env", "{key}");
    }
}

#[test]
fn update_repo_url_defaults_layers_and_names_unknown_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], true);
    assert_eq!(
        v["data"]["update"]["repo_url"],
        a2a::config::DEFAULT_UPDATE_REPO_URL
    );
    assert_eq!(v["data"]["origins"]["update.repo_url"], "default");

    std::fs::write(
        tmp.path().join("config.toml"),
        "[update]\nrepo_url = \"file:///tmp/a2a.git\"\nreop_url = \"typo\"\n",
    )
    .unwrap();
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", tmp.path())
        .args(["--json", "config", "show", "--origin"])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).into_owned();
    assert!(stderr.contains("update.reop_url"), "{stderr}");
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["update"]["repo_url"], "file:///tmp/a2a.git");
    assert_eq!(v["data"]["origins"]["update.repo_url"], "file");

    let v = show(
        tmp.path(),
        &[("A2A_UPDATE_REPO_URL", "ssh://example.invalid/a2a.git")],
        true,
    );
    assert_eq!(
        v["data"]["update"]["repo_url"],
        "ssh://example.invalid/a2a.git"
    );
    assert_eq!(v["data"]["origins"]["update.repo_url"], "env");
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
