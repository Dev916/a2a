use assert_cmd::Command;

#[test]
fn version_prints_semver_and_exits_zero() {
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .arg("--version")
        .assert()
        .success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    assert!(stdout.trim().starts_with("a2a "), "got: {stdout}");
}

#[test]
fn unknown_subcommand_exits_2() {
    Command::cargo_bin("a2a")
        .unwrap()
        .arg("definitely-not-a-command")
        .assert()
        .code(2);
}

#[test]
fn selftest_error_emits_single_json_envelope_and_exit_9() {
    let out = Command::cargo_bin("a2a")
        .unwrap()
        .args(["--json", "__selftest-error"])
        .assert()
        .code(9);
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout must be exactly one JSON object");
    assert_eq!(v["schema_version"], 1);
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["code"], "SELFTEST");
    assert!(v["error"]["message"].is_string());
}

#[test]
fn exit_codes_are_pinned() {
    use a2a::output::ExitCode::*;
    for (code, n) in [
        (Success, 0),
        (Internal, 1),
        (Usage, 2),
        (NoSuchWorker, 3),
        (WorkerFailed, 4),
        (Blocked, 5),
        (NotTerminal, 6),
        (BrokerUnavailable, 7),
        (CmdTimeout, 8),
        (Precondition, 9),
        (Orphaned, 10),
    ] {
        assert_eq!(code as i32, n);
    }
}

#[test]
fn toolchain_is_pinned() {
    let manifest = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    )
    .unwrap();
    assert!(manifest.contains("edition = \"2021\""));
    assert!(manifest.contains("rand = \"0.8") || manifest.contains("rand = { version = \"0.8"));
    assert!(manifest.contains("nix = \"0.29") || manifest.contains("nix = { version = \"0.29"));
}
