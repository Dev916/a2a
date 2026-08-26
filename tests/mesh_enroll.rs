use assert_cmd::Command;
use serde_json::Value;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn cmd(home: &Path) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.arg("--home").arg(home).env_remove("A2A_HOME");
    c
}

fn json_out(assert: &assert_cmd::assert::Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).unwrap()
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[test]
fn cli_identity_enroll_and_install_token_round_trip() {
    let root_tmp = tempfile::tempdir().unwrap();
    let join_tmp = tempfile::tempdir().unwrap();

    cmd(root_tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();

    let identity_assert = cmd(join_tmp.path())
        .args(["mesh", "gateway", "identity", "--json"])
        .assert()
        .success();
    let identity = json_out(&identity_assert);
    assert_eq!(identity["ok"], true);
    assert_eq!(identity["command"], "mesh gateway identity");
    let gateway_id = identity["data"]["gateway_id"].as_str().unwrap().to_string();
    let pubkey_b64 = identity["data"]["pubkey_b64"].as_str().unwrap().to_string();
    assert_eq!(identity["data"]["pubkey_fpr"].as_str().unwrap().len(), 64);
    assert_eq!(identity["data"]["cert_fpr"].as_str().unwrap().len(), 64);

    fs::create_dir_all(join_tmp.path().join("mesh/fleet")).unwrap();
    fs::copy(
        root_tmp.path().join("mesh/fleet/root.pub"),
        join_tmp.path().join("mesh/fleet/root.pub"),
    )
    .unwrap();

    let enroll_assert = cmd(root_tmp.path())
        .args([
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            &gateway_id,
            "--pubkey",
            &pubkey_b64,
            "--json",
        ])
        .assert()
        .success();
    let enroll = json_out(&enroll_assert);
    assert_eq!(enroll["ok"], true);
    assert_eq!(enroll["command"], "mesh fleet enroll");
    let token = enroll["data"]["token"].as_str().unwrap().to_string();
    let reminder = enroll["data"]["reminder"].as_str().unwrap();
    assert!(reminder.contains("token"), "{reminder}");
    assert!(reminder.contains("root.pub"), "{reminder}");
    assert!(reminder.contains("registry.env"), "{reminder}");

    let install_assert = cmd(join_tmp.path())
        .args(["mesh", "gateway", "install-token", "--file", "-", "--json"])
        .write_stdin(token.clone())
        .assert()
        .success();
    let install = json_out(&install_assert);
    assert_eq!(install["ok"], true);
    assert_eq!(install["command"], "mesh gateway install-token");
    assert_eq!(install["data"]["gateway_id"], gateway_id);
    assert_eq!(install["data"]["installed"], true);

    let token_path = join_tmp.path().join("mesh/gateway/gw.token");
    assert_eq!(mode(&token_path), 0o600);
    assert_eq!(fs::read_to_string(token_path).unwrap().trim(), token);
}
