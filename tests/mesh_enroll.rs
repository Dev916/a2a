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

// Live-fleet incident (a2a-mx4): read-only fleet commands resolved the fleet id
// via fleet_init, which MINTS a new root on any home without root.key — so a
// joiner running `fleet ls` silently forked itself onto a phantom fleet and
// clobbered the root.pub its owner installed.

fn joiner_home_on_fleet(root_tmp: &Path, join_tmp: &Path) {
    cmd(root_tmp)
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    fs::create_dir_all(join_tmp.join("mesh/fleet")).unwrap();
    fs::copy(
        root_tmp.join("mesh/fleet/root.pub"),
        join_tmp.join("mesh/fleet/root.pub"),
    )
    .unwrap();
    let registry_dir = root_tmp.join("file-registry");
    fs::create_dir_all(&registry_dir).unwrap();
    let url = format!("file:{}", registry_dir.display());
    fs::write(root_tmp.join("mesh/fleet/registry.env"), format!("{url}\n")).unwrap();
    fs::write(join_tmp.join("mesh/fleet/registry.env"), format!("{url}\n")).unwrap();
}

#[test]
fn fleet_ls_on_a_joiner_home_never_mints_and_uses_the_installed_fleet() {
    let root_tmp = tempfile::tempdir().unwrap();
    let join_tmp = tempfile::tempdir().unwrap();
    joiner_home_on_fleet(root_tmp.path(), join_tmp.path());
    let pub_before = fs::read(join_tmp.path().join("mesh/fleet/root.pub")).unwrap();

    let ls = cmd(join_tmp.path())
        .args(["mesh", "fleet", "ls", "--json"])
        .assert()
        .success();
    let out = json_out(&ls);
    assert_eq!(out["ok"], true, "{out}");

    assert!(
        !join_tmp.path().join("mesh/fleet/root.key").exists(),
        "fleet ls must never mint a root key on a joiner"
    );
    let pub_after = fs::read(join_tmp.path().join("mesh/fleet/root.pub")).unwrap();
    assert_eq!(pub_before, pub_after, "fleet ls must not rewrite root.pub");
}

#[test]
fn fleet_init_refuses_to_clobber_a_joiner_home() {
    let root_tmp = tempfile::tempdir().unwrap();
    let join_tmp = tempfile::tempdir().unwrap();
    joiner_home_on_fleet(root_tmp.path(), join_tmp.path());
    let pub_before = fs::read(join_tmp.path().join("mesh/fleet/root.pub")).unwrap();

    let init = cmd(join_tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .failure();
    let out = json_out(&init);
    assert_eq!(out["error"]["code"], "FLEET_ALREADY_JOINED", "{out}");

    let pub_after = fs::read(join_tmp.path().join("mesh/fleet/root.pub")).unwrap();
    assert_eq!(pub_before, pub_after, "refused init must not touch root.pub");
    assert!(!join_tmp.path().join("mesh/fleet/root.key").exists());
}

#[test]
fn fleet_rotate_root_on_a_joiner_home_refuses_instead_of_minting() {
    let root_tmp = tempfile::tempdir().unwrap();
    let join_tmp = tempfile::tempdir().unwrap();
    joiner_home_on_fleet(root_tmp.path(), join_tmp.path());

    let rotate = cmd(join_tmp.path())
        .args(["--json", "mesh", "fleet", "rotate-root"])
        .assert()
        .failure();
    let out = json_out(&rotate);
    assert_eq!(out["error"]["code"], "FLEET_ALREADY_JOINED", "{out}");
    assert!(!join_tmp.path().join("mesh/fleet/root.key").exists());
}

#[test]
fn fleet_migrate_is_root_only_and_idempotent() {
    let root_tmp = tempfile::tempdir().unwrap();
    let registry_dir = root_tmp.path().join("file-registry");
    let registry_url = format!("file:{}", registry_dir.display());

    cmd(root_tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    fs::write(
        root_tmp.path().join("mesh/fleet/registry.env"),
        format!("{registry_url}\n"),
    )
    .unwrap();

    for _ in 0..2 {
        let migrate = cmd(root_tmp.path())
            .args(["--json", "mesh", "fleet", "migrate"])
            .assert()
            .success();
        let out = json_out(&migrate);
        assert_eq!(out["ok"], true, "{out}");
        assert_eq!(out["command"], "mesh fleet migrate");
        assert_eq!(
            out["data"]["applied"],
            serde_json::json!([
                "mesh_l2_0001.sql",
                "mesh_l3_0001.sql",
                "mesh_l4_0001.sql"
            ])
        );
    }
    assert!(registry_dir.join("mesh_gateways.json").exists());
    assert!(registry_dir.join("mesh_work_items.json").exists());

    let join_tmp = tempfile::tempdir().unwrap();
    joiner_home_on_fleet(root_tmp.path(), join_tmp.path());
    let migrate = cmd(join_tmp.path())
        .args(["--json", "mesh", "fleet", "migrate"])
        .assert()
        .failure();
    let out = json_out(&migrate);
    assert_eq!(out["error"]["code"], "FLEET_ALREADY_JOINED", "{out}");
    assert!(!join_tmp.path().join("mesh/fleet/root.key").exists());
}

#[test]
fn fleet_migrate_prints_each_migrations_sql_before_applying_it() {
    let root_tmp = tempfile::tempdir().unwrap();
    let registry_dir = root_tmp.path().join("file-registry");
    let registry_url = format!("file:{}", registry_dir.display());

    cmd(root_tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    fs::write(
        root_tmp.path().join("mesh/fleet/registry.env"),
        format!("{registry_url}\n"),
    )
    .unwrap();

    let migrate = cmd(root_tmp.path())
        .args(["--json", "mesh", "fleet", "migrate"])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&migrate.get_output().stderr);
    assert!(stderr.contains("mesh_l4_0001.sql"), "{stderr}");
    assert!(
        stderr.contains("ADD COLUMN IF NOT EXISTS row_msg"),
        "{stderr}"
    );
}

#[test]
fn fleet_migrate_uses_embedded_sql_not_the_source_tree() {
    let root_tmp = tempfile::tempdir().unwrap();
    let run_tmp = tempfile::tempdir().unwrap();
    let registry_dir = root_tmp.path().join("file-registry");
    let registry_url = format!("file:{}", registry_dir.display());

    cmd(root_tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    fs::write(
        root_tmp.path().join("mesh/fleet/registry.env"),
        format!("{registry_url}\n"),
    )
    .unwrap();

    let migrate = cmd(root_tmp.path())
        .current_dir(run_tmp.path())
        .env_remove("CARGO_MANIFEST_DIR")
        .args(["--json", "mesh", "fleet", "migrate"])
        .assert()
        .success();
    let out = json_out(&migrate);
    assert_eq!(out["ok"], true, "{out}");
    assert_eq!(
        out["data"]["applied"],
        serde_json::json!([
            "mesh_l2_0001.sql",
            "mesh_l3_0001.sql",
            "mesh_l4_0001.sql"
        ])
    );
}
