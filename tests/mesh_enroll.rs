use a2a::home::A2aHome;
use a2a::mesh::enroll_ledger::{EnrollLedger, LedgerEntry};
use a2a::mesh::keys::{self, RootSet, SystemClock, TokenClaims};
use a2a::mesh::registry::{
    write_registry_env, FileRegistry, GatewayRow, Registry,
};
use a2a::mesh::row_mark::RowMark;
use a2a::mesh::row_sig::{
    gateway_id_binds_key, sign_row, verify_row, RowMsg, RowVerdict, RowVerifyCfg,
    StoredRowColumns, ROW_MSG_VER,
};
use assert_cmd::Command;
use chrono::Utc;
use ed25519_dalek::SigningKey;
use nix::fcntl::{Flock, FlockArg};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
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

#[test]
fn new_gateway_ids_commit_to_at_least_16_hex_of_the_key() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let identity = keys::gateway_identity(&home, false).unwrap();
    let fingerprint = keys::gateway_public_key_fingerprint(&home).unwrap();
    let suffix = identity.gateway_id.rsplit('-').next().unwrap();

    assert!(suffix.len() >= 16, "{}", identity.gateway_id);
    assert_eq!(suffix, &fingerprint[..suffix.len()]);
}

#[test]
fn an_existing_six_hex_id_still_verifies_against_the_same_derivation_prefix() {
    let pubkey_b64 = pubkey_b64(7);
    let fingerprint = keys::fpr_of_pubkey_b64(&pubkey_b64).unwrap();
    let claims = TokenClaims {
        fleet_id: "fleet-test".to_string(),
        gateway_id: format!("gw-old-{}", &fingerprint[..6]),
        pubkey_b64,
        key_epoch: 0,
        issued_at: 1_700_000_000,
        exp: 1_800_000_000,
    };
    assert!(gateway_id_binds_key(&claims.gateway_id, &claims));
}

#[test]
fn enroll_refuses_a_gateway_id_bound_to_a_different_pubkey_in_the_ledger_without_rebind() {
    let tmp = tempfile::tempdir().unwrap();
    cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    let original = pubkey_b64(11);
    let replacement = pubkey_b64(12);
    let original_fpr = keys::fpr_of_pubkey_b64(&original).unwrap();
    let replacement_fpr = keys::fpr_of_pubkey_b64(&replacement).unwrap();
    let gateway_id = format!("gw-ledger-{}", &original_fpr[..16]);
    cmd(tmp.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            &gateway_id,
            "--pubkey",
            &original,
        ])
        .assert()
        .success();
    assert_eq!(
        mode(&tmp.path().join("mesh/fleet/enrollments.json")),
        0o600
    );

    let refused = cmd(tmp.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            &gateway_id,
            "--pubkey",
            &replacement,
        ])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "ENROLLMENT_REBIND_REQUIRED");
    let rendered = String::from_utf8_lossy(&refused.get_output().stdout);
    assert!(rendered.contains(&original_fpr), "{rendered}");
    assert!(rendered.contains(&replacement_fpr), "{rendered}");
}

#[test]
fn enroll_refuses_a_gateway_id_bound_to_a_different_pubkey_in_the_live_row() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let fleet = keys::fleet_init(&home).unwrap();
    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    write_registry_env(&home, &format!("file:{}", registry_path.display())).unwrap();
    let original = pubkey_b64(13);
    let replacement = pubkey_b64(14);
    let original_fpr = keys::fpr_of_pubkey_b64(&original).unwrap();
    let replacement_fpr = keys::fpr_of_pubkey_b64(&replacement).unwrap();
    let gateway_id = format!("gw-live-{}", &original_fpr[..16]);
    let now = Utc::now();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(registry.upsert_self(GatewayRow {
            fleet_id: fleet.fleet_id,
            gateway_id: gateway_id.clone(),
            owner: "owner".to_string(),
            endpoint_url: None,
            pubkey_fpr: original_fpr.clone(),
            cert_fpr: None,
            version: "0.3.0".to_string(),
            created_at: now,
            last_heartbeat: now,
            revoked_at: None,
            schema_version: 1,
            row_msg: None,
            row_token: None,
            row_sig: None,
            signed_at: None,
            revocation: None,
        }))
        .unwrap();

    let refused = cmd(tmp.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            &gateway_id,
            "--pubkey",
            &replacement,
        ])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "ENROLLMENT_REBIND_REQUIRED");
    let rendered = String::from_utf8_lossy(&refused.get_output().stdout);
    assert!(rendered.contains(&original_fpr), "{rendered}");
    assert!(rendered.contains(&replacement_fpr), "{rendered}");
}

#[test]
fn enroll_rebind_increments_the_epoch_past_both_sources_and_clears_the_legacy_mirror() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    keys::fleet_init(&home).unwrap();
    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    write_registry_env(&home, &format!("file:{}", registry_path.display())).unwrap();
    let old_pubkey = pubkey_b64(21);
    let new_pubkey = pubkey_b64(22);
    let old_fpr = keys::fpr_of_pubkey_b64(&old_pubkey).unwrap();
    let gateway_id = format!("gw-rebind-{}", &old_fpr[..16]);
    let (row_token, _claims, issuer) = keys::enroll_gateway_token_at_epoch(
        &home,
        &gateway_id,
        &old_pubkey,
        4,
        &SystemClock,
    )
    .unwrap();
    let now = Utc::now();
    let revocation = keys::mint_revocation(&home, &gateway_id, 4, &SystemClock).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(registry.upsert_self(GatewayRow {
            fleet_id: keys::fleet_id(&home).unwrap(),
            gateway_id: gateway_id.clone(),
            owner: "owner".to_string(),
            endpoint_url: None,
            pubkey_fpr: old_fpr,
            cert_fpr: None,
            version: "0.3.0".to_string(),
            created_at: now,
            last_heartbeat: now,
            revoked_at: Some(now),
            schema_version: 1,
            row_msg: None,
            row_token: Some(row_token),
            row_sig: None,
            signed_at: None,
            revocation: Some(revocation.clone()),
        }))
        .unwrap();
    let mut ledger = EnrollLedger::load(&home).unwrap();
    ledger
        .record(
            &home,
            LedgerEntry {
                gateway_id: gateway_id.clone(),
                pubkey_b64: old_pubkey,
                key_epoch: 6,
                issuing_root_fpr: issuer,
                issued_at: now.timestamp(),
                exp: (now + chrono::Duration::days(90)).timestamp(),
            },
        )
        .unwrap();

    let enrolled = cmd(tmp.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            &gateway_id,
            "--pubkey",
            &new_pubkey,
            "--rebind",
        ])
        .assert()
        .success();
    let out = json_out(&enrolled);
    let token = out["data"]["token"].as_str().unwrap();
    let claims = keys::verify_token_no_clock(token, &RootSet::load(&home).unwrap())
        .unwrap()
        .0;
    assert_eq!(claims.key_epoch, 7);
    let ledger = EnrollLedger::load(&home).unwrap();
    assert_eq!(ledger.latest_for(&gateway_id).unwrap().key_epoch, 7);
    assert_eq!(ledger.latest_for(&gateway_id).unwrap().pubkey_b64, new_pubkey);
    let rows = runtime
        .block_on(registry.discover_unverified(&keys::fleet_id(&home).unwrap()))
        .unwrap();
    assert_eq!(rows[0].revoked_at, None);
    assert_eq!(rows[0].revocation.as_deref(), Some(revocation.as_str()));
}

#[test]
fn a_pre_l4_token_with_no_key_epoch_verifies_as_epoch_zero_and_is_superseded_by_epoch_one() {
    let tmp = tempfile::tempdir().unwrap();
    let root = A2aHome::resolve(Some(&tmp.path().join("root"))).unwrap();
    let member = A2aHome::resolve(Some(&tmp.path().join("member"))).unwrap();
    keys::fleet_init(&root).unwrap();
    let identity = keys::gateway_identity(&member, false).unwrap();
    let pubkey = keys::gateway_public_key_b64(&member).unwrap();
    let token = keys::enroll_gateway_token(
        &root,
        &identity.gateway_id,
        &pubkey,
        &SystemClock,
    )
    .unwrap();
    let roots = RootSet::load(&root).unwrap();
    let claims = keys::verify_token_no_clock(&token, &roots).unwrap().0;
    assert_eq!(claims.key_epoch, 0);
    let now = Utc::now();
    let msg = RowMsg {
        msg_ver: ROW_MSG_VER,
        fleet_id: claims.fleet_id.clone(),
        gateway_id: claims.gateway_id.clone(),
        endpoint_url: None,
        pubkey_fpr: keys::fpr_of_pubkey_b64(&claims.pubkey_b64).unwrap(),
        cert_fpr: None,
        version: "0.3.0".to_string(),
        owner: "owner".to_string(),
        created_at: now,
        schema_version: 1,
        key_epoch: 0,
        roots_held: "[]".to_string(),
        row_token_sha256: sha256_hex(token.as_bytes()),
        row_resign_secs: 86_400,
        registry_heartbeat_secs: 180,
        enforce_require_signed_rows: false,
        enforce_auto_push: false,
        signed_at: now,
    };
    let (row_msg, row_sig) = sign_row(&msg, &keys::gateway_signing_key(&member).unwrap()).unwrap();
    let stored = StoredRowColumns {
        fleet_id: msg.fleet_id.clone(),
        gateway_id: msg.gateway_id.clone(),
        endpoint_url: None,
        pubkey_fpr: msg.pubkey_fpr.clone(),
        cert_fpr: None,
        version: msg.version.clone(),
        owner: msg.owner.clone(),
        created_at: msg.created_at,
        schema_version: 1,
        row_msg: Some(row_msg),
        row_token: Some(token),
        row_sig: Some(row_sig),
        signed_at: Some(now),
        revocation: None,
    };
    let mark = RowMark {
        high_water: None,
        msg_digest: None,
        first_signature: true,
        pinned_pubkey_b64: Some(claims.pubkey_b64),
        highest_key_epoch: 1,
    };
    assert_eq!(
        verify_row(&stored, &roots, Some(&mark), &RowVerifyCfg::new(now)),
        RowVerdict::RowKeySuperseded
    );
}

#[test]
fn fleet_ls_warns_at_t_minus_thirty_fourteen_and_seven() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let fleet = keys::fleet_init(&home).unwrap();
    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    write_registry_env(&home, &format!("file:{}", registry_path.display())).unwrap();
    let pubkey = pubkey_b64(31);
    let fingerprint = keys::fpr_of_pubkey_b64(&pubkey).unwrap();
    let gateway_id = format!("gw-expiry-{}", &fingerprint[..16]);
    let now = Utc::now();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    for (days, warning) in [(30_i64, "T-30"), (14, "T-14"), (7, "T-7")] {
        let claims = TokenClaims {
            fleet_id: fleet.fleet_id.clone(),
            gateway_id: gateway_id.clone(),
            pubkey_b64: pubkey.clone(),
            key_epoch: 0,
            issued_at: (now - chrono::Duration::days(1)).timestamp(),
            exp: (now + chrono::Duration::days(days)).timestamp(),
        };
        let token = keys::sign_token(&claims, &home).unwrap();
        runtime
            .block_on(registry.upsert_self(GatewayRow {
                fleet_id: fleet.fleet_id.clone(),
                gateway_id: gateway_id.clone(),
                owner: "owner".to_string(),
                endpoint_url: None,
                pubkey_fpr: fingerprint.clone(),
                cert_fpr: None,
                version: "0.3.0".to_string(),
                created_at: now,
                last_heartbeat: now,
                revoked_at: None,
                schema_version: 1,
                row_msg: None,
                row_token: Some(token),
                row_sig: None,
                signed_at: None,
                revocation: None,
            }))
            .unwrap();
        let listed = cmd(tmp.path())
            .args(["--json", "mesh", "fleet", "ls"])
            .assert()
            .success();
        let out = json_out(&listed);
        let row = &out["data"]["rows"][0];
        assert_eq!(row["token_expiry_warning"], warning, "{out}");
        assert_eq!(row["days_remaining"], days, "{out}");
        assert!(row["token_expires_at"].is_string(), "{out}");
        assert!(
            row["flags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|flag| flag == warning),
            "{out}"
        );
    }
}

#[test]
fn gateway_status_and_health_surface_token_expiry_and_days_remaining() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let fleet = keys::fleet_init(&home).unwrap();
    let identity = keys::gateway_identity(&home, false).unwrap();
    let pubkey_b64 = keys::gateway_public_key_b64(&home).unwrap();
    let now = Utc::now();
    let claims = TokenClaims {
        fleet_id: fleet.fleet_id,
        gateway_id: identity.gateway_id,
        pubkey_b64,
        key_epoch: 0,
        issued_at: (now - chrono::Duration::days(1)).timestamp(),
        exp: (now + chrono::Duration::days(14)).timestamp(),
    };
    let token = keys::sign_token(&claims, &home).unwrap();
    keys::install_gateway_token(&home, &token, &SystemClock).unwrap();

    let mesh = home.root().join("mesh");
    fs::create_dir_all(&mesh).unwrap();
    let lock_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(mesh.join("gateway.lock"))
        .unwrap();
    let _lock = Flock::lock(lock_file, FlockArg::LockExclusive).unwrap();
    fs::write(
        mesh.join("gateway-health.json"),
        r#"{
          "schema_version": 2,
          "pid": 4242,
          "health_seq": 7,
          "written_at": "2026-09-07T00:00:00Z",
          "state": "running",
          "listen_port": 7787,
          "expose": "none",
          "endpoint_url": null,
          "exposure_error": null,
          "exposure_error_since": null,
          "registry_error": null,
          "registry_error_since": null,
          "loops": {},
          "replication": {"per_origin_lag": {}, "last_anti_entropy": null},
          "links": [],
          "escalation": null,
          "shutdown": null
        }"#,
    )
    .unwrap();

    let status = cmd(tmp.path())
        .args(["--json", "mesh", "gateway", "status"])
        .assert()
        .success();
    let out = json_out(&status);
    let health = &out["data"]["health"];
    assert!(health["token_expires_at"].is_string(), "{out}");
    assert_eq!(health["days_remaining"], 14, "{out}");
    assert_eq!(health["token_expiry_warning"], "T-14", "{out}");

    let human = cmd(tmp.path())
        .args(["mesh", "gateway", "status"])
        .assert()
        .success();
    let rendered = String::from_utf8_lossy(&human.get_output().stdout);
    assert!(rendered.contains("token_expires_at"), "{rendered}");
    assert!(rendered.contains("days_remaining 14"), "{rendered}");
    assert!(rendered.contains("warning T-14"), "{rendered}");
}

fn pubkey_b64(seed: u8) -> String {
    b64(&SigningKey::from_bytes(&[seed; 32]).verifying_key().to_bytes())
}

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn b64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 3) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(((b1 & 15) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(b2 & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}
