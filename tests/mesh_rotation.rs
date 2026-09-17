use a2a::home::A2aHome;
use a2a::mesh::keys::{self, RootSet, SystemClock};
use a2a::mesh::registry::{write_registry_env, FileRegistry, GatewayRow, Registry};
use a2a::mesh::rotation::{
    phase, soak_state, HandshakeMark, MemberEvidence, RotationEvidence, RotationPhase,
    SoakState,
};
use a2a::mesh::row_sig::{sign_row, RowMsg, ROW_MSG_VER};
use assert_cmd::Command;
use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

fn cmd(home: &Path) -> Command {
    let mut command = Command::cargo_bin("a2a").unwrap();
    command.arg("--home").arg(home).env_remove("A2A_HOME");
    command
}

fn json_out(assert: &assert_cmd::assert::Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).unwrap()
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn sha256(path: &Path) -> Vec<u8> {
    Sha256::digest(fs::read(path).unwrap()).to_vec()
}

fn init_owner_with_token(path: &Path) {
    cmd(path)
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    cmd(path)
        .args(["--json", "mesh", "gateway", "identity"])
        .assert()
        .success();
    let home = A2aHome::resolve(Some(path)).unwrap();
    keys::ensure_gateway_token(&home, &SystemClock).unwrap();
}

fn pubkey_b64(seed: u8) -> String {
    b64(&SigningKey::from_bytes(&[seed; 32]).verifying_key().to_bytes())
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

fn staged_owner_and_enrolled_joiner() -> (tempfile::TempDir, tempfile::TempDir, Value) {
    let owner = tempfile::tempdir().unwrap();
    let joiner = tempfile::tempdir().unwrap();
    cmd(owner.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    let identity = cmd(joiner.path())
        .args(["--json", "mesh", "gateway", "identity"])
        .assert()
        .success();
    let identity = json_out(&identity);
    fs::create_dir_all(joiner.path().join("mesh/fleet")).unwrap();
    fs::copy(
        owner.path().join("mesh/fleet/root.pub"),
        joiner.path().join("mesh/fleet/root.pub"),
    )
    .unwrap();
    let enrolled = cmd(owner.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            identity["data"]["gateway_id"].as_str().unwrap(),
            "--pubkey",
            identity["data"]["pubkey_b64"].as_str().unwrap(),
        ])
        .assert()
        .success();
    let enrolled = json_out(&enrolled);
    cmd(joiner.path())
        .args([
            "--json",
            "mesh",
            "gateway",
            "install-token",
            "--file",
            "-",
        ])
        .write_stdin(enrolled["data"]["token"].as_str().unwrap())
        .assert()
        .success();
    let staged = cmd(owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--stage"])
        .assert()
        .success();
    (owner, joiner, json_out(&staged))
}

fn owner_with_signed_member(sign_with_next: bool) -> (tempfile::TempDir, String) {
    let owner = tempfile::tempdir().unwrap();
    let member = tempfile::tempdir().unwrap();
    let owner_home = A2aHome::resolve(Some(owner.path())).unwrap();
    let member_home = A2aHome::resolve(Some(member.path())).unwrap();
    keys::fleet_init(&owner_home).unwrap();
    keys::stage_root(&owner_home, &SystemClock, false).unwrap();
    let identity = keys::gateway_identity(&member_home, false).unwrap();
    let pubkey = keys::gateway_public_key_b64(&member_home).unwrap();
    let mut enroll = cmd(owner.path());
    enroll.args([
        "--json",
        "mesh",
        "fleet",
        "enroll",
        "--gateway-id",
        &identity.gateway_id,
        "--pubkey",
        &pubkey,
    ]);
    if sign_with_next {
        enroll.args(["--sign-with", "next"]);
    }
    let enrolled = enroll.assert().success();
    let enrolled = json_out(&enrolled);
    let token = enrolled["data"]["token"].as_str().unwrap().to_string();

    let registry_path = owner.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    write_registry_env(
        &owner_home,
        &format!("file:{}", registry_path.display()),
    )
    .unwrap();
    let roots = RootSet::load(&owner_home).unwrap();
    let claims = keys::verify_token_no_clock(&token, &roots).unwrap().0;
    let now = Utc::now();
    let row_message = RowMsg {
        msg_ver: ROW_MSG_VER,
        fleet_id: claims.fleet_id.clone(),
        gateway_id: claims.gateway_id.clone(),
        endpoint_url: None,
        pubkey_fpr: keys::fpr_of_pubkey_b64(&claims.pubkey_b64).unwrap(),
        cert_fpr: Some(identity.cert_fpr),
        version: env!("CARGO_PKG_VERSION").to_string(),
        owner: "rotation-test".to_string(),
        created_at: now,
        schema_version: 1,
        key_epoch: claims.key_epoch,
        roots_held: serde_json::to_string(&roots.fingerprints()).unwrap(),
        row_token_sha256: hex_sha256(token.as_bytes()),
        row_resign_secs: 86_400,
        registry_heartbeat_secs: 180,
        enforce_require_signed_rows: true,
        enforce_auto_push: false,
        signed_at: now,
    };
    let (row_msg, row_sig) =
        sign_row(&row_message, &keys::gateway_signing_key(&member_home).unwrap()).unwrap();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(registry.upsert_self(GatewayRow {
            fleet_id: row_message.fleet_id,
            gateway_id: row_message.gateway_id.clone(),
            owner: row_message.owner,
            endpoint_url: None,
            pubkey_fpr: row_message.pubkey_fpr,
            cert_fpr: row_message.cert_fpr,
            version: row_message.version,
            created_at: now,
            last_heartbeat: now,
            revoked_at: None,
            schema_version: 1,
            row_msg: Some(row_msg),
            row_token: Some(token),
            row_sig: Some(row_sig),
            signed_at: Some(now),
            revocation: None,
        }))
        .unwrap();
    (owner, identity.gateway_id)
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct RotationStatusFixture {
    owner: tempfile::TempDir,
    registry_path: PathBuf,
    current_fpr: String,
    next_fpr: String,
    members: Vec<String>,
}

fn rotation_status_fixture(sign_with_next: &[bool]) -> RotationStatusFixture {
    let owner = tempfile::tempdir().unwrap();
    let owner_home = A2aHome::resolve(Some(owner.path())).unwrap();
    keys::fleet_init(&owner_home).unwrap();
    keys::stage_root(&owner_home, &SystemClock, false).unwrap();
    let roots = RootSet::load(&owner_home).unwrap();
    let current_fpr = roots
        .entries()
        .iter()
        .find(|entry| entry.role == keys::RootRole::Current)
        .unwrap()
        .fpr
        .clone();
    let next_fpr = roots
        .entries()
        .iter()
        .find(|entry| entry.role == keys::RootRole::Next)
        .unwrap()
        .fpr
        .clone();
    let registry_path = owner.path().join("rotation-registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    write_registry_env(
        &owner_home,
        &format!("file:{}", registry_path.display()),
    )
    .unwrap();

    let mut members = Vec::new();
    for (index, use_next) in sign_with_next.iter().copied().enumerate() {
        let member = tempfile::tempdir().unwrap();
        let member_home = A2aHome::resolve(Some(member.path())).unwrap();
        let identity = keys::gateway_identity(&member_home, false).unwrap();
        let pubkey = keys::gateway_public_key_b64(&member_home).unwrap();
        let mut enroll = cmd(owner.path());
        enroll.args([
            "--json",
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            &identity.gateway_id,
            "--pubkey",
            &pubkey,
        ]);
        if use_next {
            enroll.args(["--sign-with", "next"]);
        }
        let enrolled = json_out(&enroll.assert().success());
        let token = enrolled["data"]["token"].as_str().unwrap().to_string();
        let claims = keys::verify_token_no_clock(&token, &roots).unwrap().0;
        let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
        let row_message = RowMsg {
            msg_ver: ROW_MSG_VER,
            fleet_id: claims.fleet_id.clone(),
            gateway_id: claims.gateway_id.clone(),
            endpoint_url: None,
            pubkey_fpr: keys::fpr_of_pubkey_b64(&claims.pubkey_b64).unwrap(),
            cert_fpr: Some(identity.cert_fpr),
            version: format!("rotation-status-test-{index}"),
            owner: "rotation-status-test".to_string(),
            created_at: now,
            schema_version: 1,
            key_epoch: claims.key_epoch,
            roots_held: serde_json::to_string(&roots.fingerprints()).unwrap(),
            row_token_sha256: hex_sha256(token.as_bytes()),
            row_resign_secs: 86_400,
            registry_heartbeat_secs: 180,
            enforce_require_signed_rows: true,
            enforce_auto_push: false,
            signed_at: now,
        };
        let (row_msg, row_sig) =
            sign_row(&row_message, &keys::gateway_signing_key(&member_home).unwrap()).unwrap();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(registry.upsert_self(GatewayRow {
                fleet_id: row_message.fleet_id,
                gateway_id: row_message.gateway_id.clone(),
                owner: row_message.owner,
                endpoint_url: None,
                pubkey_fpr: row_message.pubkey_fpr,
                cert_fpr: row_message.cert_fpr,
                version: row_message.version,
                created_at: now,
                last_heartbeat: now,
                revoked_at: None,
                schema_version: 1,
                row_msg: Some(row_msg),
                row_token: Some(token),
                row_sig: Some(row_sig),
                signed_at: Some(now),
                revocation: None,
            }))
            .unwrap();
        members.push(identity.gateway_id);
    }

    RotationStatusFixture {
        owner,
        registry_path,
        current_fpr,
        next_fpr,
        members,
    }
}

fn write_gateway_health_links(home: &Path, links: &[(&str, DateTime<Utc>)]) {
    let links = links
        .iter()
        .map(|(peer, established_at)| {
            serde_json::json!({
                "peer": peer,
                "direction": "outbound",
                "established_at": established_at,
                "last_rx": established_at,
                "last_tx": established_at,
                "frames_rx": 1,
                "frames_tx": 1,
            })
        })
        .collect::<Vec<_>>();
    let health = serde_json::json!({
        "schema_version": 2,
        "pid": 4242,
        "health_seq": 1,
        "written_at": Utc::now(),
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
        "links": links,
        "escalation": null,
        "shutdown": null,
    });
    fs::create_dir_all(home.join("mesh")).unwrap();
    fs::write(
        home.join("mesh/gateway-health.json"),
        serde_json::to_vec_pretty(&health).unwrap(),
    )
    .unwrap();
}

#[test]
fn rotation_status_names_each_members_verified_issuer_and_the_phase() {
    let fixture = rotation_status_fixture(&[false, true]);
    let status = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotation-status"])
        .assert()
        .success();
    let out = json_out(&status);
    assert_eq!(out["data"]["phase"], "phase1", "{out}");
    assert_eq!(out["data"]["staged_fpr"], fixture.next_fpr, "{out}");
    let members = out["data"]["members"].as_array().unwrap();
    let current = members
        .iter()
        .find(|member| member["gateway_id"] == fixture.members[0])
        .unwrap();
    let next = members
        .iter()
        .find(|member| member["gateway_id"] == fixture.members[1])
        .unwrap();
    assert_eq!(current["verified_issuer_fpr"], fixture.current_fpr);
    assert_eq!(current["issuer_role"], "current");
    assert_eq!(next["verified_issuer_fpr"], fixture.next_fpr);
    assert_eq!(next["issuer_role"], "next");
    assert!(current["roots_held_advisory"].is_array(), "{out}");
    assert!(current["token_expires_at"].is_string(), "{out}");
    assert!(current["days_remaining"].is_i64(), "{out}");

    let human = cmd(fixture.owner.path())
        .args(["mesh", "fleet", "rotation-status"])
        .assert()
        .success();
    let human = String::from_utf8_lossy(&human.get_output().stdout);
    assert!(human.contains(&fixture.members[0]), "{human}");
    assert!(
        human.contains(&format!("issuer={}", &fixture.current_fpr[..12])),
        "{human}"
    );
    assert!(human.contains("role=current"), "{human}");
    assert!(human.contains("phase=phase1"), "{human}");

    let rows_path = fixture.registry_path.join("mesh_gateways.json");
    let mut rows: Vec<GatewayRow> =
        serde_json::from_slice(&fs::read(&rows_path).unwrap()).unwrap();
    rows.iter_mut()
        .find(|row| row.gateway_id == fixture.members[1])
        .unwrap()
        .row_sig = None;
    fs::write(&rows_path, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();

    let unsigned = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotation-status"])
        .assert()
        .success();
    let unsigned = json_out(&unsigned);
    let member = unsigned["data"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|member| member["gateway_id"] == fixture.members[1])
        .unwrap();
    assert_eq!(member["roots_held_advisory"], serde_json::json!([]));
    assert!(member["verified_issuer_fpr"].is_null(), "{unsigned}");
}

#[test]
fn rotation_status_requires_both_the_owner_ledger_and_registry() {
    let owner = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(owner.path())).unwrap();
    keys::fleet_init(&home).unwrap();

    let refused = cmd(owner.path())
        .args(["--json", "mesh", "fleet", "rotation-status"])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(
        out["error"]["code"],
        "ROTATION_STATUS_UNAVAILABLE",
        "{out}"
    );
}

#[test]
fn rotation_status_refuses_all_green_when_a_ledgered_member_has_no_row() {
    let fixture = rotation_status_fixture(&[true, true]);
    let rows_path = fixture.registry_path.join("mesh_gateways.json");
    let mut rows: Vec<GatewayRow> =
        serde_json::from_slice(&fs::read(&rows_path).unwrap()).unwrap();
    rows.retain(|row| row.gateway_id != fixture.members[1]);
    fs::write(&rows_path, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();

    let status = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotation-status"])
        .assert()
        .success();
    let out = json_out(&status);
    assert_eq!(out["data"]["phase"], "unknown", "{out}");
    assert_eq!(
        out["data"]["unaccounted"],
        serde_json::json!([fixture.members[1]])
    );
    assert_eq!(out["data"]["members"].as_array().unwrap().len(), 2);
}

#[test]
fn soak_is_row_attested_only_after_every_new_root_member_links_after_promote() {
    let fixture = rotation_status_fixture(&[true, true]);
    let promoted = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--promote"])
        .assert()
        .success();
    let promoted = json_out(&promoted);
    let state_path = fixture.owner.path().join("mesh/fleet/rotation.json");
    let mut state: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    let promoted_at = state["promoted_at"].as_i64().unwrap();
    let promoted_at_exactly = DateTime::from_timestamp(promoted_at, 0).unwrap();
    write_gateway_health_links(
        fixture.owner.path(),
        &[(fixture.members[0].as_str(), promoted_at_exactly)],
    );
    let equality_is_not_after = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotation-status"])
        .assert()
        .success();
    let equality_is_not_after = json_out(&equality_is_not_after);
    let mut all_members_missing = fixture.members.clone();
    all_members_missing.sort();
    assert_eq!(
        equality_is_not_after["data"]["soak"]["missing"],
        serde_json::json!(all_members_missing),
        "{equality_is_not_after}"
    );

    let linked_at = DateTime::from_timestamp(promoted_at + 1, 0).unwrap();
    write_gateway_health_links(
        fixture.owner.path(),
        &[(fixture.members[0].as_str(), linked_at)],
    );

    let one_missing = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotation-status"])
        .assert()
        .success();
    let one_missing = json_out(&one_missing);
    assert_eq!(one_missing["data"]["soak"]["state"], "pending");
    assert_eq!(
        one_missing["data"]["soak"]["missing"],
        serde_json::json!([fixture.members[1].clone()])
    );

    write_gateway_health_links(
        fixture.owner.path(),
        &[
            (fixture.members[0].as_str(), linked_at),
            (fixture.members[1].as_str(), linked_at),
        ],
    );
    let window_pending = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotation-status"])
        .assert()
        .success();
    let window_pending = json_out(&window_pending);
    assert_eq!(window_pending["data"]["soak"]["state"], "pending");
    let remaining = window_pending["data"]["soak"]["seconds_remaining"]
        .as_i64()
        .unwrap();
    assert!((3_590..=3_600).contains(&remaining), "{window_pending}");

    let refused = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--retire"])
        .assert()
        .failure();
    assert_eq!(json_out(&refused)["error"]["code"], "ROTATION_SOAK_UNMET");

    state["promoted_at"] = serde_json::json!(Utc::now().timestamp() - 7_200);
    fs::write(&state_path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
    let satisfied = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotation-status"])
        .assert()
        .success();
    let satisfied = json_out(&satisfied);
    assert_eq!(satisfied["data"]["soak"]["state"], "satisfied");
    assert_eq!(
        satisfied["data"]["members"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|member| member["soak"]["state"] == "satisfied")
            .count(),
        2,
        "{satisfied}"
    );
    assert_eq!(
        promoted["data"]["promoted_to_fpr"],
        satisfied["data"]["current_fpr"]
    );
    cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--retire"])
        .assert()
        .success();
}

#[test]
fn set_registry_verifies_before_it_writes_and_prints_the_fleet_state() {
    let owner = tempfile::tempdir().unwrap();
    let owner_home = A2aHome::resolve(Some(owner.path())).unwrap();
    keys::fleet_init(&owner_home).unwrap();
    let old_registry = owner.path().join("old-registry");
    FileRegistry::new(&old_registry).init_schema().unwrap();
    write_registry_env(
        &owner_home,
        &format!("file:{}", old_registry.display()),
    )
    .unwrap();
    let new_registry = owner.path().join("new-registry");
    FileRegistry::new(&new_registry).init_schema().unwrap();
    let new_url = format!("file:{}", new_registry.display());

    let changed = cmd(owner.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "set-registry",
            "--url",
            &new_url,
            "--allow-empty",
        ])
        .assert()
        .success();
    let out = json_out(&changed);
    assert!(out["data"]["rows"].is_array(), "{out}");
    let env_path = owner.path().join("mesh/fleet/registry.env");
    assert_eq!(fs::read_to_string(&env_path).unwrap(), format!("{new_url}\n"));
    assert_eq!(mode(&env_path), 0o600);
}

#[test]
fn set_registry_leaves_the_old_credential_intact_when_the_new_one_is_unreachable() {
    let owner = tempfile::tempdir().unwrap();
    let owner_home = A2aHome::resolve(Some(owner.path())).unwrap();
    keys::fleet_init(&owner_home).unwrap();
    let old_url = "file:/old/credential/kept-byte-identical";
    write_registry_env(&owner_home, old_url).unwrap();
    let env_path = owner.path().join("mesh/fleet/registry.env");
    let before = fs::read(&env_path).unwrap();

    let refused = cmd(owner.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "set-registry",
            "--url",
            "postgres://nobody@127.0.0.1:1/none",
        ])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "REGISTRY_UNREACHABLE", "{out}");
    assert_eq!(fs::read(env_path).unwrap(), before);
}

#[test]
fn set_registry_refuses_to_rehome_a_machine_to_a_different_fleet() {
    let owner = tempfile::tempdir().unwrap();
    let owner_home = A2aHome::resolve(Some(owner.path())).unwrap();
    keys::fleet_init(&owner_home).unwrap();
    write_registry_env(&owner_home, "file:/old/fleet/credential").unwrap();
    let env_path = owner.path().join("mesh/fleet/registry.env");
    let before = fs::read(&env_path).unwrap();

    let foreign = tempfile::tempdir().unwrap();
    let foreign_home = A2aHome::resolve(Some(foreign.path())).unwrap();
    let foreign_fleet = keys::fleet_init(&foreign_home).unwrap().fleet_id;
    let registry_path = owner.path().join("foreign-registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(registry.upsert_self(GatewayRow {
            fleet_id: foreign_fleet,
            gateway_id: "gw-foreign".to_string(),
            owner: "foreign".to_string(),
            endpoint_url: None,
            pubkey_fpr: "f".repeat(64),
            cert_fpr: None,
            version: "test".to_string(),
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

    let refused = cmd(owner.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "set-registry",
            "--url",
            &format!("file:{}", registry_path.display()),
        ])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "REGISTRY_SET_REFUSED", "{out}");
    assert_eq!(fs::read(env_path).unwrap(), before);
}

#[test]
fn set_registry_refuses_an_empty_registry_unless_allow_empty_is_given() {
    // a2a-9mn: a reachable, correctly-schema'd registry that holds no fleet at
    // all is a fat-finger risk (a wrong-but-empty migrated DB) -- the
    // wrong-fleet refusal only fires on conflicting rows. Refuse by default;
    // require --allow-empty to intentionally adopt a fresh registry.
    let owner = tempfile::tempdir().unwrap();
    let owner_home = A2aHome::resolve(Some(owner.path())).unwrap();
    keys::fleet_init(&owner_home).unwrap();
    write_registry_env(&owner_home, "file:/old/fleet/credential").unwrap();
    let env_path = owner.path().join("mesh/fleet/registry.env");
    let before = fs::read(&env_path).unwrap();

    let empty = owner.path().join("empty-registry");
    FileRegistry::new(&empty).init_schema().unwrap();
    let empty_url = format!("file:{}", empty.display());

    let refused = cmd(owner.path())
        .args(["--json", "mesh", "fleet", "set-registry", "--url", &empty_url])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "REGISTRY_SET_EMPTY", "{out}");
    assert_eq!(fs::read(&env_path).unwrap(), before, "old credential kept");

    // The explicit override adopts the fresh registry.
    cmd(owner.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "set-registry",
            "--url",
            &empty_url,
            "--allow-empty",
        ])
        .assert()
        .success();
    assert_eq!(fs::read_to_string(&env_path).unwrap(), format!("{empty_url}\n"));
}

#[test]
fn handshake_marks_are_rewritten_only_for_new_issuer_or_after_sixty_seconds() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let first = HandshakeMark {
        issuer_fpr: "a".repeat(64),
        at: 1_700_000_000,
    };
    a2a::mesh::rotation::RotationState::record_handshake(&home, "gw-a", first.clone()).unwrap();
    let path = tmp.path().join("mesh/fleet/rotation.json");
    let first_bytes = fs::read(&path).unwrap();

    a2a::mesh::rotation::RotationState::record_handshake(
        &home,
        "gw-a",
        HandshakeMark {
            at: first.at + 60,
            ..first.clone()
        },
    )
    .unwrap();
    assert_eq!(fs::read(&path).unwrap(), first_bytes);

    a2a::mesh::rotation::RotationState::record_handshake(
        &home,
        "gw-a",
        HandshakeMark {
            at: first.at + 61,
            ..first
        },
    )
    .unwrap();
    let state = a2a::mesh::rotation::RotationState::load(&home).unwrap();
    assert_eq!(state.handshakes["gw-a"].at, 1_700_000_061);
}

#[test]
fn stage_mints_the_next_pair_and_a_signed_announcement_without_touching_the_gateway_token() {
    let tmp = tempfile::tempdir().unwrap();
    init_owner_with_token(tmp.path());
    let token_path = tmp.path().join("mesh/gateway/gw.token");
    let token_before = fs::read(&token_path).unwrap();

    let staged = cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--stage"])
        .assert()
        .success();
    let out = json_out(&staged);
    let token_after_first_stage = fs::read(&token_path).unwrap();
    assert_eq!(token_before, token_after_first_stage);
    assert_eq!(mode(&tmp.path().join("mesh/fleet/root.key.next")), 0o600);
    assert_eq!(mode(&tmp.path().join("mesh/fleet/root.pub.next")), 0o644);
    assert_eq!(mode(&tmp.path().join("mesh/fleet/rotation.json")), 0o600);
    assert_eq!(
        out["data"]["next_fingerprint"].as_str().unwrap().len(),
        64,
        "{out}"
    );
    assert!(
        !out["data"]["announcement"].as_str().unwrap().is_empty(),
        "{out}"
    );

    let announcement_path = tmp.path().join("rotation-announcement.txt");
    cmd(tmp.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "rotate-root",
            "--stage",
            "--force",
            "--out",
            announcement_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert_eq!(
        fs::read(&token_path).unwrap(),
        token_after_first_stage,
        "forced restaging must not rewrite the gateway token"
    );
    assert_eq!(mode(&announcement_path), 0o600);
    assert!(!fs::read_to_string(announcement_path).unwrap().trim().is_empty());
}

#[test]
fn stage_twice_refuses_without_force() {
    let tmp = tempfile::tempdir().unwrap();
    cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--stage"])
        .assert()
        .success();

    let refused = cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--stage"])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "ROOT_ROTATION_STAGED", "{out}");
    assert!(
        out["error"]["remedy"]
            .as_str()
            .unwrap()
            .contains("--abort"),
        "{out}"
    );
}

#[test]
fn rotate_root_requires_an_explicit_operation() {
    let tmp = tempfile::tempdir().unwrap();
    let refused = cmd(tmp.path())
        .args(["mesh", "fleet", "rotate-root"])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&refused.get_output().stderr);
    assert!(stderr.contains("--stage"), "{stderr}");
    assert!(stderr.contains("--abort"), "{stderr}");
    assert!(stderr.contains("--promote"), "{stderr}");
    assert!(stderr.contains("--retire"), "{stderr}");
}

#[test]
fn abort_removes_only_the_staged_pair() {
    let tmp = tempfile::tempdir().unwrap();
    cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    let key_path = tmp.path().join("mesh/fleet/root.key");
    let pub_path = tmp.path().join("mesh/fleet/root.pub");
    let key_before = sha256(&key_path);
    let pub_before = sha256(&pub_path);

    cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--stage"])
        .assert()
        .success();
    cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--abort"])
        .assert()
        .success();

    assert!(!tmp.path().join("mesh/fleet/root.key.next").exists());
    assert!(!tmp.path().join("mesh/fleet/root.pub.next").exists());
    assert_eq!(sha256(&key_path), key_before);
    assert_eq!(sha256(&pub_path), pub_before);
}

#[test]
fn enroll_sign_with_next_mints_a_token_only_the_staged_root_verifies() {
    let owner = tempfile::tempdir().unwrap();
    cmd(owner.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    let staged = cmd(owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--stage"])
        .assert()
        .success();
    let staged = json_out(&staged);
    let next_fpr = staged["data"]["next_fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    let pubkey = pubkey_b64(41);
    let enrolled = cmd(owner.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            "gw-x-aaaaaa",
            "--pubkey",
            &pubkey,
            "--sign-with",
            "next",
        ])
        .assert()
        .success();
    let enrolled = json_out(&enrolled);
    assert_eq!(enrolled["data"]["issuing_root_fpr"], next_fpr);
    let token = enrolled["data"]["token"].as_str().unwrap();

    let verifier = tempfile::tempdir().unwrap();
    fs::create_dir_all(verifier.path().join("mesh/fleet")).unwrap();
    fs::copy(
        owner.path().join("mesh/fleet/root.pub"),
        verifier.path().join("mesh/fleet/root.pub"),
    )
    .unwrap();
    let verifier_home = A2aHome::resolve(Some(verifier.path())).unwrap();
    let old_roots = RootSet::load(&verifier_home).unwrap();
    assert_eq!(
        keys::verify_token(token, &old_roots, &SystemClock)
            .unwrap_err()
            .code,
        "TOKEN_INVALID"
    );
    fs::copy(
        owner.path().join("mesh/fleet/root.pub.next"),
        verifier.path().join("mesh/fleet/root.pub.next"),
    )
    .unwrap();
    keys::verify_token(
        token,
        &RootSet::load(&verifier_home).unwrap(),
        &SystemClock,
    )
    .unwrap();
}

#[test]
fn the_ledger_records_every_enrollment_with_its_issuing_root() {
    let tmp = tempfile::tempdir().unwrap();
    cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    let staged = cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--stage"])
        .assert()
        .success();
    let staged = json_out(&staged);
    let next_fpr = staged["data"]["next_fingerprint"].as_str().unwrap();
    let pubkey = pubkey_b64(42);
    cmd(tmp.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            "gw-ledger-aaaaaa",
            "--pubkey",
            &pubkey,
            "--sign-with",
            "next",
        ])
        .assert()
        .success();

    let path = tmp.path().join("mesh/fleet/enrollments.json");
    assert_eq!(mode(&path), 0o600);
    let ledger: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(ledger["v"], 1);
    let entry = &ledger["entries"][0];
    assert_eq!(entry["gateway_id"], "gw-ledger-aaaaaa");
    assert_eq!(entry["pubkey_b64"], pubkey);
    assert_eq!(entry["key_epoch"], 0);
    assert_eq!(entry["issuing_root_fpr"], next_fpr);
    assert!(entry["issued_at"].is_i64(), "{entry}");
    assert!(entry["exp"].is_i64(), "{entry}");
}

#[test]
fn abort_refuses_after_a_member_was_enrolled_under_the_staged_root() {
    let tmp = tempfile::tempdir().unwrap();
    cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--stage"])
        .assert()
        .success();
    let pubkey = pubkey_b64(43);
    cmd(tmp.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "enroll",
            "--gateway-id",
            "gw-staged-aaaaaa",
            "--pubkey",
            &pubkey,
            "--sign-with",
            "next",
        ])
        .assert()
        .success();

    let refused = cmd(tmp.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--abort"])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "ROOT_ROTATION_ABORT_REFUSED");
    assert!(
        out["error"]["message"]
            .as_str()
            .unwrap()
            .contains("gw-staged-aaaaaa"),
        "{out}"
    );
    assert!(tmp.path().join("mesh/fleet/root.key.next").exists());

    cmd(tmp.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "rotate-root",
            "--abort",
            "--force",
        ])
        .assert()
        .success();
    assert!(!tmp.path().join("mesh/fleet/root.key.next").exists());
    assert!(!tmp.path().join("mesh/fleet/root.pub.next").exists());
}

#[test]
fn install_root_next_writes_only_the_next_root_and_reverifies_the_token() {
    let (_owner, joiner, staged) = staged_owner_and_enrolled_joiner();
    let announcement_path = joiner.path().join("announcement.txt");
    fs::write(
        &announcement_path,
        staged["data"]["announcement"].as_str().unwrap(),
    )
    .unwrap();
    let current_path = joiner.path().join("mesh/fleet/root.pub");
    let current_before = sha256(&current_path);

    let installed = cmd(joiner.path())
        .args([
            "--json",
            "mesh",
            "gateway",
            "install-root-next",
            "--file",
            announcement_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let installed = json_out(&installed);
    assert_eq!(
        installed["data"]["next_fpr"],
        staged["data"]["next_fingerprint"]
    );
    assert_eq!(installed["data"]["token_reverified"], true);
    assert_eq!(sha256(&current_path), current_before);
    assert_eq!(
        mode(&joiner.path().join("mesh/fleet/root.pub.next")),
        0o644
    );

    let human = cmd(joiner.path())
        .args([
            "mesh",
            "gateway",
            "install-root-next",
            "--file",
            announcement_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let rendered = String::from_utf8_lossy(&human.get_output().stdout);
    assert!(rendered.contains("restart"), "{rendered}");
}

#[test]
fn install_root_next_refuses_another_fleets_announcement() {
    let (owner, joiner, staged) = staged_owner_and_enrolled_joiner();
    let owner_home = A2aHome::resolve(Some(owner.path())).unwrap();
    let roots = RootSet::load(&owner_home).unwrap();
    let (mut announcement, _issuer) = keys::verify_announcement(
        staged["data"]["announcement"].as_str().unwrap(),
        &roots,
    )
    .unwrap();
    let other = tempfile::tempdir().unwrap();
    let other_home = A2aHome::resolve(Some(other.path())).unwrap();
    announcement.fleet_id = keys::fleet_init(&other_home).unwrap().fleet_id;
    let foreign_claim = keys::sign_announcement(
        &announcement,
        &owner.path().join("mesh/fleet/root.key"),
    )
    .unwrap();
    let path = joiner.path().join("foreign-announcement.txt");
    fs::write(&path, foreign_claim).unwrap();

    let refused = cmd(joiner.path())
        .args([
            "--json",
            "mesh",
            "gateway",
            "install-root-next",
            "--file",
            path.to_str().unwrap(),
        ])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(
        out["error"]["code"],
        "ROOT_ANNOUNCEMENT_FLEET_MISMATCH"
    );
    assert!(!joiner.path().join("mesh/fleet/root.pub.next").exists());
}

#[test]
fn install_root_next_refuses_a_bare_root_pub_file() {
    let (owner, joiner, _staged) = staged_owner_and_enrolled_joiner();
    let refused = cmd(joiner.path())
        .args([
            "--json",
            "mesh",
            "gateway",
            "install-root-next",
            "--file",
            owner
                .path()
                .join("mesh/fleet/root.pub.next")
                .to_str()
                .unwrap(),
        ])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "ROOT_ANNOUNCEMENT_INVALID");
    assert!(
        out["error"]["remedy"]
            .as_str()
            .unwrap()
            .contains("bare root.pub.next"),
        "{out}"
    );
    assert!(!joiner.path().join("mesh/fleet/root.pub.next").exists());
}

#[test]
fn promote_refuses_until_no_row_verifies_under_the_old_root() {
    let current_fpr = "a".repeat(64);
    let next_fpr = "b".repeat(64);
    let mut evidence = RotationEvidence {
        current_fpr: current_fpr.clone(),
        next_fpr: Some(next_fpr.clone()),
        prev_fpr: None,
        promoted_at: None,
        members: vec![MemberEvidence {
            gateway_id: "gw-one".to_string(),
            verified_issuer_fpr: Some(current_fpr),
            roots_held: Vec::new(),
            last_handshake: None,
        }],
        unaccounted: Vec::new(),
    };
    assert_eq!(phase(&evidence), RotationPhase::Phase1);

    evidence.members[0].verified_issuer_fpr = Some(next_fpr);
    assert_eq!(phase(&evidence), RotationPhase::Phase2);

    evidence.unaccounted.push("gw-missing".to_string());
    assert_eq!(phase(&evidence), RotationPhase::Unknown);
}

#[test]
fn retire_is_blocked_by_soak_and_force_skips_only_the_soak() {
    let new_fpr = "b".repeat(64);
    let promoted_at = 1_700_000_000;
    let mut evidence = RotationEvidence {
        current_fpr: new_fpr.clone(),
        next_fpr: None,
        prev_fpr: Some("a".repeat(64)),
        promoted_at: Some(promoted_at),
        members: vec![MemberEvidence {
            gateway_id: "gw-one".to_string(),
            verified_issuer_fpr: Some(new_fpr.clone()),
            roots_held: Vec::new(),
            last_handshake: None,
        }],
        unaccounted: Vec::new(),
    };
    assert_eq!(phase(&evidence), RotationPhase::Phase2);
    assert_eq!(
        soak_state(&evidence, promoted_at + 3_600, 3_600),
        SoakState::Pending {
            missing: vec!["gw-one".to_string()],
            seconds_remaining: 0,
        }
    );

    evidence.members[0].last_handshake = Some(HandshakeMark {
        issuer_fpr: new_fpr,
        at: promoted_at,
    });
    assert_eq!(
        soak_state(&evidence, promoted_at + 30, 3_600),
        SoakState::Pending {
            missing: Vec::new(),
            seconds_remaining: 3_570,
        }
    );
    assert_eq!(
        soak_state(&evidence, promoted_at + 3_600, 3_600),
        SoakState::Satisfied
    );
}

#[test]
fn soak_is_cleared_after_retire_even_if_promoted_at_lingers() {
    // After retire the old root is gone (prev_fpr None, next_fpr None -> phase
    // NotStaged), but promoted_at can linger in the rotation state. The soak
    // block keys on prev_fpr, so it must report NotPromoted rather than a stale
    // Pending/Satisfied block as if a rotation were still in flight (a2a-7ku).
    let new_fpr = "b".repeat(64);
    let promoted_at = 1_700_000_000;
    let evidence = RotationEvidence {
        current_fpr: new_fpr.clone(),
        next_fpr: None,
        prev_fpr: None,
        promoted_at: Some(promoted_at),
        members: vec![MemberEvidence {
            gateway_id: "gw-one".to_string(),
            verified_issuer_fpr: Some(new_fpr),
            roots_held: Vec::new(),
            last_handshake: None,
        }],
        unaccounted: Vec::new(),
    };
    assert_eq!(phase(&evidence), RotationPhase::NotStaged);
    assert_eq!(
        soak_state(&evidence, promoted_at + 30, 3_600),
        SoakState::NotPromoted
    );
}

#[test]
fn cli_promote_names_a_member_that_still_verifies_under_the_old_root() {
    let (owner, gateway_id) = owner_with_signed_member(false);
    let refused = cmd(owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--promote"])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "ROTATION_PHASE_UNMET", "{out}");
    assert!(
        out["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&gateway_id),
        "{out}"
    );
    assert!(owner.path().join("mesh/fleet/root.key.next").exists());
    assert!(!owner.path().join("mesh/fleet/root.key.prev").exists());

    let forced_retire = cmd(owner.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "rotate-root",
            "--retire",
            "--force",
        ])
        .assert()
        .failure();
    let forced_retire = json_out(&forced_retire);
    assert_eq!(
        forced_retire["error"]["code"],
        "ROTATION_PHASE_UNMET",
        "{forced_retire}"
    );
}

#[test]
fn cli_retire_is_blocked_by_soak_and_force_skips_only_the_soak() {
    let (owner, _gateway_id) = owner_with_signed_member(true);
    let promoted = cmd(owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--promote"])
        .assert()
        .success();
    let promoted = json_out(&promoted);
    assert_eq!(promoted["data"]["promoted"], true, "{promoted}");
    assert!(owner.path().join("mesh/fleet/root.key.prev").exists());
    assert!(!owner.path().join("mesh/fleet/root.key.next").exists());
    let owner_home = A2aHome::resolve(Some(owner.path())).unwrap();
    let promoted_roots = RootSet::load(&owner_home).unwrap();
    assert_eq!(
        promoted_roots.role_of(
            promoted["data"]["promoted_to_fpr"].as_str().unwrap()
        ),
        Some(keys::RootRole::Current)
    );
    assert_eq!(
        promoted_roots.role_of(
            promoted["data"]["promoted_from_fpr"].as_str().unwrap()
        ),
        Some(keys::RootRole::Prev)
    );
    let state_path = owner.path().join("mesh/fleet/rotation.json");
    assert_eq!(mode(&state_path), 0o600);
    let state: Value = serde_json::from_slice(&fs::read(state_path).unwrap()).unwrap();
    assert_eq!(
        state["promoted_from_fpr"],
        promoted["data"]["promoted_from_fpr"]
    );
    assert_eq!(
        state["promoted_to_fpr"],
        promoted["data"]["promoted_to_fpr"]
    );
    assert!(state["promoted_at"].is_i64(), "{state}");

    let refused = cmd(owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--retire"])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "ROTATION_SOAK_UNMET", "{out}");
    assert!(owner.path().join("mesh/fleet/root.key.prev").exists());

    let forced = cmd(owner.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "rotate-root",
            "--retire",
            "--force",
        ])
        .assert()
        .success();
    let forced = json_out(&forced);
    assert_eq!(forced["data"]["soak_skipped"], true, "{forced}");
    assert!(
        forced["data"]["message"]
            .as_str()
            .unwrap()
            .contains("soak skipped by --force"),
        "{forced}"
    );
    assert!(!owner.path().join("mesh/fleet/root.key.prev").exists());
    assert!(!owner.path().join("mesh/fleet/root.pub.prev").exists());
}
