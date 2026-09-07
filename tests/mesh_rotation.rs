use a2a::home::A2aHome;
use a2a::mesh::keys::{self, RootSet, SystemClock};
use a2a::mesh::registry::{write_registry_env, FileRegistry, GatewayRow, Registry};
use a2a::mesh::rotation::{
    phase, soak_state, HandshakeMark, MemberEvidence, RotationEvidence, RotationPhase,
    SoakState,
};
use a2a::mesh::row_sig::{sign_row, RowMsg, ROW_MSG_VER};
use assert_cmd::Command;
use chrono::Utc;
use ed25519_dalek::SigningKey;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

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
