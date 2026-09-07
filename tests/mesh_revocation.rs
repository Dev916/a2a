use a2a::home::A2aHome;
use a2a::mesh::keys::{
    self, enroll_gateway_token_at_epoch, fleet_init, gateway_identity,
    gateway_public_key_b64, mint_revocation, verify_revocation, RootSet, SystemClock,
};
use a2a::mesh::registry::{
    open_registry, write_registry_env, FileRegistry, GatewayRow, Registry,
};
use a2a::mesh::row_mark::{RevocationCache, RowMarkStore};
use a2a::mesh::row_sig::{sign_row, RowMsg, RowVerdict, RowVerifyCfg, ROW_MSG_VER};
use assert_cmd::Command;
use chrono::{Duration, Utc};
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;
use rand::RngCore;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

#[test]
fn revocation_record_verifies_under_the_root_and_is_not_a_token() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let fleet = fleet_init(&home).unwrap();
    let record = mint_revocation(&home, "gw-a-0123456789abcdef", 3, &SystemClock).unwrap();
    let roots = RootSet::load(&home).unwrap();

    let claims = verify_revocation(&record, &roots).unwrap();
    assert_eq!(claims.fleet_id, fleet.fleet_id);
    assert_eq!(claims.gateway_id, "gw-a-0123456789abcdef");
    assert_eq!(claims.key_epoch, 3);
    assert!(keys::verify_token_no_clock(&record, &roots).is_err());
}

#[test]
fn revocation_records_naming_epoch_n_revoke_epochs_at_or_below_n() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    fleet_init(&home).unwrap();
    let record = mint_revocation(&home, "gw-a-0123456789abcdef", 2, &SystemClock).unwrap();
    let claims = verify_revocation(&record, &RootSet::load(&home).unwrap()).unwrap();
    let mut cache = RevocationCache::open(&home).unwrap();
    cache.observe(&claims.gateway_id, &claims).unwrap();

    assert!(cache.is_revoked(&claims.gateway_id, 0));
    assert!(cache.is_revoked(&claims.gateway_id, 2));
    assert!(!cache.is_revoked(&claims.gateway_id, 3));
    let path = home
        .root()
        .join("mesh/revocations")
        .join(format!("{}.json", claims.gateway_id));
    assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o777, 0o600);
}

#[test]
fn a_revocation_signed_by_a_non_root_key_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let fleet = fleet_init(&home).unwrap();
    let roots = RootSet::load(&home).unwrap();
    let claims = serde_json::to_vec(&BTreeMap::from([
        ("fleet_id", json!(fleet.fleet_id)),
        ("gateway_id", json!("gw-a-0123456789abcdef")),
        ("key_epoch", json!(0)),
        ("revoked_at", json!(1_700_000_000)),
    ]))
    .unwrap();
    let mut seed = [0_u8; 32];
    OsRng.fill_bytes(&mut seed);
    let attacker = SigningKey::from_bytes(&seed);
    let signature = attacker.sign(&claims);
    let envelope = serde_json::to_vec(&json!({
        "v": 1,
        "claims_b64": b64(&claims),
        "sig_b64": b64(&signature.to_bytes()),
    }))
    .unwrap();
    let record = b64(&envelope);

    let err = verify_revocation(&record, &roots).unwrap_err();
    assert_eq!(err.code, "REVOCATION_INVALID");
}

#[test]
fn fleet_revoke_is_root_gated_and_writes_the_signed_record_and_legacy_mirror() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(&tmp.path().join("root"))).unwrap();
    let joiner = A2aHome::resolve(Some(&tmp.path().join("joiner"))).unwrap();
    let fleet = fleet_init(&home).unwrap();
    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    let registry_url = format!("file:{}", registry_path.display());
    write_registry_env(&home, &registry_url).unwrap();
    let now = Utc::now();
    let gateway_id = "gw-revoke-0123456789abcdef";
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(registry.upsert_self(row(&fleet.fleet_id, gateway_id, now)))
        .unwrap();

    command(home.root())
        .args(["--json", "mesh", "fleet", "revoke", gateway_id])
        .assert()
        .success();
    let rows = runtime
        .block_on(registry.discover_unverified(&fleet.fleet_id))
        .unwrap();
    let stored = &rows[0];
    let record = stored.revocation.as_deref().unwrap();
    let claims = verify_revocation(record, &RootSet::load(&home).unwrap()).unwrap();
    assert_eq!(claims.fleet_id, fleet.fleet_id);
    assert_eq!(claims.gateway_id, gateway_id);
    assert_eq!(claims.key_epoch, 0);
    assert_eq!(
        stored.revoked_at.unwrap().timestamp(),
        claims.revoked_at,
        "legacy mirror and signed tombstone must be written together"
    );

    fs::create_dir_all(joiner.root().join("mesh/fleet")).unwrap();
    fs::copy(
        home.root().join("mesh/fleet/root.pub"),
        joiner.root().join("mesh/fleet/root.pub"),
    )
    .unwrap();
    write_registry_env(&joiner, &registry_url).unwrap();
    command(joiner.root())
        .args(["--json", "mesh", "fleet", "revoke", gateway_id])
        .assert()
        .failure()
        .stdout(predicates::str::contains("FLEET_ALREADY_JOINED"));
}

#[test]
fn a_misplaced_revocation_seen_by_the_daemon_never_enters_the_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let fleet = fleet_init(&home).unwrap();
    let record = mint_revocation(&home, "gw-a-0123456789abcdef", 0, &SystemClock).unwrap();
    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    let now = Utc::now();
    let mut carrier = row(&fleet.fleet_id, "gw-b-0123456789abcdef", now);
    carrier.revocation = Some(record);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(registry.upsert_self(carrier)).unwrap();
    let handle = open_registry(
        &format!("file:{}", registry_path.display()),
        RootSet::load(&home).unwrap(),
        fleet.fleet_id,
    )
    .unwrap()
    .with_revocation_cache(RevocationCache::open(&home).unwrap())
    .with_reader_state(Default::default(), RowVerifyCfg::new(now));

    let rows = runtime.block_on(handle.discover()).unwrap();
    assert_eq!(rows[0].1, RowVerdict::RowRevocationMisplaced);
    let cache = RevocationCache::open(&home).unwrap();
    assert!(!cache.is_revoked("gw-a-0123456789abcdef", 0));
    assert!(!cache.is_revoked("gw-b-0123456789abcdef", 0));
}

#[test]
fn a_higher_epoch_root_token_clears_the_daemon_revocation_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(tmp.path())).unwrap();
    let fleet = fleet_init(&home).unwrap();
    let identity = gateway_identity(&home, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(&home).unwrap();
    let (token, token_claims, _issuer) = enroll_gateway_token_at_epoch(
        &home,
        &identity.gateway_id,
        &pubkey_b64,
        1,
        &SystemClock,
    )
    .unwrap();
    let roots = RootSet::load(&home).unwrap();
    let record = mint_revocation(&home, &identity.gateway_id, 0, &SystemClock).unwrap();
    let revocation = verify_revocation(&record, &roots).unwrap();
    let mut cache = RevocationCache::open(&home).unwrap();
    cache.observe(&identity.gateway_id, &revocation).unwrap();

    let now = Utc::now();
    let msg = RowMsg {
        msg_ver: ROW_MSG_VER,
        fleet_id: fleet.fleet_id.clone(),
        gateway_id: identity.gateway_id.clone(),
        endpoint_url: None,
        pubkey_fpr: keys::fpr_of_pubkey_b64(&pubkey_b64).unwrap(),
        cert_fpr: None,
        version: "0.3.0".to_string(),
        owner: "owner".to_string(),
        created_at: now,
        schema_version: 1,
        key_epoch: token_claims.key_epoch,
        roots_held: "[]".to_string(),
        row_token_sha256: hex_sha256(token.as_bytes()),
        row_resign_secs: 86_400,
        registry_heartbeat_secs: 180,
        enforce_require_signed_rows: false,
        enforce_auto_push: false,
        signed_at: now,
    };
    let (row_msg, row_sig) = sign_row(&msg, &keys::gateway_signing_key(&home).unwrap()).unwrap();
    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(registry.upsert_self(GatewayRow {
            fleet_id: fleet.fleet_id.clone(),
            gateway_id: identity.gateway_id.clone(),
            owner: msg.owner,
            endpoint_url: msg.endpoint_url,
            pubkey_fpr: msg.pubkey_fpr,
            cert_fpr: msg.cert_fpr,
            version: msg.version,
            created_at: msg.created_at,
            last_heartbeat: now,
            revoked_at: Some(now),
            schema_version: msg.schema_version,
            row_msg: Some(row_msg),
            row_token: Some(token),
            row_sig: Some(row_sig),
            signed_at: Some(msg.signed_at),
            revocation: Some(record),
        }))
        .unwrap();
    let handle = open_registry(
        &format!("file:{}", registry_path.display()),
        roots,
        fleet.fleet_id,
    )
    .unwrap()
    .with_reader_state(
        RowMarkStore::open(&home).unwrap(),
        RowVerifyCfg::new(now + Duration::seconds(1)),
    )
    .with_revocation_cache(RevocationCache::open(&home).unwrap());

    let rows = runtime.block_on(handle.discover()).unwrap();
    assert_eq!(rows[0].1, RowVerdict::ValidSigned);
    let reopened = RevocationCache::open(&home).unwrap();
    assert!(!reopened.is_revoked(&identity.gateway_id, 0));
    assert!(!home
        .root()
        .join("mesh/revocations")
        .join(format!("{}.json", identity.gateway_id))
        .exists());
}

fn command(home: &Path) -> Command {
    let mut command = Command::cargo_bin("a2a").unwrap();
    command.arg("--home").arg(home).env_remove("A2A_HOME");
    command
}

fn row(fleet_id: &str, gateway_id: &str, now: chrono::DateTime<Utc>) -> GatewayRow {
    GatewayRow {
        fleet_id: fleet_id.to_string(),
        gateway_id: gateway_id.to_string(),
        owner: "owner".to_string(),
        endpoint_url: None,
        pubkey_fpr: "00".repeat(32),
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
    }
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

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
