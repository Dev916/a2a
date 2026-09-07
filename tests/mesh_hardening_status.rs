use assert_cmd::Command;
use a2a::home::A2aHome;
use a2a::mesh::keys::{
    self, enroll_gateway_token, fleet_init, gateway_identity, gateway_public_key_b64,
    install_gateway_token, RootSet, SystemClock, TokenClaims,
};
use a2a::mesh::registry::{FileRegistry, GatewayRow, Registry, REGISTRY_SCHEMA_VERSION};
use a2a::mesh::row_mark::RowMarkStore;
use a2a::mesh::row_sig::{canonical_row_msg, sign_row, RowMsg, ROW_MSG_VER};
use chrono::{DateTime, Timelike, Utc};
use nix::fcntl::{Flock, FlockArg};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

mod common;

fn command(home: &std::path::Path, work: &std::path::Path) -> Command {
    common::a2a(home, work)
}

fn json_output(assert: &assert_cmd::assert::Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).unwrap()
}

#[test]
fn fleet_init_writes_require_signed_rows_true_for_a_new_fleet() {
    let tmp = tempfile::tempdir().unwrap();
    let untouched_home = tmp.path().join("untouched-home");
    let new_home = tmp.path().join("new-home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();

    let untouched = command(&untouched_home, &work)
        .args(["--json", "config", "show", "--origin"])
        .assert()
        .success();
    let untouched = json_output(&untouched);
    assert_eq!(
        untouched["data"]["mesh"]["gateway"]["require_signed_rows"],
        false
    );
    assert_eq!(
        untouched["data"]["origins"]["mesh.gateway.require_signed_rows"],
        "default"
    );

    fs::create_dir_all(&new_home).unwrap();
    fs::write(
        new_home.join("config.toml"),
        "# operator comment\n[mesh]\nstale_after_secs = 42\n",
    )
    .unwrap();
    command(&new_home, &work)
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();

    let text = fs::read_to_string(new_home.join("config.toml")).unwrap();
    let config: toml::Table = text.parse().unwrap();
    assert_eq!(
        config["mesh"]["gateway"]["require_signed_rows"].as_bool(),
        Some(true),
        "{text}"
    );
    assert!(text.contains("# operator comment"), "{text}");
    assert!(text.contains("stale_after_secs = 42"), "{text}");

    let initialized = command(&new_home, &work)
        .args(["--json", "config", "show", "--origin"])
        .assert()
        .success();
    let initialized = json_output(&initialized);
    assert_eq!(
        initialized["data"]["mesh"]["gateway"]["require_signed_rows"],
        true
    );
    assert_eq!(
        initialized["data"]["origins"]["mesh.gateway.require_signed_rows"],
        "file"
    );

    let mut operator_value = text.replace("require_signed_rows = true", "require_signed_rows = false");
    operator_value.push_str("# retained after repeated init\n");
    fs::write(new_home.join("config.toml"), operator_value).unwrap();
    command(&new_home, &work)
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    let repeated = fs::read_to_string(new_home.join("config.toml")).unwrap();
    assert!(repeated.contains("require_signed_rows = false"), "{repeated}");
    assert!(repeated.contains("# retained after repeated init"), "{repeated}");

    let explicit_home = tmp.path().join("explicit-home");
    fs::create_dir_all(&explicit_home).unwrap();
    fs::write(
        explicit_home.join("config.toml"),
        "[mesh.gateway]\nrequire_signed_rows = false # operator chose upgrade posture\n",
    )
    .unwrap();
    command(&explicit_home, &work)
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    let explicit = fs::read_to_string(explicit_home.join("config.toml")).unwrap();
    assert!(
        explicit.contains("require_signed_rows = false # operator chose upgrade posture"),
        "{explicit}"
    );
}

#[test]
fn hardening_status_counts_only_verified_rows_and_reports_the_unknowns() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(&tmp.path().join("reader"))).unwrap();
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    fleet_init(&home).unwrap();
    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    fs::write(
        home.root().join("mesh/fleet/registry.env"),
        format!("file:{}\n", registry_path.display()),
    )
    .unwrap();

    let (enforcing, _enforcing_msg, _enforcing_claims) =
        signed_member(&home, &tmp.path().join("enforcing"), true, true);
    let (not_enforcing, _not_enforcing_msg, _not_enforcing_claims) =
        signed_member(&home, &tmp.path().join("not-enforcing"), false, false);
    let (mut bad_sig, mut bad_msg, bad_claims) =
        signed_member(&home, &tmp.path().join("bad-sig"), false, false);
    let mut marks = RowMarkStore::open(&home).unwrap();
    marks
        .apply_verified(&bad_sig.gateway_id, &bad_msg, &bad_claims)
        .unwrap();
    marks.try_forget_signed(&bad_sig.gateway_id).unwrap();
    bad_msg.enforce_require_signed_rows = true;
    bad_msg.enforce_auto_push = true;
    bad_sig.row_msg = Some(canonical_row_msg(&bad_msg).unwrap());

    let rt = tokio::runtime::Runtime::new().unwrap();
    for row in [enforcing, not_enforcing, bad_sig] {
        rt.block_on(registry.upsert_self(row)).unwrap();
    }

    let status = command(home.root(), &work)
        .args(["mesh", "fleet", "hardening-status", "--json"])
        .assert()
        .success();
    let out = json_output(&status);
    assert_eq!(
        out["data"]["require_signed_rows"],
        serde_json::json!({"enforcing": 1, "not_enforcing": 1, "unknown": 1}),
        "{out}"
    );
    assert_eq!(
        out["data"]["auto_push"],
        serde_json::json!({"enforcing": 1, "not_enforcing": 1, "unknown": 1}),
        "{out}"
    );
    let rows = out["data"]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3, "{out}");
    let bad = rows
        .iter()
        .find(|row| row["verdict"] == "BAD_SIG")
        .unwrap_or_else(|| panic!("tampered row is surfaced: {out}"));
    assert!(bad["enforce_require_signed_rows"].is_null(), "{out}");
    assert!(bad["enforce_auto_push"].is_null(), "{out}");

    let human = command(home.root(), &work)
        .args(["mesh", "fleet", "hardening-status"])
        .assert()
        .success();
    let stdout = String::from_utf8(human.get_output().stdout.clone()).unwrap();
    assert!(
        stdout.contains("require_signed_rows enforcing=1 not_enforcing=1 unknown=1"),
        "{stdout}"
    );
    assert!(
        stdout.contains("auto_push enforcing=1 not_enforcing=1 unknown=1"),
        "{stdout}"
    );
}

#[test]
fn gateway_status_reports_the_running_daemons_effective_flags() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let mesh = home.join("mesh");
    fs::create_dir_all(&mesh).unwrap();
    fs::create_dir_all(&work).unwrap();
    fs::write(
        home.join("config.toml"),
        "[mesh.gateway]\nrequire_signed_rows = false\n",
    )
    .unwrap();
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
          "require_signed_rows": true,
          "auto_push": true,
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

    let status = command(&home, &work)
        .args(["--json", "mesh", "gateway", "status"])
        .assert()
        .success();
    let out = json_output(&status);
    assert_eq!(out["data"]["health"]["require_signed_rows"], true, "{out}");
    assert_eq!(out["data"]["health"]["auto_push"], true, "{out}");

    let human = command(&home, &work)
        .args(["mesh", "gateway", "status"])
        .assert()
        .success();
    let stdout = String::from_utf8(human.get_output().stdout.clone()).unwrap();
    assert!(stdout.contains("require_signed_rows true"), "{stdout}");
    assert!(stdout.contains("auto_push true"), "{stdout}");
}

fn signed_member(
    root: &A2aHome,
    path: &Path,
    enforce_require_signed_rows: bool,
    enforce_auto_push: bool,
) -> (GatewayRow, RowMsg, TokenClaims) {
    let member = A2aHome::resolve(Some(path)).unwrap();
    fs::create_dir_all(member.root().join("mesh/fleet")).unwrap();
    fs::copy(
        root.root().join("mesh/fleet/root.pub"),
        member.root().join("mesh/fleet/root.pub"),
    )
    .unwrap();
    let identity = gateway_identity(&member, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(&member).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey_b64, &SystemClock)
        .unwrap();
    let claims = install_gateway_token(&member, &token, &SystemClock).unwrap();
    let now = trunc_us(Utc::now());
    let msg = RowMsg {
        msg_ver: ROW_MSG_VER,
        fleet_id: claims.fleet_id.clone(),
        gateway_id: claims.gateway_id.clone(),
        endpoint_url: Some(format!("wss://{}.example.test:7787", claims.gateway_id)),
        pubkey_fpr: keys::fpr_of_pubkey_b64(&claims.pubkey_b64).unwrap(),
        cert_fpr: Some(format!("cert-{}", claims.gateway_id)),
        version: "0.3.0".to_string(),
        owner: "hardening-test".to_string(),
        created_at: now,
        schema_version: REGISTRY_SCHEMA_VERSION,
        key_epoch: claims.key_epoch,
        roots_held: serde_json::to_string(&RootSet::load(root).unwrap().fingerprints()).unwrap(),
        row_token_sha256: sha256_hex(token.as_bytes()),
        row_resign_secs: 86_400,
        registry_heartbeat_secs: 180,
        enforce_require_signed_rows,
        enforce_auto_push,
        signed_at: now,
    };
    let (row_msg, row_sig) = sign_row(&msg, &keys::gateway_signing_key(&member).unwrap()).unwrap();
    (
        GatewayRow {
            fleet_id: msg.fleet_id.clone(),
            gateway_id: msg.gateway_id.clone(),
            owner: msg.owner.clone(),
            endpoint_url: msg.endpoint_url.clone(),
            pubkey_fpr: msg.pubkey_fpr.clone(),
            cert_fpr: msg.cert_fpr.clone(),
            version: msg.version.clone(),
            created_at: msg.created_at,
            last_heartbeat: now,
            revoked_at: None,
            schema_version: msg.schema_version,
            row_msg: Some(row_msg),
            row_token: Some(token),
            row_sig: Some(row_sig),
            signed_at: Some(now),
            revocation: None,
        },
        msg,
        claims,
    )
}

fn trunc_us(now: DateTime<Utc>) -> DateTime<Utc> {
    now.with_nanosecond(now.nanosecond() / 1_000 * 1_000)
        .unwrap()
}

fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
