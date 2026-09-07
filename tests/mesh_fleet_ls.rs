mod common;

use a2a::home::A2aHome;
use a2a::mesh::keys::{
    self, enroll_gateway_token, fleet_id, fleet_init, gateway_identity,
    gateway_public_key_b64, install_gateway_token, RootSet, SystemClock, TokenClaims,
};
use a2a::mesh::registry::{write_registry_env, FileRegistry, GatewayRow, Registry};
use a2a::mesh::row_mark::RowMarkStore;
use a2a::mesh::row_sig::{sign_row, RowMsg, ROW_MSG_VER};
use assert_cmd::Command;
use chrono::{Timelike, Utc};
use predicates::prelude::*;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;

struct FleetFixture {
    _tmp: tempfile::TempDir,
    reader: A2aHome,
    registry: FileRegistry,
    work: PathBuf,
    claims: TokenClaims,
    msg: RowMsg,
    row: GatewayRow,
}

impl FleetFixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let reader = A2aHome::resolve(Some(&tmp.path().join("reader"))).unwrap();
        let member = A2aHome::resolve(Some(&tmp.path().join("member"))).unwrap();
        let work = tmp.path().join("work");
        fs::create_dir_all(&work).unwrap();
        fleet_init(&reader).unwrap();
        fs::create_dir_all(member.root().join("mesh/fleet")).unwrap();
        fs::copy(
            reader.root().join("mesh/fleet/root.pub"),
            member.root().join("mesh/fleet/root.pub"),
        )
        .unwrap();
        let identity = gateway_identity(&member, false).unwrap();
        let pubkey = gateway_public_key_b64(&member).unwrap();
        let token = enroll_gateway_token(
            &reader,
            &identity.gateway_id,
            &pubkey,
            &SystemClock,
        )
        .unwrap();
        install_gateway_token(&member, &token, &SystemClock).unwrap();
        let roots = RootSet::load(&reader).unwrap();
        let claims = keys::verify_token_no_clock(&token, &roots).unwrap().0;
        let now = Utc::now();
        let now = now
            .with_nanosecond(now.timestamp_subsec_micros() * 1_000)
            .unwrap();
        let msg = RowMsg {
            msg_ver: ROW_MSG_VER,
            fleet_id: fleet_id(&reader).unwrap(),
            gateway_id: claims.gateway_id.clone(),
            endpoint_url: Some("wss://member.example.test:7787".to_string()),
            pubkey_fpr: keys::fpr_of_pubkey_b64(&claims.pubkey_b64).unwrap(),
            cert_fpr: Some("member-cert-fingerprint".to_string()),
            version: "0.3.0".to_string(),
            owner: "fleet-test-owner".to_string(),
            created_at: now,
            schema_version: 1,
            key_epoch: claims.key_epoch,
            roots_held: "[]".to_string(),
            row_token_sha256: sha256_hex(token.as_bytes()),
            row_resign_secs: 86_400,
            registry_heartbeat_secs: 180,
            enforce_require_signed_rows: false,
            enforce_auto_push: false,
            signed_at: now,
        };
        let (row_msg, row_sig) = sign_row(&msg, &keys::gateway_signing_key(&member).unwrap()).unwrap();
        let row = GatewayRow {
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
            signed_at: Some(msg.signed_at),
            revocation: None,
        };
        let registry_path = tmp.path().join("registry");
        let registry = FileRegistry::new(&registry_path);
        registry.init_schema().unwrap();
        write_registry_env(
            &reader,
            &format!("file:{}", registry_path.display()),
        )
        .unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(registry.upsert_self(row.clone())).unwrap();
        Self {
            _tmp: tmp,
            reader,
            registry,
            work,
            claims,
            msg,
            row,
        }
    }

    fn command(&self) -> Command {
        common::a2a(self.reader.root(), &self.work)
    }

    fn replace_row(&mut self, row: GatewayRow) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(self.registry.upsert_self(row.clone())).unwrap();
        self.row = row;
    }

    fn mark_valid_signature(&self) {
        let mut marks = RowMarkStore::open(&self.reader).unwrap();
        marks
            .apply_verified(&self.msg.gateway_id, &self.msg, &self.claims)
            .unwrap();
    }
}

#[test]
fn fleet_ls_renders_the_verdict_column_first_with_a_remedy() {
    let mut fixture = FleetFixture::new();
    fixture.mark_valid_signature();
    let mut row = fixture.row.clone();
    row.version = "tampered-column".to_string();
    fixture.replace_row(row);

    let output = fixture
        .command()
        .args(["mesh", "fleet", "ls"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).unwrap();
    assert!(output.starts_with("BAD_SIG"), "{output}");
    assert!(output.contains("a2a mesh fleet clear-signature"), "{output}");
}

#[test]
fn fleet_ls_marks_a_cold_reader_as_first_trust() {
    let fixture = FleetFixture::new();
    let cold = fixture
        .command()
        .args(["mesh", "fleet", "ls"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let cold = String::from_utf8(cold).unwrap();
    assert!(cold.contains("first-trust"), "{cold}");

    let mut marks = RowMarkStore::open(&fixture.reader).unwrap();
    marks
        .apply_verified(&fixture.msg.gateway_id, &fixture.msg, &fixture.claims)
        .unwrap();
    fixture
        .command()
        .args([
            "mesh",
            "fleet",
            "clear-signature",
            &fixture.msg.gateway_id,
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("CLEARED first-signature bit"));
    let cleared = fixture
        .command()
        .args(["mesh", "fleet", "ls"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let cleared = String::from_utf8(cleared).unwrap();
    assert!(cleared.contains("signature-cleared"), "{cleared}");
    assert!(!cleared.contains("first-trust"), "{cleared}");
}

#[test]
fn fleet_ls_shows_a_revoked_row_flagged_instead_of_hiding_it() {
    let fixture = FleetFixture::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(fixture.registry.revoke(
            &fixture.msg.fleet_id,
            &fixture.msg.gateway_id,
            Utc::now(),
        ))
        .unwrap();

    let output = fixture
        .command()
        .args(["mesh", "fleet", "ls"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).unwrap();
    assert!(output.contains(&fixture.msg.gateway_id), "{output}");
    assert!(output.contains("legacy-revoked-at"), "{output}");
}

#[test]
fn endpoint_url_with_escape_and_bidi_codepoints_is_escaped_and_the_verdict_still_prints_first() {
    let mut fixture = FleetFixture::new();
    fixture.mark_valid_signature();
    let mut row = fixture.row.clone();
    row.endpoint_url = Some("wss://bad.example/\u{1b}[2K\r\u{202e}".to_string());
    fixture.replace_row(row);

    let output = fixture
        .command()
        .args(["mesh", "fleet", "ls"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output = String::from_utf8(output).unwrap();
    assert!(output.starts_with("BAD_SIG"), "{output}");
    assert!(output.contains("\\u001B[2K\\u000D\\u202E"), "{output}");
    assert!(output.contains("non-ascii"), "{output}");
    assert!(!output.contains('\u{1b}'), "{output:?}");
    assert!(!output.contains('\u{202e}'), "{output:?}");
}

#[test]
fn fleet_ls_does_not_create_a_row_mark_directory_for_a_cold_reader() {
    let fixture = FleetFixture::new();
    let marks = fixture.reader.root().join("mesh/row-marks");
    assert!(!marks.exists());
    fixture
        .command()
        .args(["mesh", "fleet", "ls"])
        .assert()
        .success();
    assert!(!marks.exists());
}

#[test]
fn forget_signed_command_logs_the_deliberate_reset_and_keeps_the_signature_bit() {
    let fixture = FleetFixture::new();
    fixture.mark_valid_signature();
    fixture
        .command()
        .args([
            "mesh",
            "fleet",
            "forget-signed",
            &fixture.msg.gateway_id,
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "CLEARED replay high-water and row digest",
        ))
        .stderr(predicate::str::contains("KEPT first-signature bit"));

    let marks = RowMarkStore::open(&fixture.reader).unwrap();
    let mark = marks.get(&fixture.msg.gateway_id).unwrap();
    assert_eq!(mark.high_water, None);
    assert_eq!(mark.msg_digest, None);
    assert!(mark.first_signature);
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
