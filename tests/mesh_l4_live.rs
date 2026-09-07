//! Gated live tier for layer-4 signed mesh registry rows.
//!
//! Required invocation:
//!
//!   A2A_MESH_L4_LIVE=1 \
//!   A2A_MESH_L4_REGISTRY_URL=postgres://... \
//!   cargo test --test mesh_l4_live -- --ignored

mod common;

use a2a::home::A2aHome;
use a2a::mesh::gateway::expose::RowSigningContext;
use a2a::mesh::gateway::link::discover_outbound_peers;
use a2a::mesh::keys::{self, SystemClock};
use a2a::mesh::registry::{
    open_registry, reader_roots, GatewayRow, PgRegistry, Registry, REGISTRY_SCHEMA_VERSION,
};
use a2a::mesh::row_mark::{RowMark, RowMarkStore};
use a2a::mesh::row_sig::{
    parse_row_msg_strict, sign_row, Disposition, RowMsg, RowVerdict, RowVerifyCfg, ROW_MSG_VER,
};
use assert_cmd::assert::Assert;
use chrono::{DateTime, Duration, SecondsFormat, Timelike, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::future::Future;
use std::panic::resume_unwind;
use std::sync::OnceLock;

const LIVE_ENV: &str = "A2A_MESH_L4_LIVE";
const REGISTRY_URL_ENV: &str = "A2A_MESH_L4_REGISTRY_URL";
const ORIGINAL_ENDPOINT: &str = "wss://l4-live.example.invalid:443";
const TAMPERED_ENDPOINT: &str = "wss://tamper.invalid:443";

fn gated(what: &str) -> bool {
    if std::env::var(LIVE_ENV).as_deref() != Ok("1") {
        eprintln!("{LIVE_ENV} != 1 - skipping {what}");
        return false;
    }
    true
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for {LIVE_ENV}=1"))
}

fn json_out(assert: &Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).unwrap()
}

fn live_serial() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct LiveFixture {
    tmp: tempfile::TempDir,
    home: A2aHome,
    reader: A2aHome,
    registry_url: String,
    registry: PgRegistry,
    fleet_id: String,
    gateway_id: String,
    endpoint_url: String,
}

impl LiveFixture {
    async fn new(registry_url: String) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(&tmp.path().join("gateway"))).unwrap();
        let reader = A2aHome::resolve(Some(&tmp.path().join("reader"))).unwrap();
        keys::fleet_init(&home).unwrap();

        let fleet_id = format!("fleet-l4-live-{}", ulid::Ulid::new());
        set_fleet_id(&home, &fleet_id);
        install_reader_root(&home, &reader);

        let identity = keys::gateway_identity(&home, false).unwrap();
        keys::ensure_gateway_token(&home, &SystemClock).unwrap();
        let gateway_id = identity.gateway_id.clone();
        let endpoint_url = ORIGINAL_ENDPOINT.to_string();
        fs::write(
            home.root().join("config.toml"),
            format!(
                "[mesh.gateway]\nexpose = \"manual\"\nendpoint_url = \"{endpoint_url}\"\n"
            ),
        )
        .unwrap();
        a2a::mesh::registry::write_registry_env(&home, &registry_url).unwrap();

        for _ in 0..2 {
            let migrated = common::a2a(home.root(), tmp.path())
                .args(["--json", "mesh", "fleet", "migrate"])
                .assert()
                .success();
            let migrated = json_out(&migrated);
            let applied = migrated["data"]["applied"].as_array().unwrap();
            assert!(
                applied
                    .iter()
                    .any(|name| name.as_str() == Some("mesh_l4_0001.sql")),
                "{migrated}"
            );
        }

        let cfg = a2a::config::load(&home).unwrap().0;
        let signing = RowSigningContext::from_config(&home, &cfg);
        let roots = reader_roots(&home).unwrap();
        let row_token = keys::gateway_token(&home).unwrap();
        let (claims, _issuer) =
            keys::verify_token_with_issuer(&row_token, &roots, &SystemClock).unwrap();
        assert_eq!(claims.fleet_id, fleet_id);
        assert_eq!(claims.gateway_id, gateway_id);

        let mut held_roots = roots.fingerprints();
        held_roots.sort();
        let created_at = trunc_us(Utc::now() - Duration::seconds(20));
        let signed_at = trunc_us(Utc::now() - Duration::seconds(10));
        let msg = RowMsg {
            msg_ver: ROW_MSG_VER,
            fleet_id: fleet_id.clone(),
            gateway_id: gateway_id.clone(),
            endpoint_url: Some(endpoint_url.clone()),
            pubkey_fpr: keys::gateway_public_key_fingerprint(&home).unwrap(),
            cert_fpr: Some(identity.cert_fpr.clone()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            owner: common::TEST_OWNER.to_string(),
            created_at,
            schema_version: REGISTRY_SCHEMA_VERSION,
            key_epoch: claims.key_epoch,
            roots_held: serde_json::to_string(&held_roots).unwrap(),
            row_token_sha256: sha256_hex(row_token.as_bytes()),
            row_resign_secs: signing.row_resign_secs,
            registry_heartbeat_secs: signing.registry_heartbeat_secs,
            enforce_require_signed_rows: signing.enforce_require_signed_rows,
            enforce_auto_push: signing.enforce_auto_push,
            signed_at,
        };
        let (row_msg, row_sig) =
            sign_row(&msg, &keys::gateway_signing_key(&home).unwrap()).unwrap();
        let row = GatewayRow {
            fleet_id: fleet_id.clone(),
            gateway_id: gateway_id.clone(),
            owner: msg.owner.clone(),
            endpoint_url: msg.endpoint_url.clone(),
            pubkey_fpr: msg.pubkey_fpr.clone(),
            cert_fpr: msg.cert_fpr.clone(),
            version: msg.version.clone(),
            created_at: msg.created_at,
            last_heartbeat: signed_at,
            revoked_at: None,
            schema_version: msg.schema_version,
            row_msg: Some(row_msg),
            row_token: Some(row_token),
            row_sig: Some(row_sig),
            signed_at: Some(signed_at),
            revocation: None,
        };
        let registry = PgRegistry::connect_lazy(&registry_url, 30).unwrap();
        registry.upsert_self(row).await.unwrap();

        Self {
            tmp,
            home,
            reader,
            registry_url,
            registry,
            fleet_id,
            gateway_id,
            endpoint_url,
        }
    }

    async fn verified_row(&self) -> (GatewayRow, RowVerdict) {
        let registry = open_registry(
            &self.registry_url,
            reader_roots(&self.reader).unwrap(),
            self.fleet_id.clone(),
        )
        .unwrap()
        .with_reader_state(
            RowMarkStore::open(&self.reader).unwrap(),
            RowVerifyCfg::new(Utc::now()),
        );
        let mut rows = registry.discover().await.unwrap();
        assert_eq!(rows.len(), 1, "live fleet must contain only its namespaced row");
        rows.pop().unwrap()
    }

    fn accept_for_reader(&self, row: &GatewayRow) -> RowMark {
        let msg = parse_row_msg_strict(row.row_msg.as_deref().unwrap()).unwrap();
        let roots = reader_roots(&self.reader).unwrap();
        let (claims, _issuer) = keys::verify_token_with_issuer(
            row.row_token.as_deref().unwrap(),
            &roots,
            &SystemClock,
        )
        .unwrap();
        let mut marks = RowMarkStore::open(&self.reader).unwrap();
        marks
            .apply_verified(&self.gateway_id, &msg, &claims)
            .unwrap();
        marks.get(&self.gateway_id).cloned().unwrap()
    }

    fn reader_mark(&self) -> RowMark {
        RowMarkStore::open(&self.reader)
            .unwrap()
            .get(&self.gateway_id)
            .cloned()
            .unwrap()
    }

    fn resign(&self) -> Value {
        let resigned = common::a2a(self.home.root(), self.tmp.path())
            .args(["--json", "mesh", "gateway", "resign"])
            .assert()
            .success();
        let resigned = json_out(&resigned);
        assert_eq!(resigned["command"], "mesh gateway resign");
        assert_eq!(resigned["data"]["gateway_id"], self.gateway_id);
        resigned
    }
}

async fn run_live_case<F, Fut>(registry_url: String, case: F)
where
    F: FnOnce(LiveFixture) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let fixture = LiveFixture::new(registry_url.clone()).await;
    let fleet_id = fixture.fleet_id.clone();
    let result = tokio::spawn(case(fixture)).await;

    let cleanup = PgRegistry::connect_lazy(&registry_url, 30).unwrap();
    cleanup
        .apply_migration(&format!(
            "DELETE FROM mesh_gateways WHERE fleet_id = {}",
            sql_text(&fleet_id)
        ))
        .await
        .unwrap();
    assert!(
        cleanup
            .discover_unverified(&fleet_id)
            .await
            .unwrap()
            .is_empty(),
        "live fixture row was not removed"
    );

    match result {
        Ok(()) => {}
        Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
        Err(error) => panic!("live case task did not finish: {error}"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SignatureColumns {
    row_msg: String,
    row_token: String,
    row_sig: String,
    signed_at: DateTime<Utc>,
}

impl SignatureColumns {
    fn capture(row: &GatewayRow) -> Self {
        Self {
            row_msg: row.row_msg.clone().unwrap(),
            row_token: row.row_token.clone().unwrap(),
            row_sig: row.row_sig.clone().unwrap(),
            signed_at: row.signed_at.unwrap(),
        }
    }

    fn restore_sql(&self, fleet_id: &str, gateway_id: &str) -> String {
        format!(
            "UPDATE mesh_gateways SET row_msg = {}, row_token = {}, row_sig = {}, \
             signed_at = {}::timestamptz WHERE fleet_id = {} AND gateway_id = {}",
            sql_text(&self.row_msg),
            sql_text(&self.row_token),
            sql_text(&self.row_sig),
            sql_text(
                &self
                    .signed_at
                    .to_rfc3339_opts(SecondsFormat::Micros, true)
            ),
            sql_text(fleet_id),
            sql_text(gateway_id),
        )
    }
}

fn set_fleet_id(home: &A2aHome, fleet_id: &str) {
    for name in ["root.key", "root.pub"] {
        let path = home.root().join("mesh/fleet").join(name);
        let mut json: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        json["fleet_id"] = Value::String(fleet_id.to_string());
        fs::write(path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();
    }
}

fn install_reader_root(source: &A2aHome, reader: &A2aHome) {
    let reader_fleet = reader.root().join("mesh/fleet");
    fs::create_dir_all(&reader_fleet).unwrap();
    fs::copy(
        source.root().join("mesh/fleet/root.pub"),
        reader_fleet.join("root.pub"),
    )
    .unwrap();
}

fn trunc_us(value: DateTime<Utc>) -> DateTime<Utc> {
    value
        .with_nanosecond((value.nanosecond() / 1_000) * 1_000)
        .unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn signed_rows_round_trip_on_neon() {
    if !gated("mesh layer-4 signed-row round-trip") {
        return;
    }
    let _serial = live_serial().lock().await;

    run_live_case(required_env(REGISTRY_URL_ENV), |fixture| async move {
        let (row, verdict) = fixture.verified_row().await;
        assert_eq!(verdict, RowVerdict::ValidSigned);
        assert!(row.row_msg.is_some());
        assert!(row.row_sig.is_some());
        assert!(row.row_token.is_some());
        assert!(row.signed_at.is_some());
        fixture.accept_for_reader(&row);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_hand_tampered_endpoint_is_refused_live_and_repaired_by_a_resign() {
    if !gated("mesh layer-4 live tamper and repair") {
        return;
    }
    let _serial = live_serial().lock().await;

    run_live_case(required_env(REGISTRY_URL_ENV), |fixture| async move {
        let (original, original_verdict) = fixture.verified_row().await;
        assert_eq!(original_verdict, RowVerdict::ValidSigned);
        let accepted_mark = fixture.accept_for_reader(&original);
        let fleet_id = &fixture.fleet_id;
        let gateway_id = &fixture.gateway_id;

        // apply_migration is the only public raw-SQL entry point; the live tier is
        // the only caller allowed to use it as one, and it stays inside this file.
        fixture
            .registry
            .apply_migration(&format!(
                "UPDATE mesh_gateways SET endpoint_url = 'wss://tamper.invalid:443' \
                 WHERE fleet_id = '{fleet_id}' AND gateway_id = '{gateway_id}'"
            ))
            .await
            .unwrap();

        let (tampered, tampered_verdict) = fixture.verified_row().await;
        assert_eq!(tampered.endpoint_url.as_deref(), Some(TAMPERED_ENDPOINT));
        assert_eq!(tampered_verdict, RowVerdict::BadSig);
        assert_eq!(tampered_verdict.disposition(false), Disposition::Refused);
        assert_eq!(fixture.reader_mark(), accepted_mark);

        let tampered_rows = vec![(tampered.clone(), tampered_verdict)];
        assert!(discover_outbound_peers("", &tampered_rows, false).is_empty());
        let valid_control = vec![(tampered, RowVerdict::ValidSigned)];
        assert_eq!(discover_outbound_peers("", &valid_control, false).len(), 1);

        fixture.resign();
        let (repaired, repaired_verdict) = fixture.verified_row().await;
        assert_eq!(repaired_verdict, RowVerdict::ValidSigned);
        assert_eq!(
            repaired.endpoint_url.as_deref(),
            Some(fixture.endpoint_url.as_str())
        );
        assert!(repaired.signed_at > original.signed_at);
        fixture.accept_for_reader(&repaired);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_captured_older_row_is_refused_by_the_reader_mark() {
    if !gated("mesh layer-4 live row replay") {
        return;
    }
    let _serial = live_serial().lock().await;

    run_live_case(required_env(REGISTRY_URL_ENV), |fixture| async move {
        let (first, first_verdict) = fixture.verified_row().await;
        assert_eq!(first_verdict, RowVerdict::ValidSigned);
        let first_mark = fixture.accept_for_reader(&first);
        assert_eq!(first_mark.high_water, first.signed_at);
        let captured = SignatureColumns::capture(&first);

        fixture.resign();
        let (newer, newer_verdict) = fixture.verified_row().await;
        assert_eq!(newer_verdict, RowVerdict::ValidSigned);
        assert!(newer.signed_at > first.signed_at);
        let newer_mark = fixture.accept_for_reader(&newer);
        assert_eq!(newer_mark.high_water, newer.signed_at);
        assert!(newer_mark.high_water > first_mark.high_water);

        fixture
            .registry
            .apply_migration(&captured.restore_sql(&fixture.fleet_id, &fixture.gateway_id))
            .await
            .unwrap();
        let restored = fixture
            .registry
            .discover_unverified(&fixture.fleet_id)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(SignatureColumns::capture(&restored), captured);

        let (_replayed, replay_verdict) = fixture.verified_row().await;
        assert_eq!(replay_verdict, RowVerdict::RowReplayed);
        assert_eq!(replay_verdict.label(), "ROW_REPLAYED");
        assert_eq!(replay_verdict.disposition(false), Disposition::Refused);
        assert_eq!(fixture.reader_mark(), newer_mark);
    })
    .await;
}
