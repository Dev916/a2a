use a2a::mesh::gateway::expose::{
    run_exposure_loop, run_exposure_loop_with_events, Exposure, ExposurePublisher, ExposureRow,
    ManualExposure, NgrokApi, NgrokExposure, NgrokTunnel, NgrokTunnelConfig, NgrokTunnels,
    NoneExposure, RowSigningContext,
};
use a2a::mesh::gateway::link::{accept_once_io, dial_gateway_io};
use a2a::mesh::gateway::{gateway_root, run_registry_slot_for_test, RegistrySlotParams};
use a2a::mesh::keys::{self, Clock, SystemClock};
use a2a::mesh::registry::{
    ClaimOutcome, FileRegistry, GatewayRow, Registry, TransitionOutcome, WorkListFilter,
    WorkOwner, WorkPatch, WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION,
};
use a2a::mesh::row_sig::{self, RowMsg, RowSigFault, RowVerdict};
use a2a::mesh::wire::TlsIdentity;
use a2a::output::{AppError, ExitCode};
use assert_cmd::Command;
use chrono::{DateTime, TimeZone, Utc};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

struct SignedPublisherFixture {
    home: a2a::home::A2aHome,
    cfg: a2a::config::Config,
    registry: FileRegistry,
    registry_dir: PathBuf,
    row: ExposureRow,
    fleet_id: String,
    gateway_id: String,
    root_fpr: String,
}

fn signed_publisher_fixture(root: &Path, manual_endpoint: Option<&str>) -> SignedPublisherFixture {
    let home = a2a::home::A2aHome::resolve(Some(&root.join("home"))).unwrap();
    keys::fleet_init(&home).unwrap();
    let identity = keys::gateway_identity(&home, false).unwrap();
    keys::ensure_gateway_token(&home, &SystemClock).unwrap();
    if let Some(endpoint) = manual_endpoint {
        std::fs::write(
            home.root().join("config.toml"),
            format!(
                "[mesh.gateway]\nexpose = \"manual\"\nendpoint_url = \"{endpoint}\"\n"
            ),
        )
        .unwrap();
    }
    let cfg = a2a::config::load(&home).unwrap().0;
    let registry_dir = root.join("registry");
    let registry = FileRegistry::new(&registry_dir);
    registry.init_schema().unwrap();
    a2a::mesh::registry::write_registry_env(
        &home,
        &format!("file:{}", registry_dir.display()),
    )
    .unwrap();
    let fleet_id = keys::fleet_id(&home).unwrap();
    let gateway_id = identity.gateway_id.clone();
    let row = ExposureRow {
        fleet_id: fleet_id.clone(),
        gateway_id: gateway_id.clone(),
        owner: "signed-test-owner".to_string(),
        pubkey_fpr: keys::gateway_public_key_fingerprint(&home).unwrap(),
        cert_fpr: identity.cert_fpr,
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    SignedPublisherFixture {
        root_fpr: keys::fleet_root_fingerprint(&home).unwrap(),
        home,
        cfg,
        registry,
        registry_dir,
        row,
        fleet_id,
        gateway_id,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn writer_verdict(
    home: &a2a::home::A2aHome,
    row: &GatewayRow,
) -> (RowVerdict, Option<RowMsg>) {
    let (row_msg, row_token, row_sig, signed_at) =
        match (&row.row_msg, &row.row_token, &row.row_sig, row.signed_at) {
            (Some(row_msg), Some(row_token), Some(row_sig), Some(signed_at)) => {
                (row_msg, row_token, row_sig, signed_at)
            }
            (None, None, None, None) => return (RowVerdict::Unsigned { stripped: false }, None),
            _ => return (RowVerdict::RowSigIncomplete, None),
        };
    let pubkey = keys::gateway_public_key_b64(home).unwrap();
    let parsed = match row_sig::verify_row_bytes(row_msg, row_sig, &pubkey) {
        Ok(parsed) => parsed,
        Err(RowSigFault::UnknownVersion(_)) => {
            return (RowVerdict::RowSigUnknownVersion, None)
        }
        Err(_) => return (RowVerdict::BadSig, None),
    };
    let projection_matches = parsed.fleet_id == row.fleet_id
        && parsed.gateway_id == row.gateway_id
        && parsed.endpoint_url == row.endpoint_url
        && parsed.pubkey_fpr == row.pubkey_fpr
        && parsed.cert_fpr == row.cert_fpr
        && parsed.version == row.version
        && parsed.owner == row.owner
        && parsed.created_at == row.created_at
        && parsed.schema_version == row.schema_version
        && parsed.signed_at == signed_at
        && parsed.row_token_sha256 == sha256_hex(row_token.as_bytes());
    if !projection_matches {
        return (RowVerdict::BadSig, Some(parsed));
    }
    (RowVerdict::ValidSigned, Some(parsed))
}

fn rewrite_registry_row(registry_dir: &Path, mutate: impl FnOnce(&mut GatewayRow)) {
    let path = registry_dir.join("mesh_gateways.json");
    let mut rows: Vec<GatewayRow> =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    mutate(rows.first_mut().unwrap());
    std::fs::write(path, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
}

fn cli(home: &Path) -> Command {
    let mut command = Command::cargo_bin("a2a").unwrap();
    command
        .arg("--home")
        .arg(home)
        .env_remove("A2A_HOME")
        .env_remove("DATABASE_URL");
    command
}

#[tokio::test]
async fn none_and_manual_exposures_return_pinned_urls() {
    assert_eq!(NoneExposure.current_url().await.unwrap(), None);

    let manual = ManualExposure::new(Some("wss://example.test:7787".to_string()));
    assert_eq!(
        manual.current_url().await.unwrap(),
        Some("wss://example.test:7787".to_string())
    );
}

#[tokio::test]
async fn publisher_tick_writes_a_row_that_verifies_as_valid_signed() {
    let tmp = tempfile::tempdir().unwrap();
    let fixture = signed_publisher_fixture(tmp.path(), None);
    let endpoint = Some("wss://signed.example.test:7787".to_string());
    let exposure = ManualExposure::new(endpoint.clone());
    let mut publisher = ExposurePublisher::new(fixture.row.clone()).with_signing(
        RowSigningContext::from_config(&fixture.home, &fixture.cfg),
    );
    let now = Utc.timestamp_opt(1_700_000_000, 123_456_000).unwrap();

    let tick = publisher
        .tick(&fixture.registry, &exposure, now)
        .await
        .unwrap();
    assert!(tick.resigned);
    let row = fixture
        .registry
        .discover_unverified(&fixture.fleet_id)
        .await
        .unwrap()
        .remove(0);
    let (verdict, parsed) = writer_verdict(&fixture.home, &row);
    let parsed = parsed.unwrap();

    assert_eq!(verdict, RowVerdict::ValidSigned);
    assert!(row.row_msg.is_some());
    assert_eq!(row.signed_at, Some(parsed.signed_at));
    assert_eq!(
        parsed.enforce_require_signed_rows,
        fixture.cfg.mesh.gateway.require_signed_rows
    );
    assert_eq!(parsed.row_resign_secs, 86_400);
    assert_eq!(
        parsed.roots_held,
        serde_json::to_string(&vec![fixture.root_fpr]).unwrap()
    );
    assert_eq!(row.endpoint_url, endpoint);
}

#[tokio::test]
async fn unchanged_signed_rows_heartbeat_until_the_resign_cadence() {
    let tmp = tempfile::tempdir().unwrap();
    let fixture = signed_publisher_fixture(tmp.path(), None);
    let exposure = ManualExposure::new(Some("wss://cadence.example.test:7787".to_string()));
    let mut publisher = ExposurePublisher::new(fixture.row.clone()).with_signing(
        RowSigningContext::from_config(&fixture.home, &fixture.cfg),
    );
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    assert!(
        publisher
            .tick(&fixture.registry, &exposure, t0)
            .await
            .unwrap()
            .resigned
    );

    let heartbeat = publisher
        .tick(
            &fixture.registry,
            &exposure,
            t0 + chrono::Duration::seconds(1),
        )
        .await
        .unwrap();
    assert!(!heartbeat.resigned);
    let row = fixture.registry.discover_unverified(&fixture.fleet_id).await.unwrap().remove(0);
    assert_eq!(row.signed_at, Some(t0));
    assert_eq!(row.last_heartbeat, t0 + chrono::Duration::seconds(1));

    let cadence = publisher
        .tick(
            &fixture.registry,
            &exposure,
            t0 + chrono::Duration::seconds(86_400),
        )
        .await
        .unwrap();
    assert!(cadence.resigned);
    let row = fixture.registry.discover_unverified(&fixture.fleet_id).await.unwrap().remove(0);
    assert_eq!(row.signed_at, Some(t0 + chrono::Duration::seconds(86_400)));
    assert_eq!(writer_verdict(&fixture.home, &row).0, RowVerdict::ValidSigned);
}

#[tokio::test]
async fn publisher_reloads_the_gateway_token_and_held_roots_each_tick() {
    let tmp = tempfile::tempdir().unwrap();
    let fixture = signed_publisher_fixture(tmp.path(), None);
    let exposure = ManualExposure::new(Some("wss://roots.example.test:7787".to_string()));
    let mut publisher = ExposurePublisher::new(fixture.row.clone()).with_signing(
        RowSigningContext::from_config(&fixture.home, &fixture.cfg),
    );
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    publisher
        .tick(&fixture.registry, &exposure, t0)
        .await
        .unwrap();
    let old_token = keys::gateway_token(&fixture.home).unwrap();
    let rotation = keys::rotate_root(&fixture.home, &SystemClock).unwrap();
    assert!(rotation.gateway_token_rotated);

    let tick = publisher
        .tick(
            &fixture.registry,
            &exposure,
            t0 + chrono::Duration::seconds(1),
        )
        .await
        .unwrap();
    assert!(tick.resigned);
    let row = fixture.registry.discover_unverified(&fixture.fleet_id).await.unwrap().remove(0);
    assert_ne!(row.row_token.as_deref(), Some(old_token.as_str()));
    let parsed = writer_verdict(&fixture.home, &row).1.unwrap();
    let held: Vec<String> = serde_json::from_str(&parsed.roots_held).unwrap();
    assert_eq!(held.len(), 2);
    assert!(held.contains(&rotation.current_fingerprint));
    assert!(held.contains(&rotation.next_fingerprint));
    assert_eq!(writer_verdict(&fixture.home, &row).0, RowVerdict::ValidSigned);
}

#[tokio::test]
async fn restarted_publisher_signs_the_registrys_original_created_at() {
    let tmp = tempfile::tempdir().unwrap();
    let fixture = signed_publisher_fixture(tmp.path(), None);
    let exposure = ManualExposure::new(Some("wss://restart.example.test:7787".to_string()));
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let mut first = ExposurePublisher::new(fixture.row.clone()).with_signing(
        RowSigningContext::from_config(&fixture.home, &fixture.cfg),
    );
    first
        .tick(&fixture.registry, &exposure, t0)
        .await
        .unwrap();
    drop(first);

    let mut restarted = ExposurePublisher::new(fixture.row.clone()).with_signing(
        RowSigningContext::from_config(&fixture.home, &fixture.cfg),
    );
    let tick = restarted
        .tick(
            &fixture.registry,
            &exposure,
            t0 + chrono::Duration::hours(1),
        )
        .await
        .unwrap();
    assert!(tick.resigned);
    let row = fixture
        .registry
        .discover_unverified(&fixture.fleet_id)
        .await
        .unwrap()
        .remove(0);
    let (verdict, parsed) = writer_verdict(&fixture.home, &row);

    assert_eq!(verdict, RowVerdict::ValidSigned);
    assert_eq!(parsed.unwrap().created_at, t0);
    assert_eq!(row.created_at, t0);
}

#[tokio::test]
async fn tampered_own_row_is_detected_and_repaired_within_one_tick() {
    let tmp = tempfile::tempdir().unwrap();
    let fixture = signed_publisher_fixture(tmp.path(), None);
    let honest = "wss://honest.example.test:7787";
    let exposure = ManualExposure::new(Some(honest.to_string()));
    let mut publisher = ExposurePublisher::new(fixture.row.clone()).with_signing(
        RowSigningContext::from_config(&fixture.home, &fixture.cfg),
    );
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    publisher
        .tick(&fixture.registry, &exposure, t0)
        .await
        .unwrap();
    rewrite_registry_row(&fixture.registry_dir, |row| {
        row.endpoint_url = Some("wss://evil.test:1".to_string());
    });

    let tick = publisher
        .tick(
            &fixture.registry,
            &exposure,
            t0 + chrono::Duration::seconds(1),
        )
        .await
        .unwrap();
    assert_eq!(tick.self_tamper, Some("BAD_SIG".to_string()));
    assert!(tick.resigned);
    let row = fixture
        .registry
        .discover_unverified(&fixture.fleet_id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(row.endpoint_url.as_deref(), Some(honest));
    assert_eq!(writer_verdict(&fixture.home, &row).0, RowVerdict::ValidSigned);
}

#[tokio::test]
async fn registry_loop_logs_one_self_tamper_transition_and_repairs_the_row() {
    let tmp = tempfile::tempdir().unwrap();
    let fixture = signed_publisher_fixture(tmp.path(), None);
    let honest = "wss://logged.example.test:7787";
    let exposure = ManualExposure::new(Some(honest.to_string()));
    let clock = StepClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let token = CancellationToken::new();
    let root = gateway_root(&fixture.home);
    let run = run_registry_slot_for_test(
        &root,
        &fixture.cfg,
        RegistrySlotParams {
            registry: &fixture.registry,
            exposure: &exposure,
            publisher: ExposurePublisher::new(fixture.row.clone()).with_signing(
                RowSigningContext::from_config(&fixture.home, &fixture.cfg),
            ),
            heartbeat_interval: Duration::from_millis(20),
            clock: &clock,
            stall_shutdown_slot: None,
        },
        token.clone(),
    );
    tokio::pin!(run);
    wait_for_row_in_fleet_while_running(
        &mut run,
        &fixture.registry,
        &fixture.fleet_id,
        Some(honest),
    )
    .await;
    rewrite_registry_row(&fixture.registry_dir, |row| {
        row.endpoint_url = Some("wss://evil.test:1".to_string());
    });
    clock.set(Utc.timestamp_opt(1_700_000_001, 0).unwrap());

    let line = wait_for_log_line_while_running(
        &mut run,
        &root,
        "\"verdict\":\"BAD_SIG\"",
    )
    .await;
    assert!(line.contains("\"kind\":\"row_self_tamper_detected\""));
    assert!(line.contains(&format!("\"gateway_id\":\"{}\"", fixture.gateway_id)));
    wait_for_row_in_fleet_while_running(
        &mut run,
        &fixture.registry,
        &fixture.fleet_id,
        Some(honest),
    )
    .await;
    for _ in 0..4 {
        tokio::select! {
            result = &mut run => panic!("registry slot exited unexpectedly: {:?}", result),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
    assert_eq!(count_log_lines(&root, "\"verdict\":\"BAD_SIG\""), 1);
    let row = fixture
        .registry
        .discover_unverified(&fixture.fleet_id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(writer_verdict(&fixture.home, &row).0, RowVerdict::ValidSigned);

    token.cancel();
    tokio::select! {
        result = &mut run => result.unwrap(),
        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("registry slot did not stop after cancellation"),
    }
}

#[tokio::test]
async fn unexposing_publishes_a_valid_signed_row_with_a_null_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let fixture = signed_publisher_fixture(tmp.path(), None);
    let exposure = RotatingExposure::new([
        Some("wss://visible.example.test:7787"),
        None,
    ]);
    let mut publisher = ExposurePublisher::new(fixture.row.clone()).with_signing(
        RowSigningContext::from_config(&fixture.home, &fixture.cfg),
    );
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    publisher
        .tick(&fixture.registry, &exposure, t0)
        .await
        .unwrap();

    let tick = publisher
        .tick(
            &fixture.registry,
            &exposure,
            t0 + chrono::Duration::seconds(1),
        )
        .await
        .unwrap();
    assert!(tick.resigned);
    let row = fixture
        .registry
        .discover_unverified(&fixture.fleet_id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(row.endpoint_url, None);
    assert_eq!(row.cert_fpr, None);
    assert_eq!(writer_verdict(&fixture.home, &row).0, RowVerdict::ValidSigned);
}

#[tokio::test]
async fn gateway_resign_republishes_a_freshly_signed_row() {
    let tmp = tempfile::tempdir().unwrap();
    let honest = "wss://resign.example.test:7787";
    let fixture = signed_publisher_fixture(tmp.path(), Some(honest));
    let exposure = ManualExposure::new(Some(honest.to_string()));
    let mut publisher = ExposurePublisher::new(fixture.row.clone()).with_signing(
        RowSigningContext::from_config(&fixture.home, &fixture.cfg),
    );
    let old_time = Utc::now() - chrono::Duration::hours(1);
    publisher
        .tick(&fixture.registry, &exposure, old_time)
        .await
        .unwrap();
    let old_signed_at = fixture
        .registry
        .discover_unverified(&fixture.fleet_id)
        .await
        .unwrap()[0]
        .signed_at
        .unwrap();
    rewrite_registry_row(&fixture.registry_dir, |row| {
        row.endpoint_url = Some("wss://evil.test:1".to_string());
    });

    let output = cli(fixture.home.root())
        .args(["--json", "mesh", "gateway", "resign"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(output["command"], "mesh gateway resign");
    assert_eq!(output["data"]["gateway_id"], fixture.gateway_id);
    assert_eq!(
        output["data"]["row_msg_sha256"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    let row = fixture
        .registry
        .discover_unverified(&fixture.fleet_id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(row.endpoint_url.as_deref(), Some(honest));
    assert!(row.signed_at.unwrap() > old_signed_at);
    assert_eq!(
        output["data"]["signed_at"],
        serde_json::to_value(row.signed_at.unwrap()).unwrap()
    );
    assert_eq!(writer_verdict(&fixture.home, &row).0, RowVerdict::ValidSigned);
}

#[tokio::test]
async fn install_token_forces_an_immediate_registry_resign() {
    let tmp = tempfile::tempdir().unwrap();
    let endpoint = "wss://install-token.example.test:7787";
    let fixture = signed_publisher_fixture(tmp.path(), Some(endpoint));
    let exposure = ManualExposure::new(Some(endpoint.to_string()));
    let mut publisher = ExposurePublisher::new(fixture.row.clone()).with_signing(
        RowSigningContext::from_config(&fixture.home, &fixture.cfg),
    );
    publisher
        .tick(
            &fixture.registry,
            &exposure,
            Utc::now() - chrono::Duration::hours(1),
        )
        .await
        .unwrap();
    let before = fixture
        .registry
        .discover_unverified(&fixture.fleet_id)
        .await
        .unwrap()[0]
        .signed_at
        .unwrap();
    let token = keys::gateway_token(&fixture.home).unwrap();

    let output = cli(fixture.home.root())
        .args([
            "--json",
            "mesh",
            "gateway",
            "install-token",
            "--file",
            "-",
        ])
        .write_stdin(token)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(output["command"], "mesh gateway install-token");
    assert!(output["data"]["signed_at"].is_string());
    let row = fixture
        .registry
        .discover_unverified(&fixture.fleet_id)
        .await
        .unwrap()
        .remove(0);
    assert!(row.signed_at.unwrap() > before);
    assert_eq!(row.row_token.as_deref(), Some(keys::gateway_token(&fixture.home).unwrap().as_str()));
    assert_eq!(writer_verdict(&fixture.home, &row).0, RowVerdict::ValidSigned);
}

#[tokio::test]
async fn ngrok_tcp_exposure_reuses_starts_and_maps_agent_urls_to_wss() {
    let fake = FakeNgrok::new(vec![NgrokTunnels { tunnels: vec![] }]);
    let ngrok = NgrokExposure::with_api(7787, fake);

    assert_eq!(
        ngrok.current_url().await.unwrap(),
        Some("wss://1.tcp.ngrok.test:19001".to_string())
    );
    assert_eq!(
        ngrok.api_requests(),
        vec!["GET /api/tunnels", "POST /api/tunnels"]
    );

    let fake = FakeNgrok::new(vec![NgrokTunnels {
        tunnels: vec![NgrokTunnel {
            proto: Some("tcp".to_string()),
            public_url: "tcp://2.tcp.ngrok.test:19002".to_string(),
            config: Some(NgrokTunnelConfig {
                addr: "localhost:7787".to_string(),
            }),
        }],
    }]);
    let ngrok = NgrokExposure::with_api(7787, fake);
    assert_eq!(
        ngrok.current_url().await.unwrap(),
        Some("wss://2.tcp.ngrok.test:19002".to_string())
    );
    assert_eq!(ngrok.api_requests(), vec!["GET /api/tunnels"]);
}

#[tokio::test]
async fn ngrok_unavailable_is_typed_and_names_install_step() {
    let ngrok = NgrokExposure::with_api_base(7787, "http://127.0.0.1:1".to_string());
    let err = ngrok.current_url().await.unwrap_err();
    assert_eq!(err.code, "NGROK_UNAVAILABLE");
    assert_eq!(err.exit, ExitCode::BrokerUnavailable);
    assert!(err.remedy.unwrap().contains("install ngrok"));
}

#[tokio::test]
async fn changed_exposure_reupserts_file_registry_row_with_cert_fingerprint() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    registry.init_schema().unwrap();
    let exposure = RotatingExposure::new([
        Some("wss://old.example.test:10000"),
        Some("wss://old.example.test:10000"),
        Some("wss://new.example.test:10001"),
        None,
    ]);
    let row = ExposureRow {
        fleet_id: "fleet-a".to_string(),
        gateway_id: "gw-a".to_string(),
        owner: "owner-a".to_string(),
        pubkey_fpr: "pub-a".to_string(),
        cert_fpr: "cert-a".to_string(),
        version: "test".to_string(),
    };
    let mut publisher = ExposurePublisher::new(row);

    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    assert!(
        publisher
            .tick(&registry, &exposure, t0)
            .await
            .unwrap()
            .upserted
    );
    assert_eq!(
        registry.discover_unverified("fleet-a").await.unwrap()[0]
            .endpoint_url
            .as_deref(),
        Some("wss://old.example.test:10000")
    );
    assert_eq!(
        registry.discover_unverified("fleet-a").await.unwrap()[0]
            .cert_fpr
            .as_deref(),
        Some("cert-a")
    );

    let t1 = Utc.timestamp_opt(1_700_000_001, 0).unwrap();
    let same = publisher.tick(&registry, &exposure, t1).await.unwrap();
    assert!(!same.upserted);
    assert_eq!(
        registry.discover_unverified("fleet-a").await.unwrap()[0].last_heartbeat,
        t1
    );

    let t2 = Utc.timestamp_opt(1_700_000_002, 0).unwrap();
    assert!(
        publisher
            .tick(&registry, &exposure, t2)
            .await
            .unwrap()
            .upserted
    );
    assert_eq!(
        registry.discover_unverified("fleet-a").await.unwrap()[0]
            .endpoint_url
            .as_deref(),
        Some("wss://new.example.test:10001")
    );

    let t3 = Utc.timestamp_opt(1_700_000_003, 0).unwrap();
    assert!(
        publisher
            .tick(&registry, &exposure, t3)
            .await
            .unwrap()
            .upserted
    );
    assert_eq!(
        registry.discover_unverified("fleet-a").await.unwrap()[0].endpoint_url,
        None
    );
}

#[tokio::test]
async fn upsert_failure_still_heartbeats_and_records_the_registry_error() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FlippingRegistry::with_failure(tmp.path(), 1, registry_schema_stale_failure);
    registry.inner.init_schema().unwrap();
    let exposure = ManualExposure::new(Some("wss://changed.example.test:7787".to_string()));
    let row = ExposureRow {
        fleet_id: "fleet-a".to_string(),
        gateway_id: "gw-a".to_string(),
        owner: "owner-a".to_string(),
        pubkey_fpr: "pub-a".to_string(),
        cert_fpr: "cert-a".to_string(),
        version: "test".to_string(),
    };
    let mut publisher = ExposurePublisher::new(row);

    let tick = publisher
        .tick(
            &registry,
            &exposure,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        )
        .await
        .unwrap();

    assert!(!tick.upserted);
    assert!(
        tick.registry_error
            .expect("upsert failure is recorded")
            .starts_with("REGISTRY_SCHEMA_STALE")
    );
    assert_eq!(registry.heartbeat_calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn exposure_loop_rechecks_on_heartbeat_interval_and_reupserts_rotated_url() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    registry.init_schema().unwrap();
    let exposure = RotatingExposure::new([
        Some("wss://old.example.test:10000"),
        Some("wss://new.example.test:10001"),
    ]);
    let clock = Arc::new(StepClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap()));
    let token = CancellationToken::new();
    let row = ExposureRow {
        fleet_id: "fleet-a".to_string(),
        gateway_id: "gw-a".to_string(),
        owner: "owner-a".to_string(),
        pubkey_fpr: "pub-a".to_string(),
        cert_fpr: "cert-a".to_string(),
        version: "test".to_string(),
    };
    let mut publisher = ExposurePublisher::new(row);
    let run_registry = registry.clone();
    let run_clock = Arc::clone(&clock);
    let run_token = token.clone();
    let task = tokio::spawn(async move {
        run_exposure_loop(
            &run_registry,
            &exposure,
            &mut publisher,
            Duration::from_secs(10),
            run_clock.as_ref(),
            run_token,
        )
        .await
    });

    wait_for_row(&registry, Some("wss://old.example.test:10000")).await;
    clock.set(Utc.timestamp_opt(1_700_000_011, 0).unwrap());
    tokio::time::advance(Duration::from_secs(11)).await;
    wait_for_row(&registry, Some("wss://new.example.test:10001")).await;

    token.cancel();
    tokio::time::advance(Duration::from_secs(11)).await;
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn real_registry_slot_with_fake_ngrok_publishes_row_and_health_endpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let home = a2a::home::A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
    keys::fleet_init(&home).unwrap();
    let identity = keys::gateway_identity(&home, false).unwrap();
    keys::ensure_gateway_token(&home, &SystemClock).unwrap();

    let registry = FileRegistry::new(tmp.path().join("registry"));
    registry.init_schema().unwrap();
    let ngrok =
        NgrokExposure::with_api(7787, FakeNgrok::new(vec![NgrokTunnels { tunnels: vec![] }]));
    let cfg = a2a::config::load(&home).unwrap().0;
    let fleet_id = keys::fleet_id(&home).unwrap();
    let pubkey_fpr = keys::gateway_public_key_fingerprint(&home).unwrap();
    let row = ExposureRow {
        fleet_id: fleet_id.clone(),
        gateway_id: identity.gateway_id.clone(),
        owner: "test-owner".to_string(),
        pubkey_fpr: pubkey_fpr.clone(),
        cert_fpr: identity.cert_fpr.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let token = CancellationToken::new();
    let root = gateway_root(&home);
    let run = run_registry_slot_for_test(
        &root,
        &cfg,
        RegistrySlotParams {
            registry: &registry,
            exposure: &ngrok,
            publisher: ExposurePublisher::new(row),
            heartbeat_interval: Duration::from_secs(60),
            clock: &SystemClock,
            stall_shutdown_slot: None,
        },
        token.clone(),
    );
    tokio::pin!(run);
    let mut published = false;
    for _ in 0..100 {
        tokio::select! {
            result = &mut run => panic!("registry slot exited before publish: {:?}", result),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                let rows = registry.discover_unverified(&fleet_id).await.unwrap();
                let health_endpoint = std::fs::read_to_string(home.root().join("mesh/gateway-health.json"))
                    .ok()
                    .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
                    .and_then(|health| health["endpoint_url"].as_str().map(str::to_string));
                if rows
                    .first()
                    .map(|row| row.endpoint_url.as_deref() == Some("wss://1.tcp.ngrok.test:19001"))
                    .unwrap_or(false)
                    && health_endpoint.as_deref() == Some("wss://1.tcp.ngrok.test:19001")
                {
                    published = true;
                    break;
                }
            }
        }
    }
    assert!(
        published,
        "registry slot did not publish row and health endpoint"
    );
    let rows = registry.discover_unverified(&fleet_id).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].gateway_id, identity.gateway_id);
    assert_eq!(
        rows[0].endpoint_url.as_deref(),
        Some("wss://1.tcp.ngrok.test:19001")
    );
    assert_eq!(
        rows[0].cert_fpr.as_deref(),
        Some(identity.cert_fpr.as_str())
    );
    assert_eq!(rows[0].pubkey_fpr, pubkey_fpr);
    assert_eq!(rows[0].version, env!("CARGO_PKG_VERSION"));

    token.cancel();
    tokio::select! {
        result = &mut run => result.unwrap(),
        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("registry slot did not stop after cancellation"),
    }
}

#[tokio::test]
async fn registry_slot_keeps_heartbeating_when_ngrok_resolution_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let home = a2a::home::A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
    keys::fleet_init(&home).unwrap();
    let identity = keys::gateway_identity(&home, false).unwrap();
    keys::ensure_gateway_token(&home, &SystemClock).unwrap();

    let registry = FileRegistry::new(tmp.path().join("registry"));
    registry.init_schema().unwrap();
    let cfg = a2a::config::load(&home).unwrap().0;
    let fleet_id = keys::fleet_id(&home).unwrap();
    let pubkey_fpr = keys::gateway_public_key_fingerprint(&home).unwrap();
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    registry
        .upsert_self(GatewayRow {
            fleet_id: fleet_id.clone(),
            gateway_id: identity.gateway_id.clone(),
            owner: "test-owner".to_string(),
            endpoint_url: Some("wss://stale.tcp.ngrok.test:19000".to_string()),
            pubkey_fpr: pubkey_fpr.clone(),
            cert_fpr: Some(identity.cert_fpr.clone()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: t0,
            last_heartbeat: t0,
            revoked_at: None,
            schema_version: REGISTRY_SCHEMA_VERSION,
            row_msg: None,
            row_token: None,
            row_sig: None,
            signed_at: None,
            revocation: None,
        })
        .await
        .unwrap();

    let row = ExposureRow {
        fleet_id: fleet_id.clone(),
        gateway_id: identity.gateway_id.clone(),
        owner: "test-owner".to_string(),
        pubkey_fpr,
        cert_fpr: identity.cert_fpr,
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let ngrok = NgrokExposure::with_api(7787, FakeNgrok::always_error("agent API refused"));
    let clock = Arc::new(StepClock::new(t0));
    let token = CancellationToken::new();
    let root = gateway_root(&home);
    let run = run_registry_slot_for_test(
        &root,
        &cfg,
        RegistrySlotParams {
            registry: &registry,
            exposure: &ngrok,
            publisher: ExposurePublisher::new(row),
            heartbeat_interval: Duration::from_millis(20),
            clock: clock.as_ref(),
            stall_shutdown_slot: None,
        },
        token.clone(),
    );
    tokio::pin!(run);

    let t1 = Utc.timestamp_opt(1_700_000_001, 0).unwrap();
    clock.set(t1);
    let first = wait_for_heartbeat_after_while_running(&mut run, &registry, &fleet_id, t0).await;
    assert_eq!(
        first.endpoint_url.as_deref(),
        Some("wss://stale.tcp.ngrok.test:19000")
    );

    let t2 = Utc.timestamp_opt(1_700_000_002, 0).unwrap();
    clock.set(t2);
    let second = wait_for_heartbeat_after_while_running(&mut run, &registry, &fleet_id, t1).await;
    assert_eq!(
        second.endpoint_url.as_deref(),
        Some("wss://stale.tcp.ngrok.test:19000")
    );

    let health = wait_for_health_while_running(&mut run, &root, |health| {
        health["exposure_error"].is_string()
    })
    .await;
    assert!(health["exposure_error"]
        .as_str()
        .unwrap()
        .contains("agent API refused"));
    assert!(health["exposure_error_since"].is_string());
    let log = wait_for_log_line_while_running(&mut run, &root, "\"kind\":\"exposure_error\"").await;
    assert!(log.contains("\"state\":\"down\""));
    assert_eq!(
        count_log_lines(&root, "\"kind\":\"exposure_error\""),
        1,
        "down->still-down must not spam exposure_error log lines"
    );

    token.cancel();
    tokio::select! {
        result = &mut run => result.unwrap(),
        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("registry slot did not stop after cancellation"),
    }
}

#[tokio::test]
async fn registry_slot_clears_exposure_error_after_ngrok_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let home = a2a::home::A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
    keys::fleet_init(&home).unwrap();
    let identity = keys::gateway_identity(&home, false).unwrap();
    keys::ensure_gateway_token(&home, &SystemClock).unwrap();

    let registry = FileRegistry::new(tmp.path().join("registry"));
    registry.init_schema().unwrap();
    let cfg = a2a::config::load(&home).unwrap().0;
    let fleet_id = keys::fleet_id(&home).unwrap();
    let pubkey_fpr = keys::gateway_public_key_fingerprint(&home).unwrap();
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    registry
        .upsert_self(GatewayRow {
            fleet_id: fleet_id.clone(),
            gateway_id: identity.gateway_id.clone(),
            owner: "test-owner".to_string(),
            endpoint_url: Some("wss://stale.tcp.ngrok.test:19000".to_string()),
            pubkey_fpr: pubkey_fpr.clone(),
            cert_fpr: Some(identity.cert_fpr.clone()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: t0,
            last_heartbeat: t0,
            revoked_at: None,
            schema_version: REGISTRY_SCHEMA_VERSION,
            row_msg: None,
            row_token: None,
            row_sig: None,
            signed_at: None,
            revocation: None,
        })
        .await
        .unwrap();

    let row = ExposureRow {
        fleet_id: fleet_id.clone(),
        gateway_id: identity.gateway_id.clone(),
        owner: "test-owner".to_string(),
        pubkey_fpr,
        cert_fpr: identity.cert_fpr,
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let ngrok = NgrokExposure::with_api(
        7787,
        FakeNgrok::scripted(vec![
            Err("agent API refused"),
            Err("agent API refused"),
            Ok(NgrokTunnels { tunnels: vec![] }),
        ]),
    );
    let clock = Arc::new(StepClock::new(t0));
    let token = CancellationToken::new();
    let root = gateway_root(&home);
    let run = run_registry_slot_for_test(
        &root,
        &cfg,
        RegistrySlotParams {
            registry: &registry,
            exposure: &ngrok,
            publisher: ExposurePublisher::new(row),
            heartbeat_interval: Duration::from_millis(20),
            clock: clock.as_ref(),
            stall_shutdown_slot: None,
        },
        token.clone(),
    );
    tokio::pin!(run);

    let t1 = Utc.timestamp_opt(1_700_000_001, 0).unwrap();
    clock.set(t1);
    wait_for_health_while_running(&mut run, &root, |health| {
        health["exposure_error"].is_string()
    })
    .await;

    let t2 = Utc.timestamp_opt(1_700_000_002, 0).unwrap();
    clock.set(t2);
    wait_for_row_in_fleet_while_running(
        &mut run,
        &registry,
        &fleet_id,
        Some("wss://1.tcp.ngrok.test:19001"),
    )
    .await;
    let health = wait_for_health_while_running(&mut run, &root, |health| {
        health["endpoint_url"].as_str() == Some("wss://1.tcp.ngrok.test:19001")
            && health["exposure_error"].is_null()
            && health["exposure_error_since"].is_null()
    })
    .await;
    assert_eq!(
        health["endpoint_url"].as_str(),
        Some("wss://1.tcp.ngrok.test:19001")
    );
    let recovery = wait_for_log_line_while_running(&mut run, &root, "\"state\":\"recovered\"").await;
    assert!(recovery.contains("\"kind\":\"exposure_error\""));

    token.cancel();
    tokio::select! {
        result = &mut run => result.unwrap(),
        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("registry slot did not stop after cancellation"),
    }
}

#[tokio::test]
async fn registry_slot_retries_registry_write_failures_and_reports_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let home = a2a::home::A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
    keys::fleet_init(&home).unwrap();
    let identity = keys::gateway_identity(&home, false).unwrap();
    keys::ensure_gateway_token(&home, &SystemClock).unwrap();

    let registry = FlippingRegistry::new(tmp.path().join("registry"), 3);
    registry.inner.init_schema().unwrap();
    let cfg = a2a::config::load(&home).unwrap().0;
    let fleet_id = keys::fleet_id(&home).unwrap();
    let row = ExposureRow {
        fleet_id: fleet_id.clone(),
        gateway_id: identity.gateway_id,
        owner: "test-owner".to_string(),
        pubkey_fpr: keys::gateway_public_key_fingerprint(&home).unwrap(),
        cert_fpr: identity.cert_fpr,
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let exposure = ManualExposure::new(Some("wss://manual.example.test:7787".to_string()));
    let clock = Arc::new(StepClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap()));
    let token = CancellationToken::new();
    let root = gateway_root(&home);
    let run = run_registry_slot_for_test(
        &root,
        &cfg,
        RegistrySlotParams {
            registry: &registry,
            exposure: &exposure,
            publisher: ExposurePublisher::new(row),
            heartbeat_interval: Duration::from_millis(20),
            clock: clock.as_ref(),
            stall_shutdown_slot: None,
        },
        token.clone(),
    );
    tokio::pin!(run);

    let health = wait_for_health_while_running(&mut run, &root, |health| {
        health["registry_error"].is_string()
            && health["registry_error_since"].is_string()
            && health["loops"]["registry"]["restarts"].as_u64() == Some(0)
    })
    .await;
    assert!(health["registry_error"]
        .as_str()
        .unwrap()
        .contains("registry test failure"));
    let down = wait_for_log_line_while_running(&mut run, &root, "\"kind\":\"registry_error\"").await;
    assert!(down.contains("\"state\":\"down\""));
    assert_eq!(
        count_log_lines(&root, "\"kind\":\"registry_error\""),
        1,
        "down->still-down must not spam registry_error log lines"
    );

    clock.set(Utc.timestamp_opt(1_700_000_004, 0).unwrap());
    wait_for_row_in_fleet_while_running(
        &mut run,
        &registry.inner,
        &fleet_id,
        Some("wss://manual.example.test:7787"),
    )
    .await;
    let health = wait_for_health_while_running(&mut run, &root, |health| {
        health["registry_error"].is_null()
            && health["registry_error_since"].is_null()
            && health["endpoint_url"].as_str() == Some("wss://manual.example.test:7787")
            && health["loops"]["registry"]["restarts"].as_u64() == Some(0)
    })
    .await;
    assert_eq!(health["loops"]["registry"]["last_exit"], serde_json::Value::Null);
    let recovered =
        wait_for_log_line_while_running(&mut run, &root, "\"state\":\"recovered\"").await;
    assert!(recovered.contains("\"kind\":\"registry_error\""));
    assert_eq!(count_log_lines(&root, "\"kind\":\"loop_exit\""), 0);

    token.cancel();
    tokio::select! {
        result = &mut run => result.unwrap(),
        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("registry slot did not stop after cancellation"),
    }
}

#[tokio::test]
async fn exposure_loop_retries_registry_failures_without_exiting() {
    let exposure = ManualExposure::new(Some("wss://example.test:7787".to_string()));
    let clock = StepClock::new(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let token = CancellationToken::new();
    let row = ExposureRow {
        fleet_id: "fleet-a".to_string(),
        gateway_id: "gw-a".to_string(),
        owner: "owner-a".to_string(),
        pubkey_fpr: "pub-a".to_string(),
        cert_fpr: "cert-a".to_string(),
        version: "test".to_string(),
    };
    let mut publisher = ExposurePublisher::new(row);
    let tmp = tempfile::tempdir().unwrap();
    let registry = FlippingRegistry::new(tmp.path(), 1);
    registry.inner.init_schema().unwrap();
    let (events_tx, mut events_rx) = tokio::sync::mpsc::channel(4);

    let run = run_exposure_loop_with_events(
        &registry,
        &exposure,
        &mut publisher,
        Duration::from_millis(20),
        &clock,
        token.clone(),
        Some(events_tx),
    );
    tokio::pin!(run);

    let first = tokio::select! {
        result = &mut run => panic!("exposure loop exited on registry failure: {:?}", result),
        tick = events_rx.recv() => tick.expect("first tick is sent"),
    };
    assert!(first.registry_error.is_some());
    assert!(first.registry_error_since.is_some());
    assert!(first.registry_error_change.is_some());

    let second = tokio::select! {
        result = &mut run => panic!("exposure loop exited after registry failure: {:?}", result),
        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("exposure loop did not retry"),
        tick = events_rx.recv() => tick.expect("second tick is sent"),
    };
    assert!(second.registry_error.is_none());
    assert!(second.registry_error_since.is_none());
    assert_eq!(
        second.endpoint_url.as_deref(),
        Some("wss://example.test:7787")
    );

    token.cancel();
    tokio::select! {
        result = &mut run => result.unwrap(),
        _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("exposure loop did not stop after cancellation"),
    }
}

#[tokio::test]
async fn dialer_uses_discovered_cert_fingerprint_and_rejects_wrong_pin() {
    let tls = TlsIdentity::self_signed(vec!["localhost".to_string()]).unwrap();
    let (client, server) = tokio::io::duplex(a2a::mesh::wire::MAX_FRAME_BYTES * 2 + 4096);
    let accept = tokio::spawn(accept_once_io(server, tls));
    let row = a2a::mesh::registry::GatewayRow {
        fleet_id: "fleet-a".to_string(),
        gateway_id: "gw-b".to_string(),
        owner: "gw-b".to_string(),
        endpoint_url: Some("wss://127.0.0.1:7787".to_string()),
        pubkey_fpr: "gw-b-pub".to_string(),
        cert_fpr: Some("0".repeat(64)),
        version: "test".to_string(),
        created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        last_heartbeat: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        revoked_at: None,
        schema_version: a2a::mesh::registry::REGISTRY_SCHEMA_VERSION,
        row_msg: None,
        row_token: None,
        row_sig: None,
        signed_at: None,
        revocation: None,
    };

    let err = dial_gateway_io(client, &row).await.unwrap_err();
    assert_eq!(err.code, "TLS_CERT_PIN_MISMATCH");
    accept.await.unwrap().unwrap_err();
}

#[tokio::test]
async fn none_exposure_keeps_null_endpoint_and_outbound_discovery_skips_it() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    registry.init_schema().unwrap();
    let row = ExposureRow {
        fleet_id: "fleet-a".to_string(),
        gateway_id: "gw-none".to_string(),
        owner: "owner-a".to_string(),
        pubkey_fpr: "pub-a".to_string(),
        cert_fpr: "cert-a".to_string(),
        version: "test".to_string(),
    };
    let mut publisher = ExposurePublisher::new(row);

    publisher
        .tick(
            &registry,
            &NoneExposure,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        )
        .await
        .unwrap();
    let rows = registry.discover_unverified("fleet-a").await.unwrap();
    assert_eq!(rows[0].endpoint_url, None);
    let verified_rows = rows
        .into_iter()
        .map(|row| (row, RowVerdict::Unsigned { stripped: false }))
        .collect::<Vec<_>>();
    assert!(a2a::mesh::gateway::link::discover_outbound_peers(
        "gw-a",
        &verified_rows,
        false,
    )
    .is_empty());
}

struct RotatingExposure {
    urls: Mutex<VecDeque<Option<String>>>,
}

impl RotatingExposure {
    fn new(urls: impl IntoIterator<Item = Option<&'static str>>) -> Self {
        Self {
            urls: Mutex::new(
                urls.into_iter()
                    .map(|url| url.map(str::to_string))
                    .collect(),
            ),
        }
    }
}

impl Exposure for RotatingExposure {
    async fn current_url(&self) -> Result<Option<String>, a2a::output::AppError> {
        Ok(self.urls.lock().unwrap().pop_front().unwrap_or(None))
    }
}

struct StepClock {
    now: Mutex<DateTime<Utc>>,
}

impl StepClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(now),
        }
    }

    fn set(&self, now: DateTime<Utc>) {
        *self.now.lock().unwrap() = now;
    }
}

impl Clock for StepClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

async fn wait_for_row(registry: &FileRegistry, expected_url: Option<&str>) {
    wait_for_row_in_fleet(registry, "fleet-a", expected_url).await;
}

async fn wait_for_row_in_fleet(
    registry: &FileRegistry,
    fleet_id: &str,
    expected_url: Option<&str>,
) {
    for _ in 0..100 {
        let rows = registry.discover_unverified(fleet_id).await.unwrap();
        if rows
            .first()
            .map(|row| row.endpoint_url.as_deref() == expected_url)
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "registry did not publish expected url {:?}: {:?}",
        expected_url,
        registry.discover_unverified(fleet_id).await.unwrap()
    );
}

async fn wait_for_heartbeat_after_while_running<F>(
    run: &mut std::pin::Pin<&mut F>,
    registry: &FileRegistry,
    fleet_id: &str,
    after: DateTime<Utc>,
) -> GatewayRow
where
    F: std::future::Future<Output = Result<(), AppError>>,
{
    for _ in 0..100 {
        let rows = registry.discover_unverified(fleet_id).await.unwrap();
        if let Some(row) = rows
            .first()
            .filter(|row| row.last_heartbeat > after)
            .cloned()
        {
            return row;
        }
        tokio::select! {
            result = run.as_mut() => panic!("registry slot exited unexpectedly: {:?}", result),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    panic!(
        "registry heartbeat did not advance after {:?}: {:?}",
        after,
        registry.discover_unverified(fleet_id).await.unwrap()
    );
}

async fn wait_for_row_in_fleet_while_running<F>(
    run: &mut std::pin::Pin<&mut F>,
    registry: &FileRegistry,
    fleet_id: &str,
    expected_url: Option<&str>,
) where
    F: std::future::Future<Output = Result<(), AppError>>,
{
    for _ in 0..100 {
        let rows = registry.discover_unverified(fleet_id).await.unwrap();
        if rows
            .first()
            .map(|row| row.endpoint_url.as_deref() == expected_url)
            .unwrap_or(false)
        {
            return;
        }
        tokio::select! {
            result = run.as_mut() => panic!("registry slot exited unexpectedly: {:?}", result),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    panic!(
        "registry did not publish expected url {:?}: {:?}",
        expected_url,
        registry.discover_unverified(fleet_id).await.unwrap()
    );
}

async fn wait_for_health_while_running<F>(
    run: &mut std::pin::Pin<&mut F>,
    root: &std::path::Path,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value
where
    F: std::future::Future<Output = Result<(), AppError>>,
{
    for _ in 0..100 {
        if let Ok(text) = std::fs::read_to_string(root.join("gateway-health.json")) {
            if let Ok(health) = serde_json::from_str::<serde_json::Value>(&text) {
                if predicate(&health) {
                    return health;
                }
            }
        }
        tokio::select! {
            result = run.as_mut() => panic!("registry slot exited unexpectedly: {:?}", result),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    panic!("gateway health did not match expected predicate");
}

async fn wait_for_log_line_while_running<F>(
    run: &mut std::pin::Pin<&mut F>,
    root: &std::path::Path,
    needle: &str,
) -> String
where
    F: std::future::Future<Output = Result<(), AppError>>,
{
    for _ in 0..100 {
        if let Ok(text) = std::fs::read_to_string(root.join("gateway.jsonl")) {
            if let Some(line) = text.lines().find(|line| line.contains(needle)) {
                return line.to_string();
            }
        }
        tokio::select! {
            result = run.as_mut() => panic!("registry slot exited unexpectedly: {:?}", result),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    panic!("gateway jsonl did not contain {needle}");
}

fn count_log_lines(root: &std::path::Path, needle: &str) -> usize {
    std::fs::read_to_string(root.join("gateway.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter(|line| line.contains(needle))
        .count()
}

trait NgrokTestExt {
    fn api_requests(&self) -> Vec<String>;
}

impl NgrokTestExt for NgrokExposure<FakeNgrok> {
    fn api_requests(&self) -> Vec<String> {
        self.api().requests.lock().unwrap().clone()
    }
}

enum FakeNgrokResponse {
    Ok(NgrokTunnels),
    Err(String),
}

struct FakeNgrok {
    gets: Mutex<VecDeque<FakeNgrokResponse>>,
    default_error: Option<String>,
    requests: Mutex<Vec<String>>,
}

impl FakeNgrok {
    fn new(gets: Vec<NgrokTunnels>) -> Self {
        Self {
            gets: Mutex::new(gets.into_iter().map(FakeNgrokResponse::Ok).collect()),
            default_error: None,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn scripted(gets: Vec<Result<NgrokTunnels, &'static str>>) -> Self {
        Self {
            gets: Mutex::new(
                gets.into_iter()
                    .map(|result| match result {
                        Ok(tunnels) => FakeNgrokResponse::Ok(tunnels),
                        Err(message) => FakeNgrokResponse::Err(message.to_string()),
                    })
                    .collect(),
            ),
            default_error: None,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn always_error(message: &'static str) -> Self {
        Self {
            gets: Mutex::new(VecDeque::new()),
            default_error: Some(message.to_string()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl NgrokApi for FakeNgrok {
    async fn tunnels(&self) -> Result<NgrokTunnels, AppError> {
        self.requests
            .lock()
            .unwrap()
            .push("GET /api/tunnels".to_string());
        match self.gets.lock().unwrap().pop_front() {
            Some(FakeNgrokResponse::Ok(tunnels)) => Ok(tunnels),
            Some(FakeNgrokResponse::Err(message)) => Err(AppError::new(
                "NGROK_UNAVAILABLE",
                ExitCode::BrokerUnavailable,
                message,
            )),
            None => {
                if let Some(message) = &self.default_error {
                    Err(AppError::new(
                        "NGROK_UNAVAILABLE",
                        ExitCode::BrokerUnavailable,
                        message.clone(),
                    ))
                } else {
                    Ok(NgrokTunnels { tunnels: vec![] })
                }
            }
        }
    }

    async fn start_tunnel(&self, listen_port: u64) -> Result<NgrokTunnel, AppError> {
        self.requests
            .lock()
            .unwrap()
            .push("POST /api/tunnels".to_string());
        assert_eq!(listen_port, 7787);
        Ok(NgrokTunnel {
            proto: Some("tcp".to_string()),
            public_url: "tcp://1.tcp.ngrok.test:19001".to_string(),
            config: Some(NgrokTunnelConfig {
                addr: "localhost:7787".to_string(),
            }),
        })
    }
}

struct FlippingRegistry {
    inner: FileRegistry,
    failures_remaining: Mutex<usize>,
    failure: fn() -> AppError,
    heartbeat_calls: AtomicUsize,
}

impl FlippingRegistry {
    fn new(root: impl AsRef<std::path::Path>, failures: usize) -> Self {
        Self::with_failure(root, failures, registry_test_failure)
    }

    fn with_failure(
        root: impl AsRef<std::path::Path>,
        failures: usize,
        failure: fn() -> AppError,
    ) -> Self {
        Self {
            inner: FileRegistry::new(root),
            failures_remaining: Mutex::new(failures),
            failure,
            heartbeat_calls: AtomicUsize::new(0),
        }
    }

    fn maybe_fail(&self) -> Result<(), AppError> {
        let mut remaining = self.failures_remaining.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            Err((self.failure)())
        } else {
            Ok(())
        }
    }

    fn heartbeat_calls(&self) -> usize {
        self.heartbeat_calls.load(Ordering::SeqCst)
    }
}

impl Registry for FlippingRegistry {
    async fn upsert_self(&self, row: GatewayRow) -> Result<(), AppError> {
        self.maybe_fail()?;
        self.inner.upsert_self(row).await
    }

    async fn heartbeat(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
        self.maybe_fail()?;
        self.inner.heartbeat(fleet_id, gateway_id, now).await
    }

    async fn discover_unverified(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> {
        self.inner.discover_unverified(fleet_id).await
    }

    async fn revoke(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        self.inner.revoke(fleet_id, gateway_id, now).await
    }

    async fn schema_version(&self) -> Result<i32, AppError> {
        self.inner.schema_version().await
    }

    async fn work_insert(&self, row: WorkRow) -> Result<(), AppError> {
        self.inner.work_insert(row).await
    }

    async fn work_claim(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer_gw: &str,
    ) -> Result<ClaimOutcome, AppError> {
        self.inner.work_claim(fleet_id, work_id, claimer_gw).await
    }

    async fn work_transition(
        &self,
        fleet_id: &str,
        work_id: &str,
        from: WorkStatus,
        to: WorkStatus,
        owner: WorkOwner,
        patch: WorkPatch,
    ) -> Result<TransitionOutcome, AppError> {
        self.inner
            .work_transition(fleet_id, work_id, from, to, owner, patch)
            .await
    }

    async fn work_get(&self, fleet_id: &str, work_id: &str) -> Result<Option<WorkRow>, AppError> {
        self.inner.work_get(fleet_id, work_id).await
    }

    async fn work_list(
        &self,
        fleet_id: &str,
        filter: WorkListFilter,
    ) -> Result<Vec<WorkRow>, AppError> {
        self.inner.work_list(fleet_id, filter).await
    }

    async fn work_touch_running(
        &self,
        fleet_id: &str,
        work_id: &str,
        claimer: &str,
    ) -> Result<(), AppError> {
        self.inner.work_touch_running(fleet_id, work_id, claimer).await
    }

    async fn work_sweep_expired(
        &self,
        fleet_id: &str,
        swept_by: &str,
    ) -> Result<Vec<String>, AppError> {
        self.inner.work_sweep_expired(fleet_id, swept_by).await
    }

    async fn work_sweep_abandoned(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        stall: Duration,
    ) -> Result<Vec<WorkRow>, AppError> {
        self.inner.work_sweep_abandoned(fleet_id, claimer_gw, stall).await
    }
}

fn registry_test_failure() -> AppError {
    AppError::new(
        "REGISTRY_TEST_FAILURE",
        ExitCode::BrokerUnavailable,
        "registry test failure",
    )
}

fn registry_schema_stale_failure() -> AppError {
    AppError::new(
        "REGISTRY_SCHEMA_STALE",
        ExitCode::Precondition,
        "mesh gateway registry schema is stale",
    )
    .remedy("the fleet root machine must run `a2a mesh fleet migrate`")
}
