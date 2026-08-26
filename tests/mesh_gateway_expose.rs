use a2a::mesh::gateway::expose::{
    run_exposure_loop, Exposure, ExposurePublisher, ExposureRow, ManualExposure, NgrokApi,
    NgrokExposure, NgrokTunnel, NgrokTunnelConfig, NgrokTunnels, NoneExposure,
};
use a2a::mesh::gateway::link::{accept_once_io, dial_gateway_io};
use a2a::mesh::gateway::{gateway_root, run_registry_slot_for_test, RegistrySlotParams};
use a2a::mesh::keys::{self, Clock, SystemClock};
use a2a::mesh::registry::{FileRegistry, Registry};
use a2a::mesh::wire::TlsIdentity;
use a2a::output::ExitCode;
use chrono::{DateTime, TimeZone, Utc};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

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
        registry.discover("fleet-a").await.unwrap()[0]
            .endpoint_url
            .as_deref(),
        Some("wss://old.example.test:10000")
    );
    assert_eq!(
        registry.discover("fleet-a").await.unwrap()[0]
            .cert_fpr
            .as_deref(),
        Some("cert-a")
    );

    let t1 = Utc.timestamp_opt(1_700_000_001, 0).unwrap();
    let same = publisher.tick(&registry, &exposure, t1).await.unwrap();
    assert!(!same.upserted);
    assert_eq!(
        registry.discover("fleet-a").await.unwrap()[0].last_heartbeat,
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
        registry.discover("fleet-a").await.unwrap()[0]
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
        registry.discover("fleet-a").await.unwrap()[0].endpoint_url,
        None
    );
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
                let rows = registry.discover(&fleet_id).await.unwrap();
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
    let rows = registry.discover(&fleet_id).await.unwrap();
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
    let rows = registry.discover("fleet-a").await.unwrap();
    assert_eq!(rows[0].endpoint_url, None);
    assert!(a2a::mesh::gateway::link::discover_outbound_peers("gw-a", &rows).is_empty());
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
        let rows = registry.discover(fleet_id).await.unwrap();
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
        registry.discover(fleet_id).await.unwrap()
    );
}

trait NgrokTestExt {
    fn api_requests(&self) -> Vec<String>;
}

impl NgrokTestExt for NgrokExposure<FakeNgrok> {
    fn api_requests(&self) -> Vec<String> {
        self.api().requests.lock().unwrap().clone()
    }
}

struct FakeNgrok {
    gets: Mutex<VecDeque<NgrokTunnels>>,
    requests: Mutex<Vec<String>>,
}

impl FakeNgrok {
    fn new(gets: Vec<NgrokTunnels>) -> Self {
        Self {
            gets: Mutex::new(gets.into()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl NgrokApi for FakeNgrok {
    async fn tunnels(&self) -> Result<NgrokTunnels, a2a::output::AppError> {
        self.requests
            .lock()
            .unwrap()
            .push("GET /api/tunnels".to_string());
        Ok(self
            .gets
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(NgrokTunnels { tunnels: vec![] }))
    }

    async fn start_tunnel(&self, listen_port: u64) -> Result<NgrokTunnel, a2a::output::AppError> {
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
