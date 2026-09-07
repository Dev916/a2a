mod common;

use a2a::home::A2aHome;
use a2a::mesh::gateway::expose::{ExposurePublisher, ExposureRow, ManualExposure};
use a2a::mesh::gateway::notes_wire::{deliver_spooled_once, NoteDeliveryTarget};
use a2a::mesh::gateway::presence_wire;
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_id, fleet_init, gateway_identity, gateway_public_key_b64,
    gateway_public_key_fingerprint, install_gateway_token, SystemClock,
};
use a2a::mesh::registry::{
    map_gateway_sqlstate, open_registry, reader_roots, write_registry_env, ClaimOutcome,
    FileRegistry, GatewayRow, Registry, TransitionOutcome, WorkListFilter, WorkOwner, WorkPatch,
    WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION,
};
use a2a::mesh::row_mark::RowMarkStore;
use a2a::mesh::row_sig::{Disposition, RowVerdict, RowVerifyCfg};
use a2a::mesh::work_store;
use a2a::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use rand::{rngs::StdRng, SeedableRng};
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn copy_root_pub(root: &A2aHome, joiner: &A2aHome) {
    fs::create_dir_all(joiner.root().join("mesh/fleet")).unwrap();
    fs::copy(
        root.root().join("mesh/fleet/root.pub"),
        joiner.root().join("mesh/fleet/root.pub"),
    )
    .unwrap();
}

fn enroll_joiner(root: &A2aHome, joiner: &A2aHome) -> String {
    let identity = gateway_identity(joiner, false).unwrap();
    let pubkey = gateway_public_key_b64(joiner).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey, &SystemClock).unwrap();
    install_gateway_token(joiner, &token, &SystemClock).unwrap();
    identity.gateway_id
}

fn unsigned_row(home: &A2aHome, fleet: &str, version: &str) -> GatewayRow {
    let identity = gateway_identity(home, false).unwrap();
    let now = Utc::now();
    GatewayRow {
        fleet_id: fleet.to_string(),
        gateway_id: identity.gateway_id,
        owner: "mixed-version-test".to_string(),
        endpoint_url: None,
        pubkey_fpr: gateway_public_key_fingerprint(home).unwrap(),
        cert_fpr: Some(identity.cert_fpr),
        version: version.to_string(),
        created_at: now,
        last_heartbeat: now,
        revoked_at: None,
        schema_version: REGISTRY_SCHEMA_VERSION,
        row_msg: None,
        row_token: None,
        row_sig: None,
        signed_at: None,
        revocation: None,
    }
}

#[tokio::test]
async fn an_unsigned_pre_l4_row_is_usable_with_a_warning_under_flag_false_and_refused_under_true(
) {
    let tmp = tempfile::tempdir().unwrap();
    let reader = home_at(&tmp.path().join("reader"));
    fleet_init(&reader).unwrap();
    let fleet = fleet_id(&reader).unwrap();
    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    let row = unsigned_row(&reader, &fleet, "0.3.0");
    registry.upsert_self(row.clone()).await.unwrap();
    assert!(row.row_msg.is_none());
    assert!(row.row_token.is_none());
    assert!(row.row_sig.is_none());
    assert!(row.signed_at.is_none());

    for (require_signed_rows, expected) in [
        (false, Disposition::UsableWarn),
        (true, Disposition::Refused),
    ] {
        fs::write(
            reader.root().join("config.toml"),
            format!("[mesh.gateway]\nrequire_signed_rows = {require_signed_rows}\n"),
        )
        .unwrap();
        let cfg = a2a::config::load(&reader).unwrap().0;
        let url = format!("file:{}", registry_path.display());
        let handle = open_registry(&url, reader_roots(&reader).unwrap(), fleet.clone())
            .unwrap()
            .with_reader_state(
                RowMarkStore::open(&reader).unwrap(),
                RowVerifyCfg::from_config(Utc::now(), &cfg),
            );
        let rows = handle.discover().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.gateway_id, row.gateway_id);
        assert_eq!(
            rows[0].1,
            RowVerdict::Unsigned { stripped: false }
        );
        assert_eq!(
            rows[0]
                .1
                .disposition(cfg.mesh.gateway.require_signed_rows),
            expected
        );
    }
}

struct Sqlstate42703Registry {
    inner: FileRegistry,
    heartbeat_calls: AtomicUsize,
}

impl Sqlstate42703Registry {
    fn new(path: &Path) -> Self {
        let inner = FileRegistry::new(path);
        inner.init_schema().unwrap();
        Self {
            inner,
            heartbeat_calls: AtomicUsize::new(0),
        }
    }

    fn schema_stale_error() -> AppError {
        map_gateway_sqlstate(Some("42703")).expect("SQLSTATE 42703 must be typed")
    }
}

impl Registry for Sqlstate42703Registry {
    async fn upsert_self(&self, _row: GatewayRow) -> Result<(), AppError> {
        Err(Self::schema_stale_error())
    }

    async fn heartbeat(
        &self,
        _fleet_id: &str,
        _gateway_id: &str,
        _now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn discover_unverified(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> {
        self.inner.discover_unverified(fleet_id).await
    }

    async fn revoke(
        &self,
        fleet_id: &str,
        gateway_id: &str,
        revocation: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        self.inner
            .revoke(fleet_id, gateway_id, revocation, now)
            .await
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
        self.inner
            .work_touch_running(fleet_id, work_id, claimer)
            .await
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
        self.inner
            .work_sweep_abandoned(fleet_id, claimer_gw, stall)
            .await
    }

    async fn work_sweep_release_stalled(
        &self,
        fleet_id: &str,
        claimer_gw: &str,
        horizon: Duration,
    ) -> Result<Vec<WorkRow>, AppError> {
        self.inner
            .work_sweep_release_stalled(fleet_id, claimer_gw, horizon)
            .await
    }
}

#[tokio::test]
async fn a_pre_migration_registry_reports_schema_stale_with_a_migrate_remedy() {
    // The real pre-migration PostgreSQL path is exercised by the A2A_MESH_L4_LIVE tier (F2).
    let tmp = tempfile::tempdir().unwrap();
    let registry = Sqlstate42703Registry::new(&tmp.path().join("registry"));
    let mapped = Sqlstate42703Registry::schema_stale_error();
    assert_eq!(mapped.code, "REGISTRY_SCHEMA_STALE");
    assert_eq!(mapped.exit, ExitCode::Precondition);
    assert!(
        mapped
            .remedy
            .as_deref()
            .unwrap()
            .contains("a2a mesh fleet migrate")
    );

    let mut publisher = ExposurePublisher::new(ExposureRow {
        fleet_id: "fleet-pre-migration".to_string(),
        gateway_id: "gw-pre-migration-0123456789abcdef".to_string(),
        owner: "mixed-version-test".to_string(),
        pubkey_fpr: "pubkey-fingerprint".to_string(),
        cert_fpr: "certificate-fingerprint".to_string(),
        version: "0.3.0".to_string(),
    });
    let tick = publisher
        .tick(
            &registry,
            &ManualExposure::new(Some("wss://changed.example.test:7787".to_string())),
            Utc::now(),
        )
        .await
        .unwrap();

    assert!(!tick.upserted);
    assert!(
        tick.registry_error
            .as_deref()
            .unwrap()
            .starts_with("REGISTRY_SCHEMA_STALE")
    );
    assert_eq!(registry.heartbeat_calls.load(Ordering::SeqCst), 1);
}

fn json_out(assert: &assert_cmd::assert::Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).unwrap()
}

#[test]
fn work_send_still_reaches_a_0_3_0_floor_peer() {
    let tmp = tempfile::tempdir().unwrap();
    let root = home_at(&tmp.path().join("root"));
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();

    fleet_init(&root).unwrap();
    copy_root_pub(&root, &sender);
    copy_root_pub(&root, &receiver);
    let sender_gw = enroll_joiner(&root, &sender);
    let receiver_gw = enroll_joiner(&root, &receiver);
    fs::write(
        sender.root().join("config.toml"),
        "[mesh.gateway]\nrequire_signed_rows = false\n",
    )
    .unwrap();
    fs::write(
        receiver.root().join("config.toml"),
        "[mesh.gateway]\nrequire_signed_rows = false\n",
    )
    .unwrap();

    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    let registry_url = format!("file:{}", registry_path.display());
    write_registry_env(&sender, &registry_url).unwrap();
    write_registry_env(&receiver, &registry_url).unwrap();
    let fleet = fleet_id(&sender).unwrap();
    let sender_row = unsigned_row(&sender, &fleet, "0.3.0");
    let receiver_row = unsigned_row(&receiver, &fleet, "0.3.0");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        registry.upsert_self(sender_row.clone()).await.unwrap();
        registry.upsert_self(receiver_row).await.unwrap();
    });
    presence_wire::refresh_registry_cache(
        &receiver,
        &[(sender_row, RowVerdict::Unsigned { stripped: false })],
        Utc::now(),
    )
    .unwrap();

    let brief = common::write_brief(
        &work,
        "---\nlane: impl\nacceptance_criteria:\n  - mixed-version send succeeds\nverify:\n  - cargo test --test mesh_mixed_version\n---\n# Mixed-version work\n",
    );
    let sent = common::a2a(sender.root(), &work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &receiver_gw,
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Mixed-version handoff",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "mixed-version send succeeds",
        ])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&sent.get_output().stderr);
    assert!(stderr.contains("registry row is UNSIGNED"), "{stderr}");
    let out = json_out(&sent);
    let work_id = out["data"]["work_id"].as_str().unwrap();
    assert_eq!(out["data"]["to"], receiver_gw);

    let cfg = a2a::config::load(&receiver).unwrap().0;
    let mut rng = StdRng::seed_from_u64(7);
    let report = deliver_spooled_once(
        &cfg,
        &sender,
        NoteDeliveryTarget::Gateway {
            to_gateway: &receiver_gw,
            home: Some(&receiver),
        },
        Utc::now(),
        &mut rng,
    )
    .unwrap();
    assert_eq!(report.delivered, 1);
    assert_eq!(report.rejected, 0);
    assert!(work_store::load(&receiver, work_id).unwrap().is_some());
    let log = fs::read_to_string(receiver.root().join("mesh/gateway/gateway.jsonl")).unwrap();
    assert!(log.contains("work_ingest_warning"), "{log}");
    assert!(log.contains("pubkey_fpr check was skipped"), "{log}");
    assert!(log.contains(&sender_gw), "{log}");
}
