mod common;

use a2a::home::A2aHome;
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_id, fleet_init, gateway_identity, gateway_public_key_b64,
    install_gateway_token, SystemClock,
};
use a2a::mesh::registry::{
    ClaimOutcome, FileRegistry, GatewayRow, Registry, TransitionOutcome, WorkListFilter,
    WorkOwner, WorkPatch, WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION,
};
use a2a::mesh::work::{
    sign_envelope, verify_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkRepo,
};
use a2a::mesh::work_exec::{
    self, approve_dry_run, advance_approve, advance_approve_until, ApproveOutcome,
    ApproveStopAfter, GitAdapter, RealSpawner, WorkPhase, WorkSpawner,
};
use a2a::mesh::work_store::{self, WorkSource};
use a2a::output::{AppError, ExitCode};
use chrono::{TimeZone, Utc};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Runtime::new().unwrap().block_on(future)
    }
}

fn copy_root_pub(root: &Path, joiner: &Path) {
    std::fs::create_dir_all(joiner.join("mesh/fleet")).unwrap();
    std::fs::copy(
        root.join("mesh/fleet/root.pub"),
        joiner.join("mesh/fleet/root.pub"),
    )
    .unwrap();
}

fn enroll_joiner(root: &A2aHome, joiner: &A2aHome) -> String {
    let clock = SystemClock;
    let identity = gateway_identity(joiner, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(joiner).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey_b64, &clock).unwrap();
    install_gateway_token(joiner, &token, &clock).unwrap();
    identity.gateway_id
}

#[derive(Clone)]
struct Fixture {
    _tmp: Arc<tempfile::TempDir>,
    receiver: A2aHome,
    registry: CountingRegistry,
    work_id: String,
    receiver_gw: String,
}

fn fixture() -> Fixture {
    let tmp = Arc::new(tempfile::tempdir().unwrap());
    let root = home_at(&tmp.path().join("root"));
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    fleet_init(&root).unwrap();
    copy_root_pub(root.root(), sender.root());
    copy_root_pub(root.root(), receiver.root());
    let sender_gw = enroll_joiner(&root, &sender);
    let receiver_gw = enroll_joiner(&root, &receiver);

    let env = sign_envelope(
        &sender,
        UnsignedWorkEnvelope {
            work_id: "work-approve-1".to_string(),
            session: "user:test".to_string(),
            addressing: WorkAddressing::Pool,
            repo: WorkRepo {
                remote_url: "https://example.com/a2a-work-approve.git".to_string(),
                base_ref: "main".to_string(),
            },
            title: "Approve work".to_string(),
            brief_text: "---\nlane: impl\nacceptance_criteria:\n  - focused test passes\nverify:\n  - cargo test --test mesh_work_approve\n---\n# Build it\n".to_string(),
            acceptance: "focused test passes".to_string(),
            ttl_days: 7,
            max_bytes: 262_144,
        },
        &SystemClock,
    )
    .unwrap();
    let verified = verify_envelope(&receiver, &env, &SystemClock).unwrap();
    work_store::store_offer(&receiver, verified, WorkSource::Pool).unwrap();

    let registry = FileRegistry::new(tmp.path().join("registry"));
    registry.init_schema().unwrap();
    let row = WorkRow {
        fleet_id: fleet_id(&receiver).unwrap(),
        work_id: env.work_id.clone(),
        sender_gw,
        addressing: "pool".to_string(),
        status: WorkStatus::Offered,
        claimed_by: None,
        result_ref: None,
        result_delivery: None,
        swept_by: None,
        created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        updated_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        expires_at: Utc.timestamp_opt(1_700_086_400, 0).unwrap(),
        schema_version: REGISTRY_SCHEMA_VERSION,
    };
    block_on(registry.work_insert(row)).unwrap();

    Fixture {
        _tmp: tmp,
        receiver,
        registry: CountingRegistry::new(registry),
        work_id: env.work_id,
        receiver_gw,
    }
}

#[derive(Clone, Default)]
struct Counts {
    claims: usize,
    running_transitions: usize,
    clones: usize,
    spawns: usize,
}

#[derive(Clone)]
struct CountingRegistry {
    inner: FileRegistry,
    counts: Arc<Mutex<Counts>>,
}

impl CountingRegistry {
    fn new(inner: FileRegistry) -> Self {
        Self {
            inner,
            counts: Arc::new(Mutex::new(Counts::default())),
        }
    }

    fn counts(&self) -> Counts {
        self.counts.lock().unwrap().clone()
    }
}

#[allow(async_fn_in_trait)]
impl Registry for CountingRegistry {
    async fn upsert_self(&self, row: GatewayRow) -> Result<(), AppError> {
        self.inner.upsert_self(row).await
    }
    async fn heartbeat(&self, fleet_id: &str, gateway_id: &str, now: chrono::DateTime<Utc>) -> Result<(), AppError> {
        self.inner.heartbeat(fleet_id, gateway_id, now).await
    }
    async fn discover(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> {
        self.inner.discover(fleet_id).await
    }
    async fn retire_self(&self, fleet_id: &str, gateway_id: &str, now: chrono::DateTime<Utc>) -> Result<(), AppError> {
        self.inner.retire_self(fleet_id, gateway_id, now).await
    }
    async fn revoke(&self, fleet_id: &str, gateway_id: &str, now: chrono::DateTime<Utc>) -> Result<(), AppError> {
        self.inner.revoke(fleet_id, gateway_id, now).await
    }
    async fn schema_version(&self) -> Result<i32, AppError> {
        self.inner.schema_version().await
    }
    async fn work_insert(&self, row: WorkRow) -> Result<(), AppError> {
        self.inner.work_insert(row).await
    }
    async fn work_claim(&self, fleet_id: &str, work_id: &str, claimer_gw: &str) -> Result<ClaimOutcome, AppError> {
        self.counts.lock().unwrap().claims += 1;
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
        if from == WorkStatus::Approved && to == WorkStatus::Running {
            self.counts.lock().unwrap().running_transitions += 1;
        }
        self.inner.work_transition(fleet_id, work_id, from, to, owner, patch).await
    }
    async fn work_get(&self, fleet_id: &str, work_id: &str) -> Result<Option<WorkRow>, AppError> {
        self.inner.work_get(fleet_id, work_id).await
    }
    async fn work_list(&self, fleet_id: &str, filter: WorkListFilter) -> Result<Vec<WorkRow>, AppError> {
        self.inner.work_list(fleet_id, filter).await
    }
    async fn work_touch_running(&self, fleet_id: &str, work_id: &str, claimer: &str) -> Result<(), AppError> {
        self.inner.work_touch_running(fleet_id, work_id, claimer).await
    }
    async fn work_sweep_expired(&self, fleet_id: &str, swept_by: &str) -> Result<Vec<String>, AppError> {
        self.inner.work_sweep_expired(fleet_id, swept_by).await
    }
    async fn work_sweep_abandoned(&self, fleet_id: &str, claimer_gw: &str, stall: Duration) -> Result<Vec<WorkRow>, AppError> {
        self.inner.work_sweep_abandoned(fleet_id, claimer_gw, stall).await
    }
}

#[derive(Clone, Default)]
struct FakeGit {
    counts: Arc<Mutex<Counts>>,
    ls_remote_error: Arc<Mutex<Option<AppError>>>,
}

impl FakeGit {
    fn fail_ls_remote(code: &'static str) -> Self {
        Self {
            counts: Arc::new(Mutex::new(Counts::default())),
            ls_remote_error: Arc::new(Mutex::new(Some(AppError::new(code, ExitCode::Precondition, "nope")))),
        }
    }
}

#[allow(async_fn_in_trait)]
impl GitAdapter for FakeGit {
    async fn ls_remote(&self, _remote_url: &str, _base_ref: &str) -> Result<(), AppError> {
        if let Some(err) = self.ls_remote_error.lock().unwrap().take() {
            return Err(err);
        }
        Ok(())
    }

    async fn clone_checkout(&self, _remote_url: &str, _base_ref: &str, checkout: &Path) -> Result<(), AppError> {
        self.counts.lock().unwrap().clones += 1;
        std::fs::create_dir_all(checkout).unwrap();
        Ok(())
    }
}

#[derive(Clone, Default)]
struct FakeSpawner {
    counts: Arc<Mutex<Counts>>,
    preflight_error: Arc<Mutex<Option<AppError>>>,
}

impl FakeSpawner {
    fn fail_preflight(err: AppError) -> Self {
        Self {
            counts: Arc::new(Mutex::new(Counts::default())),
            preflight_error: Arc::new(Mutex::new(Some(err))),
        }
    }
}

impl WorkSpawner for FakeSpawner {
    fn preflight(&self, _home: &A2aHome, _checkout: &Path) -> Result<(), AppError> {
        if let Some(err) = self.preflight_error.lock().unwrap().take() {
            return Err(err);
        }
        Ok(())
    }

    fn spawn(&self, _home: &A2aHome, _checkout: &Path, _brief: &Path) -> Result<String, AppError> {
        self.counts.lock().unwrap().spawns += 1;
        Ok("impl-0902t0609-work".to_string())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn approve_resumes_after_each_phase_boundary_with_exactly_once_effects() {
    for stop in [
        ApproveStopAfter::StateWritten,
        ApproveStopAfter::Claimed,
        ApproveStopAfter::Cloned,
        ApproveStopAfter::Spawned,
    ] {
        let f = fixture();
        let git = FakeGit::default();
        let spawner = FakeSpawner::default();
        let interrupted = advance_approve_until(
            &f.receiver,
            &f.registry,
            &git,
            &spawner,
            &f.work_id,
            Some(stop),
        )
        .await
        .unwrap();
        assert!(matches!(interrupted, ApproveOutcome::Interrupted { .. }));

        let resumed = advance_approve(&f.receiver, &f.registry, &git, &spawner, &f.work_id)
            .await
            .unwrap();
        let ApproveOutcome::Running { worker_id, checkout, .. } = resumed else {
            panic!("approve must finish running after {stop:?}: {resumed:?}");
        };
        assert_eq!(worker_id, "impl-0902t0609-work");
        assert!(checkout.ends_with("repo"));
        let state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
        assert_eq!(state.phase, WorkPhase::Running);
        assert_eq!(state.worker_id.as_deref(), Some("impl-0902t0609-work"));
        assert!(state.result_note_id.len() >= 26);

        let counts = f.registry.counts();
        assert_eq!(counts.running_transitions, 1, "registry running transition after {stop:?}");
        assert_eq!(git.counts.lock().unwrap().clones, 1, "clone count after {stop:?}");
        assert_eq!(spawner.counts.lock().unwrap().spawns, 1, "spawn count after {stop:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cas_lost_to_my_existing_claim_resumes_but_another_winner_cleans_claiming_state() {
    let mine = fixture();
    let git = FakeGit::default();
    let spawner = FakeSpawner::default();
    let state = work_exec::initial_claiming_state(&mine.receiver, &mine.work_id).unwrap();
    work_exec::save_state(&mine.receiver, &state).unwrap();
    mine.registry
        .work_claim(&fleet_id(&mine.receiver).unwrap(), &mine.work_id, &mine.receiver_gw)
        .await
        .unwrap();

    let resumed = advance_approve(&mine.receiver, &mine.registry, &git, &spawner, &mine.work_id)
        .await
        .unwrap();
    assert!(matches!(resumed, ApproveOutcome::Running { .. }));

    let other = fixture();
    let state = work_exec::initial_claiming_state(&other.receiver, &other.work_id).unwrap();
    work_exec::save_state(&other.receiver, &state).unwrap();
    other
        .registry
        .work_claim(&fleet_id(&other.receiver).unwrap(), &other.work_id, "gw-other-winner")
        .await
        .unwrap();
    let lost = advance_approve(&other.receiver, &other.registry, &git, &spawner, &other.work_id)
        .await
        .unwrap();
    assert_eq!(
        lost,
        ApproveOutcome::Lost {
            work_id: other.work_id.clone(),
            status: "approved".to_string(),
            claimed_by: Some("gw-other-winner".to_string()),
        }
    );
    assert!(work_exec::load_state(&other.receiver, &other.work_id).unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn dry_run_and_preflight_failures_happen_before_local_or_registry_writes() {
    let f = fixture();
    let git = FakeGit::default();
    let spawner = FakeSpawner::default();
    let preview = approve_dry_run(&f.receiver, &git, &spawner, &f.work_id)
        .await
        .unwrap();
    assert_eq!(preview["work_id"], json!(f.work_id));
    assert!(work_exec::load_state(&f.receiver, &f.work_id).unwrap().is_none());
    assert_eq!(f.registry.counts().claims, 0);
    assert_eq!(git.counts.lock().unwrap().clones, 0);
    assert_eq!(spawner.counts.lock().unwrap().spawns, 0);

    let err = advance_approve(
        &f.receiver,
        &f.registry,
        &FakeGit::fail_ls_remote("GIT_NO_REMOTE"),
        &spawner,
        &f.work_id,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "WORK_REPO_UNREACHABLE");
    assert!(work_exec::load_state(&f.receiver, &f.work_id).unwrap().is_none());

    let daemon_down = FakeSpawner::fail_preflight(AppError::new(
        "WORK_DAEMON_DOWN",
        ExitCode::BrokerUnavailable,
        "mesh gateway is not running",
    ));
    let err = advance_approve(&f.receiver, &f.registry, &git, &daemon_down, &f.work_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, "WORK_DAEMON_DOWN");

    let stale = FakeSpawner::fail_preflight(AppError::new(
        "CWD_NOT_ALLOWED",
        ExitCode::Precondition,
        "stale broker",
    ));
    let err = advance_approve(&f.receiver, &f.registry, &git, &stale, &f.work_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, "CWD_NOT_ALLOWED");
    assert!(err.message.contains("broker restart"), "{err:?}");
}

#[test]
fn state_files_are_0600_and_load_states_lists_local_work() {
    let f = fixture();
    let state = work_exec::initial_claiming_state(&f.receiver, &f.work_id).unwrap();
    work_exec::save_state(&f.receiver, &state).unwrap();
    let mode = std::fs::metadata(work_exec::state_path(&f.receiver, &f.work_id))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
    let states = work_exec::load_states(&f.receiver).unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].work_id, f.work_id);
}

#[test]
fn real_spawner_maps_gateway_down_to_work_daemon_down() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(&tmp.path().join("home"));
    let err = RealSpawner
        .preflight(&home, &home.root().join("work/one/repo"))
        .unwrap_err();
    assert_eq!(err.code, "WORK_DAEMON_DOWN");
    assert!(err.remedy.unwrap().contains("a2a mesh gateway run --daemon"));
}
