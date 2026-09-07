mod common;

use a2a::config::Config;
use a2a::home::A2aHome;
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_id, fleet_init, gateway_identity, gateway_public_key_b64,
    install_gateway_token, SystemClock,
};
use a2a::mesh::gateway::notes_wire;
use a2a::mesh::registry::{
    ClaimOutcome, FileRegistry, GatewayRow, Registry, TransitionOutcome, WorkListFilter,
    WorkOwner, WorkPatch, WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION,
};
use a2a::mesh::work::{
    sign_envelope, verify_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkRepo,
};
use a2a::mesh::work_exec::{
    self, advance_completion_until, CompletionStopAfter, RealWorkCompletionIo, WorkCompletion,
    WorkCompletionIo, WorkLoopOptions, WorkPhase, WorkState,
};
use a2a::mesh::work_store::{self, WorkSource};
use a2a::output::{AppError, ExitCode};
use chrono::{TimeZone, Utc};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
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
    let identity = gateway_identity(joiner, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(joiner).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey_b64, &SystemClock).unwrap();
    install_gateway_token(joiner, &token, &SystemClock).unwrap();
    identity.gateway_id
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Runtime::new().unwrap().block_on(future)
    }
}

fn cfg_for(home: &A2aHome) -> Config {
    let mut cfg = a2a::config::load(home).unwrap().0;
    cfg.mesh.work.auto_push = true;
    cfg
}

#[derive(Clone)]
struct Fixture {
    _tmp: Arc<tempfile::TempDir>,
    sender: A2aHome,
    receiver: A2aHome,
    registry: CountingRegistry,
    work_id: String,
    sender_gw: String,
    receiver_gw: String,
}

fn fixture(_case: &str) -> Fixture {
    let tmp = Arc::new(tempfile::tempdir().unwrap());
    let root = home_at(&tmp.path().join("root"));
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    fleet_init(&root).unwrap();
    copy_root_pub(root.root(), sender.root());
    copy_root_pub(root.root(), receiver.root());
    let sender_gw = enroll_joiner(&root, &sender);
    let receiver_gw = enroll_joiner(&root, &receiver);
    let work_id = ulid::Ulid::new().to_string();

    let env = sign_envelope(
        &sender,
        UnsignedWorkEnvelope {
            work_id: work_id.clone(),
            session: "user:test".to_string(),
            addressing: WorkAddressing::Pool,
            repo: WorkRepo {
                remote_url: "file:///tmp/a2a-work-loop.git".to_string(),
                base_ref: "main".to_string(),
            },
            title: "Route completion".to_string(),
            brief_text: "---\nlane: impl\nacceptance_criteria:\n  - focused test passes\nverify:\n  - cargo test --test mesh_work_loop\n---\n# Build it\n".to_string(),
            acceptance: "focused test passes".to_string(),
            ttl_days: 7,
            max_bytes: 262_144,
        },
        &SystemClock,
    )
    .unwrap();
    let verified = verify_envelope(&receiver, &env, &SystemClock).unwrap();
    work_store::store_offer(&receiver, verified, WorkSource::Pool).unwrap();

    let registry_dir = tmp.path().join("registry");
    let inner = FileRegistry::new(&registry_dir);
    inner.init_schema().unwrap();
    a2a::mesh::registry::write_registry_env(&receiver, &format!("file:{}", registry_dir.display()))
        .unwrap();
    let row = WorkRow {
        fleet_id: fleet_id(&receiver).unwrap(),
        work_id: work_id.clone(),
        sender_gw: sender_gw.clone(),
        addressing: "pool".to_string(),
        status: WorkStatus::Running,
        claimed_by: Some(receiver_gw.clone()),
        result_ref: None,
        result_delivery: None,
        swept_by: None,
        created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        updated_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        expires_at: Utc.timestamp_opt(1_700_086_400, 0).unwrap(),
        schema_version: REGISTRY_SCHEMA_VERSION,
    };
    block_on(inner.work_insert(row)).unwrap();
    let registry = CountingRegistry::new(inner);

    let checkout = receiver.work_dir().join(&work_id).join("repo");
    run_git(&checkout, &["init", "-b", "main"]);
    run_git(&checkout, &["config", "user.email", "test@example.invalid"]);
    run_git(&checkout, &["config", "user.name", "Test User"]);
    run_git(&checkout, &["config", "commit.gpgsign", "false"]);
    std::fs::write(checkout.join("README.md"), "reviewed base\n").unwrap();
    run_git(&checkout, &["add", "README.md"]);
    run_git(&checkout, &["commit", "-m", "reviewed base"]);
    let base_sha = git_stdout(&checkout, &["rev-parse", "HEAD"]);

    let mut state = WorkState {
        phase: WorkPhase::Running,
        work_id: work_id.clone(),
        envelope_digest: "digest".to_string(),
        result_note_id: format!("fixed-note-{work_id}"),
        milestone_posted: false,
        worker_id: Some("impl-terminal".to_string()),
        result_ref: None,
        checkout,
        push_attempts: 0,
        next_push_after: None,
        result_summary: None,
        evidence_excerpt: None,
        result_status: None,
        base_sha: Some(base_sha),
        held_commit_sha: None,
        closure_digest: None,
        base_ref: Some("main".to_string()),
        remote_url: Some("file:///tmp/a2a-work-loop.git".to_string()),
        held_since: None,
        hold_note_id: None,
        hold_hit_count: None,
        observed_terminal: None,
    };
    work_exec::save_state(&receiver, &state).unwrap();
    state.checkout = work_exec::load_state(&receiver, &work_id)
        .unwrap()
        .unwrap()
        .checkout;

    Fixture {
        _tmp: tmp,
        sender,
        receiver,
        registry,
        work_id,
        sender_gw,
        receiver_gw,
    }
}

#[derive(Clone, Default)]
struct Counts {
    touches: usize,
    done_transitions: usize,
    failed_transitions: usize,
    expired_sweeps: usize,
    abandoned_sweeps: usize,
    release_stalled_sweeps: usize,
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

    async fn row(&self, fleet: &str, work_id: &str) -> WorkRow {
        self.inner.work_get(fleet, work_id).await.unwrap().unwrap()
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
    async fn discover_unverified(&self, fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> {
        self.inner.discover_unverified(fleet_id).await
    }
    async fn revoke(&self, fleet_id: &str, gateway_id: &str, revocation: &str, now: chrono::DateTime<Utc>) -> Result<(), AppError> {
        self.inner.revoke(fleet_id, gateway_id, revocation, now).await
    }
    async fn schema_version(&self) -> Result<i32, AppError> {
        self.inner.schema_version().await
    }
    async fn work_insert(&self, row: WorkRow) -> Result<(), AppError> {
        self.inner.work_insert(row).await
    }
    async fn work_claim(&self, fleet_id: &str, work_id: &str, claimer_gw: &str) -> Result<ClaimOutcome, AppError> {
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
        if to == WorkStatus::Done {
            self.counts.lock().unwrap().done_transitions += 1;
        }
        if to == WorkStatus::Failed {
            self.counts.lock().unwrap().failed_transitions += 1;
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
        self.counts.lock().unwrap().touches += 1;
        self.inner.work_touch_running(fleet_id, work_id, claimer).await
    }
    async fn work_sweep_expired(&self, fleet_id: &str, swept_by: &str) -> Result<Vec<String>, AppError> {
        self.counts.lock().unwrap().expired_sweeps += 1;
        self.inner.work_sweep_expired(fleet_id, swept_by).await
    }
    async fn work_sweep_abandoned(&self, fleet_id: &str, claimer_gw: &str, stall: Duration) -> Result<Vec<WorkRow>, AppError> {
        self.counts.lock().unwrap().abandoned_sweeps += 1;
        self.inner.work_sweep_abandoned(fleet_id, claimer_gw, stall).await
    }
    async fn work_sweep_release_stalled(&self, fleet_id: &str, claimer_gw: &str, horizon: Duration) -> Result<Vec<WorkRow>, AppError> {
        self.counts.lock().unwrap().release_stalled_sweeps += 1;
        self.inner.work_sweep_release_stalled(fleet_id, claimer_gw, horizon).await
    }
}

#[derive(Clone)]
struct FakeIo {
    completion: WorkCompletion,
    pushes: Arc<Mutex<usize>>,
    pushed_refs: Arc<Mutex<BTreeSet<String>>>,
    fail_push: Arc<Mutex<bool>>,
}

impl FakeIo {
    fn terminal() -> Self {
        Self {
            completion: WorkCompletion::Done {
                summary: "finished".to_string(),
                evidence_excerpt: "Authorization: Bearer sk-testtesttesttesttesttesttesttesttest".to_string(),
            },
            pushes: Arc::new(Mutex::new(0)),
            pushed_refs: Arc::new(Mutex::new(BTreeSet::new())),
            fail_push: Arc::new(Mutex::new(false)),
        }
    }

    fn failing_push() -> Self {
        Self {
            completion: WorkCompletion::Done {
                summary: "finished".to_string(),
                evidence_excerpt: "no secrets".to_string(),
            },
            pushes: Arc::new(Mutex::new(0)),
            pushed_refs: Arc::new(Mutex::new(BTreeSet::new())),
            fail_push: Arc::new(Mutex::new(true)),
        }
    }
}

impl WorkCompletionIo for FakeIo {
    fn reconcile_worker(
        &self,
        _home: &A2aHome,
        _cfg: &Config,
        _worker_id: &str,
    ) -> Result<WorkCompletion, AppError> {
        Ok(self.completion.clone())
    }

    fn push_result_branch(
        &self,
        _checkout: &Path,
        work_id: &str,
        _remote_url: &str,
        _held: &a2a::mesh::work_gate::ResultRange,
    ) -> Result<(), AppError> {
        if !*self.fail_push.lock().unwrap() {
            let inserted = self.pushed_refs.lock().unwrap().insert(work_id.to_string());
            if inserted {
                *self.pushes.lock().unwrap() += 1;
            }
            return Ok(());
        }
        if *self.fail_push.lock().unwrap() {
            *self.pushes.lock().unwrap() += 1;
            return Err(AppError::new("WORK_PUSH_FAILED", ExitCode::Precondition, "push failed"));
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn completion_tick_is_idempotent_across_every_phase_boundary() {
    for stop_after in [
        CompletionStopAfter::Pushed,
        CompletionStopAfter::Persisted,
        CompletionStopAfter::Transitioned,
        CompletionStopAfter::Spooled,
        CompletionStopAfter::MilestonePosted,
    ] {
        let f = fixture(&format!("work-loop-{stop_after:?}"));
        let cfg = cfg_for(&f.receiver);
        let io = FakeIo::terminal();
        let mut options = WorkLoopOptions::for_config(&cfg);
        options.stop_after = Some(stop_after);
        advance_completion_until(
            &f.receiver,
            &cfg,
            &f.registry,
            &io,
            work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap(),
            options,
        )
        .await
        .unwrap();

        let mut finish_options = WorkLoopOptions::for_config(&cfg);
        finish_options.now = Some(Utc::now() + chrono::Duration::minutes(1));
        let report = work_exec::work_loop_tick(&f.receiver, &cfg, &f.registry, &io, finish_options).await;
        assert_eq!(report.registry_error, None, "{report:?}");

        let state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
        assert_eq!(state.phase, WorkPhase::Reported);
        let expected_ref = format!("a2a/work/{}", f.work_id);
        assert_eq!(state.result_ref.as_deref(), Some(expected_ref.as_str()));
        assert!(state.milestone_posted);
        assert!(state.held_commit_sha.is_some());
        assert_eq!(state.closure_digest.as_deref().map(str::len), Some(64));
        let row = f.registry.row(&fleet_id(&f.receiver).unwrap(), &f.work_id).await;
        assert_eq!(row.status, WorkStatus::Done);
        assert_eq!(row.result_ref.as_deref(), Some(expected_ref.as_str()));

        let outbound = spooled_result_notes(&f.receiver, &f.sender_gw);
        assert_eq!(outbound.len(), 1, "fixed note id prevents duplicate spool files after {stop_after:?}");
        assert_eq!(outbound[0]["note"]["note_id"], json!(format!("fixed-note-{}", f.work_id)));
        let evidence = outbound[0]["note"]["refs"]["result"]["evidence_excerpt"]
            .as_str()
            .unwrap();
        assert!(evidence.contains("[REDACTED:"), "{evidence}");
        assert!(!evidence.contains("sk-testtesttesttesttesttesttesttesttest"), "{evidence}");
        assert_eq!(count_feed_milestones(&f.receiver, &f.work_id), 1);
        assert_eq!(*io.pushes.lock().unwrap(), 1, "push side effect count after {stop_after:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_worker_halts_at_awaiting_release_and_announces_it_once() {
    let f = fixture("work-loop-held");
    let mut cfg = cfg_for(&f.receiver);
    cfg.mesh.work.auto_push = false;
    let checkout = f.receiver.work_dir().join(&f.work_id).join("repo");
    let bare = f._tmp.path().join("held-remote.git");
    run_git(f._tmp.path(), &["init", "--bare", bare.to_str().unwrap()]);
    std::fs::write(checkout.join("result.txt"), "review me\n").unwrap();
    run_git(&checkout, &["add", "result.txt"]);
    run_git(&checkout, &["commit", "-m", "held result"]);
    let mut state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
    state.remote_url = Some(format!("file://{}", bare.display()));
    work_exec::save_state(&f.receiver, &state).unwrap();

    let report = work_exec::work_loop_tick(
        &f.receiver,
        &cfg,
        &f.registry,
        &FakeIo::terminal(),
        WorkLoopOptions::for_config(&cfg),
    )
    .await;
    assert_eq!(report.registry_error, None, "{report:?}");
    let state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
    assert_eq!(state.phase, WorkPhase::AwaitingRelease);
    assert!(state.held_since.is_some());
    assert_eq!(state.hold_hit_count, Some(0));
    let row = f
        .registry
        .row(&fleet_id(&f.receiver).unwrap(), &f.work_id)
        .await;
    assert_eq!(row.status, WorkStatus::AwaitingRelease);

    let pushed = Command::new("git")
        .args([
            "--git-dir",
            bare.to_str().unwrap(),
            "show-ref",
            &format!("refs/heads/a2a/work/{}", f.work_id),
        ])
        .output()
        .unwrap();
    assert!(!pushed.status.success(), "held work must not be pushed");

    let held = spooled_notes_of_kind(&f.receiver, &f.sender_gw, work_store::WORK_HELD_KIND);
    assert_eq!(held.len(), 1, "one hold announcement: {held:?}");
    let note_id = held[0]["trace_id"].as_str().unwrap();
    assert_ne!(note_id, state.result_note_id);
    ulid::Ulid::from_string(note_id).unwrap();
    assert_eq!(held[0]["note"]["kind"], work_store::WORK_HELD_KIND);
    let refs = held[0]["note"]["refs"].as_object().unwrap();
    assert_eq!(refs.keys().collect::<Vec<_>>(), vec!["held"]);
    let held_ref = refs["held"].as_object().unwrap();
    let keys = held_ref.keys().map(String::as_str).collect::<BTreeSet<_>>();
    assert_eq!(
        keys,
        BTreeSet::from(["held_since", "hit_count", "status", "work_id"])
    );
    assert_eq!(held_ref["work_id"], f.work_id);
    assert_eq!(held_ref["status"], "held");

    let second = work_exec::work_loop_tick(
        &f.receiver,
        &cfg,
        &f.registry,
        &FakeIo::terminal(),
        WorkLoopOptions::for_config(&cfg),
    )
    .await;
    assert_eq!(second.registry_error, None, "{second:?}");
    assert_eq!(
        spooled_notes_of_kind(&f.receiver, &f.sender_gw, work_store::WORK_HELD_KIND).len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn release_pushes_transitions_notes_and_posts_the_milestone_and_is_idempotent() {
    let f = fixture("work-loop-release");
    let mut cfg = cfg_for(&f.receiver);
    cfg.mesh.work.auto_push = false;
    let checkout = f.receiver.work_dir().join(&f.work_id).join("repo");
    let bare = f._tmp.path().join("release-remote.git");
    run_git(f._tmp.path(), &["init", "--bare", bare.to_str().unwrap()]);
    std::fs::write(checkout.join("released.txt"), "released\n").unwrap();
    run_git(&checkout, &["add", "released.txt"]);
    run_git(&checkout, &["commit", "-m", "release result"]);
    let expected_sha = git_stdout(&checkout, &["rev-parse", "HEAD"]);
    let mut state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
    state.remote_url = Some(format!("file://{}", bare.display()));
    work_exec::save_state(&f.receiver, &state).unwrap();

    let held = work_exec::work_loop_tick(
        &f.receiver,
        &cfg,
        &f.registry,
        &FakeIo::terminal(),
        WorkLoopOptions::for_config(&cfg),
    )
    .await;
    assert_eq!(held.registry_error, None, "{held:?}");
    assert_eq!(
        work_exec::load_state(&f.receiver, &f.work_id)
            .unwrap()
            .unwrap()
            .phase,
        WorkPhase::AwaitingRelease
    );

    common::a2a(f.receiver.root(), f._tmp.path())
        .args(["--json", "mesh", "work", "release", &f.work_id])
        .assert()
        .success();

    let remote = git_stdout(
        f._tmp.path(),
        &[
            "--git-dir",
            bare.to_str().unwrap(),
            "rev-parse",
            &format!("refs/heads/a2a/work/{}", f.work_id),
        ],
    );
    assert_eq!(remote, expected_sha);
    let state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
    assert_eq!(state.phase, WorkPhase::Reported);
    let row = f
        .registry
        .row(&fleet_id(&f.receiver).unwrap(), &f.work_id)
        .await;
    assert_eq!(row.status, WorkStatus::Done);
    assert_eq!(row.result_ref.as_deref(), Some(format!("a2a/work/{}", f.work_id).as_str()));
    assert_eq!(spooled_result_notes(&f.receiver, &f.sender_gw).len(), 1);
    assert_eq!(count_feed_milestones(&f.receiver, &f.work_id), 1);

    common::a2a(f.receiver.root(), f._tmp.path())
        .args(["--json", "mesh", "work", "release", &f.work_id])
        .assert()
        .success();
    assert_eq!(spooled_result_notes(&f.receiver, &f.sender_gw).len(), 1);
    assert_eq!(count_feed_milestones(&f.receiver, &f.work_id), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn push_failure_backs_off_and_retry_push_forces_one_attempt() {
    let f = fixture("work-loop-retry-push");
    let cfg = cfg_for(&f.receiver);
    let io = FakeIo::failing_push();
    let mut options = WorkLoopOptions::for_config(&cfg);
    options.now = Some(Utc.timestamp_opt(1_700_000_000, 0).unwrap());
    let report = work_exec::work_loop_tick(&f.receiver, &cfg, &f.registry, &io, options.clone()).await;
    assert_eq!(report.push_failures, 1, "{report:?}");
    assert_eq!(*io.pushes.lock().unwrap(), 1);

    let state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
    assert_eq!(state.phase, WorkPhase::Pushing);
    assert_eq!(state.push_attempts, 1);
    assert!(state.next_push_after.unwrap() <= Utc.timestamp_opt(1_700_000_010, 0).unwrap());

    let skipped = work_exec::work_loop_tick(&f.receiver, &cfg, &f.registry, &io, options.clone()).await;
    assert_eq!(skipped.skipped_backoff, 1, "{skipped:?}");
    assert_eq!(*io.pushes.lock().unwrap(), 1);

    let mut forced = options;
    forced.force_push = true;
    let forced_report = work_exec::work_loop_tick(&f.receiver, &cfg, &f.registry, &io, forced).await;
    assert_eq!(forced_report.push_failures, 1, "{forced_report:?}");
    assert_eq!(*io.pushes.lock().unwrap(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn tick_materializes_pool_feed_and_runs_expiry_and_abandoned_sweeps() {
    let f = fixture("work-loop-sweeps");
    let cfg = cfg_for(&f.receiver);
    let fleet = fleet_id(&f.receiver).unwrap();
    let pool_work_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let pool_env = sign_envelope(
        &f.sender,
        UnsignedWorkEnvelope {
            work_id: pool_work_id.to_string(),
            session: "user:test".to_string(),
            addressing: WorkAddressing::Pool,
            repo: WorkRepo {
                remote_url: "file:///tmp/a2a-pool.git".to_string(),
                base_ref: "main".to_string(),
            },
            title: "Pool materialization".to_string(),
            brief_text: "---\nlane: impl\nacceptance_criteria:\n  - focused test passes\nverify:\n  - cargo test --test mesh_work_loop\n---\n# Build it\n".to_string(),
            acceptance: "focused test passes".to_string(),
            ttl_days: 7,
            max_bytes: 32_768,
        },
        &SystemClock,
    )
    .unwrap();
    a2a::mesh::feed::post(
        &f.receiver,
        &cfg,
        &a2a::mesh::PeerId {
            provider: a2a::mesh::Provider::User,
            uuid: "mesh-work".to_string(),
        },
        &format!("work offered: Pool materialization ({pool_work_id})"),
        json!({"work": pool_env}),
    )
    .unwrap();

    let now = Utc::now();
    f.registry
        .work_insert(WorkRow {
            fleet_id: fleet.clone(),
            work_id: "expired-offer".to_string(),
            sender_gw: f.sender_gw.clone(),
            addressing: "pool".to_string(),
            status: WorkStatus::Offered,
            claimed_by: None,
            result_ref: None,
            result_delivery: None,
            swept_by: None,
            created_at: now - chrono::Duration::days(10),
            updated_at: now - chrono::Duration::days(10),
            expires_at: now - chrono::Duration::days(1),
            schema_version: REGISTRY_SCHEMA_VERSION,
        })
        .await
        .unwrap();
    f.registry
        .work_insert(WorkRow {
            fleet_id: fleet.clone(),
            work_id: "abandoned-mine".to_string(),
            sender_gw: f.sender_gw.clone(),
            addressing: "pool".to_string(),
            status: WorkStatus::Running,
            claimed_by: Some(f.receiver_gw.clone()),
            result_ref: None,
            result_delivery: None,
            swept_by: None,
            created_at: now - chrono::Duration::hours(1),
            updated_at: now - chrono::Duration::hours(1),
            expires_at: now + chrono::Duration::days(1),
            schema_version: REGISTRY_SCHEMA_VERSION,
        })
        .await
        .unwrap();
    f.registry
        .work_insert(WorkRow {
            fleet_id: fleet.clone(),
            work_id: "abandoned-foreign".to_string(),
            sender_gw: f.sender_gw.clone(),
            addressing: "pool".to_string(),
            status: WorkStatus::Running,
            claimed_by: Some("gw-foreign".to_string()),
            result_ref: None,
            result_delivery: None,
            swept_by: None,
            created_at: now - chrono::Duration::hours(1),
            updated_at: now - chrono::Duration::hours(1),
            expires_at: now + chrono::Duration::days(1),
            schema_version: REGISTRY_SCHEMA_VERSION,
        })
        .await
        .unwrap();

    let report = work_exec::work_loop_tick(&f.receiver, &cfg, &f.registry, &FakeIo::terminal(), WorkLoopOptions::for_config(&cfg)).await;
    assert!(report.pool_materialized >= 1, "{report:?}");
    assert!(work_store::load(&f.receiver, pool_work_id)
        .unwrap()
        .is_some());
    assert_eq!(f.registry.row(&fleet, "expired-offer").await.status, WorkStatus::Expired);
    assert_eq!(f.registry.row(&fleet, "abandoned-mine").await.status, WorkStatus::Failed);
    assert_eq!(f.registry.row(&fleet, "abandoned-foreign").await.status, WorkStatus::Running);
    let counts = f.registry.counts();
    assert_eq!(counts.expired_sweeps, 1);
    assert_eq!(counts.abandoned_sweeps, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn awaiting_release_survives_the_abandoned_sweep_and_is_swept_only_by_release_stall() {
    let f = fixture("work-loop-release-sweep");
    let fleet = fleet_id(&f.receiver).unwrap();
    let work_id = "release-stall-row";
    let now = Utc::now();
    f.registry
        .work_insert(WorkRow {
            fleet_id: fleet.clone(),
            work_id: work_id.to_string(),
            sender_gw: f.sender_gw.clone(),
            addressing: "pool".to_string(),
            status: WorkStatus::AwaitingRelease,
            claimed_by: Some(f.receiver_gw.clone()),
            result_ref: None,
            result_delivery: None,
            swept_by: None,
            created_at: now - chrono::Duration::hours(3),
            updated_at: now - chrono::Duration::hours(2),
            expires_at: now + chrono::Duration::days(1),
            schema_version: REGISTRY_SCHEMA_VERSION,
        })
        .await
        .unwrap();

    let abandoned = f
        .registry
        .work_sweep_abandoned(&fleet, &f.receiver_gw, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(abandoned.iter().all(|row| row.work_id != work_id));
    assert_eq!(
        f.registry.row(&fleet, work_id).await.status,
        WorkStatus::AwaitingRelease
    );

    let stalled = f
        .registry
        .work_sweep_release_stalled(&fleet, &f.receiver_gw, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(stalled.len(), 1);
    let row = f.registry.row(&fleet, work_id).await;
    assert_eq!(row.status, WorkStatus::Failed);
    assert_eq!(row.swept_by.as_deref(), Some(f.receiver_gw.as_str()));
    assert_eq!(row.result_delivery.as_deref(), Some("release_stalled"));
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_restores_a_release_stalled_item_from_failed() {
    let f = fixture("work-loop-reopen");
    let mut cfg = cfg_for(&f.receiver);
    cfg.mesh.work.auto_push = false;
    let checkout = f.receiver.work_dir().join(&f.work_id).join("repo");
    std::fs::write(checkout.join("result.txt"), "held\n").unwrap();
    run_git(&checkout, &["add", "result.txt"]);
    run_git(&checkout, &["commit", "-m", "held before stall"]);
    work_exec::work_loop_tick(
        &f.receiver,
        &cfg,
        &f.registry,
        &FakeIo::terminal(),
        WorkLoopOptions::for_config(&cfg),
    )
    .await;

    let fleet = fleet_id(&f.receiver).unwrap();
    let aged = f
        .registry
        .work_transition(
            &fleet,
            &f.work_id,
            WorkStatus::AwaitingRelease,
            WorkStatus::AwaitingRelease,
            WorkOwner::Claimer(f.receiver_gw.clone()),
            WorkPatch {
                updated_at: Some(Utc::now() - chrono::Duration::hours(2)),
                ..WorkPatch::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(aged, TransitionOutcome::Applied);
    let swept = f
        .registry
        .work_sweep_release_stalled(&fleet, &f.receiver_gw, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(swept.len(), 1);
    assert_eq!(f.registry.row(&fleet, &f.work_id).await.status, WorkStatus::Failed);

    common::a2a(f.receiver.root(), f._tmp.path())
        .args(["--json", "mesh", "work", "reopen", &f.work_id])
        .assert()
        .success();
    let row = f.registry.row(&fleet, &f.work_id).await;
    assert_eq!(row.status, WorkStatus::AwaitingRelease);
    assert_eq!(row.swept_by, None);
    assert_eq!(row.result_delivery, None);
    assert_eq!(
        work_exec::load_state(&f.receiver, &f.work_id)
            .unwrap()
            .unwrap()
            .phase,
        WorkPhase::AwaitingRelease
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn registry_errors_are_health_events_not_loop_errors() {
    let f = fixture("work-loop-registry-error");
    let cfg = cfg_for(&f.receiver);
    let report =
        work_exec::work_loop_tick(&f.receiver, &cfg, &AlwaysErrRegistry, &FakeIo::terminal(), WorkLoopOptions::for_config(&cfg)).await;
    assert!(report.registry_error.as_deref().unwrap_or("").contains("REGISTRY_ALWAYS_ERR"), "{report:?}");
    assert_eq!(report.restarts_requested, 0);
}

#[test]
fn two_work_ids_colliding_under_safe_file_part_resolve_in_exactly_one_bucket() {
    let f = fixture("work-loop-store-collision");
    let make = |work_id: &str, title: &str| {
        let env = sign_envelope(
            &f.sender,
            UnsignedWorkEnvelope {
                work_id: work_id.to_string(),
                session: "user:test".to_string(),
                addressing: WorkAddressing::Pool,
                repo: WorkRepo {
                    remote_url: "file:///tmp/collision.git".to_string(),
                    base_ref: "main".to_string(),
                },
                title: title.to_string(),
                brief_text: "---\nlane: impl\nacceptance_criteria:\n  - pass\nverify:\n  - true\n---\n# Test\n".to_string(),
                acceptance: "pass".to_string(),
                ttl_days: 7,
                max_bytes: 262_144,
            },
            &SystemClock,
        )
        .unwrap();
        verify_envelope(&f.receiver, &env, &SystemClock).unwrap()
    };
    let first_id = "same/path";
    let second_id = "samepath";
    work_store::store_offer(
        &f.receiver,
        make(first_id, "first"),
        WorkSource::Pool,
    )
    .unwrap();
    work_store::store_offer(
        &f.receiver,
        make(second_id, "second"),
        WorkSource::Pool,
    )
    .unwrap();
    assert_eq!(
        work_store::load(&f.receiver, first_id)
            .unwrap()
            .unwrap()
            .verified
            .envelope
            .title,
        "first"
    );
    assert_eq!(
        work_store::load(&f.receiver, second_id)
            .unwrap()
            .unwrap()
            .verified
            .envelope
            .title,
        "second"
    );

    let duplicate_id = "duplicate/bucket";
    work_store::store_offer(
        &f.receiver,
        make(duplicate_id, "directed"),
        WorkSource::Directed,
    )
    .unwrap();
    work_store::store_offer(
        &f.receiver,
        make(duplicate_id, "pool"),
        WorkSource::Pool,
    )
    .unwrap();
    let err = work_store::load(&f.receiver, duplicate_id).unwrap_err();
    assert_eq!(err.code, "WORK_STATE_CONFLICT");
}

#[test]
fn load_state_files_skips_an_unparseable_item_and_keeps_scanning() {
    let f = fixture("work-loop-bad-state");
    let bad_id = ulid::Ulid::new().to_string();
    let bad_dir = f.receiver.work_dir().join(&bad_id);
    std::fs::create_dir_all(&bad_dir).unwrap();
    std::fs::write(bad_dir.join("state.json"), b"{not-json\n").unwrap();

    let states = work_exec::load_state_files(&f.receiver).unwrap();
    assert!(states.iter().any(|entry| entry.state.work_id == f.work_id));
    assert!(!states.iter().any(|entry| entry.state.work_id == bad_id));
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_result_ack_records_result_delivery_on_registry_row() {
    let f = fixture("work-loop-rejected-ack");
    let cfg = cfg_for(&f.receiver);
    let report = work_exec::work_loop_tick(
        &f.receiver,
        &cfg,
        &f.registry,
        &FakeIo::terminal(),
        WorkLoopOptions::for_config(&cfg),
    )
    .await;
    assert_eq!(report.registry_error, None, "{report:?}");

    assert!(notes_wire::handle_ack(
        &f.receiver,
        &f.sender_gw,
        &format!("fixed-note-{}", f.work_id),
        "WORK_TOO_LARGE",
    )
    .unwrap());

    let fleet = fleet_id(&f.receiver).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let row = f.registry.row(&fleet, &f.work_id).await;
        if row.result_delivery.as_deref() == Some("rejected:WORK_TOO_LARGE") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "result_delivery was not recorded: {row:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[test]
fn push_uses_recorded_sha_and_remote_url_and_ignores_hooks_and_pushurl() {
    let f = fixture("work-loop-real-git");
    let cfg = cfg_for(&f.receiver);
    let checkout = f.receiver.work_dir().join(&f.work_id).join("repo");
    let bare = f._tmp.path().join("remote.git");
    run_git(f._tmp.path(), &["init", "--bare", bare.to_str().unwrap()]);
    std::fs::write(checkout.join("result.txt"), "result\n").unwrap();
    run_git(&checkout, &["add", "result.txt"]);
    run_git(&checkout, &["commit", "-m", "result"]);
    run_git(&checkout, &["remote", "add", "origin", &format!("file://{}", bare.display())]);
    run_git(&checkout, &["config", "remote.origin.pushurl", "file:///dev/null"]);
    run_git(&checkout, &["config", "diff.external", "/bin/false"]);
    let hook = checkout.join(".git/hooks/pre-push");
    std::fs::write(&hook, "#!/bin/sh\ntouch \"$PWD/HOOK_RAN\"\n").unwrap();
    let mut permissions = std::fs::metadata(&hook).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&hook, permissions).unwrap();

    let mut state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
    state.phase = WorkPhase::Pushing;
    state.result_status = Some(WorkStatus::Done);
    state.result_summary = Some("done".to_string());
    state.evidence_excerpt = Some("clean".to_string());
    let held = a2a::mesh::work_gate::freeze_range(
        &checkout,
        state.base_sha.as_deref().unwrap(),
    )
    .unwrap();
    state.held_commit_sha = Some(held.head_sha.clone());
    state.closure_digest = Some(held.closure_digest.clone());
    state.remote_url = Some(format!("file://{}", bare.display()));
    work_exec::save_state(&f.receiver, &state).unwrap();

    block_on(work_exec::work_loop_tick(
        &f.receiver,
        &cfg,
        &f.registry,
        &RealWorkCompletionIo,
        WorkLoopOptions::for_config(&cfg),
    ));
    let heads = Command::new("git")
        .args(["--git-dir", bare.to_str().unwrap(), "show-ref"])
        .arg(format!("refs/heads/a2a/work/{}", f.work_id))
        .output()
        .unwrap();
    assert!(heads.status.success(), "{}", String::from_utf8_lossy(&heads.stderr));
    assert_eq!(
        String::from_utf8_lossy(&heads.stdout)
            .split_whitespace()
            .next(),
        Some(held.head_sha.as_str())
    );
    assert!(!checkout.join("HOOK_RAN").exists());

    let retry = common::a2a(f.receiver.root(), f._tmp.path())
        .args(["--json", "mesh", "work", "retry-push", &f.work_id])
        .assert()
        .success();
    let out: Value = serde_json::from_slice(&retry.get_output().stdout).unwrap();
    assert_eq!(out["data"]["forced"], true, "{out}");
}

#[tokio::test(flavor = "multi_thread")]
async fn push_is_refused_when_the_result_carries_a_credential() {
    let f = fixture("work-loop-secret-gate");
    let cfg = cfg_for(&f.receiver);
    let checkout = f.receiver.work_dir().join(&f.work_id).join("repo");
    let bare = f._tmp.path().join("secret-gate-remote.git");
    run_git(f._tmp.path(), &["init", "--bare", bare.to_str().unwrap()]);
    std::fs::write(
        checkout.join("credentials.txt"),
        "postgresql://a2a:push-only-secret@db.example.invalid/mesh\n",
    )
    .unwrap();
    run_git(&checkout, &["add", "--", "credentials.txt"]);
    run_git(&checkout, &["commit", "-m", "credential-bearing result"]);

    let mut state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
    state.phase = WorkPhase::Pushing;
    state.result_status = Some(WorkStatus::Done);
    state.result_summary = Some("done".to_string());
    state.evidence_excerpt = Some("clean".to_string());
    let held = a2a::mesh::work_gate::freeze_range(
        &checkout,
        state.base_sha.as_deref().unwrap(),
    )
    .unwrap();
    state.held_commit_sha = Some(held.head_sha);
    state.closure_digest = Some(held.closure_digest);
    state.remote_url = Some(format!("file://{}", bare.display()));
    work_exec::save_state(&f.receiver, &state).unwrap();

    let report = work_exec::work_loop_tick(
        &f.receiver,
        &cfg,
        &f.registry,
        &RealWorkCompletionIo,
        WorkLoopOptions::for_config(&cfg),
    )
    .await;
    assert_eq!(report.item_errors, 0, "{report:?}");

    let pushed = Command::new("git")
        .args([
            "--git-dir",
            bare.to_str().unwrap(),
            "show-ref",
            &format!("refs/heads/a2a/work/{}", f.work_id),
        ])
        .output()
        .unwrap();
    assert!(!pushed.status.success(), "result branch must not land");
    let saved = work_exec::load_state(&f.receiver, &f.work_id)
        .unwrap()
        .unwrap();
    assert_eq!(saved.phase, WorkPhase::AwaitingRelease);
    let summary = saved.result_summary.as_deref().unwrap_or_default();
    assert!(summary.contains("WORK_SECRET_IN_RESULT"), "{saved:?}");
    assert!(!summary.contains("push-only-secret"), "{saved:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn push_refuses_after_head_moves_past_the_recorded_sha() {
    let f = fixture("work-loop-head-moved");
    let cfg = cfg_for(&f.receiver);
    let checkout = f.receiver.work_dir().join(&f.work_id).join("repo");
    let bare = f._tmp.path().join("head-moved-remote.git");
    run_git(f._tmp.path(), &["init", "--bare", bare.to_str().unwrap()]);
    std::fs::write(checkout.join("result.txt"), "held result\n").unwrap();
    run_git(&checkout, &["add", "result.txt"]);
    run_git(&checkout, &["commit", "-m", "held result"]);

    let mut state = work_exec::load_state(&f.receiver, &f.work_id).unwrap().unwrap();
    state.phase = WorkPhase::Pushing;
    state.result_status = Some(WorkStatus::Done);
    let held = a2a::mesh::work_gate::freeze_range(
        &checkout,
        state.base_sha.as_deref().unwrap(),
    )
    .unwrap();
    state.held_commit_sha = Some(held.head_sha);
    state.closure_digest = Some(held.closure_digest);
    state.remote_url = Some(format!("file://{}", bare.display()));
    work_exec::save_state(&f.receiver, &state).unwrap();

    std::fs::write(checkout.join("after-review.txt"), "moved\n").unwrap();
    run_git(&checkout, &["add", "after-review.txt"]);
    run_git(&checkout, &["commit", "-m", "move after review"]);

    let err = advance_completion_until(
        &f.receiver,
        &cfg,
        &f.registry,
        &RealWorkCompletionIo,
        state,
        WorkLoopOptions::for_config(&cfg),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "WORK_HEAD_MOVED", "{err:?}");
    let pushed = Command::new("git")
        .args([
            "--git-dir",
            bare.to_str().unwrap(),
            "show-ref",
            &format!("refs/heads/a2a/work/{}", f.work_id),
        ])
        .output()
        .unwrap();
    assert!(!pushed.status.success());
}

fn spooled_result_notes(home: &A2aHome, to_gateway: &str) -> Vec<Value> {
    spooled_notes_of_kind(home, to_gateway, work_store::WORK_RESULT_KIND)
}

fn spooled_notes_of_kind(home: &A2aHome, to_gateway: &str, kind: &str) -> Vec<Value> {
    let dir = home.root().join("mesh").join("outbound").join(to_gateway);
    let mut notes = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|entry| serde_json::from_str::<Value>(&std::fs::read_to_string(entry.path()).unwrap()).unwrap())
        .filter(|value| value["note"]["kind"] == kind)
        .collect::<Vec<_>>();
    notes.sort_by(|a, b| a["trace_id"].as_str().cmp(&b["trace_id"].as_str()));
    notes
}

fn count_feed_milestones(home: &A2aHome, work_id: &str) -> usize {
    a2a::mesh::feed::read(home, 0)
        .unwrap()
        .into_iter()
        .filter(|entry| entry.refs["work_id"] == work_id && entry.text.contains("work done:"))
        .count()
}

fn run_git(cwd: &Path, args: &[&str]) {
    std::fs::create_dir_all(cwd).unwrap();
    let out = Command::new("git").current_dir(cwd).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?} failed\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn git_stdout(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

struct AlwaysErrRegistry;

#[allow(async_fn_in_trait)]
impl Registry for AlwaysErrRegistry {
    async fn upsert_self(&self, _row: GatewayRow) -> Result<(), AppError> { unreachable!() }
    async fn heartbeat(&self, _fleet_id: &str, _gateway_id: &str, _now: chrono::DateTime<Utc>) -> Result<(), AppError> { unreachable!() }
    async fn discover_unverified(&self, _fleet_id: &str) -> Result<Vec<GatewayRow>, AppError> { Err(always_err()) }
    async fn revoke(&self, _fleet_id: &str, _gateway_id: &str, _revocation: &str, _now: chrono::DateTime<Utc>) -> Result<(), AppError> { unreachable!() }
    async fn schema_version(&self) -> Result<i32, AppError> { Err(always_err()) }
    async fn work_insert(&self, _row: WorkRow) -> Result<(), AppError> { Err(always_err()) }
    async fn work_claim(&self, _fleet_id: &str, _work_id: &str, _claimer_gw: &str) -> Result<ClaimOutcome, AppError> { Err(always_err()) }
    async fn work_transition(&self, _fleet_id: &str, _work_id: &str, _from: WorkStatus, _to: WorkStatus, _owner: WorkOwner, _patch: WorkPatch) -> Result<TransitionOutcome, AppError> { Err(always_err()) }
    async fn work_get(&self, _fleet_id: &str, _work_id: &str) -> Result<Option<WorkRow>, AppError> { Err(always_err()) }
    async fn work_list(&self, _fleet_id: &str, _filter: WorkListFilter) -> Result<Vec<WorkRow>, AppError> { Err(always_err()) }
    async fn work_touch_running(&self, _fleet_id: &str, _work_id: &str, _claimer: &str) -> Result<(), AppError> { Err(always_err()) }
    async fn work_sweep_expired(&self, _fleet_id: &str, _swept_by: &str) -> Result<Vec<String>, AppError> { Err(always_err()) }
    async fn work_sweep_abandoned(&self, _fleet_id: &str, _claimer_gw: &str, _stall: Duration) -> Result<Vec<WorkRow>, AppError> { Err(always_err()) }
    async fn work_sweep_release_stalled(&self, _fleet_id: &str, _claimer_gw: &str, _horizon: Duration) -> Result<Vec<WorkRow>, AppError> { Err(always_err()) }
}

fn always_err() -> AppError {
    AppError::new("REGISTRY_ALWAYS_ERR", ExitCode::BrokerUnavailable, "registry unavailable")
}
