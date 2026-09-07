use crate::brokerclient;
use crate::cli::SpawnArgs;
use crate::config::{Config, Lane};
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::mesh::keys::{self, SystemClock};
use crate::mesh::registry::{
    ClaimOutcome, Registry, TransitionOutcome, WorkOwner, WorkPatch, WorkRow, WorkStatus,
};
use crate::mesh::row_mark::RevocationCache;
use crate::mesh::row_sig::{Disposition, RowVerdict};
use crate::mesh::work::{canonical_envelope_without_sig, verify_envelope, VerifiedEnvelope};
use crate::mesh::{
    feed,
    gateway::{notes_wire, presence_wire},
    work_gate, Note, PeerId, Provider,
};
use crate::mesh::work_store;
use crate::output::{AppError, ExitCode};
use crate::redact;
use crate::{fsm::WorkerState, reconcile};
use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use nix::fcntl::{Flock, FlockArg};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkPhase {
    Claiming,
    Cloning,
    Spawning,
    Running,
    AwaitingRelease,
    Pushing,
    Reporting,
    Reported,
    Failed,
    Rejected,
}

impl WorkPhase {
    pub fn is_non_terminal_work_hold(self) -> bool {
        matches!(
            self,
            WorkPhase::Claiming
                | WorkPhase::Cloning
                | WorkPhase::Spawning
                | WorkPhase::Running
                | WorkPhase::AwaitingRelease
                | WorkPhase::Pushing
                | WorkPhase::Reporting
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkState {
    pub phase: WorkPhase,
    pub work_id: String,
    pub envelope_digest: String,
    pub result_note_id: String,
    pub milestone_posted: bool,
    pub worker_id: Option<String>,
    pub result_ref: Option<String>,
    pub checkout: PathBuf,
    #[serde(default)]
    pub push_attempts: u32,
    #[serde(default)]
    pub next_push_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub result_summary: Option<String>,
    #[serde(default)]
    pub evidence_excerpt: Option<String>,
    #[serde(default)]
    pub result_status: Option<WorkStatus>,
    #[serde(default)]
    pub base_sha: Option<String>,
    #[serde(default)]
    pub held_commit_sha: Option<String>,
    #[serde(default)]
    pub closure_digest: Option<String>,
    #[serde(default)]
    pub base_ref: Option<String>,
    #[serde(default)]
    pub remote_url: Option<String>,
    #[serde(default)]
    pub held_since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub hold_note_id: Option<String>,
    #[serde(default)]
    pub hold_hit_count: Option<usize>,
    #[serde(default)]
    pub observed_terminal: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocatedWorkState {
    pub state: WorkState,
    pub dir: PathBuf,
    pub state_path: PathBuf,
    pub modified_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ApproveOutcome {
    Running {
        work_id: String,
        worker_id: String,
        checkout: PathBuf,
    },
    Lost {
        work_id: String,
        status: String,
        claimed_by: Option<String>,
    },
    Interrupted {
        work_id: String,
        phase: WorkPhase,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApproveStopAfter {
    StateWritten,
    Claimed,
    Cloned,
    Spawned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorroborationStage {
    Ingest,
    Approve,
}

pub fn corroborate_sender(
    home: &A2aHome,
    verified: &VerifiedEnvelope,
    stage: CorroborationStage,
    require_signed_rows: bool,
) -> Result<Option<String>, AppError> {
    let claims = &verified.sender_claims;
    if RevocationCache::open(home)?.is_revoked(&claims.gateway_id, claims.key_epoch) {
        return Err(sender_row_invalid(
            claims.gateway_id.clone(),
            "REVOKED",
            format!(
                "work sender {} key epoch {} is revoked by the local root-verified cache",
                claims.gateway_id, claims.key_epoch
            ),
        ));
    }

    let cache_available = presence_wire::registry_cache_available(home);
    let cached = match presence_wire::cached_gateway(home, &claims.gateway_id) {
        Ok(cached) => cached,
        Err(error) => {
            let detail = format!(
                "sender {} registry cache is unavailable: {}: {}",
                claims.gateway_id, error.code, error.message
            );
            return degrade_at_ingest_or_refuse(
                stage,
                claims.gateway_id.clone(),
                "CACHE_UNAVAILABLE",
                detail,
            );
        }
    };
    let Some(cached) = cached else {
        let detail = if cache_available {
            format!(
                "sender {} row is absent from the registry cache",
                claims.gateway_id
            )
        } else {
            format!(
                "sender {} registry cache is unavailable; its row cannot be corroborated",
                claims.gateway_id
            )
        };
        return degrade_at_ingest_or_refuse(
            stage,
            claims.gateway_id.clone(),
            if cache_available {
                "ROW_ABSENT"
            } else {
                "CACHE_UNAVAILABLE"
            },
            detail,
        );
    };
    let verdict_label = cached.verdict.as_deref().unwrap_or("UNKNOWN");
    let Some(verdict) = row_verdict_from_label(verdict_label) else {
        return degrade_at_ingest_or_refuse(
            stage,
            claims.gateway_id.clone(),
            verdict_label,
            format!(
                "sender {} registry row has unknown cached verdict {verdict_label}",
                claims.gateway_id
            ),
        );
    };

    match verdict.disposition(require_signed_rows) {
        Disposition::Refused => degrade_at_ingest_or_refuse(
            stage,
            claims.gateway_id.clone(),
            verdict_label,
            format!(
                "sender {} registry row verdict {verdict_label} is refused under require_signed_rows={require_signed_rows}",
                claims.gateway_id
            ),
        ),
        Disposition::UsableWarn => Ok(Some(format!(
            "sender {} registry row is {verdict_label} and only usable with a warning; verified row_msg.pubkey_fpr check was skipped",
            claims.gateway_id
        ))),
        Disposition::Usable => {
            let envelope_fpr = keys::fpr_of_pubkey_b64(&verified.envelope.sender.pubkey_b64)
                .map_err(|error| {
                    sender_row_invalid(
                        claims.gateway_id.clone(),
                        verdict_label,
                        format!(
                            "sender {} envelope public key fingerprint cannot be computed: {}",
                            claims.gateway_id, error.message
                        ),
                    )
                })?;
            if cached.pubkey_fpr.as_deref() != Some(envelope_fpr.as_str()) {
                return Err(sender_row_invalid(
                    claims.gateway_id.clone(),
                    verdict_label,
                    format!(
                        "sender {} verified row_msg.pubkey_fpr {:?} does not match envelope key {envelope_fpr}",
                        claims.gateway_id, cached.pubkey_fpr
                    ),
                ));
            }
            Ok(None)
        }
    }
}

fn degrade_at_ingest_or_refuse(
    stage: CorroborationStage,
    gateway_id: String,
    verdict: &str,
    detail: String,
) -> Result<Option<String>, AppError> {
    match stage {
        CorroborationStage::Ingest => Ok(Some(format!("{detail}; proceeding at ingest"))),
        CorroborationStage::Approve => Err(sender_row_invalid(gateway_id, verdict, detail)),
    }
}

fn sender_row_invalid(gateway_id: String, verdict: &str, detail: String) -> AppError {
    AppError::new(
        "WORK_SENDER_ROW_INVALID",
        ExitCode::Precondition,
        detail,
    )
    .remedy(
        "capture the sender row with `a2a mesh fleet ls --json`; approve only after its flag-resolved disposition and verified key binding are usable",
    )
    .data(json!({"gateway_id": gateway_id, "verdict": verdict}))
}

fn row_verdict_from_label(label: &str) -> Option<RowVerdict> {
    match label {
        "valid-signed" => Some(RowVerdict::ValidSigned),
        "UNSIGNED" => Some(RowVerdict::Unsigned { stripped: false }),
        "UNSIGNED (stripped)" => Some(RowVerdict::Unsigned { stripped: true }),
        "UNSIGNED-STALE" => Some(RowVerdict::UnsignedStale),
        "UNVERIFIED" => Some(RowVerdict::Unverified),
        "BAD_SIG" => Some(RowVerdict::BadSig),
        "ROW_SIG_INCOMPLETE" => Some(RowVerdict::RowSigIncomplete),
        "ROW_SIG_UNKNOWN_VERSION" => Some(RowVerdict::RowSigUnknownVersion),
        "ROW_KEY_SUPERSEDED" => Some(RowVerdict::RowKeySuperseded),
        "ROW_REPLAYED" => Some(RowVerdict::RowReplayed),
        "ROW_CLOCK_AHEAD" => Some(RowVerdict::RowClockAhead),
        "REVOKED" => Some(RowVerdict::Revoked),
        "ROW_REVOCATION_MISPLACED" => Some(RowVerdict::RowRevocationMisplaced),
        _ => None,
    }
}

#[allow(async_fn_in_trait)]
pub trait GitAdapter {
    async fn ls_remote(&self, remote_url: &str, base_ref: &str) -> Result<(), AppError>;
    async fn clone_checkout(
        &self,
        remote_url: &str,
        base_ref: &str,
        checkout: &Path,
    ) -> Result<(), AppError>;
}

pub trait WorkSpawner {
    fn preflight(&self, home: &A2aHome, checkout: &Path) -> Result<(), AppError>;
    fn spawn(&self, home: &A2aHome, checkout: &Path, brief: &Path) -> Result<String, AppError>;
}

pub struct RealGit;

#[allow(async_fn_in_trait)]
impl GitAdapter for RealGit {
    async fn ls_remote(&self, remote_url: &str, base_ref: &str) -> Result<(), AppError> {
        let output = Command::new("git")
            .args(["ls-remote", "--", remote_url, base_ref])
            .output()
            .map_err(|e| repo_unreachable(remote_url, format!("running git ls-remote: {e}")))?;
        if output.status.success() {
            return Ok(());
        }
        let detail = stderr_or_status(&output);
        Err(repo_unreachable(remote_url, detail))
    }

    async fn clone_checkout(
        &self,
        remote_url: &str,
        base_ref: &str,
        checkout: &Path,
    ) -> Result<(), AppError> {
        if checkout.join(".git").exists() {
            return Ok(());
        }
        if let Some(parent) = checkout.parent() {
            create_dir_0700(parent).map_err(|e| io_err("creating work checkout parent", e))?;
        }
        let status = Command::new("git")
            .args(["clone", "--depth", "50", "--", remote_url])
            .arg(checkout)
            .status()
            .map_err(|e| io_err("running git clone", e))?;
        if !status.success() {
            return Err(AppError::new(
                "WORK_CLONE_FAILED",
                ExitCode::Precondition,
                format!("git clone exited with {status}"),
            ));
        }
        let status = work_gate::git_pinned(checkout)
            .args(["checkout", "--end-of-options"])
            .arg(base_ref)
            .status()
            .map_err(|e| io_err("running git checkout", e))?;
        if !status.success() {
            return Err(AppError::new(
                "WORK_CLONE_FAILED",
                ExitCode::Precondition,
                format!("git checkout {base_ref} exited with {status}"),
            ));
        }
        Ok(())
    }
}

pub struct RealSpawner;

impl WorkSpawner for RealSpawner {
    fn preflight(&self, home: &A2aHome, checkout: &Path) -> Result<(), AppError> {
        if !gateway_running_no_touch(home) {
            return Err(work_daemon_down());
        }
        if let Ok(mut client) = brokerclient::connect(home.root()) {
            let info = client.request("info", json!({}))?;
            ensure_broker_allows_checkout(checkout, &info)?;
        }
        Ok(())
    }

    fn spawn(&self, home: &A2aHome, checkout: &Path, brief: &Path) -> Result<String, AppError> {
        let args = SpawnArgs {
            lane: Some(Lane::Impl),
            cwd: Some(checkout.to_path_buf()),
            brief: Some(brief.to_path_buf()),
            template: None,
            context: Vec::new(),
            schema: None,
            profile: None,
            model: None,
            effort: None,
            transport: None,
            worktree: false,
            no_queue: false,
            owner: None,
            no_worktree: true,
            base_ref: None,
            danger_full_access: false,
            reason: None,
        };
        crate::commands::spawn::run(home, &args, true, true)?
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                AppError::new(
                    "WORK_SPAWN_FAILED",
                    ExitCode::Internal,
                    "spawn did not return a worker id",
                )
            })
    }
}

fn gateway_running_no_touch(home: &A2aHome) -> bool {
    let path = home.root().join("mesh").join(crate::mesh::gateway::GATEWAY_LOCK);
    let Ok(file) = OpenOptions::new().write(true).open(path) else {
        return false;
    };
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(lock) => {
            drop(lock);
            false
        }
        Err((_file, nix::errno::Errno::EWOULDBLOCK)) => true,
        Err(_) => false,
    }
}

pub async fn approve_dry_run<G, S>(
    home: &A2aHome,
    git: &G,
    spawner: &S,
    work_id: &str,
) -> Result<Value, AppError>
where
    G: GitAdapter + ?Sized,
    S: WorkSpawner + ?Sized,
{
    let require_signed_rows = crate::config::load(home)?.0.mesh.gateway.require_signed_rows;
    let preflight = preflight(home, git, spawner, work_id, require_signed_rows).await?;
    Ok(json!({
        "work_id": work_id,
        "sender_gateway": preflight.verified.envelope.sender.gateway_id,
        "sender_session": preflight.verified.envelope.sender.session,
        "token_fleet_id": preflight.verified.sender_claims.fleet_id,
        "repo": preflight.verified.envelope.repo.remote_url,
        "base_ref": preflight.verified.envelope.repo.base_ref,
        "title": preflight.verified.envelope.title,
        "acceptance": preflight.verified.envelope.acceptance,
        "brief_text": preflight.verified.envelope.brief_text,
        "preflight": {
            "repo_reachable": true,
            "checkout_root_admitted": true,
            "daemon_running": true
        }
    }))
}

pub async fn advance_approve<R, G, S>(
    home: &A2aHome,
    registry: &R,
    git: &G,
    spawner: &S,
    work_id: &str,
) -> Result<ApproveOutcome, AppError>
where
    R: Registry + ?Sized,
    G: GitAdapter + ?Sized,
    S: WorkSpawner + ?Sized,
{
    advance_approve_until(home, registry, git, spawner, work_id, None).await
}

pub async fn advance_approve_until<R, G, S>(
    home: &A2aHome,
    registry: &R,
    git: &G,
    spawner: &S,
    work_id: &str,
    stop_after: Option<ApproveStopAfter>,
) -> Result<ApproveOutcome, AppError>
where
    R: Registry + ?Sized,
    G: GitAdapter + ?Sized,
    S: WorkSpawner + ?Sized,
{
    work_store::validate_work_id(work_id)?;
    if let Some(observed_at) = work_store::terminal_observed(home, work_id) {
        let _lock = lock_state(home, work_id)?;
        if let Some(mut state) = load_state_unlocked(home, work_id)? {
            state.observed_terminal = Some(observed_at);
            save_state_unlocked(home, &state)?;
        }
        return Err(AppError::new(
            "WORK_TERMINAL_OBSERVED",
            ExitCode::Precondition,
            format!("work item {work_id} was locally observed in a terminal state"),
        )
        .remedy("do not approve this id; ask the sender to issue a fresh work item"));
    }
    let require_signed_rows = crate::config::load(home)?.0.mesh.gateway.require_signed_rows;
    let _lock = lock_state(home, work_id)?;
    let loaded = load_state_unlocked(home, work_id)?;
    let preflight = if loaded.is_none() {
        Some(preflight(home, git, spawner, work_id, require_signed_rows).await?)
    } else {
        warn_corroboration(corroborate_sender(
            home,
            &verified_for(home, work_id)?,
            CorroborationStage::Approve,
            require_signed_rows,
        )?);
        None
    };
    let mut state = match loaded {
        Some(state) => state,
        None => {
            let state = initial_claiming_state_from_verified(home, work_id, &preflight.unwrap().verified)?;
            save_state_unlocked(home, &state)?;
            if stop_after == Some(ApproveStopAfter::StateWritten) {
                return Ok(ApproveOutcome::Interrupted {
                    work_id: work_id.to_string(),
                    phase: WorkPhase::Claiming,
                });
            }
            state
        }
    };

    if state.phase == WorkPhase::Claiming {
        let fleet_id = keys::fleet_id(home)?;
        let claimer = keys::gateway_id(home)?;
        match registry.work_claim(&fleet_id, work_id, &claimer).await? {
            ClaimOutcome::Won => {}
            ClaimOutcome::Lost(row) if is_my_existing_claim(&row, &claimer) => {}
            ClaimOutcome::Lost(row) => {
                remove_state_unlocked(home, work_id)?;
                return Ok(lost_outcome(row));
            }
        }
        if stop_after == Some(ApproveStopAfter::Claimed) {
            return Ok(ApproveOutcome::Interrupted {
                work_id: work_id.to_string(),
                phase: WorkPhase::Claiming,
            });
        }
        state.phase = WorkPhase::Cloning;
        save_state_unlocked(home, &state)?;
    }

    if state.phase == WorkPhase::Cloning {
        let stored = verified_for(home, work_id)?;
        git.clone_checkout(
            &stored.envelope.repo.remote_url,
            &stored.envelope.repo.base_ref,
            &state.checkout,
        )
        .await
        .map_err(|err| mark_failed(home, state.clone(), err))?;
        let head = work_gate::rev_parse_head(&state.checkout)?;
        state.base_sha = Some(head);
        state.remote_url = Some(stored.envelope.repo.remote_url.clone());
        state.base_ref = Some(stored.envelope.repo.base_ref.clone());
        if stop_after == Some(ApproveStopAfter::Cloned) {
            state.phase = WorkPhase::Spawning;
            save_state_unlocked(home, &state)?;
            return Ok(ApproveOutcome::Interrupted {
                work_id: work_id.to_string(),
                phase: WorkPhase::Spawning,
            });
        }
        state.phase = WorkPhase::Spawning;
        save_state_unlocked(home, &state)?;
    }

    if state.phase == WorkPhase::Spawning {
        let stored = verified_for(home, work_id)?;
        let brief_path = item_dir(home, work_id).join("brief.md");
        write_brief(&brief_path, &stored.envelope.brief_text)?;
        let worker_id = spawner
            .spawn(home, &state.checkout, &brief_path)
            .map_err(|err| mark_failed(home, state.clone(), err))?;
        state.worker_id = Some(worker_id.clone());
        state.phase = WorkPhase::Running;
        save_state_unlocked(home, &state)?;
        if stop_after == Some(ApproveStopAfter::Spawned) {
            return Ok(ApproveOutcome::Interrupted {
                work_id: work_id.to_string(),
                phase: WorkPhase::Running,
            });
        }
    }

    if state.phase == WorkPhase::Running {
        let fleet_id = keys::fleet_id(home)?;
        let claimer = keys::gateway_id(home)?;
        let outcome = registry
            .work_transition(
                &fleet_id,
                work_id,
                WorkStatus::Approved,
                WorkStatus::Running,
                WorkOwner::Claimer(claimer.clone()),
                WorkPatch::default(),
            )
            .await?;
        if let TransitionOutcome::Lost(row) = outcome {
            if !(row.status == WorkStatus::Running && row.claimed_by.as_deref() == Some(&claimer)) {
                return Ok(lost_outcome(row));
            }
        }
        let worker_id = state.worker_id.clone().ok_or_else(|| {
            AppError::new(
                "WORK_STATE_INVALID",
                ExitCode::Internal,
                "running work state has no worker_id",
            )
        })?;
        return Ok(ApproveOutcome::Running {
            work_id: work_id.to_string(),
            worker_id,
            checkout: state.checkout,
        });
    }

    Err(AppError::new(
        "WORK_STATE_INVALID",
        ExitCode::Precondition,
        format!("approve cannot resume from phase {:?}", state.phase),
    ))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkCompletion {
    Running,
    Done {
        summary: String,
        evidence_excerpt: String,
    },
    Failed {
        summary: String,
        evidence_excerpt: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionStopAfter {
    Pushed,
    Persisted,
    Transitioned,
    Spooled,
    MilestonePosted,
}

#[derive(Clone, Debug)]
pub struct WorkLoopOptions {
    pub force_push: bool,
    pub stop_after: Option<CompletionStopAfter>,
    pub retry_cap: Duration,
    pub claim_stall: Duration,
    pub now: Option<DateTime<Utc>>,
}

impl WorkLoopOptions {
    pub fn for_config(cfg: &Config) -> Self {
        Self {
            force_push: false,
            stop_after: None,
            retry_cap: Duration::from_secs(cfg.mesh.gateway.outbound_retry_max_secs.max(1)),
            claim_stall: Duration::from_secs(cfg.mesh.work.claim_stall_secs.max(1)),
            now: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkLoopTick {
    pub running: usize,
    pub completed: usize,
    pub push_failures: usize,
    pub skipped_backoff: usize,
    pub item_errors: usize,
    pub pool_materialized: usize,
    pub expired_swept: usize,
    pub abandoned_swept: usize,
    pub release_stalled_swept: usize,
    pub registry_error: Option<String>,
    pub restarts_requested: usize,
}

pub trait WorkCompletionIo: Clone + Send + Sync + 'static {
    fn reconcile_worker(
        &self,
        home: &A2aHome,
        cfg: &Config,
        worker_id: &str,
    ) -> Result<WorkCompletion, AppError>;
    fn push_result_branch(
        &self,
        checkout: &Path,
        work_id: &str,
        remote_url: &str,
        held: &work_gate::ResultRange,
    ) -> Result<(), AppError>;
}

#[derive(Clone, Copy)]
pub struct RealWorkCompletionIo;

impl WorkCompletionIo for RealWorkCompletionIo {
    fn reconcile_worker(
        &self,
        home: &A2aHome,
        cfg: &Config,
        worker_id: &str,
    ) -> Result<WorkCompletion, AppError> {
        let snap = reconcile::reconcile(home, cfg, worker_id, Utc::now())?;
        let result = worker_result_excerpt(home, worker_id);
        match snap.state {
            WorkerState::Done => Ok(WorkCompletion::Done {
                summary: result.unwrap_or_else(|| "done".to_string()),
                evidence_excerpt: String::new(),
            }),
            WorkerState::Failed | WorkerState::Cancelled => Ok(WorkCompletion::Failed {
                summary: result.unwrap_or_else(|| snap.state.as_str().to_string()),
                evidence_excerpt: String::new(),
            }),
            _ => Ok(WorkCompletion::Running),
        }
    }

    fn push_result_branch(
        &self,
        checkout: &Path,
        work_id: &str,
        remote_url: &str,
        held: &work_gate::ResultRange,
    ) -> Result<(), AppError> {
        work_gate::assert_head_unmoved(checkout, held)?;
        let refspec = format!("{}:refs/heads/a2a/work/{work_id}", held.head_sha);
        let output = work_gate::git_push_pinned(checkout)
            .args(["push", "--no-verify", "--atomic", "--"])
            .arg(remote_url)
            .arg(&refspec)
            .output()
            .map_err(|e| work_push_failed(format!("running git push: {e}")))?;
        if output.status.success() {
            return Ok(());
        }
        Err(work_push_failed(stderr_or_status(&output)))
    }
}

pub async fn advance_completion<R, I>(
    home: &A2aHome,
    cfg: &Config,
    registry: &R,
    io: &I,
    work_state: WorkState,
) -> Result<(), AppError>
where
    R: Registry + ?Sized,
    I: WorkCompletionIo,
{
    advance_completion_until(
        home,
        cfg,
        registry,
        io,
        work_state,
        WorkLoopOptions::for_config(cfg),
    )
    .await?;
    Ok(())
}

pub async fn advance_completion_until<R, I>(
    home: &A2aHome,
    cfg: &Config,
    registry: &R,
    io: &I,
    mut state: WorkState,
    options: WorkLoopOptions,
) -> Result<WorkState, AppError>
where
    R: Registry + ?Sized,
    I: WorkCompletionIo,
{
    let _lock = lock_state(home, &state.work_id)?;
    let now = options.now.unwrap_or_else(Utc::now);
    let fleet_id = keys::fleet_id(home)?;
    let claimer = keys::gateway_id(home)?;

    if state.phase == WorkPhase::Running {
        let worker_id = state.worker_id.clone().ok_or_else(|| {
            AppError::new(
                "WORK_STATE_INVALID",
                ExitCode::Internal,
                "running work state has no worker_id",
            )
        })?;
        let home_for_block = home.clone();
        let cfg_for_block = cfg.clone();
        let io_for_block = io.clone();
        let checkout_for_block = state.checkout.clone();
        let base_sha_for_block = state.base_sha.clone();
        let work_id_for_block = state.work_id.clone();
        let (completion, frozen) = tokio::task::spawn_blocking(move || {
            let completion =
                io_for_block.reconcile_worker(&home_for_block, &cfg_for_block, &worker_id)?;
            // Range freezing invokes git, so it stays in the existing blocking region with
            // reconciliation instead of becoming an uncancellable subprocess on the async path.
            let frozen = if matches!(
                &completion,
                WorkCompletion::Done { .. } | WorkCompletion::Failed { .. }
            ) {
                let base_sha = base_sha_for_block.as_deref().ok_or_else(|| {
                    missing_work_state_field(&work_id_for_block, "base_sha")
                        .remedy("approve records base_sha; this item predates the gate")
                })?;
                Some(work_gate::freeze_range(&checkout_for_block, base_sha)?)
            } else {
                None
            };
            Ok::<_, AppError>((completion, frozen))
        })
        .await
        .map_err(|e| {
            AppError::new("WORK_RECONCILE_FAILED", ExitCode::Internal, e.to_string())
                .remedy("inspect the worker with `a2a status <worker_id>` and retry the gateway tick")
        })??;
        registry
            .work_touch_running(&fleet_id, &state.work_id, &claimer)
            .await?;
        match completion {
            WorkCompletion::Running => return Ok(state),
            WorkCompletion::Done {
                summary,
                evidence_excerpt,
            } => {
                let held = frozen.as_ref().ok_or_else(|| {
                    missing_work_state_field(&state.work_id, "held result range")
                })?;
                state.result_status = Some(WorkStatus::Done);
                state.result_summary = Some(redact::redact_egress(&summary));
                state.evidence_excerpt = Some(redact::redact_egress(&evidence_excerpt));
                state.held_commit_sha = Some(held.head_sha.clone());
                state.closure_digest = Some(held.closure_digest.clone());
                if cfg.mesh.work.auto_push {
                    state.phase = WorkPhase::Pushing;
                    save_state_unlocked(home, &state)?;
                } else {
                    let (report, scan_error) = scan_result_for_hold(
                        home,
                        cfg,
                        &state.checkout,
                        held,
                    )
                    .await;
                    if let Some(err) = scan_error {
                        state.result_summary = Some(redact::redact_egress(&format!(
                            "{}: {}",
                            err.code, err.message
                        )));
                    }
                    enter_hold(home, cfg, registry, &mut state, &report).await?;
                    return Ok(state);
                }
            }
            WorkCompletion::Failed {
                summary,
                evidence_excerpt,
            } => {
                let held = frozen.as_ref().ok_or_else(|| {
                    missing_work_state_field(&state.work_id, "held result range")
                })?;
                state.result_status = Some(WorkStatus::Failed);
                state.result_summary = Some(redact::redact_egress(&summary));
                state.evidence_excerpt = Some(redact::redact_egress(&evidence_excerpt));
                state.held_commit_sha = Some(held.head_sha.clone());
                state.closure_digest = Some(held.closure_digest.clone());
                if cfg.mesh.work.auto_push {
                    state.phase = WorkPhase::Pushing;
                    save_state_unlocked(home, &state)?;
                } else {
                    let (report, scan_error) = scan_result_for_hold(
                        home,
                        cfg,
                        &state.checkout,
                        held,
                    )
                    .await;
                    if let Some(err) = scan_error {
                        state.result_summary = Some(redact::redact_egress(&format!(
                            "{}: {}",
                            err.code, err.message
                        )));
                    }
                    enter_hold(home, cfg, registry, &mut state, &report).await?;
                    return Ok(state);
                }
            }
        }
    }

    if state.phase == WorkPhase::AwaitingRelease {
        let report = work_gate::ScanReport {
            hits: Vec::new(),
            scanned_bytes: 0,
        };
        enter_hold(home, cfg, registry, &mut state, &report).await?;
        return Ok(state);
    }

    if state.phase == WorkPhase::Pushing {
        if !options.force_push {
            if let Some(next) = state.next_push_after {
                if next > now {
                    return Ok(state);
                }
            }
        }
        let checkout = state.checkout.clone();
        let work_id = state.work_id.clone();
        let remote_url = state.remote_url.clone().ok_or_else(|| {
            missing_work_state_field(&state.work_id, "remote_url")
                .remedy("approve records remote_url; this item predates the pinned push gate")
        })?;
        let held = held_range_from_state(&state)?;
        if let Err(err) = work_gate::gate_push(home, cfg, &state.checkout, &held).await {
            state.result_summary = Some(redact::redact_egress(&format!(
                "{}: {}",
                err.code, err.message
            )));
            let (report, _) = scan_result_for_hold(home, cfg, &state.checkout, &held).await;
            enter_hold(home, cfg, registry, &mut state, &report).await?;
            if options.force_push {
                return Err(err);
            }
            return Ok(state);
        }
        let io_for_block = io.clone();
        let pushed = tokio::task::spawn_blocking(move || {
            io_for_block.push_result_branch(&checkout, &work_id, &remote_url, &held)
        })
        .await
        .map_err(|e| AppError::new("WORK_PUSH_FAILED", ExitCode::Internal, e.to_string()))?;
        if let Err(err) = pushed {
            state.push_attempts = state.push_attempts.saturating_add(1);
            state.next_push_after = Some(now + jittered_push_backoff(state.push_attempts, options.retry_cap));
            state.result_summary = Some(redact::redact_egress(&format!(
                "{}: {}",
                err.code, err.message
            )));
            save_state_unlocked(home, &state)?;
            return Err(err);
        }
        if options.stop_after == Some(CompletionStopAfter::Pushed) {
            return Ok(state);
        }
        state.phase = WorkPhase::Reporting;
        state.result_ref = Some(format!("a2a/work/{}", state.work_id));
        state.next_push_after = None;
        save_state_unlocked(home, &state)?;
        if options.stop_after == Some(CompletionStopAfter::Persisted) {
            return Ok(state);
        }
    }

    if state.phase == WorkPhase::Reporting {
        let status = state.result_status.unwrap_or(WorkStatus::Done);
        let from = if state.held_since.is_some() {
            WorkStatus::AwaitingRelease
        } else {
            WorkStatus::Running
        };
        let result_ref = state
            .result_ref
            .clone()
            .unwrap_or_else(|| format!("a2a/work/{}", state.work_id));
        let transition = registry
            .work_transition(
                &fleet_id,
                &state.work_id,
                from,
                status,
                WorkOwner::Claimer(claimer.clone()),
                WorkPatch {
                    result_ref: Some(Some(result_ref.clone())),
                    ..WorkPatch::default()
                },
            )
            .await?;
        if let TransitionOutcome::Lost(row) = transition {
            if row.claimed_by.as_deref() != Some(&claimer)
                || row.status != status
                || row.result_ref.as_deref() != Some(result_ref.as_str())
            {
                return Err(AppError::new(
                    "WORK_STATE_CONFLICT",
                    ExitCode::Precondition,
                    format!("work {} is {}", state.work_id, status_name(row.status)),
                ));
            }
        }
        if options.stop_after == Some(CompletionStopAfter::Transitioned) {
            return Ok(state);
        }

        spool_result_note_once(home, cfg, registry, &mut state).await?;
        if options.stop_after == Some(CompletionStopAfter::Spooled) {
            return Ok(state);
        }

        post_milestone_once(home, cfg, &state)?;
        if options.stop_after == Some(CompletionStopAfter::MilestonePosted) {
            return Ok(state);
        }
        state.milestone_posted = true;
        state.phase = WorkPhase::Reported;
        save_state_unlocked(home, &state)?;
    }

    Ok(state)
}

pub async fn enter_hold<R>(
    home: &A2aHome,
    cfg: &Config,
    registry: &R,
    state: &mut WorkState,
    report: &work_gate::ScanReport,
) -> Result<(), AppError>
where
    R: Registry + ?Sized,
{
    let held_since = state.held_since.unwrap_or_else(|| truncate_to_micros(Utc::now()));
    state.held_since = Some(held_since);
    if state.hold_note_id.is_none() {
        state.hold_note_id = Some(keys::mint_ulid(&SystemClock));
    }
    if state.hold_hit_count.is_none() {
        state.hold_hit_count = Some(report.hits.len());
    }
    state.phase = WorkPhase::AwaitingRelease;

    // Persist the local authorization record first. A cancelled async turn can safely resume the
    // CAS and announcement from this exact state without losing or duplicating the hold.
    save_state_unlocked(home, state)?;

    let fleet_id = keys::fleet_id(home)?;
    let claimer = keys::gateway_id(home)?;
    let transition = registry
        .work_transition(
            &fleet_id,
            &state.work_id,
            WorkStatus::Running,
            WorkStatus::AwaitingRelease,
            WorkOwner::Claimer(claimer.clone()),
            WorkPatch::default(),
        )
        .await?;
    let announce = match transition {
        TransitionOutcome::Applied => true,
        TransitionOutcome::Lost(row)
            if row.status == WorkStatus::AwaitingRelease
                && row.claimed_by.as_deref() == Some(claimer.as_str()) =>
        {
            true
        }
        TransitionOutcome::Lost(row)
            if row.status == WorkStatus::Failed
                && row.result_delivery.as_deref() == Some("release_stalled") =>
        {
            false
        }
        TransitionOutcome::Lost(row) if is_observed_terminal(row.status) => {
            let observed_at =
                work_store::mark_terminal_observed(home, &state.work_id, row.status)?;
            state.observed_terminal = Some(observed_at);
            save_state_unlocked(home, state)?;
            false
        }
        TransitionOutcome::Lost(row) => {
            return Err(AppError::new(
                "WORK_STATE_CONFLICT",
                ExitCode::Precondition,
                format!(
                    "work {} cannot enter awaiting_release from {}",
                    state.work_id,
                    status_name(row.status)
                ),
            )
            .remedy("inspect `a2a mesh work status <id>` before retrying the work loop"));
        }
    };
    if announce {
        spool_hold_note_once(home, cfg, state)?;
    }
    Ok(())
}

pub fn release_from_hold(home: &A2aHome, mut state: WorkState) -> Result<WorkState, AppError> {
    if state.phase != WorkPhase::AwaitingRelease || state.held_since.is_none() {
        return Err(AppError::new(
            "WORK_NOT_HELD",
            ExitCode::Precondition,
            format!("work item {} is not awaiting release", state.work_id),
        )
        .remedy("run `a2a mesh work status <id>` and release or reject only a held item"));
    }
    state.phase = WorkPhase::Pushing;
    save_state(home, &state)?;
    Ok(state)
}

async fn scan_result_for_hold(
    home: &A2aHome,
    cfg: &Config,
    checkout: &Path,
    range: &work_gate::ResultRange,
) -> (work_gate::ScanReport, Option<AppError>) {
    let home = home.clone();
    let cfg = cfg.clone();
    let checkout = checkout.to_path_buf();
    let range = range.clone();
    match tokio::task::spawn_blocking(move || {
        work_gate::scan_added_blobs(&home, &cfg, &checkout, &range)
    })
    .await
    {
        Ok(Ok(report)) => (report, None),
        Ok(Err(err)) => (
            work_gate::ScanReport {
                hits: Vec::new(),
                scanned_bytes: 0,
            },
            Some(err),
        ),
        Err(err) => (
            work_gate::ScanReport {
                hits: Vec::new(),
                scanned_bytes: 0,
            },
            Some(
                AppError::new(
                    "INTERNAL",
                    ExitCode::Internal,
                    format!("work result scan task failed: {err}"),
                )
                .remedy("retry the gateway work loop; inspect the checkout if this repeats"),
            ),
        ),
    }
}

fn spool_hold_note_once(
    home: &A2aHome,
    cfg: &Config,
    state: &WorkState,
) -> Result<PathBuf, AppError> {
    let note_id = state.hold_note_id.as_deref().ok_or_else(|| {
        missing_work_state_field(&state.work_id, "hold_note_id")
    })?;
    let stored = stored_for_exact_id(home, &state.work_id)?;
    let sender_gateway = stored.verified.envelope.sender.gateway_id;
    if let Some(path) = existing_spooled_note(home, &sender_gateway, note_id)? {
        return Ok(path);
    }
    let held_since = state.held_since.ok_or_else(|| {
        missing_work_state_field(&state.work_id, "held_since")
    })?;
    let refs = json!({
        "held": {
            "work_id": state.work_id,
            "status": "held",
            "hit_count": state.hold_hit_count.unwrap_or(0),
            "held_since": held_since.to_rfc3339_opts(SecondsFormat::Micros, true),
        }
    });
    let note = Note {
        note_id: Some(note_id.to_string()),
        from: local_peer().to_string(),
        to: "user:mesh-work".to_string(),
        ts: held_since,
        kind: work_store::WORK_HELD_KIND.to_string(),
        text: format!(
            "work held: {}; review with `a2a mesh work diff {}` then `release` or `reject`",
            state.work_id, state.work_id
        ),
        refs,
        extra: serde_json::Map::new(),
    };
    let from_gateway = keys::gateway_id(home)?;
    notes_wire::spool_note(home, cfg, &from_gateway, &sender_gateway, note)
}

pub async fn work_loop_tick<R, I>(
    home: &A2aHome,
    cfg: &Config,
    registry: &R,
    io: &I,
    options: WorkLoopOptions,
) -> WorkLoopTick
where
    R: Registry + ?Sized,
    I: WorkCompletionIo,
{
    let mut report = WorkLoopTick::default();
    let fleet_id = match keys::fleet_id(home) {
        Ok(id) => id,
        Err(err) => {
            report.item_errors += 1;
            report.registry_error = Some(format!("{}: {}", err.code, err.message));
            return report;
        }
    };
    let gateway_id = match keys::gateway_id(home) {
        Ok(id) => id,
        Err(err) => {
            report.item_errors += 1;
            report.registry_error = Some(format!("{}: {}", err.code, err.message));
            return report;
        }
    };

    match materialize_pool_offers(home, cfg) {
        Ok(n) => report.pool_materialized = n,
        Err(_) => report.item_errors += 1,
    }

    let states = match load_states(home) {
        Ok(states) => states,
        Err(_) => {
            report.item_errors += 1;
            Vec::new()
        }
    };
    for mut state in states {
        match registry.work_get(&fleet_id, &state.work_id).await {
            Ok(Some(row)) if is_observed_terminal(row.status) => {
                match work_store::mark_terminal_observed(home, &state.work_id, row.status) {
                    Ok(observed_at) => {
                        state.observed_terminal = Some(observed_at);
                        if save_state(home, &state).is_err() {
                            report.item_errors += 1;
                        }
                    }
                    Err(_) => report.item_errors += 1,
                }
                continue;
            }
            Ok(_) => {}
            Err(err) => {
                report.registry_error = Some(format!("{}: {}", err.code, err.message));
                continue;
            }
        }
        if !matches!(
            state.phase,
            WorkPhase::Running
                | WorkPhase::AwaitingRelease
                | WorkPhase::Pushing
                | WorkPhase::Reporting
        ) {
            continue;
        }
        if state
            .next_push_after
            .is_some_and(|next| !options.force_push && next > options.now.unwrap_or_else(Utc::now))
        {
            report.skipped_backoff += 1;
            continue;
        }
        match advance_completion_until(home, cfg, registry, io, state, options.clone()).await {
            Ok(next) => {
                if next.phase == WorkPhase::Reported {
                    report.completed += 1;
                } else {
                    report.running += 1;
                }
            }
            Err(err) => {
                if err.code == "WORK_PUSH_FAILED" {
                    report.push_failures += 1;
                } else if err.code.starts_with("REGISTRY") {
                    report.registry_error = Some(format!("{}: {}", err.code, err.message));
                } else {
                    report.item_errors += 1;
                }
            }
        }
    }

    match registry.work_sweep_expired(&fleet_id, &gateway_id).await {
        Ok(ids) => report.expired_swept = ids.len(),
        Err(err) => report.registry_error = Some(format!("{}: {}", err.code, err.message)),
    }
    match registry
        .work_sweep_abandoned(&fleet_id, &gateway_id, options.claim_stall)
        .await
    {
        Ok(rows) => report.abandoned_swept = rows.len(),
        Err(err) => report.registry_error = Some(format!("{}: {}", err.code, err.message)),
    }
    let release_horizon = Duration::from_secs(
        cfg.mesh
            .work
            .release_stall_days
            .saturating_mul(86_400),
    );
    match registry
        .work_sweep_release_stalled(&fleet_id, &gateway_id, release_horizon)
        .await
    {
        Ok(rows) => report.release_stalled_swept = rows.len(),
        Err(err) => report.registry_error = Some(format!("{}: {}", err.code, err.message)),
    }
    report
}

async fn spool_result_note_once<R>(
    home: &A2aHome,
    cfg: &Config,
    registry: &R,
    state: &mut WorkState,
) -> Result<PathBuf, AppError>
where
    R: Registry + ?Sized,
{
    let fleet_id = keys::fleet_id(home)?;
    let row = registry
        .work_get(&fleet_id, &state.work_id)
        .await?
        .ok_or_else(|| {
            AppError::new(
                "WORK_NOT_FOUND",
                ExitCode::Precondition,
                format!("work item {} was not found", state.work_id),
            )
        })?;
    if is_observed_terminal(row.status) {
        let observed_at = work_store::mark_terminal_observed(home, &state.work_id, row.status)?;
        state.observed_terminal = Some(observed_at);
        save_state_unlocked(home, state)?;
    }
    let from_gateway = keys::gateway_id(home)?;
    let stored = stored_for_exact_id(home, &state.work_id)?;
    let sender_gateway = stored.verified.envelope.sender.gateway_id;
    if let Some(path) = existing_spooled_note(home, &sender_gateway, &state.result_note_id)? {
        return Ok(path);
    }
    let status = state.result_status.unwrap_or(WorkStatus::Done);
    let status_text = status_name(status);
    let result_ref = state
        .result_ref
        .clone()
        .unwrap_or_else(|| format!("a2a/work/{}", state.work_id));
    let summary = capped_result_text(
        state.result_summary.as_deref().unwrap_or(status_text),
        cfg.mesh.work.result_note_max_bytes,
    );
    let evidence_excerpt = capped_result_text(
        state.evidence_excerpt.as_deref().unwrap_or_default(),
        cfg.mesh.work.result_note_max_bytes,
    );
    let title = title_for(home, &state.work_id)?;
    let refs = json!({
        "result": {
            "work_id": state.work_id,
            "status": status_text,
            "result_ref": result_ref,
            "summary": summary,
            "evidence_excerpt": evidence_excerpt,
        }
    });
    feed::validate_refs_size(&refs, cfg.mesh.work.result_note_max_bytes)?;
    let note = Note {
        note_id: Some(state.result_note_id.clone()),
        from: local_peer().to_string(),
        to: "user:mesh-work".to_string(),
        ts: Utc::now(),
        kind: work_store::WORK_RESULT_KIND.to_string(),
        text: format!(
            "work {status_text}: {} ({}) -> {result_ref}",
            clean(&title),
            state.work_id,
        ),
        refs,
        extra: serde_json::Map::new(),
    };
    notes_wire::spool_note(home, cfg, &from_gateway, &sender_gateway, note)
}

fn existing_spooled_note(
    home: &A2aHome,
    to_gateway: &str,
    note_id: &str,
) -> Result<Option<PathBuf>, AppError> {
    let outbound = home.root().join("mesh").join("outbound");
    for dir in [
        outbound.join(to_gateway),
        outbound.join("sent").join(to_gateway),
        outbound.join("rejected").join(to_gateway),
    ] {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io_err("reading result note spool", e)),
        };
        for entry in entries {
            let entry = entry.map_err(|e| io_err("reading result note spool", e))?;
            if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let spooled = notes_wire::read_spooled(&entry.path())?;
            if spooled.trace_id == note_id {
                return Ok(Some(entry.path()));
            }
        }
    }
    Ok(None)
}

fn post_milestone_once(home: &A2aHome, cfg: &Config, state: &WorkState) -> Result<u64, AppError> {
    if state.milestone_posted {
        return Ok(0);
    }
    if feed::read(home, 0)?
        .into_iter()
        .any(|entry| entry.refs["work_id"] == state.work_id)
    {
        return Ok(0);
    }
    let status = status_name(state.result_status.unwrap_or(WorkStatus::Done));
    feed::post(
        home,
        cfg,
        &local_peer(),
        &format!("work {status}: {} ({})", title_for(home, &state.work_id)?, state.work_id),
        json!({ "work_id": state.work_id, "status": status }),
    )
}

fn materialize_pool_offers(home: &A2aHome, cfg: &Config) -> Result<usize, AppError> {
    let mut count = 0;
    for entry in feed::read(home, 0)? {
        let Some(value) = entry.refs.get("work") else {
            continue;
        };
        let Ok(envelope) = serde_json::from_value(value.clone()) else {
            continue;
        };
        let Ok(verified) = verify_envelope(home, &envelope, &SystemClock) else {
            continue;
        };
        if work_store::validate_work_id(&verified.envelope.work_id).is_err() {
            continue;
        }
        if work_store::load(home, &verified.envelope.work_id)?.is_some() {
            continue;
        }
        work_store::store_offer(home, verified, work_store::WorkSource::Pool)?;
        count += 1;
    }
    let _ = cfg;
    Ok(count)
}

fn title_for(home: &A2aHome, work_id: &str) -> Result<String, AppError> {
    Ok(work_store::load(home, work_id)?
        .map(|stored| clean(&stored.verified.envelope.title))
        .unwrap_or_else(|| work_id.to_string()))
}

fn capped_result_text(text: &str, cap: u64) -> String {
    let redacted = redact::redact_egress(text);
    if cap == 0 || redacted.len() <= cap as usize {
        return redacted;
    }
    let limit = cap.saturating_sub(3) as usize;
    let mut out = String::new();
    for c in redacted.chars() {
        if out.len() + c.len_utf8() > limit {
            break;
        }
        out.push(c);
    }
    out.push_str("...");
    out
}

fn clean(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .trim()
        .to_string()
}

fn local_peer() -> PeerId {
    PeerId {
        provider: Provider::User,
        uuid: "mesh-work".to_string(),
    }
}

fn jittered_push_backoff(attempts: u32, cap: Duration) -> chrono::Duration {
    let factor = 2u64.saturating_pow(attempts.min(16));
    let cap_ms = cap.as_millis().min(u128::from(i64::MAX as u64)) as u64;
    let upper = 1000u64.saturating_mul(factor).min(cap_ms).max(1);
    let millis = rand::thread_rng().gen_range(0..=upper);
    chrono::Duration::milliseconds(millis as i64)
}

fn work_push_failed(detail: impl Into<String>) -> AppError {
    AppError::new(
        "WORK_PUSH_FAILED",
        ExitCode::Precondition,
        format!("git push failed: {}", detail.into()),
    )
    .remedy("fix git credentials or the remote, then run `a2a mesh work retry-push <id>`")
}

fn held_range_from_state(state: &WorkState) -> Result<work_gate::ResultRange, AppError> {
    Ok(work_gate::ResultRange {
        base_sha: state
            .base_sha
            .clone()
            .ok_or_else(|| missing_work_state_field(&state.work_id, "base_sha"))?,
        head_sha: state
            .held_commit_sha
            .clone()
            .ok_or_else(|| missing_work_state_field(&state.work_id, "held_commit_sha"))?,
        closure_digest: state
            .closure_digest
            .clone()
            .ok_or_else(|| missing_work_state_field(&state.work_id, "closure_digest"))?,
    })
}

fn stored_for_exact_id(home: &A2aHome, work_id: &str) -> Result<work_store::StoredWork, AppError> {
    let stored = work_store::load(home, work_id)?.ok_or_else(|| {
        AppError::new(
            "WORK_NOT_FOUND",
            ExitCode::Precondition,
            format!("work {work_id} is not in the local work store"),
        )
    })?;
    if stored.verified.envelope.work_id != work_id {
        return Err(AppError::new(
            "WORK_STATE_CONFLICT",
            ExitCode::Precondition,
            format!("stored work id does not match requested id {work_id}"),
        )
        .remedy("remove the mismatched local work entry and ask the sender to re-send it"));
    }
    Ok(stored)
}

fn is_observed_terminal(status: WorkStatus) -> bool {
    matches!(
        status,
        WorkStatus::Cancelled
            | WorkStatus::Expired
            | WorkStatus::Declined
            | WorkStatus::Rejected
            | WorkStatus::Acked
    )
}

fn truncate_to_micros(value: DateTime<Utc>) -> DateTime<Utc> {
    value
        .with_nanosecond(value.nanosecond() / 1_000 * 1_000)
        .unwrap_or(value)
}

fn missing_work_state_field(work_id: &str, field: &str) -> AppError {
    AppError::new(
        "WORK_STATE_INVALID",
        ExitCode::Precondition,
        format!("work item {work_id} has no recorded {field}"),
    )
    .remedy("re-approve a fresh work item so the reviewed result gate can record every field")
}

fn worker_result_excerpt(home: &A2aHome, worker_id: &str) -> Option<String> {
    let dir = home.worker_dir(worker_id);
    for name in [
        reconcile::RESULT_JSON,
        reconcile::RESULT_MD,
        reconcile::RESULT_RAW,
    ] {
        let path = dir.join(name);
        if let Ok(text) = fs::read_to_string(path) {
            return Some(redact::redact_egress(text.trim()));
        }
    }
    None
}

pub fn initial_claiming_state(home: &A2aHome, work_id: &str) -> Result<WorkState, AppError> {
    let verified = verified_for(home, work_id)?;
    initial_claiming_state_from_verified(home, work_id, &verified)
}

fn initial_claiming_state_from_verified(
    home: &A2aHome,
    work_id: &str,
    verified: &VerifiedEnvelope,
) -> Result<WorkState, AppError> {
    Ok(WorkState {
        phase: WorkPhase::Claiming,
        work_id: work_id.to_string(),
        envelope_digest: envelope_digest(verified)?,
        result_note_id: keys::mint_ulid(&SystemClock),
        milestone_posted: false,
        worker_id: None,
        result_ref: None,
        checkout: item_dir(home, work_id).join("repo"),
        push_attempts: 0,
        next_push_after: None,
        result_summary: None,
        evidence_excerpt: None,
        result_status: None,
        base_sha: None,
        held_commit_sha: None,
        closure_digest: None,
        base_ref: None,
        remote_url: None,
        held_since: None,
        hold_note_id: None,
        hold_hit_count: None,
        observed_terminal: None,
    })
}

pub fn load_states(home: &A2aHome) -> Result<Vec<WorkState>, AppError> {
    Ok(load_state_files(home)?
        .into_iter()
        .map(|entry| entry.state)
        .collect())
}

pub fn load_state_files(home: &A2aHome) -> Result<Vec<LocatedWorkState>, AppError> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(home.work_dir()) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(io_err("reading work states", e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| io_err("reading work states", e))?;
        let dir = entry.path();
        let path = dir.join("state.json");
        if path.exists() {
            let modified_at = fs::metadata(&path)
                .and_then(|m| m.modified())
                .ok()
                .map(DateTime::<Utc>::from);
            let state = match read_state_file(&path) {
                Ok(state) => state,
                Err(err) => {
                    let work_id = dir
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_else(|| dir.display().to_string());
                    eprintln!(
                        "a2a: warning: work {work_id}: {}: {} — state skipped",
                        err.code, err.message
                    );
                    continue;
                }
            };
            out.push(LocatedWorkState {
                state,
                dir,
                state_path: path,
                modified_at,
            });
        }
    }
    out.sort_by(|a, b| a.state.work_id.cmp(&b.state.work_id));
    Ok(out)
}

pub fn load_state(home: &A2aHome, work_id: &str) -> Result<Option<WorkState>, AppError> {
    let _lock = lock_state(home, work_id)?;
    load_state_unlocked(home, work_id)
}

pub fn save_state(home: &A2aHome, state: &WorkState) -> Result<(), AppError> {
    let _lock = lock_state(home, &state.work_id)?;
    save_state_unlocked(home, state)
}

pub fn state_path(home: &A2aHome, work_id: &str) -> PathBuf {
    item_dir(home, work_id).join("state.json")
}

pub fn ack_marker_path(home: &A2aHome, work_id: &str) -> PathBuf {
    home.work_dir()
        .join("acked")
        .join(format!("{work_id}.json"))
}

pub fn acked_locally(home: &A2aHome, work_id: &str) -> bool {
    ack_marker_path(home, work_id).exists()
}

async fn preflight<G, S>(
    home: &A2aHome,
    git: &G,
    spawner: &S,
    work_id: &str,
    require_signed_rows: bool,
) -> Result<Preflight, AppError>
where
    G: GitAdapter + ?Sized,
    S: WorkSpawner + ?Sized,
{
    work_store::validate_work_id(work_id)?;
    let verified = verified_for(home, work_id)?;
    warn_corroboration(corroborate_sender(
        home,
        &verified,
        CorroborationStage::Approve,
        require_signed_rows,
    )?);
    let checkout = item_dir(home, work_id).join("repo");
    git.ls_remote(&verified.envelope.repo.remote_url, &verified.envelope.repo.base_ref)
        .await
        .map_err(|err| {
            if err.code == "WORK_REPO_UNREACHABLE" {
                err
            } else {
                repo_unreachable(&verified.envelope.repo.remote_url, err.message)
            }
        })?;
    spawner
        .preflight(home, &checkout)
        .map_err(map_spawner_preflight_error)?;
    Ok(Preflight { verified })
}

struct Preflight {
    verified: VerifiedEnvelope,
}

fn warn_corroboration(warning: Option<String>) {
    if let Some(warning) = warning {
        eprintln!("a2a: warning: {warning}");
    }
}

fn verified_for(home: &A2aHome, work_id: &str) -> Result<VerifiedEnvelope, AppError> {
    let stored = stored_for_exact_id(home, work_id)?;
    verify_envelope(home, &stored.verified.envelope, &SystemClock)
}

fn envelope_digest(verified: &VerifiedEnvelope) -> Result<String, AppError> {
    let bytes = canonical_envelope_without_sig(&verified.envelope)?;
    Ok(hex(&Sha256::digest(bytes)))
}

fn save_state_unlocked(home: &A2aHome, state: &WorkState) -> Result<(), AppError> {
    let path = state_path(home, &state.work_id);
    if let Some(parent) = path.parent() {
        create_dir_0700(parent).map_err(|e| io_err("creating work item dir", e))?;
    }
    let mut bytes = serde_json::to_vec_pretty(state)
        .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))?;
    bytes.push(b'\n');
    crate::store::write_atomic(&path, &bytes).map_err(|e| io_err("writing work state", e))
}

fn load_state_unlocked(home: &A2aHome, work_id: &str) -> Result<Option<WorkState>, AppError> {
    let path = state_path(home, work_id);
    if !path.exists() {
        return Ok(None);
    }
    read_state_file(&path).map(Some)
}

fn read_state_file(path: &Path) -> Result<WorkState, AppError> {
    let bytes = fs::read(path).map_err(|e| io_err("reading work state", e))?;
    serde_json::from_slice(&bytes).map_err(|e| {
        AppError::new(
            "WORK_STATE_INVALID",
            ExitCode::Precondition,
            format!("decoding {}: {e}", path.display()),
        )
    })
}

fn remove_state_unlocked(home: &A2aHome, work_id: &str) -> Result<(), AppError> {
    match fs::remove_file(state_path(home, work_id)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_err("removing work state", e)),
    }
}

fn lock_state(home: &A2aHome, work_id: &str) -> Result<Flock<File>, AppError> {
    let dir = item_dir(home, work_id);
    create_dir_0700(&dir).map_err(|e| io_err("creating work item dir", e))?;
    let path = dir.join("state.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .map_err(|e| io_err("opening work state lock", e))?;
    Flock::lock(file, FlockArg::LockExclusive).map_err(|(_file, errno)| {
        AppError::new(
            "IO",
            ExitCode::Internal,
            format!("locking work state: {errno}"),
        )
    })
}

fn write_brief(path: &Path, text: &str) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        create_dir_0700(parent).map_err(|e| io_err("creating work brief parent", e))?;
    }
    let mut file = write_new_0600(path).map_err(|e| io_err("writing work brief", e))?;
    file.write_all(text.as_bytes())
        .map_err(|e| io_err("writing work brief", e))?;
    file.sync_all()
        .map_err(|e| io_err("syncing work brief", e))
}

fn mark_failed(home: &A2aHome, mut state: WorkState, err: AppError) -> AppError {
    state.phase = WorkPhase::Failed;
    let _ = save_state_unlocked(home, &state);
    err
}

fn item_dir(home: &A2aHome, work_id: &str) -> PathBuf {
    home.work_dir().join(work_id)
}

fn is_my_existing_claim(row: &WorkRow, claimer: &str) -> bool {
    row.claimed_by.as_deref() == Some(claimer)
        && matches!(row.status, WorkStatus::Approved | WorkStatus::Running)
}

fn lost_outcome(row: WorkRow) -> ApproveOutcome {
    ApproveOutcome::Lost {
        work_id: row.work_id,
        status: status_name(row.status).to_string(),
        claimed_by: row.claimed_by,
    }
}

fn status_name(status: WorkStatus) -> &'static str {
    match status {
        WorkStatus::Offered => "offered",
        WorkStatus::Approved => "approved",
        WorkStatus::Running => "running",
        WorkStatus::AwaitingRelease => "awaiting_release",
        WorkStatus::Done => "done",
        WorkStatus::Failed => "failed",
        WorkStatus::Rejected => "rejected",
        WorkStatus::Acked => "acked",
        WorkStatus::Declined => "declined",
        WorkStatus::Cancelled => "cancelled",
        WorkStatus::Expired => "expired",
    }
}

fn map_spawner_preflight_error(err: AppError) -> AppError {
    if err.code == "CWD_NOT_ALLOWED" {
        return AppError::new(
            "CWD_NOT_ALLOWED",
            ExitCode::Precondition,
            format!(
                "{}; broker restart required for the implicit $A2A_HOME/work root after upgrade",
                err.message
            ),
        )
        .remedy("run `a2a broker restart` so the broker reloads allowed_roots");
    }
    err
}

fn ensure_broker_allows_checkout(checkout: &Path, info: &Value) -> Result<(), AppError> {
    let canon = std::fs::canonicalize(checkout).unwrap_or_else(|_| checkout.to_path_buf());
    let roots = info
        .get("allowed_roots")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let admitted = roots.iter().any(|root| {
        root.as_str()
            .map(PathBuf::from)
            .is_some_and(|root| canon.starts_with(root))
    });
    if admitted {
        return Ok(());
    }
    Err(AppError::new(
        "CWD_NOT_ALLOWED",
        ExitCode::Precondition,
        format!(
            "running broker does not allow mesh work checkout {}; broker restart required",
            canon.display()
        ),
    )
    .remedy("run `a2a broker restart` so the broker reloads allowed_roots"))
}

fn work_daemon_down() -> AppError {
    AppError::new(
        "WORK_DAEMON_DOWN",
        ExitCode::BrokerUnavailable,
        "mesh gateway daemon is not running; completion routing lives in the gateway",
    )
    .remedy("run `a2a mesh gateway run --daemon`")
}

fn repo_unreachable(remote_url: &str, detail: impl Into<String>) -> AppError {
    AppError::new(
        "WORK_REPO_UNREACHABLE",
        ExitCode::Precondition,
        format!("git ls-remote failed for {remote_url}: {}", detail.into()),
    )
    .remedy(format!(
        "check credentials for this remote and confirm {remote_url} is reachable"
    ))
}

fn stderr_or_status(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        format!("git exited with {}", output.status)
    } else {
        stderr
    }
}

fn io_err(context: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
