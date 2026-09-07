use crate::brief;
use crate::cli::{MeshWorkCmd, MeshWorkCreateArgs, MeshWorkOfferArgs, MeshWorkSendArgs};
use crate::commands::{owner, spawn};
use crate::config::{Config, Lane};
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::mesh::{
    feed,
    gateway::notes_wire,
    keys,
    registry::{
        open_registry, read_registry_env, reader_roots, Registry, RegistryHandle,
        TransitionOutcome, WorkListFilter, WorkOwner, WorkPatch, WorkRow, WorkStatus,
        REGISTRY_SCHEMA_VERSION,
    },
    row_mark::RowMarkStore,
    row_sig::{endpoint_conflicts, Disposition, RowVerdict, RowVerifyCfg},
    render::escape_untrusted,
    work::{sign_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkEnvelope, WorkRepo},
    render,
    work_exec::{self, RealGit, RealSpawner},
    work_gate::{self, ResultRange},
    work_store::{self, StoredWork, WORK_CANCELLED_KIND, WORK_OFFER_KIND, WORK_RESULT_KIND},
    Note, PeerId,
};
use crate::output::{AppError, ExitCode, Outcome};
use crate::redact;
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

const MIN_WORK_PEER_VERSION: &str = crate::broker::wire::BINARY_VERSION;
const RESULT_NOTE_ID_PREFIX: &str = "work-result-";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkResultStatus {
    AwaitingRelease,
    Done,
    Failed,
    Declined,
    Rejected,
}

impl WorkResultStatus {
    fn as_str(self) -> &'static str {
        match self {
            WorkResultStatus::AwaitingRelease => "awaiting_release",
            WorkResultStatus::Done => "done",
            WorkResultStatus::Failed => "failed",
            WorkResultStatus::Declined => "declined",
            WorkResultStatus::Rejected => "rejected",
        }
    }
}

pub fn run(
    home: &A2aHome,
    cfg: &Config,
    cmd: &MeshWorkCmd,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    validate_command_work_id(cmd)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| AppError::new("RUNTIME", ExitCode::Internal, format!("starting runtime: {e}")))?;
    rt.block_on(async {
        let out = match cmd {
            MeshWorkCmd::Send(args) => send(home, cfg, args).await?,
            MeshWorkCmd::Offer(args) => offer(home, cfg, args).await?,
            MeshWorkCmd::List => list(home, quiet).await?,
            MeshWorkCmd::Show { work_id } => show(home, work_id).await?,
            MeshWorkCmd::Approve { work_id, dry_run } => {
                approve(home, work_id, *dry_run, json_mode).await?
            }
            MeshWorkCmd::Decline { work_id, reason } => {
                decline(home, cfg, work_id, reason.as_deref()).await?
            }
            MeshWorkCmd::Cancel { work_id } => cancel(home, cfg, work_id).await?,
            MeshWorkCmd::Status { work_id } => status(home, work_id.as_deref()).await?,
            MeshWorkCmd::Ack { work_id } => ack(home, work_id).await?,
            MeshWorkCmd::Diff { work_id, raw, json } => {
                diff(home, cfg, work_id, *raw, json_mode || *json)?
            }
            MeshWorkCmd::RetryPush { work_id } => retry_push(home, cfg, work_id).await?,
            MeshWorkCmd::Release {
                work_id,
                allow_secrets,
                confirm_token,
            } => {
                release(
                    home,
                    cfg,
                    work_id,
                    *allow_secrets,
                    confirm_token.as_deref(),
                )
                .await?
            }
            MeshWorkCmd::Reject {
                work_id,
                reason,
                notify_sender,
            } => reject(home, cfg, work_id, reason, *notify_sender).await?,
            MeshWorkCmd::Reopen { work_id } => reopen(home, work_id).await?,
        };
        Ok(Outcome::from(sanitize_value(out)))
    })
}

fn validate_command_work_id(cmd: &MeshWorkCmd) -> Result<(), AppError> {
    let work_id = match cmd {
        MeshWorkCmd::Send(args) => args.common.work_id.as_deref(),
        MeshWorkCmd::Offer(args) => args.common.work_id.as_deref(),
        MeshWorkCmd::Status { work_id } => work_id.as_deref(),
        MeshWorkCmd::Show { work_id }
        | MeshWorkCmd::Approve { work_id, .. }
        | MeshWorkCmd::Decline { work_id, .. }
        | MeshWorkCmd::Cancel { work_id }
        | MeshWorkCmd::Ack { work_id }
        | MeshWorkCmd::Diff { work_id, .. }
        | MeshWorkCmd::RetryPush { work_id }
        | MeshWorkCmd::Release { work_id, .. }
        | MeshWorkCmd::Reject { work_id, .. }
        | MeshWorkCmd::Reopen { work_id } => Some(work_id.as_str()),
        MeshWorkCmd::List => None,
    };
    if let Some(work_id) = work_id {
        work_store::validate_work_id(work_id)?;
    }
    Ok(())
}

async fn send(home: &A2aHome, cfg: &Config, args: &MeshWorkSendArgs) -> Result<Value, AppError> {
    let registry = registry_with_verify_cfg(home, RowVerifyCfg::from_config(Utc::now(), cfg))?;
    let addressee = addressee_row(&registry, &args.to).await?;
    if let Some((row, verdict)) = &addressee {
        match verdict.disposition(cfg.mesh.gateway.require_signed_rows) {
            Disposition::Refused => {
                return Err(AppError::new(
                    "WORK_PEER_ROW_INVALID",
                    ExitCode::Precondition,
                    format!(
                        "gateway {} has registry verdict {} and cannot receive directed work",
                        args.to,
                        verdict.label()
                    ),
                )
                .remedy("capture the row for evidence first (`a2a mesh fleet ls --json`); do NOT ask the peer to restart or republish")
                .data(json!({
                    "gateway_id": args.to,
                    "verdict": verdict.label(),
                    "version": row.version,
                })));
            }
            Disposition::UsableWarn => {
                eprintln!(
                    "a2a: warning: gateway {} registry row is {}: {}",
                    args.to,
                    verdict.label(),
                    verdict.remedy()
                );
            }
            Disposition::Usable => {}
        }
    }
    preflight_peer_version(
        &args.to,
        addressee
            .as_ref()
            .map(|(row, _verdict)| row.version.as_str()),
    )?;
    let env = build_envelope(
        home,
        &args.common,
        WorkAddressing::Directed(args.to.clone()),
        cfg.mesh.work.ttl_days,
        cfg.mesh.work.max_directed_bytes as usize,
    )?;
    insert_work_row(&registry, &env).await?;
    spool_work_offer(home, cfg, &args.to, &env)?;
    let feed_seq = feed::post(
        home,
        cfg,
        &local_peer(),
        &format!("work sent: {} -> {} ({})", clean(&env.title), args.to, env.work_id),
        json!({ "work_id": env.work_id, "to": args.to }),
    )?;
    Ok(json!({
        "work_id": env.work_id,
        "addressing": env.addressing,
        "to": args.to,
        "registry_status": "offered",
        "feed_seq": feed_seq
    }))
}

async fn offer(home: &A2aHome, cfg: &Config, args: &MeshWorkOfferArgs) -> Result<Value, AppError> {
    let registry = registry(home)?;
    let env = build_envelope(
        home,
        &args.common,
        WorkAddressing::Pool,
        cfg.mesh.work.ttl_days,
        cfg.mesh.work.max_pool_bytes as usize,
    )?;
    let refs = json!({ "work": env });
    feed::validate_refs_size(&refs, cfg.mesh.work.max_pool_bytes)?;
    insert_work_row(&registry, &env).await?;
    let feed_seq = feed::post(
        home,
        cfg,
        &local_peer(),
        &format!("work offered: {} ({})", clean(&env.title), env.work_id),
        refs,
    )?;
    Ok(json!({
        "work_id": env.work_id,
        "addressing": "pool",
        "registry_status": "offered",
        "feed_seq": feed_seq
    }))
}

async fn list(home: &A2aHome, quiet: bool) -> Result<Value, AppError> {
    let local = work_store::list(home)?;
    let local_gateway = keys::gateway_id(home)?;
    let mut rows = local_views(&local);
    let mut registry_available = false;
    let mut warning = None;

    match registry_rows(home, None).await {
        Ok(registry_rows) => {
            registry_available = true;
            overlay_registry_rows(&mut rows, registry_rows, &local_gateway, None);
        }
        Err(err) => {
            let msg = format!("registry unavailable: {}: {}", err.code, err.message);
            if !quiet {
                eprintln!("a2a: warning: {msg}");
            }
            warning = Some(msg);
        }
    }

    Ok(json!({
        "registry_available": registry_available,
        "warning": warning,
        "items": rows.into_values().collect::<Vec<_>>()
    }))
}

async fn show(home: &A2aHome, work_id: &str) -> Result<Value, AppError> {
    let local = work_store::load(home, work_id)?;
    let registry = registry_row_optional(home, work_id).await?;
    if local.is_none() && registry.is_none() {
        return Err(work_not_found(work_id));
    }
    let mut out = json!({
        "work_id": work_id,
        "source": local.as_ref().map(source_name),
        "received_at": local.as_ref().map(|stored| stored.received_at),
        "title": local.as_ref().map(|stored| stored.verified.envelope.title.clone()),
        "brief_text": local.as_ref().map(|stored| stored.verified.envelope.brief_text.clone()),
        "acceptance": local.as_ref().map(|stored| stored.verified.envelope.acceptance.clone()),
        "repo": local.as_ref().map(|stored| stored.verified.envelope.repo.clone()),
        "provenance": local.as_ref().map(provenance),
        "registry": registry.as_ref().map(registry_view),
    });
    if out["title"].is_null() {
        out["title"] = json!(work_id);
    }
    Ok(out)
}

async fn approve(
    home: &A2aHome,
    work_id: &str,
    dry_run: bool,
    json_mode: bool,
) -> Result<Value, AppError> {
    let git = RealGit;
    let spawner = RealSpawner;
    if dry_run {
        let preview = work_exec::approve_dry_run(home, &git, &spawner, work_id).await?;
        if json_mode {
            return Ok(preview);
        }
        let sandbox = preview["sandbox"].as_str().unwrap_or("workspace-write");
        let network = preview["network"].as_bool().unwrap_or(false);
        let brief = preview["brief_text"].as_str().unwrap_or_default();
        let brief = render::escape_untrusted(brief, usize::MAX);
        return Ok(Value::String(format!(
            "sandbox: {sandbox}, network: {network}\n{}",
            render::fence_untrusted(&brief.text)
        )));
    }
    let registry = registry(home)?;
    let outcome = work_exec::advance_approve(home, &registry, &git, &spawner, work_id).await?;
    serde_json::to_value(outcome)
        .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))
}

async fn decline(
    home: &A2aHome,
    cfg: &Config,
    work_id: &str,
    reason: Option<&str>,
) -> Result<Value, AppError> {
    let registry = registry(home)?;
    let fleet_id = keys::fleet_id(home)?;
    let local_gateway = keys::gateway_id(home)?;
    let row = registry
        .work_get(&fleet_id, work_id)
        .await?
        .ok_or_else(|| work_not_found(work_id))?;
    ensure_can_receive(&row, &local_gateway)?;
    let outcome = registry
        .work_transition(
            &fleet_id,
            work_id,
            WorkStatus::Offered,
            WorkStatus::Declined,
            WorkOwner::Any,
            WorkPatch::default(),
        )
        .await?;
    if let TransitionOutcome::Lost(actual) = outcome {
        return Err(state_conflict(
            work_id,
            "offered",
            status_name(actual.status),
            "decline only applies to offered work; use `a2a mesh work reject <id>` once the item is held",
        ));
    }
    let summary = reason.unwrap_or("declined");
    let note_id = format!("{RESULT_NOTE_ID_PREFIX}{work_id}");
    let note = send_result_note(
        home,
        cfg,
        &registry,
        work_id,
        WorkResultStatus::Declined,
        summary,
        &note_id,
    )
    .await?;
    let feed_seq = feed::post(
        home,
        cfg,
        &local_peer(),
        &format!("work declined: {} ({work_id})", title_for(home, work_id)?),
        json!({ "work_id": work_id, "status": "declined" }),
    )?;
    Ok(json!({
        "work_id": work_id,
        "status": "declined",
        "result_note": note,
        "feed_seq": feed_seq
    }))
}

async fn cancel(home: &A2aHome, cfg: &Config, work_id: &str) -> Result<Value, AppError> {
    let registry = registry(home)?;
    let fleet_id = keys::fleet_id(home)?;
    let local_gateway = keys::gateway_id(home)?;
    let outcome = registry
        .work_transition(
            &fleet_id,
            work_id,
            WorkStatus::Offered,
            WorkStatus::Cancelled,
            WorkOwner::Sender(local_gateway.clone()),
            WorkPatch::default(),
        )
        .await?;
    let row = match outcome {
        TransitionOutcome::Applied => registry
            .work_get(&fleet_id, work_id)
            .await?
            .ok_or_else(|| work_not_found(work_id))?,
        TransitionOutcome::Lost(actual) => {
            return Err(state_conflict(
                work_id,
                "offered and sent by this gateway",
                status_name(actual.status),
                "cancel only applies to work you sent that is still offered",
            ));
        }
    };
    let note = if let Some(to_gateway) = row.addressing.strip_prefix("directed:") {
        Some(spool_cancelled_note(home, cfg, to_gateway, work_id, &title_for(home, work_id)?)?)
    } else {
        None
    };
    let feed_seq = feed::post(
        home,
        cfg,
        &local_peer(),
        &format!("work cancelled: {} ({work_id})", title_for(home, work_id)?),
        json!({ "work_id": work_id, "status": "cancelled" }),
    )?;
    Ok(json!({
        "work_id": work_id,
        "status": "cancelled",
        "cancel_note": note,
        "feed_seq": feed_seq
    }))
}

async fn status(home: &A2aHome, work_id: Option<&str>) -> Result<Value, AppError> {
    let local_gateway = keys::gateway_id(home)?;
    let mut rows = local_views(&work_store::list(home)?);
    let registry_rows = registry_rows(home, work_id).await?;
    overlay_registry_rows(&mut rows, registry_rows, &local_gateway, work_id);
    overlay_held_statuses(&mut rows, &held_announcements(home)?);
    if let Some(work_id) = work_id {
        if rows.is_empty() {
            return Err(work_not_found(work_id));
        }
    }
    Ok(json!({ "items": rows.into_values().collect::<Vec<_>>() }))
}

async fn ack(home: &A2aHome, work_id: &str) -> Result<Value, AppError> {
    let registry = registry(home)?;
    let fleet_id = keys::fleet_id(home)?;
    let local_gateway = keys::gateway_id(home)?;
    let row = registry
        .work_get(&fleet_id, work_id)
        .await?
        .ok_or_else(|| work_not_found(work_id))?;
    if row.sender_gw != local_gateway {
        return Err(state_conflict(
            work_id,
            "sent by this gateway",
            &format!("sent by {}", row.sender_gw),
            "ack only applies to work you sent",
        ));
    }
    let from = match row.status {
        WorkStatus::Done | WorkStatus::Failed => row.status,
        other => {
            return Err(state_conflict(
                work_id,
                "done or failed",
                status_name(other),
                "ack closes completed work only",
            ));
        }
    };
    let outcome = registry
        .work_transition(
            &fleet_id,
            work_id,
            from,
            WorkStatus::Acked,
            WorkOwner::Sender(local_gateway),
            WorkPatch::default(),
        )
        .await?;
    if let TransitionOutcome::Lost(actual) = outcome {
        return Err(state_conflict(
            work_id,
            "done or failed",
            status_name(actual.status),
            "ack closes completed work only",
        ));
    }
    write_ack_marker(home, work_id)?;
    Ok(json!({ "work_id": work_id, "status": "acked" }))
}

fn diff(
    home: &A2aHome,
    cfg: &Config,
    work_id: &str,
    raw: bool,
    json_mode: bool,
) -> Result<Value, AppError> {
    ensure_exact_stored_id(home, work_id)?;
    let state = work_exec::load_state(home, work_id)?.ok_or_else(|| work_not_found(work_id))?;
    let base_sha = state.base_sha.clone().ok_or_else(|| {
        AppError::new(
            "WORK_STATE_INVALID",
            ExitCode::Precondition,
            format!("work item {work_id} has no recorded base_sha"),
        )
        .remedy("approve records base_sha; this item predates the gate")
    })?;
    let range = if let Some(head_sha) = state.held_commit_sha.clone() {
        let closure_digest = state.closure_digest.clone().ok_or_else(|| {
            AppError::new(
                "WORK_STATE_INVALID",
                ExitCode::Precondition,
                format!("work item {work_id} has a held commit without a closure digest"),
            )
            .remedy("reconcile the completed work item to freeze a complete review range")
        })?;
        ResultRange {
            base_sha,
            head_sha,
            closure_digest,
        }
    } else {
        work_gate::freeze_range(&state.checkout, &base_sha)?
    };
    let report = work_gate::scan_review_unit(home, cfg, &state.checkout, &range, &state)?;
    let (note_payload, _) = work_gate::render_note_payload(&state, cfg)?;
    let hit_set_digest = report.digest();
    let rendered_diff = work_gate::render_diff(&state.checkout, &range, raw)?;
    if raw {
        eprintln!(
            "a2a: warning: --raw prints unmasked worker output; never pipe it into agent context"
        );
    }

    if json_mode {
        let hits = report
            .hits
            .iter()
            .map(|hit| {
                json!({
                    "path": render::escape_untrusted(&hit.path, 4_096).text,
                    "line": hit.line,
                    "rule": hit.rule,
                })
            })
            .collect::<Vec<_>>();
        return Ok(json!({
            "work_id": work_id,
            "diff": rendered_diff,
            "outgoing_note_payload": note_payload,
            "hits": hits,
            "hit_set_digest": hit_set_digest,
        }));
    }

    let mut output = rendered_diff;
    output.push('\n');
    output.push_str("===== OUTGOING NOTE PAYLOAD =====\n");
    output.push_str(&serde_json::to_string_pretty(&note_payload).map_err(|err| {
        AppError::new(
            "SERIALIZE",
            ExitCode::Internal,
            format!("serializing outgoing work note payload: {err}"),
        )
    })?);
    output.push('\n');
    for hit in &report.hits {
        let path = render::escape_untrusted(&hit.path, 4_096);
        output.push_str(&format!(
            "hit: {}:{} ({})\n",
            path.text, hit.line, hit.rule
        ));
    }
    output.push_str(&format!("hit_set_digest: {hit_set_digest}"));
    if !report.hits.is_empty() && std::io::stdout().is_terminal() {
        let token = work_gate::AllowSecretsToken::try_mint(&state, &hit_set_digest)?;
        output.push_str(&format!("\nconfirm_token: {token}"));
    }
    Ok(Value::String(output))
}

async fn retry_push(home: &A2aHome, cfg: &Config, work_id: &str) -> Result<Value, AppError> {
    let state = work_exec::load_state(home, work_id)?
        .ok_or_else(|| work_not_found(work_id))?;
    if state.phase == work_exec::WorkPhase::AwaitingRelease {
        return Err(AppError::new(
            "WORK_HELD",
            ExitCode::Precondition,
            format!("work item {work_id} is awaiting release"),
        )
        .remedy(
            "review it with `a2a mesh work diff <id>`, then use `release` or `reject`",
        ));
    }
    let registry = registry(home)?;
    let mut options = work_exec::WorkLoopOptions::for_config(cfg);
    options.force_push = true;
    let before = state.push_attempts;
    let next = work_exec::advance_completion_until(
        home,
        cfg,
        &registry,
        &work_exec::RealWorkCompletionIo,
        state,
        options,
    )
    .await?;
    Ok(json!({
        "work_id": work_id,
        "forced": true,
        "phase": format!("{:?}", next.phase).to_lowercase(),
        "attempted": next.push_attempts == before || next.push_attempts == before + 1,
        "result_ref": next.result_ref,
    }))
}

async fn release(
    home: &A2aHome,
    cfg: &Config,
    work_id: &str,
    allow_secrets: bool,
    confirm_token: Option<&str>,
) -> Result<Value, AppError> {
    ensure_exact_stored_id(home, work_id)?;
    if allow_secrets && confirm_token.is_none() {
        return Err(work_gate::allow_secrets_required(work_id));
    }
    let mut state = work_exec::load_state(home, work_id)?
        .ok_or_else(|| work_not_found(work_id))?;
    if state.phase == work_exec::WorkPhase::Reported && state.held_since.is_some() {
        if allow_secrets {
            verify_allow_secrets(
                home,
                cfg,
                &state,
                confirm_token.expect("checked above"),
            )?;
        }
        return Ok(json!({
            "work_id": work_id,
            "status": state.result_status.map(status_name),
            "phase": "reported",
            "result_ref": state.result_ref,
            "idempotent": true,
        }));
    }
    if state.phase != work_exec::WorkPhase::AwaitingRelease || state.held_since.is_none() {
        return Err(work_not_held(work_id));
    }

    let registry = registry(home)?;
    let fleet_id = keys::fleet_id(home)?;
    let local_gateway = keys::gateway_id(home)?;
    let row = registry
        .work_get(&fleet_id, work_id)
        .await?
        .ok_or_else(|| work_not_found(work_id))?;
    if row.status != WorkStatus::AwaitingRelease
        || row.claimed_by.as_deref() != Some(local_gateway.as_str())
    {
        return Err(work_not_held(work_id));
    }
    if allow_secrets {
        let hit_set_digest = verify_allow_secrets(
            home,
            cfg,
            &state,
            confirm_token.expect("checked above"),
        )?;
        spawn::append_audit(
            home,
            &json!({
                "event": "work_release_allow_secrets",
                "work_id": work_id,
                "hit_set_digest": hit_set_digest,
                "principal": owner::caller(),
            }),
        )?;
    }
    state = work_exec::release_from_hold(home, state)?;

    let mut options = work_exec::WorkLoopOptions::for_config(cfg);
    options.force_push = true;
    options.allow_secrets = allow_secrets;
    let next = work_exec::advance_completion_until(
        home,
        cfg,
        &registry,
        &work_exec::RealWorkCompletionIo,
        state,
        options,
    )
    .await?;
    Ok(json!({
        "work_id": work_id,
        "status": next.result_status.map(status_name),
        "phase": format!("{:?}", next.phase).to_lowercase(),
        "result_ref": next.result_ref,
        "idempotent": false,
    }))
}

fn verify_allow_secrets(
    home: &A2aHome,
    cfg: &Config,
    state: &work_exec::WorkState,
    token: &str,
) -> Result<String, AppError> {
    let base_sha = state.base_sha.as_deref().ok_or_else(|| {
        AppError::new(
            "WORK_STATE_INVALID",
            ExitCode::Precondition,
            format!("work item {} has no recorded base_sha", state.work_id),
        )
        .remedy("re-approve a fresh item before allowing a secret-bearing result")
    })?;
    let current_range = work_gate::freeze_range(&state.checkout, base_sha)?;
    let report =
        work_gate::scan_review_unit(home, cfg, &state.checkout, &current_range, state)?;
    let hit_set_digest = report.digest();
    work_gate::AllowSecretsToken::verify(home, &state.work_id, token, &hit_set_digest)?;
    Ok(hit_set_digest)
}

async fn reject(
    home: &A2aHome,
    cfg: &Config,
    work_id: &str,
    reason: &str,
    notify_sender: bool,
) -> Result<Value, AppError> {
    ensure_exact_stored_id(home, work_id)?;
    let mut state = work_exec::load_state(home, work_id)?
        .ok_or_else(|| work_not_found(work_id))?;
    let registry = registry(home)?;
    let fleet_id = keys::fleet_id(home)?;
    let local_gateway = keys::gateway_id(home)?;
    let row = registry
        .work_get(&fleet_id, work_id)
        .await?
        .ok_or_else(|| work_not_found(work_id))?;

    let already_rejected = row.status == WorkStatus::Rejected;
    if !already_rejected {
        if state.phase != work_exec::WorkPhase::AwaitingRelease || state.held_since.is_none() {
            return Err(work_not_held(work_id));
        }
        if row.status != WorkStatus::AwaitingRelease
            || row.claimed_by.as_deref() != Some(local_gateway.as_str())
        {
            return Err(work_not_held(work_id));
        }
        let outcome = registry
            .work_transition(
                &fleet_id,
                work_id,
                WorkStatus::AwaitingRelease,
                WorkStatus::Rejected,
                WorkOwner::Claimer(local_gateway),
                WorkPatch::default(),
            )
            .await?;
        if let TransitionOutcome::Lost(actual) = outcome {
            if actual.status != WorkStatus::Rejected {
                return Err(state_conflict(
                    work_id,
                    "awaiting_release",
                    status_name(actual.status),
                    "reject only applies to locally held work",
                ));
            }
        }
    }
    state.phase = work_exec::WorkPhase::Rejected;
    state.result_status = Some(WorkStatus::Rejected);
    state.result_ref = None;
    state.result_summary = Some(reason.to_string());
    state.evidence_excerpt = None;
    work_exec::save_state(home, &state)?;

    let note = if notify_sender {
        Some(
            send_result_note(
                home,
                cfg,
                &registry,
                work_id,
                WorkResultStatus::Rejected,
                reason,
                &state.result_note_id,
            )
            .await?,
        )
    } else {
        None
    };
    let feed_seq = if feed::read(home, 0)?.into_iter().any(|entry| {
        entry.refs["work_id"] == work_id && entry.refs["status"] == "rejected"
    }) {
        0
    } else {
        feed::post(
            home,
            cfg,
            &local_peer(),
            &format!("work rejected: {} ({work_id})", title_for(home, work_id)?),
            json!({ "work_id": work_id, "status": "rejected" }),
        )?
    };
    Ok(json!({
        "work_id": work_id,
        "status": "rejected",
        "summary": "rejected by the receiver",
        "result_ref": null,
        "result_note": note,
        "feed_seq": feed_seq,
        "idempotent": already_rejected,
    }))
}

async fn reopen(home: &A2aHome, work_id: &str) -> Result<Value, AppError> {
    ensure_exact_stored_id(home, work_id)?;
    let mut state = work_exec::load_state(home, work_id)?
        .ok_or_else(|| work_not_found(work_id))?;
    if state.held_since.is_none() {
        return Err(work_not_held(work_id));
    }
    let registry = registry(home)?;
    let fleet_id = keys::fleet_id(home)?;
    let local_gateway = keys::gateway_id(home)?;
    let row = registry
        .work_get(&fleet_id, work_id)
        .await?
        .ok_or_else(|| work_not_found(work_id))?;
    if row.status == WorkStatus::AwaitingRelease
        && row.claimed_by.as_deref() == Some(local_gateway.as_str())
    {
        state.phase = work_exec::WorkPhase::AwaitingRelease;
        work_exec::save_state(home, &state)?;
        return Ok(json!({
            "work_id": work_id,
            "status": "awaiting_release",
            "idempotent": true,
        }));
    }
    if row.status != WorkStatus::Failed
        || row.result_delivery.as_deref() != Some("release_stalled")
        || row.claimed_by.as_deref() != Some(local_gateway.as_str())
    {
        return Err(AppError::new(
            "WORK_NOT_HELD",
            ExitCode::Precondition,
            format!("work item {work_id} is not a locally held release-stalled item"),
        )
        .remedy("reopen only a held item failed by the release-stall sweep"));
    }
    let outcome = registry
        .work_transition(
            &fleet_id,
            work_id,
            WorkStatus::Failed,
            WorkStatus::AwaitingRelease,
            WorkOwner::Claimer(local_gateway),
            WorkPatch {
                result_delivery: Some(None),
                swept_by: Some(None),
                ..WorkPatch::default()
            },
        )
        .await?;
    if let TransitionOutcome::Lost(actual) = outcome {
        return Err(state_conflict(
            work_id,
            "failed{release_stalled}",
            status_name(actual.status),
            "reopen only a held item failed by the release-stall sweep",
        ));
    }
    state.phase = work_exec::WorkPhase::AwaitingRelease;
    work_exec::save_state(home, &state)?;
    Ok(json!({
        "work_id": work_id,
        "status": "awaiting_release",
        "idempotent": false,
    }))
}

pub async fn send_result_note<R: Registry + ?Sized>(
    home: &A2aHome,
    cfg: &Config,
    registry: &R,
    work_id: &str,
    status: WorkResultStatus,
    reason_or_summary: &str,
    note_id: &str,
) -> Result<Value, AppError> {
    let fleet_id = keys::fleet_id(home)?;
    let row = registry
        .work_get(&fleet_id, work_id)
        .await?
        .ok_or_else(|| work_not_found(work_id))?;
    let local_gateway = keys::gateway_id(home)?;
    let stored = ensure_exact_stored_id(home, work_id)?;
    let sender_gateway = stored.verified.envelope.sender.gateway_id;
    let title = title_for(home, work_id)?;
    let result_ref = match status {
        WorkResultStatus::Done | WorkResultStatus::Failed => Some(
            row.result_ref
                .clone()
                .unwrap_or_else(|| format!("a2a/work/{work_id}")),
        ),
        WorkResultStatus::AwaitingRelease
        | WorkResultStatus::Declined
        | WorkResultStatus::Rejected => None,
    };
    let summary = capped_result_text(reason_or_summary, cfg.mesh.work.result_note_max_bytes)?;
    let refs = json!({
        "result": {
            "work_id": work_id,
            "status": status.as_str(),
            "result_ref": result_ref,
            "summary": summary,
            "evidence_excerpt": ""
        }
    });
    feed::validate_refs_size(&refs, cfg.mesh.work.result_note_max_bytes)?;
    let text = match result_ref.as_deref() {
        Some(result_ref) => format!(
            "work {}: {} ({}) -> {}",
            status.as_str(),
            clean(&title),
            work_id,
            result_ref
        ),
        None => format!("work {}: {} ({})", status.as_str(), clean(&title), work_id),
    };
    let note = Note {
        note_id: Some(note_id.to_string()),
        from: local_peer().to_string(),
        to: "user:mesh-work".to_string(),
        ts: Utc::now(),
        kind: WORK_RESULT_KIND.to_string(),
        text,
        refs,
        extra: serde_json::Map::new(),
    };
    let path = notes_wire::spool_note(home, cfg, &local_gateway, &sender_gateway, note)?;
    Ok(json!({
        "note_id": note_id,
        "to_gateway": sender_gateway,
        "path": path
    }))
}

fn build_envelope(
    home: &A2aHome,
    args: &MeshWorkCreateArgs,
    addressing: WorkAddressing,
    default_ttl_days: u64,
    max_bytes: usize,
) -> Result<WorkEnvelope, AppError> {
    let brief_text = read_brief(&args.brief)?;
    validate_work_brief(&brief_text, &args.brief)?;
    scan_work_text("title", &args.title)?;
    scan_work_text("acceptance", &args.acceptance)?;
    let work_id = args
        .work_id
        .clone()
        .unwrap_or_else(|| ulid::Ulid::new().to_string());
    work_store::validate_work_id(&work_id)?;
    sign_envelope(
        home,
        UnsignedWorkEnvelope {
            work_id,
            session: args
                .session
                .clone()
                .unwrap_or_else(|| format!("user:{}", safe_peer_component(&owner::caller()))),
            addressing,
            repo: WorkRepo {
                remote_url: args.repo.clone(),
                base_ref: args.base_ref.clone(),
            },
            title: args.title.clone(),
            brief_text,
            acceptance: args.acceptance.clone(),
            ttl_days: args
                .ttl_days
                .unwrap_or_else(|| i64::try_from(default_ttl_days).unwrap_or(i64::MAX)),
            max_bytes,
        },
        &keys::SystemClock,
    )
}

fn read_brief(path: &Path) -> Result<String, AppError> {
    if path == Path::new("-") {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading brief: {e}")))?;
        return Ok(text);
    }
    std::fs::read_to_string(path).map_err(|e| {
        AppError::new(
            "IO",
            ExitCode::Precondition,
            format!("reading brief {}: {e}", path.display()),
        )
        .remedy("pass --brief with a readable brief file, or --brief - to read stdin")
    })
}

fn validate_work_brief(text: &str, path: &Path) -> Result<(), AppError> {
    let parsed = brief::parse(text).map_err(|e| {
        AppError::new(
            "WORK_BRIEF_INVALID",
            ExitCode::Precondition,
            format!("{}: {}", path.display(), redact::redact_egress(&e)),
        )
        .remedy("fix the brief front matter so it parses before sending work")
    })?;
    let lane = parsed.lane.unwrap_or(Lane::Impl);
    if let Err(missing) = brief::validate(&parsed, lane) {
        return Err(AppError::new(
            "WORK_BRIEF_INVALID",
            ExitCode::Precondition,
            format!(
                "{}: the {lane} lane requires front-matter key(s): {}",
                path.display(),
                missing.join(", ")
            ),
        )
        .remedy("add the missing keys to the brief's YAML front matter"));
    }
    scan_work_text("brief", text)
}

fn scan_work_text(field: &'static str, text: &str) -> Result<(), AppError> {
    let hits = redact::scan_text(text);
    if hits.is_empty() {
        return Ok(());
    }
    let where_ = hits
        .iter()
        .take(5)
        .map(|hit| format!("line {} ({})", hit.line, hit.rule))
        .collect::<Vec<_>>()
        .join(", ");
    Err(AppError::new(
        "SECRET_IN_BRIEF",
        ExitCode::Precondition,
        format!(
            "work {field} contains {} secret-shaped value(s): {where_}",
            hits.len()
        ),
    )
    .remedy("remove the credential before sending work"))
}

async fn insert_work_row(registry: &RegistryHandle, env: &WorkEnvelope) -> Result<(), AppError> {
    let now = Utc::now();
    registry
        .work_insert(WorkRow {
            fleet_id: env.fleet_id.clone(),
            work_id: env.work_id.clone(),
            sender_gw: env.sender.gateway_id.clone(),
            addressing: serde_json::to_value(&env.addressing)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "pool".to_string()),
            status: WorkStatus::Offered,
            claimed_by: None,
            result_ref: None,
            result_delivery: None,
            swept_by: None,
            created_at: env.created_at,
            updated_at: now,
            expires_at: env.expires_at,
            schema_version: REGISTRY_SCHEMA_VERSION,
        })
        .await
}

fn spool_work_offer(
    home: &A2aHome,
    cfg: &Config,
    to_gateway: &str,
    env: &WorkEnvelope,
) -> Result<PathBuf, AppError> {
    let note = Note {
        note_id: Some(env.work_id.clone()),
        from: local_peer().to_string(),
        to: format!("gateway:{to_gateway}"),
        ts: Utc::now(),
        kind: WORK_OFFER_KIND.to_string(),
        text: format!("work offer: {} ({})", clean(&env.title), env.work_id),
        refs: json!({ "work": env }),
        extra: serde_json::Map::new(),
    };
    let from_gateway = keys::gateway_id(home)?;
    notes_wire::spool_note(home, cfg, &from_gateway, to_gateway, note)
}

fn spool_cancelled_note(
    home: &A2aHome,
    cfg: &Config,
    to_gateway: &str,
    work_id: &str,
    title: &str,
) -> Result<PathBuf, AppError> {
    let note_id = format!("work-cancelled-{work_id}");
    let note = Note {
        note_id: Some(note_id),
        from: local_peer().to_string(),
        to: "user:mesh-work".to_string(),
        ts: Utc::now(),
        kind: WORK_CANCELLED_KIND.to_string(),
        text: format!("work cancelled: {} ({work_id})", clean(title)),
        refs: json!({ "work_id": work_id }),
        extra: serde_json::Map::new(),
    };
    let from_gateway = keys::gateway_id(home)?;
    notes_wire::spool_note(home, cfg, &from_gateway, to_gateway, note)
}

fn registry(home: &A2aHome) -> Result<RegistryHandle, AppError> {
    registry_with_verify_cfg(home, RowVerifyCfg::new(Utc::now()))
}

fn registry_with_verify_cfg(
    home: &A2aHome,
    verify_cfg: RowVerifyCfg,
) -> Result<RegistryHandle, AppError> {
    let url = read_registry_env(home)?;
    Ok(open_registry(&url, reader_roots(home)?, keys::fleet_id(home)?)?
        .with_reader_state(RowMarkStore::open(home)?, verify_cfg))
}

async fn registry_rows(home: &A2aHome, work_id: Option<&str>) -> Result<Vec<WorkRow>, AppError> {
    let registry = registry(home)?;
    let fleet_id = keys::fleet_id(home)?;
    if let Some(work_id) = work_id {
        return Ok(registry
            .work_get(&fleet_id, work_id)
            .await?
            .into_iter()
            .collect());
    }
    registry.work_list(&fleet_id, WorkListFilter::default()).await
}

async fn registry_row_optional(
    home: &A2aHome,
    work_id: &str,
) -> Result<Option<WorkRow>, AppError> {
    let registry = registry(home)?;
    let fleet_id = keys::fleet_id(home)?;
    registry.work_get(&fleet_id, work_id).await
}

async fn addressee_row(
    registry: &RegistryHandle,
    gateway_id: &str,
) -> Result<Option<(crate::mesh::registry::GatewayRow, RowVerdict)>, AppError> {
    let rows = registry.discover().await?;
    if let Some(incumbent) = endpoint_conflicts(&rows).get(gateway_id) {
        let rendered_gateway = escape_untrusted(gateway_id, 128).text;
        let rendered_incumbent = escape_untrusted(incumbent, 128).text;
        return Err(AppError::new(
            "WORK_PEER_ROW_INVALID",
            ExitCode::Precondition,
            format!(
                "gateway {rendered_gateway} has ROW_ENDPOINT_CONFLICT with incumbent {rendered_incumbent} and cannot receive directed work"
            ),
        )
        .remedy("capture both rows for evidence first; keep the incumbent endpoint and investigate the later row")
        .data(json!({
            "gateway_id": rendered_gateway,
            "flag": "ROW_ENDPOINT_CONFLICT",
            "incumbent_gateway_id": rendered_incumbent,
        })));
    }
    Ok(rows
        .into_iter()
        .find(|(row, _verdict)| row.gateway_id == gateway_id))
}

fn preflight_peer_version(gateway_id: &str, version: Option<&str>) -> Result<(), AppError> {
    if version.is_some_and(|version| version_at_least(version, MIN_WORK_PEER_VERSION)) {
        return Ok(());
    }
    let recorded = version.filter(|v| !v.trim().is_empty()).unwrap_or("missing");
    Err(AppError::new(
        "WORK_PEER_TOO_OLD",
        ExitCode::Precondition,
        format!(
            "gateway {gateway_id} records version {recorded}, but mesh work requires >= {MIN_WORK_PEER_VERSION}"
        ),
    )
    .remedy("ask them to upgrade and restart their gateway; the registry version refreshes only on gateway restart")
    .data(json!({ "gateway_id": gateway_id, "version": recorded, "minimum": MIN_WORK_PEER_VERSION })))
}

pub(crate) fn version_at_least(actual: &str, minimum: &str) -> bool {
    let Some(actual) = parse_version(actual) else {
        return false;
    };
    let Some(minimum) = parse_version(minimum) else {
        return false;
    };
    actual >= minimum
}

fn parse_version(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.trim().split('.');
    let major = parse_version_part(parts.next()?)?;
    let minor = parse_version_part(parts.next()?)?;
    let patch = parse_version_part(parts.next().unwrap_or("0"))?;
    Some((major, minor, patch))
}

fn parse_version_part(part: &str) -> Option<u64> {
    let digits = part
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

fn local_views(local: &[StoredWork]) -> BTreeMap<String, Value> {
    local
        .iter()
        .map(|stored| {
            (
                stored.work_id.clone(),
                json!({
                    "work_id": stored.work_id,
                    "source": source_name(stored),
                    "status": "local",
                    "title": stored.verified.envelope.title,
                    "sender_gw": stored.verified.envelope.sender.gateway_id,
                    "sender_session": stored.verified.envelope.sender.session,
                    "addressing": stored.verified.envelope.addressing,
                    "claimed_by": null,
                    "result_ref": null,
                    "result_delivery": null,
                    "received_at": stored.received_at,
                    "provenance": provenance(stored),
                }),
            )
        })
        .collect()
}

fn overlay_registry_rows(
    rows: &mut BTreeMap<String, Value>,
    registry_rows: Vec<WorkRow>,
    local_gateway: &str,
    exact_work_id: Option<&str>,
) {
    let directed_to_me = format!("directed:{local_gateway}");
    let local_ids = rows.keys().cloned().collect::<BTreeSet<_>>();
    for row in registry_rows {
        if exact_work_id.is_none()
            && !local_ids.contains(&row.work_id)
            && row.sender_gw != local_gateway
            && row.claimed_by.as_deref() != Some(local_gateway)
            && row.addressing != "pool"
            && row.addressing != directed_to_me
        {
            continue;
        }
        let entry = rows.entry(row.work_id.clone()).or_insert_with(|| {
            json!({
                "work_id": row.work_id,
                "source": "registry",
                "title": row.work_id,
                "received_at": null,
                "provenance": null,
            })
        });
        entry["status"] = json!(status_name(row.status));
        entry["sender_gw"] = json!(row.sender_gw);
        entry["addressing"] = json!(row.addressing);
        entry["claimed_by"] = json!(row.claimed_by);
        entry["result_ref"] = json!(row.result_ref);
        entry["result_delivery"] = json!(row.result_delivery);
        if row.status == WorkStatus::Rejected {
            entry["summary"] = json!("rejected by the receiver");
        }
        entry["registry"] = registry_view(&row);
    }
}

fn held_announcements(home: &A2aHome) -> Result<BTreeMap<String, String>, AppError> {
    let peer = PeerId::parse("user:mesh-work")?;
    let inbox = crate::mesh::peer_dir(home, &peer).join("inbox");
    let mut newest = BTreeMap::<String, (chrono::DateTime<Utc>, String)>::new();
    for dir in [inbox.clone(), inbox.join("read")] {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(AppError::new(
                    "IO",
                    ExitCode::Internal,
                    format!("reading held work notes: {err}"),
                ));
            }
        };
        for entry in entries {
            let entry = entry.map_err(|err| {
                AppError::new(
                    "IO",
                    ExitCode::Internal,
                    format!("reading held work note entry: {err}"),
                )
            })?;
            if !entry
                .file_type()
                .map_err(|err| {
                    AppError::new(
                        "IO",
                        ExitCode::Internal,
                        format!("reading held work note type: {err}"),
                    )
                })?
                .is_file()
            {
                continue;
            }
            let bytes = fs::read(entry.path()).map_err(|err| {
                AppError::new(
                    "IO",
                    ExitCode::Internal,
                    format!("reading held work note: {err}"),
                )
            })?;
            let Ok(note) = serde_json::from_slice::<Note>(&bytes) else {
                continue;
            };
            if note.kind != work_store::WORK_HELD_KIND {
                continue;
            }
            let Some(held) = note.refs.get("held") else {
                continue;
            };
            let Some(work_id) = held.get("work_id").and_then(Value::as_str) else {
                continue;
            };
            let Some(held_since) = held.get("held_since").and_then(Value::as_str) else {
                continue;
            };
            if work_store::validate_work_id(work_id).is_err() {
                continue;
            }
            let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(held_since) else {
                continue;
            };
            let parsed = parsed.with_timezone(&Utc);
            let replace = newest
                .get(work_id)
                .is_none_or(|(existing, _)| parsed > *existing);
            if replace {
                newest.insert(work_id.to_string(), (parsed, held_since.to_string()));
            }
        }
    }
    Ok(newest
        .into_iter()
        .map(|(work_id, (_, held_since))| (work_id, held_since))
        .collect())
}

fn overlay_held_statuses(
    rows: &mut BTreeMap<String, Value>,
    held: &BTreeMap<String, String>,
) {
    for (work_id, held_since) in held {
        let Some(row) = rows.get_mut(work_id) else {
            continue;
        };
        if row["registry"]["status"] != "awaiting_release" {
            continue;
        }
        let Some(gateway) = row["claimed_by"].as_str().map(str::to_string) else {
            continue;
        };
        row["held_since"] = json!(held_since);
        row["status"] = json!(format!(
            "running (held on {gateway} since {held_since})"
        ));
    }
}

fn registry_view(row: &WorkRow) -> Value {
    json!({
        "fleet_id": row.fleet_id,
        "work_id": row.work_id,
        "sender_gw": row.sender_gw,
        "addressing": row.addressing,
        "status": status_name(row.status),
        "claimed_by": row.claimed_by,
        "result_ref": row.result_ref,
        "result_delivery": row.result_delivery,
        "swept_by": row.swept_by,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "expires_at": row.expires_at,
        "schema_version": row.schema_version,
    })
}

fn provenance(stored: &StoredWork) -> Value {
    json!({
        "sender_gateway": stored.verified.envelope.sender.gateway_id,
        "sender_session": stored.verified.envelope.sender.session,
        "sender_pubkey_b64": stored.verified.envelope.sender.pubkey_b64,
        "token_fleet_id": stored.verified.sender_claims.fleet_id,
        "token_gateway_id": stored.verified.sender_claims.gateway_id,
        "source": source_name(stored),
    })
}

fn ensure_exact_stored_id(home: &A2aHome, work_id: &str) -> Result<StoredWork, AppError> {
    let stored = work_store::load(home, work_id)?.ok_or_else(|| work_not_found(work_id))?;
    if stored.verified.envelope.work_id != work_id {
        return Err(state_conflict(
            work_id,
            "an exact local envelope id",
            &stored.verified.envelope.work_id,
            "remove the mismatched local work entry and ask the sender to re-send it",
        ));
    }
    Ok(stored)
}

fn source_name(stored: &StoredWork) -> &'static str {
    match stored.source {
        work_store::WorkSource::Directed => "directed",
        work_store::WorkSource::Pool => "pool",
    }
}

fn ensure_can_receive(row: &WorkRow, local_gateway: &str) -> Result<(), AppError> {
    let directed_to_me = format!("directed:{local_gateway}");
    if row.addressing == "pool" || row.addressing == directed_to_me {
        return Ok(());
    }
    Err(state_conflict(
        &row.work_id,
        "addressed to this gateway or pool",
        &row.addressing,
        "decline work from the gateway that received it",
    ))
}

fn write_ack_marker(home: &A2aHome, work_id: &str) -> Result<(), AppError> {
    let dir = home.root().join("work").join("acked");
    create_dir_0700(&dir).map_err(|e| AppError::new("IO", ExitCode::Internal, format!("creating ack marker dir: {e}")))?;
    let path = dir.join(format!("{work_id}.json"));
    let mut file = write_new_0600(&path)
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("writing ack marker: {e}")))?;
    file.write_all(
        serde_json::to_string_pretty(&json!({
            "work_id": work_id,
            "acked_at": Utc::now(),
        }))
        .map_err(|e| AppError::new("SERIALIZE", ExitCode::Internal, e.to_string()))?
        .as_bytes(),
    )
    .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("writing ack marker: {e}")))?;
    file.write_all(b"\n")
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("writing ack marker: {e}")))?;
    file.sync_all()
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("syncing ack marker: {e}")))
}

fn title_for(home: &A2aHome, work_id: &str) -> Result<String, AppError> {
    Ok(work_store::load(home, work_id)?
        .map(|stored| clean(&stored.verified.envelope.title))
        .unwrap_or_else(|| work_id.to_string()))
}

fn capped_result_text(text: &str, cap: u64) -> Result<String, AppError> {
    let redacted = redact::redact_egress(text);
    if cap == 0 || redacted.len() <= cap as usize {
        return Ok(redacted);
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
    Ok(out)
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

fn state_conflict(
    work_id: &str,
    expected: &str,
    actual: &str,
    remedy: &'static str,
) -> AppError {
    AppError::new(
        "WORK_STATE_CONFLICT",
        ExitCode::Precondition,
        format!("work item {work_id} expected {expected}, but is {actual}"),
    )
    .remedy(remedy)
    .data(json!({ "work_id": work_id, "expected": expected, "actual": actual }))
}

fn work_not_found(work_id: &str) -> AppError {
    AppError::new(
        "WORK_NOT_FOUND",
        ExitCode::Precondition,
        format!("work item {work_id} was not found"),
    )
    .remedy("check `a2a mesh work list` or ask the sender to re-send the work")
}

fn work_not_held(work_id: &str) -> AppError {
    AppError::new(
        "WORK_NOT_HELD",
        ExitCode::Precondition,
        format!("work item {work_id} is not awaiting release"),
    )
    .remedy("use `a2a mesh work release <id>` or `reject <id>` only for locally held work")
}

/// Work JSON sanitation preserves line structure and is explicitly not the terminal-safe
/// `mesh::render::escape_untrusted` contract used for peer-authored fleet display fields.
fn clean(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_control() || matches!(c, '\n' | '\r' | '\t'))
        .collect()
}

fn sanitize_value(value: Value) -> Value {
    match value {
        Value::String(s) => Value::String(clean(&s)),
        Value::Array(values) => Value::Array(values.into_iter().map(sanitize_value).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| (key, sanitize_value(value)))
                .collect(),
        ),
        other => other,
    }
}

fn local_peer() -> PeerId {
    PeerId::parse(&format!("user:{}", safe_peer_component(&owner::caller())))
        .unwrap_or_else(|_| PeerId::parse("user:mesh-work-cli").expect("static peer id parses"))
}

fn safe_peer_component(s: &str) -> String {
    let out = s
        .chars()
        .filter(|c| !c.is_control() && !matches!(c, ':' | '/' | '\\') && !c.is_whitespace())
        .collect::<String>();
    if out.is_empty() {
        "mesh-work-cli".to_string()
    } else {
        out
    }
}
