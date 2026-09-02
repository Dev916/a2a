use crate::brief;
use crate::cli::{MeshWorkCmd, MeshWorkCreateArgs, MeshWorkOfferArgs, MeshWorkSendArgs};
use crate::commands::owner;
use crate::config::{Config, Lane};
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::mesh::{
    feed,
    gateway::notes_wire,
    keys,
    registry::{
        open_registry, read_registry_env, Registry, RegistryHandle, TransitionOutcome,
        WorkListFilter, WorkOwner, WorkPatch, WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION,
    },
    work::{sign_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkEnvelope, WorkRepo},
    work_exec::{self, RealGit, RealSpawner},
    work_store::{self, StoredWork, WORK_CANCELLED_KIND, WORK_OFFER_KIND, WORK_RESULT_KIND},
    Note, PeerId,
};
use crate::output::{AppError, ExitCode, Outcome};
use crate::redact;
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MIN_WORK_PEER_VERSION: &str = crate::broker::wire::BINARY_VERSION;
const RESULT_NOTE_ID_PREFIX: &str = "work-result-";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkResultStatus {
    Done,
    Failed,
    Declined,
}

impl WorkResultStatus {
    fn as_str(self) -> &'static str {
        match self {
            WorkResultStatus::Done => "done",
            WorkResultStatus::Failed => "failed",
            WorkResultStatus::Declined => "declined",
        }
    }
}

pub fn run(
    home: &A2aHome,
    cfg: &Config,
    cmd: &MeshWorkCmd,
    quiet: bool,
) -> Result<Outcome, AppError> {
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
            MeshWorkCmd::Approve { work_id, dry_run } => approve(home, work_id, *dry_run).await?,
            MeshWorkCmd::Decline { work_id, reason } => {
                decline(home, cfg, work_id, reason.as_deref()).await?
            }
            MeshWorkCmd::Cancel { work_id } => cancel(home, cfg, work_id).await?,
            MeshWorkCmd::Status { work_id } => status(home, work_id.as_deref()).await?,
            MeshWorkCmd::Ack { work_id } => ack(home, work_id).await?,
            MeshWorkCmd::RetryPush { work_id } => retry_push(home, cfg, work_id).await?,
        };
        Ok(Outcome::from(sanitize_value(out)))
    })
}

async fn send(home: &A2aHome, cfg: &Config, args: &MeshWorkSendArgs) -> Result<Value, AppError> {
    let registry = registry(home)?;
    let fleet_id = keys::fleet_id(home)?;
    let addressee = addressee_row(&registry, &fleet_id, &args.to).await?;
    preflight_peer_version(&args.to, addressee.as_ref().map(|row| row.version.as_str()))?;
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

async fn approve(home: &A2aHome, work_id: &str, dry_run: bool) -> Result<Value, AppError> {
    let git = RealGit;
    let spawner = RealSpawner;
    if dry_run {
        return work_exec::approve_dry_run(home, &git, &spawner, work_id).await;
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
            "decline only applies to offered work",
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

async fn retry_push(home: &A2aHome, cfg: &Config, work_id: &str) -> Result<Value, AppError> {
    let state = work_exec::load_state(home, work_id)?
        .ok_or_else(|| work_not_found(work_id))?;
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
    let title = title_for(home, work_id)?;
    let result_ref = row
        .result_ref
        .clone()
        .unwrap_or_else(|| format!("a2a/work/{work_id}"));
    let summary = capped_result_text(reason_or_summary, cfg.mesh.work.result_note_max_bytes)?;
    let refs = json!({
        "result": {
            "status": status.as_str(),
            "result_ref": result_ref,
            "summary": summary,
            "evidence_excerpt": ""
        }
    });
    feed::validate_refs_size(&refs, cfg.mesh.work.result_note_max_bytes)?;
    let note = Note {
        note_id: Some(note_id.to_string()),
        from: local_peer().to_string(),
        to: "user:mesh-work".to_string(),
        ts: Utc::now(),
        kind: WORK_RESULT_KIND.to_string(),
        text: format!(
            "work {}: {} ({}) -> {}",
            status.as_str(),
            clean(&title),
            work_id,
            result_ref
        ),
        refs,
        extra: serde_json::Map::new(),
    };
    let path = notes_wire::spool_note(home, cfg, &local_gateway, &row.sender_gw, note)?;
    Ok(json!({
        "note_id": note_id,
        "to_gateway": row.sender_gw,
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
    let work_id = ulid::Ulid::new().to_string();
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
    let url = read_registry_env(home)?;
    open_registry(&url)
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
    fleet_id: &str,
    gateway_id: &str,
) -> Result<Option<crate::mesh::registry::GatewayRow>, AppError> {
    Ok(registry
        .discover(fleet_id)
        .await?
        .into_iter()
        .find(|row| row.gateway_id == gateway_id))
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

fn version_at_least(actual: &str, minimum: &str) -> bool {
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
        entry["registry"] = registry_view(&row);
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
    let path = dir.join(format!("{}.json", safe_file_component(work_id)));
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
        WorkStatus::Done => "done",
        WorkStatus::Failed => "failed",
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

fn safe_file_component(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect()
}
