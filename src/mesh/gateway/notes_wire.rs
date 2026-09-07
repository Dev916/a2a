use crate::config::Config;
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::mesh::{
    feed,
    keys::{self, SystemClock},
    registry::{self, Registry, WorkOwner, WorkPatch, WorkStatus},
    row_mark::RowMarkStore,
    row_sig::RowVerifyCfg,
    peer_dir, scan_or_refuse,
    work::{verify_envelope, WorkEnvelope},
    work_exec::{corroborate_sender, CorroborationStage},
    work_store::{
        self, WorkSource, WORK_CANCELLED_KIND, WORK_OFFER_KIND, WORK_RESULT_KIND,
    },
    Note, PeerId, NOTE_MAX_BYTES,
};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SPOOL_SCHEMA_VERSION: u32 = 1;
const DEDUP_FILE: &str = "note-dedup.json";
const DEDUP_CAP_PER_ORIGIN: usize = 4096;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpooledNote {
    pub schema_version: u32,
    pub trace_id: String,
    pub from_gateway: String,
    pub to_gateway: String,
    pub to_peer: String,
    pub attempts: u32,
    pub next_attempt_after: Option<DateTime<Utc>>,
    pub note: Note,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NoteAck {
    Delivered,
    Duplicate,
    Rejected { code: String },
}

#[derive(Clone, Copy)]
pub enum NoteDeliveryTarget<'a> {
    Gateway {
        to_gateway: &'a str,
        home: Option<&'a A2aHome>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpoolPassReport {
    pub delivered: usize,
    pub duplicates: usize,
    pub rejected: usize,
    pub retrying: usize,
    pub skipped: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct DedupFile {
    #[serde(default)]
    origins: std::collections::BTreeMap<String, Vec<String>>,
}

pub fn spool_note(
    home: &A2aHome,
    _cfg: &Config,
    from_gateway: &str,
    to_gateway: &str,
    mut note: Note,
) -> Result<PathBuf, AppError> {
    let trace_id = note.note_id.get_or_insert_with(mint_note_id).clone();
    let envelope = SpooledNote {
        schema_version: SPOOL_SCHEMA_VERSION,
        trace_id: trace_id.clone(),
        from_gateway: from_gateway.to_string(),
        to_gateway: to_gateway.to_string(),
        to_peer: note.to.clone(),
        attempts: 0,
        next_attempt_after: None,
        note,
    };
    let dir = outbound_dir(home, to_gateway);
    create_dir_0700(&dir).map_err(|e| io_err("creating outbound note dir", e))?;
    let path = dir.join(format!(
        "{}-{}-{}.note.json",
        Utc::now().timestamp_millis().max(0),
        safe_file_part(&trace_id),
        rand8_hex()
    ));
    write_envelope_new(&path, &envelope)?;
    log_note_action(
        home,
        &envelope.from_gateway,
        &envelope.to_gateway,
        &envelope.to_peer,
        &envelope.trace_id,
        "queued",
    );
    Ok(path)
}

pub fn read_spooled(path: &Path) -> Result<SpooledNote, AppError> {
    let bytes = fs::read(path).map_err(|e| io_err("reading spooled note", e))?;
    serde_json::from_slice(&bytes).map_err(|e| data_err("decoding spooled note", e))
}

pub fn deliver_spooled_once<R: Rng + ?Sized>(
    cfg: &Config,
    sender: &A2aHome,
    target: NoteDeliveryTarget<'_>,
    now: DateTime<Utc>,
    rng: &mut R,
) -> Result<SpoolPassReport, AppError> {
    let NoteDeliveryTarget::Gateway {
        to_gateway,
        home: receiver,
    } = target;
    let dir = outbound_dir(sender, to_gateway);
    let mut report = SpoolPassReport::default();
    let mut paths = note_paths(&dir)?;
    paths.sort();
    for path in paths {
        let mut envelope = read_spooled(&path)?;
        if envelope
            .next_attempt_after
            .map(|due| due > now)
            .unwrap_or(false)
        {
            report.skipped += 1;
            continue;
        }
        let Some(receiver) = receiver else {
            schedule_retry(cfg, &path, &mut envelope, now, rng)?;
            report.retrying += 1;
            continue;
        };
        match receive_note(receiver, cfg, &envelope)? {
            NoteAck::Delivered => {
                archive(sender, &path, "sent", to_gateway)?;
                log_note_action(
                    sender,
                    &envelope.from_gateway,
                    &envelope.to_gateway,
                    &envelope.to_peer,
                    &envelope.trace_id,
                    "delivered",
                );
                report.delivered += 1;
            }
            NoteAck::Duplicate => {
                archive(sender, &path, "sent", to_gateway)?;
                log_note_action(
                    sender,
                    &envelope.from_gateway,
                    &envelope.to_gateway,
                    &envelope.to_peer,
                    &envelope.trace_id,
                    "delivered",
                );
                report.delivered += 1;
                report.duplicates += 1;
            }
            NoteAck::Rejected { .. } => {
                archive(sender, &path, "rejected", to_gateway)?;
                log_note_action(
                    sender,
                    &envelope.from_gateway,
                    &envelope.to_gateway,
                    &envelope.to_peer,
                    &envelope.trace_id,
                    "rejected",
                );
                report.rejected += 1;
            }
        }
    }
    Ok(report)
}

pub fn handle_ack(
    sender: &A2aHome,
    to_gateway: &str,
    ref_id: &str,
    outcome: &str,
) -> Result<bool, AppError> {
    let path = find_spooled_by_trace(sender, to_gateway, ref_id)?;
    let Some(path) = path else {
        return Ok(false);
    };
    let envelope = read_spooled(&path)?;
    match outcome {
        "delivered" => {
            archive(sender, &path, "sent", to_gateway)?;
            record_result_delivery_ack(sender, &envelope, "delivered");
            log_note_action(
                sender,
                &envelope.from_gateway,
                &envelope.to_gateway,
                &envelope.to_peer,
                &envelope.trace_id,
                "delivered",
            );
            Ok(true)
        }
        "ingress_refused" | "TOKEN_EXPIRED" | "WORK_SIGNATURE_INVALID" | "WORK_SENDER_UNKNOWN"
        | "WORK_TOKEN_MISMATCH" | "WORK_EXPIRED" | "WORK_TOO_LARGE" | "WORK_BAD_REPO_URL"
        | "WORK_WRONG_FLEET" | "WORK_SCHEMA_MISSING" | "WORK_CONTROL_CHARS"
        | "WORK_ID_INVALID" => {
            archive(sender, &path, "rejected", to_gateway)?;
            record_result_delivery_ack(sender, &envelope, &format!("rejected:{outcome}"));
            log_note_action(
                sender,
                &envelope.from_gateway,
                &envelope.to_gateway,
                &envelope.to_peer,
                &envelope.trace_id,
                "rejected",
            );
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn record_result_delivery_ack(sender: &A2aHome, envelope: &SpooledNote, delivery: &str) {
    if envelope.note.kind != WORK_RESULT_KIND {
        return;
    }
    let Some(work_id) = envelope
        .note
        .refs
        .get("result")
        .and_then(|result| result.get("work_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
    else {
        return;
    };
    let home = sender.clone();
    let delivery = delivery.to_string();
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            let _ = record_result_delivery_ack_async(home, work_id, delivery).await;
        });
    } else if let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
        let _ = rt.block_on(record_result_delivery_ack_async(home, work_id, delivery));
    }
}

async fn record_result_delivery_ack_async(
    home: A2aHome,
    work_id: String,
    delivery: String,
) -> Result<(), AppError> {
    let registry_url = registry::read_registry_env(&home)?;
    let registry = registry::open_registry(
        &registry_url,
        registry::reader_roots(&home)?,
        keys::fleet_id(&home)?,
    )?
    .with_reader_state(RowMarkStore::open(&home)?, RowVerifyCfg::new(Utc::now()));
    let fleet_id = keys::fleet_id(&home)?;
    let Some(row) = registry.work_get(&fleet_id, &work_id).await? else {
        return Ok(());
    };
    let _ = registry
        .work_transition(
            &fleet_id,
            &work_id,
            row.status,
            row.status,
            WorkOwner::Any,
            WorkPatch {
                result_delivery: Some(Some(delivery)),
                ..WorkPatch::default()
            },
        )
        .await?;
    Ok(())
}

pub fn receive_note(
    home: &A2aHome,
    cfg: &Config,
    envelope: &SpooledNote,
) -> Result<NoteAck, AppError> {
    let Some(note_id) = envelope.note.note_id.as_deref().filter(|s| !s.is_empty()) else {
        log_note_action(
            home,
            &envelope.from_gateway,
            &envelope.to_gateway,
            &envelope.to_peer,
            &envelope.trace_id,
            "ingress_refused",
        );
        return Ok(NoteAck::Rejected {
            code: "ingress_refused".to_string(),
        });
    };
    if delivered_contains(home, &envelope.from_gateway, note_id)? {
        log_note_action(
            home,
            &envelope.from_gateway,
            &envelope.to_gateway,
            &envelope.to_peer,
            &envelope.trace_id,
            "duplicate",
        );
        return Ok(NoteAck::Duplicate);
    }
    if envelope.note.kind == WORK_OFFER_KIND {
        return receive_work_offer(home, cfg, envelope, note_id);
    }
    if validate_note_ingress(cfg, &envelope.note).is_err() {
        log_note_action(
            home,
            &envelope.from_gateway,
            &envelope.to_gateway,
            &envelope.to_peer,
            &envelope.trace_id,
            "ingress_refused",
        );
        return Ok(NoteAck::Rejected {
            code: "ingress_refused".to_string(),
        });
    }
    if envelope.note.kind == WORK_CANCELLED_KIND {
        let work_id = envelope
            .note
            .refs
            .get("work_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                AppError::new(
                    "WORK_SCHEMA_MISSING",
                    ExitCode::Precondition,
                    "work_cancelled note refs must contain work_id",
                )
            })?;
        work_store::validate_work_id(work_id)?;
        work_store::mark_terminal_observed(home, work_id, WorkStatus::Cancelled)?;
    }
    let peer = PeerId::parse(&envelope.to_peer)?;
    write_note_file_with_id(home, &peer, &envelope.note)?;
    remember_delivered(home, &envelope.from_gateway, note_id)?;
    log_note_action(
        home,
        &envelope.from_gateway,
        &envelope.to_gateway,
        &envelope.to_peer,
        &envelope.trace_id,
        "delivered",
    );
    Ok(NoteAck::Delivered)
}

fn receive_work_offer(
    home: &A2aHome,
    cfg: &Config,
    envelope: &SpooledNote,
    note_id: &str,
) -> Result<NoteAck, AppError> {
    let result = validate_note_ingress(cfg, &envelope.note)
        .and_then(|_| validate_gateway_addressee(home, &envelope.note.to))
        .and_then(|_| feed::validate_refs_size(&envelope.note.refs, cfg.mesh.work.max_directed_bytes))
        .and_then(|_| work_envelope_from_refs(&envelope.note.refs))
        .and_then(|work| {
            work_store::validate_work_id(&work.work_id)?;
            crate::mesh::work::validate_envelope_hygiene(
                &work,
                cfg.mesh.work.max_directed_bytes as usize,
            )?;
            verify_envelope(home, &work, &SystemClock)
        })
        .and_then(|verified| {
            if let Some(warning) = corroborate_sender(
                home,
                &verified,
                CorroborationStage::Ingest,
                cfg.mesh.gateway.require_signed_rows,
            )? {
                log_work_ingest_warning(
                    home,
                    &envelope.from_gateway,
                    &envelope.to_gateway,
                    &envelope.to_peer,
                    &envelope.trace_id,
                    &warning,
                );
            }
            work_store::store_offer(home, verified, WorkSource::Directed)
        });
    match result {
        Ok(_) => {
            remember_delivered(home, &envelope.from_gateway, note_id)?;
            log_note_action(
                home,
                &envelope.from_gateway,
                &envelope.to_gateway,
                &envelope.to_peer,
                &envelope.trace_id,
                "delivered",
            );
            Ok(NoteAck::Delivered)
        }
        Err(err) => {
            log_work_ingest_refused(
                home,
                &envelope.from_gateway,
                &envelope.to_gateway,
                &envelope.to_peer,
                &envelope.trace_id,
                err.code,
            );
            Ok(NoteAck::Rejected {
                code: err.code.to_string(),
            })
        }
    }
}

pub(crate) fn write_note_file_with_id(
    home: &A2aHome,
    peer: &PeerId,
    note: &Note,
) -> Result<PathBuf, AppError> {
    let inbox = peer_dir(home, peer).join("inbox");
    create_dir_0700(&inbox.join("read")).map_err(|e| io_err("creating peer inbox", e))?;
    let seq = next_seq(&inbox);
    let note_part = note
        .note_id
        .as_deref()
        .map(safe_file_part)
        .filter(|s| !s.is_empty());
    let path = match note_part {
        Some(note_id) => inbox.join(format!(
            "{seq:06}-{}-{note_id}-{}.note.json",
            Utc::now().timestamp_millis().max(0),
            rand8_hex()
        )),
        None => inbox.join(format!(
            "{seq:06}-{}-{}.note.json",
            Utc::now().timestamp_millis().max(0),
            rand8_hex()
        )),
    };
    let bytes = serde_json::to_vec_pretty(note).map_err(|e| data_err("serializing note", e))?;
    let mut f = write_new_0600(&path).map_err(|e| io_err("writing note", e))?;
    f.write_all(&bytes).map_err(|e| io_err("writing note", e))?;
    f.write_all(b"\n").map_err(|e| io_err("writing note", e))?;
    f.sync_all().map_err(|e| io_err("syncing note", e))?;
    Ok(path)
}

pub(crate) fn mint_note_id() -> String {
    ulid::Ulid::new().to_string()
}

fn validate_note_ingress(cfg: &Config, note: &Note) -> Result<(), AppError> {
    scan_or_refuse(&note.text, "SECRET_IN_NOTE")?;
    let len = note.text.len();
    if len > NOTE_MAX_BYTES {
        return Err(AppError::new(
            "NOTE_TOO_LARGE",
            ExitCode::Precondition,
            format!("text is {len} bytes, exceeding cap {NOTE_MAX_BYTES} bytes"),
        ));
    }
    feed::validate_refs_size(&note.refs, cfg.mesh.work.max_directed_bytes)?;
    Ok(())
}

fn validate_gateway_addressee(home: &A2aHome, to: &str) -> Result<(), AppError> {
    let Some(gateway_id) = to.strip_prefix("gateway:") else {
        return Err(AppError::new(
            "WORK_TOKEN_MISMATCH",
            ExitCode::Precondition,
            "work_offer notes must be addressed to gateway:<gateway_id>",
        ));
    };
    let local_gateway = crate::mesh::keys::gateway_id(home)?;
    if gateway_id != local_gateway {
        return Err(AppError::new(
            "WORK_TOKEN_MISMATCH",
            ExitCode::Precondition,
            format!("work_offer targets gateway {gateway_id}, local gateway is {local_gateway}"),
        ));
    }
    Ok(())
}

fn work_envelope_from_refs(refs: &serde_json::Value) -> Result<WorkEnvelope, AppError> {
    let Some(work) = refs.get("work") else {
        return Err(AppError::new(
            "WORK_SCHEMA_MISSING",
            ExitCode::Precondition,
            "work_offer note refs must contain refs.work",
        ));
    };
    serde_json::from_value(work.clone()).map_err(|e| {
        AppError::new(
            "WORK_SCHEMA_MISSING",
            ExitCode::Precondition,
            format!("decoding refs.work: {e}"),
        )
    })
}

fn schedule_retry<R: Rng + ?Sized>(
    cfg: &Config,
    path: &Path,
    envelope: &mut SpooledNote,
    now: DateTime<Utc>,
    rng: &mut R,
) -> Result<(), AppError> {
    envelope.attempts = envelope.attempts.saturating_add(1);
    let delay = jittered_retry_backoff(
        envelope.attempts,
        Duration::from_secs(cfg.mesh.gateway.outbound_retry_max_secs),
        rng,
    );
    envelope.next_attempt_after =
        Some(now + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::MAX));
    write_envelope_atomic(path, envelope)
}

pub fn jittered_retry_backoff<R: Rng + ?Sized>(
    attempt: u32,
    cap: Duration,
    rng: &mut R,
) -> Duration {
    let base_ms = 1_000u128;
    let factor = 2u128.saturating_pow(attempt.saturating_sub(1).min(32));
    let cap_ms = cap.as_millis().max(1);
    let upper = base_ms.saturating_mul(factor).min(cap_ms).max(1);
    Duration::from_millis(rng.gen_range(0..=upper) as u64)
}

fn delivered_contains(home: &A2aHome, origin: &str, note_id: &str) -> Result<bool, AppError> {
    Ok(load_dedup(home)?
        .origins
        .get(origin)
        .map(|ids| ids.iter().any(|id| id == note_id))
        .unwrap_or(false))
}

fn remember_delivered(home: &A2aHome, origin: &str, note_id: &str) -> Result<(), AppError> {
    let mut dedup = load_dedup(home)?;
    let ids = dedup.origins.entry(origin.to_string()).or_default();
    if !ids.iter().any(|id| id == note_id) {
        ids.push(note_id.to_string());
        if ids.len() > DEDUP_CAP_PER_ORIGIN {
            let drain = ids.len() - DEDUP_CAP_PER_ORIGIN;
            ids.drain(0..drain);
        }
    }
    let path = dedup_path(home);
    if let Some(parent) = path.parent() {
        create_dir_0700(parent).map_err(|e| io_err("creating note dedup dir", e))?;
    }
    let bytes =
        serde_json::to_vec_pretty(&dedup).map_err(|e| data_err("serializing note dedup", e))?;
    crate::store::write_atomic(&path, &bytes).map_err(|e| io_err("writing note dedup", e))
}

fn load_dedup(home: &A2aHome) -> Result<DedupFile, AppError> {
    match fs::read(dedup_path(home)) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| data_err("decoding note dedup", e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(DedupFile::default()),
        Err(e) => Err(io_err("reading note dedup", e)),
    }
}

fn archive(home: &A2aHome, path: &Path, bucket: &str, gateway: &str) -> Result<(), AppError> {
    let dir = outbound_root(home).join(bucket).join(gateway);
    create_dir_0700(&dir).map_err(|e| io_err("creating note archive", e))?;
    let name = path.file_name().ok_or_else(|| {
        AppError::new(
            "NOTE_SPOOL",
            ExitCode::Internal,
            "spooled note path has no filename",
        )
    })?;
    let dest = dir.join(name);
    if dest.exists() {
        fs::remove_file(path).map_err(|e| io_err("removing duplicate archived note", e))?;
    } else {
        fs::rename(path, dest).map_err(|e| io_err("archiving note", e))?;
    }
    Ok(())
}

fn note_paths(dir: &Path) -> Result<Vec<PathBuf>, AppError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_err("reading outbound notes", e)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| io_err("reading outbound notes", e))?;
        if !entry
            .file_type()
            .map_err(|e| io_err("reading outbound notes", e))?
            .is_file()
        {
            continue;
        }
        if entry.file_name().to_string_lossy().ends_with(".note.json") {
            out.push(entry.path());
        }
    }
    Ok(out)
}

fn find_spooled_by_trace(
    home: &A2aHome,
    to_gateway: &str,
    trace_id: &str,
) -> Result<Option<PathBuf>, AppError> {
    for path in note_paths(&outbound_dir(home, to_gateway))? {
        let envelope = read_spooled(&path)?;
        if envelope.trace_id == trace_id || envelope.note.note_id.as_deref() == Some(trace_id) {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

fn write_envelope_new(path: &Path, envelope: &SpooledNote) -> Result<(), AppError> {
    let bytes =
        serde_json::to_vec_pretty(envelope).map_err(|e| data_err("serializing spooled note", e))?;
    let mut f = write_new_0600(path).map_err(|e| io_err("writing spooled note", e))?;
    f.write_all(&bytes)
        .map_err(|e| io_err("writing spooled note", e))?;
    f.write_all(b"\n")
        .map_err(|e| io_err("writing spooled note", e))?;
    f.sync_all().map_err(|e| io_err("syncing spooled note", e))
}

fn write_envelope_atomic(path: &Path, envelope: &SpooledNote) -> Result<(), AppError> {
    let bytes =
        serde_json::to_vec_pretty(envelope).map_err(|e| data_err("serializing spooled note", e))?;
    crate::store::write_atomic(path, &bytes).map_err(|e| io_err("writing spooled note", e))
}

fn outbound_root(home: &A2aHome) -> PathBuf {
    crate::mesh::mesh_root(home).join("outbound")
}

fn outbound_dir(home: &A2aHome, gateway: &str) -> PathBuf {
    outbound_root(home).join(gateway)
}

fn dedup_path(home: &A2aHome) -> PathBuf {
    home.root().join("mesh").join("gateway").join(DEDUP_FILE)
}

fn next_seq(inbox: &Path) -> u64 {
    [inbox.to_path_buf(), inbox.join("read")]
        .into_iter()
        .filter_map(|dir| fs::read_dir(dir).ok())
        .flat_map(|entries| entries.flatten())
        .filter_map(|e| seq_of(&e.file_name().to_string_lossy()))
        .max()
        .unwrap_or(0)
        + 1
}

fn seq_of(name: &str) -> Option<u64> {
    name.split('-').next()?.parse::<u64>().ok()
}

fn log_note_action(
    home: &A2aHome,
    from_gateway: &str,
    to_gateway: &str,
    to_peer: &str,
    trace_id: &str,
    action: &str,
) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut f) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "note",
            "trace_id": trace_id,
            "from_gateway": from_gateway,
            "to_gateway": to_gateway,
            "to_peer": to_peer,
            "action": action,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = f.write_all(&bytes);
        }
    }
}

fn log_work_ingest_refused(
    home: &A2aHome,
    from_gateway: &str,
    to_gateway: &str,
    to_peer: &str,
    trace_id: &str,
    code: &str,
) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut f) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "note",
            "trace_id": trace_id,
            "from_gateway": from_gateway,
            "to_gateway": to_gateway,
            "to_peer": to_peer,
            "action": "work_ingest_refused",
            "code": code,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = f.write_all(&bytes);
        }
    }
}

fn log_work_ingest_warning(
    home: &A2aHome,
    from_gateway: &str,
    to_gateway: &str,
    to_peer: &str,
    trace_id: &str,
    warning: &str,
) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut f) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "note",
            "trace_id": trace_id,
            "from_gateway": from_gateway,
            "to_gateway": to_gateway,
            "to_peer": to_peer,
            "action": "work_ingest_warning",
            "warning": warning,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = f.write_all(&bytes);
        }
    }
}

fn safe_file_part(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect()
}

fn rand8_hex() -> String {
    format!("{:08x}", rand::thread_rng().gen::<u32>())
}

fn io_err(context: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}

fn data_err(context: &'static str, e: serde_json::Error) -> AppError {
    AppError::new("DATA", ExitCode::Internal, format!("{context}: {e}"))
}
