use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::mesh::registry::WorkStatus;
use crate::mesh::work::VerifiedEnvelope;
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const WORK_OFFER_KIND: &str = "work_offer";
pub const WORK_RESULT_KIND: &str = "work_result";
pub const WORK_CANCELLED_KIND: &str = "work_cancelled";
pub const WORK_HELD_KIND: &str = "work_held";

const RESERVED_WORK_IDS: [&str; 4] = ["inbox", "pool", "acked", "terminal"];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkSource {
    Directed,
    Pool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredWork {
    pub work_id: String,
    pub source: WorkSource,
    pub received_at: DateTime<Utc>,
    pub verified: VerifiedEnvelope,
    #[serde(skip)]
    pub path: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct StoredWorkFile {
    work_id: String,
    source: WorkSource,
    received_at: DateTime<Utc>,
    verified: VerifiedEnvelope,
}

#[derive(Serialize, Deserialize)]
struct TerminalObservedFile {
    work_id: String,
    status: WorkStatus,
    observed_at: DateTime<Utc>,
}

pub fn validate_work_id(work_id: &str) -> Result<(), AppError> {
    let reserved = RESERVED_WORK_IDS
        .iter()
        .any(|reserved| work_id.eq_ignore_ascii_case(reserved));
    if reserved || work_id.len() != 26 || ulid::Ulid::from_string(work_id).is_err() {
        return Err(AppError::new(
            "WORK_ID_INVALID",
            ExitCode::Precondition,
            format!("work id {work_id:?} is not a 26-character Crockford ULID"),
        )
        .remedy("use the generated work id from `a2a mesh work send` or `offer`"));
    }
    Ok(())
}

pub fn store_offer(
    home: &A2aHome,
    verified: VerifiedEnvelope,
    source: WorkSource,
) -> Result<PathBuf, AppError> {
    let work_id = verified.envelope.work_id.clone();
    let path = path_for(home, source, &work_id);
    if let Some(parent) = path.parent() {
        create_dir_0700(parent).map_err(|e| io_err("creating work store", e))?;
    }
    let stored = StoredWorkFile {
        work_id,
        source,
        received_at: Utc::now(),
        verified,
    };
    let bytes = serde_json::to_vec_pretty(&stored)
        .map_err(|e| AppError::new("DATA", ExitCode::Internal, format!("serializing work: {e}")))?;
    let mut file = write_new_0600(&path).map_err(|e| io_err("writing work", e))?;
    file.write_all(&bytes)
        .map_err(|e| io_err("writing work", e))?;
    file.write_all(b"\n")
        .map_err(|e| io_err("writing work", e))?;
    file.sync_all().map_err(|e| io_err("syncing work", e))?;
    Ok(path)
}

pub fn list(home: &A2aHome) -> Result<Vec<StoredWork>, AppError> {
    let mut out = Vec::new();
    for source in [WorkSource::Directed, WorkSource::Pool] {
        out.extend(read_dir(home, source)?);
    }
    out.sort_by(|a, b| {
        a.received_at
            .cmp(&b.received_at)
            .then_with(|| a.work_id.cmp(&b.work_id))
    });
    Ok(out)
}

pub fn load(home: &A2aHome, work_id: &str) -> Result<Option<StoredWork>, AppError> {
    let mut found = None;
    for source in [WorkSource::Directed, WorkSource::Pool] {
        let path = path_for(home, source, work_id);
        if !path.exists() {
            continue;
        }
        let stored = read_file(&path)?;
        if stored.work_id != work_id || stored.verified.envelope.work_id != work_id {
            return Err(AppError::new(
                "WORK_STATE_CONFLICT",
                ExitCode::Precondition,
                format!(
                    "work store entry {} does not contain requested work id {work_id}",
                    path.display()
                ),
            )
            .remedy("remove the mismatched local work entry and ask the sender to re-send it"));
        }
        if found.is_some() {
            return Err(AppError::new(
                "WORK_STATE_CONFLICT",
                ExitCode::Precondition,
                format!("work id {work_id} exists in both inbox and pool buckets"),
            )
            .remedy("keep exactly one verified local work entry before approving it"));
        }
        found = Some(stored);
    }
    Ok(found)
}

pub fn remove(home: &A2aHome, work_id: &str) -> Result<bool, AppError> {
    let mut removed = false;
    for source in [WorkSource::Directed, WorkSource::Pool] {
        let path = path_for(home, source, work_id);
        match fs::remove_file(&path) {
            Ok(()) => removed = true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("removing work", e)),
        }
    }
    Ok(removed)
}

fn read_dir(home: &A2aHome, source: WorkSource) -> Result<Vec<StoredWork>, AppError> {
    let dir = dir_for(home, source);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_err("reading work store", e)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| io_err("reading work store", e))?;
        if !entry
            .file_type()
            .map_err(|e| io_err("reading work store", e))?
            .is_file()
        {
            continue;
        }
        if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        out.push(read_file(&entry.path())?);
    }
    Ok(out)
}

fn read_file(path: &Path) -> Result<StoredWork, AppError> {
    let bytes = fs::read(path).map_err(|e| io_err("reading work", e))?;
    let stored: StoredWorkFile = serde_json::from_slice(&bytes)
        .map_err(|e| AppError::new("DATA", ExitCode::Internal, format!("decoding work: {e}")))?;
    Ok(StoredWork {
        work_id: stored.work_id,
        source: stored.source,
        received_at: stored.received_at,
        verified: stored.verified,
        path: path.to_path_buf(),
    })
}

fn path_for(home: &A2aHome, source: WorkSource, work_id: &str) -> PathBuf {
    dir_for(home, source).join(format!("{work_id}.json"))
}

fn dir_for(home: &A2aHome, source: WorkSource) -> PathBuf {
    let bucket = match source {
        WorkSource::Directed => "inbox",
        WorkSource::Pool => "pool",
    };
    home.root().join("work").join(bucket)
}

pub fn mark_terminal_observed(
    home: &A2aHome,
    work_id: &str,
    status: WorkStatus,
) -> Result<DateTime<Utc>, AppError> {
    validate_work_id(work_id)?;
    if let Some(observed_at) = terminal_observed(home, work_id) {
        return Ok(observed_at);
    }
    let dir = home.root().join("work").join("terminal");
    create_dir_0700(&dir).map_err(|e| io_err("creating terminal work marker dir", e))?;
    let observed_at = truncate_to_micros(Utc::now());
    let marker = TerminalObservedFile {
        work_id: work_id.to_string(),
        status,
        observed_at,
    };
    let mut bytes = serde_json::to_vec_pretty(&marker).map_err(|e| {
        AppError::new(
            "DATA",
            ExitCode::Internal,
            format!("serializing terminal work marker: {e}"),
        )
    })?;
    bytes.push(b'\n');
    crate::store::write_atomic(&terminal_path(home, work_id), &bytes)
        .map_err(|e| io_err("writing terminal work marker", e))?;
    Ok(observed_at)
}

pub fn terminal_observed(home: &A2aHome, work_id: &str) -> Option<DateTime<Utc>> {
    validate_work_id(work_id).ok()?;
    let bytes = fs::read(terminal_path(home, work_id)).ok()?;
    let marker: TerminalObservedFile = serde_json::from_slice(&bytes).ok()?;
    (marker.work_id == work_id).then_some(marker.observed_at)
}

fn terminal_path(home: &A2aHome, work_id: &str) -> PathBuf {
    home.root()
        .join("work")
        .join("terminal")
        .join(format!("{work_id}.json"))
}

fn truncate_to_micros(value: DateTime<Utc>) -> DateTime<Utc> {
    value
        .with_nanosecond(value.nanosecond() / 1_000 * 1_000)
        .unwrap_or(value)
}

fn io_err(context: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}
