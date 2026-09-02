use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::mesh::work::VerifiedEnvelope;
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const WORK_OFFER_KIND: &str = "work_offer";
pub const WORK_RESULT_KIND: &str = "work_result";
pub const WORK_CANCELLED_KIND: &str = "work_cancelled";

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
    for source in [WorkSource::Directed, WorkSource::Pool] {
        let path = path_for(home, source, work_id);
        if !path.exists() {
            continue;
        }
        return read_file(&path).map(Some);
    }
    Ok(None)
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
    dir_for(home, source).join(format!("{}.json", safe_file_part(work_id)))
}

fn dir_for(home: &A2aHome, source: WorkSource) -> PathBuf {
    let bucket = match source {
        WorkSource::Directed => "inbox",
        WorkSource::Pool => "pool",
    };
    home.root().join("work").join(bucket)
}

fn safe_file_part(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect()
}

fn io_err(context: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}
