use crate::config::Config;
use crate::home::A2aHome;
use crate::inbox::{self, MsgKind};
use crate::mesh::{notes, scan_or_refuse, PeerId, NOTE_MAX_BYTES};
use crate::output::{AppError, ExitCode};
use crate::state::WorkerSnapshot;
use serde::Deserialize;
use serde_json::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq)]
pub struct OutboxNote {
    pub path: PathBuf,
    pub note_to: String,
    pub text: String,
    pub refs: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Routed {
    Delivered(PathBuf),
    Rejected(String),
}

#[derive(Deserialize)]
struct RawOutboxNote {
    to: String,
    text: String,
    #[serde(default = "empty_refs")]
    refs: Value,
}

pub fn scan_outbox(worker_dir: &Path) -> io::Result<Vec<OutboxNote>> {
    let entries = match fs::read_dir(worker_dir.join("outbox")) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".note.json") {
            continue;
        }
        let bytes = fs::read(entry.path())?;
        let raw: RawOutboxNote = serde_json::from_slice(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        out.push(OutboxNote {
            path: entry.path(),
            note_to: raw.to,
            text: raw.text,
            refs: raw.refs,
        });
    }
    out.sort_by(|a, b| a.path.file_name().cmp(&b.path.file_name()));
    Ok(out)
}

pub fn has_unrouted_note(worker_dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(worker_dir.join("outbox")) else {
        return false;
    };
    entries.flatten().any(|entry| {
        entry.file_type().map(|ty| ty.is_file()).unwrap_or(false)
            && entry.file_name().to_string_lossy().ends_with(".note.json")
    })
}

pub fn route(
    home: &A2aHome,
    cfg: &Config,
    from_worker: &WorkerSnapshot,
    n: &OutboxNote,
) -> Result<Routed, AppError> {
    if let Err(e) = gate_text(&n.text) {
        mark(&n.path, "rejected").map_err(|err| io_err("marking note rejected", err))?;
        return Ok(Routed::Rejected(e.code.to_string()));
    }

    let session = from_worker.session_id.as_deref().ok_or_else(|| {
        AppError::new(
            "MESH_WORKER_NO_SESSION",
            ExitCode::Precondition,
            format!("worker {} has no recorded codex session id", from_worker.id),
        )
    })?;
    let from = PeerId::parse(&format!("codex:{session}"))?;
    let target = notes::resolve_target(home, cfg, &n.note_to)?;
    let peer = notes::target_peer(&target);
    let note = crate::mesh::Note {
        note_id: Some(crate::mesh::gateway::notes_wire::mint_note_id()),
        from: from.to_string(),
        to: peer.to_string(),
        ts: chrono::Utc::now(),
        kind: "note".to_string(),
        text: n.text.clone(),
        refs: n.refs.clone(),
        extra: serde_json::Map::new(),
    };
    let delivered = notes::write_note_file(home, peer, &note)?;

    if let Some((worker_id, snap)) = notes::routable_worker(home, &target, peer)? {
        if !snap.state.is_terminal() {
            inbox::queue_message(
                &home.worker_dir(&worker_id),
                &from.to_string(),
                MsgKind::Steer,
                &n.text,
            )
            .map_err(|e| io_err("queueing mesh note to worker inbox", e))?;
        }
    }
    mark(&n.path, "routed").map_err(|err| io_err("marking note routed", err))?;
    Ok(Routed::Delivered(delivered))
}

fn gate_text(text: &str) -> Result<(), AppError> {
    scan_or_refuse(text, "SECRET_IN_NOTE")?;
    let len = text.len();
    if len > NOTE_MAX_BYTES {
        return Err(AppError::new(
            "NOTE_TOO_LARGE",
            ExitCode::Precondition,
            format!("text is {len} bytes, exceeding cap {NOTE_MAX_BYTES} bytes"),
        ));
    }
    Ok(())
}

fn mark(path: &Path, fate: &str) -> io::Result<()> {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "note path has no file name",
        ));
    };
    let next = if let Some(prefix) = name.strip_suffix(".note.json") {
        format!("{prefix}.note.{fate}.json")
    } else {
        format!("{name}.{fate}")
    };
    fs::rename(path, path.with_file_name(next))
}

fn empty_refs() -> Value {
    Value::Object(serde_json::Map::new())
}

fn io_err(context: &'static str, e: io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}
