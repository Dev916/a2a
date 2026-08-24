use crate::commands::send;
use crate::config::Config;
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::ids;
use crate::inbox::MsgKind;
use crate::mesh::{mesh_root, peer_dir, presence, scan_or_refuse, Note, PeerId, Provider, NOTE_MAX_BYTES};
use crate::output::{AppError, ExitCode};
use crate::state::{load_snapshot, WorkerSnapshot};
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub enum Target {
    Peer(PeerId),
    Worker { worker_id: String, peer: PeerId },
}

#[derive(Clone, Debug, Serialize)]
pub struct RosterRow {
    pub peer: String,
    pub provider: String,
    pub worker_id: Option<String>,
    pub label: presence::Label,
    pub status: Option<String>,
    pub working_on: Option<String>,
    pub unread_notes: usize,
}

pub fn resolve_target(home: &A2aHome, _cfg: &Config, s: &str) -> Result<Target, AppError> {
    if let Ok(peer) = PeerId::parse(s) {
        return Ok(Target::Peer(peer));
    }
    if let Some(peer) = resolve_peer_prefix(home, s)? {
        return Ok(Target::Peer(peer));
    }
    let worker_id = ids::resolve(home, s)?;
    let snap = load_snapshot(&home.worker_dir(&worker_id)).map_err(AppError::from)?;
    let session = snap.session_id.ok_or_else(|| {
        AppError::new(
            "MESH_WORKER_NO_SESSION",
            ExitCode::Precondition,
            format!("worker {worker_id} has no recorded codex session id"),
        )
        .remedy("wait for the worker's session to be identified — a2a status <id>")
    })?;
    let peer = PeerId::parse(&format!("codex:{session}"))?;
    Ok(Target::Worker { worker_id, peer })
}

pub fn write_note(
    home: &A2aHome,
    _cfg: &Config,
    from: &PeerId,
    to: &Target,
    text: &str,
    refs: Value,
) -> Result<PathBuf, AppError> {
    scan_or_refuse(text, "SECRET_IN_NOTE")?;
    let len = text.len();
    if len > NOTE_MAX_BYTES {
        return Err(AppError::new(
            "NOTE_TOO_LARGE",
            ExitCode::Precondition,
            format!("text is {len} bytes, exceeding cap {NOTE_MAX_BYTES} bytes"),
        ));
    }

    let peer = target_peer(to);
    let note = Note {
        from: from.to_string(),
        to: peer.to_string(),
        ts: Utc::now(),
        kind: "note".to_string(),
        text: text.to_string(),
        refs,
        extra: serde_json::Map::new(),
    };
    let path = write_note_file(home, peer, &note)?;

    if let Some((worker_id, snap)) = routable_worker(home, to, peer)? {
        if !snap.state.is_terminal() {
            let _ = send::run(
                home,
                &worker_id,
                text,
                &from.to_string(),
                MsgKind::Steer,
                false,
                true,
                false,
                true,
            )?;
        }
    }
    Ok(path)
}

pub fn unread(home: &A2aHome, id: &PeerId) -> Result<Vec<(PathBuf, Note)>, AppError> {
    let inbox = peer_dir(home, id).join("inbox");
    let entries = match fs::read_dir(&inbox) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_err("reading peer inbox", e)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| io_err("reading peer inbox", e))?;
        if !entry.file_type().map_err(|e| io_err("reading peer inbox", e))?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if seq_of(&name).is_none() || !name.ends_with(".note.json") {
            continue;
        }
        let bytes = fs::read(entry.path()).map_err(|e| io_err("reading note", e))?;
        let note = serde_json::from_slice(&bytes).map_err(|e| data_err("decoding note", e))?;
        out.push((entry.path(), note));
    }
    out.sort_by(|a, b| a.0.file_name().cmp(&b.0.file_name()));
    Ok(out)
}

pub fn consume(home: &A2aHome, id: &PeerId, files: &[PathBuf]) -> Result<(), AppError> {
    if files.is_empty() {
        return Ok(());
    }
    let inbox = peer_dir(home, id).join("inbox");
    let read = inbox.join("read");
    create_dir_0700(&read).map_err(|e| io_err("creating inbox/read", e))?;
    let mut last = None;
    for file in files {
        let Some(name) = file.file_name().map(|n| n.to_owned()) else {
            continue;
        };
        fs::rename(file, read.join(&name)).map_err(|e| io_err("moving note to inbox/read", e))?;
        last = Some(name.to_string_lossy().into_owned());
    }
    let Some(last) = last else {
        return Ok(());
    };
    let now = Utc::now();
    let mut p = match presence::load(home, id)? {
        Some(p) => p,
        None => presence::new_presence(home, id, now)?,
    };
    p.cursors.inbox = Some(last);
    presence::save(home, &p)
}

pub fn roster(home: &A2aHome, cfg: &Config, now: DateTime<Utc>) -> Result<Vec<RosterRow>, AppError> {
    let mut rows = BTreeMap::new();

    for id in peer_ids(home)? {
        let p = presence::load(home, &id)?;
        let peer = id.to_string();
        let (worker_id, label, status, working_on) = match &p {
            Some(p) => (
                p.worker_id.clone(),
                presence::staleness(p.last_seen, p.retired, now, cfg.mesh.stale_after_secs),
                p.status.clone(),
                p.working_on.clone(),
            ),
            None => (None, presence::Label::Stale, None, None),
        };
        rows.insert(
            peer.clone(),
            RosterRow {
                peer,
                provider: id.provider.to_string(),
                worker_id,
                label,
                status,
                working_on,
                unread_notes: unread(home, &id)?.len(),
            },
        );
    }

    for (worker_id, snap) in live_workers(home)? {
        let peer = match &snap.session_id {
            Some(session) => format!("codex:{session}"),
            None => format!("codex:pending-{worker_id}"),
        };
        let id = snap
            .session_id
            .as_ref()
            .and_then(|session| PeerId::parse(&format!("codex:{session}")).ok());
        let unread_notes = match &id {
            Some(id) => unread(home, id)?.len(),
            None => 0,
        };
        let existing = rows.remove(&peer);
        let status = existing.as_ref().and_then(|r| r.status.clone());
        let unread_notes = existing.map(|r| r.unread_notes).unwrap_or(unread_notes);
        rows.insert(
            peer.clone(),
            RosterRow {
                peer,
                provider: "codex".to_string(),
                worker_id: Some(worker_id.clone()),
                label: presence::Label::Active,
                status,
                working_on: Some(format!("{}: {}", snap.lane.as_str(), brief_title(home, &worker_id, &snap))),
                unread_notes,
            },
        );
    }

    Ok(rows.into_values().collect())
}

pub(crate) fn target_peer(target: &Target) -> &PeerId {
    match target {
        Target::Peer(peer) => peer,
        Target::Worker { peer, .. } => peer,
    }
}

pub(crate) fn target_warning(home: &A2aHome, target: &Target) -> Result<Option<&'static str>, AppError> {
    let peer = target_peer(target);
    let presence = presence::load(home, peer)?;
    if let Some(p) = &presence {
        if p.retired {
            return Ok(Some("retired"));
        }
    }
    if let Some((_worker_id, snap)) = routable_worker(home, target, peer)? {
        if snap.state.is_terminal() {
            return Ok(Some("terminal"));
        }
        return Ok(None);
    }
    if presence.is_none() && matches!(target, Target::Peer(_)) {
        return Ok(Some("unknown"));
    }
    Ok(None)
}

fn resolve_peer_prefix(home: &A2aHome, s: &str) -> Result<Option<PeerId>, AppError> {
    if s.contains(':') || s.contains('/') || s.trim().is_empty() {
        return Ok(None);
    }
    let mut matches = Vec::new();
    for id in peer_ids(home)? {
        if id.uuid == s || id.uuid.starts_with(s) {
            matches.push(id);
        }
    }
    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.into_iter().next()),
        _ => {
            let candidates = matches.iter().map(ToString::to_string).collect::<Vec<_>>();
            Err(AppError::new(
                "AMBIGUOUS_PEER",
                ExitCode::NoSuchWorker,
                format!("'{s}' is ambiguous: {}", candidates.join(", ")),
            )
            .data(json!({ "candidates": candidates })))
        }
    }
}

fn write_note_file(home: &A2aHome, peer: &PeerId, note: &Note) -> Result<PathBuf, AppError> {
    let inbox = peer_dir(home, peer).join("inbox");
    create_dir_0700(&inbox.join("read")).map_err(|e| io_err("creating peer inbox", e))?;
    let seq = next_seq(&inbox);
    let path = inbox.join(format!(
        "{seq:06}-{}-{}.note.json",
        Utc::now().timestamp_millis().max(0),
        rand8_hex()
    ));
    let bytes = serde_json::to_vec_pretty(note).map_err(|e| data_err("serializing note", e))?;
    let mut f = write_new_0600(&path).map_err(|e| io_err("writing note", e))?;
    f.write_all(&bytes).map_err(|e| io_err("writing note", e))?;
    f.write_all(b"\n").map_err(|e| io_err("writing note", e))?;
    f.sync_all().map_err(|e| io_err("syncing note", e))?;
    Ok(path)
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

fn routable_worker(
    home: &A2aHome,
    target: &Target,
    peer: &PeerId,
) -> Result<Option<(String, WorkerSnapshot)>, AppError> {
    match target {
        Target::Worker { worker_id, .. } => {
            let snap = load_snapshot(&home.worker_dir(worker_id)).map_err(AppError::from)?;
            Ok(Some((worker_id.clone(), snap)))
        }
        Target::Peer(_) if peer.provider == Provider::Codex => worker_for_peer(home, peer),
        Target::Peer(_) => Ok(None),
    }
}

fn worker_for_peer(home: &A2aHome, peer: &PeerId) -> Result<Option<(String, WorkerSnapshot)>, AppError> {
    for id in home
        .list_worker_ids()
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("listing workers: {e}")))?
    {
        let snap = match load_snapshot(&home.worker_dir(&id)) {
            Ok(snap) => snap,
            Err(_) => continue,
        };
        if snap.session_id.as_deref() == Some(peer.uuid.as_str()) {
            return Ok(Some((id, snap)));
        }
    }
    Ok(None)
}

fn peer_ids(home: &A2aHome) -> Result<Vec<PeerId>, AppError> {
    let peers = mesh_root(home).join("peers");
    let entries = match fs::read_dir(&peers) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_err("reading mesh peers", e)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| io_err("reading mesh peers", e))?;
        if !entry.file_type().map_err(|e| io_err("reading mesh peers", e))?.is_dir() {
            continue;
        }
        let Some(id) = peer_from_dir_name(&entry.file_name().to_string_lossy()) else {
            continue;
        };
        out.push(id);
    }
    out.sort_by_key(ToString::to_string);
    Ok(out)
}

fn peer_from_dir_name(name: &str) -> Option<PeerId> {
    let (provider, uuid) = name.split_once('-')?;
    PeerId::parse(&format!("{provider}:{uuid}")).ok()
}

fn live_workers(home: &A2aHome) -> Result<Vec<(String, WorkerSnapshot)>, AppError> {
    let mut out = Vec::new();
    for id in home
        .list_worker_ids()
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("listing workers: {e}")))?
    {
        let snap = match load_snapshot(&home.worker_dir(&id)) {
            Ok(snap) => snap,
            Err(_) => continue,
        };
        if !snap.state.is_terminal() {
            out.push((id, snap));
        }
    }
    Ok(out)
}

fn brief_title(home: &A2aHome, worker_id: &str, snap: &WorkerSnapshot) -> String {
    let brief = home.worker_dir(worker_id).join("brief.md");
    let Ok(text) = fs::read_to_string(brief) else {
        return snap.state.as_str().to_string();
    };
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && *line != "---" && !line.contains(':'))
        .map(|line| line.trim_start_matches('#').trim().to_string())
        .filter(|line| !line.is_empty())
        .unwrap_or_else(|| snap.state.as_str().to_string())
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
