use crate::home::{create_dir_0700, create_new_0600, A2aHome};
use crate::mesh::{mesh_root, peer_dir, Cursors, PeerId, Presence, Provider};
use crate::output::{AppError, ExitCode};
use crate::store::write_atomic;
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

const PRESENCE_FILE: &str = "presence.json";
const USER_IDENTITY_FILE: &str = "user-identity.json";

pub fn resolve_session(arg: Option<&str>) -> Result<String, AppError> {
    if let Some(session) = resolve_session_source(arg) {
        return Ok(session);
    }
    Err(AppError::new(
        "MESH_SESSION_UNKNOWN",
        ExitCode::Precondition,
        "could not resolve mesh session from --session, CLAUDE_SESSION_ID, or BEADS_ACTOR cc-<uuid> suffix",
    ))
}

pub fn resolve_peer_id(
    home: &A2aHome,
    provider: Provider,
    arg: Option<&str>,
) -> Result<PeerId, AppError> {
    if let Some(session) = resolve_session_source(arg) {
        return PeerId::parse(&format!("{provider}:{session}"));
    }
    let session = load_or_mint_user_identity(home)?;
    PeerId::parse(&format!("{}:{session}", Provider::User))
}

fn resolve_session_source(arg: Option<&str>) -> Option<String> {
    if let Some(session) = clean(arg) {
        return Some(session);
    }
    if let Some(session) = clean(std::env::var("CLAUDE_SESSION_ID").ok().as_deref()) {
        return Some(session);
    }
    if let Some(actor) = clean(std::env::var("BEADS_ACTOR").ok().as_deref()) {
        if let Some((_, suffix)) = actor.rsplit_once("cc-") {
            if let Some(session) = clean(Some(suffix)) {
                return Some(session);
            }
        }
    }
    None
}

fn load_or_mint_user_identity(home: &A2aHome) -> Result<String, AppError> {
    let root = mesh_root(home);
    let path = root.join(USER_IDENTITY_FILE);
    match read_user_identity(&path) {
        Ok(identity) => return Ok(identity.session_uuid),
        Err(e) if e.code == "IO_NOT_FOUND" => {}
        Err(e) => return Err(e),
    }

    let identity = UserIdentity {
        session_uuid: random_uuid_v4(),
        created_at: Utc::now(),
    };
    let bytes = serde_json::to_vec_pretty(&identity)
        .map_err(|e| data_err("serializing user-identity.json", e))?;
    let tmp = root.join(format!(
        ".{USER_IDENTITY_FILE}.tmp-{}-{}",
        std::process::id(),
        identity.session_uuid
    ));
    {
        let mut file = create_new_0600(&tmp)
            .map_err(|e| io_err("creating temporary user-identity.json", e))?;
        file.write_all(&bytes)
            .map_err(|e| io_err("writing user-identity.json", e))?;
        file.sync_all()
            .map_err(|e| io_err("syncing user-identity.json", e))?;
    }

    match fs::hard_link(&tmp, &path) {
        Ok(()) => {
            let _ = fs::remove_file(&tmp);
            fs::File::open(&root)
                .and_then(|dir| dir.sync_all())
                .map_err(|e| io_err("syncing mesh directory", e))?;
            Ok(identity.session_uuid)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&tmp);
            read_user_identity(&path).map(|identity| identity.session_uuid)
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(io_err("publishing user-identity.json", e))
        }
    }
}

fn read_user_identity(path: &Path) -> Result<UserIdentity, AppError> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|e| data_err("reading user-identity.json", e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(AppError::new(
            "IO_NOT_FOUND",
            ExitCode::Internal,
            "user-identity.json not found",
        )),
        Err(e) => Err(io_err("reading user-identity.json", e)),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserIdentity {
    session_uuid: String,
    created_at: DateTime<Utc>,
}

fn random_uuid_v4() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

pub fn load(home: &A2aHome, id: &PeerId) -> Result<Option<Presence>, AppError> {
    let path = presence_path(home, id);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| data_err("reading presence.json", e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err("reading presence.json", e)),
    }
}

pub fn save(home: &A2aHome, p: &Presence) -> Result<(), AppError> {
    let id = PeerId::parse(&p.peer)?;
    create_dir_0700(&peer_dir(home, &id)).map_err(|e| io_err("creating peer directory", e))?;
    let bytes =
        serde_json::to_vec_pretty(p).map_err(|e| data_err("serializing presence.json", e))?;
    write_atomic(&presence_path(home, &id), &bytes).map_err(|e| io_err("writing presence.json", e))
}

pub fn save_inject_cursors(
    home: &A2aHome,
    p: &mut Presence,
    feed_seq: u64,
    inbox: Option<String>,
    now: DateTime<Utc>,
) -> Result<(), AppError> {
    p.cursors.feed_seq = feed_seq;
    if let Some(inbox) = inbox {
        p.cursors.inbox = Some(inbox);
    }
    p.last_seen = now;
    p.retired = false;
    save(home, p)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Label {
    Active,
    Stale,
    Retired,
}

pub fn staleness(
    last_seen: DateTime<Utc>,
    retired: bool,
    now: DateTime<Utc>,
    stale_after: u64,
) -> Label {
    if retired {
        return Label::Retired;
    }
    if (now - last_seen).num_seconds() >= stale_after as i64 {
        Label::Stale
    } else {
        Label::Active
    }
}

pub(crate) fn new_presence(
    home: &A2aHome,
    id: &PeerId,
    now: DateTime<Utc>,
) -> Result<Presence, AppError> {
    Ok(Presence {
        peer: id.to_string(),
        provider: id.provider.to_string(),
        worker_id: None,
        status: None,
        working_on: None,
        cwd: None,
        registered_at: now,
        last_seen: now,
        retired: false,
        presence_seq: 0,
        home_gateway: None,
        cursors: Cursors {
            feed_seq: current_feed_tip(home)?,
            inbox: None,
        },
        extra: serde_json::Map::new(),
    })
}

fn presence_path(home: &A2aHome, id: &PeerId) -> std::path::PathBuf {
    peer_dir(home, id).join(PRESENCE_FILE)
}

fn current_feed_tip(home: &A2aHome) -> Result<u64, AppError> {
    let root = mesh_root(home);
    let mut tip = 0;
    for name in ["feed.jsonl.1", "feed.jsonl"] {
        tip = tip.max(feed_tip_in(&root.join(name))?);
    }
    Ok(tip)
}

fn feed_tip_in(path: &Path) -> Result<u64, AppError> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(io_err("reading feed.jsonl", e)),
    };
    let mut tip = 0;
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|e| io_err("reading feed.jsonl", e))?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(&line) {
            Ok(v) => {
                if let Some(seq) = v.get("seq").and_then(Value::as_u64) {
                    tip = tip.max(seq);
                }
            }
            Err(_) => break,
        }
    }
    Ok(tip)
}

fn clean(v: Option<&str>) -> Option<String> {
    v.map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

fn io_err(context: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}

fn data_err(context: &'static str, e: serde_json::Error) -> AppError {
    AppError::new("DATA", ExitCode::Internal, format!("{context}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};

    #[test]
    fn staleness_boundary_is_active_before_the_cutoff_and_stale_at_it() {
        let now = Utc.with_ymd_and_hms(2026, 8, 24, 12, 0, 0).unwrap();
        assert_eq!(
            staleness(now - Duration::seconds(899), false, now, 900),
            Label::Active
        );
        assert_eq!(
            staleness(now - Duration::seconds(900), false, now, 900),
            Label::Stale
        );
    }

    #[test]
    fn retired_wins_over_liveness_age() {
        let now = Utc.with_ymd_and_hms(2026, 8, 24, 12, 0, 0).unwrap();
        assert_eq!(staleness(now, true, now, 900), Label::Retired);
        assert_eq!(
            staleness(now - Duration::seconds(900), true, now, 900),
            Label::Retired
        );
    }
}
