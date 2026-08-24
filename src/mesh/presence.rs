use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::{mesh_root, peer_dir, Cursors, PeerId, Presence};
use crate::output::{AppError, ExitCode};
use crate::store::write_atomic;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

const PRESENCE_FILE: &str = "presence.json";

pub fn resolve_session(arg: Option<&str>) -> Result<String, AppError> {
    if let Some(session) = clean(arg) {
        return Ok(session);
    }
    if let Some(session) = clean(std::env::var("CLAUDE_SESSION_ID").ok().as_deref()) {
        return Ok(session);
    }
    if let Some(actor) = clean(std::env::var("BEADS_ACTOR").ok().as_deref()) {
        if let Some((_, suffix)) = actor.rsplit_once("cc-") {
            if let Some(session) = clean(Some(suffix)) {
                return Ok(session);
            }
        }
    }
    Err(AppError::new(
        "MESH_SESSION_UNKNOWN",
        ExitCode::Precondition,
        "could not resolve mesh session from --session, CLAUDE_SESSION_ID, or BEADS_ACTOR cc-<uuid> suffix",
    ))
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
    write_atomic(&presence_path(home, &id), &bytes)
        .map_err(|e| io_err("writing presence.json", e))
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

pub(crate) fn new_presence(home: &A2aHome, id: &PeerId, now: DateTime<Utc>) -> Result<Presence, AppError> {
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
