use crate::cli::{MeshCmd, MeshPeerArgs};
use crate::config::Config;
use crate::home::A2aHome;
use crate::mesh::{feed, presence, scan_or_refuse, FeedEntry, PeerId, Provider, STATUS_MAX_BYTES};
use crate::output::{AppError, ExitCode, Outcome};
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Value};
use std::path::Path;
use std::io::Read;

pub fn run(
    home: &A2aHome,
    cfg: &Config,
    cmd: &MeshCmd,
    json_mode: bool,
    _quiet: bool,
) -> Result<Outcome, AppError> {
    match cmd {
        MeshCmd::Register(args) => {
            register(home, &peer_id(args)?)?;
            Ok(Outcome::from(Value::Null))
        }
        MeshCmd::Heartbeat {
            peer,
            working_on,
            cwd,
        } => {
            let id = peer_id(peer)?;
            let mut p = load_or_register(home, &id)?;
            p.last_seen = Utc::now();
            p.retired = false;
            if let Some(working_on) = working_on {
                p.working_on = Some(working_on.clone());
            }
            if let Some(cwd) = cwd {
                p.cwd = Some(cwd.to_string_lossy().into_owned());
            }
            presence::save(home, &p)?;
            Ok(Outcome::from(Value::Null))
        }
        MeshCmd::Retire(args) => {
            let id = peer_id(args)?;
            let mut p = load_or_register(home, &id)?;
            p.last_seen = Utc::now();
            p.retired = true;
            presence::save(home, &p)?;
            Ok(Outcome::from(Value::Null))
        }
        MeshCmd::Status { peer, text } => {
            let id = peer_id(peer)?;
            let mut text = text.clone();
            if text == "-" {
                text.clear();
                std::io::stdin()
                    .read_to_string(&mut text)
                    .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading stdin: {e}")))?;
            }
            refuse_status(&text)?;
            let mut p = load_or_register(home, &id)?;
            p.last_seen = Utc::now();
            p.retired = false;
            p.status = if text.is_empty() { None } else { Some(text) };
            presence::save(home, &p)?;
            Ok(Outcome::from(Value::Null))
        }
        MeshCmd::Post {
            peer,
            text,
            message_file,
            ref_beads,
            ref_worker,
        } => {
            let id = peer_id(peer)?;
            let text = post_text(text.as_deref(), message_file.as_deref())?;
            let mut p = load_or_register(home, &id)?;
            p.last_seen = Utc::now();
            p.retired = false;
            presence::save(home, &p)?;
            let seq = feed::post(home, cfg, &id, &text, refs(ref_beads, ref_worker))?;
            Ok(Outcome::from(json!(seq)))
        }
        MeshCmd::Feed { since, json } => {
            let entries = feed::read(home, since.unwrap_or(0))?;
            if json_mode || *json {
                let value = serde_json::to_value(entries)
                    .map_err(|e| AppError::new("DATA", ExitCode::Internal, format!("serializing feed: {e}")))?;
                return Ok(Outcome::from(crate::commands::tail::redact_value(&value)));
            }
            if entries.is_empty() {
                return Ok(Outcome::from(Value::Null));
            }
            Ok(Outcome::from(Value::String(human_feed(&entries))))
        }
    }
}

fn peer_id(args: &MeshPeerArgs) -> Result<PeerId, AppError> {
    let provider = Provider::from(args.provider);
    let session = presence::resolve_session(args.session.as_deref())?;
    PeerId::parse(&format!("{provider}:{session}"))
}

fn register(home: &A2aHome, id: &PeerId) -> Result<(), AppError> {
    let now = Utc::now();
    let mut p = match presence::load(home, id)? {
        Some(p) => p,
        None => presence::new_presence(home, id, now)?,
    };
    p.last_seen = now;
    p.retired = false;
    presence::save(home, &p)
}

fn load_or_register(home: &A2aHome, id: &PeerId) -> Result<crate::mesh::Presence, AppError> {
    match presence::load(home, id)? {
        Some(p) => Ok(p),
        None => presence::new_presence(home, id, Utc::now()),
    }
}

fn refuse_status(text: &str) -> Result<(), AppError> {
    let len = text.len();
    if len > STATUS_MAX_BYTES {
        return Err(AppError::new(
            "STATUS_TOO_LARGE",
            ExitCode::Precondition,
            format!("text is {len} bytes, exceeding cap {STATUS_MAX_BYTES} bytes"),
        ));
    }
    scan_or_refuse(text, "SECRET_IN_STATUS")
}

fn post_text(text: Option<&str>, message_file: Option<&Path>) -> Result<String, AppError> {
    match (text, message_file) {
        (Some(_), Some(_)) => Err(AppError::new(
            "POST_MESSAGE_SOURCE",
            ExitCode::Precondition,
            "provide post text, stdin `-`, or --message-file, not more than one",
        )),
        (Some("-"), None) => {
            let mut text = String::new();
            std::io::stdin()
                .read_to_string(&mut text)
                .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading stdin: {e}")))?;
            Ok(text)
        }
        (Some(text), None) => Ok(text.to_string()),
        (None, Some(path)) if path == Path::new("-") => {
            let mut text = String::new();
            std::io::stdin()
                .read_to_string(&mut text)
                .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading stdin: {e}")))?;
            Ok(text)
        }
        (None, Some(path)) => std::fs::read_to_string(path).map_err(|e| {
            AppError::new("IO", ExitCode::Precondition, format!("reading {}: {e}", path.display()))
        }),
        (None, None) => Err(AppError::new(
            "POST_MESSAGE_REQUIRED",
            ExitCode::Precondition,
            "provide post text, stdin `-`, or --message-file",
        )),
    }
}

fn refs(beads: &[String], workers: &[String]) -> Value {
    let mut obj = serde_json::Map::new();
    if !beads.is_empty() {
        obj.insert("beads".to_string(), json!(beads));
    }
    if !workers.is_empty() {
        obj.insert("worker".to_string(), json!(workers));
    }
    Value::Object(obj)
}

fn human_feed(entries: &[FeedEntry]) -> String {
    entries
        .iter()
        .map(|entry| {
            crate::redact::redact_egress(&format!(
                "{} {} {} — {}",
                entry.seq,
                entry.ts.to_rfc3339_opts(SecondsFormat::Secs, true),
                short_peer(&entry.from),
                entry.text.lines().next().unwrap_or("")
            ))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn short_peer(peer: &str) -> String {
    let Some((provider, uuid)) = peer.split_once(':') else {
        return peer.chars().take(12).collect();
    };
    format!("{}:{}", provider, uuid.chars().take(8).collect::<String>())
}
