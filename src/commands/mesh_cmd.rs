use crate::cli::{MeshCmd, MeshPeerArgs};
use crate::config::Config;
use crate::home::A2aHome;
use crate::mesh::{feed, notes::{self, RosterRow}, presence, scan_or_refuse, FeedEntry, Note, PeerId, Provider, STATUS_MAX_BYTES};
use crate::output::{AppError, ExitCode, Outcome};
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Value};
use std::io::Read;
use std::path::Path;

pub fn run(
    home: &A2aHome,
    cfg: &Config,
    cmd: &MeshCmd,
    json_mode: bool,
    quiet: bool,
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
        MeshCmd::Note {
            peer,
            target,
            stdin,
            message_file,
            ref_beads,
            ref_worker,
        } => {
            let from = peer_id(peer)?;
            let text = note_text(stdin.as_deref(), message_file.as_deref())?;
            let target = notes::resolve_target(home, cfg, target)?;
            notes::write_note(home, cfg, &from, &target, &text, refs(ref_beads, ref_worker))?;
            if !quiet {
                if let Some(state) = notes::target_warning(home, &target)? {
                    eprintln!(
                        "a2a: warning: peer {} is {state} — the note will wait",
                        notes::target_peer(&target)
                    );
                }
            }
            Ok(Outcome::from(Value::Null))
        }
        MeshCmd::Inbox {
            peer,
            json,
            consume,
        } => {
            let id = peer_id(peer)?;
            let rows = notes::unread(home, &id)?;
            let files = rows.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>();
            let notes_only = rows.into_iter().map(|(_, n)| n).collect::<Vec<_>>();
            if *consume {
                notes::consume(home, &id, &files)?;
            }
            if json_mode || *json {
                let value = serde_json::to_value(notes_only)
                    .map_err(|e| AppError::new("DATA", ExitCode::Internal, format!("serializing inbox: {e}")))?;
                return Ok(Outcome::from(crate::commands::tail::redact_value(&value)));
            }
            if notes_only.is_empty() {
                return Ok(Outcome::from(Value::Null));
            }
            Ok(Outcome::from(Value::String(human_notes(&notes_only))))
        }
        MeshCmd::Roster { json } => {
            let rows = notes::roster(home, cfg, Utc::now())?;
            if json_mode || *json {
                let value = serde_json::to_value(rows)
                    .map_err(|e| AppError::new("DATA", ExitCode::Internal, format!("serializing roster: {e}")))?;
                return Ok(Outcome::from(crate::commands::tail::redact_value(&value)));
            }
            if rows.is_empty() {
                return Ok(Outcome::from(Value::Null));
            }
            Ok(Outcome::from(Value::String(human_roster(&rows))))
        }
        MeshCmd::Inject { peer, json } => {
            let id = peer_id(peer)?;
            let Some(block) = notes::inject(home, cfg, &id, Utc::now())? else {
                return Ok(Outcome::from(Value::Null));
            };
            if json_mode || *json {
                return Ok(Outcome::from(json!({ "block": block })));
            }
            Ok(Outcome::from(Value::String(block)))
        }
        MeshCmd::Hooks { print } => run_hooks(*print),
    }
}

pub fn run_hooks(print: bool) -> Result<Outcome, AppError> {
    if !print {
        return Err(AppError::new(
            "MESH_HOOKS_PRINT_REQUIRED",
            ExitCode::Precondition,
            "use --print; a2a never edits Claude Code settings",
        ));
    }
    eprintln!("a2a: merge this into ~/.claude/settings.json yourself — a2a never edits your settings");
    Ok(Outcome::from(Value::String(
        crate::templates::MESH_HOOKS_JSON
            .strip_suffix('\n')
            .unwrap_or(crate::templates::MESH_HOOKS_JSON)
            .to_string(),
    )))
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

fn note_text(stdin: Option<&str>, message_file: Option<&Path>) -> Result<String, AppError> {
    match (stdin, message_file) {
        (Some(_), Some(_)) => Err(AppError::new(
            "NOTE_MESSAGE_SOURCE",
            ExitCode::Precondition,
            "provide note text on stdin `-` or --message-file, not both",
        )),
        (Some("-"), None) => {
            let mut text = String::new();
            std::io::stdin()
                .read_to_string(&mut text)
                .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading stdin: {e}")))?;
            Ok(text)
        }
        (Some(_), None) => Err(AppError::new(
            "NOTE_MESSAGE_SOURCE",
            ExitCode::Precondition,
            "note text must be read from stdin `-` or --message-file",
        )),
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
            "NOTE_MESSAGE_REQUIRED",
            ExitCode::Precondition,
            "provide note text on stdin `-` or --message-file",
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

fn human_notes(notes: &[Note]) -> String {
    notes
        .iter()
        .map(|note| {
            crate::redact::redact_egress(&format!(
                "{} {} -> {} — {}",
                note.ts.to_rfc3339_opts(SecondsFormat::Secs, true),
                short_peer(&note.from),
                short_peer(&note.to),
                note.text.lines().next().unwrap_or("")
            ))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn human_roster(rows: &[RosterRow]) -> String {
    rows.iter()
        .map(|row| {
            crate::redact::redact_egress(&format!(
                "{} {} notes={}{}{}",
                row.peer,
                label(row.label),
                row.unread_notes,
                row.worker_id
                    .as_deref()
                    .map(|id| format!(" worker={id}"))
                    .unwrap_or_default(),
                row.working_on
                    .as_deref()
                    .map(|w| format!(" — {w}"))
                    .unwrap_or_default()
            ))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn label(label: presence::Label) -> &'static str {
    match label {
        presence::Label::Active => "active",
        presence::Label::Stale => "stale",
        presence::Label::Retired => "retired",
    }
}

fn short_peer(peer: &str) -> String {
    let Some((provider, uuid)) = peer.split_once(':') else {
        return peer.chars().take(12).collect();
    };
    format!("{}:{}", provider, uuid.chars().take(8).collect::<String>())
}
