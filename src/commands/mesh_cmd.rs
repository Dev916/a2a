use crate::cli::{MeshCmd, MeshFleetCmd, MeshGatewayCmd, MeshPeerArgs};
use crate::config::Config;
use crate::home::A2aHome;
use crate::mesh::{
    feed,
    notes::{self, RosterRow},
    presence,
    registry::{
        open_registry, read_registry_env, staleness, write_registry_env, GatewayRow, Registry,
        DEFAULT_REGISTRY_HEARTBEAT_SECS,
    },
    scan_or_refuse, FeedEntry, Note, PeerId, Provider, STATUS_MAX_BYTES,
};
use crate::output::{AppError, ExitCode, Outcome};
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Value};
use std::io::Read;
use std::path::Path;

const MESH_L2_MIGRATION_SQL: &str = include_str!("../../migrations/mesh_l2_0001.sql");

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
            save_presence_change(home, &mut p)?;
            Ok(Outcome::from(Value::Null))
        }
        MeshCmd::Retire(args) => {
            let id = peer_id(args)?;
            let mut p = load_or_register(home, &id)?;
            p.last_seen = Utc::now();
            p.retired = true;
            save_presence_change(home, &mut p)?;
            Ok(Outcome::from(Value::Null))
        }
        MeshCmd::Status { peer, text } => {
            let id = peer_id(peer)?;
            let mut text = text.clone();
            if text == "-" {
                text.clear();
                std::io::stdin().read_to_string(&mut text).map_err(|e| {
                    AppError::new("IO", ExitCode::Internal, format!("reading stdin: {e}"))
                })?;
            }
            refuse_status(&text)?;
            let mut p = load_or_register(home, &id)?;
            p.last_seen = Utc::now();
            p.retired = false;
            p.status = if text.is_empty() { None } else { Some(text) };
            save_presence_change(home, &mut p)?;
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
            feed::validate_post_text(&text)?;
            let mut p = load_or_register(home, &id)?;
            p.last_seen = Utc::now();
            p.retired = false;
            save_presence_change(home, &mut p)?;
            let seq = feed::post(home, cfg, &id, &text, refs(ref_beads, ref_worker))?;
            Ok(Outcome::from(json!(seq)))
        }
        MeshCmd::Feed { since, json } => {
            let entries = feed::read(home, since.unwrap_or(0))?;
            if json_mode || *json {
                let value = serde_json::to_value(entries).map_err(|e| {
                    AppError::new("DATA", ExitCode::Internal, format!("serializing feed: {e}"))
                })?;
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
            notes::write_note(
                home,
                cfg,
                &from,
                &target,
                &text,
                refs(ref_beads, ref_worker),
            )?;
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
                let value = serde_json::to_value(notes_only).map_err(|e| {
                    AppError::new(
                        "DATA",
                        ExitCode::Internal,
                        format!("serializing inbox: {e}"),
                    )
                })?;
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
                let value = serde_json::to_value(rows).map_err(|e| {
                    AppError::new(
                        "DATA",
                        ExitCode::Internal,
                        format!("serializing roster: {e}"),
                    )
                })?;
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
        MeshCmd::Fleet { cmd } => run_fleet(home, cmd, json_mode),
        MeshCmd::Gateway { cmd } => run_gateway(home, cfg, cmd, json_mode, quiet),
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
    eprintln!(
        "a2a: merge this into ~/.claude/settings.json yourself — a2a never edits your settings"
    );
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
    save_presence_change(home, &mut p)
}

fn save_presence_change(home: &A2aHome, p: &mut crate::mesh::Presence) -> Result<(), AppError> {
    if let Some(gateway_id) = feed::existing_gateway_id(home) {
        return crate::mesh::gateway::presence_wire::save_local_change(home, p, &gateway_id);
    }
    presence::save(home, p)
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
            std::io::stdin().read_to_string(&mut text).map_err(|e| {
                AppError::new("IO", ExitCode::Internal, format!("reading stdin: {e}"))
            })?;
            Ok(text)
        }
        (Some(text), None) => Ok(text.to_string()),
        (None, Some(path)) if path == Path::new("-") => {
            let mut text = String::new();
            std::io::stdin().read_to_string(&mut text).map_err(|e| {
                AppError::new("IO", ExitCode::Internal, format!("reading stdin: {e}"))
            })?;
            Ok(text)
        }
        (None, Some(path)) => std::fs::read_to_string(path).map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Precondition,
                format!("reading {}: {e}", path.display()),
            )
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
            std::io::stdin().read_to_string(&mut text).map_err(|e| {
                AppError::new("IO", ExitCode::Internal, format!("reading stdin: {e}"))
            })?;
            Ok(text)
        }
        (Some(_), None) => Err(AppError::new(
            "NOTE_MESSAGE_SOURCE",
            ExitCode::Precondition,
            "note text must be read from stdin `-` or --message-file",
        )),
        (None, Some(path)) if path == Path::new("-") => {
            let mut text = String::new();
            std::io::stdin().read_to_string(&mut text).map_err(|e| {
                AppError::new("IO", ExitCode::Internal, format!("reading stdin: {e}"))
            })?;
            Ok(text)
        }
        (None, Some(path)) => std::fs::read_to_string(path).map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Precondition,
                format!("reading {}: {e}", path.display()),
            )
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
                feed::origin_citation(entry),
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
                "{} {}{} notes={}{}{}",
                row.peer,
                label(row.label),
                row.home_gateway
                    .as_deref()
                    .map(|gateway| format!(" via {gateway}"))
                    .unwrap_or_default(),
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

fn run_fleet(home: &A2aHome, cmd: &MeshFleetCmd, json_mode: bool) -> Result<Outcome, AppError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            AppError::new(
                "RUNTIME",
                ExitCode::Internal,
                format!("starting registry runtime: {e}"),
            )
        })?;
    match cmd {
        MeshFleetCmd::Init { registry } => {
            let fleet = crate::mesh::keys::fleet_init(home)?;
            let root_fingerprint = crate::mesh::keys::fleet_root_fingerprint(home)?;
            if let Some(registry) = registry {
                let url = registry_input(registry)?;
                write_registry_env(home, &url)?;
                rt.block_on(async {
                    let registry = open_registry(&url)?;
                    registry.apply_migration(MESH_L2_MIGRATION_SQL).await
                })?;
            }
            Ok(Outcome::from(json!({
                "fleet_id": fleet.fleet_id,
                "root_fingerprint": root_fingerprint
            })))
        }
        MeshFleetCmd::Ls { json } => {
            let url = read_registry_env(home)?;
            let fleet_id = crate::mesh::keys::fleet_init(home)?.fleet_id;
            let rows = rt.block_on(async {
                let registry = open_registry(&url)?;
                registry.discover(&fleet_id).await
            })?;
            let now = Utc::now();
            let data = rows
                .into_iter()
                .map(|row| fleet_row_json(row, now))
                .collect::<Vec<_>>();
            if json_mode || *json {
                return Ok(Outcome::from(json!({ "rows": data })));
            }
            Ok(Outcome::from(Value::String(human_fleet_rows(&data))))
        }
        MeshFleetCmd::Revoke { gateway_id } => {
            let url = read_registry_env(home)?;
            let fleet_id = crate::mesh::keys::fleet_init(home)?.fleet_id;
            rt.block_on(async {
                let registry = open_registry(&url)?;
                registry.revoke(&fleet_id, gateway_id, Utc::now()).await
            })?;
            Ok(Outcome::from(Value::Null))
        }
        MeshFleetCmd::RotateRoot => {
            let rotation = crate::mesh::keys::rotate_root(home, &crate::mesh::keys::SystemClock)?;
            Ok(Outcome::from(json!({
                "fleet_id": rotation.fleet_id,
                "current_root_fingerprint": rotation.current_fingerprint,
                "next_root_fingerprint": rotation.next_fingerprint,
                "gateway_token_rotated": rotation.gateway_token_rotated
            })))
        }
    }
}

fn run_gateway(
    home: &A2aHome,
    cfg: &Config,
    cmd: &MeshGatewayCmd,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    match cmd {
        MeshGatewayCmd::Run => crate::mesh::gateway::run(home, cfg, quiet),
        MeshGatewayCmd::Status => crate::mesh::gateway::status(home, json_mode),
        MeshGatewayCmd::Stop => crate::mesh::gateway::stop(home),
    }
}

fn registry_input(registry: &str) -> Result<String, AppError> {
    if registry == "-" {
        let mut text = String::new();
        std::io::stdin().read_to_string(&mut text).map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Internal,
                format!("reading registry URL: {e}"),
            )
        })?;
        let url = text.trim();
        if url.is_empty() {
            return Err(AppError::new(
                "REGISTRY_URL_REQUIRED",
                ExitCode::Precondition,
                "registry URL on stdin was empty",
            ));
        }
        return Ok(url.to_string());
    }
    let url = registry.trim();
    if url.is_empty() {
        return Err(AppError::new(
            "REGISTRY_URL_REQUIRED",
            ExitCode::Precondition,
            "registry URL was empty",
        ));
    }
    Ok(url.to_string())
}

fn fleet_row_json(row: GatewayRow, now: chrono::DateTime<Utc>) -> Value {
    let state = staleness(row.last_heartbeat, now, DEFAULT_REGISTRY_HEARTBEAT_SECS);
    json!({
        "fleet_id": row.fleet_id,
        "gateway_id": row.gateway_id,
        "owner": row.owner,
        "endpoint_url": row.endpoint_url,
        "pubkey_fpr": row.pubkey_fpr,
        "cert_fpr": row.cert_fpr,
        "version": row.version,
        "created_at": row.created_at,
        "last_heartbeat": row.last_heartbeat,
        "revoked_at": row.revoked_at,
        "schema_version": row.schema_version,
        "staleness": state,
    })
}

fn human_fleet_rows(rows: &[Value]) -> String {
    rows.iter()
        .map(|row| {
            format!(
                "{} {} {}",
                row["gateway_id"].as_str().unwrap_or(""),
                row["staleness"].as_str().unwrap_or(""),
                row["endpoint_url"].as_str().unwrap_or("outbound-only")
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}
