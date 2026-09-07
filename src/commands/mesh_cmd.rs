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
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::time::Duration;

const FEED_FOLLOW_POLL: Duration = Duration::from_secs(1);

pub fn run(
    home: &A2aHome,
    cfg: &Config,
    home_arg: Option<&Path>,
    cmd: &MeshCmd,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    match cmd {
        MeshCmd::Register(args) => {
            register(home, &peer_id(home, args)?)?;
            Ok(Outcome::from(Value::Null))
        }
        MeshCmd::Heartbeat {
            peer,
            working_on,
            cwd,
        } => {
            let id = peer_id(home, peer)?;
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
            let id = peer_id(home, args)?;
            let mut p = load_or_register(home, &id)?;
            p.last_seen = Utc::now();
            p.retired = true;
            save_presence_change(home, &mut p)?;
            Ok(Outcome::from(Value::Null))
        }
        MeshCmd::Status { peer, text } => {
            let id = peer_id(home, peer)?;
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
            let id = peer_id(home, peer)?;
            let text = post_text(text.as_deref(), message_file.as_deref())?;
            feed::validate_post_text(&text)?;
            let mut p = load_or_register(home, &id)?;
            p.last_seen = Utc::now();
            p.retired = false;
            save_presence_change(home, &mut p)?;
            let seq = feed::post(home, cfg, &id, &text, refs(ref_beads, ref_worker))?;
            Ok(Outcome::from(json!(seq)))
        }
        MeshCmd::Feed {
            since,
            json,
            follow,
        } => {
            if *follow {
                follow_feed(home, since.unwrap_or(0), json_mode || *json)?;
                std::process::exit(0);
            }
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
            let from = peer_id(home, peer)?;
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
            let id = peer_id(home, peer)?;
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
            let id = peer_id(home, peer)?;
            let Some(block) = notes::inject(home, cfg, &id, Utc::now())? else {
                return Ok(Outcome::from(Value::Null));
            };
            if json_mode || *json {
                return Ok(Outcome::from(json!({ "block": block })));
            }
            Ok(Outcome::from(Value::String(block)))
        }
        MeshCmd::Hooks { print } => run_hooks(*print),
        MeshCmd::Links => crate::mesh::gateway::links(home, json_mode, quiet),
        MeshCmd::Fleet { cmd } => run_fleet(home, cmd, json_mode),
        MeshCmd::Gateway { cmd } => run_gateway(home, cfg, home_arg, cmd, json_mode, quiet),
        MeshCmd::Work { cmd } => crate::commands::work_cmd::run(home, cfg, cmd, quiet),
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

fn peer_id(home: &A2aHome, args: &MeshPeerArgs) -> Result<PeerId, AppError> {
    let provider = Provider::from(args.provider);
    presence::resolve_peer_id(home, provider, args.session.as_deref())
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

fn follow_feed(home: &A2aHome, since: u64, json: bool) -> Result<(), AppError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| AppError::new("ASYNC", ExitCode::Internal, format!("starting runtime: {e}")))?;
    runtime.block_on(async move {
        let mut cursor = since;
        loop {
            let entries = feed::read(home, cursor)?;
            if !entries.is_empty() {
                print_feed_entries(&entries, json)?;
                cursor = entries
                    .iter()
                    .map(|entry| entry.seq)
                    .max()
                    .unwrap_or(cursor);
            }
            tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(|e| {
                        AppError::new("SIGNAL", ExitCode::Internal, format!("waiting for Ctrl-C: {e}"))
                    })?;
                    return Ok(());
                }
                _ = tokio::time::sleep(FEED_FOLLOW_POLL) => {}
            }
        }
    })
}

fn print_feed_entries(entries: &[FeedEntry], json: bool) -> Result<(), AppError> {
    if json {
        for entry in entries {
            let value = serde_json::to_value(entry).map_err(|e| {
                AppError::new("DATA", ExitCode::Internal, format!("serializing feed: {e}"))
            })?;
            let value = crate::commands::tail::redact_value(&value);
            println!("{}", serde_json::to_string(&value).map_err(|e| {
                AppError::new("DATA", ExitCode::Internal, format!("serializing feed: {e}"))
            })?);
        }
    } else {
        let text = human_feed(entries);
        if !text.is_empty() {
            println!("{text}");
        }
    }
    std::io::stdout()
        .flush()
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("flushing stdout: {e}")))
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
                    apply_mesh_migrations(&registry).await.map(|_| ())
                })?;
            }
            Ok(Outcome::from(json!({
                "fleet_id": fleet.fleet_id,
                "root_fingerprint": root_fingerprint
            })))
        }
        MeshFleetCmd::Ls { json } => {
            let url = read_registry_env(home)?;
            let fleet_id = crate::mesh::keys::fleet_id(home)?;
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
        MeshFleetCmd::Migrate => {
            require_fleet_root_key(home)?;
            let url = read_registry_env(home)?;
            let applied = rt.block_on(async {
                let registry = open_registry(&url)?;
                apply_mesh_migrations(&registry).await
            })?;
            Ok(Outcome::from(json!({ "applied": applied })))
        }
        MeshFleetCmd::Revoke { gateway_id } => {
            let url = read_registry_env(home)?;
            let fleet_id = crate::mesh::keys::fleet_id(home)?;
            rt.block_on(async {
                let registry = open_registry(&url)?;
                registry.revoke(&fleet_id, gateway_id, Utc::now()).await
            })?;
            Ok(Outcome::from(Value::Null))
        }
        MeshFleetCmd::Enroll {
            gateway_id,
            pubkey,
            out,
        } => {
            let token = crate::mesh::keys::enroll_gateway_token(
                home,
                gateway_id,
                pubkey,
                &crate::mesh::keys::SystemClock,
            )?;
            let reminder =
                "joining machine needs: token + mesh/fleet/root.pub + mesh/fleet/registry.env";
            if out == "-" {
                if json_mode {
                    return Ok(Outcome::from(json!({
                        "token": token,
                        "reminder": reminder
                    })));
                }
                eprintln!("a2a: {reminder}");
                return Ok(Outcome::from(Value::String(token)));
            }
            write_file_0600(Path::new(out), &(token.clone() + "\n"))?;
            Ok(Outcome::from(json!({
                "token_path": out,
                "reminder": reminder
            })))
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
    home_arg: Option<&Path>,
    cmd: &MeshGatewayCmd,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    match cmd {
        MeshGatewayCmd::Run {
            daemon,
            daemonized,
        } => crate::mesh::gateway::run(home, cfg, quiet, *daemon, *daemonized),
        MeshGatewayCmd::Status => crate::mesh::gateway::status(home, json_mode),
        MeshGatewayCmd::Stop => crate::mesh::gateway::stop(home),
        MeshGatewayCmd::InstallService => {
            crate::mesh::gateway::install_service(home, cfg, home_arg, quiet)
        }
        MeshGatewayCmd::UninstallService => crate::mesh::gateway::uninstall_service(home, quiet),
        MeshGatewayCmd::Identity => {
            let identity = crate::mesh::keys::gateway_identity(home, false)?;
            let pubkey_b64 = crate::mesh::keys::gateway_public_key_b64(home)?;
            let pubkey_fpr = crate::mesh::keys::gateway_public_key_fingerprint(home)?;
            if !json_mode {
                return Ok(Outcome::from(Value::String(format!(
                    "gateway_id={} pubkey_b64={} pubkey_fpr={} cert_fpr={}",
                    identity.gateway_id, pubkey_b64, pubkey_fpr, identity.cert_fpr
                ))));
            }
            Ok(Outcome::from(json!({
                "gateway_id": identity.gateway_id,
                "pubkey_b64": pubkey_b64,
                "pubkey_fpr": pubkey_fpr,
                "cert_fpr": identity.cert_fpr,
            })))
        }
        MeshGatewayCmd::InstallToken { file } => {
            let token = token_input(file)?;
            let claims = crate::mesh::keys::install_gateway_token(
                home,
                token.trim(),
                &crate::mesh::keys::SystemClock,
            )?;
            if !json_mode {
                return Ok(Outcome::from(Value::String(format!(
                    "installed gateway token for {}",
                    claims.gateway_id
                ))));
            }
            Ok(Outcome::from(json!({
                "installed": true,
                "fleet_id": claims.fleet_id,
                "gateway_id": claims.gateway_id,
                "exp": claims.exp,
            })))
        }
    }
}

fn token_input(file: &str) -> Result<String, AppError> {
    if file == "-" {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading token: {e}")))?;
        let token = text.trim();
        if token.is_empty() {
            return Err(AppError::new(
                "TOKEN_REQUIRED",
                ExitCode::Precondition,
                "token on stdin was empty",
            ));
        }
        return Ok(token.to_string());
    }
    let text = std::fs::read_to_string(file).map_err(|e| {
        AppError::new(
            "IO",
            ExitCode::Precondition,
            format!("reading token file {file}: {e}"),
        )
    })?;
    let token = text.trim();
    if token.is_empty() {
        return Err(AppError::new(
            "TOKEN_REQUIRED",
            ExitCode::Precondition,
            "token file was empty",
        ));
    }
    Ok(token.to_string())
}

fn write_file_0600(path: &Path, text: &str) -> Result<(), AppError> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| {
            AppError::new(
                "IO",
                ExitCode::Precondition,
                format!("writing {path:?}: {e}"),
            )
        })?;
    file.write_all(text.as_bytes())
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("writing {path:?}: {e}")))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
        AppError::new(
            "IO",
            ExitCode::Internal,
            format!("setting token permissions for {path:?}: {e}"),
        )
    })
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

fn require_fleet_root_key(home: &A2aHome) -> Result<(), AppError> {
    let fleet_dir = home.root().join("mesh").join("fleet");
    if fleet_dir.join("root.key").exists() {
        return Ok(());
    }
    if fleet_dir.join("root.pub").exists() {
        return crate::mesh::keys::fleet_init(home).map(|_| ());
    }
    Err(AppError::new(
        "FLEET_ROOT_KEY_MISSING",
        ExitCode::Precondition,
        "fleet migration must run on the root machine that holds mesh/fleet/root.key",
    )
    .remedy("run `a2a mesh fleet init` on the fleet root machine, or copy registry.env there and retry"))
}

async fn apply_mesh_migrations(
    registry: &crate::mesh::registry::RegistryHandle,
) -> Result<Vec<String>, AppError> {
    let migrations = mesh_migration_files();
    let mut applied = Vec::with_capacity(migrations.len());
    for (name, sql) in migrations {
        eprintln!("a2a: applying {name}:\n{sql}");
        registry.apply_migration(sql).await?;
        applied.push(name.to_string());
    }
    Ok(applied)
}

pub const MESH_MIGRATIONS: [(&str, &str); 3] = [
    (
        "mesh_l2_0001.sql",
        include_str!("../../migrations/mesh_l2_0001.sql"),
    ),
    (
        "mesh_l3_0001.sql",
        include_str!("../../migrations/mesh_l3_0001.sql"),
    ),
    (
        "mesh_l4_0001.sql",
        include_str!("../../migrations/mesh_l4_0001.sql"),
    ),
];

fn mesh_migration_files() -> [(&'static str, &'static str); 3] {
    MESH_MIGRATIONS
}

#[cfg(test)]
fn statement_terminators(sql: &str) -> usize {
    sql.lines()
        .map(|line| line.split_once("--").map(|(sql, _)| sql).unwrap_or(line))
        .map(|line| line.matches(';').count())
        .sum()
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

#[cfg(test)]
mod tests {
    use super::{statement_terminators, MESH_MIGRATIONS};

    #[test]
    fn mesh_migrations_are_sorted_by_name() {
        assert_eq!(
            MESH_MIGRATIONS.map(|(name, _)| name),
            [
                "mesh_l2_0001.sql",
                "mesh_l3_0001.sql",
                "mesh_l4_0001.sql"
            ]
        );
    }

    #[test]
    fn mesh_migrations_each_contain_one_statement() {
        for (name, sql) in MESH_MIGRATIONS {
            assert_eq!(statement_terminators(sql), 1, "{name}");
            assert!(sql.trim_end().ends_with(';'), "{name}");
        }
    }
}
