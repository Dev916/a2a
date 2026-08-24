use crate::cli::{MeshCmd, MeshPeerArgs};
use crate::home::A2aHome;
use crate::mesh::{presence, scan_or_refuse, PeerId, Provider, STATUS_MAX_BYTES};
use crate::output::{AppError, ExitCode, Outcome};
use chrono::Utc;
use serde_json::Value;
use std::io::Read;

pub fn run(
    home: &A2aHome,
    cmd: &MeshCmd,
    _json_mode: bool,
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
