use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::gateway::replicate::CursorStore;
use crate::mesh::registry::GatewayRow;
use crate::mesh::wire::Frame;
use crate::mesh::{mesh_root, presence, PeerId, Presence};
use crate::output::{AppError, ExitCode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const PRESENCE_STATE_FILE: &str = "presence-state.json";
const REGISTRY_CACHE_FILE: &str = "registry-cache.json";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PresenceCounterState {
    #[serde(default)]
    presence_seq: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct RegistryCache {
    #[serde(default)]
    gateways: BTreeMap<String, CachedGateway>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CachedGateway {
    last_heartbeat: DateTime<Utc>,
    #[serde(default)]
    revoked_at: Option<DateTime<Utc>>,
    #[serde(default)]
    missing_since: Option<DateTime<Utc>>,
    #[serde(default)]
    retired_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct OriginRetirementReport {
    pub retired: Vec<RetiredOrigin>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RetiredOrigin {
    pub gateway_id: String,
    pub cursors_dropped: bool,
    pub archived: usize,
}

pub fn record_local_change(
    home: &A2aHome,
    id: &PeerId,
    now: DateTime<Utc>,
    gateway_id: &str,
) -> Result<Presence, AppError> {
    let mut p = match presence::load(home, id)? {
        Some(p) => p,
        None => presence::new_presence(home, id, now)?,
    };
    p.last_seen = now;
    p.retired = false;
    save_local_change(home, &mut p, gateway_id)?;
    Ok(p)
}

pub fn save_local_change(
    home: &A2aHome,
    p: &mut Presence,
    gateway_id: &str,
) -> Result<(), AppError> {
    let mut state = load_presence_state(home)?;
    state.presence_seq = state.presence_seq.max(p.presence_seq).saturating_add(1);
    save_presence_state(home, &state)?;

    p.presence_seq = state.presence_seq;
    p.home_gateway = Some(gateway_id.to_string());
    presence::save(home, p)?;
    Ok(())
}

pub fn gossip_frame(home: &A2aHome, local_gateway_id: &str) -> Result<Frame, AppError> {
    let mut peers = Vec::new();
    for id in peer_ids(home)? {
        let Some(mut p) = presence::load(home, &id)? else {
            continue;
        };
        if p.home_gateway
            .as_deref()
            .filter(|s| !s.is_empty())
            .is_none()
        {
            p.home_gateway = Some(local_gateway_id.to_string());
        }
        peers.push(p);
    }
    Ok(Frame::Presence { peers })
}

pub fn merge_presence(
    home: &A2aHome,
    mut incoming: Presence,
    source_gateway: &str,
) -> Result<bool, AppError> {
    let id = PeerId::parse(&incoming.peer)?;
    if incoming
        .home_gateway
        .as_deref()
        .filter(|gateway| !gateway.is_empty())
        .is_none()
    {
        incoming.home_gateway = Some(source_gateway.to_string());
    }
    let incoming_tuple = presence_tuple(&incoming, source_gateway);
    let Some(cached) = presence::load(home, &id)? else {
        presence::save(home, &incoming)?;
        return Ok(true);
    };
    if incoming_tuple > presence_tuple(&cached, "") {
        presence::save(home, &incoming)?;
        return Ok(true);
    }
    Ok(false)
}

pub fn merge_presence_frame(
    home: &A2aHome,
    source_gateway: &str,
    peers: Vec<Presence>,
) -> Result<usize, AppError> {
    let mut accepted = 0usize;
    for peer in peers {
        if merge_presence(home, peer, source_gateway)? {
            accepted += 1;
        }
    }
    Ok(accepted)
}

pub fn refresh_registry_cache(
    home: &A2aHome,
    rows: &[GatewayRow],
    now: DateTime<Utc>,
) -> Result<(), AppError> {
    let mut cache = load_registry_cache(home)?;
    let mut seen = BTreeSet::new();
    for row in rows {
        seen.insert(row.gateway_id.clone());
        cache.gateways.insert(
            row.gateway_id.clone(),
            CachedGateway {
                last_heartbeat: row.last_heartbeat,
                revoked_at: row.revoked_at,
                missing_since: None,
                retired_at: cache
                    .gateways
                    .get(&row.gateway_id)
                    .and_then(|cached| cached.retired_at),
            },
        );
    }
    for (gateway_id, cached) in &mut cache.gateways {
        if !seen.contains(gateway_id) && cached.missing_since.is_none() {
            cached.missing_since = Some(now);
        }
    }
    save_registry_cache(home, &cache)
}

pub fn home_gateway_registry_retired(
    home: &A2aHome,
    gateway_id: &str,
    now: DateTime<Utc>,
    gc_after_secs: i64,
) -> Result<bool, AppError> {
    let cache = load_registry_cache(home)?;
    Ok(cache
        .gateways
        .get(gateway_id)
        .map(|cached| registry_retirable(cached, now, gc_after_secs))
        .unwrap_or(false))
}

pub fn retire_stale_origins(
    home: &A2aHome,
    now: DateTime<Utc>,
    gc_after_secs: i64,
) -> Result<OriginRetirementReport, AppError> {
    let mut cache = load_registry_cache(home)?;
    let retire = cache
        .gateways
        .iter()
        .filter(|(_, cached)| registry_retirable(cached, now, gc_after_secs))
        .map(|(gateway_id, _)| gateway_id.clone())
        .collect::<Vec<_>>();
    if retire.is_empty() {
        return Ok(OriginRetirementReport::default());
    }

    let mut cursors = CursorStore::load_cached(home)?;
    let mut cursors_changed = false;
    let mut report = OriginRetirementReport::default();
    for gateway_id in retire {
        let cursors_dropped = cursors.drop_origin(&gateway_id);
        cursors_changed |= cursors_dropped;
        let archived = archive_outbound(home, &gateway_id)?;
        let first_retirement = cache
            .gateways
            .get(&gateway_id)
            .and_then(|cached| cached.retired_at)
            .is_none();
        if first_retirement || cursors_dropped || archived > 0 {
            log_origin_retired(home, &gateway_id, archived, cursors_dropped);
            report.retired.push(RetiredOrigin {
                gateway_id: gateway_id.clone(),
                cursors_dropped,
                archived,
            });
        }
        if let Some(cached) = cache.gateways.get_mut(&gateway_id) {
            cached.retired_at = Some(now);
        }
    }
    if cursors_changed {
        cursors.persist(home)?;
    }
    save_registry_cache(home, &cache)?;
    Ok(report)
}

fn presence_tuple(p: &Presence, fallback_gateway: &str) -> (u64, DateTime<Utc>, String) {
    let gateway = p
        .home_gateway
        .as_deref()
        .filter(|gateway| !gateway.is_empty())
        .unwrap_or(fallback_gateway)
        .to_string();
    (p.presence_seq, p.last_seen, gateway)
}

fn registry_retirable(cached: &CachedGateway, now: DateTime<Utc>, gc_after_secs: i64) -> bool {
    if cached.revoked_at.is_none() && cached.missing_since.is_none() {
        return false;
    }
    now.signed_duration_since(cached.last_heartbeat)
        .num_seconds()
        > gc_after_secs
}

fn peer_ids(home: &A2aHome) -> Result<Vec<PeerId>, AppError> {
    let peers = home.root().join("mesh").join("peers");
    let entries = match fs::read_dir(&peers) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_err("reading mesh peers", e)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| io_err("reading mesh peers", e))?;
        if !entry
            .file_type()
            .map_err(|e| io_err("reading mesh peers", e))?
            .is_dir()
        {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some((provider, uuid)) = name.split_once('-') else {
            continue;
        };
        if let Ok(id) = PeerId::parse(&format!("{provider}:{uuid}")) {
            out.push(id);
        }
    }
    out.sort_by_key(ToString::to_string);
    Ok(out)
}

fn archive_outbound(home: &A2aHome, gateway_id: &str) -> Result<usize, AppError> {
    let root = mesh_root(home).join("outbound");
    let source = root.join(gateway_id);
    if !source.exists() {
        return Ok(0);
    }
    let dest = root.join("retired").join(gateway_id);
    create_dir_0700(&dest).map_err(|e| io_err("creating retired outbound dir", e))?;
    let mut archived = 0usize;
    for entry in fs::read_dir(&source).map_err(|e| io_err("reading outbound origin", e))? {
        let entry = entry.map_err(|e| io_err("reading outbound origin", e))?;
        if !entry
            .file_type()
            .map_err(|e| io_err("reading outbound origin", e))?
            .is_file()
        {
            continue;
        }
        let name = entry.file_name();
        let to = dest.join(&name);
        if to.exists() {
            fs::remove_file(entry.path())
                .map_err(|e| io_err("removing duplicate retired spool", e))?;
        } else {
            fs::rename(entry.path(), to).map_err(|e| io_err("retiring outbound spool", e))?;
        }
        archived += 1;
    }
    fs::remove_dir_all(&source).map_err(|e| io_err("removing retired outbound origin", e))?;
    Ok(archived)
}

fn log_origin_retired(home: &A2aHome, gateway_id: &str, archived: usize, cursors_dropped: bool) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut f) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "origin_retired",
            "gateway_id": gateway_id,
            "archived": archived,
            "cursors_dropped": cursors_dropped,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = f.write_all(&bytes);
        }
    }
}

fn load_presence_state(home: &A2aHome) -> Result<PresenceCounterState, AppError> {
    read_json(&presence_state_path(home), "reading presence state")
}

fn save_presence_state(home: &A2aHome, state: &PresenceCounterState) -> Result<(), AppError> {
    write_json(&presence_state_path(home), state, "writing presence state")
}

fn load_registry_cache(home: &A2aHome) -> Result<RegistryCache, AppError> {
    read_json(&registry_cache_path(home), "reading registry cache")
}

fn save_registry_cache(home: &A2aHome, cache: &RegistryCache) -> Result<(), AppError> {
    write_json(&registry_cache_path(home), cache, "writing registry cache")
}

fn read_json<T>(path: &Path, context: &'static str) -> Result<T, AppError>
where
    T: Default + for<'de> Deserialize<'de>,
{
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| data_err(context, e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(T::default()),
        Err(e) => Err(io_err(context, e)),
    }
}

fn write_json<T>(path: &Path, value: &T, context: &'static str) -> Result<(), AppError>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        create_dir_0700(parent).map_err(|e| io_err("creating gateway state dir", e))?;
    }
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| data_err(context, e))?;
    crate::store::write_atomic(path, &bytes).map_err(|e| io_err(context, e))
}

fn presence_state_path(home: &A2aHome) -> PathBuf {
    home.root()
        .join("mesh")
        .join("gateway")
        .join(PRESENCE_STATE_FILE)
}

fn registry_cache_path(home: &A2aHome) -> PathBuf {
    home.root()
        .join("mesh")
        .join("gateway")
        .join(REGISTRY_CACHE_FILE)
}

fn io_err(context: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}

fn data_err(context: &'static str, e: serde_json::Error) -> AppError {
    AppError::new("DATA", ExitCode::Internal, format!("{context}: {e}"))
}
