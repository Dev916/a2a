use crate::config::Config;
use crate::home::A2aHome;
use crate::mesh::{mesh_root, scan_or_refuse, FeedEntry, PeerId, POST_MAX_BYTES};
use crate::output::{AppError, ExitCode};
use crate::store;
use chrono::Utc;
use serde_json::Value;

pub fn post(
    home: &A2aHome,
    cfg: &Config,
    from: &PeerId,
    text: &str,
    refs: serde_json::Value,
) -> Result<u64, AppError> {
    validate_post_text(text)?;

    let entry = FeedEntry {
        seq: 0,
        origin: None,
        origin_seq: None,
        from: from.to_string(),
        ts: Utc::now(),
        text: text.to_string(),
        refs,
        extra: serde_json::Map::new(),
    };
    append_entry(home, cfg, entry)
}

pub fn append_entry(home: &A2aHome, cfg: &Config, entry: FeedEntry) -> Result<u64, AppError> {
    let root = mesh_root(home);
    let mut row = serde_json::to_value(entry).map_err(|e| data_err("serializing feed entry", e))?;
    let mut log = store::open_named(&root, "feed", cfg.mesh.feed_max_bytes)
        .map_err(|e| io_err("opening feed.jsonl", e))?;
    log.append_json(&mut row)
        .map_err(|e| io_err("appending feed.jsonl", e))
}

pub fn validate_post_text(text: &str) -> Result<(), AppError> {
    scan_or_refuse(text, "SECRET_IN_POST")?;
    let len = text.len();
    if len > POST_MAX_BYTES {
        return Err(AppError::new(
            "POST_TOO_LARGE",
            ExitCode::Precondition,
            format!("text is {len} bytes, exceeding cap {POST_MAX_BYTES} bytes"),
        ));
    }
    Ok(())
}

pub fn validate_refs_size(refs: &Value, max_bytes: u64) -> Result<(), AppError> {
    let bytes = serde_json::to_vec(refs).map_err(|e| data_err("serializing refs", e))?;
    let len = bytes.len() as u64;
    if max_bytes != 0 && len > max_bytes {
        return Err(AppError::new(
            "WORK_TOO_LARGE",
            ExitCode::Precondition,
            format!("refs are {len} bytes, exceeding cap {max_bytes} bytes"),
        )
        .remedy("shorten the work envelope or increase the configured mesh.work cap"));
    }
    Ok(())
}

pub fn read(home: &A2aHome, since_seq: u64) -> Result<Vec<FeedEntry>, AppError> {
    read_with_local_origin(home, since_seq, existing_gateway_id(home).as_deref())
}

pub fn read_with_local_origin(
    home: &A2aHome,
    since_seq: u64,
    local_origin: Option<&str>,
) -> Result<Vec<FeedEntry>, AppError> {
    let root = home.root().join("mesh");
    if !root.exists() {
        return Ok(Vec::new());
    }
    // Always walk from seq 0: dense local-origin numbering depends on every
    // preceding local entry, so it cannot be computed from a partial window.
    let mut entries = Vec::new();
    let mut next_local: u64 = 1;
    for value in
        store::read_json_named(&root, "feed", 0).map_err(|e| io_err("reading feed.jsonl", e))?
    {
        if is_rotation_marker(&value) {
            continue;
        }
        if is_compaction_marker(&value) {
            continue;
        }
        let mut entry: FeedEntry =
            serde_json::from_value(value).map_err(|e| data_err("decoding feed entry", e))?;
        if let Some(local) = local_origin {
            if entry.origin.as_deref() == Some(local) {
                if let Some(origin_seq) = entry.origin_seq {
                    next_local = next_local.max(origin_seq.saturating_add(1));
                }
            } else if entry.origin.is_none() && entry.origin_seq.is_none() {
                // Claim unstamped local entries with a DENSE per-origin counter,
                // not the global feed seq: replicated entries interleave with
                // local ones, and a gap in the advertised origin_seq stalls
                // peers forever (they wait for a seq that will never exist).
                entry.origin = Some(local.to_string());
                entry.origin_seq = Some(next_local);
                next_local = next_local.saturating_add(1);
            }
        }
        if entry.seq > since_seq {
            entries.push(entry);
        }
    }
    Ok(entries)
}

pub fn origin_citation(entry: &FeedEntry) -> String {
    match (&entry.origin, entry.origin_seq) {
        (Some(origin), Some(origin_seq)) => format!("{origin}:{origin_seq}"),
        _ => entry.seq.to_string(),
    }
}

pub fn existing_gateway_id(home: &A2aHome) -> Option<String> {
    let text =
        std::fs::read_to_string(home.root().join("mesh").join("gateway").join("gw.key")).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    value
        .get("gateway_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

pub fn is_feed_bookkeeping(value: &Value) -> bool {
    is_rotation_marker(value) || is_compaction_marker(value)
}

fn is_rotation_marker(value: &serde_json::Value) -> bool {
    value
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind == "events_rotated")
}

fn is_compaction_marker(value: &serde_json::Value) -> bool {
    value
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind == "feed_compacted")
}

fn io_err(context: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}

fn data_err(context: &'static str, e: serde_json::Error) -> AppError {
    AppError::new("DATA", ExitCode::Internal, format!("{context}: {e}"))
}
