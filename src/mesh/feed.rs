use crate::config::Config;
use crate::home::A2aHome;
use crate::mesh::{mesh_root, scan_or_refuse, FeedEntry, PeerId, POST_MAX_BYTES};
use crate::output::{AppError, ExitCode};
use crate::store;
use chrono::Utc;

pub fn post(
    home: &A2aHome,
    cfg: &Config,
    from: &PeerId,
    text: &str,
    refs: serde_json::Value,
) -> Result<u64, AppError> {
    scan_or_refuse(text, "SECRET_IN_POST")?;
    let len = text.len();
    if len > POST_MAX_BYTES {
        return Err(AppError::new(
            "POST_TOO_LARGE",
            ExitCode::Precondition,
            format!("text is {len} bytes, exceeding cap {POST_MAX_BYTES} bytes"),
        ));
    }

    let entry = FeedEntry {
        seq: 0,
        from: from.to_string(),
        ts: Utc::now(),
        text: text.to_string(),
        refs,
        extra: serde_json::Map::new(),
    };
    let root = mesh_root(home);
    let mut row = serde_json::to_value(entry).map_err(|e| data_err("serializing feed entry", e))?;
    let mut log = store::open_named(&root, "feed", cfg.mesh.feed_max_bytes)
        .map_err(|e| io_err("opening feed.jsonl", e))?;
    log.append_json(&mut row)
        .map_err(|e| io_err("appending feed.jsonl", e))
}

pub fn read(home: &A2aHome, since_seq: u64) -> Result<Vec<FeedEntry>, AppError> {
    let root = mesh_root(home);
    let mut entries = Vec::new();
    for value in store::read_json_named(&root, "feed", since_seq)
        .map_err(|e| io_err("reading feed.jsonl", e))?
    {
        if is_rotation_marker(&value) {
            continue;
        }
        entries.push(
            serde_json::from_value(value).map_err(|e| data_err("decoding feed entry", e))?,
        );
    }
    Ok(entries)
}

fn is_rotation_marker(value: &serde_json::Value) -> bool {
    value
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind == "events_rotated")
}

fn io_err(context: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}

fn data_err(context: &'static str, e: serde_json::Error) -> AppError {
    AppError::new("DATA", ExitCode::Internal, format!("{context}: {e}"))
}
