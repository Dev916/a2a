use crate::config::Config;
use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::{feed, FeedEntry};
use crate::output::{AppError, ExitCode};
use crate::store;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;

const CURSORS_FILE: &str = "cursors.json";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginCursor {
    #[serde(default)]
    pub last_seq: u64,
    #[serde(default)]
    pub compacted_before_seq: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct CursorFile {
    #[serde(default)]
    origins: BTreeMap<String, OriginCursor>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CursorStore {
    origins: BTreeMap<String, OriginCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorAdvance {
    pub origin: String,
    pub origin_seq: u64,
}

impl CursorAdvance {
    pub fn new(origin: impl Into<String>, origin_seq: u64) -> Self {
        Self {
            origin: origin.into(),
            origin_seq,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestDecision {
    Accepted { local_seq: u64 },
    Duplicate,
    Compacted { from_seq: u64 },
}

#[derive(Clone, Debug, PartialEq)]
pub enum WantReply {
    Entries(Vec<FeedEntry>),
    Compacted { from_seq: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FeedCompaction {
    pub removed: usize,
    pub compacted_before: BTreeMap<String, u64>,
}

impl CursorStore {
    pub fn load_or_rebuild(home: &A2aHome) -> Result<Self, AppError> {
        let scanned = scan_origin_maxima(home)?;
        let path = cursor_path(home);
        let mut store = match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<CursorFile>(&bytes) {
                Ok(file) => CursorStore {
                    origins: file.origins,
                },
                Err(_) => return Ok(Self::from_scan(scanned)),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::from_scan(scanned));
            }
            Err(e) => return Err(io_err("reading cursors.json", e)),
        };
        for (origin, cursor) in store.origins.iter_mut() {
            let max_seq = scanned.get(origin).copied().unwrap_or(0);
            if cursor.last_seq > max_seq {
                cursor.last_seq = max_seq;
            }
        }
        for (origin, max_seq) in scanned {
            store.origins.entry(origin).or_insert_with(|| OriginCursor {
                last_seq: max_seq,
                compacted_before_seq: 0,
            });
        }
        for cursor in store.origins.values_mut() {
            cursor.last_seq = cursor.last_seq.max(cursor.compacted_before_seq.saturating_sub(1));
        }
        Ok(store)
    }

    fn from_scan(scanned: BTreeMap<String, u64>) -> Self {
        CursorStore {
            origins: scanned
                .into_iter()
                .map(|(origin, last_seq)| {
                    (
                        origin,
                        OriginCursor {
                            last_seq,
                            compacted_before_seq: 0,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn persist(&self, home: &A2aHome) -> Result<(), AppError> {
        let path = cursor_path(home);
        if let Some(parent) = path.parent() {
            create_dir_0700(parent).map_err(|e| io_err("creating gateway cursor dir", e))?;
        }
        let bytes = serde_json::to_vec_pretty(&CursorFile {
            origins: self.origins.clone(),
        })
        .map_err(data_err)?;
        store::write_atomic(&path, &bytes).map_err(|e| io_err("writing cursors.json", e))
    }

    pub fn starting_cursors(&self) -> BTreeMap<String, u64> {
        self.origins
            .iter()
            .map(|(origin, cursor)| (origin.clone(), cursor.last_seq))
            .collect()
    }

    pub fn apply_advance(&mut self, advance: CursorAdvance) {
        let cursor = self.origins.entry(advance.origin).or_default();
        cursor.last_seq = cursor.last_seq.max(advance.origin_seq);
    }

    pub fn last_seq(&self, origin: &str) -> u64 {
        self.origins
            .get(origin)
            .map(|cursor| cursor.last_seq)
            .unwrap_or(0)
    }

    pub fn compacted_before_seq(&self, origin: &str) -> u64 {
        self.origins
            .get(origin)
            .map(|cursor| cursor.compacted_before_seq)
            .unwrap_or(0)
    }

    pub fn set_compacted_before_seq(&mut self, origin: impl Into<String>, seq: u64) {
        let cursor = self.origins.entry(origin.into()).or_default();
        cursor.compacted_before_seq = seq;
    }

    fn raise_compacted_before_seq(&mut self, origin: impl Into<String>, seq: u64) {
        let cursor = self.origins.entry(origin.into()).or_default();
        cursor.compacted_before_seq = cursor.compacted_before_seq.max(seq);
    }
}

pub fn append_remote_entry(
    home: &A2aHome,
    cfg: &Config,
    cursors: &mut CursorStore,
    frame_origin: &str,
    mut entry: FeedEntry,
) -> Result<IngestDecision, AppError> {
    if frame_origin.is_empty() {
        return Err(AppError::new(
            "FEED_ORIGIN_INVALID",
            ExitCode::Precondition,
            "feed frame origin is empty",
        ));
    }
    feed::validate_post_text(&entry.text)?;
    let origin_seq = entry.origin_seq.unwrap_or(entry.seq);
    if origin_seq == 0 {
        return Err(AppError::new(
            "FEED_ORIGIN_SEQ_INVALID",
            ExitCode::Precondition,
            "feed entry origin_seq must be greater than zero",
        ));
    }
    let compacted_before = cursors.compacted_before_seq(frame_origin);
    if origin_seq < compacted_before {
        return Ok(IngestDecision::Compacted {
            from_seq: compacted_before,
        });
    }
    let on_disk_max = scan_origin_maxima(home)?
        .remove(frame_origin)
        .unwrap_or(0);
    let seen = cursors.last_seq(frame_origin).max(on_disk_max);
    if origin_seq <= seen {
        cursors.apply_advance(CursorAdvance::new(frame_origin, seen));
        return Ok(IngestDecision::Duplicate);
    }

    entry.seq = 0;
    entry.origin = Some(frame_origin.to_string());
    entry.origin_seq = Some(origin_seq);
    let local_seq = feed::append_entry(home, cfg, entry)?;
    cursors.apply_advance(CursorAdvance::new(frame_origin, origin_seq));
    Ok(IngestDecision::Accepted { local_seq })
}

pub fn entries_for_want(
    home: &A2aHome,
    origin: &str,
    from_seq: u64,
    cursors: &CursorStore,
) -> Result<WantReply, AppError> {
    let compacted_before = cursors.compacted_before_seq(origin);
    if from_seq < compacted_before {
        return Ok(WantReply::Compacted {
            from_seq: compacted_before,
        });
    }
    let local = feed::existing_gateway_id(home);
    let entries = feed::read_with_local_origin(home, 0, local.as_deref())?
        .into_iter()
        .filter(|entry| {
            entry.origin.as_deref() == Some(origin)
                && entry.origin_seq.is_some_and(|seq| seq >= from_seq)
        })
        .collect();
    Ok(WantReply::Entries(entries))
}

pub fn compacted_err(from_seq: u64) -> crate::mesh::wire::Frame {
    crate::mesh::wire::Frame::Err {
        code: "COMPACTED".to_string(),
        detail: json!({ "from_seq": from_seq }).to_string(),
    }
}

pub fn replicate_feed_once(
    cfg: &Config,
    from: &A2aHome,
    from_gateway_id: &str,
    to: &A2aHome,
    to_cursors: &mut CursorStore,
) -> Result<usize, AppError> {
    let entries = deltas_for_link(from, from_gateway_id, &to_cursors.starting_cursors())?;
    let mut accepted = 0usize;
    for entry in entries {
        let Some(origin) = entry.origin.clone() else {
            continue;
        };
        if let IngestDecision::Accepted { .. } =
            append_remote_entry(to, cfg, to_cursors, &origin, entry)?
        {
            accepted += 1;
        }
    }
    to_cursors.persist(to)?;
    Ok(accepted)
}

pub fn deltas_for_link(
    home: &A2aHome,
    local_gateway_id: &str,
    link_cursors: &BTreeMap<String, u64>,
) -> Result<Vec<FeedEntry>, AppError> {
    let mut entries = Vec::new();
    for entry in feed::read_with_local_origin(home, 0, Some(local_gateway_id))? {
        let Some(origin) = entry.origin.as_deref() else {
            continue;
        };
        let Some(origin_seq) = entry.origin_seq else {
            continue;
        };
        if origin_seq > link_cursors.get(origin).copied().unwrap_or(0) {
            entries.push(entry);
        }
    }
    entries.sort_by(|a, b| {
        a.origin
            .cmp(&b.origin)
            .then_with(|| a.origin_seq.cmp(&b.origin_seq))
            .then_with(|| a.seq.cmp(&b.seq))
    });
    Ok(entries)
}

pub fn compact_feed(
    home: &A2aHome,
    _cfg: &Config,
    now: DateTime<Utc>,
    retain: Duration,
    cursors: &mut CursorStore,
) -> Result<FeedCompaction, AppError> {
    let root = home.root().join("mesh");
    if !root.exists() {
        return Ok(FeedCompaction {
            removed: 0,
            compacted_before: BTreeMap::new(),
        });
    }
    let cutoff = now - retain;
    let rows = store::read_json_named(&root, "feed", 0).map_err(|e| io_err("reading feed.jsonl", e))?;
    let mut kept = Vec::new();
    let mut compacted_before = BTreeMap::<String, u64>::new();
    let mut removed = 0usize;
    let mut max_seq = 0u64;
    let mut max_kept_seq = 0u64;

    for value in rows {
        if feed::is_feed_bookkeeping(&value) {
            continue;
        }
        max_seq = max_seq.max(value.get("seq").and_then(Value::as_u64).unwrap_or(0));
        let entry = match serde_json::from_value::<FeedEntry>(value.clone()) {
            Ok(entry) => entry,
            Err(_) => {
                max_kept_seq = max_kept_seq.max(value.get("seq").and_then(Value::as_u64).unwrap_or(0));
                kept.push(value);
                continue;
            }
        };
        let Some(origin) = entry.origin.clone() else {
            max_kept_seq = max_kept_seq.max(entry.seq);
            kept.push(value);
            continue;
        };
        let Some(origin_seq) = entry.origin_seq else {
            max_kept_seq = max_kept_seq.max(entry.seq);
            kept.push(value);
            continue;
        };
        if entry.ts < cutoff {
            removed += 1;
            let watermark = origin_seq.saturating_add(1);
            compacted_before
                .entry(origin)
                .and_modify(|seq| *seq = (*seq).max(watermark))
                .or_insert(watermark);
        } else {
            max_kept_seq = max_kept_seq.max(entry.seq);
            kept.push(value);
        }
    }

    for (origin, seq) in &compacted_before {
        cursors.raise_compacted_before_seq(origin.clone(), *seq);
    }
    if removed > 0 {
        if max_kept_seq < max_seq {
            kept.push(json!({
                "seq": max_seq,
                "kind": "feed_compacted",
                "ts": now,
                "compacted_before": compacted_before,
            }));
        }
        write_feed_window(&root, &kept)?;
        let rotated = root.join("feed.jsonl.1");
        match std::fs::remove_file(&rotated) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("removing compacted feed generation", e)),
        }
        cursors.persist(home)?;
    }
    Ok(FeedCompaction {
        removed,
        compacted_before,
    })
}

fn scan_origin_maxima(home: &A2aHome) -> Result<BTreeMap<String, u64>, AppError> {
    let root = home.root().join("mesh");
    if !root.exists() {
        return Ok(BTreeMap::new());
    }
    let rows = store::read_json_named(&root, "feed", 0).map_err(|e| io_err("reading feed.jsonl", e))?;
    let mut out = BTreeMap::<String, u64>::new();
    for value in rows {
        if feed::is_feed_bookkeeping(&value) {
            continue;
        }
        let Ok(entry) = serde_json::from_value::<FeedEntry>(value) else {
            continue;
        };
        if let (Some(origin), Some(origin_seq)) = (entry.origin, entry.origin_seq) {
            out.entry(origin)
                .and_modify(|seq| *seq = (*seq).max(origin_seq))
                .or_insert(origin_seq);
        }
    }
    Ok(out)
}

fn write_feed_window(root: &std::path::Path, rows: &[Value]) -> Result<(), AppError> {
    create_dir_0700(root).map_err(|e| io_err("creating mesh root", e))?;
    let mut bytes = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut bytes, row).map_err(data_err)?;
        bytes.push(b'\n');
    }
    store::write_atomic(&root.join("feed.jsonl"), &bytes).map_err(|e| io_err("writing feed.jsonl", e))
}

fn cursor_path(home: &A2aHome) -> PathBuf {
    home.root().join("mesh").join("gateway").join(CURSORS_FILE)
}

fn io_err(context: &'static str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{context}: {e}"))
}

fn data_err(e: serde_json::Error) -> AppError {
    AppError::new("DATA", ExitCode::Internal, e.to_string())
}
