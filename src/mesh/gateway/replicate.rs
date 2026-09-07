use crate::config::Config;
use crate::home::{create_dir_0700, A2aHome};
use crate::mesh::registry::GatewayRow;
use crate::mesh::wire::{Frame, OriginDigest, OriginWant};
use crate::mesh::{
    feed,
    keys::SystemClock,
    work::{verify_envelope, WorkAddressing, WorkEnvelope},
    work_store::{self, WorkSource},
    FeedEntry,
};
use crate::output::{AppError, ExitCode};
use crate::store;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Mutex as StdMutex;

const CURSORS_FILE: &str = "cursors.json";
static CURSOR_PERSIST_LOCK: StdMutex<()> = StdMutex::new(());
pub const DEFAULT_WANT_ENTRY_CAP: usize = 128;
pub const DEFAULT_REORDER_BUFFER_CAP: usize = 1024;

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
    Rejected { reason: String },
}

#[derive(Clone, Debug, PartialEq)]
pub enum WantReply {
    Entries(Vec<FeedEntry>),
    Compacted { from_seq: u64 },
}

impl WantReply {
    pub fn entries(&self) -> &[FeedEntry] {
        match self {
            WantReply::Entries(entries) => entries,
            WantReply::Compacted { .. } => &[],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FeedCompaction {
    pub removed: usize,
    pub compacted_before: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReorderEviction {
    pub origin: String,
    pub origin_seq: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FeedReceiveReport {
    pub accepted: usize,
    pub duplicates: usize,
    pub compacted: usize,
    pub rejected: usize,
    pub buffered: usize,
    pub evictions: Vec<ReorderEviction>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MeteredFeedReceiveReport {
    pub receive: FeedReceiveReport,
    pub deferred: bool,
    pub feed_entry_refunds: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FeedReorderBuffers {
    cap_per_origin: usize,
    origins: BTreeMap<String, BTreeMap<u64, FeedEntry>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationHealth {
    pub per_origin_lag: BTreeMap<String, i64>,
    pub last_anti_entropy: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DriftEvent {
    pub kind: &'static str,
    pub origin: String,
    pub lag: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AntiEntropyReport {
    pub wants: Vec<OriginWant>,
    pub repaired_entries: usize,
    pub health: ReplicationHealth,
    pub drift: Vec<DriftEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AntiEntropyCycle {
    sustained_lag: Duration,
    drift_after_cycles: u64,
}

pub struct AntiEntropyApply<'a> {
    pub home: &'a A2aHome,
    pub cfg: &'a Config,
    pub cursors: &'a mut CursorStore,
    pub peer_origin: &'a str,
    pub remote_digest: Vec<OriginDigest>,
    pub repair_entries: Vec<FeedEntry>,
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerRevocationTracker {
    missing_cycles: BTreeMap<String, u8>,
}

impl CursorStore {
    pub fn load_cached(home: &A2aHome) -> Result<Self, AppError> {
        match std::fs::read(cursor_path(home)) {
            Ok(bytes) => {
                let file: CursorFile = serde_json::from_slice(&bytes).map_err(data_err)?;
                Ok(CursorStore {
                    origins: file.origins,
                })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(io_err("reading cursors.json", e)),
        }
    }

    pub fn load_or_rebuild(home: &A2aHome) -> Result<Self, AppError> {
        let scanned = scan_origin_sequences(home)?;
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
            let prefix = scanned
                .get(origin)
                .map(|seqs| contiguous_prefix(seqs, cursor.compacted_before_seq))
                .unwrap_or(0);
            if cursor.last_seq > prefix {
                cursor.last_seq = prefix;
            }
        }
        for (origin, seqs) in scanned {
            store.origins.entry(origin).or_insert_with(|| OriginCursor {
                last_seq: contiguous_prefix(&seqs, 0),
                compacted_before_seq: 0,
            });
        }
        for cursor in store.origins.values_mut() {
            cursor.last_seq = cursor
                .last_seq
                .max(cursor.compacted_before_seq.saturating_sub(1));
        }
        Ok(store)
    }

    fn from_scan(scanned: BTreeMap<String, BTreeSet<u64>>) -> Self {
        CursorStore {
            origins: scanned
                .into_iter()
                .map(|(origin, seqs)| {
                    (
                        origin,
                        OriginCursor {
                            last_seq: contiguous_prefix(&seqs, 0),
                            compacted_before_seq: 0,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn persist(&self, home: &A2aHome) -> Result<(), AppError> {
        let _guard = CURSOR_PERSIST_LOCK.lock().map_err(|_| {
            AppError::new(
                "INTERNAL",
                ExitCode::Internal,
                "gateway cursor persistence lock is poisoned",
            )
        })?;
        self.persist_unlocked(home)
    }

    pub fn persist_merged(
        &self,
        home: &A2aHome,
        owned_origins: &[String],
    ) -> Result<(), AppError> {
        let _guard = CURSOR_PERSIST_LOCK.lock().map_err(|_| {
            AppError::new(
                "INTERNAL",
                ExitCode::Internal,
                "gateway cursor persistence lock is poisoned",
            )
        })?;
        let mut merged = match std::fs::read(cursor_path(home)) {
            Ok(bytes) => {
                let file: CursorFile = serde_json::from_slice(&bytes).map_err(data_err)?;
                CursorStore {
                    origins: file.origins,
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => CursorStore::default(),
            Err(e) => return Err(io_err("reading cursors.json", e)),
        };
        for origin in owned_origins {
            let Some(incoming) = self.origins.get(origin) else {
                continue;
            };
            let stored = merged.origins.entry(origin.clone()).or_default();
            stored.last_seq = stored.last_seq.max(incoming.last_seq);
            stored.compacted_before_seq = stored
                .compacted_before_seq
                .max(incoming.compacted_before_seq);
        }
        merged.persist_unlocked(home)
    }

    pub(crate) fn retain_origins(&mut self, owned_origins: &[String]) {
        self.origins
            .retain(|origin, _| owned_origins.iter().any(|owned| owned == origin));
    }

    fn persist_unlocked(&self, home: &A2aHome) -> Result<(), AppError> {
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

    pub fn drop_origin(&mut self, origin: &str) -> bool {
        self.origins.remove(origin).is_some()
    }

    fn set_last_seq(&mut self, origin: impl Into<String>, origin_seq: u64) {
        let cursor = self.origins.entry(origin.into()).or_default();
        cursor.last_seq = origin_seq.max(cursor.compacted_before_seq.saturating_sub(1));
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

impl FeedReorderBuffers {
    pub fn new(cap_per_origin: usize) -> Self {
        Self {
            cap_per_origin,
            origins: BTreeMap::new(),
        }
    }

    fn insert(
        &mut self,
        origin: &str,
        origin_seq: u64,
        entry: FeedEntry,
    ) -> (bool, Option<ReorderEviction>) {
        let pending = self.origins.entry(origin.to_string()).or_default();
        if pending.contains_key(&origin_seq) {
            return (false, None);
        }
        pending.insert(origin_seq, entry);
        if pending.len() <= self.cap_per_origin {
            return (true, None);
        }
        let evicted_seq = pending
            .keys()
            .next()
            .copied()
            .expect("pending is non-empty");
        pending.remove(&evicted_seq);
        (
            true,
            Some(ReorderEviction {
                origin: origin.to_string(),
                origin_seq: evicted_seq,
            }),
        )
    }

    fn take(&mut self, origin: &str, origin_seq: u64) -> Option<FeedEntry> {
        let pending = self.origins.get_mut(origin)?;
        let entry = pending.remove(&origin_seq);
        if pending.is_empty() {
            self.origins.remove(origin);
        }
        entry
    }
}

impl Default for FeedReorderBuffers {
    fn default() -> Self {
        Self::new(DEFAULT_REORDER_BUFFER_CAP)
    }
}

impl AntiEntropyCycle {
    pub fn new(sustained_lag: Duration, drift_after_cycles: u64) -> Self {
        Self {
            sustained_lag,
            drift_after_cycles,
        }
    }

    pub fn apply(&self, input: AntiEntropyApply<'_>) -> Result<AntiEntropyReport, AppError> {
        let wants = wants_from_digest(input.home, input.cursors, &input.remote_digest)?;
        let before = digest_map(&digest_for_home(input.home)?);
        let mut reorder = FeedReorderBuffers::default();
        let mut repaired_entries = 0usize;
        for mut entry in input.repair_entries {
            let origin = entry
                .origin
                .clone()
                .unwrap_or_else(|| input.peer_origin.to_string());
            entry.origin = Some(origin.clone());
            let report = receive_feed_entries(
                input.home,
                input.cfg,
                input.cursors,
                &mut reorder,
                &origin,
                vec![entry],
            )?;
            repaired_entries += report.accepted;
        }
        input.cursors.persist(input.home)?;

        let after = digest_map(&digest_for_home(input.home)?);
        let mut health = ReplicationHealth {
            per_origin_lag: BTreeMap::new(),
            last_anti_entropy: Some(input.now),
        };
        let mut drift = Vec::new();
        for remote in input.remote_digest {
            let local_max = after.get(&remote.origin).map(|d| d.max_seq).unwrap_or(0);
            let lag = remote.max_seq.saturating_sub(local_max) as i64;
            health.per_origin_lag.insert(remote.origin.clone(), lag);
            let old_lag = before
                .get(&remote.origin)
                .map(|d| remote.max_seq.saturating_sub(d.max_seq) as i64)
                .unwrap_or(remote.max_seq as i64);
            if lag > 0
                && old_lag > 0
                && self.drift_after_cycles <= 1
                && self.sustained_lag.num_seconds() >= 0
            {
                drift.push(DriftEvent {
                    kind: "drift",
                    origin: remote.origin,
                    lag,
                });
            }
        }
        Ok(AntiEntropyReport {
            wants,
            repaired_entries,
            health,
            drift,
        })
    }
}

impl PeerRevocationTracker {
    pub fn closed_after_discovery<I, S>(
        &mut self,
        local_gateway_id: &str,
        active_peer_gateway_ids: I,
        discovered: &[GatewayRow],
    ) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let live: BTreeSet<&str> = discovered
            .iter()
            .filter(|row| row.gateway_id != local_gateway_id)
            .map(|row| row.gateway_id.as_str())
            .collect();
        let mut closed = Vec::new();
        for peer in active_peer_gateway_ids {
            let peer = peer.as_ref();
            if live.contains(peer) {
                self.missing_cycles.remove(peer);
                continue;
            }
            let cycles = self.missing_cycles.entry(peer.to_string()).or_default();
            *cycles = cycles.saturating_add(1);
            if *cycles >= 2 {
                closed.push(peer.to_string());
            }
        }
        for peer in &closed {
            self.missing_cycles.remove(peer);
        }
        closed
    }
}

pub fn append_remote_entry(
    home: &A2aHome,
    cfg: &Config,
    cursors: &mut CursorStore,
    frame_origin: &str,
    entry: FeedEntry,
) -> Result<IngestDecision, AppError> {
    append_remote_entry_with_prepared_work(home, cfg, cursors, frame_origin, entry, None)
}

fn append_remote_entry_with_prepared_work(
    home: &A2aHome,
    cfg: &Config,
    cursors: &mut CursorStore,
    frame_origin: &str,
    mut entry: FeedEntry,
    prepared_work: Option<Result<Option<crate::mesh::work::VerifiedEnvelope>, AppError>>,
) -> Result<IngestDecision, AppError> {
    if frame_origin.is_empty() {
        return Err(AppError::new(
            "FEED_ORIGIN_INVALID",
            ExitCode::Precondition,
            "feed frame origin is empty",
        ));
    }
    let origin_seq = entry.origin_seq.unwrap_or(entry.seq);
    if origin_seq == 0 {
        return Ok(IngestDecision::Rejected {
            reason: "FEED_ORIGIN_SEQ_INVALID".to_string(),
        });
    }
    if let Err(err) = feed::validate_post_text(&entry.text) {
        cursors.apply_advance(CursorAdvance::new(frame_origin, origin_seq));
        log_feed_ingest_refused(home, frame_origin, origin_seq, err.code);
        return Ok(IngestDecision::Rejected {
            reason: err.code.to_string(),
        });
    }
    let compacted_before = cursors.compacted_before_seq(frame_origin);
    if origin_seq < compacted_before {
        return Ok(IngestDecision::Compacted {
            from_seq: compacted_before,
        });
    }
    let mut scanned = scan_origin_sequences(home)?;
    let present = scanned.entry(frame_origin.to_string()).or_default();
    if present.contains(&origin_seq) {
        let prefix = contiguous_prefix(present, compacted_before);
        cursors.set_last_seq(frame_origin, prefix);
        return Ok(IngestDecision::Duplicate);
    }

    let previous = cursors.last_seq(frame_origin);
    let verified_work = match prepared_work.unwrap_or_else(|| prepare_work_refs(home, cfg, &entry)) {
        Ok(verified) => verified,
        Err(err) => {
            cursors.apply_advance(CursorAdvance::new(frame_origin, origin_seq));
            log_feed_ingest_refused(home, frame_origin, origin_seq, err.code);
            return Ok(IngestDecision::Rejected {
                reason: err.code.to_string(),
            });
        }
    };
    if let Some(verified) = verified_work {
        if let Err(err) = work_store::store_offer(home, verified, WorkSource::Pool) {
            cursors.apply_advance(CursorAdvance::new(frame_origin, origin_seq));
            log_feed_ingest_refused(home, frame_origin, origin_seq, err.code);
            return Ok(IngestDecision::Rejected {
                reason: err.code.to_string(),
            });
        }
    }

    entry.seq = 0;
    entry.origin = Some(frame_origin.to_string());
    entry.origin_seq = Some(origin_seq);
    let local_seq = feed::append_entry(home, cfg, entry)?;
    present.insert(origin_seq);
    let prefix = contiguous_prefix(present, compacted_before);
    if origin_seq <= previous.saturating_add(1) {
        cursors.set_last_seq(frame_origin, previous.max(origin_seq).max(prefix));
    } else {
        cursors.set_last_seq(frame_origin, prefix);
    }
    Ok(IngestDecision::Accepted { local_seq })
}

pub fn receive_feed_entries(
    home: &A2aHome,
    cfg: &Config,
    cursors: &mut CursorStore,
    reorder: &mut FeedReorderBuffers,
    frame_origin: &str,
    entries: Vec<FeedEntry>,
) -> Result<FeedReceiveReport, AppError> {
    let mut report = FeedReceiveReport::default();
    for entry in entries {
        let origin_seq = entry.origin_seq.unwrap_or(entry.seq);
        if origin_seq == 0 {
            report.rejected += 1;
            continue;
        }
        let expected = cursors.last_seq(frame_origin).saturating_add(1);
        if origin_seq > expected {
            let (inserted, eviction) = reorder.insert(frame_origin, origin_seq, entry);
            if inserted {
                report.buffered += 1;
            }
            if let Some(eviction) = eviction {
                report.evictions.push(eviction);
            }
            continue;
        }
        apply_entry_for_report(home, cfg, cursors, frame_origin, entry, &mut report)?;
        drain_contiguous_buffer(home, cfg, cursors, reorder, frame_origin, &mut report)?;
    }
    Ok(report)
}

pub fn receive_feed_entries_metered<F>(
    home: &A2aHome,
    cfg: &Config,
    cursors: &mut CursorStore,
    reorder: &mut FeedReorderBuffers,
    frame_origin: &str,
    entries: Vec<FeedEntry>,
    mut admit: F,
) -> Result<MeteredFeedReceiveReport, AppError>
where
    F: FnMut(&FeedEntry, bool) -> bool,
{
    let mut metered = MeteredFeedReceiveReport::default();
    for entry in entries {
        let origin_seq = entry.origin_seq.unwrap_or(entry.seq);
        if origin_seq == 0 {
            metered.receive.rejected += 1;
            continue;
        }
        let expected = cursors.last_seq(frame_origin).saturating_add(1);
        if origin_seq > expected {
            let (inserted, eviction) = reorder.insert(frame_origin, origin_seq, entry);
            if inserted {
                metered.receive.buffered += 1;
            }
            if let Some(eviction) = eviction {
                metered.receive.evictions.push(eviction);
            }
            continue;
        }
        if !apply_entry_for_metered_report(
            home,
            cfg,
            cursors,
            frame_origin,
            entry,
            &mut metered,
            &mut admit,
        )? {
            metered.deferred = true;
            break;
        }
        loop {
            let expected = cursors.last_seq(frame_origin).saturating_add(1);
            let Some(entry) = reorder.take(frame_origin, expected) else {
                break;
            };
            if !apply_entry_for_metered_report(
                home,
                cfg,
                cursors,
                frame_origin,
                entry,
                &mut metered,
                &mut admit,
            )? {
                metered.deferred = true;
                break;
            }
        }
        if metered.deferred {
            break;
        }
    }
    Ok(metered)
}

pub fn entries_for_want(
    home: &A2aHome,
    origin: &str,
    from_seq: u64,
    cursors: &CursorStore,
) -> Result<WantReply, AppError> {
    entries_for_want_capped(home, origin, from_seq, cursors, DEFAULT_WANT_ENTRY_CAP)
}

pub fn entries_for_want_capped(
    home: &A2aHome,
    origin: &str,
    from_seq: u64,
    cursors: &CursorStore,
    max_entries: usize,
) -> Result<WantReply, AppError> {
    let compacted_before = cursors.compacted_before_seq(origin);
    if from_seq < compacted_before {
        return Ok(WantReply::Compacted {
            from_seq: compacted_before,
        });
    }
    let local = feed::existing_gateway_id(home);
    let mut entries: Vec<FeedEntry> = feed::read_with_local_origin(home, 0, local.as_deref())?
        .into_iter()
        .filter(|entry| {
            entry.origin.as_deref() == Some(origin)
                && entry.origin_seq.is_some_and(|seq| seq >= from_seq)
        })
        .collect();
    entries.sort_by(|a, b| {
        a.origin_seq
            .cmp(&b.origin_seq)
            .then_with(|| a.seq.cmp(&b.seq))
    });
    let mut contiguous = Vec::new();
    let mut expected = from_seq;
    for entry in entries {
        if contiguous.len() == max_entries {
            break;
        }
        match entry.origin_seq {
            Some(seq) if seq == expected => {
                contiguous.push(entry);
                expected = expected.saturating_add(1);
            }
            Some(seq) if seq < expected => {}
            _ => break,
        }
    }
    Ok(WantReply::Entries(contiguous))
}

pub fn want_for_origins<I, S>(origins: I, cursors: &CursorStore) -> Frame
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let origins = origins
        .into_iter()
        .map(|origin| {
            let origin = origin.as_ref().to_string();
            OriginWant {
                from_seq: cursors.last_seq(&origin).saturating_add(1),
                origin,
            }
        })
        .collect();
    Frame::want(origins)
}

pub fn digest_for_home(home: &A2aHome) -> Result<Vec<OriginDigest>, AppError> {
    let local = feed::existing_gateway_id(home);
    let mut origins = BTreeMap::<String, OriginDigest>::new();
    for entry in feed::read_with_local_origin(home, 0, local.as_deref())? {
        let (Some(origin), Some(origin_seq)) = (entry.origin, entry.origin_seq) else {
            continue;
        };
        origins
            .entry(origin.clone())
            .and_modify(|digest| {
                digest.max_seq = digest.max_seq.max(origin_seq);
                digest.count = digest.count.saturating_add(1);
            })
            .or_insert(OriginDigest {
                origin,
                max_seq: origin_seq,
                count: 1,
            });
    }
    Ok(origins.into_values().collect())
}

pub fn wants_from_digest(
    home: &A2aHome,
    cursors: &CursorStore,
    remote_digest: &[OriginDigest],
) -> Result<Vec<OriginWant>, AppError> {
    let scanned = scan_origin_sequences(home)?;
    let mut wants = Vec::new();
    for remote in remote_digest {
        let seqs = scanned.get(&remote.origin);
        let local_max = seqs
            .and_then(|seqs| seqs.iter().next_back().copied())
            .unwrap_or(0);
        let local_count_same_range = seqs
            .map(|seqs| seqs.range(..=remote.max_seq).count() as u64)
            .unwrap_or(0);
        let from_seq = if local_max < remote.max_seq {
            seqs.map(|seqs| contiguous_prefix(seqs, cursors.compacted_before_seq(&remote.origin)))
                .unwrap_or(0)
                .saturating_add(1)
        } else if local_count_same_range != remote.count {
            first_missing(seqs, 1, remote.max_seq)
        } else {
            continue;
        };
        wants.push(OriginWant {
            origin: remote.origin.clone(),
            from_seq,
        });
    }
    Ok(wants)
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
    let rows =
        store::read_json_named(&root, "feed", 0).map_err(|e| io_err("reading feed.jsonl", e))?;
    let mut kept = Vec::new();
    let mut compacted_before = BTreeMap::<String, u64>::new();
    let mut removed = 0usize;
    let mut max_seq = 0u64;
    let mut max_kept_seq = 0u64;
    // Compaction rewrites the window and may drop the rotated generation, so
    // unstamped local entries must be claimed persistently here — otherwise
    // the dense read-time numbering (see feed::read_with_local_origin) would
    // shift when earlier local entries disappear.
    let local_origin = feed::existing_gateway_id(home);
    let mut next_local: u64 = 1;

    for mut value in rows {
        if feed::is_feed_bookkeeping(&value) {
            continue;
        }
        max_seq = max_seq.max(value.get("seq").and_then(Value::as_u64).unwrap_or(0));
        let mut entry = match serde_json::from_value::<FeedEntry>(value.clone()) {
            Ok(entry) => entry,
            Err(_) => {
                max_kept_seq =
                    max_kept_seq.max(value.get("seq").and_then(Value::as_u64).unwrap_or(0));
                kept.push(value);
                continue;
            }
        };
        if let Some(local) = local_origin.as_deref() {
            if entry.origin.as_deref() == Some(local) {
                if let Some(origin_seq) = entry.origin_seq {
                    next_local = next_local.max(origin_seq.saturating_add(1));
                }
            } else if entry.origin.is_none() && entry.origin_seq.is_none() {
                entry.origin = Some(local.to_string());
                entry.origin_seq = Some(next_local);
                value["origin"] = Value::String(local.to_string());
                value["origin_seq"] = Value::from(next_local);
                next_local = next_local.saturating_add(1);
            }
        }
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
        let owned_origins = compacted_before.keys().cloned().collect::<Vec<_>>();
        cursors.persist_merged(home, &owned_origins)?;
    }
    Ok(FeedCompaction {
        removed,
        compacted_before,
    })
}

fn apply_entry_for_report(
    home: &A2aHome,
    cfg: &Config,
    cursors: &mut CursorStore,
    frame_origin: &str,
    entry: FeedEntry,
    report: &mut FeedReceiveReport,
) -> Result<(), AppError> {
    match append_remote_entry(home, cfg, cursors, frame_origin, entry)? {
        IngestDecision::Accepted { .. } => report.accepted += 1,
        IngestDecision::Duplicate => report.duplicates += 1,
        IngestDecision::Compacted { .. } => report.compacted += 1,
        IngestDecision::Rejected { .. } => report.rejected += 1,
    }
    Ok(())
}

fn apply_entry_for_metered_report<F>(
    home: &A2aHome,
    cfg: &Config,
    cursors: &mut CursorStore,
    frame_origin: &str,
    entry: FeedEntry,
    metered: &mut MeteredFeedReceiveReport,
    admit: &mut F,
) -> Result<bool, AppError>
where
    F: FnMut(&FeedEntry, bool) -> bool,
{
    let reaches_append = entry_reaches_append(home, cursors, frame_origin, &entry)?;
    if !admit(&entry, false) {
        return Ok(false);
    }
    let prepared_work = reaches_append.then(|| prepare_metered_work_refs(home, cfg, &entry));
    let will_append = reaches_append
        && prepared_work
            .as_ref()
            .is_some_and(|prepared| prepared.is_ok());
    if will_append && !admit(&entry, true) {
        return Ok(false);
    }
    let decision = append_remote_entry_with_prepared_work(
        home,
        cfg,
        cursors,
        frame_origin,
        entry,
        prepared_work,
    )?;
    if will_append && !matches!(decision, IngestDecision::Accepted { .. }) {
        metered.feed_entry_refunds = metered.feed_entry_refunds.saturating_add(1);
    }
    match decision {
        IngestDecision::Accepted { .. } => metered.receive.accepted += 1,
        IngestDecision::Duplicate => metered.receive.duplicates += 1,
        IngestDecision::Compacted { .. } => metered.receive.compacted += 1,
        IngestDecision::Rejected { .. } => metered.receive.rejected += 1,
    }
    Ok(true)
}

fn entry_reaches_append(
    home: &A2aHome,
    cursors: &CursorStore,
    frame_origin: &str,
    entry: &FeedEntry,
) -> Result<bool, AppError> {
    let origin_seq = entry.origin_seq.unwrap_or(entry.seq);
    if origin_seq == 0 || feed::validate_post_text(&entry.text).is_err() {
        return Ok(false);
    }
    if origin_seq < cursors.compacted_before_seq(frame_origin) {
        return Ok(false);
    }
    Ok(!scan_origin_sequences(home)?
        .get(frame_origin)
        .is_some_and(|present| present.contains(&origin_seq)))
}

fn drain_contiguous_buffer(
    home: &A2aHome,
    cfg: &Config,
    cursors: &mut CursorStore,
    reorder: &mut FeedReorderBuffers,
    frame_origin: &str,
    report: &mut FeedReceiveReport,
) -> Result<(), AppError> {
    loop {
        let expected = cursors.last_seq(frame_origin).saturating_add(1);
        let Some(entry) = reorder.take(frame_origin, expected) else {
            return Ok(());
        };
        apply_entry_for_report(home, cfg, cursors, frame_origin, entry, report)?;
    }
}

fn prepare_work_refs(
    home: &A2aHome,
    cfg: &Config,
    entry: &FeedEntry,
) -> Result<Option<crate::mesh::work::VerifiedEnvelope>, AppError> {
    feed::validate_refs_size(&entry.refs, cfg.mesh.work.max_pool_bytes)?;
    let Some(work) = entry.refs.get("work") else {
        return Ok(None);
    };
    let envelope: WorkEnvelope = serde_json::from_value(work.clone()).map_err(|e| {
        AppError::new(
            "WORK_SCHEMA_MISSING",
            ExitCode::Precondition,
            format!("decoding refs.work: {e}"),
        )
    })?;
    crate::mesh::work::validate_envelope_hygiene(&envelope, cfg.mesh.work.max_pool_bytes as usize)?;
    let verified = verify_envelope(home, &envelope, &SystemClock)?;
    if verified.envelope.addressing != WorkAddressing::Pool {
        return Err(AppError::new(
            "WORK_TOKEN_MISMATCH",
            ExitCode::Precondition,
            "feed refs.work must carry a pool-addressed work envelope",
        ));
    }
    Ok(Some(verified))
}

fn prepare_metered_work_refs(
    home: &A2aHome,
    cfg: &Config,
    entry: &FeedEntry,
) -> Result<Option<crate::mesh::work::VerifiedEnvelope>, AppError> {
    let verified = prepare_work_refs(home, cfg, entry)?;
    if let Some(verified) = &verified {
        work_store::ensure_pool_offer_capacity(home, &verified.envelope.work_id)?;
    }
    Ok(verified)
}

fn log_feed_ingest_refused(
    home: &A2aHome,
    origin: &str,
    origin_seq: u64,
    code: &str,
) {
    let dir = home.root().join("mesh").join("gateway");
    if create_dir_0700(&dir).is_err() {
        return;
    }
    if let Ok(mut f) = crate::exec::open_append_0600(&dir.join(super::GATEWAY_JSONL)) {
        let line = json!({
            "ts": Utc::now(),
            "kind": "feed",
            "origin": origin,
            "origin_seq": origin_seq,
            "action": "work_ingest_refused",
            "code": code,
        });
        if let Ok(mut bytes) = serde_json::to_vec(&line) {
            bytes.push(b'\n');
            let _ = std::io::Write::write_all(&mut f, &bytes);
        }
    }
}

fn scan_origin_sequences(home: &A2aHome) -> Result<BTreeMap<String, BTreeSet<u64>>, AppError> {
    let root = home.root().join("mesh");
    if !root.exists() {
        return Ok(BTreeMap::new());
    }
    let rows =
        store::read_json_named(&root, "feed", 0).map_err(|e| io_err("reading feed.jsonl", e))?;
    let mut out = BTreeMap::<String, BTreeSet<u64>>::new();
    for value in rows {
        if feed::is_feed_bookkeeping(&value) {
            continue;
        }
        let Ok(entry) = serde_json::from_value::<FeedEntry>(value) else {
            continue;
        };
        if let (Some(origin), Some(origin_seq)) = (entry.origin, entry.origin_seq) {
            out.entry(origin).or_default().insert(origin_seq);
        }
    }
    Ok(out)
}

fn contiguous_prefix(seqs: &BTreeSet<u64>, compacted_before: u64) -> u64 {
    let mut next = compacted_before.max(1);
    let mut prefix = next.saturating_sub(1);
    while seqs.contains(&next) {
        prefix = next;
        next = next.saturating_add(1);
    }
    prefix
}

fn first_missing(seqs: Option<&BTreeSet<u64>>, from_seq: u64, through_seq: u64) -> u64 {
    let mut seq = from_seq;
    while seq <= through_seq {
        if !seqs.map(|seqs| seqs.contains(&seq)).unwrap_or(false) {
            return seq;
        }
        seq = seq.saturating_add(1);
    }
    through_seq.saturating_add(1)
}

fn digest_map(digest: &[OriginDigest]) -> BTreeMap<String, OriginDigest> {
    digest
        .iter()
        .map(|origin| (origin.origin.clone(), origin.clone()))
        .collect()
}

fn write_feed_window(root: &std::path::Path, rows: &[Value]) -> Result<(), AppError> {
    create_dir_0700(root).map_err(|e| io_err("creating mesh root", e))?;
    let mut bytes = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut bytes, row).map_err(data_err)?;
        bytes.push(b'\n');
    }
    store::write_atomic(&root.join("feed.jsonl"), &bytes)
        .map_err(|e| io_err("writing feed.jsonl", e))
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
