//! Crash-safe on-disk primitives (spec §4.6).
//!
//! Two shapes of durability live here:
//!
//! * [`write_atomic`] for whole-file state (`state.json`, `result.*`, indexes):
//!   a 0600 temp file in the SAME directory → `fsync(file)` → `rename` →
//!   `fsync(dir)`. A reader therefore only ever observes the complete old or
//!   the complete new content, never a partial write, and no temp litter
//!   survives a successful call.
//! * [`EventLog`] for the WAL (`events.jsonl`): a single-writer `O_APPEND`
//!   file, one complete line per `write`, `sync_data` before the caller is
//!   allowed to act on the event (append-before-apply).
//!
//! A crash mid-append can still leave a torn trailing line. Readers
//! ([`read_events`]) tolerate it by dropping the torn tail, and
//! [`EventLog::open`] REPAIRS it (truncates back to the last complete line) so
//! the next append is not glued onto a fragment.
//!
//! ## Rotation (spec §4.5)
//!
//! A worker whose transport floods would otherwise grow `events.jsonl` without
//! bound — and every append reopens the log and scans it, so an unbounded log
//! is an unbounded per-event cost, not merely disk. At the cap the current file
//! is RENAMED to [`EVENTS_ROTATED_FILE`] and a fresh one is opened, which keeps
//! the ordering the whole design rests on (the log leads, the snapshot
//! follows): the rename is atomic, `seq` continues across the split, and the
//! new file's FIRST line is a marker naming what moved. Reads
//! ([`read_events`], and `next_seq` recovery in [`EventLog::open_capped`]) span
//! both files, so a fold from zero after one rotation is byte-for-byte the same
//! fold as one over an unrotated log.
//!
//! Retention is a SINGLE generation: a second rotation replaces `.1` and its
//! marker says `discarded_prior_generation: true`, so the gap a rebuild would
//! hit is recorded rather than inferred.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// The worker's write-ahead log.
pub const EVENTS_FILE: &str = "events.jsonl";
/// The one retained prior generation of [`EVENTS_FILE`].
pub const EVENTS_ROTATED_FILE: &str = "events.jsonl.1";

/// Write `bytes` to `path` atomically: 0600 temp in the same dir → fsync →
/// rename → fsync(dir).
pub fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path.parent().expect("write_atomic needs a parent dir");
    let tmp = dir.join(format!(
        ".{}.tmp-{}",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Single-writer append-only handle on a worker's `events.jsonl`.
pub struct EventLog {
    file: File,
    next_seq: u64,
    /// The directory, kept so a rotation can rename in place.
    dir: PathBuf,
    /// Basename without `.jsonl`; `"events"` for the worker WAL.
    base: String,
    /// Bytes in the CURRENT file (`0` right after a rotation).
    bytes: u64,
    /// Rotate past this; `0` disables rotation entirely.
    max_bytes: u64,
}

impl EventLog {
    /// Open (creating if needed) `<worker_dir>/events.jsonl` for appending,
    /// with the default cap ([`crate::config::DEFAULT_EVENTS_MAX_BYTES`]).
    ///
    /// The overwhelming majority of writers want exactly this — one envelope
    /// per command — so the cap stays a detail of the two writers that can
    /// actually produce volume (see [`Self::open_capped`]).
    pub fn open(worker_dir: &Path) -> io::Result<Self> {
        Self::open_named(worker_dir, "events", crate::config::DEFAULT_EVENTS_MAX_BYTES)
    }

    /// [`Self::open`] with an explicit cap in bytes; `0` disables rotation.
    ///
    /// Scans the existing log for the last complete, parseable line to resume
    /// the `seq` counter, and repairs a torn tail via `set_len` before any
    /// append can extend it. The scan spans `events.jsonl.1` too, so a `seq`
    /// resumed after a rotation still continues the whole history rather than
    /// restarting inside the new generation.
    pub fn open_capped(worker_dir: &Path, max_bytes: u64) -> io::Result<Self> {
        Self::open_named(worker_dir, "events", max_bytes)
    }

    /// Open (creating if needed) `<dir>/<base>.jsonl` for appending, with one
    /// retained rotated generation at `<base>.jsonl.1`.
    pub fn open_named(dir: &Path, base: &str, max_bytes: u64) -> io::Result<Self> {
        let names = LogNames::new(base)?;
        let path = dir.join(&names.current);
        let mut last_seq = 0u64;
        let mut bytes = 0u64;
        if path.exists() {
            let data = fs::read(&path)?;
            let mut end_of_complete = 0usize;
            for line in complete_lines(&data) {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(line.bytes_) {
                    if let Some(seq) = v.get("seq").and_then(serde_json::Value::as_u64) {
                        last_seq = last_seq.max(seq);
                    }
                    end_of_complete = line.end;
                } else {
                    break;
                }
            }
            if end_of_complete < data.len() {
                // torn tail (or trailing garbage): repair so future appends stay parseable
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(end_of_complete as u64)?;
                f.sync_all()?;
            }
            bytes = end_of_complete as u64; // post-repair length
        }
        // `next_seq` is the maximum over BOTH generations. The rotated file is
        // only consulted when the current one yields nothing, which is not a
        // shortcut but the same answer for less IO: seqs are strictly
        // increasing across the split, so a current file with any parseable
        // line already holds the maximum. (After a rename the fresh file is
        // empty for exactly one append — that is the window this covers.)
        if last_seq == 0 {
            let rotated = dir.join(&names.rotated);
            if rotated.exists() {
                let data = fs::read(&rotated)?;
                for line in complete_lines(&data) {
                    match serde_json::from_slice::<serde_json::Value>(line.bytes_) {
                        Ok(v) => {
                            if let Some(seq) = v.get("seq").and_then(serde_json::Value::as_u64) {
                                last_seq = last_seq.max(seq);
                            }
                        }
                        Err(_) => break, // torn tail: it and everything after are not history
                    }
                }
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&path)?;
        Ok(Self {
            file,
            next_seq: last_seq + 1,
            dir: dir.to_path_buf(),
            base: base.to_string(),
            bytes,
            max_bytes,
        })
    }

    /// Stamp `env.seq`, append it as one complete line, and `sync_data` before
    /// returning. Returns the assigned seq.
    ///
    /// Rotates FIRST when this line would take the file past the cap, so the
    /// caller's event lands in the new generation whole — a line is never split
    /// across the two files.
    pub fn append(&mut self, env: &mut crate::events::Envelope) -> io::Result<u64> {
        env.seq = self.next_seq;
        let mut line = serde_json::to_vec(env)?;
        line.push(b'\n');
        // `self.bytes > 0` is what keeps an oversized single line from rotating
        // a FRESH file forever: rotating an empty log buys nothing and the next
        // append would be just as oversized.
        if self.max_bytes != 0
            && self.bytes > 0
            && self.bytes + line.len() as u64 > self.max_bytes
        {
            self.rotate(env)?;
            // The marker consumed a seq, so the caller's envelope is restamped
            // and re-serialized (the seq is IN the bytes).
            env.seq = self.next_seq;
            line = serde_json::to_vec(env)?;
            line.push(b'\n');
        }
        self.file.write_all(&line)?; // one write; O_APPEND
        self.file.sync_data()?; // WAL durability before any side effect
        self.bytes += line.len() as u64;
        self.next_seq = env.seq + 1;
        Ok(env.seq)
    }

    /// Stamp an object-shaped JSON row with `seq`, append it as one complete
    /// line, and rotate the named log with the same durability as [`Self::append`].
    ///
    /// This is for named logs whose row shape is not [`crate::events::Envelope`]
    /// (currently the mesh feed). The rotation marker is a bookkeeping row
    /// without a `seq`, so application-level cursors remain gapless.
    pub fn append_json(&mut self, value: &mut serde_json::Value) -> io::Result<u64> {
        let Some(obj) = value.as_object_mut() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log row must be a JSON object",
            ));
        };
        obj.insert(
            "seq".to_string(),
            serde_json::Value::Number(self.next_seq.into()),
        );
        let mut line = serde_json::to_vec(value)?;
        line.push(b'\n');
        if self.max_bytes != 0
            && self.bytes > 0
            && self.bytes + line.len() as u64 > self.max_bytes
        {
            self.rotate_json()?;
            let obj = value.as_object_mut().expect("object shape checked above");
            obj.insert(
                "seq".to_string(),
                serde_json::Value::Number(self.next_seq.into()),
            );
            line = serde_json::to_vec(value)?;
            line.push(b'\n');
        }
        self.file.write_all(&line)?;
        self.file.sync_data()?;
        self.bytes += line.len() as u64;
        let seq = self.next_seq;
        self.next_seq += 1;
        Ok(seq)
    }

    /// Rename the current generation aside, open a fresh log, and write the
    /// rotation marker as its first line.
    ///
    /// `trigger` is the envelope whose append forced this: the marker inherits
    /// its `epoch` and `transport` so the row belongs to the same run as the
    /// events either side of it, and a fold fencing a stale epoch fences the
    /// marker with them.
    fn rotate(&mut self, trigger: &crate::events::Envelope) -> io::Result<()> {
        use crate::events::{EventClass, EventKind, Envelope};

        let names = LogNames::new(&self.base)?;
        let discarded_prior_generation = self.rotate_files(&names)?;
        self.bytes = 0;

        // `next_seq` is NOT reset: seqs continue across the split, which is what
        // lets `tail --since` and the fold treat the two files as one log.
        let last_rotated_seq = self.next_seq.saturating_sub(1);
        let mut marker = Envelope::new(
            trigger.epoch,
            trigger.transport,
            EventClass::Lifecycle,
            EventKind::EventsRotated,
            serde_json::json!({
                "rotated_to": names.rotated,
                "last_rotated_seq": last_rotated_seq,
                "discarded_prior_generation": discarded_prior_generation,
            }),
            chrono::Utc::now(),
        );
        marker.seq = self.next_seq;
        let mut line = serde_json::to_vec(&marker)?;
        line.push(b'\n');
        self.file.write_all(&line)?;
        self.file.sync_data()?;
        self.bytes += line.len() as u64;
        self.next_seq += 1;
        Ok(())
    }

    fn rotate_json(&mut self) -> io::Result<()> {
        let names = LogNames::new(&self.base)?;
        let discarded_prior_generation = self.rotate_files(&names)?;
        self.bytes = 0;
        let marker = serde_json::json!({
            "kind": "events_rotated",
            "ts": chrono::Utc::now(),
            "rotated_to": names.rotated,
            "last_rotated_seq": self.next_seq.saturating_sub(1),
            "discarded_prior_generation": discarded_prior_generation,
        });
        let mut line = serde_json::to_vec(&marker)?;
        line.push(b'\n');
        self.file.write_all(&line)?;
        self.file.sync_data()?;
        self.bytes += line.len() as u64;
        Ok(())
    }

    fn rotate_files(&mut self, names: &LogNames) -> io::Result<bool> {
        let path = self.dir.join(&names.current);
        let rotated = self.dir.join(&names.rotated);
        // Single-generation retention: a `.1` already here is about to be lost,
        // and that is a fact the log records rather than one a reader infers.
        let discarded_prior_generation = rotated.exists();
        fs::rename(&path, &rotated)?; // atomic, replaces any existing `.1`
        File::open(&self.dir)?.sync_all()?; // the rename is durable before anything else
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&path)?;
        // ... and so is the fresh file's own directory entry, or a crash could
        // lose an `fsync`ed append to a file nobody can find.
        File::open(&self.dir)?.sync_all()?;
        Ok(discarded_prior_generation)
    }
}

/// [`EventLog::open_named`] as a module-level helper for callers that do not
/// already spell the log type.
pub fn open_named(dir: &Path, base: &str, max_bytes: u64) -> io::Result<EventLog> {
    EventLog::open_named(dir, base, max_bytes)
}

/// Read every parseable envelope with `seq > since_seq`, tolerating a torn
/// trailing line (it and everything after it are dropped, without an error).
///
/// Spans both generations — `events.jsonl.1` first, then `events.jsonl` — so a
/// rotation is invisible to every reader: the returned sequence is the same one
/// an unrotated log would have produced, minus whatever a *second* rotation
/// already discarded.
pub fn read_events(worker_dir: &Path, since_seq: u64) -> io::Result<Vec<crate::events::Envelope>> {
    read_events_named(worker_dir, "events", since_seq)
}

/// Read every parseable envelope with `seq > since_seq` from a named log.
pub fn read_events_named(
    dir: &Path,
    base: &str,
    since_seq: u64,
) -> io::Result<Vec<crate::events::Envelope>> {
    let names = LogNames::new(base)?;
    let mut out = vec![];
    read_one_file(&dir.join(&names.rotated), since_seq, &mut out)?;
    read_one_file(&dir.join(&names.current), since_seq, &mut out)?;
    Ok(out)
}

/// Read object-shaped rows from a named log with `seq > since_seq`, spanning
/// the retained generation first and then the current file.
pub fn read_json_named(
    dir: &Path,
    base: &str,
    since_seq: u64,
) -> io::Result<Vec<serde_json::Value>> {
    let names = LogNames::new(base)?;
    let mut out = vec![];
    read_one_json_file(&dir.join(&names.rotated), since_seq, &mut out)?;
    read_one_json_file(&dir.join(&names.current), since_seq, &mut out)?;
    Ok(out)
}

struct LogNames {
    current: String,
    rotated: String,
}

impl LogNames {
    fn new(base: &str) -> io::Result<Self> {
        if base.is_empty() || base.contains('/') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log base must be a non-empty file basename",
            ));
        }
        let current = format!("{base}.jsonl");
        let rotated = format!("{current}.1");
        Ok(Self { current, rotated })
    }
}

/// One generation, with the torn-tail tolerance applied to IT — a torn line in
/// `.1` must not swallow the current file, which is newer and intact.
fn read_one_file(
    path: &Path,
    since_seq: u64,
    out: &mut Vec<crate::events::Envelope>,
) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let data = fs::read(path)?;
    for line in complete_lines(&data) {
        match serde_json::from_slice::<crate::events::Envelope>(line.bytes_) {
            Ok(e) if e.seq > since_seq => out.push(e),
            Ok(_) => {}
            Err(_) => return Ok(()), // torn/garbled: drop this and everything after
        }
    }
    // a final line without a trailing newline may still be complete JSON — try it
    if let Some(tail) = trailing_fragment(&data) {
        if let Ok(e) = serde_json::from_slice::<crate::events::Envelope>(tail) {
            if e.seq > since_seq {
                out.push(e);
            }
        }
    }
    Ok(())
}

fn read_one_json_file(
    path: &Path,
    since_seq: u64,
    out: &mut Vec<serde_json::Value>,
) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let data = fs::read(path)?;
    for line in complete_lines(&data) {
        match serde_json::from_slice::<serde_json::Value>(line.bytes_) {
            Ok(v) => push_json_after(v, since_seq, out),
            Err(_) => return Ok(()),
        }
    }
    if let Some(tail) = trailing_fragment(&data) {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(tail) {
            push_json_after(v, since_seq, out);
        }
    }
    Ok(())
}

fn push_json_after(value: serde_json::Value, since_seq: u64, out: &mut Vec<serde_json::Value>) {
    if value
        .get("seq")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|seq| seq > since_seq)
    {
        out.push(value);
    }
}

struct Line<'a> {
    bytes_: &'a [u8],
    end: usize,
}

/// Newline-terminated lines only (offset-tracked); the unterminated tail is excluded.
fn complete_lines(data: &[u8]) -> Vec<Line<'_>> {
    let mut out = vec![];
    let mut start = 0;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            out.push(Line {
                bytes_: &data[start..i],
                end: i + 1,
            });
            start = i + 1;
        }
    }
    out
}

fn trailing_fragment(data: &[u8]) -> Option<&[u8]> {
    match data.iter().rposition(|&b| b == b'\n') {
        Some(i) if i + 1 < data.len() => Some(&data[i + 1..]),
        None if !data.is_empty() => Some(data),
        _ => None,
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventClass, EventKind, Envelope, Transport};
    use serde_json::json;

    /// One envelope with `pad` bytes of filler, so a test can say how big a
    /// line is instead of discovering it.
    fn padded(pad: usize) -> Envelope {
        Envelope::new(
            3,
            Transport::AppServer,
            EventClass::Trace,
            EventKind::AgentMessage,
            json!({ "text": "x".repeat(pad) }),
            chrono::Utc::now(),
        )
    }

    fn line_len(env: &Envelope) -> u64 {
        serde_json::to_vec(env).unwrap().len() as u64 + 1
    }

    fn markers(dir: &Path) -> Vec<Envelope> {
        read_events(dir, 0)
            .unwrap()
            .into_iter()
            .filter(|e| e.kind == EventKind::EventsRotated)
            .collect()
    }

    /// The threshold is a threshold: it fires when the next line would cross
    /// it, once, and the fresh generation then absorbs the rest.
    #[test]
    fn rotation_happens_once_and_only_once_at_the_threshold() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let one = line_len(&padded(100));
        // Room for six lines, then a seventh that crosses: exactly one
        // rotation, with the remainder comfortably inside the new generation.
        let mut log = EventLog::open_capped(dir, one * 6 + one / 2).unwrap();
        for _ in 0..8 {
            log.append(&mut padded(100)).unwrap();
        }
        assert!(dir.join(EVENTS_ROTATED_FILE).exists(), "it rotated");
        let m = markers(dir);
        assert_eq!(m.len(), 1, "exactly one rotation, not one per append");
        assert_eq!(
            m[0].raw,
            json!({
                "rotated_to": "events.jsonl.1",
                "last_rotated_seq": 6,
                "discarded_prior_generation": false,
            }),
            "the marker's shape is pinned"
        );
        assert_eq!(m[0].class, EventClass::Lifecycle);
        assert_eq!(m[0].epoch, 3, "the triggering envelope's epoch");
        assert_eq!(m[0].transport, Transport::AppServer, "and its transport");
        assert!(m[0].raw.get("fsm").is_none(), "bookkeeping, not an FSM edge");

        // seq continues across the split: 8 events + 1 marker, gapless from 1.
        let seqs: Vec<u64> = read_events(dir, 0).unwrap().iter().map(|e| e.seq).collect();
        assert_eq!(seqs, (1..=9).collect::<Vec<_>>(), "gapless across the split");
        assert_eq!(m[0].seq, 7, "the marker is the new file's first line");
    }

    /// The `bytes > 0` guard, stated as the loop it prevents: a single line
    /// bigger than the whole cap must be written, not rotated at forever.
    #[test]
    fn an_oversized_line_on_a_fresh_log_does_not_rotate() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut log = EventLog::open_capped(dir, 64).unwrap();
        let mut big = padded(4096);
        assert_eq!(log.append(&mut big).unwrap(), 1);
        assert!(
            !dir.join(EVENTS_ROTATED_FILE).exists(),
            "an empty log has nothing to rotate away"
        );
        assert!(markers(dir).is_empty(), "and nothing to mark");
        assert_eq!(read_events(dir, 0).unwrap().len(), 1, "it was written");
        // The NEXT append is what may rotate — the file is no longer empty.
        log.append(&mut padded(10)).unwrap();
        assert!(dir.join(EVENTS_ROTATED_FILE).exists());
    }

    /// Reopening after a rotation resumes from the whole history's maximum, not
    /// from the new generation's first line.
    #[test]
    fn open_after_a_rotation_continues_the_seq() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let one = line_len(&padded(100));
        let mut log = EventLog::open_capped(dir, one * 3 + one / 2).unwrap();
        for _ in 0..5 {
            log.append(&mut padded(100)).unwrap();
        }
        let previous_max = read_events(dir, 0)
            .unwrap()
            .iter()
            .map(|e| e.seq)
            .max()
            .unwrap();
        drop(log);

        let mut log = EventLog::open_capped(dir, 0).unwrap();
        let mut next = padded(10);
        assert_eq!(log.append(&mut next).unwrap(), previous_max + 1);

        // ... and even when the fresh generation holds nothing but the marker's
        // predecessor: truncate the current file and reopen. `.1` is then the
        // only history there is, and it still decides the seq.
        let cur = dir.join(EVENTS_FILE);
        std::fs::write(&cur, b"").unwrap();
        let rotated_max = read_events(dir, 0).unwrap().last().unwrap().seq;
        let mut log = EventLog::open_capped(dir, 0).unwrap();
        let mut after = padded(10);
        assert_eq!(log.append(&mut after).unwrap(), rotated_max + 1);
    }

    /// Single-generation retention, said out loud: the second rotation drops a
    /// generation and the marker is where that shows.
    #[test]
    fn a_second_rotation_replaces_the_first_and_says_so() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let one = line_len(&padded(100));
        let mut log = EventLog::open_capped(dir, one * 2 + one / 2).unwrap();
        for _ in 0..9 {
            log.append(&mut padded(100)).unwrap();
        }
        let m = markers(dir);
        assert!(!m.is_empty(), "the readable window holds a marker");
        let last = m.last().unwrap();
        assert_eq!(
            last.raw["discarded_prior_generation"],
            json!(true),
            "a generation was lost, and the log says which way"
        );
        assert_eq!(last.raw["rotated_to"], json!("events.jsonl.1"));
        // Only two generations exist on disk, ever.
        let jsonl: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("events.jsonl"))
            .collect();
        assert_eq!(jsonl.len(), 2, "one live file and one retained: {jsonl:?}");
        // The window that survives is still internally gapless and ordered...
        let seqs: Vec<u64> = read_events(dir, 0).unwrap().iter().map(|e| e.seq).collect();
        assert!(
            seqs.windows(2).all(|w| w[1] == w[0] + 1),
            "the surviving window is contiguous: {seqs:?}"
        );
        // ... and it no longer starts at 1, which is exactly what the marker's
        // `discarded_prior_generation` exists to explain.
        assert!(seqs[0] > 1, "the oldest generation is gone: {seqs:?}");
    }

    /// `open` is the default cap and nothing more — the const dependency runs
    /// store → config, never the other way.
    #[test]
    fn open_is_open_capped_at_the_configured_default() {
        let tmp = tempfile::tempdir().unwrap();
        let log = EventLog::open(tmp.path()).unwrap();
        assert_eq!(log.max_bytes, crate::config::DEFAULT_EVENTS_MAX_BYTES);
        let capped = EventLog::open_capped(tmp.path(), 0).unwrap();
        assert_eq!(capped.max_bytes, 0, "0 is a value, not an unset");
    }

    #[test]
    fn a_named_log_rotates_and_reads_across_the_split() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let one = line_len(&padded(100));
        let mut log = EventLog::open_named(dir, "feed", one * 2 + one / 2).unwrap();
        for _ in 0..3 {
            log.append(&mut padded(100)).unwrap();
        }

        assert!(dir.join("feed.jsonl").exists());
        assert!(dir.join("feed.jsonl.1").exists());
        assert!(!dir.join(EVENTS_FILE).exists(), "the base name is honored");
        let seqs: Vec<u64> = read_events_named(dir, "feed", 0)
            .unwrap()
            .iter()
            .map(|e| e.seq)
            .collect();
        assert_eq!(
            seqs,
            (1..=4).collect::<Vec<_>>(),
            "seqs stay continuous across the named split"
        );
        let marker = read_events_named(dir, "feed", 0)
            .unwrap()
            .into_iter()
            .find(|e| e.kind == EventKind::EventsRotated)
            .unwrap();
        assert_eq!(marker.raw["rotated_to"], json!("feed.jsonl.1"));
    }
}
