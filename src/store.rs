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

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

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
}

impl EventLog {
    /// Open (creating if needed) `<worker_dir>/events.jsonl` for appending.
    ///
    /// Scans the existing log for the last complete, parseable line to resume
    /// the `seq` counter, and repairs a torn tail via `set_len` before any
    /// append can extend it.
    pub fn open(worker_dir: &Path) -> io::Result<Self> {
        let path = worker_dir.join("events.jsonl");
        let mut last_seq = 0u64;
        if path.exists() {
            let data = fs::read(&path)?;
            let mut end_of_complete = 0usize;
            for line in complete_lines(&data) {
                if let Ok(e) = serde_json::from_slice::<crate::events::Envelope>(line.bytes_) {
                    last_seq = e.seq;
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
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&path)?;
        Ok(Self {
            file,
            next_seq: last_seq + 1,
        })
    }

    /// Stamp `env.seq`, append it as one complete line, and `sync_data` before
    /// returning. Returns the assigned seq.
    pub fn append(&mut self, env: &mut crate::events::Envelope) -> io::Result<u64> {
        env.seq = self.next_seq;
        let mut line = serde_json::to_vec(env)?;
        line.push(b'\n');
        self.file.write_all(&line)?; // one write; O_APPEND
        self.file.sync_data()?; // WAL durability before any side effect
        self.next_seq += 1;
        Ok(env.seq)
    }
}

/// Read every parseable envelope with `seq > since_seq`, tolerating a torn
/// trailing line (it and everything after it are dropped, without an error).
pub fn read_events(worker_dir: &Path, since_seq: u64) -> io::Result<Vec<crate::events::Envelope>> {
    let path = worker_dir.join("events.jsonl");
    if !path.exists() {
        return Ok(vec![]);
    }
    let data = fs::read(&path)?;
    let mut out = vec![];
    for line in complete_lines(&data) {
        match serde_json::from_slice::<crate::events::Envelope>(line.bytes_) {
            Ok(e) if e.seq > since_seq => out.push(e),
            Ok(_) => {}
            Err(_) => break, // torn/garbled: drop this and everything after
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
    Ok(out)
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
