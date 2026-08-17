//! `a2a gc [--age <dur>] [--dry-run]` — reclaim finished workers (spec §7).
//!
//! A worker directory is a WAL, a brief, staged context and a result: valuable
//! while the work is live, pure disk once it is over. `gc` is how that space
//! comes back, and every rule below exists because deleting the wrong directory
//! is the one mistake in this system that cannot be undone by replaying a log.
//!
//! **Terminal states only, with no force path.** `done`/`failed`/`cancelled` are
//! absorbing; everything else — `running`, `awaiting-input`, `escalated`,
//! `orphaned` — is a worker somebody can still act on. `orphaned` especially:
//! spec §7 makes orphans exactly the population `a2a resume` exists for, so a
//! `--force` that reaped them would delete the session ids that make recovery
//! possible. There is deliberately no such flag in Phase 1; the remedy an
//! operator gets names `a2a kill` first, which lands the worker in `cancelled`
//! through the FSM and therefore through the WAL.
//!
//! **Reconcile before deciding.** A worker whose process died while nobody was
//! watching is still recorded `running` on disk. Reconciling each candidate
//! first (the same read path `status` uses) means gc judges what a worker *is*,
//! not what the last reader happened to observe — otherwise a fleet that
//! finished overnight would be un-reclaimable until someone ran `status` on each
//! member.
//!
//! **Age is anchored to the last event's `ts`**, not to mtime and not to `now`:
//! the WAL is the source of truth about when a worker last did anything, and it
//! is the one clock that survives a copy of the home tree. `--age` defaults to
//! `7d`, because the common reason to want a finished worker's directory is to
//! read a result you only remembered about the next morning.
//!
//! **`reclaimable` and `eligible` are different questions**, and the payload
//! answers both. `reclaimable` is every terminal worker with its size — the
//! disk-usage report ("what is gc holding for me, and how much is it worth?").
//! `eligible` marks the subset `--age` actually admits, which is exactly the set
//! a non-dry run deletes. Reporting only the second would make `gc --dry-run`
//! (7d by default) print nothing on a home full of yesterday's finished workers
//! and leave an operator believing there was nothing to reclaim.
//!
//! **The tombstone is written before the delete.** `index.jsonl` keeps
//! `{id, session_id, rollout_path, cwd, state, removed_at}` for every removal,
//! which is what lets `spawn` refuse to reuse a reclaimed id and what lets a
//! human find the codex session behind a worker that no longer exists. Writing
//! it first means a crash mid-gc costs a tombstone for a directory that is still
//! there (harmless, and visible) rather than a deleted directory whose session
//! id is gone forever.
//!
//! **Traversal safety.** `workers/` is walked with `symlink_metadata`, and an
//! entry is only ever considered when it is a real directory whose name passes
//! [`ids::is_valid`]. A symlink is never followed — `workers/evil-link -> ~/src`
//! must cost a warning, not a source tree — and a name like `..evil` is refused
//! on sight rather than joined onto a path. Everything skipped is announced on
//! stderr: silence about a directory gc declined to touch is how home trees grow
//! mysterious.
//!
//! **`CODEX_HOME` is not ours.** a2a never prunes `~/.codex/sessions` (spec §7,
//! a stated choice): those rollouts are what `codex exec resume` reads, they are
//! shared with the user's own codex usage, and their retention belongs to codex.
//! `doctor` reports their size; gc does not touch them, which is why nothing in
//! this file so much as reads the variable.
//!
//! Phase 4 adds the worktree rule from spec §7 (refuse to reclaim a worker whose
//! git worktree is dirty or unmerged without `--force`); Phase 1 workers have no
//! worktrees, so there is nothing yet to refuse.

use crate::commands::ls::last_event_ts;
use crate::config;
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use crate::state::WorkerSnapshot;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::Duration;

/// Tombstones: one JSON line per reclaimed worker, at the home root.
pub const INDEX_FILE: &str = "index.jsonl";

/// What an operator is told to do about a worker gc will not reap. Named here
/// once so the stderr line and the `--json` payload cannot drift apart.
const NON_TERMINAL_REMEDY: &str = "stop it first with `a2a kill <id>` (or let it finish), then re-run gc — gc never removes a worker that is still actionable";

pub fn run(
    home: &A2aHome,
    age: Duration,
    dry_run: bool,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let (cfg, _origins, warnings) = config::load(home)?;
    let warn = |msg: String| {
        if !quiet {
            eprintln!("a2a: warning: {msg}");
        }
    };
    for w in &warnings {
        warn(w.clone());
    }

    // One clock for the whole sweep: two workers' ages are then comparable, and
    // a long gc cannot reap a worker that was too young when it started.
    let now = Utc::now();
    let age_secs = age.as_secs() as i64;

    let mut reclaimable: Vec<Candidate> = Vec::new();
    let mut kept: Vec<Value> = Vec::new();
    let mut skipped: Vec<Value> = Vec::new();

    for name in entries(home)? {
        let path = home.workers_dir().join(&name);
        // symlink_metadata, never metadata: `is_dir()` on a followed link would
        // report the TARGET's type and invite a recursive delete outside the
        // home tree. This check comes first so the warning names the real
        // hazard even when the name would also have failed validation.
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) => {
                warn(format!("workers/{name}: unreadable ({e}) — skipped"));
                skipped.push(json!({"entry": name, "reason": "unreadable"}));
                continue;
            }
        };
        if meta.file_type().is_symlink() {
            warn(format!(
                "workers/{name}: symlinked entry — skipped (gc never follows a link out of the home tree)"
            ));
            skipped.push(json!({"entry": name, "reason": "symlink"}));
            continue;
        }
        if !meta.is_dir() {
            warn(format!("workers/{name}: not a directory — skipped"));
            skipped.push(json!({"entry": name, "reason": "not a directory"}));
            continue;
        }
        if !ids::is_valid(&name) {
            warn(format!(
                "workers/{name}: not a valid worker id — skipped (gc only removes what it can name)"
            ));
            skipped.push(json!({"entry": name, "reason": "invalid worker id"}));
            continue;
        }

        // Reconciled, not merely read: a worker that died unobserved becomes
        // terminal HERE, before the decision that depends on its state.
        let snap = match reconcile(home, &cfg, &name, now) {
            Ok(s) => s,
            Err(e) => {
                // Unreadable is emphatically not reclaimable — a worker we
                // cannot interpret is the last thing to delete.
                warn(format!("{name}: {}: {} — skipped", e.code, e.message));
                skipped.push(json!({"entry": name, "reason": e.code}));
                continue;
            }
        };
        if !snap.state.is_terminal() {
            kept.push(json!({
                "id": name,
                "state": snap.state.as_str(),
                "reason": "not terminal",
                "remedy": NON_TERMINAL_REMEDY,
            }));
            continue;
        }
        // The WAL's own account of when this worker last did anything. A worker
        // with no readable event falls back to when it entered its terminal
        // state, which is the next-most-honest answer available.
        let anchor = last_event_ts(&path).unwrap_or(snap.entered_state_at);
        let idle_secs = (now - anchor).num_seconds().max(0);
        reclaimable.push(Candidate::new(
            &name,
            &snap,
            &path,
            idle_secs,
            idle_secs >= age_secs,
        ));
    }

    reclaimable.sort_by(|a, b| a.id.cmp(&b.id));
    let reclaimable_bytes: u64 = reclaimable.iter().map(|c| c.bytes).sum();
    let eligible: Vec<&Candidate> = reclaimable.iter().filter(|c| c.eligible).collect();
    let eligible_bytes: u64 = eligible.iter().map(|c| c.bytes).sum();

    let mut removed: Vec<String> = Vec::new();
    let mut reclaimed_bytes = 0u64;
    if !dry_run {
        for c in &eligible {
            // Tombstone first: a crash between the two leaves a tombstone for a
            // directory that still exists (recoverable, and visible in `ls`),
            // never a deleted worker whose session id is unrecoverable.
            append_tombstone(home, &c.tombstone(now))?;
            let dir = home.workers_dir().join(&c.id);
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {
                    removed.push(c.id.clone());
                    reclaimed_bytes += c.bytes;
                }
                Err(e) => {
                    // The tombstone stands: the id stays retired either way, and
                    // a half-removed directory must never be silently reported
                    // as reclaimed.
                    warn(format!("{}: could not remove: {e}", c.id));
                    skipped.push(json!({"entry": c.id, "reason": "remove failed"}));
                }
            }
        }
    }

    if !quiet {
        report(
            Counts {
                reclaimable: reclaimable.len(),
                reclaimable_bytes,
                eligible: eligible.len(),
                eligible_bytes,
                removed: removed.len(),
                reclaimed_bytes,
            },
            &kept,
            age_secs,
            dry_run,
        );
    }

    let data = if json_mode {
        json!({
            "dry_run": dry_run,
            "age_secs": age_secs,
            "reclaimable": reclaimable.iter().map(Candidate::to_json).collect::<Vec<_>>(),
            "reclaimable_bytes": reclaimable_bytes,
            "eligible": eligible.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            "eligible_bytes": eligible_bytes,
            "removed": removed,
            "reclaimed_bytes": reclaimed_bytes,
            "kept": kept,
            "skipped": skipped,
        })
    } else {
        // stdout carries the ids and nothing else — the list a caller pipes into
        // the next command. Under `--dry-run` that is what WOULD go, which is
        // the age-eligible set, not everything that is merely terminal.
        let ids: Vec<&str> = if dry_run {
            eligible.iter().map(|c| c.id.as_str()).collect()
        } else {
            removed.iter().map(String::as_str).collect()
        };
        if ids.is_empty() {
            Value::Null
        } else {
            Value::String(ids.join("\n"))
        }
    };
    Ok(Outcome::from(data))
}

/// A safely-named, terminal worker: reclaimable in principle, and `eligible`
/// when `--age` also admits it.
struct Candidate {
    id: String,
    state: String,
    session_id: Option<String>,
    rollout_path: Option<String>,
    cwd: String,
    bytes: u64,
    last_event_age_secs: i64,
    eligible: bool,
}

impl Candidate {
    fn new(
        id: &str,
        snap: &WorkerSnapshot,
        dir: &Path,
        last_event_age_secs: i64,
        eligible: bool,
    ) -> Self {
        Self {
            id: id.to_string(),
            state: snap.state.as_str().to_string(),
            session_id: snap.session_id.clone(),
            rollout_path: snap
                .rollout_path
                .as_ref()
                .map(|p| p.display().to_string()),
            cwd: snap.cwd.display().to_string(),
            bytes: dir_bytes(dir),
            last_event_age_secs,
            eligible,
        }
    }

    /// The surviving record of a worker (spec §4.5: id → session ids/cwd).
    fn tombstone(&self, now: DateTime<Utc>) -> Value {
        json!({
            "id": self.id,
            "session_id": self.session_id,
            "rollout_path": self.rollout_path,
            "cwd": self.cwd,
            "state": self.state,
            "removed_at": now,
        })
    }

    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "state": self.state,
            "session_id": self.session_id,
            "rollout_path": self.rollout_path,
            "cwd": self.cwd,
            "bytes": self.bytes,
            "last_event_age_secs": self.last_event_age_secs,
            "eligible": self.eligible,
        })
    }
}

/// Every name directly under `workers/`, sorted, with no filtering and no
/// following: classification is the caller's job, entry by entry, so that every
/// rejected entry can be reported rather than silently dropped.
fn entries(home: &A2aHome) -> Result<Vec<String>, AppError> {
    let mut names: Vec<String> = std::fs::read_dir(home.workers_dir())
        .map_err(|e| io_err("listing workers", e))?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    Ok(names)
}

/// Append one tombstone line: O_APPEND, 0600, one complete line, `sync_data`
/// before the caller is allowed to delete anything (the same append-before-act
/// discipline the WAL uses).
fn append_tombstone(home: &A2aHome, tombstone: &Value) -> Result<(), AppError> {
    let path = home.root().join(INDEX_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&path)
        .map_err(|e| io_err("opening index.jsonl", e))?;
    let mut line = serde_json::to_vec(tombstone).map_err(|e| {
        AppError::new(
            "IO",
            ExitCode::Internal,
            format!("serializing a tombstone: {e}"),
        )
    })?;
    line.push(b'\n');
    file.write_all(&line)
        .map_err(|e| io_err("appending a tombstone", e))?;
    file.sync_data()
        .map_err(|e| io_err("syncing index.jsonl", e))?;
    Ok(())
}

/// Bytes held by a worker directory. Walks with `symlink_metadata`, so a link
/// inside a worker dir counts as the link it is and its target is never sized
/// (or, later, deleted) as if it belonged to us.
fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let Ok(meta) = std::fs::symlink_metadata(entry.path()) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        total += if meta.is_dir() {
            dir_bytes(&entry.path())
        } else {
            meta.len()
        };
    }
    total
}

/// Everything the stderr summary counts, so the report cannot be handed the
/// wrong total by a caller passing five bare numbers in the wrong order.
struct Counts {
    reclaimable: usize,
    reclaimable_bytes: u64,
    eligible: usize,
    eligible_bytes: u64,
    removed: usize,
    reclaimed_bytes: u64,
}

/// Human decoration, stderr only. The counts matter more than the ids here:
/// "nothing was reclaimable" and "12 workers, 40 MB" are the two answers an
/// operator running gc actually wants — plus, on a dry run, how much of that
/// the current `--age` would really admit.
fn report(c: Counts, kept: &[Value], age_secs: i64, dry_run: bool) {
    if dry_run {
        eprintln!(
            "a2a: gc --dry-run: {} terminal worker(s) holding {}; {} eligible under --age {} ({}) — nothing was deleted",
            c.reclaimable,
            human_bytes(c.reclaimable_bytes),
            c.eligible,
            human_age(age_secs),
            human_bytes(c.eligible_bytes),
        );
    } else {
        eprintln!(
            "a2a: gc: removed {} worker(s), {} reclaimed ({} terminal worker(s) held back by --age {})",
            c.removed,
            human_bytes(c.reclaimed_bytes),
            c.reclaimable - c.eligible,
            human_age(age_secs),
        );
    }
    let not_terminal = kept
        .iter()
        .filter(|k| k["reason"] == json!("not terminal"))
        .count();
    if not_terminal > 0 {
        eprintln!("a2a: gc: kept {not_terminal} non-terminal worker(s) — {NON_TERMINAL_REMEDY}");
    }
}

/// Echo `--age` back in the spelling it was typed in, so the summary line reads
/// as the flag the operator would pass to change it.
fn human_age(secs: i64) -> String {
    match secs {
        s if s % 86_400 == 0 && s != 0 => format!("{}d", s / 86_400),
        s if s % 3600 == 0 && s != 0 => format!("{}h", s / 3600),
        s if s % 60 == 0 && s != 0 => format!("{}m", s / 60),
        s => format!("{s}s"),
    }
}

fn human_bytes(n: u64) -> String {
    match n {
        n if n < 1024 => format!("{n} B"),
        n if n < 1024 * 1024 => format!("{:.1} KB", n as f64 / 1024.0),
        n if n < 1024 * 1024 * 1024 => format!("{:.1} MB", n as f64 / 1_048_576.0),
        n => format!("{:.1} GB", n as f64 / 1_073_741_824.0),
    }
}

fn io_err(ctx: &str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsm::WorkerState;

    #[test]
    fn tombstone_carries_every_field_the_id_outlives() {
        let tmp = tempfile::tempdir().unwrap();
        let mut snap = WorkerSnapshot::new_for_test("general-0814t1300-aaaa", tmp.path());
        snap.state = WorkerState::Done;
        snap.session_id = Some("sess-9".into());
        snap.rollout_path = Some("/r/9.jsonl".into());
        let c = Candidate::new("general-0814t1300-aaaa", &snap, tmp.path(), 42, true);
        let now = Utc::now();
        let t = c.tombstone(now);
        assert_eq!(t["id"], json!("general-0814t1300-aaaa"));
        assert_eq!(t["session_id"], json!("sess-9"));
        assert_eq!(t["rollout_path"], json!("/r/9.jsonl"));
        assert_eq!(t["cwd"], json!(tmp.path().display().to_string()));
        assert_eq!(t["state"], json!("done"));
        assert!(t["removed_at"].is_string(), "removed_at must be stamped");
    }

    #[test]
    fn tombstones_append_one_line_each_at_0600() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        append_tombstone(&home, &json!({"id": "a"})).unwrap();
        append_tombstone(&home, &json!({"id": "b"})).unwrap();
        let path = home.root().join(INDEX_FILE);
        let text = std::fs::read_to_string(&path).unwrap();
        let ids: Vec<Value> = text
            .lines()
            .map(|l| serde_json::from_str::<Value>(l).unwrap()["id"].clone())
            .collect();
        assert_eq!(ids, vec![json!("a"), json!("b")], "appended, not replaced");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn dir_bytes_sums_files_recursively_and_never_sizes_a_symlink_target() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a"), vec![0u8; 100]).unwrap();
        std::fs::create_dir(tmp.path().join("sub")).unwrap();
        std::fs::write(tmp.path().join("sub/b"), vec![0u8; 50]).unwrap();
        assert_eq!(dir_bytes(tmp.path()), 150);

        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("huge"), vec![0u8; 4096]).unwrap();
        std::os::unix::fs::symlink(outside.path(), tmp.path().join("link")).unwrap();
        assert_eq!(dir_bytes(tmp.path()), 150, "a link is not our disk usage");
        assert_eq!(dir_bytes(&tmp.path().join("missing")), 0);
    }

    #[test]
    fn human_age_echoes_the_flag_spelling() {
        assert_eq!(human_age(604_800), "7d");
        assert_eq!(human_age(7200), "2h");
        assert_eq!(human_age(300), "5m");
        assert_eq!(human_age(45), "45s");
        assert_eq!(human_age(0), "0s");
        assert_eq!(human_age(90), "90s", "not a whole minute");
    }

    #[test]
    fn human_bytes_picks_one_unit() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(2048), "2.0 KB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.0 GB");
    }

    /// The traversal guard, at the unit where it is decided: no name that is not
    /// a well-formed worker id may ever be joined onto `workers/` and removed.
    #[test]
    fn only_well_formed_ids_can_name_a_removal_candidate() {
        for hostile in ["..evil", "not-an-id", "..", ".", "evil-link", "", "../../etc"] {
            assert!(!ids::is_valid(hostile), "{hostile} must never pass");
        }
        assert!(ids::is_valid("general-0814t1300-aaaa"));
    }
}
