//! `a2a gc [--age <dur>] [--dry-run]` — reclaim finished workers (spec §7).
//!
//! A worker directory is a WAL, a brief, staged context and a result: valuable
//! while the work is live, pure disk once it is over. `gc` is how that space
//! comes back, and every rule below exists because deleting the wrong directory
//! is the one mistake in this system that cannot be undone by replaying a log.
//!
//! **Terminal states only, and `--force` does not widen that.**
//! `done`/`failed`/`cancelled` are absorbing; everything else — `running`,
//! `awaiting-input`, `escalated`, `orphaned` — is a worker somebody can still
//! act on. `orphaned` especially: spec §7 makes orphans exactly the population
//! `a2a resume` exists for, so a flag that reaped them would delete the session
//! ids that make recovery possible. Phase 4's `--force` widens the **worktree**
//! rule and nothing else (see below): a non-terminal worker is never reclaimed
//! with it or without it, and the remedy an operator gets names `a2a kill`
//! first, which lands the worker in `cancelled` through the FSM and therefore
//! through the WAL.
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
//! **Somebody else's worker is not gc's to reclaim** (spec §4.8). The sweep
//! covers the caller's own workers plus every unowned one — the same population
//! `ls` shows by default and the same one the mutating commands allow — and
//! `--all` widens it to the machine. A worker held back for that reason is
//! reported in `kept`, exactly like a non-terminal one: gc that silently skips
//! things is gc that leaves an operator wondering where their disk went.
//!
//! **The worktree rails** (spec §7). A worker that ran in its own git worktree
//! holds two things gc must never destroy quietly: uncommitted work in the tree
//! and commits on its branch the base ref never received. Each is a refusal —
//! a `kept` row carrying the remedy that would change the answer — until
//! `--force` says otherwise. git protects only the first of the two (a clean
//! tree whose branch is ahead is removed by `git worktree remove` without a
//! murmur), which is why the second rule has to live here.
//!
//! What no flag reaches is the **branch**: it is the deliverable a2a produces
//! (spec §4.8, "a2a never merges branches"), and it outlives the tree, the
//! worker directory and the tombstone. The tombstone is in fact what keeps it
//! findable, which is why it carries `worktree_branch` and
//! `worktree_base_ref`.
//!
//! Every git call runs `-C <wt.repo>` — the **parent** repo recorded in
//! genesis. It is not derivable at gc time: a linked worktree's `.git` is a
//! pointer file that dies with the directory, and `rev-parse --show-toplevel`
//! run inside a linked worktree answers for that worktree, never its parent.
//! The same recorded repo is what makes a *missing* tree recoverable: gc still
//! owes it a `worktree prune`, and there would be nothing to run that against
//! otherwise. When the parent repo itself is gone, gc asks git nothing, removes
//! the orphaned directory and carries on — an unreachable repo must not make a
//! worker un-reclaimable forever.
//!
//! Order of operations is the irreversibility order: **tombstone, then the
//! tree, then the worker directory**. A worker directory deleted while its
//! worktree was still registered would strand a git registration with no id
//! left to name it, so a failed removal keeps the directory and reports a
//! `skipped` row.

use crate::commands::ls::last_event_ts;
use crate::commands::owner;
use crate::config;
use crate::home::A2aHome;
use crate::ids;
use crate::mesh::{notes, presence, PeerId, Presence};
use crate::output::{AppError, ExitCode, Outcome};
use crate::redact;
use crate::reconcile::reconcile;
use crate::state::{WorkerSnapshot, WorktreeRef};
use crate::worktree;
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

/// ...and for a worker that belongs to somebody else. Same shape, same reason:
/// the `kept` row has to say what would change the answer.
const NOT_OWNER_REMEDY: &str =
    "re-run with `--all` to sweep the whole machine, or as that owner (`A2A_OWNER=<owner> a2a gc`)";

/// How many dirty paths a `kept` row carries. Enough to recognise the work at a
/// glance; not so many that one forgotten `target/` fills the payload.
const DIRTY_SAMPLE: usize = 10;

pub fn run(
    home: &A2aHome,
    age: Duration,
    dry_run: bool,
    all: bool,
    force: bool,
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
    // One caller identity for the whole sweep, for the same reason there is one
    // clock: a scope that could shift mid-sweep is not a scope.
    let caller = owner::caller();

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
        // Ownership before terminality: a worker somebody else owns is not this
        // caller's to judge, whatever state it is in.
        if !all && !owner::permitted(snap.owner.as_deref(), &caller) {
            kept.push(json!({
                "id": name,
                "state": snap.state.as_str(),
                "owner": snap.owner,
                "reason": "not owner",
                "remedy": NOT_OWNER_REMEDY,
            }));
            continue;
        }
        if !snap.state.is_terminal() {
            kept.push(json!({
                "id": name,
                "state": snap.state.as_str(),
                "reason": "not terminal",
                "remedy": NON_TERMINAL_REMEDY,
            }));
            continue;
        }
        // What git can still say about this worker's tree, decided once, here:
        // the answer feeds the size report, the `--json` row, the refusal and
        // the removal, and a second reading could disagree with the first.
        let wt = match snap.worktree.clone() {
            None => None,
            Some(wt) => match plan_for(&name, &wt, force, &warn) {
                Some(plan) => Some(WtInfo { wt, plan }),
                // git answered with an error: a tree gc cannot judge is the
                // last one it should delete, so the worker stays put.
                None => {
                    skipped.push(json!({"entry": name, "reason": "WORKTREE_STATUS_FAILED"}));
                    continue;
                }
            },
        };
        // The WAL's own account of when this worker last did anything. A worker
        // with no readable event falls back to when it entered its terminal
        // state, which is the next-most-honest answer available.
        let anchor = last_event_ts(&path).unwrap_or(snap.entered_state_at);
        let idle_secs = (now - anchor).num_seconds().max(0);
        reclaimable.push(Candidate::new(
            &name,
            &snap,
            &path,
            wt,
            idle_secs,
            idle_secs >= age_secs,
        ));
    }

    reclaimable.sort_by(|a, b| a.id.cmp(&b.id));
    let reclaimable_bytes: u64 = reclaimable.iter().map(|c| c.bytes).sum();
    // The worktree rule applies to exactly the set gc would otherwise delete,
    // and applies in a dry run too: `--dry-run` has to name what WOULD go, and
    // a tree holding live work would not.
    let mut eligible: Vec<&Candidate> = Vec::new();
    for c in reclaimable.iter().filter(|c| c.eligible) {
        match if force { None } else { c.worktree_hold() } {
            Some(row) => kept.push(row),
            None => eligible.push(c),
        }
    }
    let eligible_bytes: u64 = eligible.iter().map(|c| c.bytes).sum();

    let mut removed: Vec<String> = Vec::new();
    let mut reclaimed_bytes = 0u64;
    if !dry_run {
        for c in &eligible {
            // Tombstone first: a crash between the two leaves a tombstone for a
            // directory that still exists (recoverable, and visible in `ls`),
            // never a deleted worker whose session id is unrecoverable.
            append_tombstone(home, &c.tombstone(now))?;
            // Then the tree, and only then the directory: the worker dir is
            // the last thing that can name a worktree, so a registration must
            // never outlive it. A failure here keeps the directory — the
            // tombstone stands, exactly as it does for a failed remove below.
            if let Some(info) = &c.worktree {
                if let Err(e) = reclaim_worktree(info, force) {
                    warn(format!(
                        "{}: {}: {} — worker directory left in place",
                        c.id, e.code, e.message
                    ));
                    skipped.push(json!({"entry": c.id, "reason": "worktree remove failed"}));
                    continue;
                }
            }
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

    let mesh = sweep_mesh(home, &cfg, now, dry_run, &warn)?;

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
        report_mesh(&mesh, dry_run);
    }

    let data = if json_mode {
        json!({
            "dry_run": dry_run,
            "age_secs": age_secs,
            "all": all,
            "caller": caller,
            "reclaimable": reclaimable.iter().map(Candidate::to_json).collect::<Vec<_>>(),
            "reclaimable_bytes": reclaimable_bytes,
            "eligible": eligible.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            "eligible_bytes": eligible_bytes,
            "removed": removed,
            "reclaimed_bytes": reclaimed_bytes,
            "kept": kept,
            "skipped": skipped,
            "mesh": mesh.to_json(),
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

/// The mesh half of a gc run. It is separate from worker reclaim on purpose:
/// workers keep their tombstone/worktree rails, and mesh peers have their own
/// much smaller lifecycle (retired, or stale past `[mesh].gc_after_days`).
struct MeshSweep {
    reclaimable: Vec<MeshCandidate>,
    kept: Vec<Value>,
    skipped: Vec<Value>,
    removed: Vec<String>,
    reclaimed_bytes: u64,
}

impl MeshSweep {
    fn empty() -> Self {
        Self {
            reclaimable: Vec::new(),
            kept: Vec::new(),
            skipped: Vec::new(),
            removed: Vec::new(),
            reclaimed_bytes: 0,
        }
    }

    fn to_json(&self) -> Value {
        let reclaimable_bytes: u64 = self.reclaimable.iter().map(|c| c.bytes).sum();
        json!({
            "reclaimable": self.reclaimable.iter().map(MeshCandidate::to_json).collect::<Vec<_>>(),
            "reclaimable_bytes": reclaimable_bytes,
            "eligible": self.reclaimable.iter().map(|c| c.peer.as_str()).collect::<Vec<_>>(),
            "eligible_bytes": reclaimable_bytes,
            "removed": self.removed,
            "reclaimed_bytes": self.reclaimed_bytes,
            "kept": self.kept,
            "skipped": self.skipped,
        })
    }
}

struct MeshCandidate {
    peer: String,
    dir_name: String,
    bytes: u64,
    unread_notes: usize,
    notes_preview: Vec<Value>,
    reason: &'static str,
    last_seen_age_secs: i64,
}

impl MeshCandidate {
    fn to_json(&self) -> Value {
        json!({
            "peer": self.peer,
            "bytes": self.bytes,
            "unread_notes": self.unread_notes,
            "notes_preview": self.notes_preview,
            "reason": self.reason,
            "last_seen_age_secs": self.last_seen_age_secs,
        })
    }
}

fn sweep_mesh(
    home: &A2aHome,
    cfg: &config::Config,
    now: DateTime<Utc>,
    dry_run: bool,
    warn: &impl Fn(String),
) -> Result<MeshSweep, AppError> {
    let peers = home.root().join("mesh").join("peers");
    let entries = match std::fs::read_dir(&peers) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(MeshSweep::empty()),
        Err(e) => return Err(io_err("listing mesh peers", e)),
    };

    let gc_after_secs = cfg
        .mesh
        .gc_after_days
        .saturating_mul(86_400)
        .min(i64::MAX as u64) as i64;
    let mut sweep = MeshSweep::empty();

    for entry in entries {
        let entry = entry.map_err(|e| io_err("listing mesh peers", e))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = peers.join(&name);
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) => {
                warn(format!("mesh/peers/{name}: unreadable ({e}) — skipped"));
                sweep.skipped.push(json!({"entry": name, "reason": "unreadable"}));
                continue;
            }
        };
        if meta.file_type().is_symlink() {
            warn(format!(
                "mesh/peers/{name}: symlinked entry — skipped (gc never follows a link out of the home tree)"
            ));
            sweep.skipped.push(json!({"entry": name, "reason": "symlink"}));
            continue;
        }
        if !meta.is_dir() {
            warn(format!("mesh/peers/{name}: not a directory — skipped"));
            sweep.skipped.push(json!({"entry": name, "reason": "not a directory"}));
            continue;
        }
        let Some(id) = mesh_peer_from_dir_name(&name) else {
            warn(format!(
                "mesh/peers/{name}: not a valid peer directory — skipped (gc only removes what it can name)"
            ));
            sweep.skipped.push(json!({"entry": name, "reason": "invalid peer id"}));
            continue;
        };
        let peer = id.to_string();
        let Some(presence) = presence::load(home, &id)? else {
            sweep.kept.push(json!({
                "peer": peer,
                "reason": "missing presence",
            }));
            continue;
        };
        let last_seen_age_secs = (now - presence.last_seen).num_seconds().max(0);
        let label = presence::staleness(
            presence.last_seen,
            presence.retired,
            now,
            cfg.mesh.stale_after_secs,
        );
        let reason = mesh_reclaim_reason(&presence, label, last_seen_age_secs, gc_after_secs);
        let Some(reason) = reason else {
            sweep.kept.push(json!({
                "peer": peer,
                "reason": if label == presence::Label::Active {
                    "active"
                } else {
                    "stale under gc threshold"
                },
                "last_seen_age_secs": last_seen_age_secs,
            }));
            continue;
        };
        let unread = match notes::unread(home, &id) {
            Ok(unread) => unread,
            Err(e) => {
                warn(format!(
                    "mesh/peers/{name}: {}: {} — skipped",
                    e.code, e.message
                ));
                sweep.skipped.push(json!({"entry": name, "reason": e.code}));
                continue;
            }
        };
        sweep.reclaimable.push(MeshCandidate {
            peer,
            dir_name: name,
            bytes: dir_bytes(&path),
            unread_notes: unread.len(),
            notes_preview: mesh_notes_preview(&unread),
            reason,
            last_seen_age_secs,
        });
    }

    sweep.reclaimable.sort_by(|a, b| a.peer.cmp(&b.peer));

    if !dry_run {
        for c in &sweep.reclaimable {
            let dir = peers.join(&c.dir_name);
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {
                    sweep.removed.push(c.peer.clone());
                    sweep.reclaimed_bytes += c.bytes;
                }
                Err(e) => {
                    warn(format!("mesh/peers/{}: could not remove: {e}", c.dir_name));
                    sweep
                        .skipped
                        .push(json!({"entry": c.dir_name, "reason": "remove failed"}));
                }
            }
        }
    }

    Ok(sweep)
}

fn mesh_reclaim_reason(
    presence: &Presence,
    label: presence::Label,
    last_seen_age_secs: i64,
    gc_after_secs: i64,
) -> Option<&'static str> {
    if presence.retired {
        return Some("retired");
    }
    if label == presence::Label::Stale && last_seen_age_secs >= gc_after_secs {
        return Some("stale");
    }
    None
}

fn mesh_notes_preview(unread: &[(std::path::PathBuf, crate::mesh::Note)]) -> Vec<Value> {
    if unread.len() > 5 {
        return Vec::new();
    }
    unread
        .iter()
        .map(|(_, note)| {
            json!({
                "from": redact::redact_egress(&note.from),
                "first_line": redact::redact_egress(note.text.lines().next().unwrap_or("")),
            })
        })
        .collect()
}

fn mesh_peer_from_dir_name(name: &str) -> Option<PeerId> {
    let (provider, uuid) = name.split_once('-')?;
    PeerId::parse(&format!("{provider}:{uuid}")).ok()
}

/// What gc may do about one worker's worktree, decided once per sweep.
enum WtPlan {
    /// git spoke for the tree: this is what it said.
    Live(worktree::Status),
    /// The parent repo is unreachable, so git cannot be asked and there is no
    /// registration left to prune — the directory is all that survives.
    Orphaned,
}

/// A worker's worktree plus the plan for it.
struct WtInfo {
    wt: WorktreeRef,
    plan: WtPlan,
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
    worktree: Option<WtInfo>,
}

impl Candidate {
    fn new(
        id: &str,
        snap: &WorkerSnapshot,
        dir: &Path,
        worktree: Option<WtInfo>,
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
            // A worktree is disk this worker is holding just as surely as its
            // own directory is, and it is usually the larger half — a report
            // that counted only the worker dir would understate what gc is
            // offering to give back.
            bytes: dir_bytes(dir)
                + worktree
                    .as_ref()
                    .map(|i| dir_bytes(&i.wt.path))
                    .unwrap_or(0),
            last_event_age_secs,
            eligible,
            worktree,
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
            // The branch outlives everything else about this worker, and once
            // the directory is gone the tombstone is the only thing that can
            // still name it — or say what it was cut from.
            "worktree_branch": self.worktree.as_ref().map(|i| i.wt.branch.clone()),
            "worktree_base_ref": self.worktree.as_ref().map(|i| i.wt.base_ref.clone()),
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
            "worktree": self.worktree_json(),
        })
    }

    /// The `worktree` cell of a `--json` candidate row: what the tree is, and
    /// what it is holding, so `gc --dry-run` tells the whole truth.
    fn worktree_json(&self) -> Value {
        let Some(info) = &self.worktree else {
            return Value::Null;
        };
        // `null`, not `0`, when git could not be asked: a zero there would be
        // an answer rather than the absence of one.
        let (ahead, dirty_count) = match &info.plan {
            WtPlan::Live(s) => (json!(s.ahead), json!(s.dirty.len())),
            WtPlan::Orphaned => (Value::Null, Value::Null),
        };
        json!({
            "path": info.wt.path.display().to_string(),
            "branch": info.wt.branch,
            "ahead": ahead,
            "dirty_count": dirty_count,
        })
    }

    /// The refusal this worker's worktree imposes, as a `kept` row — or `None`
    /// when there is nothing in the tree worth stopping for.
    ///
    /// Consulted only for an age-eligible candidate, and only without
    /// `--force`. A tree that is already gone holds nothing and refuses
    /// nothing; an unreachable parent repo cannot be asked and, per the
    /// recovery rule, must not strand the worker either.
    fn worktree_hold(&self) -> Option<Value> {
        let info = self.worktree.as_ref()?;
        let WtPlan::Live(s) = &info.plan else {
            return None;
        };
        if !s.present {
            return None;
        }
        if !s.dirty.is_empty() {
            return Some(json!({
                "id": self.id,
                "state": self.state,
                "reason": "worktree dirty",
                "remedy": format!(
                    "commit or discard the work in {}, or re-run gc --force",
                    info.wt.path.display()
                ),
                "dirty": s.dirty.iter().take(DIRTY_SAMPLE).collect::<Vec<_>>(),
            }));
        }
        if s.ahead > 0 {
            return Some(json!({
                "id": self.id,
                "state": self.state,
                "reason": "worktree unmerged",
                "ahead": s.ahead,
                "remedy": format!(
                    "merge or cherry-pick {} (a2a never merges), or re-run gc --force",
                    info.wt.branch
                ),
            }));
        }
        None
    }
}

/// Ask git about a worker's tree — or decide it cannot be asked.
///
/// `None` means "git answered with an error": the caller skips the worker
/// entirely, because a tree gc cannot judge is not a tree gc may delete. The
/// checks are on **`wt.repo`**, the recorded parent, never on `wt.path`: the
/// worktree's own `.git` is a pointer file that dies with the directory.
fn plan_for(id: &str, wt: &WorktreeRef, force: bool, warn: &impl Fn(String)) -> Option<WtPlan> {
    if !wt.repo.exists() || worktree::repo_root(&wt.repo).is_none() {
        warn(format!(
            "{id}: parent repo {} is gone — removing the orphaned worktree at {} without git",
            wt.repo.display(),
            wt.path.display()
        ));
        return Some(WtPlan::Orphaned);
    }
    match worktree::status(&wt.path, &wt.repo, &wt.branch, &wt.base_ref) {
        Ok(s) => Some(WtPlan::Live(s)),
        Err(e) if force => {
            warn(format!(
                "{id}: {}: {} — --force: removing {} as an orphan",
                e.code,
                e.message,
                wt.path.display()
            ));
            Some(WtPlan::Orphaned)
        }
        Err(e) => {
            warn(format!(
                "{id}: {}: {} — skipped (re-run with --force to reclaim it anyway)",
                e.code, e.message
            ));
            None
        }
    }
}

/// Take a reclaimed worker's tree away. Never its branch.
fn reclaim_worktree(info: &WtInfo, force: bool) -> Result<(), AppError> {
    match &info.plan {
        // Gone by some other hand: nothing to remove, but the parent repo is
        // still carrying a registration that only `wt.repo` can address.
        WtPlan::Live(s) if !s.present => worktree::prune(&info.wt.repo),
        WtPlan::Live(_) => worktree::remove(&info.wt.repo, &info.wt.path, force),
        WtPlan::Orphaned => {
            // Best-effort, because "orphaned" covers both a repo that is gone
            // (nothing to prune, and git will simply fail) and one that would
            // not answer for this tree (where a prune is the tidy thing to do).
            let _ = worktree::prune(&info.wt.repo);
            match std::fs::remove_dir_all(&info.wt.path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(io_err("removing an orphaned worktree", e)),
            }
        }
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
    let held = kept
        .iter()
        .filter(|k| {
            k["reason"] == json!("worktree dirty") || k["reason"] == json!("worktree unmerged")
        })
        .count();
    if held > 0 {
        eprintln!(
            "a2a: gc: kept {held} worker(s) with live worktree work — commit it, merge it, or re-run with --force (the branch is never deleted either way)"
        );
    }
}

fn report_mesh(mesh: &MeshSweep, dry_run: bool) {
    let bytes: u64 = mesh.reclaimable.iter().map(|c| c.bytes).sum();
    if dry_run {
        eprintln!(
            "a2a: gc --dry-run: {} mesh peer(s) eligible under [mesh].gc_after_days ({}) — nothing was deleted",
            mesh.reclaimable.len(),
            human_bytes(bytes),
        );
    } else if !mesh.reclaimable.is_empty() || !mesh.removed.is_empty() {
        eprintln!(
            "a2a: gc: removed {} mesh peer(s), {} reclaimed",
            mesh.removed.len(),
            human_bytes(mesh.reclaimed_bytes),
        );
    }

    let rows = mesh
        .reclaimable
        .iter()
        .filter(|c| dry_run || mesh.removed.contains(&c.peer))
        .collect::<Vec<_>>();
    for c in rows {
        eprintln!(
            "a2a: gc: mesh peer {} bytes={} unread_notes={} reason={}",
            c.peer,
            human_bytes(c.bytes),
            c.unread_notes,
            c.reason,
        );
        for note in &c.notes_preview {
            eprintln!(
                "a2a: gc:   {} -> {}",
                note["from"].as_str().unwrap_or(""),
                note["first_line"].as_str().unwrap_or("")
            );
        }
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

    /// A worktree on the snapshot, shaped like the one `spawn` records.
    fn wt_ref(id: &str) -> WorktreeRef {
        WorktreeRef {
            path: std::path::PathBuf::from("/home/worktrees").join(id),
            branch: format!("a2a/impl/{id}"),
            base_ref: "0123456789abcdef0123456789abcdef01234567".into(),
            repo: std::path::PathBuf::from("/repo"),
        }
    }

    #[test]
    fn tombstone_carries_every_field_the_id_outlives() {
        let tmp = tempfile::tempdir().unwrap();
        let mut snap = WorkerSnapshot::new_for_test("general-0814t1300-aaaa", tmp.path());
        snap.state = WorkerState::Done;
        snap.session_id = Some("sess-9".into());
        snap.rollout_path = Some("/r/9.jsonl".into());
        let c = Candidate::new("general-0814t1300-aaaa", &snap, tmp.path(), None, 42, true);
        let now = Utc::now();
        let t = c.tombstone(now);
        assert_eq!(t["id"], json!("general-0814t1300-aaaa"));
        assert_eq!(t["session_id"], json!("sess-9"));
        assert_eq!(t["rollout_path"], json!("/r/9.jsonl"));
        assert_eq!(t["cwd"], json!(tmp.path().display().to_string()));
        assert_eq!(t["state"], json!("done"));
        assert!(t["removed_at"].is_string(), "removed_at must be stamped");
        assert!(t["worktree_branch"].is_null(), "no worktree, no branch");
        assert!(t["worktree_base_ref"].is_null());

        // …and with a worktree, the two keys that keep the deliverable
        // findable after the directory that named it is gone.
        let id = "impl-0814t1300-bbbb";
        let mut snap = WorkerSnapshot::new_for_test(id, tmp.path());
        snap.state = WorkerState::Done;
        snap.worktree = Some(wt_ref(id));
        let live = WtPlan::Live(worktree::Status {
            present: true,
            dirty: vec![],
            ahead: 0,
        });
        let c = Candidate::new(
            id,
            &snap,
            tmp.path(),
            Some(WtInfo {
                wt: wt_ref(id),
                plan: live,
            }),
            42,
            true,
        );
        let t = c.tombstone(now);
        assert_eq!(t["worktree_branch"], json!(format!("a2a/impl/{id}")));
        assert_eq!(
            t["worktree_base_ref"],
            json!("0123456789abcdef0123456789abcdef01234567")
        );
        assert_eq!(
            t["state"],
            json!("done"),
            "the pre-existing keys are intact"
        );
    }

    /// The two refusals, at the unit where their wording is decided: a `kept`
    /// row that does not say what would change the answer is a dead end.
    #[test]
    fn a_dirty_or_unmerged_tree_is_a_kept_row_with_a_remedy_and_ten_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let id = "impl-0814t1300-cccc";
        let mut snap = WorkerSnapshot::new_for_test(id, tmp.path());
        snap.state = WorkerState::Done;
        snap.worktree = Some(wt_ref(id));
        let candidate = |plan| {
            Candidate::new(
                id,
                &snap,
                tmp.path(),
                Some(WtInfo {
                    wt: wt_ref(id),
                    plan,
                }),
                42,
                true,
            )
        };

        let dirty: Vec<String> = (0..25).map(|i| format!("src/f{i}.rs")).collect();
        let c = candidate(WtPlan::Live(worktree::Status {
            present: true,
            dirty: dirty.clone(),
            // Dirt wins over distance: it is the half git would refuse too.
            ahead: 3,
        }));
        let row = c.worktree_hold().expect("uncommitted work is a refusal");
        assert_eq!(row["reason"], json!("worktree dirty"));
        assert_eq!(
            row["remedy"],
            json!(format!(
                "commit or discard the work in /home/worktrees/{id}, or re-run gc --force"
            ))
        );
        assert_eq!(
            row["dirty"].as_array().unwrap().len(),
            10,
            "a sample, not the whole tree"
        );
        assert_eq!(row["dirty"][0], json!("src/f0.rs"));

        let c = candidate(WtPlan::Live(worktree::Status {
            present: true,
            dirty: vec![],
            ahead: 2,
        }));
        let row = c
            .worktree_hold()
            .expect("git does not protect this — a2a does");
        assert_eq!(row["reason"], json!("worktree unmerged"));
        assert_eq!(row["ahead"], json!(2));
        assert_eq!(
            row["remedy"],
            json!(format!(
                "merge or cherry-pick a2a/impl/{id} (a2a never merges), or re-run gc --force"
            ))
        );

        // Clean and level: nothing to hold. Already gone, or unreachable:
        // nothing to hold either — a refusal there would strand the worker.
        for plan in [
            WtPlan::Live(worktree::Status {
                present: true,
                dirty: vec![],
                ahead: 0,
            }),
            WtPlan::Live(worktree::Status {
                present: false,
                dirty: vec![],
                ahead: 0,
            }),
            WtPlan::Orphaned,
        ] {
            assert!(candidate(plan).worktree_hold().is_none());
        }
    }

    /// The size report counts the tree, and the `--json` row says what it is.
    #[test]
    fn a_candidate_row_carries_the_worktree_and_its_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("worker");
        let tree = tmp.path().join("tree");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::create_dir_all(&tree).unwrap();
        std::fs::write(dir.join("state.json"), vec![0u8; 100]).unwrap();
        std::fs::write(tree.join("big.bin"), vec![0u8; 900]).unwrap();

        let id = "impl-0814t1300-dddd";
        let mut snap = WorkerSnapshot::new_for_test(id, tmp.path());
        snap.state = WorkerState::Done;
        let mut wt = wt_ref(id);
        wt.path = tree.clone();
        snap.worktree = Some(wt.clone());
        let c = Candidate::new(
            id,
            &snap,
            &dir,
            Some(WtInfo {
                wt,
                plan: WtPlan::Live(worktree::Status {
                    present: true,
                    dirty: vec!["a.txt".into(), "b.txt".into()],
                    ahead: 1,
                }),
            }),
            42,
            true,
        );
        assert_eq!(c.bytes, 1000, "the worker dir AND the tree it is holding");
        let row = c.to_json();
        assert_eq!(row["bytes"], json!(1000));
        assert_eq!(row["worktree"]["path"], json!(tree.display().to_string()));
        assert_eq!(row["worktree"]["branch"], json!(format!("a2a/impl/{id}")));
        assert_eq!(row["worktree"]["ahead"], json!(1));
        assert_eq!(row["worktree"]["dirty_count"], json!(2));

        // No tree at all: a null cell, never a fabricated empty one.
        let plain = Candidate::new(id, &snap, &dir, None, 42, true);
        assert!(plain.to_json()["worktree"].is_null());
        assert_eq!(plain.bytes, 100);
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
