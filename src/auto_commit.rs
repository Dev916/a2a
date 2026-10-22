//! Framework commit on `done`.
//!
//! Codex's `workspace-write` sandbox makes `.git` read-only, so a worker can
//! finish a change in-tree but can never commit it. Before this module the
//! owner committed by hand after every task — the slow step when fanning out.
//! Now a worker that reaches `done` has its change committed from OUTSIDE the
//! sandbox, through the same pinned git the mesh result path uses
//! ([`crate::mesh::work_gate::git_pinned`]: hooks off, no external diff, no
//! fsmonitor), as the fixed identity `a2a <a2a@localhost>`.
//!
//! **Where it runs.** Exactly one implementation, two call sites — one per
//! transport, at the one moment each already concludes a turn under
//! `state.lock`:
//!
//! * exec: [`crate::reconcile::reconcile`], right after the locked reconcile
//!   pass (exec workers belong to the CLI's reconcile path; the broker never
//!   sees them);
//! * app-server: the broker's `WorkerActor`, inside the same persistence step
//!   that concludes the turn — so the commit lands BEFORE the settlement is
//!   announced, which is what makes `auto_review`'s diff (taken on settlement)
//!   include it, and the actor stays the sole writer of its worker's WAL.
//!
//! **What it stages** ([`plan_staging`]):
//!
//! | result `files_changed` | worker has its own worktree | staged            |
//! |------------------------|-----------------------------|-------------------|
//! | non-empty              | either                      | exactly those     |
//! | empty / absent         | yes                         | `git add -A`      |
//! | empty / absent         | no (`--no-worktree`)        | nothing: skipped  |
//!
//! `-A` is never run in a shared cwd other sessions may be editing, and a
//! listed path that escapes the checkout or names `.git` refuses the commit.
//! In a shared cwd the commit is `--only` the listed paths, so anything the
//! human had staged for other paths stays staged and uncommitted.
//!
//! **Failure never changes the worker.** It stays `done`; a `commit_failed`
//! row records why, and `a2a result` shows it. Every outcome is recorded once
//! ([`should_run`] is false once `auto_commit` is set), so a reconcile loop
//! never retries a failing commit into an event storm.

use crate::config::Lane;
use crate::events::{EventClass, EventKind, Envelope};
use crate::fold;
use crate::fsm::WorkerState;
use crate::mesh::work_gate::{git_pinned, rev_parse_head};
use crate::output::AppError;
use crate::reconcile::{RESULT_JSON, RESULT_MD};
use crate::redact;
use crate::state::WorkerSnapshot;
use crate::store::{self, EventLog};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};

/// The commit subject's summary is trimmed to this many characters.
pub const SUMMARY_MAX_CHARS: usize = 72;

/// The trailer every framework commit carries: the work is Codex's.
pub const CO_AUTHOR_TRAILER: &str = "Co-Authored-By: Codex <noreply@openai.com>";

/// What one framework commit did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoCommitOutcome {
    /// A commit was made on `branch`, carrying `files` paths.
    Committed {
        sha: String,
        files: usize,
        branch: String,
    },
    /// Nothing was staged: the tree was already clean for what this worker
    /// was allowed to stage.
    Clean,
    /// The policy declined to stage anything (`reason` says why).
    Skipped { reason: String },
    /// Git, the result, or the conflict-marker check said no.
    Failed { reason: String },
}

impl AutoCommitOutcome {
    /// The WAL row this outcome is recorded as.
    pub fn event_kind(&self) -> EventKind {
        match self {
            AutoCommitOutcome::Committed { .. } => EventKind::ResultCommitted,
            AutoCommitOutcome::Clean | AutoCommitOutcome::Skipped { .. } => {
                EventKind::CommitSkipped
            }
            AutoCommitOutcome::Failed { .. } => EventKind::CommitFailed,
        }
    }

    /// That row's payload. No `fsm` key, ever: the fold consumes the seq and
    /// moves no state.
    pub fn payload(&self) -> Value {
        match self {
            AutoCommitOutcome::Committed { sha, files, branch } => {
                json!({"sha": sha, "files": files, "branch": branch})
            }
            AutoCommitOutcome::Clean => {
                json!({"outcome": "clean", "reason": "nothing to commit"})
            }
            AutoCommitOutcome::Skipped { reason } => json!({"outcome": "skipped", "reason": reason}),
            AutoCommitOutcome::Failed { reason } => json!({"reason": reason}),
        }
    }
}

/// Review and research never touch code; only these two lanes may ever commit.
pub fn lane_may_commit(lane: Lane) -> bool {
    matches!(lane, Lane::Impl | Lane::General)
}

/// Is this worker owed a framework commit right now? `done`, opted in at
/// spawn, on a lane that writes, and not already answered.
pub fn should_run(snap: &WorkerSnapshot) -> bool {
    snap.state == WorkerState::Done
        && snap.auto_commit_enabled
        && lane_may_commit(snap.lane)
        && snap.auto_commit.is_none()
}

/// The two facts the commit needs from the worker's result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResultFacts {
    /// `None` when the result has no `files_changed` key at all.
    pub files_changed: Option<Vec<String>>,
    pub summary: Option<String>,
}

/// Read `result.json` (the devloop result shape) for `files_changed` and
/// `summary`, falling back to `result.md`'s first non-empty line for the
/// summary. A `result.json` that exists but does not parse is an error — the
/// commit would otherwise silently fall back to a broader staging rule.
pub fn read_result(dir: &Path) -> Result<ResultFacts, String> {
    let mut facts = ResultFacts::default();
    match std::fs::read_to_string(dir.join(RESULT_JSON)) {
        Ok(text) => {
            let v: Value = serde_json::from_str(&text)
                .map_err(|e| format!("{RESULT_JSON} is not JSON: {e}"))?;
            if let Some(files) = v.get("files_changed").and_then(Value::as_array) {
                facts.files_changed = Some(
                    files
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect(),
                );
            }
            facts.summary = v
                .get("summary")
                .and_then(Value::as_str)
                .and_then(first_line);
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("reading {RESULT_JSON}: {e}")),
    }
    if facts.summary.is_none() {
        facts.summary = std::fs::read_to_string(dir.join(RESULT_MD))
            .ok()
            .as_deref()
            .and_then(first_line);
    }
    Ok(facts)
}

fn first_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(|l| l.trim_start_matches('#').trim().to_string())
        .filter(|l| !l.is_empty())
}

/// The staging decision ([module docs](self) table).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Staging {
    Paths(Vec<String>),
    All,
    Skip(String),
}

pub fn plan_staging(files_changed: Option<&[String]>, own_worktree: bool) -> Staging {
    match files_changed {
        Some(files) if !files.is_empty() => Staging::Paths(files.to_vec()),
        _ if own_worktree => Staging::All,
        _ => Staging::Skip("no files_changed in a shared cwd".to_string()),
    }
}

/// One `files_changed` entry as a path relative to the checkout's toplevel.
///
/// Repo-relative is the schema's contract; an absolute path is accepted only
/// when it lies inside the checkout. Anything that escapes it (`..`, another
/// absolute root) or names `.git` anywhere is refused — the worker does not get
/// to steer a commit outside its own tree.
pub fn normalize_path(top: &Path, raw: &str) -> Result<String, String> {
    // `files_changed` is worker-controlled, and a git pathspec has magic: a
    // leading `:` (`:/`, `:!x`, `:(top)…`) or a glob (`*`, `?`, `[`) would let
    // a worker widen what gets staged. Every git call here also runs with
    // GIT_LITERAL_PATHSPECS=1; refusing the shapes outright fails closed.
    if raw.starts_with(':') {
        return Err(format!("`{raw}` looks like pathspec magic"));
    }
    if raw.contains(['*', '?', '[']) {
        return Err(format!("`{raw}` contains a glob character"));
    }
    let given = Path::new(raw);
    let rel: PathBuf = if given.is_absolute() {
        given
            .strip_prefix(top)
            .map_err(|_| format!("`{raw}` is outside the checkout"))?
            .to_path_buf()
    } else {
        given.to_path_buf()
    };
    let mut out = PathBuf::new();
    for c in rel.components() {
        match c {
            Component::Normal(part) => {
                if part == ".git" {
                    return Err(format!("`{raw}` names .git"));
                }
                out.push(part);
            }
            Component::CurDir => {}
            _ => return Err(format!("`{raw}` escapes the checkout")),
        }
    }
    let s = out.to_string_lossy().into_owned();
    if s.is_empty() {
        return Err(format!("`{raw}` is the checkout itself, not a file in it"));
    }
    Ok(s)
}

/// `<lane> <worker-id>: <summary>` + blank line + the two trailers.
///
/// The summary is model-authored text headed for permanent history, so it is
/// redacted on the way in, and trimmed to [`SUMMARY_MAX_CHARS`].
pub fn commit_message(snap: &WorkerSnapshot, summary: Option<&str>) -> String {
    let summary = summary
        .map(redact::redact_egress)
        .map(|s| s.lines().next().unwrap_or("").trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "worker result".to_string());
    let summary: String = summary.chars().take(SUMMARY_MAX_CHARS).collect();
    let transport = serde_json::to_value(snap.transport)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string());
    format!(
        "{} {}: {}\n\nWorker: {} ({}/{}, {})\n{CO_AUTHOR_TRAILER}\n",
        snap.lane.as_str(),
        snap.id,
        summary.trim_end(),
        snap.id,
        snap.model,
        snap.effort,
        transport
    )
}

/// The pinned git, with nothing ambient able to redirect it: no identity, date
/// or repository override from the caller's environment.
fn git(top: &Path) -> Command {
    let mut cmd = git_pinned(top);
    for var in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_AUTHOR_NAME",
        "GIT_AUTHOR_EMAIL",
        "GIT_AUTHOR_DATE",
        "GIT_COMMITTER_NAME",
        "GIT_COMMITTER_EMAIL",
        "GIT_COMMITTER_DATE",
    ] {
        cmd.env_remove(var);
    }
    // Every pathspec this module passes is a worker-supplied file name, never
    // a pattern: no globs, no `:(magic)`.
    cmd.env("GIT_LITERAL_PATHSPECS", "1");
    cmd
}

fn run_git(top: &Path, args: &[&str], paths: &[String]) -> Result<Output, String> {
    let mut cmd = git(top);
    cmd.args(args);
    if !paths.is_empty() {
        cmd.arg("--").args(paths);
    }
    cmd.output()
        .map_err(|e| format!("running git {}: {e}", args.join(" ")))
}

fn git_ok(top: &Path, args: &[&str], paths: &[String]) -> Result<Output, String> {
    let out = run_git(top, args, paths)?;
    if out.status.success() {
        Ok(out)
    } else {
        Err(format!(
            "git {} failed: {}",
            args.first().copied().unwrap_or(""),
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

fn nul_split(bytes: &[u8]) -> Vec<String> {
    bytes
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect()
}

/// Commit a `done` worker's change. Pure of the WAL: the caller records the
/// outcome ([`record`]) under `state.lock`. Never panics and never returns an
/// error — every failure is an [`AutoCommitOutcome::Failed`] with its reason.
pub fn commit_worker(dir: &Path, snap: &WorkerSnapshot) -> AutoCommitOutcome {
    match commit_inner(dir, snap) {
        Ok(outcome) => outcome,
        Err(reason) => AutoCommitOutcome::Failed {
            reason: redact::redact_egress(&reason),
        },
    }
}

fn commit_inner(dir: &Path, snap: &WorkerSnapshot) -> Result<AutoCommitOutcome, String> {
    let facts = read_result(dir)?;
    let own_worktree = snap.worktree.is_some();
    let staging = plan_staging(facts.files_changed.as_deref(), own_worktree);
    if let Staging::Skip(reason) = &staging {
        return Ok(AutoCommitOutcome::Skipped {
            reason: reason.clone(),
        });
    }

    // The checkout's toplevel: the worker's own tree, or the repo the shared
    // cwd sits in (the cwd may be a subdirectory; `files_changed` is
    // repo-relative).
    let top = match &snap.worktree {
        Some(wt) => wt.path.clone(),
        None => {
            let out = run_git(&snap.cwd, &["rev-parse", "--show-toplevel"], &[])?;
            if !out.status.success() {
                return Ok(AutoCommitOutcome::Skipped {
                    reason: "the cwd is not a git checkout".to_string(),
                });
            }
            PathBuf::from(String::from_utf8_lossy(&out.stdout).trim())
        }
    };

    let paths = match &staging {
        Staging::Paths(raw) => {
            let mut paths = Vec::with_capacity(raw.len());
            for p in raw {
                let p = normalize_path(&top, p)?;
                if !paths.contains(&p) {
                    paths.push(p);
                }
            }
            // A listed path git cannot stage — gone and never tracked, or
            // ignored — is dropped rather than failing the whole commit.
            let tracked = nul_split(&git_ok(&top, &["ls-files", "-z"], &paths)?.stdout);
            let ignored = ignored_paths(&top, &paths)?;
            paths.retain(|p| {
                let exists = std::fs::symlink_metadata(top.join(p)).is_ok();
                (exists || tracked.contains(p)) && !ignored.contains(p)
            });
            if paths.is_empty() {
                return Ok(AutoCommitOutcome::Clean);
            }
            paths
        }
        _ => Vec::new(),
    };
    let scoped = !paths.is_empty();

    git_ok(&top, &["add", "-A"], &paths)?;
    let unstage = |top: &Path| {
        let _ = run_git(top, &["reset", "-q"], &paths);
    };

    // Anything staged at all?
    let quiet = run_git(&top, &["diff", "--cached", "--quiet"], &paths)?;
    match quiet.status.code() {
        Some(0) => return Ok(AutoCommitOutcome::Clean),
        Some(1) => {}
        _ => {
            unstage(&top);
            return Err(format!(
                "git diff --cached failed: {}",
                String::from_utf8_lossy(&quiet.stderr).trim()
            ));
        }
    }
    let staged = nul_split(&git_ok(&top, &["diff", "--cached", "--name-only", "-z"], &paths)?.stdout);
    // The scoped branch commits only what the worker listed: anything staged
    // under those pathspecs that is not one of them (or under one of them, for
    // a listed directory) refuses the commit.
    if scoped {
        let outside: Vec<&String> = staged
            .iter()
            .filter(|f| {
                !paths
                    .iter()
                    .any(|p| *f == p || f.starts_with(&format!("{p}/")))
            })
            .collect();
        if !outside.is_empty() {
            unstage(&top);
            return Err(format!(
                "staged paths outside files_changed: {}",
                outside
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }

    // An unresolved merge is never committed on the worker's behalf.
    let diff = match git_ok(
        &top,
        &["diff", "--cached", "-U0", "--no-color", "--no-ext-diff"],
        &paths,
    ) {
        Ok(out) => out,
        Err(e) => {
            unstage(&top);
            return Err(e);
        }
    };
    let marked = conflict_marked_files(&String::from_utf8_lossy(&diff.stdout));
    if !marked.is_empty() {
        unstage(&top);
        return Err(format!(
            "conflict markers in staged files: {}",
            marked.join(", ")
        ));
    }

    let message = commit_message(snap, facts.summary.as_deref());
    let mut args = vec![
        "-c",
        "user.name=a2a",
        "-c",
        "user.email=a2a@localhost",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--no-verify",
        "--no-gpg-sign",
        "-q",
        "-m",
        &message,
    ];
    if scoped {
        args.push("--only");
    }
    if let Err(e) = git_ok(&top, &args, &paths) {
        unstage(&top);
        return Err(e);
    }
    let sha = rev_parse_head(&top).map_err(|e| e.message)?;
    let branch = match &snap.worktree {
        Some(wt) => wt.branch.clone(),
        None => git_ok(&top, &["rev-parse", "--abbrev-ref", "HEAD"], &[])
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|_| "HEAD".to_string()),
    };
    Ok(AutoCommitOutcome::Committed {
        sha,
        files: staged.len(),
        branch,
    })
}

/// Which of `paths` are ignored (and untracked). Paths go over `--stdin`,
/// where `check-ignore` reads PATH NAMES rather than pathspecs — it does not
/// support the `literal` magic GIT_LITERAL_PATHSPECS would impose, and stdin
/// sidesteps pathspec parsing entirely.
fn ignored_paths(top: &Path, paths: &[String]) -> Result<Vec<String>, String> {
    use std::io::Write;
    let mut cmd = git(top);
    cmd.env_remove("GIT_LITERAL_PATHSPECS")
        .args(["check-ignore", "-z", "--stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("running git check-ignore: {e}"))?;
    let mut input = Vec::new();
    for p in paths {
        input.extend_from_slice(p.as_bytes());
        input.push(0);
    }
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(&input)
            .map_err(|e| format!("writing to git check-ignore: {e}"))?;
    }
    let out = child
        .wait_with_output()
        .map_err(|e| format!("running git check-ignore: {e}"))?;
    match out.status.code() {
        Some(0) => Ok(nul_split(&out.stdout)),
        Some(1) => Ok(Vec::new()),
        _ => Err(format!(
            "git check-ignore failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )),
    }
}

/// Files whose staged additions carry a conflict marker, from a `-U0` diff.
pub fn conflict_marked_files(diff: &str) -> Vec<String> {
    let mut current: Option<String> = None;
    let mut out: Vec<String> = Vec::new();
    for line in diff.lines() {
        if let Some(path) = line.strip_prefix("+++ ") {
            current = Some(path.strip_prefix("b/").unwrap_or(path).to_string());
            continue;
        }
        if line.starts_with("+<<<<<<<") || line.starts_with("+>>>>>>>") {
            if let Some(f) = &current {
                if !out.contains(f) {
                    out.push(f.clone());
                }
            }
        }
    }
    out
}

/// Record `outcome` on the worker: one bookkeeping row (no `fsm` key), folded
/// and checkpointed; for a commit, the sha also lands in `result.json` when
/// that is a JSON object (`result.md` is left alone). Caller holds
/// `state.lock`.
pub fn record(
    log: &mut EventLog,
    dir: &Path,
    snap: WorkerSnapshot,
    outcome: &AutoCommitOutcome,
    now: DateTime<Utc>,
) -> Result<WorkerSnapshot, AppError> {
    let mut env = Envelope::new(
        snap.epoch,
        snap.transport,
        EventClass::Lifecycle,
        outcome.event_kind(),
        outcome.payload(),
        now,
    );
    let snap = fold::append_envelope_and_fold(log, dir, snap, &mut env, now)?;
    if let AutoCommitOutcome::Committed { sha, .. } = outcome {
        stamp_result_json(dir, sha);
    }
    Ok(snap)
}

fn stamp_result_json(dir: &Path, sha: &str) {
    let path = dir.join(RESULT_JSON);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };
    let Ok(Value::Object(mut obj)) = serde_json::from_str::<Value>(&text) else {
        return;
    };
    obj.insert("committed_sha".to_string(), json!(sha));
    if let Ok(bytes) = serde_json::to_vec(&Value::Object(obj)) {
        let _ = store::write_atomic(&path, &bytes);
    }
}

/// The one entry point both transports call, with `state.lock` held, right
/// after a turn concluded: commit if this worker is owed one, record it, and
/// hand back the snapshot. Infallible on purpose — a framework commit must
/// never cost a worker its `done` — so a failure to even RECORD the outcome is
/// a warning and the snapshot comes back as it was.
pub fn run_locked(
    log: &mut EventLog,
    dir: &Path,
    snap: WorkerSnapshot,
    now: DateTime<Utc>,
) -> WorkerSnapshot {
    if !should_run(&snap) {
        return snap;
    }
    let outcome = commit_worker(dir, &snap);
    let fallback = snap.clone();
    match record(log, dir, snap, &outcome, now) {
        Ok(snap) => snap,
        Err(e) => {
            eprintln!(
                "a2a: warning: {}: recording the auto-commit outcome: {}",
                fallback.id, e.message
            );
            fallback
        }
    }
}

/// The exec transport's call site: [`crate::reconcile::reconcile`] calls this
/// with `state.lock` held, right after its locked pass. An app-server worker is
/// the broker's (its `WorkerActor` calls [`run_locked`] itself), so it is left
/// alone here — the CLI must not be a second writer of a broker-owned log.
pub fn after_exec_reconcile(
    dir: &Path,
    cfg: &crate::config::Config,
    snap: WorkerSnapshot,
    now: DateTime<Utc>,
) -> WorkerSnapshot {
    if snap.transport != crate::events::Transport::Exec || !should_run(&snap) {
        return snap;
    }
    match EventLog::open_capped(dir, cfg.broker.events_max_bytes) {
        Ok(mut log) => run_locked(&mut log, dir, snap, now),
        Err(e) => {
            eprintln!("a2a: warning: {}: opening events.jsonl for the auto-commit: {e}", snap.id);
            snap
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::WorktreeRef;

    fn git_in(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn repo() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let r = tmp.path();
        git_in(r, &["init", "-q", "-b", "main"]);
        git_in(r, &["config", "user.email", "t@example.invalid"]);
        git_in(r, &["config", "user.name", "t"]);
        git_in(r, &["config", "commit.gpgsign", "false"]);
        std::fs::write(r.join("base.txt"), "base\n").unwrap();
        git_in(r, &["add", "-A"]);
        git_in(r, &["commit", "-q", "-m", "base"]);
        tmp
    }

    fn done_snap(id: &str, cwd: &Path, worktree: bool) -> WorkerSnapshot {
        let mut s = WorkerSnapshot::new_for_test(id, cwd);
        s.state = WorkerState::Done;
        s.auto_commit_enabled = true;
        if worktree {
            s.worktree = Some(WorktreeRef {
                path: std::fs::canonicalize(cwd).unwrap(),
                branch: "main".into(),
                base_ref: "x".into(),
                repo: cwd.to_path_buf(),
            });
        }
        s
    }

    fn result_json(dir: &Path, files: &[&str], summary: &str) {
        std::fs::write(
            dir.join(RESULT_JSON),
            json!({"status": "pass", "summary": summary, "files_changed": files}).to_string(),
        )
        .unwrap();
    }

    #[test]
    fn the_staging_table() {
        let some = vec!["a.txt".to_string()];
        assert_eq!(plan_staging(Some(&some), false), Staging::Paths(some.clone()));
        assert_eq!(plan_staging(Some(&some), true), Staging::Paths(some));
        assert_eq!(plan_staging(Some(&[]), true), Staging::All);
        assert_eq!(plan_staging(None, true), Staging::All);
        assert!(matches!(plan_staging(Some(&[]), false), Staging::Skip(r) if r.contains("shared cwd")));
        assert!(matches!(plan_staging(None, false), Staging::Skip(_)));
    }

    #[test]
    fn paths_that_escape_or_name_git_are_refused() {
        let top = Path::new("/repo");
        assert_eq!(normalize_path(top, "src/a.rs").unwrap(), "src/a.rs");
        assert_eq!(normalize_path(top, "./src/a.rs").unwrap(), "src/a.rs");
        assert_eq!(normalize_path(top, "/repo/src/a.rs").unwrap(), "src/a.rs");
        assert!(normalize_path(top, "../x").is_err());
        assert!(normalize_path(top, "src/../../x").is_err());
        assert!(normalize_path(top, "/etc/passwd").is_err());
        assert!(normalize_path(top, ".git/config").is_err());
        assert!(normalize_path(top, "sub/.git/hooks/pre-commit").is_err());
        assert!(normalize_path(top, ".").is_err());
        // Pathspec magic and globs are refused before git ever sees them.
        for magic in [":/", ":!a.txt", ":(top)a.txt", "*", "src/*.rs", "a?.txt", "[ab].txt"] {
            assert!(normalize_path(top, magic).is_err(), "{magic}");
        }
        assert_eq!(normalize_path(top, "a:b.txt").unwrap(), "a:b.txt");
    }

    #[test]
    fn the_message_is_subject_blank_line_trailers() {
        let tmp = tempfile::tempdir().unwrap();
        let s = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        let long = "x".repeat(100);
        let msg = commit_message(&s, Some(&format!("{long}\nsecond line")));
        let lines: Vec<&str> = msg.lines().collect();
        assert_eq!(lines[0], format!("impl impl-0814t1300-aaaa: {}", "x".repeat(72)));
        assert_eq!(lines[1], "");
        assert_eq!(lines[2], "Worker: impl-0814t1300-aaaa (gpt-5.5/medium, exec)");
        assert_eq!(lines[3], CO_AUTHOR_TRAILER);
        assert!(commit_message(&s, None).starts_with("impl impl-0814t1300-aaaa: worker result"));
    }

    #[test]
    fn the_summary_falls_back_to_result_md() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(RESULT_MD), "\n# Did the thing\nmore\n").unwrap();
        let f = read_result(tmp.path()).unwrap();
        assert_eq!(f.summary.as_deref(), Some("Did the thing"));
        assert_eq!(f.files_changed, None);
        std::fs::write(tmp.path().join(RESULT_JSON), "{not json").unwrap();
        assert!(read_result(tmp.path()).is_err());
    }

    #[test]
    fn conflict_markers_are_found_per_file() {
        let diff = "diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ -0,0 +1,3 @@\n+<<<<<<< HEAD\n+a\n+>>>>>>> other\n\
                    diff --git a/y b/y\n--- a/y\n+++ b/y\n@@ -0,0 +1 @@\n+fine\n";
        assert_eq!(conflict_marked_files(diff), vec!["x".to_string()]);
    }

    #[test]
    fn listed_paths_only_and_the_pinned_identity() {
        let r = repo();
        let wdir = tempfile::tempdir().unwrap();
        std::fs::write(r.path().join("a.txt"), "a\n").unwrap();
        std::fs::write(r.path().join("unlisted.txt"), "u\n").unwrap();
        result_json(wdir.path(), &["a.txt"], "add a");
        let snap = done_snap("impl-0814t1300-aaaa", r.path(), false);
        let out = commit_worker(wdir.path(), &snap);
        let AutoCommitOutcome::Committed { sha, files, branch } = out else {
            panic!("{out:?}")
        };
        assert_eq!((files, branch.as_str()), (1, "main"));
        let shown = git_in(r.path(), &["show", "--name-only", "--format=%an <%ae>|%cn|%s", &sha]);
        assert!(shown.starts_with("a2a <a2a@localhost>|a2a|impl impl-0814t1300-aaaa: add a"), "{shown}");
        assert!(shown.contains("a.txt") && !shown.contains("unlisted.txt"), "{shown}");
        let status = git_in(r.path(), &["status", "--porcelain"]);
        assert!(status.contains("?? unlisted.txt"), "{status}");
    }

    #[test]
    fn ignored_and_vanished_listed_paths_are_dropped_not_fatal() {
        let r = repo();
        let wdir = tempfile::tempdir().unwrap();
        std::fs::write(r.path().join(".gitignore"), "*.log\n").unwrap();
        git_in(r.path(), &["add", ".gitignore"]);
        git_in(r.path(), &["commit", "-q", "-m", "ignore logs"]);
        std::fs::write(r.path().join("build.log"), "noise\n").unwrap();
        std::fs::write(r.path().join("a.txt"), "a\n").unwrap();
        result_json(wdir.path(), &["build.log", "gone.txt", "a.txt"], "add a");
        let out = commit_worker(wdir.path(), &done_snap("impl-0814t1300-aaaa", r.path(), false));
        let AutoCommitOutcome::Committed { sha, files, .. } = out else { panic!("{out:?}") };
        assert_eq!(files, 1);
        assert_eq!(git_in(r.path(), &["show", "--name-only", "--format=", &sha]).trim(), "a.txt");
    }

    #[test]
    fn a_human_staged_change_elsewhere_is_left_staged_in_a_shared_cwd() {
        let r = repo();
        let wdir = tempfile::tempdir().unwrap();
        std::fs::write(r.path().join("human.txt"), "h\n").unwrap();
        git_in(r.path(), &["add", "human.txt"]);
        std::fs::write(r.path().join("a.txt"), "a\n").unwrap();
        result_json(wdir.path(), &["a.txt"], "add a");
        let out = commit_worker(wdir.path(), &done_snap("impl-0814t1300-aaaa", r.path(), false));
        assert!(matches!(out, AutoCommitOutcome::Committed { files: 1, .. }), "{out:?}");
        let status = git_in(r.path(), &["status", "--porcelain"]);
        assert!(status.contains("A  human.txt"), "{status}");
    }

    #[test]
    fn a_worktree_with_no_list_stages_everything_and_a_clean_one_is_clean() {
        let r = repo();
        let wdir = tempfile::tempdir().unwrap();
        result_json(wdir.path(), &[], "nothing listed");
        let snap = done_snap("impl-0814t1300-aaaa", r.path(), true);
        assert_eq!(commit_worker(wdir.path(), &snap), AutoCommitOutcome::Clean);
        std::fs::write(r.path().join("b.txt"), "b\n").unwrap();
        std::fs::write(r.path().join("base.txt"), "changed\n").unwrap();
        let out = commit_worker(wdir.path(), &snap);
        assert!(matches!(out, AutoCommitOutcome::Committed { files: 2, .. }), "{out:?}");
        assert_eq!(git_in(r.path(), &["status", "--porcelain"]), "");
    }

    #[test]
    fn a_conflict_marker_fails_and_unstages() {
        let r = repo();
        let wdir = tempfile::tempdir().unwrap();
        std::fs::write(r.path().join("a.txt"), "<<<<<<< HEAD\nx\n=======\ny\n>>>>>>> b\n").unwrap();
        result_json(wdir.path(), &["a.txt"], "oops");
        let out = commit_worker(wdir.path(), &done_snap("impl-0814t1300-aaaa", r.path(), false));
        assert!(
            matches!(&out, AutoCommitOutcome::Failed { reason } if reason.contains("conflict markers") && reason.contains("a.txt")),
            "{out:?}"
        );
        assert!(git_in(r.path(), &["status", "--porcelain"]).contains("?? a.txt"));
    }

    #[test]
    fn an_escaping_path_fails_the_commit() {
        let r = repo();
        let wdir = tempfile::tempdir().unwrap();
        result_json(wdir.path(), &["../outside.txt"], "sneaky");
        let out = commit_worker(wdir.path(), &done_snap("impl-0814t1300-aaaa", r.path(), false));
        assert!(matches!(out, AutoCommitOutcome::Failed { .. }), "{out:?}");
    }

    #[test]
    fn should_run_needs_done_opt_in_a_writing_lane_and_no_prior_answer() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = done_snap("impl-0814t1300-aaaa", tmp.path(), false);
        assert!(should_run(&s));
        s.auto_commit_enabled = false;
        assert!(!should_run(&s));
        s.auto_commit_enabled = true;
        s.state = WorkerState::Failed;
        assert!(!should_run(&s));
        s.state = WorkerState::Done;
        s.lane = Lane::Review;
        assert!(!should_run(&s));
        s.lane = Lane::General;
        assert!(should_run(&s));
        s.auto_commit = Some(crate::state::AutoCommitRecord {
            outcome: "clean".into(),
            sha: None,
            reason: None,
        });
        assert!(!should_run(&s));
    }

    #[test]
    fn record_folds_the_outcome_and_stamps_result_json() {
        let r = repo();
        let wdir = tempfile::tempdir().unwrap();
        std::fs::write(r.path().join("a.txt"), "a\n").unwrap();
        result_json(wdir.path(), &["a.txt"], "add a");
        let snap = done_snap("impl-0814t1300-aaaa", r.path(), false);
        let mut log = EventLog::open(wdir.path()).unwrap();
        let after = run_locked(&mut log, wdir.path(), snap, Utc::now());
        let sha = after.committed_sha.clone().expect("a sha");
        assert_eq!(after.auto_commit.as_ref().unwrap().outcome, "committed");
        assert_eq!(after.state, WorkerState::Done);
        let rj: Value =
            serde_json::from_str(&std::fs::read_to_string(wdir.path().join(RESULT_JSON)).unwrap())
                .unwrap();
        assert_eq!(rj["committed_sha"], json!(sha));
        let events = store::read_events(wdir.path(), 0).unwrap();
        assert_eq!(events.last().unwrap().kind, EventKind::ResultCommitted);
        // Once answered, never again.
        let again = run_locked(&mut log, wdir.path(), after, Utc::now());
        assert_eq!(store::read_events(wdir.path(), 0).unwrap().len(), events.len());
        assert_eq!(again.committed_sha, Some(sha));
    }
}
