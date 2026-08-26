//! The acceptance gate: what a worker changed, whether any of it can *execute*,
//! and how much of it the brief said it would touch (spec §5).
//!
//! `a2a verify` re-runs the **brief's** verify commands against the worker's
//! workspace. Running a command in a tree a worker just edited is only safe if
//! the edit cannot hijack the command, so the run is fronted by a refusal:
//! when the changed-path set contains something the toolchain *executes*
//! (`.github/workflows/**`, `build.rs`, a `Makefile`, a lockfile's resolution
//! rules…), verify refuses with `EXEC_BEARING_DIFF` and the human reads the
//! diff instead. `--force-verify` bypasses the refusal, never the record.
//!
//! This module is the gate half and is **pure** apart from the two git calls it
//! delegates to [`crate::worktree`]; its unit tests feed path lists directly
//! and need no git at all.
//!
//! # Why there is no `.git/**` row — a recorded gap, not an oversight
//!
//! Earlier drafts of [`EXEC_BEARING_RULES`] carried `.git/config` and
//! `.git/hooks/**`. Both were **unreachable** and are deleted. The gate's only
//! input is [`crate::worktree::changed_paths`] — the union of
//! `git diff --name-only <base>` and the `??` entries of
//! `git status --porcelain --untracked-files=all` — and **git reports nothing
//! under `.git/` from either** (probed live). In a *linked* worktree it is
//! structurally impossible anyway: `<wt>/.git` is a FILE, so `<wt>/.git/config`
//! and `<wt>/.git/hooks/**` cannot exist at all; the real config and hooks live
//! in the parent repo's common dir. Because this module's unit tests feed path
//! strings directly, those two rows would have tested **green** while never
//! firing on a real diff — false assurance on the surface this phase calls the
//! most bypassable thing in it.
//!
//! So: **git-dir escape (`core.hooksPath`, hook installation, `.git/config`
//! edits) is out of reach of a diff-based gate and is DEFERRED.** Closing it
//! needs a different mechanism — record a hash of the resolved gitdir's
//! `config` plus its `hooks/` listing at spawn, re-read both at verify, flag a
//! mismatch — which is **Phase 5** work.
//!
//! What spec §5's worktree-escape row still buys here is the `**/.gitmodules`
//! and `**/.gitattributes` rules: ordinary tracked files, visible to the
//! changed-path set, and both able to redirect what a later git command does.
//!
//! # Why "no diff base" is a refusal rather than a shrug
//!
//! A workspace we cannot diff is a workspace whose `Makefile` we cannot vouch
//! for. A worker with `base_ref == None` (a non-git cwd, or a pre-Phase-4
//! worker) therefore meets [`no_diff_base_error`] rather than an ungated run;
//! `--force-verify` is the way to say "I trust this tree" out loud.
//!
//! # The runner ([`run`])
//!
//! `a2a verify <id>` is synchronous and talks to no broker: it reads the
//! worker's record and its workspace, and nothing else. That is the whole
//! reason the two transports verify identically.
//!
//! Order of operations, pinned: resolve → reconcile → read+hash brief →
//! base-ref branch → changed paths → gate → run the commands → write
//! `verification.json` → exit.
//!
//! **On the FSM contract, precisely.** In a2a *reading a worker is what
//! advances it*: [`crate::reconcile::reconcile`] is itself the path that
//! appends envelopes and commits transitions, and verify calls it because it
//! needs a *current* `base_ref`/`worktree`/`brief_sha` — a stale snapshot is
//! exactly how verify would end up diffing against the wrong anchor. So verify
//! is not "appends nothing"; the pin is that **verify contributes no transition
//! of its own**. It appends nothing beyond what the shared reconcile would have
//! appended for any reader at that instant, and it never writes a
//! verify-specific event.

use crate::config;
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use crate::worktree;
use crate::{brief, exec, redact, store};
use chrono::{DateTime, Utc};
use nix::errno::Errno;
use nix::sys::signal::{killpg, Signal};
use nix::unistd::{setsid, Pid};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::{self, Read};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The execution-bearing pattern list (spec §5), one string per rule.
///
/// A path matches when **any** rule matches, and matching is on the
/// repo-relative, `/`-separated path. Two pattern shapes appear here and both
/// are read by the one matcher in this module:
///
/// * `<prefix>/**` — anchored at the repo root, `**` standing for the rest;
/// * `**/<tail>` — `<tail>` matched against the path's trailing segments, so a
///   basename rule fires at any depth *including* the root.
///
/// A `*` inside a segment never crosses a `/` (`**/requirements-*.txt`).
pub const EXEC_BEARING_RULES: &[&str] = &[
    // CI: a workflow file is code the *forge* runs.
    ".github/workflows/**",
    // Build scripts and task runners: code the toolchain runs on `build`/`test`.
    "**/build.rs",
    "**/package.json",
    "**/Makefile",
    "**/makefile",
    "**/GNUmakefile",
    "**/justfile",
    "**/Justfile",
    "**/.justfile",
    // Cargo's config can rewrite the linker, the runner and the target.
    "**/.cargo/config.toml",
    "**/.cargo/config",
    // pytest imports conftest.py before it collects a single test.
    "**/conftest.py",
    "**/pyproject.toml",
    // Dependency manifests: what gets fetched, and from where.
    "**/Cargo.toml",
    "**/Cargo.lock",
    "**/package-lock.json",
    "**/pnpm-lock.yaml",
    "**/yarn.lock",
    "**/requirements.txt",
    "**/requirements-*.txt",
    "**/Gemfile",
    "**/Gemfile.lock",
    "**/go.mod",
    "**/go.sum",
    // Spec §5's worktree-escape row, narrowed to what a diff can actually see.
    "**/.gitmodules",
    "**/.gitattributes",
];

/// The human + machine snapshot of what a worker touched.
///
/// Serialized into `verification.json` (Task 6) exactly as declared here.
#[derive(serde::Serialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct Scope {
    /// `git diff --stat <base_ref>` verbatim — the human half.
    pub diff_stat: String,
    /// The union changed-path set: sorted, deduplicated, repo-relative.
    pub changed: Vec<String>,
    /// Changed paths no `files_in_scope` entry claimed. Empty when the brief
    /// declared nothing — see [`Scope::files_in_scope_declared`].
    pub outside_scope: Vec<String>,
    /// Did the brief declare a `files_in_scope` at all? Without this an empty
    /// `outside_scope` reads as "the worker stayed in its lane" when it in fact
    /// means "no lane was drawn".
    pub files_in_scope_declared: bool,
}

/// Does this path match any rule in [`EXEC_BEARING_RULES`]?
pub fn is_exec_bearing(path: &str) -> bool {
    EXEC_BEARING_RULES
        .iter()
        .any(|rule| rule_matches(rule, path))
}

/// The execution-bearing subset of a changed-path set, sorted.
pub fn gate(changed: &[String]) -> Vec<String> {
    let mut hits: Vec<String> = changed
        .iter()
        .filter(|p| is_exec_bearing(p))
        .cloned()
        .collect();
    hits.sort();
    hits.dedup();
    hits
}

/// The refusal built from a non-empty [`gate`] result.
///
/// The sentence names at most ten paths (a 400-file rename should not fill a
/// terminal); the **full** list travels in `data.paths`, which is what an
/// orchestrator branches on. `data.count` is the total, not the truncation.
pub fn exec_bearing_error(hits: &[String]) -> AppError {
    const SHOWN: usize = 10;
    let shown = hits
        .iter()
        .take(SHOWN)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let more = hits.len().saturating_sub(SHOWN);
    let listed = if more > 0 {
        format!("{shown} (+{more} more)")
    } else {
        shown
    };
    AppError::new(
        "EXEC_BEARING_DIFF",
        ExitCode::Precondition,
        format!(
            "the diff touches {} execution-bearing path(s), so running the brief's commands \
             would run this worker's code: {listed}",
            hits.len()
        ),
    )
    .remedy(
        "review the diff by hand, then re-run with --force-verify to run the brief's \
         commands anyway",
    )
    .data(json!({"paths": hits, "count": hits.len()}))
}

/// The refusal for a worker with no diff anchor (`snap.base_ref == None`).
///
/// Not in Task 5's `Interfaces` block but pinned by its acceptance criteria;
/// Task 6's base-ref branch calls it so the exact remedy has one home.
pub fn no_diff_base_error(id: &str) -> AppError {
    AppError::new(
        "NO_DIFF_BASE",
        ExitCode::Precondition,
        format!(
            "worker {id} recorded no base ref, so there is nothing to diff its workspace \
             against and no way to tell what it changed"
        ),
    )
    .remedy(
        "--force-verify runs the brief's commands without a diff gate — only do that on a \
         workspace you trust",
    )
}

/// The scope snapshot: everything that changed since `base_ref`, and how much
/// of it the brief claimed.
///
/// The only impure function in this module, and both of its effects are git
/// reads delegated to [`crate::worktree`].
pub fn scope(dir: &Path, base_ref: &str, files_in_scope: &[String]) -> Result<Scope, AppError> {
    let changed = worktree::changed_paths(dir, base_ref)?;
    let diff_stat = worktree::diff_stat(dir, base_ref)?;
    let declared = !files_in_scope.is_empty();
    // Nothing declared ⇒ nothing is outside it. "Declare your scope" is taught
    // in the orchestrate skill, not enforced by turning silence into a fault.
    let outside_scope = if declared {
        changed
            .iter()
            .filter(|p| !files_in_scope.iter().any(|e| in_scope(e, p)))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    Ok(Scope {
        diff_stat,
        changed,
        outside_scope,
        files_in_scope_declared: declared,
    })
}

/// Does one `files_in_scope` entry claim `path`? Four forms, all pinned:
///
/// 1. **equality** — `src/a.rs` claims `src/a.rs`;
/// 2. **directory prefix**, component-wise — `src/` claims `src/a/b.rs` and not
///    `srcfoo/b.rs`. The trailing `/` is what makes it a directory: an entry
///    without one is an exact path, and a brief that means a tree writes
///    `src/` or `src/**`;
/// 3. **single-`*` glob**, within one segment — `src/*.rs` claims `src/a.rs`
///    and not `src/a/b.rs`;
/// 4. **`**` glob**, crossing any number of segments — `src/**` claims
///    `src/a.rs` and `src/a/b/c.rs`, and neither `srcfoo/a.rs` nor
///    `other/src/a.rs`.
fn in_scope(entry: &str, path: &str) -> bool {
    if let Some(dir) = entry.strip_suffix('/') {
        let dir = segments(dir);
        let path = segments(path);
        // A directory claims what is *under* it, never itself.
        return path.len() > dir.len() && path[..dir.len()] == dir[..];
    }
    glob_match(&segments(entry), &segments(path))
}

/// One [`EXEC_BEARING_RULES`] entry against one path.
fn rule_matches(rule: &str, path: &str) -> bool {
    glob_match(&segments(rule), &segments(path))
}

/// Repo-relative path ⇒ its non-empty segments.
fn segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// Segment-wise glob: `**` consumes zero or more whole segments, everything
/// else is matched one segment at a time by [`segment_match`], so a `*` can
/// never cross a `/`.
fn glob_match(pattern: &[&str], path: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((&"**", rest)) => (0..=path.len()).any(|skip| glob_match(rest, &path[skip..])),
        Some((head, rest)) => match path.split_first() {
            Some((seg, tail)) if segment_match(head, seg) => glob_match(rest, tail),
            _ => false,
        },
    }
}

/// One segment against one pattern segment, with at most one `*`.
fn segment_match(pattern: &str, segment: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == segment,
        Some((prefix, suffix)) => {
            segment.len() >= prefix.len() + suffix.len()
                && segment.starts_with(prefix)
                && segment.ends_with(suffix)
        }
    }
}

// ---------------------------------------------------------------------------
// the runner (Task 6)
// ---------------------------------------------------------------------------

/// The artifact `a2a verify` leaves in the worker directory.
///
/// A rerun **overwrites** it; unlike `result.json` there is no rotation. The
/// latest verification is the one that counts, and the WAL is not involved.
pub const VERIFICATION_FILE: &str = "verification.json";

/// Per-command wall clock before the group is killed (`--timeout` overrides).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600);

/// Who ran these commands. Not "codex", not the worker — the point of the
/// artifact is that an independent process re-ran the brief's own checks.
const RAN_BY: &str = "a2a-verify";

/// How much of each command's stderr reaches the human (the artifact itself
/// carries no output text at all — spec §5, it must not become an egress
/// channel).
const STDERR_TAIL: usize = 4096;

/// Wait granularity for the bounded per-command wait.
const WAIT_POLL: Duration = Duration::from_millis(10);

/// `verification.json`, in declaration order.
///
/// A `#[derive(Serialize)]` struct rather than a `json!` literal on purpose:
/// `serde_json::Map` is a `BTreeMap` (the crate is declared with no features),
/// so anything routed through a [`Value`] comes back out alphabetized. The file
/// is written from this struct directly; only the `--json` envelope, which
/// nobody diffs, goes through [`Value`].
#[derive(Serialize, Debug)]
pub struct Verification {
    pub schema_version: u32,
    pub id: String,
    pub ran_at: DateTime<Utc>,
    pub ran_by: &'static str,
    pub workspace: PathBuf,
    /// The commit the workspace was diffed against — `null` only on the
    /// `--force-verify` + no-base path.
    pub base_ref: Option<String>,
    /// Did `--force-verify` run this? Forcing bypasses the refusal, never the
    /// record.
    pub forced: bool,
    /// The execution-bearing paths the gate found. Non-empty here implies
    /// `forced`, because otherwise the run would have been refused.
    pub exec_bearing: Vec<String>,
    pub commands: Vec<CommandRun>,
    pub scope: Scope,
    pub ok: bool,
}

/// One brief command's outcome — exactly seven keys.
///
/// The brief pins `{command, exit_code, duration_ms, stdout_sha256, ran_at,
/// ran_by}`; `timed_out` is the seventh, added because `exit_code: null` alone
/// is ambiguous (killed by us? killed by something else?).
#[derive(Serialize, Debug)]
pub struct CommandRun {
    pub command: String,
    /// `null` when the command was killed rather than exited.
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub duration_ms: u64,
    /// Lowercase hex SHA-256 of the child's **stdout bytes**. The text itself
    /// is never stored (spec §5).
    pub stdout_sha256: String,
    pub ran_at: DateTime<Utc>,
    pub ran_by: &'static str,
}

/// Re-run the BRIEF's verify commands in the worker's workspace.
///
/// Never runs a command the worker proposed: the list is
/// `brief::parse(<worker>/brief.md).verify`, and the brief is re-hashed against
/// the `brief_sha` pinned at spawn first.
pub fn run(
    home: &A2aHome,
    id_arg: &str,
    force_verify: bool,
    timeout: Duration,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let (cfg, _origins, warnings) = config::load(home)?;
    if !quiet {
        for w in &warnings {
            eprintln!("a2a: warning: {w}");
        }
    }
    let id = ids::resolve(home, id_arg)?;
    // The read path `status` uses, for the reason in the module doc-comment.
    let snap = reconcile(home, &cfg, &id, Utc::now())?;
    let wdir = home.worker_dir(&id);

    // --- the brief, and the two guards that make it the ONLY source of commands
    let brief_path = wdir.join("brief.md");
    let brief_text = std::fs::read_to_string(&brief_path).map_err(|e| {
        AppError::new(
            "BRIEF_UNREADABLE",
            ExitCode::Precondition,
            format!("cannot read {}: {e}", brief_path.display()),
        )
        .remedy(
            "verify runs the brief's commands, so a worker without its brief cannot be verified",
        )
    })?;
    if brief::sha256_hex(&brief_text) != snap.brief_sha {
        return Err(AppError::new(
            "BRIEF_TAMPERED",
            ExitCode::Precondition,
            format!(
                "{}'s brief no longer hashes to the value recorded at spawn",
                id
            ),
        )
        .remedy(
            "the brief is immutable after spawn; a2a will not run commands from an edited brief",
        )
        .data(json!({"id": id, "expected": snap.brief_sha})));
    }
    let parsed = brief::parse(&brief_text).map_err(|e| {
        AppError::new("BRIEF_INVALID", ExitCode::Precondition, e)
            .remedy("fix the brief's front matter and respawn")
    })?;
    if parsed.verify.is_empty() {
        return Err(AppError::new(
            "NO_VERIFY_COMMANDS",
            ExitCode::Precondition,
            format!("{id}'s brief declares no `verify:` commands, so there is nothing to run"),
        )
        .remedy("add `verify:` commands to the brief's front matter and respawn"));
    }

    // --- the workspace: the worktree when there is one, else the plain cwd
    let workspace = snap
        .worktree
        .as_ref()
        .map(|w| w.path.clone())
        .unwrap_or_else(|| snap.cwd.clone());
    if !workspace.is_dir() {
        return Err(AppError::new(
            "WORKSPACE_MISSING",
            ExitCode::Precondition,
            format!("{id}'s workspace {} is gone", workspace.display()),
        )
        .remedy("nothing can be verified in a tree that no longer exists")
        .data(json!({"id": id, "workspace": workspace})));
    }

    // --- the base-ref branch, then the gate
    let (base_ref, scope_snapshot, exec_bearing) = match snap.base_ref.as_deref() {
        Some(base) => {
            let snapshot = scope(&workspace, base, &parsed.files_in_scope)?;
            let hits = gate(&snapshot.changed);
            if !hits.is_empty() && !force_verify {
                return Err(exec_bearing_error(&hits));
            }
            (Some(base.to_string()), snapshot, hits)
        }
        // No anchor: refuse, unless the operator said "I trust this tree" out
        // loud. `scope()` is deliberately NOT called on the forced path —
        // there is no ref to hand it, and the artifact's shape is pinned to the
        // empty snapshot below.
        None => {
            if !force_verify {
                return Err(no_diff_base_error(&id));
            }
            (
                None,
                Scope {
                    files_in_scope_declared: !parsed.files_in_scope.is_empty(),
                    ..Scope::default()
                },
                Vec::new(),
            )
        }
    };
    if !exec_bearing.is_empty() {
        // Deliberately NOT gated on `--quiet`: this is the notice that a
        // refusal was overridden, not decoration.
        eprintln!(
            "a2a: WARNING: --force-verify: running the brief's commands against a diff that \
             touches {} execution-bearing path(s): {}",
            exec_bearing.len(),
            exec_bearing.join(", ")
        );
    }

    // --- run every command, in brief order, even after one fails
    let mut commands = Vec::with_capacity(parsed.verify.len());
    for command in &parsed.verify {
        commands.push(run_one(command, &workspace, &id, timeout, quiet)?);
    }
    let ok = commands
        .iter()
        .all(|c| !c.timed_out && c.exit_code == Some(0));

    let artifact = Verification {
        schema_version: 1,
        id: id.clone(),
        ran_at: Utc::now(),
        ran_by: RAN_BY,
        workspace,
        base_ref,
        forced: force_verify,
        exec_bearing,
        commands,
        scope: scope_snapshot,
        ok,
    };
    let mut bytes = serde_json::to_vec_pretty(&artifact).map_err(|e| {
        AppError::new(
            "INTERNAL",
            ExitCode::Internal,
            format!("serializing the verification: {e}"),
        )
    })?;
    bytes.push(b'\n');
    store::write_atomic(&wdir.join(VERIFICATION_FILE), &bytes).map_err(|e| {
        AppError::new(
            "IO",
            ExitCode::Internal,
            format!("writing {VERIFICATION_FILE}: {e}"),
        )
    })?;

    let data = if json_mode {
        serde_json::to_value(&artifact).map_err(|e| {
            AppError::new(
                "INTERNAL",
                ExitCode::Internal,
                format!("rendering the verification: {e}"),
            )
        })?
    } else {
        Value::String(plain_lines(&artifact.commands))
    };
    Ok(Outcome::with(
        data,
        if ok {
            ExitCode::Success
        } else {
            // Commands that failed are a WORKER failure, not a precondition
            // one: verify itself answered the question correctly.
            ExitCode::WorkerFailed
        },
    ))
}

/// `<exit_code> <duration_ms>ms <command>`, one line per command. A killed
/// command has no exit code, so it renders as `-`.
fn plain_lines(commands: &[CommandRun]) -> String {
    commands
        .iter()
        .map(|c| {
            let code = c
                .exit_code
                .map(|c| c.to_string())
                .unwrap_or_else(|| "-".to_string());
            format!("{code} {}ms {}", c.duration_ms, c.command)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Run ONE brief command and report what it did.
///
/// # Why `/bin/sh -c <command>` is safe here, and only here
///
/// `<command>` is passed as a **single argv element, verbatim from the brief**.
/// This is the one shell use in the phase, and three facts guard it: the brief
/// is human-authored; its `brief_sha` is pinned at spawn and re-checked by
/// [`run`] before this function is ever reached; and **no worker output, steer,
/// inbox message or any other machine-generated text ever reaches this argv**.
/// A verify command therefore says exactly what its author typed.
///
/// # Why two reader threads
///
/// The obvious implementation — `spawn()` plus a `try_wait()` poll that reads
/// the pipes after exit — **deadlocks** the moment a command's stdout exceeds
/// the ~64 KiB pipe buffer: the child blocks writing, the parent blocks
/// waiting, the timeout expires, and a perfectly healthy `cargo test` is
/// recorded `timed_out: true, exit_code: null`. So stdout and stderr are
/// drained by their own threads *concurrently* with the wait, and both are
/// joined **before** the hash is read, so `stdout_sha256` always covers the
/// full byte stream.
fn run_one(
    command: &str,
    workspace: &Path,
    id: &str,
    timeout: Duration,
    quiet: bool,
) -> Result<CommandRun, AppError> {
    let ran_at = Utc::now();
    let started = Instant::now();

    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(command) // ONE argv element — never a built-up shell string
        .current_dir(workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // The caller's environment minus the credentials a worker may never see
    // (spec §4.2, §5) — the same list `exec` scrubs.
    for key in exec::SCRUBBED_ENV {
        cmd.env_remove(key);
    }
    cmd.env("A2A_WORKER_ID", id).env("A2A_WORKSPACE", workspace);
    unsafe {
        cmd.pre_exec(|| {
            // Own session + process group — the same idiom as
            // `exec::spawn_detached` — so a timeout can reap the whole tree
            // with one `killpg` instead of orphaning the grandchildren.
            match setsid() {
                Ok(_) => Ok(()),
                Err(Errno::EPERM) => Ok(()), // already a session leader
                Err(e) => Err(io::Error::from(e)),
            }
        });
    }
    let mut child = cmd.spawn().map_err(|e| {
        AppError::new(
            "VERIFY_SPAWN_FAILED",
            ExitCode::Internal,
            format!("could not run `{command}`: {e}"),
        )
    })?;
    let pid = child.id() as i32;
    let mut out = child.stdout.take().expect("stdout is piped");
    let mut err = child.stderr.take().expect("stderr is piped");
    let hasher = std::thread::spawn(move || hash_stream(&mut out));
    let tailer = std::thread::spawn(move || tail_stream(&mut err));

    let deadline = started + timeout;
    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break Some(s),
            Ok(None) => {}
            Err(e) => {
                return Err(AppError::new(
                    "VERIFY_WAIT_FAILED",
                    ExitCode::Internal,
                    format!("waiting for `{command}`: {e}"),
                ))
            }
        }
        if Instant::now() >= deadline {
            timed_out = true;
            // The child setsid'd, so its pid IS its process group: one signal
            // takes the whole tree.
            let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
            let _ = child.wait(); // reap, so nothing is left as a zombie
            break None;
        }
        std::thread::sleep(WAIT_POLL);
    };

    // Joined BEFORE the hash is used: a hash taken while a reader is still
    // draining would cover a prefix of the stream.
    let stdout_sha256 = hasher.join().unwrap_or_else(|_| hex(&Sha256::digest([])));
    let stderr_tail = tailer.join().unwrap_or_default();
    let duration_ms = started.elapsed().as_millis() as u64;

    if !stderr_tail.is_empty() && !quiet {
        // The human gets the tail, egress-redacted; the artifact gets none of
        // it (spec §5 — it must not become an egress channel).
        eprint!(
            "a2a: verify: `{command}` stderr (last {} bytes):\n{}",
            stderr_tail.len(),
            redact::redact_egress(&String::from_utf8_lossy(&stderr_tail))
        );
        if !stderr_tail.ends_with(b"\n") {
            eprintln!();
        }
    }

    Ok(CommandRun {
        command: command.to_string(),
        exit_code: if timed_out {
            None
        } else {
            status.and_then(|s| s.code())
        },
        timed_out,
        duration_ms,
        stdout_sha256,
        ran_at,
        ran_by: RAN_BY,
    })
}

/// Drain a pipe to its end, hashing as it goes: the bytes are never held.
fn hash_stream(r: &mut impl Read) -> String {
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        match r.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => hasher.update(&buf[..n]),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    hex(&hasher.finalize())
}

/// Drain a pipe to its end, keeping only the last [`STDERR_TAIL`] bytes.
fn tail_stream(r: &mut impl Read) -> Vec<u8> {
    let mut keep: Vec<u8> = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match r.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                keep.extend_from_slice(&buf[..n]);
                if keep.len() > STDERR_TAIL {
                    let cut = keep.len() - STDERR_TAIL;
                    keep.drain(..cut);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    keep
}

/// Lowercase hex, the same rendering as [`crate::brief::sha256_hex`].
fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String is infallible.
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One canonical hit per rule, at the root and at depth where the rule
    /// allows both. The coverage assertion below is what keeps this list
    /// honest when a rule is added.
    const HITS: &[&str] = &[
        ".github/workflows/ci.yml",
        ".github/workflows/nested/deploy.yaml",
        "build.rs",
        "crates/inner/build.rs",
        "package.json",
        "web/package.json",
        "Makefile",
        "sub/Makefile",
        "makefile",
        "GNUmakefile",
        "justfile",
        "Justfile",
        ".justfile",
        "sub/justfile",
        ".cargo/config.toml",
        "sub/.cargo/config.toml",
        ".cargo/config",
        "conftest.py",
        "tests/conftest.py",
        "pyproject.toml",
        "Cargo.toml",
        "Cargo.lock",
        "package-lock.json",
        "pnpm-lock.yaml",
        "yarn.lock",
        "requirements.txt",
        "requirements-dev.txt",
        "tools/requirements-test.txt",
        "Gemfile",
        "Gemfile.lock",
        "go.mod",
        "go.sum",
        ".gitmodules",
        "sub/.gitattributes",
    ];

    /// The other direction. The first five are pinned by the plan; the rest
    /// guard the shapes the matcher could plausibly get wrong.
    const NEAR_MISSES: &[&str] = &[
        "src/github/workflows/x.yml",
        "notpackage.json",
        "Makefile.in",
        "build.rs.bak",
        "docs/justfile.md",
        ".github/ci.yml",
        "workflows/ci.yml",
        "docs/.github/workflows/x.yml",
        "config.toml",
        "sub/config.toml",
        ".cargo/config.toml.bak",
        "my-package-lock.json",
        "go.mod.orig",
        "requirements.txt.md",
        "src/main.rs",
        "README.md",
        "a.txt",
        "note.txt",
    ];

    #[test]
    fn every_rule_fires_on_its_canonical_path() {
        for path in HITS {
            assert!(is_exec_bearing(path), "{path} must be execution-bearing");
        }
        // Both directions per RULE, not merely per path: a rule that no listed
        // hit exercises is a rule nothing tests.
        for rule in EXEC_BEARING_RULES {
            assert!(
                HITS.iter().any(|p| rule_matches(rule, p)),
                "rule {rule} has no canonical hit in HITS"
            );
        }
    }

    #[test]
    fn near_misses_never_fire() {
        for path in NEAR_MISSES {
            assert!(
                !is_exec_bearing(path),
                "{path} must NOT be execution-bearing"
            );
            // And name the rule that would have been wrong, if any.
            for rule in EXEC_BEARING_RULES {
                assert!(
                    !rule_matches(rule, path),
                    "rule {rule} wrongly matched {path}"
                );
            }
        }
    }

    /// The gap is deliberate (see the module doc-comment): a `.git/**` rule
    /// would test green and never fire.
    #[test]
    fn the_table_carries_no_unreachable_git_dir_rows() {
        for rule in EXEC_BEARING_RULES {
            assert!(
                !rule.contains(".git/"),
                "{rule} names the git dir, which no changed-path set can ever contain"
            );
        }
    }

    #[test]
    fn the_gate_returns_only_the_hits_sorted() {
        let changed: Vec<String> = [
            "src/main.rs",
            "Makefile",
            ".github/workflows/ci.yml",
            "a.txt",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            gate(&changed),
            vec![
                ".github/workflows/ci.yml".to_string(),
                "Makefile".to_string()
            ]
        );
        let clean: Vec<String> = ["src/main.rs", "a.txt"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(gate(&clean).is_empty());
        assert!(gate(&[]).is_empty());
    }

    #[test]
    fn the_refusal_truncates_the_sentence_and_never_the_data() {
        let hits: Vec<String> = (0..13).map(|i| format!("d{i:02}/package.json")).collect();
        let err = exec_bearing_error(&hits);
        assert_eq!(err.code, "EXEC_BEARING_DIFF");
        assert_eq!(err.exit, ExitCode::Precondition);
        assert_eq!(
            err.remedy.as_deref(),
            Some(
                "review the diff by hand, then re-run with --force-verify to run the brief's \
                 commands anyway"
            )
        );
        assert!(err.message.contains("d00/package.json"));
        assert!(err.message.contains("d09/package.json"));
        assert!(!err.message.contains("d10/package.json"));
        assert!(!err.message.contains("d12/package.json"));
        assert!(err.message.contains("(+3 more)"));
        let data = err.data.expect("machine-readable detail");
        assert_eq!(data["count"], 13);
        assert_eq!(data["paths"].as_array().unwrap().len(), 13);
        assert_eq!(data["paths"][12], "d12/package.json");
    }

    #[test]
    fn a_short_refusal_lists_everything_and_says_no_more() {
        let hits = vec!["Makefile".to_string(), "build.rs".to_string()];
        let err = exec_bearing_error(&hits);
        assert!(err.message.contains("Makefile"));
        assert!(err.message.contains("build.rs"));
        assert!(!err.message.contains("more)"));
        assert_eq!(err.data.unwrap()["count"], 2);
    }

    #[test]
    fn a_worker_with_no_base_ref_is_a_precondition_failure() {
        let err = no_diff_base_error("impl-0818t1200-aaaa");
        assert_eq!(err.code, "NO_DIFF_BASE");
        assert_eq!(err.exit, ExitCode::Precondition);
        assert!(err.message.contains("impl-0818t1200-aaaa"));
        assert_eq!(
            err.remedy.as_deref(),
            Some(
                "--force-verify runs the brief's commands without a diff gate — only do that \
                 on a workspace you trust"
            )
        );
    }

    /// The non-JSON rendering, including the one shape no integration test
    /// reaches: a killed command has no exit code, and `-` is its spelling.
    #[test]
    fn the_plain_rendering_names_the_code_the_duration_and_the_command() {
        let runs = vec![
            CommandRun {
                command: "cargo test".to_string(),
                exit_code: Some(0),
                timed_out: false,
                duration_ms: 1234,
                stdout_sha256: String::new(),
                ran_at: Utc::now(),
                ran_by: RAN_BY,
            },
            CommandRun {
                command: "sleep 300".to_string(),
                exit_code: None,
                timed_out: true,
                duration_ms: 1000,
                stdout_sha256: String::new(),
                ran_at: Utc::now(),
                ran_by: RAN_BY,
            },
        ];
        assert_eq!(
            plain_lines(&runs),
            "0 1234ms cargo test\n- 1000ms sleep 300"
        );
    }

    #[test]
    fn scope_form_one_is_equality() {
        assert!(in_scope("src/a.rs", "src/a.rs"));
        assert!(!in_scope("src/a.rs", "src/b.rs"));
        assert!(!in_scope("src/a.rs", "x/src/a.rs"));
        assert!(!in_scope("src/a.rs", "src/a.rs.bak"));
    }

    #[test]
    fn scope_form_two_is_a_component_wise_directory_prefix() {
        assert!(in_scope("src/", "src/a.rs"));
        assert!(in_scope("src/", "src/a/b.rs"));
        assert!(!in_scope("src/", "srcfoo/b.rs"));
        assert!(!in_scope("src/", "other/src/a.rs"));
        // A directory claims what is under it, not the name itself.
        assert!(!in_scope("src/", "src"));
    }

    #[test]
    fn scope_form_three_is_one_segment_of_glob() {
        assert!(in_scope("src/*.rs", "src/a.rs"));
        assert!(!in_scope("src/*.rs", "src/a/b.rs"));
        assert!(!in_scope("src/*.rs", "src/a.txt"));
        assert!(!in_scope("src/*.rs", "srcx/a.rs"));
        assert!(!in_scope("src/*.rs", "a.rs"));
    }

    #[test]
    fn scope_form_four_crosses_segments() {
        assert!(in_scope("src/**", "src/a.rs"));
        assert!(in_scope("src/**", "src/a/b/c.rs"));
        assert!(!in_scope("src/**", "srcfoo/a.rs"));
        assert!(!in_scope("src/**", "other/src/a.rs"));
        assert!(in_scope("**/a.rs", "src/deep/a.rs"));
        assert!(in_scope("**/a.rs", "a.rs"));
        assert!(!in_scope("**/a.rs", "src/b.rs"));
    }
}
