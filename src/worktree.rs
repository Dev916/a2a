//! Git worktree lifecycle and scope queries (spec §4.8, §5).
//!
//! A dispatched worker gets its own tree: `$A2A_HOME/worktrees/<id>` checked
//! out on `a2a/<lane>/<id>`, branched from an explicit base commit. Creation is
//! CLI-side and pre-spawn — the broker never learns about worktrees, it just
//! receives a cwd — so everything here is plain blocking code that shells out
//! to `git`.
//!
//! Two rules make this module worth having as a module:
//!
//! * **Every invocation is an argv array.** Branch names, refs and paths come
//!   from operator input and from brief text; a command *line* would turn any
//!   of them into code. `tests/worktree_git.rs` scans this file's source for a
//!   shell, and that scan guards THIS file only — which is why no other module
//!   in the phase is allowed to run `git` itself.
//! * **The changed-path set is a UNION.** `git diff --name-only <base>` covers
//!   committed and uncommitted changes to *tracked* files and says nothing at
//!   all about an untracked `.github/workflows/evil.yml`; `git status
//!   --porcelain` without `--untracked-files=all` collapses that file's whole
//!   directory to one entry and hides it. [`changed_paths`] is the union of the
//!   two, because the acceptance gate downstream is exactly as good as its
//!   input.
//!
//! What this module deliberately does NOT do: delete branches. `remove` takes
//! away a directory and a git registration; the branch is the deliverable a2a
//! produces and it survives every path in here (spec §4.8).

use crate::config::Lane;
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode};
use crate::redact::redact_egress;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The one program this module runs.
const GIT: &str = "git";

/// ASCII double quote, spelled as a byte so this file stays scannable: the
/// source-hygiene test reads string literals, and a char literal holding a
/// double quote would make its scanner think a string had started.
const QUOTE: u8 = 34;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Worktree {
    /// Canonicalized `$A2A_HOME/worktrees/<id>`.
    pub path: PathBuf,
    /// `a2a/<lane>/<id>`.
    pub branch: String,
    /// 40-hex commit the branch was cut from.
    pub base_ref: String,
    /// Canonicalized toplevel of the parent repo.
    pub repo: PathBuf,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Status {
    pub present: bool,
    /// Every path `git status --porcelain --untracked-files=all` reports.
    pub dirty: Vec<String>,
    /// Commits on the branch that the base ref does not have.
    pub ahead: u64,
}

/// `a2a/<lane>/<id>` — the only branch name a2a ever creates.
pub fn branch_name(lane: Lane, id: &str) -> String {
    format!("a2a/{lane}/{id}")
}

/// The canonical toplevel of the work tree `dir` sits in, or `None`.
///
/// `git rev-parse --show-toplevel` exits 128 outside a repo, and a missing
/// `git` is also "no answer" here — the typed [`AppError`] for that surfaces
/// from [`create`], which is where a caller can act on it.
pub fn repo_root(dir: &Path) -> Option<PathBuf> {
    repo_root_checked(dir).ok().flatten()
}

/// The 40-hex commit `HEAD` names in `repo`.
///
/// `--quiet` is pinned: without it an empty repo prints `fatal: Needed a single
/// revision` and exits 128 rather than 1, so the refusal keys off *any* nonzero
/// status and survives both spellings of the probe.
pub fn head_ref(repo: &Path) -> Result<String, AppError> {
    let run = git(&argv_in(repo, &["rev-parse", "--verify", "--quiet", "HEAD"]))?;
    if !run.ok() {
        return Err(AppError::new(
            "WORKTREE_NO_BASE",
            ExitCode::Precondition,
            format!("{} has no commit to branch from{}", repo.display(), why(&run)),
        )
        .remedy(format!(
            "make at least one commit in {} — a worktree needs a base ref to branch from",
            repo.display()
        )));
    }
    Ok(run.stdout.trim().to_string())
}

/// An operator-supplied `--base-ref` resolved to a 40-hex commit.
///
/// The ref travels as ONE argv element (peeled with `^{commit}` so an annotated
/// tag resolves to the commit it points at, not to the tag object).
pub fn resolve_commit(repo: &Path, refname: &str) -> Result<String, AppError> {
    let peeled = format!("{refname}^{{commit}}");
    let run = git(&argv_in(repo, &["rev-parse", "--verify", "--quiet", &peeled]))?;
    if !run.ok() {
        return Err(AppError::new(
            "WORKTREE_BASE_UNKNOWN",
            ExitCode::Precondition,
            format!(
                "`{refname}` does not name a commit in {}{}",
                repo.display(),
                why(&run)
            ),
        )
        .remedy(format!(
            "name a ref that exists in {} (git rev-parse --verify <ref>)",
            repo.display()
        )));
    }
    Ok(run.stdout.trim().to_string())
}

/// `git -C <repo> worktree add --no-track -b a2a/<lane>/<id> <path> <base>`.
///
/// `--no-track` is pinned: without it git sets the new branch's upstream to the
/// base branch, i.e. a2a would write config into a repo it does not own.
pub fn create(
    home: &A2aHome,
    id: &str,
    lane: Lane,
    target: &Path,
    base: Option<&str>,
) -> Result<Worktree, AppError> {
    let Some(repo) = repo_root_checked(target)? else {
        return Err(AppError::new(
            "WORKTREE_NOT_A_REPO",
            ExitCode::Precondition,
            format!("{} is not inside a git work tree", target.display()),
        )
        .remedy("run `git init` in the target directory, or pass --no-worktree"));
    };
    let base_ref = match base {
        Some(r) => resolve_commit(&repo, r)?,
        None => head_ref(&repo)?,
    };
    let path = home.worktrees_dir().join(id);
    // Checked BEFORE git runs so the message names a2a's path rather than
    // git's paraphrase of it.
    if path.symlink_metadata().is_ok() {
        return Err(AppError::new(
            "WORKTREE_PATH_EXISTS",
            ExitCode::Precondition,
            format!("{} already exists", path.display()),
        )
        .remedy("reclaim the old worker (`a2a gc`) or use a different id"));
    }
    let branch = branch_name(lane, id);
    let mut argv = argv_in(
        &repo,
        &["worktree", "add", "--no-track", "-b", branch.as_str()],
    );
    argv.push(path_arg(&path));
    argv.push(base_ref.clone());
    let run = git(&argv)?;
    if !run.ok() {
        let evidence = redact_egress(run.stderr.trim());
        // git's own wording is the pin; a2a classifies rather than reinterprets.
        if evidence.contains("already exists") {
            return Err(AppError::new(
                "WORKTREE_BRANCH_EXISTS",
                ExitCode::Precondition,
                format!("could not create {branch}: {evidence}"),
            )
            .remedy(format!(
                "rename or delete {branch} yourself, or use a different id — a2a never deletes a branch"
            )));
        }
        return Err(AppError::new(
            "WORKTREE_CREATE_FAILED",
            ExitCode::Precondition,
            format!("could not create a worktree at {}: {evidence}", path.display()),
        )
        .remedy("check the repo is healthy (`git worktree list`) and the home is writable"));
    }
    let path = path.canonicalize().unwrap_or(path);
    Ok(Worktree {
        path,
        branch,
        base_ref,
        repo,
    })
}

/// Append `path` to the parent repo's `info/exclude`, once.
///
/// Belt-and-braces (spec §4.8): a worktree created *under* the repo would
/// otherwise show up as an untracked directory in the human's own `git status`.
/// `.gitignore` is the human's file and is never read or written here.
///
/// Returns `Ok(true)` when the line was added, `Ok(false)` when it was already
/// there — or when the git dir could not be located at all, which is a warning
/// on stderr rather than an error, because a missing exclude line has never
/// made a worktree wrong.
pub fn add_to_info_exclude(repo: &Path, path: &Path) -> Result<bool, AppError> {
    let Some(gitdir) = git_dir(repo) else {
        eprintln!(
            "a2a: warning: no git dir under {} — skipping the info/exclude entry",
            repo.display()
        );
        return Ok(false);
    };
    let info = gitdir.join("info");
    let file = info.join("exclude");
    let line = path_arg(path);
    let current = std::fs::read_to_string(&file).unwrap_or_default();
    if current.lines().any(|l| l == line) {
        return Ok(false);
    }
    std::fs::create_dir_all(&info).map_err(io_err)?;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file)
        .map_err(io_err)?;
    if !current.is_empty() && !current.ends_with('\n') {
        f.write_all(b"\n").map_err(io_err)?;
    }
    writeln!(f, "{line}").map_err(io_err)?;
    Ok(true)
}

/// Is the tree still there, what is uncommitted in it, and how far has it moved.
///
/// `--untracked-files=all` is pinned: plain `--porcelain` collapses a new
/// directory to one entry, so the file that matters is the file it hides.
pub fn status(path: &Path, repo: &Path, branch: &str, base_ref: &str) -> Result<Status, AppError> {
    if !path.exists() {
        return Ok(Status::default());
    }
    let run = git(&argv_in(
        path,
        &["status", "--porcelain", "--untracked-files=all"],
    ))?;
    if !run.ok() {
        return Err(status_failed(path, &run));
    }
    let dirty: Vec<String> = run.stdout.lines().filter_map(porcelain_path).collect();
    let range = format!("{base_ref}..{branch}");
    let counted = git(&argv_in(repo, &["rev-list", "--count", &range]))?;
    if !counted.ok() {
        return Err(status_failed(path, &counted));
    }
    Ok(Status {
        present: true,
        dirty,
        ahead: counted.stdout.trim().parse().unwrap_or(0),
    })
}

/// `git worktree remove [--force] <path>`, then `git worktree prune` — always.
///
/// The branch is never touched. git itself refuses a tree with modified *or
/// untracked* files (exit 128); that refusal is a2a's `WORKTREE_DIRTY`.
pub fn remove(repo: &Path, path: &Path, force: bool) -> Result<(), AppError> {
    let mut rest: Vec<&str> = vec!["worktree", "remove"];
    if force {
        rest.push("--force");
    }
    let mut argv = argv_in(repo, &rest);
    argv.push(path_arg(path));
    let run = git(&argv)?;
    // Unconditional: a remove that half-succeeded leaves a stale registration,
    // and a prune with nothing to do is a silent no-op.
    let _ = prune(repo);
    if !run.ok() {
        let evidence = redact_egress(run.stderr.trim());
        if evidence.contains("contains modified or untracked files") {
            return Err(AppError::new(
                "WORKTREE_DIRTY",
                ExitCode::Precondition,
                format!("{}: {evidence}", path.display()),
            )
            .remedy(format!(
                "re-run with --force to discard uncommitted work in {}",
                path.display()
            )));
        }
        return Err(AppError::new(
            "WORKTREE_REMOVE_FAILED",
            ExitCode::Precondition,
            format!("could not remove {}: {evidence}", path.display()),
        )
        .remedy("inspect the tree with `git worktree list` and remove it by hand"));
    }
    Ok(())
}

/// `git -C <repo> worktree prune` — drop registrations whose directory is gone.
///
/// Called on its own when a worker's tree has vanished by some other hand:
/// there is nothing left to [`remove`], only the parent repo's stale
/// registration, and the recorded parent repo is the only thing that can still
/// address it. A prune with nothing to do exits 0 silently.
pub fn prune(repo: &Path) -> Result<(), AppError> {
    let run = git(&argv_in(repo, &["worktree", "prune"]))?;
    if !run.ok() {
        return Err(AppError::new(
            "WORKTREE_PRUNE_FAILED",
            ExitCode::Precondition,
            format!(
                "could not prune stale worktrees in {}{}",
                repo.display(),
                why(&run)
            ),
        )
        .remedy("check the repo is healthy (`git worktree list`)"));
    }
    Ok(())
}

/// Every repo-relative path that differs from `base_ref`, sorted and deduped.
///
/// The UNION of `git diff --name-only <base>` (committed *and* uncommitted
/// changes to tracked files) and the `??` entries of `git status --porcelain
/// --untracked-files=all` (everything git has never seen). Either half alone is
/// a hole: the diff misses an untracked file entirely, and it is precisely an
/// untracked file that an escape would arrive as.
pub fn changed_paths(dir: &Path, base_ref: &str) -> Result<Vec<String>, AppError> {
    let diff = git(&argv_in(dir, &["diff", "--name-only", base_ref]))?;
    if !diff.ok() {
        return Err(diff_failed(dir, &diff));
    }
    let mut out: Vec<String> = diff
        .stdout
        .lines()
        .map(unquote)
        .filter(|p| !p.is_empty())
        .collect();
    let st = git(&argv_in(
        dir,
        &["status", "--porcelain", "--untracked-files=all"],
    ))?;
    if !st.ok() {
        return Err(status_failed(dir, &st));
    }
    for line in st.stdout.lines() {
        if let Some(rest) = line.strip_prefix("?? ") {
            let p = unquote(rest);
            if !p.is_empty() {
                out.push(p);
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// `git diff --stat <base>` verbatim — the human half of a scope snapshot.
pub fn diff_stat(dir: &Path, base_ref: &str) -> Result<String, AppError> {
    let run = git(&argv_in(dir, &["diff", "--stat", base_ref]))?;
    if !run.ok() {
        return Err(diff_failed(dir, &run));
    }
    Ok(run.stdout)
}

// ---------------------------------------------------------------------------
// internals
// ---------------------------------------------------------------------------

/// One finished git run. Never a `Result` on the exit code: a nonzero git is
/// an *answer* here (no such ref, dirty tree), and each caller classifies it.
#[derive(Debug)]
struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Run {
    fn ok(&self) -> bool {
        self.code == 0
    }
}

/// Spawn `prog` with an argv array. The ONLY process spawn in this module.
fn run_prog(prog: &str, args: &[String]) -> Result<Run, AppError> {
    let out = Command::new(prog)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                AppError::new(
                    "GIT_NOT_FOUND",
                    ExitCode::Precondition,
                    format!("`{prog}` is not on PATH"),
                )
                .remedy("install git — a2a worktrees are plain git worktrees")
            } else {
                AppError::new(
                    "GIT_SPAWN_FAILED",
                    ExitCode::Precondition,
                    format!("could not run `{prog}`: {e}"),
                )
                .remedy("check the git binary is runnable on this platform")
            }
        })?;
    Ok(Run {
        code: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

fn git(args: &[String]) -> Result<Run, AppError> {
    run_prog(GIT, args)
}

/// `-C <dir>` followed by the rest — every argv in this module starts here.
fn argv_in(dir: &Path, rest: &[&str]) -> Vec<String> {
    let mut argv = Vec::with_capacity(rest.len() + 2);
    argv.push("-C".to_string());
    argv.push(path_arg(dir));
    argv.extend(rest.iter().map(|s| (*s).to_string()));
    argv
}

fn path_arg(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

/// git's stderr as *evidence inside* an a2a error, egress-redacted.
fn why(run: &Run) -> String {
    let text = redact_egress(run.stderr.trim());
    if text.is_empty() {
        String::new()
    } else {
        format!(": {text}")
    }
}

fn repo_root_checked(dir: &Path) -> Result<Option<PathBuf>, AppError> {
    let run = git(&argv_in(dir, &["rev-parse", "--show-toplevel"]))?;
    if !run.ok() {
        return Ok(None);
    }
    let top = PathBuf::from(run.stdout.trim());
    Ok(Some(top.canonicalize().unwrap_or(top)))
}

/// The real git dir for `repo`, following a linked worktree's `gitdir:` file.
fn git_dir(repo: &Path) -> Option<PathBuf> {
    let dot = repo.join(".git");
    if std::fs::metadata(&dot).ok()?.is_dir() {
        return Some(dot);
    }
    let text = std::fs::read_to_string(&dot).ok()?;
    let raw = text.lines().find_map(|l| l.strip_prefix("gitdir:"))?.trim();
    if raw.is_empty() {
        return None;
    }
    let p = PathBuf::from(raw);
    Some(if p.is_absolute() { p } else { repo.join(p) })
}

/// The path field of one `--porcelain` line (`XY <path>`, or `XY <old> -> <new>`).
fn porcelain_path(line: &str) -> Option<String> {
    if line.len() < 4 {
        return None;
    }
    let rest = &line[3..];
    let field = match rest.split_once(" -> ") {
        Some((_, new)) => new,
        None => rest,
    };
    let p = unquote(field);
    if p.is_empty() {
        None
    } else {
        Some(p)
    }
}

/// git quotes a path containing a space (or worse) and escapes what is inside.
///
/// Undoing that matters because the changed-path set feeds a gate: a path left
/// wearing its quotes is a path no rule matches. Octal escapes for non-ASCII
/// bytes are left as written — the path stays *in* the set either way, which is
/// the fail-closed direction.
fn unquote(s: &str) -> String {
    let t = s.trim();
    let b = t.as_bytes();
    if b.len() < 2 || b[0] != QUOTE || b[b.len() - 1] != QUOTE {
        return t.to_string();
    }
    let inner = &t[1..t.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

fn status_failed(dir: &Path, run: &Run) -> AppError {
    AppError::new(
        "WORKTREE_STATUS_FAILED",
        ExitCode::Precondition,
        format!("could not read git status in {}{}", dir.display(), why(run)),
    )
    .remedy("check the worktree is still registered (`git worktree list`)")
}

fn diff_failed(dir: &Path, run: &Run) -> AppError {
    AppError::new(
        "WORKTREE_DIFF_FAILED",
        ExitCode::Precondition,
        format!("could not diff {} against its base{}", dir.display(), why(run)),
    )
    .remedy("check the base ref still exists in the parent repo")
}

fn io_err(e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A machine without git is a precondition failure with a remedy, not an
    /// io::Error stringified into somebody's log. Exercised through the private
    /// spawn seam so no test has to mutate this process's PATH.
    #[test]
    fn a_missing_git_is_typed() {
        let err = run_prog("a2a-no-such-program-xyzzy", &[]).unwrap_err();
        assert_eq!(err.code, "GIT_NOT_FOUND");
        assert_eq!(err.exit, ExitCode::Precondition);
        assert!(err.remedy.unwrap().contains("install git"));
    }

    #[test]
    fn branch_names_carry_the_lane() {
        assert_eq!(branch_name(Lane::Impl, "w-1"), "a2a/impl/w-1");
        assert_eq!(branch_name(Lane::Review, "w-2"), "a2a/review/w-2");
    }

    #[test]
    fn quoted_porcelain_paths_are_unwrapped() {
        assert_eq!(unquote("a.txt"), "a.txt");
        assert_eq!(unquote("\"a file.txt\""), "a file.txt");
        assert_eq!(unquote("\"a\\\"b.txt\""), "a\"b.txt");
        assert_eq!(porcelain_path("?? note.txt").unwrap(), "note.txt");
        assert_eq!(porcelain_path(" M a.txt").unwrap(), "a.txt");
        assert_eq!(porcelain_path("R  old.txt -> new.txt").unwrap(), "new.txt");
        assert_eq!(porcelain_path(""), None);
    }
}
