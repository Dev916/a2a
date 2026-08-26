//! `src/worktree.rs` against the REAL `git` binary, in throwaway repos.
//!
//! Everything here is module-level: the worktree layer is a library the spawn
//! path calls before there is a worker to name, so its contract is asserted
//! directly rather than through the CLI. Every fixture repo lives in a
//! `tempfile::tempdir()`; nothing under `$HOME` is read or written, and each
//! repo pins the git settings a developer's global config could otherwise
//! break (branch name, signing, hooks).
//!
//! The load-bearing test in this file is [`union_of_diff_and_untracked_is_the_changed_set`]:
//! the acceptance gate downstream is only as good as its input, and
//! `git diff --name-only <base>` alone does not see an untracked
//! `.github/workflows/evil.yml`.

use a2a::config::Lane;
use a2a::home::A2aHome;
use a2a::output::ExitCode;
use a2a::worktree::{self, Status};
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

/// Run `git -C <dir> <args…>` and return (exit code, stdout, stderr).
fn git(dir: &Path, args: &[&str]) -> (i32, String, String) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("git is on PATH");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn git_ok(dir: &Path, args: &[&str]) -> String {
    let (code, stdout, stderr) = git(dir, args);
    assert_eq!(code, 0, "git {args:?} failed: {stderr}");
    stdout
}

/// A repo whose behaviour does not depend on the developer's `~/.gitconfig`:
/// the branch is `main`, commits are unsigned, and hooks point at nothing.
fn init_repo(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    git_ok(dir, &["init", "-q", "-b", "main"]);
    git_ok(dir, &["config", "user.email", "a2a@example.invalid"]);
    git_ok(dir, &["config", "user.name", "a2a test"]);
    git_ok(dir, &["config", "commit.gpgsign", "false"]);
    git_ok(dir, &["config", "core.hooksPath", "no-such-hooks"]);
}

/// A fixture repo with exactly one commit touching `a.txt`.
fn fixture_repo() -> (TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    init_repo(&repo);
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    git_ok(&repo, &["add", "a.txt"]);
    git_ok(&repo, &["commit", "-q", "-m", "init"]);
    (tmp, repo)
}

fn home_in(tmp: &TempDir) -> A2aHome {
    A2aHome::resolve(Some(&tmp.path().join("a2a-home"))).unwrap()
}

fn write(path: &Path, body: &str) {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p).unwrap();
    }
    std::fs::write(path, body).unwrap();
}

// ---------------------------------------------------------------------------
// The union rule (security-load-bearing)
// ---------------------------------------------------------------------------

/// A changed-path set built on `git diff` alone is bypassable: drop a workflow
/// file in without committing it and the diff says nothing happened.
#[test]
fn union_of_diff_and_untracked_is_the_changed_set() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let wt = worktree::create(&home, "w-union", Lane::Impl, &repo, None).unwrap();

    // committed change to a tracked file
    write(&wt.path.join("a.txt"), "two\n");
    git_ok(&wt.path, &["add", "a.txt"]);
    git_ok(&wt.path, &["commit", "-q", "-m", "edit"]);
    // untracked, in a directory that did not exist before
    write(&wt.path.join(".github/workflows/evil.yml"), "on: push\n");
    // untracked, at the top level
    write(&wt.path.join("note.txt"), "hi\n");

    assert_eq!(
        worktree::changed_paths(&wt.path, &wt.base_ref).unwrap(),
        vec![
            ".github/workflows/evil.yml".to_string(),
            "a.txt".to_string(),
            "note.txt".to_string(),
        ]
    );

    // …and the bypass this union exists to close, proven live:
    let diff_only = git_ok(&wt.path, &["diff", "--name-only", &wt.base_ref]);
    let diff_lines: Vec<&str> = diff_only.lines().collect();
    assert_eq!(
        diff_lines,
        vec!["a.txt"],
        "git diff alone must be exactly the tracked change — if it grew, the \
         union test above stopped proving anything"
    );
    assert!(
        !diff_only.contains("evil.yml"),
        "the untracked workflow is invisible to git diff — that is the point"
    );

    // `--untracked-files=all` is the other half of the pin: plain --porcelain
    // collapses the new directory to `?? .github/`.
    let collapsed = git_ok(&wt.path, &["status", "--porcelain"]);
    assert!(
        collapsed.contains("?? .github/") && !collapsed.contains("evil.yml"),
        "plain --porcelain no longer collapses untracked dirs: {collapsed}"
    );
}

#[test]
fn changed_paths_is_sorted_deduplicated_and_empty_on_a_clean_tree() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let wt = worktree::create(&home, "w-clean", Lane::Impl, &repo, None).unwrap();
    assert!(worktree::changed_paths(&wt.path, &wt.base_ref)
        .unwrap()
        .is_empty());

    // A path that is BOTH staged-and-modified appears in diff and in status;
    // it must appear once.
    write(&wt.path.join("z.txt"), "z\n");
    git_ok(&wt.path, &["add", "z.txt"]);
    write(&wt.path.join("z.txt"), "zz\n");
    write(&wt.path.join("a.txt"), "changed\n");
    let paths = worktree::changed_paths(&wt.path, &wt.base_ref).unwrap();
    assert_eq!(paths, vec!["a.txt".to_string(), "z.txt".to_string()]);
}

#[test]
fn changed_paths_sees_untracked_paths_with_spaces() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let wt = worktree::create(&home, "w-space", Lane::Impl, &repo, None).unwrap();
    write(&wt.path.join("a file.txt"), "x\n");
    assert_eq!(
        worktree::changed_paths(&wt.path, &wt.base_ref).unwrap(),
        vec!["a file.txt".to_string()],
        "git quotes paths with spaces in --porcelain; a gate that keeps the \
         quotes has a hole shaped like a filename"
    );
}

#[test]
fn diff_stat_is_git_verbatim_and_empty_when_clean() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let wt = worktree::create(&home, "w-stat", Lane::Impl, &repo, None).unwrap();
    assert_eq!(worktree::diff_stat(&wt.path, &wt.base_ref).unwrap(), "");

    write(&wt.path.join("a.txt"), "two\n");
    let stat = worktree::diff_stat(&wt.path, &wt.base_ref).unwrap();
    assert!(
        stat.contains("a.txt") && stat.contains("1 file changed"),
        "unexpected diff --stat rendering: {stat:?}"
    );
    assert_eq!(
        stat,
        git_ok(&wt.path, &["diff", "--stat", &wt.base_ref]),
        "diff_stat must be git's own bytes"
    );
}

// ---------------------------------------------------------------------------
// Probes: repo_root / head_ref / resolve_commit
// ---------------------------------------------------------------------------

#[test]
fn repo_root_answers_only_inside_a_work_tree() {
    let (tmp, repo) = fixture_repo();
    let canon = repo.canonicalize().unwrap();
    assert_eq!(worktree::repo_root(&repo), Some(canon.clone()));

    let nested = repo.join("sub/dir");
    std::fs::create_dir_all(&nested).unwrap();
    assert_eq!(worktree::repo_root(&nested), Some(canon));

    let plain = tmp.path().join("not-a-repo");
    std::fs::create_dir_all(&plain).unwrap();
    assert_eq!(worktree::repo_root(&plain), None);
}

#[test]
fn head_ref_is_40_hex_and_an_empty_repo_is_typed() {
    let (tmp, repo) = fixture_repo();
    let head = worktree::head_ref(&repo).unwrap();
    assert_eq!(head.len(), 40, "not a full sha: {head:?}");
    assert!(head.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(head, git_ok(&repo, &["rev-parse", "HEAD"]).trim());

    let empty = tmp.path().join("empty");
    init_repo(&empty);
    let err = worktree::head_ref(&empty).unwrap_err();
    assert_eq!(err.code, "WORKTREE_NO_BASE");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(
        err.remedy.unwrap().contains("at least one commit"),
        "the remedy has to say what to do about an empty repo"
    );
}

#[test]
fn resolve_commit_peels_refs_and_refuses_unknown_ones() {
    let (_tmp, repo) = fixture_repo();
    let head = worktree::head_ref(&repo).unwrap();
    assert_eq!(worktree::resolve_commit(&repo, "main").unwrap(), head);
    assert_eq!(worktree::resolve_commit(&repo, "HEAD").unwrap(), head);
    assert_eq!(worktree::resolve_commit(&repo, &head).unwrap(), head);

    // An annotated tag peels to the commit, not to the tag object.
    git_ok(&repo, &["tag", "-a", "v1", "-m", "tag"]);
    assert_eq!(worktree::resolve_commit(&repo, "v1").unwrap(), head);

    let err = worktree::resolve_commit(&repo, "no/such/ref").unwrap_err();
    assert_eq!(err.code, "WORKTREE_BASE_UNKNOWN");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(err.remedy.unwrap().contains("rev-parse"));
}

// ---------------------------------------------------------------------------
// create
// ---------------------------------------------------------------------------

#[test]
fn create_checks_out_an_untracked_branch_at_the_base() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let head = worktree::head_ref(&repo).unwrap();
    let wt = worktree::create(&home, "w-create", Lane::Impl, &repo, None).unwrap();

    assert_eq!(wt.branch, "a2a/impl/w-create");
    assert_eq!(worktree::branch_name(Lane::Impl, "w-create"), wt.branch);
    assert_eq!(wt.base_ref, head);
    assert_eq!(wt.repo, repo.canonicalize().unwrap());
    assert_eq!(
        wt.path,
        home.worktrees_dir()
            .join("w-create")
            .canonicalize()
            .unwrap(),
        "path is the canonicalized $A2A_HOME/worktrees/<id>"
    );
    assert_eq!(git_ok(&wt.path, &["rev-parse", "HEAD"]).trim(), head);
    assert_eq!(
        git_ok(&wt.path, &["rev-parse", "--abbrev-ref", "HEAD"]).trim(),
        wt.branch
    );

    // `--no-track`: a2a never writes upstream config into a user's repo.
    let (code, out, _) = git(
        &repo,
        &["config", "--get", "branch.a2a/impl/w-create.remote"],
    );
    assert_ne!(code, 0, "upstream remote was set: {out}");
    let (code, out, _) = git(
        &repo,
        &["config", "--get", "branch.a2a/impl/w-create.merge"],
    );
    assert_ne!(code, 0, "upstream merge was set: {out}");

    // A linked worktree's `.git` is a FILE — the escape surface spec §5 names.
    let dot_git = wt.path.join(".git");
    assert!(dot_git.is_file(), "a linked worktree's .git must be a file");
    assert!(std::fs::read_to_string(&dot_git)
        .unwrap()
        .contains("gitdir:"));
}

#[test]
fn create_lane_names_the_branch() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    for (lane, id) in [
        (Lane::Impl, "l-impl"),
        (Lane::Review, "l-review"),
        (Lane::Research, "l-research"),
        (Lane::General, "l-general"),
    ] {
        let wt = worktree::create(&home, id, lane, &repo, None).unwrap();
        assert_eq!(wt.branch, format!("a2a/{lane}/{id}"));
    }
}

#[test]
fn create_honours_an_explicit_base_ref() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let first = worktree::head_ref(&repo).unwrap();
    write(&repo.join("b.txt"), "b\n");
    git_ok(&repo, &["add", "b.txt"]);
    git_ok(&repo, &["commit", "-q", "-m", "second"]);
    let second = worktree::head_ref(&repo).unwrap();
    assert_ne!(first, second);

    let wt = worktree::create(&home, "w-base", Lane::Impl, &repo, Some(&first)).unwrap();
    assert_eq!(wt.base_ref, first);
    assert_eq!(git_ok(&wt.path, &["rev-parse", "HEAD"]).trim(), first);
    assert!(!wt.path.join("b.txt").exists());

    let err = worktree::create(&home, "w-bad-base", Lane::Impl, &repo, Some("nope")).unwrap_err();
    assert_eq!(err.code, "WORKTREE_BASE_UNKNOWN");
    assert_eq!(err.exit, ExitCode::Precondition);
}

#[test]
fn create_failure_taxonomy_is_typed_and_exit_9() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_in(&tmp);

    // not a repo
    let plain = tmp.path().join("plain");
    std::fs::create_dir_all(&plain).unwrap();
    let err = worktree::create(&home, "w1", Lane::Impl, &plain, None).unwrap_err();
    assert_eq!(err.code, "WORKTREE_NOT_A_REPO");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(err.remedy.unwrap().contains("--no-worktree"));

    let (tmp2, repo) = fixture_repo();
    let home2 = home_in(&tmp2);

    // path already exists — refused BEFORE git runs, so the message is a2a's
    let taken = home2.worktrees_dir().join("w-taken");
    std::fs::create_dir_all(&taken).unwrap();
    let err = worktree::create(&home2, "w-taken", Lane::Impl, &repo, None).unwrap_err();
    assert_eq!(err.code, "WORKTREE_PATH_EXISTS");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(
        err.message.contains("w-taken"),
        "message must name a2a's path: {}",
        err.message
    );

    // branch already exists
    git_ok(&repo, &["branch", "a2a/impl/w-branch"]);
    let err = worktree::create(&home2, "w-branch", Lane::Impl, &repo, None).unwrap_err();
    assert_eq!(err.code, "WORKTREE_BRANCH_EXISTS");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(
        err.message.contains("already exists"),
        "git's own stderr is the evidence: {}",
        err.message
    );
    assert!(
        !home2.worktrees_dir().join("w-branch").exists(),
        "a refused create leaves nothing behind"
    );
}

// ---------------------------------------------------------------------------
// info/exclude
// ---------------------------------------------------------------------------

#[test]
fn info_exclude_is_idempotent_and_never_touches_gitignore() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let gitignore = repo.join(".gitignore");
    std::fs::write(&gitignore, "target/\n").unwrap();
    let before = std::fs::read(&gitignore).unwrap();

    let wt = worktree::create(&home, "w-excl", Lane::Impl, &repo, None).unwrap();
    assert!(worktree::add_to_info_exclude(&wt.repo, &wt.path).unwrap());
    assert!(!worktree::add_to_info_exclude(&wt.repo, &wt.path).unwrap());

    let excl = std::fs::read_to_string(repo.join(".git/info/exclude")).unwrap();
    let line = wt.path.display().to_string();
    assert_eq!(
        excl.lines().filter(|l| *l == line).count(),
        1,
        "the absolute path appears exactly once:\n{excl}"
    );
    assert_eq!(
        std::fs::read(&gitignore).unwrap(),
        before,
        ".gitignore is not a2a's file to write"
    );
}

#[test]
fn info_exclude_creates_the_info_dir_and_leaves_no_gitignore() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let info = repo.join(".git/info");
    if info.exists() {
        std::fs::remove_dir_all(&info).unwrap();
    }
    assert!(!repo.join(".gitignore").exists());

    let wt = worktree::create(&home, "w-noinfo", Lane::Impl, &repo, None).unwrap();
    assert!(worktree::add_to_info_exclude(&wt.repo, &wt.path).unwrap());
    assert!(repo.join(".git/info/exclude").is_file());
    assert!(
        !repo.join(".gitignore").exists(),
        "a2a must not conjure a .gitignore"
    );
}

#[test]
fn info_exclude_resolves_a_gitdir_file_and_never_errors() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    // A linked worktree is itself a "repo" whose `.git` is a FILE.
    let outer = worktree::create(&home, "w-outer", Lane::Impl, &repo, None).unwrap();
    assert!(outer.path.join(".git").is_file());

    let inner = home.worktrees_dir().join("w-inner-marker");
    assert!(worktree::add_to_info_exclude(&outer.path, &inner).unwrap());
    // resolved through `gitdir:` into the parent repo's per-worktree git dir
    let per_wt = repo.join(".git/worktrees/w-outer/info/exclude");
    assert!(per_wt.is_file(), "expected {}", per_wt.display());
    assert!(std::fs::read_to_string(&per_wt)
        .unwrap()
        .contains(&inner.display().to_string()));

    // An unresolvable git dir is belt-and-braces failing, never an error.
    let plain = tmp.path().join("no-repo-here");
    std::fs::create_dir_all(&plain).unwrap();
    assert!(!worktree::add_to_info_exclude(&plain, &inner).unwrap());
}

// ---------------------------------------------------------------------------
// status / remove
// ---------------------------------------------------------------------------

#[test]
fn status_reports_untracked_all_and_ahead() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let wt = worktree::create(&home, "w-status", Lane::Impl, &repo, None).unwrap();

    assert_eq!(
        worktree::status(&wt.path, &wt.repo, &wt.branch, &wt.base_ref).unwrap(),
        Status {
            present: true,
            dirty: vec![],
            ahead: 0
        }
    );

    write(&wt.path.join("a.txt"), "two\n");
    git_ok(&wt.path, &["add", "a.txt"]);
    git_ok(&wt.path, &["commit", "-q", "-m", "edit"]);
    write(&wt.path.join(".github/workflows/evil.yml"), "on: push\n");

    let st = worktree::status(&wt.path, &wt.repo, &wt.branch, &wt.base_ref).unwrap();
    assert!(st.present);
    assert_eq!(st.ahead, 1, "one commit past the base");
    assert_eq!(
        st.dirty,
        vec![".github/workflows/evil.yml".to_string()],
        "--untracked-files=all is the pin: plain --porcelain says `.github/`"
    );

    // Absent path ⇒ present:false and nothing else claimed.
    let gone = home.worktrees_dir().join("never-made");
    assert_eq!(
        worktree::status(&gone, &wt.repo, &wt.branch, &wt.base_ref).unwrap(),
        Status {
            present: false,
            dirty: vec![],
            ahead: 0
        }
    );
}

#[test]
fn remove_refuses_dirty_then_forces_and_the_branch_survives() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let wt = worktree::create(&home, "w-rm", Lane::Impl, &repo, None).unwrap();
    write(&wt.path.join("a.txt"), "two\n");
    git_ok(&wt.path, &["add", "a.txt"]);
    git_ok(&wt.path, &["commit", "-q", "-m", "work"]);
    write(&wt.path.join("scratch.txt"), "uncommitted\n");

    let err = worktree::remove(&wt.repo, &wt.path, false).unwrap_err();
    assert_eq!(err.code, "WORKTREE_DIRTY");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(err.remedy.unwrap().contains("--force"));
    assert!(wt.path.exists(), "a refused remove removes nothing");

    worktree::remove(&wt.repo, &wt.path, true).unwrap();
    assert!(!wt.path.exists());
    assert!(
        git_ok(&repo, &["branch", "--list", "a2a/*"]).contains(&wt.branch),
        "the branch IS the deliverable — a2a never deletes it"
    );
    // …and git no longer lists the worktree (prune always runs).
    assert!(!git_ok(&repo, &["worktree", "list", "--porcelain"]).contains("w-rm"));
}

#[test]
fn remove_of_a_clean_worktree_needs_no_force() {
    let (tmp, repo) = fixture_repo();
    let home = home_in(&tmp);
    let wt = worktree::create(&home, "w-rm-clean", Lane::Impl, &repo, None).unwrap();
    worktree::remove(&wt.repo, &wt.path, false).unwrap();
    assert!(!wt.path.exists());
    assert!(git_ok(&repo, &["branch", "--list", "a2a/*"]).contains(&wt.branch));
}

// ---------------------------------------------------------------------------
// Input hygiene, asserted as source (spec §5)
// ---------------------------------------------------------------------------

/// `src/worktree.rs` with every comment blanked and string literals intact —
/// the same idiom as the broker's async-discipline scans, minus raw strings
/// (asserted absent) and minus char literals holding a quote (ditto), both of
/// which the module has no need for.
fn worktree_code() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/worktree.rs");
    let src = std::fs::read_to_string(&path).expect("src/worktree.rs exists");
    assert!(
        !src.contains("r#\""),
        "the scanner does not track hashed raw strings; keep them out of src/worktree.rs"
    );
    assert!(
        !src.contains("'\"'"),
        "a char literal holding a quote would derail the scanner; spell it as a byte"
    );
    let ch: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < ch.len() {
        // line comment
        if ch[i] == '/' && i + 1 < ch.len() && ch[i + 1] == '/' {
            while i < ch.len() && ch[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // block comment (nesting, as rustc does)
        if ch[i] == '/' && i + 1 < ch.len() && ch[i + 1] == '*' {
            let mut depth = 1;
            i += 2;
            while i < ch.len() && depth > 0 {
                if ch[i] == '/' && i + 1 < ch.len() && ch[i + 1] == '*' {
                    depth += 1;
                    i += 2;
                } else if ch[i] == '*' && i + 1 < ch.len() && ch[i + 1] == '/' {
                    depth -= 1;
                    i += 2;
                } else {
                    if ch[i] == '\n' {
                        out.push('\n');
                    }
                    i += 1;
                }
            }
            continue;
        }
        // string literal, kept whole
        if ch[i] == '"' {
            out.push('"');
            i += 1;
            while i < ch.len() {
                if ch[i] == '\\' && i + 1 < ch.len() {
                    out.push(ch[i]);
                    out.push(ch[i + 1]);
                    i += 2;
                    continue;
                }
                out.push(ch[i]);
                i += 1;
                if ch[i - 1] == '"' {
                    break;
                }
            }
            continue;
        }
        out.push(ch[i]);
        i += 1;
    }
    out
}

/// Every git invocation is an argv array. A shell in here would turn a brief's
/// text — or a branch name — into code.
#[test]
fn worktree_never_builds_a_command_line() {
    let code = worktree_code();
    for (n, line) in code.lines().enumerate() {
        let at = n + 1;
        assert!(
            !line.contains("\"sh\""),
            "src/worktree.rs:{at}: a shell has no business here:\n{line}"
        );
        assert!(
            !line.contains("/bin/sh"),
            "src/worktree.rs:{at}: a shell has no business here:\n{line}"
        );
        assert!(
            !line.contains("\"-c\""),
            "src/worktree.rs:{at}: `-c` is how a shell is handed a command line:\n{line}"
        );
    }
    // No string literal is a command LINE — argv elements never carry the
    // program name, so anything starting `git ` is a shell string in disguise.
    assert!(
        !code.contains("\"git "),
        "src/worktree.rs builds a command line rather than an argv array"
    );
    // …and the only program this module runs is git.
    let mut spawns = 0;
    let mut at = 0;
    while let Some(found) = code[at..].find("Command::new(") {
        let start = at + found + "Command::new(".len();
        let rest = &code[start..];
        assert!(
            rest.starts_with("prog") || rest.starts_with("\"git\""),
            "src/worktree.rs spawns something that is not git: {}",
            rest.chars().take(40).collect::<String>()
        );
        spawns += 1;
        at = start;
    }
    assert_eq!(spawns, 1, "expected exactly one spawn site, found {spawns}");
}
