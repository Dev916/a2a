//! `a2a verify`'s acceptance gate — the execution-bearing refusal and the scope
//! snapshot (Task 5). The command itself arrives in Task 6; everything here is
//! asserted at the module boundary the command will call.
//!
//! The gate cases are pure. The scope cases run REAL `git` in a
//! `tempfile::tempdir()` fixture repo, because `verify::scope`'s whole job is
//! to hand the gate the union changed-path set that `src/worktree.rs` computes
//! — the seam where a diff-only implementation silently stops seeing an
//! untracked `.github/workflows/evil.yml`. Nothing under `$HOME` is read or
//! written.

use a2a::commands::verify::{self, Scope};
use a2a::output::ExitCode;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

fn git_ok(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("git is on PATH");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// A repo whose behaviour does not depend on the developer's `~/.gitconfig`.
fn init_repo(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    git_ok(dir, &["init", "-q", "-b", "main"]);
    git_ok(dir, &["config", "user.email", "a2a@example.invalid"]);
    git_ok(dir, &["config", "user.name", "a2a test"]);
    git_ok(dir, &["config", "commit.gpgsign", "false"]);
    git_ok(dir, &["config", "core.hooksPath", "no-such-hooks"]);
}

/// One commit touching `src/a.rs` and `a.txt`, then, on top of it:
/// a *committed* edit to `src/a.rs`, an *uncommitted* edit to `a.txt`, an
/// untracked `note.txt` and an untracked `.github/workflows/evil.yml`.
///
/// Returns `(tmp, repo, base_ref)` — `base_ref` is the first commit.
fn workspace_with_a_stray_workflow() -> (TempDir, PathBuf, String) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    init_repo(&repo);
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("src/a.rs"), "fn main() {}\n").unwrap();
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    git_ok(&repo, &["add", "-A"]);
    git_ok(&repo, &["commit", "-q", "-m", "base"]);
    let base = git_ok(&repo, &["rev-parse", "HEAD"]).trim().to_string();

    std::fs::write(repo.join("src/a.rs"), "fn main() { /* worked */ }\n").unwrap();
    git_ok(&repo, &["add", "-A"]);
    git_ok(&repo, &["commit", "-q", "-m", "work"]);
    std::fs::write(repo.join("a.txt"), "two\n").unwrap();
    std::fs::write(repo.join("note.txt"), "scratch\n").unwrap();
    std::fs::create_dir_all(repo.join(".github/workflows")).unwrap();
    std::fs::write(
        repo.join(".github/workflows/evil.yml"),
        "on: push\njobs: {}\n",
    )
    .unwrap();

    (tmp, repo, base)
}

fn show(label: &str, scope: &Scope) {
    println!(
        "{label}: {}",
        serde_json::to_string(scope).expect("Scope serializes")
    );
}

#[test]
fn the_gate_refuses_a_diff_that_can_execute() {
    let changed: Vec<String> = [
        "src/main.rs",
        ".github/workflows/evil.yml",
        "docs/justfile.md",
        "Makefile",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    let hits = verify::gate(&changed);
    assert_eq!(
        hits,
        vec![
            ".github/workflows/evil.yml".to_string(),
            "Makefile".to_string()
        ],
        "sorted, and the near-miss docs/justfile.md is not in it"
    );

    let err = verify::exec_bearing_error(&hits);
    assert_eq!(err.code, "EXEC_BEARING_DIFF");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert_eq!(
        err.remedy.as_deref(),
        Some(
            "review the diff by hand, then re-run with --force-verify to run the brief's \
             commands anyway"
        )
    );
    let data = err
        .data
        .expect("an orchestrator branches on data, not prose");
    assert_eq!(data["count"], 2);
    assert_eq!(data["paths"][0], ".github/workflows/evil.yml");
    assert_eq!(data["paths"][1], "Makefile");
}

#[test]
fn a_clean_diff_passes_the_gate() {
    let changed: Vec<String> = ["src/main.rs", "src/lib.rs", "docs/README.md"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(verify::gate(&changed).is_empty());
}

#[test]
fn scope_over_a_real_worktree_sees_the_untracked_workflow_and_splits_the_declaration() {
    let (_tmp, repo, base) = workspace_with_a_stray_workflow();
    let declared: Vec<String> = ["src/**", "a.txt"].iter().map(|s| s.to_string()).collect();

    let scope = verify::scope(&repo, &base, &declared).expect("scope over a real repo");
    show("declared", &scope);

    // The UNION: a committed change, an uncommitted change, and two untracked
    // files — one of them inside a directory `git status --porcelain` alone
    // would have collapsed.
    assert_eq!(
        scope.changed,
        vec![
            ".github/workflows/evil.yml".to_string(),
            "a.txt".to_string(),
            "note.txt".to_string(),
            "src/a.rs".to_string(),
        ]
    );
    assert!(scope.files_in_scope_declared);
    assert_eq!(
        scope.outside_scope,
        vec![
            ".github/workflows/evil.yml".to_string(),
            "note.txt".to_string(),
        ],
        "`src/**` claims src/a.rs and `a.txt` claims itself; nothing claims the rest"
    );
    assert!(
        scope.diff_stat.contains("src/a.rs") && scope.diff_stat.contains("changed"),
        "diff_stat is git's own rendering: {:?}",
        scope.diff_stat
    );

    // And the gate fires on exactly the file the union exists to surface.
    let hits = verify::gate(&scope.changed);
    assert_eq!(hits, vec![".github/workflows/evil.yml".to_string()]);
}

#[test]
fn an_undeclared_scope_puts_nothing_outside_itself() {
    let (_tmp, repo, base) = workspace_with_a_stray_workflow();

    let scope = verify::scope(&repo, &base, &[]).expect("scope over a real repo");
    show("undeclared", &scope);

    assert!(!scope.files_in_scope_declared);
    assert!(
        scope.outside_scope.is_empty(),
        "nothing was declared, so nothing is outside it"
    );
    // The changed set is unaffected by the declaration.
    assert_eq!(scope.changed.len(), 4);
    assert!(scope
        .changed
        .contains(&".github/workflows/evil.yml".to_string()));
}

#[test]
fn a_clean_worktree_scopes_to_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    init_repo(&repo);
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    git_ok(&repo, &["add", "-A"]);
    git_ok(&repo, &["commit", "-q", "-m", "base"]);
    let base = git_ok(&repo, &["rev-parse", "HEAD"]).trim().to_string();

    let declared = vec!["src/**".to_string()];
    let scope = verify::scope(&repo, &base, &declared).expect("scope over a clean repo");
    show("clean", &scope);

    assert!(scope.changed.is_empty());
    assert!(scope.outside_scope.is_empty());
    assert!(scope.files_in_scope_declared);
    assert_eq!(scope.diff_stat, "");
    assert!(verify::gate(&scope.changed).is_empty());
}

#[test]
fn an_unknown_base_ref_surfaces_the_worktree_layers_typed_error() {
    let (_tmp, repo, _base) = workspace_with_a_stray_workflow();
    let err = verify::scope(&repo, "0000000000000000000000000000000000000000", &[])
        .expect_err("a base ref that is not in the repo");
    assert_eq!(err.exit, ExitCode::Precondition);
}
