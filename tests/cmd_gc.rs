// tests/cmd_gc.rs
mod common;
use common::*;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

#[test]
fn dry_run_lists_but_deletes_nothing_and_age_zero_reaps_with_tombstone() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(0);
    let out = a2a(&home, tmp.path()).args(["--json", "gc", "--dry-run"]).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["reclaimable"][0]["id"], id.as_str());
    assert!(home.join("workers").join(&id).exists(), "dry-run must not delete");
    a2a(&home, tmp.path()).args(["gc", "--age", "0s"]).assert().success();
    assert!(!home.join("workers").join(&id).exists());
    let index = std::fs::read_to_string(home.join("index.jsonl")).unwrap();
    let t: serde_json::Value = serde_json::from_str(index.lines().next().unwrap()).unwrap();
    assert_eq!(t["id"], id.as_str());
    assert_eq!(t["session_id"], "stub-sess-1");
}

#[test]
fn non_terminal_workers_are_never_removed() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "20")]);
    a2a(&home, tmp.path()).args(["gc", "--age", "0s"]).assert().success();
    assert!(home.join("workers").join(&id).exists(), "running worker must survive gc");
    // `--force` widens the WORKTREE rule and nothing else: a worker somebody
    // can still act on stays, flag or no flag.
    a2a(&home, tmp.path()).args(["gc", "--age", "0s", "--force"]).assert().success();
    assert!(
        home.join("workers").join(&id).exists(),
        "--force must not reach a non-terminal worker"
    );
}

#[test]
fn invalid_names_and_symlinks_are_skipped_victims_survive() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let workers = home.join("workers");
    std::fs::create_dir_all(workers.join("..evil")).unwrap();
    std::fs::create_dir_all(workers.join("not-an-id")).unwrap();
    let victim = tmp.path().join("victim"); std::fs::create_dir_all(&victim).unwrap();
    std::fs::write(victim.join("precious.txt"), "keep me").unwrap();
    std::os::unix::fs::symlink(&victim, workers.join("evil-link")).unwrap();
    let out = a2a(&home, tmp.path()).args(["gc", "--age", "0s"]).assert().success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(stderr.contains("..evil") && stderr.contains("not-an-id") && stderr.contains("evil-link"));
    assert!(workers.join("..evil").exists() && workers.join("not-an-id").exists());
    assert!(victim.join("precious.txt").exists(), "symlink target must survive");
}

#[test]
fn codex_home_is_never_touched() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let codex_home = tmp.path().join("codex-home");
    std::fs::create_dir_all(codex_home.join("sessions")).unwrap();
    std::fs::write(codex_home.join("sessions/sentinel"), "x").unwrap();
    a2a(&home, tmp.path()).env("CODEX_HOME", &codex_home)
        .args(["gc", "--age", "0s"]).assert().success();
    assert!(codex_home.join("sessions/sentinel").exists());
}

// ===========================================================================
// Task 7: the worktree rails.
//
// Every case below spawns a REAL impl-lane worker (stub codex) whose cwd is a
// REAL git repo in a tempdir, so the worker gets a REAL `git worktree` — the
// only way to assert that gc refuses live work, that it prunes the parent
// repo's registration, and that the branch outlives all of it. Nothing under
// `$HOME` is read or written; every repo and home is a `tempfile::tempdir()`.
// ===========================================================================

/// `git -C <dir> <args…>`, asserting success.
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

/// One finished impl-lane worker and its tree.
struct WtWorker {
    tmp: TempDir,
    home: PathBuf,
    /// The parent repo — the only handle gc has on the worktree's git side.
    repo: PathBuf,
    id: String,
    /// `$A2A_HOME/worktrees/<id>`, canonical.
    wt: PathBuf,
    /// `a2a/impl/<id>` — the deliverable, which nothing here may delete.
    branch: String,
}

/// A devloop-shaped result (`templates/impl-result.schema.json`) — what an impl
/// worker has to write now that the lane ships a default schema.
const DEVLOOP_RESULT: &str =
    r#"{"status":"pass","iterations":1,"summary":"ok","evidence":{},"files_changed":[],"next_step_hint":null}"#;

/// Spawn an impl-lane worker into a fresh repo (`seed` writes anything extra
/// the base commit should carry), wait for it to die, and settle it so gc's
/// own `reconcile` is a no-op.
fn spawn_worktree_worker(seed: impl FnOnce(&Path)) -> WtWorker {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = tmp.path().join("proj");
    init_repo(&repo);
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    seed(&repo);
    git_ok(&repo, &["add", "-A"]);
    git_ok(&repo, &["commit", "-q", "-m", "base"]);
    let repo = std::fs::canonicalize(&repo).unwrap();
    let brief = write_brief(
        tmp.path(),
        "---\nacceptance_criteria:\n  - it works\nverify:\n  - true\n---\ndo the thing\n",
    );

    let out = a2a(&home, tmp.path())
        // The impl lane's default result schema (spec §4.9) is enforced on
        // every impl result, so the fixture writes the devloop shape rather
        // than the stub's plain-text `done` — a worker that reached its
        // terminal state cleanly is the one gc's rails are about.
        .env("STUB_LAST_MSG", DEVLOOP_RESULT)
        .args(["--json", "spawn", "--lane", "impl", "--cwd"])
        .arg(&repo)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id = v["data"]["id"].as_str().expect("an id").to_string();
    wait_dead(&home, &id, 30);
    let _ = a2a(&home, tmp.path()).args(["status", &id]).output();

    let snap = read_snapshot(&home, &id);
    let wt = PathBuf::from(
        snap["worktree"]["path"]
            .as_str()
            .unwrap_or_else(|| panic!("an impl worker gets a worktree: {snap}")),
    );
    let branch = snap["worktree"]["branch"].as_str().unwrap().to_string();
    assert!(wt.is_dir(), "the worktree is a real directory: {}", wt.display());
    WtWorker { tmp, home, repo, id, wt, branch }
}

/// `a2a --json gc <extra…>` → the `data` payload.
fn gc_json(w: &WtWorker, extra: &[&str]) -> Value {
    let out = a2a(&w.home, w.tmp.path())
        .args(["--json", "gc"])
        .args(extra)
        .assert()
        .success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    v["data"].clone()
}

fn a2a_branches(w: &WtWorker) -> String {
    git_ok(&w.repo, &["branch", "--list", "a2a/*"])
}

fn worktree_registrations(w: &WtWorker) -> String {
    git_ok(&w.repo, &["worktree", "list", "--porcelain"])
}

fn tombstones(w: &WtWorker) -> Vec<Value> {
    std::fs::read_to_string(w.home.join("index.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// Uncommitted work in the tree stops gc dead — and `--force` takes the tree
/// away while the branch, as always, survives.
#[test]
fn a_dirty_worktree_is_kept_until_force_and_the_branch_survives() {
    let w = spawn_worktree_worker(|_| {});
    std::fs::write(w.wt.join("scratch.txt"), "uncommitted\n").unwrap();

    let data = gc_json(&w, &["--age", "0s"]);
    let kept = &data["kept"][0];
    assert_eq!(kept["id"], w.id.as_str(), "{data}");
    assert_eq!(kept["reason"], "worktree dirty");
    assert_eq!(
        kept["remedy"],
        format!(
            "commit or discard the work in {}, or re-run gc --force",
            w.wt.display()
        )
    );
    assert_eq!(kept["dirty"][0], "scratch.txt", "{kept}");
    assert!(data["removed"].as_array().unwrap().is_empty(), "{data}");
    assert!(
        w.home.join("workers").join(&w.id).exists(),
        "a refusal deletes nothing — not the worker dir"
    );
    assert!(w.wt.join("scratch.txt").exists(), "…and not the work in the tree");
    assert!(tombstones(&w).is_empty(), "…and not even a tombstone");

    let data = gc_json(&w, &["--age", "0s", "--force"]);
    assert_eq!(data["removed"][0], w.id.as_str(), "{data}");
    assert!(!w.home.join("workers").join(&w.id).exists());
    assert!(!w.wt.exists(), "the worktree directory goes with the worker");
    assert!(
        a2a_branches(&w).contains(&w.branch),
        "a2a NEVER deletes a branch: {}",
        a2a_branches(&w)
    );
    let listed = worktree_registrations(&w);
    assert!(
        !listed.contains(w.wt.to_str().unwrap()),
        "the registration is pruned: {listed}"
    );
    let t = &tombstones(&w)[0];
    assert_eq!(t["id"], w.id.as_str());
    assert_eq!(t["worktree_branch"], w.branch.as_str(), "{t}");
    assert_eq!(
        t["worktree_base_ref"].as_str().unwrap().len(),
        40,
        "the tombstone carries the base ref the branch was cut from: {t}"
    );
}

/// git itself does not protect unmerged commits — a2a does.
#[test]
fn an_unmerged_worktree_is_kept_until_force_and_the_commits_survive() {
    let w = spawn_worktree_worker(|_| {});
    std::fs::write(w.wt.join("done.txt"), "the deliverable\n").unwrap();
    git_ok(&w.wt, &["add", "-A"]);
    git_ok(&w.wt, &["commit", "-q", "-m", "work"]);
    let sha = git_ok(&w.wt, &["rev-parse", "HEAD"]).trim().to_string();

    let data = gc_json(&w, &["--age", "0s"]);
    let kept = &data["kept"][0];
    assert_eq!(kept["id"], w.id.as_str(), "{data}");
    assert_eq!(kept["reason"], "worktree unmerged");
    assert_eq!(kept["ahead"], 1, "{kept}");
    assert_eq!(
        kept["remedy"],
        format!(
            "merge or cherry-pick {} (a2a never merges), or re-run gc --force",
            w.branch
        )
    );
    assert!(w.home.join("workers").join(&w.id).exists());
    assert!(w.wt.exists(), "a clean-but-ahead tree is still not gc's to take");

    let data = gc_json(&w, &["--age", "0s", "--force"]);
    assert_eq!(data["removed"][0], w.id.as_str(), "{data}");
    assert!(!w.wt.exists());
    assert!(a2a_branches(&w).contains(&w.branch));
    assert_eq!(
        git_ok(&w.repo, &["rev-parse", &w.branch]).trim(),
        sha,
        "the commits are still reachable through the branch"
    );
}

/// The dry-run report tells the truth about what a worktree costs, and a clean
/// tree needs no `--force` at all.
#[test]
fn a_clean_worktree_is_sized_reported_and_reclaimed_without_force() {
    let w = spawn_worktree_worker(|repo| {
        std::fs::write(repo.join("big.bin"), vec![b'x'; 300_000]).unwrap();
    });

    let data = gc_json(&w, &["--dry-run", "--age", "0s"]);
    let row = &data["reclaimable"][0];
    assert_eq!(row["id"], w.id.as_str(), "{data}");
    assert_eq!(row["worktree"]["path"], w.wt.to_string_lossy().as_ref(), "{row}");
    assert_eq!(row["worktree"]["branch"], w.branch.as_str(), "{row}");
    assert_eq!(row["worktree"]["ahead"], 0, "{row}");
    assert_eq!(row["worktree"]["dirty_count"], 0, "{row}");
    assert!(
        row["bytes"].as_u64().unwrap() >= 300_000,
        "bytes must count the worktree, not just the worker dir: {row}"
    );
    assert!(w.wt.exists(), "a dry run deletes nothing");

    let data = gc_json(&w, &["--age", "0s"]);
    assert_eq!(data["removed"][0], w.id.as_str(), "{data}");
    assert!(!w.wt.exists());
    assert!(a2a_branches(&w).contains(&w.branch));
    let listed = worktree_registrations(&w);
    assert!(!listed.contains(w.wt.to_str().unwrap()), "{listed}");
}

/// A tree somebody deleted by hand is not a refusal: gc still owes the parent
/// repo the prune, which is only addressable because `wt.repo` is recorded.
#[test]
fn a_hand_deleted_worktree_is_reclaimed_and_its_registration_pruned() {
    let w = spawn_worktree_worker(|_| {});
    std::fs::remove_dir_all(&w.wt).unwrap();
    assert!(
        worktree_registrations(&w).contains(w.wt.to_str().unwrap()),
        "the stale registration is what this test is about"
    );

    let data = gc_json(&w, &["--age", "0s"]);
    assert_eq!(data["removed"][0], w.id.as_str(), "{data}");
    let listed = worktree_registrations(&w);
    assert!(
        !listed.contains(w.wt.to_str().unwrap()),
        "gc prunes the registration of a tree that is already gone: {listed}"
    );
    assert!(a2a_branches(&w).contains(&w.branch));
}

/// An unreachable parent repo must not make a worker un-reclaimable forever.
#[test]
fn a_worktree_whose_parent_repo_is_gone_is_still_reclaimed() {
    let w = spawn_worktree_worker(|_| {});
    // Dirty on purpose: with no repo there is nothing to ask and nothing to
    // refuse with, so the orphaned directory goes either way.
    std::fs::write(w.wt.join("scratch.txt"), "uncommitted\n").unwrap();
    std::fs::remove_dir_all(&w.repo).unwrap();

    let out = a2a(&w.home, w.tmp.path())
        .args(["--json", "gc", "--age", "0s"])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(
        stderr.contains("parent repo"),
        "the recovery is announced, never silent: {stderr}"
    );
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["removed"][0], w.id.as_str(), "{v}");
    assert!(!w.home.join("workers").join(&w.id).exists());
    assert!(!w.wt.exists(), "the orphaned tree is removed with it");
}

/// A tree gc cannot take away keeps its worker: a worker directory deleted
/// while its worktree is still registered would strand that registration with
/// no id left to name it. The tombstone stands either way — the id is retired
/// the moment it is written, and re-running gc is what finishes the job.
#[test]
fn a_worktree_removal_failure_keeps_the_worker_directory_and_the_tombstone() {
    use std::os::unix::fs::PermissionsExt;
    let w = spawn_worktree_worker(|_| {});
    let trees = w.home.join("worktrees");
    // The durable way to make `git worktree remove` fail (verified: exit 255,
    // `error: failed to delete …`): git may empty the tree, but it cannot
    // unlink it from a parent directory it is not allowed to write.
    let mode = std::fs::metadata(&trees).unwrap().permissions().mode();
    std::fs::set_permissions(&trees, std::fs::Permissions::from_mode(0o500)).unwrap();

    let out = a2a(&w.home, w.tmp.path())
        .args(["--json", "gc", "--age", "0s"])
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    // Restored before the assertions, so a red test cannot leave an
    // undeletable tempdir behind.
    std::fs::set_permissions(&trees, std::fs::Permissions::from_mode(mode)).unwrap();

    let data = &v["data"];
    assert_eq!(data["skipped"][0]["entry"], w.id.as_str(), "{data}");
    assert_eq!(data["skipped"][0]["reason"], "worktree remove failed", "{data}");
    assert!(data["removed"].as_array().unwrap().is_empty(), "{data}");
    assert!(
        w.home.join("workers").join(&w.id).exists(),
        "the worker directory is left in place, registration and all"
    );
    assert!(
        stderr.contains("WORKTREE_REMOVE_FAILED"),
        "the failure is announced: {stderr}"
    );
    assert_eq!(
        tombstones(&w)[0]["id"],
        w.id.as_str(),
        "the tombstone stands — the id stays retired"
    );
}
