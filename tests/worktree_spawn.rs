// tests/worktree_spawn.rs
//
// The spawn ⇄ worktree wiring, end to end against the REAL `git` binary in
// throwaway repos. Every worktree here is one `a2a spawn` created: the point of
// Task 3 is that nothing has to stage a genesis row by hand any more, so a test
// that staged one would no longer be testing the thing that ships.
//
// The load-bearing cases are the two-rule split on a cwd that is not a git work
// tree (an explicit `--worktree` refuses, a lane default warns and proceeds) and
// the lossless-rebuild pin: `state.json` is a derived checkpoint, so a worktree
// only it knew about would be a tree no `gc` could ever address.
mod common;
use common::*;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Run `git -C <dir> <args…>`, asserting success.
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

/// A repo with one commit, canonicalized (macOS `/var` → `/private/var`, which
/// is the form every path a2a records is in).
fn fixture_repo(under: &Path) -> PathBuf {
    let repo = under.join("proj");
    init_repo(&repo);
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    git_ok(&repo, &["add", "a.txt"]);
    git_ok(&repo, &["commit", "-q", "-m", "init"]);
    std::fs::canonicalize(&repo).unwrap()
}

/// A devloop-shaped result, for the stub to write as its last message.
///
/// The impl lane ships a DEFAULT result schema (spec §4.9), so
/// `reconcile::conclude_turn` now runs the schema gate on every impl worker
/// that produces a result — and the fixture's plain-text `done` fails it with
/// `result.md is not JSON` ⇒ `failed{schema_violation}`. Every impl worker here
/// that is expected to finish cleanly therefore pins this shape.
const DEVLOOP_RESULT: &str =
    r#"{"status":"pass","iterations":1,"summary":"ok","evidence":{},"files_changed":[],"next_step_hint":null}"#;

/// The impl lane's brief has to declare its acceptance criteria and how they
/// are verified (spec §4.7).
fn impl_brief(dir: &Path) -> PathBuf {
    write_brief(
        dir,
        "---\nacceptance_criteria:\n  - it works\nverify:\n  - cargo test\n---\ndo the thing\n",
    )
}

struct Spawned {
    id: String,
    data: serde_json::Value,
    stderr: String,
}

/// `a2a --json spawn --lane <lane> --cwd <cwd> --brief <brief> <extra…>`.
fn spawn(
    home: &Path,
    allowed: &Path,
    lane: &str,
    cwd: &Path,
    brief: &Path,
    extra: &[&str],
    env: &[(&str, &str)],
) -> Spawned {
    let mut c = a2a(home, allowed);
    c.args(["--json", "spawn", "--lane", lane, "--cwd"])
        .arg(cwd)
        .arg("--brief")
        .arg(brief)
        .args(extra);
    for (k, v) in env {
        c.env(k, v);
    }
    let out = c.assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    Spawned {
        id: v["data"]["id"].as_str().expect("an id").to_string(),
        data: v["data"].clone(),
        stderr: String::from_utf8_lossy(&out.get_output().stderr).to_string(),
    }
}

fn genesis(home: &Path, id: &str) -> serde_json::Value {
    let events = home.join("workers").join(id).join("events.jsonl");
    let text = std::fs::read_to_string(events).expect("events.jsonl");
    let first: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(first["raw"]["fsm"], "Spawn", "line 1 is genesis");
    first["raw"].clone()
}

fn status_json(home: &Path, allowed: &Path, id: &str) -> serde_json::Value {
    let out = a2a(home, allowed)
        .args(["--json", "status", id])
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    v["data"].clone()
}

// ---------------------------------------------------------------------------
// The default: the impl lane gets its own tree
// ---------------------------------------------------------------------------

/// The whole wiring in one pass: the lane default creates the tree, the worker
/// RUNS in it (`codex -C <worktree>`), the repo's `info/exclude` learns about
/// it, `.gitignore` is never touched, and the branch survives as the
/// deliverable.
#[test]
fn the_impl_lane_defaults_to_its_own_worktree_and_codex_runs_in_it() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = fixture_repo(tmp.path());
    let brief = impl_brief(tmp.path());
    let argv_file = tmp.path().join("argv.txt");

    let s = spawn(
        &home,
        tmp.path(),
        "impl",
        &repo,
        &brief,
        &[],
        &[
            ("STUB_ARGV_FILE", argv_file.to_str().unwrap()),
            ("STUB_LAST_MSG", DEVLOOP_RESULT),
        ],
    );
    let wt = std::fs::canonicalize(home.join("worktrees").join(&s.id)).unwrap();
    let branch = format!("a2a/impl/{}", s.id);

    // --json carries the same four fields `status --json` reports.
    assert_eq!(s.data["worktree"]["path"], serde_json::json!(wt), "{}", s.data);
    assert_eq!(s.data["worktree"]["branch"], serde_json::json!(branch));
    assert_eq!(s.data["worktree"]["repo"], serde_json::json!(repo));
    assert_eq!(s.data["cwd"], serde_json::json!(wt), "the cwd IS the worktree");
    assert!(
        s.stderr.contains(&format!("on {branch}")),
        "the human line names the branch: {}",
        s.stderr
    );

    // The tree is a real, checked-out worktree of the parent repo.
    assert!(wt.join("a.txt").is_file(), "the base commit is checked out");
    assert!(wt.join(".git").is_file(), "a linked worktree's .git is a FILE");
    let listed = git_ok(&repo, &["worktree", "list", "--porcelain"]);
    assert!(listed.contains(wt.to_str().unwrap()), "{listed}");
    let base = s.data["worktree"]["base_ref"].as_str().unwrap().to_string();
    assert_eq!(base.len(), 40, "a 40-hex commit, not a ref name: {base}");
    assert_eq!(base, git_ok(&repo, &["rev-parse", "HEAD"]).trim());
    // `--no-track`: a2a never writes upstream config into somebody's repo.
    let upstream = Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["config", "--get", &format!("branch.{branch}.remote")])
        .output()
        .unwrap();
    assert!(!upstream.status.success(), "no upstream may be configured");

    // Belt-and-braces: info/exclude, never .gitignore.
    let exclude = std::fs::read_to_string(repo.join(".git/info/exclude")).unwrap();
    assert!(
        exclude.lines().any(|l| l == wt.to_str().unwrap()),
        "the worktree path must be excluded: {exclude}"
    );
    assert!(!repo.join(".gitignore").exists(), ".gitignore is the human's file");

    // The codex child was pointed AT the worktree (`-C` comes from `snap.cwd`).
    wait_dead(&home, &s.id, 10);
    let argv = std::fs::read_to_string(&argv_file).expect("the stub recorded its argv");
    assert!(
        argv.contains(&format!("-C {}", wt.display())),
        "codex must run in the worktree: {argv}"
    );

    // …and the snapshot agrees with genesis on all of it.
    let data = status_json(&home, tmp.path(), &s.id);
    assert_eq!(data["worktree"]["branch"], serde_json::json!(branch), "{data}");
    assert_eq!(data["base_ref"], serde_json::json!(base), "{data}");
    let g = genesis(&home, &s.id);
    assert_eq!(g["worktree_path"], serde_json::json!(wt), "{g}");
    assert_eq!(g["worktree_branch"], serde_json::json!(branch), "{g}");
    assert_eq!(g["worktree_base_ref"], serde_json::json!(base), "{g}");
    assert_eq!(g["worktree_repo"], serde_json::json!(repo), "{g}");
    assert_eq!(g["base_ref"], serde_json::json!(base), "{g}");
    assert_eq!(g["cwd"], serde_json::json!(wt), "{g}");
}

/// The lossless pin. Genesis is the only writer of identity, so a worker's
/// worktree, its diff anchor and its result schema must all come back after the
/// derived checkpoint is deleted — otherwise `state.json` has quietly become
/// the source of truth for them.
#[test]
fn the_worktree_base_ref_and_schema_survive_losing_state_json() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = fixture_repo(tmp.path());
    let brief = impl_brief(tmp.path());
    let schema = tmp.path().join("impl-result.schema.json");
    std::fs::write(&schema, br#"{"type":"object","required":["summary"]}"#).unwrap();

    let s = spawn(
        &home,
        tmp.path(),
        "impl",
        &repo,
        &brief,
        &["--schema", schema.to_str().unwrap()],
        &[("STUB_LAST_MSG", r#"{"summary":"ok"}"#)],
    );
    wait_dead(&home, &s.id, 10);
    // Reading a worker is what advances it: this commits the terminal
    // transition to the WAL, so the rebuild below replays a FINISHED worker.
    a2a(&home, tmp.path())
        .args(["status", &s.id])
        .assert()
        .success();

    let wt = std::fs::canonicalize(home.join("worktrees").join(&s.id)).unwrap();
    let branch = format!("a2a/impl/{}", s.id);
    let base = s.data["worktree"]["base_ref"].as_str().unwrap().to_string();
    std::fs::remove_file(home.join("workers").join(&s.id).join("state.json")).unwrap();

    let data = status_json(&home, tmp.path(), &s.id);
    assert_eq!(data["worktree"]["path"], serde_json::json!(wt), "{data}");
    assert_eq!(data["worktree"]["branch"], serde_json::json!(branch), "{data}");
    assert_eq!(data["worktree"]["base_ref"], serde_json::json!(base), "{data}");
    assert_eq!(data["worktree"]["repo"], serde_json::json!(repo), "{data}");
    assert_eq!(data["base_ref"], serde_json::json!(base), "{data}");

    // ...and the rebuild PERSISTED all of it, schema_path included — the key
    // genesis has recorded since Phase 1 and the fold never read.
    let snap = read_snapshot(&home, &s.id);
    assert_eq!(snap["worktree"]["branch"], serde_json::json!(branch), "{snap}");
    assert_eq!(snap["base_ref"], serde_json::json!(base), "{snap}");
    // The recorded path is the worker's OWN copy of the schema, not the flag's
    // file: a worker is judged by something inside its own directory, so the
    // verdict cannot change when the operator edits or moves the original.
    let own = home.join("workers").join(&s.id).join("result-schema.json");
    assert_eq!(snap["schema_path"], serde_json::json!(own), "{snap}");
    assert_eq!(
        std::fs::read_to_string(&own).unwrap(),
        std::fs::read_to_string(&schema).unwrap()
    );
    assert_eq!(snap["schema_source"], serde_json::json!("flag"), "{snap}");
    assert!(
        snap.get("fold_rejections")
            .map(|r| r.as_array().is_some_and(|a| a.is_empty()))
            .unwrap_or(true),
        "a rebuild from zero must replay cleanly: {snap}"
    );
}

// ---------------------------------------------------------------------------
// The two-rule split on a cwd that is not a git work tree
// ---------------------------------------------------------------------------

/// An explicit `--worktree` is a REQUEST, and a request that cannot be honoured
/// is a refusal — before the first byte, so no worker and no tree is left.
#[test]
fn an_explicit_worktree_on_a_non_git_cwd_refuses_and_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let plain = tmp.path().join("plain");
    std::fs::create_dir_all(&plain).unwrap();
    let brief = write_brief(tmp.path(), "do the thing\n");

    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "general", "--worktree", "--cwd"])
        .arg(&plain)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "WORKTREE_NOT_A_REPO", "{v}");
    assert!(
        v["error"]["remedy"].as_str().unwrap().contains("--no-worktree"),
        "the remedy must name the opt-out: {v}"
    );
    // Nothing was written: the refusal is a gate, not a rollback.
    let workers = std::fs::read_dir(home.join("workers")).unwrap().count();
    assert_eq!(workers, 0, "a gate leaves no worker behind");
    assert_eq!(std::fs::read_dir(home.join("worktrees")).unwrap().count(), 0);
}

/// A lane DEFAULT is a policy, not a request: an impl worker in a directory
/// that is not a repo still runs, and the reason it has no tree is recorded in
/// genesis — so it survives the rebuild that makes the absence visible again.
#[test]
fn a_lane_default_worktree_on_a_non_git_cwd_warns_and_proceeds() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let plain = tmp.path().join("plain");
    std::fs::create_dir_all(&plain).unwrap();
    let brief = impl_brief(tmp.path());

    let s = spawn(
        &home,
        tmp.path(),
        "impl",
        &plain,
        &brief,
        &[],
        &[("STUB_LAST_MSG", DEVLOOP_RESULT)],
    );
    assert_eq!(s.data["worktree"], serde_json::Value::Null, "{}", s.data);
    assert!(
        s.stderr.contains("not inside a git work tree"),
        "the skip is warned about: {}",
        s.stderr
    );
    assert!(!home.join("worktrees").join(&s.id).exists());

    let g = genesis(&home, &s.id);
    assert_eq!(g["worktree_skipped"], serde_json::json!("not-a-git-repo"), "{g}");
    assert_eq!(g.get("worktree_path"), None, "{g}");
    assert_eq!(g.get("base_ref"), None, "{g}");

    wait_dead(&home, &s.id, 10);
    a2a(&home, tmp.path()).args(["status", &s.id]).assert().success();
    std::fs::remove_file(home.join("workers").join(&s.id).join("state.json")).unwrap();
    let _ = status_json(&home, tmp.path(), &s.id);
    let snap = read_snapshot(&home, &s.id);
    assert_eq!(
        snap["worktree_skipped"],
        serde_json::json!("not-a-git-repo"),
        "the explanation must survive a rebuild from zero: {snap}"
    );
    assert_eq!(snap["worktree"], serde_json::Value::Null, "{snap}");
}

// ---------------------------------------------------------------------------
// Opting out, and choosing the base
// ---------------------------------------------------------------------------

/// `--no-worktree` beats the lane default, says so exactly once, and still
/// records the diff anchor — `a2a verify` needs it in both cases.
#[test]
fn no_worktree_opts_out_but_keeps_the_diff_anchor() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = fixture_repo(tmp.path());
    let brief = impl_brief(tmp.path());

    let s = spawn(
        &home,
        tmp.path(),
        "impl",
        &repo,
        &brief,
        &["--no-worktree"],
        &[("STUB_LAST_MSG", DEVLOOP_RESULT)],
    );
    assert!(
        s.stderr
            .lines()
            .any(|l| l == "a2a: warning: --no-worktree: spawning directly in --cwd"),
        "the opt-out warning is a pin: {}",
        s.stderr
    );
    assert_eq!(s.data["worktree"], serde_json::Value::Null, "{}", s.data);
    assert_eq!(s.data["cwd"], serde_json::json!(repo), "{}", s.data);
    assert!(!home.join("worktrees").join(&s.id).exists());

    let head = git_ok(&repo, &["rev-parse", "HEAD"]).trim().to_string();
    let g = genesis(&home, &s.id);
    assert_eq!(g["base_ref"], serde_json::json!(head), "{g}");
    assert_eq!(
        status_json(&home, tmp.path(), &s.id)["base_ref"],
        serde_json::json!(head)
    );
}

/// `--base-ref` picks the commit the branch is cut from, and a ref that names
/// no commit refuses before anything is created.
#[test]
fn base_ref_chooses_the_branch_point_and_an_unknown_ref_refuses() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = fixture_repo(tmp.path());
    let brief = impl_brief(tmp.path());
    let first = git_ok(&repo, &["rev-parse", "HEAD"]).trim().to_string();
    std::fs::write(repo.join("b.txt"), "two\n").unwrap();
    git_ok(&repo, &["add", "b.txt"]);
    git_ok(&repo, &["commit", "-q", "-m", "second"]);

    let s = spawn(
        &home,
        tmp.path(),
        "impl",
        &repo,
        &brief,
        &["--base-ref", &first],
        &[],
    );
    assert_eq!(s.data["worktree"]["base_ref"], serde_json::json!(first), "{}", s.data);
    let wt = home.join("worktrees").join(&s.id);
    assert!(wt.join("a.txt").is_file());
    assert!(!wt.join("b.txt").exists(), "the tree is cut from --base-ref");

    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "impl", "--base-ref", "no/such/ref", "--cwd"])
        .arg(&repo)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "WORKTREE_BASE_UNKNOWN", "{v}");
    // One worktree (the good one) and one worker: the refusal left nothing.
    assert_eq!(std::fs::read_dir(home.join("worktrees")).unwrap().count(), 1);
    assert_eq!(std::fs::read_dir(home.join("workers")).unwrap().count(), 1);
}

/// A repo with no commit has nothing to branch from, and says so with a remedy
/// rather than letting git's paraphrase out.
#[test]
fn an_empty_repo_has_no_base_to_branch_from() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = tmp.path().join("empty");
    init_repo(&repo);
    let brief = impl_brief(tmp.path());

    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "impl", "--cwd"])
        .arg(&repo)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "WORKTREE_NO_BASE", "{v}");
    assert_eq!(std::fs::read_dir(home.join("workers")).unwrap().count(), 0);
}
