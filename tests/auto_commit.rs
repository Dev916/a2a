// tests/auto_commit.rs
//
// Framework commit on `done`, end to end on the exec transport (the stub tier:
// `common::a2a` pins `A2A_BROKER_DISABLE`, so every worker here is concluded by
// the CLI's own reconcile). The app-server leg — and the ordering against
// `auto_review` — lives in `tests/auto_review.rs`, which already owns a broker
// fixture.
//
// Every worker is one `a2a spawn` into a REAL git repo in a temp dir; the
// worker's "change" is a file the stub writes into its `-C` workspace
// (`STUB_WRITE`), and its result is a devloop-shaped `result.json`
// (`STUB_LAST_MSG`) whose `files_changed` drives the staging policy.

mod common;

use common::*;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

fn devloop(files: &[&str], summary: &str) -> String {
    json!({
        "status": "pass",
        "iterations": 1,
        "summary": summary,
        "evidence": {"logs_excerpts": []},
        "files_changed": files,
        "next_step_hint": null,
    })
    .to_string()
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("git is on PATH");
    assert!(
        out.status.success(),
        "git {args:?} in {dir:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

struct World {
    tmp: TempDir,
    home: PathBuf,
    repo: PathBuf,
    brief: PathBuf,
    head: String,
}

fn world() -> World {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = tmp.path().join("proj");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["config", "user.email", "human@example.invalid"]);
    git(&repo, &["config", "user.name", "human"]);
    git(&repo, &["config", "commit.gpgsign", "false"]);
    git(&repo, &["config", "core.hooksPath", "no-such-hooks"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "base"]);
    let repo = std::fs::canonicalize(&repo).unwrap();
    let head = git(&repo, &["rev-parse", "HEAD"]).trim().to_string();
    let brief = tmp.path().join("brief.md");
    std::fs::write(
        &brief,
        "---\nacceptance_criteria:\n  - the file lands\nverify:\n  - \"true\"\n---\ndo the thing\n",
    )
    .unwrap();
    World {
        tmp,
        home,
        repo,
        brief,
        head,
    }
}

impl World {
    fn cli(&self) -> assert_cmd::Command {
        a2a(&self.home, self.tmp.path())
    }

    /// Spawn, wait for a terminal state, return the id.
    fn run_worker(&self, lane: &str, result: &str, write: &str, extra: &[&str]) -> String {
        let mut c = self.cli();
        c.env("STUB_LAST_MSG", result);
        if !write.is_empty() {
            c.env("STUB_WRITE", write);
        }
        let out = c
            .args(["--json", "spawn", "--lane", lane, "--cwd"])
            .arg(&self.repo)
            .arg("--brief")
            .arg(&self.brief)
            .args(extra)
            .assert()
            .success();
        let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
        let id = v["data"]["id"].as_str().unwrap().to_string();
        self.cli()
            .args(["wait", &id, "--until", "terminal", "--timeout", "30s"])
            .assert()
            .code(0);
        id
    }

    fn snap(&self, id: &str) -> Value {
        read_snapshot(&self.home, id)
    }

    fn events(&self, id: &str) -> Vec<Value> {
        std::fs::read_to_string(self.home.join("workers").join(id).join("events.jsonl"))
            .unwrap()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }

    fn commit_events(&self, id: &str) -> Vec<Value> {
        self.events(id)
            .into_iter()
            .filter(|e| {
                matches!(
                    e["kind"].as_str(),
                    Some("result_committed" | "commit_skipped" | "commit_failed")
                )
            })
            .collect()
    }

    fn worktree(&self, id: &str) -> PathBuf {
        std::fs::canonicalize(self.home.join("worktrees").join(id)).unwrap()
    }
}

/// The headline: a done impl worker in its own worktree with
/// `files_changed: [a.txt]` gets exactly one commit on its branch, carrying
/// a.txt, with the pinned identity and the specified message; the sha is in
/// state.json, events.jsonl and result.json, and the worker is still `done`.
#[test]
fn a_done_impl_worktree_worker_is_committed_once_on_its_branch() {
    let w = world();
    let id = w.run_worker("impl", &devloop(&["a.txt"], "add a.txt\nmore detail"), "a.txt", &[]);
    let branch = format!("a2a/impl/{id}");

    let snap = w.snap(&id);
    assert_eq!(snap["state"], "done", "{snap}");
    assert_eq!(snap["auto_commit_enabled"], true);
    let sha = snap["committed_sha"].as_str().expect("a committed sha").to_string();
    assert_eq!(snap["auto_commit"]["outcome"], "committed", "{snap}");
    assert_eq!(snap["auto_commit"]["sha"], json!(sha));

    // Exactly one commit beyond the base, on the worker's branch, holding a.txt.
    let range = format!("{}..{branch}", w.head);
    let log = git(&w.repo, &["rev-list", &range]);
    assert_eq!(log.lines().collect::<Vec<_>>(), vec![sha.as_str()], "{log}");
    let files = git(&w.repo, &["show", "--name-only", "--format=", &sha]);
    assert_eq!(files.trim(), "a.txt");
    let who = git(&w.repo, &["show", "-s", "--format=%an <%ae>|%cn <%ce>", &sha]);
    assert_eq!(who.trim(), "a2a <a2a@localhost>|a2a <a2a@localhost>");
    let msg = git(&w.repo, &["show", "-s", "--format=%B", &sha]);
    assert_eq!(
        msg.trim_end(),
        format!(
            "impl {id}: add a.txt\n\nWorker: {id} (gpt-5.5/high, exec)\nCo-Authored-By: Codex <noreply@openai.com>"
        )
    );
    // The main checkout is untouched.
    assert_eq!(git(&w.repo, &["rev-parse", "HEAD"]).trim(), w.head);
    assert_eq!(git(&w.worktree(&id), &["status", "--porcelain"]), "");

    // One lifecycle row, no `fsm` key, the documented payload.
    let rows = w.commit_events(&id);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["kind"], "result_committed");
    assert_eq!(rows[0]["class"], "lifecycle");
    assert_eq!(
        rows[0]["raw"],
        json!({"sha": sha, "files": 1, "branch": branch})
    );

    // result.json carries the sha too.
    let rj: Value = serde_json::from_str(
        &std::fs::read_to_string(w.home.join("workers").join(&id).join("result.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(rj["committed_sha"], json!(sha));

    // Re-reconciling never commits twice.
    w.cli().args(["status", &id]).assert().code(0);
    assert_eq!(w.commit_events(&id).len(), 1);
    assert_eq!(git(&w.repo, &["rev-list", &range]).lines().count(), 1);
}

/// `--no-worktree` with `files_changed`: only the listed paths are staged and
/// committed onto the shared cwd's current branch; an unlisted dirty file in
/// the same cwd stays exactly as the human left it.
#[test]
fn a_shared_cwd_commits_only_the_listed_paths() {
    let w = world();
    std::fs::write(w.repo.join("human.txt"), "mine\n").unwrap();
    std::fs::write(w.repo.join("README.md"), "edited by a human\n").unwrap();
    let id = w.run_worker(
        "impl",
        &devloop(&["a.txt"], "add a"),
        "a.txt",
        &["--no-worktree"],
    );
    let snap = w.snap(&id);
    let sha = snap["committed_sha"].as_str().expect("a sha").to_string();
    assert_eq!(git(&w.repo, &["rev-parse", "HEAD"]).trim(), sha);
    assert_eq!(
        git(&w.repo, &["show", "--name-only", "--format=", &sha]).trim(),
        "a.txt"
    );
    let status = git(&w.repo, &["status", "--porcelain"]);
    assert!(status.contains("?? human.txt"), "{status}");
    assert!(status.contains(" M README.md"), "{status}");
    assert_eq!(w.commit_events(&id)[0]["raw"]["branch"], "main");
}

/// `--no-worktree` without `files_changed`: never `-A` in a shared cwd. No
/// commit, no failure, and the row says why.
#[test]
fn a_shared_cwd_without_files_changed_is_skipped_with_a_reason() {
    let w = world();
    let id = w.run_worker("impl", &devloop(&[], "did things"), "a.txt", &["--no-worktree"]);
    let snap = w.snap(&id);
    assert_eq!(snap["state"], "done");
    assert_eq!(snap["committed_sha"], Value::Null);
    assert_eq!(snap["auto_commit"]["outcome"], "skipped", "{snap}");
    let rows = w.commit_events(&id);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["kind"], "commit_skipped");
    assert_eq!(
        rows[0]["raw"],
        json!({"outcome": "skipped", "reason": "no files_changed in a shared cwd"})
    );
    assert_eq!(git(&w.repo, &["rev-parse", "HEAD"]).trim(), w.head);
    assert!(git(&w.repo, &["status", "--porcelain"]).contains("?? a.txt"));
}

/// A worker that changed nothing: `clean`, no commit, no failure.
#[test]
fn an_empty_tree_is_clean_and_makes_no_commit() {
    let w = world();
    let id = w.run_worker("impl", &devloop(&[], "nothing to do"), "", &[]);
    let snap = w.snap(&id);
    assert_eq!(snap["state"], "done");
    assert_eq!(snap["auto_commit"]["outcome"], "clean", "{snap}");
    assert_eq!(snap["committed_sha"], Value::Null);
    let branch = format!("a2a/impl/{id}");
    assert_eq!(
        git(&w.repo, &["rev-parse", &branch]).trim(),
        w.head,
        "no commit on the worker's branch"
    );
    assert_eq!(w.commit_events(&id)[0]["kind"], "commit_skipped");
}

/// `--no-commit` on an impl worker: the decision is recorded at spawn, and
/// nothing is committed or recorded at `done`.
#[test]
fn no_commit_turns_it_off_for_one_worker() {
    let w = world();
    let id = w.run_worker(
        "impl",
        &devloop(&["a.txt"], "add a"),
        "a.txt",
        &["--no-commit"],
    );
    let snap = w.snap(&id);
    assert_eq!(snap["state"], "done");
    assert_eq!(snap["auto_commit_enabled"], false);
    assert_eq!(snap["auto_commit"], Value::Null);
    assert!(w.commit_events(&id).is_empty());
    let branch = format!("a2a/impl/{id}");
    assert_eq!(git(&w.repo, &["rev-parse", &branch]).trim(), w.head);
    assert!(git(&w.worktree(&id), &["status", "--porcelain"]).contains("?? a.txt"));
}

/// A git failure (a stale `index.lock` in the shared repo) never changes the
/// worker: it stays `done`, and `commit_failed` records the reason.
#[test]
fn a_git_failure_leaves_the_worker_done_and_records_why() {
    let w = world();
    std::fs::write(w.repo.join(".git/index.lock"), "").unwrap();
    let id = w.run_worker(
        "impl",
        &devloop(&["a.txt"], "add a"),
        "a.txt",
        &["--no-worktree"],
    );
    let snap = w.snap(&id);
    assert_eq!(snap["state"], "done", "{snap}");
    assert_eq!(snap["committed_sha"], Value::Null);
    assert_eq!(snap["auto_commit"]["outcome"], "failed", "{snap}");
    let rows = w.commit_events(&id);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["kind"], "commit_failed");
    let reason = rows[0]["raw"]["reason"].as_str().unwrap();
    assert!(reason.contains("index.lock"), "{reason}");
    assert!(rows[0]["raw"].get("fsm").is_none());
    assert_eq!(git(&w.repo, &["rev-parse", "HEAD"]).trim(), w.head);
}

/// The lane defaults: general is off unless `--commit` asks; review/research
/// can never commit, and asking is a warning rather than a refusal.
#[test]
fn general_commits_only_on_request_and_review_never() {
    let w = world();
    let id = w.run_worker("general", "plain answer", "g.txt", &["--worktree"]);
    assert_eq!(w.snap(&id)["auto_commit_enabled"], false);
    assert!(w.commit_events(&id).is_empty());

    let id = w.run_worker("general", "did g2", "g2.txt", &["--worktree", "--commit"]);
    let snap = w.snap(&id);
    assert_eq!(snap["auto_commit"]["outcome"], "committed", "{snap}");
    let sha = snap["committed_sha"].as_str().unwrap();
    assert_eq!(
        git(&w.repo, &["show", "--name-only", "--format=%s", sha]).trim(),
        format!("general {id}: did g2\n\ng2.txt")
    );

    let out = w
        .cli()
        .env("STUB_LAST_MSG", r#"{"findings":[]}"#)
        .args(["spawn", "--lane", "review", "--cwd"])
        .arg(&w.repo)
        .arg("--brief")
        .arg(&w.brief)
        .arg("--commit")
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).into_owned();
    assert!(stderr.contains("--commit has no effect on the review lane"), "{stderr}");
}

/// `respawn` inherits the parent's decision, whichever way it went.
#[test]
fn respawn_inherits_the_parents_decision() {
    let w = world();
    for (extra, want) in [(&["--no-commit"][..], false), (&[][..], true)] {
        let parent = w.run_worker("impl", &devloop(&[], "x"), "", extra);
        let out = w
            .cli()
            .env("STUB_LAST_MSG", devloop(&[], "x"))
            .args(["--json", "respawn", &parent])
            .assert()
            .success();
        let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
        assert_eq!(v["data"]["auto_commit"], want, "{v}");
        let child = v["data"]["id"].as_str().unwrap();
        assert_eq!(w.snap(child)["auto_commit_enabled"], want);
    }
}

/// `files_changed` is worker-controlled: pathspec magic, globs and escapes
/// each fail closed with `commit_failed` and stage nothing — while a plain
/// listed path in the same shape of world still commits.
#[test]
fn hostile_files_changed_entries_fail_closed() {
    for hostile in [":/", "*", ":!a.txt", "../x"] {
        let w = world();
        let id = w.run_worker(
            "impl",
            &devloop(&[hostile], "sneaky"),
            "a.txt",
            &["--no-worktree"],
        );
        let snap = w.snap(&id);
        assert_eq!(snap["state"], "done", "{hostile}: {snap}");
        assert_eq!(snap["auto_commit"]["outcome"], "failed", "{hostile}: {snap}");
        let rows = w.commit_events(&id);
        assert_eq!(rows.len(), 1, "{hostile}: {rows:?}");
        assert_eq!(rows[0]["kind"], "commit_failed", "{hostile}");
        assert_eq!(git(&w.repo, &["rev-parse", "HEAD"]).trim(), w.head, "{hostile}");
        assert_eq!(
            git(&w.repo, &["diff", "--cached", "--name-only"]),
            "",
            "{hostile}: nothing left staged"
        );
        assert!(git(&w.repo, &["status", "--porcelain"]).contains("?? a.txt"), "{hostile}");
    }
    // ...and the plain spelling still commits.
    let w = world();
    let id = w.run_worker("impl", &devloop(&["a.txt"], "add a"), "a.txt", &["--no-worktree"]);
    assert_eq!(w.snap(&id)["auto_commit"]["outcome"], "committed");
}
