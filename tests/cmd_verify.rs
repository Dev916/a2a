//! `a2a verify` — the acceptance gate (Task 5) and the command that runs the
//! BRIEF's verify commands and writes `verification.json` (Task 6).
//!
//! The gate cases are pure. The scope cases run REAL `git` in a
//! `tempfile::tempdir()` fixture repo, because `verify::scope`'s whole job is
//! to hand the gate the union changed-path set that `src/worktree.rs` computes
//! — the seam where a diff-only implementation silently stops seeing an
//! untracked `.github/workflows/evil.yml`. The command cases spawn a REAL
//! fixture worker (stub codex) into a temp `A2A_HOME` and run the real CLI.
//! Nothing under `$HOME` is read or written.

mod common;

use a2a::commands::verify::{self, Scope};
use a2a::output::ExitCode;
use common::*;
use serde_json::Value;
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

// ===========================================================================
// Task 6 — the command: `a2a verify <id>`
//
// Every case below spawns a REAL worker through the real CLI (stub codex) into
// a temp `A2A_HOME`, then runs `a2a verify` against it. The workspace is the
// worker's own git worktree, which is what makes the diff gate and the scope
// snapshot mean anything.
// ===========================================================================

/// One spawned fixture worker, plus everything a verify assertion needs.
struct Worker {
    tmp: TempDir,
    home: PathBuf,
    /// Where `verify` will run the brief's commands: the worker's worktree when
    /// it has one, else its `--cwd`.
    workspace: PathBuf,
    id: String,
}

/// A brief with an impl-lane-shaped front matter: acceptance criteria, an
/// optional declared scope, and the verify commands under test.
fn brief_text(verify_cmds: &[&str], files_in_scope: &[&str]) -> String {
    let mut s = String::from("---\nacceptance_criteria:\n  - it works\n");
    if !files_in_scope.is_empty() {
        s.push_str("files_in_scope:\n");
        for f in files_in_scope {
            s.push_str(&format!("  - \"{f}\"\n"));
        }
    }
    if !verify_cmds.is_empty() {
        s.push_str("verify:\n");
        for c in verify_cmds {
            s.push_str(&format!("  - \"{c}\"\n"));
        }
    }
    s.push_str("---\ndo the thing\n");
    s
}

/// A devloop-shaped result (`templates/impl-result.schema.json`) — what an impl
/// worker has to write now that the lane ships a default schema.
const DEVLOOP_RESULT: &str =
    r#"{"status":"pass","iterations":1,"summary":"ok","evidence":{},"files_changed":[],"next_step_hint":null}"#;

/// Spawn one worker and settle it, so every later `reconcile` is a no-op and
/// the events-differential assertion has a stable baseline.
fn spawn_fixture(lane: &str, in_a_repo: bool, verify_cmds: &[&str], scope: &[&str]) -> Worker {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    if in_a_repo {
        init_repo(&work);
        std::fs::create_dir_all(work.join("src")).unwrap();
        std::fs::write(work.join("src/a.rs"), "fn main() {}\n").unwrap();
        std::fs::write(work.join("a.txt"), "one\n").unwrap();
        git_ok(&work, &["add", "-A"]);
        git_ok(&work, &["commit", "-q", "-m", "base"]);
    } else {
        std::fs::create_dir_all(&work).unwrap();
    }
    let brief = tmp.path().join("the-brief.md");
    std::fs::write(&brief, brief_text(verify_cmds, scope)).unwrap();

    let out = a2a(&home, tmp.path())
        // The impl lane ships a default result schema (spec §4.9), so every
        // impl worker's result goes through `schema_gate` — the fixture's
        // plain-text `done` would land `failed{schema_violation}` and put every
        // verify case below on a worker that failed for an unrelated reason.
        .env("STUB_LAST_MSG", DEVLOOP_RESULT)
        .args(["--json", "spawn", "--lane", lane, "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id = v["data"]["id"].as_str().expect("an id").to_string();
    wait_dead(&home, &id, 30);
    status_json(&home, tmp.path(), &id); // settle

    let snap = read_snapshot(&home, &id);
    let workspace = PathBuf::from(
        snap["worktree"]["path"]
            .as_str()
            .unwrap_or_else(|| snap["cwd"].as_str().expect("a cwd")),
    );
    Worker {
        tmp,
        home,
        workspace,
        id,
    }
}

fn status_json(home: &Path, allowed: &Path, id: &str) -> Value {
    let out = a2a(home, allowed).args(["--json", "status", id]).output().unwrap();
    let v: Value = serde_json::from_slice(&out.stdout).expect("one envelope");
    v["data"].clone()
}

fn events_len(home: &Path, id: &str) -> usize {
    std::fs::read_to_string(home.join("workers").join(id).join("events.jsonl"))
        .unwrap()
        .lines()
        .count()
}

/// `a2a --json verify <id> [extra…]` — the whole envelope plus the exit code.
fn verify_run(w: &Worker, extra: &[&str]) -> (i32, Value, String) {
    let out = a2a(&w.home, w.tmp.path())
        .args(["--json", "verify", &w.id])
        .args(extra)
        .output()
        .unwrap();
    let v: Value = serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|_| panic!("one JSON envelope on stdout, got {:?}", out.stdout));
    (
        out.status.code().expect("a2a exits, never signals"),
        v,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn artifact_path(w: &Worker) -> PathBuf {
    w.home
        .join("workers")
        .join(&w.id)
        .join("verification.json")
}

fn artifact(w: &Worker) -> Value {
    serde_json::from_str(&std::fs::read_to_string(artifact_path(w)).expect("verification.json"))
        .expect("verification.json is JSON")
}

// ---------------------------------------------------------------------------
// the green run: the artifact, the shape, and the FSM contract
// ---------------------------------------------------------------------------

/// The whole happy path in one pass: the brief's commands run in the worker's
/// worktree, the artifact carries the pinned shape at 0600, and — the FSM pin —
/// `verify` contributes **no transition of its own**. It is not "appends
/// nothing": `reconcile` is the shared read path and reading a worker is what
/// advances it, so the assertion is differential (a `status` first, then the
/// line count is unchanged BY the verify call).
#[test]
fn a_green_run_writes_the_pinned_artifact_and_contributes_no_transition() {
    let w = spawn_fixture("impl", true, &["echo hello", "true"], &["src/**"]);

    status_json(&w.home, w.tmp.path(), &w.id);
    let before = events_len(&w.home, &w.id);

    let (code, env, _stderr) = verify_run(&w, &[]);
    assert_eq!(code, 0, "every command exited 0");
    assert_eq!(env["ok"], true);
    assert_eq!(env["command"], "verify");
    assert_eq!(
        events_len(&w.home, &w.id),
        before,
        "verify contributes no transition of its own"
    );

    let a = artifact(&w);
    assert_eq!(a, env["data"], "--json emits the whole artifact as data");
    assert_eq!(a["schema_version"], 1);
    assert_eq!(a["id"], w.id.as_str());
    assert_eq!(a["ran_by"], "a2a-verify");
    assert_eq!(a["workspace"], w.workspace.to_string_lossy().as_ref());
    assert_eq!(a["forced"], false);
    assert_eq!(a["exec_bearing"].as_array().unwrap().len(), 0);
    assert_eq!(a["ok"], true);
    let base = a["base_ref"].as_str().expect("a 40-hex base ref");
    assert_eq!(base.len(), 40);
    assert!(base.chars().all(|c| c.is_ascii_hexdigit()));
    chrono::DateTime::parse_from_rfc3339(a["ran_at"].as_str().unwrap()).expect("rfc3339");

    // commands[] — exactly seven keys, in brief order, stdout hashed and never
    // stored.
    let cmds = a["commands"].as_array().unwrap();
    assert_eq!(cmds.len(), 2);
    for c in cmds {
        let keys: Vec<&str> = c.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            vec![
                "command",
                "duration_ms",
                "exit_code",
                "ran_at",
                "ran_by",
                "stdout_sha256",
                "timed_out"
            ],
            "exactly the seven pinned keys (serde_json sorts them on the way in)"
        );
        assert_eq!(c["exit_code"], 0);
        assert_eq!(c["timed_out"], false);
        assert_eq!(c["ran_by"], "a2a-verify");
        assert!(c["duration_ms"].is_u64());
        chrono::DateTime::parse_from_rfc3339(c["ran_at"].as_str().unwrap()).expect("rfc3339");
    }
    assert_eq!(cmds[0]["command"], "echo hello");
    assert_eq!(
        cmds[0]["stdout_sha256"],
        a2a::brief::sha256_hex("hello\n"),
        "the hash covers the child's stdout bytes"
    );
    assert_eq!(cmds[1]["command"], "true");
    assert_eq!(cmds[1]["stdout_sha256"], a2a::brief::sha256_hex(""));

    // The scope snapshot rode along.
    assert_eq!(a["scope"]["files_in_scope_declared"], true);
    assert!(a["scope"]["changed"].is_array());

    // 0600, and the FILE's key order is the declaration order — the artifact is
    // written from a derived struct, never round-tripped through a
    // `serde_json::Value` (whose Map is a BTreeMap and would alphabetize it).
    let text = std::fs::read_to_string(artifact_path(&w)).unwrap();
    let order: Vec<usize> = [
        "\"schema_version\"",
        "\"id\"",
        "\"ran_at\"",
        "\"ran_by\"",
        "\"workspace\"",
        "\"base_ref\"",
        "\"forced\"",
        "\"exec_bearing\"",
        "\"commands\"",
        "\"scope\"",
        "\"ok\"",
    ]
    .iter()
    .map(|k| text.find(k).unwrap_or_else(|| panic!("{k} in the artifact")))
    .collect();
    assert!(
        order.windows(2).all(|p| p[0] < p[1]),
        "verification.json keys are in declaration order: {order:?}"
    );
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(artifact_path(&w)).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "the artifact is 0600");
}

/// The non-JSON rendering: one line per command, `<exit_code> <ms>ms <command>`.
#[test]
fn the_plain_rendering_is_one_line_per_command() {
    let w = spawn_fixture("impl", true, &["echo hello"], &[]);
    let out = a2a(&w.home, w.tmp.path())
        .args(["verify", &w.id])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout.lines().next().expect("one line per command");
    assert!(line.starts_with("0 "), "exit code first: {line:?}");
    assert!(line.ends_with(" echo hello"), "command last: {line:?}");
    assert!(line.contains("ms "), "duration in ms: {line:?}");
}

// ---------------------------------------------------------------------------
// failure, refusal and force
// ---------------------------------------------------------------------------

/// Every command runs even after one fails — an orchestrator wants the whole
/// picture — and the run exits 4 (a worker failure, not a precondition one).
#[test]
fn a_failing_command_does_not_stop_the_rest_and_exits_four() {
    let w = spawn_fixture("impl", true, &["exit 3", "echo second"], &[]);
    let (code, env, _stderr) = verify_run(&w, &[]);
    assert_eq!(code, 4, "a verify run whose commands failed is a WORKER failure");
    assert_eq!(env["ok"], true, "the COMMAND succeeded; the worker did not");
    let a = artifact(&w);
    assert_eq!(a["ok"], false);
    let cmds = a["commands"].as_array().unwrap();
    assert_eq!(cmds.len(), 2, "the second command ran anyway");
    assert_eq!(cmds[0]["exit_code"], 3);
    assert_eq!(cmds[1]["exit_code"], 0);
    assert_eq!(cmds[1]["stdout_sha256"], a2a::brief::sha256_hex("second\n"));
}

/// A diff that can execute is refused BEFORE a single command runs, and the
/// refusal leaves no artifact behind to be mistaken for a verification.
#[test]
fn an_execution_bearing_diff_is_refused_before_anything_runs() {
    let w = spawn_fixture("impl", true, &["echo hello"], &[]);
    std::fs::write(w.workspace.join("Makefile"), "all:\n\techo pwned\n").unwrap();

    let (code, env, _stderr) = verify_run(&w, &[]);
    assert_eq!(code, 9, "a static precondition");
    assert_eq!(env["ok"], false);
    assert_eq!(env["error"]["code"], "EXEC_BEARING_DIFF");
    assert_eq!(env["error"]["data"]["paths"][0], "Makefile");
    assert_eq!(env["error"]["data"]["count"], 1);
    assert!(
        !artifact_path(&w).exists(),
        "a refusal is not a verification"
    );
}

/// `--force-verify` bypasses the refusal, never the record: the hits land in
/// the artifact and on stderr, and the commands run.
#[test]
fn force_verify_records_the_hits_and_runs_anyway() {
    let w = spawn_fixture("impl", true, &["echo hello"], &[]);
    std::fs::write(w.workspace.join("Makefile"), "all:\n\techo pwned\n").unwrap();

    let (code, _env, stderr) = verify_run(&w, &["--force-verify"]);
    assert_eq!(code, 0);
    let a = artifact(&w);
    assert_eq!(a["forced"], true);
    assert_eq!(
        a["exec_bearing"].as_array().unwrap(),
        &vec![Value::from("Makefile")]
    );
    assert!(
        stderr.contains("Makefile"),
        "the bypass is loud on stderr: {stderr}"
    );
    assert_eq!(a["ok"], true, "the commands still decide ok");
}

/// The `--force-verify` + no-diff-base path, shaped explicitly: `scope()` is
/// never called, so the scope object is the empty one and `base_ref` is null.
#[test]
fn no_base_ref_refuses_and_force_verify_has_the_pinned_empty_shape() {
    // A non-git cwd ⇒ no diff anchor was ever recorded.
    let w = spawn_fixture("general", false, &["echo hello"], &["src/**"]);

    let (code, env, _stderr) = verify_run(&w, &[]);
    assert_eq!(code, 9);
    assert_eq!(env["error"]["code"], "NO_DIFF_BASE");
    assert!(!artifact_path(&w).exists());

    let (code, _env, _stderr) = verify_run(&w, &["--force-verify"]);
    assert_eq!(code, 0);
    let a = artifact(&w);
    assert_eq!(a["base_ref"], Value::Null);
    assert_eq!(a["forced"], true);
    assert_eq!(a["exec_bearing"].as_array().unwrap().len(), 0);
    assert_eq!(
        a["scope"],
        serde_json::json!({
            "diff_stat": "", "changed": [], "outside_scope": [],
            "files_in_scope_declared": true
        }),
        "the brief's declaration survives; nothing else was computed"
    );
    assert_eq!(a["ok"], true);
}

/// Only the BRIEF's commands ever run, and the brief is immutable after spawn.
#[test]
fn an_edited_brief_is_refused() {
    let w = spawn_fixture("impl", true, &["echo hello"], &[]);
    let brief = w.home.join("workers").join(&w.id).join("brief.md");
    let text = std::fs::read_to_string(&brief).unwrap();
    std::fs::write(
        &brief,
        text.replace("echo hello", "echo hello; touch /tmp/a2a-should-never-exist"),
    )
    .unwrap();

    let (code, env, _stderr) = verify_run(&w, &[]);
    assert_eq!(code, 9);
    assert_eq!(env["error"]["code"], "BRIEF_TAMPERED");
    assert!(!artifact_path(&w).exists());
    assert!(!Path::new("/tmp/a2a-should-never-exist").exists());
}

/// A brief with nothing to run is a refusal, not an empty pass.
#[test]
fn a_brief_with_no_verify_commands_is_refused() {
    let w = spawn_fixture("general", true, &[], &[]);
    let (code, env, _stderr) = verify_run(&w, &[]);
    assert_eq!(code, 9);
    assert_eq!(env["error"]["code"], "NO_VERIFY_COMMANDS");
}

/// A workspace that is gone cannot be verified.
#[test]
fn a_missing_workspace_is_refused() {
    let w = spawn_fixture("impl", true, &["echo hello"], &[]);
    std::fs::remove_dir_all(&w.workspace).unwrap();
    let (code, env, _stderr) = verify_run(&w, &[]);
    assert_eq!(code, 9);
    assert_eq!(env["error"]["code"], "WORKSPACE_MISSING");
}

// ---------------------------------------------------------------------------
// the runner's two load-bearing mechanics: the timeout and the drain
// ---------------------------------------------------------------------------

/// A command that overruns its timeout is killed **as a process group** — the
/// backgrounded grandchild dies with it — and is recorded `exit_code: null,
/// timed_out: true`, which counts as a failure.
#[test]
fn a_timeout_kills_the_whole_group_and_counts_as_a_failure() {
    let w = spawn_fixture(
        "impl",
        true,
        &["sleep 300 & echo $! > child.pid; wait"],
        &[],
    );
    let started = std::time::Instant::now();
    let (code, _env, _stderr) = verify_run(&w, &["--force-verify", "--timeout", "1s"]);
    let elapsed = started.elapsed();
    assert_eq!(code, 4, "a timed-out command counts as a failure");
    assert!(
        elapsed < std::time::Duration::from_secs(60),
        "the wait is bounded: {elapsed:?}"
    );

    let a = artifact(&w);
    assert_eq!(a["ok"], false);
    let c = &a["commands"][0];
    assert_eq!(c["timed_out"], true);
    assert_eq!(c["exit_code"], Value::Null);
    assert!(c["duration_ms"].as_u64().unwrap() >= 1000);

    // The grandchild the shell backgrounded is gone too: a child that outlives
    // its wait() is a leaked test runner.
    let pid = std::fs::read_to_string(w.workspace.join("child.pid"))
        .expect("the shell recorded its background child")
        .trim()
        .to_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let alive = Command::new("kill")
            .args(["-0", &pid])
            .output()
            .unwrap()
            .status
            .success();
        if !alive {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "pid {pid} outlived the killpg"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// The drain: a command emitting more than 1 MiB of stdout must complete with
/// `timed_out: false`, a correct exit code and a hash of the FULL stream. A
/// read-after-wait implementation deadlocks here at the ~64 KiB pipe buffer and
/// records a perfectly healthy command as a timeout.
#[test]
fn a_flood_of_stdout_is_hashed_whole_and_never_reads_as_a_timeout() {
    let w = spawn_fixture("impl", true, &["yes | head -c 2000000"], &[]);
    let (code, _env, _stderr) = verify_run(&w, &["--timeout", "60s"]);
    assert_eq!(code, 0);
    let c = &artifact(&w)["commands"][0];
    assert_eq!(c["timed_out"], false);
    assert_eq!(c["exit_code"], 0);
    assert_eq!(
        c["stdout_sha256"],
        a2a::brief::sha256_hex(&"y\n".repeat(1_000_000)),
        "the full 2 MB stream, hashed independently in the test"
    );
}

// ---------------------------------------------------------------------------
// parity and rerun
// ---------------------------------------------------------------------------

/// verify reads the workspace and nothing else, so the two transports produce
/// the same verification. FIELD-WISE, not byte-wise: `duration_ms` is a
/// wall-clock measurement and every `commands[]` entry carries its own
/// `ran_at`, so a byte-identical assertion would be unsatisfiable.
#[test]
fn the_two_transports_verify_identically_field_wise() {
    let w = spawn_fixture("impl", true, &["echo hello", "exit 3"], &["src/**"]);
    let (exec_code, _env, _stderr) = verify_run(&w, &[]);
    let exec_run = artifact(&w);

    // The same worker, now claiming the app-server transport.
    let state = w.home.join("workers").join(&w.id).join("state.json");
    let mut snap: Value = serde_json::from_str(&std::fs::read_to_string(&state).unwrap()).unwrap();
    snap["transport"] = Value::from("app-server");
    a2a::store::write_atomic(&state, snap.to_string().as_bytes()).unwrap();
    assert_eq!(
        status_json(&w.home, w.tmp.path(), &w.id)["transport"],
        "app-server",
        "the patch took"
    );

    let (app_code, _env, _stderr) = verify_run(&w, &[]);
    let app_run = artifact(&w);

    assert_eq!(exec_code, app_code);
    for key in ["schema_version", "id", "ran_by", "workspace", "base_ref", "forced",
                "exec_bearing", "scope", "ok"] {
        assert_eq!(exec_run[key], app_run[key], "{key} differs across transports");
    }
    let (a, b) = (
        exec_run["commands"].as_array().unwrap(),
        app_run["commands"].as_array().unwrap(),
    );
    assert_eq!(a.len(), b.len());
    for (x, y) in a.iter().zip(b) {
        for key in ["command", "exit_code", "timed_out", "stdout_sha256", "ran_by"] {
            assert_eq!(x[key], y[key], "commands[].{key} differs across transports");
        }
        // The two that necessarily vary: type-checked on both sides, compared
        // on neither.
        for c in [x, y] {
            assert!(c["duration_ms"].is_u64());
            chrono::DateTime::parse_from_rfc3339(c["ran_at"].as_str().unwrap()).expect("rfc3339");
        }
    }
}

/// A rerun OVERWRITES the artifact. Unlike `result.json` there is no rotation:
/// the latest verification is the one that counts.
#[test]
fn a_rerun_overwrites_the_artifact_without_rotating_it() {
    let w = spawn_fixture("impl", true, &["echo hello"], &[]);
    verify_run(&w, &[]);
    let first = artifact(&w);
    std::thread::sleep(std::time::Duration::from_millis(1100));
    verify_run(&w, &[]);
    let second = artifact(&w);

    assert_ne!(
        first["ran_at"], second["ran_at"],
        "the artifact is the LATEST verification"
    );
    let rotated: Vec<String> = std::fs::read_dir(w.home.join("workers").join(&w.id))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains("verification"))
        .collect();
    assert_eq!(
        rotated,
        vec!["verification.json".to_string()],
        "no rotation, no temp litter"
    );
}
