// tests/e2e_worktree.rs
//
// Phase 4, end to end: the impl pipeline with a git worktree, on BOTH
// transports, through the shipped CLI only.
//
// Nothing here stages state by hand. Every worker is one `a2a spawn` into a
// REAL git repo in a `tempfile::tempdir()`, every worktree is the one a2a
// created, every verification is `a2a verify` re-running the BRIEF's commands,
// and every reclamation is `a2a gc`. What that buys is the only claim this file
// is allowed to make: the parts Tasks 1-9 built compose.
//
// Three things are proven, and only the first is "the feature works":
//
//   1. **The pipeline.** spawn -> worktree on its own branch -> codex pointed at
//      it -> `done` -> `a2a verify` -> `gc`. The verify command is a shell
//      one-liner that writes a sentinel INTO the workspace, so "the commands ran
//      in the worktree" is a file on disk rather than an inference.
//   2. **The broker needed no change.** The same spawn over the live transport
//      produces the same worktree payload, and the fake app-server's transcript
//      shows the worktree arriving as `thread/start`'s `params.cwd` — the whole
//      of spec §4.8's "worktree creation is CLI-side, pre-spawn". The two
//      transports' `verification.json` then match field-wise, which is Task 6's
//      parity pin proven end to end rather than by patching a snapshot.
//   3. **The gate holds.** An untracked `.github/workflows/ci.yml` — the case a
//      `git diff` alone never sees — refuses the verification with exit 9 and
//      leaves no artifact, and `--force-verify` records what it overrode.
//
// Every test uses a fresh temp `A2A_HOME`, every broker is retired through
// `common::BrokerGuard` + `common::assert_no_broker_left`, and nothing under
// `$HOME` is read or written.

mod common;

use common::*;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// A devloop-shaped result (`templates/impl-result.schema.json`).
///
/// The impl lane ships a DEFAULT result schema (spec §4.9), so
/// `reconcile::conclude_turn` runs the schema gate on every impl result and the
/// fixtures' plain-text `done` would land `failed{schema_violation}`. Both legs
/// therefore pin this shape — and
/// [`a_plain_text_result_fails_the_shipped_default_schema`] pins the other
/// direction, which is what makes the default wiring observably live.
const DEVLOOP_RESULT: &str = r#"{"status":"pass","iterations":1,"summary":"ok","evidence":{},"files_changed":[],"next_step_hint":null}"#;

/// The brief's one verify command: toolchain-free, and its whole effect is a
/// file in the working directory — so "it ran, and it ran THERE" is observable
/// without trusting anything the worker said.
const VERIFY_CMD: &str = "printf ok > .a2a-verified";
const SENTINEL: &str = ".a2a-verified";

/// The convergence window every poll here shares. Nothing asserts how fast a
/// daemon or a worker gets there, only the state it must reach.
const CONVERGE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------

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

/// An impl-lane brief: acceptance criteria, a declared scope in the `src/**`
/// form the matcher pins, and the sentinel command.
fn brief_body() -> String {
    format!(
        "---\n\
         acceptance_criteria:\n  - the sentinel lands in the workspace\n\
         files_in_scope:\n  - \"src/**\"\n\
         verify:\n  - \"{VERIFY_CMD}\"\n\
         ---\ndo the thing\n"
    )
}

/// One test's world: a real repo with one commit, a temp `A2A_HOME` under the
/// same (allowed) root, and the brief every worker in it is spawned from.
struct World {
    tmp: TempDir,
    home: PathBuf,
    repo: PathBuf,
    brief: PathBuf,
    /// The repo's `HEAD` at fixture time — the base ref every worktree is cut
    /// from.
    head: String,
}

fn world() -> World {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = tmp.path().join("proj");
    init_repo(&repo);
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("src/lib.rs"), "pub fn work() {}\n").unwrap();
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    git_ok(&repo, &["add", "-A"]);
    git_ok(&repo, &["commit", "-q", "-m", "base"]);
    // macOS `/var` -> `/private/var`: the canonical form every path a2a records
    // is in.
    let repo = std::fs::canonicalize(&repo).unwrap();
    let head = git_ok(&repo, &["rev-parse", "HEAD"]).trim().to_string();
    let brief = write_brief(tmp.path(), &brief_body());
    World {
        tmp,
        home,
        repo,
        brief,
        head,
    }
}

impl World {
    /// The exec transport: the Phase-1 stub, no socket.
    fn cli(&self) -> assert_cmd::Command {
        a2a(&self.home, self.tmp.path())
    }

    /// The live transport: [`common::a2a_broker`]'s command half. The guard is
    /// created ONCE per test (a second one would shut the daemon down when it
    /// left scope), so this deliberately hands back only the `Command`.
    fn broker_cli(&self) -> assert_cmd::Command {
        let mut c = self.cli();
        c.env_remove("A2A_BROKER_DISABLE")
            .env("A2A_CODEX_BIN", fake_app_server_path());
        c
    }

    fn worker_dir(&self, id: &str) -> PathBuf {
        self.home.join("workers").join(id)
    }

    /// `$A2A_HOME/worktrees/<id>`, canonical.
    fn worktree(&self, id: &str) -> PathBuf {
        std::fs::canonicalize(self.home.join("worktrees").join(id))
            .unwrap_or_else(|e| panic!("the worktree for {id} exists: {e}"))
    }

    fn status(&self, mut cmd: assert_cmd::Command, id: &str) -> Value {
        let out = cmd.args(["--json", "status", id]).output().unwrap();
        let v: Value = serde_json::from_slice(&out.stdout).expect("one envelope");
        v["data"].clone()
    }

    /// `a2a --json verify <id> [extra…]` -> (exit code, envelope, stderr).
    fn verify(
        &self,
        mut cmd: assert_cmd::Command,
        id: &str,
        extra: &[&str],
    ) -> (i32, Value, String) {
        let out = cmd
            .args(["--json", "verify", id])
            .args(extra)
            .output()
            .unwrap();
        let v: Value = serde_json::from_slice(&out.stdout)
            .unwrap_or_else(|_| panic!("one JSON envelope: {:?}", out.stdout));
        (
            out.status.code().expect("a2a exits, never signals"),
            v,
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    fn artifact_path(&self, id: &str) -> PathBuf {
        self.worker_dir(id).join("verification.json")
    }

    fn artifact(&self, id: &str) -> Value {
        serde_json::from_str(
            &std::fs::read_to_string(self.artifact_path(id)).expect("verification.json"),
        )
        .expect("verification.json is JSON")
    }
}

/// Spawn one impl worker into `w.repo` through `cmd`, returning `(id, data)`.
fn spawn_impl(w: &World, mut cmd: assert_cmd::Command) -> (String, Value) {
    let out = cmd
        .args(["--json", "spawn", "--lane", "impl", "--cwd"])
        .arg(&w.repo)
        .arg("--brief")
        .arg(&w.brief)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "spawn: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).expect("one envelope");
    let data = v["data"].clone();
    (data["id"].as_str().expect("an id").to_string(), data)
}

fn until<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + CONVERGE;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {CONVERGE:?} waiting for {what}"
        );
        std::thread::sleep(POLL);
    }
}

/// The four `worktree` fields a spawn reports, asserted against the repo — the
/// same payload on both transports, which is the point of asserting it twice.
fn assert_worktree_payload(w: &World, id: &str, data: &Value) {
    let wt = w.worktree(id);
    let branch = format!("a2a/impl/{id}");
    assert_eq!(data["worktree"]["path"], serde_json::json!(wt), "{data}");
    assert_eq!(
        data["worktree"]["branch"],
        serde_json::json!(branch),
        "{data}"
    );
    assert_eq!(
        data["worktree"]["repo"],
        serde_json::json!(w.repo),
        "{data}"
    );
    assert_eq!(
        data["worktree"]["base_ref"],
        serde_json::json!(w.head),
        "the branch is cut from the repo's HEAD: {data}"
    );
    assert_eq!(
        data["cwd"],
        serde_json::json!(wt),
        "the cwd IS the worktree"
    );
}

// ===========================================================================
// the exec leg
// ===========================================================================

/// The whole impl pipeline on the exec transport, in the order an orchestrator
/// drives it: spawn -> the tree exists and is registered -> the workspace
/// travels in `-C` -> `done` -> `verify` -> a rebuild from genesis -> `gc`.
#[test]
fn the_exec_leg_runs_in_its_worktree_verifies_it_and_gc_reclaims_it() {
    let w = world();
    let argv_file = w.tmp.path().join("argv.txt");
    let pwd_file = w.tmp.path().join("pwd.txt");

    // --- 1. spawn: the worktree payload
    let mut cmd = w.cli();
    cmd.env("STUB_LAST_MSG", DEVLOOP_RESULT)
        .env("STUB_ARGV_FILE", &argv_file)
        .env("STUB_PWD_FILE", &pwd_file);
    let (id, data) = spawn_impl(&w, cmd);
    assert_eq!(data["transport"], "exec", "{data}");
    assert_worktree_payload(&w, &id, &data);
    let wt = w.worktree(&id);
    let branch = format!("a2a/impl/{id}");

    // --- 2. it is a real, registered worktree of the parent repo
    assert!(
        wt.join("src/lib.rs").is_file(),
        "the base commit is checked out"
    );
    assert!(
        wt.join(".git").is_file(),
        "a linked worktree's .git is a FILE"
    );
    let listed = git_ok(&w.repo, &["worktree", "list", "--porcelain"]);
    assert!(listed.contains(wt.to_str().unwrap()), "{listed}");
    assert!(
        listed.contains(&format!("branch refs/heads/{branch}")),
        "the registration names the branch: {listed}"
    );
    assert_eq!(
        git_ok(&wt, &["rev-parse", "--abbrev-ref", "HEAD"]).trim(),
        branch
    );

    // --- 3. info/exclude exactly once, .gitignore never
    let exclude = std::fs::read_to_string(w.repo.join(".git/info/exclude")).unwrap();
    assert_eq!(
        exclude
            .lines()
            .filter(|l| *l == wt.to_str().unwrap())
            .count(),
        1,
        "the worktree path is excluded exactly once: {exclude}"
    );
    assert!(
        !w.repo.join(".gitignore").exists(),
        ".gitignore is the human's file and was never created"
    );

    // --- 5. converged state (before 4: the stub's witnesses are only complete
    //        once the child has run)
    w.cli()
        .args(["wait", &id, "--until", "terminal", "--timeout", "30s"])
        .assert()
        .code(0);
    let status = w.status(w.cli(), &id);
    assert_eq!(status["state"], "done", "{status}");

    // --- the schema interaction, end to end: this worker reached `done`
    //     BECAUSE its result satisfied the impl lane's shipped default schema,
    //     and the schema it was judged by is its OWN copy.
    let snap = read_snapshot(&w.home, &id);
    let schema_path = snap["schema_path"]
        .as_str()
        .expect("an impl worker is schema-gated");
    assert!(
        schema_path.ends_with("result-schema.json"),
        "the worker is judged by a file inside its own directory: {schema_path}"
    );
    assert!(w.worker_dir(&id).join("result-schema.json").is_file());

    // --- 4. the workspace travels in `-C`, NOT in the process cwd
    let argv = std::fs::read_to_string(&argv_file).expect("the stub recorded its argv");
    assert!(
        argv.contains(&format!("-C {}", wt.display())),
        "codex must be pointed at the worktree: {argv}"
    );
    let pwd = std::fs::read_to_string(&pwd_file).expect("the stub recorded its pwd");
    let pwd = pwd.trim();
    // Deliberate, and asserted as documentation rather than discovered as a
    // surprise: `exec::spawn_detached` sets the SHIM's `current_dir` to the
    // worker dir and `run_shim` never calls `current_dir` on the codex child,
    // so the child inherits it. The workspace is carried by `-C` alone.
    let worker_dir = std::fs::canonicalize(w.worker_dir(&id)).unwrap();
    assert_eq!(
        pwd,
        worker_dir.to_string_lossy(),
        "the child's cwd is the WORKER DIR, not the worktree"
    );
    assert_ne!(pwd, wt.to_string_lossy(), "…and never the worktree");

    // --- 6. verify: the brief's command, run in the worktree
    let (code, env, _stderr) = w.verify(w.cli(), &id, &[]);
    assert_eq!(code, 0, "every command exited 0: {env}");
    let a = w.artifact(&id);
    assert_eq!(a["ok"], true, "{a}");
    assert_eq!(a["workspace"], serde_json::json!(wt), "{a}");
    let c = &a["commands"][0];
    assert_eq!(
        c["command"], VERIFY_CMD,
        "the BRIEF's command, verbatim: {a}"
    );
    assert_eq!(c["exit_code"], 0, "{a}");
    assert_eq!(c["ran_by"], "a2a-verify", "{a}");
    let hash = c["stdout_sha256"].as_str().expect("a hash");
    assert_eq!(hash.len(), 64, "{hash}");
    assert!(
        hash.chars().all(|ch| matches!(ch, '0'..='9' | 'a'..='f')),
        "64 lowercase hex: {hash}"
    );
    assert_eq!(
        std::fs::read_to_string(wt.join(SENTINEL)).expect("the sentinel is IN the worktree"),
        "ok",
        "the command ran in the worktree, not in the repo or the worker dir"
    );
    assert!(
        !w.repo.join(SENTINEL).exists(),
        "…and nowhere near the parent repo"
    );

    // --- 7. lossless genesis: state.json is a derived checkpoint
    std::fs::remove_file(w.worker_dir(&id).join("state.json")).unwrap();
    let rebuilt = w.status(w.cli(), &id);
    assert_eq!(
        rebuilt["worktree"]["path"],
        serde_json::json!(wt),
        "{rebuilt}"
    );
    assert_eq!(
        rebuilt["worktree"]["branch"],
        serde_json::json!(branch),
        "{rebuilt}"
    );
    assert_eq!(
        rebuilt["worktree"]["base_ref"],
        serde_json::json!(w.head),
        "{rebuilt}"
    );
    assert_eq!(rebuilt["base_ref"], serde_json::json!(w.head), "{rebuilt}");

    // --- 8. gc refuses while the tree is dirty, and cleans with --force
    std::fs::write(wt.join("scratch.txt"), "uncommitted\n").unwrap();
    let out = w
        .cli()
        .args(["--json", "gc", "--age", "0s"])
        .assert()
        .success();
    let kept: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let kept = kept["data"].clone();
    assert_eq!(kept["kept"][0]["id"], id.as_str(), "{kept}");
    assert_eq!(kept["kept"][0]["reason"], "worktree dirty", "{kept}");
    assert!(kept["removed"].as_array().unwrap().is_empty(), "{kept}");
    assert!(wt.exists(), "a refusal takes nothing away");
    assert!(w.worker_dir(&id).exists());

    let out = w
        .cli()
        .args(["--json", "gc", "--age", "0s", "--force"])
        .assert()
        .success();
    let forced: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(forced["data"]["removed"][0], id.as_str(), "{forced}");
    assert!(!wt.exists(), "the worktree goes with the worker");
    assert!(!w.worker_dir(&id).exists());
    let listed = git_ok(&w.repo, &["worktree", "list", "--porcelain"]);
    assert!(
        !listed.contains(wt.to_str().unwrap()),
        "the registration is pruned: {listed}"
    );
    assert!(
        git_ok(&w.repo, &["branch", "--list", "a2a/*"]).contains(&branch),
        "a2a NEVER deletes a branch — the branch IS the deliverable"
    );
    let index = std::fs::read_to_string(w.home.join("index.jsonl")).unwrap();
    let last: Value =
        serde_json::from_str(index.lines().next_back().expect("a tombstone")).unwrap();
    assert_eq!(last["id"], id.as_str(), "{last}");
    assert_eq!(last["worktree_branch"], branch.as_str(), "{last}");
}

/// The other direction of the shipped default schema, on the same fixture: with
/// the stub left to write its plain-text `done`, the impl worker does NOT reach
/// `done` — it lands `failed{schema_violation}`. Without this the exec leg's
/// green run could be explained by a gate that never fires.
#[test]
fn a_plain_text_result_fails_the_shipped_default_schema() {
    let w = world();
    // No STUB_LAST_MSG: the fixture writes `done`, which is not JSON.
    let (id, _data) = spawn_impl(&w, w.cli());
    wait_dead(&w.home, &id, 30);

    let status = w.status(w.cli(), &id);
    assert_eq!(status["state"], "failed", "{status}");
    assert_eq!(status["failure_class"], "schema_violation", "{status}");
    let snap = read_snapshot(&w.home, &id);
    assert!(
        snap["schema_path"]
            .as_str()
            .is_some_and(|p| p.ends_with("result-schema.json")),
        "the lane's DEFAULT schema is what judged it: {snap}"
    );
}

// ===========================================================================
// the app-server leg
// ===========================================================================

/// The exec leg's verification of the same fixture, for the parity comparison.
fn exec_leg_verification() -> Value {
    let w = world();
    let (id, data) = spawn_impl(&w, {
        let mut c = w.cli();
        c.env("STUB_LAST_MSG", DEVLOOP_RESULT);
        c
    });
    assert_worktree_payload(&w, &id, &data);
    wait_dead(&w.home, &id, 30);
    assert_eq!(w.status(w.cli(), &id)["state"], "done");
    let (code, env, _stderr) = w.verify(w.cli(), &id, &[]);
    assert_eq!(code, 0, "the exec leg's verification: {env}");
    w.artifact(&id)
}

/// The same spawn over the live transport — **proving the broker needed no
/// change**. It received the worktree as an ordinary `cwd`, its own `check_cwd`
/// accepted it (the implicit worktrees root is inert here: `home/worktrees` is
/// already under the allowed root), and `thread/start` carried it verbatim.
#[test]
fn the_app_server_leg_spawns_into_the_same_worktree_and_verifies_identically() {
    let exec_run = exec_leg_verification();

    let w = world();
    let transcript = w.tmp.path().join("transcript.jsonl");
    let (mut cmd, guard) = a2a_broker(&w.home, w.tmp.path());
    cmd.env("FAKE_MESSAGE", DEVLOOP_RESULT)
        .env("FAKE_TRANSCRIPT", &transcript);

    // --- 1. the same worktree payload, over the socket
    let (id, data) = spawn_impl(&w, cmd);
    assert_eq!(
        data["transport"], "app-server",
        "`--transport auto` with a reachable broker is the live transport: {data}"
    );
    assert_worktree_payload(&w, &id, &data);
    let wt = w.worktree(&id);

    // --- 2. the worktree is what `thread/start` was given
    let start_cwd = until("the broker to open a thread on the worktree", || {
        let text = std::fs::read_to_string(&transcript).ok()?;
        text.lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .find(|f| f["dir"] == "in" && f["frame"]["method"] == "thread/start")
            .map(|f| f["frame"]["params"]["cwd"].clone())
    });
    assert_eq!(
        start_cwd,
        serde_json::json!(wt),
        "the broker passed the worktree through as the thread's cwd"
    );

    // --- 3. converged state, through the shipped command
    let status = until("the worker to reach `done`", || {
        let d = w.status(w.broker_cli(), &id);
        (d["state"] == "done").then_some(d)
    });
    assert_eq!(
        status["worktree"]["path"],
        serde_json::json!(wt),
        "{status}"
    );

    // --- 4. the verification matches the exec leg's, field-wise
    let (code, env, _stderr) = w.verify(w.broker_cli(), &id, &[]);
    assert_eq!(code, 0, "{env}");
    let app_run = w.artifact(&id);
    assert_eq!(app_run["ok"], true, "{app_run}");
    assert_eq!(
        std::fs::read_to_string(wt.join(SENTINEL)).expect("the sentinel is in THIS worktree"),
        "ok"
    );

    let (a, b) = (
        exec_run["commands"].as_array().unwrap(),
        app_run["commands"].as_array().unwrap(),
    );
    assert_eq!(a.len(), b.len(), "the same brief, the same command list");
    for (x, y) in a.iter().zip(b) {
        for key in [
            "command",
            "exit_code",
            "timed_out",
            "stdout_sha256",
            "ran_by",
        ] {
            assert_eq!(x[key], y[key], "commands[].{key} differs across transports");
        }
        // The two that necessarily vary: type-checked on both sides, compared
        // on neither.
        for c in [x, y] {
            assert!(c["duration_ms"].is_u64(), "{c}");
            chrono::DateTime::parse_from_rfc3339(c["ran_at"].as_str().unwrap()).expect("rfc3339");
        }
    }
    assert_eq!(
        exec_run["scope"], app_run["scope"],
        "verify reads the workspace and nothing else, so the scope snapshot is transport-blind"
    );

    // --- 5. the daemon is retired, and nothing outlives it
    drop(guard);
    assert_no_broker_left(&w.home);
}

// ===========================================================================
// the gate, end to end
// ===========================================================================

/// The untracked-workflow case, on a worker that ran the whole pipeline: a
/// `git diff` alone never sees `.github/workflows/ci.yml`, and
/// `git status --porcelain` without `--untracked-files=all` collapses it to
/// `?? .github/`. The union changed-path set is what surfaces it, and the
/// refusal is exit 9 with NO artifact — a refusal is not a verification.
#[test]
fn an_untracked_workflow_refuses_the_verification_until_it_is_forced() {
    let w = world();
    let (id, data) = spawn_impl(&w, {
        let mut c = w.cli();
        c.env("STUB_LAST_MSG", DEVLOOP_RESULT);
        c
    });
    assert_worktree_payload(&w, &id, &data);
    let wt = w.worktree(&id);
    wait_dead(&w.home, &id, 30);
    assert_eq!(w.status(w.cli(), &id)["state"], "done");

    std::fs::create_dir_all(wt.join(".github/workflows")).unwrap();
    std::fs::write(wt.join(".github/workflows/ci.yml"), "on: push\njobs: {}\n").unwrap();

    let (code, env, _stderr) = w.verify(w.cli(), &id, &[]);
    assert_eq!(code, 9, "a static precondition: {env}");
    assert_eq!(env["ok"], false, "{env}");
    assert_eq!(env["error"]["code"], "EXEC_BEARING_DIFF", "{env}");
    let paths = env["error"]["data"]["paths"].as_array().expect("the hits");
    assert!(
        paths.contains(&Value::from(".github/workflows/ci.yml")),
        "the untracked workflow is what the union set exists to surface: {env}"
    );
    assert!(
        env["error"]["remedy"]
            .as_str()
            .expect("a remedy")
            .contains("--force-verify"),
        "the remedy names the override: {env}"
    );
    assert!(
        !w.artifact_path(&id).exists(),
        "a refusal is not a verification"
    );
    assert!(
        !wt.join(SENTINEL).exists(),
        "the refusal happened BEFORE a single command ran"
    );

    // The override runs the commands and records what it overrode.
    let (code, env, stderr) = w.verify(w.cli(), &id, &["--force-verify"]);
    assert_eq!(code, 0, "the brief's command still exits 0: {env}");
    let a = w.artifact(&id);
    assert_eq!(a["forced"], true, "{a}");
    assert_eq!(
        a["exec_bearing"].as_array().unwrap(),
        &vec![Value::from(".github/workflows/ci.yml")],
        "{a}"
    );
    assert!(
        stderr.contains(".github/workflows/ci.yml"),
        "the bypass is loud on stderr: {stderr}"
    );
    assert_eq!(
        std::fs::read_to_string(wt.join(SENTINEL)).expect("the sentinel"),
        "ok",
        "…and the commands did run this time"
    );
}
