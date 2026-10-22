//! `auto_review` end to end (spec §4.8): a finished impl worker's worktree diff
//! becomes a review-lane worker, and that worker's findings become
//! `<impl-worker>/review.json`.
//!
//! Everything here goes through the shipped CLI and a real git repository in a
//! `tempfile::tempdir()`. Nothing stages a snapshot by hand: the worktree is the
//! one `a2a spawn` created, the commit under review is a real commit on the
//! worker's own branch, and the reviewer is the one the daemon decided to make.
//!
//! **Two fixture facts shape every test in this file, and both are the fake's,
//! not the feature's.**
//!
//! 1. `FAKE_MESSAGE` is ONE environment variable on ONE app-server child shared
//!    by every worker — there is no per-thread message. So the impl worker's
//!    final message is the same findings-shaped JSON the reviewer is meant to
//!    emit, and the impl lane's DEFAULT result schema would fail it at
//!    `reconcile::conclude_turn` with `schema_violation`: it would never reach
//!    `done`, and the trigger would never fire. Every impl spawn below
//!    therefore carries an explicit `--schema` pointing at the findings schema,
//!    which wins over the lane default. A per-thread message knob on the fake is
//!    the cleaner fix and is not this task's.
//! 2. `hang-turn` holds a turn open until it is steered. That is what makes the
//!    diff race-free: the change under review is committed to the worker's
//!    branch while the worker is demonstrably still mid-turn, and only then is
//!    the turn finished. It applies to the REVIEWER too — it shares the child —
//!    so the reviewer is steered as well, once it exists.
//!
//! The home deliberately sits OUTSIDE `A2A_ALLOWED_ROOTS`. That is not
//! incidental: `$A2A_HOME/workers/<review-id>` — the reviewer's cwd — is then
//! not under any allowed root, so an auto-review `SpawnRequest` carrying that
//! cwd would be refused by `check_cwd` with `CWD_NOT_ALLOWED`. It spawns anyway,
//! because the request carries `cwd: None`. This is the one test that would have
//! caught a feature that is green in a temp home and dead in production.

mod common;

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Convergence window. Nothing here asserts how fast a daemon gets anywhere —
/// only the state it must reach. Longer than `budgets.rs`'s because this
/// pipeline is two workers, a `git diff` and a writeback.
const CONVERGE: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

/// A findings document that satisfies `templates/review-findings.schema.json`.
/// Shared by both workers, for the reason in this file's header.
const FINDINGS: &str = r#"{"findings":[{"file":"src/lib.rs","line":2,"severity":"low","rationale":"the marker line is unexplained"}]}"#;

/// The line the test commits into the impl worker's branch. It appears in the
/// diff and nowhere else, so finding it in the reviewer's brief proves the diff
/// travelled rather than that some file happened to be copied.
const DIFF_MARKER: &str = "AUTO_REVIEW_DIFF_MARKER";

/// The impl brief's acceptance criterion, asserted verbatim inside the
/// reviewer's brief under the fixed `## Acceptance criteria under review` H2.
const IMPL_CRITERION: &str = "the marker lands in the workspace";

fn a2a_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("a2a")
}

/// The findings schema this repo ships — the one the reviewer gets by lane
/// default, handed to the IMPL worker explicitly so the shared `FAKE_MESSAGE`
/// passes its gate too.
fn findings_schema() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("templates/review-findings.schema.json")
}

struct Ran {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Ran {
    fn json(&self) -> Value {
        serde_json::from_str(&self.stdout)
            .unwrap_or_else(|e| panic!("stdout is one JSON envelope ({e}): {:?}", self.stdout))
    }
    fn data(&self) -> Value {
        self.json()["data"].clone()
    }
}

fn run(mut cmd: Command, scratch: &Path, tag: &str) -> Ran {
    std::fs::create_dir_all(scratch).unwrap();
    let out = std::fs::File::create(scratch.join(format!("{tag}.out"))).unwrap();
    let err = std::fs::File::create(scratch.join(format!("{tag}.err"))).unwrap();
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("the a2a binary runs");
    let status = child.wait().expect("the invocation ends");
    Ran {
        code: status.code().unwrap_or(-1),
        stdout: std::fs::read_to_string(scratch.join(format!("{tag}.out"))).unwrap_or_default(),
        stderr: std::fs::read_to_string(scratch.join(format!("{tag}.err"))).unwrap_or_default(),
    }
}

fn until<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + CONVERGE;
    loop {
        if let Some(v) = f() {
            return v;
        }
        if Instant::now() >= deadline {
            panic!("timed out after {CONVERGE:?} waiting for {what}");
        }
        std::thread::sleep(POLL);
    }
}

fn git_ok(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("git is on PATH");
    assert!(
        out.status.success(),
        "git {args:?} in {dir:?} failed: {}",
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

/// One test's world.
///
/// `roots` (the repo's parent) and `home` are SEPARATE temp dirs on purpose —
/// see this file's header. `$A2A_HOME/worktrees` is still reachable, because
/// `config::load` appends it as the one implicit root when the home sits
/// outside the configured ones; `$A2A_HOME/workers/<id>` is not, which is the
/// whole point.
struct World {
    roots: tempfile::TempDir,
    /// Held only to keep the home alive for the test's duration — the home
    /// deliberately does NOT live under `roots`.
    #[allow(dead_code)]
    homedir: tempfile::TempDir,
    home: PathBuf,
    repo: PathBuf,
    brief: PathBuf,
    scratch: PathBuf,
}

fn world() -> World {
    let roots = tempfile::tempdir().unwrap();
    let homedir = tempfile::tempdir().unwrap();
    let home = homedir.path().join("home");
    let repo = roots.path().join("proj");
    init_repo(&repo);
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("src/lib.rs"), "pub fn work() {}\n").unwrap();
    git_ok(&repo, &["add", "-A"]);
    git_ok(&repo, &["commit", "-q", "-m", "base"]);
    let repo = std::fs::canonicalize(&repo).unwrap();
    let brief = roots.path().join("impl.md");
    std::fs::write(
        &brief,
        format!(
            "---\nlane: impl\nacceptance_criteria:\n  - {IMPL_CRITERION}\nverify:\n  - \"true\"\n---\ndo the thing\n"
        ),
    )
    .unwrap();
    let scratch = roots.path().join("scratch");
    World {
        roots,
        homedir,
        home,
        repo,
        brief,
        scratch,
    }
}

impl World {
    /// `a2a --home <home> …` on the live transport, with auto-review ON.
    fn cli(&self, args: &[&str]) -> Command {
        let mut c = Command::new(a2a_bin());
        c.arg("--home")
            .arg(&self.home)
            .args(args)
            .env_remove("A2A_HOME")
            .env_remove("A2A_BROKER_DISABLE")
            .env("A2A_CODEX_BIN", common::fake_app_server_path())
            // The allowlist is the REPO's tree only. The home is elsewhere.
            .env("A2A_ALLOWED_ROOTS", self.roots.path())
            .env("A2A_BROKER_BIN", a2a_bin())
            .env("A2A_SHIM_BIN", a2a_bin())
            .env("A2A_OWNER", common::TEST_OWNER)
            .env("A2A_LANES_IMPL_AUTO_REVIEW", "1")
            .env("FAKE_SCENARIO", "hang-turn")
            .env("FAKE_MESSAGE", FINDINGS);
        c
    }

    fn worker_dir(&self, id: &str) -> PathBuf {
        self.home.join("workers").join(id)
    }

    fn worktree(&self, id: &str) -> PathBuf {
        std::fs::canonicalize(self.home.join("worktrees").join(id))
            .unwrap_or_else(|e| panic!("the worktree for {id} exists: {e}"))
    }

    fn snap(&self, id: &str) -> Option<Value> {
        let text = std::fs::read_to_string(self.worker_dir(id).join("state.json")).ok()?;
        serde_json::from_str(text.trim()).ok()
    }

    fn until_state(&self, id: &str, want: &str) -> Value {
        until(&format!("worker {id} to reach `{want}`"), || {
            self.snap(id).filter(|s| s["state"] == json!(want))
        })
    }

    fn rows(&self) -> Vec<Value> {
        let ran = run(self.cli(&["--json", "ls"]), &self.scratch, "ls");
        ran.data()["workers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }

    fn broker_log(&self) -> Vec<Value> {
        std::fs::read_to_string(self.home.join("broker.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }

    /// Spawn one impl worker into the repo. Returns its id.
    fn spawn_impl(&self, extra: &[&str], tag: &str) -> String {
        let mut cmd = self.cli(&["--json", "spawn", "--lane", "impl", "--cwd"]);
        cmd.arg(&self.repo)
            .arg("--brief")
            .arg(&self.brief)
            // See the header: the shared FAKE_MESSAGE is findings-shaped, and
            // the impl lane's default schema would fail it.
            .arg("--schema")
            .arg(findings_schema())
            .args(extra);
        let ran = run(cmd, &self.scratch, tag);
        assert_eq!(ran.code, 0, "spawn: {} {}", ran.stdout, ran.stderr);
        ran.data()["id"].as_str().expect("an id").to_string()
    }

    /// Steer a `hang-turn` worker so its turn completes with `FAKE_MESSAGE`.
    fn finish(&self, id: &str, tag: &str) {
        // The worker must be mid-turn before a steer can land.
        self.until_state(id, "running");
        let ran = run(
            self.cli(&["--json", "send", id, "finish up"]),
            &self.scratch,
            tag,
        );
        assert_eq!(ran.code, 0, "send {id}: {} {}", ran.stdout, ran.stderr);
    }

    /// Commit `body` into the impl worker's own worktree branch, so
    /// `git diff <base_ref>..<branch>` has something to show.
    fn commit_in_worktree(&self, id: &str, file: &str, body: &str) {
        let wt = self.worktree(id);
        std::fs::write(wt.join(file), body).unwrap();
        git_ok(&wt, &["add", "-A"]);
        git_ok(&wt, &["commit", "-q", "-m", "worker change"]);
    }

    /// The reviewer the daemon made for `impl_id`, once it exists.
    fn until_reviewer(&self, impl_id: &str) -> Value {
        until(&format!("a review worker for {impl_id}"), || {
            self.rows()
                .into_iter()
                .find(|r| r["lane"] == json!("review") && r["auto_review_of"] == json!(impl_id))
        })
    }
}

// ---------------------------------------------------------------------------
// the pipeline
// ---------------------------------------------------------------------------

/// The whole of spec §4.8 in one run: an impl worker with a worktree finishes,
/// the broker diffs its branch, seeds a review-lane worker off that diff, and
/// the reviewer's findings land in the IMPL worker's directory as `review.json`.
#[test]
fn a_finished_impl_worktree_becomes_a_reviewer_whose_findings_land_as_review_json() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let id = w.spawn_impl(&[], "spawn");
    // The worker is mid-turn (`hang-turn`), so the change under review can be
    // committed to its branch with nothing racing it.
    w.until_state(&id, "running");
    w.commit_in_worktree(&id, "src/marker.rs", &format!("// {DIFF_MARKER}\n"));
    w.finish(&id, "send-impl");
    let done = w.until_state(&id, "done");
    let delivered_at_done = done["last_delivered_seq"].clone();

    // --- 1. a second worker exists, on the review lane, marked as this one's
    let reviewer = w.until_reviewer(&id);
    let rid = reviewer["id"].as_str().expect("an id").to_string();
    assert_ne!(rid, id);
    assert_eq!(
        reviewer["parent_id"],
        Value::Null,
        "a review is not a retry: {reviewer}"
    );
    assert_eq!(reviewer["attempt"], Value::Null, "{reviewer}");

    // --- 2. its brief carries BOTH halves: the diff and the criteria
    let rbrief = std::fs::read_to_string(w.worker_dir(&rid).join("brief.md"))
        .expect("the reviewer has a brief");
    assert!(
        rbrief.contains(DIFF_MARKER),
        "the reviewer's brief carries the diff: {rbrief}"
    );
    assert!(
        rbrief.contains("## Acceptance criteria under review") && rbrief.contains(IMPL_CRITERION),
        "the reviewer's brief carries the impl brief's criteria: {rbrief}"
    );

    // --- 3. the reviewer's cwd is its OWN worker dir, recorded in genesis —
    //        and `$A2A_HOME/workers/<id>` is NOT under an allowed root here.
    let rdir = w.worker_dir(&rid);
    let genesis: Value = serde_json::from_str(
        std::fs::read_to_string(rdir.join("events.jsonl"))
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(genesis["raw"]["fsm"], json!("Spawn"));
    assert_eq!(
        genesis["raw"]["cwd"],
        json!(rdir),
        "the reviewer runs in its own worker dir: {genesis}"
    );
    assert_eq!(genesis["raw"]["auto_review_of"], json!(id), "{genesis}");
    assert_eq!(genesis["raw"]["lane"], json!("review"));
    // The lane-default rung, which is what makes `result.json` exist at all.
    assert_eq!(genesis["raw"]["schema_source"], json!("lane-default"));
    assert_eq!(
        std::fs::read_to_string(rdir.join("result-schema.json")).unwrap(),
        std::fs::read_to_string(findings_schema()).unwrap(),
        "the reviewer is judged by the review lane's shipped findings schema"
    );

    // --- 4. steer the reviewer (it shares the `hang-turn` child) and converge
    //        on its findings, in the IMPL worker's directory
    w.finish(&rid, "send-review");
    w.until_state(&rid, "done");
    let review: Value = until("review.json in the impl worker's dir", || {
        let text = std::fs::read_to_string(w.worker_dir(&id).join("review.json")).ok()?;
        serde_json::from_str(&text).ok()
    });
    let findings = review["findings"]
        .as_array()
        .unwrap_or_else(|| panic!("review.json parses with a findings array: {review}"));
    assert_eq!(findings.len(), 1, "{review}");
    assert_eq!(findings[0]["file"], json!("src/lib.rs"), "{review}");

    // --- 6. ONE envelope says so, and it moves no state. The envelope is
    //        appended AFTER the file, so it is converged on rather than read.
    let notices: Vec<Value> = until("the writeback notice on the impl worker's WAL", || {
        let found: Vec<Value> = std::fs::read_to_string(w.worker_dir(&id).join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|e| e["raw"]["path"] == json!("review.json"))
            .collect();
        (!found.is_empty()).then_some(found)
    });
    assert_eq!(
        notices.len(),
        1,
        "exactly one writeback notice: {notices:?}"
    );
    assert_eq!(notices[0]["kind"], json!("outbox_notice"));
    assert_eq!(
        notices[0]["raw"],
        json!({"review_of": rid, "path": "review.json"}),
        "the payload has no `fsm` key: {}",
        notices[0]
    );

    // --- 7. the impl worker is untouched by any of it
    let after = w.snap(&id).expect("the impl worker still has a snapshot");
    assert_eq!(after["state"], json!("done"), "{after}");
    assert_eq!(
        after["last_delivered_seq"], delivered_at_done,
        "the writeback delivers nothing to the worker: {after}"
    );
    assert!(
        after.get("fold_rejections").is_none(),
        "the writeback envelope carries no `fsm` key, so the fold rejected nothing: {after}"
    );

    // --- 8. `ls --json` reports the marker as its own key
    let rows = w.rows();
    let row = rows.iter().find(|r| r["id"] == json!(rid)).unwrap();
    assert_eq!(row["auto_review_of"], json!(id), "{row}");
    let impl_row = rows.iter().find(|r| r["id"] == json!(id)).unwrap();
    assert_eq!(impl_row["auto_review_of"], Value::Null, "{impl_row}");
    // No recursion: the reviewer settled `done` too, and a worker carrying
    // `auto_review_of` never earns a review of its own.
    assert_eq!(rows.len(), 2, "exactly two workers: {rows:?}");

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

/// Framework commit on `done`, app-server leg: the worker leaves its change
/// UNCOMMITTED (it cannot commit — the sandbox makes `.git` read-only), the
/// broker's actor commits it in the same step that concludes the turn, and only
/// then does the settlement reach `auto_review` — so the reviewer's diff, taken
/// as `base_ref..branch`, includes the framework's commit.
#[test]
fn the_framework_commit_lands_before_the_reviewer_diffs_the_branch() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let id = w.spawn_impl(&[], "spawn");
    w.until_state(&id, "running");
    let wt = w.worktree(&id);
    // In-tree and uncommitted, exactly as a sandboxed worker leaves it.
    std::fs::write(wt.join("src/marker.rs"), format!("// {DIFF_MARKER}\n")).unwrap();
    w.finish(&id, "send-impl");
    // The verdict row follows the `done` checkpoint inside the same locked
    // step (and so precedes the settlement); converge on it.
    let done = until(&format!("worker {id} done and committed"), || {
        w.snap(&id)
            .filter(|s| s["state"] == json!("done") && !s["auto_commit"].is_null())
    });
    let sha = done["committed_sha"]
        .as_str()
        .unwrap_or_else(|| panic!("committed in the turn's own step: {done}"))
        .to_string();
    assert_eq!(done["auto_commit"]["outcome"], json!("committed"), "{done}");
    let branch = format!("a2a/impl/{id}");
    assert_eq!(git_ok(&w.repo, &["rev-parse", &branch]).trim(), sha);
    let msg = git_ok(&w.repo, &["show", "-s", "--format=%an|%B", &sha]);
    assert!(
        msg.starts_with(&format!("a2a|impl {id}: ")) && msg.contains("app-server)"),
        "{msg}"
    );
    assert_eq!(git_ok(&wt, &["status", "--porcelain"]), "", "nothing left behind");

    // ...so the reviewer's brief carries the framework-committed diff.
    let reviewer = w.until_reviewer(&id);
    let rid = reviewer["id"].as_str().unwrap().to_string();
    let rbrief = std::fs::read_to_string(w.worker_dir(&rid).join("brief.md")).unwrap();
    assert!(
        rbrief.contains(DIFF_MARKER),
        "the review's diff includes the framework commit: {rbrief}"
    );
    // A reviewer is never itself committed.
    let rsnap = w.snap(&rid).unwrap();
    assert_eq!(rsnap["auto_commit_enabled"], json!(false), "{rsnap}");

    // `a2a result --json` reports it, through the broker-owned worker.
    let ran = run(w.cli(&["--json", "result", &id]), &w.scratch, "result");
    assert_eq!(ran.data()["committed_sha"], json!(sha), "{}", ran.stdout);

    w.finish(&rid, "send-review");
    w.until_state(&rid, "done");
    drop(guard);
    common::assert_no_broker_left(&w.home);
}

/// A diff past the 200 KiB cap is truncated with the EXACT marker — the
/// reviewer must be able to tell it was handed part of a change.
#[test]
fn a_diff_over_the_cap_is_truncated_with_the_pinned_marker() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let id = w.spawn_impl(&[], "spawn");
    w.until_state(&id, "running");
    // Comfortably over 204800 bytes of diff body, in one file so the marker
    // lands inside the hunk rather than between files.
    let big = format!(
        "// {DIFF_MARKER}\n{}",
        "// filler filler filler\n".repeat(12_000)
    );
    w.commit_in_worktree(&id, "src/big.rs", &big);
    w.finish(&id, "send-impl");
    w.until_state(&id, "done");

    let reviewer = w.until_reviewer(&id);
    let rid = reviewer["id"].as_str().unwrap().to_string();
    let rbrief = std::fs::read_to_string(w.worker_dir(&rid).join("brief.md")).unwrap();
    assert!(
        rbrief.contains("\n\n[... diff truncated at 204800 bytes ...]\n"),
        "the pinned truncation marker is in the brief"
    );
    assert!(
        rbrief.contains(DIFF_MARKER),
        "the head of the diff survived the cut"
    );

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

// ---------------------------------------------------------------------------
// the guards
// ---------------------------------------------------------------------------

/// No worktree, no review — and the refusal is RECORDED, so an operator can see
/// the feature decline rather than wonder whether it ran. Reviewing a diff that
/// does not exist is worse than not reviewing.
#[test]
fn an_impl_worker_without_a_worktree_is_skipped_and_says_so() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let id = w.spawn_impl(&["--no-worktree"], "spawn");
    w.until_state(&id, "running");
    w.finish(&id, "send-impl");
    w.until_state(&id, "done");

    let line = until("an auto_review_skipped line", || {
        w.broker_log()
            .into_iter()
            .find(|l| l["kind"] == json!("auto_review_skipped") && l["worker"] == json!(id))
    });
    assert_eq!(line["reason"], json!("no_worktree"), "{line}");

    // Nothing else happened: no reviewer, no findings.
    assert!(
        !w.worker_dir(&id).join("review.json").exists(),
        "a skipped review writes nothing"
    );
    assert!(
        w.rows().iter().all(|r| r["lane"] != json!("review")),
        "no review worker was made: {:?}",
        w.rows()
    );

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

/// The flag is OFF by default, and only `lanes.impl.auto_review` is ever read:
/// turning it on for the REVIEW lane changes nothing at all.
#[test]
fn auto_review_is_off_by_default_and_only_the_impl_lanes_key_is_read() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let mut cmd = w.cli(&["--json", "spawn", "--lane", "impl", "--cwd"]);
    cmd.arg(&w.repo)
        .arg("--brief")
        .arg(&w.brief)
        .arg("--schema")
        .arg(findings_schema())
        // The impl lane's key back to its default, and the REVIEW lane's key
        // turned on — which is the whole of the asymmetry under test.
        .env("A2A_LANES_IMPL_AUTO_REVIEW", "0")
        .env("A2A_LANES_REVIEW_AUTO_REVIEW", "1");
    let ran = run(cmd, &w.scratch, "spawn");
    assert_eq!(ran.code, 0, "spawn: {} {}", ran.stdout, ran.stderr);
    let id = ran.data()["id"].as_str().unwrap().to_string();

    w.until_state(&id, "running");
    w.commit_in_worktree(&id, "src/marker.rs", &format!("// {DIFF_MARKER}\n"));
    w.finish(&id, "send-impl");
    w.until_state(&id, "done");

    // A negative that has to be given time to be wrong: the trigger is two
    // hops, so "no reviewer yet" is only meaningful after the daemon has had
    // long enough to make one.
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        w.rows().iter().all(|r| r["lane"] != json!("review")),
        "auto_review is off: {:?}",
        w.rows()
    );
    assert!(!w.worker_dir(&id).join("review.json").exists());
    assert!(
        w.broker_log()
            .iter()
            .all(|l| l["kind"] != json!("auto_review_skipped")),
        "a disabled feature does not even decline: {:?}",
        w.broker_log()
    );

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

/// `git diff` failing is not fatal to the impl worker: it did its work, and a
/// review a2a could not prepare is a2a's problem. One recorded line, and stop.
///
/// The failure is forced by removing the worktree out from under the daemon,
/// which is exactly the shape of the real cases (a tree an operator cleaned up,
/// a repo that moved) rather than a mocked error.
#[test]
fn a_failing_git_diff_is_recorded_and_leaves_the_impl_worker_alone() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let id = w.spawn_impl(&[], "spawn");
    w.until_state(&id, "running");
    let wt = w.worktree(&id);
    w.finish(&id, "send-impl");
    w.until_state(&id, "done");
    // After `done`, before the diff: the worker is settled and the hook has to
    // read a tree that is no longer there.
    std::fs::remove_dir_all(&wt).ok();

    let line = until("an auto_review_skipped{git_diff_failed} line", || {
        w.broker_log().into_iter().find(|l| {
            l["kind"] == json!("auto_review_skipped") && l["reason"] == json!("git_diff_failed")
        })
    });
    assert_eq!(line["worker"], json!(id), "{line}");
    assert!(
        line["detail"]
            .as_str()
            .unwrap_or_default()
            .contains("git diff"),
        "the line says what failed: {line}"
    );

    let after = w.snap(&id).unwrap();
    assert_eq!(
        after["state"],
        json!("done"),
        "the impl worker is untouched: {after}"
    );
    assert!(!w.worker_dir(&id).join("review.json").exists());

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

/// The reviewer goes through the SAME admission door as any spawn: at the cap
/// it queues, and an outright refusal abandons the review with a recorded code
/// while the impl worker is untouched and nothing is left on disk.
#[test]
fn the_reviewer_is_admitted_queued_or_declined_like_any_other_spawn() {
    // --- queued: the review lane has no room, but the queue does.
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);
    let mut cmd = w.cli(&["--json", "spawn", "--lane", "impl", "--cwd"]);
    cmd.arg(&w.repo)
        .arg("--brief")
        .arg(&w.brief)
        .arg("--schema")
        .arg(findings_schema())
        .env("A2A_ADMISSION_LANES_REVIEW_CAP", "0")
        .env("A2A_ADMISSION_QUEUE_CAP", "4");
    let ran = run(cmd, &w.scratch, "spawn");
    assert_eq!(ran.code, 0, "spawn: {} {}", ran.stdout, ran.stderr);
    let id = ran.data()["id"].as_str().unwrap().to_string();
    w.until_state(&id, "running");
    w.commit_in_worktree(&id, "src/marker.rs", &format!("// {DIFF_MARKER}\n"));
    w.finish(&id, "send-impl");
    w.until_state(&id, "done");

    let reviewer = w.until_reviewer(&id);
    let rid = reviewer["id"].as_str().unwrap().to_string();
    w.until_state(&rid, "queued");
    // Converged, not instant: `queue_position` is stamped by a LATER broker
    // write (`restamp_queue`) than the one that put the worker in `queued`, so
    // reading it off the snapshot that first said `queued` is a race.
    let queued = until("the reviewer's queue position to be stamped", || {
        w.snap(&rid)
            .filter(|s| s["state"] == json!("queued") && s["queue_position"] == json!(1))
    });
    assert_eq!(queued["queue_position"], json!(1), "{queued}");
    drop(guard);
    common::assert_no_broker_left(&w.home);

    // --- declined: no room AND no queue.
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);
    let mut cmd = w.cli(&["--json", "spawn", "--lane", "impl", "--cwd"]);
    cmd.arg(&w.repo)
        .arg("--brief")
        .arg(&w.brief)
        .arg("--schema")
        .arg(findings_schema())
        .env("A2A_ADMISSION_LANES_REVIEW_CAP", "0")
        .env("A2A_ADMISSION_QUEUE_CAP", "0");
    let ran = run(cmd, &w.scratch, "spawn");
    assert_eq!(ran.code, 0, "spawn: {} {}", ran.stdout, ran.stderr);
    let id = ran.data()["id"].as_str().unwrap().to_string();
    w.until_state(&id, "running");
    w.finish(&id, "send-impl");
    w.until_state(&id, "done");

    let line = until("an auto_review_declined line", || {
        w.broker_log()
            .into_iter()
            .find(|l| l["kind"] == json!("auto_review_declined"))
    });
    assert_eq!(line["worker"], json!(id), "{line}");
    assert_eq!(line["code"], json!("QUEUE_FULL"), "{line}");
    // A refusal leaves NOTHING behind: no half-worker for `ls`/`gc` to trip on.
    let ids: Vec<String> = std::fs::read_dir(w.home.join("workers"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        ids,
        vec![id.clone()],
        "only the impl worker exists: {ids:?}"
    );
    assert_eq!(w.snap(&id).unwrap()["state"], json!("done"));

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

/// Kill the reviewer mid-run: the impl worker stays exactly `done`, gains no
/// `review.json` and no new lifecycle transition, and ONE `broker.jsonl` line
/// names the reviewer's fate.
#[test]
fn a_reviewer_that_never_finishes_never_touches_the_impl_worker() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let id = w.spawn_impl(&[], "spawn");
    w.until_state(&id, "running");
    w.commit_in_worktree(&id, "src/marker.rs", &format!("// {DIFF_MARKER}\n"));
    w.finish(&id, "send-impl");
    // `done` AND the framework commit's verdict row (here `clean` — the change
    // was committed by hand), which the actor writes in the same locked step
    // just after the `done` checkpoint and before the settlement: the baseline
    // is the WAL as the reviewer found it.
    let done = until(&format!("worker {id} done with its auto-commit verdict"), || {
        w.snap(&id)
            .filter(|s| s["state"] == json!("done") && !s["auto_commit"].is_null())
    });
    assert_eq!(done["auto_commit"]["outcome"], json!("clean"), "{done}");
    let events_at_done = std::fs::read_to_string(w.worker_dir(&id).join("events.jsonl"))
        .unwrap()
        .lines()
        .count();

    let reviewer = w.until_reviewer(&id);
    let rid = reviewer["id"].as_str().unwrap().to_string();
    // It is `hang-turn`: the reviewer is mid-turn and will never finish on its
    // own, which is what makes the kill the only thing that ends it.
    w.until_state(&rid, "running");
    let killed = run(w.cli(&["--json", "kill", &rid]), &w.scratch, "kill-review");
    assert_eq!(killed.code, 0, "kill: {} {}", killed.stdout, killed.stderr);
    let fate = w.until_state(&rid, "cancelled");
    assert_eq!(fate["state"], json!("cancelled"), "{fate}");

    let line = until("an auto_review_failed line", || {
        w.broker_log()
            .into_iter()
            .find(|l| l["kind"] == json!("auto_review_failed"))
    });
    assert_eq!(line["worker"], json!(id), "{line}");
    assert_eq!(line["review"], json!(rid), "{line}");
    assert_eq!(line["state"], json!("cancelled"), "{line}");

    let after = w.snap(&id).unwrap();
    assert_eq!(after["state"], json!("done"), "{after}");
    assert_eq!(
        after["entered_state_at"], done["entered_state_at"],
        "{after}"
    );
    assert!(!w.worker_dir(&id).join("review.json").exists());
    assert_eq!(
        std::fs::read_to_string(w.worker_dir(&id).join("events.jsonl"))
            .unwrap()
            .lines()
            .count(),
        events_at_done,
        "no new row on the impl worker's WAL"
    );

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

/// `review.json` is the idempotence guard: a fresh daemon that re-adopts a
/// `done` impl worker does not review it a second time, and does not spend a
/// second worker's quota overwriting the first's answer.
///
/// This is also where "no recursion" is observable: the reviewer is itself a
/// settled worker, and no third worker ever appears.
#[test]
fn a_reviewed_worker_is_not_reviewed_again_after_a_broker_restart() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let id = w.spawn_impl(&[], "spawn");
    w.until_state(&id, "running");
    w.commit_in_worktree(&id, "src/marker.rs", &format!("// {DIFF_MARKER}\n"));
    w.finish(&id, "send-impl");
    w.until_state(&id, "done");
    let reviewer = w.until_reviewer(&id);
    let rid = reviewer["id"].as_str().unwrap().to_string();
    w.finish(&rid, "send-review");
    w.until_state(&rid, "done");
    until("review.json", || {
        std::fs::read_to_string(w.worker_dir(&id).join("review.json")).ok()
    });
    let first = std::fs::read_to_string(w.worker_dir(&id).join("review.json")).unwrap();
    assert_eq!(w.rows().len(), 2, "exactly two workers: {:?}", w.rows());

    // A new daemon, adopting both workers from disk.
    drop(guard);
    common::assert_no_broker_left(&w.home);
    let guard = common::BrokerGuard::new(&w.home);
    let rows = run(w.cli(&["--json", "ls"]), &w.scratch, "ls-restart");
    assert_eq!(rows.code, 0, "ls: {}", rows.stderr);

    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(
        w.rows().len(),
        2,
        "no second review, and no review of the review: {:?}",
        w.rows()
    );
    assert_eq!(
        std::fs::read_to_string(w.worker_dir(&id).join("review.json")).unwrap(),
        first,
        "the findings were not rewritten"
    );

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

// ---------------------------------------------------------------------------
// the actor discipline, as source
// ---------------------------------------------------------------------------

/// **The `WorkerSettled` arm does no blocking work.** The core loop is
/// `handle(msg).await`, so a `git diff` — or a `spawn_blocking` — awaited from
/// that arm stalls `status`, `admit`, `kill`, `send` and the notification demux
/// for its whole duration. The two-hop shape is the fix, and this is the check
/// that keeps it: the arm may DECIDE and it may `tracker.spawn`, and it may not
/// await anything that runs a subprocess or a blocking thread.
///
/// Source-scanned rather than timed, for the reason `tests/cmd_broker.rs`'s
/// neighbours are: the failure mode is a stall under load, not a wrong answer.
#[test]
fn the_worker_settled_arm_never_awaits_a_subprocess_or_a_blocking_task() {
    let core =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/broker/core.rs"))
            .expect("src/broker/core.rs");

    // The arm itself: from `CoreMsg::WorkerSettled(settled) => {` to the
    // matching close, found by brace depth.
    let start = core
        .find("CoreMsg::WorkerSettled(settled) => {")
        .expect("the WorkerSettled arm");
    let body = &core[start..];
    let mut depth = 0usize;
    let mut end = body.len();
    for (i, c) in body.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = i + 1;
                    break;
                }
            }
            _ => {}
        }
    }
    let arm = &body[..end];
    for forbidden in [
        "spawn_blocking",
        "Command::new",
        "std::fs::read",
        "load_snapshot(",
    ] {
        // `state::load_snapshot` IS allowed (it is the one small read the
        // guards need, and `register` does the same on this task); everything
        // that forks a process or parks a thread is not.
        if forbidden == "load_snapshot(" {
            continue;
        }
        assert!(
            !arm.contains(forbidden),
            "the WorkerSettled arm must not `{forbidden}`:\n{arm}"
        );
    }
    // ...and it DOES hand the work on.
    assert!(
        arm.contains("on_settled_for_review"),
        "the arm delegates to the two-hop hook:\n{arm}"
    );
    let hook = core
        .split("async fn on_settled_for_review")
        .nth(1)
        .expect("the first hop");
    let hook = &hook[..hook.find("\n    /// **Second hop.**").unwrap_or(hook.len())];
    for forbidden in ["spawn_blocking", "Command::new"] {
        assert!(
            !hook.contains(forbidden),
            "the first hop must not `{forbidden}`:\n{hook}"
        );
    }
    assert!(
        hook.contains("self.tracker.spawn"),
        "the first hop detaches the work:\n{hook}"
    );
}
