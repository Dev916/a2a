//! `broker.jsonl` as the fleet's transition log (spec §4.5), and the fleet smoke
//! that exercises Phase 5 end to end under an admission cap.
//!
//! Two halves, in the order they were built:
//!
//! 1. **The transition log.** `broker.jsonl` gains one `state_change` line per
//!    state transition — `{ts,kind,worker,from,to,event,epoch}` — written by the
//!    `WorkerActor` for every transition it commits, and by the CORE for the two
//!    it commits itself (`queued → spawned` in `admit_queued`, `queued →
//!    cancelled` in `cancel_queued`). `event` is whatever `what` label the
//!    persist call carried, and **the label set is OPEN**: Task 1's
//!    `trip_budget` and Task 5's paths already added to it, so nothing here
//!    asserts a fixed enumeration — only that `event` is a non-empty string.
//! 2. **The fleet smoke.** Three concurrent spawns from three threads plus the
//!    auto-reviewer, against `A2A_ADMISSION_GLOBAL_CAP=2`: the cap holds on
//!    every sample, one worker trips its budget, the rest finish, the reviewer
//!    is admitted through the same door, and `broker.jsonl` tells the whole
//!    story coherently. The cap against three spawns is what guarantees at least
//!    one worker is born `queued`, which is what makes this the test that fails
//!    if the two CORE writers were skipped.
//!
//! Conventions carried from `tests/budgets.rs` and `tests/auto_review.rs`, both
//! load-bearing: **converged-state assertions** (nothing asserts how fast a
//! daemon gets anywhere) and **a guard per home** (a daemon that outlives its
//! temp home is a stray, not a pass).
//!
//! The shared `FAKE_MESSAGE` is findings-shaped for the reason `auto_review.rs`
//! documents: it is ONE environment variable on ONE app-server child shared by
//! every worker, so the impl worker carries an explicit `--schema` pointing at
//! the findings schema the reviewer gets by lane default.

mod common;

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The AC's window: a fleet of four workers behind a cap of two, on a machine
/// that may be busy. Nothing here asserts speed.
const CONVERGE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);

/// A findings document that satisfies `templates/review-findings.schema.json`
/// — and therefore both the reviewer's lane default and the impl worker's
/// explicit `--schema`.
const FINDINGS: &str = r#"{"findings":[{"file":"src/lib.rs","line":2,"severity":"low","rationale":"the marker line is unexplained"}]}"#;

fn a2a_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("a2a")
}

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
/// `roots` (the repo's parent) and `home` are SEPARATE temp dirs, as in
/// `tests/auto_review.rs`: `$A2A_HOME/workers/<id>` is then NOT under an allowed
/// root, which is what keeps the auto-reviewer's `cwd: None` request honest.
struct World {
    roots: tempfile::TempDir,
    #[allow(dead_code)]
    homedir: tempfile::TempDir,
    home: PathBuf,
    repo: PathBuf,
    work: PathBuf,
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
    let work = roots.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let scratch = roots.path().join("scratch");
    World {
        roots,
        homedir,
        home,
        repo,
        work,
        scratch,
    }
}

impl World {
    /// `a2a --home <home> …` on the live transport. Every knob the DAEMON reads
    /// is here rather than on one spawn: whichever invocation autostarts the
    /// broker is the one whose environment the daemon inherits, and a smoke
    /// whose cap depended on which thread won the race would not be a smoke.
    fn cli(&self, args: &[&str]) -> Command {
        let mut c = Command::new(a2a_bin());
        c.arg("--home")
            .arg(&self.home)
            .args(args)
            .env_remove("A2A_HOME")
            .env_remove("A2A_BROKER_DISABLE")
            .env("A2A_CODEX_BIN", common::fake_app_server_path())
            .env("A2A_ALLOWED_ROOTS", self.roots.path())
            .env("A2A_BROKER_BIN", a2a_bin())
            .env("A2A_SHIM_BIN", a2a_bin())
            .env("A2A_OWNER", common::TEST_OWNER)
            .env("A2A_ADMISSION_GLOBAL_CAP", "2")
            .env("A2A_LANES_IMPL_AUTO_REVIEW", "1")
            .env("FAKE_SCENARIO", "hang-turn")
            .env("FAKE_MESSAGE", FINDINGS);
        c
    }

    fn worker_dir(&self, id: &str) -> PathBuf {
        self.home.join("workers").join(id)
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

    fn broker_log(&self) -> Vec<Value> {
        std::fs::read_to_string(self.home.join("broker.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                serde_json::from_str(l)
                    .unwrap_or_else(|e| panic!("every broker.jsonl line parses ({e}): {l}"))
            })
            .collect()
    }

    /// Every `state_change` line for one worker, in file order.
    fn transitions(&self, id: &str) -> Vec<Value> {
        self.broker_log()
            .into_iter()
            .filter(|l| l["kind"] == json!("state_change") && l["worker"] == json!(id))
            .collect()
    }

    fn rows(&self) -> Vec<Value> {
        let ran = run(self.cli(&["--json", "ls"]), &self.scratch, "ls");
        ran.data()["workers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }

    /// Steer a `hang-turn` worker so its turn completes with `FAKE_MESSAGE`.
    fn finish(&self, id: &str, tag: &str) {
        self.until_state(id, "running");
        let ran = run(
            self.cli(&["--json", "send", id, "finish up"]),
            &self.scratch,
            tag,
        );
        assert_eq!(ran.code, 0, "send {id}: {} {}", ran.stdout, ran.stderr);
    }

    /// Bring the daemon up with THIS environment, before anything races for it.
    /// `broker run` double-forks and returns; `broker status` never autostarts,
    /// so converging on it is a question that does not change its own answer.
    fn start_broker(&self) {
        let ran = run(self.cli(&["broker", "run"]), &self.scratch, "broker-run");
        assert_eq!(ran.code, 0, "broker run: {} {}", ran.stdout, ran.stderr);
        until("the daemon to answer `broker status`", || {
            let ran = run(
                self.cli(&["--json", "broker", "status"]),
                &self.scratch,
                "status-up",
            );
            (ran.code == 0).then_some(())
        });
    }

    /// One `broker status --json` sample. `None` once the daemon is gone.
    fn status_now(&self, tag: &str) -> Option<Value> {
        let ran = run(
            self.cli(&["--json", "broker", "status"]),
            &self.scratch,
            tag,
        );
        (ran.code == 0).then(|| ran.data())
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

/// This pid's command line, if it is still a live process.
///
/// The stray check is scoped to a pid THIS test created (the `child_pid` its own
/// daemon reported) rather than to a `ps` sweep for "codex": a global sweep on a
/// developer's machine finds the developer's own codex and calls the suite red.
fn command_of(pid: u64) -> Option<String> {
    let out = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
        .expect("ps runs");
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (out.status.success() && !text.is_empty()).then_some(text)
}

/// Every `state_change` line's shape, asserted the ONE way the AC allows: the
/// six siblings of `ts` are present, `from`/`to` are kebab-case `WorkerState`
/// spellings, and `event` is a **non-empty string** — never a member of a
/// pinned enumeration, because Task 1's `trip_budget`, Task 5's paths and the
/// core's own `admit`/`cancel_queued` all add to that set.
fn assert_line_shape(line: &Value) {
    for key in ["ts", "kind", "worker", "from", "to", "event", "epoch"] {
        assert!(
            line.get(key).is_some(),
            "a state_change line carries `{key}`: {line}"
        );
    }
    assert_eq!(line["kind"], json!("state_change"), "{line}");
    assert!(
        line["ts"]
            .as_str()
            .is_some_and(|s| chrono::DateTime::parse_from_rfc3339(s).is_ok()),
        "`ts` is rfc3339: {line}"
    );
    assert!(
        line["worker"].as_str().is_some_and(|s| !s.is_empty()),
        "`worker` is a non-empty string: {line}"
    );
    assert!(
        line["event"].as_str().is_some_and(|s| !s.is_empty()),
        "`event` is a non-empty string: {line}"
    );
    assert!(
        line["epoch"].as_u64().is_some(),
        "`epoch` is a number: {line}"
    );
    for key in ["from", "to"] {
        let s = line[key]
            .as_str()
            .unwrap_or_else(|| panic!("`{key}` is a string: {line}"));
        assert!(
            STATES.contains(&s),
            "`{key}` is a WorkerState::as_str() spelling, got {s:?}: {line}"
        );
    }
}

/// `WorkerState::as_str()`, kebab-case — the wire/CLI spelling.
const STATES: [&str; 10] = [
    "spawned",
    "queued",
    "running",
    "awaiting-input",
    "escalated",
    "done",
    "failed",
    "cancelled",
    "orphaned",
    "unknown",
];

// ---------------------------------------------------------------------------
// 1. the transition log
// ---------------------------------------------------------------------------

/// The plainest statement of spec §4.5: a worker that runs and finishes leaves
/// an ordered trail of `state_change` lines in `broker.jsonl` whose `to` values
/// walk from its genesis state to the state its `state.json` ends on, with **no
/// line for a self-loop**.
#[test]
fn broker_jsonl_carries_one_state_change_line_per_transition() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let brief = w.roots.path().join("general.md");
    std::fs::write(&brief, "do the thing\n").unwrap();
    let mut cmd = w.cli(&["--json", "spawn", "--lane", "general", "--cwd"]);
    cmd.arg(&w.work).arg("--brief").arg(&brief);
    let ran = run(cmd, &w.scratch, "spawn");
    assert_eq!(ran.code, 0, "spawn: {} {}", ran.stdout, ran.stderr);
    let id = ran.data()["id"].as_str().unwrap().to_string();

    // `hang-turn`: the worker is demonstrably mid-turn, so `spawned → running`
    // has certainly been committed before the steer that ends it.
    w.finish(&id, "send");
    w.until_state(&id, "done");

    let lines = until("the worker's transitions to end at `done`", || {
        let lines = w.transitions(&id);
        lines
            .last()
            .filter(|l| l["to"] == json!("done"))
            .map(|_| lines.clone())
    });
    for line in &lines {
        assert_line_shape(line);
    }

    // The chain is a chain: each line's `from` is the previous line's `to`, it
    // starts where genesis put the worker, and no line is a self-loop.
    assert_eq!(lines[0]["from"], json!("spawned"), "{lines:?}");
    for pair in lines.windows(2) {
        assert_eq!(
            pair[1]["from"], pair[0]["to"],
            "the chain is contiguous: {lines:?}"
        );
    }
    for line in &lines {
        assert_ne!(line["from"], line["to"], "no line for a self-loop: {line}");
    }
    let tos: Vec<&str> = lines.iter().map(|l| l["to"].as_str().unwrap()).collect();
    assert!(
        tos.contains(&"running") && tos.last() == Some(&"done"),
        "the trail walks spawned → running → done: {tos:?}"
    );

    // And the worker's OWN writes vastly outnumber its transitions: `persist`
    // is called for every trace flush and every delivery too, and only the ones
    // that changed state may appear here.
    assert!(
        lines.len() <= 4,
        "one line per transition, not one per persist: {lines:?}"
    );

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

/// The transition the ACTOR cannot write: a worker born `queued` is admitted by
/// `BrokerCore::admit_queued`, which does not go through `WorkerActor::persist`
/// at all. Without the core's own writer there is no `from:"queued"` line
/// anywhere — and the fleet smoke's first-`from` assertion is unsatisfiable.
#[test]
fn the_core_writes_the_two_transitions_the_actor_never_sees() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);

    let brief = w.roots.path().join("general.md");
    std::fs::write(&brief, "do the thing\n").unwrap();
    let spawn = |tag: &str| -> String {
        let mut cmd = w.cli(&["--json", "spawn", "--lane", "general", "--cwd"]);
        cmd.arg(&w.work)
            .arg("--brief")
            .arg(&brief)
            // A cap of one, so the second and third spawns must wait.
            .env("A2A_ADMISSION_GLOBAL_CAP", "1");
        let ran = run(cmd, &w.scratch, tag);
        assert_eq!(ran.code, 0, "spawn: {} {}", ran.stdout, ran.stderr);
        ran.data()["id"].as_str().unwrap().to_string()
    };

    let first = spawn("spawn-1");
    w.until_state(&first, "running");
    let second = spawn("spawn-2");
    let third = spawn("spawn-3");
    w.until_state(&second, "queued");
    w.until_state(&third, "queued");

    // --- `queued → cancelled`, committed by `cancel_queued`.
    let killed = run(w.cli(&["--json", "kill", &third]), &w.scratch, "kill");
    assert_eq!(killed.code, 0, "kill: {} {}", killed.stdout, killed.stderr);
    w.until_state(&third, "cancelled");
    let cancel = until("the queued worker's cancel line", || {
        w.transitions(&third)
            .into_iter()
            .find(|l| l["to"] == json!("cancelled"))
    });
    assert_line_shape(&cancel);
    assert_eq!(cancel["from"], json!("queued"), "{cancel}");
    assert_eq!(cancel["event"], json!("cancel_queued"), "{cancel}");

    // --- `queued → spawned`, committed by `admit_queued` when a slot frees.
    w.finish(&first, "send-1");
    w.until_state(&first, "done");
    w.until_state(&second, "running");
    // `broker.jsonl` trails `state.json`: `WorkerActor::persist` commits the
    // snapshot first and appends the transition line afterwards, so the state
    // read above can win the race against the admission line and the
    // `spawned → …` line that follows it. Poll for both, as the `until` above
    // does, rather than indexing a log that is still catching up.
    let lines = until("the admitted worker's first two transitions", || {
        let lines = w.transitions(&second);
        (lines.len() >= 2).then_some(lines)
    });
    assert_line_shape(&lines[0]);
    assert_eq!(
        lines[0]["from"],
        json!("queued"),
        "the admission is the queued worker's first transition: {lines:?}"
    );
    assert_eq!(lines[0]["to"], json!("spawned"), "{lines:?}");
    assert_eq!(lines[0]["event"], json!("admit"), "{lines:?}");
    assert_eq!(lines[1]["from"], json!("spawned"), "{lines:?}");

    w.finish(&second, "send-2");
    w.until_state(&second, "done");

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

// ---------------------------------------------------------------------------
// 2. the fleet smoke
// ---------------------------------------------------------------------------

/// Phase 5, end to end, on one daemon: **three concurrent spawns from three
/// threads against a global cap of two**, plus the auto-reviewer the impl worker
/// earns when it finishes.
///
/// What it pins, in the order the assertions read:
///
/// * the cap held — `broker status --json`'s `live` is sampled every 100ms for
///   the whole run and is `<= 2` on EVERY sample, not on one snapshot;
/// * the budget trip is one `failed{budget_exhausted}` and the rest are `done`;
/// * the auto-reviewer went through the same admission door as anything else
///   (it may legitimately have waited in the queue) and reached a terminal state;
/// * `broker.jsonl` is coherent: every line parses, every `state_change` line
///   carries the seven pinned keys, and each worker's ordered `to` values end at
///   exactly the state its `state.json` ends on, starting from the state its
///   genesis seeded — `spawned` or `queued`;
/// * nothing was left behind: no broker, and no app-server child.
///
/// The daemon is started EXPLICITLY before the race. Every knob the core reads
/// (the cap, `auto_review`, the fake's scenario and message) belongs to the
/// daemon's own environment, and a smoke whose cap depended on which of three
/// threads won the autostart race would be measuring the race.
#[test]
fn a_fleet_of_four_survives_a_cap_of_two_and_broker_jsonl_tells_the_story() {
    let w = world();
    let guard = common::BrokerGuard::new(&w.home);
    w.start_broker();
    let child_pid = w
        .status_now("status-child")
        .and_then(|d| d["child_pid"].as_u64());

    // --- three briefs, and the barrier that makes the three spawns a race
    //     rather than a queue of three sequential invocations.
    let w1_brief = w.roots.path().join("w1.md");
    std::fs::write(&w1_brief, "hang about\n").unwrap();
    let w3_brief = w.roots.path().join("w3.md");
    std::fs::write(&w3_brief, "do the thing\n").unwrap();
    let w2_brief = w.roots.path().join("w2.md");
    std::fs::write(
        &w2_brief,
        "---\nlane: impl\nacceptance_criteria:\n  - the marker lands in the workspace\nverify:\n  - \"true\"\n---\ndo the thing\n",
    )
    .unwrap();
    let barrier = std::sync::Barrier::new(3);
    let spawns = [
        // W1: a zero turn budget of its OWN (genesis records it, clamped by
        // nothing), so this worker — and only this worker — trips on its first
        // turn while the others hang.
        ("w1", "general", w.work.clone(), w1_brief.clone()),
        // W2: the impl lane, which defaults to a worktree (D1) — the diff the
        // auto-reviewer will be given.
        ("w2", "impl", w.repo.clone(), w2_brief.clone()),
        ("w3", "general", w.work.clone(), w3_brief.clone()),
    ];

    // --- the sampler: every 100ms, for the duration, on EVERY sample.
    let stop = Arc::new(AtomicBool::new(false));
    let samples = std::thread::scope(|scope| {
        let sampler = {
            let stop = Arc::clone(&stop);
            let w = &w;
            scope.spawn(move || {
                let mut seen: Vec<u64> = Vec::new();
                let mut n = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    if let Some(data) = w.status_now(&format!("sample-{n}")) {
                        if let Some(live) = data["live"].as_u64() {
                            seen.push(live);
                        }
                    }
                    n += 1;
                    std::thread::sleep(POLL);
                }
                seen
            })
        };

        // --- three spawns, issued concurrently from three threads.
        let threads: Vec<_> = spawns
            .iter()
            .map(|(tag, lane, cwd, brief)| {
                let barrier = &barrier;
                let w = &w;
                scope.spawn(move || {
                    let mut cmd = w.cli(&["--json", "spawn", "--lane"]);
                    cmd.arg(lane)
                        .arg("--cwd")
                        .arg(cwd)
                        .arg("--brief")
                        .arg(brief);
                    if *tag == "w1" {
                        cmd.env("A2A_LANES_GENERAL_MAX_TURNS", "0");
                    } else if *tag == "w2" {
                        // The shared `FAKE_MESSAGE` is findings-shaped, and the
                        // impl lane's default schema would fail it — see the
                        // header, and `tests/auto_review.rs`'s.
                        cmd.arg("--schema").arg(findings_schema());
                    }
                    barrier.wait();
                    let ran = run(cmd, &w.scratch, tag);
                    assert_eq!(ran.code, 0, "spawn {tag}: {} {}", ran.stdout, ran.stderr);
                    ran.data()["id"].as_str().unwrap().to_string()
                })
            })
            .collect();
        let ids: Vec<String> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        let (w1, w2, w3) = (ids[0].clone(), ids[1].clone(), ids[2].clone());

        // --- converge. W2 and W3 are `hang-turn` and finish only when steered;
        //     W1 needs nothing from anyone. Steering first is what frees the
        //     slots a queued worker is waiting for, so the order is: hand the
        //     hangers their ending, then wait for the trip.
        w.finish(&w2, "send-w2");
        w.finish(&w3, "send-w3");
        let failed = w.until_state(&w1, "failed");
        assert_eq!(
            failed["failure_class"],
            json!("budget_exhausted"),
            "W1 spent a budget of zero: {failed}"
        );
        w.until_state(&w2, "done");
        w.until_state(&w3, "done");

        // --- the reviewer went through the same door, and reached the end.
        let reviewer = w.until_reviewer(&w2);
        let rid = reviewer["id"].as_str().unwrap().to_string();
        w.finish(&rid, "send-review");
        w.until_state(&rid, "done");
        until("review.json in the impl worker's dir", || {
            std::fs::read_to_string(w.worker_dir(&w2).join("review.json")).ok()
        });

        stop.store(true, Ordering::Relaxed);
        let samples = sampler.join().unwrap();
        (samples, w1, w2, w3, rid)
    });
    let (samples, w1, w2, w3, rid) = samples;

    // --- the cap held, on every sample.
    assert!(
        samples.len() >= 5,
        "the sampler ran for the duration: {samples:?}"
    );
    assert!(
        samples.iter().all(|&live| live <= 2),
        "`live` exceeded the cap of 2 on some sample: {samples:?}"
    );

    // --- final states: exactly one failed{budget_exhausted}, the rest done.
    let rows = w.rows();
    assert_eq!(rows.len(), 4, "four workers, no more: {rows:?}");
    let failed: Vec<&Value> = rows
        .iter()
        .filter(|r| r["state"] == json!("failed"))
        .collect();
    assert_eq!(failed.len(), 1, "exactly one failure: {rows:?}");
    assert_eq!(failed[0]["id"], json!(w1), "{rows:?}");
    assert_eq!(
        w.snap(&w1).unwrap()["failure_class"],
        json!("budget_exhausted"),
        "{rows:?}"
    );
    for id in [&w2, &w3, &rid] {
        let row = rows.iter().find(|r| r["id"] == json!(*id)).unwrap();
        assert_eq!(row["state"], json!("done"), "{row}");
    }
    let review_row = rows.iter().find(|r| r["id"] == json!(rid)).unwrap();
    assert_eq!(review_row["auto_review_of"], json!(w2), "{review_row}");

    // --- `broker.jsonl` coherence, per worker.
    //     `broker_log()` panics on a line that does not parse, so "every line is
    //     JSON" is asserted by reading it at all.
    let mut queued_born = 0usize;
    for id in [&w1, &w2, &w3, &rid] {
        let lines = until(
            &format!("worker {id}'s transitions to reach its final state"),
            || {
                let lines = w.transitions(id);
                let final_state = w.snap(id).map(|s| s["state"].clone())?;
                lines
                    .last()
                    .filter(|l| l["to"] == final_state)
                    .map(|_| lines.clone())
            },
        );
        for line in &lines {
            assert_line_shape(line);
            assert_ne!(line["from"], line["to"], "no line for a self-loop: {line}");
        }
        for pair in lines.windows(2) {
            assert_eq!(
                pair[1]["from"], pair[0]["to"],
                "worker {id}'s trail is contiguous: {lines:?}"
            );
        }
        let first = lines[0]["from"].as_str().unwrap();
        assert!(
            first == "spawned" || first == "queued",
            "worker {id} starts where its genesis put it, got {first:?}: {lines:?}"
        );
        if first == "queued" {
            queued_born += 1;
            assert_eq!(lines[0]["event"], json!("admit"), "{lines:?}");
        }
    }
    // The arrangement — a cap of two against three concurrent spawns plus a
    // reviewer — exists to make this reachable, and this is the assertion that
    // makes the smoke fail if `BrokerCore::admit_queued`'s writer was skipped:
    // a worker born `queued` would then have no `from:"queued"` line at all.
    // WHICH worker waits is a race (it is the impl worker in practice — its
    // `git worktree add` makes it the last request to arrive); THAT one waits
    // is the arrangement.
    assert!(
        queued_born >= 1,
        "a cap of 2 against 3 concurrent spawns plus a reviewer left nobody \
         queued — the `from:\"queued\"` writer is then untested here"
    );

    // --- every Phase-5 config key is reachable through `config show --origin`.
    assert_phase5_keys_default();

    // --- no strays: no broker, and no app-server child of this test's daemon.
    drop(guard);
    common::assert_no_broker_left(&w.home);
    if let Some(pid) = child_pid {
        let left = command_of(pid);
        assert!(
            !left
                .as_deref()
                .unwrap_or_default()
                .contains("fake-app-server"),
            "the daemon's app-server child outlived it: pid {pid} is {left:?}"
        );
    }
}

/// `a2a config show --origin` lists every Phase-5 key with origin `default` on a
/// FRESH home — so a key that was added to a struct but never wired into the
/// origins map cannot ship silently.
///
/// A home of its own, and an environment scrubbed of every `A2A_*` knob the rest
/// of this file sets: "origin default" is only a fact about a config nobody
/// overrode.
fn assert_phase5_keys_default() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("fresh");
    let mut cmd = Command::new(a2a_bin());
    cmd.arg("--home")
        .arg(&home)
        .args(["--json", "config", "show", "--origin"])
        .env_remove("A2A_HOME")
        .env("A2A_BROKER_DISABLE", "1");
    for key in [
        "A2A_ADMISSION_GLOBAL_CAP",
        "A2A_ADMISSION_QUEUE_CAP",
        "A2A_ADMISSION_BREAKER_FAILURES",
        "A2A_ADMISSION_BREAKER_WINDOW_SECS",
        "A2A_ADMISSION_MAX_ATTEMPTS",
        "A2A_BROKER_EVENTS_MAX_BYTES",
        "A2A_BROKER_TURN_STALL_SECS",
    ] {
        cmd.env_remove(key);
    }
    for lane in ["IMPL", "REVIEW", "RESEARCH", "GENERAL"] {
        cmd.env_remove(format!("A2A_LANES_{lane}_MAX_TURNS"));
        cmd.env_remove(format!("A2A_LANES_{lane}_AUTO_REVIEW"));
    }
    let ran = run(cmd, tmp.path(), "config-show");
    assert_eq!(ran.code, 0, "config show: {} {}", ran.stdout, ran.stderr);
    let data = ran.data();
    let origins = data["origins"]
        .as_object()
        .unwrap_or_else(|| panic!("config show --origin has an origins map: {}", ran.stdout));

    let mut keys: Vec<String> = vec![
        "admission.breaker_failures".into(),
        "admission.breaker_window_secs".into(),
        "admission.max_attempts".into(),
        "broker.events_max_bytes".into(),
        "broker.turn_stall_secs".into(),
    ];
    for lane in ["impl", "review", "research", "general"] {
        keys.push(format!("lanes.{lane}.max_turns"));
        keys.push(format!("lanes.{lane}.auto_review"));
    }
    for key in &keys {
        assert_eq!(
            origins.get(key.as_str()),
            Some(&json!("default")),
            "`{key}` is listed with origin default: {:?}",
            origins.get(key.as_str())
        );
    }

    // ...and the values behind them are the documented defaults.
    assert_eq!(
        data["admission"]["breaker_failures"],
        json!(5),
        "{}",
        ran.stdout
    );
    assert_eq!(
        data["admission"]["breaker_window_secs"],
        json!(300),
        "{}",
        ran.stdout
    );
    assert_eq!(
        data["admission"]["max_attempts"],
        json!(3),
        "{}",
        ran.stdout
    );
    assert_eq!(
        data["broker"]["events_max_bytes"],
        json!(10_485_760),
        "{}",
        ran.stdout
    );
    assert_eq!(
        data["broker"]["turn_stall_secs"],
        json!(600),
        "{}",
        ran.stdout
    );
    for lane in ["impl", "review", "research", "general"] {
        assert_eq!(
            data["lanes"][lane]["auto_review"],
            json!(false),
            "{}",
            ran.stdout
        );
        assert!(
            data["lanes"][lane]["max_turns"].as_u64().is_some(),
            "lanes.{lane}.max_turns is a number: {}",
            ran.stdout
        );
    }
}
