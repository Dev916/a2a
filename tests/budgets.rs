//! Per-lane turn budgets as an operator meets them (spec §4.9): the config
//! surface, the clamp a brief gets when it asks for more than its lane allows,
//! the fold-derived counter, and the broker-side trip that turns a runaway
//! worker into `failed{budget_exhausted}` instead of an invoice.
//!
//! Two conventions carried over from `tests/admission.rs`, load-bearing for the
//! same reasons: **converged-state assertions** (nothing here asserts how fast
//! a daemon does anything) and **a guard per home** (a daemon that outlives its
//! temp home is a stray, not a pass).
//!
//! The `hang-turn` fake is what makes the trip race-free: it opens a turn and
//! holds it until steered or interrupted, so a worker that has spent its budget
//! is demonstrably still mid-turn when the actor stops it — the assertion is
//! about the budget rather than about who finished first.

mod common;

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CONVERGE: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(50);

fn a2a_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("a2a")
}

/// `a2a --home <home> …` with the broker environment: the scripted app-server
/// as the codex binary, a temp allowlist, and no inherited `A2A_HOME`.
fn a2a(home: &Path, allowed_root: &Path, args: &[&str]) -> Command {
    let mut c = Command::new(a2a_bin());
    c.arg("--home")
        .arg(home)
        .args(args)
        .env_remove("A2A_HOME")
        .env_remove("A2A_BROKER_DISABLE")
        .env("A2A_CODEX_BIN", common::fake_app_server_path())
        .env("A2A_ALLOWED_ROOTS", allowed_root)
        .env("A2A_BROKER_BIN", a2a_bin())
        .env("A2A_SHIM_BIN", a2a_bin())
        .env("A2A_OWNER", common::TEST_OWNER);
    c
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
            panic!("timed out waiting for {what}");
        }
        std::thread::sleep(POLL);
    }
}

fn snap_of(home: &Path, id: &str) -> Option<Value> {
    let text = std::fs::read_to_string(home.join("workers").join(id).join("state.json")).ok()?;
    serde_json::from_str(&text).ok()
}

fn until_state(home: &Path, id: &str, want: &str) -> Value {
    until(&format!("worker {id} to reach `{want}`"), || {
        snap_of(home, id).filter(|s| s["state"] == json!(want))
    })
}

fn events_of(home: &Path, id: &str) -> Vec<Value> {
    std::fs::read_to_string(home.join("workers").join(id).join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

fn write_brief(dir: &Path, name: &str, body: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, body).unwrap();
    p
}

/// Every assertion about a trip also asserts this: the budget lands on an
/// EXISTING FSM edge with a refined diagnosis, so nothing the fold saw was
/// rejected. A `fold_rejections` entry would mean the machine was asked for a
/// transition it does not have — which is precisely what "the FSM stays frozen"
/// forbids.
fn assert_no_fold_rejections(snap: &Value) {
    assert!(
        snap.get("fold_rejections").is_none(),
        "the trip rides an existing edge: {:?}",
        snap.get("fold_rejections")
    );
}

// ---------------------------------------------------------------------------
// the trip
// ---------------------------------------------------------------------------

/// `max_turns = 0` is "no turns allowed" — the same safe reading every other cap
/// in `config.rs` has — so the FIRST `turn/started` is the one too many.
///
/// `hang-turn` is what makes this a statement about the budget: the fake never
/// ends the turn, so the only thing that can move this worker off `running` is
/// the actor deciding it has run out.
#[test]
fn a_zero_budget_trips_on_the_first_turn() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "zero.md", "do the thing\n");

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_SCENARIO", "hang-turn")
        .env("A2A_LANES_GENERAL_MAX_TURNS", "0");
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().unwrap().to_string();

    let failed = until_state(&home, &id, "failed");
    assert_eq!(
        failed["failure_class"],
        json!("budget_exhausted"),
        "{failed}"
    );
    assert_eq!(failed["turns_this_epoch"], json!(1), "{failed}");
    assert_eq!(failed["max_turns"], json!(0), "{failed}");
    assert_no_fold_rejections(&failed);

    // Exactly one — the trip is a single commit, not a per-notification retry.
    let trips: Vec<Value> = events_of(&home, &id)
        .into_iter()
        .filter(|e| e["kind"] == json!("budget_exhausted"))
        .collect();
    assert_eq!(trips.len(), 1, "one trip, one envelope: {trips:?}");
    assert_eq!(
        trips[0]["raw"],
        json!({"fsm": "Timeout", "kind": "task", "failure_class": "budget_exhausted",
               "turns_this_epoch": 1, "max_turns": 0}),
        "the payload is an EXISTING FSM edge with a refined diagnosis"
    );
    assert_eq!(trips[0]["class"], json!("lifecycle"));

    // The operator-facing answer: exit 4, and the class on `error.data`.
    let status = run(
        a2a(&home, tmp.path(), &["--json", "status", &id]),
        &scratch,
        "status",
    );
    assert_eq!(
        status.code, 4,
        "a failed worker is exit 4: {}",
        status.stderr
    );
    assert_eq!(
        status.data()["failure_class"],
        json!("budget_exhausted"),
        "{}",
        status.stdout
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The budget counts turns across a PARK, not just within one: turn 1 asks a
/// question and parks, the answer opens turn 2, and turn 2 is the one that
/// exceeds `max_turns = 1`.
///
/// The general lane is put on `outbox-only` for the same reason
/// `tests/cmd_broker.rs` reaches for the research lane in its `blocked` tests:
/// an outbox-only thread is rooted on the WORKER DIR, which is where the fake
/// writes its question. The budget itself is untouched by that — it is the
/// lane's `max_turns` under test, not its sandbox.
#[test]
fn the_budget_counts_turns_across_a_park() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "park.md", "A or B?\n");

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_SCENARIO", "blocked")
        .env("A2A_LANES_GENERAL_SANDBOX", "outbox-only")
        .env("A2A_LANES_GENERAL_MAX_TURNS", "1");
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().unwrap().to_string();

    // Turn 1: one turn, inside the budget, parked on its question.
    let parked = until_state(&home, &id, "awaiting-input");
    assert_eq!(parked["state_reason"], json!("blocked"), "{parked}");
    assert_eq!(
        parked["turns_this_epoch"],
        json!(1),
        "one turn spent, and one allowed — no trip yet: {parked}"
    );

    // The answer opens turn 2, which is the turn too many.
    let sent = run(
        a2a(&home, tmp.path(), &["--json", "send", &id, "go on"]),
        &scratch,
        "send",
    );
    assert_eq!(sent.code, 0, "send: {}", sent.stderr);

    let failed = until_state(&home, &id, "failed");
    assert_eq!(
        failed["failure_class"],
        json!("budget_exhausted"),
        "{failed}"
    );
    assert_eq!(
        failed["turns_this_epoch"],
        json!(2),
        "the park did not reset the count: {failed}"
    );
    assert_eq!(failed["max_turns"], json!(1), "{failed}");
    assert_no_fold_rejections(&failed);

    drop(guard);
    common::assert_no_broker_left(&home);
}

// ---------------------------------------------------------------------------
// the surface
// ---------------------------------------------------------------------------

/// Genesis records the EFFECTIVE budget, the brief may only lower it, and the
/// snapshot is rebuild-lossless: delete `state.json`, run `a2a status`, and the
/// budget is still there because the WAL — not the checkpoint — is the truth.
///
/// The impl lane, because the pinned clamp wording names it. `--no-worktree`
/// keeps this about the budget rather than about git.
#[test]
fn a_brief_may_only_lower_the_budget_and_genesis_survives_a_rebuild() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let brief = write_brief(
        tmp.path(),
        "greedy.md",
        "---\nacceptance_criteria:\n  - it works\nverify:\n  - cargo test\nmax_turns: 99\n---\n\ndo the thing\n",
    );

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &[
            "--json",
            "spawn",
            "--lane",
            "impl",
            "--no-worktree",
            "--cwd",
        ],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        // The exec path: this test is about what `spawn` records and prints, and
        // a daemon would only add a child to tear down.
        .env("A2A_BROKER_DISABLE", "1")
        .env("A2A_CODEX_BIN", common::stub_path());
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().unwrap().to_string();

    assert!(
        spawned.stderr.lines().any(|l| l
            == "a2a: warning: brief max_turns 99 exceeds the impl lane's 40 — clamped to 40 \
                (a brief may only lower the turn budget)"),
        "the clamp warning, prefixed exactly once: {:?}",
        spawned.stderr
    );

    let genesis = events_of(&home, &id)
        .into_iter()
        .find(|e| e["raw"]["fsm"] == json!("Spawn"))
        .expect("a genesis row");
    assert_eq!(
        genesis["raw"]["max_turns"],
        json!(40),
        "genesis records the EFFECTIVE budget, not what the brief asked for"
    );

    common::wait_dead(&home, &id, 10);
    let before = run(
        a2a(&home, tmp.path(), &["--json", "status", &id]),
        &scratch,
        "status-before",
    );
    assert!(before.code == 0 || before.code == 4, "{}", before.stderr);
    let snap = snap_of(&home, &id).expect("a snapshot");
    assert_eq!(snap["max_turns"], json!(40));
    assert_eq!(
        snap["turns_this_epoch"],
        json!(1),
        "one turn, counted: {snap}"
    );

    // Rebuild-lossless: the checkpoint is derived, so losing it costs a replay
    // and nothing else. The BUDGET is what genesis carries and therefore what
    // the rebuild has to restore; the counter is asserted above, on the live
    // snapshot, because a rebuild-from-zero also loses the exec ingest offset
    // and re-reads the stream — pre-existing behaviour this task does not own.
    let state = home.join("workers").join(&id).join("state.json");
    std::fs::remove_file(&state).unwrap();
    let rebuilt = run(
        a2a(&home, tmp.path(), &["--json", "status", &id]),
        &scratch,
        "status-rebuilt",
    );
    assert!(rebuilt.code == 0 || rebuilt.code == 4, "{}", rebuilt.stderr);
    let snap = snap_of(&home, &id).expect("the rebuilt snapshot");
    assert_eq!(
        snap["max_turns"],
        json!(40),
        "the budget came back out of the WAL: {snap}"
    );
    assert!(
        snap["turns_this_epoch"].as_u64().unwrap_or(0) >= 1,
        "and so did the count: {snap}"
    );
}

/// `a2a resume` on a `failed{budget_exhausted}` worker grants a fresh budget:
/// the epoch bump — which is what a resume IS — resets the counter to zero.
///
/// Seeded rather than tripped, and on the exec transport, because the fact
/// under test is the resume, not the trip: this way the worker is in exactly
/// the state an operator finds one in, with `max_turns = 1` already spent.
#[test]
fn resume_gives_a_budget_exhausted_worker_a_fresh_budget() {
    let (home, tmp, id) = common::spawn_with(&[]);
    common::wait_dead(&home, &id, 10);
    common::a2a(&home, tmp.path())
        .args(["status", &id])
        .assert()
        .code(0);

    // The state an operator meets: out of turns, one turn allowed, terminal.
    let dir = home.join("workers").join(&id);
    let mut v = common::read_snapshot(&home, &id);
    v["state"] = json!("failed");
    v["state_reason"] = Value::Null;
    v["failure_class"] = json!("budget_exhausted");
    v["max_turns"] = json!(1);
    v["turns_this_epoch"] = json!(2);
    a2a::store::write_atomic(&dir.join("state.json"), v.to_string().as_bytes()).unwrap();

    common::a2a(&home, tmp.path())
        .args(["resume", &id, "you may have more turns"])
        .assert()
        .success();

    let after = common::read_snapshot(&home, &id);
    assert_eq!(after["epoch"], json!(2), "a resume is a new epoch: {after}");
    assert!(
        after["turns_this_epoch"] == json!(0) || after["turns_this_epoch"] == json!(1),
        "the counter restarted at zero for the new epoch (the resumed run may \
         already have opened its first turn): {after}"
    );
    assert_eq!(
        after["max_turns"],
        json!(1),
        "the budget itself is unchanged — a resume grants a fresh one, not a bigger one"
    );
}

/// `ls --json` carries both numbers on every row; the human table is unchanged.
#[test]
fn ls_json_reports_the_counter_and_the_budget() {
    let (home, tmp, id) = common::spawn_with(&[]);
    common::wait_dead(&home, &id, 10);
    common::a2a(&home, tmp.path())
        .args(["status", &id])
        .assert()
        .code(0);

    let out = common::a2a(&home, tmp.path())
        .args(["--json", "ls"])
        .assert()
        .success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let row = v["data"]["workers"]
        .as_array()
        .expect("an array of rows")
        .iter()
        .find(|r| r["id"] == json!(id))
        .expect("the worker is listed")
        .clone();
    assert_eq!(row["max_turns"], json!(30), "the general lane's default");
    assert_eq!(row["turns_this_epoch"], json!(1), "{row}");

    let human = common::a2a(&home, tmp.path())
        .args(["ls"])
        .assert()
        .success();
    let text = String::from_utf8_lossy(&human.get_output().stdout).to_string();
    assert!(
        !text.to_uppercase().contains("TURNS"),
        "the human table is unchanged: {text}"
    );
}

/// The config surface end to end: `config show --origin` names the key, the env
/// override is the layer it reports, and a malformed value is `CONFIG_INVALID`
/// naming the dotted path.
#[test]
fn the_lane_budget_is_visible_and_overridable_through_the_config_surface() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");

    let mut cmd = a2a(&home, tmp.path(), &["--json", "config", "show", "--origin"]);
    cmd.env("A2A_BROKER_DISABLE", "1")
        .env("A2A_LANES_REVIEW_MAX_TURNS", "3");
    let shown = run(cmd, &scratch, "config");
    assert_eq!(shown.code, 0, "config show: {}", shown.stderr);
    let d = shown.data();
    assert_eq!(d["lanes"]["review"]["max_turns"], json!(3), "{d}");
    assert_eq!(d["lanes"]["impl"]["max_turns"], json!(40), "{d}");
    assert_eq!(d["origins"]["lanes.review.max_turns"], json!("env"), "{d}");
    assert_eq!(
        d["origins"]["lanes.impl.max_turns"],
        json!("default"),
        "{d}"
    );

    let mut bad = a2a(&home, tmp.path(), &["--json", "config", "show"]);
    bad.env("A2A_BROKER_DISABLE", "1")
        .env("A2A_LANES_IMPL_MAX_TURNS", "lots");
    let refused = run(bad, &scratch, "config-bad");
    assert_eq!(refused.code, 9, "a static precondition: {}", refused.stderr);
    let e = refused.json()["error"].clone();
    assert_eq!(e["code"], json!("CONFIG_INVALID"), "{e}");
    assert!(
        e["message"]
            .as_str()
            .unwrap()
            .contains("A2A_LANES_IMPL_MAX_TURNS"),
        "the refusal names the key that is wrong: {e}"
    );
}
