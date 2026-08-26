//! The broker's circuit breaker as an operator meets it (spec §4.9): K failed
//! settlements inside a window and the daemon stops admitting new work, loudly,
//! until somebody says the cause is fixed.
//!
//! The conventions are `tests/budgets.rs`'s, for the same reasons:
//! **converged-state assertions** (nothing here asserts how fast a daemon does
//! anything) and **a guard per home** (a daemon that outlives its temp home is
//! a stray, not a pass).
//!
//! Task 1's turn budget is what makes a failure cheap enough to script: with
//! `max_turns = 0` and the `hang-turn` fake, a worker fails on its first turn
//! without a model, a network or a wall-clock timeout being involved. The
//! breaker is what is under test; the budget is only the failure generator.

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
    fn error(&self) -> Value {
        self.json()["error"].clone()
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

fn worker_dirs(home: &Path) -> Vec<String> {
    let Ok(rd) = std::fs::read_dir(home.join("workers")) else {
        return Vec::new();
    };
    let mut names: Vec<String> = rd
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn health(home: &Path) -> Value {
    let text = std::fs::read_to_string(home.join("broker-health.json"))
        .expect("the daemon writes broker-health.json");
    serde_json::from_str(&text).expect("broker-health.json is JSON")
}

fn write_brief(dir: &Path, name: &str, body: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, body).unwrap();
    p
}

/// One spawn that trips Task 1's zero budget on its first turn: the cheapest
/// `failed` settlement this suite can produce, and the only failure generator
/// the breaker cases need.
fn spawn_failing(
    home: &Path,
    root: &Path,
    scratch: &Path,
    tag: &str,
    brief: &Path,
    work: &Path,
) -> Ran {
    let mut cmd = a2a(
        home,
        root,
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(work)
        .arg("--brief")
        .arg(brief)
        .env("FAKE_SCENARIO", "hang-turn")
        .env("A2A_ADMISSION_BREAKER_FAILURES", "2")
        .env("A2A_LANES_GENERAL_MAX_TURNS", "0");
    run(cmd, scratch, tag)
}

// ---------------------------------------------------------------------------
// trip, refuse, reset
// ---------------------------------------------------------------------------

/// Two failures at a threshold of two and the door shuts: the third spawn is
/// refused with `BREAKER_OPEN` (exit 9, a static precondition — no broker
/// appearing or disappearing changes the answer), the refusal names the count,
/// the window and the moment, and `broker-health.json` says so too.
///
/// The refusal happens at `admit`, BEFORE the CLI writes a byte of genesis, so
/// the pinned consequence is that the refused spawn leaves no worker directory
/// for `ls`/`gc` to trip over.
#[test]
fn two_failures_open_the_breaker_and_the_next_spawn_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "b.md", "do the thing\n");

    // Failure one. Sequenced (not raced) on purpose: the count that opens the
    // breaker is the count of SETTLED failures, so each spawn waits for its own.
    let first = spawn_failing(&home, tmp.path(), &scratch, "one", &brief, &work);
    assert_eq!(first.code, 0, "spawn one: {}", first.stderr);
    let a = first.data()["id"].as_str().unwrap().to_string();
    let failed = until_state(&home, &a, "failed");
    assert_eq!(
        failed["failure_class"],
        json!("budget_exhausted"),
        "{failed}"
    );

    // Failure two — the one that opens it.
    let second = spawn_failing(&home, tmp.path(), &scratch, "two", &brief, &work);
    assert_eq!(
        second.code, 0,
        "one failure is under the threshold, so this one is still admitted: {}",
        second.stderr
    );
    let b = second.data()["id"].as_str().unwrap().to_string();
    until_state(&home, &b, "failed");

    // The breaker opens on the SETTLEMENT, which the failing worker's own
    // `state.json` does not itself sequence — so converge on the daemon's own
    // record of it before asking for a third spawn. (Retrying the spawn until
    // it is refused would leave the worker directories a succeeding attempt
    // wrote, which is the very thing asserted below.)
    until("the breaker to open", || {
        (health(&home)["breaker"]["open"] == json!(true)).then_some(())
    });

    let refused = spawn_failing(&home, tmp.path(), &scratch, "three", &brief, &work);
    assert_eq!(
        refused.code, 9,
        "a breaker refusal is a static precondition: {}",
        refused.stderr
    );
    let err = refused.error();
    assert_eq!(err["code"], json!("BREAKER_OPEN"), "{}", refused.stdout);
    assert_eq!(err["data"]["failures"], json!(2), "{}", refused.stdout);
    assert_eq!(err["data"]["window_secs"], json!(300), "{}", refused.stdout);
    assert!(
        err["data"]["opened_at"]
            .as_str()
            .is_some_and(|s| s.contains('T')),
        "the moment it opened, in rfc3339: {}",
        refused.stdout
    );
    assert_eq!(
        err["remedy"],
        json!("`a2a broker reset-breaker` once the cause is fixed, or `a2a broker restart`"),
        "{}",
        refused.stdout
    );

    // Nothing was written for the spawn that was refused.
    assert_eq!(
        worker_dirs(&home),
        {
            let mut want = vec![a.clone(), b.clone()];
            want.sort();
            want
        },
        "a refused spawn leaves no worker directory behind"
    );

    let h = health(&home);
    assert_eq!(h["breaker"]["open"], json!(true), "{h}");
    assert_eq!(h["breaker"]["failures"], json!(2), "{h}");
    assert_eq!(h["breaker"]["window_secs"], json!(300), "{h}");

    // ... and `broker status` says it out loud, on both channels.
    let status = run(
        a2a(&home, tmp.path(), &["--json", "broker", "status"]),
        &scratch,
        "status",
    );
    assert_eq!(status.code, 0, "broker status: {}", status.stderr);
    assert_eq!(
        status.data()["breaker"]["open"],
        json!(true),
        "{}",
        status.stdout
    );
    assert!(
        status
            .stderr
            .contains(", breaker OPEN (2 failures in 300s)"),
        "the human line is loud about it: {:?}",
        status.stderr
    );

    // -- the reset ----------------------------------------------------------
    let reset = run(
        a2a(&home, tmp.path(), &["--json", "broker", "reset-breaker"]),
        &scratch,
        "reset",
    );
    assert_eq!(reset.code, 0, "reset-breaker: {}", reset.stderr);
    assert_eq!(
        reset.data(),
        json!({"reset": true, "was_open": true}),
        "{}",
        reset.stdout
    );

    // The door is open again. This one carries no `max_turns` override, so it
    // keeps the general lane's default budget and simply runs — the assertion
    // is that it was ADMITTED, not that it failed differently.
    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work).arg("--brief").arg(&brief);
    let after = run(cmd, &scratch, "after-reset");
    assert_eq!(
        after.code, 0,
        "a reset breaker admits again: {}",
        after.stderr
    );

    // Idempotent, and honest about it: the second reset reset nothing.
    let again = run(
        a2a(&home, tmp.path(), &["--json", "broker", "reset-breaker"]),
        &scratch,
        "reset-again",
    );
    assert_eq!(again.code, 0, "a second reset-breaker: {}", again.stderr);
    assert_eq!(
        again.data(),
        json!({"reset": true, "was_open": false}),
        "{}",
        again.stdout
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// Resetting a breaker on a broker that is not running is **exit 7**, not an
/// autostart: starting a daemon in order to reset a breaker it never had would
/// be a lie about what happened.
#[test]
fn resetting_a_breaker_with_no_broker_is_exit_seven() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");

    let ran = run(
        a2a(&home, tmp.path(), &["--json", "broker", "reset-breaker"]),
        &scratch,
        "reset-nobody",
    );
    assert_eq!(
        ran.code, 7,
        "no broker is a reachability answer, not a precondition: {}",
        ran.stderr
    );
    assert_eq!(
        ran.error()["code"],
        json!("BROKER_UNAVAILABLE"),
        "{}",
        ran.stdout
    );
    common::assert_no_broker_left(&home);
}

/// A worker directory laid out by hand, so the socket `resume` op has something
/// to aim at that the running daemon has not already registered. Seeded AFTER
/// the broker is up, precisely so startup adoption does not claim it.
fn seed_worker(home: &Path, cwd: &Path, id: &str) -> PathBuf {
    use a2a::store::EventLog;
    let dir = home.join("workers").join(id);
    for sub in ["outbox", "inbox", "inbox/delivered", "scratch", "context"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    let mut log = EventLog::open(&dir).unwrap();
    a2a::fold::append_and_fold(
        &mut log,
        &dir,
        a2a::state::WorkerSnapshot::new_for_test(id, cwd),
        a2a::events::EventKind::Spawned,
        json!({"fsm": "Spawn", "id": id, "lane": "general", "cwd": cwd,
               "transport": "app-server"}),
        chrono::Utc::now(),
    )
    .unwrap();
    let path = dir.join("state.json");
    let mut value: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let obj = value.as_object_mut().unwrap();
    obj.insert("state".into(), json!("done"));
    obj.insert("transport".into(), json!("app-server"));
    obj.insert("session_id".into(), json!("thread-done"));
    a2a::store::write_atomic(&path, value.to_string().as_bytes()).unwrap();
    dir
}

/// The breaker is checked at BOTH doors. A socket `resume` is new work — it goes
/// through `register(capped)`, not `decide_admission` — and is refused
/// identically; and the persisted breaker is observational, so the daemon a
/// `broker restart` leaves behind starts closed and admits again.
#[test]
fn a_socket_resume_meets_the_same_door_and_a_restart_is_the_second_reset_path() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "b.md", "do the thing\n");

    let spawn = |tag: &str, extra: &[(&str, &str)]| {
        let mut cmd = a2a(
            &home,
            tmp.path(),
            &["--json", "spawn", "--lane", "general", "--cwd"],
        );
        cmd.arg(&work)
            .arg("--brief")
            .arg(&brief)
            .env("FAKE_SCENARIO", "hang-turn")
            .env("A2A_ADMISSION_BREAKER_FAILURES", "1");
        for (k, v) in extra {
            cmd.env(k, v);
        }
        run(cmd, &scratch, tag)
    };

    // One failure at a threshold of one: the door shuts.
    let first = spawn("one", &[("A2A_LANES_GENERAL_MAX_TURNS", "0")]);
    assert_eq!(first.code, 0, "spawn: {}", first.stderr);
    until_state(&home, first.data()["id"].as_str().unwrap(), "failed");
    until("the breaker to open", || {
        (health(&home)["breaker"]["open"] == json!(true)).then_some(())
    });

    // A resume off the socket is NEW work, so it meets the breaker in
    // `register`'s capped branch rather than in `decide_admission`.
    let seeded = seed_worker(&home, &work, "general-0819t1200-seed");
    let mut cmd = a2a(&home, tmp.path(), &["--json", "__broker-call", "resume"]);
    cmd.arg(json!({"dir": seeded}).to_string());
    let refused = run(cmd, &scratch, "resume");
    assert_eq!(
        refused.code, 9,
        "a resume with the breaker open: {}",
        refused.stderr
    );
    assert_eq!(
        refused.error()["code"],
        json!("BREAKER_OPEN"),
        "{}",
        refused.stdout
    );
    assert_eq!(
        snap_of(&home, "general-0819t1200-seed").unwrap()["state"],
        json!("done"),
        "a refused resume changes nothing"
    );

    // The second reset path: a fresh daemon starts closed, whatever
    // `broker-health.json` remembers.
    let mut restart = a2a(&home, tmp.path(), &["--json", "broker", "restart"]);
    restart.env("FAKE_SCENARIO", "hang-turn");
    let restarted = run(restart, &scratch, "restart");
    assert_eq!(restarted.code, 0, "broker restart: {}", restarted.stderr);

    let after = spawn("after-restart", &[]);
    assert_eq!(
        after.code, 0,
        "a fresh daemon starts closed: {}",
        after.stderr
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

// ---------------------------------------------------------------------------
// the breaker refuses NEW work only
// ---------------------------------------------------------------------------

/// An open breaker stops the door, not the room: a worker that was already
/// waiting for a slot still gets it when one frees, while a fresh spawn is
/// still refused.
///
/// The construction is deterministic rather than timed. `general` and `review`
/// each have a cap of one and the global cap is two, so: **A** takes the general
/// slot and holds it (`hang-turn` never ends a turn), **B** can only queue, and
/// **C** goes to the review lane — where a zero budget fails it and opens a
/// one-failure breaker. Killing A is then the only thing that can move B, and
/// B moving is the whole claim.
#[test]
fn a_queued_worker_still_drains_while_the_breaker_is_open() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let general_brief = write_brief(tmp.path(), "general.md", "hold the slot\n");
    // `brief::required_keys(Lane::Review)` is `["acceptance_criteria"]` and
    // `spawn` REJECTS rather than warns, so a review brief without it never
    // reaches admission at all.
    let review_brief = write_brief(
        tmp.path(),
        "review.md",
        "---\nlane: review\nacceptance_criteria:\n  - the diff is reviewed\n---\n\nreview the diff\n",
    );

    let caps: &[(&str, &str)] = &[
        ("A2A_ADMISSION_GLOBAL_CAP", "2"),
        ("A2A_ADMISSION_LANES_GENERAL_CAP", "1"),
        ("A2A_ADMISSION_LANES_REVIEW_CAP", "1"),
        ("A2A_ADMISSION_BREAKER_FAILURES", "1"),
    ];
    let spawn_cmd = |lane: &str, brief: &Path, extra: &[(&str, &str)]| {
        let mut cmd = a2a(
            &home,
            tmp.path(),
            &["--json", "spawn", "--lane", lane, "--cwd"],
        );
        cmd.arg(&work)
            .arg("--brief")
            .arg(brief)
            .env("FAKE_SCENARIO", "hang-turn");
        for (k, v) in caps.iter().chain(extra.iter()) {
            cmd.env(k, v);
        }
        cmd
    };

    // A holds the general slot: five turns allowed, one turn opened, held open.
    let ran_a = run(
        spawn_cmd(
            "general",
            &general_brief,
            &[("A2A_LANES_GENERAL_MAX_TURNS", "5")],
        ),
        &scratch,
        "a",
    );
    assert_eq!(ran_a.code, 0, "A: {}", ran_a.stderr);
    let a = ran_a.data()["id"].as_str().unwrap().to_string();
    until_state(&home, &a, "running");

    // B can only wait: the general lane's cap is one, and A has it.
    let ran_b = run(spawn_cmd("general", &general_brief, &[]), &scratch, "b");
    assert_eq!(
        ran_b.code, 0,
        "a queued spawn still SUCCEEDS: {}",
        ran_b.stderr
    );
    let b = ran_b.data()["id"].as_str().unwrap().to_string();
    assert_eq!(ran_b.data()["state"], json!("queued"), "{}", ran_b.stdout);
    assert_eq!(ran_b.data()["queue_position"], json!(1), "{}", ran_b.stdout);

    // C runs in the OTHER lane, fails on its first turn, and opens the breaker.
    let ran_c = run(
        spawn_cmd(
            "review",
            &review_brief,
            &[("A2A_LANES_REVIEW_MAX_TURNS", "0")],
        ),
        &scratch,
        "c",
    );
    assert_eq!(ran_c.code, 0, "C: {}", ran_c.stderr);
    let c = ran_c.data()["id"].as_str().unwrap().to_string();
    let failed = until_state(&home, &c, "failed");
    assert_eq!(
        failed["failure_class"],
        json!("budget_exhausted"),
        "{failed}"
    );
    until("the breaker to open", || {
        (health(&home)["breaker"]["open"] == json!(true)).then_some(())
    });

    // B is still waiting — C's slot was the review lane's, not the general one.
    assert_eq!(
        snap_of(&home, &b).unwrap()["state"],
        json!("queued"),
        "the breaker did not admit B early, and neither did C's failure"
    );

    // Freeing the general slot is what moves B, breaker or no breaker.
    let killed = run(
        a2a(&home, tmp.path(), &["--json", "kill", &a]),
        &scratch,
        "kill-a",
    );
    assert_eq!(killed.code, 0, "kill A: {}", killed.stderr);
    let drained = until("B to leave the queue", || {
        snap_of(&home, &b).filter(|s| s["state"] != json!("queued"))
    });
    assert_ne!(drained["state"], json!("queued"), "{drained}");

    // ... and the door is still shut to anything new.
    let ran_d = run(spawn_cmd("general", &general_brief, &[]), &scratch, "d");
    assert_eq!(
        ran_d.code, 9,
        "a fresh spawn is still refused: {}",
        ran_d.stderr
    );
    assert_eq!(
        ran_d.error()["code"],
        json!("BREAKER_OPEN"),
        "{}",
        ran_d.stdout
    );
    assert_eq!(
        ran_d.error()["data"]["failures"],
        json!(1),
        "{}",
        ran_d.stdout
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}
