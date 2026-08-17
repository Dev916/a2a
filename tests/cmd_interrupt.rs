//! `a2a interrupt <id>` — the command that only the live transport can honour.
//!
//! Interrupting is asynchronous by nature: the turn this command ends was
//! started by the broker's actor, not by the process running the assertion. So
//! every test here goes through the **convergence gate** the plan pins — poll
//! `a2a --json tail <id>` until the worker's WAL carries a `turn_started`
//! envelope (≤10s at 50ms) — before it interrupts anything. An instant
//! assertion on an async turn is a flake with a plausible-looking stack trace.
//!
//! The `hang-turn` scenario is what makes the gate meaningful: the fake holds
//! its turn open forever and completes it only when steered or interrupted, so
//! "the turn was live when we interrupted it" is a fact, not a hope.

mod common;

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The convergence window every poll in this file shares.
const CONVERGE: Duration = Duration::from_secs(10);
/// The gate's poll interval, pinned by the plan.
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
        .env("A2A_SHIM_BIN", a2a_bin());
    c
}

/// What one `a2a` invocation did. stdio goes to FILES rather than pipes: the
/// daemon inherits its parent's descriptors until it redirects them, and a test
/// that blocks reading a pipe to EOF is a test that can hang on its own subject.
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
    let status = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("the a2a binary runs")
        .wait()
        .expect("the invocation ends");
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

fn write_brief(dir: &Path, body: &str) -> PathBuf {
    let p = dir.join("the-brief.md");
    std::fs::write(&p, body).unwrap();
    p
}

fn snap_of(home: &Path, id: &str) -> Option<Value> {
    let text = std::fs::read_to_string(home.join("workers").join(id).join("state.json")).ok()?;
    serde_json::from_str(&text).ok()
}

fn until_state(home: &Path, id: &str, want: &str) -> Value {
    until(&format!("worker {id} to reach {want}"), || {
        snap_of(home, id).filter(|s| s["state"] == json!(want))
    })
}

/// **The convergence gate.** Poll the shipped `a2a --json tail` until the
/// worker's WAL carries a `turn_started` envelope, so a steering command that
/// follows is aimed at a turn that demonstrably exists.
fn until_turn_started(home: &Path, allowed: &Path, scratch: &Path, id: &str) {
    until(&format!("a turn_started envelope for {id}"), || {
        let tailed = run(a2a(home, allowed, &["--json", "tail", id]), scratch, "gate");
        if tailed.code != 0 {
            return None;
        }
        let events = tailed.data()["events"].as_array().cloned().unwrap_or_default();
        events
            .iter()
            .any(|e| e["kind"] == json!("turn_started"))
            .then_some(())
    });
}

fn kinds_of(home: &Path, id: &str) -> Vec<String> {
    std::fs::read_to_string(home.join("workers").join(id).join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|e| e["kind"].as_str().map(str::to_string))
        .collect()
}

/// Spawn one app-server worker on the given scenario and return its id.
fn spawn_app_server(
    home: &Path,
    allowed: &Path,
    cwd: &Path,
    scratch: &Path,
    scenario: &str,
) -> String {
    let brief = write_brief(allowed, "do the thing\n");
    let mut cmd = a2a(
        home,
        allowed,
        &["--json", "spawn", "--lane", "general", "--transport", "app-server", "--cwd"],
    );
    cmd.arg(cwd)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_SCENARIO", scenario);
    let spawned = run(cmd, scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    assert_eq!(spawned.data()["transport"], json!("app-server"));
    spawned.data()["id"].as_str().unwrap().to_string()
}

// ---------------------------------------------------------------------------

/// The whole point of the live transport: a turn that is running right now can
/// be ended, and the worker parks where an operator can steer it.
#[test]
fn interrupt_ends_a_live_turn_and_parks_the_worker_awaiting_input() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);

    let id = spawn_app_server(&home, tmp.path(), &work, &scratch, "hang-turn");
    until_state(&home, &id, "running");
    until_turn_started(&home, tmp.path(), &scratch, &id);

    let out = run(
        a2a(&home, tmp.path(), &["--json", "interrupt", &id]),
        &scratch,
        "interrupt",
    );
    assert_eq!(out.code, 0, "interrupt exits 0: {}", out.stderr);
    let d = out.data();
    assert_eq!(d["noop"], json!(false), "a live turn was really interrupted: {d}");
    assert_eq!(d["state"], json!("awaiting-input"), "{d}");

    let parked = until_state(&home, &id, "awaiting-input");
    assert_eq!(parked["state_reason"], json!("interrupted"), "{parked}");
    assert!(
        kinds_of(&home, &id).contains(&"interrupted".to_string()),
        "the interrupt is a WAL row: {:?}",
        kinds_of(&home, &id)
    );
    assert!(
        snap_of(&home, &id).unwrap().get("fold_rejections").is_none(),
        "the interrupt commit is a legal row"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The recorded deviation from the spec: interrupting a turn that is already
/// over is exit 0 with `data.noop`, not a dedicated failure code. Nothing about
/// the worker changes.
#[test]
fn interrupt_after_the_turn_is_over_is_a_zero_exit_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);

    let id = spawn_app_server(&home, tmp.path(), &work, &scratch, "happy");
    let done = until_state(&home, &id, "done");
    let before = done["epoch"].clone();

    let out = run(
        a2a(&home, tmp.path(), &["--json", "interrupt", &id]),
        &scratch,
        "interrupt",
    );
    assert_eq!(out.code, 0, "a finished turn is not an error: {}", out.stderr);
    let d = out.data();
    assert_eq!(d["noop"], json!(true), "{d}");
    assert_eq!(d["state"], json!("done"), "the worker is untouched: {d}");

    let after = snap_of(&home, &id).unwrap();
    assert_eq!(after["state"], json!("done"));
    assert_eq!(after["epoch"], before, "a noop interrupt bumps nothing");
    assert!(
        !kinds_of(&home, &id).contains(&"interrupted".to_string()),
        "a noop writes no interrupt row: {:?}",
        kinds_of(&home, &id)
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The exec transport has no way into a live turn. That is a **static**
/// precondition — exit 9, decided from the worker's own record, and reached
/// without a broker anywhere in sight.
#[test]
fn interrupt_on_an_exec_worker_is_a_static_precondition() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "spawn", "--lane", "general", "--transport", "exec", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("A2A_CODEX_BIN", common::stub_path());
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    assert_eq!(spawned.data()["transport"], json!("exec"));
    let id = spawned.data()["id"].as_str().unwrap().to_string();

    let out = run(
        a2a(&home, tmp.path(), &["--json", "interrupt", &id]),
        &scratch,
        "interrupt",
    );
    assert_eq!(
        out.code, 9,
        "an unsupported transport is a static precondition, never a reachability failure: {}",
        out.stderr
    );
    assert_eq!(
        out.json()["error"]["code"],
        json!("TRANSPORT_UNSUPPORTED"),
        "{}",
        out.stdout
    );
    assert!(
        out.json()["error"]["remedy"]
            .as_str()
            .unwrap_or_default()
            .contains("a2a kill"),
        "the refusal names what an exec worker CAN be stopped with: {}",
        out.stdout
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// No broker, no interrupt: the turn belongs to a daemon that is not there, and
/// that is exit 7 (reachability) rather than a quiet success.
#[test]
fn interrupt_without_a_broker_is_a_reachability_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);

    let id = spawn_app_server(&home, tmp.path(), &work, &scratch, "hang-turn");
    until_state(&home, &id, "running");
    until_turn_started(&home, tmp.path(), &scratch, &id);

    let stopped = run(
        a2a(&home, tmp.path(), &["broker", "shutdown"]),
        &scratch,
        "shutdown",
    );
    assert_eq!(stopped.code, 0, "shutdown: {}", stopped.stderr);
    until("the socket to be gone", || {
        (!home.join("broker.sock").exists()).then_some(())
    });

    let out = run(
        a2a(&home, tmp.path(), &["--json", "interrupt", &id]),
        &scratch,
        "interrupt",
    );
    assert_eq!(out.code, 7, "a missing broker is exit 7: {}", out.stderr);
    assert_eq!(
        out.json()["error"]["code"],
        json!("BROKER_UNAVAILABLE"),
        "{}",
        out.stdout
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}
