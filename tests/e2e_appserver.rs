// tests/e2e_appserver.rs
//
// End-to-end pipelines over the **live transport**, driven entirely through the
// public CLI — the Phase-2 twin of `tests/e2e_exec.rs`.
//
// Three things are being proved here, and only one of them is "the feature
// works":
//
//   1. **Parity.** An orchestrator that learned `a2a` on the exec transport must
//      not have to learn it again: the same five commands in the same order, the
//      same exit codes, and the same event vocabulary out of `a2a tail`
//      ([`PIPELINE_VOCABULARY`]). Both pipelines assert that one list, which is
//      what makes it a parity pin rather than two independent snapshots.
//   2. **Recovery.** A broker that is `kill -9`d mid-run is not a lost fleet.
//      The round trip — orphan committed by the CLI, adopted by the next daemon,
//      worked to completion — is asserted with no escape hatches: the pid comes
//      from `broker.pid`, the signal is SIGKILL, nothing in the test helps the
//      recovery along (no socket unlinked by hand, no state.json patched).
//   3. **Containment.** Every daemon here is retired by its own
//      [`common::BrokerGuard`], and every stray-process assertion names a pid
//      this test created. A suite that greps `ps` for "any app-server" is a
//      suite that fails when a colleague runs the tests on the same machine.
//
// Everything in the first three sections runs against
// `tests/fixtures/fake-app-server` and `tests/fixtures/stub-codex`: green from a
// clean checkout, no real codex, no network, no tokens.
//
// The last section is different in kind. It is the **tier-5 proof** (spec §8):
// `#[ignore]`d *and* `A2A_E2E=1`-gated, it runs the real `codex` on `PATH`
// against the real `~/.codex` auth (which it READS and never writes under), and
// it is the only thing in the repo that can catch app-server *vocabulary drift*
// — a fake can only ever confirm the vocabulary we wrote into it.

mod common;

use serde_json::{json, Value};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The convergence window every poll in this file shares — the same one
/// `tests/cmd_broker.rs` uses, for the same reason: nothing here asserts how
/// fast a daemon comes up, only the state it must reach.
const CONVERGE: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(100);

/// **The parity pin.** The lifecycle vocabulary a completed worker's `a2a tail`
/// reports, distinct kinds in first-appearance order, on BOTH transports.
///
/// Distinct-and-ordered rather than the raw sequence on purpose: the two
/// transports legitimately differ in how many times a kind repeats (the
/// app-server learns its thread from a `thread/started` notification *and* from
/// its own `SessionIdentified` row; the exec transport re-states the session at
/// ingest), and pinning multiplicities would make this a test of plumbing
/// trivia. What an orchestrator actually consumes is *which* events arrive and
/// *in what order* — spawned, transport attached, session known, turn opened,
/// the worker's own words, turn closed — and that must be one contract, not two.
const PIPELINE_VOCABULARY: [&str; 6] = [
    "spawned",
    "state_change",
    "session_identified",
    "turn_started",
    "agent_message",
    "turn_completed",
];

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

fn a2a_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("a2a")
}

/// `a2a --home <home> …` on the **live transport**: the scripted app-server as
/// the codex binary, no `A2A_BROKER_DISABLE`, and a `$FAKE_STATE` that outlives
/// any one child (the thread map a `thread/resume` is answered from).
fn broker_cli(env: &Env, args: &[&str]) -> Command {
    let mut c = Command::new(a2a_bin());
    c.arg("--home")
        .arg(&env.home)
        .args(args)
        .env_remove("A2A_HOME")
        .env_remove("A2A_BROKER_DISABLE")
        .env("A2A_CODEX_BIN", common::fake_app_server_path())
        .env("A2A_ALLOWED_ROOTS", &env.allowed)
        .env("A2A_OWNER", common::TEST_OWNER)
        .env("A2A_BROKER_BIN", a2a_bin())
        .env("A2A_SHIM_BIN", a2a_bin())
        .env("FAKE_STATE", &env.fake_state);
    c
}

/// `a2a --home <home> …` on the **exec transport**: the Phase-1 stub, and
/// `A2A_BROKER_DISABLE=1` so the CLI opens no socket at all.
fn exec_cli(env: &Env, args: &[&str]) -> Command {
    let mut c = Command::new(a2a_bin());
    c.arg("--home")
        .arg(&env.home)
        .args(args)
        .env_remove("A2A_HOME")
        .env("A2A_CODEX_BIN", common::stub_path())
        .env("A2A_ALLOWED_ROOTS", &env.allowed)
        .env("A2A_OWNER", common::TEST_OWNER)
        .env("A2A_BROKER_DISABLE", "1")
        .env("A2A_SHIM_BIN", a2a_bin());
    c
}

/// One test's directories. `_tmp` is held so the tree outlives every borrow of
/// it; dropping it is what removes the whole thing.
struct Env {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    allowed: PathBuf,
    work: PathBuf,
    scratch: PathBuf,
    fake_state: PathBuf,
}

impl Env {
    fn new() -> Env {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        let env = Env {
            home: root.join("home"),
            allowed: root.clone(),
            work: root.join("work"),
            scratch: root.join("scratch"),
            fake_state: root.join("fake-state"),
            _tmp: tmp,
        };
        for dir in [&env.work, &env.scratch, &env.fake_state] {
            std::fs::create_dir_all(dir).unwrap();
        }
        env
    }

    fn brief(&self, body: &str) -> PathBuf {
        let p = self.allowed.join("the-brief.md");
        std::fs::write(&p, body).unwrap();
        p
    }
}

/// What one `a2a` invocation did. stdio goes to FILES rather than pipes: a
/// detached daemon inherits its launcher's descriptors until it redirects them,
/// and a test that reads such a pipe to EOF can hang on its own subject.
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
    let mut child: Child = cmd
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

fn until<T>(what: &str, f: impl FnMut() -> Option<T>) -> T {
    until_within(CONVERGE, what, f)
}

/// [`until`] with an explicit window — the real-codex proofs wait on a model
/// rather than on a daemon, and [`CONVERGE`] is sized for the latter.
fn until_within<T>(window: Duration, what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + window;
    loop {
        if let Some(v) = f() {
            return v;
        }
        if Instant::now() >= deadline {
            panic!("timed out after {window:?} waiting for {what}");
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

fn is_socket(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.file_type().is_socket())
        .unwrap_or(false)
}

fn read_pidfile(home: &Path) -> Option<u32> {
    std::fs::read_to_string(home.join("broker.pid"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// A *socket*, not merely a path that exists: the pidfile is written before the
/// listener is bound, and a crashed predecessor's stale regular file also
/// "exists".
///
/// `previous` is the pid this home is being handed over FROM, if any. A
/// SIGKILLed daemon leaves both its socket and its pidfile behind, so "a socket
/// exists and a pidfile parses" is satisfied by the corpse — the successor is
/// only observable as a *different, live* pid.
fn wait_for_broker(home: &Path, previous: Option<u32>) -> u32 {
    until("the broker to publish its socket and pidfile", || {
        if !is_socket(&home.join("broker.sock")) {
            return None;
        }
        read_pidfile(home).filter(|pid| Some(*pid) != previous && alive(*pid))
    })
}

/// Is *this* pid alive? Scoped by construction — the caller always passes a pid
/// this test created, never a pattern that could match somebody else's daemon.
fn alive(pid: u32) -> bool {
    let out = Command::new("ps")
        .args(["-o", "pid=", "-p", &pid.to_string()])
        .output()
        .expect("ps runs");
    out.status.success() && !String::from_utf8_lossy(&out.stdout).trim().is_empty()
}

fn events_of(home: &Path, id: &str) -> Vec<Value> {
    std::fs::read_to_string(home.join("workers").join(id).join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

/// The distinct lifecycle kinds `a2a tail` reports, in first-appearance order —
/// read back through the SHIPPED command, not off the file, because the
/// projection is part of the contract being pinned.
fn tail_vocabulary(ran: &Ran) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    for event in ran.data()["events"].as_array().expect("tail lists events") {
        let kind = event["kind"].as_str().expect("every event has a kind");
        if !seen.iter().any(|k| k == kind) {
            seen.push(kind.to_string());
        }
    }
    seen
}

// ---------------------------------------------------------------------------
// the happy pipeline, on the live transport
// ---------------------------------------------------------------------------

/// Everything an orchestrator does with a worker it never has to talk to:
/// spawn → wait → status → result → tail → gc.
///
/// The chain is the assertion. The id `spawn` printed is the id `wait` blocks
/// on, is the id `status` calls `done`, is the id whose `result` carries the
/// worker's own words, is the id whose tail speaks [`PIPELINE_VOCABULARY`], is
/// the id `gc` retires — and the thread the broker opened survives all of it,
/// down into the tombstone, which is the only place that id still exists once
/// the directory is gone.
#[test]
fn broker_happy_pipeline_spawn_wait_status_result_tail_gc() {
    let env = Env::new();
    let guard = common::BrokerGuard::new(&env.home);
    let brief = env.brief("summarize the repository\n");

    let mut cmd = broker_cli(
        &env,
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&env.work)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_MESSAGE", "A2A-APPSERVER-OK");
    let spawned = run(cmd, &env.scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let d = spawned.data();
    assert_eq!(
        d["transport"], json!("app-server"),
        "`--transport auto` with a reachable broker is the live transport: {d}"
    );
    let id = d["id"].as_str().expect("an id").to_string();
    assert!(id.starts_with("general-"), "id carries its lane: {id}");
    assert!(
        is_socket(&env.home.join("broker.sock")),
        "nobody ran `broker run` — the CLI autostarted the daemon it needed"
    );

    // wait → exit 0 (the state reached IS the exit code)
    let waited = run(broker_cli(&env, &["wait", &id]), &env.scratch, "wait");
    assert_eq!(waited.code, 0, "wait: {}", waited.stderr);

    // status → done, exit 0
    let status = run(
        broker_cli(&env, &["--json", "status", &id]),
        &env.scratch,
        "status",
    );
    assert_eq!(status.code, 0, "status: {}", status.stderr);
    let s = status.data();
    assert_eq!(s["state"], json!("done"), "{s}");
    let thread_id = s["thread_id"].as_str().expect("a thread of record").to_string();
    assert_eq!(
        s["session_id"], json!(thread_id),
        "thread.id IS the session id on this transport: {s}"
    );
    assert!(
        s.get("fold_rejections").is_none(),
        "a happy pipeline folds cleanly: {s}"
    );

    // result → the worker's final agent message
    let result = run(
        broker_cli(&env, &["--json", "result", &id]),
        &env.scratch,
        "result",
    );
    assert_eq!(result.code, 0, "result: {}", result.stderr);
    assert_eq!(
        result.data()["result"], json!("A2A-APPSERVER-OK"),
        "the final agent message IS the result on this transport"
    );

    // tail → the parity vocabulary
    let tailed = run(
        broker_cli(&env, &["--json", "tail", &id]),
        &env.scratch,
        "tail",
    );
    assert_eq!(tailed.code, 0, "tail: {}", tailed.stderr);
    assert_eq!(
        tail_vocabulary(&tailed),
        PIPELINE_VOCABULARY,
        "the live transport's tail vocabulary drifted from the parity pin"
    );

    // gc → the directory goes, the thread id does not
    let gc = run(
        broker_cli(&env, &["--json", "gc", "--age", "0s"]),
        &env.scratch,
        "gc",
    );
    assert_eq!(gc.code, 0, "gc: {}", gc.stderr);
    assert_eq!(gc.data()["removed"], json!([id.as_str()]), "{}", gc.stdout);
    assert!(
        !env.home.join("workers").join(&id).exists(),
        "a reclaimed worker's directory is gone"
    );
    let index = std::fs::read_to_string(env.home.join("index.jsonl")).unwrap();
    let tombstone: Value = serde_json::from_str(index.lines().next().expect("one tombstone")).unwrap();
    assert_eq!(tombstone["id"], json!(id.as_str()), "{tombstone}");
    assert_eq!(
        tombstone["session_id"], json!(thread_id.as_str()),
        "the tombstone is the last place the thread id exists — losing it would \
         strand the conversation on the codex side: {tombstone}"
    );
    assert_eq!(tombstone["state"], json!("done"), "{tombstone}");

    drop(guard);
    common::assert_no_broker_left(&env.home);
}

/// The exec fallback, end to end, and the other half of the parity pin.
///
/// `A2A_BROKER_DISABLE=1` is the escape hatch Phase 1 keeps: the same `spawn`
/// with no `--transport` resolves to `exec`, opens no socket, and produces a
/// worker whose tail speaks the SAME vocabulary as the live transport's. That
/// last equality is the promise a2a makes to an orchestrator — the transport is
/// an implementation detail of how the worker ran, not a second API.
#[test]
fn exec_fallback_pipeline_is_green_and_speaks_the_same_vocabulary() {
    let env = Env::new();
    let brief = env.brief("summarize the repository\n");

    let mut cmd = exec_cli(&env, &["--json", "spawn", "--lane", "general", "--cwd"]);
    cmd.arg(&env.work)
        .arg("--brief")
        .arg(&brief)
        .env("STUB_LAST_MSG", "A2A-EXEC-OK");
    let spawned = run(cmd, &env.scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let d = spawned.data();
    assert_eq!(d["transport"], json!("exec"), "the disable seam wins: {d}");
    let id = d["id"].as_str().expect("an id").to_string();
    assert!(
        !env.home.join("broker.sock").exists(),
        "a disabled spawn opens no socket at all"
    );

    let waited = run(exec_cli(&env, &["wait", &id]), &env.scratch, "wait");
    assert_eq!(waited.code, 0, "wait: {}", waited.stderr);

    let status = run(
        exec_cli(&env, &["--json", "status", &id]),
        &env.scratch,
        "status",
    );
    assert_eq!(status.code, 0, "status: {}", status.stderr);
    let s = status.data();
    assert_eq!(s["state"], json!("done"), "{s}");
    assert_eq!(s["requested_transport"], json!("exec"), "{s}");
    // `transport_selection` is genesis bookkeeping rather than a status field,
    // so it is read where it lives: `auto` is what makes this the FALLBACK
    // rather than an operator who asked for exec by name.
    let snap = snap_of(&env.home, &id).expect("a snapshot");
    assert_eq!(snap["transport_selection"], json!("auto"), "{snap}");

    let result = run(
        exec_cli(&env, &["--json", "result", &id]),
        &env.scratch,
        "result",
    );
    assert_eq!(result.code, 0, "result: {}", result.stderr);
    assert!(
        result.data()["result"]
            .as_str()
            .is_some_and(|r| r.contains("A2A-EXEC-OK")),
        "result was: {}",
        result.stdout
    );

    let tailed = run(exec_cli(&env, &["--json", "tail", &id]), &env.scratch, "tail");
    assert_eq!(tailed.code, 0, "tail: {}", tailed.stderr);
    assert_eq!(
        tail_vocabulary(&tailed),
        PIPELINE_VOCABULARY,
        "the exec transport's tail vocabulary drifted from the parity pin"
    );

    // Nothing here ever needed a daemon, so nothing may have left one behind.
    common::assert_no_broker_left(&env.home);
}

// ---------------------------------------------------------------------------
// the blocked pipeline
// ---------------------------------------------------------------------------

/// The steering loop on the live transport: a worker that asks a question parks,
/// the orchestrator is told **by an exit code** rather than by parsing prose,
/// answers, and the same worker converges.
///
/// `wait --until blocked` exiting 5 is the point — it is what lets an
/// orchestrator sleep on N workers and be woken only by the one that needs a
/// human. The research lane is used because it is outbox-only: its thread is
/// rooted on the worker directory, which is where the fake writes the question
/// (env cannot be per-worker on a child every worker shares).
#[test]
fn broker_blocked_pipeline_parks_then_the_answer_converges_it() {
    let env = Env::new();
    let guard = common::BrokerGuard::new(&env.home);
    let brief = env.brief("pick an option, ask if unsure\n");

    let mut cmd = broker_cli(&env, &["--json", "spawn", "--lane", "research", "--brief"]);
    cmd.arg(&brief)
        .env("FAKE_SCENARIO", "blocked")
        .env("FAKE_MESSAGE", "A2A-ANSWERED-B");
    let spawned = run(cmd, &env.scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().expect("an id").to_string();

    // wait --until blocked → exit 5
    let blocked = run(
        broker_cli(&env, &["wait", &id, "--until", "blocked"]),
        &env.scratch,
        "blocked",
    );
    assert_eq!(blocked.code, 5, "blocked is exit 5: {}", blocked.stderr);

    // the question is readable before it is answered
    assert!(
        env.home
            .join("workers")
            .join(&id)
            .join("outbox/1.question.json")
            .exists(),
        "the BLOCKED file is the authority"
    );
    let parked = snap_of(&env.home, &id).expect("a snapshot");
    assert_eq!(parked["state"], json!("awaiting-input"), "{parked}");
    assert_eq!(parked["state_reason"], json!("blocked"), "{parked}");

    // answer → the answer opens a turn (there is none to steer at a boundary)
    let sent = run(
        broker_cli(&env, &["--json", "send", &id, "use option B"]),
        &env.scratch,
        "send",
    );
    assert_eq!(sent.code, 0, "send: {}", sent.stderr);
    assert_eq!(sent.data()["delivered"], json!(true), "{}", sent.stdout);
    assert_eq!(
        sent.data()["send_path"], json!("started"),
        "off a turn boundary there is nothing to steer: {}",
        sent.stdout
    );

    // wait → exit 0
    let waited = run(broker_cli(&env, &["wait", &id]), &env.scratch, "wait");
    assert_eq!(waited.code, 0, "wait: {}", waited.stderr);
    let done = until_state(&env.home, &id, "done");
    assert!(
        done.get("fold_rejections").is_none(),
        "the answered round trip folds cleanly: {done}"
    );

    let result = run(
        broker_cli(&env, &["--json", "result", &id]),
        &env.scratch,
        "result",
    );
    assert_eq!(result.code, 0, "result: {}", result.stderr);
    assert_eq!(
        result.data()["result"], json!("A2A-ANSWERED-B"),
        "an answered worker's result is the answer turn's own message"
    );

    drop(guard);
    common::assert_no_broker_left(&env.home);
}

// ---------------------------------------------------------------------------
// crash recovery
// ---------------------------------------------------------------------------

/// **The recovery round trip, with no escape hatches.**
///
/// A worker is mid-turn when its daemon is `kill -9`d — the pid read from
/// `broker.pid`, the signal uncatchable, so nothing ran on the way out: no
/// graceful flush, no socket unlinked, no state written. Then, in order:
///
/// * `a2a status` is the only thing that can say what the broker cannot say
///   about itself — that it is gone — and commits `orphaned` (exit 10).
/// * `broker run` reaps the previous instance's app-server child by the pid and
///   start-witness the worker remembers, then re-opens the thread
///   (`thread/resume`) and adopts the worker.
/// * A successful re-attach is a **`Resume`**: the worker is `running` again,
///   under a live daemon, on the same conversation and a fresh epoch. That is
///   the whole reason `orphaned` is non-terminal and in the adoption set.
/// * And it finishes the job: a `send` opens the next turn and the worker lands
///   `done` with a result.
///
/// The test helps with none of it. Every assertion below is a converged poll on
/// state the daemon reached by itself.
#[test]
fn a_kill_dash_nine_broker_is_recovered_by_the_next_one() {
    let env = Env::new();
    let guard = common::BrokerGuard::new(&env.home);
    let brief = env.brief("do the thing\n");

    // `hang-turn` holds its turn open forever, so "mid-run" is a state rather
    // than a race with a fixture that finishes in milliseconds.
    let mut cmd = broker_cli(&env, &["--json", "spawn", "--lane", "general", "--cwd"]);
    cmd.arg(&env.work)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_SCENARIO", "hang-turn");
    let spawned = run(cmd, &env.scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().expect("an id").to_string();
    assert_eq!(spawned.data()["transport"], json!("app-server"));
    // `running` and a thread of record are two facts, written by two events —
    // waiting only for the first would race the `SessionIdentified` row that
    // makes the worker resumable at all.
    let running = until("the worker to be running on a known thread", || {
        snap_of(&env.home, &id).filter(|s| {
            s["state"] == json!("running") && s["session_id"].is_string() && s["pid"].is_u64()
        })
    });
    let thread_id = running["session_id"].as_str().unwrap().to_string();
    let child_pid = running["pid"].as_u64().unwrap() as u32;

    // The daemon's own pid, from its own pidfile — the ops path, not a `ps` grep.
    let broker_pid = wait_for_broker(&env.home, None);
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(broker_pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    until("the broker to be gone", || (!alive(broker_pid)).then_some(()));

    // A crashed daemon leaves its address behind and nothing removes it here:
    // the next `broker run` must unlink it after a failed connect, and `status`
    // must read a failed handshake as "no broker" rather than as an error.
    assert!(
        env.home.join("broker.sock").exists(),
        "a SIGKILLed daemon cannot have cleaned up after itself"
    );

    let orphan = run(
        broker_cli(&env, &["--json", "status", &id]),
        &env.scratch,
        "orphan",
    );
    assert_eq!(
        orphan.code, 10,
        "a live worker with no broker is orphaned: {}",
        orphan.stderr
    );
    assert_eq!(orphan.data()["state"], json!("orphaned"), "{}", orphan.stdout);

    // The next daemon. No `FAKE_SCENARIO`, so this generation's model answers
    // instead of hanging — the same conversation, resumed, with a server that
    // now finishes what it is asked.
    let restarted = run(broker_cli(&env, &["broker", "run"]), &env.scratch, "restart");
    assert_eq!(restarted.code, 0, "broker run: {}", restarted.stderr);
    let new_pid = wait_for_broker(&env.home, Some(broker_pid));
    assert_ne!(new_pid, broker_pid, "a new daemon, not the corpse");

    let status = run(
        broker_cli(&env, &["--json", "broker", "status"]),
        &env.scratch,
        "brokerstatus",
    );
    assert_eq!(status.code, 0, "broker status: {}", status.stderr);
    let reconciled = status.data()["reconciled"].clone();
    assert_eq!(
        reconciled["adopted"], json!([id.as_str()]),
        "the orphan is adopted by the next daemon: {reconciled}"
    );
    // Scoped stray check: the ONE child this test's first daemon owned, named by
    // the pid its own worker recorded — never a scan for other people's fakes.
    assert_eq!(
        reconciled["reaped"][0]["pid"], json!(child_pid),
        "the reap names the previous instance's child: {reconciled}"
    );
    assert!(!alive(child_pid), "the previous app-server child is gone");

    // Adoption is asynchronous, so this is a converged poll and not an instant
    // assertion: the worker is BACK IN `running`, under the new daemon.
    let readopted = until_state(&env.home, &id, "running");
    assert_eq!(
        readopted["session_id"], json!(thread_id.as_str()),
        "the thread survived the crash: {readopted}"
    );
    assert_eq!(
        readopted["epoch"], json!(2),
        "a resume is a new epoch on an old conversation: {readopted}"
    );
    assert!(
        readopted.get("fold_rejections").is_none(),
        "every row of the recovery is a legal transition: {readopted}"
    );
    let kinds: Vec<String> = events_of(&env.home, &id)
        .iter()
        .filter_map(|e| e["kind"].as_str().map(str::to_string))
        .collect();
    assert!(
        kinds.iter().any(|k| k == "orphaned") && kinds.iter().any(|k| k == "resumed"),
        "the crash and the recovery are both in the WAL: {kinds:?}"
    );

    // ... and it finishes the job.
    let sent = run(
        broker_cli(&env, &["--json", "send", &id, "wrap it up"]),
        &env.scratch,
        "send",
    );
    assert_eq!(sent.code, 0, "send to a recovered worker: {}", sent.stderr);
    let waited = run(broker_cli(&env, &["wait", &id]), &env.scratch, "wait");
    assert_eq!(waited.code, 0, "wait: {}", waited.stderr);
    let done = until_state(&env.home, &id, "done");
    assert_eq!(done["state"], json!("done"), "{done}");

    let result = run(
        broker_cli(&env, &["--json", "result", &id]),
        &env.scratch,
        "result",
    );
    assert_eq!(result.code, 0, "result: {}", result.stderr);
    assert!(
        result.data()["result"].as_str().is_some_and(|r| !r.is_empty()),
        "a recovered worker produces a real result: {}",
        result.stdout
    );

    drop(guard);
    common::assert_no_broker_left(&env.home);
    assert!(!alive(new_pid), "the second daemon is gone too");
}

// ---------------------------------------------------------------------------
// tier 5: the real-codex proofs (gated)
// ---------------------------------------------------------------------------
//
// Three separate claims, each of which only the real binary can settle:
//
//   1. **The live transport works end to end against real codex**, and the
//      notification methods `events::map_appserver_notification` branches on are
//      the ones a real server actually sends (the drift gate).
//   2. **A worker is not stranded by its transport.** A thread opened by the
//      broker is resumable by `codex exec resume` after the broker is gone —
//      `thread.id == thread.sessionId` is not a curiosity, it is what makes the
//      exec fallback a fallback rather than a different product.
//   3. **`turn/interrupt` really ends a turn**, landing the pinned parked state.
//
// Plus one zero-token probe of the real error vocabulary behind
// `adapter::is_steer_precondition_failure`.
//
// Every one of them is `#[ignore]`d AND re-checks `A2A_E2E=1` at runtime:
// `--ignored` alone would spend tokens the moment somebody ran the suite with
// `--include-ignored`.

/// The runtime half of the gate.
fn gated(what: &str) -> bool {
    if std::env::var("A2A_E2E").as_deref() != Ok("1") {
        eprintln!("A2A_E2E != 1 — skipping {what}");
        return false;
    }
    true
}

/// `a2a --home <home> …` against the **real** codex on `PATH` and a **real**
/// broker.
///
/// `A2A_CODEX_BIN` is *removed* rather than overridden — PATH's codex is the
/// subject. `A2A_BROKER_BIN`/`A2A_SHIM_BIN` are pinned to the built binary for
/// the same reason `tests/common` pins them: under the harness `current_exe()`
/// is the test binary, and a test binary told to `broker run` is a hang with a
/// confusing name.
fn real_cli(env: &Env, args: &[&str]) -> Command {
    let mut c = Command::new(a2a_bin());
    c.arg("--home")
        .arg(&env.home)
        .args(args)
        .env_remove("A2A_HOME")
        .env_remove("A2A_CODEX_BIN")
        .env_remove("A2A_BROKER_DISABLE")
        .env("A2A_ALLOWED_ROOTS", &env.allowed)
        .env("A2A_OWNER", common::TEST_OWNER)
        .env("A2A_BROKER_BIN", a2a_bin())
        .env("A2A_SHIM_BIN", a2a_bin());
    c
}

/// The same real codex, on the **exec** transport: the broker switched off
/// entirely, so the only way this can reach the conversation is
/// `codex exec resume <session_id>`.
fn real_exec_cli(env: &Env, args: &[&str]) -> Command {
    let mut c = real_cli(env, args);
    c.env("A2A_BROKER_DISABLE", "1");
    c
}

/// `codex` as `PATH` resolves it — the same binary the broker will spawn.
fn real_codex_bin() -> PathBuf {
    let out = Command::new("sh")
        .args(["-c", "command -v codex"])
        .output()
        .expect("sh runs");
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(!path.is_empty(), "no `codex` on PATH — the gated smoke needs one");
    PathBuf::from(path)
}

/// A git repo under the allowed root: a real worker gets a real workspace.
fn git_repo(at: &Path) -> PathBuf {
    std::fs::create_dir_all(at).unwrap();
    assert!(Command::new("git")
        .args(["init", "-q"])
        .current_dir(at)
        .status()
        .expect("git runs")
        .success());
    at.to_path_buf()
}

/// An item type as [`crate::events`] normalizes it: lowercased, `_` stripped, so
/// `mcpToolCall` and `mcp_tool_call` are the same word.
fn norm(v: &Value) -> String {
    v.as_str()
        .unwrap_or_default()
        .chars()
        .filter(|c| *c != '_')
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// The census printed under `--nocapture`, so a mapping regression is
/// diagnosable from the test output alone: every envelope, its kind, and the
/// top-level keys of the notification params it was mapped from.
fn print_census(home: &Path, id: &str) {
    eprintln!("observed app-server vocabulary for {id}:");
    for e in events_of(home, id) {
        let mut keys: Vec<&str> = e["raw"]
            .as_object()
            .map(|o| o.keys().map(String::as_str).collect())
            .unwrap_or_default();
        keys.sort_unstable();
        eprintln!(
            "  seq {:>3}  epoch {}  {:<20} {:<10} raw{:?}",
            e["seq"],
            e["epoch"],
            e["kind"].as_str().unwrap_or("?"),
            e["transport"].as_str().unwrap_or("?"),
            keys
        );
    }
}

/// Everything a failing real run needs and a tempdir is about to destroy: the
/// child's stderr and each epoch's raw transport stream.
fn dump_worker(home: &Path, id: &str) {
    let dir = home.join("workers").join(id);
    eprintln!("---- worker dir {} ----", dir.display());
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    eprintln!("  files: {names:?}");
    for name in names
        .iter()
        .filter(|n| n.starts_with("exec.") || *n == "stderr.log" || *n == "state.json")
    {
        let body = std::fs::read_to_string(dir.join(name)).unwrap_or_default();
        let tail: Vec<&str> = body.lines().rev().take(25).collect();
        eprintln!("---- {name} (last {} lines) ----", tail.len());
        for line in tail.into_iter().rev() {
            eprintln!("  {line}");
        }
    }
}

/// **The real smoke, and the cross-transport resume proof it makes possible.**
///
/// One worker, two transports, in the order an operator would hit them:
///
/// * `--transport app-server` explicitly (not `auto`): this test is about the
///   live transport, so a silent degradation to exec must fail it rather than
///   quietly produce a green run that proves nothing.
/// * The **drift gate** asserts the load-bearing mappings *fired from real
///   notifications* — the params shapes `events::map_appserver_notification`
///   branches on (`thread/started` → `{thread:{id}}`, `turn/started` →
///   `{threadId,turn}`, `item/completed` → `{item:{type:"agentMessage"}}`,
///   `turn/completed` → `{turn:{status}}`), not merely that a kind exists. A
///   kind can be synthesized by a2a itself; a params shape cannot.
/// * Then `broker shutdown`, and `a2a resume` with the broker DISABLED. The
///   thread the broker opened is handed to `codex exec resume <session_id>` and
///   must continue the same conversation. That is the whole cross-transport
///   claim, and it is why `thread_id == session_id` is asserted above rather
///   than assumed.
///
/// The gate is deliberately NOT "no `unknown` events": unknown is the designed
/// landing spot for a notification a newer codex invented, so failing on it
/// would break this test on additions that are explicitly fine.
#[test]
#[ignore = "real codex + tokens; run with A2A_E2E=1 cargo test --test e2e_appserver -- --ignored"]
fn real_codex_smoke_then_cross_transport_resume() {
    if !gated("the real app-server smoke") {
        return;
    }
    let env = Env::new();
    let guard = common::BrokerGuard::new(&env.home);
    let repo = git_repo(&env.work);
    let brief = env.brief("Reply with exactly the text: A2A-P2-OK\n");

    // ---- the live transport, on the real binary -------------------------
    let mut cmd = real_cli(
        &env,
        &[
            "--json",
            "spawn",
            "--transport",
            "app-server",
            "--lane",
            "general",
            "--cwd",
        ],
    );
    cmd.arg(&repo).arg("--brief").arg(&brief);
    let spawned = run(cmd, &env.scratch, "real-spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let d = spawned.data();
    assert_eq!(
        d["transport"],
        json!("app-server"),
        "`--transport app-server` is an ask, not a preference: {d}"
    );
    let id = d["id"].as_str().expect("an id").to_string();
    eprintln!("real app-server worker: {id}");

    let waited = run(
        real_cli(&env, &["wait", &id, "--timeout", "300s"]),
        &env.scratch,
        "real-wait",
    );
    assert_eq!(
        waited.code, 0,
        "wait: {} (worker {id})",
        waited.stderr
    );

    let status = run(
        real_cli(&env, &["--json", "status", &id]),
        &env.scratch,
        "real-status",
    );
    assert_eq!(status.code, 0, "status: {}", status.stderr);
    let s = status.data();
    assert_eq!(s["state"], json!("done"), "{s}");
    let thread_id = s["thread_id"]
        .as_str()
        .expect("a real thread of record")
        .to_string();
    let session_id = s["session_id"]
        .as_str()
        .expect("a real session of record")
        .to_string();
    assert!(!thread_id.is_empty(), "{s}");
    assert_eq!(
        thread_id, session_id,
        "the ground-truth identity `thread.id == thread.sessionId` no longer holds \
         on the real binary — the exec fallback resumes by session id, so this is \
         the load-bearing fact behind the cross-transport proof below: {s}"
    );
    assert!(
        s.get("fold_rejections").is_none(),
        "a real happy run folds cleanly: {s}"
    );

    let result = run(
        real_cli(&env, &["--json", "result", &id]),
        &env.scratch,
        "real-result",
    );
    assert_eq!(result.code, 0, "result: {}", result.stderr);
    let text = result.data()["result"].as_str().unwrap_or_default().to_string();
    eprintln!("app-server result: {text:?}");
    assert!(
        text.contains("A2A-P2-OK"),
        "the worker's own words are the result on this transport; got {text:?}"
    );

    // ---- the broker is healthy while it is holding this fleet -----------
    let broker = run(
        real_cli(&env, &["--json", "broker", "status"]),
        &env.scratch,
        "real-brokerstatus",
    );
    assert_eq!(broker.code, 0, "broker status: {}", broker.stderr);
    let b = broker.data();
    assert_eq!(b["running"], json!(true), "{b}");
    assert_eq!(b["transport"], json!("app-server"), "{b}");
    assert!(b["pid"].is_u64(), "a daemon reports its own pid: {b}");
    assert!(
        b["child_pid"].is_u64(),
        "a healthy broker owns a live app-server child: {b}"
    );

    // ---- the drift gate --------------------------------------------------
    print_census(&env.home, &id);
    let evs = events_of(&env.home, &id);
    // `thread/started` params are `{thread:{id,…}}` — NO top-level threadId.
    // Observed on codex-cli 0.140.0: raw keys ["fsm","rollout_path",
    // "session_id","thread"].
    assert!(
        evs.iter()
            .any(|e| e["kind"] == "session_identified" && e["raw"]["thread"]["id"].is_string()),
        "no `thread/started` notification reached the fold — \
         map_appserver_notification has drifted"
    );
    // `turn/started` = `{threadId, turn}`.
    assert!(
        evs.iter().any(|e| e["kind"] == "turn_started"
            && e["raw"]["threadId"].is_string()
            && e["raw"]["turn"]["id"].is_string()),
        "no `turn/started` notification reached the fold — \
         map_appserver_notification has drifted"
    );
    // `item/completed` carrying an `agentMessage` item is what becomes the
    // result. None of the three TOOL discriminants contains the lowercase
    // substring "tool", which is why the mapping normalizes instead of
    // `contains("tool")`-ing; the same normalization is applied here.
    assert!(
        evs.iter()
            .any(|e| e["kind"] == "agent_message" && norm(&e["raw"]["item"]["type"]) == "agentmessage"),
        "no `item/completed{{item.type:agentMessage}}` reached the fold — \
         map_appserver_notification has drifted"
    );
    // `turn/completed` = `{threadId, turn}` with `TurnStatus`.
    assert!(
        evs.iter()
            .any(|e| e["kind"] == "turn_completed" && e["raw"]["turn"]["status"] == json!("completed")),
        "no `turn/completed{{turn.status:completed}}` reached the fold — \
         map_appserver_notification has drifted"
    );

    // ---- cross-transport resume -----------------------------------------
    // The daemon goes away FIRST and by the front door: a clean shutdown is the
    // case an operator actually hits, and it is the one that makes the next line
    // a genuine exec-transport run rather than a broker round trip in disguise.
    let stopped = run(
        real_cli(&env, &["--json", "broker", "shutdown"]),
        &env.scratch,
        "real-shutdown",
    );
    assert_eq!(stopped.code, 0, "broker shutdown: {}", stopped.stderr);
    common::assert_no_broker_left(&env.home);

    let resumed = run(
        real_exec_cli(
            &env,
            &["--json", "resume", &id, "now reply with exactly: A2A-XT-OK"],
        ),
        &env.scratch,
        "real-resume",
    );
    assert_eq!(
        resumed.code, 0,
        "`codex exec resume <session_id>` refused the thread the broker opened. \
         If this is an identity divergence rather than a transient failure, the \
         Phase-3 fix is the recorded `rollout_path` fallback (snapshot field \
         `rollout_path`, resume by rollout file rather than by session id) — \
         document it, do NOT paper over it. stderr: {}",
        resumed.stderr
    );
    let r = resumed.data();
    assert_eq!(
        r["session_id"],
        json!(session_id.as_str()),
        "a resume is a new epoch on the SAME conversation: {r}"
    );
    assert_eq!(r["epoch"], json!(2), "{r}");

    let waited = run(
        real_exec_cli(&env, &["wait", &id, "--timeout", "300s"]),
        &env.scratch,
        "real-xt-wait",
    );
    if waited.code != 0 {
        // The evidence lives in the worker dir, and the worker dir lives in a
        // tempdir that this panic is about to delete. Print it first: a
        // resumed child that dies instantly says why on its own stderr.
        dump_worker(&env.home, &id);
    }
    assert_eq!(waited.code, 0, "wait after resume: {}", waited.stderr);

    let result = run(
        real_exec_cli(&env, &["--json", "result", &id]),
        &env.scratch,
        "real-xt-result",
    );
    assert_eq!(result.code, 0, "result after resume: {}", result.stderr);
    let text = result.data()["result"].as_str().unwrap_or_default().to_string();
    eprintln!("cross-transport result: {text:?}");
    assert!(
        text.contains("A2A-XT-OK"),
        "the resumed turn ran, but not on this conversation; got {text:?}"
    );

    // …and it really was the exec transport: epoch 2 has its own `codex exec`
    // stream file, and every envelope folded from it is stamped `exec`.
    print_census(&env.home, &id);
    assert!(
        env.home
            .join("workers")
            .join(&id)
            .join("exec.2.jsonl")
            .exists(),
        "the resumed epoch is a `codex exec` run with its own stream"
    );
    assert!(
        events_of(&env.home, &id).iter().any(|e| e["epoch"] == json!(2)
            && e["transport"] == json!("exec")
            && e["kind"] == json!("agent_message")),
        "the resumed worker's own words must arrive on the EXEC transport"
    );

    drop(guard);
    common::assert_no_broker_left(&env.home);
}

/// **A real turn, really interrupted.**
///
/// The fake can be told to hang; a real model cannot, so the brief buys time
/// instead — a long answer is a long turn, and a long turn is a window to
/// interrupt in. The loop is bounded at three attempts and tolerant of losing
/// the race on any one of them (`noop:true` is exit 0 by design, spec deviation
/// recorded in the plan); what is NOT tolerated is the worker reaching a
/// terminal state, because then there was never a turn to interrupt and the test
/// proved nothing.
#[test]
#[ignore = "real codex + tokens; run with A2A_E2E=1 cargo test --test e2e_appserver -- --ignored"]
fn real_codex_interrupt_parks_the_worker_as_interrupted() {
    if !gated("the real interrupt proof") {
        return;
    }
    let env = Env::new();
    let guard = common::BrokerGuard::new(&env.home);
    let repo = git_repo(&env.work);
    let brief = env.brief(
        "Write a long, detailed essay of at least 3000 words on the history of the \
         shipping container. Take your time and be thorough.\n",
    );

    let mut cmd = real_cli(
        &env,
        &[
            "--json",
            "spawn",
            "--transport",
            "app-server",
            "--lane",
            "general",
            "--cwd",
        ],
    );
    cmd.arg(&repo).arg("--brief").arg(&brief);
    let spawned = run(cmd, &env.scratch, "int-spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().expect("an id").to_string();
    eprintln!("real interrupt worker: {id}");

    // A turn must be OPEN before there is anything to interrupt: `turn/started`
    // in the WAL is that fact, and waiting for it is what makes the three
    // attempts below about the model's timing rather than about the daemon's
    // startup.
    until_within(
        Duration::from_secs(180),
        "a real turn to be open",
        || {
            events_of(&env.home, &id)
                .iter()
                .any(|e| e["kind"] == json!("turn_started"))
                .then_some(())
        },
    );

    let mut parked = None;
    for attempt in 1..=3 {
        let interrupted = run(
            real_cli(&env, &["--json", "interrupt", &id]),
            &env.scratch,
            &format!("int-{attempt}"),
        );
        assert_eq!(
            interrupted.code, 0,
            "interrupt (attempt {attempt}): {}",
            interrupted.stderr
        );
        let data = interrupted.data();
        eprintln!("interrupt attempt {attempt}: {data}");

        // The commit is the broker's, so the converged snapshot is the answer —
        // a short bounded poll, not an instant read of a value that is still in
        // flight.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let snap = snap_of(&env.home, &id);
            if let Some(s) = &snap {
                if s["state"] == json!("awaiting-input")
                    && s["state_reason"] == json!("interrupted")
                {
                    parked = snap;
                    break;
                }
                assert_ne!(
                    s["state"],
                    json!("done"),
                    "the turn finished before any interrupt landed — the brief is \
                     not buying enough time to prove anything: {s}"
                );
            }
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(POLL);
        }
        if parked.is_some() {
            break;
        }
    }

    let parked = parked.expect("three attempts and the turn was never interrupted");
    assert_eq!(parked["state"], json!("awaiting-input"), "{parked}");
    assert_eq!(parked["state_reason"], json!("interrupted"), "{parked}");
    assert!(
        parked.get("fold_rejections").is_none(),
        "an interrupt is a legal transition: {parked}"
    );
    // `a2a wait --until blocked` is how an orchestrator learns about it.
    let blocked = run(
        real_cli(&env, &["wait", &id, "--until", "blocked", "--timeout", "30s"]),
        &env.scratch,
        "int-wait",
    );
    assert_eq!(
        blocked.code, 5,
        "an interrupted worker is reported as blocked: {}",
        blocked.stderr
    );

    drop(guard);
    common::assert_no_broker_left(&env.home);
}

/// **The steer-precondition matcher, against the real error.**
///
/// `adapter::is_steer_precondition_failure` is the ONE predicate that decides
/// whether a failed `turn/steer` is the expected turn-boundary race (retry as
/// `turn/start`) or a real error (surface it). Getting it wrong is not a loud
/// failure: it is a worker that silently double-drives, or a `send` that dies at
/// a boundary it was designed to survive.
///
/// Zero tokens: a `turn/steer` naming a turn that does not exist must fail its
/// precondition before any model is called, so this drives the real binary
/// through `initialize` + `thread/start` and never opens a turn at all.
#[tokio::test]
#[ignore = "real codex; run with A2A_E2E=1 cargo test --test e2e_appserver -- --ignored"]
async fn real_steer_precondition_error_is_the_one_the_matcher_knows() {
    if !gated("the real steer-precondition probe") {
        return;
    }
    use a2a::adapter;
    use a2a::config::{LaneConfig, SandboxMode};
    use a2a::rpc::{Client, RpcError};

    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let budget = Duration::from_secs(30);

    let (client, _incoming, mut child) = Client::spawn(
        &real_codex_bin(),
        &adapter::APP_SERVER_ARGS,
        &[],
        &tmp.path().join("stderr.log"),
    )
    .await
    .expect("the real codex app-server starts");

    adapter::initialize(&client, budget)
        .await
        .expect("the real handshake");
    let lane = LaneConfig {
        sandbox: SandboxMode::WorkspaceWrite,
        network: false,
        worktree_default: false,
        model: a2a::config::DEFAULT_MODEL.to_string(),
        effort: "medium".to_string(),
        start_timeout_secs: 120,
        task_timeout_secs: 3600,
        blocked_timeout_secs: 1800,
        allow_elevation: false,
        max_turns: 30,
    };
    let thread = adapter::thread_start(&client, &lane, &work, &work, budget)
        .await
        .expect("thread/start on the real server");

    // No turn was ever started, so this `expectedTurnId` is exactly the
    // condition a lost steer race produces.
    let err = adapter::turn_steer(
        &client,
        &thread.thread_id,
        "turn_that_never_existed",
        "steer me",
        budget,
    )
    .await
    .expect_err("steering a turn that does not exist must fail");
    eprintln!("observed steer-precondition error: {err:?}");

    assert!(
        adapter::is_steer_precondition_failure(&err),
        "the real steer-precondition error is not the one \
         `adapter::is_steer_precondition_failure` recognizes ({err:?}) — fix the \
         predicate (`adapter::STEER_PRECONDITION_MARKERS`) and transcribe the \
         observed message there. Do NOT widen it to the error code: 0.140.0 \
         answers -32600 to `thread not found` too, and an unknown failure \
         retried as a fresh turn double-drives the worker"
    );
    // …and it is a *remote* error, not a transport failure that happened to
    // contain the right words.
    assert!(
        matches!(err, RpcError::Remote { .. }),
        "expected a JSON-RPC error from the server, got {err:?}"
    );

    // The other half, and the reason this predicate is message-shaped: the live
    // server answers the SAME error code for a wrong thread id. If that were
    // routed as a lost race, `send` would answer a typo by opening a second
    // turn on a running worker.
    let not_a_race = adapter::turn_steer(
        &client,
        "00000000-0000-7000-8000-000000000000",
        "turn_that_never_existed",
        "steer me",
        budget,
    )
    .await
    .expect_err("steering an unknown thread must fail");
    eprintln!("observed non-race error under the same code: {not_a_race:?}");
    assert!(
        !adapter::is_steer_precondition_failure(&not_a_race),
        "an unknown thread id must SURFACE, never be retried as a fresh turn: \
         {not_a_race:?}"
    );

    let _ = child.kill().await;
}
