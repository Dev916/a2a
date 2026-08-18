//! Admission control as an operator meets it (spec §4.9): caps, the queue, and
//! the four ways a spawn can be told to wait — or told no.
//!
//! Every test here drives the SHIPPED binary against a real daemon, because the
//! thing under test is a property of a *fleet*: how many workers may run at
//! once, what happens to the ones that may not, and who admits them when a slot
//! frees. An in-process core could assert the arithmetic but not the handover —
//! and the handover (genesis carries `queued`, the fold reads it, the broker
//! admits it) is where the design lives.
//!
//! Two conventions, inherited from `tests/cmd_broker.rs` and load-bearing for
//! the same reasons: **converged-state assertions** (nothing here asserts how
//! fast a daemon does anything) and **a guard per home** (a daemon that outlives
//! its temp home is a stray, not a pass).
//!
//! The `hang-turn` fake is what makes a cap observable at all: it holds its turn
//! open until it is steered or interrupted, so a worker that has been admitted
//! demonstrably *stays* admitted, and the second spawn meets a broker that is
//! genuinely full rather than one that is merely slow.

mod common;

use serde_json::{json, Value};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CONVERGE: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(100);

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

fn socket(home: &Path) -> PathBuf {
    home.join("broker.sock")
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

fn wait_for_broker(home: &Path) -> u32 {
    until("the broker to publish its socket and pidfile", || {
        if !is_socket(&socket(home)) {
            return None;
        }
        read_pidfile(home)
    })
}

fn alive(pid: u32) -> bool {
    let out = Command::new("ps")
        .args(["-o", "pid=", "-p", &pid.to_string()])
        .output()
        .expect("ps runs");
    out.status.success() && !String::from_utf8_lossy(&out.stdout).trim().is_empty()
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

fn has_fsm_row(home: &Path, id: &str, fsm: &str) -> bool {
    events_of(home, id)
        .iter()
        .any(|e| e["raw"]["fsm"] == json!(fsm))
}

fn worker_dirs(home: &Path) -> usize {
    std::fs::read_dir(home.join("workers"))
        .map(|e| e.flatten().filter(|e| e.path().is_dir()).count())
        .unwrap_or(0)
}

fn write_brief(dir: &Path, body: &str) -> PathBuf {
    let p = dir.join("the-brief.md");
    std::fs::write(&p, body).unwrap();
    p
}

/// The daemon's own view: how many workers are live, and how many are waiting.
fn broker_info(home: &Path, allowed: &Path, scratch: &Path, tag: &str) -> Value {
    let out = run(
        a2a(home, allowed, &["--json", "broker", "status"]),
        scratch,
        tag,
    );
    assert_eq!(out.code, 0, "broker status: {}", out.stderr);
    out.data()
}

/// One `a2a spawn`, with the admission caps in the environment.
///
/// The caps travel on EVERY invocation on purpose: the daemon inherits
/// `A2A_ADMISSION_*` through the autostart prefix list, so the first spawn is
/// what configures the broker the rest of the test meets.
fn spawn_cmd(
    home: &Path,
    allowed: &Path,
    work: &Path,
    brief: &Path,
    lane: &str,
    caps: &[(&str, &str)],
    extra: &[&str],
) -> Command {
    let mut c = a2a(home, allowed, &["--json", "spawn", "--lane", lane, "--cwd"]);
    c.arg(work).arg("--brief").arg(brief).args(extra);
    c.env("FAKE_SCENARIO", "hang-turn");
    for (k, v) in caps {
        c.env(k, v);
    }
    c
}

/// A worker directory laid out as `a2a spawn` leaves one, patched to the state
/// a previous run would have left behind.
fn seed_worker(home: &Path, cwd: &Path, id: &str, patch: Value) -> PathBuf {
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
    let (Some(into), Some(from)) = (value.as_object_mut(), patch.as_object()) else {
        panic!("a patch is an object");
    };
    for (k, v) in from {
        into.insert(k.clone(), v.clone());
    }
    a2a::store::write_atomic(&path, value.to_string().as_bytes()).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// the queue
// ---------------------------------------------------------------------------

/// The whole of admission control in one story: at the cap a spawn is **queued**
/// rather than refused, the queue position is a fact the CLI prints and the
/// snapshot carries, and the settle of the worker that was holding the slot is
/// what admits it — into the ordinary start flow, so it finishes like any other.
#[test]
fn at_the_cap_a_spawn_queues_and_the_next_settle_admits_it() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");
    let caps = [("A2A_ADMISSION_GLOBAL_CAP", "1")];

    let first = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "first",
    );
    assert_eq!(first.code, 0, "the first spawn: {}", first.stderr);
    let a = first.data()["id"].as_str().unwrap().to_string();
    assert_eq!(
        first.data()["queue_position"],
        Value::Null,
        "a worker that was admitted is not waiting for anything"
    );
    let running = until_state(&home, &a, "running");
    assert_eq!(running["queue_position"], Value::Null);

    // The cap is one, and it is spent.
    let second = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "second",
    );
    assert_eq!(
        second.code, 0,
        "a queued spawn SUCCEEDS — the worker exists, it is just waiting: {}",
        second.stderr
    );
    let b = second.data()["id"].as_str().unwrap().to_string();
    assert_eq!(second.data()["state"], json!("queued"), "{}", second.stdout);
    assert_eq!(
        second.data()["queue_position"],
        json!(1),
        "first in line is position 1: {}",
        second.stdout
    );

    // The genesis event is where `queued` lives, so a snapshot rebuilt from the
    // WAL alone lands in the same state.
    let genesis = events_of(&home, &b)
        .into_iter()
        .find(|e| e["raw"]["fsm"] == json!("Spawn"))
        .expect("a genesis row");
    assert_eq!(genesis["raw"]["queued"], json!(true), "{genesis}");
    let queued = snap_of(&home, &b).expect("a snapshot");
    assert_eq!(queued["state"], json!("queued"));
    assert_eq!(queued["queue_position"], json!(1));
    assert!(
        !has_fsm_row(&home, &b, "ProcessStarted"),
        "a queued worker has no process and no thread"
    );

    // ... and it has no actor: the queue is the registry's other half.
    let info = broker_info(&home, tmp.path(), &scratch, "info");
    assert_eq!(info["live"], json!(1), "{info}");
    assert_eq!(info["queued"], json!(1), "{info}");
    let listed = run(
        a2a(&home, tmp.path(), &["--json", "__broker-call", "list"]),
        &scratch,
        "list",
    );
    let ids: Vec<String> = listed.data()["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec![a.clone()], "only the admitted worker has an actor");

    // `ls` says the same thing, which is where an operator actually reads it.
    let listed = run(a2a(&home, tmp.path(), &["--json", "ls"]), &scratch, "ls");
    let row = listed.data()["workers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["id"] == json!(b))
        .cloned()
        .expect("the queued worker is listed");
    assert_eq!(row["state"], json!("queued"), "{row}");
    assert_eq!(row["queue_position"], json!(1), "{row}");

    // The settle is the drain trigger.
    let killed = run(
        a2a(&home, tmp.path(), &["--json", "kill", &a]),
        &scratch,
        "kill",
    );
    assert_eq!(killed.code, 0, "kill: {}", killed.stderr);
    assert_eq!(killed.data()["state"], json!("cancelled"));

    let admitted = until_state(&home, &b, "running");
    assert!(
        has_fsm_row(&home, &b, "Admit"),
        "the queued worker was admitted through the machine, not around it"
    );
    assert_eq!(
        admitted["queue_position"],
        Value::Null,
        "an admitted worker is not in the queue any more"
    );
    assert!(
        admitted.get("fold_rejections").is_none(),
        "queued → spawned → running is a legal path: {admitted}"
    );

    // And it is a worker like any other: steered to completion.
    let sent = run(
        a2a(&home, tmp.path(), &["--json", "send", &b, "finish up"]),
        &scratch,
        "send",
    );
    assert_eq!(sent.code, 0, "send: {}", sent.stderr);
    until_state(&home, &b, "done");

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The waiting room is bounded too. A caller who meets a full queue gets exit 9
/// with a `retry_after_secs` it can back off on — and nothing is left on disk,
/// because the refusal happens before genesis.
#[test]
fn a_full_queue_refuses_with_queue_full_and_a_retry_hint() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");
    let caps = [
        ("A2A_ADMISSION_GLOBAL_CAP", "1"),
        ("A2A_ADMISSION_QUEUE_CAP", "1"),
    ];

    let first = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "first",
    );
    assert_eq!(first.code, 0, "{}", first.stderr);
    until_state(&home, first.data()["id"].as_str().unwrap(), "running");

    let second = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "second",
    );
    assert_eq!(second.code, 0, "{}", second.stderr);
    assert_eq!(second.data()["queue_position"], json!(1));

    let before = worker_dirs(&home);
    let third = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "third",
    );
    assert_eq!(third.code, 9, "a full queue is a precondition: {}", third.stderr);
    let err = third.error();
    assert_eq!(err["code"], json!("QUEUE_FULL"), "{err}");
    assert_eq!(
        err["data"]["retry_after_secs"], json!(30),
        "the one refusal that clears on its own says when to come back: {err}"
    );
    assert_eq!(err["data"]["queue_cap"], json!(1), "{err}");
    assert!(
        err["remedy"].as_str().unwrap_or_default().contains("queue_cap"),
        "the refusal names the knob: {err}"
    );
    assert_eq!(
        worker_dirs(&home),
        before,
        "a refused spawn is refused BEFORE genesis: nothing is left behind"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// `--no-queue` is the caller saying "now or not at all". At the cap that is
/// exit 9 `AT_CAPACITY`, and the flag reaches the broker from Phase 1's argv
/// with nothing new wired in between.
#[test]
fn no_queue_refuses_at_capacity_rather_than_waiting() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");
    let caps = [("A2A_ADMISSION_GLOBAL_CAP", "1")];

    let first = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "first",
    );
    assert_eq!(first.code, 0, "{}", first.stderr);
    until_state(&home, first.data()["id"].as_str().unwrap(), "running");

    let before = worker_dirs(&home);
    let refused = run(
        spawn_cmd(
            &home,
            tmp.path(),
            &work,
            &brief,
            "general",
            &caps,
            &["--no-queue"],
        ),
        &scratch,
        "refused",
    );
    assert_eq!(refused.code, 9, "at capacity is a precondition: {}", refused.stderr);
    let err = refused.error();
    assert_eq!(err["code"], json!("AT_CAPACITY"), "{err}");
    assert_eq!(err["data"]["global_cap"], json!(1), "{err}");
    assert_eq!(err["data"]["lane"], json!("general"), "{err}");
    assert_eq!(
        worker_dirs(&home),
        before,
        "nothing is written for a spawn that was never admitted"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// A resume is new work, and there is no queued form of it: the machine has no
/// `terminal → queued` row, so a full broker refuses rather than parking it in a
/// state the FSM would reject.
#[test]
fn a_resume_at_the_cap_is_refused_rather_than_queued() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");
    let caps = [("A2A_ADMISSION_GLOBAL_CAP", "1")];

    // A worker a previous run finished, with a thread that could be resumed.
    let finished = seed_worker(
        &home,
        &work,
        "general-0817t1200-fini",
        json!({"state": "done", "transport": "app-server", "session_id": "thread-done"}),
    );

    let first = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "first",
    );
    assert_eq!(first.code, 0, "{}", first.stderr);
    until_state(&home, first.data()["id"].as_str().unwrap(), "running");

    let mut cmd = a2a(&home, tmp.path(), &["--json", "__broker-call", "resume"]);
    cmd.arg(json!({"dir": finished}).to_string());
    let refused = run(cmd, &scratch, "resume");
    assert_eq!(refused.code, 9, "a resume at the cap: {}", refused.stderr);
    assert_eq!(refused.error()["code"], json!("AT_CAPACITY"), "{}", refused.stdout);
    assert_eq!(
        snap_of(&home, "general-0817t1200-fini").unwrap()["state"],
        json!("done"),
        "a refused resume changes nothing"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// A queued worker can be stopped before it ever runs — and once stopped it is
/// never admitted, however many slots free up afterwards.
#[test]
fn killing_a_queued_worker_cancels_it_and_it_is_never_admitted() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");
    let caps = [("A2A_ADMISSION_GLOBAL_CAP", "1")];

    let first = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "first",
    );
    let a = first.data()["id"].as_str().unwrap().to_string();
    until_state(&home, &a, "running");

    let second = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "second",
    );
    let b = second.data()["id"].as_str().unwrap().to_string();
    assert_eq!(second.data()["queue_position"], json!(1));

    let killed = run(
        a2a(&home, tmp.path(), &["--json", "kill", &b]),
        &scratch,
        "kill-queued",
    );
    assert_eq!(killed.code, 0, "kill on a queued worker: {}", killed.stderr);
    assert_eq!(killed.data()["state"], json!("cancelled"), "{}", killed.stdout);
    assert_eq!(
        killed.data()["signal"],
        json!("broker"),
        "there is no process to signal — the broker cancels it where it waits: {}",
        killed.stdout
    );
    let cancelled = snap_of(&home, &b).unwrap();
    assert_eq!(cancelled["queue_position"], Value::Null);
    assert!(
        cancelled.get("fold_rejections").is_none(),
        "queued → cancelled is a legal row: {cancelled}"
    );
    until("the broker's queue to be empty", || {
        (broker_info(&home, tmp.path(), &scratch, "info-empty")["queued"] == json!(0)).then_some(())
    });

    // Now free the slot: nothing may be admitted, because nothing is waiting.
    run(
        a2a(&home, tmp.path(), &["--json", "kill", &a]),
        &scratch,
        "kill-running",
    );
    until_state(&home, &a, "cancelled");
    // A drain that admitted a cancelled worker would show up as an `Admit` row
    // (and a rejection behind it); converge on the broker being idle first, so
    // the assertion is not merely early.
    until("the broker to report nothing live", || {
        (broker_info(&home, tmp.path(), &scratch, "info-idle")["live"] == json!(0)).then_some(())
    });
    assert!(
        !has_fsm_row(&home, &b, "Admit"),
        "a cancelled worker is never admitted"
    );
    assert_eq!(snap_of(&home, &b).unwrap()["state"], json!("cancelled"));

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The queue is on disk, not in the daemon: a broker that dies with workers
/// waiting leaves them `queued`, and the next one takes them back in the order
/// they were created.
#[test]
fn queued_workers_survive_a_broker_death_and_the_next_broker_re_enqueues_them() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");
    let caps = [("A2A_ADMISSION_GLOBAL_CAP", "1")];

    let first = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "first",
    );
    let a = first.data()["id"].as_str().unwrap().to_string();
    until_state(&home, &a, "running");
    let second = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "second",
    );
    let b = second.data()["id"].as_str().unwrap().to_string();
    assert_eq!(second.data()["state"], json!("queued"));

    // The daemon dies without a chance to say anything.
    let pid = wait_for_broker(&home);
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    until("the broker to be gone", || (!alive(pid)).then_some(()));
    let _ = std::fs::remove_file(socket(&home));
    assert_eq!(
        snap_of(&home, &b).unwrap()["state"],
        json!("queued"),
        "a queued worker outlives the broker that queued it"
    );

    let mut restarted = a2a(&home, tmp.path(), &["broker", "run"]);
    restarted.env("FAKE_SCENARIO", "hang-turn");
    for (k, v) in caps {
        restarted.env(k, v);
    }
    run(restarted, &scratch, "restart");
    wait_for_broker(&home);

    let info = until("the new broker to finish reconciling", || {
        let info = broker_info(&home, tmp.path(), &scratch, "info-after");
        info["reconciled"]["adopted_at"].is_string().then_some(info)
    });
    assert_eq!(
        info["reconciled"]["requeued"],
        json!([b.clone()]),
        "the queued worker is re-enqueued, in created_at order: {info}"
    );
    assert_eq!(
        info["queued"], json!(1),
        "and it is in THIS broker's queue, waiting for a slot: {info}"
    );
    assert_eq!(
        snap_of(&home, &b).unwrap()["state"],
        json!("queued"),
        "re-enqueued, not admitted: the cap is still one and {a} still holds it"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// Exec spawns are deliberately NOT counted against the broker's cap (deferred
/// to Phase 3): their processes are their own, the broker never learns they
/// finished, and a slot it cannot release is a slot lost forever.
#[test]
fn exec_spawns_do_not_count_against_the_brokers_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");
    let caps = [("A2A_ADMISSION_GLOBAL_CAP", "1")];

    let first = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "first",
    );
    assert_eq!(first.data()["transport"], json!("app-server"));
    until_state(&home, first.data()["id"].as_str().unwrap(), "running");

    // The Phase-1 stub as this spawn's codex: an exec worker is a real process
    // of its own, and the broker is not in the path at all.
    let mut cmd = spawn_cmd(
        &home,
        tmp.path(),
        &work,
        &brief,
        "general",
        &caps,
        &["--transport", "exec"],
    );
    cmd.env("A2A_CODEX_BIN", common::stub_path());
    let exec = run(cmd, &scratch, "exec");
    assert_eq!(exec.code, 0, "an exec spawn at the cap: {}", exec.stderr);
    assert_eq!(exec.data()["transport"], json!("exec"), "{}", exec.stdout);
    assert_ne!(
        exec.data()["state"],
        json!("queued"),
        "exec spawns are never queued: {}",
        exec.stdout
    );
    let id = exec.data()["id"].as_str().unwrap().to_string();
    assert_eq!(
        snap_of(&home, &id).unwrap()["queue_position"],
        Value::Null
    );
    assert_eq!(
        broker_info(&home, tmp.path(), &scratch, "info")["queued"],
        json!(0),
        "the broker's queue never saw it"
    );

    // Leave nothing running behind: the exec worker is this test's process.
    run(
        a2a(&home, tmp.path(), &["--json", "kill", &id]),
        &scratch,
        "kill-exec",
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The per-lane cap is what stops one lane's fleet from spending the whole
/// broker: an `impl` lane at its own cap queues, while a `general` spawn with
/// room in its lane is admitted straight past it.
#[test]
fn a_lane_at_its_cap_queues_without_blocking_another_lane() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    // The impl lane's brief must declare its acceptance criteria and how they
    // are verified (spec §4.7); the general one below reuses the same file.
    let brief = write_brief(
        tmp.path(),
        "---\nacceptance_criteria:\n  - it works\nverify:\n  - cargo test\n---\ndo the thing\n",
    );
    // The global cap is the default 8; only the impl lane is narrow.
    let caps = [("A2A_ADMISSION_LANES_IMPL_CAP", "1")];

    let first = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "impl", &caps, &[]),
        &scratch,
        "impl-first",
    );
    assert_eq!(first.code, 0, "{}", first.stderr);
    until_state(&home, first.data()["id"].as_str().unwrap(), "running");

    let second = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "impl", &caps, &[]),
        &scratch,
        "impl-second",
    );
    assert_eq!(second.code, 0, "{}", second.stderr);
    assert_eq!(
        second.data()["state"],
        json!("queued"),
        "the impl lane is full: {}",
        second.stdout
    );
    assert_eq!(second.data()["queue_position"], json!(1));

    let other = run(
        spawn_cmd(&home, tmp.path(), &work, &brief, "general", &caps, &[]),
        &scratch,
        "general",
    );
    assert_eq!(other.code, 0, "{}", other.stderr);
    assert_eq!(
        other.data()["queue_position"],
        Value::Null,
        "another lane's queue is not this lane's problem: {}",
        other.stdout
    );
    until_state(&home, other.data()["id"].as_str().unwrap(), "running");

    drop(guard);
    common::assert_no_broker_left(&home);
}
