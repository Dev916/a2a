//! `a2a broker …` as an operator meets it: a real daemon, really detached.
//!
//! Every test here drives the SHIPPED binary in another process — no in-process
//! core, no injected supervision. That is the point: singleton, detachment,
//! pidfile, reconciliation and the env boundary are all facts about a process
//! tree, and a test that fakes the process tree cannot observe any of them.
//!
//! Two conventions, both load-bearing:
//!
//! * **Converged-state assertions.** Nothing here asserts how fast the daemon
//!   comes up; everything polls a bounded window for the state it must reach.
//! * **Every home gets a [`common::BrokerGuard`] and ends with
//!   `assert_no_broker_left`.** A daemon that outlives its temp home is the
//!   failure this file is most able to cause, so it is also the one it checks
//!   hardest.

mod common;

use a2a::fsm::WorkerState;
use a2a::state::WorkerSnapshot;
use a2a::store::EventLog;
use a2a::{events::EventKind, fold};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The convergence window every poll in this file shares (the plan pins ≤10s
/// at 100ms for the twenty-way cold start; the rest inherit it).
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
    let child = spawn(&mut cmd, scratch, tag);
    finish(child, scratch, tag)
}

fn spawn(cmd: &mut Command, scratch: &Path, tag: &str) -> Child {
    std::fs::create_dir_all(scratch).unwrap();
    let out = std::fs::File::create(scratch.join(format!("{tag}.out"))).unwrap();
    let err = std::fs::File::create(scratch.join(format!("{tag}.err"))).unwrap();
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("the a2a binary runs")
}

fn finish(mut child: Child, scratch: &Path, tag: &str) -> Ran {
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

fn wait_for_broker(home: &Path) -> u32 {
    until("the broker to publish its socket and pidfile", || {
        // A *socket*, not merely a path that exists: the pidfile is written
        // before the listener is bound, and a stale regular file left by a
        // crashed predecessor also "exists" — waiting on existence alone would
        // return while the daemon is still unlinking it.
        if !is_socket(&socket(home)) {
            return None;
        }
        read_pidfile(home)
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

fn mode_of(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

fn ps_field(pid: u32, field: &str) -> Option<String> {
    let out = Command::new("ps")
        .args(["-o", &format!("{field}="), "-p", &pid.to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (out.status.success() && !text.is_empty()).then_some(text)
}

fn alive(pid: u32) -> bool {
    ps_field(pid, "pid").is_some()
}

/// Every process whose parent is `pid` — how "exactly one app-server child" is
/// asked without a global scan that would see other tests' fakes.
fn children_of(pid: u32) -> Vec<u32> {
    let out = Command::new("ps")
        .args(["-axo", "pid=,ppid="])
        .output()
        .expect("ps runs");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let mut it = line.split_whitespace();
            let child: u32 = it.next()?.parse().ok()?;
            let parent: u32 = it.next()?.parse().ok()?;
            (parent == pid).then_some(child)
        })
        .collect()
}

/// A worker directory laid out exactly as `a2a spawn` leaves one, then patched
/// to the state a previous broker instance would have left behind.
fn seed_worker(home: &Path, cwd: &Path, id: &str, patch: Value) -> PathBuf {
    let dir = home.join("workers").join(id);
    for sub in ["outbox", "inbox", "inbox/delivered", "scratch", "context"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    let mut log = EventLog::open(&dir).unwrap();
    let snap = fold::append_and_fold(
        &mut log,
        &dir,
        WorkerSnapshot::new_for_test(id, cwd),
        EventKind::Spawned,
        json!({"fsm": "Spawn", "id": id, "lane": "general", "cwd": cwd,
               "transport": "app-server"}),
        Utc::now(),
    )
    .unwrap();
    assert_eq!(snap.state, WorkerState::Spawned);

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

fn ago(mins: i64) -> DateTime<Utc> {
    Utc::now() - ChronoDuration::minutes(mins)
}

/// A live stand-in for a previous instance's app-server child: a process this
/// test owns, whose pid and start-time witness a seeded worker remembers.
///
/// The [`Child`] is kept so death is observed by `waitpid`, not by `ps`: a
/// killed process this test never reaps is a zombie, and a zombie is still a
/// row in `ps`.
fn stand_in_child() -> (Child, u32, String) {
    let child = Command::new("sleep")
        .arg("300")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("sleep runs");
    let pid = child.id();
    let witness = until("the stand-in child to be visible to ps", || {
        a2a::exec::start_witness(pid)
    });
    (child, pid, witness)
}

// ---------------------------------------------------------------------------
// singleton + detachment
// ---------------------------------------------------------------------------

/// The pinned order: **flock, then fork**. What that buys is observable from
/// outside — the process the shell started exits immediately, the daemon it
/// left behind belongs to init, it wrote its own pid, it survives a SIGHUP, and
/// a second `broker run` says so and exits 0 rather than stealing the address.
#[test]
fn broker_run_detaches_holds_the_singleton_and_a_second_run_exits_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");
    let guard = common::BrokerGuard::new(&home);

    // A stale socket from a crashed predecessor: unlinked only because the
    // connect to it fails.
    run(
        a2a(&home, tmp.path(), &["config", "show"]),
        &scratch,
        "layout",
    );
    std::fs::write(socket(&home), b"not a socket").unwrap();

    let first = run(
        a2a(&home, tmp.path(), &["broker", "run"]),
        &scratch,
        "first",
    );
    assert_eq!(
        first.code, 0,
        "the launching process exits: {}",
        first.stderr
    );
    let pid = wait_for_broker(&home);

    assert_eq!(
        mode_of(&home.join("broker.pid")),
        0o600,
        "the pidfile is 0600"
    );
    assert!(alive(pid), "the pidfile names a live process");
    until("the daemon to be reparented to init", || {
        (ps_field(pid, "ppid")?.trim() == "1").then_some(())
    });
    assert!(
        home.join("broker.log").exists(),
        "the daemon's stdio goes to broker.log"
    );
    assert!(
        is_socket(&socket(&home)),
        "the stale regular file was unlinked and a real socket bound in its place"
    );

    // SIGHUP is ignored: a daemon that dies when its launching terminal closes
    // is not detached, whatever its ppid says.
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::Signal::SIGHUP,
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(alive(pid), "SIGHUP is ignored");

    // ... and the singleton holds: contention is a message and an exit 0.
    let second = run(
        a2a(&home, tmp.path(), &["broker", "run"]),
        &scratch,
        "second",
    );
    assert_eq!(
        second.code, 0,
        "a second `broker run` is a no-op, not a failure"
    );
    assert!(
        second.stderr.contains("a2a: broker already running"),
        "the second run says so on stderr: {:?}",
        second.stderr
    );
    assert_eq!(
        read_pidfile(&home),
        Some(pid),
        "the incumbent daemon is untouched"
    );
    assert_eq!(
        common::broker_processes(&home).len(),
        1,
        "exactly one daemon on this home"
    );

    drop(guard);
    assert!(!socket(&home).exists(), "shutdown takes the socket with it");
    common::assert_no_broker_left(&home);
    assert!(!alive(pid), "the daemon process itself is gone");
    // Teardown order: the socket goes first, the pidfile with the process, and
    // the lock last — so a free lock means everything else is already gone.
    assert!(
        !home.join("broker.pid").exists(),
        "a daemon that exited took its pidfile with it"
    );
    assert!(lock_is_free(&home), "and released the singleton last");
}

/// Twenty cold starts at once. The lock is what decides — nineteen of them find
/// it held and leave, one daemonizes — and the convergence is checked as a
/// state, not as a timing.
#[test]
fn twenty_concurrent_cold_starts_converge_on_one_daemon() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");
    let guard = common::BrokerGuard::new(&home);

    let racers: Vec<std::process::Child> = (0..20)
        .map(|i| {
            let mut cmd = a2a(&home, tmp.path(), &["broker", "run"]);
            spawn(&mut cmd, &scratch, &format!("racer-{i}"))
        })
        .collect();
    let outcomes: Vec<Ran> = racers
        .into_iter()
        .enumerate()
        .map(|(i, child)| finish(child, &scratch, &format!("racer-{i}")))
        .collect();

    let unhappy: Vec<(i32, &str)> = outcomes
        .iter()
        .filter(|r| r.code != 0)
        .map(|r| (r.code, r.stderr.trim()))
        .collect();
    assert!(
        unhappy.is_empty(),
        "every racer exits 0; these did not: {unhappy:?}"
    );
    let already = outcomes
        .iter()
        .filter(|r| r.stderr.contains("a2a: broker already running"))
        .count();
    assert_eq!(already, 19, "nineteen clean `already running` exits");

    let pid = wait_for_broker(&home);
    until("exactly one broker process on this home", || {
        (common::broker_processes(&home).len() == 1).then_some(())
    });
    until("exactly one app-server child under the daemon", || {
        (children_of(pid).len() == 1).then_some(())
    });
    assert!(
        !lock_is_free(&home),
        "the surviving daemon still holds broker.lock"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
    until("the lock to be released with the daemon", || {
        lock_is_free(&home).then_some(())
    });
}

/// `--foreground` skips the double fork and nothing else: the same lock, the
/// same pidfile, the same socket — but the process the operator started is the
/// daemon, and it is still theirs.
#[test]
fn a_foreground_run_stays_attached_and_is_itself_the_daemon() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");
    let guard = common::BrokerGuard::new(&home);

    let mut cmd = a2a(&home, tmp.path(), &["broker", "run", "--foreground"]);
    let child = spawn(&mut cmd, &scratch, "fg");
    let launched = child.id();
    let pid = wait_for_broker(&home);

    assert_eq!(
        pid, launched,
        "--foreground: the process the operator started IS the daemon"
    );
    assert_eq!(
        ps_field(pid, "ppid").unwrap().trim(),
        std::process::id().to_string(),
        "and it is still a child of whoever started it"
    );
    assert!(!lock_is_free(&home), "it holds the same singleton");

    let stopped = run(
        a2a(&home, tmp.path(), &["broker", "shutdown"]),
        &scratch,
        "stop",
    );
    assert_eq!(stopped.code, 0);
    let ran = finish(child, &scratch, "fg");
    assert_eq!(
        ran.code, 0,
        "an attached daemon exits 0 on `broker shutdown`"
    );
    assert!(
        ran.stdout.contains("\"started\": true"),
        "an attached daemon reports on ITS OWN stdout: {:?}",
        ran.stdout
    );
    assert!(!home.join("broker.pid").exists());
    assert!(lock_is_free(&home));

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// A non-blocking probe: `true` when nobody holds `broker.lock`.
#[allow(deprecated)]
fn lock_is_free(home: &Path) -> bool {
    use std::os::fd::AsRawFd;
    let path = home.join("broker.lock");
    let Ok(f) = std::fs::OpenOptions::new().write(true).open(&path) else {
        return true; // no lock file at all
    };
    let held =
        nix::fcntl::flock(f.as_raw_fd(), nix::fcntl::FlockArg::LockExclusiveNonblock).is_err();
    if !held {
        let _ = nix::fcntl::flock(f.as_raw_fd(), nix::fcntl::FlockArg::Unlock);
    }
    !held
}

// ---------------------------------------------------------------------------
// status / restart
// ---------------------------------------------------------------------------

/// `broker status` answers with the daemon's own facts, `status --broker` is
/// the same answer under the spec §4.1 spelling, and both are exit 7 when there
/// is nothing to ask.
#[test]
fn broker_status_reports_the_daemon_and_status_broker_is_an_alias() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");
    let guard = common::BrokerGuard::new(&home);

    // Down: exit 7, and asking must not conjure a daemon.
    let down = run(
        a2a(&home, tmp.path(), &["--json", "broker", "status"]),
        &scratch,
        "down",
    );
    assert_eq!(down.code, 7, "a broker that is not there is exit 7");
    assert_eq!(down.json()["ok"], json!(false));
    assert!(
        !socket(&home).exists(),
        "`broker status` never autostarts a broker"
    );

    run(a2a(&home, tmp.path(), &["broker", "run"]), &scratch, "run");
    let pid = wait_for_broker(&home);

    let up = run(
        a2a(&home, tmp.path(), &["--json", "broker", "status"]),
        &scratch,
        "up",
    );
    assert_eq!(up.code, 0);
    let d = up.data();
    assert_eq!(d["running"], json!(true));
    assert_eq!(
        d["pid"],
        json!(pid),
        "the broker's own pid, matching broker.pid"
    );
    assert_eq!(d["transport"], json!("app-server"), "transport mode: {d}");
    assert!(d["uptime_secs"].is_u64(), "uptime: {d}");
    assert!(d["child_pid"].is_u64(), "the app-server child's pid: {d}");
    assert_eq!(d["restarts"], json!(0), "restart count: {d}");
    assert_eq!(d["workers"], json!(0), "worker count: {d}");
    assert_eq!(
        d["clients"],
        json!(1),
        "client count — the asker itself: {d}"
    );

    let alias = run(
        a2a(&home, tmp.path(), &["--json", "status", "--broker"]),
        &scratch,
        "alias",
    );
    assert_eq!(alias.code, 0);
    let a = alias.data();
    for key in [
        "running",
        "pid",
        "transport",
        "child_pid",
        "restarts",
        "workers",
        "clients",
    ] {
        assert_eq!(a[key], d[key], "`status --broker` agrees about `{key}`");
    }

    // The alias does not cost `status` its argument, and neither shape is a
    // silent no-op.
    let neither = run(
        a2a(&home, tmp.path(), &["--json", "status"]),
        &scratch,
        "neither",
    );
    assert_eq!(
        neither.code, 2,
        "`status` with no id and no --broker is usage"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// `broker restart` is a shutdown followed by a run: a genuinely different
/// daemon, and exactly one of them.
#[test]
fn broker_restart_replaces_the_daemon_with_exactly_one_successor() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");
    let guard = common::BrokerGuard::new(&home);

    run(a2a(&home, tmp.path(), &["broker", "run"]), &scratch, "run");
    let first = wait_for_broker(&home);

    let restarted = run(
        a2a(&home, tmp.path(), &["--json", "broker", "restart"]),
        &scratch,
        "restart",
    );
    assert_eq!(restarted.code, 0, "restart: {}", restarted.stderr);
    let second = restarted.data()["pid"]
        .as_u64()
        .expect("the new daemon's pid") as u32;

    assert_ne!(second, first, "a restart is a new process");
    until("the retired daemon to exit", || {
        (!alive(first)).then_some(())
    });
    assert_eq!(read_pidfile(&home), Some(second), "the pidfile followed");
    until("exactly one broker process after a restart", || {
        (common::broker_processes(&home).len() == 1).then_some(())
    });

    drop(guard);
    common::assert_no_broker_left(&home);
}

// ---------------------------------------------------------------------------
// startup reconciliation
// ---------------------------------------------------------------------------

/// Reap first, adopt second (spec §4.2).
///
/// The previous instance's app-server child is a live process a seeded worker
/// remembers by pid AND start-time witness; it must be dead before anything is
/// resumed, or a resumed thread would be talking to two servers. Queued workers
/// come back in `created_at` order, and exec workers are not this daemon's
/// business at all.
#[test]
fn startup_reconciliation_reaps_the_previous_child_before_it_adopts() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    // The home layout, as `a2a` would leave it.
    run(
        a2a(&home, tmp.path(), &["config", "show"]),
        &scratch,
        "layout",
    );

    let (mut previous, old_child, witness) = stand_in_child();
    let (mut untouched, exec_pid, exec_witness) = stand_in_child();

    let running = seed_worker(
        &home,
        &work,
        "general-0817t1201-aaaa",
        json!({"state": "running", "transport": "app-server", "session_id": "thread-running",
               "pid": old_child, "pid_start_witness": witness, "created_at": ago(50)}),
    );
    let orphaned = seed_worker(
        &home,
        &work,
        "general-0817t1202-bbbb",
        json!({"state": "orphaned", "transport": "app-server", "session_id": "thread-orphaned",
               "pid": old_child, "pid_start_witness": witness, "created_at": ago(40)}),
    );
    // Queued, seeded YOUNGEST FIRST so `created_at` order cannot be confused
    // with directory or insertion order.
    seed_worker(
        &home,
        &work,
        "general-0817t1203-cccc",
        json!({"state": "queued", "transport": "app-server", "created_at": ago(10)}),
    );
    seed_worker(
        &home,
        &work,
        "general-0817t1204-dddd",
        json!({"state": "queued", "transport": "app-server", "created_at": ago(30)}),
    );
    let exec_dir = seed_worker(
        &home,
        &work,
        "general-0817t1205-eeee",
        json!({"state": "running", "transport": "exec", "session_id": "sess-exec",
               "pid": exec_pid, "pid_start_witness": exec_witness, "created_at": ago(20)}),
    );
    let exec_before = std::fs::read(exec_dir.join("state.json")).unwrap();

    run(a2a(&home, tmp.path(), &["broker", "run"]), &scratch, "run");
    wait_for_broker(&home);

    until(
        "the previous instance's app-server child to be reaped",
        || previous.try_wait().unwrap().map(|_| ()),
    );
    assert!(
        untouched.try_wait().unwrap().is_none(),
        "an exec worker's process is NOT the broker's to kill"
    );

    let status = run(
        a2a(&home, tmp.path(), &["--json", "broker", "status"]),
        &scratch,
        "status",
    );
    assert_eq!(status.code, 0, "{}", status.stderr);
    let r = status.data()["reconciled"].clone();

    assert_eq!(
        r["reaped"][0]["pid"],
        json!(old_child),
        "the reap names the child it killed: {r}"
    );
    assert_eq!(r["reaped"][0]["outcome"], json!("killed"), "{r}");
    assert_eq!(
        r["adopted"],
        json!(["general-0817t1201-aaaa", "general-0817t1202-bbbb"]),
        "running AND orphaned app-server workers are re-adopted: {r}"
    );
    assert_eq!(
        r["requeued"],
        json!(["general-0817t1204-dddd", "general-0817t1203-cccc"]),
        "queued workers come back in created_at order (oldest first): {r}"
    );
    assert_eq!(
        r["exec_untouched"],
        json!(["general-0817t1205-eeee"]),
        "exec workers are recorded and left alone: {r}"
    );

    // The order is a fact on disk, not a claim in a report: the reap line
    // precedes the adopt line in broker.jsonl.
    let lines: Vec<Value> = std::fs::read_to_string(home.join("broker.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let phase = |name: &str| {
        lines
            .iter()
            .position(|l| l["broker"] == json!("startup") && l["phase"] == json!(name))
            .unwrap_or_else(|| panic!("broker.jsonl records the {name} phase: {lines:?}"))
    };
    assert!(phase("reap") < phase("adopt"), "reap first, adopt second");

    // The adopted workers really are this broker's now; the queued and exec
    // ones are not.
    let list = run(
        a2a(&home, tmp.path(), &["--json", "__broker-call", "list"]),
        &scratch,
        "list",
    );
    let ids: Vec<String> = list.data()["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        vec![
            "general-0817t1201-aaaa".to_string(),
            "general-0817t1202-bbbb".to_string()
        ],
        "only the adopted workers have actors"
    );
    assert_eq!(
        a2a::state::load_snapshot(&running).unwrap().state,
        WorkerState::Running,
        "adoption re-states nothing that is still true"
    );
    // The orphan is the exception, and it is the reason orphans are in the
    // adoption set at all: `orphaned` means "recorded live, with no broker to be
    // live in", and this daemon has just re-opened the conversation. Committing
    // the machine's `orphaned + Resume → running` row is what makes the worker
    // usable again — left orphaned, its own remedy (`a2a resume`) would start a
    // second, exec transport against the thread this broker now holds. Polled,
    // because the actor writes it: the report above only says it was adopted.
    let revived = until("the adopted orphan to be running again", || {
        let snap = a2a::state::load_snapshot(&orphaned).unwrap();
        (snap.state == WorkerState::Running).then_some(snap)
    });
    assert_eq!(
        revived.epoch, 2,
        "a resume is a new epoch on an old conversation"
    );
    assert_eq!(
        std::fs::read(exec_dir.join("state.json")).unwrap(),
        exec_before,
        "an exec worker's state.json is not rewritten by a broker start"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
    let _ = untouched.kill();
    let _ = untouched.wait();
}

// ---------------------------------------------------------------------------
// the env boundary
// ---------------------------------------------------------------------------

/// The allowlist is scoped to the CHILD, not to the daemon (Global
/// Constraints): the app-server sees a scrubbed, allowlisted environment while
/// the daemon keeps the `A2A_*` variables it needs to resolve anything at all.
#[test]
fn the_child_environment_is_scrubbed_while_the_daemon_keeps_its_own() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");
    let codex_home = tmp.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).unwrap();
    let dump = tmp.path().join("child-env.txt");
    let guard = common::BrokerGuard::new(&home);

    let mut cmd = a2a(&home, tmp.path(), &["broker", "run"]);
    cmd.env("FAKE_DUMP_ENV", &dump)
        .env("CODEX_HOME", &codex_home)
        .env("OPENAI_API_KEY", "sk-must-not-travel")
        .env("AWS_SECRET_ACCESS_KEY", "must-not-travel")
        .env("AWS_REGION", "us-east-1")
        .env("DEPOT_TOKEN", "must-not-travel");
    run(cmd, &scratch, "run");
    wait_for_broker(&home);

    let child_env: Vec<(String, String)> = until("the child to dump its environment", || {
        let text = std::fs::read_to_string(&dump).ok()?;
        let pairs: Vec<(String, String)> = text
            .lines()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        (!pairs.is_empty()).then_some(pairs)
    });
    let get = |key: &str| {
        child_env
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    };

    assert_eq!(
        get("OPENAI_API_KEY"),
        None,
        "the scrub list wins: {child_env:?}"
    );
    assert_eq!(get("DEPOT_TOKEN"), None, "the scrub list wins");
    assert!(
        !child_env.iter().any(|(k, _)| k.starts_with("AWS_")),
        "AWS_* is a prefix scrub: {child_env:?}"
    );
    assert_eq!(
        get("CODEX_HOME"),
        Some(codex_home.to_string_lossy().into_owned()),
        "the allowlist keeps CODEX_HOME"
    );
    assert!(get("PATH").is_some(), "the allowlist keeps PATH");
    assert_eq!(
        get("A2A_ALLOWED_ROOTS"),
        None,
        "the daemon's own config does not travel"
    );

    // ... and the daemon itself kept what the allowlist would have stripped.
    let status = run(
        a2a(&home, tmp.path(), &["--json", "broker", "status"]),
        &scratch,
        "status",
    );
    let d = status.data();
    assert_eq!(
        d["codex_bin"],
        json!(common::fake_app_server_path()),
        "the daemon read A2A_CODEX_BIN: {d}"
    );
    assert_eq!(
        d["allowed_roots"],
        json!([tmp.path().canonicalize().unwrap()]),
        "the daemon read A2A_ALLOWED_ROOTS: {d}"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

// ---------------------------------------------------------------------------
// source-scan gates (Task 10)
// ---------------------------------------------------------------------------
//
// Three properties of the async half of `a2a` that no runtime test can catch,
// because their failure mode is a rare interleaving rather than a wrong answer:
// a lock where a message belongs, a `write_all` a cancelled future can abandon
// half-written, and a blocking sleep on a reactor thread. Each is cheap to
// avoid and expensive to find later, so they are checked as source.
//
// The scan reads CODE, not comments: this very file's neighbours document the
// forbidden constructs by name, and a gate that cannot tell a rule from its
// violation is a gate nobody keeps.

/// Every file the async discipline binds: the broker's actor tree, plus the
/// JSON-RPC codec underneath it.
fn async_sources() -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<PathBuf> = std::fs::read_dir(root.join("src/broker"))
        .expect("src/broker exists")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "rs").unwrap_or(false))
        .collect();
    files.extend(
        std::fs::read_dir(root.join("src/mesh/gateway"))
            .expect("src/mesh/gateway exists")
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "rs").unwrap_or(false)),
    );
    files.push(root.join("src/rpc.rs"));
    files.sort();
    assert!(
        files.len() >= 7,
        "the scan lost track of the broker's modules: {files:?}"
    );
    files
        .into_iter()
        .map(|p| {
            let text = std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{p:?}: {e}"));
            let name = p
                .strip_prefix(root)
                .unwrap_or(&p)
                .to_string_lossy()
                .into_owned();
            (name, code_only(&text))
        })
        .collect()
}

/// `src` with every comment blanked out, string literals (raw ones included)
/// left alone. Newlines are preserved so a hit can still be located by line.
fn code_only(src: &str) -> String {
    let b: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < b.len() {
        // line comment
        if b[i] == '/' && i + 1 < b.len() && b[i + 1] == '/' {
            while i < b.len() && b[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // block comment (nesting, as rustc does)
        if b[i] == '/' && i + 1 < b.len() && b[i + 1] == '*' {
            let mut depth = 1;
            i += 2;
            while i < b.len() && depth > 0 {
                if b[i] == '/' && i + 1 < b.len() && b[i + 1] == '*' {
                    depth += 1;
                    i += 2;
                } else if b[i] == '*' && i + 1 < b.len() && b[i + 1] == '/' {
                    depth -= 1;
                    i += 2;
                } else {
                    if b[i] == '\n' {
                        out.push('\n');
                    }
                    i += 1;
                }
            }
            continue;
        }
        // raw string: r"…", r#"…"#, br##"…"##
        if b[i] == 'r' || (b[i] == 'b' && i + 1 < b.len() && b[i + 1] == 'r') {
            let start = i;
            let mut j = if b[i] == 'b' { i + 2 } else { i + 1 };
            let mut hashes = 0;
            while j < b.len() && b[j] == '#' {
                hashes += 1;
                j += 1;
            }
            if j < b.len() && b[j] == '"' {
                out.extend(&b[start..=j]);
                i = j + 1;
                loop {
                    if i >= b.len() {
                        break;
                    }
                    if b[i] == '"' && b[i + 1..].iter().take(hashes).all(|c| *c == '#') {
                        out.push('"');
                        i += 1 + hashes;
                        break;
                    }
                    out.push(b[i]);
                    i += 1;
                }
                continue;
            }
        }
        // ordinary string
        if b[i] == '"' {
            out.push('"');
            i += 1;
            while i < b.len() {
                if b[i] == '\\' && i + 1 < b.len() {
                    out.push(b[i]);
                    out.push(b[i + 1]);
                    i += 2;
                    continue;
                }
                out.push(b[i]);
                i += 1;
                if b[i - 1] == '"' {
                    break;
                }
            }
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    out
}

/// Every `select!` block in `code`, as (line number, block text).
fn select_blocks(code: &str) -> Vec<(usize, String)> {
    let chars: Vec<char> = code.chars().collect();
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(found) = code[at..].find("select!") {
        let start = at + found;
        let line = code[..start].matches('\n').count() + 1;
        // Byte offset → char index, so brace matching walks characters.
        let char_start = code[..start].chars().count();
        let mut i = char_start;
        while i < chars.len() && chars[i] != '{' {
            i += 1;
        }
        let mut depth = 0;
        let block_start = i;
        while i < chars.len() {
            match chars[i] {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        out.push((
            line,
            chars[block_start..=i.min(chars.len() - 1)].iter().collect(),
        ));
        at = start + "select!".len();
    }
    out
}

/// The registry is reached by messages. A lock in `src/broker/` would be a
/// second owner of state that has exactly one — and the bugs that follow are
/// interleavings, not wrong answers.
#[test]
fn the_broker_holds_no_shared_locks() {
    for (name, code) in async_sources() {
        for (n, line) in code.lines().enumerate() {
            assert!(
                !line.contains("Arc<Mutex") && !line.contains("Arc<RwLock"),
                "{name}:{}: shared lock in the actor tree — send a message instead:\n{line}",
                n + 1
            );
        }
    }
}

/// A `write_all` inside a `select!` is a half-written line waiting for the
/// first cancellation: every write goes through a dedicated writer task fed by
/// a bounded channel.
#[test]
fn no_write_all_inside_a_select() {
    let mut checked = 0;
    for (name, code) in async_sources() {
        for (line, block) in select_blocks(&code) {
            checked += 1;
            assert!(
                !block.contains("write_all"),
                "{name}:{line}: a write inside a select! can be cancelled half-done:\n{block}"
            );
        }
    }
    assert!(checked >= 5, "the select! scan found nothing to scan");
}

/// A blocking sleep on a reactor thread stalls every task that thread owns.
/// `broker_cmd.rs` is deliberately out of scope: it is a SYNC command that
/// waits on a flock before the runtime exists.
#[test]
fn no_blocking_sleep_in_the_async_modules() {
    for (name, code) in async_sources() {
        for (n, line) in code.lines().enumerate() {
            assert!(
                !line.contains("std::thread::sleep") && !line.contains("thread::sleep("),
                "{name}:{}: a blocking sleep on a reactor thread:\n{line}",
                n + 1
            );
        }
    }
}

/// The gates are only worth having if they can see a violation: each pattern is
/// checked against a sample the scanner must reject, and against the comment
/// that describes it, which it must not.
#[test]
fn the_scan_reads_code_and_not_comments() {
    let sample = r##"
//! A doc comment naming Arc<Mutex<T>> and std::thread::sleep.
/* a block comment with write_all inside a select! */
fn f() {
    let banner = "select! { write_all }";  // a string is not code either
    let raw = r#"Arc<Mutex<Fake>>"#;
}
"##;
    let code = code_only(sample);
    assert!(
        !code.contains("Arc<Mutex<T>>") && !code.contains("std::thread::sleep"),
        "comments must not be scanned: {code}"
    );
    assert!(
        code.contains("let banner"),
        "code around the comments survives: {code}"
    );
    // ... and the positive control: real code IS seen.
    let real = code_only("fn g() { let x: Arc<Mutex<u8>> = todo!(); }");
    assert!(real.contains("Arc<Mutex"), "{real}");
    let blocks = select_blocks(&code_only(
        "async fn h() { tokio::select! { _ = a => { w.write_all(b).await; } } }",
    ));
    assert_eq!(blocks.len(), 1);
    assert!(blocks[0].1.contains("write_all"));
}

// ---------------------------------------------------------------------------
// Task 11: `a2a spawn` via the broker — transport auto-selection
// ---------------------------------------------------------------------------
//
// Everything below is the SHIPPED `a2a spawn`, from the outside: the CLI picks a
// transport, the broker re-validates what the CLI sent it, and the worker's own
// files are the evidence. The conventions of this file still hold — a real
// daemon per test, a `BrokerGuard` per home, converged-state polling only.

/// A worker's `state.json`, or `None` while it is not there yet.
fn snap_of(home: &Path, id: &str) -> Option<Value> {
    let text = std::fs::read_to_string(home.join("workers").join(id).join("state.json")).ok()?;
    serde_json::from_str(&text).ok()
}

/// Poll a worker's snapshot until it reports `want`.
fn until_state(home: &Path, id: &str, want: &str) -> Value {
    until(&format!("worker {id} to reach `{want}`"), || {
        snap_of(home, id).filter(|s| s["state"] == json!(want))
    })
}

/// A worker's snapshot bytes, once they have stopped changing.
///
/// Two identical reads a poll apart: the broker owns this file, so the only way
/// to attribute a later change to somebody else is to start from a moment when
/// its owner had finished with it.
fn until_quiescent(state_file: &Path) -> Vec<u8> {
    until("the broker to stop writing the worker's snapshot", || {
        let first = std::fs::read(state_file).ok()?;
        std::thread::sleep(POLL * 3);
        let second = std::fs::read(state_file).ok()?;
        (first == second).then_some(second)
    })
}

fn events_of(home: &Path, id: &str) -> Vec<Value> {
    std::fs::read_to_string(home.join("workers").join(id).join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

fn kinds_of(home: &Path, id: &str) -> Vec<String> {
    events_of(home, id)
        .iter()
        .filter_map(|e| e["kind"].as_str().map(str::to_string))
        .collect()
}

fn write_brief(dir: &Path, body: &str) -> PathBuf {
    let p = dir.join("the-brief.md");
    std::fs::write(&p, body).unwrap();
    p
}

/// **The convergence gate for steering (Task 12).** Poll the shipped
/// `a2a --json tail` until the worker's WAL carries a `turn_started` envelope,
/// so a `send` that follows is aimed at a turn that demonstrably exists. ≤10s
/// at 50ms — never an instant assertion on an async turn.
fn until_turn_started(home: &Path, allowed: &Path, scratch: &Path, id: &str) {
    let deadline = Instant::now() + CONVERGE;
    loop {
        let tailed = run(a2a(home, allowed, &["--json", "tail", id]), scratch, "gate");
        if tailed.code == 0
            && tailed.data()["events"]
                .as_array()
                .is_some_and(|es| es.iter().any(|e| e["kind"] == json!("turn_started")))
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for a turn_started envelope for {id}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Files (not directories) directly under `inbox/` — the messages that are
/// queued but not yet acknowledged.
fn inbox_files(home: &Path, id: &str) -> usize {
    std::fs::read_dir(home.join("workers").join(id).join("inbox"))
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                .count()
        })
        .unwrap_or(0)
}

fn delivered_files(home: &Path, id: &str) -> usize {
    std::fs::read_dir(home.join("workers").join(id).join("inbox/delivered"))
        .map(|entries| entries.flatten().count())
        .unwrap_or(0)
}

/// The `inbox_delivered` rows a worker's WAL carries, newest last.
fn deliveries_of(home: &Path, id: &str) -> Vec<Value> {
    events_of(home, id)
        .into_iter()
        .filter(|e| e["kind"] == json!("inbox_delivered"))
        .collect()
}

/// A client that speaks the wire by hand — the only way to send the broker a
/// frame the CLI would never send (an out-of-roots cwd, for one).
struct Raw {
    stream: std::os::unix::net::UnixStream,
    reader: std::io::BufReader<std::os::unix::net::UnixStream>,
}

impl Raw {
    fn hello(home: &Path) -> Raw {
        let stream = std::os::unix::net::UnixStream::connect(socket(home))
            .expect("the broker socket accepts a peer of our own uid");
        stream.set_read_timeout(Some(CONVERGE)).unwrap();
        let reader = std::io::BufReader::new(stream.try_clone().unwrap());
        let mut raw = Raw { stream, reader };
        raw.send(&json!({"v": 1, "id": 0, "type": "hello",
                         "body": {"protocol_version": 1, "binary_version": "test"}}));
        let reply = raw.recv().expect("the broker answers a hello");
        assert_eq!(reply["type"], json!("hello"), "handshake: {reply}");
        raw
    }

    fn send(&mut self, frame: &Value) {
        writeln!(self.stream, "{frame}").unwrap();
        self.stream.flush().unwrap();
    }

    fn req(&mut self, id: u64, op: &str, params: Value) -> Value {
        let mut body = json!({"op": op});
        if let (Some(into), Some(from)) = (body.as_object_mut(), params.as_object()) {
            for (k, v) in from {
                into.insert(k.clone(), v.clone());
            }
        }
        self.send(&json!({"v": 1, "id": id, "type": "req", "body": body}));
        self.recv().expect("the broker answers a request")
    }

    fn recv(&mut self) -> Option<Value> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(serde_json::from_str(&line).expect("every frame is one JSON object")),
        }
    }
}

/// `--transport auto` with a reachable broker: the app-server transport, the
/// whole lifecycle on the fake, and a snapshot that can be thrown away.
#[test]
fn spawn_auto_selects_the_app_server_and_the_worker_runs_to_done() {
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
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_MESSAGE", "THE ANSWER")
        .env("A2A_OWNER", "test-owner");
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let d = spawned.data();
    assert_eq!(
        d["transport"],
        json!("app-server"),
        "auto chose the broker: {d}"
    );
    let id = d["id"].as_str().expect("an id").to_string();

    // The daemon came up because the CLI needed it — nobody ran `broker run`.
    assert!(socket(&home).exists(), "auto autostarted a broker");

    let waited = run(a2a(&home, tmp.path(), &["wait", &id]), &scratch, "wait");
    assert_eq!(waited.code, 0, "wait: {}", waited.stderr);

    let done = until_state(&home, &id, "done");
    assert_eq!(done["transport"], json!("app-server"));
    assert_eq!(
        done["requested_transport"],
        json!("app-server"),
        "requested = the resolved transport when none was named: {done}"
    );
    assert_eq!(done["transport_selection"], json!("auto"), "{done}");
    assert_eq!(done["owner"], json!("test-owner"), "{done}");
    let thread_id = done["thread_id"]
        .as_str()
        .expect("a thread of record")
        .to_string();
    assert_eq!(
        done["session_id"],
        json!(thread_id),
        "thread.id IS the session id on 0.140.0: {done}"
    );
    assert_eq!(
        done["resolved_sandbox"]["type"],
        json!("workspaceWrite"),
        "the sandbox the server resolved is recorded: {done}"
    );
    assert!(
        done.get("fold_rejections").is_none(),
        "a happy path folds cleanly: {done}"
    );

    let result = run(
        a2a(&home, tmp.path(), &["--json", "result", &id]),
        &scratch,
        "result",
    );
    assert_eq!(result.code, 0);
    assert_eq!(
        result.data()["result"],
        json!("THE ANSWER"),
        "the final agent message IS the result on this transport"
    );

    // Lossless rebuild: the checkpoint is derived, the WAL is the truth.
    std::fs::remove_file(home.join("workers").join(&id).join("state.json")).unwrap();
    let rebuilt = run(
        a2a(&home, tmp.path(), &["--json", "status", &id]),
        &scratch,
        "rebuilt",
    );
    assert_eq!(
        rebuilt.code, 0,
        "status after losing state.json: {}",
        rebuilt.stderr
    );
    let r = rebuilt.data();
    assert_eq!(r["state"], json!("done"), "{r}");
    assert_eq!(
        r["owner"],
        json!("test-owner"),
        "owner survives a rebuild: {r}"
    );
    assert_eq!(
        r["thread_id"],
        json!(thread_id),
        "thread_id survives a rebuild: {r}"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// `A2A_BROKER_DISABLE=1` is the Phase-1 escape hatch, and it is absolute: a
/// broker can be RUNNING and reachable and the spawn still never speaks to it.
#[test]
fn spawn_auto_stays_on_exec_when_the_broker_is_disabled_even_with_one_running() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");

    run(a2a(&home, tmp.path(), &["broker", "run"]), &scratch, "run");
    wait_for_broker(&home);

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("A2A_BROKER_DISABLE", "1")
        .env("A2A_CODEX_BIN", common::stub_path());
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let d = spawned.data();
    assert_eq!(d["transport"], json!("exec"), "the disable seam wins: {d}");
    let id = d["id"].as_str().unwrap().to_string();

    // An exec worker is advanced by whoever reads it (Phase 1's rule, intact).
    let waited = run(a2a(&home, tmp.path(), &["wait", &id]), &scratch, "wait");
    assert_eq!(waited.code, 0, "wait: {}", waited.stderr);
    let snap = until_state(&home, &id, "done");
    assert_eq!(snap["transport"], json!("exec"));
    assert_eq!(snap["requested_transport"], json!("exec"));
    assert_eq!(snap["transport_selection"], json!("auto"));
    assert!(
        !kinds_of(&home, &id).contains(&"transport_degraded".to_string()),
        "a configured exec spawn is not a degradation: {:?}",
        kinds_of(&home, &id)
    );

    // The proof the socket was never touched: the live broker never heard of it.
    let list = run(
        a2a(&home, tmp.path(), &["--json", "__broker-call", "list"]),
        &scratch,
        "list",
    );
    assert_eq!(
        list.data()["workers"],
        json!([]),
        "a disabled spawn does not register with the running broker"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The blocked lifecycle, and the ownership rule that comes with it: while the
/// broker is alive the CLI's reconcile is READ-ONLY.
#[test]
fn a_blocked_app_server_worker_parks_awaiting_input_and_the_cli_reconcile_is_read_only() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "A or B?\n");

    // The research lane is outbox-only, so its thread is rooted on the WORKER
    // DIR — which is where the fake writes the question.
    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "spawn", "--lane", "research", "--brief"],
    );
    cmd.arg(&brief).env("FAKE_SCENARIO", "blocked");
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().unwrap().to_string();

    let parked = until_state(&home, &id, "awaiting-input");
    assert_eq!(parked["state_reason"], json!("blocked"), "{parked}");
    assert!(
        home.join("workers")
            .join(&id)
            .join("outbox/1.question.json")
            .exists(),
        "the worker asked its question through the outbox"
    );

    // "The CLI wrote nothing" is only observable against a worker its OWNER has
    // stopped writing: the broker is asynchronous, and a frame still in flight
    // when the bytes were sampled would look exactly like a CLI write.
    let state_file = home.join("workers").join(&id).join("state.json");
    let before = until_quiescent(&state_file);
    let waited = run(
        a2a(&home, tmp.path(), &["wait", &id, "--until", "blocked"]),
        &scratch,
        "wait",
    );
    assert_eq!(waited.code, 5, "blocked is exit 5: {}", waited.stderr);
    let status = run(
        a2a(&home, tmp.path(), &["--json", "status", &id]),
        &scratch,
        "status",
    );
    assert_eq!(
        status.code, 5,
        "status of a parked worker: {}",
        status.stderr
    );
    assert_eq!(
        std::fs::read(&state_file).unwrap(),
        before,
        "the broker owns this worker's transitions — the CLI read it and wrote nothing"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// Defense in depth (spec §5): the socket must not out-privilege the CLI, so
/// the broker re-validates the cwd it is handed against ITS OWN allowed roots —
/// and creates nothing when it refuses.
#[test]
fn the_broker_revalidates_the_wire_cwd_against_its_own_allowed_roots() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let allowed = tmp.path().join("allowed");
    let elsewhere = tmp.path().join("elsewhere");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&allowed).unwrap();
    std::fs::create_dir_all(&elsewhere).unwrap();
    let resolved = a2a::home::A2aHome::resolve(Some(&home)).unwrap();
    let work_repo = resolved.root().join("work").join("work-1").join("repo");
    std::fs::create_dir_all(&work_repo).unwrap();
    let guard = common::BrokerGuard::new(&home);

    // The daemon's allowlist is `allowed/` and nothing else.
    run(a2a(&home, &allowed, &["broker", "run"]), &scratch, "run");
    wait_for_broker(&home);
    let status = run(
        a2a(&home, &allowed, &["--json", "broker", "status"]),
        &scratch,
        "status-work-root",
    );
    let roots = status.data()["allowed_roots"].as_array().unwrap().clone();
    assert!(
        roots.contains(&json!(resolved.root().join("work").canonicalize().unwrap())),
        "broker should start with the implicit mesh work root: {roots:?}"
    );

    let ghost = home.join("workers").join("general-0817t1200-ghos");
    let mut raw = Raw::hello(&home);
    let refused = raw.req(
        1,
        "spawn",
        json!({"dir": ghost, "cwd": elsewhere, "lane": "general"}),
    );
    assert_eq!(refused["type"], json!("err"), "{refused}");
    assert_eq!(
        refused["body"]["code"],
        json!("CWD_NOT_ALLOWED"),
        "{refused}"
    );
    assert_eq!(refused["body"]["exit"], json!(9), "{refused}");
    assert!(!ghost.exists(), "a refused spawn creates nothing");

    let admitted = raw.req(
        2,
        "admit",
        json!({"dir": ghost, "cwd": work_repo, "lane": "general"}),
    );
    assert_eq!(admitted["type"], json!("resp"), "{admitted}");
    assert_eq!(admitted["body"]["decision"], json!("admit"), "{admitted}");

    // ... and the research rule is re-run too: research runs in its own scratch
    // dir, never in a repo, whoever is asking.
    let research = home.join("workers").join("research-0817t1200-rsch");
    let refused = raw.req(
        3,
        "spawn",
        json!({"dir": research, "cwd": allowed, "lane": "research"}),
    );
    assert_eq!(
        refused["body"]["code"],
        json!("CWD_NOT_ALLOWED"),
        "{refused}"
    );
    assert!(
        !research.exists(),
        "a refused research spawn creates nothing"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// A broker that cannot be started at all: `auto` degrades (and says so in the
/// WAL), while an explicit `--transport app-server` is exit 7 — reachability,
/// never the static exit 9.
#[test]
fn a_broker_that_will_not_start_degrades_auto_to_exec_and_hard_fails_explicit_app_server() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");

    // A "broker binary" that exits immediately: autostart succeeds, the daemon
    // never appears, and the bounded retry-connect is what notices.
    let dud = tmp.path().join("dud-broker");
    std::fs::write(&dud, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&dud, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("A2A_BROKER_BIN", &dud)
        .env("A2A_CODEX_BIN", common::stub_path());
    let spawned = run(cmd, &scratch, "auto");
    assert_eq!(
        spawned.code, 0,
        "auto degrades rather than failing: {}",
        spawned.stderr
    );
    let id = spawned.data()["id"].as_str().unwrap().to_string();
    assert_eq!(spawned.data()["transport"], json!("exec"));

    let waited = run(a2a(&home, tmp.path(), &["wait", &id]), &scratch, "wait");
    assert_eq!(waited.code, 0, "wait: {}", waited.stderr);
    let done = until_state(&home, &id, "done");
    assert_eq!(done["transport"], json!("exec"));
    assert_eq!(done["requested_transport"], json!("exec"), "{done}");
    let degraded: Vec<Value> = events_of(&home, &id)
        .into_iter()
        .filter(|e| e["kind"] == json!("transport_degraded"))
        .collect();
    assert_eq!(
        degraded.len(),
        1,
        "the degradation is an event: {:?}",
        kinds_of(&home, &id)
    );
    assert_eq!(
        degraded[0]["raw"]["requested"],
        json!("app-server"),
        "{}",
        degraded[0]
    );
    assert_eq!(
        degraded[0]["raw"]["selected"],
        json!("exec"),
        "{}",
        degraded[0]
    );

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &[
            "--json",
            "spawn",
            "--lane",
            "general",
            "--transport",
            "app-server",
            "--cwd",
        ],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("A2A_BROKER_BIN", &dud)
        .env("A2A_CODEX_BIN", common::stub_path());
    let hard = run(cmd, &scratch, "explicit");
    assert_eq!(
        hard.code, 7,
        "explicit app-server is a reachability failure: {}",
        hard.stderr
    );
    assert_eq!(
        hard.json()["error"]["code"],
        json!("BROKER_UNAVAILABLE"),
        "{}",
        hard.stdout
    );
    // An unknown transport is the one that stays a static precondition.
    let mut cmd = a2a(
        &home,
        tmp.path(),
        &[
            "--json",
            "spawn",
            "--lane",
            "general",
            "--transport",
            "carrier-pigeon",
            "--cwd",
        ],
    );
    cmd.arg(&work).arg("--brief").arg(&brief);
    let unknown = run(cmd, &scratch, "unknown");
    assert_eq!(unknown.code, 9, "an unknown transport string is exit 9");
    assert_eq!(unknown.json()["error"]["code"], json!("TRANSPORT_UNKNOWN"));

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// Spec §4.2: the broker pins its codex at start and re-checks on every spawn.
/// A binary that changed underneath it (nvm switch, `codex update`) refuses new
/// work rather than mixing versions against one session store.
#[test]
fn a_codex_binary_that_drifted_under_the_broker_refuses_new_spawns() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");

    // A codex whose `--version` is whatever the version file says today.
    let version_file = tmp.path().join("codex-version.txt");
    std::fs::write(&version_file, "codex-cli 0.140.0\n").unwrap();
    let wrapper = tmp.path().join("codex-wrapper");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then cat \"$A2A_TEST_VERSION_FILE\"; exit 0; fi\nexec {} \"$@\"\n",
            common::fake_app_server_path().display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cmd = a2a(&home, tmp.path(), &["broker", "run"]);
    cmd.env("A2A_CODEX_BIN", &wrapper)
        .env("A2A_TEST_VERSION_FILE", &version_file);
    run(cmd, &scratch, "run");
    wait_for_broker(&home);
    // The socket is bound before the core finishes starting, and the codex
    // identity is pinned inside that window — so the version must not move until
    // the daemon is ANSWERING, or the test would be racing the thing it pins.
    until("the broker to finish starting", || {
        let up = run(
            a2a(&home, tmp.path(), &["--json", "broker", "status"]),
            &scratch,
            "up",
        );
        (up.code == 0 && up.data()["child_pid"].is_u64()).then_some(())
    });

    // The world moves: same path, different binary.
    std::fs::write(&version_file, "codex-cli 0.147.0\n").unwrap();

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("A2A_CODEX_BIN", &wrapper)
        .env("A2A_TEST_VERSION_FILE", &version_file);
    let drifted = run(cmd, &scratch, "drift");
    assert_ne!(
        drifted.code, 0,
        "a drifted codex refuses the spawn: {}",
        drifted.stdout
    );
    let err = drifted.json()["error"].clone();
    assert_eq!(err["code"], json!("CODEX_DRIFT"), "{err}");
    assert!(
        err["remedy"]
            .as_str()
            .unwrap_or_default()
            .contains("a2a broker restart"),
        "the refusal names the way out: {err}"
    );
    assert!(
        err["message"]
            .as_str()
            .unwrap_or_default()
            .contains("0.147.0"),
        "the refusal names what it found: {err}"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The broker died with work in flight. The CLI may then — and only then —
/// commit `orphaned`; and a kill on this transport NEVER signals the recorded
/// pid, because that pid is the app-server child every worker shares.
#[test]
fn a_dead_broker_orphans_its_workers_and_kill_never_signals_the_shared_child() {
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
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_SCENARIO", "hang-turn");
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().unwrap().to_string();
    until_state(&home, &id, "running");

    // Two more workers the dead broker leaves behind, in the two states the
    // plan pins: a spawned one never got a thread, a queued one was never
    // admitted and is the NEXT broker's to re-enqueue.
    seed_worker(
        &home,
        &work,
        "general-0817t1210-neve",
        json!({"state": "spawned", "transport": "app-server"}),
    );
    seed_worker(
        &home,
        &work,
        "general-0817t1211-queu",
        json!({"state": "queued", "transport": "app-server"}),
    );

    let broker_pid = read_pidfile(&home).expect("a pidfile");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(broker_pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    until("the broker to be gone", || {
        (!alive(broker_pid)).then_some(())
    });
    // A crashed daemon leaves its address behind; nobody else will remove it.
    let _ = std::fs::remove_file(socket(&home));

    // A process this test owns, standing in for the shared app-server child the
    // dead broker used to own — a `kill` that signalled it would be the bug.
    let (mut stand_in, pid, witness) = stand_in_child();
    let state_file = home.join("workers").join(&id).join("state.json");
    let mut snap: Value =
        serde_json::from_str(&std::fs::read_to_string(&state_file).unwrap()).unwrap();
    snap["pid"] = json!(pid);
    snap["pid_start_witness"] = json!(witness);
    a2a::store::write_atomic(&state_file, snap.to_string().as_bytes()).unwrap();

    let status = run(
        a2a(&home, tmp.path(), &["--json", "status", &id]),
        &scratch,
        "orphan",
    );
    assert_eq!(
        status.code, 10,
        "a live worker with no broker is orphaned: {}",
        status.stderr
    );
    assert_eq!(status.data()["state"], json!("orphaned"));
    assert!(
        snap_of(&home, &id)
            .unwrap()
            .get("fold_rejections")
            .is_none(),
        "the orphan commit is a legal row"
    );

    let never = run(
        a2a(
            &home,
            tmp.path(),
            &["--json", "status", "general-0817t1210-neve"],
        ),
        &scratch,
        "never",
    );
    assert_eq!(
        never.code, 4,
        "a spawned worker whose broker died never started"
    );
    assert_eq!(never.data()["state"], json!("failed"));
    assert_eq!(never.data()["failure_class"], json!("never_started"));

    let queued = run(
        a2a(
            &home,
            tmp.path(),
            &["--json", "status", "general-0817t1211-queu"],
        ),
        &scratch,
        "queued",
    );
    assert_eq!(queued.code, 0);
    assert_eq!(
        queued.data()["state"],
        json!("queued"),
        "a queued worker is left for the next broker to re-enqueue"
    );

    let killed = run(
        a2a(&home, tmp.path(), &["--json", "kill", &id]),
        &scratch,
        "kill",
    );
    assert_eq!(killed.code, 0, "kill: {}", killed.stderr);
    assert_eq!(killed.data()["state"], json!("cancelled"));
    assert_eq!(
        killed.data()["signal"],
        json!("no_signal"),
        "the app-server child is every worker's — kill routes through the broker, never a signal"
    );
    assert!(
        stand_in.try_wait().unwrap().is_none(),
        "the shared child's stand-in was signalled by a worker-scoped kill"
    );
    let _ = stand_in.kill();
    let _ = stand_in.wait();

    drop(guard);
    common::assert_no_broker_left(&home);
}

// ---------------------------------------------------------------------------
// send, per transport (Task 12)
// ---------------------------------------------------------------------------

/// The live transport's headline: a message reaches a worker that is **already
/// mid-turn**, through `turn/steer`, and the WAL says which door it went
/// through.
///
/// The `hang-turn` fake holds its turn open until it is steered, so "the turn
/// was live" is a fact the gate below establishes rather than a hope — and the
/// worker converging to `done` afterwards is proof the steer landed.
#[test]
fn send_steers_a_running_app_server_worker_mid_turn() {
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
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_SCENARIO", "hang-turn");
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    assert_eq!(spawned.data()["transport"], json!("app-server"));
    let id = spawned.data()["id"].as_str().unwrap().to_string();

    until_state(&home, &id, "running");
    until_turn_started(&home, tmp.path(), &scratch, &id);

    let sent = run(
        a2a(&home, tmp.path(), &["--json", "send", &id, "use option B"]),
        &scratch,
        "send",
    );
    assert_eq!(sent.code, 0, "send: {}", sent.stderr);
    let d = sent.data();
    assert_eq!(d["queued"], json!(true), "{d}");
    assert_eq!(
        d["delivered"],
        json!(true),
        "a live turn takes the message now: {d}"
    );
    assert_eq!(
        d["send_path"],
        json!("steered"),
        "mid-turn is the steer path: {d}"
    );

    // The event is the durable half of the same claim.
    let deliveries = deliveries_of(&home, &id);
    assert_eq!(deliveries.len(), 1, "one delivery: {deliveries:?}");
    assert_eq!(
        deliveries[0]["raw"]["send_path"],
        json!("steered"),
        "{}",
        deliveries[0]
    );
    assert_eq!(
        deliveries[0]["raw"]["seqs"],
        json!([1]),
        "{}",
        deliveries[0]
    );
    assert_eq!(inbox_files(&home, &id), 0, "the message was acknowledged");
    assert_eq!(delivered_files(&home, &id), 1);

    // The steered turn completes, so the worker finishes — which is only
    // possible if the steer actually reached the held-open turn.
    until_state(&home, &id, "done");

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// Off a turn boundary there is no turn to steer, so the same command opens the
/// answer turn instead — one `send`, two transports of one idea.
#[test]
fn send_to_a_parked_app_server_worker_starts_the_answer_turn() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let scratch = tmp.path().join("scratch");
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "A or B?\n");

    // The research lane is outbox-only, so the fake's question lands in the
    // worker dir it was handed as `cwd`.
    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "spawn", "--lane", "research", "--brief"],
    );
    cmd.arg(&brief).env("FAKE_SCENARIO", "blocked");
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().unwrap().to_string();

    let parked = until_state(&home, &id, "awaiting-input");
    assert_eq!(parked["state_reason"], json!("blocked"), "{parked}");

    let sent = run(
        a2a(&home, tmp.path(), &["--json", "send", &id, "option B"]),
        &scratch,
        "send",
    );
    assert_eq!(sent.code, 0, "send: {}", sent.stderr);
    let d = sent.data();
    assert_eq!(d["delivered"], json!(true), "{d}");
    assert_eq!(
        d["send_path"],
        json!("started"),
        "no open turn means a new one, not a steer: {d}"
    );

    let deliveries = deliveries_of(&home, &id);
    assert_eq!(deliveries.len(), 1, "{deliveries:?}");
    assert_eq!(
        deliveries[0]["raw"]["send_path"],
        json!("started"),
        "{}",
        deliveries[0]
    );
    assert_eq!(inbox_files(&home, &id), 0, "the answer was acknowledged");

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The regression pin: routing keys off the WORKER's transport, not off whether
/// a broker happens to be reachable. An exec worker with a live broker in the
/// same home still takes the Phase-1 queue path — running means queue-only, and
/// no `send_path` is claimed, because nothing was steered.
#[test]
fn send_to_a_running_exec_worker_keeps_the_phase_one_queue_path() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);
    let brief = write_brief(tmp.path(), "do the thing\n");

    // A broker is up and reachable for the whole test; the exec worker below
    // must never reach it.
    run(a2a(&home, tmp.path(), &["broker", "run"]), &scratch, "run");
    wait_for_broker(&home);

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &[
            "--json",
            "spawn",
            "--lane",
            "general",
            "--transport",
            "exec",
            "--cwd",
        ],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("A2A_CODEX_BIN", common::stub_path())
        .env("STUB_SLEEP", "5");
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    assert_eq!(spawned.data()["transport"], json!("exec"));
    let id = spawned.data()["id"].as_str().unwrap().to_string();
    until_state(&home, &id, "running");

    let mut cmd = a2a(
        &home,
        tmp.path(),
        &["--json", "send", &id, "fyi: keep going"],
    );
    cmd.env("A2A_CODEX_BIN", common::stub_path());
    let sent = run(cmd, &scratch, "send");
    assert_eq!(sent.code, 0, "send: {}", sent.stderr);
    let d = sent.data();
    assert_eq!(d["queued"], json!(true), "{d}");
    assert_eq!(
        d["delivered"],
        json!(false),
        "exec has no way into a live turn: the message waits for the boundary: {d}"
    );
    assert!(
        d.get("send_path").is_none(),
        "nothing was steered, so nothing claims a steer path: {d}"
    );
    assert_eq!(
        inbox_files(&home, &id),
        1,
        "the message is queued, not delivered"
    );
    assert!(
        deliveries_of(&home, &id).is_empty(),
        "no delivery was recorded: {:?}",
        kinds_of(&home, &id)
    );
    // The broker never heard of it, either.
    let list = run(
        a2a(&home, tmp.path(), &["--json", "__broker-call", "list"]),
        &scratch,
        "list",
    );
    assert_eq!(
        list.data()["workers"],
        json!([]),
        "an exec worker is not the broker's: {}",
        list.stdout
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// `--queue-only` is unchanged by the live transport: still a pure enqueue,
/// still no delivery — and the next plain `send` carries the whole backlog into
/// one steered turn, in seq order, exactly as the exec transport batches it.
#[test]
fn queue_only_stays_a_pure_enqueue_and_the_next_send_carries_the_backlog() {
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
        &["--json", "spawn", "--lane", "general", "--cwd"],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_SCENARIO", "hang-turn");
    let spawned = run(cmd, &scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let id = spawned.data()["id"].as_str().unwrap().to_string();
    until_state(&home, &id, "running");
    until_turn_started(&home, tmp.path(), &scratch, &id);

    for (tag, text) in [("q1", "part one"), ("q2", "part two")] {
        let queued = run(
            a2a(
                &home,
                tmp.path(),
                &["--json", "send", &id, text, "--queue-only"],
            ),
            &scratch,
            tag,
        );
        assert_eq!(queued.code, 0, "{tag}: {}", queued.stderr);
        assert_eq!(
            queued.data()["delivered"],
            json!(false),
            "{}",
            queued.stdout
        );
        assert!(
            queued.data().get("send_path").is_none(),
            "{}",
            queued.stdout
        );
    }
    assert_eq!(
        inbox_files(&home, &id),
        2,
        "two staged messages, nothing delivered"
    );
    assert!(
        deliveries_of(&home, &id).is_empty(),
        "--queue-only delivers nothing: {:?}",
        kinds_of(&home, &id)
    );
    assert_eq!(
        snap_of(&home, &id).unwrap()["state"],
        json!("running"),
        "a staged message does not end the turn"
    );

    let sent = run(
        a2a(&home, tmp.path(), &["--json", "send", &id, "part three"]),
        &scratch,
        "send",
    );
    assert_eq!(sent.code, 0, "send: {}", sent.stderr);
    assert_eq!(
        sent.data()["send_path"],
        json!("steered"),
        "{}",
        sent.stdout
    );

    let deliveries = deliveries_of(&home, &id);
    assert_eq!(
        deliveries.len(),
        1,
        "one delivery carried all three: {deliveries:?}"
    );
    assert_eq!(
        deliveries[0]["raw"]["seqs"],
        json!([1, 2, 3]),
        "the backlog travels in seq order: {}",
        deliveries[0]
    );
    assert_eq!(inbox_files(&home, &id), 0);
    assert_eq!(delivered_files(&home, &id), 3);

    until_state(&home, &id, "done");

    drop(guard);
    common::assert_no_broker_left(&home);
}
