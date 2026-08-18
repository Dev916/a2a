//! `a2a tail <id> --follow` — the streaming half of the WAL reader (spec §4.8).
//!
//! Every test here runs the binary through `std::process::Command` with a
//! **piped stdout and a reader thread**, and every follow carries `--timeout`.
//! That shape is the point, not a convenience: `assert_cmd`'s `.assert()`
//! collects the child's output to EOF on the calling thread, so a follow that
//! never ends would hang the suite on its own subject. With a reader thread the
//! test can watch the stream *while it is still running* — which is what a
//! mid-follow `send`, a mid-follow `broker shutdown` and a lag recovery all
//! need — and [`Follower::finish`] bounds the wait for the exit itself.
//!
//! The two bounded exits are what make that possible at all: `--timeout <dur>`
//! (exit 6, "not terminal yet", the same code `a2a wait` uses for the same
//! situation) and the automatic exit 0 the moment the worker reaches a terminal
//! state.

mod common;

use serde_json::{json, Value};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The convergence window every poll in this file shares.
const CONVERGE: Duration = Duration::from_secs(20);
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

/// What one *finished* `a2a` invocation did. stdio goes to files for everything
/// that is not the followed stream itself (the daemon inherits its parent's
/// descriptors until it redirects them).
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

// ---------------------------------------------------------------------------
// the follow runner
// ---------------------------------------------------------------------------

/// A running `a2a tail --follow`, with its stdout drained by a thread so the
/// test can read the stream as it arrives instead of after it ends.
struct Follower {
    child: std::process::Child,
    lines: Arc<Mutex<Vec<String>>>,
    reader: Option<std::thread::JoinHandle<()>>,
    err_path: PathBuf,
}

struct Followed {
    code: i32,
    lines: Vec<String>,
    stderr: String,
}

impl Follower {
    fn start(mut cmd: Command, scratch: &Path, tag: &str) -> Follower {
        std::fs::create_dir_all(scratch).unwrap();
        let err_path = scratch.join(format!("{tag}.err"));
        let err = std::fs::File::create(&err_path).unwrap();
        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(err))
            .spawn()
            .expect("the a2a binary runs");
        let stdout = child.stdout.take().expect("stdout is piped");
        let lines = Arc::new(Mutex::new(Vec::new()));
        let sink = lines.clone();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(l) => sink.lock().unwrap().push(l),
                    Err(_) => return,
                }
            }
        });
        Follower {
            child,
            lines,
            reader: Some(reader),
            err_path,
        }
    }

    /// What the stream has produced so far — the live view.
    fn lines_now(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    /// Wait for the follow to end **within `secs`**, then collect everything.
    /// A follow still running past its own bound is the failure this test file
    /// exists to prevent, so it is killed and reported rather than waited on.
    fn finish(mut self, secs: u64) -> Followed {
        let deadline = Instant::now() + Duration::from_secs(secs);
        let status = loop {
            match self.child.try_wait().expect("try_wait") {
                Some(status) => break status,
                None if Instant::now() >= deadline => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    panic!(
                        "the follow was still running after {secs}s — it is not bounded.\n\
                         lines so far: {:?}\nstderr:\n{}",
                        self.lines_now(),
                        std::fs::read_to_string(&self.err_path).unwrap_or_default()
                    );
                }
                None => std::thread::sleep(POLL),
            }
        };
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        Followed {
            code: status.code().unwrap_or(-1),
            lines: self.lines_now(),
            stderr: std::fs::read_to_string(&self.err_path).unwrap_or_default(),
        }
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

/// **The convergence gate** (copied from `tests/cmd_interrupt.rs`, where the
/// plan pinned it): poll the shipped `a2a --json tail` until the worker's WAL
/// carries a `turn_started` envelope, so anything aimed at that turn afterwards
/// is aimed at a turn that demonstrably exists.
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

/// The lifecycle projection of the worker's WAL, straight off disk:
/// `(seq, kind)` for every lifecycle row — exactly what `tail` prints.
fn wal_lifecycle(home: &Path, id: &str) -> Vec<(u64, String)> {
    std::fs::read_to_string(home.join("workers").join(id).join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|e| e["class"] == json!("lifecycle"))
        .filter_map(|e| {
            Some((
                e["seq"].as_u64()?,
                e["kind"].as_str().unwrap_or_default().to_string(),
            ))
        })
        .collect()
}

/// `<seq> <ts> <kind>` → `(seq, kind)`, asserting the line contract on the way
/// through: three whitespace-separated fields, seq first.
fn parse_line(line: &str) -> (u64, String) {
    let parts: Vec<&str> = line.splitn(3, ' ').collect();
    assert_eq!(parts.len(), 3, "line contract is `<seq> <ts> <kind>`: {line}");
    let seq: u64 = parts[0]
        .parse()
        .unwrap_or_else(|_| panic!("the first field is a seq: {line}"));
    assert!(parts[1].ends_with('Z'), "the second field is a timestamp: {line}");
    (seq, parts[2].to_string())
}

fn parsed(lines: &[String]) -> Vec<(u64, String)> {
    lines.iter().map(|l| parse_line(l)).collect()
}

/// Spawn one app-server worker on the given scenario and return its id.
fn spawn_app_server(
    home: &Path,
    allowed: &Path,
    cwd: &Path,
    scratch: &Path,
    scenario: &str,
    env: &[(&str, &str)],
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
    for (k, v) in env {
        cmd.env(k, v);
    }
    let spawned = run(cmd, scratch, "spawn");
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    assert_eq!(spawned.data()["transport"], json!("app-server"));
    spawned.data()["id"].as_str().unwrap().to_string()
}

// ---------------------------------------------------------------------------

/// The shape of the feature: what is already on disk is replayed, what happens
/// next arrives live, both come out in the same line format, and the command
/// ends **by itself** when the worker settles.
#[test]
fn follow_replays_the_log_then_streams_live_frames_and_exits_at_the_terminal_state() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);

    // `hang-turn` holds its turn open forever, so there is a real BEFORE and a
    // real AFTER: the replay half is written, the live half is not yet.
    let id = spawn_app_server(&home, tmp.path(), &work, &scratch, "hang-turn", &[]);
    until_state(&home, &id, "running");
    until_turn_started(&home, tmp.path(), &scratch, &id);
    let before = wal_lifecycle(&home, &id);
    assert!(
        before.iter().any(|(_, k)| k == "turn_started"),
        "the gate leaves a turn_started in the log: {before:?}"
    );

    let follower = Follower::start(
        a2a(&home, tmp.path(), &["tail", &id, "--follow", "--timeout", "30s"]),
        &scratch,
        "follow",
    );
    // The replay lands before anything else happens.
    until("the replay to reach the stream", || {
        (follower.lines_now().len() >= before.len()).then_some(())
    });
    assert_eq!(
        parsed(&follower.lines_now())[..before.len()],
        before[..],
        "the replay is the log, in order"
    );

    // ... and now the live half: steering the fake completes its held turn.
    let sent = run(
        a2a(&home, tmp.path(), &["send", &id, "go on then"]),
        &scratch,
        "send",
    );
    assert_eq!(sent.code, 0, "send: {}", sent.stderr);

    let out = follower.finish(40);
    assert_eq!(out.code, 0, "a settled worker ends the follow: {}", out.stderr);
    let seen = parsed(&out.lines);
    let after = wal_lifecycle(&home, &id);
    assert_eq!(seen, after, "the follow is the WAL's lifecycle projection, whole");
    assert!(
        seen.iter().any(|(_, k)| k == "turn_completed"),
        "the live half arrived: {seen:?}"
    );
    assert_eq!(snap_of(&home, &id).unwrap()["state"], json!("done"));

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The other bounded exit: a worker that never settles does not hold the
/// command open. Expiry is exit 6 — the same "not terminal yet" `a2a wait`
/// reports for exactly this situation.
#[test]
fn follow_gives_up_at_its_timeout_rather_than_blocking_forever() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);

    let id = spawn_app_server(&home, tmp.path(), &work, &scratch, "hang-turn", &[]);
    until_state(&home, &id, "running");
    until_turn_started(&home, tmp.path(), &scratch, &id);

    let started = Instant::now();
    let out = Follower::start(
        a2a(&home, tmp.path(), &["tail", &id, "--follow", "--timeout", "2s"]),
        &scratch,
        "follow",
    )
    .finish(30);
    assert_eq!(out.code, 6, "expiry is exit 6: {}", out.stderr);
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the timeout is the bound, not a formality"
    );
    assert!(
        parsed(&out.lines).iter().any(|(_, k)| k == "turn_started"),
        "the replay still happened: {:?}",
        out.lines
    );
    assert_eq!(
        snap_of(&home, &id).unwrap()["state"],
        json!("running"),
        "a follow reads; it never moves the worker"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// A broker going down under a live tail is news, not an error: the follow says
/// so on stderr and exits 0. (A bare EOF would be indistinguishable from a
/// crash, which is why the daemon sends a farewell frame at all.)
#[test]
fn a_broker_shutdown_mid_follow_is_a_notice_on_stderr_and_a_zero_exit() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);

    let id = spawn_app_server(&home, tmp.path(), &work, &scratch, "hang-turn", &[]);
    until_state(&home, &id, "running");
    until_turn_started(&home, tmp.path(), &scratch, &id);

    let follower = Follower::start(
        a2a(&home, tmp.path(), &["tail", &id, "--follow", "--timeout", "60s"]),
        &scratch,
        "follow",
    );
    until("the follow to be streaming", || {
        (!follower.lines_now().is_empty()).then_some(())
    });

    let stopped = run(
        a2a(&home, tmp.path(), &["broker", "shutdown"]),
        &scratch,
        "shutdown",
    );
    assert_eq!(stopped.code, 0, "shutdown: {}", stopped.stderr);

    let out = follower.finish(30);
    assert_eq!(out.code, 0, "a broker going down is not the tail's failure");
    assert!(
        out.stderr.contains("broker"),
        "the reason is on stderr: {:?}",
        out.stderr
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The exec transport has no socket to subscribe to, so `--follow` polls the
/// WAL instead — same flags, same line contract, same exits, and no broker is
/// started to serve a worker that does not need one.
#[test]
fn an_exec_worker_follows_through_the_poll_fallback_with_the_same_flags_and_exits() {
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

    let mut follow = a2a(&home, tmp.path(), &["tail", &id, "--follow", "--timeout", "30s"]);
    follow.env("A2A_CODEX_BIN", common::stub_path());
    let out = Follower::start(follow, &scratch, "follow").finish(40);

    assert_eq!(out.code, 0, "the exec worker settled: {}", out.stderr);
    let seen = parsed(&out.lines);
    assert!(!seen.is_empty(), "the poll fallback still prints the log");
    assert!(
        seen.windows(2).all(|w| w[0].0 < w[1].0),
        "seqs are strictly increasing: {seen:?}"
    );
    assert_eq!(snap_of(&home, &id).unwrap()["state"], json!("done"));
    assert!(
        !home.join("broker.sock").exists(),
        "an exec follow starts no broker"
    );

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// A burst of events, straight through: every lifecycle row in the WAL comes
/// out of the follow exactly once, in order. No gaps (a dropped frame) and no
/// duplicates (a re-read that forgot its cursor) — the two ways a lag recovery
/// can be wrong.
#[test]
fn a_flood_of_events_arrives_with_no_gaps_and_no_duplicates() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);

    // The fake emits FAKE_FLOOD extra agent messages before it finishes the
    // turn: ~65 lifecycle rows, as fast as the child can write them.
    let id = spawn_app_server(
        &home,
        tmp.path(),
        &work,
        &scratch,
        "happy",
        &[("FAKE_FLOOD", "60")],
    );

    let out = Follower::start(
        a2a(&home, tmp.path(), &["tail", &id, "--follow", "--timeout", "60s"]),
        &scratch,
        "follow",
    )
    .finish(90);
    assert_eq!(out.code, 0, "the worker settled: {}", out.stderr);

    let seen = parsed(&out.lines);
    let wal = wal_lifecycle(&home, &id);
    assert!(wal.len() > 60, "the flood really happened: {} rows", wal.len());
    assert_eq!(seen, wal, "no gaps, no duplicates: the stream IS the log");
    let mut seqs: Vec<u64> = seen.iter().map(|(s, _)| *s).collect();
    let before = seqs.len();
    seqs.dedup();
    assert_eq!(before, seqs.len(), "no duplicates");

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// Redaction is `tail`'s own, on both projections and on both modes: the
/// followed stream masks what it renders rather than trusting the file.
#[test]
fn follow_redacts_its_egress_in_raw_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);

    let id = spawn_app_server(
        &home,
        tmp.path(),
        &work,
        &scratch,
        "happy",
        &[("FAKE_MESSAGE", "the token is xoxb-1234567890-abcdef ok")],
    );

    let out = Follower::start(
        a2a(
            &home,
            tmp.path(),
            &["tail", &id, "--follow", "--raw", "--timeout", "30s"],
        ),
        &scratch,
        "follow",
    )
    .finish(40);
    assert_eq!(out.code, 0, "{}", out.stderr);
    let text = out.lines.join("\n");
    assert!(
        text.contains("agent_message"),
        "the raw stream carries the envelopes: {text}"
    );
    assert!(!text.contains("xoxb-1234567890"), "leak: {text}");
    assert!(text.contains("[REDACTED:"), "masked, not dropped: {text}");
    for line in &out.lines {
        serde_json::from_str::<Value>(line)
            .unwrap_or_else(|e| panic!("--raw stays one JSON object per line ({e}): {line}"));
    }

    drop(guard);
    common::assert_no_broker_left(&home);
}

/// The idle timer's other half (Task 10): somebody watching is somebody using.
/// The followed worker here is never adopted by the daemon — it is `spawned`,
/// which startup skips — so the broker's registry is empty and the ONLY thing
/// standing between it and its idle timeout is the tail.
#[test]
fn a_follow_client_keeps_an_idle_broker_alive() {
    use a2a::config::Lane;
    use a2a::events::EventKind;
    use a2a::home::A2aHome;
    use a2a::state::WorkerSnapshot;
    use a2a::store::EventLog;
    use a2a::{fold, fsm::WorkerState};

    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir_all(&work).unwrap();
    A2aHome::resolve(Some(&home)).unwrap();
    let guard = common::BrokerGuard::new(&home);

    // A worker directory exactly as `a2a spawn` leaves one, left in `spawned`:
    // startup adopts running/awaiting-input/orphaned workers, so this one stays
    // out of the registry and cannot be what keeps the daemon up.
    let id = "general-0817t1600-ffff";
    let dir = home.join("workers").join(id);
    for sub in ["outbox", "inbox", "inbox/delivered", "scratch", "context"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    let mut log = EventLog::open(&dir).unwrap();
    let snap = fold::append_and_fold(
        &mut log,
        &dir,
        WorkerSnapshot::new_for_test(id, &work),
        EventKind::Spawned,
        json!({"fsm":"Spawn","id":id,"lane":Lane::General.as_str(),"cwd":&work,
               "transport":"app-server"}),
        chrono::Utc::now(),
    )
    .unwrap();
    assert_eq!(snap.state, WorkerState::Spawned);

    let mut cmd = a2a(&home, tmp.path(), &["broker", "run"]);
    cmd.env("A2A_BROKER_IDLE_TIMEOUT_SECS", "3");
    let started = run(cmd, &scratch, "broker-run");
    assert_eq!(started.code, 0, "broker run: {}", started.stderr);
    let pid = until("the broker to publish its pidfile", || {
        std::fs::read_to_string(home.join("broker.pid"))
            .ok()
            .and_then(|t| t.trim().parse::<u32>().ok())
    });

    let follower = Follower::start(
        a2a(&home, tmp.path(), &["tail", id, "--follow", "--timeout", "12s"]),
        &scratch,
        "follow",
    );
    // The subscription must be ESTABLISHED before the clock starts, or the
    // test is asserting nothing: a replayed line only proves the follow read
    // the file. Two clients — the tail, and the process doing the asking.
    until("the follow to reach the daemon", || {
        let seen = run(
            a2a(&home, tmp.path(), &["--json", "status", "--broker"]),
            &scratch,
            "broker-status",
        );
        (seen.code == 0 && seen.data()["clients"].as_u64().unwrap_or(0) >= 2).then_some(())
    });

    // Twice the idle timeout with nobody else connected and nothing in the
    // registry: an unwatched broker would have retired by now.
    std::thread::sleep(Duration::from_secs(6));
    assert!(
        alive(pid),
        "a broker with somebody tailing it is not idle: {}",
        std::fs::read_to_string(home.join("broker.jsonl")).unwrap_or_default()
    );

    let out = follower.finish(30);
    assert_eq!(out.code, 6, "the follow ended on its own timeout: {}", out.stderr);
    // ... and once nobody is watching, the safety net works again.
    until("the unwatched broker to retire itself", || {
        (!alive(pid)).then_some(())
    });

    drop(guard);
    common::assert_no_broker_left(&home);
}

fn alive(pid: u32) -> bool {
    std::process::Command::new("ps")
        .args(["-o", "pid=", "-p", &pid.to_string()])
        .output()
        .map(|o| o.status.success() && !o.stdout.is_empty())
        .unwrap_or(false)
}
