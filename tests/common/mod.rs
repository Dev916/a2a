// tests/common/mod.rs
//
// Shared helpers for every integration test. Each `tests/*.rs` is its own
// crate, so this file is compiled into each one that declares `mod common;` —
// hence the blanket dead_code allow (no single test binary uses all of it).
#![allow(dead_code)]
use assert_cmd::Command;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub fn stub_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/stub-codex")
}
/// Command with hermetic env: temp home, stub codex, tmp-wide allowlist —
/// and **no broker**.
///
/// `A2A_BROKER_DISABLE=1` is the pin that keeps every Phase-1 test meaningful:
/// with it set, `a2a` takes the pure exec path and opens no socket at all, so
/// these tests still assert Phase-1 behaviour rather than "whatever the broker
/// happened to do". Broker tests opt in through [`a2a_broker`], which removes
/// it.
///
/// `A2A_OWNER=test-owner` is the other determinism pin (spec §4.8): without it
/// a worker's owner would be `anon-<uid>` — different on every machine, and
/// different again inside a container — and every test that spawns then mutates
/// would depend on the *ambient* environment agreeing with itself. One fixed
/// owner for both halves means the owner guard is exercised on the happy path
/// by every existing test, and asserted deliberately in `tests/owner_scope.rs`.
pub const TEST_OWNER: &str = "test-owner";

pub fn a2a(home: &Path, allowed_root: &Path) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home)
        .env("A2A_CODEX_BIN", stub_path())
        .env("A2A_ALLOWED_ROOTS", allowed_root)
        .env("A2A_BROKER_DISABLE", "1")
        .env("A2A_OWNER", TEST_OWNER)
        // Determinism under the test harness: `exec::spawn_detached` would
        // otherwise resolve the shim relative to the running executable.
        .env("A2A_SHIM_BIN", assert_cmd::cargo::cargo_bin("a2a"));
    c
}
pub fn fake_app_server_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-app-server")
}

/// The broker opt-in: [`a2a`]'s environment MINUS `A2A_BROKER_DISABLE` (which
/// keeps every Phase-1 test on the pure exec path), plus the scripted
/// app-server as the codex binary the daemon will own.
///
/// The returned [`BrokerGuard`] is the reason this is a pair and not a
/// `Command`: a broker autostarted by a test outlives the test process unless
/// something retires it, and a leaked daemon holding a deleted temp home is the
/// kind of stray that makes a suite unreliable rather than merely red.
pub fn a2a_broker(home: &Path, allowed_root: &Path) -> (Command, BrokerGuard) {
    let mut c = a2a(home, allowed_root);
    c.env_remove("A2A_BROKER_DISABLE")
        .env("A2A_CODEX_BIN", fake_app_server_path());
    (c, BrokerGuard::new(home))
}

/// Every live `broker run` process whose `--home` is `home`.
///
/// The socket disappearing is not the same fact as the daemon exiting, and a
/// daemon that survives its own test — holding a deleted temp home, an
/// app-server child and a listener nobody can reach — is the failure this
/// exists to catch.
pub fn broker_processes(home: &Path) -> Vec<String> {
    let out = std::process::Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .expect("ps runs");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|line| line.contains("broker run") && line.contains(&*home.to_string_lossy()))
        .map(str::to_string)
        .collect()
}

/// Bounded wait for every broker on `home` to be gone.
pub fn assert_no_broker_left(home: &Path) {
    let deadline = std::time::Instant::now() + BrokerGuard::DRAIN_WAIT;
    loop {
        let left = broker_processes(home);
        if left.is_empty() {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("broker processes outlived their home {home:?}: {left:?}");
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Drop = `a2a broker shutdown`, then a bounded wait for the socket to vanish.
pub struct BrokerGuard {
    home: PathBuf,
}

impl BrokerGuard {
    pub fn new(home: &Path) -> BrokerGuard {
        BrokerGuard {
            home: home.to_path_buf(),
        }
    }
    /// How long a guard waits for the daemon to take its socket down before it
    /// gives up and says so.
    pub const DRAIN_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
}

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let sock = self.home.join("broker.sock");
        let deadline = std::time::Instant::now() + Self::DRAIN_WAIT;
        loop {
            // Unconditional, and RETRIED until nothing is left on this home.
            // `broker shutdown` on an empty home is a ~10ms no-op, but it is
            // also a no-op against a daemon that has not bound its socket yet —
            // and "not bound yet" is a real window, widest exactly where the
            // crash tests live: a SIGKILLed daemon leaves its socket file
            // behind, so a successor coming up finds the address occupied,
            // unlinks it and rebinds. One shot into that window would fail to
            // connect, report success, and leave a daemon holding a temp home
            // that is about to be deleted. Firing until `ps` says the home has
            // no `broker run` on it is what closes it.
            let _ = std::process::Command::new(assert_cmd::cargo::cargo_bin("a2a"))
                .arg("--home")
                .arg(&self.home)
                .args(["broker", "shutdown"])
                .env_remove("A2A_HOME")
                .output();
            let left = broker_processes(&self.home);
            if left.is_empty() && !sock.exists() {
                return;
            }
            if std::time::Instant::now() >= deadline {
                if !std::thread::panicking() {
                    panic!(
                        "broker outlived its guard on {}: socket {}, processes {left:?}",
                        self.home.display(),
                        if sock.exists() { "still there" } else { "gone" }
                    );
                }
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
}

pub fn write_brief(dir: &Path, body: &str) -> PathBuf {
    let p = dir.join("the-brief.md");
    std::fs::write(&p, body).unwrap();
    p
}

pub struct TestMeshLink {
    pub a_state: a2a::mesh::gateway::link::LinkStateHandle,
    pub b_state: a2a::mesh::gateway::link::LinkStateHandle,
    token: tokio_util::sync::CancellationToken,
    a_task: tokio::task::JoinHandle<Result<(), a2a::output::AppError>>,
    b_task: tokio::task::JoinHandle<Result<(), a2a::output::AppError>>,
}

impl TestMeshLink {
    pub async fn stop(self) {
        self.token.cancel();
        self.a_task.await.unwrap().unwrap();
        self.b_task.await.unwrap().unwrap();
    }
}

pub async fn start_mesh_link(
    home_a: a2a::home::A2aHome,
    cfg_a: a2a::config::Config,
    gateway_a: &str,
    home_b: a2a::home::A2aHome,
    cfg_b: a2a::config::Config,
    gateway_b: &str,
) -> TestMeshLink {
    let clock = a2a::mesh::keys::TestClock::new(chrono::Utc::now());
    let (side_a, side_b) = a2a::mesh::wire::test_link(&clock).await;
    let a_state =
        a2a::mesh::gateway::link::LinkStateHandle::new(&home_a, Duration::from_secs(60)).unwrap();
    let b_state =
        a2a::mesh::gateway::link::LinkStateHandle::new(&home_b, Duration::from_secs(60)).unwrap();
    let token = tokio_util::sync::CancellationToken::new();
    let a_task = tokio::spawn(a2a::mesh::gateway::link::run_link_session(
        side_a,
        a2a::mesh::gateway::link::LinkSessionParams::new(
            home_a,
            cfg_a,
            gateway_a,
            gateway_b,
            a2a::mesh::gateway::link::LinkDirection::Outbound,
            a_state.clone(),
            token.child_token(),
        ),
    ));
    let b_task = tokio::spawn(a2a::mesh::gateway::link::run_link_session(
        side_b,
        a2a::mesh::gateway::link::LinkSessionParams::new(
            home_b,
            cfg_b,
            gateway_b,
            gateway_a,
            a2a::mesh::gateway::link::LinkDirection::Inbound,
            b_state.clone(),
            token.child_token(),
        ),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if a_state.is_live(gateway_b).await && b_state.is_live(gateway_a).await {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "test mesh link did not register both sessions"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    TestMeshLink {
        a_state,
        b_state,
        token,
        a_task,
        b_task,
    }
}

pub async fn wait_until(mut pred: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if pred() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition was not met before timeout"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Spawns a general-lane worker with the given STUB_* env; returns (home, tmp_keepalive, worker_id).
pub fn spawn_with(stub_env: &[(&str, &str)]) -> (PathBuf, tempfile::TempDir, String) {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = write_brief(tmp.path(), "do the thing\n");
    let mut c = a2a(&home, tmp.path());
    for (k, v) in stub_env {
        c.env(k, v);
    }
    let out = c
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    (home, tmp, v["data"]["id"].as_str().unwrap().to_string())
}
pub fn read_snapshot(home: &Path, id: &str) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(home.join("workers").join(id).join("state.json")).unwrap(),
    )
    .unwrap()
}
// nix 0.29 deprecates the free `flock` in favour of `Flock::lock`, which takes
// the File by value; the probe-and-release shape below is exactly what the
// plan pins, so the deprecation is silenced rather than the helper rewritten.
#[allow(deprecated)]
pub fn wait_dead(home: &Path, id: &str, secs: u64) {
    let lock = home.join("workers").join(id).join("run.lock");
    for _ in 0..(secs * 10) {
        if !lock.exists() {
            return;
        }
        if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&lock) {
            use std::os::fd::AsRawFd;
            if nix::fcntl::flock(f.as_raw_fd(), nix::fcntl::FlockArg::LockExclusiveNonblock).is_ok()
            {
                return; // lock free → shim gone
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("worker {id} still alive after {secs}s");
}
pub fn rewrite_deadline_to_past(home: &Path, id: &str) {
    let dir = home.join("workers").join(id);
    let mut v = read_snapshot(home, id);
    v["deadline_at"] = serde_json::json!(chrono::Utc::now() - chrono::Duration::hours(1));
    a2a_lib_write_atomic(&dir.join("state.json"), v.to_string().as_bytes());
}
fn a2a_lib_write_atomic(p: &Path, b: &[u8]) {
    a2a::store::write_atomic(p, b).unwrap()
}

/// The Phase-3-owned `codex mcp list --json` answerer. Emphatically NOT
/// [`stub_path`]: that fixture falls through its argument loop and answers
/// `mcp list --json` with exec NDJSON and exit 0, which would make a sync
/// validation "pass" for reasons that have nothing to do with the config.
pub fn stub_codex_mcp_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sync/stub-codex-mcp")
}

/// The ONE entry point every `sync` test builds its command with.
///
/// [`a2a`] sets neither `CODEX_HOME` nor any Claude pointer, and a sync test
/// that forgot one of them would resolve to the developer's REAL `~/.codex`
/// submodule and `~/.claude/skills` — a mistake nothing on disk would undo.
/// This helper is half of the answer (the other half is `sync`'s own runtime
/// guard, which refuses the real `$HOME` unless `A2A_SYNC_ALLOW_REAL_HOME` is
/// set — and which is why this removes that variable rather than trusting the
/// ambient environment to be free of it).
///
/// `claude_dir` is passed through the config layer's env spelling, so the
/// resolved account is the fixture's and `--claude-config-dir` is left free for
/// tests that want to assert the flag outranks it.
pub fn sync_a2a(home: &Path, codex_home: &Path, claude_dir: &Path, allowed_root: &Path) -> Command {
    let mut c = a2a(home, allowed_root);
    c.env("CODEX_HOME", codex_home)
        .env("A2A_SYNC_CLAUDE_CONFIG_DIR", claude_dir)
        .env("A2A_CODEX_BIN", stub_codex_mcp_path())
        // Both pointers below would otherwise be inherited from whoever ran
        // `cargo test` — and this developer's shell really does set
        // `CLAUDE_CONFIG_DIR` for the second account.
        .env_remove("CLAUDE_CONFIG_DIR")
        .env_remove("A2A_SYNC_ALLOW_REAL_HOME")
        .env_remove("A2A_SYNC_ALLOW_SKILLS");
    c
}
