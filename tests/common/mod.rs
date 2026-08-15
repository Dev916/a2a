// tests/common/mod.rs
//
// Shared helpers for every integration test. Each `tests/*.rs` is its own
// crate, so this file is compiled into each one that declares `mod common;` —
// hence the blanket dead_code allow (no single test binary uses all of it).
#![allow(dead_code)]
use assert_cmd::Command;
use std::path::{Path, PathBuf};

pub fn stub_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/stub-codex")
}
/// Command with hermetic env: temp home, stub codex, tmp-wide allowlist.
pub fn a2a(home: &Path, allowed_root: &Path) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home)
        .env("A2A_CODEX_BIN", stub_path())
        .env("A2A_ALLOWED_ROOTS", allowed_root)
        // Determinism under the test harness: `exec::spawn_detached` would
        // otherwise resolve the shim relative to the running executable.
        .env("A2A_SHIM_BIN", assert_cmd::cargo::cargo_bin("a2a"));
    c
}
pub fn write_brief(dir: &Path, body: &str) -> PathBuf {
    let p = dir.join("the-brief.md");
    std::fs::write(&p, body).unwrap();
    p
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
