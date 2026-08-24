mod common;

use a2a::events::Transport;
use a2a::fsm::WorkerState;
use a2a::state::WorkerSnapshot;
use assert_cmd::Command;
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::{Duration, Instant};

const A: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const B: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const W_SESSION: &str = "stub-sess-1";
const SECRET: &str = "xoxb-1234567890-abcdef";
const CONVERGE: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(100);

fn cli(home: &Path, allowed: &Path) -> Command {
    common::a2a(home, allowed)
}

fn data(stdout: &[u8]) -> Value {
    let v: Value = serde_json::from_slice(stdout).unwrap();
    v.get("data").cloned().unwrap_or(v)
}

fn data_str(stdout: &str) -> Value {
    let v: Value = serde_json::from_str(stdout).unwrap();
    v.get("data").cloned().unwrap_or(v)
}

fn array(stdout: &[u8]) -> Vec<Value> {
    serde_json::from_value(data(stdout)).unwrap()
}

fn inject(home: &Path, allowed: &Path, session: &str) -> String {
    let out = cli(home, allowed)
        .args(["mesh", "inject", "--session", session])
        .assert()
        .success();
    String::from_utf8(out.get_output().stdout.clone()).unwrap()
}

fn post(home: &Path, allowed: &Path, session: &str, text: &str) {
    cli(home, allowed)
        .args(["mesh", "post", "--session", session, text])
        .assert()
        .success();
}

fn note(home: &Path, allowed: &Path, from: &str, to: &str, text: &str) {
    cli(home, allowed)
        .args(["mesh", "note", "--session", from, to, "-"])
        .write_stdin(text)
        .assert()
        .success();
}

fn note_files(home: &Path, provider: &str, session: &str) -> Vec<PathBuf> {
    let inbox = home
        .join("mesh")
        .join("peers")
        .join(format!("{provider}-{session}"))
        .join("inbox");
    let mut out: Vec<_> = match fs::read_dir(inbox) {
        Ok(entries) => entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.ends_with(".note.json")))
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("reading note inbox: {e}"),
    };
    out.sort();
    out
}

fn worker_inbox_files(home: &Path, worker_id: &str) -> Vec<PathBuf> {
    let inbox = home.join("workers").join(worker_id).join("inbox");
    let mut out: Vec<_> = match fs::read_dir(inbox) {
        Ok(entries) => entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("reading worker inbox: {e}"),
    };
    out.sort();
    out
}

fn snapshot_tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, at: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        let Ok(entries) = fs::read_dir(at) else {
            return;
        };
        let mut entries: Vec<_> = entries.map(|e| e.unwrap()).collect();
        entries.sort_by_key(|e| e.path());
        for entry in entries {
            let path = entry.path();
            let rel = path.strip_prefix(root).unwrap().to_string_lossy().into_owned();
            let ty = entry.file_type().unwrap();
            if ty.is_dir() {
                out.insert(format!("{rel}/"), Vec::new());
                walk(root, &path, out);
            } else if ty.is_file() {
                out.insert(rel, fs::read(&path).unwrap());
            }
        }
    }

    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

fn wait_for<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
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

fn read_snapshot(home: &Path, worker_id: &str) -> Value {
    serde_json::from_str(&fs::read_to_string(home.join("workers").join(worker_id).join("state.json")).unwrap()).unwrap()
}

fn peer_presence(home: &Path, provider: &str, session: &str) -> PathBuf {
    home.join("mesh")
        .join("peers")
        .join(format!("{provider}-{session}"))
        .join("presence.json")
}

fn set_last_seen(home: &Path, provider: &str, session: &str, days_old: i64) {
    let path = peer_presence(home, provider, session);
    let mut p: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    p["last_seen"] = json!(Utc::now() - ChronoDuration::days(days_old));
    a2a::store::write_atomic(&path, p.to_string().as_bytes()).unwrap();
}

fn row_for<'a>(rows: &'a [Value], peer: &str) -> &'a Value {
    rows.iter()
        .find(|row| row["peer"] == peer)
        .unwrap_or_else(|| panic!("missing roster row for {peer}: {rows:?}"))
}

fn spawn_exec_worker(home: &Path, allowed: &Path, work: &Path, note_to_a: &str) -> String {
    let note = json!({"to": format!("claude:{A}"), "text": note_to_a, "refs": {"worker": ["stub"]}}).to_string();
    let brief = common::write_brief(allowed, "write a mesh note back to A\n");
    let out = cli(home, allowed)
        .env("STUB_SLEEP", "2")
        .env("STUB_MESH_NOTE", note)
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(work)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    data(out.get_output().stdout.as_slice())["id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn seed_worker(home: &Path, id: &str, session: &str, state: WorkerState) {
    let a2a_home = a2a::home::A2aHome::resolve(Some(home)).unwrap();
    let dir = a2a_home.worker_dir(id);
    fs::create_dir_all(dir.join("outbox")).unwrap();
    fs::create_dir_all(dir.join("inbox")).unwrap();
    let mut snap = WorkerSnapshot::new_for_test(id, home);
    snap.state = state;
    snap.transport = Transport::Exec;
    snap.requested_transport = Transport::Exec;
    snap.session_id = Some(session.to_string());
    a2a::state::save_snapshot(&dir, &snap).unwrap();
}

struct BrokerGuard {
    home: PathBuf,
}

impl BrokerGuard {
    fn new(home: &Path) -> BrokerGuard {
        BrokerGuard {
            home: home.to_path_buf(),
        }
    }
}

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let _ = StdCommand::new(assert_cmd::cargo::cargo_bin("a2a"))
            .arg("--home")
            .arg(&self.home)
            .args(["broker", "shutdown"])
            .env_remove("A2A_HOME")
            .output();
    }
}

fn assert_no_broker_left(home: &Path) {
    let deadline = Instant::now() + CONVERGE;
    loop {
        let _ = StdCommand::new(assert_cmd::cargo::cargo_bin("a2a"))
            .arg("--home")
            .arg(home)
            .args(["broker", "shutdown"])
            .env_remove("A2A_HOME")
            .output();
        if !home.join("broker.sock").exists() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("broker socket outlived its guard on {}", home.display());
        }
        std::thread::sleep(POLL);
    }
}

#[test]
fn hermetic_mesh_exec_e2e_cursors_notes_worker_refusals_roster_and_gc() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();

    post(&home, tmp.path(), A, "A first post");
    cli(&home, tmp.path())
        .args(["mesh", "register", "--session", B])
        .assert()
        .success();
    assert_eq!(inject(&home, tmp.path(), B), "");

    post(&home, tmp.path(), A, "A second post");
    let block = inject(&home, tmp.path(), B);
    assert!(block.contains("A second post"), "{block}");
    assert!(!block.contains("A first post"), "{block}");

    note(&home, tmp.path(), A, &format!("claude:{B}"), "A note to B");
    let block = inject(&home, tmp.path(), B);
    assert!(block.contains("A note to B"), "{block}");

    note(&home, tmp.path(), B, &A[..8], "B note to A by prefix");
    let out = cli(&home, tmp.path())
        .args(["mesh", "inbox", "--session", A, "--consume", "--json"])
        .assert()
        .success();
    let inbox = array(out.get_output().stdout.as_slice());
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0]["text"], "B note to A by prefix");
    assert!(note_files(&home, "claude", A).is_empty());
    assert!(home.join("mesh/peers").join(format!("claude-{A}/inbox/read")).read_dir().unwrap().next().is_some());

    let worker_id = spawn_exec_worker(&home, tmp.path(), &work, "W outbox note to A");
    wait_for("worker session", || {
        cli(&home, tmp.path())
            .args(["--json", "status", &worker_id])
            .assert()
            .success();
        let snap = read_snapshot(&home, &worker_id);
        (snap["session_id"] == W_SESSION).then_some(())
    });
    cli(&home, tmp.path())
        .args(["mesh", "register", "--provider", "codex", "--session", W_SESSION])
        .assert()
        .success();
    note(&home, tmp.path(), A, &worker_id, "A note to W by worker id");
    let queued = wait_for("worker send-inbox note", || {
        let files = worker_inbox_files(&home, &worker_id);
        (files.len() == 1).then_some(files)
    });
    assert!(fs::read_to_string(&queued[0]).unwrap().contains("A note to W by worker id"));
    assert_eq!(note_files(&home, "codex", W_SESSION).len(), 1);

    let out = cli(&home, tmp.path())
        .args(["mesh", "roster", "--json"])
        .assert()
        .success();
    let rows = array(out.get_output().stdout.as_slice());
    assert_eq!(row_for(&rows, &format!("claude:{A}"))["label"], "active");
    assert_eq!(row_for(&rows, &format!("claude:{B}"))["label"], "active");
    let w_row = row_for(&rows, &format!("codex:{W_SESSION}"));
    assert_eq!(w_row["label"], "active");
    assert_eq!(w_row["worker_id"], worker_id);

    common::wait_dead(&home, &worker_id, 10);
    cli(&home, tmp.path())
        .args(["--json", "status", &worker_id])
        .assert()
        .success();
    let block = inject(&home, tmp.path(), A);
    assert!(block.contains("W outbox note to A"), "{block}");
    assert!(home.join("workers").join(&worker_id).join("outbox/1.note.routed.json").exists());
    common::wait_dead(&home, &worker_id, 10);
    cli(&home, tmp.path())
        .args(["--json", "status", &worker_id])
        .assert()
        .success();

    let before = snapshot_tree(&home);
    for (subcommand, args) in [
        ("post", vec!["mesh", "post", "--session", A, SECRET]),
        ("note", vec!["mesh", "note", "--session", A, &format!("claude:{B}"), "-"]),
        ("status", vec!["mesh", "status", "--session", A, SECRET]),
    ] {
        let mut cmd = cli(&home, tmp.path());
        cmd.arg("--json").args(args);
        if subcommand == "note" {
            cmd.write_stdin(SECRET);
        }
        let out = cmd.assert().code(9);
        let v = data(out.get_output().stdout.as_slice());
        assert_eq!(v["error"]["code"], format!("SECRET_IN_{}", subcommand.to_ascii_uppercase()));
        assert!(!v.to_string().contains(SECRET), "{v}");
        assert_eq!(snapshot_tree(&home), before, "{subcommand} wrote after refusing a secret");
    }

    cli(&home, tmp.path())
        .args(["mesh", "retire", "--session", B])
        .assert()
        .success();
    set_last_seen(&home, "codex", W_SESSION, 8);
    let out = cli(&home, tmp.path())
        .args(["mesh", "roster", "--json"])
        .assert()
        .success();
    let rows = array(out.get_output().stdout.as_slice());
    assert_eq!(row_for(&rows, &format!("claude:{B}"))["label"], "retired");
    assert_eq!(row_for(&rows, &format!("codex:{W_SESSION}"))["label"], "stale");

    let out = cli(&home, tmp.path())
        .args(["--json", "gc"])
        .assert()
        .success();
    let v = data(out.get_output().stdout.as_slice());
    let removed = v["mesh"]["removed"].as_array().unwrap();
    assert!(removed.contains(&json!(format!("claude:{B}"))), "{v}");
    assert!(removed.contains(&json!(format!("codex:{W_SESSION}"))), "{v}");
    assert!(home.join("mesh/peers").join(format!("claude-{A}")).is_dir());
    assert!(!home.join("mesh/peers").join(format!("claude-{B}")).exists());
    assert!(!home.join("mesh/peers").join(format!("codex-{W_SESSION}")).exists());
}

#[test]
fn broker_transport_routes_worker_originated_note_into_claude_inject() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let fake_state = tmp.path().join("fake-state");
    fs::create_dir_all(&fake_state).unwrap();

    {
        let _guard = BrokerGuard::new(&home);
        cli(&home, tmp.path())
            .args(["mesh", "register", "--session", A])
            .assert()
            .success();
        let note = json!({"to": format!("claude:{A}"), "text": "broker W outbox note to A", "refs": {}}).to_string();
        let brief = common::write_brief(tmp.path(), "write a broker mesh note\n");
        let out = StdCommand::new(assert_cmd::cargo::cargo_bin("a2a"))
            .arg("--home")
            .arg(&home)
            .args(["--json", "spawn", "--lane", "research", "--brief"])
            .arg(&brief)
            .env_remove("A2A_HOME")
            .env_remove("A2A_BROKER_DISABLE")
            .env("A2A_CODEX_BIN", common::fake_app_server_path())
            .env("A2A_ALLOWED_ROOTS", tmp.path())
            .env("A2A_OWNER", common::TEST_OWNER)
            .env("A2A_BROKER_BIN", assert_cmd::cargo::cargo_bin("a2a"))
            .env("A2A_SHIM_BIN", assert_cmd::cargo::cargo_bin("a2a"))
            .env("FAKE_STATE", &fake_state)
            .env("FAKE_MESH_NOTE", note)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(
            out.status.code(),
            Some(0),
            "spawn stdout: {}\nspawn stderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let d = data_str(&String::from_utf8(out.stdout).unwrap());
        if d["transport"] != "app-server" {
            eprintln!("skipping app-server e2e assertion: transport was {}", d["transport"]);
            return;
        }
        let worker_id = d["id"].as_str().unwrap().to_string();

        wait_for("broker note to reach A inject", || {
            let block = inject(&home, tmp.path(), A);
            block.contains("broker W outbox note to A").then_some(block)
        });
        assert!(home.join("workers").join(&worker_id).join("outbox/1.note.routed.json").exists());
        let broker_log = fs::read_to_string(home.join("broker.jsonl")).unwrap_or_default();
        assert!(broker_log.contains("mesh_note_routed"), "{broker_log}");
    }

    assert_no_broker_left(&home);
}

#[test]
fn worker_originated_broker_route_can_target_codex_send_inbox() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let fake_state = tmp.path().join("fake-state");
    fs::create_dir_all(&fake_state).unwrap();
    let target_worker = "general-0824t2201-targ";
    let target_session = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    seed_worker(&home, target_worker, target_session, WorkerState::AwaitingInput);

    {
        let _guard = BrokerGuard::new(&home);
        let note = json!({"to": target_worker, "text": "broker note to codex worker", "refs": {}}).to_string();
        let brief = common::write_brief(tmp.path(), "write a broker mesh note to codex\n");
        let out = StdCommand::new(assert_cmd::cargo::cargo_bin("a2a"))
            .arg("--home")
            .arg(&home)
            .args(["--json", "spawn", "--lane", "research", "--brief"])
            .arg(&brief)
            .env_remove("A2A_HOME")
            .env_remove("A2A_BROKER_DISABLE")
            .env("A2A_CODEX_BIN", common::fake_app_server_path())
            .env("A2A_ALLOWED_ROOTS", tmp.path())
            .env("A2A_OWNER", common::TEST_OWNER)
            .env("A2A_BROKER_BIN", assert_cmd::cargo::cargo_bin("a2a"))
            .env("A2A_SHIM_BIN", assert_cmd::cargo::cargo_bin("a2a"))
            .env("FAKE_STATE", &fake_state)
            .env("FAKE_MESH_NOTE", note)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(
            out.status.code(),
            Some(0),
            "spawn stdout: {}\nspawn stderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let d = data_str(&String::from_utf8(out.stdout).unwrap());
        if d["transport"] != "app-server" {
            eprintln!("skipping app-server codex-target assertion: transport was {}", d["transport"]);
            return;
        }
        wait_for("broker note to target worker inbox", || {
            let files = worker_inbox_files(&home, target_worker);
            (files.len() == 1).then_some(files)
        });
        assert_eq!(note_files(&home, "codex", target_session).len(), 1);
    }

    assert_no_broker_left(&home);
}
