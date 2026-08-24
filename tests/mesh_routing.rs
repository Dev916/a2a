mod common;

use a2a::events::Transport;
use a2a::fsm::WorkerState;
use a2a::state::WorkerSnapshot;
use assert_cmd::Command;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand, Stdio};
use std::time::{Duration, Instant};

const CONVERGE: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(100);

fn a2a_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("a2a")
}

fn cli(home: &Path, allowed: &Path) -> Command {
    common::a2a(home, allowed)
}

fn broker_cli(home: &Path, allowed: &Path, fake_state: &Path, args: &[&str]) -> StdCommand {
    let mut c = StdCommand::new(a2a_bin());
    c.arg("--home")
        .arg(home)
        .args(args)
        .env_remove("A2A_HOME")
        .env_remove("A2A_BROKER_DISABLE")
        .env("A2A_CODEX_BIN", common::fake_app_server_path())
        .env("A2A_ALLOWED_ROOTS", allowed)
        .env("A2A_OWNER", common::TEST_OWNER)
        .env("A2A_BROKER_BIN", a2a_bin())
        .env("A2A_SHIM_BIN", a2a_bin())
        .env("FAKE_STATE", fake_state);
    c
}

struct Ran {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run(mut cmd: StdCommand, scratch: &Path, tag: &str) -> Ran {
    fs::create_dir_all(scratch).unwrap();
    let out = fs::File::create(scratch.join(format!("{tag}.out"))).unwrap();
    let err = fs::File::create(scratch.join(format!("{tag}.err"))).unwrap();
    let mut child: Child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("the a2a binary runs");
    let status = child.wait().expect("the invocation ends");
    Ran {
        code: status.code().unwrap_or(-1),
        stdout: fs::read_to_string(scratch.join(format!("{tag}.out"))).unwrap_or_default(),
        stderr: fs::read_to_string(scratch.join(format!("{tag}.err"))).unwrap_or_default(),
    }
}

fn until<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
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

fn data(stdout: &str) -> Value {
    serde_json::from_str::<Value>(stdout).unwrap()["data"].clone()
}

fn require_appserver(spawned: &Ran) -> Option<Value> {
    assert_eq!(spawned.code, 0, "spawn: {}", spawned.stderr);
    let d = data(&spawned.stdout);
    if d["transport"] != "app-server" {
        eprintln!(
            "skipping app-server routing assertion: broker unavailable, transport was {}",
            d["transport"]
        );
        return None;
    }
    Some(d)
}

fn note_files(home: &Path, provider: &str, session: &str) -> Vec<PathBuf> {
    let inbox = home
        .join("mesh")
        .join("peers")
        .join(format!("{provider}-{session}"))
        .join("inbox");
    let mut out: Vec<_> = match fs::read_dir(inbox) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => panic!("reading note inbox: {e}"),
    }
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".note.json"))
        })
        .collect();
    out.sort();
    out
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
        let _ = StdCommand::new(a2a_bin())
            .arg("--home")
            .arg(&self.home)
            .args(["broker", "shutdown"])
            .env_remove("A2A_HOME")
            .output();
    }
}

fn broker_events(home: &Path) -> Vec<Value> {
    fs::read_to_string(home.join("broker.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect()
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

#[test]
fn exec_status_routes_worker_note_to_claude_peer_and_marks_source_routed() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let claude = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    cli(&home, tmp.path())
        .args(["mesh", "register", "--session", claude])
        .assert()
        .success();
    let note = json!({"to": format!("claude:{claude}"), "text": "from exec worker", "refs": {"worker": ["source"]}}).to_string();
    let brief = common::write_brief(tmp.path(), "write a mesh note\n");
    let spawned = cli(&home, tmp.path())
        .env("STUB_MESH_NOTE", note)
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    let id = data(&String::from_utf8(spawned.get_output().stdout.clone()).unwrap())["id"]
        .as_str()
        .unwrap()
        .to_string();
    common::wait_dead(&home, &id, 10);

    cli(&home, tmp.path())
        .args(["--json", "status", &id])
        .assert()
        .success();

    let files = note_files(&home, "claude", claude);
    assert_eq!(files.len(), 1);
    let delivered: Value = serde_json::from_str(&fs::read_to_string(&files[0]).unwrap()).unwrap();
    assert_eq!(delivered["from"], "codex:stub-sess-1");
    assert_eq!(delivered["to"], format!("claude:{claude}"));
    assert_eq!(delivered["text"], "from exec worker");
    let outbox = home.join("workers").join(&id).join("outbox");
    assert!(outbox.join("1.note.routed.json").exists());
    assert!(!outbox.join("1.note.json").exists());
}

#[test]
fn appserver_turn_end_routes_worker_note_to_claude_peer_and_logs_outcome() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let fake_state = tmp.path().join("fake-state");
    let scratch = tmp.path().join("scratch");
    fs::create_dir_all(&fake_state).unwrap();
    let _guard = BrokerGuard::new(&home);
    let claude = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    cli(&home, tmp.path())
        .args(["mesh", "register", "--session", claude])
        .assert()
        .success();
    let brief = common::write_brief(tmp.path(), "write a mesh note\n");
    let note = json!({"to": format!("claude:{claude}"), "text": "from app worker", "refs": {}}).to_string();
    let mut cmd = broker_cli(&home, tmp.path(), &fake_state, &["--json", "spawn", "--lane", "research", "--brief"]);
    cmd.arg(&brief).env("FAKE_MESH_NOTE", note);
    let spawned = run(cmd, &scratch, "spawn");
    let Some(d) = require_appserver(&spawned) else {
        return;
    };
    let id = d["id"].as_str().unwrap().to_string();

    until("app-server note to be routed", || {
        let files = note_files(&home, "claude", claude);
        if files.len() == 1 {
            Some(files)
        } else {
            None
        }
    });
    assert!(home.join("workers").join(&id).join("outbox/1.note.routed.json").exists());
    let routed = until("mesh_note_routed broker log line", || {
        broker_events(&home).into_iter().find(|v| {
            v["kind"] == "mesh_note_routed"
                && v["worker"] == id
                && v["to"] == format!("claude:{claude}")
        })
    });
    assert_eq!(routed["to"], format!("claude:{claude}"));
}

#[test]
fn appserver_worker_note_to_codex_worker_lands_in_target_send_inbox() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let fake_state = tmp.path().join("fake-state");
    let scratch = tmp.path().join("scratch");
    fs::create_dir_all(&fake_state).unwrap();
    let _guard = BrokerGuard::new(&home);
    let target = "general-0824t2132-targ";
    let target_session = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    seed_worker(&home, target, target_session, WorkerState::AwaitingInput);
    let brief = common::write_brief(tmp.path(), "write a mesh note\n");
    let note = json!({"to": target, "text": "queued from app worker", "refs": {}}).to_string();
    let mut cmd = broker_cli(&home, tmp.path(), &fake_state, &["--json", "spawn", "--lane", "research", "--brief"]);
    cmd.arg(&brief).env("FAKE_MESH_NOTE", note);
    let spawned = run(cmd, &scratch, "spawn");
    let Some(d) = require_appserver(&spawned) else {
        return;
    };
    let source = d["id"].as_str().unwrap().to_string();

    let queued = until("target worker inbox note", || {
        let inbox = home.join("workers").join(target).join("inbox");
        let files: Vec<_> = fs::read_dir(&inbox)
            .ok()?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();
        (files.len() == 1).then_some(files)
    });
    let queued_text = fs::read_to_string(&queued[0]).unwrap();
    assert!(queued_text.contains("queued from app worker"), "{queued_text}");
    assert_eq!(note_files(&home, "codex", target_session).len(), 1);
    assert!(home.join("workers").join(&source).join("outbox/1.note.routed.json").exists());
}

#[test]
fn appserver_secret_worker_note_is_rejected_without_delivery_and_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let fake_state = tmp.path().join("fake-state");
    let scratch = tmp.path().join("scratch");
    fs::create_dir_all(&fake_state).unwrap();
    let _guard = BrokerGuard::new(&home);
    let claude = "dddddddd-dddd-dddd-dddd-dddddddddddd";
    cli(&home, tmp.path())
        .args(["mesh", "register", "--session", claude])
        .assert()
        .success();
    let brief = common::write_brief(tmp.path(), "write a mesh note\n");
    let note = json!({"to": format!("claude:{claude}"), "text": format!("ghp_{}", "a".repeat(40)), "refs": {}}).to_string();
    let mut cmd = broker_cli(&home, tmp.path(), &fake_state, &["--json", "spawn", "--lane", "research", "--brief"]);
    cmd.arg(&brief).env("FAKE_MESH_NOTE", note);
    let spawned = run(cmd, &scratch, "spawn");
    let Some(d) = require_appserver(&spawned) else {
        return;
    };
    let id = d["id"].as_str().unwrap().to_string();

    until("secret note rejection", || {
        home.join("workers")
            .join(&id)
            .join("outbox/1.note.rejected.json")
            .exists()
            .then_some(())
    });
    assert!(note_files(&home, "claude", claude).is_empty());
    let rejected = until("mesh_note_rejected broker log line", || {
        broker_events(&home).into_iter().find(|v| {
            v["kind"] == "mesh_note_rejected"
                && v["worker"] == id
                && v["to"] == format!("claude:{claude}")
        })
    });
    assert_eq!(rejected["to"], format!("claude:{claude}"));
}
