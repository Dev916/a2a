//! Owner scoping as an operator meets it (spec §4.8).
//!
//! Two orchestrators sharing one machine is the normal case, not the exotic
//! one: a human shell, a long-running agent, a CI job. Owner scoping is what
//! keeps them from stepping on each other — and, just as importantly, what
//! keeps them from being *locked out* of the workers that existed before owners
//! did.
//!
//! Everything here drives the shipped binary on the **exec** transport
//! (`A2A_BROKER_DISABLE=1`), because ownership is a CLI-layer policy: it is
//! decided from the worker's own record before any transport is consulted, so
//! the guard has to hold with no daemon anywhere in sight.
//!
//! Three properties are pinned:
//!
//! * **Lossless.** The owner lives in the genesis event, so deleting
//!   `state.json` costs a replay and not an owner.
//! * **Open by default to nobody's workers.** `owner: null` — every worker
//!   spawned before this feature existed — matches ANY caller. That is the
//!   null-owner mutation test below, and it is the difference between a new
//!   guard and a fleet-wide lockout.
//! * **Reads are never scoped.** `status`/`tail`/`result`/`wait` answer
//!   questions; only the commands that CHANGE a worker check who is asking.

mod common;

use assert_cmd::Command;
use serde_json::Value;
use std::path::{Path, PathBuf};

/// `a2a` with the Phase-1 hermetic environment, acting as `owner` — or as
/// nobody at all (`None` removes `A2A_OWNER`, which is how `anon-<uid>` is
/// reached under a harness that may well export one).
fn a2a_as(home: &Path, root: &Path, owner: Option<&str>) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home)
        .env("A2A_CODEX_BIN", common::stub_path())
        .env("A2A_ALLOWED_ROOTS", root)
        .env("A2A_BROKER_DISABLE", "1")
        .env("A2A_SHIM_BIN", assert_cmd::cargo::cargo_bin("a2a"));
    match owner {
        Some(o) => c.env("A2A_OWNER", o),
        None => c.env_remove("A2A_OWNER"),
    };
    c
}

struct Fx {
    tmp: tempfile::TempDir,
    home: PathBuf,
    work: PathBuf,
    brief: PathBuf,
}

impl Fx {
    fn new() -> Fx {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let work = tmp.path().join("proj");
        std::fs::create_dir_all(&work).unwrap();
        let brief = common::write_brief(tmp.path(), "do the thing\n");
        Fx {
            home,
            work,
            brief,
            tmp,
        }
    }
    fn root(&self) -> &Path {
        self.tmp.path()
    }
    fn as_(&self, owner: Option<&str>) -> Command {
        a2a_as(&self.home, self.root(), owner)
    }
    /// Spawn a general-lane worker; returns its id.
    fn spawn(
        &self,
        env_owner: Option<&str>,
        flag_owner: Option<&str>,
        stub_env: &[(&str, &str)],
    ) -> String {
        let mut c = self.as_(env_owner);
        c.args(["--json", "spawn", "--lane", "general", "--cwd"])
            .arg(&self.work)
            .arg("--brief")
            .arg(&self.brief);
        if let Some(o) = flag_owner {
            c.args(["--owner", o]);
        }
        for (k, v) in stub_env {
            c.env(k, v);
        }
        let out = c.assert().success();
        json_of(out.get_output())["data"]["id"]
            .as_str()
            .expect("spawn prints an id")
            .to_string()
    }
    /// Spawn and let the stub run to completion.
    fn spawn_done(&self, owner: Option<&str>) -> String {
        let id = self.spawn(owner, None, &[]);
        common::wait_dead(&self.home, &id, 10);
        self.as_(owner)
            .args(["status", &id])
            .assert()
            .code(0)
            .to_string();
        id
    }
    fn snapshot(&self, id: &str) -> Value {
        common::read_snapshot(&self.home, id)
    }
    /// Erase a worker's owner in place, exactly as a worker spawned before
    /// Phase 15 has it: `owner: null` on disk, and no owner key that any later
    /// fold could put back.
    fn unown(&self, id: &str) {
        let p = self.home.join("workers").join(id).join("state.json");
        let mut v: Value = serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        v["owner"] = Value::Null;
        a2a::store::write_atomic(&p, v.to_string().as_bytes()).unwrap();
        assert_eq!(self.snapshot(id)["owner"], Value::Null);
    }
}

fn json_of(out: &std::process::Output) -> Value {
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("stdout is one JSON envelope ({e}): {:?}", out.stdout))
}

/// Run `args` as `owner` and return `(exit code, envelope)`.
fn run(fx: &Fx, owner: Option<&str>, args: &[&str]) -> (i32, Value) {
    let out = fx.as_(owner).args(args).output().expect("a2a runs");
    let code = out.status.code().unwrap_or(-1);
    let body = serde_json::from_slice(&out.stdout).unwrap_or(Value::Null);
    (code, body)
}

/// The pinned refusal: exit 9, code `NOT_OWNER`, and machine-readable detail
/// (an orchestrator that meets one decides whether to re-run as the owner or to
/// force, and it should not have to parse a sentence to do it).
fn assert_not_owner(code: i32, env: &Value, worker_owner: &str, caller: &str) {
    assert_eq!(code, 9, "cross-owner mutation must exit 9: {env}");
    assert_eq!(env["ok"], Value::Bool(false), "{env}");
    assert_eq!(env["error"]["code"], "NOT_OWNER", "{env}");
    assert_eq!(env["error"]["data"]["owner"], worker_owner, "{env}");
    assert_eq!(env["error"]["data"]["caller"], caller, "{env}");
}

// --------------------------------------------------------------- who owns it

#[test]
fn spawn_owner_precedence_is_flag_then_env_then_anon() {
    let fx = Fx::new();

    let flagged = fx.spawn(Some("bob"), Some("alice"), &[]);
    assert_eq!(
        fx.snapshot(&flagged)["owner"], "alice",
        "--owner outranks A2A_OWNER"
    );

    let from_env = fx.spawn(Some("bob"), None, &[]);
    assert_eq!(fx.snapshot(&from_env)["owner"], "bob");

    let anon = fx.spawn(None, None, &[]);
    let owner = fx.snapshot(&anon)["owner"]
        .as_str()
        .expect("an owner is always recorded")
        .to_string();
    assert!(
        owner.starts_with("anon-"),
        "no A2A_OWNER falls back to anon-<uid>, got {owner}"
    );
}

/// The lossless pin: the owner is genesis, and genesis is the WAL. Losing the
/// derived checkpoint must cost a replay, never an owner.
#[test]
fn the_owner_survives_losing_state_json() {
    let fx = Fx::new();
    let id = fx.spawn_done(Some("alice"));
    std::fs::remove_file(fx.home.join("workers").join(&id).join("state.json")).unwrap();

    let (code, env) = run(&fx, Some("alice"), &["--json", "status", &id]);
    assert_eq!(code, 0, "status after losing state.json: {env}");
    assert_eq!(env["data"]["owner"], "alice", "{env}");
    assert_eq!(fx.snapshot(&id)["owner"], "alice", "the rebuild persisted it");
}

// ------------------------------------------------------------------------ ls

#[test]
fn ls_defaults_to_own_plus_unowned_and_all_adds_the_owner_column() {
    let fx = Fx::new();
    let mine = fx.spawn_done(Some("alice"));
    let theirs = fx.spawn_done(Some("bob"));
    let legacy = fx.spawn_done(Some("alice"));
    fx.unown(&legacy);

    let (code, env) = run(&fx, Some("alice"), &["--json", "ls"]);
    assert_eq!(code, 0, "{env}");
    let ids: Vec<&str> = env["data"]["workers"]
        .as_array()
        .expect("a workers array")
        .iter()
        .map(|w| w["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&mine.as_str()), "own worker is listed: {env}");
    assert!(
        ids.contains(&legacy.as_str()),
        "an unowned worker belongs to everyone: {env}"
    );
    assert!(
        !ids.contains(&theirs.as_str()),
        "another owner's worker is hidden by default: {env}"
    );
    let row = env["data"]["workers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["id"] == mine.as_str())
        .unwrap()
        .clone();
    assert_eq!(row["owner"], "alice", "owner is in the ls payload: {row}");

    let (code, env) = run(&fx, Some("alice"), &["--json", "ls", "--all"]);
    assert_eq!(code, 0, "{env}");
    let all: Vec<&str> = env["data"]["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["id"].as_str().unwrap())
        .collect();
    for want in [&mine, &theirs, &legacy] {
        assert!(all.contains(&want.as_str()), "--all lists {want}: {env}");
    }
    let legacy_row = env["data"]["workers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["id"] == legacy.as_str())
        .unwrap()
        .clone();
    assert_eq!(
        legacy_row["owner"],
        Value::Null,
        "an unowned worker reports null, not a guess: {legacy_row}"
    );

    // The table form: `--all` grows an OWNER column, and the header names it.
    let out = fx
        .as_(Some("alice"))
        .args(["ls", "--all"])
        .output()
        .expect("a2a runs");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(stderr.contains("OWNER"), "header names the column: {stderr}");
    let bob_line = stdout
        .lines()
        .find(|l| l.starts_with(&theirs))
        .unwrap_or_else(|| panic!("a row for {theirs} in:\n{stdout}"));
    assert!(bob_line.contains("bob"), "{bob_line}");
    let legacy_line = stdout
        .lines()
        .find(|l| l.starts_with(&legacy))
        .unwrap_or_else(|| panic!("a row for {legacy} in:\n{stdout}"));
    assert!(
        legacy_line.contains(" -  "),
        "an unowned worker prints `-` in the OWNER column: {legacy_line}"
    );

    // ...and the default table does NOT, so the Phase-1 five-column projection
    // is unchanged for the caller who never asked about owners.
    let out = fx.as_(Some("alice")).args(["ls"]).output().unwrap();
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains("OWNER"),
        "the default view is the Phase-1 one"
    );
}

// ------------------------------------------------------------- the guard

#[test]
fn cross_owner_mutations_are_refused_with_not_owner() {
    let fx = Fx::new();
    let id = fx.spawn_done(Some("alice"));

    for args in [
        vec!["--json", "send", id.as_str(), "hello"],
        vec!["--json", "send", id.as_str(), "hello", "--queue-only"],
        vec!["--json", "kill", id.as_str()],
        vec!["--json", "resume", id.as_str()],
        vec!["--json", "interrupt", id.as_str()],
    ] {
        let (code, env) = run(&fx, Some("bob"), &args);
        assert_not_owner(code, &env, "alice", "bob");
        assert_eq!(
            fx.snapshot(&id)["state"], "done",
            "a refused {args:?} changed nothing"
        );
    }
    // The refusal is not a side effect of the message reaching disk either.
    assert!(
        !fx.home.join("workers").join(&id).join("inbox").join("1.json").exists(),
        "a refused send queues nothing"
    );
}

/// The pin that makes this feature safe to ship: every worker that existed
/// before owners did has `owner: null`, and null matches ANY caller. Otherwise
/// the upgrade that introduces the guard is also the upgrade that strands the
/// running fleet.
#[test]
fn a_null_owner_worker_stays_open_to_every_caller() {
    let fx = Fx::new();
    let id = fx.spawn(Some("alice"), None, &[("STUB_SLEEP", "20")]);
    fx.unown(&id);

    let (code, env) = run(&fx, Some("bob"), &["--json", "send", &id, "hi", "--queue-only"]);
    assert_eq!(code, 0, "an unowned worker accepts anyone's send: {env}");

    let (code, env) = run(&fx, Some("carol"), &["--json", "kill", &id]);
    assert_eq!(code, 0, "an unowned worker accepts anyone's kill: {env}");
    assert_eq!(env["data"]["state"], "cancelled", "{env}");

    // ...including the caller who has no A2A_OWNER at all.
    let (code, env) = run(&fx, None, &["--json", "status", &id]);
    assert_eq!(code, 0, "{env}");
}

#[test]
fn force_crosses_the_owner_boundary() {
    let fx = Fx::new();
    let id = fx.spawn(Some("alice"), None, &[("STUB_SLEEP", "20")]);

    let (code, env) = run(&fx, Some("bob"), &["--json", "kill", &id]);
    assert_not_owner(code, &env, "alice", "bob");

    let (code, env) = run(&fx, Some("bob"), &["--json", "kill", &id, "--force"]);
    assert_eq!(code, 0, "--force acts across owners: {env}");
    assert_eq!(env["data"]["state"], "cancelled", "{env}");
    assert_eq!(
        fx.snapshot(&id)["owner"],
        "alice",
        "forcing does not steal the worker"
    );
}

#[test]
fn read_commands_are_never_owner_scoped() {
    let fx = Fx::new();
    let id = fx.spawn_done(Some("alice"));

    for args in [
        vec!["--json", "status", id.as_str()],
        vec!["--json", "tail", id.as_str()],
        vec!["--json", "result", id.as_str()],
        vec!["--json", "wait", id.as_str()],
        vec!["--json", "ls", "--all"],
    ] {
        let (code, env) = run(&fx, Some("bob"), &args);
        assert_eq!(code, 0, "{args:?} is a question, not a mutation: {env}");
        assert_eq!(env["ok"], Value::Bool(true), "{args:?}: {env}");
    }
}

// ------------------------------------------------------------------------ gc

#[test]
fn gc_scopes_to_own_plus_unowned_until_all() {
    let fx = Fx::new();
    let mine = fx.spawn_done(Some("alice"));
    let theirs = fx.spawn_done(Some("bob"));
    let legacy = fx.spawn_done(Some("alice"));
    fx.unown(&legacy);

    let (code, env) = run(
        &fx,
        Some("alice"),
        &["--json", "gc", "--age", "0s", "--dry-run"],
    );
    assert_eq!(code, 0, "{env}");
    let reclaimable: Vec<&str> = env["data"]["reclaimable"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    assert!(reclaimable.contains(&mine.as_str()), "{env}");
    assert!(reclaimable.contains(&legacy.as_str()), "{env}");
    assert!(
        !reclaimable.contains(&theirs.as_str()),
        "another owner's worker is not gc's to reclaim: {env}"
    );
    let kept = env["data"]["kept"].as_array().unwrap().clone();
    let held = kept
        .iter()
        .find(|k| k["id"] == theirs.as_str())
        .unwrap_or_else(|| panic!("a kept row explains the omission: {env}"));
    assert_eq!(held["reason"], "not owner", "{held}");

    let (code, env) = run(&fx, Some("alice"), &["--json", "gc", "--age", "0s"]);
    assert_eq!(code, 0, "{env}");
    assert!(!fx.home.join("workers").join(&mine).exists());
    assert!(!fx.home.join("workers").join(&legacy).exists());
    assert!(
        fx.home.join("workers").join(&theirs).exists(),
        "bob's worker survived alice's gc"
    );

    let (code, env) = run(&fx, Some("alice"), &["--json", "gc", "--age", "0s", "--all"]);
    assert_eq!(code, 0, "{env}");
    assert_eq!(env["data"]["removed"][0], theirs.as_str(), "{env}");
    assert!(!fx.home.join("workers").join(&theirs).exists());
}
