// tests/cmd_respawn.rs
//
// `a2a respawn <id>` (spec §4.9): a retry is an ORDINARY spawn that happens to
// remember where it came from. Every case here is broker-free — `common::a2a()`
// sets `A2A_BROKER_DISABLE=1`, so the child takes the exec path with the stub
// codex, and `a2a kill` is how a parent is put into a respawnable state.
mod common;
use common::*;
use serde_json::Value;
use std::path::{Path, PathBuf};

struct Fix {
    home: PathBuf,
    work: PathBuf,
    brief: PathBuf,
    tmp: tempfile::TempDir,
}

fn fixture() -> Fix {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = write_brief(tmp.path(), "do the thing\n");
    Fix {
        home,
        work,
        brief,
        tmp,
    }
}

impl Fix {
    fn root(&self) -> &Path {
        self.tmp.path()
    }

    /// A general-lane parent. `sleep` keeps the stub alive so the worker is
    /// observably `running` until something stops it.
    fn spawn(&self, sleep: Option<&str>) -> String {
        let mut c = a2a(&self.home, self.root());
        if let Some(s) = sleep {
            c.env("STUB_SLEEP", s);
        }
        let out = c
            .args(["--json", "spawn", "--lane", "general", "--cwd"])
            .arg(&self.work)
            .arg("--brief")
            .arg(&self.brief)
            .assert()
            .success();
        json_of(&out)
            .pointer("/data/id")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }

    fn kill(&self, id: &str) {
        a2a(&self.home, self.root())
            .args(["--json", "kill", id])
            .assert()
            .success();
    }

    fn cmd(&self) -> assert_cmd::Command {
        a2a(&self.home, self.root())
    }

    fn rows(&self) -> Vec<Value> {
        let out = self
            .cmd()
            .args(["--json", "ls", "--all"])
            .assert()
            .success();
        json_of(&out)["data"]["workers"].as_array().unwrap().clone()
    }

    fn row(&self, id: &str) -> Value {
        self.rows()
            .into_iter()
            .find(|r| r["id"] == id)
            .unwrap_or_else(|| panic!("{id} not listed"))
    }
}

fn json_of(out: &assert_cmd::assert::Assert) -> Value {
    serde_json::from_slice(&out.get_output().stdout).expect("stdout is one JSON envelope")
}

/// A retry inherits identity and records where it came from.
#[test]
fn a_respawn_records_lineage_and_inherits_the_parents_identity() {
    let f = fixture();
    let parent = f.spawn(Some("30"));
    f.kill(&parent);

    let out = f
        .cmd()
        .args(["--json", "respawn", &parent])
        .assert()
        .success();
    let data = json_of(&out)["data"].clone();
    let child = data["id"].as_str().unwrap().to_string();
    assert_ne!(child, parent, "a respawn is a NEW worker");
    assert_eq!(data["parent_id"], parent);
    assert_eq!(data["attempt"], 2);

    // `ls --json` is the fleet view, and it carries the lineage too.
    let row = f.row(&child);
    assert_eq!(row["parent_id"], parent);
    assert_eq!(row["attempt"], 2);
    assert_eq!(row["lane"], "general");
    assert_eq!(row["owner"], TEST_OWNER);
    // The parent is attempt 1 by absence: nothing about it changed on disk.
    let prow = f.row(&parent);
    assert_eq!(prow["parent_id"], Value::Null);
    assert_eq!(prow["attempt"], Value::Null);

    let psnap = read_snapshot(&f.home, &parent);
    let csnap = read_snapshot(&f.home, &child);
    assert_eq!(csnap["cwd"], psnap["cwd"], "same workspace");
    assert_eq!(csnap["owner"], psnap["owner"]);
    assert_eq!(csnap["lane"], psnap["lane"]);
    assert_eq!(csnap["model"], psnap["model"]);
    assert_eq!(csnap["effort"], psnap["effort"]);
    assert_eq!(
        csnap["brief_sha"], psnap["brief_sha"],
        "the same brief, byte-identical"
    );
    assert_eq!(csnap["parent_id"], parent);
    assert_eq!(csnap["attempt"], 2);
}

/// `--brief` replaces the brief — and the sha proves the child is judged
/// against the new text.
#[test]
fn a_replacement_brief_changes_the_childs_sha() {
    let f = fixture();
    let parent = f.spawn(Some("30"));
    f.kill(&parent);
    let other = f.root().join("other-brief.md");
    std::fs::write(&other, "try it a different way\n").unwrap();

    let out = f
        .cmd()
        .args(["--json", "respawn", &parent, "--brief"])
        .arg(&other)
        .assert()
        .success();
    let child = json_of(&out)["data"]["id"].as_str().unwrap().to_string();
    let psnap = read_snapshot(&f.home, &parent);
    let csnap = read_snapshot(&f.home, &child);
    assert_ne!(csnap["brief_sha"], psnap["brief_sha"]);
    assert_eq!(csnap["parent_id"], parent);
    assert_eq!(csnap["attempt"], 2);
}

/// A replacement brief goes through the SAME §5 ingress gates every spawn's
/// brief does — a respawn is not a way around the secret scan.
#[test]
fn a_replacement_brief_is_scanned_like_any_other() {
    let f = fixture();
    let parent = f.spawn(Some("30"));
    f.kill(&parent);
    let leaky = f.root().join("leaky.md");
    std::fs::write(&leaky, "key: sk-abc123456789012345678901\n").unwrap();

    let out = f
        .cmd()
        .args(["--json", "respawn", &parent, "--brief"])
        .arg(&leaky)
        .assert()
        .code(9);
    assert_eq!(json_of(&out)["error"]["code"], "SECRET_IN_BRIEF");
}

/// Three identical retries is the poison-brief signature (spec §4.9): the
/// FOURTH attempt is refused rather than spent.
#[test]
fn the_attempt_chain_is_quarantined_at_max_attempts() {
    let f = fixture();
    let a1 = f.spawn(Some("30"));
    f.kill(&a1);

    let out = f.cmd().args(["--json", "respawn", &a1]).assert().success();
    let a2 = json_of(&out)["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(read_snapshot(&f.home, &a2)["attempt"], 2);
    f.kill(&a2);

    let out = f.cmd().args(["--json", "respawn", &a2]).assert().success();
    let a3 = json_of(&out)["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(read_snapshot(&f.home, &a3)["attempt"], 3);
    assert_eq!(read_snapshot(&f.home, &a3)["parent_id"], a2);
    f.kill(&a3);

    // next = 3 + 1 = 4 > max_attempts 3.
    let out = f.cmd().args(["--json", "respawn", &a3]).assert().code(9);
    let err = json_of(&out)["error"].clone();
    assert_eq!(err["code"], "ATTEMPTS_EXHAUSTED");
    assert_eq!(
        err["data"],
        serde_json::json!({"parent_id": a3, "attempt": 3, "max_attempts": 3})
    );
    assert!(
        err["remedy"].as_str().unwrap().contains("poison-brief"),
        "{err}"
    );
    // Nothing was spawned: the fleet still has exactly three workers.
    assert_eq!(f.rows().len(), 3);
}

/// A parent that is still working is not a retry candidate.
#[test]
fn a_running_parent_is_refused() {
    let f = fixture();
    let parent = f.spawn(Some("30"));
    assert_eq!(read_snapshot(&f.home, &parent)["state"], "running");

    let out = f
        .cmd()
        .args(["--json", "respawn", &parent])
        .assert()
        .code(9);
    let err = json_of(&out)["error"].clone();
    assert_eq!(err["code"], "PARENT_ACTIVE");
    assert!(
        err["remedy"].as_str().unwrap().contains("a2a kill"),
        "{err}"
    );
    assert_eq!(f.rows().len(), 1, "nothing was spawned");
    f.kill(&parent);
}

/// A forced cross-owner respawn is a REPAIR, not a transfer: the child belongs
/// to the parent's owner, never to the caller who forced it.
#[test]
fn cross_owner_needs_force_and_the_child_keeps_the_parents_owner() {
    let f = fixture();
    let parent = f.spawn(Some("30"));
    f.kill(&parent);

    let out = f
        .cmd()
        .env("A2A_OWNER", "other")
        .args(["--json", "respawn", &parent])
        .assert()
        .code(9);
    assert_eq!(json_of(&out)["error"]["code"], "NOT_OWNER");

    let out = f
        .cmd()
        .env("A2A_OWNER", "other")
        .args(["--json", "respawn", &parent, "--force"])
        .assert()
        .success();
    let child = json_of(&out)["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        read_snapshot(&f.home, &child)["owner"],
        TEST_OWNER,
        "a forced respawn repairs the parent's work, it does not take it over"
    );
}

/// `state.json` is a derived checkpoint: the lineage lives in genesis, so it
/// comes back from the WAL.
#[test]
fn lineage_survives_losing_state_json() {
    let f = fixture();
    let parent = f.spawn(Some("30"));
    f.kill(&parent);
    let out = f
        .cmd()
        .args(["--json", "respawn", &parent])
        .assert()
        .success();
    let child = json_of(&out)["data"]["id"].as_str().unwrap().to_string();
    wait_dead(&f.home, &child, 20);
    // Settle the child first, so what the rebuild below has to reproduce is a
    // finished worker rather than a race with its own exit.
    let _ = f.cmd().args(["--json", "status", &child]).assert();

    std::fs::remove_file(f.home.join("workers").join(&child).join("state.json")).unwrap();
    // The exit CODE is the worker's state and is not what this test is about;
    // the rebuild `status` performs on the way is.
    let _ = f.cmd().args(["--json", "status", &child]).assert();

    let snap = read_snapshot(&f.home, &child);
    assert_eq!(snap["parent_id"], parent);
    assert_eq!(snap["attempt"], 2);
}
