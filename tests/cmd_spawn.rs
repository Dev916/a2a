// tests/cmd_spawn.rs
mod common;
use common::*;

#[test]
fn spawn_general_creates_worker_and_runs() {
    let (home, _tmp, id) = spawn_with(&[]);
    assert!(regex::Regex::new(r"^general-[0-9]{4}t[0-9]{4}-[a-z2-7]{4}$")
        .unwrap()
        .is_match(&id));
    let wdir = home.join("workers").join(&id);
    for f in ["brief.md", "prompt.md", "state.json", "events.jsonl"] {
        assert!(wdir.join(f).exists(), "{f}");
    }
    for d in ["outbox", "inbox", "inbox/delivered", "scratch", "context"] {
        assert!(wdir.join(d).is_dir(), "{d}");
    }
    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["state"], "running");
    assert_eq!(snap["transport"], "exec");
    assert_eq!(snap["sandbox"], "workspace-write");
    assert_eq!(snap["model"], "gpt-5.5");
    assert_eq!(snap["brief_sha"].as_str().unwrap().len(), 64);
}

#[test]
fn cwd_allowlist_canonicalizes_both_sides() {
    // spawn_with passes the UN-canonicalized tempdir as the allowed root while --cwd
    // canonicalizes to /private/var/... on macOS — success proves both sides are canonicalized.
    let (_home, _tmp, id) = spawn_with(&[]);
    assert!(!id.is_empty());
}

#[test]
fn cwd_outside_allowed_roots_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let brief = write_brief(tmp.path(), "x");
    let out = a2a(&tmp.path().join("home"), &tmp.path().join("only-here"))
        .args(["--json", "spawn", "--lane", "general", "--cwd", "/tmp", "--brief"])
        .arg(&brief)
        .assert()
        .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "CWD_NOT_ALLOWED");
}

#[test]
fn research_cwd_is_fixed_to_scratch() {
    let tmp = tempfile::tempdir().unwrap();
    let brief = write_brief(tmp.path(), "question\n");
    let home = tmp.path().join("home");
    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "research", "--cwd"])
        .arg(tmp.path())
        .arg("--brief")
        .arg(&brief)
        .assert()
        .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "RESEARCH_CWD_FIXED");
    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "research", "--brief"])
        .arg(&brief)
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let snap = read_snapshot(&home, v["data"]["id"].as_str().unwrap());
    assert!(snap["cwd"].as_str().unwrap().ends_with("/scratch"));
}

#[test]
fn secret_in_brief_fails_with_no_litter_and_no_echo() {
    let tmp = tempfile::tempdir().unwrap();
    let brief = write_brief(tmp.path(), "key: sk-abc123456789012345678901\n");
    let home = tmp.path().join("home");
    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(tmp.path())
        .arg("--brief")
        .arg(&brief)
        .assert()
        .code(9);
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.get_output().stdout),
        String::from_utf8_lossy(&out.get_output().stderr)
    );
    assert!(all.contains("SECRET_IN_BRIEF") && all.contains("line 1"));
    assert!(!all.contains("sk-abc"), "secret value echoed");
    assert_eq!(
        std::fs::read_dir(home.join("workers"))
            .map(|d| d.count())
            .unwrap_or(0),
        0
    );
}

#[test]
fn context_staging_refuses_secret_files() {
    let tmp = tempfile::tempdir().unwrap();
    let brief = write_brief(tmp.path(), "x\n");
    let envfile = tmp.path().join(".env.production");
    std::fs::write(&envfile, "TOKEN=hunter2\n").unwrap();
    let out = a2a(&tmp.path().join("home"), tmp.path())
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(tmp.path())
        .arg("--brief")
        .arg(&brief)
        .arg("--context")
        .arg(&envfile)
        .assert()
        .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "SECRET_FILE_REFUSED");
}

#[test]
fn brief_transport_and_elevation_gates() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let no_ac = write_brief(tmp.path(), "no front matter\n");
    a2a(&home, tmp.path())
        .args(["spawn", "--lane", "impl", "--cwd"])
        .arg(tmp.path())
        .arg("--brief")
        .arg(&no_ac)
        .assert()
        .code(9);
    // An explicitly-named app-server transport with no broker to reach (these
    // tests run with `A2A_BROKER_DISABLE=1`) is a REACHABILITY failure — exit 7,
    // never the static exit 9, which is now only for a transport string this
    // build could never offer at all.
    a2a(&home, tmp.path())
        .args(["spawn", "--lane", "general", "--transport", "app-server", "--cwd"])
        .arg(tmp.path())
        .arg("--brief")
        .arg(&no_ac)
        .assert()
        .code(7);
    a2a(&home, tmp.path())
        .args(["spawn", "--lane", "general", "--transport", "carrier-pigeon", "--cwd"])
        .arg(tmp.path())
        .arg("--brief")
        .arg(&no_ac)
        .assert()
        .code(9);
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("config.toml"),
        "[lanes.general]\nsandbox = \"danger-full-access\"\n",
    )
    .unwrap();
    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(tmp.path())
        .arg("--brief")
        .arg(&no_ac)
        .assert()
        .code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "ELEVATION_NOT_PERMITTED");
}
