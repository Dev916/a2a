// tests/cmd_spawn.rs
mod common;
use common::*;

#[test]
fn spawn_general_creates_worker_and_runs() {
    let (home, _tmp, id) = spawn_with(&[]);
    assert!(
        regex::Regex::new(r"^general-[0-9]{4}t[0-9]{4}-[a-z2-7]{4}$")
            .unwrap()
            .is_match(&id)
    );
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
        .args([
            "--json", "spawn", "--lane", "general", "--cwd", "/tmp", "--brief",
        ])
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
        .args([
            "spawn",
            "--lane",
            "general",
            "--transport",
            "app-server",
            "--cwd",
        ])
        .arg(tmp.path())
        .arg("--brief")
        .arg(&no_ac)
        .assert()
        .code(7);
    a2a(&home, tmp.path())
        .args([
            "spawn",
            "--lane",
            "general",
            "--transport",
            "carrier-pigeon",
            "--cwd",
        ])
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

// ---------------------------------------------------------------------------
// Result-schema resolution (spec §4.9): --schema > the brief's `result_schema`
// > the impl lane's shipped template.
// ---------------------------------------------------------------------------

use std::path::{Path, PathBuf};

/// The shipped template, read from the repo — so "the worker got the default"
/// is pinned against the file that is actually published, not a copy.
fn shipped_template() -> String {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("templates/impl-result.schema.json"),
    )
    .expect("templates/impl-result.schema.json is checked in")
}

fn brief_with(dir: &Path, front: &str) -> PathBuf {
    write_brief(
        dir,
        &format!("---\nacceptance_criteria:\n  - it works\nverify:\n  - true\n{front}---\ndo it\n"),
    )
}

/// `a2a --json spawn --lane <lane> --cwd <work> --brief <brief> [extra…]`.
fn spawn_schema(
    home: &Path,
    allowed: &Path,
    lane: &str,
    work: &Path,
    brief: &Path,
    extra: &[&str],
) -> std::process::Output {
    a2a(home, allowed)
        .args(["--json", "spawn", "--lane", lane, "--cwd"])
        .arg(work)
        .arg("--brief")
        .arg(brief)
        .args(extra)
        .output()
        .unwrap()
}

fn worker_schema(home: &Path, id: &str) -> PathBuf {
    home.join("workers").join(id).join("result-schema.json")
}

/// The impl lane's default: the shipped template, materialized 0600 inside the
/// worker, recorded on the snapshot AND in genesis — so a rebuild from the WAL
/// still knows which schema this worker was judged against.
#[test]
fn the_impl_lane_materializes_the_shipped_schema_and_records_its_source() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = brief_with(tmp.path(), "");

    let out = spawn_schema(&home, tmp.path(), "impl", &work, &brief, &[]);
    assert!(
        out.status.success(),
        "{:?}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();

    let path = worker_schema(&home, &id);
    assert_eq!(
        std::fs::read_to_string(&path).expect("the worker carries its own schema"),
        shipped_template(),
        "the impl default IS templates/impl-result.schema.json"
    );
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["schema_path"], serde_json::json!(path), "{snap}");
    assert_eq!(
        snap["schema_source"],
        serde_json::json!("lane-default"),
        "{snap}"
    );

    // Lossless: the label lives in GENESIS, so it survives a rebuild from zero.
    wait_dead(&home, &id, 30);
    a2a(&home, tmp.path())
        .args(["status", &id])
        .output()
        .unwrap();
    std::fs::remove_file(home.join("workers").join(&id).join("state.json")).unwrap();
    a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .output()
        .unwrap();
    let rebuilt = read_snapshot(&home, &id);
    assert_eq!(
        rebuilt["schema_source"],
        serde_json::json!("lane-default"),
        "the schema's provenance must survive a rebuild from zero: {rebuilt}"
    );
    assert_eq!(rebuilt["schema_path"], serde_json::json!(path), "{rebuilt}");
}

/// The middle rung: a brief's `result_schema` is resolved **relative to the
/// brief's own directory**, and its contents — not the template's — become the
/// worker's schema.
#[test]
fn a_briefs_result_schema_is_resolved_beside_the_brief() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let sibling = tmp.path().join("task.schema.json");
    let body = r#"{"type":"object","required":["verdict"]}"#;
    std::fs::write(&sibling, body).unwrap();
    let brief = brief_with(tmp.path(), "result_schema: ./task.schema.json\n");

    let out = spawn_schema(&home, tmp.path(), "impl", &work, &brief, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();

    assert_eq!(
        std::fs::read_to_string(worker_schema(&home, &id)).unwrap(),
        body,
        "the brief's schema beats the lane default"
    );
    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["schema_source"], serde_json::json!("brief"), "{snap}");
}

/// A brief naming a schema that is not there is a static precondition failure —
/// exit 9, `SCHEMA_NOT_FOUND`, and no worker left behind.
#[test]
fn a_briefs_missing_result_schema_refuses_the_spawn() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = brief_with(tmp.path(), "result_schema: ./no-such.schema.json\n");

    let out = spawn_schema(&home, tmp.path(), "impl", &work, &brief, &[]);
    assert_eq!(out.status.code(), Some(9));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["error"]["code"], "SCHEMA_NOT_FOUND", "{v}");
    assert_eq!(
        std::fs::read_dir(home.join("workers"))
            .map(|d| d.count())
            .unwrap_or(0),
        0,
        "a refused spawn leaves no worker"
    );
}

/// `--schema` outranks both, and a lane with no default keeps `schema_path`
/// null: the shipped template is impl-only.
#[test]
fn the_flag_outranks_the_brief_and_general_has_no_default() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let flagged = tmp.path().join("flag.schema.json");
    let body = r#"{"type":"object","required":["flagged"]}"#;
    std::fs::write(&flagged, body).unwrap();
    let sibling = tmp.path().join("task.schema.json");
    std::fs::write(&sibling, r#"{"type":"object","required":["verdict"]}"#).unwrap();
    let brief = brief_with(tmp.path(), "result_schema: ./task.schema.json\n");

    let out = spawn_schema(
        &home,
        tmp.path(),
        "impl",
        &work,
        &brief,
        &["--schema", flagged.to_str().unwrap()],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        std::fs::read_to_string(worker_schema(&home, &id)).unwrap(),
        body
    );
    assert_eq!(
        read_snapshot(&home, &id)["schema_source"],
        serde_json::json!("flag")
    );

    // The general lane: no flag, no brief key, no default.
    let plain = write_brief(&work, "freeform\n");
    let out = spawn_schema(&home, tmp.path(), "general", &work, &plain, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let snap = read_snapshot(&home, v["data"]["id"].as_str().unwrap());
    assert_eq!(snap["schema_path"], serde_json::Value::Null, "{snap}");
    assert_eq!(snap.get("schema_source"), None, "{snap}");
}

/// The gate that this task's default schema makes live for every impl worker:
/// a plain-text result no longer reaches `done`.
#[test]
fn an_impl_result_that_is_not_the_devloop_shape_fails_the_shipped_gate() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = brief_with(tmp.path(), "");

    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "impl", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();
    wait_dead(&home, &id, 30);
    let out = a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["data"]["state"], "failed", "{v}");
    assert_eq!(v["data"]["failure_class"], "schema_violation", "{v}");

    // …and the devloop shape passes it.
    let ok = tmp.path().join("second-brief.md");
    std::fs::copy(&brief, &ok).unwrap();
    let out = a2a(&home, tmp.path())
        .env(
            "STUB_LAST_MSG",
            r#"{"status":"pass","iterations":1,"summary":"ok","evidence":{},"files_changed":[],"next_step_hint":null}"#,
        )
        .args(["--json", "spawn", "--lane", "impl", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&ok)
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();
    wait_dead(&home, &id, 30);
    let out = a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["data"]["state"], "done", "{v}");
}
