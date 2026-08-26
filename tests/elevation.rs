// tests/elevation.rs
//
// The §5 elevation gate: `danger-full-access` is reachable, and reaching it
// costs a lane permission (`allow_elevation`, config), a per-spawn intent
// (`--danger-full-access`, flag) and an audited `--reason`. Neither half alone
// suffices — the flag without the permission refuses, the permission without
// the flag refuses — which is the whole point of the section and the reason
// the table below is exercised row by row rather than sampled.
//
// Two things here are security-load-bearing rather than merely behavioural:
// every refusal happens BEFORE the first byte is written (a refused elevation
// leaves no worker, no worktree and no audit line), and every elevated spawn
// leaves exactly one 0600 `audit.jsonl` line whose reason is the operator's
// text verbatim.
mod common;
use common::*;
use serde_json::Value;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// A general-lane brief: no front matter required, so nothing in these tests
/// depends on the impl lane's acceptance-criteria rules.
fn brief(dir: &Path) -> PathBuf {
    write_brief(dir, "do the thing\n")
}

fn write_config(home: &Path, body: &str) {
    std::fs::create_dir_all(home).unwrap();
    std::fs::write(home.join("config.toml"), body).unwrap();
}

struct Run {
    code: i32,
    data: Value,
    error: Value,
    stderr: String,
}

impl Run {
    fn id(&self) -> String {
        self.data["id"].as_str().expect("an id").to_string()
    }
}

/// `a2a --json spawn --lane <lane> --cwd <cwd> --brief <brief> <extra…>`, run
/// to completion whatever its exit code — the refusals are the subject here.
fn spawn(home: &Path, root: &Path, lane: &str, extra: &[&str], env: &[(&str, &str)]) -> Run {
    let b = brief(root);
    let mut c = a2a(home, root);
    c.args(["--json", "spawn", "--lane", lane, "--cwd"])
        .arg(root)
        .arg("--brief")
        .arg(&b)
        .args(extra);
    for (k, v) in env {
        c.env(k, v);
    }
    let out = c.output().unwrap();
    let v: Value = serde_json::from_slice(&out.stdout).unwrap_or(Value::Null);
    Run {
        code: out.status.code().unwrap_or(-1),
        data: v.get("data").cloned().unwrap_or(Value::Null),
        error: v.get("error").cloned().unwrap_or(Value::Null),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

fn audit_path(home: &Path) -> PathBuf {
    home.join("audit.jsonl")
}

fn audit_lines(home: &Path) -> Vec<Value> {
    let text = std::fs::read_to_string(audit_path(home)).expect("audit.jsonl");
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("one complete JSON object per line"))
        .collect()
}

/// No worker was created at all — the pre-first-byte guarantee.
fn assert_no_workers(home: &Path) {
    let dir = home.join("workers");
    let listed: Vec<_> = std::fs::read_dir(&dir)
        .map(|d| d.filter_map(Result::ok).map(|e| e.file_name()).collect())
        .unwrap_or_default();
    assert!(
        listed.is_empty(),
        "a refusal left workers behind: {listed:?}"
    );
}

// ---------------------------------------------------------------------------
// The seven-row gate table (spec §5)
// ---------------------------------------------------------------------------

/// Row 1: the ordinary case. No flag, no elevated lane — the lane's own
/// sandbox, no audit file anywhere.
#[test]
fn row1_no_flag_and_an_unelevated_lane_just_spawns() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let r = spawn(&home, tmp.path(), "general", &[], &[]);
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(r.data["sandbox"], "workspace-write", "{}", r.data);
    assert!(
        !audit_path(&home).exists(),
        "an unelevated spawn must not create audit.jsonl"
    );
}

/// Row 2: a lane *configured* `danger-full-access` with no permission refuses —
/// and the Phase-1 prose ("not implemented in Phase 1", "give the lane a
/// non-elevated sandbox") is gone, because both sentences are now false.
#[test]
fn row2_configured_elevation_without_permission_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    write_config(&home, "[lanes.general]\nsandbox = \"danger-full-access\"\n");
    let r = spawn(&home, tmp.path(), "general", &[], &[]);
    assert_eq!(r.code, 9, "{}", r.stderr);
    assert_eq!(r.error["code"], "ELEVATION_NOT_PERMITTED", "{}", r.error);
    let message = r.error["message"].as_str().unwrap_or_default();
    let remedy = r.error["remedy"].as_str().unwrap_or_default();
    assert!(
        !message.contains("not implemented in Phase 1"),
        "the Phase-1 sentence is false now: {message}"
    );
    assert!(
        !remedy.contains("non-elevated sandbox"),
        "the Phase-1 remedy is false now: {remedy}"
    );
    assert!(
        remedy.contains("allow_elevation = true") && remedy.contains("--danger-full-access"),
        "{remedy}"
    );
    assert_no_workers(&home);
}

/// Row 3: the flag without the permission. The config is the permission, and a
/// flag can never grant itself one.
#[test]
fn row3_the_flag_without_the_lane_permission_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let r = spawn(
        &home,
        tmp.path(),
        "general",
        &["--danger-full-access", "--reason", "why not"],
        &[],
    );
    assert_eq!(r.code, 9, "{}", r.stderr);
    assert_eq!(r.error["code"], "ELEVATION_NOT_PERMITTED", "{}", r.error);
    let remedy = r.error["remedy"].as_str().unwrap_or_default();
    assert!(
        remedy.contains("allow_elevation = true")
            && remedy.contains("[lanes.general]")
            && remedy.contains("config.toml"),
        "{remedy}"
    );
    assert_no_workers(&home);
    assert!(
        !audit_path(&home).exists(),
        "a refusal writes no audit line"
    );
}

/// Row 4: permitted, intended — and unexplained. A blank reason is no reason.
#[test]
fn row4_an_elevation_without_a_reason_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    write_config(&home, "[lanes.general]\nallow_elevation = true\n");
    for extra in [
        vec!["--danger-full-access"],
        vec!["--danger-full-access", "--reason", ""],
        vec!["--danger-full-access", "--reason", "   \t "],
    ] {
        let r = spawn(&home, tmp.path(), "general", &extra, &[]);
        assert_eq!(r.code, 9, "{extra:?}: {}", r.stderr);
        assert_eq!(
            r.error["code"], "ELEVATION_REASON_REQUIRED",
            "{extra:?}: {}",
            r.error
        );
        assert!(
            r.error["remedy"]
                .as_str()
                .unwrap_or_default()
                .contains("--reason"),
            "{}",
            r.error
        );
    }
    assert_no_workers(&home);
    assert!(!audit_path(&home).exists());
}

/// Row 5: permission + intent + reason ⇒ the worker actually runs elevated, and
/// `codex` is told so (`-s danger-full-access` in the child's argv).
#[test]
fn row5_permission_intent_and_reason_elevate_the_worker() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let argv_file = tmp.path().join("argv.txt");
    write_config(&home, "[lanes.general]\nallow_elevation = true\n");
    let r = spawn(
        &home,
        tmp.path(),
        "general",
        &["--danger-full-access", "--reason", "rotate the CI runner"],
        &[("STUB_ARGV_FILE", argv_file.to_str().unwrap())],
    );
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(r.data["sandbox"], "danger-full-access", "{}", r.data);
    wait_dead(&home, &r.id(), 10);
    let argv = std::fs::read_to_string(&argv_file).expect("the stub recorded its argv");
    assert!(
        argv.contains("-s danger-full-access"),
        "the effective sandbox must reach codex: {argv}"
    );
}

/// Row 6: `--reason` alone is a no-op, loudly. It does not elevate, and it does
/// not fail — but it never passes silently either.
#[test]
fn row6_a_reason_without_the_flag_warns_and_proceeds() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    write_config(&home, "[lanes.general]\nallow_elevation = true\n");
    let r = spawn(
        &home,
        tmp.path(),
        "general",
        &["--reason", "just in case"],
        &[],
    );
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(r.data["sandbox"], "workspace-write", "{}", r.data);
    assert!(
        r.stderr
            .contains("--reason has no effect without --danger-full-access"),
        "{}",
        r.stderr
    );
    assert!(!audit_path(&home).exists(), "no elevation, no audit line");
}

/// Row 7: a permitted, configured-elevated lane STILL needs the per-spawn
/// intent. Config alone never elevates.
#[test]
fn row7_config_alone_never_elevates() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    write_config(
        &home,
        "[lanes.general]\nsandbox = \"danger-full-access\"\nallow_elevation = true\n",
    );
    let r = spawn(&home, tmp.path(), "general", &[], &[]);
    assert_eq!(r.code, 9, "{}", r.stderr);
    assert_eq!(r.error["code"], "ELEVATION_FLAG_REQUIRED", "{}", r.error);
    assert_no_workers(&home);
}

// ---------------------------------------------------------------------------
// audit.jsonl
// ---------------------------------------------------------------------------

/// The audit file is the record that survives the worker: 0600, append-only,
/// one complete JSON line per elevated spawn carrying EXACTLY five keys.
///
/// The key *order* is deliberately unasserted — `serde_json`'s map is a
/// `BTreeMap`, so the line is alphabetical by construction and nothing reads
/// this file positionally.
#[test]
fn every_elevated_spawn_appends_one_pinned_0600_audit_line() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    write_config(&home, "[lanes.general]\nallow_elevation = true\n");

    let first = spawn(
        &home,
        tmp.path(),
        "general",
        &["--danger-full-access", "--reason", "fix the CI runner"],
        &[],
    );
    assert_eq!(first.code, 0, "{}", first.stderr);

    let mode = std::fs::metadata(audit_path(&home))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "audit.jsonl must be owner-only");

    let lines = audit_lines(&home);
    assert_eq!(lines.len(), 1, "one line per elevated spawn: {lines:?}");
    let keys: std::collections::BTreeSet<&str> = lines[0]
        .as_object()
        .expect("a JSON object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        ["id", "lane", "owner", "reason", "ts"]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "the key SET is pinned: {}",
        lines[0]
    );
    assert_eq!(lines[0]["id"], Value::String(first.id()));
    assert_eq!(lines[0]["lane"], "general");
    assert_eq!(lines[0]["owner"], TEST_OWNER);
    assert_eq!(
        lines[0]["reason"], "fix the CI runner",
        "the operator's text, verbatim"
    );
    assert!(
        chrono::DateTime::parse_from_rfc3339(lines[0]["ts"].as_str().unwrap()).is_ok(),
        "ts is RFC-3339: {}",
        lines[0]["ts"]
    );

    // O_APPEND: the second elevation adds a line, it does not replace one.
    let second = spawn(
        &home,
        tmp.path(),
        "general",
        &["--danger-full-access", "--reason", "and again"],
        &[],
    );
    assert_eq!(second.code, 0, "{}", second.stderr);
    let lines = audit_lines(&home);
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert_eq!(lines[0]["id"], Value::String(first.id()));
    assert_eq!(lines[1]["id"], Value::String(second.id()));
    assert_eq!(lines[1]["reason"], "and again");
}

// ---------------------------------------------------------------------------
// ingress: the reason is scanned exactly like the brief
// ---------------------------------------------------------------------------

/// A credential pasted into `--reason` is the same leak a credential in the
/// brief is — and the refusal names the RULE, never the value.
#[test]
fn a_secret_in_the_reason_refuses_before_anything_is_written() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    write_config(&home, "[lanes.general]\nallow_elevation = true\n");
    let secret = "sk-abcdefghijklmnopqrstuvwxyz0123";
    let r = spawn(
        &home,
        tmp.path(),
        "general",
        &[
            "--danger-full-access",
            "--reason",
            &format!("use {secret} to publish"),
        ],
        &[],
    );
    assert_eq!(r.code, 9, "{}", r.stderr);
    assert_eq!(r.error["code"], "SECRET_IN_REASON", "{}", r.error);
    let whole = format!("{}{}", r.error, r.stderr);
    assert!(whole.contains("openai_key"), "name the rule: {whole}");
    assert!(!whole.contains(secret), "never the value: {whole}");
    assert_no_workers(&home);
    assert!(!audit_path(&home).exists());
}

// ---------------------------------------------------------------------------
// transport: an elevation never rides the broker socket
// ---------------------------------------------------------------------------

/// `--transport app-server --danger-full-access` is a STATIC refusal (exit 9),
/// not a reachability one: the socket may not carry an elevation whether or not
/// a broker is listening.
#[test]
fn an_elevation_on_the_app_server_transport_is_refused_statically() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    write_config(&home, "[lanes.general]\nallow_elevation = true\n");
    let r = spawn(
        &home,
        tmp.path(),
        "general",
        &[
            "--danger-full-access",
            "--reason",
            "needs the whole disk",
            "--transport",
            "app-server",
        ],
        &[],
    );
    assert_eq!(r.code, 9, "exit 9, not 7: {}", r.stderr);
    assert_eq!(r.error["code"], "ELEVATION_EXEC_ONLY", "{}", r.error);
    assert!(
        r.error["remedy"]
            .as_str()
            .unwrap_or_default()
            .contains("--transport app-server"),
        "{}",
        r.error
    );
    assert_no_workers(&home);
    assert!(!audit_path(&home).exists());
}

/// `--transport auto` + elevation takes exec **without opening a socket at
/// all** — no broker is autostarted, and no `transport_degraded` envelope is
/// written, because nothing was lost: the elevation chose the path.
#[test]
fn an_elevated_auto_spawn_takes_exec_without_touching_the_broker() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    write_config(&home, "[lanes.general]\nallow_elevation = true\n");
    // The broker is ENABLED for this one (`A2A_BROKER_DISABLE=""` reads as
    // "not disabled"): an auto spawn that did not short-circuit would autostart
    // a daemon right here, and the guard is what retires one if it did.
    let guard = BrokerGuard::new(&home);
    let r = spawn(
        &home,
        tmp.path(),
        "general",
        &["--danger-full-access", "--reason", "full disk, briefly"],
        &[("A2A_BROKER_DISABLE", "")],
    );
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(r.data["transport"], "exec", "{}", r.data);
    assert!(
        !home.join("broker.sock").exists(),
        "an elevated spawn must not autostart a broker"
    );
    assert!(broker_processes(&home).is_empty(), "no daemon may be left");

    let events =
        std::fs::read_to_string(home.join("workers").join(r.id()).join("events.jsonl")).unwrap();
    assert!(
        !events.contains("transport_degraded"),
        "nothing was lost, so nothing degraded: {events}"
    );
    drop(guard);
}

// ---------------------------------------------------------------------------
// visibility + lossless rebuild
// ---------------------------------------------------------------------------

/// An elevated worker must never look identical to an ordinary one, and the
/// fact must survive losing `state.json`: genesis is the only writer of
/// identity, so `elevation_reason`, the effective `sandbox` and the
/// `"elevation"` transport selection all come back from the WAL alone.
#[test]
fn elevation_is_visible_everywhere_and_survives_a_rebuild_from_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    write_config(&home, "[lanes.general]\nallow_elevation = true\n");
    let reason = "rotate the signing key";
    let r = spawn(
        &home,
        tmp.path(),
        "general",
        &["--danger-full-access", "--reason", reason],
        &[],
    );
    assert_eq!(r.code, 0, "{}", r.stderr);
    let id = r.id();
    wait_dead(&home, &id, 10);

    // ls: the human table marks the STATE cell, and --json carries the reason.
    let out = a2a(&home, tmp.path()).args(["ls"]).output().unwrap();
    let table = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(table.contains("/ELEV"), "the STATE cell is marked: {table}");
    let out = a2a(&home, tmp.path())
        .args(["--json", "ls"])
        .output()
        .unwrap();
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    let row = v["data"]["workers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["id"] == Value::String(id.clone()))
        .expect("the worker is listed")
        .clone();
    assert_eq!(row["elevation_reason"], reason, "{row}");

    // …and again from zero: delete the derived checkpoint and re-read.
    let state = home.join("workers").join(&id).join("state.json");
    std::fs::remove_file(&state).unwrap();
    let out = a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .output()
        .unwrap();
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["data"]["elevation_reason"], reason, "{}", v["data"]);
    assert_eq!(
        v["data"]["sandbox"], "danger-full-access",
        "genesis records the EFFECTIVE sandbox: {}",
        v["data"]
    );
    // `extra` is flattened into the snapshot, so the bookkeeping key is
    // top-level in `state.json` (the shape `tests/e2e_appserver.rs` reads too).
    let snap = read_snapshot(&home, &id);
    assert_eq!(
        snap["transport_selection"], "elevation",
        "the elevation chose the transport: {snap}"
    );
    let genesis =
        std::fs::read_to_string(home.join("workers").join(&id).join("events.jsonl")).unwrap();
    let first: Value = serde_json::from_str(genesis.lines().next().unwrap()).unwrap();
    assert_eq!(first["raw"]["fsm"], "Spawn");
    assert_eq!(first["raw"]["elevation_reason"], reason, "{first}");
    assert_eq!(first["raw"]["sandbox"], "danger-full-access", "{first}");
    assert_eq!(first["raw"]["transport_selection"], "elevation", "{first}");
}

// ---------------------------------------------------------------------------
// the config surface
// ---------------------------------------------------------------------------

/// `allow_elevation` is an ordinary lane key: it layers (default < file < env),
/// it has the pinned env spelling, and `config show --origin` reports it.
#[test]
fn allow_elevation_is_a_layered_lane_key_with_the_pinned_env_spelling() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let out = a2a(&home, tmp.path())
        .args(["--json", "config", "show", "--origin"])
        .output()
        .unwrap();
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    for lane in ["impl", "review", "research", "general"] {
        assert_eq!(
            v["data"]["lanes"][lane]["allow_elevation"],
            Value::Bool(false),
            "every lane defaults to false: {}",
            v["data"]["lanes"][lane]
        );
        assert_eq!(
            v["data"]["origins"][format!("lanes.{lane}.allow_elevation")],
            "default"
        );
    }

    // The env spelling alone is enough to permit an elevation.
    let r = spawn(
        &home,
        tmp.path(),
        "general",
        &["--danger-full-access", "--reason", "env-permitted"],
        &[("A2A_LANES_GENERAL_ALLOW_ELEVATION", "true")],
    );
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert_eq!(r.data["sandbox"], "danger-full-access", "{}", r.data);

    let out = a2a(&home, tmp.path())
        .env("A2A_LANES_GENERAL_ALLOW_ELEVATION", "true")
        .args(["--json", "config", "show", "--origin"])
        .output()
        .unwrap();
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        v["data"]["origins"]["lanes.general.allow_elevation"], "env",
        "{}",
        v["data"]["origins"]
    );
}
