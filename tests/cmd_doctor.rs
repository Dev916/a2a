// tests/cmd_doctor.rs
mod common;
use common::*;
use a2a::mesh::registry::{
    write_registry_env, FileRegistry, Registry, WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION,
};
use a2a::mesh::work_exec::{self, WorkPhase, WorkState};
use chrono::Utc;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

/// The seventeen checks, in report order: Phase 1's six (unchanged ids,
/// unchanged order), then Phase 2's four, the three mesh checks, and Phase 3's
/// three. This list IS the contract `doctor --json` publishes — a phase that
/// adds a check adds it here in the same commit.
const CHECK_IDS: [&str; 17] = [
    "codex_present",
    "codex_version",
    "layout_version",
    "home_perms",
    "workers_health",
    "allowed_roots",
    "env_inheritance",
    "app_server_handshake",
    "codex_home_match",
    "broker_status",
    "gateway_enrollment",
    "mesh_hardening",
    "work_awaiting_release",
    "auth_mode",
    "context_canon",
    "codex_config_secrets",
    "sync_state",
];

/// `a2a --json doctor` with the two ambient facts the Phase-2 checks read
/// pinned to the temp tree: a `CODEX_HOME` that is nobody's real one, and no
/// `OPENAI_API_KEY` inherited from whoever ran the suite. Both are overridable
/// through the closure — that is how the tests below drive them.
fn checks(
    home: &std::path::Path,
    root: &std::path::Path,
    f: impl FnOnce(&mut assert_cmd::Command),
) -> (serde_json::Value, i32) {
    let mut c = a2a(home, root);
    c.args(["--json", "doctor"])
        .env("CODEX_HOME", root.join("codex-home"))
        .env_remove("OPENAI_API_KEY");
    f(&mut c);
    let out = c.assert();
    let code = out.get_output().status.code().unwrap();
    (
        serde_json::from_slice(&out.get_output().stdout).unwrap(),
        code,
    )
}
fn check<'a>(v: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    v["data"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == id)
        .unwrap()
}
fn detail(v: &serde_json::Value, id: &str) -> String {
    check(v, id)["detail"].as_str().unwrap().to_string()
}

#[test]
fn all_ok_with_stub_exits_zero_and_lists_seventeen_checks() {
    let tmp = tempfile::tempdir().unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |_| {});
    assert_eq!(code, 0);
    let ids: Vec<&str> = v["data"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, CHECK_IDS, "the published check list, in report order");
    assert_eq!(check(&v, "codex_present")["status"], "ok");
    assert_eq!(check(&v, "allowed_roots")["status"], "ok");
    assert_eq!(check(&v, "env_inheritance")["status"], "warn");
    // The stub speaks exec JSONL, not JSON-RPC: the probe fails, and a failed
    // probe is a WARN because the exec transport still works.
    assert_eq!(check(&v, "app_server_handshake")["status"], "warn");
    // ... and with no codexHome in hand, the match check refuses to guess.
    assert_eq!(check(&v, "codex_home_match")["status"], "warn");
    assert!(
        detail(&v, "codex_home_match").contains("not checked"),
        "absence is reported as absence: {}",
        detail(&v, "codex_home_match")
    );
    // A2A_BROKER_DISABLE=1 (every Phase-1 test) is a standing choice an
    // operator should be told about, not a silent ok.
    assert_eq!(check(&v, "broker_status")["status"], "warn");
    assert!(detail(&v, "broker_status").contains("A2A_BROKER_DISABLE"));
    // no auth.json under the temp CODEX_HOME
    assert_eq!(check(&v, "gateway_enrollment")["status"], "ok");
    assert!(detail(&v, "gateway_enrollment").contains("no mesh gateway material"));
    assert_eq!(check(&v, "mesh_hardening")["status"], "ok");
    assert!(detail(&v, "mesh_hardening").contains("no mesh gateway material"));
    assert_eq!(check(&v, "work_awaiting_release")["status"], "ok");
    assert!(detail(&v, "work_awaiting_release").contains("awaiting_release=0"));
    assert_eq!(check(&v, "auth_mode")["status"], "warn");
}

#[test]
fn mesh_hardening_names_the_roots_held_and_the_local_token_issuer() {
    let tmp = tempfile::tempdir().unwrap();
    let home_path = tmp.path().join("h");
    let home = a2a::home::A2aHome::resolve(Some(&home_path)).unwrap();
    a2a::mesh::keys::fleet_init(&home).unwrap();
    a2a::mesh::keys::gateway_identity(&home, false).unwrap();
    a2a::mesh::keys::ensure_gateway_token(&home, &a2a::mesh::keys::SystemClock).unwrap();
    a2a::mesh::keys::stage_root(&home, &a2a::mesh::keys::SystemClock, false).unwrap();
    let roots = a2a::mesh::keys::RootSet::load(&home).unwrap();
    let token = a2a::mesh::keys::gateway_token(&home).unwrap();
    let (_claims, issuer) = a2a::mesh::keys::verify_token_with_issuer(
        &token,
        &roots,
        &a2a::mesh::keys::SystemClock,
    )
    .unwrap();

    let (v, code) = checks(&home_path, tmp.path(), |_| {});
    assert_eq!(code, 0);
    let hardening = check(&v, "mesh_hardening");
    assert_eq!(hardening["status"], "ok", "{hardening}");
    let hardening_detail = detail(&v, "mesh_hardening");
    for root in roots.entries() {
        assert!(
            hardening_detail.contains(&root.fpr),
            "held root {} is named: {hardening_detail}",
            root.fpr
        );
    }
    assert!(
        hardening_detail.contains(&format!("token_issuer={issuer}")),
        "{hardening_detail}"
    );
    assert!(
        hardening_detail.contains("token_issuer_installed=true"),
        "{hardening_detail}"
    );
    assert!(
        hardening_detail.contains("require_signed_rows="),
        "{hardening_detail}"
    );
    assert!(
        !hardening_detail.contains("awaiting_release="),
        "the W3/F1 release-backlog field is intentionally deferred: {hardening_detail}"
    );
}

#[test]
fn work_awaiting_release_requires_the_registry_status_and_a_local_hold() {
    let tmp = tempfile::tempdir().unwrap();
    let home_path = tmp.path().join("h");
    let home = a2a::home::A2aHome::resolve(Some(&home_path)).unwrap();
    a2a::mesh::keys::fleet_init(&home).unwrap();
    a2a::mesh::keys::gateway_identity(&home, false).unwrap();
    a2a::mesh::keys::ensure_gateway_token(&home, &a2a::mesh::keys::SystemClock).unwrap();
    let fleet_id = a2a::mesh::keys::fleet_id(&home).unwrap();
    let gateway_id = a2a::mesh::keys::gateway_id(&home).unwrap();
    let registry_path = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_path);
    registry.init_schema().unwrap();
    write_registry_env(&home, &format!("file:{}", registry_path.display())).unwrap();

    let work_id = ulid::Ulid::new().to_string();
    let now = Utc::now();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(registry.work_insert(WorkRow {
            fleet_id,
            work_id: work_id.clone(),
            sender_gw: "gw-sender".to_string(),
            addressing: format!("directed:{gateway_id}"),
            status: WorkStatus::AwaitingRelease,
            claimed_by: Some(gateway_id),
            result_ref: None,
            result_delivery: None,
            swept_by: None,
            created_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::days(1),
            schema_version: REGISTRY_SCHEMA_VERSION,
        }))
        .unwrap();
    work_exec::save_state(
        &home,
        &WorkState {
            phase: WorkPhase::AwaitingRelease,
            work_id: work_id.clone(),
            envelope_digest: "digest".to_string(),
            result_note_id: ulid::Ulid::new().to_string(),
            milestone_posted: false,
            worker_id: Some("impl-held".to_string()),
            result_ref: None,
            checkout: home.work_dir().join(&work_id).join("repo"),
            push_attempts: 0,
            next_push_after: None,
            result_summary: Some("done".to_string()),
            evidence_excerpt: None,
            result_status: Some(WorkStatus::Done),
            base_sha: None,
            held_commit_sha: None,
            closure_digest: None,
            base_ref: None,
            remote_url: None,
            held_since: Some(now),
            hold_note_id: Some(ulid::Ulid::new().to_string()),
            hold_hit_count: Some(0),
            observed_terminal: None,
        },
    )
    .unwrap();

    let (v, code) = checks(&home_path, tmp.path(), |_| {});
    assert_eq!(code, 0);
    let held = check(&v, "work_awaiting_release");
    assert_eq!(held["status"], "warn", "{held}");
    assert!(detail(&v, "work_awaiting_release").contains("awaiting_release=1"));
    assert!(detail(&v, "work_awaiting_release").contains(&work_id));
    let remedy = held["remedy"].as_str().unwrap();
    assert!(remedy.contains("work diff"), "{held}");
    assert!(remedy.contains("release"), "{held}");
    assert!(remedy.contains("reject"), "{held}");
}

#[test]
fn allowed_roots_warns_when_only_implicit_worktrees_root_remains() {
    let tmp = tempfile::tempdir().unwrap();
    let home_env = tmp.path().join("user-home");
    std::fs::create_dir_all(&home_env).unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.env("HOME", &home_env).env_remove("A2A_ALLOWED_ROOTS");
    });
    assert_eq!(code, 0);
    assert_eq!(check(&v, "allowed_roots")["status"], "warn");
    assert!(check(&v, "allowed_roots")["remedy"]
        .as_str()
        .unwrap()
        .contains("a2a config add-root"));
}

#[test]
fn missing_codex_fails_exit_9() {
    let tmp = tempfile::tempdir().unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.env("A2A_CODEX_BIN", "/nonexistent/codex");
    });
    assert_eq!(code, 9);
    assert_eq!(check(&v, "codex_present")["status"], "fail");
    // No binary to probe: the handshake is not attempted, and the two checks
    // that depend on it say so rather than inventing a verdict.
    assert_eq!(check(&v, "app_server_handshake")["status"], "warn");
    assert!(detail(&v, "app_server_handshake").contains("not checked"));
    assert_eq!(check(&v, "codex_home_match")["status"], "warn");
}

#[test]
fn out_of_range_version_warns_but_exits_zero() {
    // stub-codex has no --version handling; add a fake: a script printing "codex-cli 0.99.0"
    let tmp = tempfile::tempdir().unwrap();
    let fake = tmp.path().join("old-codex");
    std::fs::write(&fake, "#!/bin/sh\necho 'codex-cli 0.99.0'\n").unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.env("A2A_CODEX_BIN", &fake);
    });
    assert_eq!(code, 0);
    assert_eq!(check(&v, "codex_version")["status"], "warn");
}

#[test]
fn bad_home_perms_fail() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    checks(&home, tmp.path(), |_| {}); // create the tree
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (v, code) = checks(&home, tmp.path(), |_| {});
    assert_eq!(code, 9);
    assert_eq!(check(&v, "home_perms")["status"], "fail");
}

#[test]
fn orphaned_worker_surfaces_in_workers_health() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "1")]);
    wait_dead(&home, &id, 10);
    // fake an orphan: rewrite state to running with a dead pid, older than grace
    let dir = home.join("workers").join(&id);
    let mut v = read_snapshot(&home, &id);
    v["state"] = "running".into();
    v["entered_state_at"] = serde_json::json!(chrono::Utc::now() - chrono::Duration::hours(1));
    a2a::store::write_atomic(&dir.join("state.json"), v.to_string().as_bytes()).unwrap();
    // events already show turn completion; doctor's reconcile pass will surface terminal or orphan —
    // either way workers_health reports a non-empty detail
    let (v, _) = checks(&home, tmp.path(), |_| {});
    assert!(
        check(&v, "workers_health")["detail"]
            .as_str()
            .unwrap()
            .len()
            > 0
    );
}

// ------------------------------------------------------- the phase-2 checks

/// A real handshake against a real app-server: the probe answers, the codexHome
/// it reports is the one this machine is configured with, and the login is
/// where codex keeps it.
///
/// The fake reports `FAKE_STATE or cwd` as its codexHome, and the probe spawns
/// its child under the env ALLOWLIST — which carries `CODEX_HOME` and not
/// `FAKE_*` — so the working directory is how these tests choose what the
/// server answers. That is the production shape too: the only channel between
/// this CLI and the server's idea of its home is `CODEX_HOME`.
#[test]
fn a_real_handshake_reports_ok_and_a_matching_codex_home() {
    let tmp = tempfile::tempdir().unwrap();
    let codex_home = tmp.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).unwrap();
    std::fs::write(codex_home.join("auth.json"), "{}").unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.env("A2A_CODEX_BIN", fake_app_server_path())
            .env("CODEX_HOME", &codex_home)
            .current_dir(&codex_home);
    });
    assert_eq!(code, 0);
    assert_eq!(check(&v, "app_server_handshake")["status"], "ok");
    assert_eq!(check(&v, "codex_home_match")["status"], "ok");
    assert_eq!(check(&v, "auth_mode")["status"], "ok");
}

/// The one new way to FAIL: two known homes that disagree. Every worker's
/// session would then be written somewhere the exec fallback cannot resume it
/// from, which is a spawn-is-broken-now fact.
#[test]
fn a_codex_home_mismatch_is_the_one_new_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let theirs = tmp.path().join("server-home");
    let ours = tmp.path().join("configured-home");
    std::fs::create_dir_all(&theirs).unwrap();
    std::fs::create_dir_all(&ours).unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.env("A2A_CODEX_BIN", fake_app_server_path())
            .env("CODEX_HOME", &ours)
            .current_dir(&theirs); // what the app-server will report
    });
    assert_eq!(code, 9, "a real mismatch is fatal");
    assert_eq!(check(&v, "codex_home_match")["status"], "fail");
    let d = detail(&v, "codex_home_match");
    assert!(
        d.contains("server-home") && d.contains("configured-home"),
        "{d}"
    );
}

/// An exported `OPENAI_API_KEY` is the billing hazard spec §4.2 names: the
/// broker scrubs it from every child, but a human running `codex` by hand in
/// the same shell is billing the API key.
#[test]
fn an_exported_api_key_warns_in_auth_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let codex_home = tmp.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).unwrap();
    std::fs::write(codex_home.join("auth.json"), "{}").unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.env("CODEX_HOME", &codex_home)
            .env("OPENAI_API_KEY", "sk-not-a-real-key");
    });
    assert_eq!(code, 0, "a billing warning is not a spawn blocker");
    assert_eq!(check(&v, "auth_mode")["status"], "warn");
    let d = detail(&v, "auth_mode");
    assert!(d.contains("OPENAI_API_KEY"), "{d}");
    assert!(
        !d.contains("sk-not-a-real-key"),
        "doctor names the variable, never its value: {d}"
    );
}

/// With no daemon listening, "down" is not a problem — it is the normal state
/// of a start-on-demand broker, and doctor must not start one by asking.
#[test]
fn a_down_but_startable_broker_is_ok_and_doctor_never_autostarts_one() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    let (v, code) = checks(&home, tmp.path(), |c| {
        c.env_remove("A2A_BROKER_DISABLE")
            .env("A2A_BROKER_BIN", assert_cmd::cargo::cargo_bin("a2a"));
    });
    assert_eq!(code, 0);
    assert_eq!(check(&v, "broker_status")["status"], "ok");
    let d = detail(&v, "broker_status");
    assert!(d.contains("not running"), "{d}");
    assert!(
        !home.join("broker.sock").exists() && !home.join("broker.pid").exists(),
        "a question that changes the answer by being asked is not one"
    );
    assert_no_broker_left(&home);
}

/// ... and with one up, doctor says so from the handshake it already has to
/// perform — never from the health file alone, which outlives its writer.
#[test]
fn a_running_broker_is_reported_up() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    let codex_home = tmp.path().join("codex-home");
    std::fs::create_dir_all(&codex_home).unwrap();
    std::fs::write(codex_home.join("auth.json"), "{}").unwrap();
    let guard = BrokerGuard::new(&home);

    let a2a_bin = assert_cmd::cargo::cargo_bin("a2a");
    let started = std::process::Command::new(&a2a_bin)
        .arg("--home")
        .arg(&home)
        .args(["broker", "run"])
        .env_remove("A2A_HOME")
        .env_remove("A2A_BROKER_DISABLE")
        .env("A2A_CODEX_BIN", fake_app_server_path())
        .env("A2A_ALLOWED_ROOTS", tmp.path())
        .env("A2A_BROKER_BIN", &a2a_bin)
        .env("A2A_SHIM_BIN", &a2a_bin)
        .env("FAKE_STATE", &codex_home)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("the a2a binary runs");
    assert!(started.success());
    wait_for_socket(&home);

    let (v, code) = checks(&home, tmp.path(), |c| {
        c.env_remove("A2A_BROKER_DISABLE")
            .env("A2A_CODEX_BIN", fake_app_server_path())
            .env("A2A_BROKER_BIN", &a2a_bin)
            .env("CODEX_HOME", &codex_home)
            .current_dir(&codex_home);
    });
    assert_eq!(code, 0);
    assert_eq!(check(&v, "broker_status")["status"], "ok");
    let d = detail(&v, "broker_status");
    assert!(d.contains("up"), "{d}");
    assert!(
        d.contains("protocol"),
        "the skew fact is reported up front: {d}"
    );
    // A live daemon is always nameable: the health file is written by a task
    // and may still be in flight, but the pidfile predates the listener.
    let pid = std::fs::read_to_string(home.join("broker.pid")).unwrap();
    assert!(d.contains(&format!("pid {}", pid.trim())), "{d}");

    drop(guard);
    assert_no_broker_left(&home);
}

#[test]
fn gateway_enrollment_warns_on_invalid_gateway_token() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    let work = tmp.path().join("work");

    a2a(&home, &work)
        .args(["--json", "mesh", "fleet", "init"])
        .assert()
        .success();
    a2a(&home, &work)
        .args(["--json", "mesh", "gateway", "identity"])
        .assert()
        .success();
    std::fs::write(home.join("mesh/gateway/gw.token"), "not a token\n").unwrap();

    let (v, code) = checks(&home, tmp.path(), |_| {});
    assert_eq!(
        code, 0,
        "gateway enrollment warnings are not fatal to doctor"
    );
    assert_eq!(check(&v, "gateway_enrollment")["status"], "warn");
    let d = detail(&v, "gateway_enrollment");
    assert!(d.contains("GATEWAY_TOKEN_INVALID"), "{d}");
    assert!(check(&v, "gateway_enrollment")["remedy"]
        .as_str()
        .unwrap()
        .contains("a2a mesh fleet enroll"));
}

fn wait_for_socket(home: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !home.join("broker.sock").exists() {
        assert!(
            Instant::now() < deadline,
            "the broker never bound its socket"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ------------------------------------------------------- the phase-3 checks

/// Copied verbatim from `tests/sync_migrate.rs`, where the migration this
/// check's remedy names is tested: a hand-written `config.toml` with two
/// secret-shaped lines, at 1-based **14** and **18**. Each `tests/*.rs` is its
/// own crate, so the const is duplicated rather than shared — and both copies
/// are pinned by the same two line numbers.
const SECRET_CONFIG: &str = r#"# The codex home's own config.toml, hand written, and tracked by git.
model = "gpt-5-codex"

[tui]
notifications = true

[mcp_servers.mx]
# mx exists on BOTH sides. a2a did not write this table and must never touch it.
command = "mx"
args = ["mcp"]

[mcp_servers.context7]
url = "https://mcp.context7.com/mcp"
http_headers = { Authorization = "Bearer FAKEcontext7keyFAKEcontext7key" }

[mcp_servers.gh]
command = "gh-mcp"
env = { GITHUB_TOKEN = "ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
"#;

/// The two literals on lines 14 and 18. Neither may appear anywhere in a
/// `doctor --json` payload, in any state of the repository.
const CONFIG_SECRETS: [&str; 2] = [
    "Bearer FAKEcontext7keyFAKEcontext7key",
    "ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
];

/// A developer's global `core.excludesFile` changes what `git check-ignore`
/// answers, and their hooks and templates would otherwise run inside a fixture
/// repository. Every process that shells out to git — the test's own `git` and
/// the binary under test — gets this.
fn nowhere(tmp: &Path) -> std::path::PathBuf {
    tmp.join("no-such-gitconfig")
}

fn hermetic_git(c: &mut assert_cmd::Command, tmp: &Path) {
    c.env("GIT_CONFIG_GLOBAL", nowhere(tmp))
        .env("GIT_CONFIG_SYSTEM", nowhere(tmp));
}

fn git(dir: &Path, tmp: &Path, args: &[&str]) {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args([
            "-c",
            "user.email=a2a@test",
            "-c",
            "user.name=a2a",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .env("GIT_CONFIG_GLOBAL", nowhere(tmp))
        .env("GIT_CONFIG_SYSTEM", nowhere(tmp))
        .output()
        .expect("git is on PATH");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A codex home that is a git repository whose `config.toml` is **committed** —
/// the exact state of this machine's real `~/.codex`, and the one the check
/// exists for.
fn tracked_secret_home(tmp: &Path) -> std::path::PathBuf {
    let codex_home = tmp.join("codex");
    std::fs::create_dir_all(&codex_home).unwrap();
    std::fs::write(codex_home.join("config.toml"), SECRET_CONFIG).unwrap();
    git(&codex_home, tmp, &["init", "-q", "-b", "main"]);
    git(&codex_home, tmp, &["add", "config.toml"]);
    git(&codex_home, tmp, &["commit", "-qm", "codex home"]);
    codex_home
}

/// The fixture Claude source, copied to the spelling `sync mcp` reads
/// (`<dir>/.claude.json`).
fn fixture_claude_dir(tmp: &Path) -> std::path::PathBuf {
    let dir = tmp.join("claude");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sync/claude.json"),
        dir.join(".claude.json"),
    )
    .unwrap();
    dir
}

/// Doctor's first security-class `fail`: a credential in a file git publishes
/// is a hard-rule violation whether or not a spawn would succeed.
#[test]
fn a_tracked_config_toml_carrying_a_secret_fails_exit_9() {
    let tmp = tempfile::tempdir().unwrap();
    let codex_home = tracked_secret_home(tmp.path());
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.env("CODEX_HOME", &codex_home);
        hermetic_git(c, tmp.path());
    });
    assert_eq!(code, 9, "a published credential is fatal");
    assert_eq!(check(&v, "codex_config_secrets")["status"], "fail");

    let d = detail(&v, "codex_config_secrets");
    assert!(
        d.contains(&*codex_home.join("config.toml").to_string_lossy()),
        "the path is named: {d}"
    );
    assert!(d.contains("lines 14, 18"), "1-based line numbers only: {d}");
    assert_eq!(
        check(&v, "codex_config_secrets")["remedy"],
        "a2a sync mcp --migrate-untrack"
    );

    // The whole payload, not merely the one detail: a value that leaked into
    // any other field would be just as published.
    let payload = v.to_string();
    for secret in CONFIG_SECRETS {
        assert!(
            !payload.contains(secret),
            "doctor names lines, never values — found {secret} in {payload}"
        );
    }
}

/// ... and the remedy it prints actually clears it.
#[test]
fn migrate_untrack_flips_the_secret_check_to_ok_and_the_exit_to_zero() {
    let tmp = tempfile::tempdir().unwrap();
    let codex_home = tracked_secret_home(tmp.path());
    let claude_dir = fixture_claude_dir(tmp.path());
    let home = tmp.path().join("h");

    // Built by `sync_a2a` with an explicit fixture Claude dir: `checks()` knows
    // nothing about a Claude source, and a bare `a2a` here would be answered by
    // the SYNC_REFUSES_REAL_HOME guard instead of testing the behaviour.
    let mut c = sync_a2a(&home, &codex_home, &claude_dir, tmp.path());
    hermetic_git(&mut c, tmp.path());
    let out = c
        .args(["--json", "sync", "mcp", "--migrate-untrack"])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let (v, code) = checks(&home, tmp.path(), |c| {
        c.env("CODEX_HOME", &codex_home);
        hermetic_git(c, tmp.path());
    });
    assert_eq!(code, 0, "nothing published, nothing fatal");
    assert_eq!(check(&v, "codex_config_secrets")["status"], "ok");
    assert!(
        detail(&v, "codex_config_secrets").contains("which git does not track"),
        "{}",
        detail(&v, "codex_config_secrets")
    );
    let payload = v.to_string();
    for secret in CONFIG_SECRETS {
        assert!(!payload.contains(secret), "{secret}");
    }
}

/// The context-canon nudge, from the process's cwd — ok when `CLAUDE.md`
/// imports `@AGENTS.md`, warn (never fail) when it does not.
#[test]
fn context_canon_is_ok_with_an_agents_import_and_warns_without_one() {
    let tmp = tempfile::tempdir().unwrap();
    let importing = tmp.path().join("importing");
    let plain = tmp.path().join("plain");
    std::fs::create_dir_all(&importing).unwrap();
    std::fs::create_dir_all(&plain).unwrap();
    std::fs::write(importing.join("CLAUDE.md"), "@AGENTS.md\n\n# Project\n").unwrap();
    std::fs::write(plain.join("CLAUDE.md"), "# Project\n\nlocal notes\n").unwrap();

    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |c| {
        c.current_dir(&importing);
    });
    assert_eq!(code, 0);
    assert_eq!(check(&v, "context_canon")["status"], "ok");

    let (v, code) = checks(&tmp.path().join("h2"), tmp.path(), |c| {
        c.current_dir(&plain);
    });
    assert_eq!(code, 0, "an un-migrated repo is nudged, never blocked");
    assert_eq!(check(&v, "context_canon")["status"], "warn");
    assert!(
        detail(&v, "context_canon").contains("Codex workers read AGENTS.md"),
        "{}",
        detail(&v, "context_canon")
    );
}

/// A ledger whose destination moved underneath it warns — and says how many of
/// each thing, so the operator knows which sync to re-run.
#[test]
fn a_drifted_manifest_entry_warns_in_sync_state() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    let source = tmp.path().join("src/techniques");
    let dest = tmp.path().join("codex/skills/techniques");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::create_dir_all(&dest).unwrap();
    std::fs::write(source.join("SKILL.md"), "source").unwrap();
    std::fs::write(dest.join("SKILL.md"), "somebody edited this").unwrap();

    let (v, _) = checks(&home, tmp.path(), |_| {}); // creates the home tree
    assert_eq!(check(&v, "sync_state")["status"], "ok");
    assert!(detail(&v, "sync_state").contains("no sync manifest"));

    let manifest = serde_json::json!({
        "schema_version": 1,
        "entries": {
            "skill:default:techniques": {
                "source_path": source,
                "dest_path": dest,
                "source_hash": "0000",
                "dest_hash": "not-the-hash-on-disk",
                "synced_at": "2026-08-18T10:00:00Z",
                "tool_version": "0.3.0",
                "kind": "skill",
                "account": "default",
            }
        }
    });
    std::fs::write(home.join("sync-manifest.json"), manifest.to_string()).unwrap();

    let (v, code) = checks(&home, tmp.path(), |_| {});
    assert_eq!(code, 0, "a stale mirror does not stop a spawn");
    assert_eq!(check(&v, "sync_state")["status"], "warn");
    let d = detail(&v, "sync_state");
    assert!(d.contains("1 drifted"), "{d}");
    assert!(
        d.contains("0 missing"),
        "the counts are named, not summed: {d}"
    );
    assert_eq!(check(&v, "sync_state")["remedy"], "a2a sync skills");
}

/// The third input state: a ledger a2a cannot read is the one condition that
/// costs the user every future update, so it may never be reported as `ok`.
#[test]
fn a_corrupt_manifest_warns_in_sync_state_and_names_the_path() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    checks(&home, tmp.path(), |_| {}); // creates the home tree
    let path = home.join("sync-manifest.json");
    std::fs::write(&path, "{ this is not the ledger you left").unwrap();

    let (v, code) = checks(&home, tmp.path(), |_| {});
    assert_eq!(code, 0);
    assert_eq!(check(&v, "sync_state")["status"], "warn");
    let d = detail(&v, "sync_state");
    assert!(d.contains(&*path.to_string_lossy()), "{d}");
    assert_eq!(
        check(&v, "sync_state")["remedy"],
        "move sync-manifest.json aside and re-run the sync — a2a will only re-adopt what it can prove it wrote"
    );
}
