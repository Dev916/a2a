mod common;

use a2a::mesh::keys::{self, TestClock, TokenClaims, SKEW_TOLERANCE_SECS};
use chrono::{TimeZone, Utc};
use a2a::mesh::registry::Registry;
use predicates::prelude::*;
use serde_json::Value;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

static NEXT_GATEWAY_PORT: AtomicU16 = AtomicU16::new(20_000);

#[test]
fn mesh_gateway_config_surface_is_pinned_and_originated() {
    for field in [
        "listen_port",
        "expose",
        "endpoint_url",
        "registry_heartbeat_secs",
        "registry_timeout_secs",
        "replication_poll_secs",
        "outbound_retry_max_secs",
        "ping_secs",
        "anti_entropy_secs",
        "feed_retain_days",
        "shutdown_grace_secs",
        "require_signed_rows",
        "row_resign_secs",
        "max_accepted_row_resign_secs",
        "rotation_soak_secs",
    ] {
        assert!(a2a::config::MESH_GATEWAY_FIELDS.contains(&field));
    }
    assert_eq!(
        a2a::config::mesh_gateway_env_key("ping_secs"),
        "A2A_MESH_GATEWAY_PING_SECS"
    );
    assert_eq!(
        a2a::config::MESH_WORK_FIELDS,
        [
            "ttl_days",
            "max_directed_bytes",
            "max_pool_bytes",
            "result_note_max_bytes",
            "work_poll_secs",
            "claim_stall_secs",
            "checkout_retain_days",
            "diff_scan_max_bytes",
        ]
    );

    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("config.toml"),
        r#"
            [mesh.gateway]
            listen_port = 8899
            expose = "manual"
            endpoint_url = "wss://127.0.0.1:8899"
            registry_heartbeat_secs = 17
            require_signed_rows = true
            unknown = 1
        "#,
    )
    .unwrap();

    let out = common::a2a(&home, &work)
        .env("A2A_MESH_GATEWAY_PING_SECS", "44")
        .env("A2A_MESH_GATEWAY_ROW_RESIGN_SECS", "43200")
        .args(["--json", "config", "show", "--origin"])
        .assert()
        .success()
        .stderr(predicate::str::contains("mesh.gateway.unknown"))
        .get_output()
        .stdout
        .clone();
    let v: Value = serde_json::from_slice(&out).unwrap();
    let data = &v["data"];
    assert_eq!(data["mesh"]["gateway"]["listen_port"], 8899);
    assert_eq!(data["mesh"]["gateway"]["expose"], "manual");
    assert_eq!(
        data["mesh"]["gateway"]["endpoint_url"],
        "wss://127.0.0.1:8899"
    );
    assert_eq!(data["mesh"]["gateway"]["registry_heartbeat_secs"], 17);
    assert_eq!(data["mesh"]["gateway"]["registry_timeout_secs"], 10);
    assert_eq!(data["mesh"]["gateway"]["replication_poll_secs"], 5);
    assert_eq!(data["mesh"]["gateway"]["outbound_retry_max_secs"], 1920);
    assert_eq!(data["mesh"]["gateway"]["ping_secs"], 44);
    assert_eq!(data["mesh"]["gateway"]["anti_entropy_secs"], 300);
    assert_eq!(data["mesh"]["gateway"]["feed_retain_days"], 30);
    assert_eq!(data["mesh"]["gateway"]["shutdown_grace_secs"], 10);
    assert_eq!(data["mesh"]["gateway"]["require_signed_rows"], true);
    assert_eq!(data["mesh"]["gateway"]["row_resign_secs"], 43_200);
    assert_eq!(
        data["mesh"]["gateway"]["max_accepted_row_resign_secs"],
        604_800
    );
    assert_eq!(data["mesh"]["work"]["work_poll_secs"], 15);
    assert_eq!(data["mesh"]["work"]["claim_stall_secs"], 900);
    assert_eq!(data["mesh"]["work"]["checkout_retain_days"], 30);
    assert_eq!(data["origins"]["mesh.gateway.listen_port"], "file");
    assert_eq!(data["origins"]["mesh.gateway.ping_secs"], "env");
    assert_eq!(
        data["origins"]["mesh.gateway.require_signed_rows"],
        "file"
    );
    assert_eq!(data["origins"]["mesh.gateway.row_resign_secs"], "env");
    for field in a2a::config::MESH_GATEWAY_FIELDS {
        assert!(
            data["origins"]
                .as_object()
                .unwrap()
                .contains_key(&format!("mesh.gateway.{field}")),
            "{field} is listed in origins"
        );
    }
    for field in a2a::config::MESH_WORK_FIELDS {
        assert!(
            data["origins"]
                .as_object()
                .unwrap()
                .contains_key(&format!("mesh.work.{field}")),
            "{field} is listed in origins"
        );
    }
}

#[test]
fn gateway_status_is_exit_7_until_a_supervisor_writes_health() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &tmp.path().join("registry"));

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "status"])
        .assert()
        .code(7)
        .stderr(predicate::str::contains("GATEWAY_UNAVAILABLE"));

    let mut child = spawn_gateway(&home, &work, &[]);
    let health = wait_for_health_with(&home, |health| {
        health["loops"]["registry"]["heartbeats"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    });
    assert_eq!(health["state"], "running");
    assert_eq!(
        health["schema_version"],
        a2a::mesh::gateway::HEALTH_SCHEMA_VERSION
    );
    assert_eq!(health["pid"], child.id());
    assert_eq!(health["loops"].as_object().unwrap().len(), 6);
    assert_eq!(health["links"].as_array().unwrap().len(), 0);
    assert!(health["loops"]["registry"]["heartbeats"].as_u64().unwrap() >= 1);
    assert!(health["loops"]["work"]["heartbeats"].as_u64().unwrap() >= 1);

    let status = common::a2a(&home, &work)
        .args(["--json", "mesh", "gateway", "status"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let status: Value = serde_json::from_slice(&status).unwrap();
    assert_eq!(status["data"]["running"], true);
    assert!(
        status["data"]["health"]["health_seq"].as_u64().unwrap()
            >= health["health_seq"].as_u64().unwrap()
    );

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("links 0"));

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "stop"])
        .assert()
        .success();
    assert_exits(&mut child, Duration::from_secs(3));

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "status"])
        .assert()
        .code(7);
}

#[test]
fn gateway_daemon_reexec_starts_reports_pid_status_and_stops() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &tmp.path().join("registry"));

    let out = daemon_gateway_cmd(&home, &work)
        .args(["--json", "mesh", "gateway", "run", "--daemon"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let started: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(started["data"]["started"], true);
    let pid = started["data"]["pid"].as_u64().unwrap();
    assert!(pid > 0);
    assert_eq!(
        started["data"]["log"].as_str(),
        Some(home.join("mesh").join("gateway.daemon.log").to_string_lossy().as_ref())
    );

    let status = common::a2a(&home, &work)
        .args(["--json", "mesh", "gateway", "status"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let status: Value = serde_json::from_slice(&status).unwrap();
    assert_eq!(status["data"]["running"], true);
    assert_eq!(status["data"]["health"]["pid"].as_u64(), Some(pid));
    assert!(home.join("mesh").join("gateway.daemon.log").exists());

    let already = daemon_gateway_cmd(&home, &work)
        .args(["--json", "mesh", "gateway", "run", "--daemon"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let already: Value = serde_json::from_slice(&already).unwrap();
    assert_eq!(already["data"]["started"], false);
    assert_eq!(already["data"]["already_running"], true);

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "stop"])
        .assert()
        .success();
    common::a2a(&home, &work)
        .args(["mesh", "gateway", "status"])
        .assert()
        .code(7);
}

#[test]
fn supervisor_escalates_panicking_loop_and_releases_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &tmp.path().join("registry"));

    let mut child = spawn_gateway(
        &home,
        &work,
        &[
            ("A2A_TEST_MESH_GATEWAY_STUB_PANIC", "work"),
            ("A2A_MESH_GATEWAY_OUTBOUND_RETRY_MAX_SECS", "1"),
        ],
    );
    let status = wait_exit(&mut child, Duration::from_secs(5));
    assert!(!status.success(), "supervisor escalation exits non-zero");
    let health = read_health(&home);
    assert_eq!(health["state"], "escalated");
    assert_eq!(health["escalation"]["loop"], "work");
    assert_eq!(health["loops"]["work"]["restart_policy"], "one-for-one");

    let mut fresh = spawn_gateway(&home, &work, &[]);
    wait_for_health_with(&home, |health| {
        health["pid"].as_u64() == Some(fresh.id() as u64)
    });
    common::a2a(&home, &work)
        .args(["mesh", "gateway", "stop"])
        .assert()
        .success();
    assert_exits(&mut fresh, Duration::from_secs(3));
}

#[test]
fn shutdown_is_bounded_even_when_a_stub_refuses_to_die() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    prepare_gateway_home(&home, &tmp.path().join("registry"));
    std::fs::write(
        home.join("config.toml"),
        "[mesh.gateway]\nshutdown_grace_secs = 1\n",
    )
    .unwrap();

    let mut child = spawn_gateway(
        &home,
        &work,
        &[("A2A_TEST_MESH_GATEWAY_STUB_STALL_SHUTDOWN", "work")],
    );
    wait_for_health(&home, 1);
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child.id() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    let started = Instant::now();
    let status = wait_exit(&mut child, Duration::from_secs(3));
    assert!(!status.success(), "drain timeout exits non-zero");
    assert!(started.elapsed() < Duration::from_secs(3));

    let log = std::fs::read_to_string(home.join("mesh").join("gateway.jsonl")).unwrap();
    assert!(log.contains(r#""kind":"drain_timeout""#), "{log}");
    assert!(log.contains(r#""outstanding_tasks":"#), "{log}");

    let mut fresh = spawn_gateway(&home, &work, &[]);
    wait_for_health_with(&home, |health| {
        health["pid"].as_u64() == Some(fresh.id() as u64)
    });
    common::a2a(&home, &work)
        .args(["mesh", "gateway", "stop"])
        .assert()
        .success();
    assert_exits(&mut fresh, Duration::from_secs(3));
}

#[test]
fn gateway_source_scan_pins_health_ownership_and_async_discipline() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let gateway_dir = root.join("src/mesh/gateway");
    let mut files = std::fs::read_dir(&gateway_dir)
        .expect("gateway dir exists")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "rs").unwrap_or(false))
        .collect::<Vec<_>>();
    files.sort();
    assert!(!files.is_empty(), "gateway dir has Rust sources");

    let mut health_write_sites = 0;
    let mut select_blocks = 0;
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        let code = code_only(&text);
        for (n, line) in code.lines().enumerate() {
            assert!(
                !line.contains("Arc<Mutex") && !line.contains("Arc<RwLock"),
                "{}:{} shared lock in gateway actor tree:\n{line}",
                file.display(),
                n + 1
            );
            assert!(
                !line.contains("std::thread::sleep") && !line.contains("thread::sleep("),
                "{}:{} blocking sleep in gateway async module:\n{line}",
                file.display(),
                n + 1
            );
            if line.contains("fn write_health") {
                health_write_sites += 1;
            }
        }
        for block in select_blocks_in(&code) {
            select_blocks += 1;
            assert!(
                !block.contains("write_all"),
                "{}: write_all inside select block:\n{block}",
                file.display()
            );
        }
        assert!(
            !code.contains("spawn_stub_loop") || !code.contains("write_health("),
            "child-loop helper must not write gateway-health.json"
        );
    }
    assert!(health_write_sites >= 1, "supervisor write path is visible");
    assert!(select_blocks >= 2, "gateway loops are select!-driven");
}

#[test]
fn gateway_startup_mints_identity_and_self_enrollment_token() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let registry_dir = tmp.path().join("registry");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &registry_dir);
    std::fs::write(
        home.join("config.toml"),
        "[mesh.gateway]\nexpose = \"manual\"\nendpoint_url = \"wss://manual.example.test:7787\"\nregistry_heartbeat_secs = 1\n",
    )
    .unwrap();

    let mut child = spawn_gateway(&home, &work, &[]);
    let health = wait_for_health_with(&home, |health| {
        health["endpoint_url"].as_str() == Some("wss://manual.example.test:7787")
            && health["loops"]["registry"]["state"] == "running"
    });
    assert_eq!(health["loops"]["registry"]["state"], "running");

    let gateway_dir = home.join("mesh").join("gateway");
    assert!(gateway_dir.join("gw.key").exists());
    assert!(gateway_dir.join("gw.crt").exists());
    assert!(gateway_dir.join("gw.tls.key").exists());
    let token = gateway_dir.join("gw.token");
    assert!(token.exists());
    assert_eq!(
        std::fs::metadata(&token).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let rows = wait_for_registry_rows(&home, &registry_dir, 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].endpoint_url.as_deref(),
        Some("wss://manual.example.test:7787")
    );
    assert_eq!(rows[0].cert_fpr.as_ref().unwrap().len(), 64);
    assert_eq!(rows[0].version, env!("CARGO_PKG_VERSION"));
    assert_eq!(rows[0].pubkey_fpr.len(), 64);

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "stop"])
        .assert()
        .success();
    assert_exits(&mut child, Duration::from_secs(3));
}

#[test]
fn all_real_gateway_slots_boot_with_file_registry_and_shutdown_gracefully() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &tmp.path().join("registry"));
    std::fs::write(
        home.join("config.toml"),
        "[mesh.gateway]\nexpose = \"none\"\nregistry_heartbeat_secs = 1\nreplication_poll_secs = 1\nanti_entropy_secs = 1\n",
    )
    .unwrap();

    let mut child = spawn_gateway(&home, &work, &[]);
    let health = wait_for_health_with(&home, |health| {
        ["registry", "dialer", "watcher", "anti-entropy", "work"]
            .into_iter()
            .all(|slot| health["loops"][slot]["heartbeats"].as_u64().unwrap_or(0) >= 1)
            && (health["loops"]["server"]["heartbeats"]
                .as_u64()
                .unwrap_or(0)
                >= 1
                || health["loops"]["server"]["last_exit"]
                    .as_str()
                    .is_some_and(|exit| exit.contains("GATEWAY_BIND")))
    });
    assert_eq!(health["state"], "running");
    assert!(
        health["loops"]["server"]["state"] == "running"
            || health["loops"]["server"]["last_exit"]
                .as_str()
                .is_some_and(|exit| exit.contains("GATEWAY_BIND")),
        "{health}"
    );
    assert_eq!(health["loops"]["dialer"]["state"], "running");
    assert_eq!(health["loops"]["watcher"]["state"], "running");
    assert_eq!(health["loops"]["anti-entropy"]["state"], "running");
    assert_eq!(health["loops"]["work"]["state"], "running");

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "stop"])
        .assert()
        .success();
    assert_exits(&mut child, Duration::from_secs(3));
    let health = read_health(&home);
    assert_eq!(health["state"], "stopped");
    assert_eq!(health["shutdown"]["graceful"], true);
}

#[test]
fn registry_unreachable_does_not_restart_or_escalate_gateway_loops() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &std::path::PathBuf::from("/nonexistent/a2a-registry-blip"));
    std::fs::write(
        home.join("config.toml"),
        "[mesh.gateway]\nexpose = \"none\"\nregistry_heartbeat_secs = 1\nreplication_poll_secs = 1\nanti_entropy_secs = 1\n",
    )
    .unwrap();

    let mut child = spawn_gateway(&home, &work, &[]);
    let health = wait_for_health_with(&home, |health| {
        ["registry", "server", "dialer", "watcher", "anti-entropy", "work"]
            .into_iter()
            .all(|slot| health["loops"][slot]["state"] == "running")
            && ["registry", "server", "dialer", "watcher", "anti-entropy", "work"]
                .into_iter()
                .all(|slot| health["loops"][slot]["restarts"].as_u64() == Some(0))
            && health["registry_error"].is_string()
            && health["registry_error_since"].is_string()
    });
    assert_eq!(health["state"], "running");
    assert!(
        health["registry_error"]
            .as_str()
            .unwrap()
            .contains("creating file registry"),
        "{health}"
    );
    let log = wait_for_log_contains(&home, r#""kind":"registry_error""#);
    assert!(log.contains(r#""kind":"registry_error""#), "{log}");
    assert!(log.contains(r#""state":"down""#), "{log}");
    assert!(!log.contains(r#""kind":"loop_exit""#), "{log}");
    assert!(!log.contains(r#""kind":"escalation""#), "{log}");

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "stop"])
        .assert()
        .success();
    assert_exits(&mut child, Duration::from_secs(3));
    let health = read_health(&home);
    assert_eq!(health["state"], "stopped");
    assert_eq!(health["shutdown"]["graceful"], true);
}

#[test]
fn gateway_startup_without_root_or_token_fails_not_enrolled() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    let registry_dir = tmp.path().join("registry");
    std::fs::create_dir_all(&work).unwrap();
    write_registry_env_for_test(&home, &registry_dir);

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("GATEWAY_NOT_ENROLLED"))
        .stderr(predicate::str::contains("a2a mesh fleet init"));
}

#[test]
fn gateway_startup_without_registry_env_fails_unconfigured() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    let resolved = a2a::home::A2aHome::resolve(Some(&home)).unwrap();
    a2a::mesh::keys::fleet_init(&resolved).unwrap();

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("REGISTRY_UNCONFIGURED"))
        .stderr(predicate::str::contains("a2a mesh fleet init --registry -"));
}

#[test]
fn gateway_startup_refuses_token_from_another_fleet_before_health() {
    let tmp = tempfile::tempdir().unwrap();
    let fleet_a = tmp.path().join("fleet-a");
    let fleet_b = tmp.path().join("fleet-b");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&fleet_a, &tmp.path().join("registry-a"));
    prepare_gateway_home(&fleet_b, &tmp.path().join("registry-b"));

    let home_b = a2a::home::A2aHome::resolve(Some(&fleet_b)).unwrap();
    let identity = keys::gateway_identity(&home_b, false).unwrap();
    let token = keys::enroll_gateway_token(
        &home_b,
        &identity.gateway_id,
        &keys::gateway_public_key_b64(&home_b).unwrap(),
        &TestClock::new(Utc::now()),
    )
    .unwrap();
    keys::install_gateway_token(&home_b, &token, &TestClock::new(Utc::now())).unwrap();
    std::fs::remove_file(fleet_b.join("mesh/fleet/root.key")).unwrap();
    std::fs::copy(
        fleet_a.join("mesh/fleet/root.pub"),
        fleet_b.join("mesh/fleet/root.pub"),
    )
    .unwrap();

    common::a2a(&fleet_b, &work)
        .args(["mesh", "gateway", "run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("GATEWAY_FLEET_MISMATCH"))
        .stderr(predicate::str::contains("restore the fleet's root.pub"));
    assert!(
        !fleet_b.join("mesh/gateway-health.json").exists(),
        "startup enrollment failure must happen before health is written"
    );
    assert!(
        !fleet_b.join("mesh/gateway.pid").exists(),
        "startup enrollment failure must happen before pidfile commit"
    );
}

#[test]
fn gateway_startup_refuses_garbage_token_as_invalid() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &tmp.path().join("registry"));
    let resolved = a2a::home::A2aHome::resolve(Some(&home)).unwrap();
    keys::gateway_identity(&resolved, false).unwrap();
    std::fs::write(home.join("mesh/gateway/gw.token"), "not a token\n").unwrap();

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("GATEWAY_TOKEN_INVALID"));
    assert!(!home.join("mesh/gateway-health.json").exists());
}

#[test]
fn gateway_startup_refuses_token_for_another_gateway_id() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &tmp.path().join("registry"));
    let resolved = a2a::home::A2aHome::resolve(Some(&home)).unwrap();
    let local = keys::gateway_identity(&resolved, false).unwrap();
    let claims = TokenClaims {
        fleet_id: String::new(),
        gateway_id: format!("{}-other", local.gateway_id),
        pubkey_b64: keys::gateway_public_key_b64(&resolved).unwrap(),
        key_epoch: 0,
        issued_at: Utc::now().timestamp(),
        exp: Utc::now().timestamp() + 3600,
    };
    let token = keys::sign_token(&claims, &resolved).unwrap();
    std::fs::write(home.join("mesh/gateway/gw.token"), format!("{token}\n")).unwrap();

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("GATEWAY_TOKEN_MISMATCH"))
        .stderr(predicate::str::contains("gateway_id"));
    assert!(!home.join("mesh/gateway-health.json").exists());
}

#[test]
fn gateway_startup_refuses_token_for_another_gateway_pubkey() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let other = tmp.path().join("other");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &tmp.path().join("registry"));
    let resolved = a2a::home::A2aHome::resolve(Some(&home)).unwrap();
    let other_home = a2a::home::A2aHome::resolve(Some(&other)).unwrap();
    let local = keys::gateway_identity(&resolved, false).unwrap();
    keys::gateway_identity(&other_home, false).unwrap();
    let claims = TokenClaims {
        fleet_id: String::new(),
        gateway_id: local.gateway_id,
        pubkey_b64: keys::gateway_public_key_b64(&other_home).unwrap(),
        key_epoch: 0,
        issued_at: Utc::now().timestamp(),
        exp: Utc::now().timestamp() + 3600,
    };
    let token = keys::sign_token(&claims, &resolved).unwrap();
    std::fs::write(home.join("mesh/gateway/gw.token"), format!("{token}\n")).unwrap();

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("GATEWAY_TOKEN_MISMATCH"))
        .stderr(predicate::str::contains("pubkey_b64"));
    assert!(!home.join("mesh/gateway-health.json").exists());
}

#[test]
fn gateway_startup_refuses_expired_token() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    prepare_gateway_home(&home, &tmp.path().join("registry"));
    let resolved = a2a::home::A2aHome::resolve(Some(&home)).unwrap();
    let identity = keys::gateway_identity(&resolved, false).unwrap();
    let issued_at = Utc.timestamp_opt(1_700_000_000, 0).unwrap().timestamp();
    let claims = TokenClaims {
        fleet_id: String::new(),
        gateway_id: identity.gateway_id,
        pubkey_b64: keys::gateway_public_key_b64(&resolved).unwrap(),
        key_epoch: 0,
        issued_at,
        exp: Utc::now().timestamp() - SKEW_TOLERANCE_SECS - 1,
    };
    let token = keys::sign_token(&claims, &resolved).unwrap();
    std::fs::write(home.join("mesh/gateway/gw.token"), format!("{token}\n")).unwrap();

    common::a2a(&home, &work)
        .args(["mesh", "gateway", "run"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("GATEWAY_TOKEN_EXPIRED"))
        .stderr(predicate::str::contains("a2a mesh fleet enroll"))
        .stderr(predicate::str::contains("a2a mesh gateway install-token"));
    assert!(!home.join("mesh/gateway-health.json").exists());
}

fn spawn_gateway(home: &std::path::Path, work: &std::path::Path, envs: &[(&str, &str)]) -> Child {
    let listen_port = NEXT_GATEWAY_PORT.fetch_add(1, Ordering::SeqCst).to_string();
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin("a2a"));
    cmd.arg("--home")
        .arg(home)
        .args(["mesh", "gateway", "run"])
        .env("A2A_ALLOWED_ROOTS", work)
        .env("A2A_CODEX_BIN", common::stub_path())
        .env("A2A_BROKER_DISABLE", "1")
        .env("A2A_OWNER", common::TEST_OWNER)
        .env("A2A_MESH_GATEWAY_LISTEN_PORT", listen_port)
        .env("A2A_TEST_MESH_GATEWAY_SERVER_BIND_OPTIONAL", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.spawn().unwrap()
}

fn daemon_gateway_cmd(home: &std::path::Path, work: &std::path::Path) -> assert_cmd::Command {
    let listen_port = NEXT_GATEWAY_PORT.fetch_add(1, Ordering::SeqCst).to_string();
    let mut cmd = common::a2a(home, work);
    cmd.env("A2A_MESH_GATEWAY_LISTEN_PORT", listen_port)
        .env("A2A_TEST_MESH_GATEWAY_SERVER_BIND_OPTIONAL", "1");
    cmd
}

fn prepare_gateway_home(home: &std::path::Path, registry_dir: &std::path::Path) {
    std::fs::create_dir_all(home).unwrap();
    let resolved = a2a::home::A2aHome::resolve(Some(home)).unwrap();
    a2a::mesh::keys::fleet_init(&resolved).unwrap();
    write_registry_env_for_test(home, registry_dir);
}

fn write_registry_env_for_test(home: &std::path::Path, registry_dir: &std::path::Path) {
    let fleet = home.join("mesh").join("fleet");
    std::fs::create_dir_all(&fleet).unwrap();
    let path = fleet.join("registry.env");
    std::fs::write(&path, format!("file://{}\n", registry_dir.display())).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

fn wait_for_registry_rows(
    home: &std::path::Path,
    registry_dir: &std::path::Path,
    min_rows: usize,
) -> Vec<a2a::mesh::registry::GatewayRow> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let rt = tokio::runtime::Runtime::new().unwrap();
    loop {
        let rows = rt.block_on(async {
            let resolved = a2a::home::A2aHome::resolve(Some(home)).unwrap();
            let fleet_id = a2a::mesh::keys::fleet_id(&resolved).unwrap();
            let registry = a2a::mesh::registry::FileRegistry::new(registry_dir);
            registry.discover_unverified(&fleet_id).await.unwrap()
        });
        if rows.len() >= min_rows {
            return rows;
        }
        assert!(
            Instant::now() < deadline,
            "registry rows were not published"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_health(home: &std::path::Path, min_seq: u64) -> Value {
    wait_for_health_with(home, |v| v["health_seq"].as_u64().unwrap_or(0) >= min_seq)
}

fn wait_for_health_with(home: &std::path::Path, pred: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(text) = std::fs::read_to_string(home.join("mesh").join("gateway-health.json")) {
            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                if pred(&v) {
                    return v;
                }
            }
        }
        assert!(Instant::now() < deadline, "gateway health was not written");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn read_health(home: &std::path::Path) -> Value {
    serde_json::from_str(
        &std::fs::read_to_string(home.join("mesh").join("gateway-health.json")).unwrap(),
    )
    .unwrap()
}

fn wait_for_log_contains(home: &std::path::Path, needle: &str) -> String {
    let path = home.join("mesh").join("gateway.jsonl");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(text) = std::fs::read_to_string(&path) {
            if text.contains(needle) {
                return text;
            }
        }
        assert!(Instant::now() < deadline, "gateway log did not contain {needle}");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn assert_exits(child: &mut Child, patience: Duration) {
    let status = wait_exit(child, patience);
    assert!(status.success(), "process exited with {status}");
}

fn wait_exit(child: &mut Child, patience: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + patience;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            if !stderr.is_empty() {
                eprintln!("gateway stderr: {stderr}");
            }
            return status;
        }
        assert!(Instant::now() < deadline, "gateway process did not exit");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn code_only(src: &str) -> String {
    let b: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == '/' && i + 1 < b.len() && b[i + 1] == '/' {
            while i < b.len() && b[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if b[i] == '/' && i + 1 < b.len() && b[i + 1] == '*' {
            let mut depth = 1;
            i += 2;
            while i < b.len() && depth > 0 {
                if b[i] == '/' && i + 1 < b.len() && b[i + 1] == '*' {
                    depth += 1;
                    i += 2;
                } else if b[i] == '*' && i + 1 < b.len() && b[i + 1] == '/' {
                    depth -= 1;
                    i += 2;
                } else {
                    if b[i] == '\n' {
                        out.push('\n');
                    }
                    i += 1;
                }
            }
            continue;
        }
        if b[i] == '"' {
            out.push('"');
            i += 1;
            while i < b.len() {
                if b[i] == '\\' && i + 1 < b.len() {
                    out.push(b[i]);
                    out.push(b[i + 1]);
                    i += 2;
                    continue;
                }
                out.push(b[i]);
                i += 1;
                if b[i - 1] == '"' {
                    break;
                }
            }
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    out
}

fn select_blocks_in(code: &str) -> Vec<String> {
    let chars: Vec<char> = code.chars().collect();
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(found) = code[at..].find("select!") {
        let start = at + found;
        let char_start = code[..start].chars().count();
        let mut i = char_start;
        while i < chars.len() && chars[i] != '{' {
            i += 1;
        }
        let mut depth = 0;
        let block_start = i;
        while i < chars.len() {
            match chars[i] {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        out.push(chars[block_start..=i.min(chars.len() - 1)].iter().collect());
        at = start + "select!".len();
    }
    out
}
