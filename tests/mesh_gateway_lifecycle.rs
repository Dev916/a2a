mod common;

use a2a::mesh::registry::Registry;
use predicates::prelude::*;
use serde_json::Value;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn mesh_gateway_config_surface_is_pinned_and_originated() {
    assert_eq!(
        a2a::config::MESH_GATEWAY_FIELDS,
        [
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
        ]
    );
    assert_eq!(
        a2a::config::mesh_gateway_env_key("ping_secs"),
        "A2A_MESH_GATEWAY_PING_SECS"
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
            unknown = 1
        "#,
    )
    .unwrap();

    let out = common::a2a(&home, &work)
        .env("A2A_MESH_GATEWAY_PING_SECS", "44")
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
    assert_eq!(data["origins"]["mesh.gateway.listen_port"], "file");
    assert_eq!(data["origins"]["mesh.gateway.ping_secs"], "env");
    for field in a2a::config::MESH_GATEWAY_FIELDS {
        assert!(
            data["origins"]
                .as_object()
                .unwrap()
                .contains_key(&format!("mesh.gateway.{field}")),
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
    assert_eq!(health["pid"], child.id());
    assert!(health["loops"]["registry"]["heartbeats"].as_u64().unwrap() >= 1);

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
            ("A2A_TEST_MESH_GATEWAY_STUB_PANIC", "registry"),
            ("A2A_MESH_GATEWAY_OUTBOUND_RETRY_MAX_SECS", "1"),
        ],
    );
    let status = wait_exit(&mut child, Duration::from_secs(5));
    assert!(!status.success(), "supervisor escalation exits non-zero");
    let health = read_health(&home);
    assert_eq!(health["state"], "escalated");
    assert_eq!(health["escalation"]["loop"], "registry");
    assert_eq!(health["loops"]["registry"]["restart_policy"], "one-for-one");

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
        &[("A2A_TEST_MESH_GATEWAY_STUB_STALL_SHUTDOWN", "watcher")],
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

fn spawn_gateway(home: &std::path::Path, work: &std::path::Path, envs: &[(&str, &str)]) -> Child {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin("a2a"));
    cmd.arg("--home")
        .arg(home)
        .args(["mesh", "gateway", "run"])
        .env("A2A_ALLOWED_ROOTS", work)
        .env("A2A_CODEX_BIN", common::stub_path())
        .env("A2A_BROKER_DISABLE", "1")
        .env("A2A_OWNER", common::TEST_OWNER)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.spawn().unwrap()
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
            registry.discover(&fleet_id).await.unwrap()
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
