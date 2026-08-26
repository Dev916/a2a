use a2a::mesh::registry::{
    open_registry, read_registry_env, registry_plan, staleness, write_registry_env, FileRegistry,
    GatewayRow, Registry, RegistryHandle, RegistryStaleness, REGISTRY_SCHEMA_VERSION,
};
use assert_cmd::Command;
use chrono::{TimeZone, Utc};
use predicates::prelude::*;
use serde_json::Value;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn row(fleet: &str, gateway: &str, last_heartbeat: i64) -> GatewayRow {
    let now = Utc.timestamp_opt(last_heartbeat, 0).unwrap();
    GatewayRow {
        fleet_id: fleet.to_string(),
        gateway_id: gateway.to_string(),
        owner: "owner".to_string(),
        endpoint_url: Some(format!("wss://{gateway}.example.test")),
        pubkey_fpr: format!("{gateway}-pub"),
        cert_fpr: Some(format!("{gateway}-cert")),
        version: "test-version".to_string(),
        created_at: now,
        last_heartbeat: now,
        revoked_at: None,
        schema_version: REGISTRY_SCHEMA_VERSION,
    }
}

fn a2a(home: &Path) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home)
        .env_remove("DATABASE_URL")
        .env_remove("A2A_MESH_L2_LIVE");
    c
}

#[tokio::test]
async fn file_registry_implements_lifecycle_and_skips_revoked_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let t1 = Utc.timestamp_opt(1_700_000_030, 0).unwrap();

    registry
        .upsert_self(row("fleet-a", "gw-a", t0.timestamp()))
        .await
        .unwrap();
    registry
        .upsert_self(row("fleet-a", "gw-b", t0.timestamp()))
        .await
        .unwrap();
    registry.heartbeat("fleet-a", "gw-a", t1).await.unwrap();
    registry.retire_self("fleet-a", "gw-a", t1).await.unwrap();
    registry.revoke("fleet-a", "gw-b", t1).await.unwrap();

    let rows = registry.discover("fleet-a").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].gateway_id, "gw-a");
    assert_eq!(rows[0].last_heartbeat, t1);
    assert_eq!(rows[0].endpoint_url, None);
    assert_eq!(
        registry.schema_version().await.unwrap(),
        REGISTRY_SCHEMA_VERSION
    );
}

#[test]
fn reader_computed_staleness_uses_three_heartbeat_intervals() {
    let now = Utc.timestamp_opt(1_700_000_600, 0).unwrap();
    assert_eq!(
        staleness(Utc.timestamp_opt(1_700_000_061, 0).unwrap(), now, 180),
        RegistryStaleness::Active
    );
    assert_eq!(
        staleness(Utc.timestamp_opt(1_700_000_060, 0).unwrap(), now, 180),
        RegistryStaleness::Stale
    );
}

#[test]
fn migration_file_is_the_spec_schema_verbatim() {
    let sql = fs::read_to_string("migrations/mesh_l2_0001.sql").unwrap();
    assert_eq!(
        sql,
        "CREATE TABLE IF NOT EXISTS mesh_gateways (\n  fleet_id       text NOT NULL,\n  gateway_id     text NOT NULL,\n  owner          text NOT NULL,\n  endpoint_url   text,                      -- wss://...; NULL = enrolled but unreachable (outbound-only)\n  pubkey_fpr     text NOT NULL,\n  cert_fpr       text,                      -- SPKI sha256 of the gateway's TLS cert; NULL until first exposure\n  version        text NOT NULL,\n  created_at     timestamptz NOT NULL DEFAULT now(),\n  last_heartbeat timestamptz NOT NULL DEFAULT now(),\n  revoked_at     timestamptz,\n  schema_version int  NOT NULL DEFAULT 1,\n  PRIMARY KEY (fleet_id, gateway_id)\n);\n"
    );
}

#[test]
fn pg_url_and_options_are_pinned_without_connecting() {
    let plan = registry_plan("postgres://u:p@db-pooler.example.test/a2a").unwrap();
    assert!(plan.ssl_verify_full);
    assert_eq!(plan.statement_cache_capacity, None);
    assert_eq!(plan.max_connections, 1);
    assert_eq!(plan.min_connections, 0);
    assert_eq!(plan.idle_timeout_secs, 60);

    let err = registry_plan("postgres://u:p@db.example.test/a2a?sslmode=prefer").unwrap_err();
    assert_eq!(err.code, "REGISTRY_TLS_REQUIRED");
    assert_eq!(err.exit, a2a::output::ExitCode::Precondition);
}

#[test]
fn shared_registry_env_helpers_preserve_cli_file_registry_behavior() {
    let tmp = tempfile::tempdir().unwrap();
    let home = a2a::home::A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
    let registry_dir = tmp.path().join("registry");
    let url = format!("file://{}", registry_dir.display());

    write_registry_env(&home, &url).unwrap();
    let env_file = home.root().join("mesh/fleet/registry.env");
    assert_eq!(
        fs::metadata(&env_file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(read_registry_env(&home).unwrap(), url);
    assert!(matches!(
        open_registry(&url).unwrap(),
        RegistryHandle::File(_)
    ));
    assert!(matches!(
        open_registry(&format!("file:{}", registry_dir.display())).unwrap(),
        RegistryHandle::File(_)
    ));
}

#[test]
fn pg_source_keeps_ddl_out_of_client_and_bounds_gucs_to_local_transactions() {
    let source = fs::read_to_string("src/mesh/registry.rs").unwrap();
    assert!(!source.contains("CREATE TABLE"), "{source}");
    assert!(!source.contains("ALTER TABLE"), "{source}");
    assert!(!source.contains("DROP TABLE"), "{source}");
    assert!(!source.contains("\"SET "), "{source}");
    assert!(source.contains("revoked_at IS NULL"), "{source}");
    assert!(source.contains("SET_LOCAL_STATEMENT_TIMEOUT"), "{source}");
}

#[test]
fn fleet_cli_init_ls_and_revoke_are_hermetic_with_file_registry() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let registry_dir = tmp.path().join("registry");
    let registry_url = format!("file://{}", registry_dir.display());

    let out = a2a(&home)
        .args(["--json", "mesh", "fleet", "init", "--registry", "-"])
        .write_stdin(format!("{registry_url}\n"))
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["command"], "mesh fleet init");
    assert!(v["data"]["fleet_id"]
        .as_str()
        .unwrap()
        .starts_with("fleet-"));
    assert_eq!(v["data"]["root_fingerprint"].as_str().unwrap().len(), 64);
    assert!(v["data"].get("registry").is_none());

    let env_file = home.join("mesh/fleet/registry.env");
    assert_eq!(
        fs::metadata(&env_file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::read_to_string(&env_file).unwrap(),
        format!("{registry_url}\n")
    );

    let reg = FileRegistry::new(&registry_dir);
    let fleet_id = v["data"]["fleet_id"].as_str().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        reg.upsert_self(row(fleet_id, "gw-a", 1_700_000_000))
            .await
            .unwrap();
    });

    let out = a2a(&home)
        .args(["--json", "mesh", "fleet", "ls"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["command"], "mesh fleet ls");
    assert_eq!(v["data"]["rows"][0]["gateway_id"], "gw-a");
    assert_eq!(v["data"]["rows"][0]["staleness"], "stale");

    a2a(&home)
        .args(["mesh", "fleet", "revoke", "gw-a"])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
    let rows = rt.block_on(async { reg.discover(fleet_id).await.unwrap() });
    assert!(rows.is_empty());
}
