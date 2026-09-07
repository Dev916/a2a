// tests/mesh_l2_live.rs
//
// Gated live tier for the layer-2 mesh registry. This file may touch a real
// Neon Postgres database only when A2A_MESH_L2_LIVE=1 is set. It writes only a
// unique row owned by this test's generated gateway id, and it never starts a
// mesh gateway daemon or points one at the real home.
//
// Required invocation:
//
//   A2A_MESH_L2_LIVE=1 \
//   A2A_MESH_L2_REGISTRY_URL=postgres://... \
//   A2A_MESH_L2_POOLER_URL=postgres://...-pooler... \
//   cargo test --test mesh_l2_live -- --ignored

use a2a::mesh::registry::{
    registry_plan, GatewayRow, PgRegistry, Registry, REGISTRY_SCHEMA_VERSION,
};
use chrono::Utc;

const LIVE_ENV: &str = "A2A_MESH_L2_LIVE";
const REGISTRY_URL_ENV: &str = "A2A_MESH_L2_REGISTRY_URL";
const POOLER_URL_ENV: &str = "A2A_MESH_L2_POOLER_URL";
const MIGRATION_SQL: &str = include_str!("../migrations/mesh_l2_0001.sql");

fn gated(what: &str) -> bool {
    if std::env::var(LIVE_ENV).as_deref() != Ok("1") {
        eprintln!("{LIVE_ENV} != 1 - skipping {what}");
        return false;
    }
    true
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for {LIVE_ENV}=1"))
}

fn own_row(fleet_id: &str, gateway_id: &str, now: chrono::DateTime<Utc>) -> GatewayRow {
    GatewayRow {
        fleet_id: fleet_id.to_string(),
        gateway_id: gateway_id.to_string(),
        owner: "mesh-l2-live-test".to_string(),
        endpoint_url: Some("wss://127.0.0.1:1/live-test".to_string()),
        pubkey_fpr: "0".repeat(64),
        cert_fpr: Some("1".repeat(64)),
        version: env!("CARGO_PKG_VERSION").to_string(),
        created_at: now,
        last_heartbeat: now,
        revoked_at: None,
        schema_version: REGISTRY_SCHEMA_VERSION,
        row_msg: None,
        row_token: None,
        row_sig: None,
        signed_at: None,
        revocation: None,
    }
}

#[tokio::test]
#[ignore]
async fn neon_registry_own_row_lifecycle_and_pooler_statement_cache_probe() {
    if !gated("mesh layer-2 live Neon registry") {
        return;
    }
    assert!(
        MIGRATION_SQL.contains("CREATE TABLE IF NOT EXISTS mesh_gateways"),
        "migration file is present in the compiled live gate"
    );

    let registry_url = required_env(REGISTRY_URL_ENV);
    let pooler_url = std::env::var(POOLER_URL_ENV).unwrap_or_else(|_| registry_url.clone());
    assert!(
        pooler_url.contains("-pooler"),
        "{POOLER_URL_ENV} or {REGISTRY_URL_ENV} must be a Neon pooler URL containing -pooler"
    );
    assert_eq!(
        registry_plan(&pooler_url).unwrap().statement_cache_capacity,
        None,
        "Neon pooler URLs disable sqlx statement caching"
    );

    let registry = PgRegistry::connect_lazy(&registry_url, 30).unwrap();
    assert_eq!(
        registry.schema_version().await.unwrap(),
        REGISTRY_SCHEMA_VERSION
    );

    let now = Utc::now();
    let fleet_id = format!("fleet-live-{}", ulid::Ulid::new());
    let gateway_id = format!("gw-live-{}", ulid::Ulid::new());
    registry
        .upsert_self(own_row(&fleet_id, &gateway_id, now))
        .await
        .unwrap();
    registry
        .heartbeat(&fleet_id, &gateway_id, now + chrono::Duration::seconds(1))
        .await
        .unwrap();
    let rows = registry.discover(&fleet_id).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].gateway_id, gateway_id);
    assert_eq!(rows[0].owner, "mesh-l2-live-test");

    let pooler = PgRegistry::connect_lazy(&pooler_url, 30).unwrap();
    assert_eq!(pooler.prepared_statement_probe().await.unwrap(), 17);
}
