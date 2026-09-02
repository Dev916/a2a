// Gated live tier for layer-3 mesh work handoff.
//
// Required invocation:
//
//   A2A_MESH_L3_LIVE=1 \
//   A2A_MESH_L3_REGISTRY_URL=postgres://... \
//   cargo test --test mesh_l3_live -- --ignored

mod common;

use a2a::home::A2aHome;
use a2a::mesh::registry::{
    ClaimOutcome, PgRegistry, Registry, WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION,
};
use assert_cmd::assert::Assert;
use chrono::Utc;
use serde_json::Value;

const LIVE_ENV: &str = "A2A_MESH_L3_LIVE";
const REGISTRY_URL_ENV: &str = "A2A_MESH_L3_REGISTRY_URL";

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

fn json_out(assert: &Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn neon_work_claim_contention_and_fleet_migrate_idempotency() {
    if !gated("mesh layer-3 live Neon work registry") {
        return;
    }

    let registry_url = required_env(REGISTRY_URL_ENV);
    let tmp = tempfile::tempdir().unwrap();
    let root = A2aHome::resolve(Some(&tmp.path().join("root"))).unwrap();
    a2a::mesh::keys::fleet_init(&root).unwrap();
    a2a::mesh::registry::write_registry_env(&root, &registry_url).unwrap();

    for _ in 0..2 {
        let migrated = common::a2a(root.root(), tmp.path())
            .args(["--json", "mesh", "fleet", "migrate"])
            .assert()
            .success();
        let migrated = json_out(&migrated);
        let applied = migrated["data"]["applied"].as_array().unwrap();
        assert!(
            applied
                .iter()
                .any(|name| name.as_str() == Some("mesh_l3_0001.sql")),
            "{migrated}"
        );
    }

    let fleet_id = format!("fleet-l3-live-{}", ulid::Ulid::new());
    let work_id = format!("work-l3-live-{}", ulid::Ulid::new());
    let now = Utc::now();
    let registry_a = PgRegistry::connect_lazy(&registry_url, 30).unwrap();
    let registry_b = PgRegistry::connect_lazy(&registry_url, 30).unwrap();
    registry_a
        .work_insert(WorkRow {
            fleet_id: fleet_id.clone(),
            work_id: work_id.clone(),
            sender_gw: "gw-l3-live-sender".to_string(),
            addressing: "pool".to_string(),
            status: WorkStatus::Offered,
            claimed_by: None,
            result_ref: None,
            result_delivery: None,
            swept_by: None,
            created_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::days(1),
            schema_version: REGISTRY_SCHEMA_VERSION,
        })
        .await
        .unwrap();

    let (claim_a, claim_b) = tokio::join!(
        registry_a.work_claim(&fleet_id, &work_id, "gw-l3-live-a"),
        registry_b.work_claim(&fleet_id, &work_id, "gw-l3-live-b"),
    );
    let claim_a = claim_a.unwrap();
    let claim_b = claim_b.unwrap();
    let wins = [&claim_a, &claim_b]
        .into_iter()
        .filter(|outcome| matches!(outcome, ClaimOutcome::Won))
        .count();
    assert_eq!(wins, 1, "exactly one live PgRegistry claim wins");

    let winner = match (&claim_a, &claim_b) {
        (ClaimOutcome::Won, ClaimOutcome::Lost(row)) => row.claimed_by.as_deref().unwrap(),
        (ClaimOutcome::Lost(row), ClaimOutcome::Won) => row.claimed_by.as_deref().unwrap(),
        other => panic!("expected one winner and one loser, got {other:?}"),
    };
    assert!(matches!(winner, "gw-l3-live-a" | "gw-l3-live-b"));

    let final_row = registry_a
        .work_get(&fleet_id, &work_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_row.status, WorkStatus::Approved);
    assert_eq!(final_row.claimed_by.as_deref(), Some(winner));
}
