use a2a::mesh::registry::{
    ClaimOutcome, FileRegistry, Registry, TransitionOutcome, WorkListFilter, WorkOwner,
    WorkPatch, WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION,
};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use std::fs;
use std::time::Duration;

fn work(work_id: &str, addressing: &str) -> WorkRow {
    let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    WorkRow {
        fleet_id: "fleet-a".to_string(),
        work_id: work_id.to_string(),
        sender_gw: "sender-gw".to_string(),
        addressing: addressing.to_string(),
        status: WorkStatus::Offered,
        claimed_by: None,
        result_ref: None,
        result_delivery: None,
        swept_by: None,
        created_at: now,
        updated_at: now,
        expires_at: now + ChronoDuration::hours(1),
        schema_version: REGISTRY_SCHEMA_VERSION,
    }
}

async fn cas_suite<R>(registry: R)
where
    R: Registry + Clone + Send + Sync + 'static,
{
    double_claim_has_one_winner(registry.clone()).await;
    directed_claim_by_non_addressee_is_refused(registry.clone()).await;
    decline_after_claim_reports_real_status(registry.clone()).await;
    cancel_race_returns_the_actual_row(registry.clone()).await;
    owner_checked_transitions(registry.clone()).await;
    expiry_sweep_uses_registry_clock(registry.clone()).await;
    abandoned_sweep_only_touches_own_claims(registry).await;
}

#[tokio::test]
async fn file_registry_work_cas_suite() {
    let tmp = tempfile::tempdir().unwrap();
    cas_suite(FileRegistry::new(tmp.path())).await;
}

#[test]
fn mesh_l3_migration_is_the_spec_schema_verbatim_and_all_migrations_are_single_statement() {
    let sql = fs::read_to_string("migrations/mesh_l3_0001.sql").unwrap();
    assert_eq!(
        sql,
        "CREATE TABLE IF NOT EXISTS mesh_work_items (\n  fleet_id        text NOT NULL,\n  work_id         text NOT NULL,\n  sender_gw       text NOT NULL,\n  addressing      text NOT NULL,          -- 'directed:<gw>' | 'pool'\n  status          text NOT NULL,          -- §3\n  claimed_by      text,                   -- gateway_id set by the claim CAS\n  result_ref      text,                   -- branch name once done\n  result_delivery text,                   -- 'delivered' | 'rejected:<code>' | NULL\n  swept_by        text,                   -- gateway that expired/abandoned it\n  created_at      timestamptz NOT NULL,\n  updated_at      timestamptz NOT NULL,\n  expires_at      timestamptz NOT NULL,\n  schema_version  integer NOT NULL,\n  PRIMARY KEY (fleet_id, work_id)\n);\n"
    );

    for entry in fs::read_dir("migrations").unwrap() {
        let path = entry.unwrap().path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.starts_with("mesh_l") || !name.ends_with(".sql") {
            continue;
        }
        let sql = fs::read_to_string(&path).unwrap();
        assert_eq!(
            statement_terminators(&sql),
            1,
            "{} must contain exactly one statement",
            path.display()
        );
        assert!(
            sql.trim_end().ends_with(';'),
            "{} must end with its only statement terminator",
            path.display()
        );
    }
}

#[test]
fn work_schema_missing_mapping_names_migrate_remedy() {
    let source = fs::read_to_string("src/mesh/registry.rs").unwrap();
    assert!(
        source.contains("db.code().as_deref() == Some(\"42P01\")"),
        "{source}"
    );
    assert!(source.contains("\"WORK_SCHEMA_MISSING\""), "{source}");
    assert!(
        source.contains("the fleet root machine must run `a2a mesh fleet migrate`"),
        "{source}"
    );
}

fn statement_terminators(sql: &str) -> usize {
    sql.lines()
        .map(|line| line.split_once("--").map(|(sql, _)| sql).unwrap_or(line))
        .map(|line| line.matches(';').count())
        .sum()
}

async fn double_claim_has_one_winner<R>(registry: R)
where
    R: Registry + Clone + Send + Sync + 'static,
{
    registry.work_insert(work("double-claim", "pool")).await.unwrap();

    let (a, b) = tokio::join!(
        registry.work_claim("fleet-a", "double-claim", "gw-a"),
        registry.work_claim("fleet-a", "double-claim", "gw-b")
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, ClaimOutcome::Won))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, ClaimOutcome::Lost(_)))
            .count(),
        1
    );
    let row = registry
        .work_get("fleet-a", "double-claim")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, WorkStatus::Approved);
    assert!(matches!(
        row.claimed_by.as_deref(),
        Some("gw-a") | Some("gw-b")
    ));
}

async fn directed_claim_by_non_addressee_is_refused<R>(registry: R)
where
    R: Registry + Clone + Send + Sync + 'static,
{
    registry
        .work_insert(work("directed", "directed:gw-allowed"))
        .await
        .unwrap();

    let lost = registry
        .work_claim("fleet-a", "directed", "gw-other")
        .await
        .unwrap();
    let ClaimOutcome::Lost(row) = lost else {
        panic!("non-addressee must not win directed claim");
    };
    assert_eq!(row.status, WorkStatus::Offered);
    assert_eq!(row.claimed_by, None);

    assert!(matches!(
        registry
            .work_claim("fleet-a", "directed", "gw-allowed")
            .await
            .unwrap(),
        ClaimOutcome::Won
    ));
}

async fn decline_after_claim_reports_real_status<R>(registry: R)
where
    R: Registry + Clone + Send + Sync + 'static,
{
    registry
        .work_insert(work("decline-after-claim", "pool"))
        .await
        .unwrap();
    assert!(matches!(
        registry
            .work_claim("fleet-a", "decline-after-claim", "gw-a")
            .await
            .unwrap(),
        ClaimOutcome::Won
    ));

    let outcome = registry
        .work_transition(
            "fleet-a",
            "decline-after-claim",
            WorkStatus::Offered,
            WorkStatus::Declined,
            WorkOwner::Sender("sender-gw".to_string()),
            WorkPatch::default(),
        )
        .await
        .unwrap();
    let TransitionOutcome::Lost(row) = outcome else {
        panic!("decline after claim must lose and return the actual row");
    };
    assert_eq!(row.status, WorkStatus::Approved);
    assert_eq!(row.claimed_by.as_deref(), Some("gw-a"));
}

async fn cancel_race_returns_the_actual_row<R>(registry: R)
where
    R: Registry + Clone + Send + Sync + 'static,
{
    registry
        .work_insert(work("cancel-race", "pool"))
        .await
        .unwrap();

    let (claim, cancel) = tokio::join!(
        registry.work_claim("fleet-a", "cancel-race", "gw-a"),
        registry.work_transition(
            "fleet-a",
            "cancel-race",
            WorkStatus::Offered,
            WorkStatus::Cancelled,
            WorkOwner::Sender("sender-gw".to_string()),
            WorkPatch::default(),
        )
    );
    let claim = claim.unwrap();
    let cancel = cancel.unwrap();
    match (claim, cancel) {
        (ClaimOutcome::Won, TransitionOutcome::Lost(row)) => {
            assert_eq!(row.status, WorkStatus::Approved);
            assert_eq!(row.claimed_by.as_deref(), Some("gw-a"));
        }
        (ClaimOutcome::Lost(row), TransitionOutcome::Applied) => {
            assert_eq!(row.status, WorkStatus::Cancelled);
            assert_eq!(row.claimed_by, None);
        }
        other => panic!("expected exactly one race winner, got {other:?}"),
    }
}

async fn owner_checked_transitions<R>(registry: R)
where
    R: Registry + Clone + Send + Sync + 'static,
{
    registry
        .work_insert(work("owner-checked", "pool"))
        .await
        .unwrap();
    registry
        .work_claim("fleet-a", "owner-checked", "gw-a")
        .await
        .unwrap();

    let wrong = registry
        .work_transition(
            "fleet-a",
            "owner-checked",
            WorkStatus::Approved,
            WorkStatus::Running,
            WorkOwner::Claimer("gw-other".to_string()),
            WorkPatch::default(),
        )
        .await
        .unwrap();
    let TransitionOutcome::Lost(row) = wrong else {
        panic!("wrong claimer must not transition");
    };
    assert_eq!(row.status, WorkStatus::Approved);

    assert!(matches!(
        registry
            .work_transition(
                "fleet-a",
                "owner-checked",
                WorkStatus::Approved,
                WorkStatus::Running,
                WorkOwner::Claimer("gw-a".to_string()),
                WorkPatch::default(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Applied
    ));

    let patch = WorkPatch {
        result_ref: Some(Some("a2a/work/owner-checked".to_string())),
        result_delivery: Some(Some("delivered".to_string())),
        ..Default::default()
    };
    assert!(matches!(
        registry
            .work_transition(
                "fleet-a",
                "owner-checked",
                WorkStatus::Running,
                WorkStatus::Done,
                WorkOwner::Claimer("gw-a".to_string()),
                patch,
            )
            .await
            .unwrap(),
        TransitionOutcome::Applied
    ));
    let row = registry
        .work_get("fleet-a", "owner-checked")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, WorkStatus::Done);
    assert_eq!(row.result_ref.as_deref(), Some("a2a/work/owner-checked"));
    assert_eq!(row.result_delivery.as_deref(), Some("delivered"));
}

async fn expiry_sweep_uses_registry_clock<R>(registry: R)
where
    R: Registry + Clone + Send + Sync + 'static,
{
    let mut expired = work("expired", "pool");
    expired.expires_at = Utc::now() - ChronoDuration::seconds(1);
    let mut fresh = work("fresh", "pool");
    fresh.expires_at = Utc::now() + ChronoDuration::hours(1);
    registry.work_insert(expired).await.unwrap();
    registry.work_insert(fresh).await.unwrap();

    let swept = registry
        .work_sweep_expired("fleet-a", "sweeper-gw")
        .await
        .unwrap();
    assert_eq!(swept, vec!["expired".to_string()]);
    let row = registry.work_get("fleet-a", "expired").await.unwrap().unwrap();
    assert_eq!(row.status, WorkStatus::Expired);
    assert_eq!(row.swept_by.as_deref(), Some("sweeper-gw"));
    let row = registry.work_get("fleet-a", "fresh").await.unwrap().unwrap();
    assert_eq!(row.status, WorkStatus::Offered);
}

async fn abandoned_sweep_only_touches_own_claims<R>(registry: R)
where
    R: Registry + Clone + Send + Sync + 'static,
{
    let old = Utc::now() - ChronoDuration::hours(2);
    for (id, claimer) in [("own-approved", "gw-a"), ("foreign-running", "gw-b")] {
        let mut row = work(id, "pool");
        row.status = if id == "own-approved" {
            WorkStatus::Approved
        } else {
            WorkStatus::Running
        };
        row.claimed_by = Some(claimer.to_string());
        row.updated_at = old;
        registry.work_insert(row).await.unwrap();
    }

    let swept = registry
        .work_sweep_abandoned("fleet-a", "gw-a", Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(swept.len(), 1);
    assert_eq!(swept[0].work_id, "own-approved");
    assert_eq!(swept[0].status, WorkStatus::Failed);
    assert_eq!(swept[0].swept_by.as_deref(), Some("gw-a"));

    let foreign = registry
        .work_get("fleet-a", "foreign-running")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(foreign.status, WorkStatus::Running);
    assert_eq!(foreign.claimed_by.as_deref(), Some("gw-b"));
}

#[tokio::test]
async fn work_list_filters_by_fleet_status_and_claimer() {
    let tmp = tempfile::tempdir().unwrap();
    let registry = FileRegistry::new(tmp.path());
    registry.work_insert(work("list-offered", "pool")).await.unwrap();
    let mut approved = work("list-approved", "pool");
    approved.status = WorkStatus::Approved;
    approved.claimed_by = Some("gw-a".to_string());
    registry.work_insert(approved).await.unwrap();
    let mut other_fleet = work("other-fleet", "pool");
    other_fleet.fleet_id = "fleet-b".to_string();
    registry.work_insert(other_fleet).await.unwrap();

    let filter = WorkListFilter {
        status: Some(WorkStatus::Approved),
        claimed_by: Some("gw-a".to_string()),
    };
    let rows = registry.work_list("fleet-a", filter).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].work_id, "list-approved");
}
