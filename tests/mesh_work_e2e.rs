mod common;

use a2a::config::Config;
use a2a::home::A2aHome;
use a2a::mesh::feed;
use a2a::mesh::gateway::link::{push_spooled_notes, push_watcher_deltas};
use a2a::mesh::gateway::presence_wire;
use a2a::mesh::keys::fleet_id;
use a2a::mesh::registry::{GatewayRow, Registry, WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION};
use a2a::mesh::row_sig::RowVerdict;
use a2a::mesh::work_exec::{
    self, ApproveOutcome, RealGit, WorkCompletion, WorkCompletionIo, WorkLoopOptions, WorkSpawner,
};
use a2a::mesh::{work_store, PeerId};
use a2a::output::AppError;
use assert_cmd::assert::Assert;
use chrono::Utc;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

fn cfg_for(home: &A2aHome) -> Config {
    let mut cfg = a2a::config::load(home).unwrap().0;
    cfg.mesh.work.auto_push = true;
    cfg
}

fn cmd(home: &A2aHome, work: &Path) -> assert_cmd::Command {
    let mut c = common::a2a(home.root(), work);
    c.env("A2A_TEST_MESH_GATEWAY_SERVER_BIND_OPTIONAL", "1");
    c
}

fn json_out(assert: &Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).unwrap()
}

fn write_work_brief(work: &Path, test_name: &str) -> PathBuf {
    common::write_brief(
        work,
        &format!(
            "---\nlane: impl\nacceptance_criteria:\n  - focused test passes\nverify:\n  - cargo test --test mesh_work_e2e {test_name}\n---\n# Build it\n"
        ),
    )
}

fn send_directed(f: &common::TestMeshWorkFixture, title: &str) -> String {
    let brief = write_work_brief(&f.work, "directed_work_round_trips_result_and_ack");
    let sent = cmd(&f.home_a, &f.work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &f.gateway_b,
            "--repo",
            &common::local_bare_url(&f.remote_bare),
            "--base-ref",
            "main",
            "--title",
            title,
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .success();
    json_out(&sent)["data"]["work_id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn registry_row(f: &common::TestMeshWorkFixture, work_id: &str) -> WorkRow {
    f.registry
        .work_get(&fleet_id(&f.home_a).unwrap(), work_id)
        .await
        .unwrap()
        .unwrap()
}

async fn wait_for_result_delivery(
    f: &common::TestMeshWorkFixture,
    work_id: &str,
    delivery: &str,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let row = registry_row(f, work_id).await;
        if row.result_delivery.as_deref() == Some(delivery) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "result_delivery did not become {delivery}: {row:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

fn result_notes(home: &A2aHome) -> Vec<Value> {
    let peer = PeerId::parse("user:mesh-work").unwrap();
    let inbox = home
        .root()
        .join("mesh")
        .join("peers")
        .join(peer.dir_name())
        .join("inbox");
    let mut notes = match std::fs::read_dir(inbox) {
        Ok(entries) => entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.file_name().unwrap().to_string_lossy().ends_with(".note.json"))
            .map(|path| serde_json::from_slice::<Value>(&std::fs::read(path).unwrap()).unwrap())
            .filter(|note| note["kind"] == work_store::WORK_RESULT_KIND)
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    notes.sort_by(|a, b| a["note_id"].as_str().cmp(&b["note_id"].as_str()));
    notes
}

fn assert_bare_has_result_ref(remote_bare: &Path, work_id: &str) {
    let refname = format!("refs/heads/a2a/work/{work_id}");
    let out = std::process::Command::new("git")
        .args(["--git-dir", remote_bare.to_str().unwrap(), "show-ref", &refname])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "missing pushed result ref {refname}\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn cache_usable_sender(home: &A2aHome, gateway_id: &str) {
    let now = Utc::now();
    presence_wire::refresh_registry_cache(
        home,
        &[(
            GatewayRow {
                fleet_id: fleet_id(home).unwrap(),
                gateway_id: gateway_id.to_string(),
                owner: "legacy-sender".to_string(),
                endpoint_url: None,
                pubkey_fpr: "unverified".to_string(),
                cert_fpr: None,
                version: "0.3.0".to_string(),
                created_at: now,
                last_heartbeat: now,
                revoked_at: None,
                schema_version: REGISTRY_SCHEMA_VERSION,
                row_msg: None,
                row_token: None,
                row_sig: None,
                signed_at: None,
                revocation: None,
            },
            RowVerdict::Unsigned { stripped: false },
        )],
        now,
    )
    .unwrap();
}

struct FakeSpawner;

impl WorkSpawner for FakeSpawner {
    fn preflight(&self, _home: &A2aHome, _checkout: &Path) -> Result<(), AppError> {
        Ok(())
    }

    fn spawn(&self, _home: &A2aHome, _checkout: &Path, _brief: &Path) -> Result<String, AppError> {
        Ok("impl-terminal-e2e".to_string())
    }
}

#[derive(Clone, Copy)]
struct TerminalGitIo;

impl WorkCompletionIo for TerminalGitIo {
    fn reconcile_worker(
        &self,
        _home: &A2aHome,
        _cfg: &Config,
        _worker_id: &str,
    ) -> Result<WorkCompletion, AppError> {
        Ok(WorkCompletion::Done {
            summary: "e2e worker finished".to_string(),
            evidence_excerpt: "cargo test --test mesh_work_e2e: ok".to_string(),
        })
    }

    fn push_result_branch(
        &self,
        checkout: &Path,
        work_id: &str,
        remote_url: &str,
        held: &a2a::mesh::work_gate::ResultRange,
    ) -> Result<(), AppError> {
        let refspec = format!("{}:refs/heads/a2a/work/{work_id}", held.head_sha);
        common::run_git(checkout, &["push", "--", remote_url, &refspec]);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn directed_work_round_trips_result_and_ack() {
    let f = common::mesh_work_fixture("directed-work-e2e").await;
    cache_usable_sender(&f.home_b, &f.gateway_a);
    let cfg_a = cfg_for(&f.home_a);
    let cfg_b = cfg_for(&f.home_b);
    let link = common::start_mesh_link(
        f.home_a.clone(),
        cfg_a.clone(),
        &f.gateway_a,
        f.home_b.clone(),
        cfg_b.clone(),
        &f.gateway_b,
    )
    .await;

    let work_id = send_directed(&f, "Directed E2E");
    assert_eq!(push_spooled_notes(&f.home_a, &link.a_state).await.unwrap(), 1);
    common::wait_until(|| work_store::load(&f.home_b, &work_id).unwrap().is_some()).await;

    let listed = cmd(&f.home_b, &f.work)
        .args(["--json", "mesh", "work", "list"])
        .assert()
        .success();
    let listed = json_out(&listed);
    let item = listed["data"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["work_id"] == work_id)
        .unwrap();
    assert_eq!(item["source"], "directed");
    assert_eq!(item["status"], "offered");
    assert_eq!(item["addressing"], format!("directed:{}", f.gateway_b));

    let shown = cmd(&f.home_b, &f.work)
        .args(["--json", "mesh", "work", "show", &work_id])
        .assert()
        .success();
    let shown = json_out(&shown);
    assert_eq!(shown["data"]["title"], "Directed E2E");
    assert_eq!(shown["data"]["provenance"]["sender_gateway"], f.gateway_a);
    assert_eq!(
        shown["data"]["provenance"]["token_fleet_id"],
        fleet_id(&f.home_b).unwrap()
    );

    let approved =
        work_exec::advance_approve(&f.home_b, &f.registry, &RealGit, &FakeSpawner, &work_id)
            .await
            .unwrap();
    assert!(matches!(approved, ApproveOutcome::Running { .. }));

    let tick = work_exec::work_loop_tick(
        &f.home_b,
        &cfg_b,
        &f.registry,
        &TerminalGitIo,
        WorkLoopOptions::for_config(&cfg_b),
    )
    .await;
    assert_eq!(tick.completed, 1, "{tick:?}");
    assert_eq!(tick.registry_error, None, "{tick:?}");
    assert_bare_has_result_ref(&f.remote_bare, &work_id);

    assert_eq!(push_spooled_notes(&f.home_b, &link.b_state).await.unwrap(), 1);
    common::wait_until(|| {
        result_notes(&f.home_a).iter().any(|note| {
            note["refs"]["result"]["work_id"] == work_id
                && note["refs"]["result"]["status"] == "done"
        })
    })
    .await;
    wait_for_result_delivery(&f, &work_id, "delivered").await;

    let status = cmd(&f.home_a, &f.work)
        .args(["--json", "mesh", "work", "status", &work_id])
        .assert()
        .success();
    let status = json_out(&status);
    assert_eq!(status["data"]["items"][0]["status"], "done", "{status}");
    assert_eq!(
        status["data"]["items"][0]["result_delivery"],
        "delivered",
        "{status}"
    );

    let acked = cmd(&f.home_a, &f.work)
        .args(["--json", "mesh", "work", "ack", &work_id])
        .assert()
        .success();
    let acked = json_out(&acked);
    assert_eq!(acked["data"]["status"], "acked", "{acked}");
    assert_eq!(registry_row(&f, &work_id).await.status, WorkStatus::Acked);
    link.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pool_work_claim_race_has_one_winner_and_loser_sees_claimant() {
    let f = common::mesh_work_fixture("pool-work-e2e").await;
    cache_usable_sender(&f.home_a, &f.gateway_a);
    cache_usable_sender(&f.home_b, &f.gateway_a);
    let cfg_a = cfg_for(&f.home_a);
    let cfg_b = cfg_for(&f.home_b);
    let link = common::start_mesh_link(
        f.home_a.clone(),
        cfg_a.clone(),
        &f.gateway_a,
        f.home_b.clone(),
        cfg_b.clone(),
        &f.gateway_b,
    )
    .await;

    let brief = write_work_brief(&f.work, "pool_work_claim_race_has_one_winner_and_loser_sees_claimant");
    let offered = cmd(&f.home_a, &f.work)
        .args([
            "--json",
            "mesh",
            "work",
            "offer",
            "--repo",
            &common::local_bare_url(&f.remote_bare),
            "--base-ref",
            "main",
            "--title",
            "Pool E2E",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .success();
    let work_id = json_out(&offered)["data"]["work_id"]
        .as_str()
        .unwrap()
        .to_string();

    assert_eq!(
        push_watcher_deltas(&f.home_a, &cfg_a, &f.gateway_a, &link.a_state)
            .await
            .unwrap(),
        1
    );
    common::wait_until(|| feed::read(&f.home_b, 0).unwrap().iter().any(|entry| entry.refs["work"]["work_id"] == work_id)).await;
    assert!(
        work_store::load(&f.home_b, &work_id).unwrap().is_some(),
        "gateway feed ingest materializes pool work on the receiving home"
    );

    let tick_a = work_exec::work_loop_tick(
        &f.home_a,
        &cfg_a,
        &f.registry,
        &TerminalGitIo,
        WorkLoopOptions::for_config(&cfg_a),
    )
    .await;
    let tick_b = work_exec::work_loop_tick(
        &f.home_b,
        &cfg_b,
        &f.registry,
        &TerminalGitIo,
        WorkLoopOptions::for_config(&cfg_b),
    )
    .await;
    assert_eq!(tick_a.pool_materialized, 1, "{tick_a:?}");
    assert_eq!(tick_b.registry_error, None, "{tick_b:?}");
    assert_eq!(tick_b.item_errors, 0, "{tick_b:?}");

    let (a, b) = tokio::join!(
        work_exec::advance_approve(&f.home_a, &f.registry, &RealGit, &FakeSpawner, &work_id),
        work_exec::advance_approve(&f.home_b, &f.registry, &RealGit, &FakeSpawner, &work_id),
    );
    let a = a.unwrap();
    let b = b.unwrap();
    let mut winners = Vec::new();
    let mut losers = Vec::new();
    for (gateway, outcome) in [(&f.gateway_a, a), (&f.gateway_b, b)] {
        match outcome {
            ApproveOutcome::Running { .. } => winners.push(gateway.clone()),
            ApproveOutcome::Lost {
                status, claimed_by, ..
            } => losers.push((gateway.clone(), status, claimed_by)),
            other => panic!("unexpected approve outcome: {other:?}"),
        }
    }
    assert_eq!(winners.len(), 1, "exactly one pool approver wins");
    assert_eq!(losers.len(), 1, "exactly one pool approver loses");
    assert_eq!(losers[0].2.as_deref(), Some(winners[0].as_str()));
    assert!(matches!(losers[0].1.as_str(), "approved" | "running"));

    let loser_home = if losers[0].0 == f.gateway_a {
        &f.home_a
    } else {
        &f.home_b
    };
    let listed = cmd(loser_home, &f.work)
        .args(["--json", "mesh", "work", "list"])
        .assert()
        .success();
    let listed = json_out(&listed);
    let item = listed["data"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["work_id"] == work_id)
        .unwrap();
    assert_eq!(item["source"], "pool");
    assert_eq!(item["claimed_by"], winners[0]);
    assert!(matches!(item["status"].as_str().unwrap(), "approved" | "running"));
    link.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn declined_with_reason_round_trips_to_sender() {
    let f = common::mesh_work_fixture("decline-work-e2e").await;
    let cfg_a = cfg_for(&f.home_a);
    let cfg_b = cfg_for(&f.home_b);
    let link = common::start_mesh_link(
        f.home_a.clone(),
        cfg_a.clone(),
        &f.gateway_a,
        f.home_b.clone(),
        cfg_b.clone(),
        &f.gateway_b,
    )
    .await;

    let work_id = send_directed(&f, "Decline E2E");
    assert_eq!(push_spooled_notes(&f.home_a, &link.a_state).await.unwrap(), 1);
    common::wait_until(|| work_store::load(&f.home_b, &work_id).unwrap().is_some()).await;

    let declined = cmd(&f.home_b, &f.work)
        .args([
            "--json",
            "mesh",
            "work",
            "decline",
            &work_id,
            "--reason",
            "No capacity this week",
        ])
        .assert()
        .success();
    let declined = json_out(&declined);
    assert_eq!(declined["data"]["status"], "declined", "{declined}");
    assert_eq!(registry_row(&f, &work_id).await.status, WorkStatus::Declined);

    assert_eq!(push_spooled_notes(&f.home_b, &link.b_state).await.unwrap(), 1);
    common::wait_until(|| {
        result_notes(&f.home_a).iter().any(|note| {
            note["refs"]["result"]
                == json!({
                    "work_id": work_id,
                    "status": "declined",
                    "result_ref": null,
                    "summary": "No capacity this week",
                    "evidence_excerpt": ""
                })
        })
    })
    .await;

    let status = cmd(&f.home_a, &f.work)
        .args(["--json", "mesh", "work", "status", &work_id])
        .assert()
        .success();
    let status = json_out(&status);
    assert_eq!(status["data"]["items"][0]["status"], "declined", "{status}");
    link.stop().await;
}
