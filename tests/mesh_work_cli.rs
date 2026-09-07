mod common;

use a2a::home::A2aHome;
use a2a::mesh::gateway::notes_wire::{
    self, deliver_spooled_once, NoteDeliveryTarget, SpooledNote,
};
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_id, fleet_init, gateway_identity, gateway_public_key_b64,
    gateway_public_key_fingerprint, install_gateway_token, SystemClock,
};
use a2a::mesh::registry::{
    write_registry_env, FileRegistry, GatewayRow, Registry, WorkRow, WorkStatus,
    REGISTRY_SCHEMA_VERSION,
};
use a2a::mesh::work_store;
use assert_cmd::Command;
use chrono::{TimeZone, Utc};
use predicates::prelude::*;
use rand::{rngs::StdRng, SeedableRng};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

fn cmd(home: &Path, work: &Path) -> Command {
    common::a2a(home, work)
}

fn json_out(assert: &assert_cmd::assert::Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).unwrap()
}

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn copy_root_pub(root: &Path, joiner: &Path) {
    fs::create_dir_all(joiner.join("mesh/fleet")).unwrap();
    fs::copy(
        root.join("mesh/fleet/root.pub"),
        joiner.join("mesh/fleet/root.pub"),
    )
    .unwrap();
}

fn enroll_joiner(root: &A2aHome, joiner: &A2aHome, clock: &SystemClock) -> String {
    let identity = gateway_identity(joiner, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(joiner).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey_b64, clock).unwrap();
    install_gateway_token(joiner, &token, clock).unwrap();
    identity.gateway_id
}

fn gateway_row(home: &A2aHome, fleet: &str, version: &str) -> GatewayRow {
    let identity = gateway_identity(home, false).unwrap();
    GatewayRow {
        fleet_id: fleet.to_string(),
        gateway_id: identity.gateway_id,
        owner: "test-owner".to_string(),
        endpoint_url: None,
        pubkey_fpr: gateway_public_key_fingerprint(home).unwrap(),
        cert_fpr: Some(identity.cert_fpr),
        version: version.to_string(),
        created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        last_heartbeat: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        revoked_at: None,
        schema_version: REGISTRY_SCHEMA_VERSION,
        row_msg: None,
        row_token: None,
        row_sig: None,
        signed_at: None,
        revocation: None,
    }
}

struct MeshFixture {
    _tmp: tempfile::TempDir,
    sender: A2aHome,
    receiver: A2aHome,
    registry: FileRegistry,
    sender_gw: String,
    receiver_gw: String,
    work: PathBuf,
}

fn mesh_fixture(receiver_version: &str) -> MeshFixture {
    let tmp = tempfile::tempdir().unwrap();
    let root = home_at(&tmp.path().join("root"));
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    let work = tmp.path().join("work");
    fs::create_dir_all(&work).unwrap();
    let clock = SystemClock;

    fleet_init(&root).unwrap();
    copy_root_pub(root.root(), sender.root());
    copy_root_pub(root.root(), receiver.root());
    let sender_gw = enroll_joiner(&root, &sender, &clock);
    let receiver_gw = enroll_joiner(&root, &receiver, &clock);
    let registry_dir = tmp.path().join("registry");
    let registry = FileRegistry::new(&registry_dir);
    registry.init_schema().unwrap();
    let registry_url = format!("file:{}", registry_dir.display());
    write_registry_env(&sender, &registry_url).unwrap();
    write_registry_env(&receiver, &registry_url).unwrap();

    let fleet = fleet_id(&sender).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        registry
            .upsert_self(gateway_row(&sender, &fleet, "0.3.0"))
            .await
            .unwrap();
        registry
            .upsert_self(gateway_row(&receiver, &fleet, receiver_version))
            .await
            .unwrap();
    });

    MeshFixture {
        _tmp: tmp,
        sender,
        receiver,
        registry,
        sender_gw,
        receiver_gw,
        work,
    }
}

fn write_valid_brief(work: &Path, name: &str) -> PathBuf {
    common::write_brief(
        work,
        &format!(
            "---\nlane: impl\nacceptance_criteria:\n  - focused test passes\nverify:\n  - cargo test --test {name}\n---\n# Do the task\n"
        ),
    )
}

fn work_row(registry: &FileRegistry, fleet: &str, work_id: &str) -> WorkRow {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(registry.work_get(fleet, work_id))
        .unwrap()
        .unwrap()
}

fn work_rows_path(fixture: &MeshFixture) -> PathBuf {
    fixture
        ._tmp
        .path()
        .join("registry")
        .join("mesh_work_items.json")
}

fn spooled_notes(home: &A2aHome, gateway: &str) -> Vec<SpooledNote> {
    let dir = home.root().join("mesh").join("outbound").join(gateway);
    let mut out = fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .map(|entry| notes_wire::read_spooled(&entry.path()).unwrap())
        .collect::<Vec<_>>();
    out.sort_by(|a, b| a.trace_id.cmp(&b.trace_id));
    out
}

fn deliver_to_receiver(fixture: &MeshFixture) {
    let cfg = a2a::config::load(&fixture.sender).unwrap().0;
    let mut rng = StdRng::seed_from_u64(7);
    let report = deliver_spooled_once(
        &cfg,
        &fixture.sender,
        NoteDeliveryTarget::Gateway {
            to_gateway: &fixture.receiver_gw,
            home: Some(&fixture.receiver),
        },
        Utc::now(),
        &mut rng,
    )
    .unwrap();
    assert_eq!(report.delivered, 1);
}

#[test]
fn send_preflights_peer_version_validates_brief_inserts_row_spools_note_and_posts_milestone() {
    let too_old = mesh_fixture("0.2.9");
    let brief = write_valid_brief(&too_old.work, "mesh_work_cli");
    let rejected = cmd(too_old.sender.root(), &too_old.work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &too_old.receiver_gw,
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Implement CLI",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .failure();
    let out = json_out(&rejected);
    assert_eq!(out["error"]["code"], "WORK_PEER_TOO_OLD", "{out}");
    assert!(out["error"]["remedy"]
        .as_str()
        .unwrap()
        .contains("upgrade and restart their gateway"));

    let fixture = mesh_fixture("0.3.0");
    let invalid = common::write_brief(&fixture.work, "missing impl acceptance");
    let invalid_out = cmd(fixture.sender.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &fixture.receiver_gw,
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Invalid brief",
            "--brief",
            invalid.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .failure();
    let invalid_json = json_out(&invalid_out);
    assert_eq!(invalid_json["error"]["code"], "WORK_BRIEF_INVALID", "{invalid_json}");
    assert!(invalid_json["error"]["remedy"].is_string(), "{invalid_json}");

    let brief = write_valid_brief(&fixture.work, "mesh_work_cli");
    let sent = cmd(fixture.sender.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &fixture.receiver_gw,
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Implement CLI",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .success();
    let out = json_out(&sent);
    assert_eq!(out["ok"], true, "{out}");
    assert_eq!(out["command"], "mesh work send");
    let work_id = out["data"]["work_id"].as_str().unwrap();
    assert_eq!(out["data"]["to"], fixture.receiver_gw);

    let row = work_row(&fixture.registry, &fleet_id(&fixture.sender).unwrap(), work_id);
    assert_eq!(row.status, WorkStatus::Offered);
    assert_eq!(row.sender_gw, fixture.sender_gw);
    assert_eq!(row.addressing, format!("directed:{}", fixture.receiver_gw));

    let notes = spooled_notes(&fixture.sender, &fixture.receiver_gw);
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].note.kind, work_store::WORK_OFFER_KIND);
    assert_eq!(notes[0].note.to, format!("gateway:{}", fixture.receiver_gw));
    assert_eq!(notes[0].note.refs["work"]["work_id"], work_id);

    let feed = cmd(fixture.sender.root(), &fixture.work)
        .args(["--json", "mesh", "feed"])
        .assert()
        .success();
    let feed = json_out(&feed);
    assert!(feed["data"].as_array().unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .contains("work sent: Implement CLI"));
}

#[test]
fn send_refuses_an_invalid_peer_row_before_the_peer_version_check() {
    let fixture = mesh_fixture("0.2.9");
    let mut invalid = gateway_row(
        &fixture.receiver,
        &fleet_id(&fixture.receiver).unwrap(),
        "0.2.9",
    );
    invalid.row_token = Some("partial-signature-shape".to_string());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(fixture.registry.upsert_self(invalid))
        .unwrap();
    let brief = write_valid_brief(&fixture.work, "mesh_work_cli");

    let rejected = cmd(fixture.sender.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &fixture.receiver_gw,
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Capture invalid row",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .failure();
    let out = json_out(&rejected);
    assert_eq!(out["error"]["code"], "WORK_PEER_ROW_INVALID", "{out}");
    let remedy = out["error"]["remedy"].as_str().unwrap();
    assert!(remedy.contains("capture the row for evidence first"), "{out}");
    assert!(remedy.contains("do NOT ask the peer to restart or republish"), "{out}");
    assert_ne!(out["error"]["code"], "WORK_PEER_TOO_OLD", "{out}");
}

#[test]
fn offer_uses_pool_feed_cap_and_writes_registry_row() {
    let fixture = mesh_fixture("0.3.0");
    let big_brief = common::write_brief(
        &fixture.work,
        &format!(
            "---\nlane: impl\nacceptance_criteria:\n  - focused test passes\nverify:\n  - cargo test --test mesh_work_cli\n---\n{}",
            "x".repeat(40 * 1024)
        ),
    );
    let rejected = cmd(fixture.sender.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "offer",
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Huge pool work",
            "--brief",
            big_brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .failure();
    let out = json_out(&rejected);
    assert_eq!(out["error"]["code"], "WORK_TOO_LARGE", "{out}");
    assert!(out["error"]["remedy"].is_string(), "{out}");

    let brief = write_valid_brief(&fixture.work, "mesh_work_cli");
    let offered = cmd(fixture.sender.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "offer",
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Pool CLI",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .success();
    let out = json_out(&offered);
    assert_eq!(out["command"], "mesh work offer");
    let work_id = out["data"]["work_id"].as_str().unwrap();
    assert_eq!(out["data"]["addressing"], "pool");

    let row = work_row(&fixture.registry, &fleet_id(&fixture.sender).unwrap(), work_id);
    assert_eq!(row.status, WorkStatus::Offered);
    assert_eq!(row.addressing, "pool");

    let feed = cmd(fixture.sender.root(), &fixture.work)
        .args(["--json", "mesh", "feed"])
        .assert()
        .success();
    let feed = json_out(&feed);
    let entry = &feed["data"].as_array().unwrap()[0];
    assert_eq!(entry["refs"]["work"]["work_id"], work_id);
    assert!(entry["text"].as_str().unwrap().contains("work offered: Pool CLI"));
}

#[test]
fn list_merges_local_store_with_registry_overlay_and_degrades_to_store_only_warning() {
    let fixture = mesh_fixture("0.3.0");
    let brief = write_valid_brief(&fixture.work, "mesh_work_cli");
    let sent = cmd(fixture.sender.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &fixture.receiver_gw,
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "List Me",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .success();
    let work_id = json_out(&sent)["data"]["work_id"].as_str().unwrap().to_string();
    deliver_to_receiver(&fixture);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        fixture
            .registry
            .work_claim(&fleet_id(&fixture.receiver).unwrap(), &work_id, &fixture.receiver_gw)
            .await
            .unwrap();
    });

    let listed = cmd(fixture.receiver.root(), &fixture.work)
        .args(["--json", "mesh", "work", "list"])
        .assert()
        .success();
    let out = json_out(&listed);
    assert_eq!(out["data"]["registry_available"], true, "{out}");
    let rows = out["data"]["items"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{out}");
    assert_eq!(rows[0]["work_id"], work_id);
    assert_eq!(rows[0]["source"], "directed");
    assert_eq!(rows[0]["status"], "approved");
    assert_eq!(rows[0]["claimed_by"], fixture.receiver_gw);

    fs::write(work_rows_path(&fixture), b"not json").unwrap();
    let degraded = cmd(fixture.receiver.root(), &fixture.work)
        .args(["--json", "mesh", "work", "list"])
        .assert()
        .success()
        .stderr(predicate::str::contains("warning: registry unavailable"));
    let out = json_out(&degraded);
    assert_eq!(out["data"]["registry_available"], false, "{out}");
    assert_eq!(out["data"]["items"].as_array().unwrap()[0]["status"], "local");
}

#[test]
fn decline_cancel_ack_status_and_show_route_notes_and_sanitize_display_strings() {
    let fixture = mesh_fixture("0.3.0");
    let brief = write_valid_brief(&fixture.work, "mesh_work_cli");
    let sent = cmd(fixture.sender.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &fixture.receiver_gw,
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Decline Me",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .success();
    let decline_id = json_out(&sent)["data"]["work_id"].as_str().unwrap().to_string();
    deliver_to_receiver(&fixture);

    let stored_path = fixture
        .receiver
        .root()
        .join("work")
        .join("inbox")
        .join(format!("{decline_id}.json"));
    let mut stored: Value = serde_json::from_slice(&fs::read(&stored_path).unwrap()).unwrap();
    stored["verified"]["envelope"]["title"] = json!("Decline\u{1b} Me");
    fs::write(&stored_path, serde_json::to_vec_pretty(&stored).unwrap()).unwrap();

    let shown = cmd(fixture.receiver.root(), &fixture.work)
        .args(["--json", "mesh", "work", "show", &decline_id])
        .assert()
        .success();
    let shown = json_out(&shown);
    assert_eq!(shown["data"]["title"], "Decline Me", "{shown}");
    assert!(!String::from_utf8_lossy(&shown.to_string().into_bytes()).contains('\u{1b}'));

    let declined = cmd(fixture.receiver.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "decline",
            &decline_id,
            "--reason",
            "No capacity",
        ])
        .assert()
        .success();
    let declined = json_out(&declined);
    assert_eq!(declined["data"]["status"], "declined", "{declined}");
    let row = work_row(
        &fixture.registry,
        &fleet_id(&fixture.receiver).unwrap(),
        &decline_id,
    );
    assert_eq!(row.status, WorkStatus::Declined);
    let notes = spooled_notes(&fixture.receiver, &fixture.sender_gw);
    assert_eq!(notes[0].note.kind, work_store::WORK_RESULT_KIND);
    assert_eq!(notes[0].note.refs["result"]["status"], "declined");
    assert_eq!(notes[0].note.refs["result"]["summary"], "No capacity");

    let second_brief = write_valid_brief(&fixture.work, "mesh_work_cli");
    let sent = cmd(fixture.sender.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &fixture.receiver_gw,
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Cancel Me",
            "--brief",
            second_brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .success();
    let cancel_id = json_out(&sent)["data"]["work_id"].as_str().unwrap().to_string();
    let cancelled = cmd(fixture.sender.root(), &fixture.work)
        .args(["--json", "mesh", "work", "cancel", &cancel_id])
        .assert()
        .success();
    let cancelled = json_out(&cancelled);
    assert_eq!(cancelled["data"]["status"], "cancelled", "{cancelled}");
    let row = work_row(&fixture.registry, &fleet_id(&fixture.sender).unwrap(), &cancel_id);
    assert_eq!(row.status, WorkStatus::Cancelled);
    let notes = spooled_notes(&fixture.sender, &fixture.receiver_gw);
    assert!(notes.iter().any(|note| note.note.kind == work_store::WORK_CANCELLED_KIND));

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut done = row;
    done.work_id = "done-work".to_string();
    done.status = WorkStatus::Done;
    done.result_ref = Some("a2a/work/done-work".to_string());
    rt.block_on(fixture.registry.work_insert(done)).unwrap();

    let status = cmd(fixture.sender.root(), &fixture.work)
        .args(["--json", "mesh", "work", "status", "done-work"])
        .assert()
        .success();
    let status = json_out(&status);
    assert_eq!(status["data"]["items"][0]["status"], "done", "{status}");

    let acked = cmd(fixture.sender.root(), &fixture.work)
        .args(["--json", "mesh", "work", "ack", "done-work"])
        .assert()
        .success();
    let acked = json_out(&acked);
    assert_eq!(acked["data"]["status"], "acked", "{acked}");
    assert!(fixture
        .sender
        .root()
        .join("work")
        .join("acked")
        .join("done-work.json")
        .exists());
    let row = work_row(&fixture.registry, &fleet_id(&fixture.sender).unwrap(), "done-work");
    assert_eq!(row.status, WorkStatus::Acked);
}
