mod common;

use a2a::commands::owner;
use a2a::home::A2aHome;
use a2a::mesh::gateway::notes_wire::{
    self, deliver_spooled_once, NoteDeliveryTarget, SpooledNote,
};
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_id, fleet_init, gateway_identity, gateway_public_key_b64,
    gateway_public_key_fingerprint, install_gateway_token, SystemClock,
};
use a2a::mesh::registry::{
    write_registry_env, FileRegistry, GatewayRow, Registry, WorkOwner, WorkPatch, WorkRow,
    WorkStatus, REGISTRY_SCHEMA_VERSION,
};
use a2a::mesh::work_exec::{self, WorkPhase};
use a2a::mesh::work_gate;
use a2a::mesh::work_store;
use a2a::mesh::work::{
    sign_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkRepo,
};
use a2a::mesh::Note;
use assert_cmd::Command;
use chrono::{SecondsFormat, TimeZone, Utc};
use nix::fcntl::{Flock, FlockArg};
use predicates::prelude::*;
use rand::{rngs::StdRng, SeedableRng};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

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
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = entries
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

fn held_item(fixture: &MeshFixture, title: &str) -> String {
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
            title,
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .success();
    let work_id = json_out(&sent)["data"]["work_id"]
        .as_str()
        .unwrap()
        .to_string();
    deliver_to_receiver(fixture);

    let fleet = fleet_id(&fixture.receiver).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        fixture
            .registry
            .work_claim(&fleet, &work_id, &fixture.receiver_gw)
            .await
            .unwrap();
        fixture
            .registry
            .work_transition(
                &fleet,
                &work_id,
                WorkStatus::Approved,
                WorkStatus::Running,
                WorkOwner::Claimer(fixture.receiver_gw.clone()),
                WorkPatch::default(),
            )
            .await
            .unwrap();
        fixture
            .registry
            .work_transition(
                &fleet,
                &work_id,
                WorkStatus::Running,
                WorkStatus::AwaitingRelease,
                WorkOwner::Claimer(fixture.receiver_gw.clone()),
                WorkPatch::default(),
            )
            .await
            .unwrap();
    });
    let mut state = work_exec::initial_claiming_state(&fixture.receiver, &work_id).unwrap();
    state.phase = WorkPhase::AwaitingRelease;
    state.worker_id = Some("impl-held".to_string());
    state.result_status = Some(WorkStatus::Done);
    state.held_since = Some(Utc::now());
    state.hold_note_id = Some(ulid::Ulid::new().to_string());
    state.hold_hit_count = Some(0);
    work_exec::save_state(&fixture.receiver, &state).unwrap();
    work_id
}

fn held_git_item(fixture: &MeshFixture, title: &str, result: &str) -> (String, PathBuf) {
    let work_id = held_item(fixture, title);
    let state = work_exec::load_state(&fixture.receiver, &work_id)
        .unwrap()
        .unwrap();
    let repo = state.checkout.clone();
    fs::create_dir_all(&repo).unwrap();
    run_git(&repo, &["init", "-b", "main"]);
    run_git(&repo, &["config", "user.email", "test@example.invalid"]);
    run_git(&repo, &["config", "user.name", "Test User"]);
    run_git(&repo, &["config", "commit.gpgsign", "false"]);
    fs::write(repo.join("README.md"), "base\n").unwrap();
    run_git(&repo, &["add", "--", "README.md"]);
    run_git(&repo, &["commit", "-m", "base"]);
    let base_sha = git_stdout(&repo, &["rev-parse", "HEAD"]);
    fs::write(repo.join("result.txt"), result).unwrap();
    run_git(&repo, &["add", "--", "result.txt"]);
    run_git(&repo, &["commit", "-m", "worker result"]);
    let range = work_gate::freeze_range(&repo, &base_sha).unwrap();

    let bare = fixture
        ._tmp
        .path()
        .join(format!("{work_id}-remote.git"));
    run_git(
        fixture._tmp.path(),
        &["init", "--bare", bare.to_str().unwrap()],
    );
    let mut state = state;
    state.base_sha = Some(base_sha);
    state.held_commit_sha = Some(range.head_sha);
    state.closure_digest = Some(range.closure_digest);
    state.base_ref = Some("main".to_string());
    state.remote_url = Some(bare.to_string_lossy().into_owned());
    state.result_summary = Some("completed".to_string());
    state.evidence_excerpt = Some("focused verification passed".to_string());
    work_exec::save_state(&fixture.receiver, &state).unwrap();
    (work_id, bare)
}

fn git_stdout(cwd: &Path, args: &[&str]) -> String {
    let output = ProcessCommand::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn run_git(cwd: &Path, args: &[&str]) {
    let output = ProcessCommand::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
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
    let done_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    done.work_id = done_id.to_string();
    done.status = WorkStatus::Done;
    done.result_ref = Some(format!("a2a/work/{done_id}"));
    rt.block_on(fixture.registry.work_insert(done)).unwrap();

    let status = cmd(fixture.sender.root(), &fixture.work)
        .args(["--json", "mesh", "work", "status", done_id])
        .assert()
        .success();
    let status = json_out(&status);
    assert_eq!(status["data"]["items"][0]["status"], "done", "{status}");

    let acked = cmd(fixture.sender.root(), &fixture.work)
        .args(["--json", "mesh", "work", "ack", done_id])
        .assert()
        .success();
    let acked = json_out(&acked);
    assert_eq!(acked["data"]["status"], "acked", "{acked}");
    assert!(fixture
        .sender
        .root()
        .join("work")
        .join("acked")
        .join(format!("{done_id}.json"))
        .exists());
    let row = work_row(&fixture.registry, &fleet_id(&fixture.sender).unwrap(), done_id);
    assert_eq!(row.status, WorkStatus::Acked);
}

#[test]
fn reject_reason_stays_local_unless_notify_sender_is_passed() {
    let fixture = mesh_fixture("0.3.0");
    let work_id = held_item(&fixture, "Reject Me");
    let reason = "found creds in build.rs";
    let rejected = cmd(fixture.receiver.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "reject",
            &work_id,
            "--reason",
            reason,
        ])
        .assert()
        .success();
    let out = json_out(&rejected);
    assert_eq!(out["data"]["status"], "rejected", "{out}");
    assert_eq!(out["data"]["result_ref"], Value::Null, "{out}");

    let row = work_row(
        &fixture.registry,
        &fleet_id(&fixture.receiver).unwrap(),
        &work_id,
    );
    assert_eq!(row.status, WorkStatus::Rejected);
    assert_eq!(row.result_ref, None);
    let state = work_exec::load_state(&fixture.receiver, &work_id)
        .unwrap()
        .unwrap();
    assert_eq!(state.phase, WorkPhase::Rejected);
    let notes = spooled_notes(&fixture.receiver, &fixture.sender_gw);
    assert!(
        notes.iter().all(|note| !serde_json::to_string(note).unwrap().contains("build.rs")),
        "plain reject must not spool its reason: {notes:?}"
    );
    assert!(
        notes
            .iter()
            .all(|note| note.note.kind != work_store::WORK_RESULT_KIND),
        "plain reject must not spool a result note: {notes:?}"
    );

    let notified = cmd(fixture.receiver.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "reject",
            &work_id,
            "--reason",
            reason,
            "--notify-sender",
        ])
        .assert()
        .success();
    assert_eq!(json_out(&notified)["data"]["summary"], "rejected by the receiver");
    let notes = spooled_notes(&fixture.receiver, &fixture.sender_gw);
    let result = notes
        .iter()
        .find(|note| note.note.kind == work_store::WORK_RESULT_KIND)
        .unwrap();
    assert_eq!(result.note.refs["result"]["status"], "rejected");
    assert_eq!(result.note.refs["result"]["result_ref"], Value::Null);
    assert_eq!(result.note.refs["result"]["summary"], reason);
    assert!(!result.note.text.contains("a2a/work/"));

    cmd(fixture.receiver.root(), &fixture.work)
        .args(["mesh", "work", "reject", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Send the reason to the sender; do not describe what you found",
        ));
}

#[test]
fn allow_secrets_is_refused_without_confirmation() {
    let fixture = mesh_fixture("0.3.0");
    let work_id = held_item(&fixture, "Needs Secret Confirmation");
    let refused = cmd(fixture.receiver.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "release",
            &work_id,
            "--allow-secrets",
        ])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "WORK_ALLOW_SECRETS_REQUIRED", "{out}");
}

#[test]
fn approve_dry_run_prints_the_resolved_sandbox_posture() {
    let fixture = mesh_fixture("0.3.0");
    let origin = fixture._tmp.path().join("approve-origin");
    fs::create_dir_all(&origin).unwrap();
    run_git(&origin, &["init", "-b", "main"]);
    run_git(&origin, &["config", "user.email", "test@example.invalid"]);
    run_git(&origin, &["config", "user.name", "Test User"]);
    fs::write(origin.join("README.md"), "base\n").unwrap();
    run_git(&origin, &["add", "--", "README.md"]);
    run_git(&origin, &["commit", "-m", "base"]);
    let bare = fixture._tmp.path().join("approve-origin.git");
    run_git(
        fixture._tmp.path(),
        &[
            "clone",
            "--bare",
            origin.to_str().unwrap(),
            bare.to_str().unwrap(),
        ],
    );

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
            &format!("file://{}", bare.display()),
            "--base-ref",
            "main",
            "--title",
            "Dry-run posture",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .success();
    let work_id = json_out(&sent)["data"]["work_id"]
        .as_str()
        .unwrap()
        .to_string();
    deliver_to_receiver(&fixture);
    a2a::mesh::gateway::presence_wire::refresh_registry_cache(
        &fixture.receiver,
        &[(
            gateway_row(
                &fixture.sender,
                &fleet_id(&fixture.receiver).unwrap(),
                "0.3.0",
            ),
            a2a::mesh::row_sig::RowVerdict::Unsigned { stripped: false },
        )],
        Utc::now(),
    )
    .unwrap();

    let lock_path = fixture
        .receiver
        .root()
        .join("mesh")
        .join(a2a::mesh::gateway::GATEWAY_LOCK);
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
        .unwrap();
    let _gateway_lock = Flock::lock(lock_file, FlockArg::LockExclusive).unwrap();

    let json = cmd(fixture.receiver.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "approve",
            &work_id,
            "--dry-run",
        ])
        .assert()
        .success();
    let json = json_out(&json);
    assert_eq!(json["data"]["sandbox"], "workspace-write", "{json}");
    assert_eq!(json["data"]["network"], false, "{json}");

    let human = cmd(fixture.receiver.root(), &fixture.work)
        .args(["mesh", "work", "approve", &work_id, "--dry-run"])
        .assert()
        .success();
    let stdout = String::from_utf8(human.get_output().stdout.clone()).unwrap();
    assert!(
        stdout.starts_with("sandbox: workspace-write, network: false\n"),
        "{stdout}"
    );
    assert!(stdout.contains("# Do the task"), "{stdout}");
}

#[test]
fn allow_secrets_token_is_single_use_and_digest_bound() {
    let fixture = mesh_fixture("0.3.0");
    let (work_id, bare) = held_git_item(
        &fixture,
        "Release Secret Result",
        "postgresql://a2a:hunter2@h/db\n",
    );
    let cfg = a2a::config::load(&fixture.receiver).unwrap().0;
    let state = work_exec::load_state(&fixture.receiver, &work_id)
        .unwrap()
        .unwrap();
    let range = work_gate::freeze_range(
        &state.checkout,
        state.base_sha.as_deref().unwrap(),
    )
    .unwrap();
    let report = work_gate::scan_review_unit(
        &fixture.receiver,
        &cfg,
        &state.checkout,
        &range,
        &state,
    )
    .unwrap();
    assert!(!report.hits.is_empty());
    let token = work_gate::AllowSecretsToken::mint(&state, &report.digest());
    assert_eq!(token.len(), 32);
    assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
    let token_path = fixture
        .receiver
        .work_dir()
        .join(&work_id)
        .join("allow-secrets.json");
    assert_eq!(
        fs::metadata(&token_path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let principal = owner::caller();
    let mut release = cmd(fixture.receiver.root(), &fixture.work);
    let released = release
        .env("A2A_OWNER", &principal)
        .args([
            "--json",
            "mesh",
            "work",
            "release",
            &work_id,
            "--allow-secrets",
            "--confirm-token",
            &token,
        ])
        .assert()
        .success();
    let released = json_out(&released);
    assert_eq!(released["data"]["phase"], "reported", "{released}");
    assert!(!token_path.exists(), "a successful token is consumed");
    let pushed = ProcessCommand::new("git")
        .args([
            "--git-dir",
            bare.to_str().unwrap(),
            "show-ref",
            &format!("refs/heads/a2a/work/{work_id}"),
        ])
        .output()
        .unwrap();
    assert!(pushed.status.success(), "secret-bearing result was not pushed");

    let mut replay = cmd(fixture.receiver.root(), &fixture.work);
    let replayed = replay
        .env("A2A_OWNER", &principal)
        .args([
            "--json",
            "mesh",
            "work",
            "release",
            &work_id,
            "--allow-secrets",
            "--confirm-token",
            &token,
        ])
        .assert()
        .failure();
    let replayed = json_out(&replayed);
    assert_eq!(
        replayed["error"]["code"],
        "WORK_ALLOW_SECRETS_REQUIRED",
        "{replayed}"
    );

    let (stale_id, _) = held_git_item(
        &fixture,
        "Stale Secret Result",
        "postgresql://a2a:first@h/db\n",
    );
    let stale_state = work_exec::load_state(&fixture.receiver, &stale_id)
        .unwrap()
        .unwrap();
    let stale_range = work_gate::freeze_range(
        &stale_state.checkout,
        stale_state.base_sha.as_deref().unwrap(),
    )
    .unwrap();
    let stale_report = work_gate::scan_review_unit(
        &fixture.receiver,
        &cfg,
        &stale_state.checkout,
        &stale_range,
        &stale_state,
    )
    .unwrap();
    let stale_token = work_gate::AllowSecretsToken::mint(&stale_state, &stale_report.digest());
    fs::write(
        stale_state.checkout.join("new-secret.txt"),
        "redis://mesh:second@cache/db\n",
    )
    .unwrap();
    run_git(&stale_state.checkout, &["add", "--", "new-secret.txt"]);
    run_git(
        &stale_state.checkout,
        &["commit", "-m", "change the hit set"],
    );
    let mut stale_release = cmd(fixture.receiver.root(), &fixture.work);
    let stale = stale_release
        .env("A2A_OWNER", &principal)
        .args([
            "--json",
            "mesh",
            "work",
            "release",
            &stale_id,
            "--allow-secrets",
            "--confirm-token",
            &stale_token,
        ])
        .assert()
        .failure();
    let stale = json_out(&stale);
    assert_eq!(stale["error"]["code"], "WORK_ALLOW_SECRETS_STALE", "{stale}");

    let audit = fs::read_to_string(fixture.receiver.root().join("audit.jsonl")).unwrap();
    let entries = audit.lines().collect::<Vec<_>>();
    assert_eq!(entries.len(), 1, "{audit}");
    let entry: Value = serde_json::from_str(entries[0]).unwrap();
    assert_eq!(
        entry.as_object().unwrap().keys().map(String::as_str).collect::<BTreeSet<_>>(),
        BTreeSet::from(["event", "hit_set_digest", "principal", "work_id"])
    );
    assert_eq!(entry["event"], "work_release_allow_secrets");
    assert_eq!(entry["work_id"], work_id);
    assert_eq!(entry["hit_set_digest"], report.digest());
    assert_eq!(entry["principal"], principal);
}

#[test]
fn decline_is_refused_past_offered_with_a_remedy_naming_reject() {
    let fixture = mesh_fixture("0.3.0");
    let work_id = held_item(&fixture, "Past Decline");
    let refused = cmd(fixture.receiver.root(), &fixture.work)
        .args(["--json", "mesh", "work", "decline", &work_id])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "WORK_STATE_CONFLICT", "{out}");
    assert!(out["error"]["remedy"]
        .as_str()
        .unwrap()
        .contains("reject"));
}

#[test]
fn retry_push_refuses_a_held_item() {
    let fixture = mesh_fixture("0.3.0");
    let work_id = held_item(&fixture, "Do Not Retry");
    let refused = cmd(fixture.receiver.root(), &fixture.work)
        .args(["--json", "mesh", "work", "retry-push", &work_id])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "WORK_HELD", "{out}");
    let remedy = out["error"]["remedy"].as_str().unwrap();
    assert!(remedy.contains("release"), "{out}");
    assert!(remedy.contains("reject"), "{out}");
}

#[test]
fn non_ulid_work_id_is_refused_at_send_and_at_ingest() {
    let fixture = mesh_fixture("0.3.0");
    let brief = write_valid_brief(&fixture.work, "mesh_work_cli");
    let refused = cmd(fixture.sender.root(), &fixture.work)
        .args([
            "--json",
            "mesh",
            "work",
            "send",
            "--to",
            &fixture.receiver_gw,
            "--work-id",
            "../etc",
            "--repo",
            "https://github.com/example/project.git",
            "--base-ref",
            "main",
            "--title",
            "Invalid id",
            "--brief",
            brief.to_str().unwrap(),
            "--acceptance",
            "focused test passes",
        ])
        .assert()
        .failure();
    let out = json_out(&refused);
    assert_eq!(out["error"]["code"], "WORK_ID_INVALID", "{out}");

    let envelope = sign_envelope(
        &fixture.sender,
        UnsignedWorkEnvelope {
            work_id: "pool".to_string(),
            session: "user:test".to_string(),
            addressing: WorkAddressing::Directed(fixture.receiver_gw.clone()),
            repo: WorkRepo {
                remote_url: "https://github.com/example/project.git".to_string(),
                base_ref: "main".to_string(),
            },
            title: "Invalid ingress id".to_string(),
            brief_text: fs::read_to_string(&brief).unwrap(),
            acceptance: "focused test passes".to_string(),
            ttl_days: 7,
            max_bytes: 262_144,
        },
        &SystemClock,
    )
    .unwrap();
    let note_id = ulid::Ulid::new().to_string();
    let spooled = SpooledNote {
        schema_version: 1,
        trace_id: note_id.clone(),
        from_gateway: fixture.sender_gw.clone(),
        to_gateway: fixture.receiver_gw.clone(),
        to_peer: format!("gateway:{}", fixture.receiver_gw),
        attempts: 0,
        next_attempt_after: None,
        note: Note {
            note_id: Some(note_id),
            from: "user:mesh-work".to_string(),
            to: format!("gateway:{}", fixture.receiver_gw),
            ts: Utc::now(),
            kind: work_store::WORK_OFFER_KIND.to_string(),
            text: "work offer with invalid id".to_string(),
            refs: json!({ "work": envelope }),
            extra: serde_json::Map::new(),
        },
    };
    let cfg = a2a::config::load(&fixture.receiver).unwrap().0;
    let ack = notes_wire::receive_note(&fixture.receiver, &cfg, &spooled).unwrap();
    assert_eq!(
        ack,
        notes_wire::NoteAck::Rejected {
            code: "WORK_ID_INVALID".to_string()
        }
    );
}

#[test]
fn sender_status_renders_the_receiver_and_held_since_from_the_hold_note() {
    let fixture = mesh_fixture("0.3.0");
    let work_id = held_item(&fixture, "Visible Hold");
    let held_since = Utc.timestamp_opt(1_800_000_000, 123_456_000).unwrap();
    let held_text = held_since.to_rfc3339_opts(SecondsFormat::Micros, true);
    let cfg = a2a::config::load(&fixture.receiver).unwrap().0;
    notes_wire::spool_note(
        &fixture.receiver,
        &cfg,
        &fixture.receiver_gw,
        &fixture.sender_gw,
        Note {
            note_id: Some(ulid::Ulid::new().to_string()),
            from: "user:mesh-work".to_string(),
            to: "user:mesh-work".to_string(),
            ts: held_since,
            kind: work_store::WORK_HELD_KIND.to_string(),
            text: format!("work held: {work_id}"),
            refs: json!({
                "held": {
                    "work_id": work_id,
                    "status": "held",
                    "hit_count": 0,
                    "held_since": held_text,
                }
            }),
            extra: serde_json::Map::new(),
        },
    )
    .unwrap();
    let mut rng = StdRng::seed_from_u64(11);
    let delivered = deliver_spooled_once(
        &cfg,
        &fixture.receiver,
        NoteDeliveryTarget::Gateway {
            to_gateway: &fixture.sender_gw,
            home: Some(&fixture.sender),
        },
        Utc::now(),
        &mut rng,
    )
    .unwrap();
    assert_eq!(delivered.delivered, 1);

    let status = cmd(fixture.sender.root(), &fixture.work)
        .args(["--json", "mesh", "work", "status", &work_id])
        .assert()
        .success();
    let out = json_out(&status);
    assert_eq!(
        out["data"]["items"][0]["status"],
        format!(
            "running (held on {} since {})",
            fixture.receiver_gw, held_text
        ),
        "{out}"
    );
    assert_eq!(
        out["data"]["items"][0]["registry"]["status"],
        "awaiting_release"
    );
}
