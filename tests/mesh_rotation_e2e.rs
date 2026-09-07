use a2a::home::A2aHome;
use a2a::mesh::keys;
use a2a::mesh::registry::{write_registry_env, FileRegistry, Registry};
use a2a::mesh::rotation::RotationState;
use assert_cmd::Command;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

fn cmd(home: &Path) -> Command {
    let mut command = Command::cargo_bin("a2a").unwrap();
    command.arg("--home").arg(home).env_remove("A2A_HOME");
    command
}

fn json_out(assert: &assert_cmd::assert::Assert) -> Value {
    serde_json::from_slice(&assert.get_output().stdout).unwrap()
}

struct RotationFixture {
    owner: tempfile::TempDir,
    member: tempfile::TempDir,
    registry_path: PathBuf,
    gateway_id: String,
    pubkey_b64: String,
    old_token: String,
}

impl RotationFixture {
    fn new() -> Self {
        let owner = tempfile::tempdir().unwrap();
        let member = tempfile::tempdir().unwrap();
        cmd(owner.path())
            .args(["--json", "mesh", "fleet", "init"])
            .assert()
            .success();
        let identity = cmd(member.path())
            .args(["--json", "mesh", "gateway", "identity"])
            .assert()
            .success();
        let identity = json_out(&identity);
        let gateway_id = identity["data"]["gateway_id"]
            .as_str()
            .unwrap()
            .to_string();
        let pubkey_b64 = identity["data"]["pubkey_b64"]
            .as_str()
            .unwrap()
            .to_string();
        fs::create_dir_all(member.path().join("mesh/fleet")).unwrap();
        fs::copy(
            owner.path().join("mesh/fleet/root.pub"),
            member.path().join("mesh/fleet/root.pub"),
        )
        .unwrap();

        let enrolled = cmd(owner.path())
            .args([
                "--json",
                "mesh",
                "fleet",
                "enroll",
                "--gateway-id",
                &gateway_id,
                "--pubkey",
                &pubkey_b64,
            ])
            .assert()
            .success();
        let old_token = json_out(&enrolled)["data"]["token"]
            .as_str()
            .unwrap()
            .to_string();

        let registry_path = owner.path().join("registry");
        FileRegistry::new(&registry_path).init_schema().unwrap();
        let registry_url = format!("file:{}", registry_path.display());
        let owner_home = A2aHome::resolve(Some(owner.path())).unwrap();
        let member_home = A2aHome::resolve(Some(member.path())).unwrap();
        write_registry_env(&owner_home, &registry_url).unwrap();
        write_registry_env(&member_home, &registry_url).unwrap();
        install_token(member.path(), &old_token);
        resign(member.path());

        Self {
            owner,
            member,
            registry_path,
            gateway_id,
            pubkey_b64,
            old_token,
        }
    }

    fn stage_and_install(&self) -> Value {
        let announcement = self.owner.path().join("rotation-announcement.txt");
        let staged = cmd(self.owner.path())
            .args([
                "--json",
                "mesh",
                "fleet",
                "rotate-root",
                "--stage",
                "--out",
                announcement.to_str().unwrap(),
            ])
            .assert()
            .success();
        let staged = json_out(&staged);
        let root_before = fs::read(self.member.path().join("mesh/fleet/root.pub")).unwrap();
        let installed = cmd(self.member.path())
            .args([
                "--json",
                "mesh",
                "gateway",
                "install-root-next",
                "--file",
                announcement.to_str().unwrap(),
            ])
            .assert()
            .success();
        let installed = json_out(&installed);
        assert_eq!(
            installed["data"]["next_fpr"],
            staged["data"]["next_fingerprint"]
        );
        assert_eq!(
            fs::read(self.member.path().join("mesh/fleet/root.pub")).unwrap(),
            root_before,
            "install-root-next must never overwrite root.pub"
        );
        resign(self.member.path());
        staged
    }

    fn enroll_and_install_next(&self) -> String {
        let enrolled = cmd(self.owner.path())
            .args([
                "--json",
                "mesh",
                "fleet",
                "enroll",
                "--gateway-id",
                &self.gateway_id,
                "--pubkey",
                &self.pubkey_b64,
                "--sign-with",
                "next",
            ])
            .assert()
            .success();
        let token = json_out(&enrolled)["data"]["token"]
            .as_str()
            .unwrap()
            .to_string();
        install_token(self.member.path(), &token);
        token
    }

    fn status(&self) -> Value {
        let status = cmd(self.owner.path())
            .args(["--json", "mesh", "fleet", "rotation-status"])
            .assert()
            .success();
        json_out(&status)
    }

    fn promote(&self) -> Value {
        let promoted = cmd(self.owner.path())
            .args(["--json", "mesh", "fleet", "rotate-root", "--promote"])
            .assert()
            .success();
        json_out(&promoted)
    }

    fn mark_soak(&self, issuer_fpr: &str) {
        let home = A2aHome::resolve(Some(self.owner.path())).unwrap();
        let state = RotationState::load(&home).unwrap();
        assert_eq!(state.promoted_to_fpr.as_deref(), Some(issuer_fpr));
        let promoted_at = state.promoted_at.unwrap();
        let established_at = chrono::DateTime::from_timestamp(promoted_at + 1, 0).unwrap();
        let health = serde_json::json!({
            "schema_version": 2,
            "pid": 4242,
            "health_seq": 1,
            "written_at": chrono::Utc::now(),
            "state": "running",
            "listen_port": 7787,
            "expose": "none",
            "endpoint_url": null,
            "exposure_error": null,
            "exposure_error_since": null,
            "registry_error": null,
            "registry_error_since": null,
            "loops": {},
            "replication": {"per_origin_lag": {}, "last_anti_entropy": null},
            "links": [{
                "peer": self.gateway_id,
                "direction": "outbound",
                "established_at": established_at,
                "last_rx": established_at,
                "last_tx": established_at,
                "frames_rx": 1,
                "frames_tx": 1
            }],
            "escalation": null,
            "shutdown": null
        });
        fs::write(
            self.owner.path().join("mesh/gateway-health.json"),
            serde_json::to_vec_pretty(&health).unwrap(),
        )
        .unwrap();
    }

    fn retire_after_zero_second_soak(&self) -> Value {
        let retired = cmd(self.owner.path())
            .env("A2A_MESH_GATEWAY_ROTATION_SOAK_SECS", "0")
            .args(["--json", "mesh", "fleet", "rotate-root", "--retire"])
            .assert()
            .success();
        json_out(&retired)
    }
}

fn install_token(home: &Path, token: &str) {
    cmd(home)
        .args([
            "--json",
            "mesh",
            "gateway",
            "install-token",
            "--file",
            "-",
        ])
        .write_stdin(token)
        .assert()
        .success();
}

fn resign(home: &Path) {
    cmd(home)
        .args(["--json", "mesh", "gateway", "resign"])
        .assert()
        .success();
}

#[test]
fn a_full_stage_enroll_promote_retire_cycle_walks_the_phases() {
    let fixture = RotationFixture::new();
    let staged = fixture.stage_and_install();
    let phase_1 = fixture.status();
    assert_eq!(phase_1["data"]["phase"], "phase1", "{phase_1}");
    assert_eq!(
        phase_1["data"]["staged_fpr"],
        staged["data"]["next_fingerprint"]
    );

    fixture.enroll_and_install_next();
    let phase_2 = fixture.status();
    assert_eq!(phase_2["data"]["phase"], "phase2", "{phase_2}");
    let promoted = fixture.promote();
    let next_fpr = promoted["data"]["promoted_to_fpr"].as_str().unwrap();
    fixture.mark_soak(next_fpr);
    let soaked = cmd(fixture.owner.path())
        .env("A2A_MESH_GATEWAY_ROTATION_SOAK_SECS", "0")
        .args(["--json", "mesh", "fleet", "rotation-status"])
        .assert()
        .success();
    let soaked = json_out(&soaked);
    assert_eq!(soaked["data"]["phase"], "phase2", "{soaked}");
    assert_eq!(soaked["data"]["soak"]["state"], "satisfied", "{soaked}");

    let retired = fixture.retire_after_zero_second_soak();
    assert_eq!(retired["data"]["retired"], true, "{retired}");
    assert!(!fixture.owner.path().join("mesh/fleet/root.pub.prev").exists());
}

#[test]
fn abort_after_a_staged_enrollment_is_refused_then_forced() {
    let fixture = RotationFixture::new();
    fixture.stage_and_install();
    fixture.enroll_and_install_next();

    let refused = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--abort"])
        .assert()
        .failure();
    let refused = json_out(&refused);
    assert_eq!(
        refused["error"]["code"],
        "ROOT_ROTATION_ABORT_REFUSED",
        "{refused}"
    );
    assert!(refused["error"]["message"]
        .as_str()
        .unwrap()
        .contains(&fixture.gateway_id));

    let forced = cmd(fixture.owner.path())
        .args([
            "--json",
            "mesh",
            "fleet",
            "rotate-root",
            "--abort",
            "--force",
        ])
        .assert()
        .success();
    assert_eq!(json_out(&forced)["data"]["aborted"], true);
    assert!(!fixture.owner.path().join("mesh/fleet/root.pub.next").exists());
}

#[test]
fn promote_is_refused_at_phase_1() {
    let fixture = RotationFixture::new();
    fixture.stage_and_install();
    let phase_1 = fixture.status();
    assert_eq!(phase_1["data"]["phase"], "phase1", "{phase_1}");

    let refused = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--promote"])
        .assert()
        .failure();
    let refused = json_out(&refused);
    assert_eq!(refused["error"]["code"], "ROTATION_PHASE_UNMET", "{refused}");
    assert!(refused["error"]["message"]
        .as_str()
        .unwrap()
        .contains(&fixture.gateway_id));
}

#[test]
fn retire_is_refused_until_the_soak_predicate_holds() {
    let fixture = RotationFixture::new();
    fixture.stage_and_install();
    fixture.enroll_and_install_next();
    assert_eq!(fixture.status()["data"]["phase"], "phase2");
    let promoted = fixture.promote();

    let refused = cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--retire"])
        .assert()
        .failure();
    assert_eq!(
        json_out(&refused)["error"]["code"],
        "ROTATION_SOAK_UNMET"
    );
    assert!(fixture.owner.path().join("mesh/fleet/root.pub.prev").exists());

    fixture.mark_soak(promoted["data"]["promoted_to_fpr"].as_str().unwrap());
    let retired = fixture.retire_after_zero_second_soak();
    assert_eq!(retired["data"]["retired"], true, "{retired}");
}

#[test]
fn a_stranded_member_is_named_by_rotation_status() {
    let fixture = RotationFixture::new();
    fixture.stage_and_install();
    fixture.enroll_and_install_next();
    let promoted = fixture.promote();
    fixture.mark_soak(promoted["data"]["promoted_to_fpr"].as_str().unwrap());
    fixture.retire_after_zero_second_soak();

    install_token(fixture.member.path(), &fixture.old_token);
    let registry = FileRegistry::new(&fixture.registry_path);
    let owner_home = A2aHome::resolve(Some(fixture.owner.path())).unwrap();
    let fleet_id = keys::fleet_id(&owner_home).unwrap();
    let rows = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(registry.discover_unverified(&fleet_id))
        .unwrap();
    let row = rows
        .iter()
        .find(|row| row.gateway_id == fixture.gateway_id)
        .unwrap();
    assert_eq!(row.row_token.as_deref(), Some(fixture.old_token.as_str()));

    cmd(fixture.owner.path())
        .args(["--json", "mesh", "fleet", "rotate-root", "--stage"])
        .assert()
        .success();
    let status = fixture.status();
    let member = status["data"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|member| member["gateway_id"] == fixture.gateway_id)
        .unwrap();
    assert!(member["verified_issuer_fpr"].is_null(), "{status}");
    assert_eq!(
        status["data"]["unaccounted"],
        serde_json::json!([fixture.gateway_id])
    );
    assert_eq!(status["data"]["phase"], "unknown", "{status}");
}
