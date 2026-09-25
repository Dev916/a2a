use a2a::home::A2aHome;
use a2a::mesh::directive::Directive;
use a2a::mesh::gateway::notes_wire;
use a2a::mesh::keys;
use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::path::Path;

fn cli(home: &Path) -> Command {
    let mut command = Command::cargo_bin("a2a").unwrap();
    command
        .env("A2A_HOME", home)
        .env("A2A_BROKER_DISABLE", "1")
        .env_remove("A2A_JSON")
        .env_remove("CLAUDE_SESSION_ID");
    command
}

fn enrolled_home(path: &Path) -> (A2aHome, String) {
    let home = A2aHome::resolve(Some(path)).unwrap();
    keys::fleet_init(&home).unwrap();
    let identity = keys::gateway_identity(&home, false).unwrap();
    keys::ensure_and_verify_gateway_enrollment(&home, &keys::SystemClock).unwrap();
    (home, identity.gateway_id)
}

#[test]
fn command_help_lists_every_subcommand() {
    let tmp = tempfile::tempdir().unwrap();
    let output = cli(tmp.path())
        .args(["mesh", "command", "--help"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let help = String::from_utf8(output).unwrap();

    for subcommand in ["grant", "revoke", "list", "send", "log"] {
        assert!(
            help.contains(subcommand),
            "mesh command help omitted {subcommand}: {help}"
        );
    }
}

#[test]
fn grant_list_and_revoke_manage_receiver_consent() {
    let tmp = tempfile::tempdir().unwrap();

    cli(tmp.path())
        .args(["mesh", "command", "grant", "gw-peer-b"])
        .assert()
        .success();
    cli(tmp.path())
        .args(["mesh", "command", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("gw-peer-b"));

    cli(tmp.path())
        .args(["mesh", "command", "revoke", "gw-peer-b"])
        .assert()
        .success();
    cli(tmp.path())
        .args(["mesh", "command", "list"])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
}

#[test]
fn send_reads_the_instruction_file_and_spools_a_signed_directive() {
    let tmp = tempfile::tempdir().unwrap();
    let (home, gateway_id) = enrolled_home(tmp.path());
    let instruction_path = tmp.path().join("instruction.txt");
    fs::write(&instruction_path, "inspect the failing build").unwrap();

    cli(tmp.path())
        .args(["mesh", "command", "send", "--to", "gw-target-b"])
        .arg("--instruction-file")
        .arg(&instruction_path)
        .assert()
        .success();

    let outbound = home.root().join("mesh/outbound/gw-target-b");
    let mut paths = fs::read_dir(&outbound)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    paths.sort();
    assert_eq!(paths.len(), 1, "one directive should be pending in the spool");

    let spooled = notes_wire::read_spooled(&paths[0]).unwrap();
    let directive: Directive =
        serde_json::from_value(spooled.note.refs["directive"].clone()).unwrap();
    assert_eq!(spooled.note.kind, "directive");
    assert_eq!(directive.from, gateway_id);
    assert_eq!(directive.target, "gw-target-b");
    assert_eq!(directive.instruction, "inspect the failing build");
    assert!(!directive.sig.is_empty(), "the routed directive must be signed");
}

#[test]
fn send_requires_exactly_one_instruction_source() {
    let tmp = tempfile::tempdir().unwrap();
    let instruction_path = tmp.path().join("instruction.txt");
    fs::write(&instruction_path, "from file").unwrap();

    cli(tmp.path())
        .args(["mesh", "command", "send", "--to", "gw-target-b"])
        .assert()
        .failure()
        .code(2);

    cli(tmp.path())
        .args([
            "mesh",
            "command",
            "send",
            "--to",
            "gw-target-b",
            "--instruction",
            "from argv",
        ])
        .arg("--instruction-file")
        .arg(&instruction_path)
        .assert()
        .failure()
        .code(2);
}

#[test]
fn send_from_an_unenrolled_home_is_a_clear_precondition_error() {
    let tmp = tempfile::tempdir().unwrap();
    let instruction_path = tmp.path().join("instruction.txt");
    fs::write(&instruction_path, "inspect the failing build").unwrap();

    cli(tmp.path())
        .args(["mesh", "command", "send", "--to", "gw-target-b"])
        .arg("--instruction-file")
        .arg(&instruction_path)
        .assert()
        .failure()
        .code(9)
        .stderr(predicate::str::contains("GATEWAY_NOT_ENROLLED"))
        .stderr(predicate::str::contains("panicked").not());
}

#[test]
fn log_prints_the_directive_audit_log() {
    let tmp = tempfile::tempdir().unwrap();
    let mesh = tmp.path().join("mesh");
    fs::create_dir_all(&mesh).unwrap();
    fs::write(
        mesh.join("command-audit.jsonl"),
        concat!(
            r#"{"ts":"2026-09-25T12:00:00Z","directive_id":"01KTESTDIRECTIVE","from":"gw-peer-b","verdict":"imperative"}"#,
            "\n"
        ),
    )
    .unwrap();

    cli(tmp.path())
        .args(["mesh", "command", "log"])
        .assert()
        .success()
        .stdout(predicate::str::contains("01KTESTDIRECTIVE"))
        .stdout(predicate::str::contains("\"verdict\":\"imperative\""));
}
