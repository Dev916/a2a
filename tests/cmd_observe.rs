use a2a::home::A2aHome;
use a2a::observe::Snapshot;
use assert_cmd::Command;

const WORKER_ID: &str = "impl-0923t0833-observe";

fn seeded_home(root: &std::path::Path) -> A2aHome {
    let home = A2aHome::resolve(Some(root)).unwrap();
    let worker = home.worker_dir(WORKER_ID);
    std::fs::create_dir_all(worker.join("outbox")).unwrap();
    std::fs::write(
        worker.join("state.json"),
        br#"{"state":"done","lane":"impl","max_turns":8,"turns_this_epoch":2,"last_delivered_seq":0}"#,
    )
    .unwrap();
    home
}

#[test]
fn snapshot_prints_the_seeded_worker_as_snapshot_json() {
    let tmp = tempfile::tempdir().unwrap();
    let home = seeded_home(&tmp.path().join("home"));

    let output = Command::cargo_bin("a2a")
        .unwrap()
        .args(["observe", "snapshot", "--home"])
        .arg(home.root())
        .arg("--json")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let snapshot: Snapshot = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(snapshot.agent.worker_count, 1);
    assert_eq!(snapshot.workers.len(), 1);
    assert_eq!(snapshot.workers[0].worker_id, WORKER_ID);
    assert_eq!(snapshot.workers[0].state, "done");
    assert_eq!(snapshot.workers[0].lane, "impl");
}

#[test]
fn observe_help_lists_the_read_only_commands_and_flags() {
    let output = Command::cargo_bin("a2a")
        .unwrap()
        .args(["observe", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let help = String::from_utf8(output.stdout).unwrap();
    for command in ["serve", "tail", "snapshot"] {
        assert!(help.contains(command), "missing {command} in:\n{help}");
    }

    let cases = [
        (
            "serve",
            [
                "--home",
                "--bind",
                "--port",
                "--token",
                "--token-file",
                "--tls",
                "--planes",
            ]
            .as_slice(),
        ),
        ("tail", ["--home", "--from", "--planes"].as_slice()),
        ("snapshot", ["--home", "--json"].as_slice()),
    ];
    for (command, flags) in cases {
        let output = Command::cargo_bin("a2a")
            .unwrap()
            .args(["observe", command, "--help"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let help = String::from_utf8(output.stdout).unwrap();
        for flag in flags {
            assert!(help.contains(flag), "missing {flag} in:\n{help}");
        }
    }
}

#[test]
fn snapshot_refuses_a_missing_home_without_creating_it() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("missing-home");

    let output = Command::cargo_bin("a2a")
        .unwrap()
        .args(["observe", "snapshot", "--home"])
        .arg(&missing)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(9), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(&missing.display().to_string()), "{stderr}");
    assert!(!missing.exists(), "observe unexpectedly initialized the home");
}

#[test]
fn snapshot_refuses_an_uninitialized_home_without_modifying_it() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("empty-home");
    std::fs::create_dir(&home).unwrap();

    let output = Command::cargo_bin("a2a")
        .unwrap()
        .args(["observe", "snapshot", "--home"])
        .arg(&home)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(9), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(&home.display().to_string()), "{stderr}");
    assert_eq!(std::fs::read_dir(&home).unwrap().count(), 0);
}

#[test]
fn serve_surfaces_tls_unsupported_before_binding() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();

    let output = Command::cargo_bin("a2a")
        .unwrap()
        .args(["observe", "serve", "--home"])
        .arg(home.root())
        .arg("--tls")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(9), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("OBSERVE_TLS_UNSUPPORTED"), "{stderr}");
}
