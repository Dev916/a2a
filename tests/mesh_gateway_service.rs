mod common;

use predicates::prelude::*;
use serde_json::Value;
use std::path::Path;

#[cfg(target_os = "macos")]
#[test]
fn install_service_writes_launch_agent_with_home_and_skip_report() {
    let tmp = tempfile::tempdir().unwrap();
    let fake_home = tmp.path().join("fake-home");
    let a2a_home = tmp.path().join("a2a-home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&fake_home).unwrap();
    std::fs::create_dir_all(&a2a_home).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(
        a2a_home.join("config.toml"),
        "[mesh.gateway]\nexpose = \"ngrok\"\n",
    )
    .unwrap();

    let out = install_cmd(&a2a_home, &work, &fake_home)
        .assert()
        .success()
        .stderr(predicate::str::contains("ngrok start --none"))
        .get_output()
        .stdout
        .clone();
    let value: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(value["data"]["installed"], true);
    assert_eq!(value["data"]["reload_skipped"], true);
    assert_eq!(
        value["data"]["status_command"],
        "launchctl print gui/$UID/dev.a2a.mesh-gateway"
    );

    let plist = fake_home
        .join("Library")
        .join("LaunchAgents")
        .join("dev.a2a.mesh-gateway.plist");
    assert_eq!(
        value["data"]["path"].as_str(),
        Some(plist.to_string_lossy().as_ref())
    );
    let text = std::fs::read_to_string(&plist).unwrap();
    let exe = assert_cmd::cargo::cargo_bin("a2a");
    assert!(text.contains("<string>dev.a2a.mesh-gateway</string>"), "{text}");
    assert!(text.contains("<key>KeepAlive</key>"), "{text}");
    assert!(text.contains("<true/>"), "{text}");
    assert!(text.contains(&format!("<string>{}</string>", exe.display())), "{text}");
    assert!(text.contains("<string>--home</string>"), "{text}");
    assert!(
        text.contains(&format!("<string>{}</string>", a2a_home.display())),
        "{text}"
    );
    assert!(text.contains("<string>mesh</string>"), "{text}");
    assert!(text.contains("<string>gateway</string>"), "{text}");
    assert!(text.contains("<string>run</string>"), "{text}");
    assert!(text.contains("gateway.daemon.log"), "{text}");

    let out = uninstall_cmd(&a2a_home, &work, &fake_home)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let value: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(value["data"]["uninstalled"], true);
    assert_eq!(value["data"]["reload_skipped"], true);
    assert!(!plist.exists());
}

#[cfg(target_os = "linux")]
#[test]
fn install_service_writes_systemd_unit_with_home_and_skip_report() {
    let tmp = tempfile::tempdir().unwrap();
    let fake_home = tmp.path().join("fake-home");
    let a2a_home = tmp.path().join("a2a-home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&fake_home).unwrap();
    std::fs::create_dir_all(&a2a_home).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(
        a2a_home.join("config.toml"),
        "[mesh.gateway]\nexpose = \"ngrok\"\n",
    )
    .unwrap();

    let out = install_cmd(&a2a_home, &work, &fake_home)
        .assert()
        .success()
        .stderr(predicate::str::contains("ngrok start --none"))
        .get_output()
        .stdout
        .clone();
    let value: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(value["data"]["installed"], true);
    assert_eq!(value["data"]["reload_skipped"], true);
    assert_eq!(
        value["data"]["status_command"],
        "systemctl --user status a2a-mesh-gateway.service"
    );

    let unit = fake_home
        .join(".config")
        .join("systemd")
        .join("user")
        .join("a2a-mesh-gateway.service");
    assert_eq!(
        value["data"]["path"].as_str(),
        Some(unit.to_string_lossy().as_ref())
    );
    let text = std::fs::read_to_string(&unit).unwrap();
    let exe = assert_cmd::cargo::cargo_bin("a2a");
    assert!(text.contains("[Unit]"), "{text}");
    assert!(text.contains("ExecStart="), "{text}");
    assert!(text.contains(&exe.to_string_lossy().to_string()), "{text}");
    assert!(text.contains("--home"), "{text}");
    assert!(text.contains(&a2a_home.to_string_lossy().to_string()), "{text}");
    assert!(text.contains("mesh gateway run"), "{text}");
    assert!(text.contains("Restart=always"), "{text}");
    assert!(text.contains("WantedBy=default.target"), "{text}");

    let out = uninstall_cmd(&a2a_home, &work, &fake_home)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let value: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(value["data"]["uninstalled"], true);
    assert_eq!(value["data"]["reload_skipped"], true);
    assert!(!unit.exists());
}

fn install_cmd(home: &Path, work: &Path, fake_home: &Path) -> assert_cmd::Command {
    let mut cmd = common::a2a(home, work);
    cmd.env("HOME", fake_home)
        .env("A2A_TEST_SKIP_SERVICE_RELOAD", "1")
        .arg("--json")
        .arg("--home")
        .arg(home)
        .args(["mesh", "gateway", "install-service"]);
    cmd
}

fn uninstall_cmd(home: &Path, work: &Path, fake_home: &Path) -> assert_cmd::Command {
    let mut cmd = common::a2a(home, work);
    cmd.env("HOME", fake_home)
        .env("A2A_TEST_SKIP_SERVICE_RELOAD", "1")
        .arg("--json")
        .arg("--home")
        .arg(home)
        .args(["mesh", "gateway", "uninstall-service"]);
    cmd
}
