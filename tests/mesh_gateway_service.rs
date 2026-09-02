mod common;

use predicates::prelude::*;
use serde_json::Value;
use std::path::Path;

#[cfg(target_os = "macos")]
use std::os::unix::fs::PermissionsExt;

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

#[cfg(target_os = "macos")]
#[test]
fn install_service_skips_bootout_when_launch_agent_is_not_loaded() {
    let tmp = tempfile::tempdir().unwrap();
    let fake_home = tmp.path().join("fake-home");
    let a2a_home = tmp.path().join("a2a-home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&fake_home).unwrap();
    std::fs::create_dir_all(&a2a_home).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    let bin = tmp.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let launchctl_log = tmp.path().join("launchctl.log");
    let launchctl = bin.join("launchctl");
    std::fs::write(&launchctl, fake_launchctl()).unwrap();
    std::fs::set_permissions(&launchctl, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let assert = install_cmd_reloading(&a2a_home, &work, &fake_home)
        .env("PATH", path)
        .env("A2A_FAKE_LAUNCHCTL_LOG", &launchctl_log)
        .assert()
        .success();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(
        !stderr.contains("Boot-out failed"),
        "an unloaded service should not leak bootout noise: {stderr}"
    );
    let log = std::fs::read_to_string(&launchctl_log).unwrap();
    assert!(log.contains("print gui/"), "{log}");
    assert!(log.contains("bootstrap gui/"), "{log}");
    assert!(
        !log.contains("bootout gui/"),
        "bootout should be state-gated away when launchctl print says unloaded: {log}"
    );
}

#[cfg(target_os = "macos")]
#[test]
fn install_service_surfaces_real_bootout_failures() {
    let tmp = tempfile::tempdir().unwrap();
    let fake_home = tmp.path().join("fake-home");
    let a2a_home = tmp.path().join("a2a-home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&fake_home).unwrap();
    std::fs::create_dir_all(&a2a_home).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    let bin = tmp.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let launchctl_log = tmp.path().join("launchctl.log");
    let launchctl = bin.join("launchctl");
    std::fs::write(&launchctl, fake_launchctl()).unwrap();
    std::fs::set_permissions(&launchctl, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let assert = install_cmd_reloading(&a2a_home, &work, &fake_home)
        .env("PATH", path)
        .env("A2A_FAKE_LAUNCHCTL_LOG", &launchctl_log)
        .env("A2A_FAKE_LAUNCHCTL_PRINT", "loaded")
        .env("A2A_FAKE_LAUNCHCTL_BOOTOUT", "fail")
        .assert()
        .failure();
    let stdout: Value = serde_json::from_slice(&assert.get_output().stdout).unwrap();
    assert_eq!(stdout["error"]["code"], "GATEWAY_SERVICE");
    assert!(
        stdout["error"]["message"]
            .as_str()
            .unwrap()
            .contains("launchctl bootout exited"),
        "{stdout}"
    );
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

#[cfg(target_os = "macos")]
fn install_cmd_reloading(home: &Path, work: &Path, fake_home: &Path) -> assert_cmd::Command {
    let mut cmd = common::a2a(home, work);
    cmd.env("HOME", fake_home)
        .arg("--json")
        .arg("--home")
        .arg(home)
        .args(["mesh", "gateway", "install-service"]);
    cmd
}

#[cfg(target_os = "macos")]
fn fake_launchctl() -> &'static str {
    r#"#!/bin/sh
printf '%s\n' "$*" >> "$A2A_FAKE_LAUNCHCTL_LOG"
case "$1" in
  print)
    if [ "$A2A_FAKE_LAUNCHCTL_PRINT" = "loaded" ]; then
      exit 0
    fi
    echo "service not found" >&2
    exit 113
    ;;
  bootout)
    echo "Boot-out failed: 5: Input/output error" >&2
    if [ "$A2A_FAKE_LAUNCHCTL_BOOTOUT" = "fail" ]; then
      exit 5
    fi
    exit 0
    ;;
  bootstrap)
    exit 0
    ;;
  load)
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"#
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
