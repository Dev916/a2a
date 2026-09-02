use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use a2a::commands::update::{
    install_binary, latest_semver_tag, run_with_env, BuiltArtifact, Channel, UpdateEnv,
};
use a2a::home::A2aHome;
use a2a::output::AppError;

#[test]
fn tag_resolution_picks_newest_semver_and_ignores_noise() {
    let tags = "\
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\trefs/tags/v0.9.9
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\trefs/tags/v0.10.0
cccccccccccccccccccccccccccccccccccccccc\trefs/tags/not-a-release
dddddddddddddddddddddddddddddddddddddddd\trefs/tags/v1.0
eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\trefs/tags/v0.10.0^{}
";
    assert_eq!(latest_semver_tag(tags).unwrap().name, "v0.10.0");
}

#[test]
fn tag_resolution_reports_no_releases() {
    let err = latest_semver_tag("abc\trefs/tags/not-semver\n").unwrap_err();
    assert_eq!(err.code, "UPDATE_NO_RELEASES");
}

#[test]
fn already_current_short_circuits_before_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
    let mut env = FakeEnv::tag("v0.2.1");
    let out = run_with_env(&home, &mut env, Channel::Tag, false, true).unwrap();

    assert_eq!(out["updated"], false);
    assert_eq!(env.ensure_repo_calls, 0);
    assert_eq!(env.build_calls, 0);
    assert_eq!(env.install_calls, 0);
}

#[test]
fn check_mode_performs_no_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
    let mut env = FakeEnv::tag("v9.9.9");
    let out = run_with_env(&home, &mut env, Channel::Tag, true, true).unwrap();

    assert_eq!(out["updated"], false);
    assert_eq!(out["from"], env!("CARGO_PKG_VERSION"));
    assert_eq!(out["to"], "9.9.9");
    assert_eq!(env.ensure_repo_calls, 0);
    assert_eq!(env.build_calls, 0);
    assert_eq!(env.install_calls, 0);
    assert!(!home.root().join("update").exists());
}

#[test]
fn update_path_builds_installs_and_reports_gateway_restart_with_fakes() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
    let mut env = FakeEnv::tag("v9.9.9").with_gateway_restarted(true);
    let out = run_with_env(&home, &mut env, Channel::Tag, false, true).unwrap();

    assert_eq!(out["updated"], true);
    assert_eq!(out["from"], env!("CARGO_PKG_VERSION"));
    assert_eq!(out["to"], "9.9.9");
    assert_eq!(out["gateway_restarted"], true);
    assert_eq!(env.ensure_repo_calls, 1);
    assert_eq!(env.checkout_calls, 1);
    assert_eq!(env.last_checkout.as_deref(), Some("v9.9.9"));
    assert_eq!(env.build_calls, 1);
    assert_eq!(env.install_calls, 1);
}

#[test]
#[cfg(unix)]
fn installer_renames_over_target_and_leaves_no_temp() {
    let tmp = tempfile::tempdir().unwrap();
    let built = tmp.path().join("built-a2a");
    let target = tmp.path().join("a2a");
    fs::write(&built, b"new binary\n").unwrap();
    fs::write(&target, b"old binary\n").unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).unwrap();
    let before = fs::metadata(&target).unwrap();

    install_binary(&built, &target).unwrap();

    let after = fs::metadata(&target).unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"new binary\n");
    assert_eq!(after.permissions().mode() & 0o777, 0o755);
    assert_ne!(
        (before.dev(), before.ino()),
        (after.dev(), after.ino()),
        "rename must replace the inode, not copy into it"
    );
    let leftovers: Vec<_> = fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".a2a-update-")
        })
        .collect();
    assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
}

#[test]
#[cfg(unix)]
fn installer_denies_unwritable_target_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let built = tmp.path().join("built-a2a");
    let dir = tmp.path().join("bin");
    let target = dir.join("a2a");
    fs::create_dir(&dir).unwrap();
    fs::write(&built, b"new binary\n").unwrap();
    fs::write(&target, b"old binary\n").unwrap();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).unwrap();

    let err = install_binary(&built, &target).unwrap_err();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();

    assert_eq!(err.code, "UPDATE_INSTALL_DENIED");
    assert!(err.message.contains(&dir.display().to_string()));
    assert_eq!(fs::read(&target).unwrap(), b"old binary\n");
}

#[test]
fn missing_cargo_is_a_typed_error() {
    let tmp = tempfile::tempdir().unwrap();
    let home = A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
    let mut env = FakeEnv::tag("v9.9.9").with_build_error(AppError::new(
        "UPDATE_TOOLCHAIN_MISSING",
        a2a::output::ExitCode::Precondition,
        "could not find `cargo` on PATH",
    ));

    let err = run_with_env(&home, &mut env, Channel::Tag, false, true).unwrap_err();
    assert_eq!(err.code, "UPDATE_TOOLCHAIN_MISSING");
}

#[test]
fn cli_check_json_envelope_uses_local_repo_tags_without_building() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo.git");
    init_bare_repo_with_tags(&repo, &["v0.1.0", "v0.2.1"]);
    let home = tmp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    fs::write(
        home.join("config.toml"),
        format!("[update]\nrepo_url = \"file://{}\"\n", repo.display()),
    )
    .unwrap();

    let out = Command::cargo_bin("a2a")
        .unwrap()
        .env("A2A_HOME", &home)
        .args(["update", "--check", "--json"])
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["schema_version"], 1);
    assert_eq!(v["ok"], true);
    assert_eq!(v["command"], "update");
    assert_eq!(v["data"]["updated"], false);
    assert_eq!(v["data"]["from"], env!("CARGO_PKG_VERSION"));
    assert_eq!(v["data"]["to"], "0.2.1");
    assert_eq!(v["data"]["gateway_restarted"], false);
    assert!(!home.join("update").exists());
}

#[test]
fn version_pin_is_021() {
    Command::cargo_bin("a2a")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::starts_with("a2a 0.2.1\n"));
}

#[derive(Default)]
struct FakeEnv {
    remote_tags: String,
    build_error: Option<AppError>,
    gateway_restarted: bool,
    ensure_repo_calls: usize,
    checkout_calls: usize,
    last_checkout: Option<String>,
    build_calls: usize,
    install_calls: usize,
}

impl FakeEnv {
    fn tag(name: &str) -> Self {
        Self {
            remote_tags: format!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\trefs/tags/{name}\n"),
            ..Self::default()
        }
    }

    fn with_build_error(mut self, err: AppError) -> Self {
        self.build_error = Some(err);
        self
    }

    fn with_gateway_restarted(mut self, restarted: bool) -> Self {
        self.gateway_restarted = restarted;
        self
    }
}

impl UpdateEnv for FakeEnv {
    fn remote_tags(&mut self, _repo_url: &str) -> Result<String, AppError> {
        Ok(self.remote_tags.clone())
    }

    fn remote_main(&mut self, _repo_url: &str) -> Result<String, AppError> {
        Ok("0123456789012345678901234567890123456789".to_string())
    }

    fn ensure_repo(&mut self, _home: &A2aHome, _repo_url: &str) -> Result<PathBuf, AppError> {
        self.ensure_repo_calls += 1;
        Ok(PathBuf::from("/fake/repo"))
    }

    fn checkout(&mut self, _repo: &Path, _channel: Channel, _target: &str) -> Result<(), AppError> {
        self.checkout_calls += 1;
        self.last_checkout = Some(_target.to_string());
        Ok(())
    }

    fn build(&mut self, _repo: &Path) -> Result<BuiltArtifact, AppError> {
        self.build_calls += 1;
        if let Some(err) = self.build_error.take() {
            return Err(err);
        }
        Ok(BuiltArtifact {
            path: PathBuf::from("/fake/repo/target/release/a2a"),
        })
    }

    fn install(&mut self, _artifact: &BuiltArtifact) -> Result<PathBuf, AppError> {
        self.install_calls += 1;
        Ok(PathBuf::from("/usr/local/bin/a2a"))
    }

    fn verify_version(&mut self, _binary: &Path) -> Result<String, AppError> {
        Ok("a2a 9.9.9".to_string())
    }

    fn restart_gateway_service(&mut self, _home: &A2aHome) -> Result<bool, AppError> {
        Ok(self.gateway_restarted)
    }

    fn gateway_running(&mut self, _home: &A2aHome) -> bool {
        false
    }

    fn broker_running(&mut self, _home: &A2aHome) -> bool {
        false
    }
}

fn init_bare_repo_with_tags(path: &Path, tags: &[&str]) {
    let work = path.with_extension("work");
    run_git(&["init", "--bare", path.to_str().unwrap()], None);
    run_git(&["init", work.to_str().unwrap()], None);
    run_git(
        &["config", "user.email", "test@example.invalid"],
        Some(&work),
    );
    run_git(&["config", "user.name", "Test User"], Some(&work));
    fs::write(work.join("README.md"), "fixture\n").unwrap();
    run_git(&["add", "README.md"], Some(&work));
    run_git(&["commit", "-m", "initial"], Some(&work));
    for tag in tags {
        run_git(&["tag", tag], Some(&work));
    }
    run_git(
        &["remote", "add", "origin", path.to_str().unwrap()],
        Some(&work),
    );
    run_git(&["push", "--tags", "origin", "HEAD:main"], Some(&work));
}

fn run_git(args: &[&str], cwd: Option<&Path>) {
    let mut cmd = std::process::Command::new("git");
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    let out = cmd.args(args).output().unwrap();
    assert!(
        out.status.success(),
        "git {:?} failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
