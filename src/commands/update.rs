use crate::config::{self, Config};
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::ffi::OsStr;
use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
#[value(rename_all = "lowercase")]
pub enum Channel {
    Tag,
    Main,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseTag {
    pub name: String,
    pub version: Semver,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Semver {
    major: u64,
    minor: u64,
    patch: u64,
}

impl Ord for Semver {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.major, self.minor, self.patch).cmp(&(other.major, other.minor, other.patch))
    }
}

impl PartialOrd for Semver {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltArtifact {
    pub path: PathBuf,
}

pub trait UpdateEnv {
    fn remote_tags(&mut self, repo_url: &str) -> Result<String, AppError>;
    fn remote_main(&mut self, repo_url: &str) -> Result<String, AppError>;
    fn ensure_repo(&mut self, home: &A2aHome, repo_url: &str) -> Result<PathBuf, AppError>;
    fn checkout(&mut self, repo: &Path, channel: Channel, target: &str) -> Result<(), AppError>;
    fn build(&mut self, repo: &Path) -> Result<BuiltArtifact, AppError>;
    fn install(&mut self, artifact: &BuiltArtifact) -> Result<PathBuf, AppError>;
    fn verify_version(&mut self, binary: &Path) -> Result<String, AppError>;
    fn restart_gateway_service(&mut self, home: &A2aHome) -> Result<bool, AppError>;
    fn gateway_running(&mut self, home: &A2aHome) -> bool;
    fn broker_running(&mut self, home: &A2aHome) -> bool;
}

pub fn run(
    home: &A2aHome,
    cfg: &Config,
    channel: Channel,
    check: bool,
    json_mode: bool,
    quiet: bool,
) -> Result<Value, AppError> {
    let mut env = RealUpdateEnv;
    run_with_env_and_repo_url(
        home,
        &cfg.update.repo_url,
        &mut env,
        channel,
        check,
        json_mode,
        quiet,
    )
}

pub fn run_with_env(
    home: &A2aHome,
    env: &mut impl UpdateEnv,
    channel: Channel,
    check: bool,
    json_mode: bool,
) -> Result<Value, AppError> {
    run_with_env_and_repo_url(
        home,
        config::DEFAULT_UPDATE_REPO_URL,
        env,
        channel,
        check,
        json_mode,
        true,
    )
}

fn run_with_env_and_repo_url(
    home: &A2aHome,
    repo_url: &str,
    env: &mut impl UpdateEnv,
    channel: Channel,
    check: bool,
    json_mode: bool,
    quiet: bool,
) -> Result<Value, AppError> {
    let current = env!("CARGO_PKG_VERSION");
    let target = resolve_target(env, repo_url, channel)?;

    if check {
        let report = UpdateReport::check(current, &target);
        if !quiet && !json_mode {
            print_check(&report);
        }
        return Ok(report.into_value(json_mode));
    }

    if channel == Channel::Tag {
        let current_version = Semver::parse(current).ok_or_else(|| {
            AppError::new(
                "UPDATE_VERSION_PARSE",
                ExitCode::Internal,
                format!("current package version `{current}` is not semver"),
            )
        })?;
        if current_version >= target.version {
            let report = UpdateReport {
                updated: false,
                from: current.to_string(),
                to: target.version_string(),
                gateway_restarted: false,
                message: format!("already current ({current})"),
            };
            if !quiet && !json_mode {
                eprintln!("a2a: {}", report.message);
            }
            return Ok(report.into_value(json_mode));
        }
    }

    let repo = env.ensure_repo(home, repo_url)?;
    env.checkout(&repo, channel, &target.checkout_ref)?;
    let artifact = env.build(&repo)?;
    let installed = env.install(&artifact)?;
    let verified = env.verify_version(&installed)?;
    let new_version = parse_version_line(&verified).unwrap_or_else(|| target.version_string());
    let gateway_restarted = env.restart_gateway_service(home)?;
    if !gateway_restarted && env.gateway_running(home) && !quiet {
        eprintln!(
            "a2a: restart your gateway to pick this up: a2a mesh gateway stop && a2a mesh gateway run --daemon"
        );
    }
    if env.broker_running(home) && !quiet {
        eprintln!(
            "a2a: in-flight workers keep their current broker; new spawns use the new binary"
        );
    }
    let report = UpdateReport {
        updated: true,
        from: current.to_string(),
        to: new_version,
        gateway_restarted,
        message: format!(
            "{current} -> {}",
            parse_version_line(&verified).unwrap_or(verified)
        ),
    };
    if !quiet && !json_mode {
        eprintln!("a2a: {}", report.message);
        if gateway_restarted {
            eprintln!("a2a: mesh gateway service restarted");
        }
    }
    Ok(report.into_value(json_mode))
}

fn resolve_target(
    env: &mut impl UpdateEnv,
    repo_url: &str,
    channel: Channel,
) -> Result<UpdateTarget, AppError> {
    match channel {
        Channel::Tag => {
            let tag = latest_semver_tag(&env.remote_tags(repo_url)?)?;
            Ok(UpdateTarget {
                checkout_ref: tag.name,
                version: tag.version,
            })
        }
        Channel::Main => {
            let head = env.remote_main(repo_url)?;
            Ok(UpdateTarget {
                checkout_ref: "origin/main".to_string(),
                version: Semver {
                    major: u64::MAX,
                    minor: 0,
                    patch: 0,
                },
            }
            .with_main_display(head))
        }
    }
}

struct UpdateTarget {
    checkout_ref: String,
    version: Semver,
}

impl UpdateTarget {
    fn with_main_display(self, head: String) -> Self {
        UpdateTarget {
            checkout_ref: format!("origin/main ({})", short_hash(&head)),
            ..self
        }
    }

    fn version_string(&self) -> String {
        if self.version.major == u64::MAX {
            self.checkout_ref.clone()
        } else {
            format!(
                "{}.{}.{}",
                self.version.major, self.version.minor, self.version.patch
            )
        }
    }
}

struct UpdateReport {
    updated: bool,
    from: String,
    to: String,
    gateway_restarted: bool,
    message: String,
}

impl UpdateReport {
    fn check(current: &str, target: &UpdateTarget) -> Self {
        let latest = target.version_string();
        let message = match (Semver::parse(current), target.version.major != u64::MAX) {
            (Some(current_version), true) if current_version >= target.version => {
                format!("already current ({current})")
            }
            _ => format!("current {current} -> latest {latest}"),
        };
        UpdateReport {
            updated: false,
            from: current.to_string(),
            to: latest,
            gateway_restarted: false,
            message,
        }
    }

    fn into_value(self, json_mode: bool) -> Value {
        let data = json!({
            "updated": self.updated,
            "from": self.from,
            "to": self.to,
            "gateway_restarted": self.gateway_restarted,
        });
        if json_mode {
            data
        } else {
            Value::String(self.message)
        }
    }
}

fn print_check(report: &UpdateReport) {
    eprintln!("a2a: {}", report.message);
}

pub fn latest_semver_tag(ls_remote_tags: &str) -> Result<ReleaseTag, AppError> {
    ls_remote_tags
        .lines()
        .filter_map(|line| {
            let reference = line.split_whitespace().nth(1)?;
            let name = reference
                .strip_prefix("refs/tags/")?
                .strip_suffix("^{}")
                .unwrap_or_else(|| reference.strip_prefix("refs/tags/").unwrap());
            let version = parse_release_tag(name)?;
            Some(ReleaseTag {
                name: name.to_string(),
                version,
            })
        })
        .max_by_key(|tag| tag.version)
        .ok_or_else(|| {
            AppError::new(
                "UPDATE_NO_RELEASES",
                ExitCode::Precondition,
                "no vMAJOR.MINOR.PATCH release tags found",
            )
        })
}

fn parse_release_tag(name: &str) -> Option<Semver> {
    let rest = name.strip_prefix('v')?;
    Semver::parse(rest)
}

impl Semver {
    fn parse(s: &str) -> Option<Self> {
        let mut parts = s.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let patch = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Semver {
            major,
            minor,
            patch,
        })
    }
}

pub fn install_binary(built: &Path, target: &Path) -> Result<(), AppError> {
    let target = canonicalize_target(target)?;
    let dir = target.parent().ok_or_else(|| {
        AppError::new(
            "UPDATE_INSTALL_DENIED",
            ExitCode::Precondition,
            format!("{} has no parent directory", target.display()),
        )
    })?;
    ensure_writable_dir(dir)?;
    let temp = dir.join(format!(".a2a-update-{}", std::process::id()));
    match fs::remove_file(&temp) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(io_update("UPDATE_INSTALL_DENIED", &temp, e)),
    }
    fs::copy(built, &temp).map_err(|e| io_update("UPDATE_INSTALL_DENIED", &temp, e))?;
    set_executable(&temp)?;
    fs::rename(&temp, &target).map_err(|e| io_update("UPDATE_INSTALL_DENIED", &target, e))?;
    Ok(())
}

fn canonicalize_target(target: &Path) -> Result<PathBuf, AppError> {
    fs::canonicalize(target).map_err(|e| io_update("UPDATE_INSTALL_DENIED", target, e))
}

fn ensure_writable_dir(dir: &Path) -> Result<(), AppError> {
    #[cfg(unix)]
    {
        let mode = fs::metadata(dir)
            .map_err(|e| io_update("UPDATE_INSTALL_DENIED", dir, e))?
            .permissions()
            .mode();
        if mode & 0o222 == 0 {
            return Err(AppError::new(
                "UPDATE_INSTALL_DENIED",
                ExitCode::Precondition,
                format!("target directory is not writable: {}", dir.display()),
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<(), AppError> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))
        .map_err(|e| io_update("UPDATE_INSTALL_DENIED", path, e))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

fn io_update(code: &'static str, path: &Path, e: io::Error) -> AppError {
    AppError::new(
        code,
        ExitCode::Precondition,
        format!("{}: {e}", path.display()),
    )
}

struct RealUpdateEnv;

impl UpdateEnv for RealUpdateEnv {
    fn remote_tags(&mut self, repo_url: &str) -> Result<String, AppError> {
        git_output(["ls-remote", "--tags", "--refs", "--", repo_url], None)
    }

    fn remote_main(&mut self, repo_url: &str) -> Result<String, AppError> {
        let out = git_output(["ls-remote", "--heads", "--", repo_url, "main"], None)?;
        out.split_whitespace()
            .next()
            .map(str::to_string)
            .ok_or_else(|| {
                AppError::new(
                    "UPDATE_GIT_FAILED",
                    ExitCode::Precondition,
                    "origin/main not found",
                )
            })
    }

    fn ensure_repo(&mut self, home: &A2aHome, repo_url: &str) -> Result<PathBuf, AppError> {
        let update_root = home.root().join("update");
        let repo = update_root.join("repo");
        if repo.join(".git").exists() {
            git_status(
                Command::new("git")
                    .arg("-C")
                    .arg(&repo)
                    .args(["fetch", "--tags", "--prune", "origin"]),
            )?;
            return Ok(repo);
        }
        fs::create_dir_all(&update_root)
            .map_err(|e| io_update("UPDATE_GIT_FAILED", &update_root, e))?;
        git_status(
            Command::new("git")
                .arg("clone")
                .arg("--")
                .arg(repo_url)
                .arg(&repo),
        )?;
        Ok(repo)
    }

    fn checkout(&mut self, repo: &Path, channel: Channel, target: &str) -> Result<(), AppError> {
        let checkout_ref = match channel {
            Channel::Tag => target,
            Channel::Main => "origin/main",
        };
        git_status(
            Command::new("git")
                .arg("-C")
                .arg(repo)
                .args(["checkout", "--detach"])
                .arg(checkout_ref),
        )
    }

    fn build(&mut self, repo: &Path) -> Result<BuiltArtifact, AppError> {
        let output = Command::new("cargo")
            .args(["build", "--release"])
            .current_dir(repo)
            .output()
            .map_err(|e| {
                if e.kind() == io::ErrorKind::NotFound {
                    AppError::new(
                        "UPDATE_TOOLCHAIN_MISSING",
                        ExitCode::Precondition,
                        "could not find `cargo` on PATH",
                    )
                    .remedy("install the Rust toolchain from https://rustup.rs, then rerun `a2a update`")
                } else {
                    AppError::new(
                        "UPDATE_BUILD_FAILED",
                        ExitCode::Precondition,
                        format!("starting cargo build --release: {e}"),
                    )
                }
            })?;
        if !output.status.success() {
            return Err(AppError::new(
                "UPDATE_BUILD_FAILED",
                ExitCode::Precondition,
                format!(
                    "cargo build --release failed with {}; tail:\n{}",
                    output.status,
                    output_tail(&output.stdout, &output.stderr, 40)
                ),
            ));
        }
        Ok(BuiltArtifact {
            path: repo.join("target/release/a2a"),
        })
    }

    fn install(&mut self, artifact: &BuiltArtifact) -> Result<PathBuf, AppError> {
        let target = std::env::current_exe()
            .map_err(|e| {
                AppError::new(
                    "UPDATE_INSTALL_DENIED",
                    ExitCode::Precondition,
                    format!("resolving current executable: {e}"),
                )
            })?
            .canonicalize()
            .map_err(|e| {
                AppError::new(
                    "UPDATE_INSTALL_DENIED",
                    ExitCode::Precondition,
                    format!("canonicalizing current executable: {e}"),
                )
            })?;
        install_binary(&artifact.path, &target)?;
        Ok(target)
    }

    fn verify_version(&mut self, binary: &Path) -> Result<String, AppError> {
        let output = Command::new(binary)
            .arg("--version")
            .output()
            .map_err(|e| {
                AppError::new(
                    "UPDATE_VERIFY_FAILED",
                    ExitCode::Precondition,
                    format!("running {} --version: {e}", binary.display()),
                )
            })?;
        if !output.status.success() {
            return Err(AppError::new(
                "UPDATE_VERIFY_FAILED",
                ExitCode::Precondition,
                format!(
                    "{} --version exited with {}",
                    binary.display(),
                    output.status
                ),
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn restart_gateway_service(&mut self, _home: &A2aHome) -> Result<bool, AppError> {
        crate::mesh::gateway::restart_installed_service()
    }

    fn gateway_running(&mut self, home: &A2aHome) -> bool {
        crate::mesh::gateway::is_running(home)
    }

    fn broker_running(&mut self, home: &A2aHome) -> bool {
        crate::broker::pid_path(home.root()).exists()
            || home.root().join(crate::broker::BROKER_HEALTH).exists()
    }
}

fn git_output<I, S>(args: I, cwd: Option<&Path>) -> Result<String, AppError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut cmd = Command::new("git");
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    cmd.args(args);
    let output = cmd.output().map_err(|e| {
        AppError::new(
            "UPDATE_GIT_FAILED",
            ExitCode::Precondition,
            format!("running git: {e}"),
        )
    })?;
    if !output.status.success() {
        return Err(AppError::new(
            "UPDATE_GIT_FAILED",
            ExitCode::Precondition,
            format!(
                "git exited with {}; tail:\n{}",
                output.status,
                output_tail(&output.stdout, &output.stderr, 20)
            ),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn git_status(cmd: &mut Command) -> Result<(), AppError> {
    let output = cmd.output().map_err(|e| {
        AppError::new(
            "UPDATE_GIT_FAILED",
            ExitCode::Precondition,
            format!("running git: {e}"),
        )
    })?;
    if !output.status.success() {
        return Err(AppError::new(
            "UPDATE_GIT_FAILED",
            ExitCode::Precondition,
            format!(
                "git exited with {}; tail:\n{}",
                output.status,
                output_tail(&output.stdout, &output.stderr, 20)
            ),
        ));
    }
    Ok(())
}

fn output_tail(stdout: &[u8], stderr: &[u8], max_lines: usize) -> String {
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(stdout),
        String::from_utf8_lossy(stderr)
    );
    let mut lines: Vec<_> = combined.lines().rev().take(max_lines).collect();
    lines.reverse();
    lines.join("\n")
}

fn parse_version_line(line: &str) -> Option<String> {
    line.split_whitespace()
        .find(|part| Semver::parse(part).is_some())
        .map(str::to_string)
}

fn short_hash(hash: &str) -> String {
    hash.chars().take(12).collect()
}
