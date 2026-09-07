use crate::config::Config;
use crate::home::A2aHome;
use crate::mesh::{
    keys::{self, Clock, SystemClock},
    registry::{self, Registry, RegistryHandle},
    row_mark::RowMarkStore,
    row_sig::RowVerifyCfg,
};
use crate::output::{AppError, ExitCode, Outcome};
use chrono::{DateTime, SecondsFormat, Utc};
use expose::{
    Exposure, ExposureErrorChange, ExposureProvider, ExposurePublisher, ExposureRow,
    RowSigningContext,
};
use futures_util::FutureExt;
use link::{LinkDirection, LinkSessionParams, LinkStateHandle};
use nix::fcntl::{Flock, FlockArg};
use nix::sys::signal::{kill, SigHandler, Signal};
use nix::unistd::Pid;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::ffi::OsString;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub mod expose;
pub mod link;
pub mod notes_wire;
pub mod presence_wire;
pub mod replicate;

pub const GATEWAY_LOCK: &str = "gateway.lock";
pub const GATEWAY_PID: &str = "gateway.pid";
pub const GATEWAY_HEALTH: &str = "gateway-health.json";
pub const GATEWAY_JSONL: &str = "gateway.jsonl";
pub const GATEWAY_DAEMON_LOG: &str = "gateway.daemon.log";
pub const HEALTH_SCHEMA_VERSION: u32 = 2;

pub type DialPeerRowIdentity = (Option<String>, Option<String>);

pub fn refresh_dial_state(
    last_seen: &mut BTreeMap<String, DialPeerRowIdentity>,
    rows: &[registry::GatewayRow],
    attempts: &mut BTreeMap<String, u32>,
    next_dial_after: &mut BTreeMap<String, Instant>,
    damping: &mut link::PeerDamping,
) -> Vec<String> {
    let reset = refresh_dial_state_maps(last_seen, rows, attempts, next_dial_after);
    for peer in &reset {
        damping.reset_peer(peer);
    }
    reset
}

fn refresh_dial_state_maps(
    last_seen: &mut BTreeMap<String, DialPeerRowIdentity>,
    rows: &[registry::GatewayRow],
    attempts: &mut BTreeMap<String, u32>,
    next_dial_after: &mut BTreeMap<String, Instant>,
) -> Vec<String> {
    let mut current = BTreeMap::new();
    for row in rows {
        current.insert(row.gateway_id.clone(), row_identity(row));
    }

    last_seen.retain(|peer, _| current.contains_key(peer));
    attempts.retain(|peer, _| current.contains_key(peer));
    next_dial_after.retain(|peer, _| current.contains_key(peer));

    let mut reset = Vec::new();
    for (peer, identity) in current {
        match last_seen.insert(peer.clone(), identity.clone()) {
            Some(previous) if previous != identity => {
                attempts.remove(&peer);
                next_dial_after.remove(&peer);
                reset.push(peer);
            }
            _ => {}
        }
    }
    reset
}

fn row_identity(row: &registry::GatewayRow) -> DialPeerRowIdentity {
    (row.endpoint_url.clone(), row.cert_fpr.clone())
}
pub const MAX_RESTARTS: usize = 5;
pub const RESTART_WINDOW: Duration = Duration::from_secs(60);

const LOG_CAPACITY: usize = 1024;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
const HEALTH_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DAEMON_START_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LoopSlot {
    Registry,
    Server,
    Dialer,
    Watcher,
    AntiEntropy,
    Work,
}

impl LoopSlot {
    const ALL: [LoopSlot; 6] = [
        LoopSlot::Registry,
        LoopSlot::Server,
        LoopSlot::Dialer,
        LoopSlot::Watcher,
        LoopSlot::AntiEntropy,
        LoopSlot::Work,
    ];

    fn as_str(self) -> &'static str {
        match self {
            LoopSlot::Registry => "registry",
            LoopSlot::Server => "server",
            LoopSlot::Dialer => "dialer",
            LoopSlot::Watcher => "watcher",
            LoopSlot::AntiEntropy => "anti-entropy",
            LoopSlot::Work => "work",
        }
    }

    fn policy(self) -> &'static str {
        match self {
            LoopSlot::Dialer => "rest-for-one",
            LoopSlot::Registry
            | LoopSlot::Server
            | LoopSlot::Watcher
            | LoopSlot::AntiEntropy
            | LoopSlot::Work => "one-for-one",
        }
    }
}

impl std::fmt::Display for LoopSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for LoopSlot {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "registry" => Ok(LoopSlot::Registry),
            "server" => Ok(LoopSlot::Server),
            "dialer" => Ok(LoopSlot::Dialer),
            "watcher" => Ok(LoopSlot::Watcher),
            "anti-entropy" => Ok(LoopSlot::AntiEntropy),
            "work" => Ok(LoopSlot::Work),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoopHealth {
    pub state: String,
    pub restart_policy: String,
    pub restarts: u64,
    pub heartbeats: u64,
    pub last_heartbeat: Option<DateTime<Utc>>,
    pub last_exit: Option<String>,
}

impl LoopHealth {
    fn new(slot: LoopSlot) -> Self {
        LoopHealth {
            state: "starting".to_string(),
            restart_policy: slot.policy().to_string(),
            restarts: 0,
            heartbeats: 0,
            last_heartbeat: None,
            last_exit: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayHealth {
    pub schema_version: u32,
    pub pid: u32,
    pub health_seq: u64,
    pub written_at: DateTime<Utc>,
    pub state: String,
    pub listen_port: u64,
    pub expose: String,
    pub endpoint_url: Option<String>,
    pub exposure_error: Option<String>,
    pub exposure_error_since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub registry_error: Option<String>,
    #[serde(default)]
    pub registry_error_since: Option<DateTime<Utc>>,
    pub loops: BTreeMap<LoopSlot, LoopHealth>,
    pub replication: replicate::ReplicationHealth,
    #[serde(default)]
    pub links: Vec<link::LinkHealth>,
    pub escalation: Option<Value>,
    pub shutdown: Option<Value>,
}

impl GatewayHealth {
    fn initial(cfg: &Config) -> Self {
        GatewayHealth {
            schema_version: HEALTH_SCHEMA_VERSION,
            pid: std::process::id(),
            health_seq: 0,
            written_at: Utc::now(),
            state: "starting".to_string(),
            listen_port: cfg.mesh.gateway.listen_port,
            expose: cfg.mesh.gateway.expose.clone(),
            endpoint_url: None,
            exposure_error: None,
            exposure_error_since: None,
            registry_error: None,
            registry_error_since: None,
            loops: LoopSlot::ALL
                .into_iter()
                .map(|slot| (slot, LoopHealth::new(slot)))
                .collect(),
            replication: replicate::ReplicationHealth::default(),
            links: Vec::new(),
            escalation: None,
            shutdown: None,
        }
    }

    pub fn read(home: &A2aHome) -> Option<Self> {
        let text = std::fs::read_to_string(gateway_root(home).join(GATEWAY_HEALTH)).ok()?;
        serde_json::from_str(&text).ok()
    }
}

#[derive(Clone)]
struct GatewayLog {
    tx: mpsc::Sender<LogMsg>,
}

struct LogMsg {
    value: Value,
    ack: oneshot::Sender<()>,
}

impl GatewayLog {
    fn start(root: PathBuf) -> Self {
        let (tx, mut rx) = mpsc::channel::<LogMsg>(LOG_CAPACITY);
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let root = root.clone();
                let _ = tokio::task::spawn_blocking(move || write_jsonl(&root, msg.value)).await;
                let _ = msg.ack.send(());
            }
        });
        GatewayLog { tx }
    }

    async fn line(&self, value: Value) {
        let (ack, done) = oneshot::channel();
        let _ = self.tx.send(LogMsg { value, ack }).await;
        let _ = done.await;
    }
}

#[derive(Clone)]
struct GatewayRuntimeConfig {
    home: A2aHome,
    config: Config,
    outbound_retry_max_secs: u64,
    shutdown_grace_secs: u64,
    panic_slot: Option<LoopSlot>,
    stall_shutdown_slot: Option<LoopSlot>,
    server_bind_optional: bool,
    registry: Arc<GatewayRegistryRuntime>,
    links: LinkStateHandle,
    link_observer: link::LinkObserver,
}

impl GatewayRuntimeConfig {
    fn from_config(
        home: &A2aHome,
        cfg: &Config,
        registry: Arc<GatewayRegistryRuntime>,
        links: LinkStateHandle,
        link_observer: link::LinkObserver,
    ) -> Self {
        GatewayRuntimeConfig {
            home: home.clone(),
            config: cfg.clone(),
            outbound_retry_max_secs: cfg.mesh.gateway.outbound_retry_max_secs,
            shutdown_grace_secs: cfg.mesh.gateway.shutdown_grace_secs,
            panic_slot: std::env::var("A2A_TEST_MESH_GATEWAY_STUB_PANIC")
                .ok()
                .and_then(|s| s.parse().ok()),
            stall_shutdown_slot: std::env::var("A2A_TEST_MESH_GATEWAY_STUB_STALL_SHUTDOWN")
                .ok()
                .and_then(|s| s.parse().ok()),
            server_bind_optional: std::env::var("A2A_TEST_MESH_GATEWAY_SERVER_BIND_OPTIONAL")
                .ok()
                .is_some_and(|value| value == "1"),
            registry,
            links,
            link_observer,
        }
    }
}

enum LoopEvent {
    Heartbeat(LoopSlot),
    Endpoint(Option<String>),
    ExposureError(ExposureErrorChange),
    RegistryError(RegistryErrorReport),
    RowSelfTamper { gateway_id: String, verdict: String },
    Replication(replicate::ReplicationHealth),
    Link(link::LinkHealthEvent),
    SessionEnd { slot: LoopSlot, detail: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryErrorChange {
    pub error: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub suppressed: u64,
}

#[derive(Clone, Debug)]
struct RegistryErrorReport {
    slot: LoopSlot,
    error: Option<String>,
    at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct RegistryErrorState {
    message: String,
    since: DateTime<Utc>,
    suppressed: u64,
}

#[derive(Debug)]
enum DaemonOutcome {
    Graceful(Value),
    NonZero(Value),
}

pub fn run(
    home: &A2aHome,
    cfg: &Config,
    quiet: bool,
    daemon: bool,
    daemonized: bool,
) -> Result<Outcome, AppError> {
    if daemon && !daemonized {
        return run_daemon_parent(home, quiet);
    }

    keys::ensure_and_verify_gateway_enrollment(home, &SystemClock)?;
    let root = gateway_root(home);
    let Some(lock) = acquire_lock(&root)? else {
        if !quiet {
            eprintln!("a2a: mesh gateway already running");
        }
        return Ok(Outcome::from(
            json!({"started": false, "already_running": true}),
        ));
    };
    write_pidfile(&root)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            AppError::new(
                "INTERNAL",
                ExitCode::Internal,
                format!("starting gateway runtime: {e}"),
            )
        })?;
    let outcome = runtime.block_on(supervise(home, root.clone(), cfg.clone(), quiet));
    runtime.shutdown_timeout(Duration::from_secs(2));

    let _ = std::fs::remove_file(root.join(GATEWAY_PID));
    let _ = std::fs::remove_file(root.join(GATEWAY_LOCK));
    drop(lock);

    match outcome {
        Ok(DaemonOutcome::Graceful(data)) => Ok(Outcome::from(data)),
        Ok(DaemonOutcome::NonZero(data)) => Ok(Outcome::with(data, ExitCode::Internal)),
        Err(e) => Err(e),
    }
}

fn run_daemon_parent(home: &A2aHome, quiet: bool) -> Result<Outcome, AppError> {
    let root = gateway_root(home);
    if !lock_is_free(&root) {
        if !quiet {
            eprintln!("a2a: mesh gateway already running");
        }
        return Ok(Outcome::from(
            json!({"started": false, "already_running": true}),
        ));
    }

    let log_path = root.join(GATEWAY_DAEMON_LOG);
    let stdout = crate::exec::open_append_0600(&log_path).map_err(|e| io_error(&log_path, e))?;
    let stderr = stdout.try_clone().map_err(|e| io_error(&log_path, e))?;
    let devnull = File::open("/dev/null").map_err(|e| io_error(Path::new("/dev/null"), e))?;
    let exe = std::env::current_exe().map_err(|e| {
        AppError::new(
            "GATEWAY_DAEMON_SPAWN",
            ExitCode::Internal,
            format!("resolving current executable: {e}"),
        )
    })?;
    let args = daemonized_args();
    let mut cmd = Command::new(exe);
    cmd.args(args)
        .stdin(Stdio::from(devnull))
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    unsafe {
        cmd.pre_exec(|| {
            nix::unistd::setsid()
                .map_err(|e| std::io::Error::other(format!("setsid: {e}")))?;
            nix::sys::signal::signal(Signal::SIGHUP, SigHandler::SigIgn)
                .map_err(|e| std::io::Error::other(format!("ignoring SIGHUP: {e}")))?;
            Ok(())
        });
    }
    let mut child = cmd.spawn().map_err(|e| {
        AppError::new(
            "GATEWAY_DAEMON_SPAWN",
            ExitCode::Internal,
            format!("spawning mesh gateway daemon: {e}"),
        )
    })?;
    let pid = child.id();
    let deadline = Instant::now() + DAEMON_START_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().map_err(|e| {
            AppError::new(
                "GATEWAY_DAEMON_START",
                ExitCode::Internal,
                format!("checking daemon startup: {e}"),
            )
        })? {
            let tail = tail_log_lines(&log_path, 40);
            return Err(AppError::new(
                "GATEWAY_DAEMON_START",
                ExitCode::Precondition,
                format!("mesh gateway daemon exited during startup with {status}: {tail}"),
            )
            .data(json!({
                "pid": pid,
                "exit_status": status.code(),
                "log": log_path,
                "log_tail": tail,
            })));
        }
        if let Some(health) = GatewayHealth::read(home) {
            if health.pid == pid && health.state == "running" {
                return Ok(Outcome::from(json!({
                    "started": true,
                    "pid": pid,
                    "log": log_path,
                })));
            }
        }
        if Instant::now() >= deadline {
            let tail = tail_log_lines(&log_path, 40);
            return Err(AppError::new(
                "GATEWAY_DAEMON_START_TIMEOUT",
                ExitCode::BrokerUnavailable,
                format!(
                    "mesh gateway daemon pid {pid} did not report healthy within {}s: {tail}",
                    DAEMON_START_TIMEOUT.as_secs()
                ),
            )
            .data(json!({"pid": pid, "log": log_path, "log_tail": tail})));
        }
        std::thread::park_timeout(Duration::from_millis(50));
    }
}

fn daemonized_args() -> Vec<OsString> {
    let mut args = Vec::new();
    let mut saw_daemonized = false;
    for arg in std::env::args_os().skip(1) {
        if arg == "--daemon" {
            continue;
        }
        if arg == "--daemonized" {
            saw_daemonized = true;
        }
        args.push(arg);
    }
    if !saw_daemonized {
        args.push(OsString::from("--daemonized"));
    }
    args
}

fn tail_log_lines(path: &Path, max_lines: usize) -> String {
    let mut text = String::new();
    if File::open(path)
        .and_then(|mut f| f.read_to_string(&mut text))
        .is_err()
    {
        return String::new();
    }
    let lines: Vec<&str> = text.lines().rev().take(max_lines).collect();
    lines.into_iter().rev().collect::<Vec<_>>().join("\n")
}

pub fn install_service(
    home: &A2aHome,
    cfg: &Config,
    home_arg: Option<&Path>,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let exe = std::env::current_exe().map_err(|e| {
        AppError::new(
            "GATEWAY_SERVICE",
            ExitCode::Internal,
            format!("resolving current executable: {e}"),
        )
    })?;
    let home_flag = home_arg.map(|_| home.root().to_path_buf());
    let log_path = gateway_root(home).join(GATEWAY_DAEMON_LOG);
    let reload_skipped = service_reload_skipped();
    let platform = install_service_for_platform(&exe, home_flag.as_deref(), &log_path, reload_skipped)?;
    if cfg.mesh.gateway.expose == "ngrok" && !quiet {
        eprintln!(
            "a2a: ngrok exposure needs a running `ngrok start --none` agent; keep it alive the same way."
        );
    }
    Ok(Outcome::from(json!({
        "installed": true,
        "path": platform.path,
        "status_command": platform.status_command,
        "reload_skipped": reload_skipped,
    })))
}

pub fn uninstall_service(home: &A2aHome, quiet: bool) -> Result<Outcome, AppError> {
    let _ = gateway_root(home);
    let reload_skipped = service_reload_skipped();
    let platform = uninstall_service_for_platform(reload_skipped)?;
    if !quiet {
        eprintln!("a2a: removed {}", platform.path.display());
        eprintln!("a2a: status command: {}", platform.status_command);
    }
    Ok(Outcome::from(json!({
        "uninstalled": true,
        "path": platform.path,
        "status_command": platform.status_command,
        "reload_skipped": reload_skipped,
    })))
}

struct ServiceInstall {
    path: PathBuf,
    status_command: &'static str,
}

fn service_reload_skipped() -> bool {
    std::env::var("A2A_TEST_SKIP_SERVICE_RELOAD")
        .ok()
        .is_some_and(|value| value == "1")
}

pub fn is_running(home: &A2aHome) -> bool {
    !lock_is_free(&gateway_root(home))
}

pub fn restart_installed_service() -> Result<bool, AppError> {
    if !service_is_installed()? {
        return Ok(false);
    }
    if service_reload_skipped() {
        return Ok(true);
    }
    restart_service_for_platform()?;
    Ok(true)
}

#[cfg(target_os = "macos")]
fn service_is_installed() -> Result<bool, AppError> {
    Ok(home_dir()?
        .join("Library/LaunchAgents/dev.a2a.mesh-gateway.plist")
        .exists())
}

#[cfg(target_os = "linux")]
fn service_is_installed() -> Result<bool, AppError> {
    Ok(home_dir()?
        .join(".config/systemd/user/a2a-mesh-gateway.service")
        .exists())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn service_is_installed() -> Result<bool, AppError> {
    Ok(false)
}

#[cfg(target_os = "macos")]
fn restart_service_for_platform() -> Result<(), AppError> {
    let uid = nix::unistd::Uid::current().as_raw().to_string();
    let service = format!("gui/{uid}/dev.a2a.mesh-gateway");
    let status = Command::new("launchctl")
        .args(["kickstart", "-k", &service])
        .status()
        .map_err(|e| service_error(format!("running launchctl kickstart: {e}")))?;
    if !status.success() {
        return Err(service_error(format!(
            "launchctl kickstart exited with {status}"
        )));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn restart_service_for_platform() -> Result<(), AppError> {
    run_service_command(Command::new("systemctl").args([
        "--user",
        "restart",
        "a2a-mesh-gateway.service",
    ]))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn restart_service_for_platform() -> Result<(), AppError> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn install_service_for_platform(
    exe: &Path,
    home_arg: Option<&Path>,
    log_path: &Path,
    reload_skipped: bool,
) -> Result<ServiceInstall, AppError> {
    let path = home_dir()?.join("Library/LaunchAgents/dev.a2a.mesh-gateway.plist");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| io_error(parent, e))?;
    }
    let args = service_args(exe, home_arg);
    std::fs::write(&path, launch_agent_plist(&args, log_path)).map_err(|e| io_error(&path, e))?;
    if !reload_skipped {
        let uid = nix::unistd::Uid::current().as_raw().to_string();
        let target = format!("gui/{uid}");
        let service = format!("{target}/dev.a2a.mesh-gateway");
        bootout_launch_agent_if_loaded(&service)?;
        let bootstrap = Command::new("launchctl")
            .arg("bootstrap")
            .arg(&target)
            .arg(&path)
            .status()
            .map_err(|e| service_error(format!("running launchctl bootstrap: {e}")))?;
        if !bootstrap.success() {
            let load = Command::new("launchctl")
                .args(["load", "-w"])
                .arg(&path)
                .status()
                .map_err(|e| service_error(format!("running launchctl load -w: {e}")))?;
            if !load.success() {
                return Err(service_error(format!(
                    "launchctl bootstrap exited with {bootstrap}; launchctl load -w exited with {load}"
                )));
            }
        }
    }
    Ok(ServiceInstall {
        path,
        status_command: "launchctl print gui/$UID/dev.a2a.mesh-gateway",
    })
}

#[cfg(target_os = "macos")]
fn uninstall_service_for_platform(reload_skipped: bool) -> Result<ServiceInstall, AppError> {
    let path = home_dir()?.join("Library/LaunchAgents/dev.a2a.mesh-gateway.plist");
    if !reload_skipped {
        let uid = nix::unistd::Uid::current().as_raw().to_string();
        let service = format!("gui/{uid}/dev.a2a.mesh-gateway");
        bootout_launch_agent_if_loaded(&service)?;
    }
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io_error(&path, e)),
    }
    Ok(ServiceInstall {
        path,
        status_command: "launchctl print gui/$UID/dev.a2a.mesh-gateway",
    })
}

#[cfg(target_os = "macos")]
fn bootout_launch_agent_if_loaded(service: &str) -> Result<(), AppError> {
    if !launch_agent_is_loaded(service)? {
        return Ok(());
    }
    let status = Command::new("launchctl")
        .args(["bootout", service])
        .status()
        .map_err(|e| service_error(format!("running launchctl bootout: {e}")))?;
    if !status.success() {
        return Err(service_error(format!(
            "launchctl bootout exited with {status}"
        )));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn launch_agent_is_loaded(service: &str) -> Result<bool, AppError> {
    let status = Command::new("launchctl")
        .args(["print", service])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| service_error(format!("running launchctl print: {e}")))?;
    Ok(status.success())
}

#[cfg(target_os = "linux")]
fn install_service_for_platform(
    exe: &Path,
    home_arg: Option<&Path>,
    log_path: &Path,
    reload_skipped: bool,
) -> Result<ServiceInstall, AppError> {
    let path = home_dir()?.join(".config/systemd/user/a2a-mesh-gateway.service");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| io_error(parent, e))?;
    }
    let args = service_args(exe, home_arg);
    std::fs::write(&path, systemd_unit(&args, log_path)).map_err(|e| io_error(&path, e))?;
    if !reload_skipped {
        run_service_command(Command::new("systemctl").args(["--user", "daemon-reload"]))?;
        run_service_command(Command::new("systemctl").args([
            "--user",
            "enable",
            "--now",
            "a2a-mesh-gateway.service",
        ]))?;
    }
    Ok(ServiceInstall {
        path,
        status_command: "systemctl --user status a2a-mesh-gateway.service",
    })
}

#[cfg(target_os = "linux")]
fn uninstall_service_for_platform(reload_skipped: bool) -> Result<ServiceInstall, AppError> {
    let path = home_dir()?.join(".config/systemd/user/a2a-mesh-gateway.service");
    if !reload_skipped {
        let _ = Command::new("systemctl")
            .args(["--user", "disable", "--now", "a2a-mesh-gateway.service"])
            .status();
    }
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io_error(&path, e)),
    }
    if !reload_skipped {
        run_service_command(Command::new("systemctl").args(["--user", "daemon-reload"]))?;
    }
    Ok(ServiceInstall {
        path,
        status_command: "systemctl --user status a2a-mesh-gateway.service",
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn install_service_for_platform(
    _exe: &Path,
    _home_arg: Option<&Path>,
    _log_path: &Path,
    _reload_skipped: bool,
) -> Result<ServiceInstall, AppError> {
    Err(service_error("gateway service install is supported on macOS and Linux"))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn uninstall_service_for_platform(_reload_skipped: bool) -> Result<ServiceInstall, AppError> {
    Err(service_error("gateway service uninstall is supported on macOS and Linux"))
}

fn service_args(exe: &Path, home_arg: Option<&Path>) -> Vec<PathBuf> {
    let mut args = vec![exe.to_path_buf()];
    if let Some(home) = home_arg {
        args.push(PathBuf::from("--home"));
        args.push(home.to_path_buf());
    }
    args.push(PathBuf::from("mesh"));
    args.push(PathBuf::from("gateway"));
    args.push(PathBuf::from("run"));
    args
}

#[cfg(target_os = "macos")]
fn launch_agent_plist(args: &[PathBuf], log_path: &Path) -> String {
    let mut plist = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>dev.a2a.mesh-gateway</string>
  <key>ProgramArguments</key>
  <array>
"#,
    );
    for arg in args {
        plist.push_str("    <string>");
        plist.push_str(&xml_escape(&arg.to_string_lossy()));
        plist.push_str("</string>\n");
    }
    plist.push_str(
        r#"  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>"#,
    );
    plist.push_str(&xml_escape(&log_path.to_string_lossy()));
    plist.push_str(
        r#"</string>
  <key>StandardErrorPath</key>
  <string>"#,
    );
    plist.push_str(&xml_escape(&log_path.to_string_lossy()));
    plist.push_str(
        r#"</string>
</dict>
</plist>
"#,
    );
    plist
}

#[cfg(target_os = "linux")]
fn systemd_unit(args: &[PathBuf], log_path: &Path) -> String {
    format!(
        "[Unit]\nDescription=a2a mesh gateway\n\n[Service]\nExecStart={}\nRestart=always\nStandardOutput=append:{}\nStandardError=append:{}\n\n[Install]\nWantedBy=default.target\n",
        args.iter()
            .map(|arg| systemd_quote(&arg.to_string_lossy()))
            .collect::<Vec<_>>()
            .join(" "),
        systemd_quote(&log_path.to_string_lossy()),
        systemd_quote(&log_path.to_string_lossy())
    )
}

fn home_dir() -> Result<PathBuf, AppError> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| service_error("HOME is not set"))
}

#[cfg(target_os = "macos")]
fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(target_os = "linux")]
fn systemd_quote(text: &str) -> String {
    if text
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-' | b':' | b'='))
    {
        return text.to_string();
    }
    let escaped = text.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

#[cfg(target_os = "linux")]
fn run_service_command(cmd: &mut Command) -> Result<(), AppError> {
    let status = cmd
        .status()
        .map_err(|e| service_error(format!("running service command: {e}")))?;
    if !status.success() {
        return Err(service_error(format!("service command exited with {status}")));
    }
    Ok(())
}

fn service_error(message: impl Into<String>) -> AppError {
    AppError::new("GATEWAY_SERVICE", ExitCode::Precondition, message)
}

pub fn status(home: &A2aHome, json_mode: bool) -> Result<Outcome, AppError> {
    let root = gateway_root(home);
    if lock_is_free(&root) {
        return Err(AppError::new(
            "GATEWAY_UNAVAILABLE",
            ExitCode::BrokerUnavailable,
            "mesh gateway is not running",
        )
        .remedy("run `a2a mesh gateway run`"));
    }
    let health = GatewayHealth::read(home).ok_or_else(|| {
        AppError::new(
            "GATEWAY_HEALTH_MISSING",
            ExitCode::BrokerUnavailable,
            "mesh gateway is running but has not written health yet",
        )
    })?;
    if json_mode {
        return Ok(Outcome::from(json!({"running": true, "health": health})));
    }
    Ok(Outcome::from(Value::String(format!(
        "mesh gateway up (pid {}, state {}, health_seq {}, listen_port {}, expose {}, links {})",
        health.pid,
        health.state,
        health.health_seq,
        health.listen_port,
        health.expose,
        health.links.len()
    ))))
}

pub fn links(home: &A2aHome, json_mode: bool, quiet: bool) -> Result<Outcome, AppError> {
    let root = gateway_root(home);
    if lock_is_free(&root) {
        if !quiet {
            eprintln!("a2a: mesh gateway is not running; showing []");
            eprintln!("a2a: hint: run `a2a mesh gateway run`");
        }
        return Ok(Outcome::from(json!([])));
    }
    let Some(health) = GatewayHealth::read(home) else {
        if !quiet {
            eprintln!("a2a: mesh gateway health is not available yet; showing []");
        }
        return Ok(Outcome::from(json!([])));
    };
    if json_mode {
        return Ok(Outcome::from(json!(health.links)));
    }
    if health.links.is_empty() {
        return Ok(Outcome::from(json!([])));
    }
    Ok(Outcome::from(Value::String(
        health
            .links
            .iter()
            .map(format_link_health)
            .collect::<Vec<_>>()
            .join("\n"),
    )))
}

fn format_link_health(link: &link::LinkHealth) -> String {
    format!(
        "{} {} established_at={} last_rx={} last_tx={} frames_rx={} frames_tx={}",
        link.peer,
        link.direction.as_str(),
        link.established_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        format_link_time(link.last_rx),
        format_link_time(link.last_tx),
        link.frames_rx,
        link.frames_tx
    )
}

fn format_link_time(ts: Option<DateTime<Utc>>) -> String {
    ts.map(|ts| ts.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_else(|| "-".to_string())
}

pub fn stop(home: &A2aHome) -> Result<Outcome, AppError> {
    let root = gateway_root(home);
    if lock_is_free(&root) {
        return Ok(Outcome::from(
            json!({"stopped": false, "already_stopped": true}),
        ));
    }
    let pid = read_pidfile(&root)
        .ok()
        .or_else(|| GatewayHealth::read(home).map(|h| h.pid))
        .ok_or_else(|| {
            AppError::new(
                "GATEWAY_PID_MISSING",
                ExitCode::BrokerUnavailable,
                "mesh gateway is running but no pid is recorded",
            )
        })?;
    kill(Pid::from_raw(pid as i32), Signal::SIGTERM).map_err(|e| {
        AppError::new(
            "GATEWAY_STOP",
            ExitCode::BrokerUnavailable,
            format!("signalling gateway pid {pid}: {e}"),
        )
    })?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(|e| {
            AppError::new(
                "INTERNAL",
                ExitCode::Internal,
                format!("starting stop runtime: {e}"),
            )
        })?;
    let released = rt.block_on(wait_for_lock_release(&root, Duration::from_secs(12)));
    if !released {
        return Err(AppError::new(
            "GATEWAY_STOP_TIMEOUT",
            ExitCode::BrokerUnavailable,
            "mesh gateway did not release its lock after SIGTERM",
        ));
    }
    Ok(Outcome::from(json!({"stopped": true, "pid": pid})))
}

async fn supervise(
    home: &A2aHome,
    root: PathBuf,
    cfg: Config,
    quiet: bool,
) -> Result<DaemonOutcome, AppError> {
    let supervisor_token = CancellationToken::new();
    let tracker = TaskTracker::new();
    let log = GatewayLog::start(root.clone());
    let registry_runtime = Arc::new(bootstrap_registry_runtime(home, &cfg)?);
    let link_state = LinkStateHandle::new(
        home,
        Duration::from_secs(cfg.mesh.gateway.outbound_retry_max_secs),
    )?;
    let (link_observer, mut link_events_rx) = link::LinkObserver::channel(LOG_CAPACITY);
    let runtime_cfg =
        GatewayRuntimeConfig::from_config(home, &cfg, registry_runtime, link_state, link_observer);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| {
            AppError::new(
                "GATEWAY_SIGNAL",
                ExitCode::Internal,
                format!("installing SIGTERM handler: {e}"),
            )
        })?;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let mut health = GatewayHealth::initial(&cfg);
    health.state = "running".to_string();
    write_health(&root, &mut health).await?;
    log.line(json!({"ts": Utc::now(), "kind": "started", "pid": health.pid}))
        .await;

    let (events_tx, mut events_rx) = mpsc::channel::<LoopEvent>(LOG_CAPACITY);
    let link_loop_tx = events_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = link_events_rx.recv().await {
            if link_loop_tx.send(LoopEvent::Link(event)).await.is_err() {
                break;
            }
        }
    });
    let mut loops = JoinSet::new();
    let loop_tx = events_tx.clone();
    for slot in LoopSlot::ALL {
        spawn_loop(
            &mut loops,
            &tracker,
            slot,
            supervisor_token.child_token(),
            loop_tx.clone(),
            runtime_cfg.clone(),
        );
    }
    drop(events_tx);
    let mut restarts: BTreeMap<LoopSlot, VecDeque<Instant>> = BTreeMap::new();
    let mut registry_error = None;
    let mut last_health_flush = Instant::now();

    let outcome = loop {
        tokio::select! {
            _ = &mut ctrl_c => {
                break shutdown(root.clone(), &log, &mut health, supervisor_token.clone(), &tracker, runtime_cfg.shutdown_grace_secs, quiet).await?;
            }
            _ = sigterm.recv() => {
                break shutdown(root.clone(), &log, &mut health, supervisor_token.clone(), &tracker, runtime_cfg.shutdown_grace_secs, quiet).await?;
            }
            Some(event) = events_rx.recv() => {
                match event {
                    LoopEvent::Heartbeat(slot) => {
                        if let Some(slot_health) = health.loops.get_mut(&slot) {
                            slot_health.state = "running".to_string();
                            slot_health.heartbeats = slot_health.heartbeats.saturating_add(1);
                            slot_health.last_heartbeat = Some(Utc::now());
                        }
                        // Heartbeats arrive at ~10Hz per loop; flushing each one
                        // rewrites the health file ~50x/sec. Coalesce to at most
                        // one heartbeat-driven flush per HEALTH_FLUSH_INTERVAL.
                        if last_health_flush.elapsed() >= HEALTH_FLUSH_INTERVAL {
                            write_health(&root, &mut health).await?;
                            last_health_flush = Instant::now();
                        }
                    }
                    LoopEvent::Endpoint(endpoint_url) => {
                        health.endpoint_url = endpoint_url;
                        write_health(&root, &mut health).await?;
                        last_health_flush = Instant::now();
                    }
                    LoopEvent::ExposureError(change) => {
                        apply_exposure_error_change(&mut health, &change);
                        write_health(&root, &mut health).await?;
                        last_health_flush = Instant::now();
                        log.line(exposure_error_log_value(LoopSlot::Registry, &change)).await;
                    }
                    LoopEvent::RegistryError(report) => {
                        if let Some((slot, change)) = apply_registry_error_report(
                            &mut health,
                            &mut registry_error,
                            report,
                        ) {
                            write_health(&root, &mut health).await?;
                            last_health_flush = Instant::now();
                            log.line(registry_error_log_value(slot, &change)).await;
                        }
                    }
                    LoopEvent::RowSelfTamper { gateway_id, verdict } => {
                        log.line(json!({
                            "ts": Utc::now(),
                            "kind": "row_self_tamper_detected",
                            "gateway_id": gateway_id,
                            "verdict": verdict,
                        })).await;
                    }
                    LoopEvent::Replication(replication) => {
                        health.replication = replication;
                        write_health(&root, &mut health).await?;
                        last_health_flush = Instant::now();
                    }
                    LoopEvent::Link(event) => {
                        apply_link_health_event(&mut health, event);
                        write_health(&root, &mut health).await?;
                        last_health_flush = Instant::now();
                    }
                    LoopEvent::SessionEnd { slot, detail } => {
                        log.line(json!({
                            "ts": Utc::now(),
                            "kind": "link_session_end",
                            "loop": slot.as_str(),
                            "detail": detail,
                        })).await;
                    }
                }
            }
            Some(exited) = loops.join_next(), if !loops.is_empty() => {
                let (slot, detail) = loop_exit(exited);
                if supervisor_token.is_cancelled() {
                    continue;
                }
                if let Some(slot_health) = health.loops.get_mut(&slot) {
                    slot_health.state = "restarting".to_string();
                    slot_health.last_exit = Some(detail.clone());
                }
                log.line(json!({
                    "ts": Utc::now(),
                    "kind": "loop_exit",
                    "loop": slot.as_str(),
                    "restart_policy": slot.policy(),
                    "detail": detail,
                })).await;

                let history = restarts.entry(slot).or_default();
                let now = Instant::now();
                while history.front().map(|t| now.duration_since(*t) > RESTART_WINDOW).unwrap_or(false) {
                    history.pop_front();
                }
                history.push_back(now);
                if history.len() > MAX_RESTARTS {
                    health.state = "escalated".to_string();
                    health.escalation = Some(json!({
                        "loop": slot.as_str(),
                        "restart_policy": slot.policy(),
                        "max_restarts": MAX_RESTARTS,
                        "within_secs": RESTART_WINDOW.as_secs(),
                        "detail": detail,
                    }));
                    write_health(&root, &mut health).await?;
                    log.line(json!({
                        "ts": Utc::now(),
                        "kind": "escalation",
                        "loop": slot.as_str(),
                        "restart_policy": slot.policy(),
                    })).await;
                    supervisor_token.cancel();
                    tracker.close();
                    let _ = tokio::time::timeout(Duration::from_secs(1), tracker.wait()).await;
                    break DaemonOutcome::NonZero(json!({
                        "started": true,
                        "shutdown": {"cause": "escalated", "loop": slot.as_str()}
                    }));
                }

                let restart_count = {
                    let slot_health = health.loops.get_mut(&slot).expect("loop health seeded");
                    slot_health.restarts = slot_health.restarts.saturating_add(1);
                    slot_health.restarts
                };
                if slot == LoopSlot::Dialer {
                    log.line(json!({
                        "ts": Utc::now(),
                        "kind": "links_restarted",
                        "restart_policy": "rest-for-one"
                    })).await;
                }
                write_health(&root, &mut health).await?;
                tokio::time::sleep(jittered_backoff(restart_count, runtime_cfg.outbound_retry_max_secs)).await;
                spawn_loop(
                    &mut loops,
                    &tracker,
                    slot,
                    supervisor_token.child_token(),
                    loop_tx.clone(),
                    runtime_cfg.clone(),
                );
            }
        }
    };
    drop(log);
    Ok(outcome)
}

struct GatewayRegistryRuntime {
    registry: RegistryHandle,
    exposure: ExposureProvider,
    row: ExposureRow,
    signing: RowSigningContext,
    heartbeat_interval: Duration,
}

fn bootstrap_registry_runtime(
    home: &A2aHome,
    cfg: &Config,
) -> Result<GatewayRegistryRuntime, AppError> {
    let registry_url = read_gateway_registry_env(home)?;
    let registry = registry::open_registry(
        &registry_url,
        registry::reader_roots(home)?,
        keys::fleet_id(home)?,
    )?
    .with_reader_state(
        RowMarkStore::open(home)?,
        RowVerifyCfg::from_config(Utc::now(), cfg),
    );
    let exposure = ExposureProvider::from_config(cfg)?;
    Ok(GatewayRegistryRuntime {
        registry,
        exposure,
        row: registry_exposure_row(home)?,
        signing: RowSigningContext::from_config(home, cfg),
        heartbeat_interval: Duration::from_secs(cfg.mesh.gateway.registry_heartbeat_secs),
    })
}

pub(crate) fn registry_exposure_row(home: &A2aHome) -> Result<ExposureRow, AppError> {
    let identity = keys::ensure_and_verify_gateway_enrollment(home, &SystemClock)?;
    Ok(ExposureRow {
        fleet_id: keys::fleet_id(home)?,
        gateway_id: identity.gateway_id,
        owner: gateway_owner(),
        pubkey_fpr: keys::gateway_public_key_fingerprint(home)?,
        cert_fpr: identity.cert_fpr,
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

fn read_gateway_registry_env(home: &A2aHome) -> Result<String, AppError> {
    registry::read_registry_env(home).map_err(|err| {
        if err.code == "REGISTRY_URL_REQUIRED" {
            AppError::new("REGISTRY_UNCONFIGURED", ExitCode::Precondition, err.message)
                .remedy("run `a2a mesh fleet init --registry -`")
        } else {
            err
        }
    })
}

fn gateway_owner() -> String {
    std::env::var("A2A_OWNER")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "unknown".to_string())
}

fn apply_exposure_error_change(health: &mut GatewayHealth, change: &ExposureErrorChange) {
    health.exposure_error = change.error.clone();
    health.exposure_error_since = change.error.as_ref().and(change.since);
}

fn apply_link_health_event(health: &mut GatewayHealth, event: link::LinkHealthEvent) {
    match event {
        link::LinkHealthEvent::Up {
            peer,
            direction,
            established_at,
        } => {
            let live = link::LinkHealth {
                peer,
                direction,
                established_at,
                last_rx: None,
                last_tx: None,
                frames_rx: 0,
                frames_tx: 0,
            };
            match health
                .links
                .iter_mut()
                .find(|link| link.peer == live.peer && link.direction == live.direction)
            {
                Some(link) => *link = live,
                None => health.links.push(live),
            }
            health.links.sort_by(|a, b| {
                a.peer
                    .cmp(&b.peer)
                    .then(a.direction.as_str().cmp(b.direction.as_str()))
            });
        }
        link::LinkHealthEvent::Stat {
            peer,
            direction,
            last_rx,
            last_tx,
            frames_rx,
            frames_tx,
        } => {
            if let Some(link) = health
                .links
                .iter_mut()
                .find(|link| link.peer == peer && link.direction == direction)
            {
                if frames_rx > 0 {
                    link.frames_rx = link.frames_rx.saturating_add(frames_rx);
                    link.last_rx = last_rx;
                }
                if frames_tx > 0 {
                    link.frames_tx = link.frames_tx.saturating_add(frames_tx);
                    link.last_tx = last_tx;
                }
            }
        }
        link::LinkHealthEvent::Down { peer, direction } => {
            health
                .links
                .retain(|link| link.peer != peer || link.direction != direction);
        }
    }
}

fn exposure_error_log_value(slot: LoopSlot, change: &ExposureErrorChange) -> Value {
    match &change.error {
        Some(error) => json!({
            "ts": Utc::now(),
            "kind": "exposure_error",
            "loop": slot.as_str(),
            "state": "down",
            "error": error,
            "since": change.since,
        }),
        None => json!({
            "ts": Utc::now(),
            "kind": "exposure_error",
            "loop": slot.as_str(),
            "state": "recovered",
            "since": change.since,
            "suppressed": change.suppressed,
        }),
    }
}

fn registry_error_down(slot: LoopSlot, err: &AppError) -> LoopEvent {
    LoopEvent::RegistryError(RegistryErrorReport {
        slot,
        error: Some(format!("{}: {}", err.code, err.message)),
        at: Utc::now(),
    })
}

fn registry_error_recovered(slot: LoopSlot) -> LoopEvent {
    LoopEvent::RegistryError(RegistryErrorReport {
        slot,
        error: None,
        at: Utc::now(),
    })
}

fn apply_registry_error_report(
    health: &mut GatewayHealth,
    state: &mut Option<RegistryErrorState>,
    report: RegistryErrorReport,
) -> Option<(LoopSlot, RegistryErrorChange)> {
    match report.error {
        Some(error) => match state {
            Some(state) => {
                state.message = error.clone();
                state.suppressed = state.suppressed.saturating_add(1);
                health.registry_error = Some(error);
                health.registry_error_since = Some(state.since);
                None
            }
            None => {
                *state = Some(RegistryErrorState {
                    message: error.clone(),
                    since: report.at,
                    suppressed: 0,
                });
                health.registry_error = Some(error.clone());
                health.registry_error_since = Some(report.at);
                Some((
                    report.slot,
                    RegistryErrorChange {
                        error: Some(error),
                        since: Some(report.at),
                        suppressed: 0,
                    },
                ))
            }
        },
        None => state.take().map(|state| {
            health.registry_error = None;
            health.registry_error_since = None;
            (
                report.slot,
                RegistryErrorChange {
                    error: None,
                    since: Some(state.since),
                    suppressed: state.suppressed,
                },
            )
        }),
    }
}

fn registry_error_log_value(slot: LoopSlot, change: &RegistryErrorChange) -> Value {
    match &change.error {
        Some(error) => json!({
            "ts": Utc::now(),
            "kind": "registry_error",
            "loop": slot.as_str(),
            "state": "down",
            "error": error,
            "since": change.since,
        }),
        None => json!({
            "ts": Utc::now(),
            "kind": "registry_error",
            "loop": slot.as_str(),
            "state": "recovered",
            "since": change.since,
            "suppressed": change.suppressed,
        }),
    }
}

async fn shutdown(
    root: PathBuf,
    log: &GatewayLog,
    health: &mut GatewayHealth,
    token: CancellationToken,
    tracker: &TaskTracker,
    shutdown_grace_secs: u64,
    quiet: bool,
) -> Result<DaemonOutcome, AppError> {
    if !quiet {
        eprintln!("a2a mesh gateway: shutting down");
    }
    health.state = "shutting-down".to_string();
    let links = health.links.len();
    health.links.clear();
    health.shutdown = Some(json!({"phase": "stop_accepting", "links": links}));
    write_health(&root, health).await?;
    log.line(json!({
        "ts": Utc::now(),
        "kind": "stop_accepting",
        "links": links
    }))
    .await;
    log.line(json!({
        "ts": Utc::now(),
        "kind": "link_close",
        "err": {"code": "SHUTTING_DOWN"},
        "links": links
    }))
    .await;

    token.cancel();
    tracker.close();
    let grace = Duration::from_secs(shutdown_grace_secs);
    let graceful = tokio::time::timeout(grace, tracker.wait()).await.is_ok();
    if graceful {
        health.state = "stopped".to_string();
        health.shutdown = Some(json!({"graceful": true, "outstanding_tasks": 0}));
        write_health(&root, health).await?;
        log.line(json!({
            "ts": Utc::now(),
            "kind": "stopped",
            "graceful": true
        }))
        .await;
        Ok(DaemonOutcome::Graceful(json!({
            "started": true,
            "shutdown": {"cause": "signal", "graceful": true}
        })))
    } else {
        let outstanding = 1;
        health.state = "drain-timeout".to_string();
        health.shutdown = Some(json!({"graceful": false, "outstanding_tasks": outstanding}));
        write_health(&root, health).await?;
        log.line(json!({
            "ts": Utc::now(),
            "kind": "drain_timeout",
            "outstanding_tasks": outstanding
        }))
        .await;
        Ok(DaemonOutcome::NonZero(json!({
            "started": true,
            "shutdown": {
                "cause": "signal",
                "graceful": false,
                "outstanding_tasks": outstanding
            }
        })))
    }
}

fn spawn_loop(
    loops: &mut JoinSet<(LoopSlot, Result<(), String>)>,
    tracker: &TaskTracker,
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) {
    let future = async move {
        let result = AssertUnwindSafe(run_loop(slot, token, tx, cfg))
            .catch_unwind()
            .await
            .map(|result| result.map_err(|err| format!("{}: {}", err.code, err.message)));
        let result = result.unwrap_or_else(|_| Err("panicked".to_string()));
        (slot, result)
    };
    let tracked = tracker.track_future(future);
    loops.spawn(tracked);
}

async fn run_loop(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) -> Result<(), AppError> {
    match slot {
        LoopSlot::Registry => registry_loop(slot, token, tx, cfg).await,
        LoopSlot::Server => server_loop(slot, token, tx, cfg).await,
        LoopSlot::Dialer => dialer_loop(slot, token, tx, cfg).await,
        LoopSlot::Watcher => watcher_loop(slot, token, tx, cfg).await,
        LoopSlot::AntiEntropy => anti_entropy_loop(slot, token, tx, cfg).await,
        LoopSlot::Work => work_loop(slot, token, tx, cfg).await,
    }
}

async fn registry_loop(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) -> Result<(), AppError> {
    if cfg.panic_slot == Some(slot) {
        panic!("gateway stub loop {slot} panicked by test hook");
    }
    let registry_runtime = Arc::clone(&cfg.registry);
    registry_exposure_loop(
        slot,
        token,
        tx,
        RegistrySlotParams {
            registry: &registry_runtime.registry,
            exposure: &registry_runtime.exposure,
            publisher: ExposurePublisher::new(registry_runtime.row.clone())
                .with_signing(registry_runtime.signing.clone()),
            heartbeat_interval: registry_runtime.heartbeat_interval,
            clock: &SystemClock,
            stall_shutdown_slot: cfg.stall_shutdown_slot,
        },
    )
    .await
}

pub struct RegistrySlotParams<'a, R, E, C: ?Sized> {
    pub registry: &'a R,
    pub exposure: &'a E,
    pub publisher: ExposurePublisher,
    pub heartbeat_interval: Duration,
    pub clock: &'a C,
    pub stall_shutdown_slot: Option<LoopSlot>,
}

async fn registry_exposure_loop<R, E, C>(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    params: RegistrySlotParams<'_, R, E, C>,
) -> Result<(), AppError>
where
    R: Registry,
    E: Exposure,
    C: Clock + ?Sized,
{
    let RegistrySlotParams {
        registry,
        exposure,
        mut publisher,
        heartbeat_interval,
        clock,
        stall_shutdown_slot,
    } = params;
    let _ = tx.send(LoopEvent::Heartbeat(slot)).await;
    let (exposure_tx, mut exposure_rx) = mpsc::channel(16);
    let gateway_id = publisher.gateway_id().to_string();
    let exposure_loop = expose::run_exposure_loop_with_events(
        registry,
        exposure,
        &mut publisher,
        heartbeat_interval,
        clock,
        token.clone(),
        Some(exposure_tx),
    );
    tokio::pin!(exposure_loop);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered_heartbeat(HEARTBEAT_INTERVAL)) => {
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
            }
            Some(tick) = exposure_rx.recv() => {
                if let Some(verdict) = tick.self_tamper.clone() {
                    if tx.send(LoopEvent::RowSelfTamper {
                        gateway_id: gateway_id.clone(),
                        verdict,
                    }).await.is_err() {
                        token.cancel();
                        return Ok(());
                    }
                }
                let registry_report = RegistryErrorReport {
                    slot,
                    error: tick.registry_error.clone(),
                    at: tick.registry_error_since.unwrap_or_else(Utc::now),
                };
                if tx.send(LoopEvent::RegistryError(registry_report)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
                if let Some(change) = tick.exposure_error_change {
                    if tx.send(LoopEvent::ExposureError(change)).await.is_err() {
                        token.cancel();
                        return Ok(());
                    }
                }
                if tx.send(LoopEvent::Endpoint(tick.endpoint_url)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
            }
            result = &mut exposure_loop => {
                // The exposure loop observes cancellation only between complete publisher
                // ticks, so discover -> self-check -> repair is never dropped half-finished.
                if token.is_cancelled() && stall_shutdown_slot == Some(slot) {
                    std::future::pending::<()>().await;
                }
                return result;
            }
        }
    }
}

#[doc(hidden)]
pub async fn run_registry_slot_for_test<R, E, C>(
    root: &Path,
    cfg: &Config,
    params: RegistrySlotParams<'_, R, E, C>,
    token: CancellationToken,
) -> Result<(), AppError>
where
    R: Registry,
    E: Exposure,
    C: Clock + ?Sized,
{
    let mut health = GatewayHealth::initial(cfg);
    health.state = "running".to_string();
    write_health(root, &mut health).await?;

    let (tx, mut rx) = mpsc::channel::<LoopEvent>(LOG_CAPACITY);
    let run = registry_exposure_loop(LoopSlot::Registry, token, tx, params);
    tokio::pin!(run);
    let mut registry_error = None;
    loop {
        tokio::select! {
            result = &mut run => return result,
            Some(event) = rx.recv() => {
                match event {
                    LoopEvent::Heartbeat(slot) => {
                        if let Some(slot_health) = health.loops.get_mut(&slot) {
                            slot_health.state = "running".to_string();
                            slot_health.heartbeats = slot_health.heartbeats.saturating_add(1);
                            slot_health.last_heartbeat = Some(Utc::now());
                        }
                        write_health(root, &mut health).await?;
                    }
                    LoopEvent::Endpoint(endpoint_url) => {
                        health.endpoint_url = endpoint_url;
                        write_health(root, &mut health).await?;
                    }
                    LoopEvent::ExposureError(change) => {
                        apply_exposure_error_change(&mut health, &change);
                        write_health(root, &mut health).await?;
                        write_jsonl(root, exposure_error_log_value(LoopSlot::Registry, &change));
                    }
                    LoopEvent::RegistryError(report) => {
                        if let Some((slot, change)) = apply_registry_error_report(
                            &mut health,
                            &mut registry_error,
                            report,
                        ) {
                            write_health(root, &mut health).await?;
                            write_jsonl(root, registry_error_log_value(slot, &change));
                        }
                    }
                    LoopEvent::RowSelfTamper { gateway_id, verdict } => {
                        write_jsonl(root, json!({
                            "ts": Utc::now(),
                            "kind": "row_self_tamper_detected",
                            "gateway_id": gateway_id,
                            "verdict": verdict,
                        }));
                    }
                    LoopEvent::SessionEnd { .. } => {}
                    LoopEvent::Replication(replication) => {
                        health.replication = replication;
                        write_health(root, &mut health).await?;
                    }
                    LoopEvent::Link(event) => {
                        apply_link_health_event(&mut health, event);
                        write_health(root, &mut health).await?;
                    }
                }
            }
        }
    }
}

async fn server_loop(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) -> Result<(), AppError> {
    if cfg.panic_slot == Some(slot) {
        panic!("gateway stub loop {slot} panicked by test hook");
    }
    let listener = TcpListener::bind(("0.0.0.0", cfg.config.mesh.gateway.listen_port as u16))
        .await
        .map_err(|e| {
            if cfg.server_bind_optional {
                AppError::new(
                    "GATEWAY_BIND_OPTIONAL",
                    ExitCode::BrokerUnavailable,
                    format!("optional mesh gateway listener unavailable: {e}"),
                )
            } else {
                AppError::new(
                    "GATEWAY_BIND",
                    ExitCode::BrokerUnavailable,
                    format!("binding mesh gateway listener: {e}"),
                )
            }
        });
    let listener = match listener {
        Ok(listener) => listener,
        Err(err) if err.code == "GATEWAY_BIND_OPTIONAL" => {
            return heartbeat_only_loop(slot, token, tx, cfg).await;
        }
        Err(err) => return Err(err),
    };
    let tls = load_tls_identity(&cfg.home)?;
    let identity = load_handshake_identity(&cfg.home)?;
    let roots = keys::RootSet::load(&cfg.home)?;
    let mut sessions = JoinSet::new();
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    let _ = tx.send(LoopEvent::Heartbeat(slot)).await;
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                if cfg.stall_shutdown_slot == Some(slot) {
                    std::future::pending::<()>().await;
                }
                while let Some(joined) = sessions.join_next().await {
                    // A failing session must not turn a clean shutdown into an
                    // error; the link is going away regardless.
                    let _ = joined;
                }
                return Ok(());
            }
            _ = heartbeat.tick() => {
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|e| AppError::new(
                    "GATEWAY_ACCEPT",
                    ExitCode::BrokerUnavailable,
                    format!("accepting gateway link: {e}"),
                ))?;
                let tls = tls.clone();
                let identity = identity.clone();
                let roots = roots.clone();
                let params_home = cfg.home.clone();
                let params_cfg = cfg.config.clone();
                let local = cfg.registry.row.gateway_id.clone();
                let state = cfg.links.clone();
                let observer = cfg.link_observer.clone();
                let child = token.child_token();
                sessions.spawn(async move {
                    let mut side = link::accept_once_io(stream, tls).await?;
                    let peer = crate::mesh::wire::handshake_acceptor(
                        &mut side,
                        &identity,
                        &roots,
                        &SystemClock,
                    )
                    .await?;
                    let params = LinkSessionParams::new(
                        params_home,
                        params_cfg,
                        local,
                        peer.gateway_id.clone(),
                        LinkDirection::Inbound,
                        state,
                        child,
                    )
                    .with_observer(observer);
                    link::run_link_session(side, params).await
                });
            }
            Some(joined) = sessions.join_next(), if !sessions.is_empty() => {
                // One bad connection (port scan, TLS garbage, peer crash) must
                // not kill the whole loop; log it and keep serving.
                let detail = match joined {
                    Ok(Ok(())) => None,
                    Ok(Err(err)) => Some(format!("{}: {}", err.code, err.message)),
                    Err(err) => Some(join_to_app_err(err).message),
                };
                if let Some(detail) = detail {
                    let _ = tx.send(LoopEvent::SessionEnd { slot, detail }).await;
                }
            }
        }
    }
}

async fn heartbeat_only_loop(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) -> Result<(), AppError> {
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    let _ = tx.send(LoopEvent::Heartbeat(slot)).await;
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                if cfg.stall_shutdown_slot == Some(slot) {
                    std::future::pending::<()>().await;
                }
                return Ok(());
            }
            _ = heartbeat.tick() => {
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
            }
        }
    }
}

async fn dialer_loop(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) -> Result<(), AppError> {
    if cfg.panic_slot == Some(slot) {
        panic!("gateway stub loop {slot} panicked by test hook");
    }
    let mut sessions = JoinSet::new();
    let mut attempts = BTreeMap::<String, u32>::new();
    let mut next_dial_after = BTreeMap::<String, Instant>::new();
    let mut last_seen = BTreeMap::<String, DialPeerRowIdentity>::new();
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    let mut poll = tokio::time::interval(Duration::from_secs(
        cfg.config.mesh.gateway.replication_poll_secs.max(1),
    ));
    let _ = tx.send(LoopEvent::Heartbeat(slot)).await;
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                if cfg.stall_shutdown_slot == Some(slot) {
                    std::future::pending::<()>().await;
                }
                while let Some(joined) = sessions.join_next().await {
                    // A failing session must not turn a clean shutdown into an
                    // error; the link is going away regardless.
                    let _ = joined;
                }
                return Ok(());
            }
            _ = heartbeat.tick() => {
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
            }
            _ = poll.tick() => {
                let rows = match cfg.registry.registry.discover().await {
                    Ok(rows) => {
                        if tx.send(registry_error_recovered(slot)).await.is_err() {
                            token.cancel();
                            return Ok(());
                        }
                        rows
                    }
                    Err(err) => {
                        if tx.send(registry_error_down(slot, &err)).await.is_err() {
                            token.cancel();
                            return Ok(());
                        }
                        continue;
                    }
                };
                let visible_rows = rows
                    .iter()
                    .map(|(row, _verdict)| row.clone())
                    .collect::<Vec<_>>();
                for peer in refresh_dial_state_maps(
                    &mut last_seen,
                    &visible_rows,
                    &mut attempts,
                    &mut next_dial_after,
                ) {
                    cfg.links.reset_peer_damping(&peer).await;
                }
                cfg.links.refresh_registry_cache(&cfg.home, &rows, Utc::now()).await?;
                for row in link::discover_outbound_peers(
                    &cfg.registry.row.gateway_id,
                    &rows,
                    cfg.config.mesh.gateway.require_signed_rows,
                ) {
                    if next_dial_after
                        .get(&row.gateway_id)
                        .is_some_and(|next| Instant::now() < *next)
                    {
                        continue;
                    }
                    if cfg.links.is_live(&row.gateway_id).await || !cfg.links.can_dial(&row.gateway_id, Utc::now()).await {
                        continue;
                    }
                    match dial_and_spawn(row.clone(), &cfg, &token, &mut sessions).await {
                        Ok(()) => {
                            attempts.remove(&row.gateway_id);
                        }
                        Err(err) => {
                            let _ = tx
                                .send(LoopEvent::SessionEnd {
                                    slot,
                                    detail: format!(
                                        "dial {} failed: {}: {}",
                                        row.gateway_id, err.code, err.message
                                    ),
                                })
                                .await;
                            let attempt = attempts.entry(row.gateway_id.clone()).or_default();
                            *attempt = attempt.saturating_add(1);
                            let cooldown = link::jittered_redial_backoff(
                                *attempt,
                                Duration::from_secs(cfg.config.mesh.gateway.outbound_retry_max_secs),
                                &mut rand::thread_rng(),
                            );
                            next_dial_after
                                .insert(row.gateway_id.clone(), Instant::now() + cooldown);
                            let _ = cfg.links.record_drop(
                                &row.gateway_id,
                                Utc::now(),
                                cooldown,
                            ).await;
                        }
                    }
                }
            }
            Some(joined) = sessions.join_next(), if !sessions.is_empty() => {
                // One bad connection (port scan, TLS garbage, peer crash) must
                // not kill the whole loop; log it and keep serving.
                let detail = match joined {
                    Ok(Ok(())) => None,
                    Ok(Err(err)) => Some(format!("{}: {}", err.code, err.message)),
                    Err(err) => Some(join_to_app_err(err).message),
                };
                if let Some(detail) = detail {
                    let _ = tx.send(LoopEvent::SessionEnd { slot, detail }).await;
                }
            }
        }
    }
}

async fn dial_and_spawn(
    row: registry::GatewayRow,
    cfg: &GatewayRuntimeConfig,
    token: &CancellationToken,
    sessions: &mut JoinSet<Result<(), AppError>>,
) -> Result<(), AppError> {
    let mut side = link::dial_gateway(&row).await?;
    let identity = load_handshake_identity(&cfg.home)?;
    let roots = keys::RootSet::load(&cfg.home)?;
    let peer =
        crate::mesh::wire::handshake_dialer(&mut side, &identity, &roots, &SystemClock).await?;
    let params = LinkSessionParams::new(
        cfg.home.clone(),
        cfg.config.clone(),
        cfg.registry.row.gateway_id.clone(),
        peer.gateway_id,
        LinkDirection::Outbound,
        cfg.links.clone(),
        token.child_token(),
    )
    .with_observer(cfg.link_observer.clone());
    sessions.spawn(link::run_link_session(side, params));
    Ok(())
}

async fn watcher_loop(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) -> Result<(), AppError> {
    if cfg.panic_slot == Some(slot) {
        panic!("gateway stub loop {slot} panicked by test hook");
    }
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    let mut poll = tokio::time::interval(Duration::from_secs(
        cfg.config.mesh.gateway.replication_poll_secs.max(1),
    ));
    let _ = tx.send(LoopEvent::Heartbeat(slot)).await;
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                if cfg.stall_shutdown_slot == Some(slot) {
                    std::future::pending::<()>().await;
                }
                return Ok(());
            }
            _ = heartbeat.tick() => {
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
            }
            _ = poll.tick() => {
                link::push_watcher_deltas(&cfg.home, &cfg.config, &cfg.registry.row.gateway_id, &cfg.links).await?;
                link::push_spooled_notes(&cfg.home, &cfg.links).await?;
                link::push_presence_gossip(&cfg.home, &cfg.registry.row.gateway_id, &cfg.links).await?;
            }
        }
    }
}

async fn anti_entropy_loop(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) -> Result<(), AppError> {
    if cfg.panic_slot == Some(slot) {
        panic!("gateway stub loop {slot} panicked by test hook");
    }
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    let mut anti_entropy = tokio::time::interval(Duration::from_secs(
        cfg.config.mesh.gateway.anti_entropy_secs.max(1),
    ));
    let _ = tx.send(LoopEvent::Heartbeat(slot)).await;
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                if cfg.stall_shutdown_slot == Some(slot) {
                    std::future::pending::<()>().await;
                }
                return Ok(());
            }
            _ = heartbeat.tick() => {
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
            }
            _ = anti_entropy.tick() => {
                link::push_anti_entropy_digest(&cfg.home, &cfg.links).await?;
                match cfg.registry.registry.discover().await {
                    Ok(rows) => {
                        if tx.send(registry_error_recovered(slot)).await.is_err() {
                            token.cancel();
                            return Ok(());
                        }
                        cfg.links
                            .refresh_registry_cache(&cfg.home, &rows, Utc::now())
                            .await?;
                    }
                    Err(err) => {
                        if tx.send(registry_error_down(slot, &err)).await.is_err() {
                            token.cancel();
                            return Ok(());
                        }
                    }
                }
                presence_wire::retire_stale_origins(
                    &cfg.home,
                    Utc::now(),
                    cfg.config
                        .mesh
                        .gc_after_days
                        .saturating_mul(86_400)
                        .min(i64::MAX as u64) as i64,
                )?;
                let mut cursors = replicate::CursorStore::load_or_rebuild(&cfg.home)?;
                replicate::compact_feed(
                    &cfg.home,
                    &cfg.config,
                    Utc::now(),
                    chrono::Duration::days(cfg.config.mesh.gateway.feed_retain_days as i64),
                    &mut cursors,
                )?;
                tx.send(LoopEvent::Replication(cfg.links.health().await)).await.ok();
            }
        }
    }
}

async fn work_loop(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) -> Result<(), AppError> {
    if cfg.panic_slot == Some(slot) {
        panic!("gateway stub loop {slot} panicked by test hook");
    }
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    let mut poll = tokio::time::interval(Duration::from_secs(
        cfg.config.mesh.work.work_poll_secs.max(1),
    ));
    let _ = tx.send(LoopEvent::Heartbeat(slot)).await;
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                if cfg.stall_shutdown_slot == Some(slot) {
                    std::future::pending::<()>().await;
                }
                return Ok(());
            }
            _ = heartbeat.tick() => {
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
            }
            _ = poll.tick() => {
                let report = super::work_exec::work_loop_tick(
                    &cfg.home,
                    &cfg.config,
                    &cfg.registry.registry,
                    &super::work_exec::RealWorkCompletionIo,
                    super::work_exec::WorkLoopOptions::for_config(&cfg.config),
                ).await;
                let event = match report.registry_error {
                    Some(error) => LoopEvent::RegistryError(RegistryErrorReport {
                        slot,
                        error: Some(error),
                        at: Utc::now(),
                    }),
                    None => registry_error_recovered(slot),
                };
                if tx.send(event).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    token.cancel();
                    return Ok(());
                }
            }
        }
    }
}

fn load_handshake_identity(
    home: &A2aHome,
) -> Result<crate::mesh::wire::HandshakeIdentity, AppError> {
    Ok(crate::mesh::wire::HandshakeIdentity::new(
        keys::gateway_token(home)?,
        keys::gateway_signing_key(home)?,
    ))
}

fn load_tls_identity(home: &A2aHome) -> Result<crate::mesh::wire::TlsIdentity, AppError> {
    let dir = home.root().join("mesh").join("gateway");
    crate::mesh::wire::TlsIdentity::from_pem_files(&dir.join("gw.crt"), &dir.join("gw.tls.key"))
}

fn join_to_app_err(err: JoinError) -> AppError {
    AppError::new(
        "GATEWAY_LINK_TASK",
        ExitCode::Internal,
        format!("gateway link task exited unexpectedly: {err}"),
    )
}

fn loop_exit(exited: Result<(LoopSlot, Result<(), String>), JoinError>) -> (LoopSlot, String) {
    match exited {
        Ok((slot, Ok(()))) => (slot, "returned".to_string()),
        Ok((slot, Err(detail))) => (slot, detail),
        Err(err) => {
            let detail = if err.is_panic() {
                "panicked"
            } else if err.is_cancelled() {
                "cancelled"
            } else {
                "exited"
            };
            (LoopSlot::Registry, detail.to_string())
        }
    }
}

async fn write_health(root: &Path, health: &mut GatewayHealth) -> Result<(), AppError> {
    health.health_seq = health.health_seq.saturating_add(1);
    health.written_at = Utc::now();
    let bytes = serde_json::to_vec_pretty(health).map_err(|e| {
        AppError::new(
            "GATEWAY_HEALTH",
            ExitCode::Internal,
            format!("serializing gateway health: {e}"),
        )
    })?;
    let path = root.join(GATEWAY_HEALTH);
    tokio::task::spawn_blocking(move || crate::store::write_atomic(&path, &bytes))
        .await
        .map_err(|e| AppError::new("GATEWAY_HEALTH", ExitCode::Internal, e.to_string()))?
        .map_err(|e| AppError::new("GATEWAY_HEALTH", ExitCode::Internal, e.to_string()))
}

fn write_jsonl(root: &Path, value: Value) {
    if let Ok(mut f) = crate::exec::open_append_0600(&root.join(GATEWAY_JSONL)) {
        let mut bytes = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
        bytes.push(b'\n');
        let _ = std::io::Write::write_all(&mut f, &bytes);
    }
}

fn jittered_backoff(restarts: u64, cap_secs: u64) -> Duration {
    let factor = 2u64.saturating_pow(restarts.min(16) as u32);
    let cap_ms = cap_secs.saturating_mul(1000).max(1);
    let upper = 50u64.saturating_mul(factor).min(cap_ms).max(1);
    Duration::from_millis(rand::thread_rng().gen_range(0..=upper))
}

fn jittered_heartbeat(base: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    let lower = base_ms.saturating_mul(9) / 10;
    let upper = base_ms.saturating_mul(11) / 10;
    Duration::from_millis(rand::thread_rng().gen_range(lower..=upper.max(lower + 1)))
}

pub fn gateway_root(home: &A2aHome) -> PathBuf {
    let root = crate::mesh::mesh_root(home);
    let gateway = root.join("gateway");
    crate::home::create_dir_0700(&gateway).expect("creating mesh gateway dir");
    root
}

fn acquire_lock(root: &Path) -> Result<Option<Flock<File>>, AppError> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = root.join(GATEWAY_LOCK);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .map_err(|e| io_error(&path, e))?;
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(lock) => Ok(Some(lock)),
        Err((_file, nix::errno::Errno::EWOULDBLOCK)) => Ok(None),
        Err((_file, errno)) => Err(AppError::new(
            "GATEWAY_LOCK",
            ExitCode::Internal,
            format!("could not lock {}: {errno}", path.display()),
        )),
    }
}

fn lock_is_free(root: &Path) -> bool {
    let Ok(file) = std::fs::OpenOptions::new()
        .write(true)
        .open(root.join(GATEWAY_LOCK))
    else {
        return true;
    };
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(lock) => {
            drop(lock);
            true
        }
        Err(_) => false,
    }
}

async fn wait_for_lock_release(root: &Path, patience: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + patience;
    loop {
        if lock_is_free(root) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn write_pidfile(root: &Path) -> Result<(), AppError> {
    use std::io::Write;
    let path = root.join(GATEWAY_PID);
    let mut f = crate::home::write_new_0600(&path).map_err(|e| io_error(&path, e))?;
    writeln!(f, "{}", std::process::id()).map_err(|e| io_error(&path, e))
}

fn read_pidfile(root: &Path) -> Result<u32, AppError> {
    let path = root.join(GATEWAY_PID);
    let text = std::fs::read_to_string(&path).map_err(|e| io_error(&path, e))?;
    text.trim().parse::<u32>().map_err(|e| {
        AppError::new(
            "GATEWAY_PID",
            ExitCode::BrokerUnavailable,
            format!("{} did not contain a pid: {e}", path.display()),
        )
    })
}

fn io_error(path: &Path, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mesh_gateway_fields_are_exactly_the_spec_keys() {
        assert_eq!(
            crate::config::MESH_GATEWAY_FIELDS,
            [
                "listen_port",
                "expose",
                "endpoint_url",
                "registry_heartbeat_secs",
                "registry_timeout_secs",
                "replication_poll_secs",
                "outbound_retry_max_secs",
                "ping_secs",
                "anti_entropy_secs",
                "feed_retain_days",
                "shutdown_grace_secs",
                "require_signed_rows",
                "row_resign_secs",
                "max_accepted_row_resign_secs",
            ]
        );
    }

    #[test]
    fn restart_policy_is_pinned_by_slot() {
        assert_eq!(LoopSlot::Registry.policy(), "one-for-one");
        assert_eq!(LoopSlot::Server.policy(), "one-for-one");
        assert_eq!(LoopSlot::Watcher.policy(), "one-for-one");
        assert_eq!(LoopSlot::Dialer.policy(), "rest-for-one");
        assert_eq!(MAX_RESTARTS, 5);
        assert_eq!(RESTART_WINDOW, Duration::from_secs(60));
    }
}
