use crate::config::Config;
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode, Outcome};
use chrono::{DateTime, Utc};
use nix::fcntl::{Flock, FlockArg};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use futures_util::FutureExt;

pub const GATEWAY_LOCK: &str = "gateway.lock";
pub const GATEWAY_PID: &str = "gateway.pid";
pub const GATEWAY_HEALTH: &str = "gateway-health.json";
pub const GATEWAY_JSONL: &str = "gateway.jsonl";
pub const HEALTH_SCHEMA_VERSION: u32 = 1;
pub const MAX_RESTARTS: usize = 5;
pub const RESTART_WINDOW: Duration = Duration::from_secs(60);

const LOG_CAPACITY: usize = 1024;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LoopSlot {
    Registry,
    Server,
    Dialer,
    Watcher,
    AntiEntropy,
}

impl LoopSlot {
    const ALL: [LoopSlot; 5] = [
        LoopSlot::Registry,
        LoopSlot::Server,
        LoopSlot::Dialer,
        LoopSlot::Watcher,
        LoopSlot::AntiEntropy,
    ];

    fn as_str(self) -> &'static str {
        match self {
            LoopSlot::Registry => "registry",
            LoopSlot::Server => "server",
            LoopSlot::Dialer => "dialer",
            LoopSlot::Watcher => "watcher",
            LoopSlot::AntiEntropy => "anti-entropy",
        }
    }

    fn policy(self) -> &'static str {
        match self {
            LoopSlot::Dialer => "rest-for-one",
            LoopSlot::Registry | LoopSlot::Server | LoopSlot::Watcher | LoopSlot::AntiEntropy => {
                "one-for-one"
            }
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
    pub loops: BTreeMap<LoopSlot, LoopHealth>,
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
            endpoint_url: cfg.mesh.gateway.endpoint_url.clone(),
            loops: LoopSlot::ALL
                .into_iter()
                .map(|slot| (slot, LoopHealth::new(slot)))
                .collect(),
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

#[derive(Clone, Debug)]
struct GatewayRuntimeConfig {
    outbound_retry_max_secs: u64,
    shutdown_grace_secs: u64,
    panic_slot: Option<LoopSlot>,
    stall_shutdown_slot: Option<LoopSlot>,
}

impl GatewayRuntimeConfig {
    fn from_config(cfg: &Config) -> Self {
        GatewayRuntimeConfig {
            outbound_retry_max_secs: cfg.mesh.gateway.outbound_retry_max_secs,
            shutdown_grace_secs: cfg.mesh.gateway.shutdown_grace_secs,
            panic_slot: std::env::var("A2A_TEST_MESH_GATEWAY_STUB_PANIC")
                .ok()
                .and_then(|s| s.parse().ok()),
            stall_shutdown_slot: std::env::var("A2A_TEST_MESH_GATEWAY_STUB_STALL_SHUTDOWN")
                .ok()
                .and_then(|s| s.parse().ok()),
        }
    }
}

enum LoopEvent {
    Heartbeat(LoopSlot),
}

#[derive(Debug)]
enum DaemonOutcome {
    Graceful(Value),
    NonZero(Value),
}

pub fn run(home: &A2aHome, cfg: &Config, quiet: bool) -> Result<Outcome, AppError> {
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
        .map_err(|e| AppError::new("INTERNAL", ExitCode::Internal, format!("starting gateway runtime: {e}")))?;
    let outcome = runtime.block_on(supervise(root.clone(), cfg.clone(), quiet));
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
        "mesh gateway up (pid {}, state {}, health_seq {}, listen_port {}, expose {})",
        health.pid, health.state, health.health_seq, health.listen_port, health.expose
    ))))
}

pub fn stop(home: &A2aHome) -> Result<Outcome, AppError> {
    let root = gateway_root(home);
    if lock_is_free(&root) {
        return Ok(Outcome::from(json!({"stopped": false, "already_stopped": true})));
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
        .map_err(|e| AppError::new("INTERNAL", ExitCode::Internal, format!("starting stop runtime: {e}")))?;
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
    root: PathBuf,
    cfg: Config,
    quiet: bool,
) -> Result<DaemonOutcome, AppError> {
    let supervisor_token = CancellationToken::new();
    let tracker = TaskTracker::new();
    let log = GatewayLog::start(root.clone());
    let runtime_cfg = GatewayRuntimeConfig::from_config(&cfg);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| AppError::new("GATEWAY_SIGNAL", ExitCode::Internal, format!("installing SIGTERM handler: {e}")))?;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let mut health = GatewayHealth::initial(&cfg);
    health.state = "running".to_string();
    write_health(&root, &mut health).await?;
    log.line(json!({"ts": Utc::now(), "kind": "started", "pid": health.pid}))
        .await;

    let (events_tx, mut events_rx) = mpsc::channel::<LoopEvent>(LOG_CAPACITY);
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
                        write_health(&root, &mut health).await?;
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
    health.shutdown = Some(json!({"phase": "stop_accepting", "links": 0}));
    write_health(&root, health).await?;
    log.line(json!({
        "ts": Utc::now(),
        "kind": "stop_accepting",
        "links": 0
    })).await;
    log.line(json!({
        "ts": Utc::now(),
        "kind": "link_close",
        "err": {"code": "SHUTTING_DOWN"},
        "links": 0
    })).await;

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
        })).await;
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
        })).await;
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
        let result = AssertUnwindSafe(stub_loop(slot, token, tx, cfg))
            .catch_unwind()
            .await;
        let result = match result {
            Ok(_) => Ok(()),
            Err(_) => Err("panicked".to_string()),
        };
        (slot, result)
    };
    let tracked = tracker.track_future(future);
    loops.spawn(tracked);
}

async fn stub_loop(
    slot: LoopSlot,
    token: CancellationToken,
    tx: mpsc::Sender<LoopEvent>,
    cfg: GatewayRuntimeConfig,
) -> LoopSlot {
    if cfg.panic_slot == Some(slot) {
        panic!("gateway stub loop {slot} panicked by test hook");
    }
    let _ = tx.send(LoopEvent::Heartbeat(slot)).await;
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                if cfg.stall_shutdown_slot == Some(slot) {
                    std::future::pending::<()>().await;
                }
                return slot;
            }
            _ = tokio::time::sleep(jittered_heartbeat(HEARTBEAT_INTERVAL)) => {
                if tx.send(LoopEvent::Heartbeat(slot)).await.is_err() {
                    return slot;
                }
            }
        }
    }
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
