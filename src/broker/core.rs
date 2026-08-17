//! `BrokerCore` — the one task that owns the worker registry, the demux, and
//! the supervised child (spec §4.2).
//!
//! ```text
//!            ┌──────────────── CoreMsg (bounded mpsc) ────────────────┐
//!  CLI/socket│ Spawn Send Interrupt Kill Resume List Status Subscribe │
//!  internals │ ConnClosed ChildDied IdleCheck WorkerSettled           │
//!            └───────────────────────┬────────────────────────────────┘
//!                                    ▼
//!   child stdout ─▶ demux ─▶ BrokerCore ─▶ WorkerActor mailbox (per worker)
//!                     │          │
//!                     │          └─▶ supervision: restart, re-attach, degrade
//!                     └─▶ broker.jsonl (broker-scoped + unroutable, never dropped)
//! ```
//!
//! Four properties are load-bearing, and each is the answer to a specific way
//! this component could be wrong:
//!
//! * **One owner, no locks.** The registry is a plain `HashMap` inside this
//!   task. Nothing else can read it, so nothing else can race it, and
//!   `Arc<Mutex<…>>` never appears in `src/broker/`. The corollary is that the
//!   core must never *block* on anything a worker does: every command is handed
//!   to a short-lived task that owns the reply channel, because a `send` that
//!   waits on a model turn would otherwise stall `status` for two minutes.
//! * **Routing keys are per-method, not "params.threadId".** `thread/started`
//!   carries its id at `params.thread.id` and nowhere else — the frame that
//!   *introduces* a thread is exactly the one a naive demux drops. And a frame
//!   that cannot be routed is written to `broker.jsonl`, never dropped: an
//!   unexplained silence is the most expensive failure mode this system has.
//! * **A restart is a re-attach, not a re-spawn.** The child owns no worker
//!   state — the thread does, and the thread survives in `CODEX_HOME`. So
//!   supervision restarts, re-`initialize`s, `thread/resume`s every live
//!   worker, publishes the new [`AdapterRef`] on the watch channel every actor
//!   reads before it writes, and re-delivers whatever was queued. The one thing
//!   `thread/resume` cannot carry is the per-turn overrides, so the next
//!   `turn/start` re-supplies them in full — otherwise a restart silently
//!   widens every worker's sandbox back to the mode default.
//! * **Failure is bounded and visible.** Three restarts in sixty seconds and
//!   the broker latches `degraded: exec` in `broker-health.json` rather than
//!   forking forever, and re-probes every five minutes so the degradation is
//!   not permanent either.

use crate::adapter::{self, AdapterRef, SendPath};
use crate::broker::child::{self, ChildSpec, Launched, RestartBudget, Supervision};
use crate::broker::worker_actor::{
    self, InterruptOutcome, Settled, WorkerHandle, WorkerMsg, RPC_TIMEOUT,
};
use crate::config::Config;
use crate::events::{self, Envelope, EventClass, EventKind, Transport};
use crate::fold;
use crate::fsm::{FailureClass, WorkerState};
use crate::inbox;
use crate::output::{AppError, ExitCode};
use crate::protocol::RpcNotification;
use crate::redact;
use crate::reconcile;
use crate::responder;
use crate::rpc::{RpcError, ServerRequestFrame};
use crate::state::{self, save_snapshot, WorkerSnapshot};
use crate::store::{self, EventLog};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinError;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Bound on the core mailbox. Every socket connection, every notification and
/// every internal event funnels through it, so it is generous — but bounded,
/// because an unbounded queue in front of a single-threaded owner is just a
/// slower way to run out of memory.
pub const CORE_MAILBOX_CAPACITY: usize = 4096;

/// Bound on the tail broadcast. `broadcast` is the ONE non-mpsc channel the
/// broker uses, and only here: a tail subscriber that cannot keep up must be
/// told it lagged, not allowed to back the transport up.
pub const TAIL_CAPACITY: usize = 1024;

/// How long the demux will wait for a full worker mailbox before recording the
/// frame as undeliverable in `broker.jsonl`. Bounded so one wedged worker
/// cannot stall every other worker's commands.
pub const WORKER_HANDOFF_GRACE: Duration = Duration::from_secs(5);

/// How long a notification for an unknown thread is held before it is written
/// off as unroutable. A `thread/started` can legitimately arrive before the
/// core has learned the mapping (the actor is still awaiting its `thread/start`
/// response) — that window is milliseconds, and this is three orders of
/// magnitude more.
pub const PENDING_TTL: Duration = Duration::from_secs(10);

/// How many such frames are held at once. Beyond this the OLDEST is written to
/// `broker.jsonl` — recorded, never silently dropped.
pub const PENDING_CAPACITY: usize = 256;

/// The value `broker-health.json` carries while the app-server transport is
/// latched off (spec §4.2: new spawns fall back to exec).
pub const DEGRADED_EXEC: &str = "exec";

pub const HEALTH_SCHEMA_VERSION: u32 = 1;

/// How many broker-scoped notices `broker-health.json` keeps. They are a
/// dashboard, not a log — the log is `broker.jsonl`, which keeps all of them.
pub const HEALTH_NOTICE_CAPACITY: usize = 20;

// ---------------------------------------------------------------------------
// the mailbox
// ---------------------------------------------------------------------------

/// Everything that can reach the core. One enum, one queue, one owner.
///
/// The first twelve are the broker's *surface*: the socket server (Task 8)
/// speaks exactly these. The remainder are internal plumbing — the demux, the
/// supervisor and the thread-learner talk to the core through the same mailbox
/// as everyone else, because a second door into the registry would be a second
/// owner.
pub enum CoreMsg {
    /// Adopt an already-laid-out worker directory (genesis written, `state.json`
    /// present) and give it a `WorkerActor`.
    Spawn {
        dir: PathBuf,
        reply: oneshot::Sender<Result<WorkerSnapshot, AppError>>,
    },
    Send {
        id: String,
        text: String,
        reply: oneshot::Sender<Result<SendPath, AppError>>,
    },
    Interrupt {
        id: String,
        reply: oneshot::Sender<Result<InterruptOutcome, AppError>>,
    },
    Kill {
        id: String,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Re-attach an existing worker directory to the live child: `thread/resume`
    /// then register. A resume that the server refuses leaves the worker
    /// `orphaned`, exactly as a failed re-attach after a restart does.
    Resume {
        dir: PathBuf,
        reply: oneshot::Sender<Result<WorkerSnapshot, AppError>>,
    },
    List {
        reply: oneshot::Sender<Vec<WorkerSnapshot>>,
    },
    Status {
        id: String,
        reply: oneshot::Sender<Result<WorkerSnapshot, AppError>>,
    },
    Subscribe {
        reply: oneshot::Sender<broadcast::Receiver<TailFrame>>,
    },
    /// A socket connection went away; its subscription dies with it.
    ConnClosed {
        conn: u64,
    },
    /// The child of `generation` is gone — stdout EOF, a failed launch, or two
    /// consecutive missed liveness probes.
    ChildDied {
        generation: u64,
        detail: String,
    },
    IdleCheck {
        reply: oneshot::Sender<IdleReport>,
    },
    /// A worker reached a state nobody is waiting on. Wraps the `Settled`
    /// channel every `WorkerActor` announces on.
    WorkerSettled(Settled),

    // ---- internal ---------------------------------------------------------
    /// One notification off the child's stdout, tagged with the generation that
    /// produced it so a frame from a dead child cannot be mistaken for news.
    Notification {
        generation: u64,
        method: String,
        params: Value,
    },
    /// One server→client request, which OWES an answer within 100ms.
    ServerRequest {
        generation: u64,
        frame: ServerRequestFrame,
    },
    /// A (re)launch finished.
    ChildStarted {
        generation: u64,
        outcome: Box<Result<Launched, AppError>>,
    },
    /// A worker's app-server thread id is now known, so its buffered
    /// notifications can be routed.
    ThreadLearned {
        id: String,
        thread_id: String,
        epoch: u32,
    },
    /// The five-minute re-promotion probe for a latched-degraded broker.
    Repromote {
        generation: u64,
    },
}

/// One envelope, as a tail subscriber sees it.
#[derive(Clone, Debug)]
pub struct TailFrame {
    pub worker_id: String,
    pub envelope: Envelope,
}

/// What the core knows about itself, for idle-shutdown (Task 10), `broker
/// status` and tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdleReport {
    /// Workers with a live actor.
    pub workers: usize,
    /// ... of which have not settled (terminal or escalated).
    pub live: usize,
    pub generation: u64,
    pub child_pid: Option<u32>,
    pub degraded: bool,
    pub restarts: u64,
}

// ---------------------------------------------------------------------------
// the handle
// ---------------------------------------------------------------------------

/// The far end of the core mailbox. Cloneable, and every method is a
/// request/response over the one queue.
#[derive(Clone)]
pub struct CoreHandle {
    tx: mpsc::Sender<CoreMsg>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

impl CoreHandle {
    pub fn sender(&self) -> &mpsc::Sender<CoreMsg> {
        &self.tx
    }

    pub async fn spawn_worker(&self, dir: impl Into<PathBuf>) -> Result<WorkerSnapshot, AppError> {
        let dir = dir.into();
        self.ask(move |reply| CoreMsg::Spawn { dir, reply }).await?
    }

    pub async fn resume_worker(&self, dir: impl Into<PathBuf>) -> Result<WorkerSnapshot, AppError> {
        let dir = dir.into();
        self.ask(move |reply| CoreMsg::Resume { dir, reply }).await?
    }

    pub async fn send(&self, id: &str, text: impl Into<String>) -> Result<SendPath, AppError> {
        let (id, text) = (id.to_string(), text.into());
        self.ask(move |reply| CoreMsg::Send { id, text, reply })
            .await?
    }

    pub async fn interrupt(&self, id: &str) -> Result<InterruptOutcome, AppError> {
        let id = id.to_string();
        self.ask(move |reply| CoreMsg::Interrupt { id, reply })
            .await?
    }

    pub async fn kill(&self, id: &str) -> Result<(), AppError> {
        let id = id.to_string();
        self.ask(move |reply| CoreMsg::Kill { id, reply }).await?
    }

    pub async fn list(&self) -> Result<Vec<WorkerSnapshot>, AppError> {
        self.ask(|reply| CoreMsg::List { reply }).await
    }

    pub async fn status(&self, id: &str) -> Result<WorkerSnapshot, AppError> {
        let id = id.to_string();
        self.ask(move |reply| CoreMsg::Status { id, reply }).await?
    }

    pub async fn subscribe(&self) -> Result<broadcast::Receiver<TailFrame>, AppError> {
        self.ask(|reply| CoreMsg::Subscribe { reply }).await
    }

    pub async fn conn_closed(&self, conn: u64) {
        let _ = self.tx.send(CoreMsg::ConnClosed { conn }).await;
    }

    pub async fn idle(&self) -> Result<IdleReport, AppError> {
        self.ask(|reply| CoreMsg::IdleCheck { reply }).await
    }

    /// Cancel everything and wait for every task the broker spawned — actors,
    /// pumps, the log writer — to finish. Bounded by the caller.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }

    async fn ask<T, F>(&self, build: F) -> Result<T, AppError>
    where
        F: FnOnce(oneshot::Sender<T>) -> CoreMsg,
    {
        let (tx, rx) = oneshot::channel();
        self.tx.send(build(tx)).await.map_err(|_| core_gone())?;
        rx.await.map_err(|_| core_gone())
    }
}

fn core_gone() -> AppError {
    AppError::new(
        "BROKER_GONE",
        ExitCode::BrokerUnavailable,
        "the broker core is not running",
    )
    .remedy("`a2a broker restart`")
}

// ---------------------------------------------------------------------------
// routing
// ---------------------------------------------------------------------------

/// Where one notification belongs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    /// Keyed to an app-server thread — which worker that is, the registry says.
    Thread(String),
    /// About the broker, not any one worker (`account/rateLimits/updated`,
    /// `deprecationNotice`, `configWarning`).
    BrokerScoped,
    /// Nothing in the frame says who it is for. Recorded, never dropped.
    Unroutable,
}

/// The per-method routing table (plan "Ground truth").
///
/// The special case is the whole point: **`thread/started` has no top-level
/// `threadId`** — its key is `params.thread.id`. Keying everything on
/// `params.threadId` would drop precisely the frame that announces a thread's
/// existence, and the failure would look like "the worker never started".
pub fn routing_key(method: &str, params: &Value) -> Route {
    if events::BROKER_SCOPED_METHODS.contains(&method) {
        return Route::BrokerScoped;
    }
    if method == "thread/started" {
        return match params.pointer("/thread/id").and_then(Value::as_str) {
            Some(id) => Route::Thread(id.to_string()),
            None => Route::Unroutable,
        };
    }
    // turn/*, item/*, error, thread/tokenUsage/updated, model/rerouted,
    // guardianWarning, thread/status/changed — and anything future-dated that
    // follows the same convention.
    if let Some(id) = params.get("threadId").and_then(Value::as_str) {
        return Route::Thread(id.to_string());
    }
    // Belt and braces for a future frame that nests its thread like
    // `thread/started` does.
    match params.pointer("/thread/id").and_then(Value::as_str) {
        Some(id) => Route::Thread(id.to_string()),
        None => Route::Unroutable,
    }
}

/// Markers that identify an expired-credentials error, matched
/// case-insensitively against the message of an `error` notification.
///
/// ONE predicate ([`is_auth_expiry`]), for the same reason
/// [`adapter::is_steer_precondition_failure`] is one: when Task 18 sees what
/// the real server says, there is exactly one place to correct.
pub const AUTH_EXPIRY_MARKERS: [&str; 7] = [
    "unauthorized",
    "401",
    "token expired",
    "expired token",
    "re-authenticate",
    "reauthenticate",
    "codex login",
];

/// True when an `error` notification means "the credentials are gone", which is
/// a *resumable* failure: retrying it automatically would burn the retry budget
/// against a wall only a human can move.
pub fn is_auth_expiry(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    if AUTH_EXPIRY_MARKERS.iter().any(|marker| m.contains(marker)) {
        return true;
    }
    // The generic shape: something about auth, something about expiry.
    (m.contains("auth") || m.contains("credential")) && (m.contains("expire") || m.contains("invalid"))
}

/// The human-readable message inside an `error` notification, wherever the
/// server chose to put it.
pub fn error_message(params: &Value) -> Option<String> {
    for pointer in ["/error/message", "/error", "/message"] {
        if let Some(s) = params.pointer(pointer).and_then(Value::as_str) {
            return Some(s.to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// the single-flight auth guard
// ---------------------------------------------------------------------------

/// Serializes the FIRST turn-start after a child (re)start (spec §4.2).
///
/// A fresh child holds no refreshed token. If ten workers start turns at once,
/// ten token refreshes race and nine of them are waste at best and a rate-limit
/// at worst. So the first turn-start goes alone; everything else waits for it
/// to finish, and after that the gate is open for the life of the child.
///
/// Owned by the core task — this is a *value*, not a shared lock. Waiters get a
/// `watch::Receiver`, so if the pilot's task dies without reporting, the
/// channel closes and every waiter proceeds rather than deadlocking.
#[derive(Default)]
pub struct AuthGate {
    pilot: Option<watch::Receiver<bool>>,
    open: bool,
}

/// What one caller must do to pass the gate.
pub enum GatePass {
    /// You are first: go, and report when you are done.
    Pilot(watch::Sender<bool>),
    /// Someone is already going: wait for them.
    Wait(watch::Receiver<bool>),
    /// The first turn is long done.
    Open,
}

impl AuthGate {
    /// A new child means new credentials: the gate closes again.
    pub fn reset(&mut self) {
        self.pilot = None;
        self.open = false;
    }

    pub fn acquire(&mut self) -> GatePass {
        if self.open {
            return GatePass::Open;
        }
        match &self.pilot {
            None => {
                let (tx, rx) = watch::channel(false);
                self.pilot = Some(rx);
                GatePass::Pilot(tx)
            }
            Some(rx) => {
                if *rx.borrow() {
                    self.open = true;
                    GatePass::Open
                } else {
                    GatePass::Wait(rx.clone())
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// broker files
// ---------------------------------------------------------------------------

/// One broker-scoped notification, kept for `a2a doctor` (Task 16).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Notice {
    pub ts: DateTime<Utc>,
    pub method: String,
    pub params: Value,
}

/// `broker-health.json` — what the daemon knows about itself.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BrokerHealth {
    pub schema_version: u32,
    pub pid: u32,
    /// Bumped on every child (re)launch; frames tagged with an older one are
    /// news about a corpse.
    pub generation: u64,
    pub child_pid: Option<u32>,
    pub child_user_agent: Option<String>,
    pub child_started_at: Option<DateTime<Utc>>,
    /// Restarts attempted since the daemon started.
    pub restarts: u64,
    /// ... of which are inside the policy window.
    pub restarts_in_window: u32,
    /// `Some("exec")` once the restart budget is spent: new spawns fall back to
    /// the exec transport until the re-promotion probe succeeds.
    pub degraded: Option<String>,
    pub degraded_since: Option<DateTime<Utc>>,
    pub degraded_reason: Option<String>,
    pub next_promotion_probe_at: Option<DateTime<Utc>>,
    pub workers: usize,
    /// The most recent broker-scoped notifications (rate limits, deprecations,
    /// config warnings) — the ones that belong to no worker.
    pub notices: Vec<Notice>,
    pub updated_at: DateTime<Utc>,
}

impl Default for BrokerHealth {
    fn default() -> Self {
        BrokerHealth {
            schema_version: HEALTH_SCHEMA_VERSION,
            pid: std::process::id(),
            generation: 0,
            child_pid: None,
            child_user_agent: None,
            child_started_at: None,
            restarts: 0,
            restarts_in_window: 0,
            degraded: None,
            degraded_since: None,
            degraded_reason: None,
            next_promotion_probe_at: None,
            workers: 0,
            notices: Vec::new(),
            updated_at: Utc::now(),
        }
    }
}

impl BrokerHealth {
    pub fn read(home: &Path) -> Option<BrokerHealth> {
        let text = std::fs::read_to_string(home.join(super::BROKER_HEALTH)).ok()?;
        serde_json::from_str(&text).ok()
    }

    fn push_notice(&mut self, notice: Notice) {
        self.notices.push(notice);
        let overflow = self.notices.len().saturating_sub(HEALTH_NOTICE_CAPACITY);
        self.notices.drain(..overflow);
    }
}

enum LogMsg {
    Line(Value),
    Health(Box<BrokerHealth>),
}

/// The sole writer of `broker.jsonl` and `broker-health.json`.
///
/// A dedicated task fed by an mpsc, like every other write path in the broker:
/// the core never touches the filesystem itself, so no cancelled future can
/// leave a half-written line and no `fsync` can stall the registry.
#[derive(Clone)]
pub struct BrokerLog {
    tx: mpsc::Sender<LogMsg>,
}

impl BrokerLog {
    pub fn start(home: PathBuf, tracker: &TaskTracker) -> Self {
        let (tx, mut rx) = mpsc::channel::<LogMsg>(CORE_MAILBOX_CAPACITY);
        tracker.spawn(async move {
            while let Some(msg) = rx.recv().await {
                let home = home.clone();
                let _ = tokio::task::spawn_blocking(move || write_log(&home, msg)).await;
            }
        });
        BrokerLog { tx }
    }

    /// Append one line to `broker.jsonl`. Awaited (not `try_send`) on purpose:
    /// the frames that come here are the ones that must never be lost.
    pub async fn line(&self, value: Value) {
        let _ = self.tx.send(LogMsg::Line(value)).await;
    }

    pub async fn health(&self, health: BrokerHealth) {
        let _ = self.tx.send(LogMsg::Health(Box::new(health))).await;
    }
}

fn write_log(home: &Path, msg: LogMsg) {
    match msg {
        LogMsg::Line(value) => {
            if let Ok(mut f) = crate::exec::open_append_0600(&home.join(super::BROKER_JSONL)) {
                let _ = writeln!(f, "{value}");
            }
        }
        LogMsg::Health(health) => {
            if let Ok(bytes) = serde_json::to_vec_pretty(&*health) {
                let _ = store::write_atomic(&home.join(super::BROKER_HEALTH), &bytes);
            }
        }
    }
}

/// Redact every string in a value before it reaches `broker.jsonl`.
///
/// An unroutable frame is written out verbatim *in shape* — dropping it would
/// be worse — but its text goes through the same egress filter every other
/// worker-facing string does (spec §5).
pub fn redact_value(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(redact::redact_egress(s)),
        Value::Array(items) => Value::Array(items.iter().map(redact_value).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), redact_value(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// panic isolation
// ---------------------------------------------------------------------------

/// The detail line for a worker whose actor panicked, or `None` for any other
/// way a task can end (a clean return, or a cancellation during shutdown).
///
/// A panic in one actor must cost exactly one worker: the `JoinSet` hands the
/// core the corpse, the core records `failed{internal_panic}` against that
/// worker's own files, and every sibling keeps running.
pub fn panic_detail(err: &JoinError) -> Option<String> {
    if !err.is_panic() {
        return None;
    }
    Some(format!("the worker actor panicked ({err})"))
}

/// Record a terminal failure against a worker whose actor is gone, from the
/// core — taking the same `state.lock` and re-opening the same `EventLog` an
/// actor or a CLI reconcile would.
///
/// `class` refines what the FSM's own `ProcessExited` edge would say
/// (`no_result`): the machine decides the STATE, the row decides the
/// diagnosis ([`fold`] honours `failure_class` on a failing transition).
pub async fn fail_worker(
    dir: PathBuf,
    cfg: Config,
    class: FailureClass,
    detail: String,
) -> Result<(), AppError> {
    let detail = redact::redact_egress(&detail);
    let joined = tokio::task::spawn_blocking(move || {
        let _lock = reconcile::lock_state(&dir)?;
        let snap = state::load_snapshot(&dir)
            .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading state.json: {e}")))?;
        // A worker that already finished is not made to fail retroactively.
        if snap.state.is_terminal() {
            return Ok(());
        }
        let mut log =
            EventLog::open(&dir).map_err(|e| AppError::new("IO", ExitCode::Internal, format!("opening events.jsonl: {e}")))?;
        let mut snap = fold::append_and_fold(
            &mut log,
            &dir,
            snap,
            EventKind::StateChange,
            json!({"fsm": "ProcessExited", "code": null, "detail": detail,
                   "failure_class": class}),
            Utc::now(),
        )?;
        reconcile::arm_deadline(&mut snap, &cfg);
        save_snapshot(&dir, &snap)
            .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("writing state.json: {e}")))?;
        Ok::<_, AppError>(())
    })
    .await;
    match joined {
        Ok(result) => result,
        Err(e) => Err(AppError::new(
            "INTERNAL",
            ExitCode::Internal,
            format!("recording a worker failure: {e}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// the core
// ---------------------------------------------------------------------------

struct Worker {
    handle: WorkerHandle,
    dir: PathBuf,
    /// The app-server thread this worker's frames are keyed by. Learned from
    /// the worker's own snapshot: the ACTOR performs `thread/start`, so the
    /// core reads the id back rather than inventing it.
    thread_id: Option<String>,
    epoch: u32,
    settled: bool,
}

struct PendingFrame {
    at: Instant,
    thread: String,
    method: String,
    params: Value,
}

pub struct BrokerCore {
    cfg: Config,
    spec: ChildSpec,
    tx: mpsc::Sender<CoreMsg>,
    rx: mpsc::Receiver<CoreMsg>,
    workers: HashMap<String, Worker>,
    /// thread id → worker id.
    threads: HashMap<String, String>,
    /// tokio task id → worker id, so a panicking actor can be named.
    task_ids: HashMap<tokio::task::Id, String>,
    actors: tokio::task::JoinSet<String>,
    pending: VecDeque<PendingFrame>,
    tracker: TaskTracker,
    cancel: CancellationToken,
    /// Cancelled and replaced on every child generation, so the previous
    /// generation's liveness prober stops probing a corpse.
    child_cancel: CancellationToken,
    adapter_tx: watch::Sender<AdapterRef>,
    adapter_rx: watch::Receiver<AdapterRef>,
    child: Option<tokio::process::Child>,
    generation: u64,
    budget: RestartBudget,
    degraded: bool,
    auth: AuthGate,
    health: BrokerHealth,
    log: BrokerLog,
    tail: broadcast::Sender<TailFrame>,
    settle_tx: mpsc::Sender<Settled>,
    supervision: Supervision,
}

/// Start the broker core: launch the child, complete the handshake, and put the
/// registry task on the air.
///
/// The initial launch is NOT retried — a broker that cannot start its child has
/// nothing to offer, and `a2a` falls back to the exec transport (Task 11).
/// Restarts are what the [`RestartPolicy`] is for.
pub async fn start(
    home: PathBuf,
    cfg: Config,
    spec: ChildSpec,
    supervision: Supervision,
) -> Result<CoreHandle, AppError> {
    let tracker = TaskTracker::new();
    let cancel = CancellationToken::new();
    let log = BrokerLog::start(home.clone(), &tracker);

    let launched = child::launch(&spec).await?;
    let (adapter_tx, adapter_rx) = watch::channel(launched.adapter.clone());
    let (tx, rx) = mpsc::channel(CORE_MAILBOX_CAPACITY);
    let (settle_tx, settle_rx) = mpsc::channel::<Settled>(CORE_MAILBOX_CAPACITY);
    let (tail, _) = broadcast::channel(TAIL_CAPACITY);

    // The settle channel is Task 6's; wrapping it here is what makes
    // `CoreMsg::WorkerSettled` the only way a settlement reaches the registry.
    {
        let tx = tx.clone();
        let mut settle_rx = settle_rx;
        tracker.spawn(async move {
            while let Some(settled) = settle_rx.recv().await {
                if tx.send(CoreMsg::WorkerSettled(settled)).await.is_err() {
                    return;
                }
            }
        });
    }

    let mut core = BrokerCore {
        cfg,
        spec,
        tx: tx.clone(),
        rx,
        workers: HashMap::new(),
        threads: HashMap::new(),
        task_ids: HashMap::new(),
        actors: tokio::task::JoinSet::new(),
        pending: VecDeque::new(),
        tracker: tracker.clone(),
        cancel: cancel.clone(),
        child_cancel: cancel.child_token(),
        adapter_tx,
        adapter_rx,
        child: None,
        generation: 0,
        budget: RestartBudget::new(supervision.policy),
        degraded: false,
        auth: AuthGate::default(),
        health: BrokerHealth::default(),
        log,
        tail,
        settle_tx,
        supervision,
    };
    core.generation = 1;
    core.adopt(1, launched).await;

    tracker.spawn(core.run());
    Ok(CoreHandle {
        tx,
        cancel,
        tracker,
    })
}

impl BrokerCore {
    async fn run(mut self) {
        let cancel = self.cancel.clone();
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                exited = self.actors.join_next_with_id(), if !self.actors.is_empty() => {
                    if let Some(exited) = exited {
                        self.on_actor_exit(exited).await;
                    }
                }
                msg = self.rx.recv() => match msg {
                    None => break,
                    Some(msg) => self.handle(msg).await,
                },
            }
        }
        // The child is killed by dropping it (`kill_on_drop`); the actors are
        // stopped by the cancellation token they share with this task.
        self.child = None;
    }

    async fn handle(&mut self, msg: CoreMsg) {
        match msg {
            CoreMsg::Spawn { dir, reply } => {
                let outcome = self.register(dir, false).await;
                let _ = reply.send(outcome);
            }
            CoreMsg::Resume { dir, reply } => {
                let outcome = self.register(dir, true).await;
                let _ = reply.send(outcome);
            }
            CoreMsg::Send { id, text, reply } => match self.worker(&id) {
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
                Ok(handle) => {
                    // The first turn after a (re)start goes alone; the rest
                    // wait for it. Never awaited HERE — a turn can take minutes
                    // and the registry has other callers.
                    let pass = self.auth.acquire();
                    self.tracker.spawn(guarded_send(pass, handle, text, reply));
                }
            },
            CoreMsg::Interrupt { id, reply } => match self.worker(&id) {
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
                Ok(handle) => {
                    self.tracker.spawn(async move {
                        let _ = reply.send(handle.interrupt().await);
                    });
                }
            },
            CoreMsg::Kill { id, reply } => match self.worker(&id) {
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
                Ok(handle) => {
                    self.tracker.spawn(async move {
                        let _ = reply.send(handle.kill().await);
                    });
                }
            },
            CoreMsg::Status { id, reply } => match self.worker(&id) {
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
                Ok(handle) => {
                    self.tracker.spawn(async move {
                        let _ = reply.send(handle.snapshot().await);
                    });
                }
            },
            CoreMsg::List { reply } => {
                let handles: Vec<WorkerHandle> =
                    self.workers.values().map(|w| w.handle.clone()).collect();
                self.tracker.spawn(async move {
                    let mut out = Vec::with_capacity(handles.len());
                    for handle in handles {
                        if let Ok(snap) = handle.snapshot().await {
                            out.push(snap);
                        }
                    }
                    out.sort_by(|a, b| a.id.cmp(&b.id));
                    let _ = reply.send(out);
                });
            }
            CoreMsg::Subscribe { reply } => {
                let _ = reply.send(self.tail.subscribe());
            }
            CoreMsg::ConnClosed { conn: _ } => {
                // A subscription lives in the connection's own task and dies
                // with it; the core records the fact and holds no per-connection
                // state of its own (Task 8 gives this teeth).
            }
            CoreMsg::IdleCheck { reply } => {
                let _ = reply.send(self.idle_report());
            }
            CoreMsg::WorkerSettled(settled) => {
                if let Some(worker) = self.workers.get_mut(&settled.id) {
                    worker.settled = true;
                }
                self.publish_health().await;
            }
            CoreMsg::Notification {
                generation,
                method,
                params,
            } => self.on_notification(generation, method, params).await,
            CoreMsg::ServerRequest { generation, frame } => {
                self.on_server_request(generation, frame).await
            }
            CoreMsg::ChildDied { generation, detail } => self.on_child_died(generation, detail).await,
            CoreMsg::ChildStarted {
                generation,
                outcome,
            } => self.on_child_started(generation, *outcome).await,
            CoreMsg::ThreadLearned {
                id,
                thread_id,
                epoch,
            } => self.on_thread_learned(id, thread_id, epoch).await,
            CoreMsg::Repromote { generation } => self.on_repromote(generation).await,
        }
    }

    // -- registry -----------------------------------------------------------

    fn worker(&self, id: &str) -> Result<WorkerHandle, AppError> {
        self.workers
            .get(id)
            .map(|w| w.handle.clone())
            .ok_or_else(|| {
                AppError::new(
                    "NO_SUCH_WORKER",
                    ExitCode::NoSuchWorker,
                    format!("the broker is not tracking a worker `{id}`"),
                )
                .remedy("`a2a ls` lists the workers this broker owns")
            })
    }

    /// Adopt a worker directory: read its snapshot, give it an actor, and start
    /// learning its thread id.
    ///
    /// `resume` additionally re-opens the thread on the live child first — the
    /// same call supervision makes after a restart, and with the same
    /// consequence when the server refuses: `orphaned`.
    async fn register(&mut self, dir: PathBuf, resume: bool) -> Result<WorkerSnapshot, AppError> {
        let snap = state::load_snapshot(&dir).map_err(|e| {
            AppError::new(
                "SNAPSHOT_UNREADABLE",
                ExitCode::Precondition,
                format!("{}: {e}", dir.display()),
            )
        })?;
        if let Some(existing) = self.workers.get(&snap.id) {
            let handle = existing.handle.clone();
            return handle.snapshot().await;
        }

        if resume {
            let thread_id = snap.session_id.clone().ok_or_else(|| {
                AppError::new(
                    "SESSION_UNKNOWN",
                    ExitCode::Precondition,
                    format!("worker {} never opened an app-server thread", snap.id),
                )
            })?;
            let lane = self.lane(&snap)?;
            let adapter = self.adapter_rx.borrow().clone();
            adapter::thread_resume(
                adapter.client(),
                &thread_id,
                &lane,
                &snap.cwd,
                &dir,
                RPC_TIMEOUT,
            )
            .await
            .map_err(|e| {
                AppError::new(
                    "APP_SERVER",
                    ExitCode::BrokerUnavailable,
                    format!("thread/resume: {e}"),
                )
            })?;
        }

        let id = snap.id.clone();
        let (handle, actor) = worker_actor::build_worker_actor(
            dir.clone(),
            snap.clone(),
            self.cfg.clone(),
            self.adapter_rx.clone(),
            self.settle_tx.clone(),
            self.cancel.child_token(),
        );
        let tracked = self.tracker.track_future(actor);
        let task_id = {
            let id = id.clone();
            self.actors
                .spawn(async move {
                    tracked.await;
                    id
                })
                .id()
        };
        self.task_ids.insert(task_id, id.clone());
        self.workers.insert(
            id.clone(),
            Worker {
                handle: handle.clone(),
                dir,
                thread_id: snap.session_id.clone(),
                epoch: snap.epoch,
                settled: snap.state.is_terminal() || snap.state == WorkerState::Escalated,
            },
        );
        match snap.session_id.clone() {
            Some(thread_id) => {
                self.threads.insert(thread_id, id.clone());
                self.flush_pending().await;
            }
            // The actor opens the thread itself; the core reads the id back as
            // soon as the actor has recorded it.
            None => {
                self.tracker
                    .spawn(learn_thread(id.clone(), handle, self.tx.clone()));
            }
        }
        self.publish_health().await;
        Ok(snap)
    }

    async fn on_thread_learned(&mut self, id: String, thread_id: String, epoch: u32) {
        if let Some(worker) = self.workers.get_mut(&id) {
            worker.thread_id = Some(thread_id.clone());
            worker.epoch = epoch;
        }
        self.threads.insert(thread_id, id);
        self.flush_pending().await;
    }

    /// One actor ended. A clean end is bookkeeping; a panic is one worker's
    /// terminal failure and nobody else's problem.
    async fn on_actor_exit(&mut self, exited: Result<(tokio::task::Id, String), JoinError>) {
        let (task_id, id, panic) = match exited {
            Ok((task_id, id)) => (task_id, Some(id), None),
            Err(e) => {
                let task_id = e.id();
                let detail = panic_detail(&e);
                (task_id, self.task_ids.get(&task_id).cloned(), detail)
            }
        };
        self.task_ids.remove(&task_id);
        let Some(id) = id else { return };
        let entry = self.workers.remove(&id);
        if let Some(worker) = &entry {
            if let Some(thread_id) = &worker.thread_id {
                self.threads.remove(thread_id);
            }
        }
        if let (Some(detail), Some(worker)) = (panic, entry) {
            let dir = worker.dir.clone();
            let cfg = self.cfg.clone();
            let detail_for_log = detail.clone();
            self.log
                .line(json!({"ts": Utc::now(), "kind": "worker_panic", "worker": id,
                             "detail": redact::redact_egress(&detail_for_log)}))
                .await;
            // Spawned, so one worker's fsync cannot stall the registry — and
            // every sibling actor is untouched by construction: they are
            // separate tasks in the same JoinSet.
            self.tracker.spawn(async move {
                if let Err(e) = fail_worker(dir, cfg, FailureClass::InternalPanic, detail).await {
                    eprintln!("a2a broker: recording a panicked worker: {}: {}", e.code, e.message);
                }
            });
        }
        self.publish_health().await;
    }

    fn idle_report(&self) -> IdleReport {
        IdleReport {
            workers: self.workers.len(),
            live: self.workers.values().filter(|w| !w.settled).count(),
            generation: self.generation,
            child_pid: self.health.child_pid,
            degraded: self.degraded,
            restarts: self.health.restarts,
        }
    }

    fn lane(&self, snap: &WorkerSnapshot) -> Result<crate::config::LaneConfig, AppError> {
        self.cfg.lanes.get(&snap.lane).cloned().ok_or_else(|| {
            AppError::new(
                "CONFIG",
                ExitCode::Precondition,
                format!("no configuration for the `{}` lane", snap.lane.as_str()),
            )
        })
    }

    // -- demux --------------------------------------------------------------

    async fn on_notification(&mut self, generation: u64, method: String, params: Value) {
        if generation != self.generation {
            return; // news about a child that is already gone
        }
        self.expire_pending().await;
        match routing_key(&method, &params) {
            Route::BrokerScoped => {
                let notice = Notice {
                    ts: Utc::now(),
                    method: method.clone(),
                    params: redact_value(&params),
                };
                self.log
                    .line(json!({"ts": notice.ts, "kind": "broker_scoped",
                                 "method": notice.method, "params": notice.params}))
                    .await;
                self.health.push_notice(notice);
                self.publish_health().await;
            }
            Route::Thread(thread) => match self.threads.get(&thread).cloned() {
                Some(worker_id) => self.deliver_envelope(&worker_id, &method, params).await,
                // The mapping is not known YET: the actor is still inside its
                // `thread/start`. Buffered in arrival order and flushed the
                // moment the id lands, so `thread/started` is never lost.
                None => self.buffer_pending(thread, method, params).await,
            },
            Route::Unroutable => {
                self.log_unroutable("no_routing_key", &method, &params, None)
                    .await
            }
        }
    }

    /// Map, stamp and hand one notification to its worker.
    async fn deliver_envelope(&mut self, worker_id: &str, method: &str, params: Value) {
        let Some((class, kind)) = events::map_appserver_notification(method, &params) else {
            return; // suppressed at the mapping layer (deltas, progress)
        };
        let mut raw = params;
        if class == EventClass::Lifecycle && kind == EventKind::Error {
            self.mark_auth_expiry(&mut raw);
        }
        let Some(worker) = self.workers.get(worker_id) else {
            self.log_unroutable("worker_gone", method, &raw, Some(worker_id))
                .await;
            return;
        };
        let env = Envelope::new(
            worker.epoch,
            Transport::AppServer,
            class,
            kind,
            raw,
            Utc::now(),
        );
        let _ = self.tail.send(TailFrame {
            worker_id: worker_id.to_string(),
            envelope: env.clone(),
        });
        let sender = worker.handle.sender().clone();
        let msg = match sender.try_send(WorkerMsg::Envelope(env)) {
            Ok(()) => return,
            Err(mpsc::error::TrySendError::Full(msg)) => msg,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.log_unroutable("worker_mailbox_closed", method, &Value::Null, Some(worker_id))
                    .await;
                return;
            }
        };
        // Backpressure, bounded: a worker that cannot keep up slows the demux
        // down, but it must not stop it forever.
        if tokio::time::timeout(WORKER_HANDOFF_GRACE, sender.send(msg))
            .await
            .is_err()
        {
            self.log_unroutable("worker_mailbox_full", method, &Value::Null, Some(worker_id))
                .await;
        }
    }

    /// An `error` notification that names expired credentials becomes a
    /// terminal, *resumable* failure with its own class — never an auto-retry.
    fn mark_auth_expiry(&self, raw: &mut Value) {
        let Some(message) = error_message(raw) else {
            return;
        };
        if !is_auth_expiry(&message) {
            return;
        }
        if let Some(obj) = raw.as_object_mut() {
            obj.insert("fsm".to_string(), json!("ProcessExited"));
            obj.insert("code".to_string(), Value::Null);
            obj.insert(
                "detail".to_string(),
                json!(redact::redact_egress(&message)),
            );
            obj.insert(
                "failure_class".to_string(),
                json!(FailureClass::AuthExpired),
            );
        }
    }

    async fn buffer_pending(&mut self, thread: String, method: String, params: Value) {
        if self.pending.len() >= PENDING_CAPACITY {
            if let Some(oldest) = self.pending.pop_front() {
                self.log_unroutable("pending_overflow", &oldest.method, &oldest.params, None)
                    .await;
            }
        }
        self.pending.push_back(PendingFrame {
            at: Instant::now(),
            thread,
            method,
            params,
        });
    }

    /// Route everything whose thread is now known, in arrival order.
    async fn flush_pending(&mut self) {
        let mut keep = VecDeque::with_capacity(self.pending.len());
        let ready: Vec<PendingFrame> = {
            let mut ready = Vec::new();
            while let Some(frame) = self.pending.pop_front() {
                if self.threads.contains_key(&frame.thread) {
                    ready.push(frame);
                } else {
                    keep.push_back(frame);
                }
            }
            ready
        };
        self.pending = keep;
        for frame in ready {
            if let Some(worker_id) = self.threads.get(&frame.thread).cloned() {
                self.deliver_envelope(&worker_id, &frame.method, frame.params)
                    .await;
            }
        }
    }

    async fn expire_pending(&mut self) {
        let now = Instant::now();
        let mut expired = Vec::new();
        let mut keep = VecDeque::with_capacity(self.pending.len());
        while let Some(frame) = self.pending.pop_front() {
            if now.duration_since(frame.at) > PENDING_TTL {
                expired.push(frame);
            } else {
                keep.push_back(frame);
            }
        }
        self.pending = keep;
        for frame in expired {
            self.log_unroutable("unknown_thread", &frame.method, &frame.params, None)
                .await;
        }
    }

    async fn log_unroutable(
        &self,
        reason: &str,
        method: &str,
        params: &Value,
        worker: Option<&str>,
    ) {
        self.log
            .line(json!({
                "ts": Utc::now(),
                "kind": "unroutable",
                "reason": reason,
                "method": method,
                "worker": worker,
                "params": redact_value(params),
            }))
            .await;
    }

    /// Answer a server→client request from the table, then record it against
    /// the owning worker — or, when it names no thread, in `broker.jsonl`.
    async fn on_server_request(&mut self, generation: u64, frame: ServerRequestFrame) {
        if generation != self.generation {
            return; // the child that asked is gone; nobody is waiting
        }
        let answer = responder::respond_to(&frame);
        let adapter = self.adapter_rx.borrow().clone();
        // Answered FIRST and unconditionally: a dropped server request is a
        // permanent hang on the far side.
        let _ = adapter
            .client()
            .respond(frame.id.clone(), answer.payload.clone())
            .await;

        let worker_id = frame
            .params
            .get("threadId")
            .and_then(Value::as_str)
            .and_then(|t| self.threads.get(t))
            .cloned();
        match worker_id.and_then(|id| self.workers.get(&id).map(|w| w.handle.clone())) {
            Some(handle) => {
                let _ = handle
                    .send(WorkerMsg::ServerRequestLogged {
                        method: answer.method.clone(),
                        disposition: answer.disposition.as_str().to_string(),
                    })
                    .await;
            }
            None => {
                let mut line = answer.raw();
                if let Some(obj) = line.as_object_mut() {
                    obj.insert("ts".to_string(), json!(Utc::now()));
                    obj.insert("kind".to_string(), json!("server_request"));
                }
                self.log.line(line).await;
            }
        }
    }

    // -- supervision --------------------------------------------------------

    /// Adopt a freshly launched child as generation `generation`.
    async fn adopt(&mut self, generation: u64, launched: Launched) {
        // Stop the previous generation's prober before starting the next.
        self.child_cancel.cancel();
        self.child_cancel = self.cancel.child_token();

        let adapter = launched.adapter.clone();
        let pid = launched.pid;
        self.child = Some(launched.process);
        // The swap every actor reads before its next write: new stdin, new pid,
        // one atomic step.
        let _ = self.adapter_tx.send(adapter.clone());
        self.auth.reset();

        self.tracker.spawn(notification_pump(
            launched.incoming.notifications,
            self.tx.clone(),
            generation,
        ));
        self.tracker.spawn(server_request_pump(
            launched.incoming.server_requests,
            self.tx.clone(),
            generation,
        ));
        self.tracker.spawn(prober(
            adapter.clone(),
            self.tx.clone(),
            generation,
            self.supervision.probe_interval,
            self.child_cancel.clone(),
        ));

        self.health.generation = generation;
        self.health.child_pid = Some(pid);
        self.health.child_user_agent = Some(launched.info.user_agent.clone());
        self.health.child_started_at = Some(Utc::now());
        if self.degraded {
            // The re-promotion probe succeeded: the transport is back.
            self.degraded = false;
            self.health.degraded = None;
            self.health.degraded_since = None;
            self.health.degraded_reason = None;
            self.health.next_promotion_probe_at = None;
            self.budget.clear();
        }
        self.publish_health().await;

        // Everything the child forgot: the threads. Re-opened, re-bound, and
        // re-fed, one worker at a time, off the registry task.
        let workers: Vec<(String, WorkerHandle, PathBuf)> = self
            .workers
            .values()
            .filter(|w| !w.settled)
            .map(|w| (w.handle.id().to_string(), w.handle.clone(), w.dir.clone()))
            .collect();
        if !workers.is_empty() {
            self.tracker.spawn(reattach(
                workers,
                self.cfg.clone(),
                adapter,
                generation,
            ));
        }
    }

    async fn on_child_died(&mut self, generation: u64, detail: String) {
        if generation != self.generation {
            return; // already superseded
        }
        // Dropping the handle kills the corpse if it is somehow still there.
        self.child = None;
        self.health.child_pid = None;

        if self.degraded {
            // Latched: only the re-promotion probe launches children now.
            self.publish_health().await;
            self.schedule_repromotion().await;
            return;
        }
        self.budget.record(Instant::now());
        if self.budget.exceeded() {
            self.latch_degraded(detail).await;
            return;
        }
        self.health.restarts += 1;
        self.health.restarts_in_window = self.budget.in_window();
        self.publish_health().await;
        let delay = self.budget.policy().backoff(self.budget.attempt());
        self.spawn_launch(delay);
    }

    async fn on_child_started(&mut self, generation: u64, outcome: Result<Launched, AppError>) {
        if generation != self.generation {
            return; // a newer generation already won; this child is dropped
        }
        match outcome {
            Ok(launched) => self.adopt(generation, launched).await,
            // A launch that failed is a death like any other: it counts against
            // the budget and backs off.
            Err(e) => self.on_child_died(generation, e.message).await,
        }
    }

    /// Schedule the next (re)launch as generation `self.generation + 1`.
    fn spawn_launch(&mut self, delay: Duration) {
        self.generation += 1;
        let generation = self.generation;
        let spec = self.spec.clone();
        let tx = self.tx.clone();
        let cancel = self.cancel.clone();
        self.tracker.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
            let outcome = child::launch(&spec).await;
            let _ = tx
                .send(CoreMsg::ChildStarted {
                    generation,
                    outcome: Box::new(outcome),
                })
                .await;
        });
    }

    /// The budget is spent: stop restarting, say so where `doctor` and the
    /// spawn path can see it, and arrange to try again in five minutes.
    async fn latch_degraded(&mut self, detail: String) {
        self.degraded = true;
        self.health.degraded = Some(DEGRADED_EXEC.to_string());
        self.health.degraded_since = Some(Utc::now());
        self.health.degraded_reason = Some(redact::redact_egress(&format!(
            "the app-server child died {} times in {}s: {detail}",
            self.budget.in_window(),
            self.budget.policy().window.as_secs()
        )));
        self.health.restarts_in_window = self.budget.in_window();
        self.log
            .line(json!({"ts": Utc::now(), "kind": "degraded",
                         "transport": DEGRADED_EXEC,
                         "reason": self.health.degraded_reason}))
            .await;
        self.schedule_repromotion().await;
    }

    async fn schedule_repromotion(&mut self) {
        let wait = self.supervision.repromotion_interval;
        self.health.next_promotion_probe_at = Some(
            Utc::now() + chrono::Duration::from_std(wait).unwrap_or_else(|_| chrono::Duration::seconds(300)),
        );
        self.publish_health().await;
        let generation = self.generation;
        let tx = self.tx.clone();
        let cancel = self.cancel.clone();
        self.tracker.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
            let _ = tx.send(CoreMsg::Repromote { generation }).await;
        });
    }

    async fn on_repromote(&mut self, generation: u64) {
        if !self.degraded || generation != self.generation {
            return;
        }
        self.spawn_launch(Duration::ZERO);
    }

    async fn publish_health(&mut self) {
        self.health.workers = self.workers.len();
        self.health.restarts_in_window = self.budget.in_window();
        self.health.updated_at = Utc::now();
        self.log.health(self.health.clone()).await;
    }
}

// ---------------------------------------------------------------------------
// the tasks the core spawns
// ---------------------------------------------------------------------------

/// Forward one child's notifications into the core mailbox; when the stream
/// ends, that IS the child's death (stdout EOF).
async fn notification_pump(
    mut notifications: mpsc::Receiver<RpcNotification>,
    tx: mpsc::Sender<CoreMsg>,
    generation: u64,
) {
    while let Some(n) = notifications.recv().await {
        if tx
            .send(CoreMsg::Notification {
                generation,
                method: n.method,
                params: n.params,
            })
            .await
            .is_err()
        {
            return;
        }
    }
    let _ = tx
        .send(CoreMsg::ChildDied {
            generation,
            detail: "the app-server child closed its stdout".to_string(),
        })
        .await;
}

async fn server_request_pump(
    mut requests: mpsc::Receiver<ServerRequestFrame>,
    tx: mpsc::Sender<CoreMsg>,
    generation: u64,
) {
    while let Some(frame) = requests.recv().await {
        if tx
            .send(CoreMsg::ServerRequest { generation, frame })
            .await
            .is_err()
        {
            return;
        }
    }
}

/// Ask the child a real question on an interval. Two consecutive misses is a
/// death — a wedged child answers nothing but closes nothing either, so EOF
/// alone would never notice it.
async fn prober(
    adapter: AdapterRef,
    tx: mpsc::Sender<CoreMsg>,
    generation: u64,
    interval: Duration,
    cancel: CancellationToken,
) {
    let mut tracker = child::ProbeTracker::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let outcome: Result<Value, RpcError> = adapter
            .client()
            .call(child::PROBE_METHOD, &json!({}), interval)
            .await;
        let dead = match outcome {
            Ok(_) => {
                tracker.hit();
                false
            }
            Err(RpcError::ChildDied) => true,
            Err(RpcError::Timeout) => tracker.miss(),
            // A remote error is an ANSWER: the child is alive and talking.
            Err(_) => {
                tracker.hit();
                false
            }
        };
        if dead {
            let _ = tx
                .send(CoreMsg::ChildDied {
                    generation,
                    detail: format!(
                        "the app-server child missed {} consecutive `{}` probes",
                        child::PROBE_DEATH_STREAK,
                        child::PROBE_METHOD
                    ),
                })
                .await;
            return;
        }
    }
}

/// Poll a worker for the thread id its actor opened.
///
/// Polling rather than a callback because [`WorkerMsg`] is Task 6's contract
/// and the mailbox is FIFO: by the time a `Snapshot` reply comes back, every
/// message the actor accepted before it — including its own `thread/start`
/// bookkeeping — is durable.
async fn learn_thread(id: String, handle: WorkerHandle, tx: mpsc::Sender<CoreMsg>) {
    const POLL: Duration = Duration::from_millis(25);
    const GIVE_UP: Duration = Duration::from_secs(60);
    let deadline = Instant::now() + GIVE_UP;
    loop {
        match handle.snapshot().await {
            Ok(snap) => {
                if let Some(thread_id) = snap.session_id.clone() {
                    let _ = tx
                        .send(CoreMsg::ThreadLearned {
                            id,
                            thread_id,
                            epoch: snap.epoch,
                        })
                        .await;
                    return;
                }
                if snap.state.is_terminal() {
                    return; // it never opened one and never will
                }
            }
            Err(_) => return, // the actor is gone
        }
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Re-attach every live worker to a freshly (re)started child.
///
/// Per worker: `thread/resume` → a `transport_restarted` row carrying the NEW
/// pid → whatever the operator queued while the transport was down. The order
/// matters: the row is what makes a later `a2a kill` aim at the right process,
/// and the re-delivery is what makes an outage invisible to the operator.
///
/// A refused resume is `orphaned`, not `failed`: the worker's thread may still
/// exist on disk, and orphaned is the state that says "recoverable, but not by
/// me".
async fn reattach(
    workers: Vec<(String, WorkerHandle, PathBuf)>,
    cfg: Config,
    adapter: AdapterRef,
    generation: u64,
) {
    // Sequential on purpose, and it is the auth single-flight for this path:
    // the re-deliveries below are turn-starts against a child whose token has
    // not been refreshed yet, and one at a time is exactly what [`AuthGate`]
    // buys for the socket path.
    for (id, handle, dir) in workers {
        let Ok(snap) = handle.snapshot().await else {
            continue;
        };
        if snap.state.is_terminal() || snap.state == WorkerState::Escalated {
            continue;
        }
        let Some(thread_id) = snap.session_id.clone() else {
            continue; // never opened a thread: there is nothing to resume
        };
        let Some(lane) = cfg.lanes.get(&snap.lane).cloned() else {
            continue;
        };
        let outcome = adapter::thread_resume(
            adapter.client(),
            &thread_id,
            &lane,
            &snap.cwd,
            &dir,
            RPC_TIMEOUT,
        )
        .await;
        match outcome {
            Ok(_) => {
                let env = Envelope::new(
                    snap.epoch,
                    Transport::AppServer,
                    EventClass::Lifecycle,
                    EventKind::TransportRestarted,
                    json!({"pid": adapter.pid(), "start_witness": adapter.start_witness(),
                           "generation": generation, "thread_id": thread_id}),
                    Utc::now(),
                );
                if handle.send(WorkerMsg::Envelope(env)).await.is_err() {
                    continue;
                }
                // Anything queued while the transport was down goes out now, as
                // one turn — and the next `turn/start` re-supplies the per-turn
                // overrides `thread/resume` could not carry.
                match inbox::undelivered(&dir, snap.last_delivered_seq) {
                    Ok(pending) if !pending.is_empty() => {
                        let text = pending
                            .iter()
                            .map(|(_, _, body)| body.as_str())
                            .collect::<Vec<_>>()
                            .join("\n\n");
                        if let Err(e) = handle.deliver(text).await {
                            eprintln!(
                                "a2a broker: {id}: re-delivering the inbox after a restart: {}: {}",
                                e.code, e.message
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("a2a broker: {id}: reading the inbox: {e}"),
                }
            }
            Err(e) => {
                let env = Envelope::new(
                    snap.epoch,
                    Transport::AppServer,
                    EventClass::Lifecycle,
                    EventKind::Orphaned,
                    json!({"fsm": "OrphanDetected",
                           "detail": redact::redact_egress(&format!("thread/resume after a transport restart: {e}"))}),
                    Utc::now(),
                );
                let _ = handle.send(WorkerMsg::Envelope(env)).await;
            }
        }
    }
}

/// One `send`, behind the single-flight auth gate.
async fn guarded_send(
    pass: GatePass,
    handle: WorkerHandle,
    text: String,
    reply: oneshot::Sender<Result<SendPath, AppError>>,
) {
    let pilot = match pass {
        GatePass::Open => None,
        GatePass::Pilot(tx) => Some(tx),
        GatePass::Wait(mut rx) => {
            // Err = the pilot's task died without reporting: proceed rather
            // than wait forever.
            let _ = rx.wait_for(|done| *done).await;
            None
        }
    };
    let outcome = handle.deliver(text).await;
    if let Some(tx) = pilot {
        let _ = tx.send(true);
    }
    let _ = reply.send(outcome);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Lane;
    use crate::home::A2aHome;

    /// The registry's surface is these twelve messages, and every one of them
    /// is a message — not a lock, not a shared map, not a second door.
    #[test]
    fn the_registry_is_reached_by_exactly_these_twelve_messages() {
        fn name(msg: &CoreMsg) -> &'static str {
            match msg {
                CoreMsg::Spawn { .. } => "Spawn",
                CoreMsg::Send { .. } => "Send",
                CoreMsg::Interrupt { .. } => "Interrupt",
                CoreMsg::Kill { .. } => "Kill",
                CoreMsg::Resume { .. } => "Resume",
                CoreMsg::List { .. } => "List",
                CoreMsg::Status { .. } => "Status",
                CoreMsg::Subscribe { .. } => "Subscribe",
                CoreMsg::ConnClosed { .. } => "ConnClosed",
                CoreMsg::ChildDied { .. } => "ChildDied",
                CoreMsg::IdleCheck { .. } => "IdleCheck",
                CoreMsg::WorkerSettled(_) => "WorkerSettled",
                // Plumbing: the demux, the supervisor and the thread-learner
                // reach the registry through the same queue as everyone else.
                _ => "internal",
            }
        }
        let dir = PathBuf::from("/tmp/w");
        let surface = vec![
            CoreMsg::Spawn { dir: dir.clone(), reply: oneshot::channel().0 },
            CoreMsg::Send { id: "w".into(), text: "hi".into(), reply: oneshot::channel().0 },
            CoreMsg::Interrupt { id: "w".into(), reply: oneshot::channel().0 },
            CoreMsg::Kill { id: "w".into(), reply: oneshot::channel().0 },
            CoreMsg::Resume { dir, reply: oneshot::channel().0 },
            CoreMsg::List { reply: oneshot::channel().0 },
            CoreMsg::Status { id: "w".into(), reply: oneshot::channel().0 },
            CoreMsg::Subscribe { reply: oneshot::channel().0 },
            CoreMsg::ConnClosed { conn: 1 },
            CoreMsg::ChildDied { generation: 1, detail: "eof".into() },
            CoreMsg::IdleCheck { reply: oneshot::channel().0 },
            CoreMsg::WorkerSettled(Settled { id: "w".into(), state: WorkerState::Done }),
        ];
        let names: Vec<&str> = surface.iter().map(name).collect();
        assert_eq!(
            names,
            vec![
                "Spawn", "Send", "Interrupt", "Kill", "Resume", "List", "Status", "Subscribe",
                "ConnClosed", "ChildDied", "IdleCheck", "WorkerSettled"
            ]
        );
        // The mailbox is bounded — an unbounded queue in front of one owner is
        // just a slower way to run out of memory.
        let (tx, _rx) = mpsc::channel::<CoreMsg>(CORE_MAILBOX_CAPACITY);
        assert_eq!(tx.max_capacity(), CORE_MAILBOX_CAPACITY);
    }

    // -- the routing table ---------------------------------------------------

    /// The pin: `thread/started` keys on `params.thread.id`, everything
    /// thread-scoped keys on `params.threadId`, the three broker-scoped methods
    /// key on nothing, and an unkeyed frame is unroutable rather than lost.
    #[test]
    fn every_notification_routes_by_its_own_key() {
        // The real 0.140.0 shape: NO top-level threadId.
        assert_eq!(
            routing_key(
                "thread/started",
                &json!({"thread": {"id": "th-1", "sessionId": "th-1"}})
            ),
            Route::Thread("th-1".into())
        );
        for method in [
            "turn/started",
            "turn/completed",
            "item/started",
            "item/completed",
            "error",
            "thread/tokenUsage/updated",
            "model/rerouted",
            "guardianWarning",
            "thread/status/changed",
        ] {
            assert_eq!(
                routing_key(method, &json!({"threadId": "th-2", "turn": {"id": "t"}})),
                Route::Thread("th-2".into()),
                "{method} keys on params.threadId"
            );
        }
        for method in events::BROKER_SCOPED_METHODS {
            assert_eq!(
                routing_key(method, &json!({"rateLimits": {}})),
                Route::BrokerScoped,
                "{method} belongs to the broker, not to a worker"
            );
        }
        assert_eq!(
            routing_key("sorcery/unicornSummoned", &json!({"horn": "spiral"})),
            Route::Unroutable
        );
        // A thread/started that somehow carries no id is unroutable, not a panic.
        assert_eq!(routing_key("thread/started", &json!({})), Route::Unroutable);
    }

    // -- auth expiry ---------------------------------------------------------

    #[test]
    fn expired_credentials_are_recognized_wherever_the_message_lives() {
        for params in [
            json!({"threadId": "t", "error": {"message": "401 Unauthorized"}, "willRetry": false}),
            json!({"threadId": "t", "error": "your token expired", "willRetry": false}),
            json!({"threadId": "t", "message": "please re-authenticate", "willRetry": false}),
        ] {
            let message = error_message(&params).expect("a message");
            assert!(is_auth_expiry(&message), "{message}");
        }
        assert_eq!(error_message(&json!({"threadId": "t"})), None);
    }

    #[test]
    fn ordinary_failures_are_not_auth_failures() {
        for message in [
            "the model returned an internal error",
            "rate limit exceeded, retry after 30s",
            "sandbox denied write to /etc/passwd",
            "connection reset by peer",
        ] {
            assert!(!is_auth_expiry(message), "{message} must not be auth_expired");
        }
    }

    /// The whole point of classifying it: the row the worker gets drives the
    /// machine to `failed{auth_expired}` — terminal, resumable, never retried.
    #[test]
    fn an_auth_error_is_marked_as_a_resumable_terminal_failure() {
        let core_marker = |raw: &mut Value| {
            let message = error_message(raw).unwrap_or_default();
            if is_auth_expiry(&message) {
                if let Some(obj) = raw.as_object_mut() {
                    obj.insert("fsm".into(), json!("ProcessExited"));
                    obj.insert("failure_class".into(), json!(FailureClass::AuthExpired));
                }
            }
        };
        let mut raw = json!({"threadId": "t", "error": {"message": "401 Unauthorized"},
                             "willRetry": false});
        core_marker(&mut raw);
        assert_eq!(raw["fsm"], json!("ProcessExited"));
        assert_eq!(raw["failure_class"], json!("auth_expired"));

        // And the fold agrees: this row lands on failed{auth_expired}.
        let mut env = Envelope::new(
            1,
            Transport::AppServer,
            EventClass::Lifecycle,
            EventKind::Error,
            raw,
            Utc::now(),
        );
        env.seq = 7;
        let mut snap = WorkerSnapshot::new_for_test("general-x", Path::new("/tmp"));
        snap.state = WorkerState::Running;
        let snap = fold::fold(snap, std::slice::from_ref(&env), Utc::now());
        assert_eq!(snap.state, WorkerState::Failed);
        assert_eq!(snap.failure_class, Some(FailureClass::AuthExpired));
    }

    // -- the single-flight auth gate ----------------------------------------

    /// N workers, one token refresh: the first caller goes, the rest wait for
    /// it, and once it has landed the gate never closes again (until the child
    /// is replaced).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_auth_gate_lets_exactly_one_turn_start_go_first() {
        let mut gate = AuthGate::default();
        let pilot = match gate.acquire() {
            GatePass::Pilot(tx) => tx,
            _ => panic!("the first caller must be the pilot"),
        };
        let mut waiters = Vec::new();
        for _ in 0..4 {
            match gate.acquire() {
                GatePass::Wait(mut rx) => waiters.push(tokio::spawn(async move {
                    rx.wait_for(|done| *done).await.is_ok()
                })),
                _ => panic!("everyone behind the pilot waits"),
            }
        }
        // Nothing proceeds while the pilot is still going.
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(waiters.iter().all(|w| !w.is_finished()), "a waiter jumped the gate");

        let _ = pilot.send(true);
        for waiter in waiters {
            assert!(waiter.await.unwrap(), "the waiter was released");
        }
        // Afterwards the gate is open: no more serialization.
        assert!(matches!(gate.acquire(), GatePass::Open));
        assert!(matches!(gate.acquire(), GatePass::Open));

        // A new child closes it again.
        gate.reset();
        assert!(matches!(gate.acquire(), GatePass::Pilot(_)));
    }

    /// The pilot's task dying must release the waiters, not deadlock them.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_dead_pilot_releases_the_gate() {
        let mut gate = AuthGate::default();
        let pilot = match gate.acquire() {
            GatePass::Pilot(tx) => tx,
            _ => unreachable!(),
        };
        let waiter = match gate.acquire() {
            GatePass::Wait(mut rx) => tokio::spawn(async move { rx.wait_for(|d| *d).await.is_ok() }),
            _ => unreachable!(),
        };
        drop(pilot);
        assert!(!waiter.await.unwrap(), "released with an error, not hung");
    }

    // -- panic isolation -----------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn only_a_panic_is_a_panic_and_it_takes_no_sibling_with_it() {
        let mut set = tokio::task::JoinSet::<&'static str>::new();
        let doomed = set.spawn(async { panic!("the actor fell over") }).id();
        set.spawn(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            "sibling"
        });
        let mut survivors = Vec::new();
        let mut panicked = None;
        while let Some(next) = set.join_next_with_id().await {
            match next {
                Ok((_, name)) => survivors.push(name),
                Err(e) => {
                    assert_eq!(e.id(), doomed);
                    panicked = panic_detail(&e);
                }
            }
        }
        assert!(panicked.is_some(), "the panic was reported as one");
        assert_eq!(survivors, vec!["sibling"], "the sibling ran to completion");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_clean_exit_is_not_reported_as_a_panic() {
        let handle = tokio::spawn(async {});
        let err = tokio::spawn(async {
            handle.await.unwrap();
            panic!("boom")
        })
        .await
        .unwrap_err();
        assert!(panic_detail(&err).is_some());

        let cancelled = {
            let h = tokio::spawn(async { std::future::pending::<()>().await });
            h.abort();
            h.await.unwrap_err()
        };
        assert!(
            panic_detail(&cancelled).is_none(),
            "a cancelled actor is a shutdown, not a failure"
        );
    }

    /// The other half of panic isolation: what the core WRITES for the worker
    /// whose actor died. It takes the same lock and goes through the same fold,
    /// so the class survives a rebuild from the WAL.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicked_worker_is_recorded_as_failed_internal_panic() {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
        let (cfg, _, _) = crate::config::load(&home).unwrap();
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&work).unwrap();

        let id = "general-0817t1200-aaaa";
        let dir = home.worker_dir(id);
        std::fs::create_dir_all(dir.join("outbox")).unwrap();
        let mut log = EventLog::open(&dir).unwrap();
        let snap = fold::append_and_fold(
            &mut log,
            &dir,
            WorkerSnapshot::new_for_test(id, &work),
            EventKind::Spawned,
            json!({"fsm":"Spawn","id":id,"lane":Lane::General.as_str(),"cwd":work,
                   "transport":"app-server"}),
            Utc::now(),
        )
        .unwrap();
        drop(log);
        assert_eq!(snap.state, WorkerState::Spawned);

        fail_worker(
            dir.clone(),
            cfg.clone(),
            FailureClass::InternalPanic,
            "the worker actor panicked".to_string(),
        )
        .await
        .unwrap();

        let snap = state::load_snapshot(&dir).unwrap();
        assert_eq!(snap.state, WorkerState::Failed);
        assert_eq!(
            snap.failure_class,
            Some(FailureClass::InternalPanic),
            "not `never_started`: the broker knows a panic when it sees one"
        );
        let rebuilt = fold::fold(
            WorkerSnapshot::new_for_test(id, &work),
            &store::read_events(&dir, 0).unwrap(),
            Utc::now(),
        );
        assert_eq!(rebuilt.failure_class, Some(FailureClass::InternalPanic));
        assert!(
            rebuilt.extra.get("fold_rejections").is_none(),
            "the row the core wrote is a legal transition"
        );

        // Idempotent where it matters: a worker that already finished is not
        // re-failed by a late corpse report.
        let before = snap.last_applied_seq;
        fail_worker(dir.clone(), cfg, FailureClass::InternalPanic, "again".into())
            .await
            .unwrap();
        let after = state::load_snapshot(&dir).unwrap();
        assert_eq!(after.last_applied_seq, before, "no second row");
    }

    // -- health & the broker log --------------------------------------------

    #[test]
    fn health_keeps_only_the_most_recent_notices() {
        let mut health = BrokerHealth::default();
        for n in 0..(HEALTH_NOTICE_CAPACITY * 3) {
            health.push_notice(Notice {
                ts: Utc::now(),
                method: "deprecationNotice".into(),
                params: json!({"n": n}),
            });
        }
        assert_eq!(health.notices.len(), HEALTH_NOTICE_CAPACITY);
        assert_eq!(
            health.notices[0].params["n"],
            json!(HEALTH_NOTICE_CAPACITY * 2),
            "the OLDEST are the ones that go"
        );
    }

    #[test]
    fn every_string_that_reaches_broker_jsonl_is_redacted() {
        let value = json!({
            "params": {"command": ["sh", "-c", "export OPENAI_API_KEY=sk-abcdefghijklmnopqrstuvwxyz012345"]},
            "n": 7,
            "ok": true,
        });
        let redacted = redact_value(&value);
        let text = redacted.to_string();
        assert!(
            !text.contains("sk-abcdefghijklmnopqrstuvwxyz012345"),
            "a secret rode into broker.jsonl: {text}"
        );
        assert_eq!(redacted["n"], json!(7), "non-strings are untouched");
        assert_eq!(redacted["ok"], json!(true));
    }

    #[test]
    fn the_degraded_latch_names_the_transport_spawns_fall_back_to() {
        assert_eq!(DEGRADED_EXEC, "exec");
        let health = BrokerHealth {
            degraded: Some(DEGRADED_EXEC.to_string()),
            ..Default::default()
        };
        let text = serde_json::to_string(&health).unwrap();
        let back: BrokerHealth = serde_json::from_str(&text).unwrap();
        assert_eq!(back.degraded.as_deref(), Some("exec"));
        assert_eq!(back.schema_version, HEALTH_SCHEMA_VERSION);
    }
}
