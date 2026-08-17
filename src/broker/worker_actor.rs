//! One `WorkerActor` per app-server worker: the sole writer to that worker's
//! directory (spec §4.2, §4.5, §4.6).
//!
//! The actor is the app-server transport's answer to the question Phase 1
//! answered with reconcile-on-read: *who owns the worker's state?* On exec,
//! every CLI read path owned it, because there was no process alive to. Here a
//! broker task holds the whole conversation, so it owns it — and everything the
//! machine learns arrives as a message on ONE bounded mailbox:
//!
//! ```text
//!   app-server notifications ──demux──▶ WorkerMsg::Envelope ─┐
//!   CLI over the socket ──────core───▶ Deliver/Interrupt/Kill ├─▶ WorkerActor
//!   lane deadlines ───────────────────▶ (its own timer)       ┘        │
//!                                                                      ▼
//!                        state.lock ▸ EventLog::open ▸ append ▸ fold ▸ state.json
//! ```
//!
//! Four rules make that safe, and each one is load-bearing:
//!
//! * **Sole writer, alongside Phase 1.** `a2a status` on an app-server worker
//!   still reconciles it from the same CLI process it always did. So every
//!   persistence step here takes [`crate::reconcile::lock_state`] and re-opens
//!   the [`EventLog`] *inside* that lock, in a `spawn_blocking` — the seq
//!   counter is read from the file every time and never cached across an
//!   `.await`, which is exactly the bug that would let a CLI append and an
//!   actor append collide on one seq.
//! * **Trace never drives the machine.** Lifecycle envelopes go through
//!   [`fold::append_envelope_and_fold`]; trace envelopes go straight to
//!   [`EventLog::append`] and are buffered, so a flood of tool-call chatter
//!   costs no folds and no snapshot writes. When the buffer overflows the
//!   OLDEST are dropped and a synthetic `events_dropped` envelope records how
//!   many — losing observability loudly beats stalling the transport quietly.
//! * **The turn conclusion is not duplicated.** [`reconcile::conclude_turn`] is
//!   the single implementation of "deliver at the boundary → schema gate →
//!   `TurnEnded`", so an app-server turn and an exec turn cannot drift. The
//!   actor's only extra step is writing `result.md` first: the app-server has no
//!   `--output-last-message`, so the turn's final agent message *is* the result
//!   ([`events::synthesize_result`]) and it has to exist before the gate reads
//!   it.
//! * **Kill is never a signal.** The pid this worker records is the app-server
//!   child EVERY worker is multiplexed over. `Kill` is `turn/interrupt` plus a
//!   `Cancelled` commit; the FSM's `KillProcess` action is exec's, and the actor
//!   performs no actions at all.
//!
//! The adapter arrives as a [`watch::Receiver`] rather than a value because
//! child supervision replaces it: after a restart the broker publishes a fresh
//! [`AdapterRef`] (new stdin, new pid) and every actor picks it up on its next
//! `Deliver`/`Interrupt`, so a post-restart delivery can never be written into a
//! dead child's stdin.

use crate::adapter::{self, AdapterRef, SendPath, TurnSpec};
use crate::config::{Config, LaneConfig};
use crate::events::{self, Envelope, EventClass, EventKind};
use crate::fold;
use crate::fsm::{self, FsmEvent, TimeoutKind, WorkerState};
use crate::inbox;
use crate::output::{AppError, ExitCode};
use crate::redact;
use crate::reconcile;
use crate::rpc::RpcError;
use crate::state::{save_snapshot, WorkerSnapshot};
use crate::store::{self, EventLog};
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Per-worker mailbox bound (spec §4.5). Bounded, so a notification storm backs
/// the demux up instead of growing without limit — and so the actor's own
/// startup (thread/start, then the `ProcessStarted` WAL write) can happen with
/// notifications already queued behind it.
pub const MAILBOX_CAPACITY: usize = 1024;

/// How many un-flushed **trace** envelopes are held before the oldest start
/// being dropped. Deliberately far below the mailbox bound: trace is the one
/// stream a2a is allowed to lose, and losing it early is what keeps the
/// lifecycle stream (which it must never lose) responsive.
pub const TRACE_BUFFER_CAPACITY: usize = 256;

/// Flush the trace buffer once this many envelopes are waiting, even if no
/// lifecycle event has arrived to force the issue.
const TRACE_FLUSH_THRESHOLD: usize = 64;

/// Per-request budget for the app-server calls the actor makes. Generous: it
/// bounds a wedged child, not a slow model.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// the mailbox
// ---------------------------------------------------------------------------

/// Everything that can reach a worker. One enum, one queue, one owner.
pub enum WorkerMsg {
    /// A mapped app-server notification for this worker. `raw` is the
    /// notification's own params (which is what [`events::synthesize_result`]
    /// reads), and `class` decides whether it folds or is pure trace.
    Envelope(events::Envelope),
    /// One server→client request was answered by [`crate::responder`]. Only the
    /// method and the disposition are recorded — never the params, which can
    /// carry a command line (spec §5).
    ServerRequestLogged { method: String, disposition: String },
    /// Hand `text` to the worker: `turn/steer` if a turn is open, `turn/start`
    /// otherwise (the adapter's decision table). Also acknowledges whatever the
    /// operator had queued in `inbox/`.
    Deliver {
        text: String,
        reply: oneshot::Sender<Result<SendPath, AppError>>,
    },
    /// True interrupt of the open turn.
    Interrupt {
        reply: oneshot::Sender<Result<InterruptOutcome, AppError>>,
    },
    /// Stop this worker: `turn/interrupt` + `Cancelled`. NEVER a process kill.
    Kill {
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// The worker's current snapshot, without touching the filesystem.
    Snapshot {
        reply: oneshot::Sender<WorkerSnapshot>,
    },
    /// Drain and stop. The trace buffer is flushed on the way out.
    Shutdown,
}

/// What an `Interrupt` did. Interrupting a turn that has already ended is a
/// deliberate no-op (exit 0 with `data.noop:true` at the CLI), not an error:
/// the operator's intent — "stop what you are doing" — is already satisfied.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InterruptOutcome {
    Interrupted,
    NoopTurnAlreadyOver,
}

/// A worker reached a state nobody is waiting on any more: terminal, or
/// `escalated` (which frees its admission slot even though it is resumable).
///
/// Defined here rather than in the core so this module has no forward
/// dependency on the broker's `CoreMsg`: the core wraps this channel, admission
/// drains against it, and idle-shutdown keys its liveness off it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Settled {
    pub id: String,
    pub state: WorkerState,
}

/// The far end of a worker's mailbox.
#[derive(Clone)]
pub struct WorkerHandle {
    id: String,
    tx: mpsc::Sender<WorkerMsg>,
}

impl WorkerHandle {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The raw sender, for the broker's notification demux (which forwards
    /// envelopes and must be able to feel this mailbox's backpressure).
    pub fn sender(&self) -> &mpsc::Sender<WorkerMsg> {
        &self.tx
    }

    pub async fn send(&self, msg: WorkerMsg) -> Result<(), AppError> {
        self.tx.send(msg).await.map_err(|_| gone(&self.id))
    }

    pub async fn snapshot(&self) -> Result<WorkerSnapshot, AppError> {
        self.ask(|reply| WorkerMsg::Snapshot { reply }).await
    }

    pub async fn deliver(&self, text: impl Into<String>) -> Result<SendPath, AppError> {
        let text = text.into();
        self.ask(move |reply| WorkerMsg::Deliver { text, reply })
            .await?
    }

    pub async fn interrupt(&self) -> Result<InterruptOutcome, AppError> {
        self.ask(|reply| WorkerMsg::Interrupt { reply }).await?
    }

    pub async fn kill(&self) -> Result<(), AppError> {
        self.ask(|reply| WorkerMsg::Kill { reply }).await?
    }

    pub async fn shutdown(&self) {
        let _ = self.tx.send(WorkerMsg::Shutdown).await;
    }

    /// Request/response over the mailbox. A closed mailbox or a dropped reply
    /// is `WORKER_GONE` — never a hang.
    async fn ask<T, F>(&self, build: F) -> Result<T, AppError>
    where
        F: FnOnce(oneshot::Sender<T>) -> WorkerMsg,
    {
        let (tx, rx) = oneshot::channel();
        self.tx.send(build(tx)).await.map_err(|_| gone(&self.id))?;
        rx.await.map_err(|_| gone(&self.id))
    }
}

fn gone(id: &str) -> AppError {
    AppError::new(
        "WORKER_GONE",
        ExitCode::BrokerUnavailable,
        format!("worker {id} is no longer being tracked by the broker"),
    )
    .remedy("`a2a status <id>` reconciles it from its own files")
}

/// Spawn the actor onto `tracker` and return its handle.
///
/// `cfg` is owned by the actor because two of its jobs need lane policy on
/// every step: the per-turn sandbox/effort it re-supplies to `turn/start`, and
/// the deadlines it arms from `start`/`task`/`blocked_timeout_secs`.
pub fn spawn_worker_actor(
    dir: PathBuf,
    snap: WorkerSnapshot,
    cfg: Config,
    adapter_rx: watch::Receiver<AdapterRef>,
    settle_tx: mpsc::Sender<Settled>,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) -> WorkerHandle {
    let (tx, rx) = mpsc::channel(MAILBOX_CAPACITY);
    let id = snap.id.clone();
    let actor = WorkerActor {
        output_schema: read_output_schema(snap.schema_path.as_deref()),
        dir,
        snap,
        cfg,
        adapter_rx,
        settle_tx,
        cancel,
        rx,
        trace: TraceBuffer::new(TRACE_BUFFER_CAPACITY),
        turn_messages: Vec::new(),
        open_turn: None,
        settled: None,
    };
    tracker.spawn(actor.run());
    WorkerHandle { id, tx }
}

// ---------------------------------------------------------------------------
// the actor
// ---------------------------------------------------------------------------

pub struct WorkerActor {
    dir: PathBuf,
    snap: WorkerSnapshot,
    cfg: Config,
    adapter_rx: watch::Receiver<AdapterRef>,
    settle_tx: mpsc::Sender<Settled>,
    cancel: CancellationToken,
    rx: mpsc::Receiver<WorkerMsg>,
    trace: TraceBuffer,
    /// This turn's agent messages, in arrival order — the input to
    /// [`events::synthesize_result`]. Only agent messages are kept: a turn that
    /// ran a thousand commands still holds at most a handful of these.
    turn_messages: Vec<Envelope>,
    /// `Some` only while a turn is OPEN. Cleared when the turn completes, which
    /// is what makes the next `Deliver` a `turn/start` instead of a `turn/steer`
    /// that is guaranteed to lose its precondition race.
    open_turn: Option<String>,
    /// `--schema`, re-supplied on EVERY `turn/start` (it is per-turn, not
    /// sticky).
    output_schema: Option<Value>,
    /// The last settle already announced, so a second write in a terminal state
    /// cannot announce it twice.
    settled: Option<WorkerState>,
}

impl WorkerActor {
    async fn run(mut self) {
        if let Err(e) = self.start_thread().await {
            self.record_start_failure(e).await;
        }
        let cancel = self.cancel.clone();
        loop {
            let deadline = self.deadline_instant();
            let msg = tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = wait_until(deadline) => {
                    self.fire_deadline().await;
                    continue;
                }
                msg = self.rx.recv() => msg,
            };
            match msg {
                // Every handle was dropped: nobody can reach this worker again.
                None => break,
                Some(WorkerMsg::Shutdown) => break,
                Some(msg) => self.handle(msg).await,
            }
        }
        // Whatever observability was buffered belongs in the log, not in a
        // dropped `VecDeque`.
        self.flush_trace().await;
    }

    async fn handle(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::Envelope(env) => self.on_envelope(env).await,
            WorkerMsg::ServerRequestLogged {
                method,
                disposition,
            } => {
                self.flush_trace().await;
                let mut env = self.envelope(
                    EventClass::Lifecycle,
                    EventKind::ServerRequest,
                    json!({"method": method, "disposition": disposition}),
                );
                if let Err(e) = self.append_lifecycle(&mut env).await {
                    self.warn("recording a server request", &e);
                }
            }
            WorkerMsg::Deliver { text, reply } => {
                let outcome = self.deliver(text).await;
                let _ = reply.send(outcome);
            }
            WorkerMsg::Interrupt { reply } => {
                let outcome = self.interrupt().await;
                let _ = reply.send(outcome);
            }
            WorkerMsg::Kill { reply } => {
                let outcome = self.kill().await;
                let _ = reply.send(outcome);
            }
            WorkerMsg::Snapshot { reply } => {
                let _ = reply.send(self.snap.clone());
            }
            // Handled by the loop so it can break; listed to stay total.
            WorkerMsg::Shutdown => {}
        }
    }

    // -- startup ------------------------------------------------------------

    /// Open this worker's thread and record that its transport is attached.
    ///
    /// The event order here is a pin, and the mailbox is what makes it
    /// achievable: nothing has been drained yet, so notifications that raced the
    /// `thread/start` response are queued (bounded) behind a `ProcessStarted`
    /// that is already durable by the time the first of them is folded.
    ///
    /// A worker that is not `spawned` was resumed or re-attached: its thread
    /// already exists and the broker re-binds it, so there is nothing to start.
    async fn start_thread(&mut self) -> Result<(), AppError> {
        if self.snap.state != WorkerState::Spawned {
            return Ok(());
        }
        let adapter = self.adapter();
        let lane = self.lane()?;
        let cwd = self.snap.cwd.clone();
        let dir = self.dir.clone();
        let info = adapter::thread_start(adapter.client(), &lane, &cwd, &dir, RPC_TIMEOUT)
            .await
            .map_err(|e| rpc_err("thread/start", e))?;

        // The shared child's identity, recorded for observability only: the
        // kill and timeout paths of an app-server worker route through the
        // broker and NEVER group-kill this pid (§4.2 — it is every worker's).
        let pid = adapter.pid().unwrap_or(0);
        let witness = adapter.start_witness().to_string();
        let thread_id = info.thread_id.clone();
        self.persist("process_started", move |log, dir, snap| {
            let snap = fold::append_and_fold(
                log,
                dir,
                snap,
                EventKind::StateChange,
                json!({"fsm": "ProcessStarted", "pid": pid, "start_witness": witness,
                       "thread_id": thread_id}),
                Utc::now(),
            )?;
            Ok((snap, ()))
        })
        .await?;

        // Gated exactly as reconcile gates the exec spelling: identity is
        // recorded only from `running`, so this can never be a rejection.
        if self.snap.state == WorkerState::Running {
            let session_id = info.session_id.clone();
            let thread_id = info.thread_id.clone();
            self.persist("session_identified", move |log, dir, snap| {
                let snap = fold::append_and_fold(
                    log,
                    dir,
                    snap,
                    EventKind::SessionIdentified,
                    json!({"fsm": "SessionIdentified", "session_id": session_id,
                           "rollout_path": null, "thread_id": thread_id}),
                    Utc::now(),
                )?;
                Ok((snap, ()))
            })
            .await?;
        }
        Ok(())
    }

    /// A thread that never started is `failed{never_started}` — the same shape
    /// `spawn` records when the exec fork fails, so an orchestrator reads one
    /// answer for "the transport never came up" whichever transport it was.
    async fn record_start_failure(&mut self, err: AppError) {
        let detail = redact::redact_egress(&format!("{}: {}", err.code, err.message));
        let outcome = self
            .persist("start_failed", move |log, dir, snap| {
                let snap = fold::append_and_fold(
                    log,
                    dir,
                    snap,
                    EventKind::StateChange,
                    json!({"fsm": "ProcessExited", "code": null, "detail": detail}),
                    Utc::now(),
                )?;
                Ok((snap, ()))
            })
            .await;
        if let Err(e) = outcome {
            self.warn("recording a failed thread start", &e);
        }
    }

    // -- notifications ------------------------------------------------------

    async fn on_envelope(&mut self, mut env: Envelope) {
        if env.class != EventClass::Lifecycle {
            self.trace.push(env);
            if self.trace.len() >= TRACE_FLUSH_THRESHOLD {
                self.flush_trace().await;
            }
            return;
        }
        // Buffered trace lines were observed BEFORE this one; flushing here is
        // what keeps `events.jsonl` in the order the transport spoke.
        self.flush_trace().await;

        match env.kind {
            EventKind::TurnStarted => {
                self.open_turn = turn_id_of(&env.raw);
                self.turn_messages.clear();
            }
            EventKind::AgentMessage => self.turn_messages.push(env.clone()),
            EventKind::SessionIdentified => self.gate_session_identified(&mut env),
            _ => {}
        }
        // A turn only concludes from `running`: an interrupted or killed worker
        // still receives its `turn/completed`, and committing `TurnEnded` from
        // `awaiting-input` would be a rejection, not a conclusion.
        let concludes = env.kind == EventKind::TurnCompleted && self.snap.state == WorkerState::Running;

        if let Err(e) = self.append_lifecycle(&mut env).await {
            self.warn("appending a lifecycle event", &e);
            return;
        }
        if concludes {
            self.conclude_turn().await;
        }
    }

    /// Merge the FSM payload into a `thread/started` notification when — and
    /// only when — it would be a legal transition carrying news.
    ///
    /// Merging (rather than appending a second envelope) keeps the wire payload
    /// and the machine event as one row: `a2a tail` shows what the server said,
    /// and the fold sees an event it can apply.
    fn gate_session_identified(&self, env: &mut Envelope) {
        if self.snap.state != WorkerState::Running {
            return;
        }
        let Some(id) = session_id_of(&env.raw) else {
            return;
        };
        if self.snap.session_id.as_deref() == Some(id.as_str()) {
            return; // already recorded: a self-loop with no news
        }
        let rollout = rollout_path_of(&env.raw);
        if let Some(obj) = env.raw.as_object_mut() {
            obj.insert("fsm".to_string(), json!("SessionIdentified"));
            obj.insert("session_id".to_string(), json!(id));
            obj.insert("rollout_path".to_string(), json!(rollout));
        }
    }

    /// The turn ended: publish its result, then conclude it through the very
    /// same code the exec transport concludes with.
    async fn conclude_turn(&mut self) {
        self.open_turn = None;
        let messages = std::mem::take(&mut self.turn_messages);

        // The app-server has no `--output-last-message`: the turn's final agent
        // message IS the result, and it must exist BEFORE the schema gate reads
        // it. `write_atomic` creates it 0600, which is stricter than what codex
        // leaves behind on exec.
        if let Some(text) = events::synthesize_result(&messages) {
            if let Err(e) = store::write_atomic(&self.dir.join(reconcile::RESULT_MD), text.as_bytes())
            {
                self.warn(
                    "writing result.md",
                    &io_err("writing the synthesized result", e),
                );
            }
        }

        let cfg = self.cfg.clone();
        let outcome = self
            .persist("turn_ended", move |log, dir, snap| {
                // Both observations are taken under the lock, so nothing can
                // land between reading them and committing what they imply.
                let has_result = reconcile::has_result(dir);
                let has_question =
                    reconcile::has_unanswered_question(dir, snap.last_delivered_seq);
                let snap = reconcile::conclude_turn(
                    log,
                    dir,
                    snap,
                    &cfg,
                    Utc::now(),
                    has_result,
                    has_question,
                )?;
                Ok((snap, ()))
            })
            .await;
        if let Err(e) = outcome {
            self.warn("concluding the turn", &e);
        }
    }

    // -- commands -----------------------------------------------------------

    /// `a2a send`, on this transport: WAL first, transport second — the same
    /// order `commands/send.rs` uses, so the operator's act is recorded whether
    /// or not the child then accepts it.
    async fn deliver(&mut self, text: String) -> Result<SendPath, AppError> {
        // Pre-checked rather than folded-and-rejected: a `send` to a terminal
        // worker is the operator's mistake and deserves the FSM's own remedy,
        // not a line in `fold_rejections`.
        fsm::step(self.snap.state, self.snap.state_reason, &FsmEvent::Send).map_err(|r| {
            AppError::new("ILLEGAL_TRANSITION", ExitCode::Precondition, r.to_string())
                .remedy(r.reason)
        })?;
        let thread_id = self.thread_id()?;
        let adapter = self.adapter();
        let lane = self.lane()?;
        let cwd = self.snap.cwd.clone();
        let dir = self.dir.clone();
        let schema = self.output_schema.clone();
        let open_turn = self.open_turn.clone();

        self.flush_trace().await;
        self.persist("send", move |log, dir, snap| {
            let snap = fold::append_and_fold(
                log,
                dir,
                snap,
                EventKind::StateChange,
                json!({"fsm": "Send"}),
                Utc::now(),
            )?;
            Ok((snap, ()))
        })
        .await?;

        let spec = TurnSpec {
            thread_id: &thread_id,
            text: &text,
            lane: &lane,
            cwd: &cwd,
            worker_dir: &dir,
            output_schema: schema.as_ref(),
        };
        let sent = adapter
            .send_message(&spec, open_turn.as_deref(), RPC_TIMEOUT)
            .await
            .map_err(|e| rpc_err("turn/steer", e))?;
        self.open_turn = Some(sent.turn_id.clone());

        // Acknowledged only after the transport took it: the queue is
        // at-least-once, and this watermark is what dedupes a re-delivery —
        // and what tells `has_unanswered_question` that a question was answered.
        let path = sent.path;
        let turn_id = sent.turn_id;
        let pid = adapter.pid();
        let witness = adapter.start_witness().to_string();
        self.persist("inbox_delivered", move |log, dir, snap| {
            let (watermark, seqs) = acknowledge_inbox(dir, &snap)?;
            let mut env = Envelope::new(
                snap.epoch,
                snap.transport,
                EventClass::Lifecycle,
                EventKind::InboxDelivered,
                json!({"watermark": watermark, "seqs": seqs, "path": path.as_str(),
                       "turn_id": turn_id, "pid": pid, "start_witness": witness}),
                Utc::now(),
            );
            let snap = fold::append_envelope_and_fold(log, dir, snap, &mut env, Utc::now())?;
            Ok((snap, ()))
        })
        .await?;
        Ok(path)
    }

    async fn interrupt(&mut self) -> Result<InterruptOutcome, AppError> {
        let Some(turn_id) = self.open_turn.clone() else {
            // Deliberate (plan §"deliberate deviations"): interrupting a turn
            // that is already over is exit 0 + `noop`, never an error.
            return Ok(InterruptOutcome::NoopTurnAlreadyOver);
        };
        let thread_id = self.thread_id()?;
        self.adapter()
            .turn_interrupt(&thread_id, &turn_id, RPC_TIMEOUT)
            .await
            .map_err(|e| rpc_err("turn/interrupt", e))?;
        self.open_turn = None;

        self.flush_trace().await;
        self.persist("interrupt", move |log, dir, snap| {
            let snap = fold::append_and_fold(
                log,
                dir,
                snap,
                EventKind::Interrupted,
                json!({"fsm": "Interrupt", "turn_id": turn_id}),
                Utc::now(),
            )?;
            Ok((snap, ()))
        })
        .await?;
        Ok(InterruptOutcome::Interrupted)
    }

    /// Stop the worker. On this transport that is `turn/interrupt` plus a
    /// `Cancelled` commit — **never** a signal: the pid recorded against this
    /// worker is the app-server child every other worker is multiplexed over.
    /// (The FSM's `KillProcess` action is exec's; the actor performs no
    /// actions, which is what makes that structural rather than remembered.)
    async fn kill(&mut self) -> Result<(), AppError> {
        if let Some(turn_id) = self.open_turn.take() {
            if let Ok(thread_id) = self.thread_id() {
                // Best effort: a turn we could not interrupt must not stop us
                // from recording that the operator cancelled the worker.
                if let Err(e) = self
                    .adapter()
                    .turn_interrupt(&thread_id, &turn_id, RPC_TIMEOUT)
                    .await
                {
                    self.warn("interrupting the turn before a kill", &rpc_err("turn/interrupt", e));
                }
            }
        }
        self.flush_trace().await;
        self.persist("kill", move |log, dir, snap| {
            let snap = fold::append_and_fold(
                log,
                dir,
                snap,
                EventKind::Killed,
                json!({"fsm": "Kill"}),
                Utc::now(),
            )?;
            Ok((snap, ()))
        })
        .await?;
        Ok(())
    }

    // -- deadlines ----------------------------------------------------------

    /// The armed deadline as a runtime instant, or `None` when this state
    /// carries none. Derived (never incremented), so repeated evaluation of an
    /// unchanged worker computes the same instant.
    fn deadline_instant(&self) -> Option<tokio::time::Instant> {
        let at = self.snap.deadline_at?;
        let remaining = (at - Utc::now()).to_std().unwrap_or(Duration::ZERO);
        Some(tokio::time::Instant::now() + remaining)
    }

    /// Fire the lane deadline this state is guarded by, through the WAL — a
    /// timeout is an event (spec §4.6), not a special case.
    async fn fire_deadline(&mut self) {
        let Some(kind) = reconcile::timeout_kind(self.snap.state) else {
            self.snap.deadline_at = None;
            return;
        };
        // A task timeout stops the TURN on this transport. The exec path's
        // `KillProcess` would signal a process group; here that group is the
        // shared child, so the equivalent is an interrupt.
        if kind == TimeoutKind::Task {
            if let Some(turn_id) = self.open_turn.take() {
                if let Ok(thread_id) = self.thread_id() {
                    let _ = self
                        .adapter()
                        .turn_interrupt(&thread_id, &turn_id, RPC_TIMEOUT)
                        .await;
                }
            }
        }
        let name = reconcile::timeout_name(kind);
        self.flush_trace().await;
        let outcome = self
            .persist("timeout", move |log, dir, snap| {
                let snap = fold::append_and_fold(
                    log,
                    dir,
                    snap,
                    EventKind::Timeout,
                    json!({"fsm": "Timeout", "kind": name}),
                    Utc::now(),
                )?;
                Ok((snap, ()))
            })
            .await;
        if let Err(e) = outcome {
            self.warn("firing a lane deadline", &e);
        }
        // Disarm anything still in the past so one expiry fires once. A state
        // that changed re-derived a future deadline already.
        if self.snap.deadline_at.is_some_and(|at| at <= Utc::now()) {
            self.snap.deadline_at = None;
        }
    }

    // -- persistence --------------------------------------------------------

    /// The ONE way this actor writes. Everything it does — lifecycle, trace,
    /// turn conclusion — funnels through here, which is why "sole writer" holds
    /// even though Phase 1's CLI paths still reconcile the same worker.
    ///
    /// Inside a `spawn_blocking` (the lock retry and `fsync` are blocking, and
    /// blocking the runtime would stall every other worker), the closure gets:
    ///
    /// 1. `state.lock`, exclusive — the same lock a `reconcile` takes;
    /// 2. a **freshly opened** [`EventLog`], so its `next_seq` is read from the
    ///    file under that lock and is never a value cached across an `.await`;
    /// 3. the snapshot, by value.
    ///
    /// The deadline is re-armed and the checkpoint written inside the same
    /// lock, so a reader never sees a snapshot whose deadline belongs to the
    /// state it just left.
    async fn persist<T, F>(&mut self, what: &'static str, f: F) -> Result<T, AppError>
    where
        F: FnOnce(&mut EventLog, &Path, WorkerSnapshot) -> Result<(WorkerSnapshot, T), AppError>
            + Send
            + 'static,
        T: Send + 'static,
    {
        let dir = self.dir.clone();
        let cfg = self.cfg.clone();
        let snap = self.snap.clone();
        let joined = tokio::task::spawn_blocking(move || {
            let _lock = reconcile::lock_state(&dir)?;
            let mut log =
                EventLog::open(&dir).map_err(|e| io_err("opening events.jsonl", e))?;
            let (mut snap, out) = f(&mut log, &dir, snap)?;
            reconcile::arm_deadline(&mut snap, &cfg);
            save_snapshot(&dir, &snap).map_err(|e| io_err("writing state.json", e))?;
            Ok::<_, AppError>((snap, out))
        })
        .await;
        let (snap, out) = match joined {
            Ok(result) => result?,
            Err(e) => {
                return Err(AppError::new(
                    "INTERNAL",
                    ExitCode::Internal,
                    format!("{what}: the worker's persistence task failed: {e}"),
                ))
            }
        };
        let previous = self.snap.state;
        self.snap = snap;
        self.announce_settled(previous).await;
        Ok(out)
    }

    /// A caller-built lifecycle envelope, through the fold.
    async fn append_lifecycle(&mut self, env: &mut Envelope) -> Result<(), AppError> {
        let mut owned = env.clone();
        let stamped = self
            .persist("lifecycle", move |log, dir, snap| {
                let snap = fold::append_envelope_and_fold(log, dir, snap, &mut owned, Utc::now())?;
                Ok((snap, owned))
            })
            .await?;
        *env = stamped;
        Ok(())
    }

    /// Write out the buffered trace, plus the `events_dropped` marker if the
    /// buffer overflowed since the last flush.
    ///
    /// Trace goes through [`EventLog::append`] and NOT through the fold: an
    /// observability line must never move the machine, and must never cost a
    /// `state.json` rewrite per line.
    async fn flush_trace(&mut self) {
        let batch = self
            .trace
            .drain(self.snap.epoch, self.snap.transport, Utc::now());
        if batch.is_empty() {
            return;
        }
        let outcome = self
            .persist("trace", move |log, _dir, snap| {
                let mut snap = snap;
                let mut highest = snap.last_event_seq;
                for mut env in batch {
                    log.append(&mut env)
                        .map_err(|e| io_err("appending a trace event", e))?;
                    highest = highest.max(env.seq);
                }
                // The applied watermark deliberately does NOT move: these
                // events were never folded, and a later replay of them is a
                // no-op by construction.
                snap.last_event_seq = highest;
                Ok((snap, ()))
            })
            .await;
        if let Err(e) = outcome {
            self.warn("flushing buffered trace events", &e);
        }
    }

    async fn announce_settled(&mut self, previous: WorkerState) {
        let state = self.snap.state;
        let settling = state.is_terminal() || state == WorkerState::Escalated;
        if !settling || state == previous || self.settled == Some(state) {
            return;
        }
        self.settled = Some(state);
        let _ = self
            .settle_tx
            .send(Settled {
                id: self.snap.id.clone(),
                state,
            })
            .await;
    }

    // -- small helpers ------------------------------------------------------

    /// The CURRENT adapter. Read fresh on every use: supervision swaps it after
    /// a child restart, and a cached one would write into a dead stdin.
    fn adapter(&self) -> AdapterRef {
        self.adapter_rx.borrow().clone()
    }

    fn lane(&self) -> Result<LaneConfig, AppError> {
        self.cfg.lanes.get(&self.snap.lane).cloned().ok_or_else(|| {
            AppError::new(
                "CONFIG",
                ExitCode::Precondition,
                format!("no configuration for the `{}` lane", self.snap.lane.as_str()),
            )
        })
    }

    /// `thread.id == thread.sessionId` on the live server, and the snapshot
    /// records the session id — so that is the thread this worker steers.
    fn thread_id(&self) -> Result<String, AppError> {
        self.snap.session_id.clone().ok_or_else(|| {
            AppError::new(
                "SESSION_UNKNOWN",
                ExitCode::Precondition,
                format!("worker {} has no app-server thread yet", self.snap.id),
            )
            .remedy("`a2a tail <id>` shows whether the thread ever started")
        })
    }

    fn envelope(&self, class: EventClass, kind: EventKind, raw: Value) -> Envelope {
        Envelope::new(self.snap.epoch, self.snap.transport, class, kind, raw, Utc::now())
    }

    /// The daemon's stderr is `broker.log`; a persistence failure is an
    /// operational fact, never a reason to abandon the worker.
    fn warn(&self, ctx: &str, e: &AppError) {
        eprintln!("a2a broker: {}: {ctx}: {}: {}", self.snap.id, e.code, e.message);
    }
}

// ---------------------------------------------------------------------------
// the trace buffer
// ---------------------------------------------------------------------------

/// Bounded, drop-**oldest** buffer for trace envelopes.
///
/// Drop-oldest rather than drop-newest on purpose: when a worker floods, the
/// interesting lines are the ones nearest the flood's end (what it was doing
/// when it went wrong), and the count of what was lost is preserved either way.
struct TraceBuffer {
    buf: VecDeque<Envelope>,
    dropped: u64,
    capacity: usize,
}

impl TraceBuffer {
    fn new(capacity: usize) -> Self {
        TraceBuffer {
            buf: VecDeque::with_capacity(capacity.min(64)),
            dropped: 0,
            capacity,
        }
    }

    fn push(&mut self, env: Envelope) {
        if self.buf.len() >= self.capacity {
            self.buf.pop_front();
            self.dropped += 1;
        }
        self.buf.push_back(env);
    }

    fn len(&self) -> usize {
        self.buf.len()
    }

    /// Everything buffered, preceded by a synthetic `events_dropped` envelope
    /// when anything was dropped since the last drain (the marker comes first
    /// because what it accounts for happened first).
    fn drain(
        &mut self,
        epoch: u32,
        transport: events::Transport,
        now: chrono::DateTime<Utc>,
    ) -> Vec<Envelope> {
        if self.buf.is_empty() && self.dropped == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.buf.len() + 1);
        if self.dropped > 0 {
            out.push(Envelope::new(
                epoch,
                transport,
                EventClass::Lifecycle,
                EventKind::EventsDropped,
                json!({"dropped": self.dropped, "capacity": self.capacity,
                       "reason": "trace backpressure: the oldest buffered trace events were dropped"}),
                now,
            ));
            self.dropped = 0;
        }
        out.extend(self.buf.drain(..));
        out
    }
}

// ---------------------------------------------------------------------------
// free helpers
// ---------------------------------------------------------------------------

/// Sleep until `deadline`, or forever when there is none — the shape a
/// `select!` arm needs so "no deadline" is not a busy loop.
async fn wait_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Move every pending `inbox/` message into `inbox/delivered/` and report the
/// new watermark — the acknowledgement half of [`inbox::deliver`], without the
/// fork the app-server transport does not need.
fn acknowledge_inbox(dir: &Path, snap: &WorkerSnapshot) -> Result<(u64, Vec<u64>), AppError> {
    let pending = inbox::undelivered(dir, snap.last_delivered_seq)
        .map_err(|e| io_err("reading the inbox", e))?;
    if pending.is_empty() {
        return Ok((snap.last_delivered_seq, Vec::new()));
    }
    let delivered = dir.join(inbox::INBOX).join(inbox::DELIVERED);
    crate::home::create_dir_0700(&delivered)
        .map_err(|e| io_err("creating inbox/delivered", e))?;
    let mut watermark = snap.last_delivered_seq;
    let mut seqs = Vec::with_capacity(pending.len());
    for (seq, path, _) in &pending {
        if let Some(name) = path.file_name() {
            std::fs::rename(path, delivered.join(name))
                .map_err(|e| io_err("acknowledging a delivered message", e))?;
        }
        watermark = watermark.max(*seq);
        seqs.push(*seq);
    }
    Ok((watermark, seqs))
}

/// `--schema` as a value, or `None`. An unreadable schema is not fatal here:
/// the §4.3 gate re-reads the file at turn end and turns that into a typed
/// `schema_violation` for the worker, which is where the operator can see it.
fn read_output_schema(path: Option<&Path>) -> Option<Value> {
    let path = path?;
    std::fs::read_to_string(path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
}

/// `turn/started` params are `{threadId, turn:{id,…}}`.
fn turn_id_of(raw: &Value) -> Option<String> {
    raw.get("turn")
        .and_then(|t| t.get("id"))
        .or_else(|| raw.get("turnId"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// `thread/started` params are `{thread:{id,…}}` — NO top-level threadId.
fn session_id_of(raw: &Value) -> Option<String> {
    raw.get("thread")
        .and_then(|t| t.get("id"))
        .or_else(|| raw.get("threadId"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn rollout_path_of(raw: &Value) -> Option<String> {
    raw.get("thread")
        .and_then(|t| t.get("path"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn rpc_err(call: &str, e: RpcError) -> AppError {
    let exit = match e {
        // The transport is gone: that is the broker's problem, and exit 7 is
        // what tells a caller to retry rather than to give up on the worker.
        RpcError::ChildDied | RpcError::Timeout => ExitCode::BrokerUnavailable,
        _ => ExitCode::Internal,
    };
    AppError::new("APP_SERVER", exit, format!("{call}: {e}"))
}

fn io_err(ctx: &str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{self, Lane};
    use crate::events::Transport;
    use crate::fsm::{FailureClass, StateReason};
    use crate::home::A2aHome;
    use crate::inbox::MsgKind;
    use crate::rpc::Client;
    use crate::state::WorkerSnapshot;
    use std::sync::Arc;
    use tokio::task::JoinHandle;

    /// Bounded wait for a converged state. Generous: these tests assert WHICH
    /// state a worker converges on, never how fast it gets there.
    const WAIT: Duration = Duration::from_secs(20);
    const POLL: Duration = Duration::from_millis(20);

    fn fake_bin() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-app-server")
    }

    // -- pure units ---------------------------------------------------------

    #[test]
    fn the_mailbox_is_bounded_at_the_spec_capacity() {
        assert_eq!(MAILBOX_CAPACITY, 1024, "spec §4.5 pins this at 1024");
        let (tx, _rx) = mpsc::channel::<WorkerMsg>(MAILBOX_CAPACITY);
        assert_eq!(tx.max_capacity(), 1024);
    }

    fn trace_env(n: usize) -> Envelope {
        Envelope::new(
            1,
            Transport::AppServer,
            EventClass::Trace,
            EventKind::CommandExec,
            json!({"item":{"type":"commandExecution","command":format!("echo {n}")}}),
            Utc::now(),
        )
    }

    /// AC: trace backpressure is drop-**oldest** + a counter + a synthetic
    /// `events_dropped` envelope. A flood must cost bounded memory and must say
    /// exactly how much observability it cost.
    #[test]
    fn a_trace_flood_drops_the_oldest_and_records_how_many() {
        let mut buf = TraceBuffer::new(TRACE_BUFFER_CAPACITY);
        for n in 0..2_000 {
            buf.push(trace_env(n));
        }
        assert_eq!(buf.len(), TRACE_BUFFER_CAPACITY, "memory stays bounded");
        assert_eq!(buf.dropped, 2_000 - TRACE_BUFFER_CAPACITY as u64);

        let drained = buf.drain(1, Transport::AppServer, Utc::now());
        assert_eq!(drained.len(), TRACE_BUFFER_CAPACITY + 1);
        // The marker accounts for events that happened FIRST, so it comes first.
        assert_eq!(drained[0].kind, EventKind::EventsDropped);
        assert_eq!(
            drained[0].raw["dropped"],
            json!(2_000 - TRACE_BUFFER_CAPACITY as u64)
        );
        // What survives is the TAIL of the flood: what the worker was doing
        // last is what an operator needs.
        assert_eq!(
            drained[1].raw["item"]["command"],
            json!(format!("echo {}", 2_000 - TRACE_BUFFER_CAPACITY))
        );
        assert_eq!(
            drained.last().unwrap().raw["item"]["command"],
            json!("echo 1999")
        );

        // Drained is drained: the counter resets, and an idle buffer flushes
        // nothing at all.
        assert_eq!(buf.dropped, 0);
        assert!(buf.drain(1, Transport::AppServer, Utc::now()).is_empty());

        // Under the cap nothing is lost.
        let mut small = TraceBuffer::new(4);
        for n in 0..4 {
            small.push(trace_env(n));
        }
        let drained = small.drain(1, Transport::AppServer, Utc::now());
        assert_eq!(drained.len(), 4);
        assert!(drained.iter().all(|e| e.kind == EventKind::CommandExec));
    }

    #[test]
    fn notification_payloads_yield_their_routing_ids() {
        assert_eq!(
            turn_id_of(&json!({"threadId":"t1","turn":{"id":"turn_1"}})).as_deref(),
            Some("turn_1")
        );
        assert_eq!(
            turn_id_of(&json!({"threadId":"t1","turnId":"turn_2"})).as_deref(),
            Some("turn_2")
        );
        assert_eq!(turn_id_of(&json!({"threadId":"t1"})), None);
        // `thread/started` has NO top-level threadId — the key is thread.id.
        assert_eq!(
            session_id_of(&json!({"thread":{"id":"th-1","path":"/r.jsonl"}})).as_deref(),
            Some("th-1")
        );
        assert_eq!(
            rollout_path_of(&json!({"thread":{"id":"th-1","path":"/r.jsonl"}})).as_deref(),
            Some("/r.jsonl")
        );
    }

    // -- the live actor -----------------------------------------------------

    struct Harness {
        tmp: tempfile::TempDir,
        dir: PathBuf,
        handle: WorkerHandle,
        tracker: TaskTracker,
        cancel: CancellationToken,
        child: tokio::process::Child,
        child_pid: u32,
        _adapter_tx: watch::Sender<AdapterRef>,
        settle_rx: mpsc::Receiver<Settled>,
        _pump: JoinHandle<()>,
    }

    /// A worker dir seeded exactly as `spawn` leaves one, an actor running over
    /// the scripted fake app-server, and a pump doing what the broker's demux
    /// will do: map each notification and forward it to the mailbox.
    async fn start(
        lane: Lane,
        state: WorkerState,
        scenario: &str,
        extra: &[(&str, &str)],
        tune: impl FnOnce(&mut Config),
    ) -> Harness {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
        let (mut cfg, _, _) = config::load(&home).unwrap();
        tune(&mut cfg);
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&work).unwrap();

        let id = format!("{}-0817t1200-aaaa", lane.as_str());
        let dir = home.worker_dir(&id);
        std::fs::create_dir_all(dir.join("outbox")).unwrap();

        let mut log = EventLog::open(&dir).unwrap();
        let mut snap = fold::append_and_fold(
            &mut log,
            &dir,
            WorkerSnapshot::new_for_test(&id, &work),
            EventKind::Spawned,
            json!({"fsm":"Spawn","id":id,"lane":lane.as_str(),"cwd":work,
                   "transport":"app-server"}),
            Utc::now(),
        )
        .unwrap();
        // A worker that is already `running` was resumed or re-attached: the
        // actor must NOT start a second thread for it.
        if state == WorkerState::Running {
            snap = fold::append_and_fold(
                &mut log,
                &dir,
                snap,
                EventKind::StateChange,
                json!({"fsm":"ProcessStarted","pid":0,"start_witness":"pre-attached"}),
                Utc::now(),
            )
            .unwrap();
            snap = fold::append_and_fold(
                &mut log,
                &dir,
                snap,
                EventKind::SessionIdentified,
                json!({"fsm":"SessionIdentified","session_id":"thread-attached"}),
                Utc::now(),
            )
            .unwrap();
        }
        drop(log);
        assert_eq!(snap.state, state);
        assert_eq!(snap.transport, Transport::AppServer);

        let root = tmp.path().join("fake");
        std::fs::create_dir_all(root.join("state")).unwrap();
        let mut env: Vec<(String, String)> = vec![
            ("FAKE_SCENARIO".into(), scenario.into()),
            (
                "FAKE_STATE".into(),
                root.join("state").to_string_lossy().into_owned(),
            ),
        ];
        env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        let (client, mut incoming, child) =
            Client::spawn(&fake_bin(), &[], &env, &root.join("stderr.log"))
                .await
                .expect("spawn fake-app-server");
        adapter::initialize(&client, Duration::from_secs(10))
            .await
            .expect("handshake");

        let child_pid = child.id().expect("the fake has a pid");
        let (adapter_tx, adapter_rx) = watch::channel(AdapterRef::with_child(
            Arc::new(client),
            child_pid,
            "fake-witness",
        ));
        let (settle_tx, settle_rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        let handle = spawn_worker_actor(
            dir.clone(),
            snap,
            cfg,
            adapter_rx,
            settle_tx,
            cancel.clone(),
            &tracker,
        );
        tracker.close();

        let tx = handle.sender().clone();
        let pump = tokio::spawn(async move {
            while let Some(n) = incoming.notifications.recv().await {
                let Some((class, kind)) =
                    events::map_appserver_notification(&n.method, &n.params)
                else {
                    continue;
                };
                let env = Envelope::new(1, Transport::AppServer, class, kind, n.params, Utc::now());
                if tx.send(WorkerMsg::Envelope(env)).await.is_err() {
                    return;
                }
            }
        });

        Harness {
            tmp,
            dir,
            handle,
            tracker,
            cancel,
            child,
            child_pid,
            _adapter_tx: adapter_tx,
            settle_rx,
            _pump: pump,
        }
    }

    impl Harness {
        /// Converged-state assertion: poll the actor's own snapshot with an
        /// explicit bounded timeout. Never a sleep-and-hope.
        async fn until(
            &self,
            what: &str,
            done: impl Fn(&WorkerSnapshot) -> bool,
        ) -> WorkerSnapshot {
            let deadline = tokio::time::Instant::now() + WAIT;
            loop {
                let snap = self.handle.snapshot().await.expect("the actor is alive");
                if done(&snap) {
                    return snap;
                }
                if tokio::time::Instant::now() > deadline {
                    panic!(
                        "timed out waiting for {what}: state={} reason={:?}",
                        snap.state.as_str(),
                        snap.state_reason
                    );
                }
                tokio::time::sleep(POLL).await;
            }
        }

        fn events(&self) -> Vec<Envelope> {
            store::read_events(&self.dir, 0).unwrap()
        }

        /// Feed one mapped notification, exactly as the broker's demux will.
        async fn notify(&self, kind: EventKind, raw: Value) {
            self.handle
                .send(WorkerMsg::Envelope(Envelope::new(
                    1,
                    Transport::AppServer,
                    EventClass::Lifecycle,
                    kind,
                    raw,
                    Utc::now(),
                )))
                .await
                .expect("the actor is alive");
        }

        /// Wait until `n` turns have started, in the WAL. Ordering barrier: a
        /// synthetic conclusion must not race the fixture's own
        /// `turn/started`, which clears the turn's message buffer.
        async fn turns_started(&self, n: usize) {
            let deadline = tokio::time::Instant::now() + WAIT;
            loop {
                let started = self
                    .events()
                    .iter()
                    .filter(|e| e.kind == EventKind::TurnStarted)
                    .count();
                if started >= n {
                    return;
                }
                assert!(
                    tokio::time::Instant::now() <= deadline,
                    "timed out waiting for {n} turns to start (saw {started})"
                );
                tokio::time::sleep(POLL).await;
            }
        }

        async fn settled(&mut self) -> Settled {
            tokio::time::timeout(WAIT, self.settle_rx.recv())
                .await
                .expect("a settle was announced")
                .expect("the settle channel is open")
        }

        async fn stop(mut self) {
            self.handle.shutdown().await;
            self.cancel.cancel();
            let _ = tokio::time::timeout(WAIT, self.tracker.wait()).await;
            let _ = self.child.kill().await;
            drop(self.tmp);
        }
    }

    /// No `fold_rejections` at all: every lifecycle row the actor writes is a
    /// legal transition from the state it wrote it in. This is the pin that
    /// catches a mis-gated `SessionIdentified` or a `TurnEnded` committed from
    /// the wrong state.
    fn assert_no_rejections(snap: &WorkerSnapshot) {
        assert!(
            snap.extra.get("fold_rejections").is_none(),
            "fold_rejections must stay empty: {:?}",
            snap.extra.get("fold_rejections")
        );
    }

    /// The whole happy path: thread start → first turn → result → `done`, with
    /// the event-order pin and the settle announcement.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_happy_path_runs_a_worker_to_done() {
        let mut h = start(
            Lane::General,
            WorkerState::Spawned,
            "happy",
            &[("FAKE_MESSAGE", "THE ANSWER")],
            |_| {},
        )
        .await;
        assert_eq!(h.handle.sender().max_capacity(), MAILBOX_CAPACITY);

        // The thread is opened by the actor itself, so the worker is `running`
        // before anything is delivered to it.
        let snap = h.until("the thread to start", |s| s.state == WorkerState::Running).await;
        assert!(snap.session_id.is_some(), "the thread id is recorded");
        assert_eq!(snap.pid, Some(h.child_pid), "the SHARED child's pid");

        assert_eq!(
            h.handle.deliver("do the thing").await.unwrap(),
            SendPath::Started,
            "no turn was open, so the first delivery starts one"
        );
        h.handle
            .send(WorkerMsg::ServerRequestLogged {
                method: "execCommandApproval".into(),
                disposition: "denied".into(),
            })
            .await
            .unwrap();

        let snap = h.until("the turn to conclude", |s| s.state == WorkerState::Done).await;
        assert_no_rejections(&snap);
        assert_eq!(
            std::fs::read_to_string(h.dir.join("result.md")).unwrap(),
            "THE ANSWER",
            "the turn's final agent message IS the result on this transport"
        );
        assert_eq!(h.settled().await, Settled { id: snap.id.clone(), state: WorkerState::Done });

        // ── the event-order pin ──────────────────────────────────────────
        let events = h.events();
        assert_eq!(events[0].kind, EventKind::Spawned, "genesis");
        assert_eq!(
            events[1].raw["fsm"],
            json!("ProcessStarted"),
            "ProcessStarted is durable BEFORE any notification is forwarded"
        );
        assert_eq!(events[1].raw["pid"], json!(h.child_pid));
        assert_eq!(events[1].raw["start_witness"], json!("fake-witness"));
        let first_notification = events
            .iter()
            .position(|e| e.raw.get("threadId").is_some() || e.raw.get("thread").is_some())
            .expect("the fake emitted notifications");
        assert!(
            first_notification > 1,
            "a notification was folded before ProcessStarted"
        );
        // Every worker file the actor wrote is app-server transport.
        assert!(events[1..].iter().all(|e| e.transport == Transport::AppServer));
        // The server request is recorded by method+disposition and nothing else.
        let sr = events
            .iter()
            .find(|e| e.kind == EventKind::ServerRequest)
            .expect("the server request was logged");
        assert_eq!(sr.raw["method"], json!("execCommandApproval"));
        assert_eq!(sr.raw["disposition"], json!("denied"));
        assert_eq!(sr.class, EventClass::Lifecycle);
        // The turn concluded through the shared code: TurnEnded is the last
        // lifecycle row and it says the turn produced a result.
        let ended = events
            .iter()
            .rev()
            .find(|e| e.raw.get("fsm") == Some(&json!("TurnEnded")))
            .expect("TurnEnded was committed");
        assert_eq!(ended.raw["has_result"], json!(true));
        assert_eq!(ended.raw["has_question"], json!(false));

        h.stop().await;
    }

    /// The parity AC on the app-server side: a question parks the worker, the
    /// ANSWER moves the delivery watermark past it, and the worker runs on to
    /// `done` instead of re-parking on a question it has already answered.
    ///
    /// The `hang-turn` scenario is what makes this deterministic: the fake
    /// opens a turn and holds it, so every turn *conclusion* below is driven by
    /// this test (exactly as the broker's demux drives it) rather than racing
    /// the fixture's own notifications.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_answered_question_lets_the_next_turn_reach_done() {
        let mut h = start(
            Lane::General,
            WorkerState::Running,
            "hang-turn",
            &[],
            |_| {},
        )
        .await;

        // ── turn 1: a question, no result → parked ───────────────────────
        h.notify(
            EventKind::TurnStarted,
            json!({"threadId":"thread-attached","turn":{"id":"turn_1","status":"inProgress"}}),
        )
        .await;
        std::fs::write(
            h.dir.join("outbox/1.question.json"),
            br#"{"seq":1,"question":"A or B?","blocking":true}"#,
        )
        .unwrap();
        h.notify(
            EventKind::TurnCompleted,
            json!({"threadId":"thread-attached","turn":{"id":"turn_1","status":"completed"}}),
        )
        .await;

        let snap = h
            .until("the worker to park on its question", |s| {
                s.state == WorkerState::AwaitingInput
            })
            .await;
        assert_eq!(snap.state_reason, Some(StateReason::Blocked));
        assert_eq!(snap.last_delivered_seq, 0);

        // ── the answer ───────────────────────────────────────────────────
        let seq = crate::inbox::queue_message(&h.dir, "orchestrator", MsgKind::Answer, "B").unwrap();
        assert_eq!(seq, 1, "the answer's seq matches the question it answers");
        assert_eq!(
            h.handle.deliver("B").await.unwrap(),
            SendPath::Started,
            "the previous turn had ended, so there was nothing to steer"
        );
        let snap = h.until("the answer to move the watermark", |s| {
            s.last_delivered_seq == 1
        })
        .await;
        assert_eq!(snap.state, WorkerState::Running);
        // The question file is STILL on disk: it is the WATERMARK, not a
        // deletion, that marks it answered.
        assert!(h.dir.join("outbox/1.question.json").exists());
        assert!(reconcile::has_unanswered_question(&h.dir, 0));
        assert!(!reconcile::has_unanswered_question(&h.dir, 1));

        // ── turn 2 ends with nothing to show ─────────────────────────────
        //
        // THE PIN: the same question is still on disk, so a conclusion that
        // dropped the watermark argument would park this worker `blocked`
        // again — on a question it answered a turn ago.
        h.turns_started(2).await; // the fake's own `turn/started`, first
        h.notify(
            EventKind::TurnCompleted,
            json!({"threadId":"thread-attached","turn":{"id":"turn_2","status":"completed"}}),
        )
        .await;
        let snap = h
            .until("turn 2 to conclude", |s| s.state == WorkerState::AwaitingInput)
            .await;
        assert_eq!(
            snap.state_reason,
            Some(StateReason::TurnEndedWithoutResult),
            "an ANSWERED question must not re-park the worker as `blocked`"
        );

        // ── turn 3 answers for real → done ───────────────────────────────
        h.handle.deliver("finish it").await.unwrap();
        h.turns_started(3).await;
        h.notify(
            EventKind::AgentMessage,
            json!({"threadId":"thread-attached","turnId":"turn_3",
                   "item":{"type":"agentMessage","id":"msg_1","text":"FINAL",
                           "phase":"final_answer"}}),
        )
        .await;
        h.notify(
            EventKind::TurnCompleted,
            json!({"threadId":"thread-attached","turn":{"id":"turn_3","status":"completed"}}),
        )
        .await;

        let snap = h.until("the answered worker to finish", |s| s.state == WorkerState::Done).await;
        assert_no_rejections(&snap);
        assert_eq!(
            std::fs::read_to_string(h.dir.join("result.md")).unwrap(),
            "FINAL",
            "synthesized from the turn's final agent message, before the gate"
        );
        assert_eq!(h.settled().await.state, WorkerState::Done);
        h.stop().await;
    }

    /// `Kill` on this transport is `turn/interrupt` + `Cancelled`. It must
    /// NEVER signal the child — that child is every other worker's transport.
    #[tokio::test(flavor = "multi_thread")]
    async fn kill_interrupts_the_turn_and_never_signals_the_shared_child() {
        let mut h = start(Lane::General, WorkerState::Spawned, "hang-turn", &[], |_| {}).await;
        h.until("the thread to start", |s| s.state == WorkerState::Running).await;
        h.handle.deliver("work forever").await.unwrap();

        h.handle.kill().await.unwrap();
        let snap = h.until("the kill to land", |s| s.state == WorkerState::Cancelled).await;
        assert_no_rejections(&snap);
        assert_eq!(
            snap.failure_class, None,
            "a human stop is `cancelled`, never `failed`"
        );
        assert_eq!(h.settled().await.state, WorkerState::Cancelled);

        // The shared app-server child is untouched: still running, still
        // answering. (The FSM's KillProcess action is exec's; the actor
        // performs no actions at all.)
        assert!(
            h.child.try_wait().unwrap().is_none(),
            "the shared app-server child was signalled"
        );
        h.stop().await;
    }

    /// Interrupting a turn that is already over is a no-op, not an error.
    #[tokio::test(flavor = "multi_thread")]
    async fn interrupting_a_finished_turn_is_a_noop() {
        let h = start(Lane::General, WorkerState::Spawned, "happy", &[], |_| {}).await;
        h.until("the thread to start", |s| s.state == WorkerState::Running).await;
        assert_eq!(
            h.handle.interrupt().await.unwrap(),
            InterruptOutcome::NoopTurnAlreadyOver,
            "no turn was ever open"
        );

        h.handle.deliver("do the thing").await.unwrap();
        let snap = h.until("the turn to conclude", |s| s.state == WorkerState::Done).await;
        assert_eq!(
            h.handle.interrupt().await.unwrap(),
            InterruptOutcome::NoopTurnAlreadyOver,
            "the turn is over: interrupting it changes nothing"
        );
        let after = h.handle.snapshot().await.unwrap();
        assert_eq!(after.state, WorkerState::Done);
        assert_eq!(after.last_applied_seq, snap.last_applied_seq, "no WAL row");
        h.stop().await;
    }

    /// AC: broker-side deadlines are armed from lane config and fired as
    /// `Timeout{kind}` through the WAL — `blocked_timeout` → `escalated` is
    /// reachable live, which it never was in Phase 1 (nothing was watching).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_parked_worker_escalates_when_its_blocked_deadline_expires() {
        // The `review` lane is `outbox-only`, so the thread is rooted on the
        // WORKER dir — which is where the fake writes its question.
        let mut h = start(
            Lane::Review,
            WorkerState::Spawned,
            "blocked",
            // A blank final message is not a result: the worker parks on its
            // question instead of finishing.
            &[("FAKE_MESSAGE", "")],
            |cfg| {
                let lane = cfg.lanes.get_mut(&Lane::Review).unwrap();
                lane.blocked_timeout_secs = 1;
            },
        )
        .await;
        h.until("the thread to start", |s| s.state == WorkerState::Running).await;
        h.handle.deliver("ask me something").await.unwrap();

        let parked = h
            .until("the worker to park", |s| s.state == WorkerState::AwaitingInput)
            .await;
        assert_eq!(parked.state_reason, Some(StateReason::Blocked));
        let deadline = parked.deadline_at.expect("the blocked deadline is armed");
        assert!(
            deadline - parked.entered_state_at <= chrono::Duration::seconds(1),
            "the deadline came from the lane's blocked_timeout_secs"
        );

        let escalated = h
            .until("the blocked deadline to fire", |s| s.state == WorkerState::Escalated)
            .await;
        assert_eq!(escalated.state_reason, Some(StateReason::BlockedTimeout));
        assert_eq!(escalated.failure_class, None);
        assert_no_rejections(&escalated);
        assert!(
            escalated.deadline_at.is_none(),
            "escalated carries no deadline"
        );
        // Escalated frees the slot, so it settles just like a terminal state.
        assert_eq!(h.settled().await.state, WorkerState::Escalated);

        let timeout = h
            .events()
            .into_iter()
            .rev()
            .find(|e| e.kind == EventKind::Timeout)
            .expect("the timeout went through the WAL");
        assert_eq!(timeout.raw["fsm"], json!("Timeout"));
        assert_eq!(timeout.raw["kind"], json!("blocked"));
        h.stop().await;
    }

    /// A thread that never opens is `failed{never_started}` — the same answer
    /// `spawn` gives when an exec fork fails.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_thread_that_never_starts_fails_never_started() {
        let mut h = start(Lane::General, WorkerState::Spawned, "always-crash", &[], |_| {}).await;
        let snap = h.until("the failure to land", |s| s.state == WorkerState::Failed).await;
        assert_eq!(snap.failure_class, Some(FailureClass::NeverStarted));
        assert_no_rejections(&snap);
        assert_eq!(h.settled().await.state, WorkerState::Failed);
        h.stop().await;
    }
}
