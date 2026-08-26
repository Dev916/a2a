//! Pure worker state machine (spec §4.6).
//!
//! Zero IO, zero clock: the caller folds WAL events through this module and
//! performs the returned actions. Time enters only as an event
//! ([`FsmEvent::Timeout`]) so the machine stays deterministic and testable.
//!
//! `docs/machines/worker.md` mirrors this table 1:1 and is a merge gate: any
//! change here changes that file and `tests/fsm_table.rs`.
//!
//! **Totality is enforced by source scan.** The transition match enumerates
//! every `(state, event)` pair explicitly — no catch-all arm — so adding a
//! variant to [`WorkerState`] or [`FsmEvent`] breaks the build until every new
//! pair is decided, rather than silently falling into a default.

use serde::{Deserialize, Serialize};

/// The worker lifecycle states. Open enum: a `state.json` written by a newer
/// `a2a` deserializes to [`WorkerState::Unknown`] instead of failing.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerState {
    Spawned,
    Queued,
    Running,
    AwaitingInput,
    Escalated,
    Done,
    Failed,
    Cancelled,
    Orphaned,
    #[serde(other)]
    Unknown,
}

impl WorkerState {
    /// Terminal states are absorbing: only [`FsmEvent::Resume`] leaves them.
    /// `orphaned` and `escalated` are deliberately NOT terminal — both are
    /// resumable and gc treats them differently.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            WorkerState::Done | WorkerState::Failed | WorkerState::Cancelled
        )
    }

    /// Wire/CLI spelling (kebab-case), matching the serde representation.
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerState::Spawned => "spawned",
            WorkerState::Queued => "queued",
            WorkerState::Running => "running",
            WorkerState::AwaitingInput => "awaiting-input",
            WorkerState::Escalated => "escalated",
            WorkerState::Done => "done",
            WorkerState::Failed => "failed",
            WorkerState::Cancelled => "cancelled",
            WorkerState::Orphaned => "orphaned",
            WorkerState::Unknown => "unknown",
        }
    }
}

/// Why a worker is in its current (non-failure) state.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum StateReason {
    Blocked,
    Interrupted,
    TurnEndedWithoutResult,
    BlockedTimeout,
    #[serde(other)]
    Unknown,
}

/// Why a worker failed. Drives retry policy — note `kill` never produces one
/// of these (a human stop must never be auto-retried).
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    RateLimit,
    AuthExpired,
    Network,
    ModelError,
    Timeout,
    SandboxDenied,
    SchemaViolation,
    NoResult,
    BudgetExhausted,
    NeverStarted,
    InternalPanic,
    SessionMissing,
    #[serde(other)]
    Unknown,
}

/// Everything that can move a worker. Timeouts are events with persisted
/// absolute deadlines (spec §4.6) — the machine never reads a clock.
#[derive(Clone, Debug)]
pub enum FsmEvent {
    /// Genesis ONLY — the fold applies it to create a worker (Task 9);
    /// `step` rejects it from every state.
    Spawn {
        id: String,
    },
    /// queued → spawned (Phase 2 admission; Phase 1 never enters queued).
    Admit,
    ProcessStarted {
        pid: u32,
        start_witness: String,
    },
    /// running → running (records the codex session/rollout ids).
    SessionIdentified {
        session_id: String,
        rollout_path: Option<String>,
    },
    TurnEnded {
        has_result: bool,
        has_question: bool,
    },
    ResultInvalid {
        detail: String,
    },
    ProcessExited {
        code: Option<i32>,
    },
    /// Emitted by reconcile: a resumed process died before its first turn.
    SessionMissing,
    Timeout {
        kind: TimeoutKind,
    },
    Interrupt,
    Kill,
    OrphanDetected,
    /// Non-running (including terminal) → running, epoch+1.
    Resume {
        answer_queued: bool,
    },
    Send,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum TimeoutKind {
    Start,
    Task,
    Blocked,
}

/// A typed rejection, surfaced verbatim by the CLI (spec §4.6) — every reason
/// names the command that WOULD work.
#[derive(Debug)]
pub struct Rejected {
    pub from: WorkerState,
    pub event: &'static str,
    pub reason: String,
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot apply `{}` to a worker in state `{}`: {}",
            self.event,
            self.from.as_str(),
            self.reason
        )
    }
}

impl std::error::Error for Rejected {}

/// Side effects the caller must perform AFTER the transition is durably
/// appended to the WAL (spec §4.6: append+fsync → side effect → snapshot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    DeliverInbox,
    KillProcess,
    RecordEscalation,
}

/// The result of an accepted event.
#[derive(Debug, Clone)]
pub struct Transition {
    pub state: WorkerState,
    pub reason: Option<StateReason>,
    pub failure: Option<FailureClass>,
    pub epoch_bump: bool,
    pub actions: Vec<Action>,
}

impl Transition {
    fn to(state: WorkerState) -> Self {
        Transition {
            state,
            reason: None,
            failure: None,
            epoch_bump: false,
            actions: Vec::new(),
        }
    }

    fn failed(class: FailureClass) -> Self {
        Transition {
            state: WorkerState::Failed,
            reason: None,
            failure: Some(class),
            epoch_bump: false,
            actions: Vec::new(),
        }
    }

    /// `resume` is the only door out of a terminal state; it fences the old
    /// run by bumping the epoch.
    fn resumed() -> Self {
        Transition {
            state: WorkerState::Running,
            reason: None,
            failure: None,
            epoch_bump: true,
            actions: Vec::new(),
        }
    }

    fn because(mut self, reason: Option<StateReason>) -> Self {
        self.reason = reason;
        self
    }

    fn act(mut self, action: Action) -> Self {
        self.actions.push(action);
        self
    }
}

/// Stable wire name of an event, used in rejections and the events log.
pub fn event_name(ev: &FsmEvent) -> &'static str {
    match ev {
        FsmEvent::Spawn { .. } => "spawn",
        FsmEvent::Admit => "admit",
        FsmEvent::ProcessStarted { .. } => "process_started",
        FsmEvent::SessionIdentified { .. } => "session_identified",
        FsmEvent::TurnEnded { .. } => "turn_ended",
        FsmEvent::ResultInvalid { .. } => "result_invalid",
        FsmEvent::ProcessExited { .. } => "process_exited",
        FsmEvent::SessionMissing => "session_missing",
        FsmEvent::Timeout { .. } => "timeout",
        FsmEvent::Interrupt => "interrupt",
        FsmEvent::Kill => "kill",
        FsmEvent::OrphanDetected => "orphan_detected",
        FsmEvent::Resume { .. } => "resume",
        FsmEvent::Send => "send",
    }
}

fn reject(from: WorkerState, ev: &FsmEvent, reason: impl Into<String>) -> Rejected {
    Rejected {
        from,
        event: event_name(ev),
        reason: reason.into(),
    }
}

/// `spawn` is the fold's genesis event and is legal from no state at all.
const GENESIS_ONLY: &str =
    "genesis only — `spawn` creates a worker; it is never applied to an existing one";

fn terminal_reason(state: WorkerState) -> String {
    format!(
        "worker is {} — use `a2a resume` to continue it",
        state.as_str()
    )
}

/// Apply one event. Pure and total: every `(state, event)` pair is either an
/// accepted [`Transition`] or an actionable [`Rejected`].
///
/// The rows below are mirrored 1:1 in `docs/machines/worker.md`.
pub fn step(
    current: WorkerState,
    reason: Option<StateReason>,
    ev: &FsmEvent,
) -> Result<Transition, Rejected> {
    match current {
        // ---- queued: Phase 2 admission control; Phase 1 never enters it ----
        WorkerState::Queued => match ev {
            FsmEvent::Admit => Ok(Transition::to(WorkerState::Spawned)),
            FsmEvent::Kill => Ok(Transition::to(WorkerState::Cancelled)),
            FsmEvent::Spawn { .. } => Err(reject(current, ev, GENESIS_ONLY)),
            FsmEvent::ProcessStarted { .. }
            | FsmEvent::SessionIdentified { .. }
            | FsmEvent::TurnEnded { .. }
            | FsmEvent::ResultInvalid { .. }
            | FsmEvent::ProcessExited { .. }
            | FsmEvent::SessionMissing
            | FsmEvent::Timeout { .. }
            | FsmEvent::Interrupt
            | FsmEvent::OrphanDetected
            | FsmEvent::Resume { .. }
            | FsmEvent::Send => Err(reject(
                current,
                ev,
                "worker is queued and has no process yet — only admission or `a2a kill` apply",
            )),
        },

        // ---- spawned: process not up yet; guarded by the start deadline ----
        WorkerState::Spawned => match ev {
            FsmEvent::ProcessStarted { .. } => Ok(Transition::to(WorkerState::Running)),
            // A worker whose process never came up must be distinguishable
            // from one that ran and died (spec §4.6): both are never_started.
            FsmEvent::ProcessExited { .. } => Ok(Transition::failed(FailureClass::NeverStarted)),
            FsmEvent::Timeout { kind } => match kind {
                TimeoutKind::Start => Ok(Transition::failed(FailureClass::NeverStarted)),
                TimeoutKind::Task | TimeoutKind::Blocked => Err(reject(
                    current,
                    ev,
                    "only the start deadline is armed before the process comes up",
                )),
            },
            FsmEvent::Kill => Ok(Transition::to(WorkerState::Cancelled)),
            FsmEvent::Spawn { .. } => Err(reject(current, ev, GENESIS_ONLY)),
            FsmEvent::Admit
            | FsmEvent::SessionIdentified { .. }
            | FsmEvent::TurnEnded { .. }
            | FsmEvent::ResultInvalid { .. }
            | FsmEvent::SessionMissing
            | FsmEvent::Interrupt
            | FsmEvent::OrphanDetected
            | FsmEvent::Resume { .. }
            | FsmEvent::Send => Err(reject(
                current,
                ev,
                "worker has not started its process yet — wait for it to run, or `a2a kill` it",
            )),
        },

        // ---- running: the only state with a live turn ----
        WorkerState::Running => match ev {
            // Self-loops preserve the current reason (idempotent bookkeeping).
            FsmEvent::SessionIdentified { .. } => {
                Ok(Transition::to(WorkerState::Running).because(reason))
            }
            // The message only queues; delivery happens at the next turn
            // boundary (spec §4.3) — hence no action here.
            FsmEvent::Send => Ok(Transition::to(WorkerState::Running).because(reason)),
            FsmEvent::TurnEnded {
                has_result,
                has_question,
            } => {
                if *has_result {
                    Ok(Transition::to(WorkerState::Done))
                } else if *has_question {
                    Ok(Transition::to(WorkerState::AwaitingInput)
                        .because(Some(StateReason::Blocked)))
                } else {
                    Ok(Transition::to(WorkerState::AwaitingInput)
                        .because(Some(StateReason::TurnEndedWithoutResult)))
                }
            }
            FsmEvent::ResultInvalid { .. } => {
                Ok(Transition::failed(FailureClass::SchemaViolation))
            }
            FsmEvent::ProcessExited { .. } => Ok(Transition::failed(FailureClass::NoResult)),
            FsmEvent::SessionMissing => Ok(Transition::failed(FailureClass::SessionMissing)),
            FsmEvent::Timeout { kind } => match kind {
                TimeoutKind::Task => {
                    Ok(Transition::failed(FailureClass::Timeout).act(Action::KillProcess))
                }
                TimeoutKind::Start => Err(reject(
                    current,
                    ev,
                    "the start deadline is disarmed once the process is running",
                )),
                TimeoutKind::Blocked => Err(reject(
                    current,
                    ev,
                    "the blocked deadline is armed only in awaiting-input",
                )),
            },
            FsmEvent::Interrupt => {
                Ok(Transition::to(WorkerState::AwaitingInput)
                    .because(Some(StateReason::Interrupted)))
            }
            // A human stop is `cancelled`, never `failed` — retry policy must
            // never auto-retry it (spec §4.6).
            FsmEvent::Kill => {
                Ok(Transition::to(WorkerState::Cancelled).act(Action::KillProcess))
            }
            FsmEvent::OrphanDetected => Ok(Transition::to(WorkerState::Orphaned)),
            FsmEvent::Spawn { .. } => Err(reject(current, ev, GENESIS_ONLY)),
            FsmEvent::Admit => Err(reject(
                current,
                ev,
                "worker was admitted already — it is running",
            )),
            FsmEvent::ProcessStarted { .. } => Err(reject(
                current,
                ev,
                "worker already has a live process for this epoch",
            )),
            FsmEvent::Resume { .. } => Err(reject(
                current,
                ev,
                "worker is already running — nothing to resume",
            )),
        },

        // ---- awaiting-input: turn boundary; the human owns the next move ----
        WorkerState::AwaitingInput => match ev {
            FsmEvent::Send => {
                Ok(Transition::to(WorkerState::Running).act(Action::DeliverInbox))
            }
            FsmEvent::Timeout { kind } => match kind {
                TimeoutKind::Blocked => Ok(Transition::to(WorkerState::Escalated)
                    .because(Some(StateReason::BlockedTimeout))
                    .act(Action::RecordEscalation)),
                TimeoutKind::Start | TimeoutKind::Task => Err(reject(
                    current,
                    ev,
                    "only the blocked deadline is armed while awaiting input",
                )),
            },
            FsmEvent::Kill => Ok(Transition::to(WorkerState::Cancelled)),
            FsmEvent::OrphanDetected => Ok(Transition::to(WorkerState::Orphaned)),
            FsmEvent::Resume { .. } => Ok(Transition::resumed()),
            FsmEvent::Spawn { .. } => Err(reject(current, ev, GENESIS_ONLY)),
            FsmEvent::Admit
            | FsmEvent::ProcessStarted { .. }
            | FsmEvent::SessionIdentified { .. }
            | FsmEvent::TurnEnded { .. }
            | FsmEvent::ResultInvalid { .. }
            | FsmEvent::ProcessExited { .. }
            | FsmEvent::SessionMissing
            | FsmEvent::Interrupt => Err(reject(
                current,
                ev,
                "worker is waiting for an answer — use `a2a send`, or `a2a resume`/`a2a kill`",
            )),
        },

        // ---- escalated: blocked past its deadline; slot freed, resumable ----
        WorkerState::Escalated => match ev {
            FsmEvent::Send => {
                Ok(Transition::to(WorkerState::Running).act(Action::DeliverInbox))
            }
            FsmEvent::Kill => Ok(Transition::to(WorkerState::Cancelled)),
            FsmEvent::Resume { .. } => Ok(Transition::resumed()),
            FsmEvent::OrphanDetected => Ok(Transition::to(WorkerState::Orphaned)),
            FsmEvent::Spawn { .. } => Err(reject(current, ev, GENESIS_ONLY)),
            FsmEvent::Admit
            | FsmEvent::ProcessStarted { .. }
            | FsmEvent::SessionIdentified { .. }
            | FsmEvent::TurnEnded { .. }
            | FsmEvent::ResultInvalid { .. }
            | FsmEvent::ProcessExited { .. }
            | FsmEvent::SessionMissing
            | FsmEvent::Timeout { .. }
            | FsmEvent::Interrupt => Err(reject(
                current,
                ev,
                "worker escalated to a human — answer with `a2a send`, or `a2a resume`/`a2a kill`",
            )),
        },

        // ---- orphaned: non-terminal, gc-exempt, resumable ----
        WorkerState::Orphaned => match ev {
            FsmEvent::Resume { .. } => Ok(Transition::resumed()),
            FsmEvent::Kill => Ok(Transition::to(WorkerState::Cancelled)),
            // Idempotent: re-detection preserves the recorded reason.
            FsmEvent::OrphanDetected => {
                Ok(Transition::to(WorkerState::Orphaned).because(reason))
            }
            FsmEvent::Spawn { .. } => Err(reject(current, ev, GENESIS_ONLY)),
            FsmEvent::Admit
            | FsmEvent::ProcessStarted { .. }
            | FsmEvent::SessionIdentified { .. }
            | FsmEvent::TurnEnded { .. }
            | FsmEvent::ResultInvalid { .. }
            | FsmEvent::ProcessExited { .. }
            | FsmEvent::SessionMissing
            | FsmEvent::Timeout { .. }
            | FsmEvent::Interrupt
            | FsmEvent::Send => Err(reject(
                current,
                ev,
                "worker is orphaned (its process is gone) — use `a2a resume` or `a2a kill`",
            )),
        },

        // ---- terminal: absorbing except for resume ----
        WorkerState::Done | WorkerState::Failed | WorkerState::Cancelled => match ev {
            FsmEvent::Resume { .. } => Ok(Transition::resumed()),
            FsmEvent::Spawn { .. } => Err(reject(current, ev, GENESIS_ONLY)),
            FsmEvent::Admit
            | FsmEvent::ProcessStarted { .. }
            | FsmEvent::SessionIdentified { .. }
            | FsmEvent::TurnEnded { .. }
            | FsmEvent::ResultInvalid { .. }
            | FsmEvent::ProcessExited { .. }
            | FsmEvent::SessionMissing
            | FsmEvent::Timeout { .. }
            | FsmEvent::Interrupt
            | FsmEvent::Kill
            | FsmEvent::OrphanDetected
            | FsmEvent::Send => Err(reject(current, ev, terminal_reason(current))),
        },

        // ---- unknown: state.json written by a newer a2a ----
        WorkerState::Unknown => match ev {
            FsmEvent::Spawn { .. }
            | FsmEvent::Admit
            | FsmEvent::ProcessStarted { .. }
            | FsmEvent::SessionIdentified { .. }
            | FsmEvent::TurnEnded { .. }
            | FsmEvent::ResultInvalid { .. }
            | FsmEvent::ProcessExited { .. }
            | FsmEvent::SessionMissing
            | FsmEvent::Timeout { .. }
            | FsmEvent::Interrupt
            | FsmEvent::Kill
            | FsmEvent::OrphanDetected
            | FsmEvent::Resume { .. }
            | FsmEvent::Send => Err(reject(
                current,
                ev,
                "unrecognized worker state — this worker was written by a newer a2a; upgrade the CLI",
            )),
        },
    }
}
