//! The app-server adapter: typed lifecycle calls, the `send` decision table,
//! the handle a `WorkerActor` holds, and the sync handshake probe (spec §4.3).
//!
//! [`crate::rpc`] owns the pipe; this module owns the *conversation*. Everything
//! here is a thin, typed wrapper over one JSON-RPC call, plus three pieces of
//! policy that are easy to get wrong and expensive to get wrong silently:
//!
//! * **Where a thread is rooted.** An `outbox-only` lane is started on its
//!   **worker dir**, not on the repo — the same relocation Phase 1's exec
//!   transport does by moving the workspace to `scratch/` ([`crate::exec`]).
//!   Root confinement then rides the *per-turn* [`SandboxPolicy`], because
//!   `thread/start` has no `writableRoots` at all.
//! * **What is sticky and what is not.** `sandboxPolicy` and `outputSchema` are
//!   per-turn: they are re-supplied on **every** `turn/start`, including the
//!   first one after a `thread/resume` (which cannot carry them). Forgetting
//!   that is not a loud failure — it is a worker quietly running with the wrong
//!   writable roots.
//! * **The steer/start race.** `turn/steer` carries `expectedTurnId`; between
//!   reading the last turn id and sending the steer, the turn can complete. That
//!   TOCTOU is *expected*, so a precondition failure is transparently retried as
//!   `turn/start` ([`is_steer_precondition_failure`] — one predicate, which is
//!   why Task 18 had exactly one place to correct once the real trigger was
//!   measured: the live server says `no active turn to steer`, under the same
//!   error code it uses for `thread not found`). Every other remote error
//!   surfaces unchanged: retrying an unknown failure as a fresh turn would
//!   double-drive the worker.
//!
//! [`handshake_probe`] is the one sanctioned exception to "the CLI is
//! synchronous": `doctor` and transport selection need a real handshake before
//! any broker exists, so it builds a private `current_thread` runtime, bounds
//! the whole exchange, and kills the child on the way out.

use crate::config::{LaneConfig, SandboxMode};
use crate::protocol::{
    ApprovalPolicy, ClientInfo, InitializeCapabilities, InitializeParams, InitializeResponse,
    SandboxModeParam, SandboxPolicy, ThreadResumeParams, ThreadStartParams, ThreadStartResponse,
    TurnInterruptParams, TurnStartParams, TurnStartResponse, TurnStatus, TurnSteerParams,
    UserInput, SUPPRESSED_NOTIFICATION_METHODS,
};
use crate::rpc::{Client, RpcError};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// The fake-fidelity helper, shared with the integration tests rather than
/// duplicated: the params this module builds are validated against the very
/// same `schema/v2-snapshot/` files the fake's transcripts are held to.
/// (Compiled once per including module — `responder.rs` does the same; the
/// duplication is deliberate and cheaper than exporting a test-only module.)
#[cfg(test)]
#[allow(clippy::duplicate_mod)]
#[path = "../tests/common/fidelity.rs"]
mod fidelity;

/// `clientInfo.name`. The version is always `CARGO_PKG_VERSION` — the handshake
/// response carries no protocol version, so this is the only version fact on the
/// wire and it must identify the real binary.
pub const CLIENT_NAME: &str = "a2a";

/// The argv that turns the codex binary into an app-server child.
pub const APP_SERVER_ARGS: [&str; 1] = ["app-server"];

/// The voice channel. Not part of the token stream (that is
/// [`SUPPRESSED_NOTIFICATION_METHODS`]) and not high-volume by itself — `a2a`
/// simply never opens a realtime session, so every one of these is noise. Pinned
/// against `schema/v2-snapshot/server_notification_methods.txt` by a test, so a
/// new realtime method upstream fails the suite instead of leaking through.
pub const SUPPRESSED_REALTIME_METHODS: [&str; 11] = [
    "thread/realtime/closed",
    "thread/realtime/error",
    "thread/realtime/item/completed",
    "thread/realtime/item/started",
    "thread/realtime/item/transcript/delta",
    "thread/realtime/itemAdded",
    "thread/realtime/outputAudio/delta",
    "thread/realtime/sdp",
    "thread/realtime/started",
    "thread/realtime/transcript/delta",
    "thread/realtime/transcript/done",
];

/// The JSON-RPC error code the live server returns for a lost steer race —
/// **and for everything else it dislikes about a request.** Observed against
/// codex-cli 0.140.0 (Task 18, `tests/e2e_appserver.rs`):
///
/// ```text
/// turn/steer, turn already over       -32600  no active turn to steer
/// turn/steer, a different turn open   -32600  expected active turn id `A` but found `B`
/// turn/steer, unknown threadId        -32600  thread not found: <uuid>
/// turn/steer, missing field           -32600  Invalid request: missing field `expectedTurnId`
/// turn/start, unknown threadId        -32600  thread not found: <uuid>
/// ```
///
/// So the code is **necessary but never sufficient**: it is the server's
/// catch-all "invalid request", and routing a failed steer on it alone would
/// retry a `thread not found` as a fresh `turn/start` — double-driving a worker
/// on the strength of a typo. The MESSAGE is the discriminator, which is why
/// [`is_steer_precondition_failure`] matches
/// [`STEER_PRECONDITION_MARKERS`] and this constant exists to document the
/// observation (and to keep `tests/fixtures/fake-app-server` speaking the real
/// vocabulary).
pub const STEER_PRECONDITION_CODE: i64 = -32600;

/// The only two messages that route a failed `turn/steer` into a retry, both
/// transcribed verbatim from codex-cli 0.140.0 and matched case-insensitively
/// as substrings (the mismatch message embeds two turn ids).
///
/// Deliberately NOT widened to `expectedturnid`: `Invalid request: missing
/// field \`expectedTurnId\`` contains that substring and is an *a2a* bug, which
/// a silent retry would hide. Adding a marker here is a decision to treat a new
/// server message as the turn-boundary race; nothing else belongs.
pub const STEER_PRECONDITION_MARKERS: [&str; 2] = [
    // The turn ended between reading its id and sending the steer — the TOCTOU
    // this whole path exists for.
    "no active turn",
    // A *different* turn is open: "expected active turn id `A` but found `B`".
    "expected active turn id",
];

// ---------------------------------------------------------------------------
// initialize
// ---------------------------------------------------------------------------

/// What the handshake tells `a2a` that it did not already know. There is no
/// protocol version in the response (hence `schema/v2-snapshot/` and its
/// canary); `codex_home` is worth recording because it is where the rollout
/// files the exec fallback resumes from actually live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitInfo {
    pub user_agent: String,
    pub codex_home: PathBuf,
}

/// The exact `optOutNotificationMethods` list: the eleven delta/progress
/// methods plus the realtime voice channel, in that order.
pub fn suppressed_notification_methods() -> Vec<String> {
    SUPPRESSED_NOTIFICATION_METHODS
        .iter()
        .chain(SUPPRESSED_REALTIME_METHODS.iter())
        .map(|m| (*m).to_string())
        .collect()
}

/// The production handshake.
///
/// `experimentalApi` is **true**: the v2 thread/turn surface this adapter is
/// built on lives behind that flag. `requestAttestation` stays false — opting in
/// would invite `attestation/generate` requests `a2a` can only answer with an
/// error ([`crate::responder`]).
///
/// (Distinct from [`InitializeParams::for_a2a`], which is the conservative
/// protocol-level constructor the codec tests use.)
pub fn initialize_params() -> InitializeParams {
    InitializeParams {
        client_info: ClientInfo {
            name: CLIENT_NAME.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            title: None,
        },
        capabilities: Some(InitializeCapabilities {
            experimental_api: true,
            opt_out_notification_methods: Some(suppressed_notification_methods()),
            request_attestation: false,
        }),
    }
}

/// Performs the handshake on an already-spawned child.
pub async fn initialize(client: &Client, timeout: Duration) -> Result<InitInfo, RpcError> {
    let response: InitializeResponse = client
        .call("initialize", &initialize_params(), timeout)
        .await?;
    Ok(InitInfo {
        user_agent: response.user_agent,
        codex_home: response.codex_home,
    })
}

// ---------------------------------------------------------------------------
// threads
// ---------------------------------------------------------------------------

/// What a started (or resumed) thread is, in the terms `state.json` records.
///
/// `thread_id` and `session_id` are the same UUID on 0.140.0 — both are kept
/// because the second is the id `codex exec resume` accepts, which is what makes
/// an app-server worker resumable on the exec fallback.
#[derive(Clone, Debug, PartialEq)]
pub struct ThreadInfo {
    pub thread_id: String,
    pub session_id: String,
    /// The sandbox the server actually resolved — the TYPED policy object it
    /// echoes back, not the kebab-case string that was sent.
    pub resolved_sandbox: SandboxPolicy,
}

/// Where the thread is rooted.
///
/// An `outbox-only` lane is rooted on its **worker dir**: `--add-dir`-style
/// extra roots sit *alongside* an always-writable workspace, so denying extra
/// roots is not enough — the workspace itself has to be moved off the repo. The
/// intended repo path travels to the worker inside the brief, as read-only
/// context. Every other lane is rooted on the target cwd.
pub fn thread_cwd(lane: &LaneConfig, cwd: &Path, worker_dir: &Path) -> PathBuf {
    match lane.sandbox {
        SandboxMode::OutboxOnly => worker_dir.to_path_buf(),
        _ => cwd.to_path_buf(),
    }
}

/// The kebab-case `SandboxMode` string `thread/start` takes. `outbox-only` is
/// expressed as `workspace-write` with the root relocated (see [`thread_cwd`])
/// — exactly as the exec transport spells it.
pub fn sandbox_mode_param(lane: &LaneConfig) -> SandboxModeParam {
    match lane.sandbox {
        SandboxMode::DangerFullAccess => SandboxModeParam::DangerFullAccess,
        SandboxMode::WorkspaceWrite | SandboxMode::OutboxOnly => SandboxModeParam::WorkspaceWrite,
    }
}

/// The **per-turn** sandbox policy — the typed four-variant object, camelCase
/// fields (never the snake_case `SandboxWorkspaceWrite` decoy, which would
/// serialize cleanly and grant nothing).
///
/// * `outbox-only` → writable roots are exactly `[<worker_dir>/outbox]`. The
///   thread is already rooted on the worker dir; naming the outbox explicitly is
///   what makes the channel writable no matter how the server treats the cwd.
/// * `workspace-write` → `[cwd, <worker_dir>/outbox]`, the app-server spelling
///   of exec's `-C <cwd> --add-dir <outbox>`.
/// * `danger-full-access` → no confinement at all.
pub fn turn_sandbox_policy(lane: &LaneConfig, cwd: &Path, worker_dir: &Path) -> SandboxPolicy {
    let outbox = worker_dir.join("outbox");
    let writable_roots = match lane.sandbox {
        SandboxMode::OutboxOnly => vec![outbox],
        _ => vec![cwd.to_path_buf(), outbox],
    };
    match lane.sandbox {
        SandboxMode::DangerFullAccess => SandboxPolicy::DangerFullAccess,
        _ => SandboxPolicy::WorkspaceWrite {
            writable_roots,
            network_access: lane.network,
            exclude_slash_tmp: false,
            exclude_tmpdir_env_var: false,
        },
    }
}

/// Pure: the params for `thread/start`. `ephemeral` is never set — an ephemeral
/// thread is not materialized on disk and cannot be resumed.
pub fn thread_start_params(lane: &LaneConfig, cwd: &Path, worker_dir: &Path) -> ThreadStartParams {
    ThreadStartParams {
        cwd: Some(thread_cwd(lane, cwd, worker_dir)),
        sandbox: Some(sandbox_mode_param(lane)),
        approval_policy: Some(ApprovalPolicy::Never),
        model: Some(lane.model.clone()),
        config: None,
        developer_instructions: None,
        ephemeral: None,
    }
}

/// Pure: the params for `thread/resume`. Deliberately carries **no**
/// `sandboxPolicy` (the call does not accept one); the next `turn/start`
/// re-supplies it.
pub fn thread_resume_params(
    thread_id: &str,
    lane: &LaneConfig,
    cwd: &Path,
    worker_dir: &Path,
) -> ThreadResumeParams {
    ThreadResumeParams {
        thread_id: thread_id.to_string(),
        cwd: Some(thread_cwd(lane, cwd, worker_dir)),
        sandbox: Some(sandbox_mode_param(lane)),
        approval_policy: Some(ApprovalPolicy::Never),
        model: Some(lane.model.clone()),
        config: None,
        developer_instructions: None,
    }
}

fn thread_info(response: ThreadStartResponse) -> ThreadInfo {
    ThreadInfo {
        thread_id: response.thread.id,
        session_id: response.thread.session_id,
        resolved_sandbox: response.sandbox,
    }
}

pub async fn thread_start(
    client: &Client,
    lane: &LaneConfig,
    cwd: &Path,
    worker_dir: &Path,
    timeout: Duration,
) -> Result<ThreadInfo, RpcError> {
    let response: ThreadStartResponse = client
        .call(
            "thread/start",
            &thread_start_params(lane, cwd, worker_dir),
            timeout,
        )
        .await?;
    Ok(thread_info(response))
}

pub async fn thread_resume(
    client: &Client,
    thread_id: &str,
    lane: &LaneConfig,
    cwd: &Path,
    worker_dir: &Path,
    timeout: Duration,
) -> Result<ThreadInfo, RpcError> {
    let response: ThreadStartResponse = client
        .call(
            "thread/resume",
            &thread_resume_params(thread_id, lane, cwd, worker_dir),
            timeout,
        )
        .await?;
    Ok(thread_info(response))
}

// ---------------------------------------------------------------------------
// turns
// ---------------------------------------------------------------------------

/// Everything one turn needs. Borrowed and `Copy` so a caller can vary the text
/// (`TurnSpec { text: …, ..spec }`) without re-stating the policy.
#[derive(Clone, Copy, Debug)]
pub struct TurnSpec<'a> {
    pub thread_id: &'a str,
    /// The prompt. It travels in `input`, never in an argv.
    pub text: &'a str,
    pub lane: &'a LaneConfig,
    /// The target working directory (the repo), regardless of lane — the lane
    /// decides whether the thread is rooted here or on the worker dir.
    pub cwd: &'a Path,
    pub worker_dir: &'a Path,
    /// Re-supplied on EVERY turn: `outputSchema` is per-turn, not sticky.
    pub output_schema: Option<&'a serde_json::Value>,
}

/// A started turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnStarted {
    pub turn_id: String,
    pub status: TurnStatus,
}

/// Pure: the params for `turn/start`. The approval policy is re-stated here as
/// well as on the thread — it is the one security-relevant param that would
/// otherwise be the only one not re-supplied per turn.
pub fn turn_start_params(spec: &TurnSpec<'_>) -> TurnStartParams {
    TurnStartParams {
        thread_id: spec.thread_id.to_string(),
        input: vec![UserInput::text(spec.text)],
        sandbox_policy: Some(turn_sandbox_policy(spec.lane, spec.cwd, spec.worker_dir)),
        approval_policy: Some(ApprovalPolicy::Never),
        cwd: None,
        effort: Some(spec.lane.effort.clone()),
        model: None,
        output_schema: spec.output_schema.cloned(),
    }
}

pub async fn turn_start(
    client: &Client,
    spec: &TurnSpec<'_>,
    timeout: Duration,
) -> Result<TurnStarted, RpcError> {
    let response: TurnStartResponse = client
        .call("turn/start", &turn_start_params(spec), timeout)
        .await?;
    Ok(TurnStarted {
        turn_id: response.turn.id,
        status: response.turn.status,
    })
}

/// Mid-turn steering. The response body is not modelled: the call either
/// succeeds or fails, and the turn's own notifications carry what happened.
pub async fn turn_steer(
    client: &Client,
    thread_id: &str,
    expected_turn_id: &str,
    text: &str,
    timeout: Duration,
) -> Result<(), RpcError> {
    let params = TurnSteerParams {
        thread_id: thread_id.to_string(),
        expected_turn_id: expected_turn_id.to_string(),
        input: vec![UserInput::text(text)],
    };
    let _: serde_json::Value = client.call("turn/steer", &params, timeout).await?;
    Ok(())
}

/// True interrupt: both ids are required, and both are tracked in `state.json`
/// from the turn lifecycle notifications.
pub async fn turn_interrupt(
    client: &Client,
    thread_id: &str,
    turn_id: &str,
    timeout: Duration,
) -> Result<(), RpcError> {
    let params = TurnInterruptParams {
        thread_id: thread_id.to_string(),
        turn_id: turn_id.to_string(),
    };
    let _: serde_json::Value = client.call("turn/interrupt", &params, timeout).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// the send decision table
// ---------------------------------------------------------------------------

/// Which arm of the decision table a message took. Recorded as an event, so an
/// operator can see whether a message steered a live turn or opened a new one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendPath {
    /// A turn was open and accepted the steer.
    Steered,
    /// No turn was known: a fresh `turn/start`.
    Started,
    /// The steer lost the race at a turn boundary and became a `turn/start`.
    RetriedAsStart,
}

impl SendPath {
    pub fn as_str(self) -> &'static str {
        match self {
            SendPath::Steered => "steered",
            SendPath::Started => "started",
            SendPath::RetriedAsStart => "retried_as_start",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendOutcome {
    pub path: SendPath,
    /// The turn the message ended up in: the steered turn, or the new one.
    pub turn_id: String,
}

/// The ONE predicate that decides whether a failed steer is the expected
/// turn-boundary race or a real error.
///
/// **Message-only, by evidence.** The first cut of this predicate also accepted
/// [`STEER_PRECONDITION_CODE`] on its own, on the theory that a re-coded error
/// should still route; Task 18 measured the real server and found that code is
/// its catch-all for *every* malformed request — including `thread not found` —
/// so accepting it would turn a wrong thread id into a second turn against a
/// live worker. The server's own wording is the only thing that distinguishes
/// the race, so that is what is matched.
///
/// Everything else — including [`RpcError::Timeout`] and
/// [`RpcError::ChildDied`] — is false: retrying an unknown failure as a fresh
/// turn would double-drive the worker.
pub fn is_steer_precondition_failure(err: &RpcError) -> bool {
    match err {
        RpcError::Remote { message, .. } => {
            let message = message.to_ascii_lowercase();
            STEER_PRECONDITION_MARKERS
                .iter()
                .any(|marker| message.contains(marker))
        }
        _ => false,
    }
}

/// `a2a send`, on the app-server transport (spec §4.3).
///
/// | last turn id | steer result | outcome |
/// |---|---|---|
/// | `Some` | ok | [`SendPath::Steered`] |
/// | `Some` | precondition failure | retried → [`SendPath::RetriedAsStart`] |
/// | `Some` | any other error | surfaced unchanged |
/// | `None` | — | [`SendPath::Started`] |
pub async fn send_message(
    client: &Client,
    spec: &TurnSpec<'_>,
    last_turn_id: Option<&str>,
    timeout: Duration,
) -> Result<SendOutcome, RpcError> {
    if let Some(turn_id) = last_turn_id {
        match turn_steer(client, spec.thread_id, turn_id, spec.text, timeout).await {
            Ok(()) => {
                return Ok(SendOutcome {
                    path: SendPath::Steered,
                    turn_id: turn_id.to_string(),
                })
            }
            Err(e) if is_steer_precondition_failure(&e) => {
                let started = turn_start(client, spec, timeout).await?;
                return Ok(SendOutcome {
                    path: SendPath::RetriedAsStart,
                    turn_id: started.turn_id,
                });
            }
            Err(e) => return Err(e),
        }
    }
    let started = turn_start(client, spec, timeout).await?;
    Ok(SendOutcome {
        path: SendPath::Started,
        turn_id: started.turn_id,
    })
}

// ---------------------------------------------------------------------------
// AdapterRef
// ---------------------------------------------------------------------------

/// The handle a `WorkerActor` holds: a shared, cheap-to-clone reference to the
/// one app-server connection every worker multiplexes over.
///
/// A plain `Arc<Client>` with **no interior lock** — `Client` needs none, since
/// its pending-call map lives inside the dispatcher task rather than behind a
/// mutex. That is what lets the broker's source scan forbid `Arc<Mutex<…>>`
/// outright.
///
/// The child's identity travels WITH the connection rather than beside it,
/// because the two are replaced together: supervision restarts the child and
/// publishes a fresh `AdapterRef` on a `watch` channel, so an actor that reads
/// the current value gets the new pid and the new stdin in one atomic step —
/// there is no window in which it could record the old pid against the new
/// connection.
#[derive(Clone)]
pub struct AdapterRef {
    client: Arc<Client>,
    pid: Option<u32>,
    start_witness: String,
}

impl AdapterRef {
    /// Without a recorded child identity — for probes and tests that never
    /// write a `ProcessStarted` row.
    pub fn new(client: Arc<Client>) -> Self {
        AdapterRef {
            client,
            pid: None,
            start_witness: String::new(),
        }
    }

    /// With the shared child's pid and its start-time witness
    /// ([`crate::exec::start_witness`]). Recorded for observability: an
    /// app-server worker's kill/timeout paths route through the broker and
    /// NEVER group-kill this pid — it is shared by every worker.
    pub fn with_child(client: Arc<Client>, pid: u32, start_witness: impl Into<String>) -> Self {
        AdapterRef {
            client,
            pid: Some(pid),
            start_witness: start_witness.into(),
        }
    }

    /// The underlying connection, for the calls the actor does not make
    /// (`respond`, thread lifecycle).
    pub fn client(&self) -> &Arc<Client> {
        &self.client
    }

    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    pub fn start_witness(&self) -> &str {
        &self.start_witness
    }

    pub async fn send_message(
        &self,
        spec: &TurnSpec<'_>,
        last_turn_id: Option<&str>,
        timeout: Duration,
    ) -> Result<SendOutcome, RpcError> {
        send_message(&self.client, spec, last_turn_id, timeout).await
    }

    pub async fn turn_start(
        &self,
        spec: &TurnSpec<'_>,
        timeout: Duration,
    ) -> Result<TurnStarted, RpcError> {
        turn_start(&self.client, spec, timeout).await
    }

    pub async fn turn_interrupt(
        &self,
        thread_id: &str,
        turn_id: &str,
        timeout: Duration,
    ) -> Result<(), RpcError> {
        turn_interrupt(&self.client, thread_id, turn_id, timeout).await
    }
}

// ---------------------------------------------------------------------------
// handshake_probe — the one sanctioned sync-CLI tokio exception
// ---------------------------------------------------------------------------

/// Spawns `<bin> app-server`, completes the handshake, and kills the child —
/// synchronously.
///
/// This is the single place the otherwise-synchronous CLI is allowed to build a
/// tokio runtime (a private `current_thread` one), because `doctor` (Task 16)
/// and transport selection (Task 11) must know whether a real app-server
/// answers *before* any broker exists.
///
/// `timeout` bounds the whole exchange, not each call, and the child is killed
/// on every path out — including expiry, which is the case that matters: a
/// probe that leaks a live app-server is worse than a probe that fails.
///
/// The child's stderr is discarded: this is a liveness check, and its verdict is
/// the returned [`RpcError`].
pub fn handshake_probe(bin: &Path, timeout: Duration) -> Result<InitInfo, RpcError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| RpcError::Decode(format!("could not build the probe runtime: {e}")))?;
    runtime.block_on(probe(bin, timeout))
}

async fn probe(bin: &Path, timeout: Duration) -> Result<InitInfo, RpcError> {
    let (client, _incoming, mut child) = Client::spawn(
        bin,
        &APP_SERVER_ARGS,
        &[],
        // Not /dev/null-by-name for cleverness: `Client::spawn` opens the path
        // for append, and /dev/null is the one path that is always writable and
        // never grows.
        Path::new("/dev/null"),
    )
    .await
    .map_err(|_| RpcError::ChildDied)?;

    let outcome = match tokio::time::timeout(timeout, initialize(&client, timeout)).await {
        Ok(result) => result,
        Err(_) => Err(RpcError::Timeout),
    };
    // Closing stdin is the polite exit signal; the kill is the guarantee.
    drop(client);
    let _ = child.kill().await;
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_MODEL;
    use serde_json::json;
    use std::collections::BTreeSet;

    fn lane_cfg(sandbox: SandboxMode, network: bool) -> LaneConfig {
        LaneConfig {
            sandbox,
            network,
            worktree_default: false,
            model: DEFAULT_MODEL.to_string(),
            effort: "medium".to_string(),
            start_timeout_secs: 120,
            task_timeout_secs: 3600,
            blocked_timeout_secs: 1800,
            allow_elevation: false,
            max_turns: 30,
            auto_review: false,
            auto_commit: false,
        }
    }

    fn cwd() -> PathBuf {
        PathBuf::from("/repo/project")
    }
    fn worker() -> PathBuf {
        PathBuf::from("/home/.a2a/workers/w-1")
    }

    // --- initialize ---------------------------------------------------------

    #[test]
    fn the_handshake_is_the_production_one() {
        let p = initialize_params();
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["clientInfo"]["name"], "a2a");
        assert_eq!(v["clientInfo"]["version"], env!("CARGO_PKG_VERSION"));
        // The v2 thread/turn surface rides behind the experimental flag ...
        assert_eq!(v["capabilities"]["experimentalApi"], true);
        // ... and attestation is never opted into: a2a has no token to answer
        // `attestation/generate` with.
        assert_eq!(v["capabilities"]["requestAttestation"], false);
        fidelity::assert_valid(&v, "v1/InitializeParams.json", "adapter initialize params");
    }

    #[test]
    fn the_opt_out_list_is_the_eleven_deltas_plus_the_voice_channel() {
        let opts = suppressed_notification_methods();
        assert_eq!(
            opts.len(),
            SUPPRESSED_NOTIFICATION_METHODS.len() + SUPPRESSED_REALTIME_METHODS.len()
        );
        assert_eq!(opts.len(), 22);
        for m in SUPPRESSED_NOTIFICATION_METHODS {
            assert!(opts.contains(&m.to_string()), "{m} missing");
        }
        for m in SUPPRESSED_REALTIME_METHODS {
            assert!(opts.contains(&m.to_string()), "{m} missing");
        }
        let unique: BTreeSet<&String> = opts.iter().collect();
        assert_eq!(unique.len(), opts.len(), "duplicate opt-out entry");
    }

    /// The realtime set is a claim about the server's vocabulary, so it is
    /// pinned against the captured method list rather than against memory.
    #[test]
    fn the_realtime_suppression_set_matches_the_snapshot_method_list() {
        let raw = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("schema/v2-snapshot/server_notification_methods.txt"),
        )
        .unwrap();
        let from_snapshot: BTreeSet<&str> = raw
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with("thread/realtime/"))
            .collect();
        let pinned: BTreeSet<&str> = SUPPRESSED_REALTIME_METHODS.iter().copied().collect();
        assert_eq!(
            from_snapshot, pinned,
            "the realtime method set drifted — regenerate and update \
             SUPPRESSED_REALTIME_METHODS"
        );
    }

    // --- thread params ------------------------------------------------------

    #[test]
    fn only_outbox_only_lanes_are_rooted_on_the_worker_dir() {
        assert_eq!(
            thread_cwd(&lane_cfg(SandboxMode::OutboxOnly, false), &cwd(), &worker()),
            worker(),
            "an outbox-only worker must not be rooted on the repo"
        );
        for mode in [SandboxMode::WorkspaceWrite, SandboxMode::DangerFullAccess] {
            assert_eq!(
                thread_cwd(&lane_cfg(mode, false), &cwd(), &worker()),
                cwd(),
                "{mode} must be rooted on the target cwd"
            );
        }
    }

    #[test]
    fn the_sandbox_string_is_kebab_case_and_outbox_only_borrows_workspace_write() {
        for (mode, want) in [
            (SandboxMode::WorkspaceWrite, "workspace-write"),
            (SandboxMode::OutboxOnly, "workspace-write"),
            (SandboxMode::DangerFullAccess, "danger-full-access"),
        ] {
            let got = sandbox_mode_param(&lane_cfg(mode, false));
            assert_eq!(serde_json::to_value(got).unwrap(), json!(want), "{mode}");
        }
    }

    #[test]
    fn thread_start_params_never_send_ephemeral_and_pin_the_approval_policy() {
        let p = thread_start_params(&lane_cfg(SandboxMode::OutboxOnly, false), &cwd(), &worker());
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["cwd"], json!(worker().to_str().unwrap()));
        assert_eq!(v["sandbox"], "workspace-write");
        assert_eq!(v["approvalPolicy"], "never");
        assert_eq!(v["model"], DEFAULT_MODEL);
        assert!(
            v.get("ephemeral").is_none(),
            "an ephemeral thread cannot be resumed: never send it"
        );
        assert!(
            v.get("writableRoots").is_none(),
            "thread/start has no writableRoots — confinement is per-turn"
        );
        fidelity::assert_valid(
            &v,
            "v2/ThreadStartParams.json",
            "adapter thread/start params",
        );
    }

    #[test]
    fn thread_resume_params_carry_no_sandbox_policy() {
        let p = thread_resume_params(
            "thread-1",
            &lane_cfg(SandboxMode::WorkspaceWrite, false),
            &cwd(),
            &worker(),
        );
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["threadId"], "thread-1");
        assert_eq!(v["cwd"], json!(cwd().to_str().unwrap()));
        assert!(
            v.get("sandboxPolicy").is_none(),
            "resume takes none — the next turn/start re-supplies it"
        );
        fidelity::assert_valid(
            &v,
            "v2/ThreadResumeParams.json",
            "adapter thread/resume params",
        );
    }

    // --- turn params --------------------------------------------------------

    #[test]
    fn an_outbox_only_turn_may_write_to_its_outbox_and_nowhere_else() {
        let policy =
            turn_sandbox_policy(&lane_cfg(SandboxMode::OutboxOnly, false), &cwd(), &worker());
        assert_eq!(
            policy,
            SandboxPolicy::WorkspaceWrite {
                writable_roots: vec![worker().join("outbox")],
                network_access: false,
                exclude_slash_tmp: false,
                exclude_tmpdir_env_var: false,
            }
        );
    }

    #[test]
    fn a_workspace_write_turn_gets_the_repo_and_the_outbox() {
        let policy = turn_sandbox_policy(
            &lane_cfg(SandboxMode::WorkspaceWrite, true),
            &cwd(),
            &worker(),
        );
        assert_eq!(
            policy,
            SandboxPolicy::WorkspaceWrite {
                writable_roots: vec![cwd(), worker().join("outbox")],
                network_access: true,
                exclude_slash_tmp: false,
                exclude_tmpdir_env_var: false,
            }
        );
        assert_eq!(
            turn_sandbox_policy(
                &lane_cfg(SandboxMode::DangerFullAccess, false),
                &cwd(),
                &worker()
            ),
            SandboxPolicy::DangerFullAccess
        );
    }

    #[test]
    fn the_per_turn_policy_uses_the_camel_case_wire_names_not_the_decoy() {
        let policy =
            turn_sandbox_policy(&lane_cfg(SandboxMode::OutboxOnly, false), &cwd(), &worker());
        let v = serde_json::to_value(&policy).unwrap();
        assert_eq!(v["type"], "workspaceWrite");
        assert!(v.get("writableRoots").is_some());
        assert!(
            v.get("writable_roots").is_none(),
            "the snake_case config type is a decoy: it grants nothing"
        );
    }

    #[test]
    fn turn_start_params_re_supply_the_per_turn_overrides() {
        let lane = lane_cfg(SandboxMode::OutboxOnly, false);
        let schema = json!({"type": "object"});
        let spec = TurnSpec {
            thread_id: "thread-1",
            text: "do the thing",
            lane: &lane,
            cwd: &cwd(),
            worker_dir: &worker(),
            output_schema: Some(&schema),
        };
        let v = serde_json::to_value(turn_start_params(&spec)).unwrap();
        assert_eq!(v["threadId"], "thread-1");
        assert_eq!(
            v["input"],
            json!([{"type": "text", "text": "do the thing"}])
        );
        assert_eq!(v["effort"], "medium");
        assert_eq!(v["approvalPolicy"], "never");
        assert_eq!(v["outputSchema"], schema);
        assert_eq!(
            v["sandboxPolicy"]["writableRoots"],
            json!([worker().join("outbox").to_str().unwrap()])
        );
        fidelity::assert_valid(&v, "v2/TurnStartParams.json", "adapter turn/start params");

        // Absent means absent — never a null the server has to interpret.
        let v = serde_json::to_value(turn_start_params(&TurnSpec {
            output_schema: None,
            ..spec
        }))
        .unwrap();
        assert!(v.get("outputSchema").is_none());
        fidelity::assert_valid(&v, "v2/TurnStartParams.json", "adapter turn/start params");
    }

    /// The lane's model is set once, on the thread; a per-turn override would
    /// be a second place for it to drift.
    #[test]
    fn the_model_is_a_thread_level_decision() {
        let lane = lane_cfg(SandboxMode::WorkspaceWrite, false);
        let spec = TurnSpec {
            thread_id: "t",
            text: "x",
            lane: &lane,
            cwd: &cwd(),
            worker_dir: &worker(),
            output_schema: None,
        };
        let v = serde_json::to_value(turn_start_params(&spec)).unwrap();
        assert!(v.get("model").is_none());
        assert_eq!(
            serde_json::to_value(thread_start_params(&lane, &cwd(), &worker())).unwrap()["model"],
            DEFAULT_MODEL
        );
    }

    // --- the steer predicate ------------------------------------------------

    #[test]
    fn a_lost_steer_race_is_recognized_by_the_message_the_live_server_sends() {
        // Both messages transcribed verbatim from codex-cli 0.140.0 (Task 18).
        // The first is the TOCTOU this path exists for; the second is a
        // *different* turn having opened in the gap.
        assert!(is_steer_precondition_failure(&RpcError::Remote {
            code: STEER_PRECONDITION_CODE,
            message: "no active turn to steer".into(),
        }));
        assert!(is_steer_precondition_failure(&RpcError::Remote {
            code: STEER_PRECONDITION_CODE,
            message: "expected active turn id `turn_a` but found `turn_b`".into(),
        }));
        // Re-coded, same wording: still the race. The message is the
        // discriminator, so the code may move.
        assert!(is_steer_precondition_failure(&RpcError::Remote {
            code: -32002,
            message: "No active turn to steer".into(),
        }));
    }

    /// The half of the predicate that protects the worker: everything the live
    /// server says under the SAME error code that is *not* the race must
    /// surface, because a retry would open a second turn on a running worker.
    #[test]
    fn nothing_else_is_treated_as_a_lost_race() {
        for err in [
            RpcError::Timeout,
            RpcError::ChildDied,
            RpcError::Decode("bad json".into()),
            // Observed on 0.140.0 under the steer code itself — the exact
            // reason the code alone can never route.
            RpcError::Remote {
                code: STEER_PRECONDITION_CODE,
                message: "thread not found: 019898f0-0000-7000-8000-000000000001".into(),
            },
            // Also observed, and an a2a bug rather than a race: retrying it
            // would hide a malformed request behind a second turn.
            RpcError::Remote {
                code: STEER_PRECONDITION_CODE,
                message: "Invalid request: missing field `expectedTurnId`".into(),
            },
            RpcError::Remote {
                code: -32001,
                message: "thread could not be resumed".into(),
            },
            RpcError::Remote {
                code: -32603,
                message: "internal error".into(),
            },
        ] {
            assert!(
                !is_steer_precondition_failure(&err),
                "{err:?} must surface unchanged, not be retried as a new turn"
            );
        }
    }

    #[test]
    fn send_paths_have_stable_wire_names() {
        assert_eq!(SendPath::Steered.as_str(), "steered");
        assert_eq!(SendPath::Started.as_str(), "started");
        assert_eq!(SendPath::RetriedAsStart.as_str(), "retried_as_start");
    }

    /// The outbox is the channel the whole worker protocol runs over: every
    /// confined lane must be able to write to it, whatever else it may touch.
    #[test]
    fn every_confined_lane_keeps_its_outbox_writable() {
        for mode in [SandboxMode::OutboxOnly, SandboxMode::WorkspaceWrite] {
            match turn_sandbox_policy(&lane_cfg(mode, false), &cwd(), &worker()) {
                SandboxPolicy::WorkspaceWrite { writable_roots, .. } => assert!(
                    writable_roots.contains(&worker().join("outbox")),
                    "{mode} cannot reach its own outbox: {writable_roots:?}"
                ),
                other => panic!("{mode} produced {other:?}"),
            }
        }
    }
}
