//! Typed subset of the `codex app-server` wire protocol (spec §4.3).
//!
//! Scope discipline: this module types only what `a2a` actually sends and
//! reads — the v1 handshake, the v2 thread/turn lifecycle, and the notification
//! envelopes the fold consumes. Everything else stays `serde_json::Value`.
//! The authoritative shapes are the checked-in schemas under
//! `schema/v2-snapshot/`, gated by `tests/protocol_canary.rs`.
//!
//! Framing, verified live against codex-cli 0.140.0: **NDJSON JSON-RPC over
//! stdio**. Requests carry `jsonrpc`, `id`, `method`, `params`; **responses omit
//! `jsonrpc`** ([`RpcResponse`] has no such field — this is not an oversight);
//! notifications carry `method`+`params` and no `id`. A frame with both `id`
//! *and* `method` is a server→client **request** and owes an answer (see
//! [`SERVER_REQUEST_METHODS`] — a dropped request is a permanent hang).
//!
//! Forward compatibility follows `events.rs`: every enum is *open*
//! (`#[serde(other)]` catch-all) and every struct that crosses the wire keeps
//! unrecognized siblings in a flattened `extra` bag. This is not theoretical —
//! the live 0.140.0 `thread/start` response already returns
//! `runtimeWorkspaceRoots` and `activePermissionProfile`, neither of which
//! appears in that release's own published schema.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

type Extra = serde_json::Map<String, serde_json::Value>;

/// The JSON-RPC version string `a2a` stamps on outbound requests.
pub const JSONRPC_VERSION: &str = "2.0";

/// Every server→client request method, sorted. The responder (spec §4.3) owes
/// an answer to each one within 100ms — an unanswered request hangs the turn
/// forever, so this array is a hard pin, cross-checked against
/// `schema/v2-snapshot/` and against a live regen by `tests/protocol_canary.rs`.
///
/// Four of these have **no deny shape at all** in their response schema
/// (`item/permissions/requestApproval`, `item/tool/requestUserInput`,
/// `account/chatgptAuthTokens/refresh`, `attestation/generate`) and must be
/// answered with a JSON-RPC *error* rather than a payload.
pub const SERVER_REQUEST_METHODS: [&str; 10] = [
    "account/chatgptAuthTokens/refresh",
    "applyPatchApproval",
    "attestation/generate",
    "execCommandApproval",
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
    "item/permissions/requestApproval",
    "item/tool/call",
    "item/tool/requestUserInput",
    "mcpServer/elicitation/request",
];

/// The exact `optOutNotificationMethods` list `a2a` sends at initialize: the
/// high-volume token/diff streams. `a2a` folds completed items, never deltas,
/// so suppressing these is pure win. `thread/realtime/*` (the voice channel) is
/// opted out too — it is not part of the token stream, it is simply unused.
pub const SUPPRESSED_NOTIFICATION_METHODS: [&str; 11] = [
    "item/agentMessage/delta",
    "item/reasoning/textDelta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/commandExecution/outputDelta",
    "item/fileChange/outputDelta",
    "item/plan/delta",
    "item/mcpToolCall/progress",
    "process/outputDelta",
    "command/exec/outputDelta",
    "turn/diff/updated",
];

// ---------------------------------------------------------------------------
// JSON-RPC framing
// ---------------------------------------------------------------------------

/// A client→server request. `id` is `Value` because the protocol's `RequestId`
/// admits both integers and strings.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    pub method: String,
    pub params: serde_json::Value,
}

impl RpcRequest {
    pub fn new(id: impl Into<serde_json::Value>, method: &str, params: serde_json::Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id: id.into(),
            method: method.to_string(),
            params,
        }
    }
}

/// A response frame. **There is deliberately no `jsonrpc` field**: codex
/// 0.140.0 answers `{"id":N,"result":…}` / `{"id":N,"error":…}` with no version
/// tag, verified live. Exactly one of `result`/`error` is populated.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcResponse {
    pub id: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcErrorObject>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RpcErrorObject {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// A server→client notification: `method` + `params`, never an `id`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcNotification {
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

impl RpcNotification {
    /// The routing key for this notification, or `None` if it is
    /// broker-scoped. Routing is **per-method**, not uniform:
    /// `thread/started` carries no top-level `threadId` (the key lives at
    /// `params.thread.id`), while `account/rateLimits/updated`,
    /// `deprecationNotice` and `configWarning` carry no thread at all.
    pub fn thread_id(&self) -> Option<&str> {
        self.params
            .get("threadId")
            .and_then(|v| v.as_str())
            .or_else(|| self.params.pointer("/thread/id").and_then(|v| v.as_str()))
    }
}

// ---------------------------------------------------------------------------
// v1: initialize  (NOTE: v1/, not v2/ — see schema/v2-snapshot/REGEN.md)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct InitializeCapabilities {
    pub experimental_api: bool,
    /// `null` and `[]` are both accepted by the server; `a2a` always sends the
    /// explicit [`SUPPRESSED_NOTIFICATION_METHODS`] list.
    #[serde(default)]
    pub opt_out_notification_methods: Option<Vec<String>>,
    /// Always `false`. Setting this true opts into `attestation/generate`
    /// requests that `a2a` has no token to answer.
    pub request_attestation: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    pub client_info: ClientInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<InitializeCapabilities>,
}

impl InitializeParams {
    /// The handshake `a2a` actually sends: deltas suppressed, no experimental
    /// API, no attestation.
    pub fn for_a2a(name: &str, version: &str) -> Self {
        Self {
            client_info: ClientInfo {
                name: name.to_string(),
                version: version.to_string(),
                title: None,
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: false,
                opt_out_notification_methods: Some(
                    SUPPRESSED_NOTIFICATION_METHODS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                ),
                request_attestation: false,
            }),
        }
    }
}

/// The handshake reply. **There is no version field anywhere in it** — which is
/// precisely why `schema/v2-snapshot/` and its canary exist. Version gating
/// continues to run off `codex --version` against
/// [`crate::config::DEFAULT_CODEX_VERSION_RANGE`].
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResponse {
    pub user_agent: String,
    pub codex_home: PathBuf,
    pub platform_family: String,
    pub platform_os: String,
    #[serde(flatten)]
    pub extra: Extra,
}

// ---------------------------------------------------------------------------
// Sandbox
// ---------------------------------------------------------------------------

/// The **string** sandbox selector `thread/start` and `thread/resume` take
/// (`SandboxMode` in the schema). Distinct from [`SandboxPolicy`], which is the
/// typed object the same call *echoes back* and that `turn/start` accepts.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxModeParam {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum NetworkAccess {
    #[default]
    Restricted,
    Enabled,
    #[serde(other)]
    Unknown,
}

/// The per-turn sandbox, internally tagged on `type` with **camelCase** field
/// names.
///
/// ⚠️ **DECOY WARNING.** The aggregate schema
/// (`codex_app_server_protocol.v2.schemas.json`, which is deliberately *not*
/// snapshotted) also defines an unrelated `SandboxWorkspaceWrite` type whose
/// fields are **snake_case** — `writable_roots`, `network_access`,
/// `exclude_slash_tmp`, `exclude_tmpdir_env_var`. It is a config-file type, not
/// this wire type. Copying its field names here produces a struct that
/// serializes cleanly, is accepted by the server, and **silently applies no
/// writable roots**. The names below are the wire names; do not "fix" them.
///
/// [`SandboxPolicy::Unknown`] exists so a future fifth variant deserializes
/// instead of erroring; `a2a` never constructs or sends it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SandboxPolicy {
    DangerFullAccess,
    #[serde(rename_all = "camelCase")]
    ReadOnly {
        #[serde(default)]
        network_access: bool,
    },
    #[serde(rename_all = "camelCase")]
    ExternalSandbox {
        #[serde(default)]
        network_access: NetworkAccess,
    },
    #[serde(rename_all = "camelCase")]
    WorkspaceWrite {
        #[serde(default)]
        writable_roots: Vec<PathBuf>,
        #[serde(default)]
        network_access: bool,
        #[serde(default)]
        exclude_slash_tmp: bool,
        #[serde(default)]
        exclude_tmpdir_env_var: bool,
    },
    #[serde(other)]
    Unknown,
}

/// The string forms of `AskForApproval`. `a2a` only ever *sends* these; the
/// schema also admits a `{granular:{…}}` object, so **read** paths keep the raw
/// `serde_json::Value` (see [`ThreadStartResponse::approval_policy`]) rather
/// than forcing it through this enum.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ApprovalPolicy {
    Untrusted,
    OnFailure,
    OnRequest,
    Never,
    #[serde(other)]
    Unknown,
}

// ---------------------------------------------------------------------------
// v2: threads
// ---------------------------------------------------------------------------

/// Params for `thread/start`. Every field is optional.
///
/// There is **no `writableRoots`** here and none on `thread/resume`: root
/// confinement rides the per-turn [`TurnStartParams::sandbox_policy`].
/// `ephemeral` is modelled for completeness only — **never send `true`** from
/// production code; an ephemeral thread is not materialized on disk and cannot
/// be resumed, which breaks crash recovery.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxModeParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<ApprovalPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub developer_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,
}

/// Params for `thread/resume`. `threadId` is the only required field.
///
/// A resume carries **no `sandboxPolicy`** — per-turn overrides must be
/// re-supplied on the next `turn/start`.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ThreadResumeParams {
    pub thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxModeParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<ApprovalPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub developer_instructions: Option<String>,
}

/// A thread as the server reports it.
///
/// **`id` and `session_id` are the same UUID** (verified live). That matters:
/// the app-server thread id *is* the id `codex exec resume` accepts, so a
/// worker started on the broker transport stays resumable on the exec fallback.
/// Both are recorded in the snapshot.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Thread {
    pub id: String,
    pub session_id: String,
    pub cwd: PathBuf,
    /// Rollout file the session is being written to, when the thread is
    /// materialized on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_version: Option<String>,
    #[serde(default)]
    pub ephemeral: bool,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Result of `thread/start`. The echoed `sandbox` is the **typed**
/// [`SandboxPolicy`] object, not the string that was sent.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartResponse {
    pub thread: Thread,
    pub model: String,
    pub model_provider: String,
    pub cwd: PathBuf,
    pub sandbox: SandboxPolicy,
    /// Left untyped: `AskForApproval` admits both a string and a
    /// `{granular:{…}}` object.
    pub approval_policy: serde_json::Value,
    pub approvals_reviewer: serde_json::Value,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub instruction_sources: Vec<serde_json::Value>,
    /// Catches, among others, `runtimeWorkspaceRoots` and
    /// `activePermissionProfile` — both returned by live 0.140.0 and both
    /// absent from that release's own published schema.
    #[serde(flatten)]
    pub extra: Extra,
}

/// `thread/started` notification params. **No top-level `threadId`** — the
/// routing key is `params.thread.id`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ThreadStartedNotification {
    pub thread: Thread,
}

// ---------------------------------------------------------------------------
// v2: turns
// ---------------------------------------------------------------------------

/// One element of a turn's `input` array. `a2a` only ever sends
/// [`UserInput::Text`]; the image variants deserialize into `Unknown`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum UserInput {
    Text {
        text: String,
    },
    #[serde(other)]
    Unknown,
}

impl UserInput {
    pub fn text(s: impl Into<String>) -> Self {
        UserInput::Text { text: s.into() }
    }
}

/// Params for `turn/start`. `threadId` and `input` are required.
///
/// `sandbox_policy` and `output_schema` are **per-turn** — neither is sticky,
/// both must be re-supplied on every turn (and after every `thread/resume`).
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartParams {
    pub thread_id: String,
    pub input: Vec<UserInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<SandboxPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<ApprovalPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<serde_json::Value>,
}

impl TurnStartParams {
    pub fn new(thread_id: impl Into<String>, input: Vec<UserInput>) -> Self {
        Self {
            thread_id: thread_id.into(),
            input,
            sandbox_policy: None,
            approval_policy: None,
            cwd: None,
            effort: None,
            model: None,
            output_schema: None,
        }
    }
}

/// Params for `turn/steer` — mid-turn steering. All three fields required.
///
/// `expected_turn_id` is the race guard: if the turn completed between the
/// caller's read and this write, the server rejects with a remote error rather
/// than silently steering the wrong turn.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TurnSteerParams {
    pub thread_id: String,
    pub expected_turn_id: String,
    pub input: Vec<UserInput>,
}

/// Params for `turn/interrupt`. Both fields required.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TurnInterruptParams {
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TurnStatus {
    Completed,
    Interrupted,
    Failed,
    InProgress,
    #[serde(other)]
    Unknown,
}

impl TurnStatus {
    /// Whether the turn has stopped moving. `InProgress` is the only
    /// non-terminal status.
    pub fn is_terminal(self) -> bool {
        !matches!(self, TurnStatus::InProgress)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TurnError {
    pub message: String,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
    pub id: String,
    #[serde(default)]
    pub items: Vec<ThreadItem>,
    pub status: TurnStatus,
    /// Populated only when `status == Failed`.
    #[serde(default)]
    pub error: Option<TurnError>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Params shared by `turn/started` and `turn/completed`.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TurnNotification {
    pub thread_id: String,
    pub turn: Turn,
}

/// Result of `turn/start`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TurnStartResponse {
    pub turn: Turn,
}

// ---------------------------------------------------------------------------
// v2: thread items
// ---------------------------------------------------------------------------

/// The `type` discriminant of a `ThreadItem`, in **camelCase** — the
/// app-server spelling. `codex exec --json` spells the same concepts in
/// snake_case, so any code mapping both must handle both.
///
/// ⚠️ None of the three tool-call variants contains the lowercase substring
/// `"tool"` (`McpToolCall`, `DynamicToolCall`, `CollabAgentToolCall` serialize
/// as `mcpToolCall`, `dynamicToolCall`, `collabAgentToolCall`). Phase 1's
/// `contains("tool")` idiom is therefore wrong on this transport — use
/// [`ThreadItemKind::is_tool_call`] or a case-insensitive test.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ThreadItemKind {
    UserMessage,
    HookPrompt,
    AgentMessage,
    Plan,
    Reasoning,
    CommandExecution,
    FileChange,
    McpToolCall,
    DynamicToolCall,
    CollabAgentToolCall,
    SubAgentActivity,
    WebSearch,
    ImageView,
    ImageGeneration,
    EnteredReviewMode,
    ExitedReviewMode,
    ContextCompaction,
    #[serde(other)]
    Unknown,
}

impl ThreadItemKind {
    pub fn is_tool_call(self) -> bool {
        matches!(
            self,
            ThreadItemKind::McpToolCall
                | ThreadItemKind::DynamicToolCall
                | ThreadItemKind::CollabAgentToolCall
        )
    }
}

/// A thread item, kept deliberately shallow: the discriminant plus the raw
/// payload. The fold cares about the kind and a couple of well-known fields;
/// typing all seventeen payloads would be churn with no consumer.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ThreadItem {
    #[serde(rename = "type")]
    pub kind: ThreadItemKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl ThreadItem {
    /// The `text` field, present on `agentMessage` (and `reasoning`).
    pub fn text(&self) -> Option<&str> {
        self.extra.get("text").and_then(|v| v.as_str())
    }
}

/// Params for `item/completed`.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ItemCompletedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item: ThreadItem,
    #[serde(default)]
    pub completed_at_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    fn snapshot_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("schema/v2-snapshot")
    }

    /// The `required` array of a snapshot schema.
    fn required_of(rel: &str) -> BTreeSet<String> {
        let raw = std::fs::read_to_string(snapshot_dir().join(rel)).unwrap();
        let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
        doc["required"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Asserts a serialized params object carries every key the snapshot marks
    /// required.
    fn assert_covers_required<T: Serialize>(value: &T, rel: &str) {
        let v = serde_json::to_value(value).unwrap();
        let obj = v.as_object().expect("params must serialize to an object");
        let missing: Vec<String> = required_of(rel)
            .into_iter()
            .filter(|k| !obj.contains_key(k))
            .collect();
        assert!(
            missing.is_empty(),
            "{rel}: serialized params missing required keys {missing:?} (got {})",
            serde_json::to_string(&v).unwrap()
        );
    }

    // --- Real captured transcripts (codex-cli 0.140.0, 2026-08-17) ----------
    //
    // Captured by driving a live `codex app-server` over stdio under a temp
    // CODEX_HOME. Verbatim lines, not hand-written fixtures — the whole point
    // is that these are shapes the real binary emitted.

    const LIVE_INITIALIZE_RESPONSE: &str = r#"{"id":1,"result":{"userAgent":"a2a/0.140.0 (Mac OS 26.6.2; arm64) iTerm.app/3.4.23 (a2a; 0.1.0)","codexHome":"/private/var/folders/ky/8mhpl8qj5ns5xlnffgzn5ssh0000gn/T/p2-codexhome-jtkqux_b","platformFamily":"unix","platformOs":"macos"}}"#;

    const LIVE_THREAD_START_RESPONSE: &str = r#"{"id":2,"result":{"thread":{"id":"01a010ee-21c9-7f90-87f2-5f0209638f7d","sessionId":"01a010ee-21c9-7f90-87f2-5f0209638f7d","forkedFromId":null,"parentThreadId":null,"preview":"","ephemeral":false,"modelProvider":"openai","createdAt":1786990436,"updatedAt":1786990436,"status":{"type":"idle"},"path":"/private/var/folders/ky/8mhpl8qj5ns5xlnffgzn5ssh0000gn/T/p2-codexhome-jtkqux_b/sessions/2026/08/17/rollout-2026-08-17T14-13-56-01a010ee-21c9-7f90-87f2-5f0209638f7d.jsonl","cwd":"/var/folders/ky/8mhpl8qj5ns5xlnffgzn5ssh0000gn/T/p2-cwd-z6rrb4ie","cliVersion":"0.140.0","source":"vscode","threadSource":null,"agentNickname":null,"agentRole":null,"gitInfo":null,"name":null,"turns":[]},"model":"gpt-5.5","modelProvider":"openai","serviceTier":null,"cwd":"/var/folders/ky/8mhpl8qj5ns5xlnffgzn5ssh0000gn/T/p2-cwd-z6rrb4ie","runtimeWorkspaceRoots":["/var/folders/ky/8mhpl8qj5ns5xlnffgzn5ssh0000gn/T/p2-cwd-z6rrb4ie"],"instructionSources":[],"approvalPolicy":"never","approvalsReviewer":"user","sandbox":{"type":"readOnly","networkAccess":false},"activePermissionProfile":null,"reasoningEffort":null}}"#;

    const LIVE_THREAD_STARTED_NOTIFICATION: &str = r#"{"method":"thread/started","params":{"thread":{"id":"01a010ee-c244-7341-9948-1dc7303e753b","sessionId":"01a010ee-c244-7341-9948-1dc7303e753b","forkedFromId":null,"parentThreadId":null,"preview":"","ephemeral":false,"modelProvider":"openai","createdAt":1786990477,"updatedAt":1786990477,"status":{"type":"idle"},"path":"/private/var/folders/ky/8mhpl8qj5ns5xlnffgzn5ssh0000gn/T/p2-authhome-mw6zvnkc/sessions/2026/08/17/rollout-2026-08-17T14-14-37-01a010ee-c244-7341-9948-1dc7303e753b.jsonl","cwd":"/var/folders/ky/8mhpl8qj5ns5xlnffgzn5ssh0000gn/T/p2-cwd-6if1p0py","cliVersion":"0.140.0","source":"vscode","threadSource":null,"agentNickname":null,"agentRole":null,"gitInfo":null,"name":null,"turns":[]}}}"#;

    const LIVE_ITEM_COMPLETED: &str = r#"{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg_063ee89bd76bd6ae016a834f959adc87d19c16d9e5d4e3b9a9","text":"OK","phase":"final_answer","memoryCitation":null},"threadId":"01a010ee-c244-7341-9948-1dc7303e753b","turnId":"01a010ee-d313-7be2-a497-38ad7abee19e","completedAtMs":1786990485749}}"#;

    const LIVE_TURN_COMPLETED: &str = r#"{"method":"turn/completed","params":{"threadId":"01a010ee-c244-7341-9948-1dc7303e753b","turn":{"id":"01a010ee-d313-7be2-a497-38ad7abee19e","items":[],"itemsView":"notLoaded","status":"completed","error":null,"startedAt":1786990482,"completedAt":1786990485,"durationMs":3563}}}"#;

    // --- framing -----------------------------------------------------------

    #[test]
    fn responses_deserialize_without_a_jsonrpc_field() {
        let r: RpcResponse = serde_json::from_str(LIVE_INITIALIZE_RESPONSE).unwrap();
        assert_eq!(r.id, serde_json::json!(1));
        assert!(r.error.is_none());
        // And the round trip does not invent one.
        let back = serde_json::to_value(&r).unwrap();
        assert!(
            back.get("jsonrpc").is_none(),
            "RpcResponse must not emit a jsonrpc field"
        );
    }

    #[test]
    fn requests_carry_the_jsonrpc_version() {
        let req = RpcRequest::new(7, "turn/interrupt", serde_json::json!({}));
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 7);
        assert_eq!(v["method"], "turn/interrupt");
    }

    #[test]
    fn remote_errors_deserialize() {
        // A real 0.140.0 error frame, verbatim (Task 18).
        let raw = r#"{"id":3,"error":{"code":-32600,"message":"no active turn to steer"}}"#;
        let r: RpcResponse = serde_json::from_str(raw).unwrap();
        let e = r.error.expect("error frame");
        assert_eq!(e.code, -32600);
        assert_eq!(e.message, "no active turn to steer");
        assert!(r.result.is_none());
    }

    #[test]
    fn notification_routing_keys_are_per_method() {
        // turn/completed keys off params.threadId ...
        let n: RpcNotification = serde_json::from_str(LIVE_TURN_COMPLETED).unwrap();
        assert_eq!(n.method, "turn/completed");
        assert_eq!(n.thread_id(), Some("01a010ee-c244-7341-9948-1dc7303e753b"));

        // ... thread/started has NO top-level threadId; key = params.thread.id.
        let n: RpcNotification = serde_json::from_str(LIVE_THREAD_STARTED_NOTIFICATION).unwrap();
        assert!(n.params.get("threadId").is_none());
        assert_eq!(n.thread_id(), Some("01a010ee-c244-7341-9948-1dc7303e753b"));

        // ... and broker-scoped notifications have none at all.
        let n: RpcNotification = serde_json::from_str(
            r#"{"method":"account/rateLimits/updated","params":{"rateLimits":{}}}"#,
        )
        .unwrap();
        assert_eq!(n.thread_id(), None);
    }

    // --- live transcripts --------------------------------------------------

    #[test]
    fn live_initialize_response_deserializes() {
        let r: RpcResponse = serde_json::from_str(LIVE_INITIALIZE_RESPONSE).unwrap();
        let init: InitializeResponse = serde_json::from_value(r.result.unwrap()).unwrap();
        assert!(init.user_agent.starts_with("a2a/0.140.0"));
        assert_eq!(init.platform_family, "unix");
        assert_eq!(init.platform_os, "macos");
        assert!(init.codex_home.is_absolute());
        // The handshake carries no protocol version — hence the schema canary.
        assert!(
            !init.extra.contains_key("version") && !init.extra.contains_key("protocolVersion"),
            "unexpected version field appeared: {:?}",
            init.extra
        );
    }

    #[test]
    fn live_thread_start_response_deserializes_with_id_equal_to_session_id() {
        let r: RpcResponse = serde_json::from_str(LIVE_THREAD_START_RESPONSE).unwrap();
        let ts: ThreadStartResponse = serde_json::from_value(r.result.unwrap()).unwrap();
        assert_eq!(
            ts.thread.id, ts.thread.session_id,
            "thread.id must equal thread.sessionId — exec resume depends on it"
        );
        assert_eq!(ts.model, "gpt-5.5");
        assert_eq!(ts.model_provider, "openai");
        assert!(!ts.thread.ephemeral);
        assert!(ts.thread.path.as_ref().unwrap().ends_with(format!(
            "rollout-2026-08-17T14-13-56-{}.jsonl",
            ts.thread.id
        )));
        // The echoed sandbox is the TYPED policy object, not the sent string.
        assert_eq!(
            ts.sandbox,
            SandboxPolicy::ReadOnly {
                network_access: false
            }
        );
        // Fields the live binary returns that its own schema omits land here.
        assert!(
            ts.extra.contains_key("runtimeWorkspaceRoots"),
            "extra bag must preserve unknown siblings, got {:?}",
            ts.extra.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn live_turn_completed_deserializes() {
        let n: RpcNotification = serde_json::from_str(LIVE_TURN_COMPLETED).unwrap();
        let t: TurnNotification = serde_json::from_value(n.params).unwrap();
        assert_eq!(t.turn.status, TurnStatus::Completed);
        assert!(t.turn.status.is_terminal());
        assert!(t.turn.error.is_none());
        assert_eq!(t.turn.id, "01a010ee-d313-7be2-a497-38ad7abee19e");
        // itemsView/startedAt/durationMs are not typed; they must survive.
        assert!(t.turn.extra.contains_key("durationMs"));
    }

    #[test]
    fn live_item_completed_deserializes() {
        let n: RpcNotification = serde_json::from_str(LIVE_ITEM_COMPLETED).unwrap();
        let i: ItemCompletedNotification = serde_json::from_value(n.params).unwrap();
        assert_eq!(i.item.kind, ThreadItemKind::AgentMessage);
        assert_eq!(i.item.text(), Some("OK"));
        assert_eq!(i.thread_id, "01a010ee-c244-7341-9948-1dc7303e753b");
        assert_eq!(i.turn_id, "01a010ee-d313-7be2-a497-38ad7abee19e");
        assert_eq!(i.completed_at_ms, Some(1786990485749));
    }

    // --- open enums --------------------------------------------------------

    #[test]
    fn unknown_discriminants_deserialize_instead_of_erroring() {
        let item: ThreadItem =
            serde_json::from_value(serde_json::json!({"type":"somethingNewIn0141","id":"x"}))
                .unwrap();
        assert_eq!(item.kind, ThreadItemKind::Unknown);

        let status: TurnStatus =
            serde_json::from_value(serde_json::json!("pausedForLunch")).unwrap();
        assert_eq!(status, TurnStatus::Unknown);
        assert!(!status.is_terminal() || status == TurnStatus::Unknown);

        let sp: SandboxPolicy =
            serde_json::from_value(serde_json::json!({"type":"quantumSandbox"})).unwrap();
        assert_eq!(sp, SandboxPolicy::Unknown);

        let ui: UserInput =
            serde_json::from_value(serde_json::json!({"type":"image","url":"http://x"})).unwrap();
        assert_eq!(ui, UserInput::Unknown);
    }

    #[test]
    fn every_thread_item_discriminant_is_pinned() {
        // Exactly the camelCase set the 0.140.0 schema defines.
        for (wire, want) in [
            ("userMessage", ThreadItemKind::UserMessage),
            ("hookPrompt", ThreadItemKind::HookPrompt),
            ("agentMessage", ThreadItemKind::AgentMessage),
            ("plan", ThreadItemKind::Plan),
            ("reasoning", ThreadItemKind::Reasoning),
            ("commandExecution", ThreadItemKind::CommandExecution),
            ("fileChange", ThreadItemKind::FileChange),
            ("mcpToolCall", ThreadItemKind::McpToolCall),
            ("dynamicToolCall", ThreadItemKind::DynamicToolCall),
            ("collabAgentToolCall", ThreadItemKind::CollabAgentToolCall),
            ("subAgentActivity", ThreadItemKind::SubAgentActivity),
            ("webSearch", ThreadItemKind::WebSearch),
            ("imageView", ThreadItemKind::ImageView),
            ("imageGeneration", ThreadItemKind::ImageGeneration),
            ("enteredReviewMode", ThreadItemKind::EnteredReviewMode),
            ("exitedReviewMode", ThreadItemKind::ExitedReviewMode),
            ("contextCompaction", ThreadItemKind::ContextCompaction),
        ] {
            let got: ThreadItemKind = serde_json::from_value(serde_json::json!(wire)).unwrap();
            assert_eq!(got, want, "discriminant {wire}");
            assert_eq!(serde_json::to_value(want).unwrap(), serde_json::json!(wire));
        }
    }

    #[test]
    fn no_tool_call_discriminant_contains_the_lowercase_substring_tool() {
        // Phase 1's `contains("tool")` idiom would miss all three.
        for kind in [
            ThreadItemKind::McpToolCall,
            ThreadItemKind::DynamicToolCall,
            ThreadItemKind::CollabAgentToolCall,
        ] {
            let wire = serde_json::to_value(kind).unwrap();
            let wire = wire.as_str().unwrap();
            assert!(
                !wire.contains("tool"),
                "{wire} unexpectedly contains lowercase \"tool\""
            );
            assert!(wire.to_ascii_lowercase().contains("tool"));
            assert!(kind.is_tool_call());
        }
        assert!(!ThreadItemKind::AgentMessage.is_tool_call());
    }

    // --- sandbox policy: all four variants ---------------------------------

    #[test]
    fn sandbox_policy_round_trips_all_four_variants() {
        let cases = [
            (
                serde_json::json!({"type":"dangerFullAccess"}),
                SandboxPolicy::DangerFullAccess,
            ),
            (
                serde_json::json!({"type":"readOnly","networkAccess":true}),
                SandboxPolicy::ReadOnly {
                    network_access: true,
                },
            ),
            (
                serde_json::json!({"type":"externalSandbox","networkAccess":"enabled"}),
                SandboxPolicy::ExternalSandbox {
                    network_access: NetworkAccess::Enabled,
                },
            ),
            (
                serde_json::json!({
                    "type":"workspaceWrite",
                    "writableRoots":["/tmp/w"],
                    "networkAccess":false,
                    "excludeSlashTmp":true,
                    "excludeTmpdirEnvVar":false
                }),
                SandboxPolicy::WorkspaceWrite {
                    writable_roots: vec![PathBuf::from("/tmp/w")],
                    network_access: false,
                    exclude_slash_tmp: true,
                    exclude_tmpdir_env_var: false,
                },
            ),
        ];
        for (wire, typed) in cases {
            let got: SandboxPolicy = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(got, typed, "deserialize {wire}");
            assert_eq!(
                serde_json::to_value(&typed).unwrap(),
                wire,
                "serialize back"
            );
        }
    }

    #[test]
    fn workspace_write_uses_camel_case_not_the_snake_case_decoy() {
        // The aggregate schema's `SandboxWorkspaceWrite` (writable_roots, …) is
        // a config type. Serializing with those names would be accepted by the
        // server and silently grant no writable roots.
        let p = SandboxPolicy::WorkspaceWrite {
            writable_roots: vec![PathBuf::from("/tmp/w")],
            network_access: false,
            exclude_slash_tmp: false,
            exclude_tmpdir_env_var: false,
        };
        let v = serde_json::to_value(&p).unwrap();
        for camel in [
            "writableRoots",
            "networkAccess",
            "excludeSlashTmp",
            "excludeTmpdirEnvVar",
        ] {
            assert!(v.get(camel).is_some(), "missing camelCase key {camel}");
        }
        for snake in [
            "writable_roots",
            "network_access",
            "exclude_slash_tmp",
            "exclude_tmpdir_env_var",
        ] {
            assert!(
                v.get(snake).is_none(),
                "decoy snake_case key {snake} leaked"
            );
        }
    }

    #[test]
    fn sandbox_mode_param_is_the_kebab_case_string() {
        for (wire, mode) in [
            ("read-only", SandboxModeParam::ReadOnly),
            ("workspace-write", SandboxModeParam::WorkspaceWrite),
            ("danger-full-access", SandboxModeParam::DangerFullAccess),
        ] {
            assert_eq!(serde_json::to_value(mode).unwrap(), serde_json::json!(wire));
            let got: SandboxModeParam = serde_json::from_value(serde_json::json!(wire)).unwrap();
            assert_eq!(got, mode);
        }
    }

    // --- outbound params vs each snapshot's `required` list -----------------

    #[test]
    fn initialize_params_cover_required_and_suppress_the_delta_stream() {
        let p = InitializeParams::for_a2a("a2a", env!("CARGO_PKG_VERSION"));
        assert_covers_required(&p, "v1/InitializeParams.json");
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["clientInfo"]["name"], "a2a");
        assert_eq!(v["capabilities"]["experimentalApi"], false);
        assert_eq!(v["capabilities"]["requestAttestation"], false);
        let opts: Vec<String> =
            serde_json::from_value(v["capabilities"]["optOutNotificationMethods"].clone()).unwrap();
        assert_eq!(opts, SUPPRESSED_NOTIFICATION_METHODS.to_vec());
        assert!(opts.contains(&"item/agentMessage/delta".to_string()));
        // The voice channel is not the token stream; it is simply unused.
        assert!(!opts.iter().any(|m| m.starts_with("thread/realtime")));
    }

    #[test]
    fn thread_start_params_cover_required_and_omit_nothing_dangerous() {
        let p = ThreadStartParams {
            cwd: Some(PathBuf::from("/tmp/w")),
            sandbox: Some(SandboxModeParam::WorkspaceWrite),
            approval_policy: Some(ApprovalPolicy::Never),
            ..Default::default()
        };
        // thread/start has no required fields at all.
        assert_covers_required(&p, "v2/ThreadStartParams.json");
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["sandbox"], "workspace-write");
        assert_eq!(v["approvalPolicy"], "never");
        // Unset optionals are omitted, never sent as null — and `ephemeral`
        // must never be sent true from production code.
        assert!(v.get("model").is_none());
        assert!(v.get("ephemeral").is_none());
        // There is no writableRoots on thread/start; confinement is per-turn.
        assert!(v.get("writableRoots").is_none());
    }

    #[test]
    fn turn_start_params_cover_required() {
        let mut p = TurnStartParams::new("thread-1", vec![UserInput::text("do it")]);
        p.sandbox_policy = Some(SandboxPolicy::WorkspaceWrite {
            writable_roots: vec![PathBuf::from("/tmp/w")],
            network_access: false,
            exclude_slash_tmp: false,
            exclude_tmpdir_env_var: false,
        });
        assert_covers_required(&p, "v2/TurnStartParams.json");
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["threadId"], "thread-1");
        assert_eq!(v["input"][0]["type"], "text");
        assert_eq!(v["input"][0]["text"], "do it");
        assert_eq!(v["sandboxPolicy"]["type"], "workspaceWrite");
    }

    #[test]
    fn turn_steer_params_cover_required() {
        let p = TurnSteerParams {
            thread_id: "t".into(),
            expected_turn_id: "turn-9".into(),
            input: vec![UserInput::text("actually, stop")],
        };
        assert_covers_required(&p, "v2/TurnSteerParams.json");
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["expectedTurnId"], "turn-9");
    }

    #[test]
    fn turn_interrupt_params_cover_required() {
        let p = TurnInterruptParams {
            thread_id: "t".into(),
            turn_id: "turn-9".into(),
        };
        assert_covers_required(&p, "v2/TurnInterruptParams.json");
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["turnId"], "turn-9");
    }

    #[test]
    fn thread_resume_params_cover_required_and_carry_no_sandbox_policy() {
        let p = ThreadResumeParams {
            thread_id: "t".into(),
            cwd: Some(PathBuf::from("/tmp/w")),
            sandbox: None,
            approval_policy: None,
            model: None,
            config: None,
            developer_instructions: None,
        };
        assert_covers_required(&p, "v2/ThreadResumeParams.json");
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["threadId"], "t");
        assert!(
            v.get("sandboxPolicy").is_none(),
            "thread/resume takes no sandboxPolicy — it is re-supplied per turn"
        );
    }

    // --- server-request pin ------------------------------------------------

    #[test]
    fn server_request_methods_are_sorted_unique_and_ten() {
        let set: BTreeSet<&str> = SERVER_REQUEST_METHODS.iter().copied().collect();
        assert_eq!(set.len(), 10, "duplicate entry in SERVER_REQUEST_METHODS");
        let sorted: Vec<&str> = set.into_iter().collect();
        assert_eq!(sorted, SERVER_REQUEST_METHODS.to_vec());
    }

    /// Offline cross-check against the snapshot: every server request has a
    /// captured `*Response.json`, and the snapshot holds no response file that
    /// is not owed an answer.
    #[test]
    fn server_request_methods_map_one_to_one_onto_snapshot_responses() {
        let table = [
            (
                "account/chatgptAuthTokens/refresh",
                "ChatgptAuthTokensRefreshResponse.json",
            ),
            ("applyPatchApproval", "ApplyPatchApprovalResponse.json"),
            ("attestation/generate", "AttestationGenerateResponse.json"),
            ("execCommandApproval", "ExecCommandApprovalResponse.json"),
            (
                "item/commandExecution/requestApproval",
                "CommandExecutionRequestApprovalResponse.json",
            ),
            (
                "item/fileChange/requestApproval",
                "FileChangeRequestApprovalResponse.json",
            ),
            (
                "item/permissions/requestApproval",
                "PermissionsRequestApprovalResponse.json",
            ),
            ("item/tool/call", "DynamicToolCallResponse.json"),
            (
                "item/tool/requestUserInput",
                "ToolRequestUserInputResponse.json",
            ),
            (
                "mcpServer/elicitation/request",
                "McpServerElicitationRequestResponse.json",
            ),
        ];
        let covered: Vec<&str> = table.iter().map(|(m, _)| *m).collect();
        assert_eq!(covered, SERVER_REQUEST_METHODS.to_vec());
        for (method, file) in table {
            assert!(
                snapshot_dir().join(file).is_file(),
                "{method} has no snapshot response schema ({file})"
            );
        }
    }

    /// The four methods whose response schema has **no deny shape** must be
    /// answered with a JSON-RPC error, not a payload. Pinned here so Task 3's
    /// responder cannot quietly invent one.
    #[test]
    fn methods_without_a_deny_shape_require_required_fields_a2a_cannot_supply() {
        for (file, required_key) in [
            ("PermissionsRequestApprovalResponse.json", "permissions"),
            ("ToolRequestUserInputResponse.json", "answers"),
            ("ChatgptAuthTokensRefreshResponse.json", "accessToken"),
            ("AttestationGenerateResponse.json", "token"),
        ] {
            let req = required_of(file);
            assert!(
                req.contains(required_key),
                "{file} no longer requires {required_key}: a deny shape may now exist \
                 — revisit the responder table"
            );
        }
        // Whereas these four DO have a deny/decline payload.
        for (file, key) in [
            ("ExecCommandApprovalResponse.json", "decision"),
            ("ApplyPatchApprovalResponse.json", "decision"),
            ("CommandExecutionRequestApprovalResponse.json", "decision"),
            ("FileChangeRequestApprovalResponse.json", "decision"),
        ] {
            assert!(required_of(file).contains(key), "{file} must require {key}");
        }
        assert!(required_of("McpServerElicitationRequestResponse.json").contains("action"));
        assert!(required_of("DynamicToolCallResponse.json").contains("success"));
    }

    /// The version gate has exactly one home. A duplicate const here would rot.
    #[test]
    fn version_range_is_not_duplicated_in_this_module() {
        // Only the production half of this file — the test half below quotes
        // the range on purpose, to pin it.
        let src = include_str!("protocol.rs");
        let prod = src.split("#[cfg(test)]").next().unwrap();
        assert!(
            !prod.contains(">=0.140"),
            "version range must be read from config::DEFAULT_CODEX_VERSION_RANGE"
        );
        assert_eq!(
            crate::config::DEFAULT_CODEX_VERSION_RANGE,
            ">=0.140, <0.150"
        );
    }
}
