# A2A Phase 2: Broker + App-Server Transport — Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the live transport: a daemon-lite broker owning a private `codex app-server` child, giving workers mid-turn steering (`send` via `turn/steer`), true `interrupt`, `tail --follow`, admission control (queued workers, caps), a hardened env allowlist, and owner scoping — with the Phase 1 exec transport demoted to automatic fallback.

**Architecture:** The `a2a` binary gains a `broker run` subcommand: a tokio daemon (the ONLY async code — every other CLI command stays sync, with exactly one sanctioned exception named in Task 16) structured as an actor tree per spec §4.2: one `BrokerCore` task owns the worker registry behind a bounded mpsc; one `WorkerActor` per worker is the sole writer to that worker's directory. The broker speaks NDJSON JSON-RPC to its app-server child and versioned NDJSON to CLI clients over a 0600 unix socket. The file layer from Phase 1 is unchanged — app-server workers write the same envelopes, snapshots, and WAL through the same fold.

**Tech Stack:** Adds tokio 1 `{rt-multi-thread, process, net, io-util, sync, time, signal, macros}`, tokio-util 0.7 `{rt}` (TaskTracker needs the `rt` feature), jsonschema (dev-dep, fake-fidelity validation); nix gains the `socket` feature (LOCAL_PEERCRED). Everything else is Phase 1's stack.

**Spec:** `docs/superpowers/specs/2026-08-14-a2a-claude-codex-design.md` (v2), §9 Phase 2 scope.

**Compatible with:** devloop skill v0.1+

## Ground truth (verified live on this machine, 2026-08-17, codex-cli 0.140.0)

Every item below was probed against the real binary/schema and BINDS the plan:

- **Framing: NDJSON JSON-RPC over stdio.** Requests `{"jsonrpc":"2.0","id":N,"method":…,"params":…}`; **responses omit `jsonrpc`** (`{"id":N,"result":…}` / `{"id":N,"error":…}`); notifications have `method`+`params`, no id. A line with `id`+`method` is a server→client REQUEST. Unsolicited notifications interleave from the first line (`remoteControl/status/changed`).
- **initialize** lives in the **v1/** schema namespace (`v1/InitializeParams.json`, `v1/InitializeResponse.json` — NOT v2/, NOT root). Params: `{clientInfo:{name,version,title?}, capabilities:{experimentalApi:bool, optOutNotificationMethods:[string]|null, requestAttestation:bool}}`. Response (all required): `{userAgent, codexHome, platformFamily, platformOs}` — no version field anywhere.
- **thread/start** (all params optional): `sandbox` takes the kebab-case string enum `"read-only"|"workspace-write"|"danger-full-access"` (`SandboxMode`); also `{cwd, approvalPolicy, model, config, developerInstructions, ephemeral}`. Response result keys: `{thread, model, modelProvider, serviceTier, cwd, runtimeWorkspaceRoots, instructionSources, approvalPolicy, approvalsReviewer, sandbox, activePermissionProfile, reasoningEffort}` — the echoed `sandbox` is the TYPED `SandboxPolicy` object. **NEVER pass `ephemeral:true` in production code.** There is NO writableRoots on thread/start or thread/resume — root confinement rides the per-turn `sandboxPolicy`.
- **`SandboxPolicy` has FOUR variants** (internally tagged on `type`, camelCase): `{type:"dangerFullAccess"}` | `{type:"readOnly", networkAccess:bool}` | `{type:"externalSandbox", networkAccess:NetworkAccess}` | `{type:"workspaceWrite", writableRoots:[AbsolutePathBuf], networkAccess:bool, excludeSlashTmp:bool, excludeTmpdirEnvVar:bool}`. **DECOY WARNING:** the same schema contains an unrelated `SandboxWorkspaceWrite` definition with snake_case fields (`writable_roots`, …) — grep-and-copy from that one produces silent deserialization failure. Model the enum with a `#[serde(other)]`-style catch-all for future variants.
- **`thread.id == thread.sessionId`** (verified live, same UUID) — the app-server thread id IS the id `codex exec resume` accepts. Record both snapshot fields.
- **turn/start**: required `{threadId, input:[UserInput]}` with `UserInput::Text = {type:"text", text}`; optional `{sandboxPolicy, approvalPolicy, cwd, effort, model, outputSchema}` (outputSchema is per-turn — re-supply every turn). **turn/steer**: required `{threadId, expectedTurnId, input}`. **turn/interrupt**: required `{threadId, turnId}`. **thread/resume**: required `{threadId}` + thread/start-style optionals (again: NO sandboxPolicy — re-supply per-turn overrides on the next turn/start after any resume).
- **Server→client requests (10)** and their ONLY valid deny/decline responses (verified against the response schemas — several have NO deny variant at all):
  | method | a2a response |
  |---|---|
  | `execCommandApproval` | `{"decision":"denied"}` (ReviewDecision, snake_case family) |
  | `applyPatchApproval` | `{"decision":"denied"}` |
  | `item/commandExecution/requestApproval` | `{"decision":"decline"}` (camelCase family) |
  | `item/fileChange/requestApproval` | `{"decision":"decline"}` |
  | `mcpServer/elicitation/request` | `{"action":"decline"}` |
  | `item/tool/call` | `{"success":false,"contentItems":[{"type":"inputText","text":"dynamic tool calls unsupported"}]}` |
  | `item/permissions/requestApproval` | JSON-RPC **error** (response REQUIRES granted permissions — no deny shape exists) |
  | `item/tool/requestUserInput` | JSON-RPC **error** (response REQUIRES answers — no decline shape) |
  | `account/chatgptAuthTokens/refresh` | JSON-RPC **error** (response REQUIRES real tokens a2a does not have; erroring lets the server run its own refresh) |
  | `attestation/generate` | JSON-RPC **error** (response REQUIRES a token; unreachable anyway — a2a never sets `requestAttestation:true`) |
  No approval response carries a `reason` field — human-readable reasons live in the worker's event log, never on the wire. Unknown method → JSON-RPC error within 100ms (a dropped request is a permanent hang).
- **Token-delta suppression:** the high-volume notifications are `item/agentMessage/delta`, `item/reasoning/textDelta`, `item/reasoning/summaryTextDelta`, `item/reasoning/summaryPartAdded`, `item/commandExecution/outputDelta`, `item/fileChange/outputDelta`, `item/plan/delta`, `item/mcpToolCall/progress`, `process/outputDelta`, `command/exec/outputDelta`, `turn/diff/updated` — THIS is the `optOutNotificationMethods` list (exact strings). `thread/realtime/*` is the voice channel; opt out of it too, but it is NOT the token stream.
- **Notification routing keys are per-method:** `thread/started` params are `{thread:{id,…}}` (NO top-level threadId — key = `params.thread.id`); `turn/started`/`turn/completed` = `{threadId, turn}`; `item/started`/`item/completed` = `{threadId, turnId, item}`; `error` = `{threadId, turnId, error, willRetry}` (**willRetry is required** — transient vs terminal); `thread/tokenUsage/updated`, `model/rerouted`, `guardianWarning`, `thread/status/changed` carry `threadId`; `account/rateLimits/updated` (`{rateLimits}`), `deprecationNotice`, `configWarning` carry NO threadId — they are broker-scoped.
- **ThreadItem discriminants** (camelCase; exec uses snake_case — map BOTH): `userMessage, hookPrompt, agentMessage, plan, reasoning, commandExecution, fileChange, mcpToolCall, dynamicToolCall, collabAgentToolCall, subAgentActivity, webSearch, imageView, imageGeneration, enteredReviewMode, exitedReviewMode, contextCompaction`. NOTE: none of the three tool variants contains the lowercase substring "tool" — Phase 1's `contains("tool")` idiom must become case-insensitive.
- `TurnStatus ∈ {completed, interrupted, failed, inProgress}`. `Turn = {id, items, status, error?, …}`.
- **`thread/loaded/list` exists** and answered `{data:[], nextCursor:null}` live — confirmed as the liveness-probe request.
- Schema regen: per-message files are byte-stable across runs; the aggregate `codex_app_server_protocol.v2.schemas.json` is NOT (definition-map ordering churns) — never snapshot the aggregate. `ClientRequest.json`/`ServerNotification.json` are ~150 KB each and change on ANY protocol addition — snapshot their extracted METHOD SETS, not bytes.

## Global Constraints (in addition to Phase 1's, which all still bind)

- **The CLI stays synchronous** except: `a2a broker run` (the daemon) and the ONE sanctioned exception — `adapter::handshake_probe` (Task 16) may build a `tokio::runtime::Builder::new_current_thread()` and block on it.
- **Actor discipline:** `BrokerCore` exclusively owns the registry behind a bounded mpsc with oneshot replies; one `WorkerActor` per worker is the sole writer to its dir; `Arc<Mutex`/`Arc<RwLock` forbidden in `src/broker/` + `src/rpc.rs` (source-scan-tested; a plain `Arc<rpc::Client>` with no interior lock is fine).
- **Cancellation safety:** all child-stdin/socket-write/event-append writes go through dedicated writer tasks fed by mpsc; `write_all` never inside `select!`; no `std::thread::sleep` in async modules (all source-scan-tested, Task 10).
- **Bounded channels only**; per-worker mailboxes bounded at **1024** (spec §4.5); `broadcast` only for tail subscribers.
- **Env allowlist (spec §4.2), scoped precisely:** it applies to the **app-server child and to workers** — NEVER to the broker daemon process itself (the daemon needs `A2A_HOME`, `A2A_CODEX_BIN`, `A2A_ALLOWED_ROOTS`, lane env, etc.). Child env = `{PATH (pinned at broker start), HOME, USER, TERM, LANG, TMPDIR, CODEX_HOME}` ∪ explicitly-passed pairs, with the scrub list (`OPENAI_API_KEY`, `AWS_*`, `DEPOT_TOKEN`) always winning. **Test seam:** the broker forwards every env var matching `FAKE_*` or `A2A_TEST_*` from its own environment into the child (how the fake fixture is driven); production spawns pass zero extra pairs.
- Broker files: `broker.sock` (0600), `broker.lock` (flock held for daemon lifetime), **`broker.pid`** (0600 — the daemon's own pid, required for crash tests and ops), `broker.log`, `broker-health.json`, `broker.jsonl`. Socket accepts peer-uid == own uid only.
- All security-relevant thread/turn params passed explicitly per spawn; the broker **re-validates** wire-supplied cwd against the allowed roots it was started with (defense in depth — the socket must not out-privilege the CLI, spec §5).
- **Exit-code discipline:** exit **7** (`BrokerUnavailable`) for anything caused by broker/handshake reachability or version skew; exit 9 (`Precondition`) only for static preconditions (unknown flag value, missing binary, config errors). The Phase-1 exit table is unchanged.
- Tests never touch `~/.a2a`/`~/.codex`; temp `A2A_HOME` everywhere; **`tests/common::a2a()` sets `A2A_BROKER_DISABLE=1`** so every Phase-1 test keeps the pure exec path (no socket syscalls); broker tests opt in via `common::a2a_broker()` which returns a `BrokerGuard` (Drop = `broker shutdown` + bounded wait for the socket to vanish). Real-codex work only in Task 18 (`A2A_E2E=1`) — with ONE carve-out: Task 1's schema canary invokes the real codex for a zero-token, no-auth schema dump under a temp `CODEX_HOME` (never a stub: the canary deliberately ignores `A2A_CODEX_BIN`).
- The FSM (`src/fsm.rs`) is not modified. **`src/fold.rs` IS modified** (genesis learns `queued`/`owner`/`thread_id`/requested-transport keys — Tasks 11/14/15); `docs/machines/worker.md` gets one Notes-column amendment (Task 12). App-server workers reuse existing rows: `ProcessStarted` = transport attached (shared child's pid recorded for observability, but **kill/timeout paths NEVER group-kill when `transport == AppServer`** — they route through the broker; the two Phase-1 call sites in `commands/kill.rs` and `reconcile::perform` are guarded in Task 11/12).
- **Deliberate deviations from the spec, recorded:** interrupt-after-completion returns exit 0 with `data.noop:true` (spec wanted a dedicated code; the pinned exit table has no free slot worth burning — revisit if callers need it). `SHELL` is NOT in the child allowlist (spec omits it; the plan matches the spec). Rate-limit-headroom-fed admission: deferred to Phase 5 (notifications are recorded now). Guardian-denial parking + `a2a approve`: deferred to Phase 3 (Phase 2 records `guardianWarning` as a lifecycle `OutboxNotice` envelope, no state change). Exec-spawn slots counting against the broker cap: deferred to Phase 3.

**Test-specification policy (unchanged from v1 of this plan):** tasks specify tests by precise acceptance criteria + Interfaces contracts rather than verbatim bodies. Every exact value an AC names is a pin. When an AC is silent, match the Phase-1 codebase's conventions (read the neighboring tests). Async ACs are ALWAYS converged-state assertions: poll with an explicit bounded timeout; `tokio::time::pause` ONLY in current_thread-flavor tests with no real child I/O.

## File Structure (additions)

```
src/
  protocol.rs            # typed v1/v2 subset (serde) — incl. 4-variant SandboxPolicy
  rpc.rs                 # NDJSON JSON-RPC codec: correlation, notification stream, writer task
  responder.rs           # ServerRequest responder (exact per-method payload table)
  adapter.rs             # lifecycle calls + AdapterRef + handshake_probe
  broker/
    mod.rs core.rs worker_actor.rs child.rs sockserv.rs wire.rs shutdown.rs
  brokerclient.rs        # sync CLI client: connect/autostart/handshake/request/subscribe
  commands/ broker_cmd.rs interrupt.rs   (+ edits to spawn/send/status/ls/tail/kill/resume/gc/doctor)
schema/
  v2-snapshot/           # checked-in subset: v1/Initialize*, v2/*, *ApprovalResponse.json, method-sets + REGEN.md
tests/
  fixtures/fake-app-server   # python3 scripted app-server (scenario-driven, $FAKE_STATE-persistent)
  protocol_canary.rs rpc_fake.rs broker_lifecycle.rs cmd_broker.rs cmd_interrupt.rs
  cmd_follow.rs admission.rs owner_scope.rs e2e_appserver.rs
```

---

### Task 1: Protocol types + checked-in schema snapshot + drift canary

**Acceptance Criteria:**
- `schema/v2-snapshot/` contains, copied from a fresh local regen: `v1/InitializeParams.json`, `v1/InitializeResponse.json`; `v2/ThreadStartParams.json`, `v2/ThreadStartResponse.json`, `v2/TurnStartParams.json`, `v2/TurnStartResponse.json`, `v2/TurnSteerParams.json`, `v2/TurnInterruptParams.json`, `v2/ThreadResumeParams.json`, `v2/TurnCompletedNotification.json`, `v2/ItemCompletedNotification.json`, `v2/ThreadStartedNotification.json`, `v2/TurnStartedNotification.json`; root-level `ExecCommandApprovalResponse.json`, `ApplyPatchApprovalResponse.json`, `CommandExecutionRequestApprovalResponse.json`, `FileChangeRequestApprovalResponse.json`, `PermissionsRequestApprovalResponse.json`, `ToolRequestUserInputResponse.json`, `McpServerElicitationRequestResponse.json` (or the actual generated name — REGEN.md records it), `DynamicToolCallResponse.json`, `ChatgptAuthTokensRefreshResponse.json`, `AttestationGenerateResponse.json`; plus `client_request_methods.txt` and `server_notification_methods.txt` (sorted extracted method names) and `REGEN.md` (regen command, subset list, and the two rules: never snapshot the aggregate file — its definition ordering churns; never byte-snapshot ClientRequest/ServerNotification — ~150 KB and any protocol addition would permanently redden the gate).
- `cargo test --test protocol_canary`: resolves codex from **PATH only** (deliberately ignoring `A2A_CODEX_BIN` — this is a protocol gate, not a worker path; documented in the test), SKIPs with an eprintln when absent; otherwise regenerates into a tempdir **with `CODEX_HOME` set to a tempdir** (never touches `~/.codex`), byte-diffs each per-message snapshot file, and set-compares the two method lists. Zero tokens, no auth (verified). Drift fails with the offending filenames.
- `src/protocol.rs` types the subset per the Ground-truth section — including the **four-variant `SandboxPolicy`** (camelCase, tagged on `type`, catch-all variant) with a doc-comment warning off the snake_case `SandboxWorkspaceWrite` decoy — plus `RpcRequest`/`RpcResponse` (no jsonrpc field)/`RpcNotification`, `UserInput::Text`, `TurnStatus`. Unit tests deserialize the REAL captured transcripts (embedded consts: the live initialize response, the live thread/start response with `thread.id == sessionId`, a turn/completed, an item/completed) and serialize params matching each snapshot's `required` list.
- `SERVER_REQUEST_METHODS: [&str; 10]` pinned and cross-checked against the snapshot. Version-range checks continue to read `config::DEFAULT_CODEX_VERSION_RANGE` (no duplicate const).

**Verify via:** cli
**Apply:** docs/development/appendix-api-design.md — versioned contracts; the snapshot canary replaces the version handshake the protocol does not offer.

**Files:** Create `src/protocol.rs`, `schema/v2-snapshot/**`; Modify `src/lib.rs`; Test `tests/protocol_canary.rs` + unit tests.

- [x] Steps: capture snapshot → failing canary+unit tests → RED → implement → GREEN → full suite → commit `feat: typed app-server protocol subset with schema snapshot and drift canary`.

---

### Task 2: NDJSON JSON-RPC client + the fake app-server fixture

**Acceptance Criteria:**
- `tests/fixtures/fake-app-server` (python3, executable) speaks the verified framing and persists per-run state under `$FAKE_STATE` (a dir; thread-map + launch counter survive process restarts). Scenarios via `FAKE_SCENARIO`: `happy` (thread lifecycle: thread/started → turn/started → item/completed{agentMessage} → turn/completed), `blocked` (**records each thread's cwd at thread/start; writes `<cwd>/outbox/1.question.json` with `{"seq":1,"question":"A or B?","blocking":true}` before turn/completed** — the broker passes the WORKER DIR as thread/start `cwd`, which is the per-thread channel; env can't be per-worker on a shared child), `steer-race` (first turn/steer → error `{"code":-32002,"message":"expectedTurnId does not match the active turn"}`; a subsequent turn/start succeeds), `hang-turn` (turn/started then holds the turn OPEN until it receives a steer or interrupt — never completes on its own), `server-request` (emits `execCommandApproval` mid-turn; completes only after a response arrives), `unknown-request` (made-up server→client request method), `crash-mid-turn` (crashes after turn/started **on its FIRST launch only** — `$FAKE_STATE` counter; subsequent launches behave like `happy` and answer `thread/resume`), `always-crash` (exits right after initialize, every launch), `resume-fail` (`thread/resume` returns a remote error), `bad-vocabulary` (unknown notification method). ALL scenarios answer `thread/resume` (echoing the persisted thread) unless stated; all honor `FAKE_DUMP_ENV` (write env to the named file) and write full transcripts to `$FAKE_TRANSCRIPT`.
- **Fidelity contract (spec §8.3):** a test helper validates every notification the fake emitted (from its transcript) against the corresponding snapshot schema, and every request the adapter sent against the corresponding Params schema, using the `jsonschema` dev-dependency — a drifted fake is a test failure.
- `rpc::Client` per the Interfaces block: correlation by id, notifications channel, server-request channel (frames with id+method), dedicated stdin writer task, per-request timeout, `ChildDied` on EOF. Unparseable lines → synthetic `__unparseable` notification (never dropped).
- Round-trip tests: lifecycle ordering; concurrent in-flight correlation; `crash-mid-turn` resolves ChildDied (no hang).
- Cargo.toml pins: tokio features as in Tech Stack, `tokio-util = {version="0.7", features=["rt"]}`, `jsonschema` as dev-dep. (`tests/cli_contract.rs::toolchain_is_pinned` greps version prefixes only — feature edits are safe.)

**Verify via:** cli
**Apply:** docs/development/rust-async-cancellation-graceful-shutdown.md §2; docs/development/appendix-frp-rust.md — bounded channels.

**Files:** Create `src/rpc.rs`, `tests/fixtures/fake-app-server`; Modify `src/lib.rs`, `Cargo.toml`; Test `tests/rpc_fake.rs`.

**Interfaces:**
```rust
pub struct Client { /* writer mpsc + correlation task */ }
pub struct Incoming { pub notifications: mpsc::Receiver<protocol::RpcNotification>,
                      pub server_requests: mpsc::Receiver<ServerRequestFrame> }
pub struct ServerRequestFrame { pub id: serde_json::Value, pub method: String, pub params: serde_json::Value }
impl Client {
    /// env: allowlist ∪ extra pairs (extra = FAKE_*/A2A_TEST_* passthrough or test-explicit); scrub always wins.
    pub async fn spawn(bin: &Path, args: &[&str], extra_env: &[(String,String)], stderr_log: &Path)
        -> io::Result<(Client, Incoming, tokio::process::Child)>;
    pub async fn call<P: Serialize, R: DeserializeOwned>(&self, method: &str, params: &P, timeout: Duration)
        -> Result<R, RpcError>;   // RpcError::{Timeout, ChildDied, Remote{code: i64, message: String}, Decode}
    pub async fn respond(&self, id: serde_json::Value, result: Result<serde_json::Value, (i64, String)>) -> io::Result<()>;
}
```

- [x] Steps: fixture → failing tests → RED → implement → GREEN → full suite → commit `feat: JSON-RPC client + scripted fake app-server with fidelity validation`.

---

### Task 3: ServerRequest responder — exact payload table

**Acceptance Criteria:**
- `responder::respond_to` answers each of the 10 methods with EXACTLY the payloads in the Ground-truth table (deny decisions per response-schema family; JSON-RPC errors where no deny shape exists — including `account/chatgptAuthTokens/refresh`, which is ERRORED so the server refreshes itself, never "allowed"). Unknown method → JSON-RPC error within 100ms. A unit test validates every canned payload against its snapshot `*Response.json` schema (jsonschema dev-dep).
- `EventKind::ServerRequest` is added HERE (with `src/events.rs` in this task's Files); `respond_to` returns `{method, disposition}` for the caller to log as a lifecycle envelope — the routing to worker streams is Task 7's demux.
- Fake-driven: `server-request` scenario's turn completes only after the deny; `unknown-request` gets an error and the connection stays usable.

**Verify via:** cli

**Files:** Create `src/responder.rs`; Modify `src/lib.rs`, `src/events.rs`; Test unit + `tests/rpc_fake.rs` (extend).

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: server-request responder with schema-exact deny payloads`.

---

### Task 4: Adapter — lifecycle calls, AdapterRef, steer decision table

**Acceptance Criteria:**
- `adapter::initialize`: clientInfo `{name:"a2a", version: CARGO_PKG_VERSION}`, capabilities `{experimentalApi:true, optOutNotificationMethods: [the ELEVEN delta/progress methods from Ground truth + the thread/realtime/* set]}`; returns `InitInfo{user_agent, codex_home}`.
- `adapter::thread_start(lane, cwd, worker_dir)`: **`cwd` = the WORKER DIR for outbox-only lanes / the target cwd for workspace-write lanes**; sandbox = the kebab-case SandboxMode string; approvalPolicy `"never"`; model from lane; ephemeral never. Returns `ThreadInfo{thread_id, session_id, resolved_sandbox}`.
- `adapter::turn_start`: input `[{type:"text",text}]`, per-turn `sandboxPolicy` (the FOUR-variant object — workspaceWrite with `writableRoots:[outbox]` for outbox-only lanes; camelCase fields), effort, optional outputSchema (re-supplied EVERY turn). `turn_steer`, `turn_interrupt`, `thread_resume` typed per Ground truth.
- `adapter::send_message` decision table: steer with last turn id → on `RpcError::Remote` with code `-32002` OR message containing `expectedTurnId` → retry as turn/start (Task 18 validates the real trigger and adjusts the match — the match predicate is a single function, `is_steer_precondition_failure`). Any OTHER remote error surfaces unchanged. Pinned against `steer-race`.
- `#[derive(Clone)] pub struct AdapterRef(Arc<rpc::Client>)` defined HERE with the methods the WorkerActor calls (`send_message`, `turn_interrupt`, `turn_start`); note: `Arc<Client>` without interior locks passes the source-scan.
- `adapter::handshake_probe(bin, timeout) -> Result<InitInfo, RpcError>`: the ONE sanctioned sync-CLI tokio exception (current_thread runtime, child killed on expiry) — consumed by doctor (Task 16) and transport selection (Task 11).

**Verify via:** cli

**Files:** Create `src/adapter.rs`; Modify `src/lib.rs`; Test `tests/rpc_fake.rs` (extend).

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: app-server adapter (typed lifecycle, steer table, AdapterRef, handshake probe)`.

---

### Task 5: Notification→envelope mapping + result synthesis

**Acceptance Criteria:**
- `events::map_appserver_notification(method, params) -> Option<(EventClass, EventKind)>` with the PINNED table: `thread/started`→(Lifecycle, SessionIdentified); `turn/started`→TurnStarted; `turn/completed`→TurnCompleted; `item/completed` unwrapped by `item.type` accepting BOTH spellings — `agentMessage|agent_message`→(Lifecycle, AgentMessage); `commandExecution|command_execution`→(Trace, CommandExec); `mcpToolCall|dynamicToolCall|collabAgentToolCall|mcp_tool_call|…`→(Trace, ToolCall) via CASE-INSENSITIVE matching (the camelCase variants do not contain lowercase "tool"); `item/started` mirrors exec semantics (agentMessage→None; other types → same as completed); `error` **branches on required `willRetry`**: true→(Trace, Error) (does NOT drive the worker), false→(Lifecycle, Error); `thread/tokenUsage/updated`→(Trace, TokenUsage); `model/rerouted`→(Lifecycle, ModelRerouted); `guardianWarning`→(Lifecycle, OutboxNotice) (recorded; parking + `a2a approve` deferred to Phase 3); every delta/progress method → None DEFENSIVELY via a case-insensitive `delta|progress` guard (belt-and-braces under the optOut); unknown → (Trace, Unknown). **Broker-scoped methods (`account/rateLimits/updated`, `deprecationNotice`, `configWarning`) are NOT mapped here** — the demux (Task 7) routes them to broker.jsonl/broker-health.
- New `EventKind` variants: `TokenUsage`, `ModelRerouted`, `TransportDegraded` (consumed by Task 11) — open-enum compatible (Phase-1 binary reads them as Unknown; pinned by test).
- **Result synthesis:** `pub fn synthesize_result(turn_item_events: &[Envelope]) -> Option<String>` extracts the final AgentMessage text — the WorkerActor writes it to `result.md` at turn completion (this transport has no `--output-last-message`). The schema gate runs on it BEFORE any TurnEnded commit (ownership pinned in Task 6 — done is absorbing).
- AC: a 200-delta fake flood produces ZERO trace envelopes on disk (deltas suppressed at both layers).

**Verify via:** cli

**Files:** Modify `src/events.rs`, `src/lib.rs`; Test unit (embedded real payloads incl. a willRetry pair).

- [x] Steps: failing tests → implement → GREEN → full suite (Phase-1 exec-mapping tests untouched) → commit `feat: app-server notification mapping (willRetry, tool camelCase, delta guards) + result synthesis`.

---

### Task 6: WorkerActor — sole writer done right

**Acceptance Criteria:**
- A `WorkerActor` per worker: bounded mpsc(**1024**); sole writer to its dir; every persistence closure **acquires `reconcile::lock_state(dir)` and re-opens `EventLog` inside `spawn_blocking`** (never caches `next_seq` across awaits) — this is what makes "sole writer" true alongside Phase-1 CLI paths (which still take the same lock).
- Persistence paths pinned: lifecycle envelopes with FSM payloads go through a NEW `fold::append_envelope_and_fold(log, dir, snap, &mut env, now)` (appends a caller-built envelope of ANY class and folds it; `append_and_fold` refactored to delegate); pure trace envelopes go through `EventLog::append` directly (never drive the FSM). Trace backpressure: drop-oldest + counter + synthetic `events_dropped` envelope (flood test).
- **Turn conclusion is extracted, not duplicated:** `src/reconcile.rs` gains `pub fn conclude_turn(log, dir, snap, cfg, now, has_result, has_question) -> Result<WorkerSnapshot, AppError>` — the lifted Phase-1 block (deliver-at-boundary → **schema gate** → result rotation → ResultWritten → TurnEnded commit), with exec-only liveness inputs left at reconcile's call site; `schema_gate`/`Gate`/`has_result` made pub(crate) as needed. The WorkerActor calls it with `has_question = reconcile::has_unanswered_question(dir, snap.last_delivered_seq)` (**the existing two-arg fn made pub — NOT renamed, NOT reduced to one arg**: the seq watermark is load-bearing; an answered question must not re-park the worker — parity AC: answer → next turn → done on both transports).
- Event-order pin: on thread-start success the actor synchronously WAL-appends `{"fsm":"ProcessStarted", pid: <shared child pid>, start_witness: <witness>}` BEFORE forwarding any notification; SessionIdentified emission is gated on `state == Running` (mirroring reconcile.rs's exec guard) — `fold_rejections` stays empty across a full happy path (pinned).
- Broker-side deadlines: armed from lane config, fired as `Timeout{kind}` through the WAL; `blocked_timeout` → Escalated becomes reachable live.
- `WorkerMsg` per the Interfaces block. **Adapter rebind is NOT a message:** the actor holds a `tokio::sync::watch::Receiver<adapter::AdapterRef>` and reads the current value before every Deliver/Interrupt — supervision (Task 7) swaps the watch after a child restart, so post-restart Delivers never write to a dead stdin.
- The actor emits **`Settled{id, state}`** on its `settle_tx` channel on every transition into a terminal state or `escalated` (`Settled` is defined HERE in worker_actor.rs — Task 7's `CoreMsg::WorkerSettled` wraps this channel; admission drain and idle-shutdown liveness both key off it — Tasks 7/10/14). This avoids a forward dependency on Task 7's CoreMsg enum.

**Verify via:** cli
**Apply:** docs/development/appendix-actor-model.md §2/§8.

**Files:** Create `src/broker/worker_actor.rs`, `src/broker/mod.rs`; Modify `src/lib.rs`, `src/fold.rs` (append_envelope_and_fold), `src/reconcile.rs` (conclude_turn extraction, has_unanswered_question pub); Test unit in worker_actor.rs + Phase-1 reconcile tests as the parity gate.

**Interfaces:**
```rust
pub enum WorkerMsg {
    Envelope(events::Envelope),
    ServerRequestLogged { method: String, disposition: String },
    Deliver { text: String, reply: oneshot::Sender<Result<adapter::SendPath, AppError>> },
    Interrupt { reply: oneshot::Sender<Result<InterruptOutcome, AppError>> }, // Interrupted | NoopTurnAlreadyOver
    Kill { reply: oneshot::Sender<Result<(), AppError>> },   // app-server kill = turn_interrupt + Cancelled, NEVER process kill
    Snapshot { reply: oneshot::Sender<WorkerSnapshot> },
    Shutdown,
}
pub struct Settled { pub id: String, pub state: WorkerState }
pub fn spawn_worker_actor(dir: PathBuf, snap: WorkerSnapshot,
    adapter_rx: tokio::sync::watch::Receiver<adapter::AdapterRef>,
    settle_tx: mpsc::Sender<Settled>, cancel: CancellationToken, tracker: &TaskTracker) -> WorkerHandle;
```

- [x] Steps: failing tests → implement (incl. the reconcile extraction, keeping ALL Phase-1 tests green) → GREEN → full suite → commit `feat: WorkerActor (locked sole-writer persistence, conclude_turn parity, watch-rebind)`.

---

### Task 7: BrokerCore + child supervision

**Acceptance Criteria:**
- `BrokerCore` owns the registry behind bounded mpsc `CoreMsg`s: `Spawn/Send/Interrupt/Kill/Resume/List/Status/Subscribe/ConnClosed/ChildDied/IdleCheck/WorkerSettled` — no shared-state maps (source-scan).
- **Demux with per-method keys** (the Ground-truth routing table): `thread/started` keyed by `params.thread.id`; turn/item/error/tokenUsage/rerouted/guardian by `params.threadId`; broker-scoped methods (`account/rateLimits/updated`, `deprecationNotice`, `configWarning`) → `broker.jsonl` + surfaced in broker-health (doctor reads them); unroutable frames → `broker.jsonl` (never dropped). Server-request frames → `responder::respond_to` → `ServerRequestLogged` on the owning worker (by threadId when present, else broker.jsonl).
- Supervision: RestartPolicy{3 in 60s, exp 500ms→30s + jitter}; child death (stdout EOF) → restart → re-initialize → for each live worker `thread/resume` + `transport_restarted` envelope + **adapter watch-channel swap** + undelivered-inbox re-delivery; resume errors → Orphaned. **Post-restart turns re-supply full per-turn overrides (sandboxPolicy/writableRoots, approvalPolicy, model, effort, outputSchema) — thread/resume alone reverts to mode defaults, silently widening the sandbox (fake transcript asserts the post-restart turn/start carries sandboxPolicy).** Budget exceeded → `degraded: exec` latch in broker-health + 5-min re-promotion probe. Tests: `crash-mid-turn` (crashes first launch only) → re-attach + a post-restart `send` SUCCEEDS (proves the rebind); `always-crash` → latch after 3 with backoff; `resume-fail` → Orphaned branch.
- Liveness probe: `thread/loaded/list` (verified real) on a config-injectable interval (`A2A_TEST_PROBE_MS` env for tests; default 30s); two consecutive timeouts = child death. Backoff/probe SCHEDULING is unit-tested pure (current_thread + `tokio::time::pause` — no child I/O in those tests); the real restart path is integration-tested with real time + short intervals.
- **Single-flight auth guard (spec §4.2):** a broker-held `tokio::sync::Mutex` (inside one task — not shared state) serializes the first turn-start after child (re)start, so N workers can't race a token refresh; `error` notifications whose message matches auth-expiry map to `failed{auth_expired}` (resumable, never auto-retried).
- Panic isolation: JoinSet::is_panic → that worker `failed{internal_panic}`, siblings unaffected.

**Verify via:** cli
**Apply:** docs/development/appendix-actor-model.md §3.

**Files:** Create `src/broker/core.rs`, `src/broker/child.rs`; Modify `src/broker/mod.rs`, `src/lib.rs`; Test `tests/broker_lifecycle.rs`.

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: BrokerCore + supervised child (per-method demux, re-attach with override re-supply, auth single-flight)`.

---

### Task 8: Unix socket server + wire protocol + sync client

**Acceptance Criteria:**
- One frame shape only: `wire::Frame{v:1, id, type:"hello"|"req"|"resp"|"event"|"err", body}` — hello is a frame like any other (`body:{protocol_version:1, binary_version}` both ways); ONE version integer (`protocol_version`) keys the skew check.
- **Skew policy per spec §4.2, both branches:** on mismatch the broker's hello-err carries its live-worker count; the sync client DRAINS-AND-RESPAWNS the broker when the count is 0 (shutdown → autostart), and exits **7** `BROKER_VERSION_SKEW` naming both binary versions with remedy `a2a broker restart` when workers are live. Both branches tested (a test-only `A2A_TEST_PROTO_VERSION` override fakes the mismatch).
- Socket: 0600 in the 0700 home; peer-uid check via nix `socket` feature (`LOCAL_PEERCRED`/XuCred on macOS, cfg-gated with an `SO_PEERCRED` arm for linux); 1 MiB frame cap → typed err + close; per-connection reader/writer task pair; one subscription per connection; lagged subscriber → `err{code:"LAGGED", last_seq}`.
- `brokerclient.rs` (sync): `connect(home)`; `autostart(home)` resolves the binary via **`A2A_BROKER_BIN` → `A2A_SHIM_BIN` → `exec::shim_bin()`** (NEVER bare current_exe — under cargo test that's the harness) and execs `<a2a> --home <home> broker run` **re-exporting `A2A_CODEX_BIN`, `A2A_CODEX_VERSION_RANGE`, `A2A_ALLOWED_ROOTS`, `A2A_LANES_*`, `A2A_ADMISSION_*`, `A2A_TEST_*`, `FAKE_*`, `A2A_SHIM_BIN` into the daemon env** (the daemon is NOT under the child allowlist — Global Constraints); bounded retry-connect 20×100ms; `request`, `subscribe`.
- `tests/common/mod.rs` gains `a2a_broker(home, root) -> (Command, BrokerGuard)` — same env as `a2a()` MINUS `A2A_BROKER_DISABLE`, plus the fake-app-server as `A2A_CODEX_BIN`; `BrokerGuard::drop` = `broker shutdown` + bounded wait (10s) for socket removal. An autostarted broker in a test NEVER creates `$HOME/.a2a` (pinned by test).

**Verify via:** cli

**Files:** Create `src/broker/sockserv.rs`, `src/broker/wire.rs`, `src/brokerclient.rs`; Modify `src/broker/mod.rs`, `src/lib.rs`, `Cargo.toml` (nix `socket` feature), `tests/common/mod.rs`; Test `tests/broker_lifecycle.rs` (extend).

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: broker socket (one-frame wire, two-branch skew policy) + sync client with safe autostart`.

---

### Task 9: Daemon-lite: singleton, detachment, `a2a broker` subcommands

**Acceptance Criteria:**
- **flock BEFORE fork** (pinned order): the foreground process acquires `broker.lock` first — on contention it prints `a2a: broker already running` to stderr and exits 0 (observable) — then double-forks/setsids keeping the lock fd open; stdio → `broker.log`; SIGHUP ignored; **`broker.pid` written (0600) after daemonization** (the crash tests and ops need the daemon's own pid; `broker status` and broker-health report it). Stale socket unlinked only after a failed connect. `--foreground` skips daemonization.
- 20 concurrent cold-start `broker run`: converge (poll ≤10s at 100ms) to exactly one socket, one lock holder, one child, nineteen clean "already running" exits.
- `a2a broker status` (exit 0 / 7 when down) prints uptime, transport mode (app-server|degraded-exec), broker pid, child pid + restart count, worker count, client count; **`a2a status --broker` is an alias** (keeps the spec §4.1 surface honest). `broker shutdown` graceful + bounded wait; `broker restart` = shutdown→run.
- Startup reconciliation: **reap first, adopt second** (spec §4.2): walk `workers/*/state.json`; for previous-instance app-server workers verify recorded pid+witness and reap survivors (SIGTERM→grace→SIGKILL of the previous child) BEFORE re-adopting via `thread/resume`; adoption set = running/awaiting-input **and orphaned** app-server workers (Orphaned+Resume row exists — Task 17's crash test pins the round trip); `queued` workers are RE-ENQUEUED in created_at order; exec workers untouched.
- Env allowlist assertions: `FAKE_DUMP_ENV` proves the child lacks `OPENAI_API_KEY`/`AWS_*`/`DEPOT_TOKEN` and keeps `CODEX_HOME`/`PATH`; the daemon itself keeps its `A2A_*` env.

**Verify via:** cli

**Files:** Create `src/commands/broker_cmd.rs`; Modify `src/broker/mod.rs`, `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`, `src/commands/status.rs` (--broker alias); Test `tests/cmd_broker.rs`.

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: daemon-lite broker (flock-first singleton, pidfile, reap-then-adopt startup)`.

---

### Task 10: Graceful + idle shutdown, source-scan gates

**Acceptance Criteria:**
- Three-phase shutdown (detect: ctrl_c/SIGTERM/idle/internal in one select of cancel-safe branches; tell: root CancellationToken + child_token per worker; wait: TaskTracker close + 10s grace + forced exit). Teardown order observable: stop accepting → workers flush to terminal/orphaned snapshots → child SIGTERM→SIGKILL → socket unlinked → flock LAST. SIGTERM test with 2 live fake workers: both snapshots orphaned within grace, no surviving processes (scoped to the test's own pids — never a global ps scan), socket+lock+pid gone, tail subscribers got `broker_shutdown` frames.
- Idle: `[broker] idle_timeout_secs` config key (**default 300** — the leaked-daemon safety net), surfaced in `config show --origin` as `broker.idle_timeout_secs`; "live" = any non-terminal worker (incl. awaiting-input) OR any exec worker OR any subscriber; test with 1s timeout: empty registry → self-exit; one awaiting-input worker → no exit.
- Source-scan gates over `src/broker/` + `src/rpc.rs`: no `Arc<Mutex`/`Arc<RwLock`; no `write_all` inside `select!` blocks; no `std::thread::sleep`.

**Verify via:** cli
**Apply:** docs/development/rust-async-cancellation-graceful-shutdown.md §5/§6/§10.

**Files:** Create `src/broker/shutdown.rs`; Modify `src/broker/mod.rs`, `src/broker/core.rs`, `src/config.rs` (idle key); Test `tests/broker_lifecycle.rs` + scans in `tests/cmd_broker.rs`.

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: three-phase shutdown, idle exit (default 300s), cancellation source gates`.

---

### Task 11: `a2a spawn` via the broker — transport auto-selection

**Acceptance Criteria:**
- `--transport auto` (default): **when `A2A_BROKER_DISABLE=1`, no socket syscall happens — pure Phase-1 exec path** (this is what keeps the entire Phase-1 suite green; `tests/common::a2a()` sets it — pinned). Otherwise connect-or-autostart → hello → Spawn; broker validates the wire cwd against ITS OWN allowed roots (canonicalized, defense in depth — raw-socket test: out-of-roots cwd refused `CWD_NOT_ALLOWED`, no dir created) and re-runs the research-scratch rule; CLI-side §5 gates (brief scan, context staging, elevation) run BEFORE the frame is sent, exactly as Phase 1.
- Success: `state.json` has `transport:"app-server"`, `thread_id` == `session_id` (**`src/state.rs` gains `pub thread_id: Option<String>` + `pub queue_position: Option<u64>`, both `#[serde(default)]`**), resolved sandbox JSON in `extra`, and the genesis event carries `owner`/`thread-of-record` — **`fold::seed_from_genesis` learns `owner`, `thread_id`, and separate `requested_transport` keys so a rebuild-from-zero is lossless** (AC: delete state.json → `a2a status` → owner+thread_id survive). Transport-selection bookkeeping: `requested_transport` = the explicit value when given, else the resolved one; `extra["transport_selection"]="auto"|"explicit"`; fallback emits a `TransportDegraded` lifecycle envelope (kind from Task 5).
- Broker-down behavior: `--transport auto` falls back to exec with the degraded envelope; `--transport app-server` exits **7 `BROKER_UNAVAILABLE`** (reachability — NOT 9; the Phase-1 static `TRANSPORT_UNAVAILABLE`/9 remains only for a transport this build cannot offer at all, i.e. unknown transport strings).
- **Codex drift gate (spec §4.2):** the broker records the resolved codex path+version at start; every Spawn re-checks and refuses on drift with remedy `a2a broker restart` (typed error; existing workers unaffected).
- Reconcile guard: for `transport == AppServer`, the CLI reconcile path is **READ-ONLY** (no save_snapshot, no arm_deadline, no death/timeout detection — the broker owns transitions), EXCEPT the broker-dead case: connect fails AND state ∈ {running, awaiting-input, escalated} → CLI commits `OrphanDetected` under state.lock with a freshly-opened EventLog (spawned/queued app-server workers with a dead broker: spawned → `ProcessExited{None}`→failed{never_started}; queued → left queued for the next broker's re-enqueue — pinned; `fold_rejections` stays empty in all cases). **The two Phase-1 kill paths are guarded here:** `commands/kill.rs` and `reconcile::perform` never call `exec::kill_worker` when `transport == AppServer` (kill routes via the broker; with the broker dead, kill marks cancelled without signaling — the shared child must never be group-killed by worker-scoped commands).
- Full lifecycle on the fake: spawn → wait 0 → status done → result (synthesized) → blocked scenario parks awaiting-input (fake writes the question file into the worker-dir cwd) → `wait --until blocked` exit 5.

**Verify via:** cli

**Files:** Modify `src/commands/spawn.rs`, `src/commands/status.rs`, `src/commands/ls.rs`, `src/commands/kill.rs`, `src/reconcile.rs`, `src/state.rs`, `src/fold.rs`, `src/events.rs` (TransportDegraded already from T5 — wire only), `src/broker/core.rs`, `tests/common/mod.rs` (A2A_BROKER_DISABLE default); Test `tests/cmd_broker.rs` (extend) + FULL Phase-1 regression.

- [x] Steps: failing tests → implement → GREEN (incl. every Phase-1 test) → commit `feat: spawn via broker (auto-selection, lossless genesis, guarded kill paths, drift gate)`.

---

### Task 12: Live steering — `a2a send` mid-turn + `a2a interrupt`

**Acceptance Criteria:**
- **Convergence gate pinned for both commands:** tests use `FAKE_SCENARIO=hang-turn` (holds the turn open until steered/interrupted) and poll `a2a --json tail <id>` for a `turn_started` envelope (≤10s at 50ms) BEFORE issuing send/interrupt — never an instant assertion on the async turn state.
- `send` on a RUNNING app-server worker delivers mid-turn (steer path; the event records `send_path`); on awaiting-input starts the answer turn; on exec workers the Phase-1 queue path is regression-pinned. `--queue-only` unchanged.
- `a2a interrupt <id>`: running → `turn/interrupt` → `awaiting-input{interrupted}` exit 0; after completion → exit 0 `data.noop:true` (recorded deviation); exec transport → exit 9 `TRANSPORT_UNSUPPORTED` (static); broker down → exit 7.
- `docs/machines/worker.md` Notes column for `running+send` amended: "the FSM emits no action; the app-server transport additionally steers the live turn broker-side (§4.3)". (`step()` itself unchanged; `tests/fsm_table.rs` untouched.)

**Verify via:** cli

**Files:** Create `src/commands/interrupt.rs`; Modify `src/commands/send.rs`, `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`, `src/broker/{core,worker_actor}.rs`, `docs/machines/worker.md`; Test `tests/cmd_interrupt.rs` + send cases in `tests/cmd_broker.rs`.

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: mid-turn send and a2a interrupt (converged gates, doc amendment)`.

---

### Task 13: `a2a tail --follow`

**Acceptance Criteria:**
- `--follow` (new flags declared in **`src/cli.rs` + `src/main.rs`** — listed): replay from log, then live frames, same line contract; **bounded exits pinned:** `--timeout <dur>` (reuses `wait::parse_duration`) AND automatic exit 0 when the worker reaches a terminal state — no unbounded-blocking test can exist; every follow test uses `std::process::Command` with piped stdout + a reader thread + `--timeout` (never a bare assert_cmd `.assert()`, pinned in the AC).
- Broker shutdown mid-follow → stderr notice, exit 0. LAGGED → transparent re-read from last seq, resume (flood test: no gaps, no duplicates). Exec workers → 500ms poll fallback with the same flags and exit conditions. A follow client keeps the broker from idle-exiting (Task 10 extension).

**Verify via:** cli

**Files:** Modify `src/commands/tail.rs`, `src/cli.rs`, `src/main.rs`, `src/brokerclient.rs`, `src/broker/sockserv.rs`; Test `tests/cmd_follow.rs`.

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: a2a tail --follow (bounded, lag-recovering, transport-spanning)`.

---

### Task 14: Admission control — `queued`, caps, `--no-queue`

**Acceptance Criteria:**
- Config layout PINNED: `[admission] global_cap = 8`, `queue_cap = 32`, `[admission.lanes.<lane>] cap = 4`; origins `admission.global_cap`, `admission.queue_cap`, `admission.lanes.impl.cap`; env `A2A_ADMISSION_GLOBAL_CAP` / `A2A_ADMISSION_QUEUE_CAP` / `A2A_ADMISSION_LANES_IMPL_CAP`. `LANE_FIELDS` unchanged (the layering test's key set is stable).
- At cap, Spawn creates the worker dir + genesis **carrying `"queued": true`** — **`fold::seed_from_genesis` seeds `WorkerState::Queued` on that marker** (a fold change, NOT an FSM change: `Queued+Admit→Spawned` already exists; `docs/machines/worker.md` already draws the queued genesis arrow). CLI prints `data.queue_position` (new snapshot field from T11). Drain: on `CoreMsg::WorkerSettled` (terminal or escalated), BrokerCore admits the FIFO head (`Admit` → thread-start flow). Cap-1/spawn-2 test: second queues at position 1, auto-admits after the first settles, completes.
- Queue full → exit 9 `QUEUE_FULL` with `retry_after_secs` — **carried via a new `AppError.data: Option<Value>` serialized into the error envelope** (`src/output.rs` in Files). `--no-queue` → exit 9 `AT_CAPACITY`. **Resume at cap → exit 9 `AT_CAPACITY`** (the FSM is frozen — no terminal→queued row exists; pinned rather than left open). `kill` on queued → cancelled, never admits. Broker-death with queued workers: they persist and the next broker re-enqueues (T9).
- Exec spawns do NOT count against the cap (deferred to Phase 3 — release accounting needs work the plan defers; the broker-down uncapped case makes partial counting incoherent).

**Verify via:** cli

**Files:** Modify `src/config.rs`, `src/broker/core.rs`, `src/commands/spawn.rs`, `src/commands/ls.rs`, `src/fold.rs`, `src/output.rs`, `src/cli.rs`, `src/main.rs` (--no-queue exists from Phase 1 — verify wiring); Test `tests/admission.rs`.

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: admission control (queued genesis, FIFO drain on settle, pinned config surface)`.

---

### Task 15: Owner scoping — `A2A_OWNER`, `ls --all`, cross-owner guards

**Acceptance Criteria:**
- `spawn` records owner (`--owner` > `A2A_OWNER` > `anon-<uid>`) — **in the genesis payload, read by `seed_from_genesis`** (rebuild-lossless, AC: delete state.json → owner survives). `ls` defaults to own + **legacy-unowned** workers; `--all` shows everything with an OWNER column. **`owner: None` (every pre-Phase-15 worker) matches ANY caller** — no lockouts, pinned by a null-owner mutation test.
- `send`/`kill`/`interrupt`/`resume` cross-owner → exit 9 `NOT_OWNER` unless `--force`; `gc` scopes to own+unowned, `gc --all` for everything; read commands stay open.
- New flags declared in `src/cli.rs` + `src/main.rs` (listed); `tests/common` sets a fixed `A2A_OWNER=test-owner` so Phase-1 tests are deterministic.

**Verify via:** cli

**Files:** Modify `src/commands/{spawn,ls,send,kill,interrupt,resume,gc}.rs`, `src/fold.rs`, `src/cli.rs`, `src/main.rs`, `tests/common/mod.rs`; Test `tests/owner_scope.rs`.

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: owner scoping (lossless genesis owner, legacy-unowned open, force guards)`.

---

### Task 16: Doctor phase-2 checks

**Acceptance Criteria:**
- Doctor grows from six to **ten** checks (data-driven append): `app_server_handshake` (via `adapter::handshake_probe` with a **2s budget, child killed on expiry** — warn on failure when codex is present, since exec still works), `codex_home_match` (**warn "not checked — no handshake" whenever the probe produced no codexHome — NEVER fail on absence**; fail only on a real mismatch of two known values — this keeps every stub-codex doctor test at exit 0), `broker_status` (ok up-to-date / ok down-but-startable / warn degraded latch / warn skew), `auth_mode` (spec §4.2: warn when `OPENAI_API_KEY` is exported or codex auth.json is absent — static sync checks only).
- The two Phase-1 count assertions (unit test `…exactly_six…` and the integration six-check test) are UPDATED to ten and renamed (pinned — do not try to keep six). Phase-1 check ids/order unchanged.

**Verify via:** cli

**Files:** Modify `src/commands/doctor.rs`; Test `tests/cmd_doctor.rs` (extend, update counts).

- [x] Steps: failing tests → implement → GREEN → full suite → commit `feat: doctor handshake/codex-home/broker/auth-mode checks (ten total)`.

---

### Task 17: E2E broker pipelines + crash recovery + parity

**Acceptance Criteria:**
- Broker happy pipeline (fake): spawn auto→app-server → wait 0 → status done → result → tail kinds parity with the exec pipeline (pinned list) → gc tombstone carries thread/session id. Blocked pipeline: park → answer → wait 0.
- **Crash recovery, no escape hatches:** kill -9 the broker (pid from `broker.pid`) mid-run → `a2a status` commits orphaned → `broker run` → startup adoption (T9 covers orphaned) → converged poll pins the worker BACK IN `running` → completes. Exec-fallback pipeline (A2A_BROKER_DISABLE) green.
- FULL `cargo test`: all Phase-1 + Phase-2, zero warnings, stray-process checks scoped to test-created pids only, every broker torn down by its BrokerGuard.

**Verify via:** cli

**Files:** Create `tests/e2e_appserver.rs`; Test itself + full suite.

- [x] Steps: failing tests → fixes only where pipelines expose them → GREEN → commit `feat: broker e2e pipelines with pinned crash recovery`.

---

### Task 18: Gated real-codex smoke + cross-transport resume proof + README

**Acceptance Criteria (`A2A_E2E=1`; reads real ~/.codex auth, writes nothing under it):**
- Real smoke: temp home, real codex, `spawn --transport app-server --lane general --brief "Reply with exactly: A2A-P2-OK"` → wait 0 → result contains it; real `thread_id`==`session_id`; `broker status` healthy; drift gate asserts the load-bearing mappings fired; vocabulary fixes to `map_appserver_notification` AND the steer-precondition matcher (`is_steer_precondition_failure` — validate the real error code/message) are in-scope, observed values transcribed in comments.
- **Cross-transport resume proof:** app-server worker completes → `broker shutdown` → `a2a resume <id> "now reply with exactly: A2A-XT-OK"` with `A2A_BROKER_DISABLE=1` → runs via `codex exec resume <session_id>` → wait 0 → result contains it. On identity failure: document the divergence + record `rollout_path` fallback as the Phase-3 fix — never paper over.
- Real interrupt: long-turn brief; interrupt lands `awaiting-input{interrupted}` (≤3 attempts, timing-tolerant).
- README: transports, broker subcommands, interrupt/follow/admission/owner; Phase-2 smoke invocation. Install refresh + real `a2a doctor`: handshake ok, codex_home_match ok, ten checks.

**Verify via:** cli

**Files:** Extend `tests/e2e_appserver.rs`; Modify `src/events.rs`/`src/adapter.rs` (real-vocabulary fixes), `README.md`.

- [x] Steps: gated cases → real run → fix drift → GREEN → README → install + doctor → commit `feat: real app-server smoke, cross-transport resume proof, Phase 2 docs`.

---

## Plan self-review notes (kept for the executor)

- **Verification provenance:** this v2 incorporates ~65 findings from three adversarial reviewers (protocol ground truth verified against codex-cli 0.140.0 schemas + live probes; codebase consistency verified against HEAD 56c0e0b; devloop executability). The Ground-truth section is empirical, not aspirational — trust it over intuition, especially the four-variant SandboxPolicy, the per-method deny payloads, the v1/ initialize location, and the delta-method optOut list.
- **Spec coverage:** T1 canary+types (§4.3), T2 codec+fake+fidelity (§4.2, §8.3), T3 responder (§4.3), T4 adapter+probe (§4.3), T5 mapping+synthesis+willRetry (§4.3, §4.5), T6 sole-writer actor+conclude_turn+deadlines (§4.2, §4.5, §4.6), T7 core+supervision+auth-single-flight+override-re-supply (§4.2), T8 socket+skew-both-branches (§4.2, §5), T9 daemon+pidfile+reap-then-adopt+alias (§4.1, §4.2), T10 shutdown+idle-default+scans (§4.2), T11 auto-selection+lossless-genesis+guarded-kills+drift-gate+wire-revalidation (§4.4, §4.2, §5), T12 steer+interrupt (§4.1, §4.3), T13 follow (§4.2, §4.8), T14 admission (§4.9), T15 owner (§4.8), T16 doctor (§4.1, §4.2), T17 e2e+crash (§8), T18 real proof (§4.4, §8).
- **Deliberate deviations & deferrals** are consolidated in the Global Constraints block — none are silent.
- **Devloop execution notes:** toolkit `cli` throughout; fresh `A2A_HOME` + `A2A_OWNER` per test; `A2A_BROKER_DISABLE=1` is the Phase-1-compatibility default in `tests/common::a2a()` from Task 11 on; broker tests use `a2a_broker()`+BrokerGuard; `tokio::time::pause` only in current_thread tests without child I/O; real codex only in T1's zero-token canary and T18's gated smoke.