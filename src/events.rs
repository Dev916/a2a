//! Transport-normalized event envelope (spec §4.5) plus the `codex exec --json`
//! mapping table.
//!
//! Forward compatibility is the whole point of this module: an older binary
//! must be able to read a newer binary's `events.jsonl` without choking. So
//! every enum here is *open* (`#[serde(other)] Unknown`) and the struct keeps
//! unrecognized sibling fields in `extra` instead of rejecting them.

use serde::{Deserialize, Serialize};

/// Envelope schema version. Bump only on a breaking envelope change.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Envelope {
    pub schema_version: u32,
    /// Stamped by `EventLog::append`; 0 until the event is durably logged.
    pub seq: u64,
    pub ts: chrono::DateTime<chrono::Utc>,
    pub epoch: u32,
    pub class: EventClass,
    pub kind: EventKind,
    pub transport: Transport,
    /// The untouched transport payload this envelope was derived from.
    #[serde(default)]
    pub raw: serde_json::Value,
    /// Fields written by a newer binary that this one does not understand.
    /// Preserved verbatim so a read/write round trip is lossless.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// `lifecycle` events drive the FSM; `trace` events are observability only.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum EventClass {
    Lifecycle,
    Trace,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Spawned,
    SessionIdentified,
    TurnStarted,
    AgentMessage,
    ToolCall,
    CommandExec,
    /// One server→client request (an approval prompt, an elicitation, a dynamic
    /// tool call) was answered by `crate::responder`. `raw` is the
    /// `{method, disposition}` pair — never the request's params, which can
    /// carry a command line.
    ServerRequest,
    Blocked,
    OutboxNotice,
    /// A batch of `inbox/` messages was handed to the worker; carries the new
    /// `last_delivered_seq` watermark, so a snapshot rebuilt from the WAL never
    /// re-delivers what was already acknowledged.
    InboxDelivered,
    StateChange,
    EventsDropped,
    TransportRestarted,
    /// `thread/tokenUsage/updated` — the running token counters for the thread.
    /// Trace-only: it feeds `state.json` counters and (Phase 5) admission
    /// control, and never moves the FSM.
    TokenUsage,
    /// `model/rerouted` — the server switched the model mid-turn. Recorded so a
    /// worker's evidence stays attributable to the model that actually ran.
    ModelRerouted,
    /// `--transport auto` fell back from app-server to exec. Emitted by the
    /// spawn path, not by the notification table.
    TransportDegraded,
    Timeout,
    Interrupted,
    Killed,
    Orphaned,
    Resumed,
    Error,
    TurnCompleted,
    ResultWritten,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum Transport {
    AppServer,
    Exec,
    #[serde(other)]
    Unknown,
}

impl Envelope {
    pub fn new(
        epoch: u32,
        transport: Transport,
        class: EventClass,
        kind: EventKind,
        raw: serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            seq: 0, // stamped by EventLog::append
            ts: now,
            epoch,
            class,
            kind,
            transport,
            raw,
            extra: serde_json::Map::new(),
        }
    }

    /// A minimal lifecycle envelope for tests and for events that have no
    /// transport payload behind them (epoch 1, exec transport, now).
    pub fn synthetic(kind: EventKind) -> Self {
        Self::new(
            1,
            Transport::Exec,
            EventClass::Lifecycle,
            kind,
            serde_json::Value::Object(serde_json::Map::new()),
            chrono::Utc::now(),
        )
    }
}

/// Maps one raw `codex exec --json` line by its `"type"` field.
/// `None` = drop the line (token-level deltas, spec §4.5).
///
/// ## THE MAPPING TABLE
///
/// This match is the single edit point for the exec vocabulary. Task 20's live
/// `codex exec --json` run corrects it against the real emitted types; nothing
/// else in the codebase should branch on raw exec type strings.
pub fn map_exec_line(raw: &serde_json::Value) -> Option<(EventClass, EventKind)> {
    let ty = raw.get("type").and_then(|v| v.as_str()).unwrap_or("");

    // Token-level deltas are dropped before anything else so that e.g.
    // `agent_message_delta` never reaches the `agent_message` arm.
    if ty.contains("delta") {
        return None;
    }

    // ── `item.*` envelopes ────────────────────────────────────────────────
    //
    // Observed live against **codex-cli 0.140.0** (2026-08-16, by this repo's
    // `tests/e2e_exec.rs::real_codex_smoke`). The real vocabulary of a turn is:
    //
    //   {"type":"thread.started","thread_id":"01a0…"}
    //   {"type":"turn.started"}
    //   {"type":"item.started",  "item":{"type":"command_execution", …}}
    //   {"type":"item.completed","item":{"type":"command_execution","exit_code":0, …}}
    //   {"type":"item.completed","item":{"type":"agent_message","text":"DONE"}}
    //   {"type":"turn.completed","usage":{…}}
    //
    // i.e. everything a worker actually *says or does* is wrapped, and the real
    // subject is the nested `item.type`. Dispatching on the outer type alone
    // filed a worker's own answer as `trace/unknown`, which made `a2a tail`
    // (lifecycle-only by default) silent about the one thing worth reading.
    if ty.starts_with("item.") {
        let item_ty = raw
            .get("item")
            .and_then(|i| i.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return Some(match item_ty {
            // Only the *completed* form carries the final text; the in-flight
            // forms are the streaming shape and are dropped like any delta.
            "agent_message" | "assistant_message" if ty == "item.completed" => {
                (EventClass::Lifecycle, EventKind::AgentMessage)
            }
            "agent_message" | "assistant_message" => return None,
            "command_execution" => (EventClass::Trace, EventKind::CommandExec),
            t if t.contains("tool") => (EventClass::Trace, EventKind::ToolCall),
            // `reasoning`, `file_change`, `web_search`, `todo_list`, … — pure
            // observability, and deliberately open: a new item type in a newer
            // codex is not an error, it is a trace line we do not name yet.
            _ => (EventClass::Trace, EventKind::Unknown),
        });
    }

    Some(match ty {
        "session.created" | "session_configured" | "thread.started" => {
            (EventClass::Lifecycle, EventKind::SessionIdentified)
        }
        "agent_message" => (EventClass::Lifecycle, EventKind::AgentMessage),
        // `turn.started`/`turn.completed` are the 0.140 spellings (observed);
        // `task_*` are the older ones, kept so an older codex still reads.
        "turn.started" | "task_started" => (EventClass::Lifecycle, EventKind::TurnStarted),
        "turn.completed" | "task_complete" => (EventClass::Lifecycle, EventKind::TurnCompleted),
        // `turn.failed` is the documented sibling of `turn.completed`; the
        // happy-path smoke never provokes it, so it is mapped by name only.
        "error" | "turn.failed" => (EventClass::Lifecycle, EventKind::Error),
        t if t.starts_with("exec_command") || t.starts_with("command") => {
            (EventClass::Trace, EventKind::CommandExec)
        }
        t if t.contains("tool") => (EventClass::Trace, EventKind::ToolCall),
        _ => (EventClass::Trace, EventKind::Unknown),
    })
}

/// Notification methods that belong to the **broker**, not to any worker: they
/// carry no `threadId`, so there is no worker dir to append them to.
/// [`map_appserver_notification`] deliberately has no arm for them — the
/// broker's demux routes them to `broker.jsonl` / `broker-health.json`.
pub const BROKER_SCOPED_METHODS: [&str; 3] = [
    "account/rateLimits/updated",
    "deprecationNotice",
    "configWarning",
];

/// Maps one app-server **notification** to an envelope class+kind.
/// `None` = drop it (spec §4.5).
///
/// ## THE APP-SERVER MAPPING TABLE
///
/// The sibling of [`map_exec_line`], and the only place in the codebase that
/// branches on raw app-server notification method strings.
///
/// Three things here are load-bearing and easy to get wrong:
///
/// * **`error` branches on `willRetry`** (a *required* field of
///   `v2/ErrorNotification`). `willRetry:true` means the server is about to try
///   again on its own — recording it as a lifecycle `Error` would drive the
///   worker off a cliff mid-retry, so it is trace-only. `false` is terminal and
///   does drive the worker. An absent/non-boolean value is treated as terminal:
///   parking a worker is recoverable, swallowing a fatal error is not.
/// * **Tool items are matched case-insensitively.** None of the three camelCase
///   tool discriminants (`mcpToolCall`, `dynamicToolCall`,
///   `collabAgentToolCall`) contains the lowercase substring `"tool"`, so Phase
///   1's `contains("tool")` idiom silently files every tool call as unknown
///   here. Item types are normalized (lowercased, `_` stripped) so the
///   camelCase and snake_case spellings of the same concept collapse onto one
///   arm — see [`crate::protocol::ThreadItemKind`].
/// * **The delta/progress guard is second-layer.** `a2a` already opts out of
///   the eleven high-volume methods at initialize
///   ([`crate::protocol::SUPPRESSED_NOTIFICATION_METHODS`]); this guard means a
///   server that ignores that list still cannot flood `events.jsonl`.
///
/// Unknown methods are `(Trace, Unknown)`: a notification from a newer codex is
/// a trace line this binary does not name yet, never a hard failure.
pub fn map_appserver_notification(
    method: &str,
    params: &serde_json::Value,
) -> Option<(EventClass, EventKind)> {
    // Belt-and-braces: dropped before anything else, so a re-spelled delta
    // method can never reach an arm below. The opt-out list is folded in so the
    // two layers cannot disagree — `item/reasoning/summaryPartAdded` and
    // `turn/diff/updated` are high-volume streams whose names carry neither
    // word.
    let lower = method.to_ascii_lowercase();
    if lower.contains("delta")
        || lower.contains("progress")
        || crate::protocol::SUPPRESSED_NOTIFICATION_METHODS.contains(&method)
    {
        return None;
    }

    Some(match method {
        // `thread/started` params are `{thread:{id,…}}` — no top-level threadId.
        "thread/started" => (EventClass::Lifecycle, EventKind::SessionIdentified),
        "turn/started" => (EventClass::Lifecycle, EventKind::TurnStarted),
        "turn/completed" => (EventClass::Lifecycle, EventKind::TurnCompleted),

        // Everything a worker says or does arrives wrapped: the real subject is
        // the nested `item.type` (exactly as on exec).
        "item/completed" | "item/started" => {
            let item_ty = params
                .get("item")
                .and_then(|i| i.get("type"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            // `mcpToolCall` and `mcp_tool_call` both normalize to `mcptoolcall`.
            let norm: String = item_ty
                .chars()
                .filter(|c| *c != '_')
                .map(|c| c.to_ascii_lowercase())
                .collect();
            match norm.as_str() {
                // Only the completed form carries the final text; the in-flight
                // form is the streaming shape and is dropped like any delta.
                "agentmessage" if method == "item/completed" => {
                    (EventClass::Lifecycle, EventKind::AgentMessage)
                }
                "agentmessage" => return None,
                "commandexecution" => (EventClass::Trace, EventKind::CommandExec),
                t if t.contains("tool") => (EventClass::Trace, EventKind::ToolCall),
                // `reasoning`, `fileChange`, `webSearch`, `contextCompaction`, …
                _ => (EventClass::Trace, EventKind::Unknown),
            }
        }

        "error" => {
            let will_retry = params
                .get("willRetry")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if will_retry {
                (EventClass::Trace, EventKind::Error)
            } else {
                (EventClass::Lifecycle, EventKind::Error)
            }
        }

        "thread/tokenUsage/updated" => (EventClass::Trace, EventKind::TokenUsage),
        "model/rerouted" => (EventClass::Lifecycle, EventKind::ModelRerouted),
        // Recorded only. Guardian-denial parking and `a2a approve` are Phase 3.
        "guardianWarning" => (EventClass::Lifecycle, EventKind::OutboxNotice),

        _ => (EventClass::Trace, EventKind::Unknown),
    })
}

/// The text of the turn's final agent message, i.e. what the app-server
/// transport writes to `result.md`.
///
/// The exec transport gets `result.md` for free from `--output-last-message`;
/// the app-server has no such flag, so the result is synthesized from the
/// turn's own item envelopes. `None` means "this turn produced no result" —
/// the same condition `reconcile::has_result` reports for an empty
/// `result.md`, which routes the worker to `awaiting-input` rather than to a
/// silently successful `done`.
///
/// The last agent message wins (a turn may narrate before answering), and a
/// blank one is not a result. Text is returned verbatim, never trimmed: it
/// becomes the file the schema gate reads.
pub fn synthesize_result(turn_item_events: &[Envelope]) -> Option<String> {
    turn_item_events
        .iter()
        .rev()
        .filter(|e| e.kind == EventKind::AgentMessage)
        .find_map(|e| agent_message_text(&e.raw))
}

/// The `text` of an agent-message payload, in the three shapes a caller can
/// legitimately have stored in `raw`: the notification params
/// (`{item:{text}}`), the bare item (`{text}`), and the exec line
/// (`{message}`). Blank text counts as absent.
fn agent_message_text(raw: &serde_json::Value) -> Option<String> {
    [
        raw.get("item").and_then(|i| i.get("text")),
        raw.get("text"),
        raw.get("message"),
    ]
    .into_iter()
    .flatten()
    .filter_map(|v| v.as_str())
    .find(|s| !s.trim().is_empty())
    .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_kind_roundtrips_as_unknown() {
        let line = r#"{"schema_version":1,"seq":9,"ts":"2026-08-14T00:00:00Z","epoch":1,
                       "class":"lifecycle","kind":"kind_from_the_future","transport":"exec","raw":{}}"#;
        let e: Envelope = serde_json::from_str(line).unwrap();
        assert!(matches!(e.kind, EventKind::Unknown));
        assert_eq!(e.seq, 9);
    }

    #[test]
    fn exec_mapping_covers_session_message_delta_unknown() {
        assert_eq!(
            map_exec_line(&json!({"type":"session.created","session_id":"s1"})),
            Some((EventClass::Lifecycle, EventKind::SessionIdentified))
        );
        assert_eq!(
            map_exec_line(&json!({"type":"agent_message","message":"hi"})),
            Some((EventClass::Lifecycle, EventKind::AgentMessage))
        );
        assert_eq!(map_exec_line(&json!({"type":"agent_message_delta","delta":"h"})), None);
        assert_eq!(
            map_exec_line(&json!({"type":"totally_new_thing"})),
            Some((EventClass::Trace, EventKind::Unknown))
        );
    }

    #[test]
    fn envelope_serializes_every_contract_field() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-14T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let e = Envelope::new(
            2,
            Transport::Exec,
            EventClass::Lifecycle,
            EventKind::AgentMessage,
            json!({"type":"agent_message","message":"hi"}),
            now,
        );
        let v = serde_json::to_value(&e).unwrap();
        for k in [
            "schema_version",
            "seq",
            "ts",
            "epoch",
            "class",
            "kind",
            "transport",
            "raw",
        ] {
            assert!(v.get(k).is_some(), "missing field {k}");
        }
        assert_eq!(v["schema_version"], json!(1));
        assert_eq!(v["seq"], json!(0)); // stamped by EventLog::append
        assert_eq!(v["ts"], json!("2026-08-14T00:00:00Z"));
        assert_eq!(v["epoch"], json!(2));
        assert_eq!(v["class"], json!("lifecycle"));
        assert_eq!(v["kind"], json!("agent_message"));
        assert_eq!(v["transport"], json!("exec"));
        assert_eq!(v["raw"]["message"], json!("hi"));
    }

    #[test]
    fn open_enums_never_fail_to_parse_and_extra_fields_survive() {
        let line = r#"{"schema_version":7,"seq":3,"ts":"2026-08-14T00:00:00Z","epoch":1,
                       "class":"class_from_the_future","kind":"spawned",
                       "transport":"carrier-pigeon","raw":{},"future_field":42}"#;
        let e: Envelope = serde_json::from_str(line).unwrap();
        assert_eq!(e.class, EventClass::Unknown);
        assert_eq!(e.kind, EventKind::Spawned);
        assert_eq!(e.transport, Transport::Unknown);
        assert_eq!(e.extra.get("future_field"), Some(&serde_json::json!(42)));

        // A line with no `raw` at all still parses (serde default).
        let bare = r#"{"schema_version":1,"seq":1,"ts":"2026-08-14T00:00:00Z","epoch":1,
                       "class":"trace","kind":"tool_call","transport":"app-server"}"#;
        let b: Envelope = serde_json::from_str(bare).unwrap();
        assert_eq!(b.transport, Transport::AppServer);
        assert_eq!(b.kind, EventKind::ToolCall);
    }

    /// The kind Task 3's responder logs. Pinned because it crosses the file
    /// layer: an older binary reading a newer `events.jsonl` must still see the
    /// same string.
    #[test]
    fn server_request_is_a_lifecycle_kind_spelled_snake_case() {
        assert_eq!(
            serde_json::to_value(EventKind::ServerRequest).unwrap(),
            json!("server_request")
        );
        let back: EventKind = serde_json::from_value(json!("server_request")).unwrap();
        assert_eq!(back, EventKind::ServerRequest);

        let e = Envelope::new(
            1,
            Transport::AppServer,
            EventClass::Lifecycle,
            EventKind::ServerRequest,
            json!({"method": "execCommandApproval", "disposition": "denied"}),
            chrono::Utc::now(),
        );
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["kind"], json!("server_request"));
        assert_eq!(v["class"], json!("lifecycle"));
        assert_eq!(v["transport"], json!("app-server"));
        assert_eq!(v["raw"]["disposition"], json!("denied"));
    }

    #[test]
    fn synthetic_is_epoch_one_exec_lifecycle() {
        let e = Envelope::synthetic(EventKind::Killed);
        assert_eq!(e.schema_version, 1);
        assert_eq!(e.seq, 0);
        assert_eq!(e.epoch, 1);
        assert_eq!(e.transport, Transport::Exec);
        assert_eq!(e.class, EventClass::Lifecycle);
        assert_eq!(e.kind, EventKind::Killed);
    }

    #[test]
    fn exec_mapping_table_full_vocabulary() {
        // deltas are dropped no matter what else the type says
        for t in ["agent_message_delta", "response.output_text.delta", "delta"] {
            assert_eq!(map_exec_line(&json!({ "type": t })), None, "{t} should drop");
        }
        // session identification, all three known spellings
        for t in ["session.created", "session_configured", "thread.started"] {
            assert_eq!(
                map_exec_line(&json!({ "type": t })),
                Some((EventClass::Lifecycle, EventKind::SessionIdentified)),
                "{t}"
            );
        }
        for t in ["turn.started", "task_started"] {
            assert_eq!(
                map_exec_line(&json!({ "type": t })),
                Some((EventClass::Lifecycle, EventKind::TurnStarted)),
                "{t}"
            );
        }
        for t in ["turn.completed", "task_complete"] {
            assert_eq!(
                map_exec_line(&json!({ "type": t })),
                Some((EventClass::Lifecycle, EventKind::TurnCompleted)),
                "{t}"
            );
        }
        for t in ["exec_command_begin", "exec_command_end", "command_start"] {
            assert_eq!(
                map_exec_line(&json!({ "type": t })),
                Some((EventClass::Trace, EventKind::CommandExec)),
                "{t}"
            );
        }
        for t in ["mcp_tool_call_begin", "tool_result"] {
            assert_eq!(
                map_exec_line(&json!({ "type": t })),
                Some((EventClass::Trace, EventKind::ToolCall)),
                "{t}"
            );
        }
        for t in ["error", "turn.failed"] {
            assert_eq!(
                map_exec_line(&json!({ "type": t, "message": "boom" })),
                Some((EventClass::Lifecycle, EventKind::Error)),
                "{t}"
            );
        }
        // a valid JSON line with no `type` at all is still trace/unknown, never a panic
        assert_eq!(
            map_exec_line(&json!({"no_type_here":true})),
            Some((EventClass::Trace, EventKind::Unknown))
        );
    }

    // ── app-server notification mapping (Task 5) ─────────────────────────
    //
    // Every payload below is transcribed from codex-cli 0.140.0: either from
    // the live fake's transcripts (`tests/fixtures/fake-app-server`) or from
    // that release's own `generate-json-schema` output (v2/ErrorNotification,
    // v2/ThreadTokenUsageUpdatedNotification, v2/ModelReroutedNotification,
    // v2/GuardianWarningNotification), so the field spellings here are the
    // wire's, not this repo's guesses.

    #[test]
    fn appserver_lifecycle_methods_map_to_the_pinned_kinds() {
        // `thread/started` has NO top-level threadId — the key is thread.id.
        assert_eq!(
            map_appserver_notification(
                "thread/started",
                &json!({"thread":{"id":"019898f0-0000-7000-8000-000000000001",
                                  "sessionId":"019898f0-0000-7000-8000-000000000001"}})
            ),
            Some((EventClass::Lifecycle, EventKind::SessionIdentified))
        );
        assert_eq!(
            map_appserver_notification(
                "turn/started",
                &json!({"threadId":"t1","turn":{"id":"turn_1","items":[],"status":"inProgress"}})
            ),
            Some((EventClass::Lifecycle, EventKind::TurnStarted))
        );
        assert_eq!(
            map_appserver_notification(
                "turn/completed",
                &json!({"threadId":"t1","turn":{"id":"turn_1","items":[],"status":"completed"}})
            ),
            Some((EventClass::Lifecycle, EventKind::TurnCompleted))
        );
    }

    #[test]
    fn item_completed_is_unwrapped_by_item_type_in_both_spellings() {
        // The exact payload the app-server emits for a worker's own answer.
        let real = json!({
            "item":{"type":"agentMessage","id":"msg_0f1e","text":"DONE",
                    "phase":"final_answer","memoryCitation":null},
            "threadId":"t1","turnId":"turn_1","completedAtMs":1_760_000_000_000i64
        });
        assert_eq!(
            map_appserver_notification("item/completed", &real),
            Some((EventClass::Lifecycle, EventKind::AgentMessage))
        );
        for spelling in ["agentMessage", "agent_message"] {
            assert_eq!(
                map_appserver_notification(
                    "item/completed",
                    &json!({"threadId":"t1","turnId":"u1","item":{"type":spelling,"text":"hi"}})
                ),
                Some((EventClass::Lifecycle, EventKind::AgentMessage)),
                "{spelling}"
            );
        }
        for spelling in ["commandExecution", "command_execution"] {
            assert_eq!(
                map_appserver_notification(
                    "item/completed",
                    &json!({"threadId":"t1","turnId":"u1",
                            "item":{"type":spelling,"command":"/bin/zsh -lc 'echo hi'","exitCode":0}})
                ),
                Some((EventClass::Trace, EventKind::CommandExec)),
                "{spelling}"
            );
        }
        // None of the three camelCase tool variants contains lowercase "tool":
        // Phase 1's `contains("tool")` idiom would file all three as unknown.
        for spelling in [
            "mcpToolCall",
            "dynamicToolCall",
            "collabAgentToolCall",
            "mcp_tool_call",
            "dynamic_tool_call",
            "collab_agent_tool_call",
        ] {
            assert_eq!(
                map_appserver_notification(
                    "item/completed",
                    &json!({"threadId":"t1","turnId":"u1","item":{"type":spelling,"server":"x"}})
                ),
                Some((EventClass::Trace, EventKind::ToolCall)),
                "{spelling}"
            );
        }
        // An item type from a newer codex is a trace line, never a failure.
        for spelling in ["reasoning", "webSearch", "contextCompaction", "brandNewThing"] {
            assert_eq!(
                map_appserver_notification(
                    "item/completed",
                    &json!({"threadId":"t1","turnId":"u1","item":{"type":spelling}})
                ),
                Some((EventClass::Trace, EventKind::Unknown)),
                "{spelling}"
            );
        }
        // Malformed (no nested item) still maps; never panics.
        assert_eq!(
            map_appserver_notification("item/completed", &json!({"threadId":"t1"})),
            Some((EventClass::Trace, EventKind::Unknown))
        );
    }

    #[test]
    fn item_started_mirrors_exec_semantics() {
        // The in-flight agent message is the streaming shape: dropped, exactly
        // as `map_exec_line` drops `item.started`+`agent_message`.
        for spelling in ["agentMessage", "agent_message"] {
            assert_eq!(
                map_appserver_notification(
                    "item/started",
                    &json!({"threadId":"t1","turnId":"u1","startedAtMs":1_760_000_000_000i64,
                            "item":{"type":spelling,"text":"DO"}})
                ),
                None,
                "{spelling}"
            );
        }
        // Every other type maps the same as its completed form.
        assert_eq!(
            map_appserver_notification(
                "item/started",
                &json!({"threadId":"t1","turnId":"u1","startedAtMs":1_760_000_000_000i64,
                        "item":{"type":"commandExecution","command":"ls"}})
            ),
            Some((EventClass::Trace, EventKind::CommandExec))
        );
        assert_eq!(
            map_appserver_notification(
                "item/started",
                &json!({"threadId":"t1","turnId":"u1","item":{"type":"collabAgentToolCall"}})
            ),
            Some((EventClass::Trace, EventKind::ToolCall))
        );
        // `item/autoApprovalReview/started` is NOT `item/started`.
        assert_eq!(
            map_appserver_notification(
                "item/autoApprovalReview/started",
                &json!({"threadId":"t1","turnId":"u1","item":{"type":"agentMessage"}})
            ),
            Some((EventClass::Trace, EventKind::Unknown))
        );
    }

    /// The willRetry pair. `willRetry` is a REQUIRED field of
    /// `v2/ErrorNotification` and it is the whole difference between "the
    /// server is about to try again" (observability) and "this turn is over"
    /// (drives the worker).
    #[test]
    fn error_branches_on_will_retry() {
        let transient = json!({
            "threadId":"019898f0-0000-7000-8000-000000000001",
            "turnId":"turn_1",
            "error":{"message":"stream disconnected before completion: connection reset",
                     "additionalDetails":null,"codexErrorInfo":null},
            "willRetry":true
        });
        assert_eq!(
            map_appserver_notification("error", &transient),
            Some((EventClass::Trace, EventKind::Error)),
            "a retryable error must NOT drive the worker"
        );

        let terminal = json!({
            "threadId":"019898f0-0000-7000-8000-000000000001",
            "turnId":"turn_1",
            "error":{"message":"We're currently experiencing high demand",
                     "additionalDetails":null,"codexErrorInfo":null},
            "willRetry":false
        });
        assert_eq!(
            map_appserver_notification("error", &terminal),
            Some((EventClass::Lifecycle, EventKind::Error))
        );

        // An absent or non-boolean willRetry is treated as terminal: parking a worker
        // is recoverable, silently swallowing a fatal error is not.
        assert_eq!(
            map_appserver_notification("error", &json!({"threadId":"t1","error":{"message":"x"}})),
            Some((EventClass::Lifecycle, EventKind::Error))
        );
    }

    #[test]
    fn token_usage_reroute_and_guardian_warning() {
        let usage = json!({
            "threadId":"t1","turnId":"turn_1",
            "tokenUsage":{
                "last":{"cachedInputTokens":0,"inputTokens":1200,"outputTokens":48,
                        "reasoningOutputTokens":0,"totalTokens":1248},
                "total":{"cachedInputTokens":0,"inputTokens":1200,"outputTokens":48,
                         "reasoningOutputTokens":0,"totalTokens":1248},
                "modelContextWindow":272000}
        });
        assert_eq!(
            map_appserver_notification("thread/tokenUsage/updated", &usage),
            Some((EventClass::Trace, EventKind::TokenUsage))
        );

        let rerouted = json!({
            "threadId":"t1","turnId":"turn_1",
            "fromModel":"gpt-5-codex","toModel":"gpt-5",
            "reason":"highRiskCyberActivity"
        });
        assert_eq!(
            map_appserver_notification("model/rerouted", &rerouted),
            Some((EventClass::Lifecycle, EventKind::ModelRerouted)),
            "evidence must stay attributable to the model that actually ran"
        );

        // Phase 2 records the guardian warning; parking + `a2a approve` is
        // Phase 3, so this is an outbox notice and nothing more.
        assert_eq!(
            map_appserver_notification(
                "guardianWarning",
                &json!({"threadId":"t1","message":"guardian denied a high-risk action"})
            ),
            Some((EventClass::Lifecycle, EventKind::OutboxNotice))
        );
    }

    /// AC: a 200-delta flood produces ZERO trace envelopes. The handshake opts
    /// out of these methods entirely; this guard is the second layer, so a
    /// server that ignores `optOutNotificationMethods` still cannot flood
    /// `events.jsonl`.
    #[test]
    fn two_hundred_delta_flood_maps_to_nothing() {
        let delta_methods = crate::protocol::SUPPRESSED_NOTIFICATION_METHODS;
        // Every method a2a opts out of at initialize must also be dropped here,
        // so the two layers can never disagree. Nine carry "delta"/"progress"
        // in the name; `item/reasoning/summaryPartAdded` and `turn/diff/updated`
        // do not, and are covered by the opt-out arm of the same guard.
        for m in delta_methods {
            assert_eq!(map_appserver_notification(m, &json!({})), None, "{m}");
        }

        let mut mapped = 0usize;
        for i in 0..200 {
            let method = delta_methods[i % delta_methods.len()];
            let params = json!({"threadId":"t1","turnId":"turn_1",
                                "itemId":format!("msg_{i}"),"delta":"tok"});
            if map_appserver_notification(method, &params).is_some() {
                mapped += 1;
            }
        }
        assert_eq!(mapped, 0, "a delta flood must produce zero envelopes");

        // The guard is case-insensitive: a re-spelled delta method still drops.
        for m in [
            "item/agentMessage/DELTA",
            "ITEM/REASONING/TEXTDELTA",
            "item/mcpToolCall/Progress",
            "some/future/Delta",
        ] {
            assert_eq!(map_appserver_notification(m, &json!({})), None, "{m}");
        }
    }

    /// Broker-scoped notifications carry no `threadId` — they belong to no
    /// worker, so this table deliberately has no arm for them; the demux routes
    /// them to `broker.jsonl` / `broker-health.json`.
    #[test]
    fn broker_scoped_methods_have_no_dedicated_arm() {
        assert_eq!(
            BROKER_SCOPED_METHODS,
            [
                "account/rateLimits/updated",
                "deprecationNotice",
                "configWarning"
            ]
        );
        for m in BROKER_SCOPED_METHODS {
            assert_eq!(
                map_appserver_notification(m, &json!({"rateLimits":{}})),
                Some((EventClass::Trace, EventKind::Unknown)),
                "{m} must not have its own arm here"
            );
        }
        // Anything else unrecognized is a trace line, never an error.
        assert_eq!(
            map_appserver_notification("sorcery/unicornSummoned", &json!({"horn":"spiral"})),
            Some((EventClass::Trace, EventKind::Unknown))
        );
    }

    /// The three new kinds cross the file layer: a Phase-1 binary reading a
    /// Phase-2 `events.jsonl` must degrade to `Unknown`, not fail the read.
    #[test]
    fn new_kinds_are_snake_case_and_read_as_unknown_by_a_phase1_binary() {
        for (kind, wire) in [
            (EventKind::TokenUsage, "token_usage"),
            (EventKind::ModelRerouted, "model_rerouted"),
            (EventKind::TransportDegraded, "transport_degraded"),
        ] {
            assert_eq!(serde_json::to_value(kind).unwrap(), json!(wire));
            let back: EventKind = serde_json::from_value(json!(wire)).unwrap();
            assert_eq!(back, kind);

            // Phase 1's exact vocabulary, replayed against the new wire string.
            let old: Phase1EventKind = serde_json::from_value(json!(wire)).unwrap();
            assert_eq!(old, Phase1EventKind::Unknown, "{wire} must degrade");

            let line = format!(
                r#"{{"schema_version":1,"seq":4,"ts":"2026-08-17T00:00:00Z","epoch":1,
                    "class":"trace","kind":"{wire}","transport":"app-server","raw":{{}}}}"#
            );
            let e: Envelope = serde_json::from_str(&line).unwrap();
            assert_eq!(e.kind, kind);
        }
    }

    /// Phase 1's `EventKind`, verbatim as of commit ef34425 — the frozen
    /// vocabulary a deployed older binary knows. Do not add to it.
    #[derive(Deserialize, PartialEq, Eq, Debug)]
    #[serde(rename_all = "snake_case")]
    enum Phase1EventKind {
        Spawned,
        SessionIdentified,
        TurnStarted,
        AgentMessage,
        ToolCall,
        CommandExec,
        ServerRequest,
        Blocked,
        OutboxNotice,
        InboxDelivered,
        StateChange,
        EventsDropped,
        TransportRestarted,
        Timeout,
        Interrupted,
        Killed,
        Orphaned,
        Resumed,
        Error,
        TurnCompleted,
        ResultWritten,
        #[serde(other)]
        Unknown,
    }

    // ── result synthesis (Task 5) ────────────────────────────────────────

    fn appserver_env(kind: EventKind, raw: serde_json::Value) -> Envelope {
        let class = if kind == EventKind::AgentMessage {
            EventClass::Lifecycle
        } else {
            EventClass::Trace
        };
        Envelope::new(
            1,
            Transport::AppServer,
            class,
            kind,
            raw,
            chrono::Utc::now(),
        )
    }

    #[test]
    fn synthesize_result_takes_the_final_agent_message() {
        let events = vec![
            appserver_env(
                EventKind::AgentMessage,
                json!({"threadId":"t1","turnId":"u1",
                       "item":{"type":"agentMessage","text":"thinking out loud"}}),
            ),
            appserver_env(
                EventKind::CommandExec,
                json!({"item":{"type":"commandExecution","command":"ls"}}),
            ),
            appserver_env(
                EventKind::AgentMessage,
                json!({"threadId":"t1","turnId":"u1",
                       "item":{"type":"agentMessage","id":"msg_2","text":"FINAL ANSWER\n",
                               "phase":"final_answer"}}),
            ),
            appserver_env(
                EventKind::TurnCompleted,
                json!({"threadId":"t1","turn":{"id":"u1","status":"completed"}}),
            ),
        ];
        assert_eq!(
            synthesize_result(&events).as_deref(),
            Some("FINAL ANSWER\n"),
            "text is preserved verbatim — it becomes result.md"
        );
    }

    #[test]
    fn synthesize_result_is_none_without_a_usable_agent_message() {
        assert_eq!(synthesize_result(&[]), None);
        // A turn that only ran commands has no result.
        let only_trace = vec![appserver_env(
            EventKind::CommandExec,
            json!({"item":{"type":"commandExecution","command":"ls"}}),
        )];
        assert_eq!(synthesize_result(&only_trace), None);
        // A trace line carrying `text` (reasoning) is not a result.
        let reasoning = vec![appserver_env(
            EventKind::Unknown,
            json!({"item":{"type":"reasoning","text":"hmm"}}),
        )];
        assert_eq!(synthesize_result(&reasoning), None);
        // An empty/whitespace final message is no result at all — the worker
        // must land in awaiting-input, not in "done" with an empty result.md.
        let blank = vec![appserver_env(
            EventKind::AgentMessage,
            json!({"item":{"type":"agentMessage","text":"   \n"}}),
        )];
        assert_eq!(synthesize_result(&blank), None);
    }

    #[test]
    fn synthesize_result_falls_back_through_the_known_text_spellings() {
        // Envelope raw that is the item itself.
        let bare_item = vec![appserver_env(
            EventKind::AgentMessage,
            json!({"type":"agentMessage","text":"bare"}),
        )];
        assert_eq!(synthesize_result(&bare_item).as_deref(), Some("bare"));
        // The exec spelling, so a mixed-transport turn still synthesizes.
        let exec_shape = vec![appserver_env(
            EventKind::AgentMessage,
            json!({"type":"agent_message","message":"from exec"}),
        )];
        assert_eq!(synthesize_result(&exec_shape).as_deref(), Some("from exec"));
    }

    /// The `item.*` envelopes exactly as codex-cli 0.140.0 emits them — these
    /// literals are transcribed from the live run recorded in `map_exec_line`'s
    /// doc comment, so this test fails if the unwrapping is ever undone.
    #[test]
    fn real_0_140_item_envelopes_are_unwrapped_by_their_inner_type() {
        assert_eq!(
            map_exec_line(&json!({"type":"item.completed",
                                  "item":{"id":"item_1","type":"agent_message","text":"DONE"}})),
            Some((EventClass::Lifecycle, EventKind::AgentMessage)),
            "the worker's own answer must be a lifecycle event, not trace/unknown"
        );
        // the in-flight form of the same item is the streaming shape: dropped
        assert_eq!(
            map_exec_line(&json!({"type":"item.started",
                                  "item":{"type":"agent_message","text":"DO"}})),
            None
        );
        for ty in ["item.started", "item.completed"] {
            assert_eq!(
                map_exec_line(&json!({ "type": ty,
                    "item":{"id":"item_0","type":"command_execution",
                            "command":"/bin/zsh -lc 'echo hi'","exit_code":0}})),
                Some((EventClass::Trace, EventKind::CommandExec)),
                "{ty}"
            );
        }
        assert_eq!(
            map_exec_line(&json!({"type":"item.completed",
                                  "item":{"type":"mcp_tool_call","server":"x"}})),
            Some((EventClass::Trace, EventKind::ToolCall))
        );
        // an item type from a newer codex is a trace line, never a hard failure
        assert_eq!(
            map_exec_line(&json!({"type":"item.completed","item":{"type":"web_search"}})),
            Some((EventClass::Trace, EventKind::Unknown))
        );
        // a malformed `item.*` (no nested item) still maps, never panics
        assert_eq!(
            map_exec_line(&json!({"type":"item.completed"})),
            Some((EventClass::Trace, EventKind::Unknown))
        );
    }
}
