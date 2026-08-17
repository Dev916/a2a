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
    Blocked,
    OutboxNotice,
    /// A batch of `inbox/` messages was handed to the worker; carries the new
    /// `last_delivered_seq` watermark, so a snapshot rebuilt from the WAL never
    /// re-delivers what was already acknowledged.
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
