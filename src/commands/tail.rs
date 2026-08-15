//! `a2a tail <id>` — read a worker's WAL back out (spec §4.8).
//!
//! Two projections over the same `events.jsonl`:
//!
//! * **default** — lifecycle events only, one `<seq> <ts> <kind>` line each.
//!   Lifecycle is what *moved the machine*; trace events (every tool call,
//!   every shell command) are observability, and piping them into an
//!   orchestrator's context is how a monitor loop eats its own window.
//! * **`--raw`** — the full envelope stream as JSONL, unfiltered. For a human
//!   debugging a worker, or a tool that wants the transport payloads.
//!
//! Both are **cursored**: `seq` is a stable, monotonic position, so an
//! orchestrator polls with `--since <last seq it saw>` and never re-reads a
//! line. `--since` filters on `seq > N`, which makes the cursor the seq of the
//! last line consumed — no off-by-one at the call site.
//!
//! Redaction runs here too, even though `reconcile` already redacted these
//! bytes at ingest (spec §5). That is belt-and-braces on purpose: `tail` is the
//! command whose output lands directly in agent context, so it masks its own
//! rendering rather than trusting the file it is reading. The at-rest guarantee
//! is the one that actually matters, and the integration test asserts both.

use crate::events::{EventClass, EventKind, Envelope};
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::redact;
use crate::store;
use chrono::SecondsFormat;
use serde_json::{json, Value};

pub fn run(
    home: &A2aHome,
    id_arg: &str,
    since: Option<u64>,
    raw: bool,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let id = ids::resolve(home, id_arg)?;
    let dir = home.worker_dir(&id);
    // `read_events` filters at the source, so `--since` costs nothing to apply.
    let events = store::read_events(&dir, since.unwrap_or(0))
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading events: {e}")))?;
    let events: Vec<Envelope> = if raw {
        events
    } else {
        events
            .into_iter()
            .filter(|e| e.class == EventClass::Lifecycle)
            .collect()
    };

    if !quiet && events.is_empty() {
        eprintln!("a2a: {id}: no events{}", since.map(|s| format!(" after seq {s}")).unwrap_or_default());
    }

    if json_mode {
        let cursor = events.last().map(|e| e.seq);
        let items: Vec<Value> = events
            .iter()
            .map(|e| {
                if raw {
                    serde_json::to_value(e).unwrap_or(Value::Null)
                } else {
                    json!({"seq": e.seq, "ts": ts(e), "kind": kind(e.kind)})
                }
            })
            .map(|v| redact_value(&v))
            .collect();
        return Ok(Outcome::from(
            json!({"id": id, "events": items, "next_seq": cursor}),
        ));
    }

    let text = events
        .iter()
        .map(|e| {
            if raw {
                serde_json::to_string(e).unwrap_or_default()
            } else {
                format!("{} {} {}", e.seq, ts(e), kind(e.kind))
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    // One mask over the whole rendering: a shape that straddles two lines is
    // still caught, and an empty stream stays empty rather than becoming "".
    if text.is_empty() {
        return Ok(Outcome::from(Value::Null));
    }
    Ok(Outcome::from(Value::String(redact::redact_egress(&text))))
}

/// Second-precision UTC with a `Z` suffix — deliberately space-free, because
/// the line format is whitespace-separated and a `<seq> <ts> <kind>` line must
/// stay parseable by `cut -d' '`.
fn ts(e: &Envelope) -> String {
    e.ts.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// The wire spelling, taken from serde so the CLI can never drift from the
/// on-disk vocabulary.
fn kind(k: EventKind) -> String {
    serde_json::to_value(k)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Mask every string leaf. Structural (rather than whole-document) redaction on
/// purpose: masking the serialized JSON could in principle corrupt it, and a
/// `tail --json` that emits invalid JSON is a worse failure than a long token.
fn redact_value(v: &Value) -> Value {
    match v {
        Value::String(s) => Value::String(redact::redact_egress(s)),
        Value::Array(a) => Value::Array(a.iter().map(redact_value).collect()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), redact_value(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_uses_the_on_disk_spelling() {
        assert_eq!(kind(EventKind::TurnCompleted), "turn_completed");
        assert_eq!(kind(EventKind::SessionIdentified), "session_identified");
        assert_eq!(kind(EventKind::Unknown), "unknown");
    }

    #[test]
    fn ts_has_no_whitespace_so_the_line_stays_three_fields() {
        let e = Envelope::synthetic(EventKind::Spawned);
        let line = format!("{} {} {}", e.seq, ts(&e), kind(e.kind));
        assert_eq!(line.split(' ').count(), 3, "{line}");
        assert!(ts(&e).ends_with('Z'), "{}", ts(&e));
    }

    #[test]
    fn redact_value_masks_string_leaves_at_every_depth() {
        let v = json!({
            "raw": {"message": "token xoxb-1234567890-abcdef here"},
            "list": ["sk-abcdefghijklmnopqrstuvwx"],
            "seq": 4,
        });
        let masked = redact_value(&v).to_string();
        assert!(!masked.contains("xoxb-1234567890"), "{masked}");
        assert!(!masked.contains("sk-abcdefghij"), "{masked}");
        assert!(masked.contains("[REDACTED:"), "{masked}");
        // non-strings survive untouched, so the document stays valid JSON
        assert_eq!(redact_value(&v)["seq"], json!(4));
    }
}
