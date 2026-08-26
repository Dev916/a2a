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
//!
//! # `--follow`
//!
//! The same two projections, kept open: the log is replayed from the cursor and
//! then extended as the worker writes. Four decisions carry it.
//!
//! * **The file is the source of truth; the broker is a doorbell.** A live
//!   subscription only ever means "look again" — every line printed is read back
//!   out of `events.jsonl` at its committed `seq`. That is what makes the two
//!   transports one code path (an exec worker simply has no doorbell and rings
//!   itself every [`POLL_INTERVAL`]), what makes the line contract identical to
//!   the one-shot form by construction, and what makes a **lag transparent**:
//!   the broker cutting a subscriber loose costs a re-read from a cursor this
//!   process already holds, never a gap and never a duplicate.
//! * **It is always bounded.** Two exits, both automatic: the worker reaching a
//!   terminal state (exit 0 — the thing the caller was waiting for) and
//!   `--timeout` expiring (exit 6, "not terminal yet" — the same code `a2a wait`
//!   returns for the same situation, and the same one-hour default). A follow
//!   that could block forever is a follow no orchestrator can supervise.
//! * **A broker going down is news, not a failure** (exit 0, one line on
//!   stderr). The daemon says goodbye on the subscription's own id precisely so
//!   this case is distinguishable, and a tail that turned it into an error would
//!   make every `broker restart` look like a broken command.
//! * **stdout stays the stream.** Every diagnostic — the lag notice, the reason
//!   the follow ended — goes to stderr; under `--json` each event is one object
//!   per line and the usual envelope is the last line, which is how a streaming
//!   consumer learns why the stream stopped.

use crate::brokerclient::{self, BrokerClient, Handshake, TailUpdate};
use crate::config;
use crate::events::{Envelope, EventClass, EventKind, Transport};
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use crate::redact;
use crate::state::WorkerSnapshot;
use crate::store;
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Value};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

/// How often a follow re-reads the log and re-checks the worker's state. The
/// same cadence `a2a wait` reconciles at, and the ceiling on a doorbell wait
/// too: a broker frame arrives *before* the actor has appended the row it
/// describes, so the poll is what guarantees convergence rather than an
/// optimisation on top of it.
pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// `--follow`'s knobs.
pub struct Follow {
    /// Hard bound on the whole command. Defaulted by the caller (`main`) to
    /// [`crate::commands::wait::DEFAULT_TIMEOUT`].
    pub timeout: Duration,
}

pub fn run(
    home: &A2aHome,
    id_arg: &str,
    since: Option<u64>,
    raw: bool,
    follow: Option<Follow>,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let id = ids::resolve(home, id_arg)?;
    let dir = home.worker_dir(&id);
    if let Some(follow) = follow {
        return follow_run(home, &id, &dir, since, raw, follow, json_mode, quiet);
    }
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
        eprintln!(
            "a2a: {id}: no events{}",
            since.map(|s| format!(" after seq {s}")).unwrap_or_default()
        );
    }

    if json_mode {
        let cursor = events.last().map(|e| e.seq);
        let items: Vec<Value> = events
            .iter()
            .map(|e| redact_value(&projection(e, raw)))
            .collect();
        return Ok(Outcome::from(
            json!({"id": id, "events": items, "next_seq": cursor}),
        ));
    }

    let text = events
        .iter()
        .map(|e| text_line(e, raw))
        .collect::<Vec<_>>()
        .join("\n");
    // One mask over the whole rendering: a shape that straddles two lines is
    // still caught, and an empty stream stays empty rather than becoming "".
    if text.is_empty() {
        return Ok(Outcome::from(Value::Null));
    }
    Ok(Outcome::from(Value::String(redact::redact_egress(&text))))
}

// ---------------------------------------------------------------------------
// --follow
// ---------------------------------------------------------------------------

/// Why a follow stopped. Each maps to one exit code, and the mapping is the
/// command's whole contract with an orchestrator.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Stop {
    /// The worker reached a terminal state — the thing being waited for.
    Terminal,
    /// `--timeout` expired with the worker still going.
    Timeout,
    /// The broker went away under the subscription.
    BrokerGone,
}

impl Stop {
    fn as_str(self) -> &'static str {
        match self {
            Stop::Terminal => "terminal",
            Stop::Timeout => "timeout",
            Stop::BrokerGone => "broker_shutdown",
        }
    }

    fn exit(self) -> ExitCode {
        match self {
            // A tail that stopped because there is nothing left to tail did its
            // job, and so did one whose broker was retired underneath it.
            Stop::Terminal | Stop::BrokerGone => ExitCode::Success,
            // Exactly `a2a wait`'s answer to the same question.
            Stop::Timeout => ExitCode::NotTerminal,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn follow_run(
    home: &A2aHome,
    id: &str,
    dir: &Path,
    since: Option<u64>,
    raw: bool,
    follow: Follow,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let (cfg, _origins, warnings) = config::load(home)?;
    if !quiet {
        for w in &warnings {
            eprintln!("a2a: warning: {w}");
        }
    }

    let deadline = Instant::now() + follow.timeout;
    let mut cursor = since.unwrap_or(0);
    let mut printed: u64 = 0;
    let mut doorbell: Option<Doorbell> = None;
    // **Observe the state, then read the log** — never the other way round. A
    // worker that settles between the two orderings takes the tail of its own
    // WAL with it: `done` is observed, and the events written just before it
    // are never looked for again. This way round the race is harmless, because
    // a terminal worker cannot write anything after the observation.
    let mut snap = reconcile(home, &cfg, id, Utc::now())?;
    let mut checked = Instant::now();
    loop {
        // 1. Whatever is on disk past the cursor, in seq order. This is the
        //    only place a line is ever produced — replay and live are the same
        //    read, which is why they cannot disagree about the format.
        let (batch, next) = drain(dir, cursor, raw, json_mode)?;
        cursor = next;
        printed += batch;

        if snap.state.is_terminal() {
            return Ok(finish(
                id,
                &snap,
                cursor,
                printed,
                Stop::Terminal,
                json_mode,
                quiet,
            ));
        }

        // 2. The bound. Checked after a full drain, so a worker that finishes
        //    exactly on the deadline is reported as finished rather than as a
        //    timeout.
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return settle(
                home,
                &cfg,
                id,
                dir,
                cursor,
                printed,
                Stop::Timeout,
                raw,
                json_mode,
                quiet,
            );
        }

        // 3. Wait for something to happen — the broker's subscription if this
        //    worker has one, the clock otherwise. Never longer than one poll
        //    interval either way, because a broker frame arrives *before* the
        //    row it describes has been appended.
        let bell = doorbell.get_or_insert_with(|| Doorbell::open(home, &snap, quiet));
        if let Wake::BrokerGone = bell.wait(left.min(POLL_INTERVAL), id, cursor, quiet)? {
            // One last read: the events that raced the farewell are already in
            // the file, and dropping them would lose exactly the ones a
            // shutdown makes interesting.
            return settle(
                home,
                &cfg,
                id,
                dir,
                cursor,
                printed,
                Stop::BrokerGone,
                raw,
                json_mode,
                quiet,
            );
        }

        // 4. ... and re-ask the machine, at most twice a second. A doorbell
        //    that rings a hundred times in a burst must not turn into a hundred
        //    observations, each of which takes the worker's `state.lock`.
        if checked.elapsed() >= POLL_INTERVAL {
            snap = observe(home, &cfg, id, dir, bell)?;
            checked = Instant::now();
        }
    }
}

/// Where the worker's state comes from, once per poll interval.
///
/// A **live subscription is already proof that the broker is up**, and on that
/// transport the broker is the only writer — so the snapshot it has committed
/// *is* the answer, and re-deriving it here would only add a `state.lock` and a
/// second handshake (`reconcile` asks the daemon whether it is alive) twice a
/// second per follower, on the very worker whose events are already streaming.
///
/// Everything else — an exec worker, an app-server worker with no daemon to
/// subscribe to — goes through `reconcile`, which is what turns a finished
/// process into `done` and a daemonless app-server worker into `orphaned`.
fn observe(
    home: &A2aHome,
    cfg: &config::Config,
    id: &str,
    dir: &Path,
    doorbell: &Doorbell,
) -> Result<WorkerSnapshot, AppError> {
    if let Doorbell::Broker(_) = doorbell {
        if let Ok(snap) = crate::state::load_snapshot(dir) {
            return Ok(snap);
        }
    }
    reconcile(home, cfg, id, Utc::now())
}

/// The last observation before a non-terminal stop: re-ask the machine, drain
/// what that observation made final, and let a worker that settled inside the
/// last poll interval report as settled rather than as a timeout.
#[allow(clippy::too_many_arguments)]
fn settle(
    home: &A2aHome,
    cfg: &config::Config,
    id: &str,
    dir: &Path,
    cursor: u64,
    printed: u64,
    stop: Stop,
    raw: bool,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let snap = reconcile(home, cfg, id, Utc::now())?;
    let (batch, cursor) = drain(dir, cursor, raw, json_mode)?;
    let stop = if snap.state.is_terminal() {
        Stop::Terminal
    } else {
        stop
    };
    Ok(finish(
        id,
        &snap,
        cursor,
        printed + batch,
        stop,
        json_mode,
        quiet,
    ))
}

/// Print every event past `cursor`; return how many and the new cursor.
///
/// The cursor moves over EVERY event, not just the printed ones: `--since`
/// filters on seq, so a lifecycle-only projection that remembered only the
/// lifecycle seqs would re-read (and re-filter) the trace rows on every pass.
fn drain(dir: &Path, cursor: u64, raw: bool, json_mode: bool) -> Result<(u64, u64), AppError> {
    let events = store::read_events(dir, cursor)
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("reading events: {e}")))?;
    let mut next = cursor;
    let mut printed = 0;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for e in &events {
        next = next.max(e.seq);
        if !raw && e.class != EventClass::Lifecycle {
            continue;
        }
        let line = if json_mode {
            redact_value(&projection(e, raw)).to_string()
        } else {
            // Per line rather than over the whole rendering, because a stream
            // has no "whole": a mask that needed both halves of a two-line
            // shape could never fire on the first half alone anyway.
            redact::redact_egress(&text_line(e, raw))
        };
        if writeln!(out, "{line}").is_err() {
            // A closed pipe (`| head`) is not this command's failure.
            return Ok((printed, next));
        }
        printed += 1;
    }
    let _ = out.flush();
    Ok((printed, next))
}

/// What a wait produced.
enum Wake {
    /// Look at the log again.
    Again,
    /// The broker that was feeding this tail is gone.
    BrokerGone,
}

/// Where "something happened" comes from.
enum Doorbell {
    /// A live subscription on the broker that owns this worker.
    Broker(Box<BrokerClient>),
    /// Nothing to subscribe to (exec transport, no daemon, the socket switched
    /// off): the clock is the doorbell.
    Clock,
}

impl Doorbell {
    /// Subscribe if this worker is one a broker could be pushing, and fall
    /// through to the clock in every case where it is not — a tail is a
    /// **read** of files this process can already see, so an absent or
    /// unreachable daemon must never be the difference between following and
    /// refusing.
    fn open(home: &A2aHome, snap: &WorkerSnapshot, quiet: bool) -> Doorbell {
        if snap.transport != Transport::AppServer || brokerclient::disabled() {
            return Doorbell::Clock;
        }
        let Ok(Some(Handshake::Ready(mut client))) = brokerclient::try_handshake(home.root())
        else {
            return Doorbell::Clock;
        };
        match client.subscribe() {
            Ok(_) => Doorbell::Broker(Box::new(client)),
            Err(e) => {
                if !quiet {
                    eprintln!(
                        "a2a: the broker refused a tail subscription ({}); polling instead",
                        e.code
                    );
                }
                Doorbell::Clock
            }
        }
    }

    fn wait(
        &mut self,
        patience: Duration,
        id: &str,
        cursor: u64,
        quiet: bool,
    ) -> Result<Wake, AppError> {
        let client = match self {
            Doorbell::Clock => {
                std::thread::sleep(patience);
                return Ok(Wake::Again);
            }
            Doorbell::Broker(client) => client,
        };
        match client.next_update(patience) {
            Ok(TailUpdate::Event(_)) | Ok(TailUpdate::Idle) => Ok(Wake::Again),
            Ok(TailUpdate::BrokerShutdown { .. }) | Ok(TailUpdate::Closed) => Ok(Wake::BrokerGone),
            // Recoverable by construction: the cursor is this process's, the
            // log is on disk, and the next pass reads from exactly where the
            // last printed line left off. Resubscribing is what keeps the
            // doorbell working afterwards; failing to is not fatal either.
            Ok(TailUpdate::Lagged { .. }) => {
                if !quiet {
                    eprintln!("a2a: {id}: the event stream lagged; re-reading from seq {cursor}");
                }
                if client.subscribe().is_err() {
                    *self = Doorbell::Clock;
                }
                Ok(Wake::Again)
            }
            // Anything else the socket can say is a reason to stop trusting it,
            // not a reason to stop tailing: the file is still there.
            Err(e) => {
                if !quiet {
                    eprintln!(
                        "a2a: {id}: the broker tail failed ({}); polling instead",
                        e.code
                    );
                }
                *self = Doorbell::Clock;
                Ok(Wake::Again)
            }
        }
    }
}

/// The last line: a summary under `--json`, a sentence on stderr otherwise.
fn finish(
    id: &str,
    snap: &WorkerSnapshot,
    cursor: u64,
    printed: u64,
    stop: Stop,
    json_mode: bool,
    quiet: bool,
) -> Outcome {
    if !quiet {
        match stop {
            Stop::Terminal => eprintln!(
                "a2a: {id}: {} — nothing left to follow",
                snap.state.as_str()
            ),
            Stop::Timeout => eprintln!(
                "a2a: {id}: still {} when the follow timed out",
                snap.state.as_str()
            ),
            Stop::BrokerGone => eprintln!(
                "a2a: {id}: the broker is shutting down; following stopped ({})",
                snap.state.as_str()
            ),
        }
    }
    let data = if json_mode {
        json!({
            "id": id,
            "state": snap.state.as_str(),
            "events": printed,
            "next_seq": cursor,
            "stopped": stop.as_str(),
        })
    } else {
        Value::Null
    };
    Outcome::with(data, stop.exit())
}

/// One event, as `--json` renders it (before redaction).
fn projection(e: &Envelope, raw: bool) -> Value {
    if raw {
        serde_json::to_value(e).unwrap_or(Value::Null)
    } else {
        json!({"seq": e.seq, "ts": ts(e), "kind": kind(e.kind)})
    }
}

/// One event, as the plain-text projections render it (before redaction).
fn text_line(e: &Envelope, raw: bool) -> String {
    if raw {
        serde_json::to_string(e).unwrap_or_default()
    } else {
        format!("{} {} {}", e.seq, ts(e), kind(e.kind))
    }
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
///
/// Shared with `result`, which has the same problem with schema-gated payloads.
pub(crate) fn redact_value(v: &Value) -> Value {
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

    /// The follow's whole contract with a caller: two ways to stop that mean
    /// "there is nothing more to see" (exit 0) and one that means "your window
    /// ran out, the worker is still going" — which is `a2a wait`'s exit 6, for
    /// exactly the same situation.
    #[test]
    fn the_bounded_exits_are_pinned() {
        assert_eq!(Stop::Terminal.exit(), ExitCode::Success);
        assert_eq!(Stop::BrokerGone.exit(), ExitCode::Success);
        assert_eq!(Stop::Timeout.exit(), ExitCode::NotTerminal);
        assert_eq!(Stop::Timeout.exit() as i32, 6);
    }

    /// Followed lines and one-shot lines come out of the same two renderers, so
    /// `--follow` cannot drift from `tail` by construction.
    #[test]
    fn both_projections_render_the_same_line_in_either_mode() {
        let mut e = Envelope::synthetic(EventKind::TurnCompleted);
        e.seq = 12;
        assert_eq!(
            text_line(&e, false),
            format!("12 {} turn_completed", ts(&e))
        );
        assert_eq!(projection(&e, false)["seq"], json!(12));
        assert_eq!(projection(&e, false)["kind"], json!("turn_completed"));
        // --raw is one complete JSON object per line, both ways
        let line = text_line(&e, true);
        assert!(!line.contains('\n'), "{line}");
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap(),
            projection(&e, true)
        );
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
