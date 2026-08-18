//! `a2a ls` — the fleet view (spec §4.8).
//!
//! This is the orchestrator's monitor loop, and the orchestrator's context
//! window is the scarcest resource in the system, so the projection is
//! deliberately narrow: one line per worker, five columns, no event text.
//! Everything an orchestrator actually decides on is here — *which* worker,
//! *what state*, and the two ages that distinguish "working" from "wedged"
//! (`dwell` = time in the current state, `last-event age` = time since the
//! worker last said anything).
//!
//! Like `status`, `ls` is a *read path that owns the machine* (spec §4.6):
//! it reconciles every worker it lists, so a fleet of finished workers reports
//! terminal states without anyone having called `status` first.
//!
//! One worker must never blind the fleet. A worker whose reconcile fails —
//! a wedged `state.lock`, a snapshot written by a newer binary — is listed with
//! state `unknown` and its error on stderr, and the rest of the fleet is still
//! reported. `last-event age` is still filled in for such a row: it is a plain
//! filesystem observation that does not depend on reconcile succeeding, and it
//! is exactly the column you want when diagnosing a stuck worker.
//!
//! **Owner filtering (`--all`, spec §4.8).** The default listing is the
//! caller's own workers plus every *unowned* one, because a fleet view that
//! shows another orchestrator's forty workers is a fleet view nobody reads. It
//! is a projection, not a permission: `--all` shows everything (with an OWNER
//! column), and the `owner` field is in the `--json` payload either way, so a
//! caller who wants to reason about ownership never has to guess from a filter.
//! Unowned workers — everything spawned before owners existed — are listed for
//! everyone, which is the same rule the mutating commands use.

use crate::commands::owner;
use crate::config;
use crate::events::Envelope;
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use crate::state::{load_snapshot, WorkerSnapshot};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// How much of the tail of `events.jsonl` is read to find the last event's
/// timestamp. Bounded on purpose: `ls` is polled in a loop, and a worker with a
/// megabyte-long WAL must not cost a megabyte of IO per poll.
const TAIL_WINDOW_BYTES: u64 = 64 * 1024;

pub fn run(home: &A2aHome, all: bool, json_mode: bool, quiet: bool) -> Result<Outcome, AppError> {
    let (cfg, _origins, warnings) = config::load(home)?;
    if !quiet {
        for w in &warnings {
            eprintln!("a2a: warning: {w}");
        }
    }
    // One clock for the whole listing: two workers' dwells are then comparable.
    let now = Utc::now();
    // ...and one caller identity, resolved once: a listing whose filter could
    // change halfway through is a listing that cannot be reasoned about.
    let caller = owner::caller();
    let ids = home
        .list_worker_ids()
        .map_err(|e| AppError::new("IO", ExitCode::Internal, format!("listing workers: {e}")))?;

    let mut rows: Vec<Row> = Vec::with_capacity(ids.len());
    for id in ids {
        let dir = home.worker_dir(&id);
        let row = match reconcile(home, &cfg, &id, now) {
            Ok(snap) => Row::reconciled(&id, &snap, &dir, now),
            Err(e) => {
                if !quiet {
                    eprintln!("a2a: warning: {id}: {}: {}", e.code, e.message);
                }
                // An unreadable worker keeps its stale owner if it has one, so a
                // wedged worker does not leak into everybody's default listing.
                Row::unreadable(&id, &dir, now)
            }
        };
        if all || owner::permitted(row.owner.as_deref(), &caller) {
            rows.push(row);
        }
    }
    // Creation order is the orchestrator's mental order — the worker it
    // dispatched first is the one it reads first. Unreadable rows have no
    // trustworthy `created_at`, so they sort last, by id.
    rows.sort_by(|a, b| match (a.created_at, b.created_at) {
        (Some(x), Some(y)) => x.cmp(&y).then_with(|| a.id.cmp(&b.id)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.id.cmp(&b.id),
    });

    if json_mode {
        return Ok(Outcome::from(
            json!({"workers": rows.iter().map(Row::to_json).collect::<Vec<_>>()}),
        ));
    }
    if rows.is_empty() {
        if !quiet {
            eprintln!("a2a: no workers");
        }
        return Ok(Outcome::from(Value::Null));
    }
    if !quiet {
        // Header is decoration, so it goes to stderr: stdout stays a clean
        // machine payload that `awk`/`cut` can read without a skip rule.
        eprintln!(
            "ID  LANE  STATE  {}{}DWELL  LAST-EVENT",
            if all { "OWNER  " } else { "" },
            if any_worktree(&rows) { "WT  " } else { "" }
        );
    }
    Ok(Outcome::from(Value::String(render_table(&rows, all))))
}

/// One listed worker. Every age is optional because "we could not tell" is a
/// distinct, printable answer from "zero seconds".
struct Row {
    id: String,
    lane: String,
    state: String,
    sandbox: String,
    /// Who this worker belongs to — `None` for a worker spawned before owners
    /// existed, which is the value that makes it everybody's (spec §4.8).
    owner: Option<String>,
    /// Which transport owns this worker — the fleet view's answer to "why is
    /// `status` not advancing this one?" (an app-server worker is the broker's).
    transport: String,
    /// Where this worker is in the broker's admission queue, when it is in one
    /// (spec §4.9). `None` for every worker that is not `queued` — which is the
    /// difference between "waiting third" and "waiting, position unknown".
    queue_position: Option<u64>,
    /// The FULL branch this worker is producing (`a2a/<lane>/<id>`), or `None`
    /// when it was spawned straight into `--cwd`. The human table gets a
    /// marker; this string is the one a caller pipes into `git`, so it lives in
    /// `--json`.
    worktree_branch: Option<String>,
    created_at: Option<DateTime<Utc>>,
    dwell_secs: Option<i64>,
    last_event_age_secs: Option<i64>,
}

impl Row {
    fn reconciled(id: &str, snap: &WorkerSnapshot, dir: &Path, now: DateTime<Utc>) -> Self {
        Self {
            id: id.to_string(),
            lane: snap.lane.as_str().to_string(),
            state: snap.state.as_str().to_string(),
            sandbox: snap.sandbox.as_str().to_string(),
            owner: snap.owner.clone(),
            transport: transport_of(Some(snap)),
            queue_position: snap.queue_position,
            worktree_branch: snap.worktree.as_ref().map(|w| w.branch.clone()),
            created_at: Some(snap.created_at),
            dwell_secs: Some(age_secs(snap.entered_state_at, now)),
            last_event_age_secs: last_event_ts(dir).map(|ts| age_secs(ts, now)),
        }
    }

    /// Reconcile failed. The state is `unknown` — NOT the stale snapshot's
    /// state, which we have just failed to confirm — but identity fields are
    /// still worth showing, so the row is recognizable and sortable.
    fn unreadable(id: &str, dir: &Path, now: DateTime<Utc>) -> Self {
        let stale = load_snapshot(dir).ok();
        Self {
            id: id.to_string(),
            lane: stale
                .as_ref()
                .map(|s| s.lane.as_str().to_string())
                .unwrap_or_else(|| "unknown".into()),
            state: "unknown".into(),
            sandbox: stale
                .as_ref()
                .map(|s| s.sandbox.as_str().to_string())
                .unwrap_or_else(|| "unknown".into()),
            owner: stale.as_ref().and_then(|s| s.owner.clone()),
            transport: transport_of(stale.as_ref()),
            queue_position: stale.as_ref().and_then(|s| s.queue_position),
            worktree_branch: stale
                .as_ref()
                .and_then(|s| s.worktree.as_ref().map(|w| w.branch.clone())),
            created_at: stale.as_ref().map(|s| s.created_at),
            dwell_secs: None,
            last_event_age_secs: last_event_ts(dir).map(|ts| age_secs(ts, now)),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "lane": self.lane,
            "state": self.state,
            "dwell_secs": self.dwell_secs,
            "last_event_age_secs": self.last_event_age_secs,
            "sandbox": self.sandbox,
            "owner": self.owner,
            "transport": self.transport,
            "queue_position": self.queue_position,
            "worktree_branch": self.worktree_branch,
        })
    }

    /// The OWNER column. `-` is "nobody" — the value that makes this worker
    /// everybody's — and it is deliberately the same dash the age columns use
    /// for "not known", because both mean "there is no value here to act on".
    fn owner_cell(&self) -> &str {
        self.owner.as_deref().unwrap_or("-")
    }

    /// The WT column: a MARKER, not the branch.
    ///
    /// `a2a/<lane>/<id>`'s last segment is exactly the worker id, which is
    /// already column 1 of the same row — printing it would widen the human
    /// table to carry zero information. The full branch is in `--json`.
    fn wt_cell(&self) -> &str {
        if self.worktree_branch.is_some() {
            "wt"
        } else {
            "-"
        }
    }
}

/// Does this listing need a WT column at all? An all-plain fleet keeps the
/// Phase-1 projection byte for byte.
fn any_worktree(rows: &[Row]) -> bool {
    rows.iter().any(|r| r.worktree_branch.is_some())
}

/// The transport as a wire string, `unknown` when the snapshot could not be
/// read at all.
fn transport_of(snap: Option<&WorkerSnapshot>) -> String {
    snap.and_then(|s| serde_json::to_value(s.transport).ok())
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".into())
}

/// Never negative: a snapshot stamped a hair in the future (clock skew, or a
/// reconcile that stamped `now` after we sampled it) must read as `0s`, not as
/// a worker that has been in its state for minus four seconds.
fn age_secs(then: DateTime<Utc>, now: DateTime<Utc>) -> i64 {
    (now - then).num_seconds().max(0)
}

/// The timestamp of the last complete, parseable event in the WAL.
///
/// Reads only the tail window and walks it backwards, so a torn trailing line
/// (a crash mid-append) is skipped rather than mistaken for "no events", and a
/// window that starts mid-line simply never reaches its leading fragment.
///
/// Shared with `gc`, which anchors its retention age to exactly this instant —
/// "how long since this worker last said anything" is the same question in both
/// commands, and it must not have two answers.
pub(crate) fn last_event_ts(dir: &Path) -> Option<DateTime<Utc>> {
    let mut file = File::open(dir.join("events.jsonl")).ok()?;
    let len = file.metadata().ok()?.len();
    file.seek(SeekFrom::Start(len.saturating_sub(TAIL_WINDOW_BYTES)))
        .ok()?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).ok()?;
    buf.split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .rev()
        .find_map(|l| serde_json::from_slice::<Envelope>(l).ok())
        .map(|e| e.ts)
}

/// `4m`, `12s`, `3h`, `2d` — one significant unit, because the column exists to
/// answer "is this worker stuck?", not to be arithmetic.
fn humanize(secs: Option<i64>) -> String {
    match secs {
        None => "-".to_string(),
        Some(s) if s < 60 => format!("{s}s"),
        Some(s) if s < 3600 => format!("{}m", s / 60),
        Some(s) if s < 86_400 => format!("{}h", s / 3600),
        Some(s) => format!("{}d", s / 86_400),
    }
}

/// The five-column projection, plus OWNER when `--all` widened the question
/// from "my fleet" to "this machine's fleet", plus WT when anybody in the
/// listing has a worktree.
fn render_table(rows: &[Row], all: bool) -> String {
    let idw = rows.iter().map(|r| r.id.len()).max().unwrap_or(0);
    let lanew = rows.iter().map(|r| r.lane.len()).max().unwrap_or(0);
    let statew = rows.iter().map(|r| r.state.len()).max().unwrap_or(0);
    let ownerw = rows.iter().map(|r| r.owner_cell().len()).max().unwrap_or(0);
    let wt = any_worktree(rows);
    let wtw = rows.iter().map(|r| r.wt_cell().len()).max().unwrap_or(0);
    rows.iter()
        .map(|r| {
            let owner = if all {
                format!("{:<ownerw$}  ", r.owner_cell())
            } else {
                String::new()
            };
            let worktree = if wt {
                format!("{:<wtw$}  ", r.wt_cell())
            } else {
                String::new()
            };
            format!(
                "{:<idw$}  {:<lanew$}  {:<statew$}  {owner}{worktree}{}  {}",
                r.id,
                r.lane,
                r.state,
                humanize(r.dwell_secs),
                humanize(r.last_event_age_secs),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventKind;
    use crate::store::EventLog;

    fn row(id: &str, state: &str, dwell: Option<i64>) -> Row {
        Row {
            id: id.into(),
            lane: "general".into(),
            state: state.into(),
            sandbox: "workspace-write".into(),
            owner: None,
            transport: "exec".into(),
            queue_position: None,
            worktree_branch: None,
            created_at: None,
            dwell_secs: dwell,
            last_event_age_secs: None,
        }
    }

    #[test]
    fn humanize_picks_one_significant_unit() {
        assert_eq!(humanize(None), "-");
        assert_eq!(humanize(Some(0)), "0s");
        assert_eq!(humanize(Some(59)), "59s");
        assert_eq!(humanize(Some(240)), "4m");
        assert_eq!(humanize(Some(3599)), "59m");
        assert_eq!(humanize(Some(7200)), "2h");
        assert_eq!(humanize(Some(172_800)), "2d");
    }

    #[test]
    fn age_never_goes_negative() {
        let now = Utc::now();
        assert_eq!(age_secs(now + chrono::Duration::seconds(30), now), 0);
        assert_eq!(age_secs(now - chrono::Duration::seconds(30), now), 30);
    }

    #[test]
    fn table_columns_align_and_carry_every_field() {
        let text = render_table(
            &[
                row("general-0814t1300-aaaa", "done", Some(240)),
                row("impl-0814t1301-bb", "running", Some(5)),
            ],
            false,
        );
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("general-0814t1300-aaaa  general  done"));
        assert!(lines[1].starts_with("impl-0814t1301-bb "), "{}", lines[1]);
        // humanized dwell, then the unknown last-event age
        assert!(lines[0].ends_with("  4m  -"), "{}", lines[0]);
        assert!(lines[1].ends_with("  5s  -"), "{}", lines[1]);
        // the short id is padded to the widest, so the state column lines up
        assert_eq!(
            lines[0].find("done"),
            lines[1].find("running"),
            "columns must align:\n{}\n{}",
            lines[0],
            lines[1]
        );
    }

    #[test]
    fn last_event_ts_reads_the_last_complete_line_over_a_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(last_event_ts(tmp.path()), None); // no log at all
        let mut log = EventLog::open(tmp.path()).unwrap();
        for kind in [EventKind::Spawned, EventKind::TurnCompleted] {
            let mut env = Envelope::synthetic(kind);
            log.append(&mut env).unwrap();
        }
        let expected = crate::store::read_events(tmp.path(), 0)
            .unwrap()
            .last()
            .unwrap()
            .ts;
        assert_eq!(last_event_ts(tmp.path()), Some(expected));

        // a crash mid-append leaves a fragment; the previous line still answers
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(tmp.path().join("events.jsonl"))
            .unwrap();
        f.write_all(br#"{"schema_version":1,"seq":3,"ts":"20"#).unwrap();
        assert_eq!(last_event_ts(tmp.path()), Some(expected));
    }

    /// `--all` inserts OWNER between STATE and DWELL, padded like every other
    /// column, and an unowned worker prints the same `-` the age columns use.
    #[test]
    fn the_all_view_adds_an_aligned_owner_column() {
        let mut mine = row("general-0814t1300-aaaa", "done", Some(240));
        mine.owner = Some("alice".into());
        let legacy = row("impl-0814t1301-bb", "running", Some(5));
        let text = render_table(&[mine, legacy], true);
        let lines: Vec<_> = text.lines().collect();
        assert!(lines[0].ends_with("alice  4m  -"), "{}", lines[0]);
        assert!(lines[1].ends_with("-      5s  -"), "{}", lines[1]);
        // OWNER is padded like every other column, so the column after it still
        // starts at the same offset on both rows.
        assert_eq!(
            lines[0].find("4m"),
            lines[1].find("5s"),
            "columns must align:\n{}\n{}",
            lines[0],
            lines[1]
        );
        // ...and without the flag the projection is exactly the Phase-1 one.
        let plain = render_table(
            &[row("general-0814t1300-aaaa", "done", Some(240))],
            false,
        );
        assert!(plain.ends_with("done  4m  -"), "{plain}");
    }

    /// The WT column costs a column, so it is only rendered when it answers
    /// something: an all-plain fleet is byte-identical to the Phase-1 table.
    #[test]
    fn the_wt_column_appears_only_for_a_fleet_that_has_one() {
        let plain = || row("general-0814t1300-aaaa", "done", Some(240));
        assert!(!any_worktree(&[plain()]));
        assert!(render_table(&[plain()], false).ends_with("done  4m  -"));

        let treed = || {
            let mut r = row("impl-0814t1301-bb", "running", Some(5));
            r.worktree_branch = Some("a2a/impl/impl-0814t1301-bb".into());
            r
        };
        assert!(any_worktree(&[plain(), treed()]));
        let text = render_table(&[plain(), treed()], false);
        let lines: Vec<_> = text.lines().collect();
        assert!(lines[0].ends_with("-   4m  -"), "{}", lines[0]);
        assert!(lines[1].ends_with("wt  5s  -"), "{}", lines[1]);
        assert!(
            !text.contains("a2a/impl/"),
            "the table carries a marker, not the branch:\n{text}"
        );
        assert_eq!(
            lines[0].find("4m"),
            lines[1].find("5s"),
            "columns must align:\n{}\n{}",
            lines[0],
            lines[1]
        );
    }

    /// The branch is the string a caller pipes into git, so `--json` carries it
    /// in full — and `null`, not `"-"`, when there is none.
    #[test]
    fn the_json_row_carries_the_full_branch_or_null() {
        let mut r = row("impl-0814t1301-bb", "running", Some(5));
        assert_eq!(r.to_json()["worktree_branch"], Value::Null);
        r.worktree_branch = Some("a2a/impl/impl-0814t1301-bb".into());
        assert_eq!(
            r.to_json()["worktree_branch"],
            json!("a2a/impl/impl-0814t1301-bb")
        );
    }

    #[test]
    fn unreadable_row_says_unknown_without_inventing_a_dwell() {
        let tmp = tempfile::tempdir().unwrap();
        let r = Row::unreadable("general-0814t1300-aaaa", tmp.path(), Utc::now());
        assert_eq!(r.state, "unknown");
        assert_eq!(r.lane, "unknown");
        assert!(r.dwell_secs.is_none() && r.created_at.is_none());
        assert_eq!(r.to_json()["dwell_secs"], Value::Null);
    }
}
