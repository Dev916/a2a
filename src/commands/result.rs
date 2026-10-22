//! `a2a result <id>` — the worker's final answer (spec §4.1, §4.3).
//!
//! This is the command an orchestrator calls once, at the end, instead of
//! reading a worker's whole event stream. Like `status` it is a *read path that
//! owns the machine* (spec §4.6): it reconciles first, so the very same call
//! that asks for the result is the one that observes the finished turn, runs
//! the schema gate, and commits `done`/`failed{schema_violation}`.
//!
//! **Which file is the result** (spec §4.3), in precedence order:
//!
//! | file            | when it exists                                  |
//! |-----------------|-------------------------------------------------|
//! | `result.json`   | spawned with `--schema` and the gate passed      |
//! | `result.md`     | no schema — codex's `--output-last-message`      |
//! | `result.raw.md` | the gate failed; the raw text is preserved       |
//!
//! `result.raw.md` is printed rather than withheld on purpose: a worker that
//! failed the schema is exactly the case where a human needs to see what it
//! actually said, and the exit code (4) already says "this did not conform".
//!
//! **Success and exit-zero are different things** here, as everywhere in this
//! CLI: `result` on a running worker *succeeded* (it answered "not yet") and
//! exits 6 with a full payload. An error envelope is reserved for the cases
//! where the command could not answer at all — no such worker, a wedged lock,
//! unreadable state.
//!
//! Everything printed goes through egress redaction (spec §5): the result is
//! model-authored text landing straight in an agent's context, so it is the
//! last place that can mask a credential the worker echoed.

use crate::commands::tail::redact_value;
use crate::config;
use crate::fsm::WorkerState;
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::{reconcile, RESULT_JSON, RESULT_MD, RESULT_RAW};
use crate::redact;
use crate::state::WorkerSnapshot;
use chrono::Utc;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// The state → exit code contract for `result`. It differs from `status` in
/// exactly one place, and that place is the point of the command: a worker that
/// is still working has no result, so `spawned`/`queued`/`running` are exit 6
/// ("not terminal yet", spec §4.1) rather than 0.
fn exit_for(state: WorkerState) -> ExitCode {
    match state {
        WorkerState::Done | WorkerState::Cancelled => ExitCode::Success,
        WorkerState::Failed => ExitCode::WorkerFailed,
        WorkerState::AwaitingInput | WorkerState::Escalated => ExitCode::Blocked,
        WorkerState::Orphaned => ExitCode::Orphaned,
        WorkerState::Spawned | WorkerState::Queued | WorkerState::Running => ExitCode::NotTerminal,
        // Written by a newer a2a: reported, never silently normalized.
        WorkerState::Unknown => ExitCode::Internal,
    }
}

/// Which file the printed result came from — part of the machine payload, so a
/// caller can tell a schema-conforming result from a preserved raw one without
/// re-deriving the precedence rule.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Format {
    Json,
    Markdown,
    Raw,
}

impl Format {
    fn as_str(self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Markdown => "markdown",
            Format::Raw => "raw",
        }
    }
}

struct Found {
    path: PathBuf,
    format: Format,
    text: String,
}

pub fn run(
    home: &A2aHome,
    id_arg: &str,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let (cfg, _origins, warnings) = config::load(home)?;
    if !quiet {
        for w in &warnings {
            eprintln!("a2a: warning: {w}");
        }
    }
    let id = ids::resolve(home, id_arg)?;
    let snap = reconcile(home, &cfg, &id, Utc::now())?;
    let dir = home.worker_dir(&id);
    let found = find_result(&dir);
    let exit = exit_for(snap.state);

    if !quiet {
        // Decoration on stderr; stdout stays exactly the result.
        match &found {
            Some(f) => eprintln!(
                "a2a: {id} {} — {} ({})",
                snap.state.as_str(),
                f.path.display(),
                f.format.as_str()
            ),
            None => eprintln!(
                "a2a: {id} {} — no result yet{}",
                snap.state.as_str(),
                hint(snap.state)
            ),
        }
        if let Some(line) = commit_line(&snap) {
            eprintln!("a2a: {id} {line}");
        }
    }

    if json_mode {
        return Ok(Outcome::with(payload(&id, &snap, found.as_ref()), exit));
    }
    let data = match &found {
        // One mask over the whole rendering (a shape straddling two lines is
        // still caught); structural masking is reserved for the JSON envelope,
        // where corrupting the document would be the worse failure.
        Some(f) => Value::String(redact::redact_egress(&f.text)),
        None => Value::Null,
    };
    Ok(Outcome::with(data, exit))
}

/// The framework commit's verdict, for the human decoration line:
/// `committed <sha>`, or why there is none. `None` when it never ran.
fn commit_line(snap: &WorkerSnapshot) -> Option<String> {
    if let Some(sha) = &snap.committed_sha {
        return Some(format!("committed {sha}"));
    }
    let rec = snap.auto_commit.as_ref()?;
    let reason = rec.reason.as_deref().unwrap_or("");
    Some(match rec.outcome.as_str() {
        "failed" => format!("commit failed: {}", redact::redact_egress(reason)),
        "clean" => "not committed: nothing to commit".to_string(),
        other => format!("not committed ({other}): {reason}"),
    })
}

/// What to suggest when there is nothing to print. Terminal-but-empty is a
/// different problem from not-finished-yet, and the remedy differs too.
fn hint(state: WorkerState) -> &'static str {
    match state {
        WorkerState::Spawned | WorkerState::Queued | WorkerState::Running => {
            " (still working — `a2a wait <id>`)"
        }
        WorkerState::AwaitingInput | WorkerState::Escalated => {
            " (worker is waiting on you — `a2a send <id> \"…\"`)"
        }
        _ => " (`a2a tail <id>` shows what it did)",
    }
}

fn payload(id: &str, snap: &WorkerSnapshot, found: Option<&Found>) -> Value {
    let result = match found {
        // A schema-gated result is re-emitted as JSON, not as a quoted string:
        // the whole point of `--schema` is that the caller gets structure.
        Some(f) if f.format == Format::Json => serde_json::from_str::<Value>(&f.text)
            .map(|v| redact_value(&v))
            .unwrap_or_else(|_| Value::String(redact::redact_egress(&f.text))),
        Some(f) => Value::String(redact::redact_egress(&f.text)),
        None => Value::Null,
    };
    json!({
        "id": id,
        "state": snap.state.as_str(),
        "state_reason": snap.state_reason,
        "failure_class": snap.failure_class,
        "epoch": snap.epoch,
        "session_id": snap.session_id,
        "format": found.map(|f| f.format.as_str()),
        "result_path": found.map(|f| f.path.clone()),
        "result": result,
        // The framework commit on `done` (`src/auto_commit.rs`): the sha when
        // one was made, and the whole verdict either way (`null` = never ran).
        "committed_sha": snap.committed_sha,
        "auto_commit": snap.auto_commit,
    })
}

/// Precedence per spec §4.3. Emptiness counts as absent: codex creates the
/// `--output-last-message` file before it has anything to put in it, so a
/// zero-byte `result.md` is a worker that produced nothing, not a blank result.
fn find_result(dir: &Path) -> Option<Found> {
    for (name, format) in [
        (RESULT_JSON, Format::Json),
        (RESULT_MD, Format::Markdown),
        (RESULT_RAW, Format::Raw),
    ] {
        let path = dir.join(name);
        match std::fs::read_to_string(&path) {
            Ok(text) if !text.trim().is_empty() => return Some(Found { path, format, text }),
            _ => continue,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_answer_not_terminal_yet_with_6() {
        for (state, code) in [
            (WorkerState::Done, ExitCode::Success),
            (WorkerState::Cancelled, ExitCode::Success),
            (WorkerState::Failed, ExitCode::WorkerFailed),
            (WorkerState::AwaitingInput, ExitCode::Blocked),
            (WorkerState::Escalated, ExitCode::Blocked),
            (WorkerState::Orphaned, ExitCode::Orphaned),
            (WorkerState::Spawned, ExitCode::NotTerminal),
            (WorkerState::Queued, ExitCode::NotTerminal),
            (WorkerState::Running, ExitCode::NotTerminal),
            (WorkerState::Unknown, ExitCode::Internal),
        ] {
            assert_eq!(exit_for(state), code, "{}", state.as_str());
        }
    }

    #[test]
    fn json_wins_over_markdown_which_wins_over_raw() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        assert!(find_result(dir).is_none());
        std::fs::write(dir.join(RESULT_RAW), "not json at all").unwrap();
        assert_eq!(find_result(dir).unwrap().format, Format::Raw);
        std::fs::write(dir.join(RESULT_MD), "plain answer").unwrap();
        assert_eq!(find_result(dir).unwrap().format, Format::Markdown);
        std::fs::write(dir.join(RESULT_JSON), r#"{"status":"pass"}"#).unwrap();
        let found = find_result(dir).unwrap();
        assert_eq!(found.format, Format::Json);
        assert_eq!(found.path, dir.join(RESULT_JSON));
    }

    #[test]
    fn an_empty_result_file_is_not_a_result() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(RESULT_MD), "   \n").unwrap();
        assert!(find_result(tmp.path()).is_none());
    }

    #[test]
    fn a_schema_gated_payload_stays_structured_and_is_redacted() {
        let tmp = tempfile::tempdir().unwrap();
        let mut snap = WorkerSnapshot::new_for_test("general-0814t1300-aaaa", tmp.path());
        snap.state = WorkerState::Done;
        std::fs::write(
            tmp.path().join(RESULT_JSON),
            r#"{"status":"pass","note":"token xoxb-1234567890-abcdef"}"#,
        )
        .unwrap();
        let v = payload(
            "general-0814t1300-aaaa",
            &snap,
            find_result(tmp.path()).as_ref(),
        );
        assert_eq!(v["result"]["status"], json!("pass"), "structure preserved");
        assert_eq!(v["format"], json!("json"));
        let note = v["result"]["note"].as_str().unwrap();
        assert!(!note.contains("xoxb-1234567890"), "{note}");
    }

    #[test]
    fn the_commit_verdict_is_in_the_payload_and_the_decoration() {
        let tmp = tempfile::tempdir().unwrap();
        let mut snap = WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", tmp.path());
        snap.state = WorkerState::Done;
        assert_eq!(commit_line(&snap), None);
        let v = payload("impl-0814t1300-aaaa", &snap, None);
        assert_eq!((v["committed_sha"].clone(), v["auto_commit"].clone()), (Value::Null, Value::Null));

        snap.committed_sha = Some("abc123".into());
        snap.auto_commit = Some(crate::state::AutoCommitRecord {
            outcome: "committed".into(),
            sha: Some("abc123".into()),
            reason: None,
        });
        assert_eq!(commit_line(&snap).as_deref(), Some("committed abc123"));
        let v = payload("impl-0814t1300-aaaa", &snap, None);
        assert_eq!(v["committed_sha"], json!("abc123"));
        assert_eq!(v["auto_commit"]["outcome"], json!("committed"));

        snap.committed_sha = None;
        snap.auto_commit = Some(crate::state::AutoCommitRecord {
            outcome: "failed".into(),
            sha: None,
            reason: Some("git add failed: locked".into()),
        });
        assert_eq!(
            commit_line(&snap).as_deref(),
            Some("commit failed: git add failed: locked")
        );
    }

    #[test]
    fn a_missing_result_is_a_null_payload_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let mut snap = WorkerSnapshot::new_for_test("general-0814t1300-aaaa", tmp.path());
        snap.state = WorkerState::Running;
        let v = payload("general-0814t1300-aaaa", &snap, None);
        assert_eq!(v["result"], Value::Null);
        assert_eq!(v["format"], Value::Null);
        assert_eq!(v["state"], json!("running"));
    }
}
