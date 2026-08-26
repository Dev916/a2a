//! `a2a respawn <id>` — retry a worker's brief in a fresh worker (spec §4.9).
//!
//! A respawn is an **ordinary spawn that remembers where it came from**. It
//! goes through the whole existing path — transport selection, the §5 elevation
//! gate, admission (including the circuit breaker), the worktree defaults — by
//! way of [`spawn::run_with_lineage`], because a retry that took a shortcut past
//! any of those gates would be the one spawn in the fleet that does not obey the
//! rules the others do.
//!
//! What this command adds on top is three refusals and an inheritance list.
//!
//! **The three refusals, in order, each independent, each exit 9** (they are
//! decided from the worker's own record plus config, and no broker appearing or
//! disappearing changes the answer):
//!
//! 1. `NOT_OWNER` — the same guard every mutating command runs, with the same
//!    `--force` escape. Asked BEFORE the state check so a caller with no
//!    business here gets the same answer whatever state the parent is in.
//! 2. `PARENT_ACTIVE` — a parent in `spawned`/`queued`/`running` is still
//!    working, and retrying it would put two workers on one brief in one
//!    workspace. `awaiting-input`, `escalated`, `orphaned` and all three
//!    terminal states ARE respawnable: a parked worker is exactly the case
//!    where "give up on this one and try again" is the right move, and forcing
//!    an operator to `kill` a blocked worker first would buy nothing.
//! 3. `ATTEMPTS_EXHAUSTED` — three identical retries is the poison-brief
//!    signature. The ceiling is `admission.max_attempts`; the refusal names the
//!    chain in `error.data` so a caller can branch on it, and the remedy points
//!    at the brief rather than at the counter.
//!
//! **The inheritance list is deliberately short:** `lane`, `cwd`, `owner`,
//! `model`, `effort`, `schema_path`, and the parent's own `brief.md` unless
//! `--brief` replaces it. Two entries carry an argument:
//!
//! * **`owner` is the PARENT's**, even under `--force`. A forced cross-owner
//!   respawn is a repair — somebody fixing another orchestrator's stuck work —
//!   and silently re-homing the retry onto the person who ran the command would
//!   make that repair look like a takeover in every later `ls`.
//! * **A replacement brief is INGRESS**, so it goes through the same front
//!   matter validation and the same secret scan every spawn's brief does. A
//!   respawn is not a way around §5.
//!
//! Two shapes of parent need translating rather than copying:
//!
//! * **Research**, whose `cwd` is its own `<worker>/scratch`. Handing that to
//!   the child would either be refused (`RESEARCH_CWD_FIXED`) or point the retry
//!   at the dead worker's directory, so the child passes no cwd at all and gets
//!   a fresh scratch (pinned).
//! * **A worktree worker**, whose `cwd` is the *worktree* — a path under
//!   `$A2A_HOME/worktrees/` that is not under any allowed root and belongs to a
//!   worker that is over. The child inherits the parent repo instead and the
//!   lane's worktree default gives it a tree of its own.

use crate::cli::SpawnArgs;
use crate::commands::owner;
use crate::commands::spawn::{self, Lineage};
use crate::config::{self, Lane};
use crate::fsm::WorkerState;
use crate::home::A2aHome;
use crate::ids;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use crate::state::WorkerSnapshot;
use chrono::Utc;
use serde_json::json;
use std::path::{Path, PathBuf};

pub fn run(
    home: &A2aHome,
    id_arg: &str,
    brief: Option<&Path>,
    force: bool,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let (cfg, _origins, warnings) = config::load(home)?;
    if !quiet {
        for w in &warnings {
            eprintln!("a2a: warning: {w}");
        }
    }
    // 1. Resolve + reconcile. A prefix is enough, exactly as everywhere else,
    //    and an ambiguous one is exit 3 with the candidates named.
    let parent_id = ids::resolve(home, id_arg)?;
    let parent_dir = home.worker_dir(&parent_id);
    let parent = reconcile(home, &cfg, &parent_id, Utc::now())?;

    // 2. Owner, then 3. liveness, then 4. the attempt ceiling.
    owner::guard(&parent_id, parent.owner.as_deref(), force)?;
    refuse_if_active(&parent_id, &parent)?;
    let attempt = next_attempt(&parent_id, &parent, cfg.admission.max_attempts)?;

    let args = inherited_args(&parent, &parent_dir, brief);
    let lineage = Lineage {
        parent_id: parent_id.clone(),
        attempt,
    };
    if !quiet {
        eprintln!(
            "a2a: respawning {parent_id} ({}) — attempt {attempt} of {}",
            parent.state.as_str(),
            cfg.admission.max_attempts
        );
    }
    spawn::run_with_lineage(home, &args, Some(lineage), json_mode, quiet).map(Outcome::from)
}

/// A parent that is still working is not a retry candidate. The three states
/// here are the ones with a live claim on the workspace; everything else —
/// `awaiting-input`, `escalated`, `orphaned` and the three terminal states — is
/// a legitimate "give up and try again".
fn refuse_if_active(id: &str, parent: &WorkerSnapshot) -> Result<(), AppError> {
    if !matches!(
        parent.state,
        WorkerState::Spawned | WorkerState::Queued | WorkerState::Running
    ) {
        return Ok(());
    }
    Err(AppError::new(
        "PARENT_ACTIVE",
        ExitCode::Precondition,
        format!(
            "worker {id} is {} — respawning it would put two workers on one brief",
            parent.state.as_str()
        ),
    )
    .remedy("wait for it, or `a2a kill` it first")
    .data(json!({"id": id, "state": parent.state.as_str()})))
}

/// `next = parent.attempt.unwrap_or(1) + 1`, refused once it passes the
/// ceiling. An ABSENT `attempt` is attempt 1 — every worker spawned before
/// lineage existed is the first try at its own brief.
fn next_attempt(id: &str, parent: &WorkerSnapshot, max_attempts: u64) -> Result<u32, AppError> {
    let attempt = parent.attempt.unwrap_or(1);
    let next = attempt.saturating_add(1);
    if u64::from(next) > max_attempts {
        return Err(AppError::new(
            "ATTEMPTS_EXHAUSTED",
            ExitCode::Precondition,
            format!(
                "worker {id} is attempt {attempt} of {max_attempts}: a fourth try at the same \
                 brief is not a retry, it is a loop"
            ),
        )
        .remedy("fix the brief — three identical retries is the poison-brief signature (spec §4.9)")
        .data(json!({
            "parent_id": id,
            "attempt": attempt,
            "max_attempts": max_attempts,
        })));
    }
    Ok(next)
}

/// The child's spawn arguments, derived from the parent's record.
///
/// Every field this does NOT set is deliberate: an elevation is never
/// inherited (§5 wants a fresh `--reason` every time), `--transport` is left to
/// resolve itself, and the worktree flags are left unset so the lane's own
/// default decides — a retry of an impl worker should get its own tree, not the
/// dead one's.
fn inherited_args(parent: &WorkerSnapshot, parent_dir: &Path, brief: Option<&Path>) -> SpawnArgs {
    SpawnArgs {
        lane: Some(parent.lane),
        cwd: inherited_cwd(parent),
        brief: Some(
            brief
                .map(Path::to_path_buf)
                .unwrap_or_else(|| parent_dir.join("brief.md")),
        ),
        // A retry re-runs a brief; it never asks for a scaffold. `--template`
        // short-circuits the spawn path before any of this is read, so the
        // neutral value here is the only one that is not a contradiction.
        template: None,
        // Context files were staged into the parent's own directory and are its
        // copies, not the caller's originals; a retry re-stages nothing rather
        // than inventing paths that may no longer exist.
        context: Vec::new(),
        // The parent's OWN copy of the schema, which is what it was judged
        // against — the caller's original may have moved or changed since.
        schema: parent.schema_path.clone(),
        model: Some(parent.model.clone()),
        effort: Some(parent.effort.clone()),
        transport: None,
        worktree: false,
        no_queue: false,
        // The PARENT's owner, not the caller's: a forced cross-owner respawn is
        // a repair, not a transfer.
        owner: parent.owner.clone(),
        no_worktree: false,
        base_ref: None,
        danger_full_access: false,
        reason: None,
    }
}

fn inherited_cwd(parent: &WorkerSnapshot) -> Option<PathBuf> {
    // Research always runs in its own scratch dir, and the child's does not
    // exist yet: pass nothing and let the spawn path make a fresh one.
    if parent.lane == Lane::Research {
        return None;
    }
    // A worktree worker's `cwd` IS its worktree — a path under `$A2A_HOME` that
    // no allowed root covers and that `gc` may already have taken. The repo it
    // was cut from is the workspace that outlives it.
    if let Some(wt) = &parent.worktree {
        return Some(wt.repo.clone());
    }
    Some(parent.cwd.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(state: WorkerState, attempt: Option<u32>) -> WorkerSnapshot {
        let mut s = WorkerSnapshot::new_for_test("general-0814t1300-aaaa", Path::new("/tmp"));
        s.state = state;
        s.attempt = attempt;
        s
    }

    /// The three live states are refused; everything a worker can be parked in
    /// is respawnable.
    #[test]
    fn only_a_working_parent_is_refused() {
        for state in [
            WorkerState::Spawned,
            WorkerState::Queued,
            WorkerState::Running,
        ] {
            let err = refuse_if_active("general-0814t1300-aaaa", &snap(state, None))
                .expect_err("{state} is still working");
            assert_eq!(err.code, "PARENT_ACTIVE");
            assert_eq!(err.exit, ExitCode::Precondition);
            assert!(err.remedy.unwrap().contains("a2a kill"));
        }
        for state in [
            WorkerState::AwaitingInput,
            WorkerState::Escalated,
            WorkerState::Orphaned,
            WorkerState::Done,
            WorkerState::Failed,
            WorkerState::Cancelled,
        ] {
            assert!(
                refuse_if_active("general-0814t1300-aaaa", &snap(state, None)).is_ok(),
                "{state:?} is a legitimate give-up-and-retry"
            );
        }
    }

    /// Absent ≡ 1, and the ceiling is on the NEXT attempt, not the parent's.
    #[test]
    fn the_chain_counts_from_one_and_stops_at_the_ceiling() {
        let id = "general-0814t1300-aaaa";
        assert_eq!(
            next_attempt(id, &snap(WorkerState::Done, None), 3).unwrap(),
            2
        );
        assert_eq!(
            next_attempt(id, &snap(WorkerState::Done, Some(1)), 3).unwrap(),
            2
        );
        assert_eq!(
            next_attempt(id, &snap(WorkerState::Done, Some(2)), 3).unwrap(),
            3
        );
        let err = next_attempt(id, &snap(WorkerState::Done, Some(3)), 3).expect_err("next = 4 > 3");
        assert_eq!(err.code, "ATTEMPTS_EXHAUSTED");
        assert_eq!(err.exit, ExitCode::Precondition);
        assert_eq!(
            err.data.unwrap(),
            json!({"parent_id": id, "attempt": 3, "max_attempts": 3})
        );
        // `max_attempts = 1` is a fleet with retries switched off, and it must
        // refuse the very first respawn rather than wrapping into one.
        assert_eq!(
            next_attempt(id, &snap(WorkerState::Done, None), 1)
                .expect_err("no retries at all")
                .code,
            "ATTEMPTS_EXHAUSTED"
        );
    }

    /// Research gets a fresh scratch; a worktree worker's retry inherits the
    /// repo, not the tree that is about to be reclaimed.
    #[test]
    fn the_two_translated_cwds() {
        let mut research = snap(WorkerState::Done, None);
        research.lane = Lane::Research;
        research.cwd = PathBuf::from("/home/w/general-0814t1300-aaaa/scratch");
        assert_eq!(inherited_cwd(&research), None);

        let mut plain = snap(WorkerState::Done, None);
        plain.cwd = PathBuf::from("/dev/proj");
        assert_eq!(inherited_cwd(&plain), Some(PathBuf::from("/dev/proj")));

        let mut wt = snap(WorkerState::Done, None);
        wt.cwd = PathBuf::from("/home/.a2a/worktrees/impl-0814t1300-aaaa");
        wt.worktree = Some(crate::state::WorktreeRef {
            path: wt.cwd.clone(),
            branch: "a2a/impl/impl-0814t1300-aaaa".into(),
            base_ref: "abc123".into(),
            repo: PathBuf::from("/dev/proj"),
        });
        assert_eq!(inherited_cwd(&wt), Some(PathBuf::from("/dev/proj")));
    }

    /// The inheritance list, pinned — including the one entry that is the
    /// PARENT's rather than the caller's.
    #[test]
    fn the_child_inherits_identity_and_the_parents_owner() {
        let mut parent = snap(WorkerState::Failed, Some(2));
        parent.lane = Lane::Impl;
        parent.cwd = PathBuf::from("/dev/proj");
        parent.owner = Some("alice".into());
        parent.model = "gpt-5.5-codex".into();
        parent.effort = "high".into();
        parent.schema_path = Some(PathBuf::from("/w/parent/schema.json"));

        let args = inherited_args(&parent, Path::new("/w/parent"), None);
        assert_eq!(args.lane, Some(Lane::Impl));
        assert_eq!(args.cwd, Some(PathBuf::from("/dev/proj")));
        assert_eq!(args.brief, Some(PathBuf::from("/w/parent/brief.md")));
        // A retry runs the brief; it never asks for a scaffold.
        assert_eq!(args.template, None);
        assert_eq!(args.owner.as_deref(), Some("alice"));
        assert_eq!(args.model.as_deref(), Some("gpt-5.5-codex"));
        assert_eq!(args.effort.as_deref(), Some("high"));
        assert_eq!(args.schema, Some(PathBuf::from("/w/parent/schema.json")));
        // Never inherited: an elevation needs a fresh `--reason` every time.
        assert!(!args.danger_full_access && args.reason.is_none());
        // The lane's own worktree default decides, so a retry gets its own tree.
        assert!(!args.worktree && !args.no_worktree);

        let replaced = inherited_args(
            &parent,
            Path::new("/w/parent"),
            Some(Path::new("/x/new.md")),
        );
        assert_eq!(replaced.brief, Some(PathBuf::from("/x/new.md")));
    }
}
