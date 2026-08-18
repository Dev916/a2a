//! Mirroring Claude-side capability into the Codex home (spec §6).
//!
//! Two things make this module safe to point at somebody's real machine, and
//! both are structural rather than disciplinary:
//!
//! * **Plan and apply are separate.** [`Plan`] is what a planner returns from
//!   reading (source tree, destination tree, manifest, config); only the
//!   appliers write. `--dry-run` runs the planner and prints it, so
//!   "dry-run has no side effects" is a fact about the call graph rather than a
//!   rule somebody has to remember.
//! * **Ownership is a ledger, not an inference.** [`manifest`] records what
//!   a2a created; anything else in the destination is somebody else's and is
//!   skipped, never updated and never removed.
//!
//! Everything in *this* file is a pure function of its arguments — no `std::env`
//! read, no clock, no filesystem beyond canonicalizing a path that already
//! exists. The single real environment read lives in `commands::sync_cmd`,
//! which is what lets all four precedence levels below be ordinary unit tests
//! instead of `set_var` in a shared test binary.

pub mod manifest;

use crate::output::{AppError, ExitCode};
use serde_json::json;
use std::path::{Path, PathBuf};

/// What an entry — in a [`Plan`] or in the manifest — is about.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Skill,
    Mcp,
}

impl Kind {
    /// The wire spelling, and the first field of a manifest key.
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Skill => "skill",
            Kind::Mcp => "mcp",
        }
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a run did (or refused to do) about one candidate.
///
/// The variant set is deliberately wide: "skipped" alone would be true of a
/// curated collision, a credential-carrying server and a directory that is not
/// a skill at all, and an operator reading a report needs to know which of
/// those three happened without re-deriving it from prose.
#[derive(serde::Serialize, PartialEq, Eq, Clone, Copy, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum Disposition {
    Synced,
    Unchanged,
    Removed,
    SkippedSideEffecting,
    SkippedCollision,
    SkippedNotASkill,
    SkippedUnowned,
    SkippedSecret,
    SkippedUntranslatable,
    RefusedDestDrift,
}

impl Disposition {
    /// The report spelling — identical to the serde one, which a unit test
    /// pins so the human line and the `--json` payload can never drift apart.
    pub fn as_str(self) -> &'static str {
        match self {
            Disposition::Synced => "synced",
            Disposition::Unchanged => "unchanged",
            Disposition::Removed => "removed",
            Disposition::SkippedSideEffecting => "skipped-side-effecting",
            Disposition::SkippedCollision => "skipped-collision",
            Disposition::SkippedNotASkill => "skipped-not-a-skill",
            Disposition::SkippedUnowned => "skipped-unowned",
            Disposition::SkippedSecret => "skipped-secret",
            Disposition::SkippedUntranslatable => "skipped-untranslatable",
            Disposition::RefusedDestDrift => "refused-dest-drift",
        }
    }
}

impl std::fmt::Display for Disposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One candidate's verdict.
#[derive(serde::Serialize, PartialEq, Clone, Debug)]
pub struct PlanEntry {
    pub name: String,
    pub kind: Kind,
    pub account: String,
    /// `None` for an entry whose source is gone (a [`Disposition::Removed`]).
    pub source_path: Option<PathBuf>,
    pub dest_path: PathBuf,
    pub disposition: Disposition,
    /// Human sentence. NEVER carries a value read from a source config — the
    /// whole report flows straight back into agent context, so this may name
    /// keys, header names, env var names and line numbers, and nothing else.
    pub detail: String,
}

/// Everything a run would do, decided before anything is written.
///
/// `PartialEq` is load-bearing rather than convenience: the planner-purity test
/// compares two whole plans, and a `Debug`-string comparison would be a weaker
/// assertion that happens to pass for the wrong reasons.
#[derive(Default, Clone, Debug, PartialEq, serde::Serialize)]
pub struct Plan {
    pub entries: Vec<PlanEntry>,
    pub warnings: Vec<String>,
}

impl Plan {
    /// `{synced, unchanged, removed, skipped, refused}` — the five pinned keys
    /// of a report's `counts` object.
    ///
    /// The verdict of a run travels HERE, not in the exit code: a fleet-wide
    /// `sync` that exited nonzero because one destination had drifted would be
    /// re-run with a bypass by every caller, which is exactly the clobber the
    /// manifest exists to prevent.
    pub fn counts(&self) -> serde_json::Value {
        let (mut synced, mut unchanged, mut removed, mut skipped, mut refused) = (0, 0, 0, 0, 0);
        for e in &self.entries {
            match e.disposition {
                Disposition::Synced => synced += 1,
                Disposition::Unchanged => unchanged += 1,
                Disposition::Removed => removed += 1,
                Disposition::RefusedDestDrift => refused += 1,
                Disposition::SkippedSideEffecting
                | Disposition::SkippedCollision
                | Disposition::SkippedNotASkill
                | Disposition::SkippedUnowned
                | Disposition::SkippedSecret
                | Disposition::SkippedUntranslatable => skipped += 1,
            }
        }
        json!({"synced": synced, "unchanged": unchanged, "removed": removed,
               "skipped": skipped, "refused": refused})
    }
}

/// The account label every SKILL entry carries, whatever `--claude-config-dir`
/// said.
///
/// Spec line 86 pins that dual Claude accounts *share* `~/.claude/skills`, so
/// per-account namespacing is an MCP-only fact. A second account syncing skills
/// must resolve to the SAME manifest keys as the first — a parallel key set
/// would have the two accounts fighting over one flat destination tree, each
/// one certain it owned nothing the other had made.
pub const SKILL_ACCOUNT: &str = "default";

/// Where the Claude side lives: `--claude-config-dir` > `[sync]
/// claude_config_dir` (which the config layer has already merged with
/// `A2A_SYNC_CLAUDE_CONFIG_DIR`) > `$CLAUDE_CONFIG_DIR` > `$HOME`.
pub fn claude_config_dir(
    cfg: &crate::config::SyncConfig,
    flag: Option<&Path>,
    env_dir: Option<&Path>,
    home: &Path,
) -> PathBuf {
    flag.map(PathBuf::from)
        .or_else(|| cfg.claude_config_dir.clone())
        .or_else(|| env_dir.map(PathBuf::from))
        .unwrap_or_else(|| home.to_path_buf())
}

/// `<dir>/.claude/skills` when `dir` IS the home, else `<dir>/skills`.
///
/// The two branches exist because `sync skills` and `sync mcp` read different
/// things out of the same directory. The home branch is what makes the default
/// account resolve to the real `~/.claude/skills`; the other branch is what
/// lets a test point `--claude-config-dir` at a fixture directory and have
/// `skills/**` be the source with no `.claude/` level in between. (`sync mcp`'s
/// source is `<dir>/.claude.json` in both branches.)
pub fn skills_source_root(dir: &Path, home: &Path) -> PathBuf {
    if same_path(dir, home) {
        dir.join(".claude").join("skills")
    } else {
        dir.join("skills")
    }
}

/// `"default"` for the home itself, else the directory's basename with a single
/// leading `.` stripped (`~/.claude-revenium` → `claude-revenium`).
///
/// A path with no basename at all (`/`) is reported as `"default"`: it is not a
/// second account, and inventing a label for it would put an unreadable key in
/// the ledger.
pub fn account_label(dir: &Path, home: &Path) -> String {
    if same_path(dir, home) {
        return SKILL_ACCOUNT.to_string();
    }
    match dir.file_name().map(|n| n.to_string_lossy().into_owned()) {
        Some(name) => name.strip_prefix('.').unwrap_or(&name).to_string(),
        None => SKILL_ACCOUNT.to_string(),
    }
}

/// Refuse to sync into the machine's real home unless somebody opted in.
///
/// `tests/common::a2a()` sets neither `CODEX_HOME` nor any Claude pointer, so a
/// sync test that forgets one env var would otherwise write into the
/// developer's real `~/.codex` submodule — a mistake nothing on disk would
/// undo. This turns that mistake into a red test with a legible reason.
/// Called once, immediately after resolution and BEFORE any read or write.
pub fn refuse_real_home(
    codex_home: &Path,
    claude_dir: &Path,
    home: &Path,
    opt_in: bool,
) -> Result<(), AppError> {
    if opt_in || !(under(codex_home, home) || under(claude_dir, home)) {
        return Ok(());
    }
    Err(AppError::new(
        "SYNC_REFUSES_REAL_HOME",
        ExitCode::Precondition,
        format!(
            "refusing to sync the real home {}: codex home {}, claude config dir {}",
            home.display(),
            codex_home.display(),
            claude_dir.display()
        ),
    )
    .remedy(
        "point CODEX_HOME and --claude-config-dir at a scratch directory, or set \
         A2A_SYNC_ALLOW_REAL_HOME=1 to sync this machine on purpose",
    ))
}

/// `p` is `home` or lives under it — compared both as written and canonicalized,
/// so a path that does not exist yet (`$HOME/.codex` on a fresh machine) is
/// still recognized, and a symlinked home (`/tmp` → `/private/tmp`) cannot be
/// used to walk around the guard.
fn under(p: &Path, home: &Path) -> bool {
    if p == home || p.starts_with(home) {
        return true;
    }
    let (rp, rh) = (real(p), real(home));
    rp == rh || rp.starts_with(&rh)
}

/// Same directory, symlinks resolved where the path exists; a path that does
/// not exist is compared as written rather than declared different.
fn same_path(a: &Path, b: &Path) -> bool {
    a == b || real(a) == real(b)
}

fn real(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SyncConfig;

    fn cfg_with(dir: Option<&str>) -> SyncConfig {
        SyncConfig {
            claude_config_dir: dir.map(PathBuf::from),
            ..Default::default()
        }
    }

    /// All four levels, with no `std::env::set_var` anywhere: every environment
    /// fact this function needs is a parameter, which is the whole reason it
    /// takes four of them.
    #[test]
    fn the_claude_dir_is_flag_then_config_then_env_then_home() {
        let home = Path::new("/home/mike");
        let flag = Path::new("/flag/dir");
        let env = Path::new("/env/dir");

        assert_eq!(
            claude_config_dir(&cfg_with(Some("/cfg/dir")), Some(flag), Some(env), home),
            PathBuf::from("/flag/dir")
        );
        assert_eq!(
            claude_config_dir(&cfg_with(Some("/cfg/dir")), None, Some(env), home),
            PathBuf::from("/cfg/dir")
        );
        assert_eq!(
            claude_config_dir(&cfg_with(None), None, Some(env), home),
            PathBuf::from("/env/dir")
        );
        assert_eq!(
            claude_config_dir(&cfg_with(None), None, None, home),
            PathBuf::from("/home/mike")
        );
    }

    #[test]
    fn the_skills_source_root_has_a_home_branch_and_a_fixture_branch() {
        let home = Path::new("/home/mike");
        assert_eq!(
            skills_source_root(home, home),
            PathBuf::from("/home/mike/.claude/skills")
        );
        assert_eq!(
            skills_source_root(Path::new("/fixtures/sync"), home),
            PathBuf::from("/fixtures/sync/skills"),
            "a fixture dir has no .claude/ level"
        );
        assert_eq!(
            skills_source_root(Path::new("/home/mike/.claude-revenium"), home),
            PathBuf::from("/home/mike/.claude-revenium/skills")
        );
    }

    #[test]
    fn the_account_label_is_default_for_home_and_a_stripped_basename_otherwise() {
        let home = Path::new("/home/mike");
        assert_eq!(account_label(home, home), "default");
        assert_eq!(
            account_label(Path::new("/home/mike/.claude-revenium"), home),
            "claude-revenium",
            "one leading dot is stripped, the rest of the name is not touched"
        );
        assert_eq!(
            account_label(Path::new("/fixtures/sync"), home),
            "sync",
            "a fixture dir is its own account"
        );
        assert_eq!(account_label(Path::new("/"), home), "default");
    }

    /// The asymmetry, pinned: a second Claude account gets its own MCP
    /// namespace but shares the ONE flat skills tree, so skill keys must not
    /// move when `--claude-config-dir` does.
    #[test]
    fn skill_entries_stay_on_the_default_account_even_for_a_second_account() {
        let home = Path::new("/home/mike");
        let second = Path::new("/home/mike/.claude-revenium");
        assert_eq!(account_label(second, home), "claude-revenium");
        assert_eq!(SKILL_ACCOUNT, "default");
        assert_eq!(
            manifest::Manifest::key(Kind::Skill, SKILL_ACCOUNT, "techniques"),
            "skill:default:techniques",
            "the same skill key whichever account asked for the sync"
        );
        assert_eq!(
            manifest::Manifest::key(Kind::Mcp, &account_label(second, home), "linear"),
            "mcp:claude-revenium:linear",
            "mcp entries DO namespace by account"
        );
    }

    #[test]
    fn the_real_home_guard_covers_four_combinations() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let scratch = tmp.path().join("scratch");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&scratch).unwrap();
        let outside_codex = scratch.join("codex");
        let outside_claude = scratch.join("claude");
        let inside_codex = home.join(".codex");
        let inside_claude = home.join(".claude-revenium");

        refuse_real_home(&outside_codex, &outside_claude, &home, false)
            .expect("both outside the home is the ordinary case");

        let err = refuse_real_home(&inside_codex, &outside_claude, &home, false).unwrap_err();
        assert_eq!(err.code, "SYNC_REFUSES_REAL_HOME");
        assert_eq!(err.exit, ExitCode::Precondition);
        assert!(err.message.contains(&*inside_codex.to_string_lossy()));
        assert!(
            err.message.contains(&*outside_claude.to_string_lossy()),
            "both resolved paths are named: {}",
            err.message
        );

        let err = refuse_real_home(&outside_codex, &inside_claude, &home, false).unwrap_err();
        assert_eq!(err.code, "SYNC_REFUSES_REAL_HOME");

        refuse_real_home(&inside_codex, &inside_claude, &home, true)
            .expect("the opt-in is the one way through");

        // The home ITSELF, not merely a descendant.
        assert!(refuse_real_home(&home, &outside_claude, &home, false).is_err());
    }

    /// Every disposition lands in exactly one of the five report buckets — a
    /// new variant that nobody counted would otherwise vanish from the summary.
    #[test]
    fn counts_bucket_every_disposition() {
        let all = [
            Disposition::Synced,
            Disposition::Unchanged,
            Disposition::Removed,
            Disposition::SkippedSideEffecting,
            Disposition::SkippedCollision,
            Disposition::SkippedNotASkill,
            Disposition::SkippedUnowned,
            Disposition::SkippedSecret,
            Disposition::SkippedUntranslatable,
            Disposition::RefusedDestDrift,
        ];
        let plan = Plan {
            entries: all
                .iter()
                .map(|d| PlanEntry {
                    name: d.as_str().to_string(),
                    kind: Kind::Skill,
                    account: SKILL_ACCOUNT.to_string(),
                    source_path: None,
                    dest_path: PathBuf::from("/dest"),
                    disposition: *d,
                    detail: String::new(),
                })
                .collect(),
            warnings: vec![],
        };
        let counts = plan.counts();
        assert_eq!(counts["synced"], 1);
        assert_eq!(counts["unchanged"], 1);
        assert_eq!(counts["removed"], 1);
        assert_eq!(counts["skipped"], 6);
        assert_eq!(counts["refused"], 1);
        let total: u64 = ["synced", "unchanged", "removed", "skipped", "refused"]
            .iter()
            .map(|k| counts[k].as_u64().unwrap())
            .sum();
        assert_eq!(total as usize, all.len(), "nothing falls out of the summary");
        assert_eq!(Plan::default().counts()["synced"], 0);
    }

    /// The human report and the `--json` payload spell a disposition the same
    /// way, forever.
    #[test]
    fn disposition_strings_match_their_serde_spelling() {
        for d in [
            Disposition::Synced,
            Disposition::SkippedSideEffecting,
            Disposition::SkippedNotASkill,
            Disposition::RefusedDestDrift,
        ] {
            assert_eq!(serde_json::to_value(d).unwrap(), d.as_str());
        }
        assert_eq!(Disposition::SkippedSideEffecting.to_string(), "skipped-side-effecting");
        assert_eq!(serde_json::to_value(Kind::Skill).unwrap(), "skill");
        assert_eq!(serde_json::to_value(Kind::Mcp).unwrap(), "mcp");
    }
}
