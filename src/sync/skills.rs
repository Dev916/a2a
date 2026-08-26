//! Which Claude skills may be mirrored into approval-free workers, and what a
//! run would do about each candidate (spec §6.1).
//!
//! Two decisions live here and nowhere else.
//!
//! **Classification is deny-by-default and shallow on purpose.** A worker that
//! runs without an approval prompt is exactly as dangerous as the most
//! dangerous skill it can reach, so a name on [`SEED_SIDE_EFFECTING`] is
//! refused outright and everything else is read through a heuristic scoped to
//! the YAML front matter — the skill's own one-line description of itself.
//! Scanning bodies was tried on the real tree and flagged nearly everything: a
//! guide that mentions `deploy` in an example is not a deploy tool, and a
//! heuristic that fires on every skill is one an operator learns to click
//! through. The only way past a refusal is `[sync] allow_skills`, which is a
//! human writing a name down.
//!
//! **Planning is reading.** [`plan`] opens files, hashes trees and returns a
//! [`Plan`]; it never writes, never shells out and never consults the clock, so
//! `--dry-run` is side-effect-free as a fact about the call graph rather than
//! as a rule somebody remembers. The appliers (Task 3) are the only writers.

use super::manifest::{hash_path, Manifest, IGNORED_ENTRIES};
use super::{Disposition, Kind, Plan, PlanEntry};
use crate::config::SyncConfig;
use crate::output::{AppError, ExitCode};
use std::collections::BTreeSet;
use std::path::Path;

/// Names refused before any heuristic runs, sorted.
///
/// Every one of these reaches something outside the machine — a Slack
/// workspace, a signature request, an AWS account, somebody's calendar — or
/// holds a credential to do so. The list is seed rather than exhaustive: it
/// exists so that the day a new skill of this shape appears, the failure mode
/// is "a2a did not mirror it" rather than "a2a handed it to a worker that
/// cannot be asked".
pub const SEED_SIDE_EFFECTING: [&str; 15] = [
    "autologin",
    "buildkite",
    "cadmv-db-refresh",
    "cadmv-grant-access",
    "devtime",
    "dmv-keys",
    "docusign",
    "meetnotes",
    "notion-cli",
    "pr",
    "revenium-enterprise-deploy",
    "slack-cli",
    "slack-login",
    "slack-send",
    "x-cli",
];

/// Words that, in a skill's front matter or its directory name, say the skill
/// acts on the world rather than reading it.
///
/// Matched case-insensitively on ASCII-alphanumeric word boundaries — NOT as
/// substrings. The two readings genuinely differ (`send` inside "sender",
/// `token` inside "tokenizer", `post` inside "postgres", `aws` inside "laws"),
/// and a substring reading would quietly condemn half the corpus while reading
/// as though it were being careful. `-`, `_`, `/` and punctuation are all
/// boundaries, so `slack-send` and `aws_profile` still fire.
pub const SIDE_EFFECT_VOCABULARY: [&str; 7] = [
    "credential",
    "send",
    "post",
    "deploy",
    "password",
    "token",
    "aws",
];

/// First line of the managed `.gitignore` block.
pub const BEGIN_MARK: &str = "# BEGIN a2a-sync";
/// Last line of the managed `.gitignore` block.
pub const END_MARK: &str = "# END a2a-sync";

/// Why a candidate is (or is not) safe to mirror into approval-free workers.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Classification {
    /// Nothing in the name or the front matter says this skill acts.
    ReadOnly,
    /// It acts, and nobody has said otherwise. `reason` names the rule that
    /// fired — a key, a word, a list — and never quotes a credential.
    SideEffecting { reason: String },
    /// It acts, and `[sync] allow_skills` names it anyway.
    Allowlisted,
    /// Not a skill at all: a plain file, or a directory with no `SKILL.md`.
    NotASkill { reason: String },
}

/// Pinned report details. They are constants because Task 3 asserts them
/// verbatim and an operator reads them far more often than this file.
const COLLISION_DETAIL: &str =
    "curated or foreign destination wins; a2a only ever updates what it created";
const DRIFT_DETAIL: &str = "destination was edited after the last sync; a2a will not overwrite it";
const REMOVAL_DRIFT_DETAIL: &str =
    "destination was edited after the last sync; a2a will not remove it";

/// The classification half of the pinned order: **not-a-skill → side-effecting
/// → read-only**. (Collision, drift and unchanged are facts about the
/// destination, so they belong to [`plan`], which is the only thing that has
/// seen one.)
pub fn classify(name: &str, skill_md: Option<&str>, allow_skills: &[String]) -> Classification {
    let Some(md) = skill_md else {
        return Classification::NotASkill {
            reason: "no SKILL.md".to_string(),
        };
    };
    match side_effect_reason(name, md) {
        None => Classification::ReadOnly,
        Some(_) if allow_skills.iter().any(|s| s == name) => Classification::Allowlisted,
        Some(reason) => Classification::SideEffecting { reason },
    }
}

/// The seed list first, then the name, then the front matter — so the reason an
/// operator reads is the most specific true one.
fn side_effect_reason(name: &str, skill_md: &str) -> Option<String> {
    if SEED_SIDE_EFFECTING.contains(&name) {
        return Some("seed list".to_string());
    }
    for word in SIDE_EFFECT_VOCABULARY {
        if contains_word(name, word) {
            return Some(format!("name matched `{word}`"));
        }
    }
    let front = front_matter(skill_md);
    for word in SIDE_EFFECT_VOCABULARY {
        if contains_word(front, word) {
            return Some(format!("front matter matched `{word}`"));
        }
    }
    None
}

/// The text between the first two `---` lines, or `""` when there is no
/// well-formed front matter (including an opened-but-never-closed one, which is
/// a broken document rather than a licence to scan the body).
fn front_matter(md: &str) -> &str {
    let after_open = match md.strip_prefix("---") {
        Some(rest) if rest.starts_with('\n') => &rest[1..],
        Some(rest) if rest.starts_with("\r\n") => &rest[2..],
        _ => return "",
    };
    let mut offset = 0usize;
    for line in after_open.split_inclusive('\n') {
        if line.trim_end() == "---" {
            return &after_open[..offset];
        }
        offset += line.len();
    }
    ""
}

/// `word` appears in `haystack` bounded by non-alphanumerics on both sides,
/// compared case-insensitively. `word` is ASCII (every entry in
/// [`SIDE_EFFECT_VOCABULARY`] is), so every index below lands on a character
/// boundary and a UTF-8 continuation byte counts as a boundary, which it is.
fn contains_word(haystack: &str, word: &str) -> bool {
    let hay = haystack.to_ascii_lowercase();
    let bytes = hay.as_bytes();
    let mut from = 0;
    while let Some(found) = hay[from..].find(word) {
        let start = from + found;
        let end = start + word.len();
        let open = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
        let close = end == bytes.len() || !bytes[end].is_ascii_alphanumeric();
        if open && close {
            return true;
        }
        from = start + 1;
    }
    false
}

/// What a `sync skills` run would do, decided entirely by reading.
///
/// `dest_root` is `<CODEX_HOME>/skills`. The order of the decision is pinned
/// and total — **not-a-skill → collision → side-effecting → drift → unchanged →
/// synced** — because every one of those is a different sentence to an
/// operator, and a candidate that could be two of them at once (a curated
/// destination whose source is on the seed list, say) must always be reported
/// as the same one.
pub fn plan(
    source_root: &Path,
    dest_root: &Path,
    manifest: &Manifest,
    cfg: &SyncConfig,
    account: &str,
) -> Result<Plan, AppError> {
    let mut listed: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(source_root).map_err(|e| source_missing(source_root, e))? {
        let entry = entry.map_err(|e| source_missing(source_root, e))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        // A `.DS_Store` beside somebody's skills is not a candidate that got
        // skipped, it is not a candidate: reporting it would train an operator
        // to skim the skip lines.
        if !IGNORED_ENTRIES.contains(&name.as_str()) {
            listed.push(name);
        }
    }
    // `read_dir` order is whatever the filesystem felt like; the plan two runs
    // produce has to be the same value, so sort.
    listed.sort();

    let mut plan = Plan::default();
    for name in &listed {
        let source = source_root.join(name);
        let dest = dest_root.join(name);
        let mut push = |disposition, detail: &str| {
            plan.entries.push(PlanEntry {
                name: name.clone(),
                kind: Kind::Skill,
                account: account.to_string(),
                // The link as GIVEN, not its target: the ledger's job is to say
                // what a2a was pointed at.
                source_path: Some(source.clone()),
                dest_path: dest.clone(),
                disposition,
                detail: detail.to_string(),
            })
        };

        // `metadata` follows symlinks, which is the whole point: a symlinked
        // skill is planned as an ordinary directory and copied as contents.
        if !std::fs::metadata(&source)
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            push(Disposition::SkippedNotASkill, "not a directory");
            continue;
        }
        let classification = classify(name, read_skill_md(&source).as_deref(), &cfg.allow_skills);
        if let Classification::NotASkill { reason } = &classification {
            push(Disposition::SkippedNotASkill, reason);
            continue;
        }

        let owned = manifest.owns(Kind::Skill, account, name);
        // Collision outranks the heuristic: somebody else's directory is not
        // ours to classify, and "skipped-side-effecting" would send an operator
        // to `allow_skills` for a problem the allowlist cannot fix.
        if owned.is_none() && exists(&dest) {
            push(Disposition::SkippedCollision, COLLISION_DETAIL);
            continue;
        }
        if let Classification::SideEffecting { reason } = &classification {
            push(Disposition::SkippedSideEffecting, reason);
            continue;
        }

        let Some(entry) = owned else {
            push(Disposition::Synced, "new managed skill");
            continue;
        };
        if !exists(&dest) {
            // Ours, and gone. Not drift — there is nothing to overwrite.
            push(
                Disposition::Synced,
                "the managed destination is missing and will be recreated",
            );
        } else if hash_path(&dest).ok().as_deref() != Some(entry.dest_hash.as_str()) {
            push(Disposition::RefusedDestDrift, DRIFT_DETAIL);
        } else if hash_path(&source).ok().as_deref() == Some(entry.source_hash.as_str()) {
            push(
                Disposition::Unchanged,
                "source and destination both match the ledger",
            );
        } else {
            push(Disposition::Synced, "source changed since the last sync");
        }
    }

    plan_removals(&listed, manifest, account, &mut plan);
    Ok(plan)
}

/// Entries the ledger owns whose source has gone away. Iteration is over a
/// `BTreeMap` keyed `skill:<account>:<name>`, so this appends in name order.
fn plan_removals(listed: &[String], manifest: &Manifest, account: &str, plan: &mut Plan) {
    for (_, entry) in manifest.entries_for(Kind::Skill) {
        if entry.account != account {
            continue;
        }
        let Some(name) = entry
            .dest_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
        else {
            continue;
        };
        if listed.contains(&name) {
            continue; // already decided against the live source above
        }
        if exists(&entry.source_path) {
            // The ledger points somewhere this run is not looking — a source
            // root that moved, most likely. Doing nothing is the only safe
            // reading: "I cannot see it" is not "it was deleted".
            plan.warnings.push(format!(
                "ledger entry for {name} points at {}, which is outside this run's source root; left alone",
                entry.source_path.display()
            ));
            continue;
        }
        let (disposition, detail) = if !exists(&entry.dest_path) {
            (
                Disposition::Removed,
                "source is gone; the managed destination is already gone",
            )
        } else if hash_path(&entry.dest_path).ok().as_deref() != Some(entry.dest_hash.as_str()) {
            // A user's edits are never deleted on the strength of a deletion
            // somewhere else.
            (Disposition::RefusedDestDrift, REMOVAL_DRIFT_DETAIL)
        } else {
            (
                Disposition::Removed,
                "source is gone; the managed destination is removed",
            )
        };
        plan.entries.push(PlanEntry {
            name,
            kind: Kind::Skill,
            account: account.to_string(),
            source_path: None,
            dest_path: entry.dest_path.clone(),
            disposition,
            detail: detail.to_string(),
        });
    }
}

/// `SKILL.md`'s text, read lossily: this feeds a heuristic over ASCII words, so
/// a stray byte in somebody's description must not turn their skill into a
/// "no SKILL.md" skip. (Hashing stays raw-byte — see
/// [`crate::sync::manifest::hash_path`].)
fn read_skill_md(source: &Path) -> Option<String> {
    let path = source.join("SKILL.md");
    if !path.is_file() {
        return None;
    }
    std::fs::read(&path)
        .ok()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// Present in any form, including a broken symlink — which is emphatically
/// something a2a must not write over.
fn exists(p: &Path) -> bool {
    std::fs::symlink_metadata(p).is_ok()
}

fn source_missing(root: &Path, e: std::io::Error) -> AppError {
    AppError::new(
        "SYNC_SKILLS_SOURCE_MISSING",
        ExitCode::Precondition,
        format!("{}: {e}", root.display()),
    )
    .remedy("point --claude-config-dir at a directory that holds a skills/ tree")
}

/// Every destination directory the ledger tracks — across ALL accounts — plus
/// every one this plan would keep or create: the exact set the managed
/// `.gitignore` block must cover before a single byte is written.
///
/// There is deliberately no `account` parameter. Skills land flat in the one
/// shared `<CODEX_HOME>/skills` and every skill entry is written under
/// [`crate::sync::SKILL_ACCOUNT`], so an account filter here would be either
/// dead or actively harmful: a second account's run would rewrite the one
/// managed block without the first account's directories, uncovering them,
/// turning the submodule dirty with no test to catch it.
pub fn managed_dirs(plan: &Plan, manifest: &Manifest) -> Vec<String> {
    let mut dirs: BTreeSet<String> = BTreeSet::new();
    let insert = |p: &Path, out: &mut BTreeSet<String>| {
        if let Some(n) = p.file_name() {
            out.insert(n.to_string_lossy().into_owned());
        }
    };
    for (_, entry) in manifest.entries_for(Kind::Skill) {
        insert(&entry.dest_path, &mut dirs);
    }
    for entry in &plan.entries {
        if matches!(
            entry.disposition,
            Disposition::Synced | Disposition::Unchanged
        ) {
            insert(&entry.dest_path, &mut dirs);
        }
    }
    dirs.into_iter().collect()
}

/// The managed block, sorted, one `/<dir>/` line per managed directory.
pub fn managed_block(dirs: &[String]) -> String {
    let unique: BTreeSet<&String> = dirs.iter().collect();
    let mut out = String::from(BEGIN_MARK);
    out.push('\n');
    for dir in unique {
        out.push('/');
        out.push_str(dir);
        out.push_str("/\n");
    }
    out.push_str(END_MARK);
    out.push('\n');
    out
}

/// True only for a well-formed block — exactly one [`BEGIN_MARK`], exactly one
/// [`END_MARK`] after it — that carries a line for every directory.
///
/// Malformed is false rather than "close enough": this is the check that runs
/// after the block is written and before any content is copied, and the whole
/// point of it is that a file somebody hand-edited into an ambiguous state does
/// not get to authorize writes into a git repository.
pub fn block_covers(gitignore: &str, dirs: &[String]) -> bool {
    let (mut begins, mut ends, mut inside, mut in_block) = (0, 0, Vec::new(), false);
    for line in gitignore.lines() {
        let trimmed = line.trim();
        if trimmed == BEGIN_MARK {
            begins += 1;
            in_block = true;
        } else if trimmed == END_MARK {
            if !in_block {
                return false; // an END before its BEGIN is not a block
            }
            ends += 1;
            in_block = false;
        } else if in_block {
            inside.push(trimmed.to_string());
        }
    }
    if begins != 1 || ends != 1 || in_block {
        return false;
    }
    dirs.iter()
        .all(|dir| inside.iter().any(|line| *line == format!("/{dir}/")))
}

/// Replace the managed block in place, preserving every line outside it, or
/// append one after a single blank line when there is none.
///
/// Duplicate or unterminated markers collapse to exactly one block: a file that
/// accumulated two of them (two runs, an interrupted edit) has to converge on
/// something, and leaving both would make [`block_covers`] false forever, which
/// would refuse every future sync.
pub fn rewrite_block(gitignore: &str, dirs: &[String]) -> String {
    let rendered = managed_block(dirs);
    let block = rendered.lines().map(str::to_string);

    let mut kept: Vec<String> = Vec::new();
    let mut insert_at: Option<usize> = None;
    let mut in_block = false;
    for line in gitignore.lines() {
        let trimmed = line.trim();
        if trimmed == BEGIN_MARK {
            insert_at.get_or_insert(kept.len());
            in_block = true;
        } else if trimmed == END_MARK {
            in_block = false; // a stray END is dropped with the rest
        } else if !in_block {
            kept.push(line.to_string());
        }
    }

    match insert_at {
        Some(at) => kept.splice(at..at, block),
        None => {
            // Exactly one blank line between whatever was there and the block.
            while kept.last().is_some_and(|l| l.trim().is_empty()) {
                kept.pop();
            }
            if !kept.is_empty() {
                kept.push(String::new());
            }
            kept.splice(kept.len().., block)
        }
    };

    let mut out = kept.join("\n");
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn skill_md(front: &str, body: &str) -> String {
        format!("---\n{front}\n---\n\n{body}\n")
    }

    /// The list is a pin: sorted, fifteen long, spelled the way the real
    /// `~/.claude/skills` spells them.
    #[test]
    fn the_seed_list_is_pinned_verbatim_and_sorted() {
        assert_eq!(SEED_SIDE_EFFECTING.len(), 15);
        assert_eq!(
            SEED_SIDE_EFFECTING,
            [
                "autologin",
                "buildkite",
                "cadmv-db-refresh",
                "cadmv-grant-access",
                "devtime",
                "dmv-keys",
                "docusign",
                "meetnotes",
                "notion-cli",
                "pr",
                "revenium-enterprise-deploy",
                "slack-cli",
                "slack-login",
                "slack-send",
                "x-cli",
            ]
        );
        let mut sorted = SEED_SIDE_EFFECTING;
        sorted.sort_unstable();
        assert_eq!(
            sorted, SEED_SIDE_EFFECTING,
            "kept sorted so a reader can scan it"
        );
        assert_eq!(
            SIDE_EFFECT_VOCABULARY,
            [
                "credential",
                "send",
                "post",
                "deploy",
                "password",
                "token",
                "aws"
            ]
        );
    }

    /// A document that *describes* dangerous operations is not a dangerous
    /// operation — which is the entire reason the heuristic is front-matter
    /// scoped.
    #[test]
    fn a_body_full_of_vocabulary_is_read_only_and_one_front_matter_word_flips_it() {
        let body = "credential send post deploy password token aws";
        let clean = skill_md("name: reader\ndescription: Explains things.", body);
        assert_eq!(
            classify("reader", Some(&clean), &[]),
            Classification::ReadOnly,
            "all seven words in the body change nothing"
        );

        let dirty = skill_md("name: reader\ndescription: Rotates a token for you.", body);
        assert_eq!(
            classify("reader", Some(&dirty), &[]),
            Classification::SideEffecting {
                reason: "front matter matched `token`".to_string()
            },
            "one word in the front matter is the whole difference"
        );
    }

    /// Word boundaries, not substrings: both readings are defensible until you
    /// notice that the substring one condemns anything mentioning postgres.
    #[test]
    fn the_vocabulary_matches_words_not_substrings() {
        let near_misses = skill_md(
            "name: notes\ndescription: Reads postgres, a sender column and a tokenizer, plus lawsuits.",
            "nothing here",
        );
        assert_eq!(
            classify("notes", Some(&near_misses), &[]),
            Classification::ReadOnly,
            "postgres/sender/tokenizer/lawsuit are not the vocabulary"
        );

        let hyphenated = skill_md(
            "name: notes\ndescription: Wraps slack-send for other skills.",
            "nothing here",
        );
        assert_eq!(
            classify("notes", Some(&hyphenated), &[]),
            Classification::SideEffecting {
                reason: "front matter matched `send`".to_string()
            },
            "a hyphen is a boundary, so slack-send fires"
        );
        assert!(contains_word("aws_profile", "aws"), "so is an underscore");
        assert!(!contains_word("laws", "aws"));
        assert!(!contains_word("", "aws"));
    }

    /// The three reasons a skill can be refused, each naming the rule that
    /// fired — an operator who cannot tell "seed list" from "front matter"
    /// cannot tell whether the allowlist is the right answer.
    #[test]
    fn the_reason_names_the_rule_that_fired() {
        let clean = skill_md("name: x\ndescription: Reads a channel.", "body");
        assert_eq!(
            classify("slack-cli", Some(&clean), &[]),
            Classification::SideEffecting {
                reason: "seed list".to_string()
            }
        );
        assert_eq!(
            classify("deploy-helper", Some(&clean), &[]),
            Classification::SideEffecting {
                reason: "name matched `deploy`".to_string()
            },
            "the directory name is scanned too"
        );
        assert_eq!(
            classify("techniques", Some(&clean), &[]),
            Classification::ReadOnly
        );
    }

    /// The allowlist is the one way past a refusal, and it is per-name.
    #[test]
    fn the_allowlist_overrides_the_seed_list_and_the_heuristic_but_not_not_a_skill() {
        let deployer = skill_md("name: p\ndescription: Deploy the stack.", "body");
        assert_eq!(
            classify("slack-cli", Some(&deployer), &allow(&["slack-cli"])),
            Classification::Allowlisted
        );
        assert_eq!(
            classify("provisioner", Some(&deployer), &allow(&["provisioner"])),
            Classification::Allowlisted
        );
        assert!(matches!(
            classify("slack-cli", Some(&deployer), &allow(&["something-else"])),
            Classification::SideEffecting { .. }
        ));
        assert_eq!(
            classify("slack-cli", None, &allow(&["slack-cli"])),
            Classification::NotASkill {
                reason: "no SKILL.md".to_string()
            },
            "an allowlist cannot conjure a skill out of a directory"
        );
    }

    /// Front matter is the text between the first two `---` lines, and an
    /// unterminated one is a broken document rather than a licence to scan the
    /// whole body.
    #[test]
    fn front_matter_is_the_first_fenced_block_only() {
        assert_eq!(front_matter("---\nname: x\n---\nbody\n"), "name: x\n");
        assert_eq!(front_matter("---\r\nname: x\r\n---\r\nbody"), "name: x\r\n");
        assert_eq!(front_matter("no front matter\n---\ntoken\n"), "");
        assert_eq!(front_matter("---\nname: x\nnever closed\n"), "");
        assert_eq!(
            classify(
                "notes",
                Some("---\nname: notes\nnever closed, mentions a token\n"),
                &[]
            ),
            Classification::ReadOnly
        );
    }

    /// `managed_dirs` spans accounts on purpose: the block is a property of the
    /// one destination tree, and a per-account view of it would uncover the
    /// other account's directories the next time anybody synced.
    #[test]
    fn managed_dirs_spans_every_account_in_the_ledger() {
        use super::super::manifest::Entry;
        let mut manifest = Manifest::default();
        for (account, name) in [("default", "techniques"), ("claude-revenium", "writing")] {
            manifest.entries.insert(
                Manifest::key(Kind::Skill, account, name),
                Entry {
                    source_path: format!("/src/{name}").into(),
                    dest_path: format!("/codex/skills/{name}").into(),
                    source_hash: "aa".into(),
                    dest_hash: "bb".into(),
                    synced_at: chrono::Utc::now(),
                    tool_version: env!("CARGO_PKG_VERSION").to_string(),
                    kind: Kind::Skill,
                    account: account.to_string(),
                },
            );
        }
        // An mcp entry is not a directory and must not reach the block.
        let mut mcp = manifest.entries["skill:default:techniques"].clone();
        mcp.kind = Kind::Mcp;
        mcp.dest_path = "/codex/config.toml".into();
        manifest
            .entries
            .insert(Manifest::key(Kind::Mcp, "default", "linear"), mcp);

        let plan = Plan {
            entries: vec![
                entry_named("brainstorming", Disposition::Synced),
                entry_named("techniques", Disposition::Unchanged),
                entry_named("devloop", Disposition::SkippedCollision),
                entry_named("slack-cli", Disposition::SkippedSideEffecting),
            ],
            warnings: vec![],
        };
        assert_eq!(
            managed_dirs(&plan, &manifest),
            ["brainstorming", "techniques", "writing"],
            "both accounts' ledger dirs plus what this plan would create — and \
             nothing a2a does not own"
        );
    }

    fn entry_named(name: &str, disposition: Disposition) -> PlanEntry {
        PlanEntry {
            name: name.to_string(),
            kind: Kind::Skill,
            account: "default".to_string(),
            source_path: Some(format!("/src/{name}").into()),
            dest_path: format!("/codex/skills/{name}").into(),
            disposition,
            detail: String::new(),
        }
    }

    fn dirs(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_block_renders_sorted_marked_and_one_line_per_dir() {
        assert_eq!(
            managed_block(&dirs(&["techniques", "brainstorming"])),
            "# BEGIN a2a-sync\n/brainstorming/\n/techniques/\n# END a2a-sync\n"
        );
        assert_eq!(managed_block(&[]), "# BEGIN a2a-sync\n# END a2a-sync\n");
    }

    #[test]
    fn block_covers_is_true_only_for_one_well_formed_complete_block() {
        let two = dirs(&["a", "b"]);
        assert!(!block_covers("*.log\n", &two), "absent");
        assert!(
            block_covers(&managed_block(&two), &two),
            "present and complete"
        );
        assert!(
            !block_covers(&managed_block(&dirs(&["a"])), &two),
            "present but missing one dir"
        );
        assert!(
            !block_covers(
                &format!("{}{}", managed_block(&two), managed_block(&two)),
                &two
            ),
            "two BEGIN markers is malformed, however complete each one looks"
        );
        assert!(
            !block_covers("# BEGIN a2a-sync\n/a/\n/b/\n", &two),
            "unterminated"
        );
        assert!(
            !block_covers("# END a2a-sync\n/a/\n/b/\n# BEGIN a2a-sync\n", &two),
            "an END before its BEGIN"
        );
    }

    #[test]
    fn rewrite_block_replaces_in_place_appends_once_and_is_idempotent() {
        let two = dirs(&["a", "b"]);

        // Absent: appended after exactly one blank line, everything above kept.
        let appended = rewrite_block("*.log\n.env\n", &two);
        assert_eq!(
            appended,
            format!("*.log\n.env\n\n{}", managed_block(&two)),
            "{appended}"
        );
        assert!(block_covers(&appended, &two));
        assert_eq!(rewrite_block("", &two), managed_block(&two), "empty file");
        assert_eq!(
            rewrite_block("*.log\n\n\n", &two),
            format!("*.log\n\n{}", managed_block(&two)),
            "trailing blank lines do not accumulate"
        );

        // Present: replaced where it stood, with the lines around it intact.
        let grown = rewrite_block(&appended, &dirs(&["a", "b", "c"]));
        assert!(grown.starts_with("*.log\n.env\n\n"), "{grown}");
        assert!(block_covers(&grown, &dirs(&["a", "b", "c"])));
        let surrounded = format!("# top\n{}\n# bottom\n", managed_block(&dirs(&["old"])));
        let rewritten = rewrite_block(&surrounded, &two);
        assert_eq!(
            rewritten,
            format!("# top\n{}\n# bottom\n", managed_block(&two)),
            "the block moves, its neighbours do not"
        );
        assert!(!rewritten.contains("/old/"));

        // Malformed: two blocks collapse to one, at the first one's position.
        let doubled = format!(
            "# top\n{}\nmiddle\n{}\n# bottom\n",
            managed_block(&dirs(&["old"])),
            managed_block(&dirs(&["older"]))
        );
        let collapsed = rewrite_block(&doubled, &two);
        assert_eq!(collapsed.matches(BEGIN_MARK).count(), 1, "{collapsed}");
        assert_eq!(collapsed.matches(END_MARK).count(), 1);
        assert!(block_covers(&collapsed, &two));
        assert!(collapsed.contains("middle\n") && collapsed.contains("# bottom\n"));

        // Idempotence, for every one of those inputs.
        for input in ["", "*.log\n.env\n", &appended, &surrounded, &doubled] {
            let once = rewrite_block(input, &two);
            assert_eq!(
                rewrite_block(&once, &two),
                once,
                "not idempotent for {input:?}"
            );
        }
    }
}
