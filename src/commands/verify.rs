//! The acceptance gate: what a worker changed, whether any of it can *execute*,
//! and how much of it the brief said it would touch (spec §5).
//!
//! `a2a verify` re-runs the **brief's** verify commands against the worker's
//! workspace. Running a command in a tree a worker just edited is only safe if
//! the edit cannot hijack the command, so the run is fronted by a refusal:
//! when the changed-path set contains something the toolchain *executes*
//! (`.github/workflows/**`, `build.rs`, a `Makefile`, a lockfile's resolution
//! rules…), verify refuses with `EXEC_BEARING_DIFF` and the human reads the
//! diff instead. `--force-verify` bypasses the refusal, never the record.
//!
//! This module is the gate half and is **pure** apart from the two git calls it
//! delegates to [`crate::worktree`]; its unit tests feed path lists directly
//! and need no git at all.
//!
//! # Why there is no `.git/**` row — a recorded gap, not an oversight
//!
//! Earlier drafts of [`EXEC_BEARING_RULES`] carried `.git/config` and
//! `.git/hooks/**`. Both were **unreachable** and are deleted. The gate's only
//! input is [`crate::worktree::changed_paths`] — the union of
//! `git diff --name-only <base>` and the `??` entries of
//! `git status --porcelain --untracked-files=all` — and **git reports nothing
//! under `.git/` from either** (probed live). In a *linked* worktree it is
//! structurally impossible anyway: `<wt>/.git` is a FILE, so `<wt>/.git/config`
//! and `<wt>/.git/hooks/**` cannot exist at all; the real config and hooks live
//! in the parent repo's common dir. Because this module's unit tests feed path
//! strings directly, those two rows would have tested **green** while never
//! firing on a real diff — false assurance on the surface this phase calls the
//! most bypassable thing in it.
//!
//! So: **git-dir escape (`core.hooksPath`, hook installation, `.git/config`
//! edits) is out of reach of a diff-based gate and is DEFERRED.** Closing it
//! needs a different mechanism — record a hash of the resolved gitdir's
//! `config` plus its `hooks/` listing at spawn, re-read both at verify, flag a
//! mismatch — which is **Phase 5** work.
//!
//! What spec §5's worktree-escape row still buys here is the `**/.gitmodules`
//! and `**/.gitattributes` rules: ordinary tracked files, visible to the
//! changed-path set, and both able to redirect what a later git command does.
//!
//! # Why "no diff base" is a refusal rather than a shrug
//!
//! A workspace we cannot diff is a workspace whose `Makefile` we cannot vouch
//! for. A worker with `base_ref == None` (a non-git cwd, or a pre-Phase-4
//! worker) therefore meets [`no_diff_base_error`] rather than an ungated run;
//! `--force-verify` is the way to say "I trust this tree" out loud.

use crate::output::{AppError, ExitCode};
use crate::worktree;
use serde_json::json;
use std::path::Path;

/// The execution-bearing pattern list (spec §5), one string per rule.
///
/// A path matches when **any** rule matches, and matching is on the
/// repo-relative, `/`-separated path. Two pattern shapes appear here and both
/// are read by the one matcher in this module:
///
/// * `<prefix>/**` — anchored at the repo root, `**` standing for the rest;
/// * `**/<tail>` — `<tail>` matched against the path's trailing segments, so a
///   basename rule fires at any depth *including* the root.
///
/// A `*` inside a segment never crosses a `/` (`**/requirements-*.txt`).
pub const EXEC_BEARING_RULES: &[&str] = &[
    // CI: a workflow file is code the *forge* runs.
    ".github/workflows/**",
    // Build scripts and task runners: code the toolchain runs on `build`/`test`.
    "**/build.rs",
    "**/package.json",
    "**/Makefile",
    "**/makefile",
    "**/GNUmakefile",
    "**/justfile",
    "**/Justfile",
    "**/.justfile",
    // Cargo's config can rewrite the linker, the runner and the target.
    "**/.cargo/config.toml",
    "**/.cargo/config",
    // pytest imports conftest.py before it collects a single test.
    "**/conftest.py",
    "**/pyproject.toml",
    // Dependency manifests: what gets fetched, and from where.
    "**/Cargo.toml",
    "**/Cargo.lock",
    "**/package-lock.json",
    "**/pnpm-lock.yaml",
    "**/yarn.lock",
    "**/requirements.txt",
    "**/requirements-*.txt",
    "**/Gemfile",
    "**/Gemfile.lock",
    "**/go.mod",
    "**/go.sum",
    // Spec §5's worktree-escape row, narrowed to what a diff can actually see.
    "**/.gitmodules",
    "**/.gitattributes",
];

/// The human + machine snapshot of what a worker touched.
///
/// Serialized into `verification.json` (Task 6) exactly as declared here.
#[derive(serde::Serialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct Scope {
    /// `git diff --stat <base_ref>` verbatim — the human half.
    pub diff_stat: String,
    /// The union changed-path set: sorted, deduplicated, repo-relative.
    pub changed: Vec<String>,
    /// Changed paths no `files_in_scope` entry claimed. Empty when the brief
    /// declared nothing — see [`Scope::files_in_scope_declared`].
    pub outside_scope: Vec<String>,
    /// Did the brief declare a `files_in_scope` at all? Without this an empty
    /// `outside_scope` reads as "the worker stayed in its lane" when it in fact
    /// means "no lane was drawn".
    pub files_in_scope_declared: bool,
}

/// Does this path match any rule in [`EXEC_BEARING_RULES`]?
pub fn is_exec_bearing(path: &str) -> bool {
    EXEC_BEARING_RULES
        .iter()
        .any(|rule| rule_matches(rule, path))
}

/// The execution-bearing subset of a changed-path set, sorted.
pub fn gate(changed: &[String]) -> Vec<String> {
    let mut hits: Vec<String> = changed
        .iter()
        .filter(|p| is_exec_bearing(p))
        .cloned()
        .collect();
    hits.sort();
    hits.dedup();
    hits
}

/// The refusal built from a non-empty [`gate`] result.
///
/// The sentence names at most ten paths (a 400-file rename should not fill a
/// terminal); the **full** list travels in `data.paths`, which is what an
/// orchestrator branches on. `data.count` is the total, not the truncation.
pub fn exec_bearing_error(hits: &[String]) -> AppError {
    const SHOWN: usize = 10;
    let shown = hits
        .iter()
        .take(SHOWN)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let more = hits.len().saturating_sub(SHOWN);
    let listed = if more > 0 {
        format!("{shown} (+{more} more)")
    } else {
        shown
    };
    AppError::new(
        "EXEC_BEARING_DIFF",
        ExitCode::Precondition,
        format!(
            "the diff touches {} execution-bearing path(s), so running the brief's commands \
             would run this worker's code: {listed}",
            hits.len()
        ),
    )
    .remedy(
        "review the diff by hand, then re-run with --force-verify to run the brief's \
         commands anyway",
    )
    .data(json!({"paths": hits, "count": hits.len()}))
}

/// The refusal for a worker with no diff anchor (`snap.base_ref == None`).
///
/// Not in Task 5's `Interfaces` block but pinned by its acceptance criteria;
/// Task 6's base-ref branch calls it so the exact remedy has one home.
pub fn no_diff_base_error(id: &str) -> AppError {
    AppError::new(
        "NO_DIFF_BASE",
        ExitCode::Precondition,
        format!(
            "worker {id} recorded no base ref, so there is nothing to diff its workspace \
             against and no way to tell what it changed"
        ),
    )
    .remedy(
        "--force-verify runs the brief's commands without a diff gate — only do that on a \
         workspace you trust",
    )
}

/// The scope snapshot: everything that changed since `base_ref`, and how much
/// of it the brief claimed.
///
/// The only impure function in this module, and both of its effects are git
/// reads delegated to [`crate::worktree`].
pub fn scope(dir: &Path, base_ref: &str, files_in_scope: &[String]) -> Result<Scope, AppError> {
    let changed = worktree::changed_paths(dir, base_ref)?;
    let diff_stat = worktree::diff_stat(dir, base_ref)?;
    let declared = !files_in_scope.is_empty();
    // Nothing declared ⇒ nothing is outside it. "Declare your scope" is taught
    // in the orchestrate skill, not enforced by turning silence into a fault.
    let outside_scope = if declared {
        changed
            .iter()
            .filter(|p| !files_in_scope.iter().any(|e| in_scope(e, p)))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    Ok(Scope {
        diff_stat,
        changed,
        outside_scope,
        files_in_scope_declared: declared,
    })
}

/// Does one `files_in_scope` entry claim `path`? Four forms, all pinned:
///
/// 1. **equality** — `src/a.rs` claims `src/a.rs`;
/// 2. **directory prefix**, component-wise — `src/` claims `src/a/b.rs` and not
///    `srcfoo/b.rs`. The trailing `/` is what makes it a directory: an entry
///    without one is an exact path, and a brief that means a tree writes
///    `src/` or `src/**`;
/// 3. **single-`*` glob**, within one segment — `src/*.rs` claims `src/a.rs`
///    and not `src/a/b.rs`;
/// 4. **`**` glob**, crossing any number of segments — `src/**` claims
///    `src/a.rs` and `src/a/b/c.rs`, and neither `srcfoo/a.rs` nor
///    `other/src/a.rs`.
fn in_scope(entry: &str, path: &str) -> bool {
    if let Some(dir) = entry.strip_suffix('/') {
        let dir = segments(dir);
        let path = segments(path);
        // A directory claims what is *under* it, never itself.
        return path.len() > dir.len() && path[..dir.len()] == dir[..];
    }
    glob_match(&segments(entry), &segments(path))
}

/// One [`EXEC_BEARING_RULES`] entry against one path.
fn rule_matches(rule: &str, path: &str) -> bool {
    glob_match(&segments(rule), &segments(path))
}

/// Repo-relative path ⇒ its non-empty segments.
fn segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}

/// Segment-wise glob: `**` consumes zero or more whole segments, everything
/// else is matched one segment at a time by [`segment_match`], so a `*` can
/// never cross a `/`.
fn glob_match(pattern: &[&str], path: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((&"**", rest)) => (0..=path.len()).any(|skip| glob_match(rest, &path[skip..])),
        Some((head, rest)) => match path.split_first() {
            Some((seg, tail)) if segment_match(head, seg) => glob_match(rest, tail),
            _ => false,
        },
    }
}

/// One segment against one pattern segment, with at most one `*`.
fn segment_match(pattern: &str, segment: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == segment,
        Some((prefix, suffix)) => {
            segment.len() >= prefix.len() + suffix.len()
                && segment.starts_with(prefix)
                && segment.ends_with(suffix)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One canonical hit per rule, at the root and at depth where the rule
    /// allows both. The coverage assertion below is what keeps this list
    /// honest when a rule is added.
    const HITS: &[&str] = &[
        ".github/workflows/ci.yml",
        ".github/workflows/nested/deploy.yaml",
        "build.rs",
        "crates/inner/build.rs",
        "package.json",
        "web/package.json",
        "Makefile",
        "sub/Makefile",
        "makefile",
        "GNUmakefile",
        "justfile",
        "Justfile",
        ".justfile",
        "sub/justfile",
        ".cargo/config.toml",
        "sub/.cargo/config.toml",
        ".cargo/config",
        "conftest.py",
        "tests/conftest.py",
        "pyproject.toml",
        "Cargo.toml",
        "Cargo.lock",
        "package-lock.json",
        "pnpm-lock.yaml",
        "yarn.lock",
        "requirements.txt",
        "requirements-dev.txt",
        "tools/requirements-test.txt",
        "Gemfile",
        "Gemfile.lock",
        "go.mod",
        "go.sum",
        ".gitmodules",
        "sub/.gitattributes",
    ];

    /// The other direction. The first five are pinned by the plan; the rest
    /// guard the shapes the matcher could plausibly get wrong.
    const NEAR_MISSES: &[&str] = &[
        "src/github/workflows/x.yml",
        "notpackage.json",
        "Makefile.in",
        "build.rs.bak",
        "docs/justfile.md",
        ".github/ci.yml",
        "workflows/ci.yml",
        "docs/.github/workflows/x.yml",
        "config.toml",
        "sub/config.toml",
        ".cargo/config.toml.bak",
        "my-package-lock.json",
        "go.mod.orig",
        "requirements.txt.md",
        "src/main.rs",
        "README.md",
        "a.txt",
        "note.txt",
    ];

    #[test]
    fn every_rule_fires_on_its_canonical_path() {
        for path in HITS {
            assert!(is_exec_bearing(path), "{path} must be execution-bearing");
        }
        // Both directions per RULE, not merely per path: a rule that no listed
        // hit exercises is a rule nothing tests.
        for rule in EXEC_BEARING_RULES {
            assert!(
                HITS.iter().any(|p| rule_matches(rule, p)),
                "rule {rule} has no canonical hit in HITS"
            );
        }
    }

    #[test]
    fn near_misses_never_fire() {
        for path in NEAR_MISSES {
            assert!(
                !is_exec_bearing(path),
                "{path} must NOT be execution-bearing"
            );
            // And name the rule that would have been wrong, if any.
            for rule in EXEC_BEARING_RULES {
                assert!(
                    !rule_matches(rule, path),
                    "rule {rule} wrongly matched {path}"
                );
            }
        }
    }

    /// The gap is deliberate (see the module doc-comment): a `.git/**` rule
    /// would test green and never fire.
    #[test]
    fn the_table_carries_no_unreachable_git_dir_rows() {
        for rule in EXEC_BEARING_RULES {
            assert!(
                !rule.contains(".git/"),
                "{rule} names the git dir, which no changed-path set can ever contain"
            );
        }
    }

    #[test]
    fn the_gate_returns_only_the_hits_sorted() {
        let changed: Vec<String> = [
            "src/main.rs",
            "Makefile",
            ".github/workflows/ci.yml",
            "a.txt",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            gate(&changed),
            vec![
                ".github/workflows/ci.yml".to_string(),
                "Makefile".to_string()
            ]
        );
        let clean: Vec<String> = ["src/main.rs", "a.txt"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(gate(&clean).is_empty());
        assert!(gate(&[]).is_empty());
    }

    #[test]
    fn the_refusal_truncates_the_sentence_and_never_the_data() {
        let hits: Vec<String> = (0..13).map(|i| format!("d{i:02}/package.json")).collect();
        let err = exec_bearing_error(&hits);
        assert_eq!(err.code, "EXEC_BEARING_DIFF");
        assert_eq!(err.exit, ExitCode::Precondition);
        assert_eq!(
            err.remedy.as_deref(),
            Some(
                "review the diff by hand, then re-run with --force-verify to run the brief's \
                 commands anyway"
            )
        );
        assert!(err.message.contains("d00/package.json"));
        assert!(err.message.contains("d09/package.json"));
        assert!(!err.message.contains("d10/package.json"));
        assert!(!err.message.contains("d12/package.json"));
        assert!(err.message.contains("(+3 more)"));
        let data = err.data.expect("machine-readable detail");
        assert_eq!(data["count"], 13);
        assert_eq!(data["paths"].as_array().unwrap().len(), 13);
        assert_eq!(data["paths"][12], "d12/package.json");
    }

    #[test]
    fn a_short_refusal_lists_everything_and_says_no_more() {
        let hits = vec!["Makefile".to_string(), "build.rs".to_string()];
        let err = exec_bearing_error(&hits);
        assert!(err.message.contains("Makefile"));
        assert!(err.message.contains("build.rs"));
        assert!(!err.message.contains("more)"));
        assert_eq!(err.data.unwrap()["count"], 2);
    }

    #[test]
    fn a_worker_with_no_base_ref_is_a_precondition_failure() {
        let err = no_diff_base_error("impl-0818t1200-aaaa");
        assert_eq!(err.code, "NO_DIFF_BASE");
        assert_eq!(err.exit, ExitCode::Precondition);
        assert!(err.message.contains("impl-0818t1200-aaaa"));
        assert_eq!(
            err.remedy.as_deref(),
            Some(
                "--force-verify runs the brief's commands without a diff gate — only do that \
                 on a workspace you trust"
            )
        );
    }

    #[test]
    fn scope_form_one_is_equality() {
        assert!(in_scope("src/a.rs", "src/a.rs"));
        assert!(!in_scope("src/a.rs", "src/b.rs"));
        assert!(!in_scope("src/a.rs", "x/src/a.rs"));
        assert!(!in_scope("src/a.rs", "src/a.rs.bak"));
    }

    #[test]
    fn scope_form_two_is_a_component_wise_directory_prefix() {
        assert!(in_scope("src/", "src/a.rs"));
        assert!(in_scope("src/", "src/a/b.rs"));
        assert!(!in_scope("src/", "srcfoo/b.rs"));
        assert!(!in_scope("src/", "other/src/a.rs"));
        // A directory claims what is under it, not the name itself.
        assert!(!in_scope("src/", "src"));
    }

    #[test]
    fn scope_form_three_is_one_segment_of_glob() {
        assert!(in_scope("src/*.rs", "src/a.rs"));
        assert!(!in_scope("src/*.rs", "src/a/b.rs"));
        assert!(!in_scope("src/*.rs", "src/a.txt"));
        assert!(!in_scope("src/*.rs", "srcx/a.rs"));
        assert!(!in_scope("src/*.rs", "a.rs"));
    }

    #[test]
    fn scope_form_four_crosses_segments() {
        assert!(in_scope("src/**", "src/a.rs"));
        assert!(in_scope("src/**", "src/a/b/c.rs"));
        assert!(!in_scope("src/**", "srcfoo/a.rs"));
        assert!(!in_scope("src/**", "other/src/a.rs"));
        assert!(in_scope("**/a.rs", "src/deep/a.rs"));
        assert!(in_scope("**/a.rs", "a.rs"));
        assert!(!in_scope("**/a.rs", "src/b.rs"));
    }
}
