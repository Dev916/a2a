//! The skills planner over the fixture tree: what a run WOULD do, decided
//! entirely by reading.
//!
//! Task 2 ships the planner half only — there is no `a2a sync skills` command
//! yet, so these drive the library the way the command will. The shapes under
//! `tests/fixtures/sync/skills/` mirror the real `~/.claude/skills` verified in
//! the plan's Ground truth: a plain `README.md`, a directory that is not a
//! skill, a symlinked skill, a `__pycache__`, a seed-list skill and a skill
//! whose front matter alone condemns it.
//!
//! Task 3 appends the command half below the planner tests: `a2a sync skills`
//! driven end to end against a writable copy of the same tree and a git repo
//! standing in for the `codex-skills` submodule.

mod common;

use a2a::config::SyncConfig;
use a2a::sync::manifest::{hash_path, Entry, Manifest};
use a2a::sync::skills;
use a2a::sync::{Disposition, Kind, Plan, SKILL_ACCOUNT};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn source_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sync/skills")
}

fn cfg(allow: &[&str]) -> SyncConfig {
    SyncConfig {
        allow_skills: allow.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

/// `name -> disposition`, which is what every assertion below is really about.
fn dispositions(plan: &Plan) -> BTreeMap<String, Disposition> {
    plan.entries
        .iter()
        .map(|e| (e.name.clone(), e.disposition))
        .collect()
}

fn detail_of(plan: &Plan, name: &str) -> String {
    plan.entries
        .iter()
        .find(|e| e.name == name)
        .unwrap_or_else(|| panic!("no plan entry for {name}: {plan:?}"))
        .detail
        .clone()
}

fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let name = e.file_name().to_string_lossy().into_owned();
        if name == "__pycache__" {
            continue;
        }
        let (from, to) = (e.path(), dst.join(e.file_name()));
        if std::fs::metadata(&from).unwrap().is_dir() {
            copy_tree(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

/// A manifest that claims `name`, with whatever hashes the caller wants to
/// pretend the last run recorded.
fn owned(manifest: &mut Manifest, name: &str, source: &Path, dest: &Path, hashes: (&str, &str)) {
    manifest.entries.insert(
        Manifest::key(Kind::Skill, SKILL_ACCOUNT, name),
        Entry {
            source_path: source.to_path_buf(),
            dest_path: dest.to_path_buf(),
            source_hash: hashes.0.to_string(),
            dest_hash: hashes.1.to_string(),
            synced_at: chrono::Utc::now(),
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
            kind: Kind::Skill,
            account: SKILL_ACCOUNT.to_string(),
        },
    );
}

/// Every shape in the fixture tree, classified in one pass against an empty
/// destination and an empty ledger — the state of a machine that has never
/// synced.
#[test]
fn the_planner_classifies_every_fixture_shape() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("skills");
    let plan = skills::plan(
        &source_root(),
        &dest,
        &Manifest::default(),
        &cfg(&[]),
        SKILL_ACCOUNT,
    )
    .unwrap();

    let d = dispositions(&plan);
    assert_eq!(
        d,
        BTreeMap::from([
            ("README.md".to_string(), Disposition::SkippedNotASkill),
            ("portable".to_string(), Disposition::Synced),
            ("provisioner".to_string(), Disposition::SkippedSideEffecting),
            ("scratch".to_string(), Disposition::SkippedNotASkill),
            ("slack-cli".to_string(), Disposition::SkippedSideEffecting),
            ("techniques".to_string(), Disposition::Synced),
            ("wordy".to_string(), Disposition::Synced),
        ]),
        "{plan:?}"
    );

    // The two skips are not the same skip, and the detail says which.
    assert_eq!(detail_of(&plan, "slack-cli"), "seed list");
    assert_eq!(
        detail_of(&plan, "provisioner"),
        "front matter matched `deploy`"
    );
    assert!(detail_of(&plan, "scratch").contains("SKILL.md"));

    // A report line may never carry a value read from a source — the whole plan
    // flows back into agent context.
    for e in &plan.entries {
        assert_eq!(e.kind, Kind::Skill);
        assert_eq!(e.account, SKILL_ACCOUNT);
        assert_eq!(e.dest_path, dest.join(&e.name));
    }

    // The source path recorded for a symlinked skill is the link as given, not
    // its target: the ledger's job is to say what a2a was pointed at.
    let portable = plan.entries.iter().find(|e| e.name == "portable").unwrap();
    assert_eq!(portable.source_path, Some(source_root().join("portable")));
    assert_eq!(
        detail_of(&plan, "techniques"),
        "new managed skill",
        "nothing is owned yet"
    );
}

/// The allowlist is the one way past the seed list, and it is per-name.
#[test]
fn allow_skills_flips_a_seed_list_skill_to_synced() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("skills");
    let plan = skills::plan(
        &source_root(),
        &dest,
        &Manifest::default(),
        &cfg(&["slack-cli"]),
        SKILL_ACCOUNT,
    )
    .unwrap();
    let d = dispositions(&plan);
    assert_eq!(d["slack-cli"], Disposition::Synced);
    assert_eq!(
        d["provisioner"],
        Disposition::SkippedSideEffecting,
        "allowlisting one skill says nothing about another"
    );
}

/// A destination the ledger does not claim belongs to somebody else — a curated
/// port, a hand-made directory, whatever another tool left behind — and a2a
/// only ever updates what it created.
#[test]
fn an_unowned_destination_is_a_collision() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("skills");
    copy_tree(&source_root().join("techniques"), &dest.join("techniques"));

    let plan = skills::plan(
        &source_root(),
        &dest,
        &Manifest::default(),
        &cfg(&[]),
        SKILL_ACCOUNT,
    )
    .unwrap();
    assert_eq!(
        dispositions(&plan)["techniques"],
        Disposition::SkippedCollision
    );
    assert_eq!(
        detail_of(&plan, "techniques"),
        "curated or foreign destination wins; a2a only ever updates what it created"
    );
}

/// Owned, both ends matching the ledger: nothing to do. This is the disposition
/// a second run with no changes must produce for everything.
#[test]
fn an_owned_destination_that_matches_both_hashes_is_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("skills");
    let src = source_root().join("techniques");
    let dst = dest.join("techniques");
    copy_tree(&src, &dst);

    let mut manifest = Manifest::default();
    let h = hash_path(&dst).unwrap();
    assert_eq!(h, hash_path(&src).unwrap(), "a fresh copy hashes equal");
    owned(&mut manifest, "techniques", &src, &dst, (&h, &h));

    let plan = skills::plan(&source_root(), &dest, &manifest, &cfg(&[]), SKILL_ACCOUNT).unwrap();
    assert_eq!(dispositions(&plan)["techniques"], Disposition::Unchanged);
    assert_eq!(plan.counts()["unchanged"], 1);
}

/// Owned but edited underneath: refused, never clobbered. The ledger is what
/// tells the difference between "a2a wrote this" and "a2a wrote this and then a
/// human improved it".
#[test]
fn an_edited_destination_is_refused_and_a_moved_source_is_synced() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("skills");
    let src = source_root().join("techniques");
    let dst = dest.join("techniques");
    copy_tree(&src, &dst);
    let fresh = hash_path(&dst).unwrap();

    // The destination moved on: the human edited it.
    let mut manifest = Manifest::default();
    owned(&mut manifest, "techniques", &src, &dst, (&fresh, &fresh));
    std::fs::write(dst.join("SKILL.md"), b"locally improved").unwrap();

    let plan = skills::plan(&source_root(), &dest, &manifest, &cfg(&[]), SKILL_ACCOUNT).unwrap();
    assert_eq!(
        dispositions(&plan)["techniques"],
        Disposition::RefusedDestDrift
    );
    assert_eq!(
        detail_of(&plan, "techniques"),
        "destination was edited after the last sync; a2a will not overwrite it"
    );
    assert_eq!(plan.counts()["refused"], 1);

    // The SOURCE moved on instead: that is an ordinary update.
    copy_tree(&src, &dst);
    std::fs::write(dst.join("SKILL.md"), std::fs::read(src.join("SKILL.md")).unwrap()).unwrap();
    let mut manifest = Manifest::default();
    owned(
        &mut manifest,
        "techniques",
        &src,
        &dst,
        ("a-hash-from-an-older-source", &fresh),
    );
    let plan = skills::plan(&source_root(), &dest, &manifest, &cfg(&[]), SKILL_ACCOUNT).unwrap();
    assert_eq!(dispositions(&plan)["techniques"], Disposition::Synced);
    assert_eq!(
        detail_of(&plan, "techniques"),
        "source changed since the last sync"
    );
}

/// A source that went away takes its managed destination with it — unless the
/// destination has drifted, in which case a user's edits are never deleted on
/// the strength of a deletion somewhere else.
#[test]
fn a_vanished_source_is_removed_unless_the_destination_drifted() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("skills");
    let ghost_src = tmp.path().join("gone/ghost");
    let ghost_dst = dest.join("ghost");
    copy_tree(&source_root().join("techniques"), &ghost_dst);
    let h = hash_path(&ghost_dst).unwrap();

    let mut manifest = Manifest::default();
    owned(&mut manifest, "ghost", &ghost_src, &ghost_dst, (&h, &h));
    let plan = skills::plan(&source_root(), &dest, &manifest, &cfg(&[]), SKILL_ACCOUNT).unwrap();
    let entry = plan.entries.iter().find(|e| e.name == "ghost").unwrap();
    assert_eq!(entry.disposition, Disposition::Removed);
    assert_eq!(entry.source_path, None, "there is no source to name");
    assert_eq!(entry.dest_path, ghost_dst);
    assert_eq!(plan.counts()["removed"], 1);

    std::fs::write(ghost_dst.join("SKILL.md"), b"kept by hand").unwrap();
    let plan = skills::plan(&source_root(), &dest, &manifest, &cfg(&[]), SKILL_ACCOUNT).unwrap();
    let entry = plan.entries.iter().find(|e| e.name == "ghost").unwrap();
    assert_eq!(entry.disposition, Disposition::RefusedDestDrift);
    assert!(
        entry.detail.contains("edited after the last sync"),
        "{}",
        entry.detail
    );
    assert!(ghost_dst.join("SKILL.md").exists(), "planning deletes nothing");
}

/// `plan()` reads and nothing else. Two runs produce an identical `Plan` — the
/// whole value, not a `Debug` rendering of it — and neither tree has moved.
#[test]
fn the_planner_is_pure_with_respect_to_the_filesystem() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("skills");
    let src = source_root().join("techniques");
    copy_tree(&src, &dest.join("techniques"));
    let mut manifest = Manifest::default();
    let h = hash_path(&dest.join("techniques")).unwrap();
    owned(
        &mut manifest,
        "techniques",
        &src,
        &dest.join("techniques"),
        (&h, &h),
    );

    let source_before = hash_path(&source_root()).unwrap();
    let dest_before = hash_path(&dest).unwrap();

    let first = skills::plan(&source_root(), &dest, &manifest, &cfg(&[]), SKILL_ACCOUNT).unwrap();
    let second = skills::plan(&source_root(), &dest, &manifest, &cfg(&[]), SKILL_ACCOUNT).unwrap();
    assert_eq!(first, second, "planning twice decides the same thing");

    assert_eq!(hash_path(&source_root()).unwrap(), source_before);
    assert_eq!(hash_path(&dest).unwrap(), dest_before);
    assert!(
        !dest.join("wordy").exists() && !dest.join("portable").exists(),
        "a planned copy is not a copy"
    );
}

/// A source root that is not there is a precondition failure the command can
/// report verbatim, not a panic and not an empty plan (which would read as
/// "every managed skill has been deleted").
#[test]
fn a_missing_source_root_is_a_precondition_failure_naming_the_path() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("nowhere/skills");
    let err = skills::plan(
        &missing,
        &tmp.path().join("dest"),
        &Manifest::default(),
        &cfg(&[]),
        SKILL_ACCOUNT,
    )
    .unwrap_err();
    assert_eq!(err.code, "SYNC_SKILLS_SOURCE_MISSING");
    assert_eq!(err.exit, a2a::output::ExitCode::Precondition);
    assert!(
        err.message.contains(&*missing.to_string_lossy()),
        "{}",
        err.message
    );
}

/// The ignore block has to cover every managed directory before a single byte
/// of content is written, so `managed_dirs` is computed off the plan the same
/// run is about to apply.
#[test]
fn managed_dirs_covers_what_the_plan_would_create() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("skills");
    let plan = skills::plan(
        &source_root(),
        &dest,
        &Manifest::default(),
        &cfg(&[]),
        SKILL_ACCOUNT,
    )
    .unwrap();
    let dirs = skills::managed_dirs(&plan, &Manifest::default());
    assert_eq!(dirs, ["portable", "techniques", "wordy"]);
    assert!(
        skills::block_covers(&skills::rewrite_block("*.log\n", &dirs), &dirs),
        "the rendered block covers exactly what the plan would create"
    );
}

// ---------------------------------------------------------------------------
// Task 3: `a2a sync skills` end to end.
//
// Every invocation below is built by `common::sync_a2a` (Global Constraints):
// it is the one entry point that sets `CODEX_HOME`, points the Claude source at
// a fixture and leaves `A2A_SYNC_ALLOW_REAL_HOME` unset, so a forgotten
// variable is a red test rather than a write into the developer's real
// `~/.codex` submodule.
// ---------------------------------------------------------------------------

use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::process::Output;

/// The Claude-side `devloop`, whose curated port already sits in the
/// destination — the live collision from the plan's Ground truth.
const SOURCE_DEVLOOP: &[u8] =
    b"---\nname: devloop\ndescription: The Claude-side original, which must never win.\n---\n\nsource\n";
/// The curated port, committed in the fixture submodule before any run.
const CURATED_DEVLOOP: &[u8] =
    b"---\nname: devloop\ndescription: The curated port, maintained in the submodule.\n---\n\ncurated\n";

/// A hermetic world: a temp `$A2A_HOME`, a temp `CODEX_HOME` whose `skills/` is
/// a git repo standing in for the `codex-skills` submodule, and a WRITABLE copy
/// of the fixture Claude tree — these tests delete a source skill and re-run,
/// which the repository's own fixture must not be asked to survive.
struct World {
    tmp: tempfile::TempDir,
    home: PathBuf,
    codex_home: PathBuf,
    claude_dir: PathBuf,
}

impl World {
    /// The submodule already has a committed `.gitignore`, which is the shape
    /// the real one has — and the reason the containment assertion is
    /// `[" M .gitignore"]` rather than "empty".
    fn new() -> World {
        World::build(true)
    }
    /// The other shape: a repo with no `.gitignore` at all, where the run's own
    /// block arrives as `?? .gitignore`.
    fn without_gitignore() -> World {
        World::build(false)
    }

    fn build(commit_gitignore: bool) -> World {
        let tmp = tempfile::tempdir().unwrap();
        let w = World {
            home: tmp.path().join("a2a"),
            codex_home: tmp.path().join("codex"),
            claude_dir: tmp.path().join("claude"),
            tmp,
        };
        copy_preserving(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sync"),
            &w.claude_dir,
        );
        std::fs::create_dir_all(w.source().join("devloop")).unwrap();
        std::fs::write(w.source().join("devloop/SKILL.md"), SOURCE_DEVLOOP).unwrap();

        std::fs::create_dir_all(w.dest().join("devloop")).unwrap();
        std::fs::write(w.dest().join("devloop/SKILL.md"), CURATED_DEVLOOP).unwrap();
        if commit_gitignore {
            std::fs::write(w.dest().join(".gitignore"), "*.log\n").unwrap();
        }
        w.git(&["init", "-q", "-b", "main"]);
        w.git(&["add", "-A"]);
        w.git(&["commit", "-q", "-m", "curated skills"]);
        w
    }

    fn source(&self) -> PathBuf {
        self.claude_dir.join("skills")
    }
    fn dest(&self) -> PathBuf {
        self.codex_home.join("skills")
    }
    fn manifest_path(&self) -> PathBuf {
        self.home.join("sync-manifest.json")
    }

    fn cmd(&self) -> assert_cmd::Command {
        common::sync_a2a(
            &self.home,
            &self.codex_home,
            &self.claude_dir,
            self.tmp.path(),
        )
    }
    fn run(&self, args: &[&str]) -> Output {
        self.run_with(&[], args)
    }
    fn run_with(&self, envs: &[(&str, &str)], args: &[&str]) -> Output {
        let mut c = self.cmd();
        for (k, v) in envs {
            c.env(k, v);
        }
        c.args(args).output().unwrap()
    }
    /// One apply, asserted successful, returning the `--json` payload.
    fn apply(&self) -> Value {
        let out = self.run(&["--json", "sync", "skills"]);
        assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
        data(&out)
    }

    /// Hermetic git, per Global Constraints: the developer's real gitconfig
    /// (hooks, signing, templates) can never reach a test.
    fn git(&self, args: &[&str]) -> String {
        let nowhere = self.tmp.path().join("no-such-gitconfig");
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(self.dest())
            .args([
                "-c",
                "user.email=a2a@test",
                "-c",
                "user.name=a2a",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .env("GIT_CONFIG_GLOBAL", &nowhere)
            .env("GIT_CONFIG_SYSTEM", &nowhere)
            .output()
            .expect("git is on PATH");
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
    fn status(&self) -> Vec<String> {
        self.git(&["status", "--porcelain"])
            .lines()
            .map(str::to_string)
            .collect()
    }
    fn manifest(&self) -> Value {
        serde_json::from_str(&std::fs::read_to_string(self.manifest_path()).unwrap()).unwrap()
    }
}

/// Copies a tree keeping symlinks as symlinks and modes as modes — `portable`
/// must still be a link and `bin/run.sh` must still be executable, or two of
/// this task's pins would be tested against a fixture that no longer has the
/// shape they are about.
fn copy_preserving(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let (from, to) = (e.path(), dst.join(e.file_name()));
        let meta = std::fs::symlink_metadata(&from).unwrap();
        if meta.file_type().is_symlink() {
            std::os::unix::fs::symlink(std::fs::read_link(&from).unwrap(), &to).unwrap();
        } else if meta.is_dir() {
            copy_preserving(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap(); // preserves the mode bits
        }
    }
}

fn envelope(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout must be one JSON envelope ({e}): {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}
fn data(out: &Output) -> Value {
    envelope(out)["data"].clone()
}
fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}
fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}
fn error_code(out: &Output) -> String {
    envelope(out)["error"]["code"].as_str().unwrap().to_string()
}
/// `name -> disposition`, read back out of the `--json` payload.
fn reported(data: &Value) -> BTreeMap<String, String> {
    data["entries"]
        .as_array()
        .expect("entries is an array")
        .iter()
        .map(|e| {
            (
                e["name"].as_str().unwrap().to_string(),
                e["disposition"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}
fn mode_of(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

/// The whole surface, in one apply: what gets copied, what emphatically does
/// not, how it is written, and the containment that has to hold before any of
/// it is allowed to happen.
#[test]
fn the_first_apply_copies_read_only_skills_and_contains_them() {
    let w = World::new();
    let out = w.run(&["--json", "sync", "skills"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let env = envelope(&out);
    assert_eq!(env["command"], "sync skills", "the pinned command name");
    let data = &env["data"];
    assert_eq!(data["kind"], "skills");
    assert_eq!(data["account"], "default");
    assert_eq!(data["dry_run"], false);
    assert_eq!(data["dest"], w.dest().to_string_lossy().into_owned());
    assert_eq!(
        data["counts"],
        json!({"synced": 3, "unchanged": 0, "removed": 0, "skipped": 5, "refused": 0})
    );
    assert_eq!(
        reported(data),
        BTreeMap::from([
            ("README.md".to_string(), "skipped-not-a-skill".to_string()),
            ("devloop".to_string(), "skipped-collision".to_string()),
            ("portable".to_string(), "synced".to_string()),
            (
                "provisioner".to_string(),
                "skipped-side-effecting".to_string(),
            ),
            ("scratch".to_string(), "skipped-not-a-skill".to_string()),
            (
                "slack-cli".to_string(),
                "skipped-side-effecting".to_string(),
            ),
            ("techniques".to_string(), "synced".to_string()),
            ("wordy".to_string(), "synced".to_string()),
        ])
    );

    // Content: a read-only skill lands byte-identical, 0600, under a 0700 dir.
    assert_eq!(
        std::fs::read(w.dest().join("techniques/SKILL.md")).unwrap(),
        std::fs::read(w.source().join("techniques/SKILL.md")).unwrap()
    );
    assert_eq!(mode_of(&w.dest().join("techniques")), 0o700);
    assert_eq!(mode_of(&w.dest().join("techniques/SKILL.md")), 0o600);
    // Non-UTF-8 content survives the copy verbatim (the hasher and the copier
    // both work in bytes).
    assert_eq!(
        std::fs::read(w.dest().join("techniques/assets/corpus.idx")).unwrap(),
        std::fs::read(w.source().join("techniques/assets/corpus.idx")).unwrap()
    );

    // A side-effecting skill has no destination AT ALL — not an empty one.
    assert!(!w.dest().join("slack-cli").exists());
    assert!(!w.dest().join("provisioner").exists());
    assert!(!w.dest().join("README.md").exists());
    assert!(!w.dest().join("scratch").exists());

    // The symlinked skill is copied as CONTENTS, with the execute bit kept.
    let portable = w.dest().join("portable");
    assert!(
        std::fs::symlink_metadata(&portable).unwrap().is_dir(),
        "the destination is a real directory, never a link into another checkout"
    );
    assert_eq!(mode_of(&portable.join("bin/run.sh")), 0o700);
    assert_eq!(mode_of(&portable.join("SKILL.md")), 0o600);

    // The curated port is untouched, bytes and all.
    assert_eq!(
        std::fs::read(w.dest().join("devloop/SKILL.md")).unwrap(),
        CURATED_DEVLOOP
    );

    // Containment: the ONLY thing the submodule sees is the ignore block this
    // run wrote. A tracked file cannot ignore itself, so " M .gitignore" is the
    // floor — everything else must be covered.
    assert_eq!(w.status(), [" M .gitignore"]);
    let gitignore = std::fs::read_to_string(w.dest().join(".gitignore")).unwrap();
    assert!(
        gitignore.starts_with("*.log\n"),
        "everything outside the block is preserved: {gitignore}"
    );
    assert!(
        gitignore.contains("# BEGIN a2a-sync\n/portable/\n/techniques/\n/wordy/\n# END a2a-sync\n"),
        "{gitignore}"
    );

    // The ledger claims exactly what was written, and nothing else.
    let manifest = w.manifest();
    assert_eq!(manifest["schema_version"], 1);
    let keys: Vec<&String> = manifest["entries"].as_object().unwrap().keys().collect();
    assert_eq!(
        keys,
        [
            "skill:default:portable",
            "skill:default:techniques",
            "skill:default:wordy"
        ]
    );
    let entry = &manifest["entries"]["skill:default:techniques"];
    assert_eq!(entry["kind"], "skill");
    assert_eq!(entry["account"], "default");
    assert_eq!(
        entry["source_path"],
        w.source().join("techniques").to_string_lossy().into_owned()
    );
    assert_eq!(
        entry["dest_path"],
        w.dest().join("techniques").to_string_lossy().into_owned()
    );
    assert_eq!(
        entry["source_hash"], entry["dest_hash"],
        "a fresh copy hashes equal at both ends"
    );
    assert_eq!(
        entry["dest_hash"].as_str().unwrap(),
        a2a::sync::manifest::hash_path(&w.dest().join("techniques")).unwrap()
    );
    assert_eq!(mode_of(&w.manifest_path()), 0o600);
}

/// The human report: one line per candidate on stderr, the summary on stdout.
#[test]
fn the_report_lists_every_candidate_on_stderr_and_summarises_on_stdout() {
    let w = World::new();
    let out = w.run(&["sync", "skills"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let err = stderr(&out);
    for line in [
        "synced techniques — new managed skill",
        "skipped-side-effecting slack-cli — seed list",
        "skipped-side-effecting provisioner — front matter matched `deploy`",
        "skipped-collision devloop — curated or foreign destination wins; a2a only ever updates what it created",
    ] {
        assert!(err.contains(line), "missing `{line}` in:\n{err}");
    }
    let summary = stdout(&out);
    assert!(
        summary.contains("3 synced") && summary.contains("5 skipped"),
        "{summary}"
    );
    assert!(
        summary.contains(&*w.dest().to_string_lossy()),
        "the summary names where it wrote: {summary}"
    );
    assert!(
        !summary.contains("skipped-collision"),
        "the per-entry report belongs on stderr: {summary}"
    );
}

/// Idempotence, stated the way the submodule sees it: nothing moves, and the
/// repository's own view of itself is byte-identical to the first run's.
#[test]
fn a_second_run_with_no_changes_reports_unchanged_and_writes_nothing() {
    let w = World::new();
    w.apply();
    let status_after_first = w.status();
    let gitignore_after_first = std::fs::read(w.dest().join(".gitignore")).unwrap();
    let tree_after_first = a2a::sync::manifest::hash_path(&w.dest()).unwrap();

    let data = w.apply();
    assert_eq!(
        data["counts"],
        json!({"synced": 0, "unchanged": 3, "removed": 0, "skipped": 5, "refused": 0})
    );
    assert_eq!(reported(&data)["techniques"], "unchanged");
    assert_eq!(w.status(), status_after_first);
    assert_eq!(
        std::fs::read(w.dest().join(".gitignore")).unwrap(),
        gitignore_after_first
    );
    assert_eq!(
        a2a::sync::manifest::hash_path(&w.dest()).unwrap(),
        tree_after_first,
        "a second run writes no content"
    );
}

/// The allowlist is the one way past a refusal — and the copier still drops
/// `__pycache__` on the way through.
#[test]
fn allow_skills_flips_a_seed_list_skill_on_the_next_run() {
    let w = World::new();
    assert_eq!(reported(&w.apply())["slack-cli"], "skipped-side-effecting");
    assert!(!w.dest().join("slack-cli").exists());

    let out = w.run_with(
        &[("A2A_SYNC_ALLOW_SKILLS", "slack-cli")],
        &["--json", "sync", "skills"],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(reported(&data(&out))["slack-cli"], "synced");
    assert!(w.dest().join("slack-cli/SKILL.md").is_file());
    assert!(
        !w.dest().join("slack-cli/__pycache__").exists(),
        "an ignored entry never travels"
    );
    // ...and the block grew to cover it before the bytes landed.
    assert_eq!(w.status(), [" M .gitignore"]);
}

/// Owned and edited: refused, never clobbered.
#[test]
fn an_edited_destination_is_refused_and_the_edit_survives() {
    let w = World::new();
    w.apply();
    let edited = w.dest().join("techniques/SKILL.md");
    std::fs::write(&edited, b"locally improved").unwrap();

    let data = w.apply();
    assert_eq!(reported(&data)["techniques"], "refused-dest-drift");
    assert_eq!(data["counts"]["refused"], 1);
    assert_eq!(
        std::fs::read(&edited).unwrap(),
        b"locally improved",
        "a2a never overwrites what it did not last write"
    );
}

/// A source that went away takes its managed destination and its ledger entry
/// with it.
#[test]
fn a_deleted_source_removes_the_destination_and_its_ledger_entry() {
    let w = World::new();
    w.apply();
    assert!(w.dest().join("wordy").is_dir());
    std::fs::remove_dir_all(w.source().join("wordy")).unwrap();

    let data = w.apply();
    assert_eq!(reported(&data)["wordy"], "removed");
    assert_eq!(data["counts"]["removed"], 1);
    assert!(!w.dest().join("wordy").exists());
    assert!(w.manifest()["entries"]
        .as_object()
        .unwrap()
        .get("skill:default:wordy")
        .is_none());
    assert!(
        w.dest().join("techniques").is_dir(),
        "one deletion is not a sweep"
    );
}

/// `--dry-run` writes NOTHING — proven the way spec §8.8 demands, by the
/// repository's own view of itself and by a hash of the whole tree.
#[test]
fn dry_run_decides_everything_and_writes_nothing() {
    let w = World::new();
    let status_before = w.status();
    let tree_before = a2a::sync::manifest::hash_path(&w.dest()).unwrap();

    let out = w.run(&["--json", "sync", "skills", "--dry-run"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let data = data(&out);
    assert_eq!(data["dry_run"], true);
    assert_eq!(
        data["counts"],
        json!({"synced": 3, "unchanged": 0, "removed": 0, "skipped": 5, "refused": 0}),
        "the full classification is still printed"
    );
    assert_eq!(reported(&data).len(), 8);

    assert_eq!(w.status(), status_before);
    assert_eq!(
        a2a::sync::manifest::hash_path(&w.dest()).unwrap(),
        tree_before
    );
    assert!(!w.dest().join("techniques").exists());
    assert!(
        !w.manifest_path().exists(),
        "a dry run does not even open the ledger for writing"
    );
}

/// Containment before content: when the block cannot be written, the run stops
/// there — with exit 9 and not one destination directory created.
#[test]
fn a_gitignore_that_cannot_be_written_stops_the_run_before_any_content() {
    let w = World::new();
    let gitignore = w.dest().join(".gitignore");
    let mut perms = std::fs::metadata(&gitignore).unwrap().permissions();
    perms.set_mode(0o444);
    std::fs::set_permissions(&gitignore, perms).unwrap();

    let out = w.run(&["--json", "sync", "skills"]);
    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(error_code(&out), "SYNC_GITIGNORE_UNCOVERED");
    for name in ["techniques", "portable", "wordy"] {
        assert!(
            !w.dest().join(name).exists(),
            "{name} was created despite the containment failure"
        );
    }
    assert_eq!(
        std::fs::read_to_string(&gitignore).unwrap(),
        "*.log\n",
        "the file is exactly as the repository left it"
    );
    assert_eq!(w.status(), Vec::<String>::new());
}

/// The other submodule shape: no `.gitignore` at all, so the run's own block
/// arrives as an untracked file.
#[test]
fn a_repo_with_no_gitignore_gets_one_and_stays_contained() {
    let w = World::without_gitignore();
    w.apply();
    assert_eq!(w.status(), ["?? .gitignore"]);
    assert!(w.dest().join("techniques").is_dir());
}

/// `--claude-config-dir` outranks the environment — the flag level of the
/// precedence, exercised through the CLI.
#[test]
fn the_flag_outranks_the_environment() {
    let w = World::new();
    let elsewhere = w.tmp.path().join("empty-account");
    std::fs::create_dir_all(&elsewhere).unwrap();

    // Pointed at an empty account, the run has no source at all...
    let out = common::sync_a2a(&w.home, &w.codex_home, &elsewhere, w.tmp.path())
        .args(["--json", "sync", "skills"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(9));
    assert_eq!(error_code(&out), "SYNC_SKILLS_SOURCE_MISSING");

    // ...and the flag puts it back.
    let out = common::sync_a2a(&w.home, &w.codex_home, &elsewhere, w.tmp.path())
        .args(["--json", "sync", "skills", "--claude-config-dir"])
        .arg(&w.claude_dir)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(data(&out)["counts"]["synced"], 3);
}

/// The static preconditions, each refusing with its own code before anything is
/// written. Exit 9 is reserved for exactly these.
#[test]
fn the_static_preconditions_refuse_before_anything_is_written() {
    let w = World::new();

    // No codex home at all.
    let mut c = w.cmd();
    let out = c
        .env_remove("CODEX_HOME")
        .env_remove("HOME")
        .args(["--json", "sync", "skills"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(error_code(&out), "SYNC_CODEX_HOME_UNKNOWN");
    assert_eq!(
        envelope(&out)["error"]["remedy"],
        "export CODEX_HOME (or HOME)"
    );

    // A ledger that cannot be read is never treated as "a2a owns nothing".
    std::fs::create_dir_all(&w.home).unwrap();
    std::fs::write(w.manifest_path(), b"{ not json").unwrap();
    let out = w.run(&["--json", "sync", "skills"]);
    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(error_code(&out), "SYNC_MANIFEST_CORRUPT");
    assert!(!w.dest().join("techniques").exists());
    std::fs::remove_file(w.manifest_path()).unwrap();

    // A source root that is not there is named, not silently read as "every
    // managed skill has been deleted".
    let empty = w.tmp.path().join("no-skills-here");
    std::fs::create_dir_all(&empty).unwrap();
    let out = common::sync_a2a(&w.home, &w.codex_home, &empty, w.tmp.path())
        .args(["--json", "sync", "skills"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(error_code(&out), "SYNC_SKILLS_SOURCE_MISSING");
    assert!(envelope(&out)["error"]["message"]
        .as_str()
        .unwrap()
        .contains(&*empty.join("skills").to_string_lossy()));

    // Nothing at all was written by any of the three.
    assert_eq!(w.status(), Vec::<String>::new());
    assert!(!w.manifest_path().exists());
}

/// The guard that makes the whole suite safe to run on a developer's machine:
/// a codex home inside the real `$HOME` is refused, before any read or write.
#[test]
fn a_codex_home_inside_the_real_home_is_refused() {
    let w = World::new();
    let real_home = PathBuf::from(std::env::var_os("HOME").expect("HOME is set for the suite"));
    let never = real_home.join(".a2a-sync-guard-must-never-create-this");

    let mut c = w.cmd();
    let out = c
        .env("CODEX_HOME", &never)
        .args(["--json", "sync", "skills"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(error_code(&out), "SYNC_REFUSES_REAL_HOME");
    assert!(
        !never.exists(),
        "the guard runs before anything touches the filesystem"
    );
    assert!(!w.manifest_path().exists());
}

/// The CLI surface itself: both halves of `sync` are listed from the release
/// that ships either, so a caller cannot tell a missing feature from a typo.
#[test]
fn sync_help_lists_exactly_skills_and_mcp() {
    let out = assert_cmd::Command::cargo_bin("a2a")
        .unwrap()
        .args(["sync", "--help"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let text = stdout(&out);
    // EXACTLY these two, in this order — `help` is clap's own and appears under
    // every command group in this binary, so it is not one of ours.
    let listed: Vec<&str> = text
        .lines()
        .skip_while(|l| !l.starts_with("Commands:"))
        .skip(1)
        .take_while(|l| l.starts_with("  "))
        .filter_map(|l| l.split_whitespace().next())
        .filter(|name| *name != "help")
        .collect();
    assert_eq!(listed, ["skills", "mcp"], "{text}");
    // `a2a sync` with no subcommand is a usage error, like every other group.
    let out = assert_cmd::Command::cargo_bin("a2a")
        .unwrap()
        .arg("sync")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
