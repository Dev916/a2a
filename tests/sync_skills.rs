//! The skills planner over the fixture tree: what a run WOULD do, decided
//! entirely by reading.
//!
//! Task 2 ships the planner half only — there is no `a2a sync skills` command
//! yet, so these drive the library the way the command will. The shapes under
//! `tests/fixtures/sync/skills/` mirror the real `~/.claude/skills` verified in
//! the plan's Ground truth: a plain `README.md`, a directory that is not a
//! skill, a symlinked skill, a `__pycache__`, a seed-list skill and a skill
//! whose front matter alone condemns it.

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
