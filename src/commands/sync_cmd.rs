//! `a2a sync skills|mcp` — mirror Claude-side capability into the Codex home
//! (spec §6.1/§6.2).
//!
//! The destination is somebody's real home directory, and three rules are what
//! make writing into it defensible. Two of them are structural and live
//! elsewhere ([`crate::sync::skills::plan`] decides everything by reading, and
//! [`crate::sync::manifest`] records what a2a owns); the third is the ordering
//! this file enforces.
//!
//! **Static preconditions first, then containment, then content.** Every
//! refusal that can be decided without touching anything is decided before
//! anything is touched — no codex home, no Claude directory, a ledger that
//! cannot be read, a source root that is not there. Only then does apply mode
//! write the managed `.gitignore` block, **re-read it from disk**, and refuse
//! unless the block really does cover every managed directory. Copying starts
//! after that and not before: the destination is a git submodule, and a skill
//! tree that lands in it uncovered turns somebody's clean checkout into
//! hundreds of untracked files they did not ask for. `SYNC_GITIGNORE_UNCOVERED`
//! is the one refusal that stands in front of the content.
//!
//! **The codex home is resolved by [`doctor::effective_codex_home`], not
//! re-derived here.** Two commands asking the filesystem the same question
//! independently is exactly the drift `doctor`'s own module doc warns about,
//! and a `sync` that disagreed with `doctor` about where `$CODEX_HOME` is would
//! make every diagnosis wrong in a way neither command could see.
//!
//! **Exit 0 for every completed run**, including runs full of skips and
//! refusals; exit 9 is reserved for the static preconditions above. The verdict
//! travels in `data.counts`, because a fleet-wide `sync` that exited nonzero
//! whenever one destination had drifted would be re-run with a bypass by every
//! caller — which is precisely the clobber the ledger exists to prevent.

use crate::commands::doctor;
use crate::config;
use crate::home::{self, A2aHome};
use crate::output::{AppError, ExitCode, Outcome};
use crate::sync::manifest::{hash_path, Entry, Manifest, IGNORED_ENTRIES};
use crate::sync::{self, skills, Disposition, Kind, Plan};
use serde_json::{json, Value};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// The opt-in that lets a run touch the machine's real home.
///
/// A run-time switch rather than a config key (it never appears in `config
/// show`): the point of it is to be absent, so that a test — or a hook — that
/// forgot `CODEX_HOME` fails loudly instead of writing into `~/.codex`.
pub const ALLOW_REAL_HOME_ENV: &str = "A2A_SYNC_ALLOW_REAL_HOME";

/// `a2a sync skills [--dry-run] [--claude-config-dir <DIR>]`.
pub fn skills(
    home: &A2aHome,
    dry_run: bool,
    claude_config_dir: Option<&Path>,
    json_mode: bool,
    quiet: bool,
) -> Result<Outcome, AppError> {
    let (cfg, _origins, config_warnings) = config::load(home)?;
    let warn = |msg: &str| {
        if !quiet {
            eprintln!("a2a: warning: {msg}");
        }
    };

    // ---------------------------------------------- static preconditions
    let codex_home = doctor::effective_codex_home().ok_or_else(codex_home_unknown)?;
    // No `$HOME` at all (a launchd job, a container) is not an error here: there
    // is simply no real home to refuse against and no default Claude directory
    // to fall back to, so an unnamed source ends up reported as missing — which
    // is what it is.
    let real_home = std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from);
    let anchor = real_home.clone().unwrap_or_default();
    let claude_dir = sync::claude_config_dir(
        &cfg.sync,
        claude_config_dir,
        env_path("CLAUDE_CONFIG_DIR").as_deref(),
        &anchor,
    );
    let source_root = sync::skills_source_root(&claude_dir, &anchor);
    if let Some(h) = &real_home {
        sync::refuse_real_home(&codex_home, &claude_dir, h, allow_real_home())?;
    }
    let mut manifest = Manifest::load(home.root())?;
    let dest_root = codex_home.join("skills");
    // Reading, and only reading: a missing source root refuses here, before the
    // ignore block or a single byte of content.
    let plan = skills::plan(
        &source_root,
        &dest_root,
        &manifest,
        &cfg.sync,
        sync::SKILL_ACCOUNT,
    )?;

    let mut warnings: Vec<String> = config_warnings;
    warnings.extend(plan.warnings.iter().cloned());

    // ------------------------------------------------------------- apply
    if !dry_run {
        contain(&dest_root, &skills::managed_dirs(&plan, &manifest))?;
        let applied = apply(&plan, &mut manifest, sync::SKILL_ACCOUNT);
        // Saved even when a copy failed: everything already written stays
        // OWNED, or the next run would meet its own output as a collision.
        let saved = manifest.save(home.root());
        applied?;
        saved?;
    }

    // ------------------------------------------------------------ report
    if !quiet {
        for w in &warnings {
            warn(w);
        }
        for e in &plan.entries {
            eprintln!("{} {} — {}", e.disposition, e.name, e.detail);
        }
    }
    let counts = plan.counts();
    let summary = format!(
        "{}{} synced, {} unchanged, {} removed, {} skipped, {} refused{}",
        if dry_run { "dry run: " } else { "" },
        counts["synced"],
        counts["unchanged"],
        counts["removed"],
        counts["skipped"],
        counts["refused"],
        if dry_run {
            " — nothing was written".to_string()
        } else {
            format!(" → {}", dest_root.display())
        }
    );
    let data = if json_mode {
        json!({
            "kind": "skills",
            "account": sync::SKILL_ACCOUNT,
            "dry_run": dry_run,
            "dest": dest_root.to_string_lossy(),
            "counts": counts,
            "entries": plan.entries,
            "warnings": warnings,
        })
    } else {
        Value::String(summary)
    };
    Ok(Outcome::from(data))
}

/// `a2a sync mcp` — the MCP half of the surface.
///
/// Declared and dispatched from the same release as [`skills`] so that
/// `a2a sync --help` has always listed both halves; the translation itself
/// needs comment-preserving TOML mutation and lands with it.
pub fn mcp() -> Result<Outcome, AppError> {
    Err(AppError::new(
        "SYNC_MCP_UNAVAILABLE",
        ExitCode::Precondition,
        "`a2a sync mcp` is not available in this build",
    )
    .remedy("`a2a sync skills` mirrors the skills half today"))
}

/// Containment, before content and independent of it: rewrite the managed block
/// in the destination repository's `.gitignore`, then **re-read the file** and
/// refuse unless what is actually on disk covers every managed directory.
///
/// Every failure here — including an IO error on the write — is
/// `SYNC_GITIGNORE_UNCOVERED`, because from the caller's point of view they are
/// the same fact: a2a could not prove the repository will ignore what it is
/// about to create, so it did not create it.
fn contain(dest_root: &Path, dirs: &[String]) -> Result<(), AppError> {
    let path = dest_root.join(".gitignore");
    home::create_dir_0700(dest_root).map_err(|e| uncovered(&path, e))?;
    let current = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(uncovered(&path, e)),
    };
    let wanted = skills::rewrite_block(&current, dirs);
    if wanted != current {
        // Deliberately a plain write, NOT `store::write_atomic`: a rename would
        // succeed straight past a mode the repository's owner set, and would
        // replace a tracked file's inode into the bargain. A file somebody made
        // read-only is a refusal to be honoured, not an obstacle to route
        // around.
        std::fs::write(&path, wanted.as_bytes()).map_err(|e| uncovered(&path, e))?;
    }
    let on_disk = std::fs::read_to_string(&path).map_err(|e| uncovered(&path, e))?;
    if !skills::block_covers(&on_disk, dirs) {
        return Err(uncovered(
            &path,
            "the managed block does not cover every managed skill directory",
        ));
    }
    Ok(())
}

/// The only writer of skill content. Walks the plan the run already printed, so
/// what happens is exactly what was reported.
fn apply(plan: &Plan, manifest: &mut Manifest, account: &str) -> Result<(), AppError> {
    let now = chrono::Utc::now();
    for e in &plan.entries {
        match e.disposition {
            Disposition::Synced => {
                let Some(source) = e.source_path.as_deref() else {
                    continue; // a Synced entry always has a source; be total anyway
                };
                // Remove-then-copy, not copy-over: a file deleted from the
                // source must not survive in the destination.
                remove_dest(&e.dest_path).map_err(|err| copy_failed(&e.name, err))?;
                if let Err(err) = copy_managed(source, &e.dest_path) {
                    // Never leave a half-copy the ledger does not claim — the
                    // next run would meet it as somebody else's directory.
                    let _ = remove_dest(&e.dest_path);
                    return Err(copy_failed(&e.name, err));
                }
                // ONE hash for both ends. The copy is content-identical by
                // construction (the same `IGNORED_ENTRIES` are skipped on both
                // sides and neither mode nor mtime is hashed), and hashing the
                // two ends separately would let a source edited mid-run land an
                // entry that claims a source state the destination never had.
                let hash = hash_path(&e.dest_path).map_err(|err| copy_failed(&e.name, err))?;
                manifest.entries.insert(
                    Manifest::key(Kind::Skill, account, &e.name),
                    Entry {
                        source_path: source.to_path_buf(),
                        dest_path: e.dest_path.clone(),
                        source_hash: hash.clone(),
                        dest_hash: hash,
                        synced_at: now,
                        tool_version: env!("CARGO_PKG_VERSION").to_string(),
                        kind: Kind::Skill,
                        account: account.to_string(),
                    },
                );
            }
            Disposition::Removed => {
                remove_dest(&e.dest_path).map_err(|err| copy_failed(&e.name, err))?;
                manifest
                    .entries
                    .remove(&Manifest::key(Kind::Skill, account, &e.name));
            }
            // Unchanged keeps its entry; every skip and the drift refusal touch
            // neither the destination nor the ledger.
            _ => {}
        }
    }
    Ok(())
}

/// Copy a skill tree as CONTENTS: symlinks are followed (a skill symlinked into
/// another checkout is mirrored, never linked back into), `IGNORED_ENTRIES` are
/// dropped at every level, directories land 0700 and files 0600 — or 0700 when
/// the source file carries any execute bit, so an allowlisted skill's helper
/// script still runs.
fn copy_managed(source: &Path, dest: &Path) -> std::io::Result<()> {
    home::create_dir_0700(dest)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if IGNORED_ENTRIES.contains(&name.as_str()) {
            continue;
        }
        let (from, to) = (entry.path(), dest.join(&name));
        // `metadata`, not `file_type`: it FOLLOWS symlinks, which is the point.
        let meta = std::fs::metadata(&from)?;
        if meta.is_dir() {
            copy_managed(&from, &to)?;
            continue;
        }
        let bytes = std::fs::read(&from)?;
        let mode = if meta.permissions().mode() & 0o111 != 0 {
            0o700
        } else {
            0o600
        };
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&to)?;
        f.write_all(&bytes)?;
    }
    Ok(())
}

/// Remove a managed destination in whatever form it is in, including a broken
/// symlink. Absent is success — this runs to make the path free, not to prove
/// something was there.
fn remove_dest(p: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(p) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
        Ok(m) if m.is_dir() => std::fs::remove_dir_all(p),
        Ok(_) => std::fs::remove_file(p),
    }
}

fn env_path(key: &str) -> Option<PathBuf> {
    std::env::var_os(key)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// True unless [`ALLOW_REAL_HOME_ENV`] is unset or set to a falsy spelling —
/// the same reading [`crate::brokerclient::disabled`] gives its own switch, so
/// a cleared-badly variable (`A2A_SYNC_ALLOW_REAL_HOME=`) does not silently
/// unlock the machine's home.
fn allow_real_home() -> bool {
    match std::env::var(ALLOW_REAL_HOME_ENV) {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "false" | "no"
        ),
        Err(_) => false,
    }
}

fn codex_home_unknown() -> AppError {
    AppError::new(
        "SYNC_CODEX_HOME_UNKNOWN",
        ExitCode::Precondition,
        "no codex home: neither CODEX_HOME nor HOME names one",
    )
    .remedy("export CODEX_HOME (or HOME)")
}

fn uncovered(path: &Path, detail: impl std::fmt::Display) -> AppError {
    AppError::new(
        "SYNC_GITIGNORE_UNCOVERED",
        ExitCode::Precondition,
        format!("{}: {detail}", path.display()),
    )
    .remedy(
        "make the skills repository's .gitignore writable and free of a hand-edited \
         `a2a-sync` block, then re-run — a2a will not copy into a repository it cannot \
         prove will ignore what it creates",
    )
}

/// An IO failure mid-copy: not a completed run, so not exit 0, and not a static
/// precondition either — the run got as far as writing and the filesystem said
/// no.
fn copy_failed(name: &str, e: std::io::Error) -> AppError {
    AppError::new(
        "SYNC_SKILL_COPY_FAILED",
        ExitCode::Internal,
        format!("{name}: {e}"),
    )
    .remedy("re-run the sync once the destination is writable; the partial copy was removed")
}
