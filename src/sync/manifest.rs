//! `sync-manifest.json` — the ownership ledger (spec §6.1).
//!
//! Everything `sync` is allowed to touch is written down here, because the
//! destination is somebody else's home directory: a curated skill, a
//! hand-written `[mcp_servers.mx]` table and a directory left behind by another
//! tool all look exactly like something a2a made, and the only defensible way
//! to tell them apart is to have recorded what a2a made. Hence the two rules
//! this file exists to enforce:
//!
//! * an entry a2a did not create is never updated and never removed;
//! * an entry a2a DID create whose destination changed underneath it is
//!   refused rather than clobbered — which is why every entry carries a
//!   `dest_hash` as well as a `source_hash`.
//!
//! The ledger lives at `$A2A_HOME/sync-manifest.json` at 0600 and is written
//! through [`crate::store::write_atomic`], so a reader (including the next run)
//! only ever sees a complete ownership set.

use super::Kind;
use crate::output::{AppError, ExitCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

pub const MANIFEST_FILE: &str = "sync-manifest.json";

/// Bumped when the [`Entry`] shape changes. A ledger written at any other
/// version is refused rather than guessed at — see [`Manifest::load`].
pub const SCHEMA_VERSION: u32 = 1;

/// Names skipped at every level of a hashed or copied tree.
///
/// These are caches and metadata that differ between two identical checkouts
/// (`__pycache__` appears the first time a skill's helper script runs, `.git`
/// is a whole second repository). Hashing them would report drift on a
/// destination nobody touched, which is the one failure that would teach an
/// operator to ignore drift reports.
pub const IGNORED_ENTRIES: [&str; 4] = [".git", ".DS_Store", "__pycache__", "node_modules"];

/// One thing a2a put somewhere, and the two hashes that say whether either end
/// has moved since.
///
/// The field set is pinned by a test: adding a field without bumping
/// [`SCHEMA_VERSION`] is a test failure, because an older binary reading a
/// newer ledger would otherwise silently drop what it could not represent —
/// and dropping an entry means forgetting that a2a owns a destination.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct Entry {
    pub source_path: PathBuf,
    pub dest_path: PathBuf,
    /// `kind == Skill` → [`hash_path`] of the source tree.
    /// `kind == Mcp` → [`canonical_server_hash`] of that ONE source server
    /// object; never a hash of the whole `.claude.json`.
    pub source_hash: String,
    /// `kind == Skill` → [`hash_path`] of the destination tree.
    /// `kind == Mcp` → `mcp::canonical_table_hash` of that one
    /// `[mcp_servers.<name>]` table.
    pub dest_hash: String,
    pub synced_at: chrono::DateTime<chrono::Utc>,
    pub tool_version: String,
    pub kind: Kind,
    pub account: String,
}

/// The whole ledger: schema version plus `"<kind>:<account>:<name>"` → entry.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Manifest {
    pub schema_version: u32,
    pub entries: BTreeMap<String, Entry>,
}

impl Default for Manifest {
    fn default() -> Self {
        Manifest {
            schema_version: SCHEMA_VERSION,
            entries: BTreeMap::new(),
        }
    }
}

impl Manifest {
    pub fn path(home: &Path) -> PathBuf {
        home.join(MANIFEST_FILE)
    }

    /// A missing ledger is an EMPTY ledger, not an error: a machine that has
    /// never synced is a normal machine. Anything else unreadable — bad JSON,
    /// a foreign schema, an IO error — is a precondition failure, because the
    /// alternative is treating "I cannot tell what I own" as "I own nothing",
    /// and a run that believes it owns nothing is one that adopts (and then
    /// overwrites) whatever it finds.
    pub fn load(home: &Path) -> Result<Manifest, AppError> {
        let path = Self::path(home);
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Manifest::default()),
            Err(e) => return Err(corrupt(&path, e)),
        };
        let manifest: Manifest = serde_json::from_str(&text).map_err(|e| corrupt(&path, e))?;
        if manifest.schema_version != SCHEMA_VERSION {
            return Err(corrupt(
                &path,
                format!(
                    "schema_version {}, this binary reads v{SCHEMA_VERSION}",
                    manifest.schema_version
                ),
            ));
        }
        Ok(manifest)
    }

    /// Written once per run, after every entry is settled: a ledger that only
    /// half-describes what just happened is worse than one written a moment
    /// late, and `write_atomic` means a crash leaves the previous complete one.
    pub fn save(&self, home: &Path) -> Result<(), AppError> {
        let path = Self::path(home);
        let mut bytes = serde_json::to_vec_pretty(self).map_err(|e| {
            AppError::new(
                "SYNC_MANIFEST_WRITE",
                ExitCode::Internal,
                format!("{}: {e}", path.display()),
            )
        })?;
        bytes.push(b'\n');
        crate::store::write_atomic(&path, &bytes).map_err(|e| {
            AppError::new(
                "SYNC_MANIFEST_WRITE",
                ExitCode::Internal,
                format!("{}: {e}", path.display()),
            )
        })
    }

    /// The ONE place the key spelling exists.
    pub fn key(kind: Kind, account: &str, name: &str) -> String {
        format!("{}:{account}:{name}", kind.as_str())
    }

    pub fn owns(&self, kind: Kind, account: &str, name: &str) -> Option<&Entry> {
        self.entries.get(&Self::key(kind, account, name))
    }

    /// Which account owns `name` for this kind, if any.
    ///
    /// Lives beside [`Manifest::key`] and is written in terms of it, so the key
    /// spelling still exists exactly once — nothing in the crate parses a key
    /// back apart, which is what keeps a name containing a `:` from turning
    /// into a silently wrong owner.
    pub fn owner_account(&self, kind: Kind, name: &str) -> Option<&str> {
        self.entries.iter().find_map(|(k, e)| {
            (*k == Self::key(kind, &e.account, name)).then_some(e.account.as_str())
        })
    }

    pub fn entries_for(&self, kind: Kind) -> impl Iterator<Item = (&String, &Entry)> {
        self.entries.iter().filter(move |(_, e)| e.kind == kind)
    }
}

/// What to do about a ledger a2a cannot read, spelled **once**.
///
/// `doctor`'s `sync_state` check reports the same condition from a different
/// direction (a warning in a preflight rather than a refusal in a run), and two
/// sentences saying the same thing slightly differently is how an operator ends
/// up believing they are two problems.
pub const CORRUPT_REMEDY: &str = "move sync-manifest.json aside and re-run the sync — a2a will only re-adopt what it can prove it wrote";

fn corrupt(path: &Path, detail: impl std::fmt::Display) -> AppError {
    AppError::new(
        "SYNC_MANIFEST_CORRUPT",
        ExitCode::Precondition,
        format!("{}: {detail}", path.display()),
    )
    .remedy(CORRUPT_REMEDY)
}

// ---------------------------------------------------------------------------
// What `doctor` reports about the ledger
// ---------------------------------------------------------------------------

/// One look at the ledger and its destinations, as counts and booleans —
/// **never** as content.
///
/// It lives here rather than next to the check that prints it because every
/// question it answers is a question about ownership, and a summary computed in
/// `doctor` would be a second implementation of the rules this file exists to
/// hold. That also makes `doctor`'s `sync_state` a pure function of this value,
/// testable at every branch with no tree on disk.
#[derive(Default, PartialEq, Eq, Clone, Debug)]
pub struct DriftSummary {
    pub entries: usize,
    /// Destinations whose content no longer hashes to what the ledger recorded.
    pub drifted: usize,
    /// Destinations the ledger owns that are not there at all.
    pub missing_dest: usize,
    /// **Skills only — an mcp entry can never contribute to this count.**
    /// [`drift_summary`] is given a codex home and no Claude source path, so it
    /// has no way to know whether a *server* left `.claude.json`; the file the
    /// ledger names is the whole 200 KB config, whose presence says nothing
    /// about one server. A count that silently meant "skills" would be worse
    /// than one that says so.
    pub orphaned_source: usize,
    /// The skills repository's managed `.gitignore` block does not cover every
    /// directory the ledger owns — i.e. a managed skill is visible to that
    /// repository's `git status` and can be committed by accident.
    pub ignore_block_uncovered: bool,
    pub last_synced_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Which kinds any of the above was found in, sorted and deduplicated, so a
    /// report can name `a2a sync skills` and `a2a sync mcp` only when they are
    /// the commands that would actually fix something.
    pub kinds_affected: Vec<Kind>,
}

/// Compare every entry in the ledger with what is at its destination today.
///
/// Reading only, and never into the report: this hashes trees and TOML tables
/// and reports counts. It is deliberately tolerant of a codex home that is
/// missing or unparseable — a preflight that failed because a destination was
/// gone would be reporting the very thing it exists to describe.
pub fn drift_summary(manifest: &Manifest, codex_home: &Path) -> DriftSummary {
    let mut summary = DriftSummary {
        entries: manifest.entries.len(),
        last_synced_at: manifest.entries.values().map(|e| e.synced_at).max(),
        ..DriftSummary::default()
    };
    let mut affected: std::collections::BTreeSet<Kind> = std::collections::BTreeSet::new();

    for (_, entry) in manifest.entries_for(Kind::Skill) {
        if !entry.dest_path.exists() {
            summary.missing_dest += 1;
            affected.insert(Kind::Skill);
        } else if hash_path(&entry.dest_path).ok().as_deref() != Some(entry.dest_hash.as_str()) {
            summary.drifted += 1;
            affected.insert(Kind::Skill);
        }
        if !entry.source_path.exists() {
            summary.orphaned_source += 1;
            affected.insert(Kind::Skill);
        }
    }

    // An unreadable or absent `config.toml` is an EMPTY document, which reports
    // every managed table as missing — true, and the sentence an operator whose
    // codex home was replaced needs to read.
    let doc = super::mcp::read_target(codex_home).unwrap_or_default();
    let names = super::mcp::dest_names(&doc);
    for (key, entry) in manifest.entries_for(Kind::Mcp) {
        // The destination's own names are matched FORWARD through
        // [`Manifest::key`] rather than by taking a key apart: the key spelling
        // exists in exactly one place, and a server name containing a `:` would
        // turn any parse of it into a silently wrong answer.
        let found = names
            .iter()
            .find(|name| Manifest::key(Kind::Mcp, &entry.account, name) == **key);
        match found {
            None => {
                summary.missing_dest += 1;
                affected.insert(Kind::Mcp);
            }
            Some(name) => {
                if super::mcp::dest_hash(&doc, name).as_deref() != Some(entry.dest_hash.as_str()) {
                    summary.drifted += 1;
                    affected.insert(Kind::Mcp);
                }
            }
        }
    }

    // The managed directories come from the ledger, and so does the tree they
    // live in: `<CODEX_HOME>/skills` is spelled by the applier, and re-deriving
    // it here would be a second spelling that could drift from it. An empty
    // ledger manages no directory and therefore cannot be uncovered.
    let dirs = super::skills::managed_dirs(&super::Plan::default(), manifest);
    if let Some(root) = manifest
        .entries_for(Kind::Skill)
        .next()
        .and_then(|(_, e)| e.dest_path.parent())
    {
        if !dirs.is_empty() {
            let ignore = std::fs::read_to_string(root.join(".gitignore")).unwrap_or_default();
            if !super::skills::block_covers(&ignore, &dirs) {
                summary.ignore_block_uncovered = true;
                affected.insert(Kind::Skill);
            }
        }
    }

    summary.kinds_affected = affected.into_iter().collect();
    summary
}

/// Content and relative layout of a file or directory — deliberately NOT
/// metadata.
///
/// Mode and mtime are excluded so that a fresh copy of a source tree hashes
/// equal to it: the question this answers is "did anybody edit the
/// destination", and a `cp` that moved an mtime is not an edit. Symlinks are
/// followed (a skill symlinked into another repo is synced as its contents),
/// and every [`IGNORED_ENTRIES`] name is excluded at every level.
///
/// Feeds `sha2::Sha256` RAW BYTES rather than going through
/// [`crate::brief::sha256_hex`], which takes `&str`: skill trees legitimately
/// carry binaries (helper scripts, images, index files), and a hasher that
/// cannot represent them would either panic or quietly hash a lossy
/// replacement of somebody's file.
pub fn hash_path(p: &Path) -> std::io::Result<String> {
    let mut hasher = Sha256::new();
    if std::fs::metadata(p)?.is_dir() {
        let mut files: Vec<(String, PathBuf)> = Vec::new();
        collect_files(p, "", &mut files)?;
        files.sort_by(|a, b| a.0.cmp(&b.0));
        for (rel, path) in files {
            let bytes = std::fs::read(&path)?;
            // `<relpath>\0<len>\0<bytes>`: the length is what stops two
            // adjacent files from hashing the same as one concatenation of
            // them, and the relpath is what makes a rename a change.
            hasher.update(rel.as_bytes());
            hasher.update([0u8]);
            hasher.update(bytes.len().to_string().as_bytes());
            hasher.update([0u8]);
            hasher.update(&bytes);
        }
    } else {
        hasher.update(std::fs::read(p)?);
    }
    Ok(hex(&hasher.finalize()))
}

/// Every file under `dir`, as `(relative path, absolute path)`. Directories
/// contribute nothing of their own — an empty directory is not content.
fn collect_files(
    dir: &Path,
    prefix: &str,
    out: &mut Vec<(String, PathBuf)>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if IGNORED_ENTRIES.contains(&name.as_str()) {
            continue;
        }
        let path = entry.path();
        let rel = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        // `metadata` (not `entry.file_type`) FOLLOWS symlinks, which is the
        // whole point: a symlinked skill is synced as its contents.
        if std::fs::metadata(&path)?.is_dir() {
            collect_files(&path, &rel, out)?;
        } else {
            out.push((rel, path));
        }
    }
    Ok(())
}

/// The `source_hash` of ONE mcp entry: sha256 over a canonical JSON projection
/// of that single server object — keys sorted, every scalar rendered as a
/// string, no whitespace.
///
/// Hashing the whole `.claude.json` is explicitly forbidden. That file is 200 KB
/// of somebody else's schema; one hash of it would be shared by every managed
/// server, so an unrelated edit anywhere in it would mark every server changed
/// and the `unchanged` disposition would never fire again.
///
/// Takes anything serializable so the mcp module's [`super::mcp::SourceServer`]
/// projection can be hashed directly (`canonical_server_hash(&server)`), with no
/// second spelling of the normalization living next to the reader. Hashing the
/// projection rather than the raw JSON object is deliberate: a key Claude adds
/// to a server object next release is not drift in a server a2a already syncs.
pub fn canonical_server_hash(server: &impl Serialize) -> String {
    let value = serde_json::to_value(server).expect("a source server projection always serializes");
    let mut canonical = String::new();
    write_canonical(&value, &mut canonical);
    hex(&Sha256::digest(canonical.as_bytes()))
}

fn write_canonical(v: &serde_json::Value, out: &mut String) {
    match v {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&quote(k));
                out.push(':');
                write_canonical(&map[k], out);
            }
            out.push('}');
        }
        serde_json::Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        // Scalars are rendered AS STRINGS, so a value that arrives as `8080`
        // from one reader and `"8080"` from another hashes the same.
        serde_json::Value::String(s) => out.push_str(&quote(s)),
        serde_json::Value::Null => out.push_str("\"\""),
        other => out.push_str(&quote(&other.to_string())),
    }
}

fn quote(s: &str) -> String {
    serde_json::to_string(s).expect("a string always serializes")
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String is infallible.
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture_skill() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sync/skills/techniques")
    }

    fn copy_tree(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for e in std::fs::read_dir(src).unwrap() {
            let e = e.unwrap();
            let (from, to) = (e.path(), dst.join(e.file_name()));
            if std::fs::metadata(&from).unwrap().is_dir() {
                copy_tree(&from, &to);
            } else {
                std::fs::copy(&from, &to).unwrap();
            }
        }
    }

    fn an_entry() -> Entry {
        Entry {
            source_path: "/src/techniques".into(),
            dest_path: "/dest/techniques".into(),
            source_hash: "aa".into(),
            dest_hash: "bb".into(),
            synced_at: chrono::Utc::now(),
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
            kind: Kind::Skill,
            account: "default".into(),
        }
    }

    /// The entry shape is a schema. A field added without a version bump is a
    /// test failure here, on purpose.
    #[test]
    fn an_entry_carries_exactly_the_pinned_field_set() {
        let entry = an_entry();
        let v = serde_json::to_value(&entry).unwrap();
        let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "account",
                "dest_hash",
                "dest_path",
                "kind",
                "source_hash",
                "source_path",
                "synced_at",
                "tool_version",
            ]
        );
        assert_eq!(v["kind"], "skill");
        assert!(
            v["synced_at"].as_str().unwrap().ends_with('Z'),
            "synced_at is RFC3339 UTC: {}",
            v["synced_at"]
        );
        let back: Entry = serde_json::from_value(v).unwrap();
        assert_eq!(back, entry);
        assert_eq!(SCHEMA_VERSION, 1);
    }

    #[test]
    fn a_key_is_kind_account_name_and_nothing_parses_it_back() {
        assert_eq!(
            Manifest::key(Kind::Skill, "default", "techniques"),
            "skill:default:techniques"
        );
        assert_eq!(
            Manifest::key(Kind::Mcp, "claude-revenium", "linear"),
            "mcp:claude-revenium:linear"
        );

        let mut m = Manifest::default();
        let mut mcp = an_entry();
        mcp.kind = Kind::Mcp;
        mcp.account = "claude-revenium".into();
        m.entries.insert(
            Manifest::key(Kind::Skill, "default", "techniques"),
            an_entry(),
        );
        m.entries.insert(
            Manifest::key(Kind::Mcp, "claude-revenium", "linear"),
            mcp.clone(),
        );

        assert!(m.owns(Kind::Skill, "default", "techniques").is_some());
        assert!(
            m.owns(Kind::Skill, "claude-revenium", "techniques")
                .is_none(),
            "ownership is per (kind, account, name), not per name"
        );
        assert_eq!(
            m.owner_account(Kind::Mcp, "linear"),
            Some("claude-revenium")
        );
        assert_eq!(m.owner_account(Kind::Skill, "linear"), None);
        assert_eq!(m.owner_account(Kind::Mcp, "techniques"), None);

        let skills: Vec<&String> = m.entries_for(Kind::Skill).map(|(k, _)| k).collect();
        assert_eq!(skills, ["skill:default:techniques"]);
        assert_eq!(m.entries_for(Kind::Mcp).count(), 1);
    }

    /// Content and layout, not metadata: a fresh copy hashes equal to its
    /// source, so a just-synced destination reports `unchanged` rather than
    /// drift on the very next run.
    #[test]
    fn hash_path_covers_content_and_layout_but_not_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let copy = tmp.path().join("techniques");
        copy_tree(&fixture_skill(), &copy);
        let source = hash_path(&fixture_skill()).unwrap();
        assert_eq!(
            hash_path(&copy).unwrap(),
            source,
            "a fresh copy is unchanged"
        );

        // Mode is not content.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            copy.join("SKILL.md"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        assert_eq!(hash_path(&copy).unwrap(), source, "mode is not content");

        // Neither is a cache directory anybody's tooling might have left.
        std::fs::create_dir_all(copy.join("__pycache__")).unwrap();
        std::fs::write(copy.join("__pycache__/x.pyc"), b"\x00junk").unwrap();
        std::fs::write(copy.join(".DS_Store"), b"finder").unwrap();
        assert_eq!(
            hash_path(&copy).unwrap(),
            source,
            "IGNORED_ENTRIES are excluded at every level"
        );

        // A real edit IS content — as is a rename, which is why the relative
        // path is hashed alongside the bytes.
        std::fs::write(copy.join("NOTES.md"), b"hello").unwrap();
        let with_file = hash_path(&copy).unwrap();
        assert_ne!(with_file, source);
        std::fs::rename(copy.join("NOTES.md"), copy.join("NOTES2.md")).unwrap();
        assert_ne!(hash_path(&copy).unwrap(), with_file, "layout is hashed too");
    }

    /// Skill trees carry binaries. The fixture carries a deliberately
    /// non-UTF-8 file so this constraint is enforced by the suite rather than
    /// by a comment above `hash_path`.
    #[test]
    fn hash_path_hashes_non_utf8_bytes() {
        let binary = fixture_skill().join("assets/corpus.idx");
        let bytes = std::fs::read(&binary).unwrap();
        assert!(
            String::from_utf8(bytes.clone()).is_err(),
            "the fixture must actually be non-UTF-8"
        );

        let tmp = tempfile::tempdir().unwrap();
        let copy = tmp.path().join("techniques");
        copy_tree(&fixture_skill(), &copy);
        let before = hash_path(&copy).unwrap();
        assert_eq!(before, hash_path(&fixture_skill()).unwrap());

        let mut flipped = bytes.clone();
        flipped[1] ^= 0x01;
        std::fs::write(copy.join("assets/corpus.idx"), &flipped).unwrap();
        assert_ne!(
            hash_path(&copy).unwrap(),
            before,
            "one flipped byte inside a binary is drift"
        );
        // A file hashes on its own too.
        assert_ne!(hash_path(&binary).unwrap(), before);
    }

    /// An mcp entry whose source vanished is not a thing this function can
    /// know: it is given a codex home and no Claude source path, so the file it
    /// would have to look in is not even named here. The count says "skills"
    /// out loud rather than quietly meaning it.
    #[test]
    fn an_mcp_entry_never_contributes_to_an_orphaned_source() {
        let tmp = tempfile::tempdir().unwrap();
        let codex = tmp.path().join("codex");
        std::fs::create_dir_all(&codex).unwrap();
        let write = |body: &str| std::fs::write(codex.join("config.toml"), body).unwrap();
        write("[mcp_servers.linear]\nurl = \"https://mcp.linear.app/mcp\"\n");

        let doc = crate::sync::mcp::read_target(&codex).unwrap();
        let mut entry = an_entry();
        entry.kind = Kind::Mcp;
        // A source path that is not there at all — an ordinary state for the
        // ledger's `.claude.json` on a machine syncing a second account.
        entry.source_path = tmp.path().join("gone/.claude.json");
        entry.dest_path = codex.join("config.toml");
        entry.dest_hash = crate::sync::mcp::dest_hash(&doc, "linear").unwrap();
        let mut m = Manifest::default();
        m.entries
            .insert(Manifest::key(Kind::Mcp, "default", "linear"), entry);

        let clean = drift_summary(&m, &codex);
        assert_eq!(clean.entries, 1);
        assert_eq!(clean.orphaned_source, 0, "mcp entries contribute 0, always");
        assert_eq!((clean.drifted, clean.missing_dest), (0, 0));
        assert!(
            !clean.ignore_block_uncovered,
            "no skill dirs, nothing to cover"
        );
        assert_eq!(clean.kinds_affected, Vec::<Kind>::new());
        assert!(clean.last_synced_at.is_some());

        // A table edited underneath the ledger is drift ...
        write("[mcp_servers.linear]\nurl = \"https://elsewhere/mcp\"\n");
        let drifted = drift_summary(&m, &codex);
        assert_eq!((drifted.drifted, drifted.missing_dest), (1, 0));
        assert_eq!(drifted.kinds_affected, vec![Kind::Mcp]);
        assert_eq!(drifted.orphaned_source, 0);

        // ... and a table that is gone is missing, which is a different verdict.
        write("model = \"gpt-5-codex\"\n");
        let missing = drift_summary(&m, &codex);
        assert_eq!((missing.drifted, missing.missing_dest), (0, 1));
    }

    /// The three skill-side conditions, each counted once and each naming its
    /// kind — a summary that reported four warnings for one broken entry would
    /// teach an operator to skim it.
    #[test]
    fn drift_summary_counts_a_drifted_a_missing_and_an_orphaned_skill() {
        let tmp = tempfile::tempdir().unwrap();
        let skills = tmp.path().join("codex/skills");
        std::fs::create_dir_all(&skills).unwrap();
        let source = tmp.path().join("src/techniques");
        copy_tree(&fixture_skill(), &source);

        let skill = |name: &str, dest_exists: bool, source_path: &Path| {
            let dest = skills.join(name);
            if dest_exists {
                copy_tree(&fixture_skill(), &dest);
            }
            let mut e = an_entry();
            e.source_path = source_path.to_path_buf();
            e.dest_path = dest.clone();
            e.dest_hash = hash_path(&fixture_skill()).unwrap();
            e
        };
        let mut m = Manifest::default();
        m.entries.insert(
            Manifest::key(Kind::Skill, "default", "clean"),
            skill("clean", true, &source),
        );
        m.entries.insert(
            Manifest::key(Kind::Skill, "default", "drifted"),
            skill("drifted", true, &source),
        );
        m.entries.insert(
            Manifest::key(Kind::Skill, "default", "gone"),
            skill("gone", false, &source),
        );
        m.entries.insert(
            Manifest::key(Kind::Skill, "default", "orphan"),
            skill("orphan", true, &tmp.path().join("src/deleted")),
        );
        std::fs::write(skills.join("drifted/SKILL.md"), "somebody edited this").unwrap();

        let s = drift_summary(&m, &tmp.path().join("codex"));
        assert_eq!(s.entries, 4);
        assert_eq!(s.drifted, 1, "only the edited destination");
        assert_eq!(s.missing_dest, 1, "only the destination that is not there");
        assert_eq!(
            s.orphaned_source, 1,
            "only the entry whose source went away"
        );
        assert_eq!(s.kinds_affected, vec![Kind::Skill]);
    }

    /// The containment block is checked against the ledger's OWN directories,
    /// read out of the entries rather than out of a second spelling of
    /// `<CODEX_HOME>/skills` — and an empty ledger has nothing to cover, which
    /// is not the same as an uncovered one.
    #[test]
    fn the_ignore_block_is_uncovered_only_when_a_managed_dir_is_missing_from_it() {
        let tmp = tempfile::tempdir().unwrap();
        let codex = tmp.path().join("codex");
        let skills = codex.join("skills");
        std::fs::create_dir_all(&skills).unwrap();

        assert!(
            !drift_summary(&Manifest::default(), &codex).ignore_block_uncovered,
            "an empty ledger manages no directory"
        );

        let mut m = Manifest::default();
        for name in ["one", "two"] {
            copy_tree(&fixture_skill(), &skills.join(name));
            let mut e = an_entry();
            e.source_path = fixture_skill();
            e.dest_path = skills.join(name);
            e.dest_hash = hash_path(&fixture_skill()).unwrap();
            m.entries
                .insert(Manifest::key(Kind::Skill, "default", name), e);
        }
        assert!(
            drift_summary(&m, &codex).ignore_block_uncovered,
            "no .gitignore at all covers nothing"
        );

        let dirs = ["one".to_string(), "two".to_string()];
        std::fs::write(
            skills.join(".gitignore"),
            crate::sync::skills::managed_block(&dirs),
        )
        .unwrap();
        let covered = drift_summary(&m, &codex);
        assert!(!covered.ignore_block_uncovered);
        assert_eq!(covered.kinds_affected, Vec::<Kind>::new());

        std::fs::write(
            skills.join(".gitignore"),
            crate::sync::skills::managed_block(&dirs[..1]),
        )
        .unwrap();
        let half = drift_summary(&m, &codex);
        assert!(half.ignore_block_uncovered, "one dir short is uncovered");
        assert_eq!(half.kinds_affected, vec![Kind::Skill]);
    }

    /// An mcp entry's `source_hash` is about ONE server. Two servers in the
    /// same file hash differently, and editing a neighbour leaves a given
    /// server's hash exactly where it was.
    #[test]
    fn canonical_server_hash_is_per_server_not_per_file() {
        let source = json!({
            "mcpServers": {
                "linear": {"type": "http", "url": "https://mcp.linear.app/mcp"},
                "mx": {"command": "mx", "args": ["mcp"], "env": {"MX_MODE": "local"}},
            }
        });
        let linear = canonical_server_hash(&source["mcpServers"]["linear"]);
        let mx = canonical_server_hash(&source["mcpServers"]["mx"]);
        assert_ne!(linear, mx, "two servers are two hashes");

        let mut edited = source.clone();
        edited["mcpServers"]["mx"]["args"] = json!(["mcp", "--verbose"]);
        assert_eq!(
            canonical_server_hash(&edited["mcpServers"]["linear"]),
            linear,
            "an unrelated server's edit is not this server's drift"
        );
        assert_ne!(canonical_server_hash(&edited["mcpServers"]["mx"]), mx);

        // Key ORDER is not content; a scalar's JSON type is not content either.
        let reordered = json!({"url": "https://mcp.linear.app/mcp", "type": "http"});
        assert_eq!(canonical_server_hash(&reordered), linear);
        assert_eq!(
            canonical_server_hash(&json!({"port": 8080})),
            canonical_server_hash(&json!({"port": "8080"}))
        );
        assert_ne!(
            canonical_server_hash(&json!({"a": "b"})),
            canonical_server_hash(&json!({"a": "c"}))
        );
    }
}
