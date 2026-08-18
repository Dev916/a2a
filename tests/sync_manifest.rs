//! `sync-manifest.json` as a file: how it is written, how it is read back, and
//! what happens when a machine hands a2a one it cannot prove it wrote.
//!
//! The unit tests beside the module cover hashing and the key spelling; these
//! cover the ledger's on-disk lifecycle, which is the half a corrupted or
//! hand-edited file exercises.

use a2a::output::ExitCode;
use a2a::sync::manifest::{Entry, Manifest, MANIFEST_FILE, SCHEMA_VERSION};
use a2a::sync::Kind;
use std::os::unix::fs::PermissionsExt;

fn entry(kind: Kind, account: &str) -> Entry {
    Entry {
        source_path: "/src/techniques".into(),
        dest_path: "/dest/techniques".into(),
        source_hash: "aaaa".to_string(),
        dest_hash: "bbbb".to_string(),
        synced_at: chrono::Utc::now(),
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        kind,
        account: account.to_string(),
    }
}

/// A machine that has never synced is a normal machine: no file, no error,
/// and a manifest that owns nothing.
#[test]
fn a_missing_manifest_loads_as_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let m = Manifest::load(tmp.path()).unwrap();
    assert_eq!(m.schema_version, SCHEMA_VERSION);
    assert!(m.entries.is_empty());
    assert!(
        !tmp.path().join(MANIFEST_FILE).exists(),
        "loading must not create the ledger"
    );
}

/// The ledger round-trips, lands at `$A2A_HOME/sync-manifest.json`, and is
/// 0600 — it names every path a2a has ever written into a foreign home.
#[test]
fn the_manifest_round_trips_at_0600() {
    let tmp = tempfile::tempdir().unwrap();
    let mut m = Manifest::load(tmp.path()).unwrap();
    m.entries.insert(
        Manifest::key(Kind::Skill, "default", "techniques"),
        entry(Kind::Skill, "default"),
    );
    m.entries.insert(
        Manifest::key(Kind::Mcp, "claude-revenium", "linear"),
        entry(Kind::Mcp, "claude-revenium"),
    );
    m.save(tmp.path()).unwrap();

    let path = tmp.path().join(MANIFEST_FILE);
    assert_eq!(path, Manifest::path(tmp.path()));
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "the ledger is 0600, like every other a2a file");

    let back = Manifest::load(tmp.path()).unwrap();
    assert_eq!(back.schema_version, SCHEMA_VERSION);
    assert_eq!(back.entries, m.entries);
    assert_eq!(
        back.owns(Kind::Skill, "default", "techniques").unwrap().kind,
        Kind::Skill
    );
    assert!(back.owns(Kind::Skill, "default", "linear").is_none());
    assert_eq!(
        back.owner_account(Kind::Mcp, "linear"),
        Some("claude-revenium")
    );
}

/// An unparseable ledger is a precondition failure with a remedy that says how
/// to get moving again — never a silent empty manifest, which would let the
/// next run adopt (and then clobber) destinations a2a cannot prove it created.
#[test]
fn an_unparseable_manifest_refuses_with_the_pinned_remedy() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join(MANIFEST_FILE), "{ not json").unwrap();
    let err = Manifest::load(tmp.path()).unwrap_err();
    assert_eq!(err.code, "SYNC_MANIFEST_CORRUPT");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(err.message.contains(MANIFEST_FILE), "{}", err.message);
    assert_eq!(
        err.remedy.as_deref(),
        Some(
            "move sync-manifest.json aside and re-run the sync — a2a will only re-adopt what it can prove it wrote"
        )
    );
}

/// A ledger from a future (or past) schema is the same refusal: a binary that
/// cannot read the ownership rules must not act on them.
#[test]
fn a_foreign_schema_version_is_the_same_refusal() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join(MANIFEST_FILE),
        r#"{"schema_version":2,"entries":{}}"#,
    )
    .unwrap();
    let err = Manifest::load(tmp.path()).unwrap_err();
    assert_eq!(err.code, "SYNC_MANIFEST_CORRUPT");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(err.message.contains('2'), "{}", err.message);
}

/// The saved file carries the schema version and the pinned entry key spelling
/// — both are what a future migration reads first.
#[test]
fn the_saved_file_carries_the_schema_version_and_the_key_spelling() {
    let tmp = tempfile::tempdir().unwrap();
    let mut m = Manifest::load(tmp.path()).unwrap();
    m.entries.insert(
        Manifest::key(Kind::Skill, "default", "techniques"),
        entry(Kind::Skill, "default"),
    );
    m.save(tmp.path()).unwrap();
    let raw: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(tmp.path().join(MANIFEST_FILE)).unwrap())
            .unwrap();
    assert_eq!(raw["schema_version"], 1);
    assert!(
        raw["entries"]["skill:default:techniques"].is_object(),
        "{raw}"
    );
}
