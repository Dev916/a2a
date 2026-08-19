//! The MCP source reader and its classifier: what a2a would translate out of a
//! Claude config, and — far more often — what it refuses to.
//!
//! Task 4 ships the classification half only. There is no `a2a sync mcp`
//! command yet (Task 5 renders the TOML and writes it), so these drive the
//! library the way the command will. The two fixtures mirror the real
//! `~/.claude.json` and `~/.claude-revenium/.claude.json` recorded in the
//! plan's Ground truth: the live server shapes, `mx` present on both sides, and
//! obviously fake credentials that still trip `redact::scan_text`.
//!
//! The assertion running underneath all of these is that a classifier bug must
//! not become a leak: every value read from a source config is masked in every
//! report, whatever the classifier decided about it.

use a2a::output::ExitCode;
use a2a::sync::manifest::{hash_path, Entry, Manifest};
use a2a::sync::mcp::{self, McpClass, SourceTransport};
use a2a::sync::{skills, Disposition, Kind, SKILL_ACCOUNT};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sync")
        .join(name)
}

fn servers(name: &str) -> BTreeMap<String, mcp::SourceServer> {
    mcp::read_source(&fixture(name))
        .expect("the fixture parses")
        .into_iter()
        .map(|s| (s.name.clone(), s))
        .collect()
}

/// Classification with the knobs a run would hand it: nothing owned, only `mx`
/// already present in the destination, a git-tracked target, no allowlist.
fn classify_all(
    servers: &BTreeMap<String, mcp::SourceServer>,
    dest_tables: &[&str],
    target_tracked: bool,
    allow: &[&str],
) -> BTreeMap<String, McpClass> {
    let allow: Vec<String> = allow.iter().map(|s| s.to_string()).collect();
    servers
        .iter()
        .map(|(name, s)| {
            (
                name.clone(),
                mcp::classify(
                    s,
                    false,
                    dest_tables.contains(&name.as_str()),
                    target_tracked,
                    &allow,
                ),
            )
        })
        .collect()
}

/// Every literal in a fixture that a report must never echo: header values and
/// env values, read straight out of the raw JSON rather than out of the
/// projection under test.
///
/// Values shorter than eight characters are excluded from the leak sweep below
/// (`OAUTHLIB_INSECURE_TRANSPORT=1` is one of them): a one-character value
/// appears by coincidence in any English sentence, so including it would turn a
/// leak test into a prose test. They still go through [`mcp::report_value`]
/// like everything else.
fn fixture_values(name: &str) -> Vec<String> {
    let raw: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(fixture(name)).unwrap()).unwrap();
    let mut out = Vec::new();
    for (_, server) in raw["mcpServers"].as_object().unwrap() {
        for group in ["headers", "env"] {
            if let Some(map) = server.get(group).and_then(|g| g.as_object()) {
                out.extend(map.values().filter_map(|v| v.as_str()).map(str::to_string));
            }
        }
    }
    out
}

/// The reader carries every live shape — and carries the one it cannot read as
/// a shape rather than dropping it. The source file is somebody else's schema
/// and it will grow.
#[test]
fn the_reader_projects_every_live_shape() {
    let servers = servers("claude.json");
    let names: Vec<&str> = servers.keys().map(String::as_str).collect();
    assert_eq!(
        names,
        [
            "Neon",
            "XcodeBuildMCP",
            "akiflow",
            "atlassian",
            "codegraph",
            "ghl",
            "google-workspace",
            "hq",
            "linear",
            "malformed",
            "mx",
            "telegram",
        ],
        "every mcpServers entry is projected, in a deterministic order"
    );

    match &servers["mx"].transport {
        SourceTransport::Stdio { command, args, env } => {
            assert_eq!(command, "mx");
            assert_eq!(args, &["mcp"]);
            assert_eq!(env["MECH_CRATE_ROOT"], "/fixture/mech-crate");
        }
        other => panic!("mx is stdio, got {other:?}"),
    }
    match &servers["hq"].transport {
        SourceTransport::Http { url, headers } => {
            assert_eq!(url, "http://127.0.0.1:7717/mcp");
            assert!(headers.is_empty());
        }
        other => panic!("hq is http, got {other:?}"),
    }
    match &servers["ghl"].transport {
        SourceTransport::Http { headers, .. } => {
            let names: Vec<&str> = headers.keys().map(String::as_str).collect();
            assert_eq!(names, ["Authorization", "locationId"]);
        }
        other => panic!("ghl is http, got {other:?}"),
    }
    assert_eq!(
        servers["malformed"].transport,
        SourceTransport::Unknown,
        "a server that is neither stdio nor http is carried, never dropped"
    );
    assert_eq!(
        servers["codegraph"].transport,
        SourceTransport::Stdio {
            command: "codegraph".to_string(),
            args: vec!["serve".to_string(), "--mcp".to_string()],
            env: BTreeMap::new(),
        },
        "a missing env is an empty env, not a failure"
    );
}

/// The two ways a source file refuses to be read are different sentences: one
/// says "point me somewhere", the other says "this file is broken".
#[test]
fn a_missing_or_unparseable_source_is_a_precondition_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join(".claude.json");
    assert_eq!(missing, mcp::source_path(tmp.path()));

    let err = mcp::read_source(&missing).unwrap_err();
    assert_eq!(err.code, "SYNC_CLAUDE_CONFIG_MISSING");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(
        err.message.contains(&*missing.to_string_lossy()),
        "the path is named: {}",
        err.message
    );

    std::fs::write(&missing, "{ \"mcpServers\": ").unwrap();
    let err = mcp::read_source(&missing).unwrap_err();
    assert_eq!(err.code, "SYNC_CLAUDE_CONFIG_UNREADABLE");
    assert_eq!(err.exit, ExitCode::Precondition);
    assert!(err.message.contains(&*missing.to_string_lossy()));

    // A Claude config with no MCP servers at all is a normal config.
    std::fs::write(&missing, "{\"numStartups\": 1}").unwrap();
    assert!(mcp::read_source(&missing).unwrap().is_empty());
}

/// The whole classification table over the real shapes, at the defaults: no
/// allowlist, a git-tracked `config.toml`, `mx` already in the destination.
#[test]
fn classification_is_total_over_the_live_shapes() {
    let servers = servers("claude.json");
    let classes = classify_all(&servers, &["mx"], true, &[]);

    assert_eq!(
        classes["Neon"],
        McpClass::BearerIndirect {
            env_var: "A2A_MCP_NEON_TOKEN".to_string()
        }
    );
    assert_eq!(
        classes["linear"],
        McpClass::BearerIndirect {
            env_var: "A2A_MCP_LINEAR_TOKEN".to_string()
        }
    );
    assert_eq!(classes["hq"], McpClass::SecretFree);
    assert_eq!(classes["akiflow"], McpClass::SecretFree);
    assert_eq!(classes["codegraph"], McpClass::SecretFree);
    assert_eq!(
        classes["XcodeBuildMCP"],
        McpClass::SecretFree,
        "XCODEBUILDMCP_ENABLED_WORKFLOWS is not a credential"
    );

    assert_eq!(
        classes["ghl"],
        McpClass::Untranslatable {
            reason: mcp::HEADER_POLICY_NOTE.to_string(),
            headers: vec!["locationId".to_string()],
        },
        "the extra header is named, the bearer beside it is not the problem"
    );
    assert_eq!(
        classes["malformed"],
        McpClass::Untranslatable {
            reason: "neither a command nor a url".to_string(),
            headers: vec![],
        }
    );

    assert_eq!(
        classes["atlassian"],
        McpClass::BlockedTrackedSecrets {
            env_keys: vec![
                "CONFLUENCE_API_TOKEN".to_string(),
                "JIRA_API_TOKEN".to_string()
            ],
        },
        "secret by key name, whatever the value's shape; the URL and the \
         username beside them are not listed"
    );
    assert_eq!(
        classes["google-workspace"],
        McpClass::BlockedTrackedSecrets {
            env_keys: vec!["GOOGLE_OAUTH_CLIENT_SECRET".to_string()],
        }
    );
    assert_eq!(
        classes["telegram"],
        McpClass::BlockedTrackedSecrets {
            env_keys: vec!["TELEGRAM_SESSION".to_string()],
        },
        "an innocent key name whose VALUE trips scan_text is still a secret"
    );

    assert_eq!(
        classes["mx"],
        McpClass::Unowned,
        "mx exists on both sides and the ledger does not own it"
    );
}

/// Untracking the destination is the whole point of the migration: the same
/// stdio servers become writable, and nothing else moves.
#[test]
fn an_untracked_target_lets_stdio_literals_through() {
    let servers = servers("claude.json");
    let classes = classify_all(&servers, &["mx"], false, &[]);
    for name in ["atlassian", "google-workspace", "telegram"] {
        assert_eq!(
            classes[name],
            McpClass::SecretFree,
            "{name} is writable once config.toml is not git-tracked"
        );
    }
    assert_eq!(
        classes["ghl"],
        McpClass::Untranslatable {
            reason: mcp::HEADER_POLICY_NOTE.to_string(),
            headers: vec!["locationId".to_string()],
        },
        "the header policy is not about git tracking"
    );
    assert_eq!(classes["mx"], McpClass::Unowned);
}

/// `allow_http_header_servers` holds SERVER names. An allowlist of header names
/// allowlists nothing at all — while reading as though it had.
#[test]
fn the_header_allowlist_takes_server_names_not_header_names() {
    let servers = servers("claude.json");

    let allowed = classify_all(&servers, &[], true, &["ghl"]);
    assert_eq!(
        allowed["ghl"],
        McpClass::ExtraHeaders {
            literal: vec!["locationId".to_string()],
            via_env: vec![],
        },
        "an opaque non-credential header goes in literally; the Authorization \
         beside it stays a bearer indirection"
    );

    let by_header = classify_all(&servers, &[], true, &["locationId"]);
    assert!(
        matches!(by_header["ghl"], McpClass::Untranslatable { .. }),
        "naming the HEADER allowlists nothing: {:?}",
        by_header["ghl"]
    );
}

/// Order is a pin: a destination table a2a does not own is skipped before
/// anything is said about the source's shape or its secrets.
#[test]
fn unowned_outranks_every_other_verdict() {
    let servers = servers("claude.json");
    let allow: Vec<String> = vec![];
    for name in ["atlassian", "ghl", "malformed", "Neon", "mx"] {
        assert_eq!(
            mcp::classify(&servers[name], false, true, true, &allow),
            McpClass::Unowned,
            "{name} is somebody else's table first and anything else second"
        );
    }
    // Owned means a2a wrote it, so the ordinary verdict applies again.
    assert_eq!(
        mcp::classify(&servers["Neon"], true, true, true, &allow),
        McpClass::BearerIndirect {
            env_var: "A2A_MCP_NEON_TOKEN".to_string()
        }
    );
}

/// The bearer report has to answer the question the operator is about to ask,
/// including the half of it nobody could verify without a live server.
#[test]
fn the_bearer_detail_names_the_variable_and_the_export_caveat() {
    let servers = servers("claude.json");
    let class = mcp::classify(&servers["Neon"], false, false, true, &[]);
    let detail = class.detail();
    assert!(detail.contains("A2A_MCP_NEON_TOKEN"), "{detail}");
    assert!(
        detail.contains(
            "export the token WITHOUT the \"Bearer \" prefix (codex supplies the scheme); \
             if the server rejects it, export the full header value instead"
        ),
        "the caveat is verbatim: {detail}"
    );
    assert_eq!(class.disposition(), Disposition::Synced);
}

/// A blocked stdio server names the offending variables and the one command
/// that unblocks them.
#[test]
fn a_blocked_stdio_detail_names_the_env_keys_and_the_migration() {
    let servers = servers("claude.json");
    let class = mcp::classify(&servers["atlassian"], false, false, true, &[]);
    let detail = class.detail();
    assert!(detail.contains("CONFLUENCE_API_TOKEN"), "{detail}");
    assert!(detail.contains("JIRA_API_TOKEN"), "{detail}");
    assert!(!detail.contains("CONFLUENCE_USERNAME"), "{detail}");
    assert!(detail.contains("a2a sync mcp --migrate-untrack"), "{detail}");
    assert_eq!(class.disposition(), Disposition::SkippedSecret);
}

/// The skipped-header warning names header names, says the skip is a policy
/// choice rather than a capability limit, and hands over TOML whose values are
/// masked.
#[test]
fn the_untranslatable_remedy_is_pasteable_toml_with_masked_values() {
    let servers = servers("claude.json");
    let class = mcp::classify(&servers["ghl"], false, false, true, &[]);
    let detail = class.detail();
    assert!(detail.contains("locationId"), "{detail}");
    assert!(
        detail.contains("policy choice") && detail.contains("allow_http_header_servers"),
        "the detail says why, and what to do about it: {detail}"
    );

    let remedy = mcp::untranslatable_remedy(&servers["ghl"]).expect("an http server has a remedy");
    assert!(remedy.contains("[mcp_servers.ghl]"), "{remedy}");
    assert!(
        remedy.contains("url = \"https://services.leadconnectorhq.com/mcp/\""),
        "a url is not a credential and a remedy without it is useless: {remedy}"
    );
    assert!(remedy.contains("bearer_token_env_var = \"A2A_MCP_GHL_TOKEN\""), "{remedy}");
    assert!(
        remedy.contains("locationId = \"[REDACTED:20]\""),
        "the value is masked, the length survives: {remedy}"
    );
    assert!(
        mcp::untranslatable_remedy(&servers["mx"]).is_none(),
        "a stdio server has no header remedy"
    );
}

/// Env var naming, both directions, against the names Ground truth recorded.
#[test]
fn env_var_names_uppercase_and_collapse_punctuation() {
    assert_eq!(mcp::bearer_env_var("Neon"), "A2A_MCP_NEON_TOKEN");
    assert_eq!(
        mcp::bearer_env_var("google-workspace"),
        "A2A_MCP_GOOGLE_WORKSPACE_TOKEN"
    );
    assert_eq!(
        mcp::header_env_var("ghl", "locationId"),
        "A2A_MCP_GHL_LOCATIONID"
    );
    assert_eq!(
        mcp::header_env_var("ghl", "X-Api-Key"),
        "A2A_MCP_GHL_X_API_KEY"
    );
}

/// A misclassification must not become a leak: reporting masks unconditionally,
/// so the classifier's verdict never decides whether a value is printable.
#[test]
fn every_reported_value_is_masked_whatever_it_is() {
    assert_eq!(
        mcp::report_value("https://example.com/mcp"),
        "[REDACTED:23]",
        "an obviously harmless value is masked too"
    );
    assert_eq!(mcp::report_value(""), "[REDACTED:0]");
    for value in fixture_values("claude.json") {
        let reported = mcp::report_value(&value);
        assert_eq!(reported, format!("[REDACTED:{}]", value.len()));
        if value.len() >= 8 {
            assert!(!reported.contains(&value), "leaked: {reported}");
        }
    }
}

/// The end-to-end version of the same rule: no fixture credential appears in
/// any sentence this module would hand an operator (and therefore an agent).
#[test]
fn no_fixture_value_survives_into_any_report_line() {
    let values = fixture_values("claude.json");
    let servers = servers("claude.json");
    let mut printed = String::new();
    for tracked in [true, false] {
        for allow in [vec![], vec!["ghl".to_string()]] {
            for server in servers.values() {
                let class = mcp::classify(server, false, false, tracked, &allow);
                printed.push_str(&class.detail());
                printed.push('\n');
                if let Some(remedy) = mcp::untranslatable_remedy(server) {
                    printed.push_str(&remedy);
                    printed.push('\n');
                }
            }
        }
    }
    for value in values.iter().filter(|v| v.len() >= 8) {
        assert!(!printed.contains(value.as_str()), "leaked {value} in report");
    }
    assert!(
        printed.contains("[REDACTED:"),
        "the reports really did carry masked values"
    );
}

/// Two accounts, one destination: the same server name from two sources
/// occupies two independent ledger entries, so neither account can silently
/// adopt the other's table.
#[test]
fn manifest_keys_namespace_mcp_entries_per_account() {
    let default = servers("claude.json");
    let revenium = servers("claude-revenium.json");

    let default_names: BTreeSet<String> = default.keys().cloned().collect();
    let revenium_names: BTreeSet<String> = revenium.keys().cloned().collect();
    let shared: Vec<&String> = default_names.intersection(&revenium_names).collect();
    assert!(
        shared.iter().any(|n| n.as_str() == "Neon") && shared.iter().any(|n| n.as_str() == "mx"),
        "the fixtures must actually overlap or disjointness is trivial: {shared:?}"
    );

    let keys = |servers: &BTreeMap<String, mcp::SourceServer>, account: &str| -> BTreeSet<String> {
        servers
            .keys()
            .map(|name| Manifest::key(Kind::Mcp, account, name))
            .collect()
    };
    let a = keys(&default, "default");
    let b = keys(&revenium, "claude-revenium");
    assert!(
        a.is_disjoint(&b),
        "two accounts must not share a manifest key: {:?}",
        a.intersection(&b).collect::<Vec<_>>()
    );
    assert!(a.contains("mcp:default:Neon"));
    assert!(b.contains("mcp:claude-revenium:Neon"));

    // And the same-named servers are genuinely different entries underneath.
    assert_ne!(
        a2a::sync::manifest::canonical_server_hash(&default["Neon"]),
        a2a::sync::manifest::canonical_server_hash(&revenium["Neon"]),
        "two accounts' Neon tokens are two source hashes"
    );
    assert_eq!(
        a2a::sync::manifest::canonical_server_hash(&default["mx"]),
        a2a::sync::manifest::canonical_server_hash(&revenium["mx"]),
        "an identical server projects to an identical hash"
    );
}

/// A `Disposition` variant with no producer is a report line nobody can ever
/// see. The exhaustive match below makes a new variant a compile error here,
/// and the assertion makes an unproduced one a test failure.
#[test]
fn every_disposition_variant_has_a_producer() {
    let mut produced: BTreeSet<&'static str> = BTreeSet::new();

    // The mcp classifier's four.
    let servers = servers("claude.json");
    for (name, dest_table, tracked) in [
        ("mx", true, true),
        ("malformed", false, true),
        ("atlassian", false, true),
        ("Neon", false, true),
    ] {
        let class = mcp::classify(&servers[name], false, dest_table, tracked, &[]);
        produced.insert(class.disposition().as_str());
    }

    // The skills planner's seven, over a tree built to reach every branch.
    let tmp = tempfile::tempdir().unwrap();
    let (src, dest) = (tmp.path().join("src"), tmp.path().join("dest"));
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&dest).unwrap();
    std::fs::write(src.join("README.md"), "not a skill\n").unwrap();
    for name in ["new-skill", "docusign", "curated", "owned-drifted", "owned-same"] {
        let dir = src.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: reads a thing\n---\n\nbody\n"),
        )
        .unwrap();
    }
    for name in ["curated", "owned-drifted", "owned-same"] {
        let dir = dest.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::copy(src.join(name).join("SKILL.md"), dir.join("SKILL.md")).unwrap();
    }
    std::fs::write(dest.join("owned-drifted/SKILL.md"), "edited by a human\n").unwrap();

    let entry = |name: &str, source_hash: String, dest_hash: String| Entry {
        source_path: src.join(name),
        dest_path: dest.join(name),
        source_hash,
        dest_hash,
        synced_at: chrono::Utc::now(),
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        kind: Kind::Skill,
        account: SKILL_ACCOUNT.to_string(),
    };
    let mut manifest = Manifest::default();
    for (name, e) in [
        (
            "owned-drifted",
            entry(
                "owned-drifted",
                hash_path(&src.join("owned-drifted")).unwrap(),
                "a-hash-from-before-the-human-edited-it".to_string(),
            ),
        ),
        (
            "owned-same",
            entry(
                "owned-same",
                hash_path(&src.join("owned-same")).unwrap(),
                hash_path(&dest.join("owned-same")).unwrap(),
            ),
        ),
        (
            "gone",
            entry("gone", "source-hash".to_string(), "dest-hash".to_string()),
        ),
    ] {
        manifest
            .entries
            .insert(Manifest::key(Kind::Skill, SKILL_ACCOUNT, name), e);
    }

    let plan = skills::plan(
        &src,
        &dest,
        &manifest,
        &a2a::config::SyncConfig::default(),
        SKILL_ACCOUNT,
    )
    .unwrap();
    for e in &plan.entries {
        produced.insert(e.disposition.as_str());
    }

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
    for d in all {
        // A new variant makes this match non-exhaustive — a compile error —
        // so the list above cannot quietly fall behind the enum.
        match d {
            Disposition::Synced
            | Disposition::Unchanged
            | Disposition::Removed
            | Disposition::SkippedSideEffecting
            | Disposition::SkippedCollision
            | Disposition::SkippedNotASkill
            | Disposition::SkippedUnowned
            | Disposition::SkippedSecret
            | Disposition::SkippedUntranslatable
            | Disposition::RefusedDestDrift => {}
        }
        assert!(
            produced.contains(d.as_str()),
            "no branch of either classifier produces {d}: {produced:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Task 5: `a2a sync mcp` end to end — the `toml_edit` writer, the codex-backed
// post-write validation, and the no-clobber rules.
//
// Every invocation below is built by `common::sync_a2a` (Global Constraints):
// it is the one entry point that sets `CODEX_HOME`, points the Claude source at
// a fixture directory and leaves `A2A_SYNC_ALLOW_REAL_HOME` unset, so a
// forgotten variable is a red test rather than a write into the developer's
// real `~/.codex`.
// ---------------------------------------------------------------------------

mod common;

use serde_json::{json, Value};
use std::process::Output;
use std::time::{Duration, Instant};

/// The codex home's hand-written config: a leading comment, an inline comment
/// on a key in an untouched table, blank-line grouping, and the live collision
/// — `[mcp_servers.mx]` exists on both sides and the ledger owns neither.
const CODEX_CONFIG: &str = r#"# The codex home's own config.toml, hand written.
model = "gpt-5-codex"
model_reasoning_effort = "high"  # an inline comment on an untouched key

[tui]
# a comment inside a table nobody is syncing
notifications = true

[mcp_servers.mx]
# mx exists on BOTH sides. a2a did not write this table and must never touch it.
command = "mx"
args = ["mcp"]
"#;

/// The account label a fixture directory named `claude` resolves to.
const ACCOUNT: &str = "claude";

/// A hermetic world: a temp `$A2A_HOME`, a temp `CODEX_HOME` that is a git
/// repository (so `config.toml` can be tracked, which is what the whole secret
/// policy turns on) and a WRITABLE copy of the fixture Claude config — several
/// of these tests delete a server from it and re-run.
struct World {
    tmp: tempfile::TempDir,
    home: PathBuf,
    codex_home: PathBuf,
    claude_dir: PathBuf,
}

impl World {
    /// `config.toml` git-TRACKED — the default, and the state the
    /// secret-never-written AC is scoped to.
    fn new() -> World {
        World::build(true)
    }
    /// The same world with `config.toml` untracked: a private file, where the
    /// stdio env literals are written.
    fn untracked() -> World {
        World::build(false)
    }

    fn build(track: bool) -> World {
        let tmp = tempfile::tempdir().unwrap();
        let w = World {
            home: tmp.path().join("a2a"),
            codex_home: tmp.path().join("codex"),
            claude_dir: tmp.path().join("claude"),
            tmp,
        };
        std::fs::create_dir_all(&w.codex_home).unwrap();
        std::fs::create_dir_all(&w.claude_dir).unwrap();
        std::fs::write(w.config(), CODEX_CONFIG).unwrap();
        std::fs::copy(fixture("claude.json"), w.source_json()).unwrap();
        w.git(&["init", "-q", "-b", "main"]);
        if track {
            w.git(&["add", "config.toml"]);
            w.git(&["commit", "-qm", "codex config"]);
        }
        w
    }

    fn config(&self) -> PathBuf {
        self.codex_home.join("config.toml")
    }
    fn config_text(&self) -> String {
        std::fs::read_to_string(self.config()).unwrap()
    }
    fn source_json(&self) -> PathBuf {
        self.claude_dir.join(".claude.json")
    }
    fn manifest_path(&self) -> PathBuf {
        self.home.join("sync-manifest.json")
    }
    fn manifest(&self) -> Value {
        serde_json::from_str(&std::fs::read_to_string(self.manifest_path()).unwrap()).unwrap()
    }
    fn manifest_keys(&self) -> Vec<String> {
        self.manifest()["entries"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    /// Rewrite the Claude source through a closure — how a test deletes a
    /// server or adds one with an awkward header set.
    fn edit_source(&self, f: impl FnOnce(&mut Value)) {
        let mut v: Value =
            serde_json::from_str(&std::fs::read_to_string(self.source_json()).unwrap()).unwrap();
        f(&mut v);
        std::fs::write(self.source_json(), serde_json::to_string_pretty(&v).unwrap()).unwrap();
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
        let out = self.run(&["--json", "sync", "mcp"]);
        assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
        data(&out)
    }

    /// Hermetic git, per Global Constraints: the developer's real gitconfig
    /// (hooks, signing, templates) can never reach a test.
    fn git(&self, args: &[&str]) -> String {
        let nowhere = self.tmp.path().join("no-such-gitconfig");
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(&self.codex_home)
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
fn warnings(data: &Value) -> Vec<String> {
    data["warnings"]
        .as_array()
        .expect("warnings is an array")
        .iter()
        .map(|w| w.as_str().unwrap().to_string())
        .collect()
}
/// `name -> (transport.type, auth_status)` as codex itself reported them.
fn validated(data: &Value) -> BTreeMap<String, (String, String)> {
    data["validated"]
        .as_array()
        .expect("validated is an array")
        .iter()
        .map(|v| {
            (
                v["name"].as_str().unwrap().to_string(),
                (
                    v["transport_type"].as_str().unwrap().to_string(),
                    v["auth_status"].as_str().unwrap().to_string(),
                ),
            )
        })
        .collect()
}

/// The document split into `header -> body`, with the blank lines that PRECEDE
/// a header attributed to the header that follows them.
///
/// That is exactly how `toml_edit` models a table's leading decor, and it is
/// what lets "byte-identical outside the touched tables" be asserted without
/// counting the separator blank line of an appended table against its
/// (untouched) neighbour.
fn spans(text: &str) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    let mut header = String::new(); // the preamble's key
    let mut body: Vec<String> = Vec::new();
    for line in text.lines() {
        if line.trim_start().starts_with('[') {
            let mut lead: Vec<String> = Vec::new();
            while body.last().map(|l| l.trim().is_empty()).unwrap_or(false) {
                lead.push(body.pop().unwrap());
            }
            out.insert(std::mem::take(&mut header), body.join("\n"));
            body = lead;
            header = line.trim().to_string();
            continue;
        }
        body.push(line.to_string());
    }
    out.insert(header, body.join("\n"));
    out
}

/// Whitespace-insensitive containment: the pins below are about the SHAPE
/// `toml_edit` renders (which keys, which values, which are omitted), not about
/// where it puts its spaces.
fn has(haystack: &str, needle: &str) -> bool {
    haystack.replace(' ', "").contains(&needle.replace(' ', ""))
}

/// Every value in a fixture that the secret policy is about, enumerated from
/// the raw JSON with the classifier's own rule — so a newly added fixture
/// secret is automatically covered by all three sweeps below.
fn fixture_secrets(name: &str) -> Vec<String> {
    let raw: Value = serde_json::from_str(&std::fs::read_to_string(fixture(name)).unwrap()).unwrap();
    let mut out = Vec::new();
    for (_, server) in raw["mcpServers"].as_object().unwrap() {
        for group in ["headers", "env"] {
            let Some(map) = server.get(group).and_then(|g| g.as_object()) else {
                continue;
            };
            for (key, value) in map {
                let Some(value) = value.as_str() else { continue };
                let secret = key.eq_ignore_ascii_case("authorization")
                    || a2a::redact::is_secret_key(&key.replace('-', "_"))
                    || !a2a::redact::scan_text(value).is_empty();
                if secret {
                    out.push(value.to_string());
                }
            }
        }
    }
    out.sort();
    out
}

/// What the fixture classifies to at the defaults, against a tracked
/// `config.toml` with a foreign `[mcp_servers.mx]` already in it.
fn first_run_dispositions() -> BTreeMap<String, String> {
    BTreeMap::from(
        [
            ("Neon", "synced"),
            ("XcodeBuildMCP", "synced"),
            ("akiflow", "synced"),
            ("atlassian", "skipped-secret"),
            ("codegraph", "synced"),
            ("ghl", "skipped-untranslatable"),
            ("google-workspace", "skipped-secret"),
            ("hq", "synced"),
            ("linear", "synced"),
            ("malformed", "skipped-untranslatable"),
            ("mx", "skipped-unowned"),
            ("telegram", "skipped-secret"),
        ]
        .map(|(a, b)| (a.to_string(), b.to_string())),
    )
}

/// The whole writer surface in one apply: what each shape renders as, what is
/// emphatically not written, and the comment preservation that makes it safe to
/// point at a hand-written file.
#[test]
fn the_first_apply_renders_every_writable_shape_and_leaves_the_rest_byte_identical() {
    let w = World::new();
    let before = w.config_text();
    let out = w.run(&["--json", "sync", "mcp"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let env = envelope(&out);
    assert_eq!(env["command"], "sync mcp", "the pinned command name");
    let data = &env["data"];
    assert_eq!(data["kind"], "mcp");
    assert_eq!(data["account"], ACCOUNT);
    assert_eq!(data["dry_run"], false);
    assert_eq!(data["dest"], w.config().to_string_lossy().into_owned());
    assert_eq!(
        data["counts"],
        json!({"synced": 6, "unchanged": 0, "removed": 0, "skipped": 6, "refused": 0})
    );
    assert_eq!(reported(data), first_run_dispositions());

    // ------------------------------------------------- the four rendered shapes
    let after = w.config_text();
    let tables = spans(&after);
    let table = |name: &str| -> String {
        tables
            .get(&format!("[mcp_servers.{name}]"))
            .unwrap_or_else(|| panic!("no [mcp_servers.{name}] in:\n{after}"))
            .clone()
    };

    // stdio: command, args, env — each omitted when empty.
    let codegraph = table("codegraph");
    assert!(has(&codegraph, r#"command = "codegraph""#), "{codegraph}");
    assert!(has(&codegraph, r#"args = ["serve", "--mcp"]"#), "{codegraph}");
    assert!(
        !codegraph.contains("env"),
        "an empty env is omitted, not written as an empty table: {codegraph}"
    );
    let xcode = table("XcodeBuildMCP");
    assert!(
        has(
            &xcode,
            r#"env = { XCODEBUILDMCP_ENABLED_WORKFLOWS = "simulator-workspace" }"#
        ),
        "a non-empty env is an inline table: {xcode}"
    );
    let telegram = tables.get("[mcp_servers.telegram]");
    assert!(
        telegram.is_none(),
        "a blocked stdio server is not written at all: {telegram:?}"
    );

    // HTTP secret-free: url and nothing else.
    let hq = table("hq");
    assert!(has(&hq, r#"url = "http://127.0.0.1:7717/mcp""#), "{hq}");
    assert!(!hq.contains("bearer_token_env_var"), "{hq}");
    assert!(!hq.contains("http_headers"), "{hq}");

    // HTTP bearer: url + the env indirection, and no header anywhere.
    let neon = table("Neon");
    assert!(has(&neon, r#"url = "https://mcp.neon.tech/mcp""#), "{neon}");
    assert!(
        has(&neon, r#"bearer_token_env_var = "A2A_MCP_NEON_TOKEN""#),
        "{neon}"
    );
    assert!(
        !neon.to_lowercase().contains("authorization"),
        "the header itself never lands: {neon}"
    );

    // ------------------------------------------------ comment preservation
    let before_spans = spans(&before);
    for (header, body) in &before_spans {
        assert_eq!(
            tables.get(header),
            Some(body),
            "{header} must be byte-identical after a run that never touched it"
        );
    }
    assert!(
        after.contains("# mx exists on BOTH sides"),
        "a foreign table keeps its comment: {after}"
    );
    assert!(
        after.contains("# an inline comment on an untouched key"),
        "and so does an inline one: {after}"
    );

    // ------------------------------------------------------------ the ledger
    assert_eq!(
        w.manifest_keys(),
        [
            "mcp:claude:Neon",
            "mcp:claude:XcodeBuildMCP",
            "mcp:claude:akiflow",
            "mcp:claude:codegraph",
            "mcp:claude:hq",
            "mcp:claude:linear",
        ],
        "exactly what was written is what is owned"
    );
    assert_eq!(w.manifest()["entries"]["mcp:claude:Neon"]["kind"], "mcp");
    assert_eq!(
        w.manifest()["entries"]["mcp:claude:Neon"]["source_path"],
        w.source_json().to_string_lossy().into_owned()
    );
}

/// Spec §5/§8.8, tracked half: whatever the classifier decided, no credential
/// from the source reaches the config, the ledger, stdout or stderr.
#[test]
fn no_secret_value_reaches_the_config_the_ledger_or_the_report() {
    let w = World::new();
    let out = w.run(&["--json", "sync", "mcp"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let secrets = fixture_secrets("claude.json");
    assert!(
        secrets.len() >= 6,
        "the sweep must actually have secrets to sweep: {secrets:?}"
    );
    let haystacks = [
        ("config.toml", w.config_text()),
        (
            "sync-manifest.json",
            std::fs::read_to_string(w.manifest_path()).unwrap(),
        ),
        ("stdout", stdout(&out)),
        ("stderr", stderr(&out)),
    ];
    for secret in &secrets {
        for (what, text) in &haystacks {
            assert!(!text.contains(secret.as_str()), "{what} leaked {secret}");
        }
    }
    // And the blocked servers really were the ones carrying them.
    let reported = reported(&data(&out));
    for name in ["atlassian", "google-workspace", "telegram"] {
        assert_eq!(reported[name], "skipped-secret");
    }
}

/// The untracked half of the same rule: a private destination DOES get the
/// stdio literals — that is the whole point of the migration Task 6 performs —
/// and they are still absent from the report and the ledger.
#[test]
fn an_untracked_target_writes_the_stdio_literals_and_still_never_reports_them() {
    let w = World::untracked();
    let out = w.run(&["--json", "sync", "mcp"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let reported = reported(&data(&out));
    for name in ["atlassian", "google-workspace", "telegram"] {
        assert_eq!(reported[name], "synced", "{name} is writable when untracked");
    }
    let config = w.config_text();
    assert!(
        config.contains("ghp_000000000000000000000000000000000000"),
        "the literal env value is written into a file git does not publish: {config}"
    );

    for secret in fixture_secrets("claude.json") {
        for (what, text) in [
            ("stdout", stdout(&out)),
            ("stderr", stderr(&out)),
            (
                "sync-manifest.json",
                std::fs::read_to_string(w.manifest_path()).unwrap(),
            ),
        ] {
            assert!(!text.contains(secret.as_str()), "{what} leaked {secret}");
        }
    }
}

/// The fourth shape: an allowlisted server's non-credential headers go in
/// literally, its credential-shaped ones through an env var NAME, and a bearer
/// beside them stays the narrower `bearer_token_env_var`.
#[test]
fn allowlisted_headers_render_http_headers_and_env_http_headers() {
    let w = World::new();
    w.edit_source(|v| {
        v["mcpServers"]["headerful"] = json!({
            "type": "http",
            "url": "https://headerful.example.com/mcp",
            "headers": {
                "Authorization": "Bearer sk-fakeheaderful0000000000000000000",
                "locationId": "FAKEloc111111111fake",
                "X-Api-Key": "FAKE-x-api-key-value",
            },
        });
    });
    let out = w.run_with(
        &[("A2A_SYNC_ALLOW_HTTP_HEADER_SERVERS", "ghl,headerful")],
        &["--json", "sync", "mcp"],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let reported = reported(&data(&out));
    assert_eq!(reported["ghl"], "synced");
    assert_eq!(reported["headerful"], "synced");

    let tables = spans(&w.config_text());
    let ghl = tables["[mcp_servers.ghl]"].clone();
    assert!(
        has(&ghl, r#"url = "https://services.leadconnectorhq.com/mcp/""#),
        "{ghl}"
    );
    assert!(
        has(&ghl, r#"bearer_token_env_var = "A2A_MCP_GHL_TOKEN""#),
        "{ghl}"
    );
    assert!(
        has(&ghl, r#"http_headers = { locationId = "FAKEloc000000000fake" }"#),
        "an opaque non-credential header is a literal: {ghl}"
    );

    let hf = tables["[mcp_servers.headerful]"].clone();
    assert!(
        has(&hf, r#"http_headers = { locationId = "FAKEloc111111111fake" }"#),
        "{hf}"
    );
    assert!(
        has(
            &hf,
            r#"env_http_headers = { X-Api-Key = "A2A_MCP_HEADERFUL_X_API_KEY" }"#
        ) || has(
            &hf,
            r#"env_http_headers = { "X-Api-Key" = "A2A_MCP_HEADERFUL_X_API_KEY" }"#
        ),
        "a credential-shaped header maps to the NAME of an env var: {hf}"
    );
    assert!(
        !hf.contains("FAKE-x-api-key-value"),
        "and never to its value: {hf}"
    );
}

/// Exit 0, a parseable array: codex's own view of what it just loaded, reported
/// back — and never obtained by running against the real codex home.
#[test]
fn a_successful_validation_reports_transport_and_auth_from_a_temp_copy() {
    let w = World::new();
    let witness = w.tmp.path().join("witness.txt");
    let out = w.run_with(
        &[("STUB_CODEX_MCP_WITNESS", witness.to_str().unwrap())],
        &["--json", "sync", "mcp"],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    // EVERY managed server is present, with the transport and auth status its
    // source shape implies — a validator that answered about some other set of
    // servers has not validated this write.
    let data = data(&out);
    let validated = validated(&data);
    for (name, disposition) in reported(&data) {
        if disposition != "synced" {
            continue;
        }
        let (transport, auth) = validated
            .get(&name)
            .unwrap_or_else(|| panic!("{name} was written but codex did not list it: {validated:?}"));
        let stdio = ["XcodeBuildMCP", "codegraph"].contains(&name.as_str());
        let bearer = ["Neon", "linear"].contains(&name.as_str());
        assert_eq!(
            transport,
            if stdio { "stdio" } else { "streamable_http" },
            "{name}"
        );
        assert_eq!(
            auth,
            if bearer { "bearer_token" } else { "unsupported" },
            "{name}"
        );
    }
    assert_eq!(
        validated["Neon"],
        ("streamable_http".to_string(), "bearer_token".to_string())
    );
    assert_eq!(
        validated["hq"],
        ("streamable_http".to_string(), "unsupported".to_string())
    );
    assert_eq!(
        validated["codegraph"],
        ("stdio".to_string(), "unsupported".to_string())
    );

    // The pinned invocation, witnessed from inside the child.
    let wit = std::fs::read_to_string(&witness).unwrap();
    assert!(wit.contains("stdin=/dev/null"), "{wit}");
    let line = wit
        .lines()
        .find(|l| l.starts_with("codex_home="))
        .expect("the child reports its CODEX_HOME");
    let validated_home = PathBuf::from(line.trim_start_matches("codex_home="));
    assert_ne!(validated_home, w.codex_home);
    assert!(
        !validated_home.starts_with(&w.codex_home),
        "validation runs against a COPY: {validated_home:?}"
    );
    assert!(
        !w.codex_home.join("tmp").exists(),
        "`codex mcp list` creates <CODEX_HOME>/tmp/ — never in the real codex home"
    );
}

/// A nonzero exit is a rejection: the real file is never touched and the run
/// quotes what codex said, `<path>:<line>:<col>` and all.
#[test]
fn a_rejected_render_never_touches_the_real_config() {
    let w = World::new();
    let before = w.config_text();
    let out = w.run_with(
        &[("STUB_CODEX_MCP_REJECT", "/candidate/config.toml:12:3")],
        &["--json", "sync", "mcp"],
    );
    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(error_code(&out), "SYNC_CODEX_REJECTED_CONFIG");
    let message = envelope(&out)["error"]["message"].to_string();
    assert!(
        message.contains("/candidate/config.toml:12:3"),
        "codex's own location is quoted: {message}"
    );
    assert_eq!(w.config_text(), before, "the real config.toml is untouched");
    assert!(
        !w.manifest_path().exists(),
        "and nothing was recorded as owned"
    );
}

/// Exit 0 with output that is not the expected array has not REJECTED
/// anything — it has failed to answer. Warn, write, never exit 9.
#[test]
fn a_validator_that_answers_nonsense_warns_and_the_write_proceeds() {
    let w = World::new();
    let out = w.run_with(&[("STUB_CODEX_MCP_GARBAGE", "1")], &["--json", "sync", "mcp"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let data = data(&out);
    assert!(data["validated"].is_null(), "{}", data);
    let warned = warnings(&data);
    assert!(
        warned
            .iter()
            .any(|w| w.contains("validation") && w.contains("stub-codex-mcp")),
        "the warning names the binary: {warned:?}"
    );
    assert!(
        stderr(&out).contains("a2a: warning:"),
        "and it is never silent: {}",
        stderr(&out)
    );
    assert!(
        w.config_text().contains("[mcp_servers.Neon]"),
        "the write proceeded"
    );
}

/// No codex on this machine at all is the same outcome, and the warning still
/// names what a2a looked for.
#[test]
fn no_resolvable_codex_warns_and_the_write_proceeds() {
    let w = World::new();
    let missing = w.tmp.path().join("no-such-codex");
    let out = w.run_with(
        &[("A2A_CODEX_BIN", missing.to_str().unwrap())],
        &["--json", "sync", "mcp"],
    );
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let data = data(&out);
    assert!(data["validated"].is_null());
    let warned = warnings(&data);
    assert!(
        warned
            .iter()
            .any(|w| w.contains("validation") && w.contains("no-such-codex")),
        "{warned:?}"
    );
    assert!(w.config_text().contains("[mcp_servers.Neon]"));
}

/// `sync` is meant to be safe to run from a hook: a codex that blocks must not
/// wedge it, and a validator that could not answer must not silently pass.
#[test]
fn a_validator_that_hangs_is_killed_at_the_budget_and_the_write_proceeds() {
    assert_eq!(
        a2a::sync::mcp::VALIDATE_BUDGET,
        Duration::from_secs(5),
        "the budget is a pin"
    );
    let w = World::new();
    let started = Instant::now();
    let out = w.run_with(&[("STUB_CODEX_MCP_SLEEP", "30")], &["--json", "sync", "mcp"]);
    let elapsed = started.elapsed();
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert!(
        elapsed < Duration::from_secs(10),
        "the child is killed at the budget, not waited on: {elapsed:?}"
    );
    let data = data(&out);
    assert!(data["validated"].is_null());
    assert!(
        warnings(&data).iter().any(|w| w.contains("validation")),
        "{data}"
    );
    assert!(w.config_text().contains("[mcp_servers.Neon]"));
}

/// The live collision, end to end: a foreign table is skipped, its bytes and
/// its comment survive, and a second identical run rewrites nothing at all.
#[test]
fn an_unowned_table_is_left_alone_and_a_second_run_rewrites_nothing() {
    let w = World::new();
    let first = w.apply();
    assert_eq!(reported(&first)["mx"], "skipped-unowned");
    let mx = spans(&w.config_text())["[mcp_servers.mx]"].clone();
    assert!(mx.contains("# mx exists on BOTH sides"), "{mx}");
    assert!(has(&mx, r#"command = "mx""#), "{mx}");

    let bytes = std::fs::read(w.config()).unwrap();
    let mtime = std::fs::metadata(w.config()).unwrap().modified().unwrap();

    let second = w.apply();
    assert_eq!(
        second["counts"],
        json!({"synced": 0, "unchanged": 6, "removed": 0, "skipped": 6, "refused": 0}),
        "everything a2a owns is unchanged the second time"
    );
    assert_eq!(spans(&w.config_text())["[mcp_servers.mx]"], mx);
    assert_eq!(std::fs::read(w.config()).unwrap(), bytes, "same bytes");
    assert_eq!(
        std::fs::metadata(w.config()).unwrap().modified().unwrap(),
        mtime,
        "and not even rewritten"
    );
}

/// Drift is about CONTENT: reformatting a managed table (or commenting it) is
/// not drift, changing a value is.
#[test]
fn drift_is_content_based_not_formatting_based() {
    let w = World::new();
    w.apply();

    // Reformat one managed table and comment another: neither is a change.
    let text = w
        .config_text()
        .replace(
            r#"url = "http://127.0.0.1:7717/mcp""#,
            "url    =    \"http://127.0.0.1:7717/mcp\"   # reformatted by a human",
        )
        .replace(
            "[mcp_servers.akiflow]",
            "# a human's note about akiflow\n[mcp_servers.akiflow]",
        );
    std::fs::write(w.config(), &text).unwrap();
    let second = w.apply();
    assert_eq!(reported(&second)["hq"], "unchanged");
    assert_eq!(reported(&second)["akiflow"], "unchanged");
    assert!(
        w.config_text().contains("# reformatted by a human"),
        "and the reformatting survives an untouched run"
    );

    // A VALUE change is drift, refused rather than clobbered.
    let text = w.config_text().replace(
        r#"bearer_token_env_var = "A2A_MCP_NEON_TOKEN""#,
        r#"bearer_token_env_var = "MY_OWN_TOKEN""#,
    );
    std::fs::write(w.config(), &text).unwrap();
    let third = w.apply();
    assert_eq!(reported(&third)["Neon"], "refused-dest-drift");
    assert_eq!(third["counts"]["refused"], 1);
    assert!(
        w.config_text().contains(r#"bearer_token_env_var = "MY_OWN_TOKEN""#),
        "the human's edit is left exactly where it is"
    );
    assert!(
        w.manifest_keys().contains(&"mcp:claude:Neon".to_string()),
        "and it is still owned — a refusal is not a disowning"
    );
}

/// A server that leaves the source takes its managed table and its ledger entry
/// with it, and nothing else moves.
#[test]
fn a_server_removed_from_the_source_loses_its_table_and_its_ledger_entry() {
    let w = World::new();
    w.apply();
    assert!(w.config_text().contains("[mcp_servers.hq]"));

    w.edit_source(|v| {
        v["mcpServers"].as_object_mut().unwrap().remove("hq");
    });
    let second = w.apply();
    assert_eq!(reported(&second)["hq"], "removed");
    assert_eq!(second["counts"]["removed"], 1);
    let after = w.config_text();
    assert!(!after.contains("[mcp_servers.hq]"), "{after}");
    assert!(after.contains("[mcp_servers.Neon]"), "{after}");
    assert!(after.contains("# mx exists on BOTH sides"), "{after}");
    assert!(!w.manifest_keys().contains(&"mcp:claude:hq".to_string()));

    // A third run has nothing left to say about it.
    let third = w.apply();
    assert!(!reported(&third).contains_key("hq"));
}

/// `--dry-run` decides everything and writes nothing — proven by the whole
/// codex-home tree, not merely by the file it would have written.
#[test]
fn dry_run_writes_nothing_anywhere_and_masks_every_source_value() {
    let w = World::new();
    let before_tree = hash_path(&w.codex_home).unwrap();
    let before = w.config_text();

    let out = w.run(&["--json", "sync", "mcp", "--dry-run"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let data = data(&out);
    assert_eq!(data["dry_run"], true);
    assert_eq!(
        data["counts"],
        json!({"synced": 6, "unchanged": 0, "removed": 0, "skipped": 6, "refused": 0}),
        "the same plan the apply would carry out"
    );
    assert_eq!(reported(&data), first_run_dispositions());
    assert_eq!(data["broker_config_stale"], false);

    assert_eq!(w.config_text(), before, "the target is untouched");
    assert_eq!(
        hash_path(&w.codex_home).unwrap(),
        before_tree,
        "and so is every other byte of the codex home — including the temp \
         validation write, which never happens in a dry run"
    );
    assert!(!w.manifest_path().exists(), "no ledger either");

    let report = stderr(&out);
    assert!(
        report.contains("[REDACTED:"),
        "the untranslatable remedy prints masked values: {report}"
    );
    for secret in fixture_secrets("claude.json") {
        assert!(!report.contains(&secret), "leaked {secret}");
        assert!(!stdout(&out).contains(&secret), "leaked {secret}");
    }
}

/// Spec §6.2: the running broker's app-server child read the config a2a just
/// replaced, and nothing in the daemon notices on its own.
#[test]
fn a_write_with_a_live_broker_warns_and_a_run_that_wrote_nothing_does_not() {
    let w = World::new();
    let guard = common::BrokerGuard::new(&w.home);
    let a2a_bin = assert_cmd::cargo::cargo_bin("a2a");
    let started = std::process::Command::new(&a2a_bin)
        .arg("--home")
        .arg(&w.home)
        .args(["broker", "run"])
        .env_remove("A2A_HOME")
        .env_remove("A2A_BROKER_DISABLE")
        .env("A2A_CODEX_BIN", common::fake_app_server_path())
        .env("A2A_ALLOWED_ROOTS", w.tmp.path())
        .env("A2A_BROKER_BIN", &a2a_bin)
        .env("A2A_SHIM_BIN", &a2a_bin)
        .env("FAKE_STATE", w.tmp.path())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("the a2a binary runs");
    assert!(started.success());

    // Converged state, bounded — never a bare sleep.
    let socket = a2a::broker::sockserv::socket_path(&w.home);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(Instant::now() < deadline, "the broker never bound its socket");
        std::thread::sleep(Duration::from_millis(50));
    }

    let out = w.run(&["--json", "sync", "mcp"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(data(&out)["broker_config_stale"], true);
    assert!(
        stderr(&out).contains(
            "a2a: warning: the running broker's app-server child read the previous MCP config — \
             run `a2a broker restart`"
        ),
        "{}",
        stderr(&out)
    );

    // The same live broker, a run that wrote nothing: there is nothing stale.
    let second = w.run(&["--json", "sync", "mcp"]);
    assert_eq!(second.status.code(), Some(0), "{}", stderr(&second));
    assert_eq!(data(&second)["counts"]["synced"], 0);
    assert_eq!(data(&second)["broker_config_stale"], false);
    assert!(
        !stderr(&second).contains("broker restart"),
        "{}",
        stderr(&second)
    );

    drop(guard);
    common::assert_no_broker_left(&w.home);
}

/// The negative case, on its own: a write with no broker at all says so.
#[test]
fn a_write_without_a_broker_reports_no_staleness() {
    let w = World::new();
    let out = w.run(&["--json", "sync", "mcp"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(data(&out)["counts"]["synced"], 6);
    assert_eq!(data(&out)["broker_config_stale"], false);
    assert!(
        !stderr(&out).contains("broker restart"),
        "{}",
        stderr(&out)
    );
    assert!(!a2a::broker::sockserv::socket_path(&w.home).exists());
}
