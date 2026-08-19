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
