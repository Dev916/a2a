//! Secret redaction — one module, both directions (spec §5).
//!
//! The machine's hard rule is that secrets never pass through agent context, so
//! this module is deliberately the *only* place secret shapes are defined and
//! the same compiled rule set drives both directions:
//!
//! * **Ingress** ([`refuse_filename`], [`scan_text`]) — staging a brief or a
//!   `context/` file that looks like (or contains) a credential **fails the
//!   spawn**, naming the file and line. It never silently redacts: a brief that
//!   quietly lost a line is worse than a brief that was rejected.
//! * **Egress** ([`redact_egress`]) — everything the orchestrator can read back
//!   (`tail`/`result`/`status`) and everything appended to `events.jsonl` is
//!   masked, so a worker that runs `env` or cats a connection string cannot
//!   plant a live credential in the on-disk artifact either.
//!
//! The egress mask is `[REDACTED:<len>]`: the length is the *only* thing that
//! survives, which is enough to tell "a 22-char Slack token was here" apart from
//! "a 51-char AWS session token was here" during debugging without leaking a
//! usable byte.
//!
//! Everything here is pure — no IO, no clock, no config — so it is unit-testable
//! and safe to call from any layer.

use regex::{Captures, Regex};
use std::sync::LazyLock;

/// A secret shape found while scanning ingress content.
///
/// Deliberately carries **only** a location and a rule name — never the matched
/// value. This struct is formatted into refusal messages which flow straight
/// back into agent context and transcripts; a `value` field here would defeat
/// the entire module. A unit test pins the `Debug` output against regression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressHit {
    /// 1-based line number within the scanned text.
    pub line: usize,
    /// Name of the rule that fired (safe to print).
    pub rule: &'static str,
}

/// Filenames whose *name alone* is grounds for refusal, as (rule, matcher).
///
/// Matched against the lowercased basename, so a caller that passes a path
/// (`context/.env`) cannot slip past a bare-name check.
const FILENAME_RULES: &[(&str, FileMatch)] = &[
    (".env*", FileMatch::DotEnv),
    ("*.pem", FileMatch::Suffix(".pem")),
    ("*.key", FileMatch::Suffix(".key")),
    ("*.pfx", FileMatch::Suffix(".pfx")),
    ("*.p12", FileMatch::Suffix(".p12")),
    ("id_rsa*", FileMatch::Prefix("id_rsa")),
    ("credentials*", FileMatch::Prefix("credentials")),
    ("secrets*", FileMatch::Prefix("secrets")),
];

enum FileMatch {
    /// `.env` itself plus `.env.<anything>` (`.env.production`, `.env.local`).
    DotEnv,
    Prefix(&'static str),
    Suffix(&'static str),
}

impl FileMatch {
    fn matches(&self, basename: &str) -> bool {
        match self {
            FileMatch::DotEnv => basename == ".env" || basename.starts_with(".env."),
            FileMatch::Prefix(p) => basename.starts_with(p),
            FileMatch::Suffix(s) => basename.ends_with(s),
        }
    }
}

/// The one compiled set of secret *value* shapes, shared by scan and redact.
///
/// Sharing matters: a shape that ingress refuses but egress cannot mask (or vice
/// versa) is a hole, and two lists inevitably drift apart.
static VALUE_RULES: LazyLock<Vec<(&'static str, Regex)>> = LazyLock::new(|| {
    [
        ("openai_key", r"sk-[A-Za-z0-9_-]{20,}"),
        ("github_pat", r"ghp_[A-Za-z0-9]{36}"),
        ("slack_token", r"xox[bcdp]-[A-Za-z0-9-]{10,}"),
        ("aws_access_key_id", r"AKIA[0-9A-Z]{16}"),
        ("bearer_token", r"(?i)bearer\s+[A-Za-z0-9._~+/=-]{16,}"),
        ("private_key_block", r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    ]
    .into_iter()
    .map(|(name, pat)| {
        // Patterns are compile-time literals; a bad one is a build-time bug, not
        // a runtime condition, and failing loudly here beats a silent hole.
        (name, Regex::new(pat).expect("secret rule must compile"))
    })
    .collect()
});

/// Return the rule name if `name` looks like a secret-bearing file, else `None`.
///
/// Accepts a bare filename or a path; only the basename is considered.
pub fn refuse_filename(name: &str) -> Option<&'static str> {
    let basename = name.rsplit('/').next().unwrap_or(name).to_ascii_lowercase();
    FILENAME_RULES
        .iter()
        .find(|(_, m)| m.matches(&basename))
        .map(|(rule, _)| *rule)
}

/// Scan `text` for secret shapes, reporting one hit per match.
///
/// Scanning is line-oriented because the caller must be able to say *where* —
/// "refused: brief.md line 12 (openai_key)" is actionable, "refused: brief.md"
/// is not. Returns an empty vec when clean.
pub fn scan_text(text: &str) -> Vec<IngressHit> {
    let mut hits = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        for (rule, re) in VALUE_RULES.iter() {
            for _ in re.find_iter(line) {
                hits.push(IngressHit {
                    line: idx + 1,
                    rule,
                });
            }
        }
    }
    hits
}

/// Mask every secret shape in `text` as `[REDACTED:<len>]`.
///
/// Unlike [`scan_text`] this runs over the whole text rather than line by line,
/// so a shape that straddles a newline (`Bearer\n<token>`) is still masked.
pub fn redact_egress(text: &str) -> String {
    let mut out = text.to_string();
    for (_, re) in VALUE_RULES.iter() {
        let replaced = re
            .replace_all(&out, |c: &Captures| format!("[REDACTED:{}]", c[0].len()))
            .into_owned();
        out = replaced;
    }
    out
}

/// Whether a KEY NAME alone is grounds for treating its value as a credential.
///
/// The value shapes above catch a token that announces itself (`sk-…`,
/// `ghp_…`). This catches the far more common case where the value announces
/// nothing and only the name knows: Ground truth's `CONFLUENCE_API_TOKEN` holds
/// 192 opaque characters matching no shape in [`VALUE_RULES`]. `sync mcp` takes
/// a value to be secret when EITHER fires, which is deny-by-default in the
/// direction that matters — a false positive costs one env-var indirection, a
/// false negative writes a live credential into a git-published file.
///
/// Deliberately narrow on the whole-name list. `KEY` on its own is an ordinary
/// variable name (a map key, a sort key, a cache key), and a rule that fired on
/// it would be one operators learn to override — which is how a rule stops
/// protecting anything. Suffix rules carry their separator (`_KEY`, not `KEY`)
/// for exactly the same reason: `MONKEY` is not a credential.
pub fn is_secret_key(name: &str) -> bool {
    /// Matched against the whole name.
    const EXACT: [&str; 5] = ["PASSWORD", "TOKEN", "SECRET", "APIKEY", "API_KEY"];
    /// Matched against the end of the name, separator included.
    const SUFFIXES: [&str; 4] = ["_TOKEN", "_KEY", "_SECRET", "_PASSWORD"];
    let upper = name.to_ascii_uppercase();
    EXACT.contains(&upper.as_str()) || SUFFIXES.iter().any(|s| upper.ends_with(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filenames_refused() {
        assert!(refuse_filename(".env.production").is_some());
        assert!(refuse_filename("server.pem").is_some());
        assert!(refuse_filename("id_rsa_1").is_some());
        assert!(refuse_filename("main.rs").is_none());
    }

    #[test]
    fn scan_reports_line_and_rule_never_value() {
        let hits = scan_text("line one\napi key: sk-abc123456789012345678901\n");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].line, 2);
        let debug = format!("{hits:?}");
        assert!(!debug.contains("sk-abc"), "hit must not carry the secret");
    }

    #[test]
    fn egress_masks_with_exact_length() {
        // "xoxb-1234567890-abcdef" is 22 chars; the mask leaks only the length
        assert_eq!(
            redact_egress("token is xoxb-1234567890-abcdef"),
            "token is [REDACTED:22]"
        );
    }

    #[test]
    fn planted_tokens_survive_no_egress() {
        for planted in [
            "sk-abc123456789012345678901",
            "xoxb-1234567890-abcdef",
            "ghp_012345678901234567890123456789012345",
            "AKIAABCDEFGHIJKLMNOP",
            "Bearer eyJhbGciOiJIUzI1NiJ9.x.y",
        ] {
            let out = redact_egress(&format!("prefix {planted} suffix"));
            assert!(!out.contains(planted), "leaked: {planted}");
            assert!(out.contains("[REDACTED:"), "not masked: {out}");
        }
    }

    #[test]
    fn every_listed_filename_rule_fires_and_ordinary_names_pass() {
        for (name, expected) in [
            (".env", ".env*"),
            (".env.local", ".env*"),
            ("context/.env.production", ".env*"),
            ("server.pem", "*.pem"),
            ("tls.key", "*.key"),
            ("cert.pfx", "*.pfx"),
            ("cert.p12", "*.p12"),
            ("id_rsa", "id_rsa*"),
            ("id_rsa.pub", "id_rsa*"),
            ("CREDENTIALS.json", "credentials*"),
            ("secrets.yaml", "secrets*"),
        ] {
            assert_eq!(refuse_filename(name), Some(expected), "for {name}");
        }
        for ok in ["main.rs", "README.md", "keyboard.ts", "environment.md"] {
            assert_eq!(refuse_filename(ok), None, "for {ok}");
        }
    }

    #[test]
    fn scan_and_redact_share_one_rule_set() {
        // Anything ingress refuses, egress must be able to mask — a shape caught
        // in only one direction is a hole.
        let planted = [
            "sk-abc123456789012345678901",
            "ghp_012345678901234567890123456789012345",
            "xoxd-1234567890-abcdef",
            "AKIAABCDEFGHIJKLMNOP",
            "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.x.y",
            "-----BEGIN OPENSSH PRIVATE KEY-----",
        ];
        for text in planted {
            assert!(!scan_text(text).is_empty(), "ingress missed: {text}");
            assert!(
                redact_egress(text).contains("[REDACTED:"),
                "egress missed: {text}"
            );
        }
    }

    #[test]
    fn clean_text_is_untouched() {
        let clean = "fn main() {\n    println!(\"hello\");\n}\n";
        assert!(scan_text(clean).is_empty());
        assert_eq!(redact_egress(clean), clean);
    }

    #[test]
    fn multiple_hits_report_their_own_lines() {
        let hits = scan_text("clean\nAKIAABCDEFGHIJKLMNOP\nstill clean\nxoxb-1234567890-abcdef\n");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].line, 2);
        assert_eq!(hits[0].rule, "aws_access_key_id");
        assert_eq!(hits[1].line, 4);
        assert_eq!(hits[1].rule, "slack_token");
    }

    #[test]
    fn mask_length_matches_the_masked_span_and_redaction_is_idempotent() {
        let once = redact_egress("k=AKIAABCDEFGHIJKLMNOP");
        assert_eq!(once, "k=[REDACTED:20]");
        // the placeholder itself must not look like a secret to a second pass
        assert_eq!(redact_egress(&once), once);
    }

    /// Both directions, against the real key names Ground truth found in
    /// `~/.claude.json` — the negatives matter as much as the positives, since
    /// a rule that flags `CONFLUENCE_URL` is a rule that gets turned off.
    #[test]
    fn secret_key_names_are_recognized_and_ordinary_ones_are_not() {
        for secret in [
            "CONFLUENCE_API_TOKEN",
            "JIRA_API_TOKEN",
            "GOOGLE_OAUTH_CLIENT_SECRET",
            "GHL_PRIVATE_KEY",
            "DB_PASSWORD",
            "PASSWORD",
            "TOKEN",
            "SECRET",
            "APIKEY",
            "API_KEY",
            // Case is not part of the rule.
            "token",
            "Confluence_Api_Token",
            "db_password",
        ] {
            assert!(is_secret_key(secret), "missed: {secret}");
        }
        for ordinary in [
            "CONFLUENCE_URL",
            "CONFLUENCE_USERNAME",
            "OAUTHLIB_INSECURE_TRANSPORT",
            "MECH_CRATE_ROOT",
            "XCODEBUILDMCP_ENABLED_WORKFLOWS",
            "PATH",
            "KEY_PATH",
            "TOKENIZER",
            "MONKEY",
            "SECRETARY",
            "locationId",
        ] {
            assert!(!is_secret_key(ordinary), "false positive: {ordinary}");
        }
    }
}
