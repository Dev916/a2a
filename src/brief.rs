//! Brief front matter — parse, per-lane validation, content hash (spec §4.9).
//!
//! A brief is a Markdown file with optional leading YAML front matter:
//!
//! ```text
//! ---
//! lane: impl
//! acceptance_criteria:
//!   - binary exits 0
//! verify:
//!   - cargo test
//! ---
//! # Task
//! Do the thing.
//! ```
//!
//! Three deliberate choices, because `spawn` **rejects** rather than warns on a
//! brief that fails its lane's table (spec §4.9) and a rejection has to be
//! defensible:
//!
//! * **Front matter is optional.** A brief with no `---` fence parses cleanly
//!   into all-empty fields; it is [`validate`], not [`parse`], that decides
//!   whether that is acceptable for the lane. An *opened but unterminated*
//!   fence is an error, though — that is a typo, not a front-matter-free brief,
//!   and silently swallowing the YAML into the body would produce a worker
//!   whose brief means something other than what its author wrote.
//! * **Unknown front-matter keys are tolerated** (mirroring the config layer's
//!   "unknown keys warn, malformed values fail"), so a brief written against a
//!   later schema still spawns instead of hard-failing on a key this build has
//!   never heard of. Malformed *values* still fail loudly.
//! * **Blank list entries are dropped at parse time**, so `acceptance_criteria:
//!   ["  "]` cannot satisfy the impl gate. The requirement is a criterion, not a
//!   line.
//!
//! The body is preserved byte-exactly after the closing fence — no trimming, no
//! normalization — because `brief_sha` and the prompt handed to codex must be
//! the text the human actually wrote.
//!
//! Everything here is pure: no IO, no clock, no config lookups.

use crate::config::Lane;
use regex::Regex;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::sync::LazyLock;

/// A parsed brief: front-matter fields plus the Markdown body after the fence.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Brief {
    pub lane: Option<Lane>,
    pub acceptance_criteria: Vec<String>,
    pub verify: Vec<String>,
    pub files_in_scope: Vec<String>,
    pub non_goals: Vec<String>,
    pub result_schema: Option<String>,
    pub max_turns: Option<u32>,
    pub body: String,
}

/// The Phase 1 per-lane requirement table, stated once and literally.
///
/// A recorded divergence from spec §4.9's full table: `files_in_scope` and
/// `non_goals` are *recommended, not required* in Phase 1 (revisited in
/// Phase 4), so a brief is never rejected for omitting them. What remains is
/// the part that makes a worker verifiable at all — what "done" means, and how
/// to check it.
pub const fn required_keys(lane: Lane) -> &'static [&'static str] {
    match lane {
        Lane::Impl => &["acceptance_criteria", "verify"],
        Lane::Review => &["acceptance_criteria"],
        Lane::Research => &[],
        Lane::General => &[],
    }
}

/// Front matter as written: every key optional, so a partial brief still parses
/// and the *validation table* — not serde — decides what is missing.
#[derive(Debug, Default, Deserialize)]
struct FrontMatter {
    #[serde(default)]
    lane: Option<String>,
    #[serde(default)]
    acceptance_criteria: Vec<String>,
    #[serde(default)]
    verify: Vec<String>,
    #[serde(default)]
    files_in_scope: Vec<String>,
    #[serde(default)]
    non_goals: Vec<String>,
    #[serde(default)]
    result_schema: Option<String>,
    #[serde(default)]
    max_turns: Option<u32>,
}

/// Parse a brief's optional YAML front matter and body.
///
/// Errors name the problem in terms the brief's author can act on; they surface
/// verbatim at the spawn gate.
pub fn parse(text: &str) -> Result<Brief, String> {
    let (front, body) = match split_front_matter(text)? {
        Some((front, body)) => (front, body),
        None => ("", text),
    };

    let raw: FrontMatter = if front.trim().is_empty() {
        FrontMatter::default()
    } else {
        serde_yaml::from_str(front)
            .map_err(|e| format!("brief front matter is not valid YAML: {e}"))?
    };

    let lane = match raw.lane.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(name) => Some(name.parse::<Lane>()?),
    };

    Ok(Brief {
        lane,
        acceptance_criteria: clean(raw.acceptance_criteria),
        verify: clean(raw.verify),
        files_in_scope: clean(raw.files_in_scope),
        non_goals: clean(raw.non_goals),
        result_schema: raw.result_schema.filter(|s| !s.trim().is_empty()),
        max_turns: raw.max_turns,
        body: body.to_string(),
    })
}

/// Check a brief against its lane's required keys ([`required_keys`]).
///
/// The error is the list of missing key names, in table order, so the caller can
/// tell the human exactly which sections to add.
pub fn validate(brief: &Brief, lane: Lane) -> Result<(), Vec<&'static str>> {
    let missing: Vec<&'static str> = required_keys(lane)
        .iter()
        .copied()
        .filter(|key| list(brief, key).is_none_or(<[String]>::is_empty))
        .collect();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

/// The spec §4.7 `BLOCKED:` marker, as a regex over ONE line.
///
/// It is applied to the last non-empty line of a final agent message and to
/// nothing else — see [`blocked_marker`] for why.
pub const BLOCKED_MARKER_RE: &str = r"^BLOCKED:[ \t]*(.*)$";

static BLOCKED_MARKER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(BLOCKED_MARKER_RE).expect("the §4.7 marker must compile"));

/// The §4.7 marker on the LAST non-empty line of a final agent message.
///
/// **Observational only.** `outbox/<seq>.question.json` is what drives the
/// `awaiting-input{blocked}` transition (spec §4.7, and a recorded Phase-5
/// deferral): a turn that ends with the marker and no file has not asked
/// anybody anything, and a matcher that could move the FSM would make prose
/// into control flow. Nothing in the FSM or the reconcile path calls this — it
/// exists so the prompt-contract eval tier can *score* a run, and so an
/// operator reading a result knows the marker was seen.
///
/// **Last non-empty line, and only that.** A mid-message `BLOCKED:` is ignored
/// on purpose: models narrate ("if I am BLOCKED: I will write a question"), and
/// a matcher that fired anywhere in the text would classify a worker that
/// finished cleanly as blocked. Trailing blank lines are tolerated because a
/// final message almost always has one, and `\r\n` because `str::lines` is what
/// splits here and it strips the carriage return.
///
/// `Some("")` for a bare `BLOCKED:` is deliberate — the marker was present, the
/// reason was not. That is a different fact from no marker at all.
pub fn blocked_marker(final_message: &str) -> Option<String> {
    let last = final_message
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())?;
    BLOCKED_MARKER
        .captures(last)
        .map(|caps| caps[1].to_string())
}

/// Hex-encoded SHA-256 of `text` — the `brief_sha` recorded at spawn, which
/// pins a brief as immutable for the life of the worker.
pub fn sha256_hex(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest.iter() {
        // Writing to a String is infallible.
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// Split a leading `---` fence from the body, if there is one.
///
/// `Ok(None)` = no front matter at all. `Err` = a fence that opened and never
/// closed.
fn split_front_matter(text: &str) -> Result<Option<(&str, &str)>, String> {
    let Some(rest) = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
    else {
        return Ok(None);
    };

    let mut offset = 0usize;
    for line in rest.split_inclusive('\n') {
        if is_fence(line) {
            return Ok(Some((&rest[..offset], &rest[offset + line.len()..])));
        }
        offset += line.len();
    }

    Err("brief front matter opened with `---` but is never closed \
         (expected a line containing only `---`)"
        .to_string())
}

/// A closing fence line: `---` (or YAML's document terminator `...`).
fn is_fence(line: &str) -> bool {
    matches!(line.trim_end_matches(['\n', '\r']), "---" | "...")
}

/// Trim entries and drop blank ones — see the module note on why blanks cannot
/// satisfy a requirement.
fn clean(items: Vec<String>) -> Vec<String> {
    items
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Resolve a required-key name to its list field.
///
/// `None` means the name is not a list field this build knows — treated as
/// missing rather than panicking, so a future table row can never take the CLI
/// down; it fails the brief loudly instead.
fn list<'a>(brief: &'a Brief, key: &str) -> Option<&'a [String]> {
    match key {
        "acceptance_criteria" => Some(&brief.acceptance_criteria),
        "verify" => Some(&brief.verify),
        "files_in_scope" => Some(&brief.files_in_scope),
        "non_goals" => Some(&brief.non_goals),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Lane;

    const GOOD: &str = "---\nlane: impl\nacceptance_criteria:\n  - binary exits 0\nverify:\n  - cargo test\nfiles_in_scope:\n  - src/main.rs\nnon_goals:\n  - no refactors\nmax_turns: 30\n---\n# Task\nDo the thing.\n";

    #[test]
    fn parses_front_matter_and_body() {
        let b = parse(GOOD).unwrap();
        assert_eq!(b.lane, Some(Lane::Impl));
        assert_eq!(b.acceptance_criteria, vec!["binary exits 0"]);
        assert_eq!(b.verify, vec!["cargo test"]);
        assert_eq!(b.max_turns, Some(30));
        assert!(b.body.starts_with("# Task"));
    }

    #[test]
    fn validation_table_per_lane() {
        let empty = parse("just a body").unwrap();
        let missing = validate(&empty, Lane::Impl).unwrap_err();
        assert!(missing.contains(&"acceptance_criteria") && missing.contains(&"verify"));
        let ac_only = parse("---\nacceptance_criteria:\n  - x\n---\nb").unwrap();
        assert!(validate(&ac_only, Lane::Impl).is_err()); // impl also needs verify
        assert!(validate(&ac_only, Lane::Review).is_ok()); // review needs AC only
        assert!(validate(&empty, Lane::Research).is_ok());
        assert!(validate(&empty, Lane::General).is_ok());
    }

    /// The four §4.7 pins, stated literally. The third one — a bare `BLOCKED:`
    /// answering `Some("")` — is the one that would quietly become `None` under
    /// a "reason must be non-empty" refactor, and it is exactly the case an
    /// operator most needs to see.
    #[test]
    fn blocked_marker_reads_the_last_non_empty_line_only() {
        assert_eq!(
            blocked_marker("done some work\nBLOCKED: needs a decision\n"),
            Some("needs a decision".to_string())
        );
        assert_eq!(
            blocked_marker("BLOCKED: mid\nand then more text"),
            None,
            "mid-message occurrences are ignored"
        );
        assert_eq!(blocked_marker("BLOCKED:"), Some(String::new()));
        assert_eq!(
            blocked_marker("work\r\nBLOCKED: crlf reason\r\n\r\n  \n"),
            Some("crlf reason".to_string()),
            "trailing blank lines and \\r\\n are tolerated"
        );
    }

    /// The tab half of `[ \t]*`, the empty haystack, and a line that merely
    /// mentions the word — the three ways a looser matcher would be wrong.
    #[test]
    fn blocked_marker_edges() {
        assert_eq!(
            blocked_marker("BLOCKED:\twith a tab"),
            Some("with a tab".to_string())
        );
        assert_eq!(blocked_marker(""), None);
        assert_eq!(blocked_marker("\n\n   \n"), None);
        assert_eq!(blocked_marker("I am not BLOCKED: really"), None);
        assert_eq!(blocked_marker("blocked: lowercase"), None);
    }

    #[test]
    fn sha_is_stable() {
        assert_eq!(
            sha256_hex("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
