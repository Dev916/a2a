//! Reading the Claude side's MCP servers, and deciding which of them may be
//! written into a git-published `config.toml` (spec §6.2).
//!
//! Three decisions live here and nowhere else.
//!
//! **Reading is total.** `~/.claude.json` is 200 KB of somebody else's schema
//! and it grows every release, so [`read_source`] projects what it recognizes
//! and carries what it does not as [`SourceTransport::Unknown`] — never a
//! panic, never a silent drop. A server that vanished from a report because a
//! future Claude version renamed a key would be indistinguishable from a server
//! a2a decided not to touch.
//!
//! **Translation is deny-by-default.** The destination is a *tracked* file in a
//! git repo with a public remote (Ground truth: `~/.codex/config.toml` is
//! tracked and already leaked a key once), so [`classify`] refuses in every
//! direction it cannot prove safe: a header it will not copy, an env value that
//! looks like a credential, a table a2a did not write. The order of those
//! refusals is pinned — **unowned → untranslatable shape → extra headers →
//! bearer → tracked secret env → secret-free** — because a server that is two
//! of them at once must always be reported as the same one.
//!
//! **Reporting is masked unconditionally.** [`report_value`] returns
//! `[REDACTED:<len>]` for *every* value read from a source config, whatever
//! [`classify`] decided about it. That is what stops a classifier bug from
//! becoming a leak: the report is not trusting the classifier's verdict about
//! what is secret, it is refusing to print values at all. Urls, commands and
//! args go out literally — they are not credentials and a report without them
//! is useless — through [`report_literal`], which still runs the egress mask as
//! a backstop.
//!
//! Everything here except [`target_is_tracked`] is a pure function of its
//! arguments: no clock, no environment, no filesystem beyond the one file
//! [`read_source`] is handed.

use super::manifest::{canonical_server_hash, Manifest};
use super::{Disposition, Kind, Plan, PlanEntry};
use crate::config::SyncConfig;
use crate::output::{AppError, ExitCode};
use crate::redact;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use toml_edit::{Array, DocumentMut, InlineTable, Item, Table};

/// The file `sync mcp` reads out of a Claude config directory. The same
/// basename in both branches of [`super::claude_config_dir`] — unlike skills,
/// whose source root moves.
pub const SOURCE_FILE: &str = ".claude.json";

/// The one file inside a codex home this module ever asks git about.
pub const TARGET_FILE: &str = "config.toml";

/// Prefix of every environment variable this module asks a human to export.
const ENV_PREFIX: &str = "A2A_MCP";

/// Verbatim in every bearer report, because the answer is not knowable from
/// here.
///
/// Ground truth could not probe whether codex sends `bearer_token_env_var`'s
/// value as the bare token or as the whole header value — that needs a live
/// MCP server. Rather than guess silently, every report states the assumption
/// AND the one-line fallback, so an operator who hits the other case fixes it
/// in seconds instead of debugging a 401.
pub const BEARER_EXPORT_NOTE: &str = "export the token WITHOUT the \"Bearer \" prefix \
     (codex supplies the scheme); if the server rejects it, export the full header value instead";

/// Why a non-`Authorization` header is skipped by default.
///
/// Recorded deviation from spec §6.2, which says codex cannot express these:
/// it can (`http_headers` / `env_http_headers`, verified on 0.140.0). The skip
/// is therefore a *policy* choice about copying account-scoping identifiers
/// into a git-published file, and the text says so — a warning that blamed a
/// capability limit would send an operator to the codex changelog instead of to
/// the one-line allowlist that actually fixes it.
pub const HEADER_POLICY_NOTE: &str = "codex 0.140.0 CAN express this header, so the skip is a \
     policy choice about copying account-scoping identifiers into a git-published file, not a \
     capability limit; add the SERVER name to [sync] allow_http_header_servers to translate it";

/// The shape reason for a server object that is neither stdio nor HTTP.
pub const UNTRANSLATABLE_SHAPE: &str = "neither a command nor a url";

/// The one command that unblocks a tracked destination.
pub const MIGRATE_REMEDY: &str = "a2a sync mcp --migrate-untrack";

/// One server as this module understands it — the projection that gets hashed,
/// classified and (in Task 5) rendered.
///
/// Deliberately *not* the raw JSON: hashing the projection means an unrelated
/// key Claude adds to a server object next release is not reported as drift in
/// a server a2a already syncs.
#[derive(serde::Serialize, PartialEq, Eq, Clone, Debug)]
pub struct SourceServer {
    pub name: String,
    pub transport: SourceTransport,
}

/// The two shapes a2a can translate, plus the one it carries without
/// understanding.
#[derive(serde::Serialize, PartialEq, Eq, Clone, Debug)]
#[serde(rename_all = "lowercase")]
pub enum SourceTransport {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
    },
    Http {
        url: String,
        headers: BTreeMap<String, String>,
    },
    /// Neither a `command` nor a `url`. Carried so it can be *reported*.
    Unknown,
}

/// What a run may do about one source server, and why.
///
/// Every variant except [`McpClass::SecretFree`] carries the names an operator
/// needs — env vars, header names, an env var to export — and none of them
/// carries a value.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum McpClass {
    /// Nothing here is a credential; write it as it stands.
    SecretFree,
    /// One `Authorization: Bearer …` header, replaced by an env indirection.
    BearerIndirect { env_var: String },
    /// Extra headers, and `[sync] allow_http_header_servers` names this SERVER.
    /// `literal` are header names whose values go into `http_headers`;
    /// `via_env` maps a credential-bearing header name to the env var name that
    /// will hold it. A bearer `Authorization` is in NEITHER list — it stays a
    /// `bearer_token_env_var`, which is the narrower indirection.
    ExtraHeaders {
        literal: Vec<String>,
        via_env: Vec<(String, String)>,
    },
    /// Stdio env values that look like credentials, while the destination is
    /// git-tracked. Carries the offending KEY names only.
    BlockedTrackedSecrets { env_keys: Vec<String> },
    /// Refused: `reason` says why, `headers` names the headers involved (empty
    /// when the refusal is about the server's shape rather than its headers).
    Untranslatable { reason: String, headers: Vec<String> },
    /// The destination already has this table and the ledger does not own it.
    Unowned,
}

impl McpClass {
    /// The pinned [`McpClass`] → [`Disposition`] mapping.
    ///
    /// The three writable classes report [`Disposition::Synced`]; the planner
    /// (Task 5) is the only thing that has seen a destination hash, so it — not
    /// this function — refines a write whose destination already matches to
    /// [`Disposition::Unchanged`].
    pub fn disposition(&self) -> Disposition {
        match self {
            McpClass::Unowned => Disposition::SkippedUnowned,
            McpClass::Untranslatable { .. } => Disposition::SkippedUntranslatable,
            McpClass::BlockedTrackedSecrets { .. } => Disposition::SkippedSecret,
            McpClass::SecretFree
            | McpClass::BearerIndirect { .. }
            | McpClass::ExtraHeaders { .. } => Disposition::Synced,
        }
    }

    /// The human half of a report line, composed to follow
    /// `<disposition> <name> — ` and never carrying a value.
    pub fn detail(&self) -> String {
        match self {
            McpClass::SecretFree => {
                "no credential in the source entry; written as it stands".to_string()
            }
            McpClass::BearerIndirect { env_var } => format!(
                "Authorization becomes bearer_token_env_var = \"{env_var}\"; {BEARER_EXPORT_NOTE}"
            ),
            McpClass::ExtraHeaders { literal, via_env } => {
                let mut detail = "allowlisted by [sync] allow_http_header_servers".to_string();
                if !literal.is_empty() {
                    detail.push_str(&format!(" — http_headers {}", literal.join(", ")));
                }
                if !via_env.is_empty() {
                    let pairs: Vec<String> = via_env
                        .iter()
                        .map(|(header, var)| format!("{header} -> ${var}"))
                        .collect();
                    detail.push_str(&format!(" — env_http_headers {}", pairs.join(", ")));
                }
                detail
            }
            McpClass::BlockedTrackedSecrets { env_keys } => format!(
                "{TARGET_FILE} is git-tracked and these env values look like credentials: {}; \
                 run `{MIGRATE_REMEDY}` to make the destination private first",
                env_keys.join(", ")
            ),
            McpClass::Untranslatable { reason, headers } if headers.is_empty() => reason.clone(),
            McpClass::Untranslatable { reason, headers } => {
                format!("{}: {reason}", headers.join(", "))
            }
            McpClass::Unowned => {
                "a foreign [mcp_servers] table wins; a2a only ever updates what it created"
                    .to_string()
            }
        }
    }
}

/// `<claude config dir>/.claude.json`.
pub fn source_path(claude_config_dir: &Path) -> PathBuf {
    claude_config_dir.join(SOURCE_FILE)
}

/// Project `mcpServers` out of a Claude config, sorted by name.
///
/// A file with no `mcpServers` at all is a normal file (a fresh Claude
/// install), so it reads as an empty list rather than an error. Everything else
/// unreadable is a precondition failure: guessing at a config a2a cannot parse
/// would mean deciding what to write into somebody's home directory on the
/// strength of half a file.
pub fn read_source(claude_json: &Path) -> Result<Vec<SourceServer>, AppError> {
    let text = match std::fs::read_to_string(claude_json) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::new(
                "SYNC_CLAUDE_CONFIG_MISSING",
                ExitCode::Precondition,
                format!("{}: {e}", claude_json.display()),
            )
            .remedy(
                "point --claude-config-dir (or [sync] claude_config_dir) at the directory that \
                 holds .claude.json",
            ))
        }
        Err(e) => return Err(unreadable(claude_json, e)),
    };
    // Only the error's *shape* is reported, never a fragment of the file:
    // `serde_json` names a line and a column, which is all an operator needs
    // and all a 200 KB config of somebody else's secrets may contribute.
    let root: Value = serde_json::from_str(&text).map_err(|e| unreadable(claude_json, e))?;
    let servers = match root.get("mcpServers") {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::Object(map)) => map,
        Some(_) => return Err(unreadable(claude_json, "mcpServers is not an object")),
    };
    let mut out: Vec<SourceServer> = servers
        .iter()
        .map(|(name, value)| SourceServer {
            name: name.clone(),
            transport: project(value),
        })
        .collect();
    // `read_source` feeds a plan, and the plan two runs produce has to be the
    // same value whatever order the source file happened to be written in.
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn unreadable(path: &Path, detail: impl std::fmt::Display) -> AppError {
    AppError::new(
        "SYNC_CLAUDE_CONFIG_UNREADABLE",
        ExitCode::Precondition,
        format!("{}: {detail}", path.display()),
    )
    .remedy("fix the JSON in that file, or point --claude-config-dir at another account")
}

/// `command` wins over `url` — a server object carrying both is a stdio server
/// with a documentation field, and running the command is what Claude does.
fn project(value: &Value) -> SourceTransport {
    if let Some(command) = value.get("command").and_then(Value::as_str) {
        SourceTransport::Stdio {
            command: command.to_string(),
            args: string_list(value.get("args")),
            env: string_map(value.get("env")),
        }
    } else if let Some(url) = value.get("url").and_then(Value::as_str) {
        SourceTransport::Http {
            url: url.to_string(),
            headers: string_map(value.get("headers")),
        }
    } else {
        SourceTransport::Unknown
    }
}

fn string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().map(scalar).collect())
        .unwrap_or_default()
}

fn string_map(value: Option<&Value>) -> BTreeMap<String, String> {
    value
        .and_then(Value::as_object)
        .map(|map| map.iter().map(|(k, v)| (k.clone(), scalar(v))).collect())
        .unwrap_or_default()
}

/// A non-string scalar is rendered rather than dropped: `"env": {"PORT": 8080}`
/// is a real thing people write, and losing the value would produce a server
/// that starts and then misbehaves.
fn scalar(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The order is the pin: **unowned → untranslatable shape → extra headers →
/// bearer → tracked secret env → secret-free**.
///
/// `owned` is the manifest's answer, `dest_has_table` the destination's;
/// ownership is checked first because somebody else's table is not ours to
/// classify — reporting a foreign `[mcp_servers.mx]` as "skipped-secret" would
/// send an operator to `--migrate-untrack` for a problem the migration cannot
/// fix.
///
/// `allow_http_header_servers` holds SERVER names, not header names.
pub fn classify(
    server: &SourceServer,
    owned: bool,
    dest_has_table: bool,
    target_tracked: bool,
    allow_http_header_servers: &[String],
) -> McpClass {
    if dest_has_table && !owned {
        return McpClass::Unowned;
    }
    match &server.transport {
        SourceTransport::Unknown => McpClass::Untranslatable {
            reason: UNTRANSLATABLE_SHAPE.to_string(),
            headers: Vec::new(),
        },
        SourceTransport::Http { headers, .. } => {
            let extras = extra_headers(headers);
            if !extras.is_empty() {
                if !allow_http_header_servers.contains(&server.name) {
                    return McpClass::Untranslatable {
                        reason: HEADER_POLICY_NOTE.to_string(),
                        headers: extras,
                    };
                }
                let (literal, via_env) = split_extra_headers(&server.name, headers);
                return McpClass::ExtraHeaders { literal, via_env };
            }
            if bearer_header(headers).is_some() {
                return McpClass::BearerIndirect {
                    env_var: bearer_env_var(&server.name),
                };
            }
            McpClass::SecretFree
        }
        SourceTransport::Stdio { env, .. } => {
            let env_keys = secret_env_keys(env);
            if target_tracked && !env_keys.is_empty() {
                McpClass::BlockedTrackedSecrets { env_keys }
            } else {
                McpClass::SecretFree
            }
        }
    }
}

/// The allowlisted split: header names whose values may be written literally
/// (`http_headers`), and header names that must travel as the NAME of an env
/// var (`env_http_headers`).
///
/// Shared by [`classify`], which reports it, and by [`render`], which writes
/// it. Two spellings of this split would let a server be *reported* as carrying
/// one literal header and *written* carrying another — the one failure mode a
/// report exists to rule out.
fn split_extra_headers(
    server: &str,
    headers: &BTreeMap<String, String>,
) -> (Vec<String>, Vec<(String, String)>) {
    let (mut literal, mut via_env) = (Vec::new(), Vec::new());
    for name in extra_headers(headers) {
        let value = headers.get(&name).map(String::as_str).unwrap_or_default();
        if header_is_secret(&name, value) {
            let var = header_env_var(server, &name);
            via_env.push((name, var));
        } else {
            literal.push(name);
        }
    }
    (literal, via_env)
}

/// Header names that are not a bearer `Authorization`, in the map's order.
///
/// A NON-bearer `Authorization` (`Basic …`) is deliberately in this list: a2a
/// has no narrower indirection for it, and writing it literally into a tracked
/// file — or, worse, dropping it and leaving an entry that fails at tool-call
/// time — are both worse than refusing.
fn extra_headers(headers: &BTreeMap<String, String>) -> Vec<String> {
    headers
        .iter()
        .filter(|(name, value)| !is_bearer(name, value))
        .map(|(name, _)| name.clone())
        .collect()
}

fn bearer_header(headers: &BTreeMap<String, String>) -> Option<&String> {
    headers
        .iter()
        .find(|(name, value)| is_bearer(name, value))
        .map(|(name, _)| name)
}

fn is_bearer(name: &str, value: &str) -> bool {
    const SCHEME: &[u8] = b"bearer ";
    name.eq_ignore_ascii_case("authorization")
        && value.len() >= SCHEME.len()
        // Compared as BYTES: a value whose first characters are multi-byte
        // would panic a `&value[..7]` slice, and a config a2a cannot classify
        // must not be a config a2a crashes on.
        && value.as_bytes()[..SCHEME.len()].eq_ignore_ascii_case(SCHEME)
}

/// A header carries a credential when its name says so, its value looks like
/// one, or it is an `Authorization` at all.
///
/// The third clause is why this is not simply [`redact::is_secret_key`]:
/// `Authorization` matches no suffix rule and a `Basic` value matches no value
/// shape, yet it is a credential by definition. The dash normalization is here
/// for the same reason — `X-Api-Key` is the header spelling of `API_KEY`, and
/// `redact::is_secret_key` is pinned to the env-var spelling.
fn header_is_secret(name: &str, value: &str) -> bool {
    name.eq_ignore_ascii_case("authorization")
        || redact::is_secret_key(&name.replace(['-', '.', ' '], "_"))
        || !redact::scan_text(value).is_empty()
}

/// Env keys whose values must not be written literally: secret by NAME, or
/// secret by VALUE SHAPE.
///
/// Either, not both, and deny-by-default in the only direction that matters —
/// a false positive costs one env-var indirection, a false negative writes a
/// live credential into a git-published file. Ground truth's real cases need
/// both halves: `CONFLUENCE_API_TOKEN` holds 192 characters matching no shape
/// at all, while a bot session token announces itself (`xoxb-…`) under a key
/// name nobody would flag.
fn secret_env_keys(env: &BTreeMap<String, String>) -> Vec<String> {
    env.iter()
        .filter(|(key, value)| redact::is_secret_key(key) || !redact::scan_text(value).is_empty())
        .map(|(key, _)| key.clone())
        .collect()
}

/// `A2A_MCP_<NAME>_TOKEN` — `Neon` → `A2A_MCP_NEON_TOKEN`,
/// `google-workspace` → `A2A_MCP_GOOGLE_WORKSPACE_TOKEN`.
pub fn bearer_env_var(server_name: &str) -> String {
    format!("{ENV_PREFIX}_{}_TOKEN", shout(server_name))
}

/// `A2A_MCP_<NAME>_<HEADER>` — `("ghl", "X-Api-Key")` →
/// `A2A_MCP_GHL_X_API_KEY`.
pub fn header_env_var(server_name: &str, header: &str) -> String {
    format!("{ENV_PREFIX}_{}_{}", shout(server_name), shout(header))
}

/// Uppercased, with every run of non-alphanumerics collapsed to a single `_`.
fn shout(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_run = false;
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
            in_run = false;
        } else if !in_run {
            out.push('_');
            in_run = true;
        }
    }
    out
}

/// Is `<codex_home>/config.toml` tracked by git?
///
/// Runs `git -C <codex_home> ls-files --error-unmatch -- config.toml` as an
/// argv array, never a shell string, and never looks inside the file: the
/// question is about the file's relationship to a repository, and a function
/// that opened it would be one more place a secret could be read.
///
/// "Not a git repository", "no git on this machine" and every other failure
/// answer **false**, which is the permissive direction — a destination a2a
/// cannot prove is published is treated as private. That is deliberate (the
/// alternative blocks every sync on a machine without git) and it is not the
/// last line of defence: `doctor`'s `codex_config_secrets` check reads the
/// written config back and fails on a tracked file that holds a credential.
pub fn target_is_tracked(codex_home: &Path) -> bool {
    std::process::Command::new("git")
        .arg("-C")
        .arg(codex_home)
        .args(["ls-files", "--error-unmatch", "--", TARGET_FILE])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// `--migrate-untrack`: taking config.toml out of git, on purpose
// ---------------------------------------------------------------------------

/// The comment the ignore entry lands under, verbatim.
///
/// A marker rather than a bare line because the next person to read that
/// `.gitignore` is entitled to know which tool put the entry there and which
/// command to blame; the migration is one-way and nothing here ever removes it.
pub const GITIGNORE_MARKER: &str = "# a2a: untracked by `a2a sync mcp --migrate-untrack`";

/// The sentence every rotation reminder carries verbatim, so it is greppable in
/// a transcript months later.
pub const ROTATION_PHRASE: &str = "already-published secrets must still be rotated";

/// The index-only removal, as an argv array.
///
/// **Never `-f`.** `git rm --cached` drops the index entry and leaves the
/// working file exactly where it is; `-f` (or a plain `git rm`) would delete
/// the config the machine actually runs on. The `--` is not decoration either:
/// it is what stops a pathname from ever being read as an option.
pub const RM_CACHED_ARGV: [&str; 4] = ["rm", "--cached", "--", TARGET_FILE];

/// The ignore file the migration appends to, in the codex home's own root.
const GITIGNORE_FILE: &str = ".gitignore";

/// What `--migrate-untrack` did — or, under `--dry-run`, did not do.
///
/// The three booleans are deliberately about *this run* rather than about the
/// end state, because the caller has to be able to tell "I untracked it" from
/// "it was already untracked" from "I would have untracked it": only the first
/// is a git-visible change somebody now has to commit.
#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    /// A git-visible change was made by this run. Always false under
    /// `--dry-run`, and false when there was nothing to do.
    pub performed: bool,
    /// `config.toml` is out of git's index now — true after a migration and
    /// true when it already was, false under `--dry-run`, where nothing moved
    /// and the classification that follows still sees a published file.
    pub untracked: bool,
    /// The managed line was appended by this run (false when the repository
    /// already ignored the file, by that line or by any other rule).
    pub gitignore_appended: bool,
    /// 1-based lines that carried a secret shape BEFORE the migration, sorted
    /// and deduplicated. Never values — this travels into agent context.
    pub secret_lines: Vec<usize>,
    /// The value-free sentence a run prints. Empty when there was no migration
    /// to remind anybody about.
    pub rotation_reminder: String,
}

/// Take `<codex_home>/config.toml` out of git's index and teach the repository
/// to ignore it, so the stdio credentials [`classify`] refuses to write into a
/// published file may be written into a private one.
///
/// The order is the point, and it is fixed: **confirm, remove from the index,
/// ignore, remind.** Confirmation first because the two ways this can be
/// inapplicable are not the same fact — a codex home that is not a repository
/// is a refusal (`SYNC_MIGRATE_NOT_APPLICABLE`), while a `config.toml` that is
/// already untracked is the desired end state and returns a no-op so the run
/// continues into the sync. Re-running the migration must be free, or nobody
/// will put it in a script.
///
/// This is the only function in the module that changes anything outside a
/// rendered document, and every static precondition of the command it serves is
/// already checked before it is called.
pub fn migrate_untrack(codex_home: &Path, dry_run: bool) -> Result<Migration, AppError> {
    if !is_work_tree(codex_home) {
        return Err(not_applicable(format!(
            "{} is not a git repository, so config.toml is not tracked and there is nothing \
             to untrack",
            codex_home.display()
        )));
    }
    let target = codex_home.join(TARGET_FILE);
    // Scanned BEFORE anything moves: the reminder is about what git has already
    // published, which is a fact about the file as it stands right now.
    let secret_lines = secret_lines(&target);
    if !target_is_tracked(codex_home) {
        // Success-shaped, and silent: the file is already private, so there is
        // no new publication to warn about and the run has a sync to get on
        // with.
        return Ok(Migration {
            performed: false,
            untracked: true,
            gitignore_appended: false,
            secret_lines,
            rotation_reminder: String::new(),
        });
    }
    let rotation_reminder = rotation_reminder(&target, &secret_lines);
    if dry_run {
        return Ok(Migration {
            performed: false,
            untracked: false,
            gitignore_appended: false,
            secret_lines,
            rotation_reminder,
        });
    }
    git_rm_cached(codex_home)?;
    let gitignore_appended = ensure_ignored(codex_home)?;
    Ok(Migration {
        performed: true,
        untracked: true,
        gitignore_appended,
        secret_lines,
        rotation_reminder,
    })
}

/// The git-visible changes a `--migrate-untrack` run would make, as the lines a
/// `--dry-run` prints.
///
/// A free function rather than a [`Migration`] field because a dry run has, by
/// construction, nothing to report about itself: the struct records what
/// happened, and these are what would.
pub fn migration_preview(codex_home: &Path) -> Vec<String> {
    let mut out = vec![format!(
        "run `git -C {} {}`, which removes the index entry and leaves the working file alone",
        codex_home.display(),
        RM_CACHED_ARGV.join(" ")
    )];
    if !already_ignored(codex_home) {
        out.push(format!(
            "append a `{TARGET_FILE}` line to {} under `{GITIGNORE_MARKER}`",
            codex_home.join(GITIGNORE_FILE).display()
        ));
    }
    out
}

/// Is `codex_home` inside a git work tree at all?
///
/// Asked separately from [`target_is_tracked`], which answers false for this
/// case too: "there is no repository here" and "the repository does not track
/// this file" need different sentences, and one boolean cannot carry both.
fn is_work_tree(codex_home: &Path) -> bool {
    std::process::Command::new("git")
        .arg("-C")
        .arg(codex_home)
        .args(["rev-parse", "--is-inside-work-tree"])
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "true")
        .unwrap_or(false)
}

fn git_rm_cached(codex_home: &Path) -> Result<(), AppError> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(codex_home)
        .args(RM_CACHED_ARGV)
        .stdin(std::process::Stdio::null())
        .output();
    match out {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => Err(migrate_failed(format!(
            "git {}: {}",
            RM_CACHED_ARGV.join(" "),
            String::from_utf8_lossy(&o.stderr).trim()
        ))),
        Err(e) => Err(migrate_failed(format!(
            "git {}: {e}",
            RM_CACHED_ARGV.join(" ")
        ))),
    }
}

/// Append the managed block unless the repository already ignores
/// `config.toml`, returning whether anything was written.
fn ensure_ignored(codex_home: &Path) -> Result<bool, AppError> {
    if already_ignored(codex_home) {
        return Ok(false);
    }
    let path = codex_home.join(GITIGNORE_FILE);
    let current = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(migrate_failed(format!("{}: {e}", path.display()))),
    };
    let mut next = current;
    if !next.is_empty() {
        if !next.ends_with('\n') {
            next.push('\n');
        }
        next.push('\n');
    }
    next.push_str(GITIGNORE_MARKER);
    next.push('\n');
    next.push_str(TARGET_FILE);
    next.push('\n');
    // Deliberately a plain write, NOT `store::write_atomic`, for the same
    // reason the skills applier writes its block that way: a rename would
    // replace the inode of a file this repository's owner maintains and walk
    // straight past a mode they set on purpose.
    std::fs::write(&path, next.as_bytes())
        .map_err(|e| migrate_failed(format!("{}: {e}", path.display())))?;
    Ok(true)
}

/// Does the repository ignore `config.toml` already — by the managed line or by
/// any other rule the human wrote?
///
/// `git check-ignore --no-index` is the truthful question (a `*.toml` in the
/// repository's own ignore file covers this file just as well as a literal
/// line), with a plain text scan as the fallback for a git that cannot answer.
/// Getting this wrong in the permissive direction appends a redundant line;
/// getting it wrong in the other direction appends one on every single run.
fn already_ignored(codex_home: &Path) -> bool {
    let asked = std::process::Command::new("git")
        .arg("-C")
        .arg(codex_home)
        .args(["check-ignore", "-q", "--no-index", "--", TARGET_FILE])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    match asked {
        // 0 = ignored, 1 = not ignored, anything else = git could not answer.
        Ok(s) if s.code() == Some(0) => true,
        Ok(s) if s.code() == Some(1) => false,
        _ => std::fs::read_to_string(codex_home.join(GITIGNORE_FILE))
            .unwrap_or_default()
            .lines()
            .any(|l| matches!(l.trim(), TARGET_FILE | "/config.toml")),
    }
}

/// The 1-based lines of `target` that carry a secret shape, sorted and
/// deduplicated — [`redact::scan_text`] reports one hit per match, and two
/// rules firing on one line is one line to rotate, not two.
///
/// A file that cannot be read reports nothing: this feeds a reminder, and a
/// reminder is not the place to fail a run.
///
/// `pub(crate)` for `doctor`'s `codex_config_secrets` check, which asks the same
/// question about the same file and must get the same answer — two scanners
/// disagreeing about which lines carry a credential is the failure that would
/// make the check's line numbers untrustworthy.
pub(crate) fn secret_lines(target: &Path) -> Vec<usize> {
    let Ok(text) = std::fs::read_to_string(target) else {
        return Vec::new();
    };
    let mut lines: Vec<usize> = redact::scan_text(&text)
        .into_iter()
        .map(|h| h.line)
        .collect();
    lines.sort_unstable();
    lines.dedup();
    lines
}

/// The reminder, which names the file and the lines and NEVER what is on them.
///
/// It exists because the migration is easy to mistake for a fix: taking a file
/// out of the index changes nothing about the copies of it already on a remote,
/// in a fork, or in somebody's CI cache. The line numbers are what make it
/// actionable without printing a single byte of the credential.
fn rotation_reminder(target: &Path, lines: &[usize]) -> String {
    let found = if lines.is_empty() {
        "no secret-shaped line is in it today, but git keeps every version that ever was"
            .to_string()
    } else {
        format!(
            "{} secret-shaped line{} ({}) {} tracked",
            lines.len(),
            if lines.len() == 1 { "" } else { "s" },
            lines
                .iter()
                .map(|l| format!("line {l}"))
                .collect::<Vec<_>>()
                .join(", "),
            if lines.len() == 1 { "was" } else { "were" }
        )
    };
    format!(
        "{}: {found}. Untracking removes the file from git's index — it does not un-publish \
         anything already pushed, so {ROTATION_PHRASE}.",
        target.display()
    )
}

/// The migration cannot apply here. A static fact about the destination,
/// decided before anything is touched, so it is exit 9 like every other
/// precondition.
fn not_applicable(detail: String) -> AppError {
    AppError::new(
        "SYNC_MIGRATE_NOT_APPLICABLE",
        ExitCode::Precondition,
        detail,
    )
    .remedy(
        "run `a2a sync mcp` without --migrate-untrack; the migration only applies to a \
         config.toml that a git repository tracks",
    )
}

/// git said no, or the filesystem did, part-way through. **Not** exit 9: the
/// run got past every static precondition and the mutation itself failed, which
/// is a different thing to tell an operator.
fn migrate_failed(detail: String) -> AppError {
    AppError::new("SYNC_MIGRATE_FAILED", ExitCode::Internal, detail).remedy(
        "run `git -C <codex home> rm --cached -- config.toml` by hand and add config.toml to \
         the repository's .gitignore; nothing else in the sync depends on this step",
    )
}

/// `[REDACTED:<len>]`, for every value read from a source config, in every
/// mode, whatever [`classify`] decided about it.
///
/// Unconditional on purpose: if the mask depended on the classification, then a
/// classifier bug would be a leak. The length is the only thing that survives —
/// enough to tell a 47-character token from a 192-character one while
/// debugging, and useless to anybody who reads the transcript.
pub fn report_value(v: &str) -> String {
    format!("[REDACTED:{}]", v.len())
}

/// Urls, command names and args, which are NOT credentials and without which a
/// report is useless — with [`redact::redact_egress`] as a backstop in case one
/// of them turns out to carry a token after all.
pub fn report_literal(v: &str) -> String {
    redact::redact_egress(v)
}

/// The exact TOML a human could paste to translate a skipped HTTP server
/// themselves, with every header value masked.
///
/// `None` for anything that is not HTTP: there is no header remedy for a stdio
/// server, and a remedy that printed an empty table would read as advice.
pub fn untranslatable_remedy(server: &SourceServer) -> Option<String> {
    let SourceTransport::Http { url, headers } = &server.transport else {
        return None;
    };
    let mut out = format!(
        "[mcp_servers.{}]\nurl = \"{}\"\n",
        server.name,
        report_literal(url)
    );
    if bearer_header(headers).is_some() {
        out.push_str(&format!(
            "bearer_token_env_var = \"{}\"\n",
            bearer_env_var(&server.name)
        ));
    }
    let extras = extra_headers(headers);
    if !extras.is_empty() {
        let pairs: Vec<String> = extras
            .iter()
            .map(|name| {
                let value = headers.get(name).map(String::as_str).unwrap_or_default();
                format!("{name} = \"{}\"", report_value(value))
            })
            .collect();
        out.push_str(&format!("http_headers = {{ {} }}\n", pairs.join(", ")));
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// The planner, the writer, and the validation that stands between them
// ---------------------------------------------------------------------------

/// The one place the destination table's name is spelled.
const SERVERS_TABLE: &str = "mcp_servers";

/// Pinned report details for the three verdicts that are facts about the
/// DESTINATION rather than about the source — so they read identically to the
/// skills planner's, which decides the same three things about a tree.
const DRIFT_DETAIL: &str = "destination was edited after the last sync; a2a will not overwrite it";
const REMOVAL_DRIFT_DETAIL: &str =
    "destination was edited after the last sync; a2a will not remove it";
const UNCHANGED_DETAIL: &str = "source and destination both match the ledger";

/// The [`Disposition::SkippedUnowned`] detail for a table a2a *did* write — for
/// another account.
///
/// [`McpClass::classify`] is told only whether THIS account owns the name, so
/// its own detail can say no more than "somebody else's". Two accounts
/// legitimately contending for one destination is the case where that is not
/// enough: an operator reading `skipped-unowned Neon` needs to know which of
/// their Claude configs got there first, or the obvious next move is to delete
/// the table and re-run — which is the ping-pong the ledger exists to prevent.
/// The owner is resolved through [`Manifest::owner_account`], never by taking a
/// manifest key apart.
fn owned_by_detail(account: &str) -> String {
    format!(
        "account `{account}` synced this server first; a2a only ever updates what it created for \
         the account it was run for"
    )
}

/// Parse `<codex_home>/config.toml`, preserving every comment and every blank
/// line. A file that is not there yet is an EMPTY document, not an error: a
/// fresh codex home is a normal codex home.
///
/// A file that does not parse is a precondition failure rather than something
/// to overwrite — codex cannot load it either, and replacing somebody's broken
/// config with a2a's idea of a good one would delete the very lines they are
/// half-way through fixing.
pub fn read_target(codex_home: &Path) -> Result<DocumentMut, AppError> {
    let path = codex_home.join(TARGET_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(target_unreadable(&path, e)),
    };
    text.parse::<DocumentMut>()
        .map_err(|e| target_unreadable(&path, e))
}

fn target_unreadable(path: &Path, detail: impl std::fmt::Display) -> AppError {
    AppError::new(
        "SYNC_CODEX_CONFIG_UNREADABLE",
        ExitCode::Precondition,
        format!("{}: {detail}", path.display()),
    )
    .remedy("fix the TOML at that location — codex cannot load this file either")
}

/// What a run would do about every server in `source`, plus every managed table
/// whose server has left the source.
///
/// Reading only: it opens `config.toml`, asks git one question about it and
/// hashes what it finds. Nothing here writes, which is what makes `--dry-run`
/// side-effect-free as a fact about the call graph.
///
/// The [`Disposition::Synced`] that [`McpClass::disposition`] returns is
/// refined HERE, because this is the only thing that has seen a destination: a
/// managed table whose hash still matches the ledger is `Unchanged`, and one
/// that does not is [`Disposition::RefusedDestDrift`] — refused, never
/// clobbered. A [`Disposition::SkippedUnowned`] is refined the same way and for
/// the same reason: only this function has the ledger, so only it can say
/// whether the table belongs to nobody or to the machine's other account (see
/// [`owned_by_detail`]).
///
/// Every entry's `source_path` is `None`: an mcp source is one object inside a
/// file shared with 200 KB of somebody else's schema, not a path of its own.
/// The ledger records `<claude dir>/.claude.json`, which the applier knows and
/// this function deliberately does not.
pub fn plan(
    source: &[SourceServer],
    codex_home: &Path,
    manifest: &Manifest,
    cfg: &SyncConfig,
    account: &str,
) -> Result<Plan, AppError> {
    let target = codex_home.join(TARGET_FILE);
    let doc = read_target(codex_home)?;
    let tracked = target_is_tracked(codex_home);
    let mut plan = Plan::default();

    for server in source {
        let owned = manifest.owns(Kind::Mcp, account, &server.name);
        let dest = dest_item(&doc, &server.name);
        let class = classify(
            server,
            owned.is_some(),
            dest.is_some(),
            tracked,
            &cfg.allow_http_header_servers,
        );
        let (mut disposition, mut detail) = (class.disposition(), class.detail());
        // A table this account does not own may still be one a2a wrote, for the
        // other account — and then the report can say whose it is.
        if disposition == Disposition::SkippedUnowned {
            if let Some(owner) = manifest.owner_account(Kind::Mcp, &server.name) {
                detail = owned_by_detail(owner);
            }
        }
        if disposition == Disposition::Synced {
            if let (Some(entry), Some(item)) = (owned, dest) {
                if item_hash(item) != entry.dest_hash {
                    disposition = Disposition::RefusedDestDrift;
                    detail = DRIFT_DETAIL.to_string();
                } else if entry.source_hash == canonical_server_hash(server) {
                    disposition = Disposition::Unchanged;
                    detail = UNCHANGED_DETAIL.to_string();
                }
            }
        }
        if disposition == Disposition::SkippedUntranslatable {
            if let Some(remedy) = untranslatable_remedy(server) {
                plan.warnings.push(format!(
                    "{} was not translated — paste this into {} to do it yourself:\n{remedy}",
                    server.name,
                    target.display()
                ));
            }
        }
        plan.entries.push(PlanEntry {
            name: server.name.clone(),
            kind: Kind::Mcp,
            account: account.to_string(),
            source_path: None,
            dest_path: target.clone(),
            disposition,
            detail,
        });
    }

    plan_removals(source, &doc, &target, manifest, account, &mut plan);
    Ok(plan)
}

/// Managed tables whose server is no longer in the source. A destination that
/// has been edited since is refused rather than removed: a user's edits are
/// never deleted on the strength of a deletion somewhere else.
fn plan_removals(
    source: &[SourceServer],
    doc: &DocumentMut,
    target: &Path,
    manifest: &Manifest,
    account: &str,
    plan: &mut Plan,
) {
    // The ledger key is built, never parsed — except here, where the PREFIX is
    // built from the same function and only the remainder is read off. A server
    // name containing a `:` therefore still round-trips.
    let prefix = Manifest::key(Kind::Mcp, account, "");
    for (key, entry) in manifest.entries_for(Kind::Mcp) {
        if entry.account != account {
            continue;
        }
        let Some(name) = key.strip_prefix(&prefix) else {
            continue;
        };
        if source.iter().any(|s| s.name == name) {
            continue; // already decided against the live source above
        }
        let (disposition, detail) = match dest_item(doc, name) {
            None => (
                Disposition::Removed,
                "source is gone; the managed table is already gone",
            ),
            Some(item) if item_hash(item) != entry.dest_hash => {
                (Disposition::RefusedDestDrift, REMOVAL_DRIFT_DETAIL)
            }
            Some(_) => (
                Disposition::Removed,
                "source is gone; the managed table is removed",
            ),
        };
        plan.entries.push(PlanEntry {
            name: name.to_string(),
            kind: Kind::Mcp,
            account: account.to_string(),
            source_path: None,
            dest_path: target.to_path_buf(),
            disposition,
            detail: detail.to_string(),
        });
    }
}

/// Apply `plan` to `doc`: insert or replace the tables it marked
/// [`Disposition::Synced`], remove the ones it marked [`Disposition::Removed`],
/// and touch **nothing** else — every skip, every refusal and every unchanged
/// table keeps the bytes it already had, comments and all.
///
/// The plan is the authority on *whether* a server is written and the source is
/// the authority on *what* is written. That split is why this needs no
/// classifier: a [`Disposition::Synced`] HTTP server carrying extra headers can
/// only have got that verdict by being allowlisted.
pub fn render(doc: &mut DocumentMut, plan: &Plan, source: &[SourceServer]) -> Result<(), AppError> {
    let managed: Vec<&PlanEntry> = plan
        .entries
        .iter()
        .filter(|e| matches!(e.disposition, Disposition::Synced | Disposition::Removed))
        .collect();
    if managed.is_empty() {
        return Ok(());
    }
    let root = doc.as_table_mut();
    if root.get(SERVERS_TABLE).is_none() {
        let mut table = Table::new();
        // Implicit: `[mcp_servers]` itself carries no keys, so it is a heading
        // nobody wrote and nobody should have to read.
        table.set_implicit(true);
        root.insert(SERVERS_TABLE, Item::Table(table));
    }
    let servers = root
        .get_mut(SERVERS_TABLE)
        .and_then(Item::as_table_mut)
        .ok_or_else(|| {
            AppError::new(
                "SYNC_CODEX_CONFIG_UNWRITABLE",
                ExitCode::Precondition,
                format!("{SERVERS_TABLE} is not a table of tables in the codex config"),
            )
            .remedy("rewrite [mcp_servers] as ordinary tables, then re-run")
        })?;

    for entry in managed {
        match entry.disposition {
            Disposition::Synced => {
                let Some(server) = source.iter().find(|s| s.name == entry.name) else {
                    continue; // a Synced entry always has a source; be total anyway
                };
                servers.insert(&entry.name, Item::Table(server_table(server)));
            }
            Disposition::Removed => {
                servers.remove(&entry.name);
            }
            _ => {}
        }
    }
    Ok(())
}

/// One `[mcp_servers.<name>]` table, in the four pinned shapes: stdio
/// (`command`, `args`, `env` — the last two omitted when empty), HTTP
/// (`url`), HTTP with a bearer (`+ bearer_token_env_var`) and HTTP with
/// allowlisted extra headers (`+ http_headers` for values that may be written
/// and `+ env_http_headers` mapping a header name to the NAME of an env var).
fn server_table(server: &SourceServer) -> Table {
    let mut table = Table::new();
    // One blank line before the header, so a file a human maintains stays one a
    // human can read.
    table.decor_mut().set_prefix("\n");
    match &server.transport {
        SourceTransport::Stdio { command, args, env } => {
            table.insert("command", toml_edit::value(command.as_str()));
            if !args.is_empty() {
                let mut array = Array::new();
                for arg in args {
                    array.push(arg.as_str());
                }
                table.insert("args", toml_edit::value(array));
            }
            if !env.is_empty() {
                table.insert("env", toml_edit::value(inline(env.iter())));
            }
        }
        SourceTransport::Http { url, headers } => {
            table.insert("url", toml_edit::value(url.as_str()));
            if bearer_header(headers).is_some() {
                table.insert(
                    "bearer_token_env_var",
                    toml_edit::value(bearer_env_var(&server.name).as_str()),
                );
            }
            let (literal, via_env) = split_extra_headers(&server.name, headers);
            if !literal.is_empty() {
                let pairs = literal.iter().filter_map(|name| {
                    headers.get(name).map(|value| (name.clone(), value.clone()))
                });
                table.insert("http_headers", toml_edit::value(inline(pairs)));
            }
            if !via_env.is_empty() {
                table.insert(
                    "env_http_headers",
                    toml_edit::value(inline(via_env.iter().cloned())),
                );
            }
        }
        // Unreachable through a plan (an unknown shape is never Synced), and
        // total anyway: an empty table is refused by nothing and writes nothing
        // of anybody's.
        SourceTransport::Unknown => {}
    }
    table
}

fn inline<K: AsRef<str>, V: AsRef<str>>(pairs: impl Iterator<Item = (K, V)>) -> InlineTable {
    let mut table = InlineTable::new();
    for (key, value) in pairs {
        table.insert(key.as_ref(), value.as_ref().into());
    }
    table
}

/// `sha256` of a canonical JSON projection of one `[mcp_servers.<name>]` table
/// — keys sorted, every scalar rendered as a string.
///
/// Content, deliberately not formatting: reformatting a managed table or adding
/// a comment to it must not read as "a human edited this", or the drift refusal
/// would fire on every run that followed somebody tidying their config, and an
/// operator who sees a spurious refusal learns to bypass a real one.
pub fn canonical_table_hash(table: &Table) -> String {
    canonical_server_hash(&table_json(table))
}

/// The destination entry for `name`, whatever shape it is in — a `[table]`, a
/// dotted key or an inline table all count as "the destination already has
/// this", which is the question ownership turns on.
fn dest_item<'a>(doc: &'a DocumentMut, name: &str) -> Option<&'a Item> {
    doc.get(SERVERS_TABLE)?
        .as_table_like()?
        .get(name)
        .filter(|item| !item.is_none())
}

/// The `dest_hash` a run records for `name`, or `None` when there is no such
/// table. Same projection as [`canonical_table_hash`], for whichever shape the
/// destination happens to use.
pub fn dest_hash(doc: &DocumentMut, name: &str) -> Option<String> {
    dest_item(doc, name).map(item_hash)
}

/// Every server name the destination document defines, in document order.
///
/// Exists so the ledger's mcp entries can be matched back to their destinations
/// **forwards**, through [`Manifest::key`](super::manifest::Manifest::key): a
/// caller reads a name here, rebuilds the key it would have, and asks the map.
/// Nothing in the crate takes a manifest key apart, which is what keeps a
/// server name containing a `:` from resolving to a silently wrong entry.
pub fn dest_names(doc: &DocumentMut) -> Vec<String> {
    let Some(table) = doc.get(SERVERS_TABLE).and_then(|item| item.as_table_like()) else {
        return Vec::new();
    };
    table
        .iter()
        .filter(|(_, item)| !item.is_none())
        .map(|(name, _)| name.to_string())
        .collect()
}

fn item_hash(item: &Item) -> String {
    canonical_server_hash(&item_json(item))
}

fn table_json(table: &Table) -> Value {
    Value::Object(
        table
            .iter()
            .map(|(key, item)| (key.to_string(), item_json(item)))
            .collect(),
    )
}

fn item_json(item: &Item) -> Value {
    match item {
        Item::None => Value::Null,
        Item::Value(v) => value_json(v),
        Item::Table(t) => table_json(t),
        Item::ArrayOfTables(a) => Value::Array(a.iter().map(table_json).collect()),
    }
}

/// Scalars become strings, which is what [`canonical_server_hash`] does to them
/// anyway: a `port = 8080` and a `port = "8080"` are the same destination.
fn value_json(value: &toml_edit::Value) -> Value {
    match value {
        toml_edit::Value::String(s) => Value::String(s.value().clone()),
        toml_edit::Value::Integer(i) => Value::String(i.value().to_string()),
        toml_edit::Value::Float(f) => Value::String(f.value().to_string()),
        toml_edit::Value::Boolean(b) => Value::String(b.value().to_string()),
        toml_edit::Value::Datetime(d) => Value::String(d.value().to_string()),
        toml_edit::Value::Array(a) => Value::Array(a.iter().map(value_json).collect()),
        toml_edit::Value::InlineTable(t) => Value::Object(
            t.iter()
                .map(|(key, v)| (key.to_string(), value_json(v)))
                .collect(),
        ),
    }
}

/// How long the post-write validation waits for codex, pinned.
///
/// `sync` is meant to be safe to run from a hook, so a `codex` that blocks on
/// stdin or takes a slow cold start must not wedge it. Mirrors Phase 2's
/// `doctor::HANDSHAKE_BUDGET`, the only other child this binary spawns to ask a
/// question.
pub const VALIDATE_BUDGET: Duration = Duration::from_secs(5);

/// How often the validation asks whether the child is done.
const VALIDATE_POLL: Duration = Duration::from_millis(20);

/// One server as `codex mcp list --json` reported it back.
#[derive(serde::Serialize, PartialEq, Eq, Clone, Debug)]
pub struct ValidatedServer {
    pub name: String,
    /// `"stdio"` or `"streamable_http"`.
    pub transport_type: String,
    /// `"bearer_token"` for an HTTP entry with a `bearer_token_env_var`,
    /// `"unsupported"` for stdio and for plain HTTP.
    pub auth_status: String,
}

/// `CODEX_HOME=<temp> <codex_bin> mcp list --json` against a COPY of the
/// rendered document, with stdin on `/dev/null` and the child killed at
/// [`VALIDATE_BUDGET`].
///
/// The copy is not caution, it is necessity: `codex mcp list` creates
/// `<CODEX_HOME>/tmp/` as a side effect, and a validator that dirties the real
/// codex home is not a validator.
///
/// This proves the config **loads**, not that every field is recognised: codex
/// 0.140.0 tolerates unknown fields, and `--strict-config` is nondeterministic
/// on that release (Ground truth), so a spelling mistake in a key codex ignores
/// will pass here and show up as a server that never starts.
///
/// `Ok(None)` is **validation unavailable** — no codex resolved, one that could
/// not be spawned, one that exited 0 with something that is not the expected
/// array, or one that ran out of budget. The caller warns and proceeds; it
/// never silently passes. A nonzero exit is a different fact entirely: codex
/// read the document and refused it, which is
/// `Err(SYNC_CODEX_REJECTED_CONFIG)`.
pub fn validate_with_codex(
    rendered: &str,
    codex_bin: Option<&Path>,
) -> Result<Option<Vec<ValidatedServer>>, AppError> {
    let Some(bin) = codex_bin else {
        return Ok(None);
    };
    let Some(scratch) = Scratch::new() else {
        return Ok(None);
    };
    let home = scratch.0.join("codex-home");
    let (out_path, err_path) = (scratch.0.join("stdout"), scratch.0.join("stderr"));
    if crate::home::create_dir_0700(&home).is_err()
        || std::fs::write(home.join(TARGET_FILE), rendered).is_err()
    {
        return Ok(None);
    }
    let (Ok(out_file), Ok(err_file)) = (
        std::fs::File::create(&out_path),
        std::fs::File::create(&err_path),
    ) else {
        return Ok(None);
    };
    // Files rather than pipes: this is a poll loop, and a child that filled a
    // pipe nobody was draining would deadlock against its own budget.
    let Ok(mut child) = std::process::Command::new(bin)
        .args(["mcp", "list", "--json"])
        .env("CODEX_HOME", &home)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(out_file))
        .stderr(std::process::Stdio::from(err_file))
        .spawn()
    else {
        return Ok(None);
    };

    let deadline = std::time::Instant::now() + VALIDATE_BUDGET;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Err(_) => break None,
            Ok(None) => {}
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(VALIDATE_POLL);
    };
    let Some(status) = status else {
        return Ok(None); // wedged or unwaitable: unavailable, never a pass
    };
    if !status.success() {
        let stderr = std::fs::read_to_string(&err_path).unwrap_or_default();
        return Err(rejected(bin, &stderr));
    }
    let stdout = std::fs::read_to_string(&out_path).unwrap_or_default();
    Ok(parse_listing(&stdout))
}

/// codex read the rendered document and refused it. Its own message carries
/// `<path>:<line>:<col>`, which is the only thing that says WHERE, so it is
/// quoted rather than summarized.
fn rejected(bin: &Path, stderr: &str) -> AppError {
    let said = stderr
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("(no message)");
    AppError::new(
        "SYNC_CODEX_REJECTED_CONFIG",
        ExitCode::Precondition,
        format!("{} rejected the rendered config: {said}", bin.display()),
    )
    .remedy(
        "the real config.toml was NOT touched — re-run with --dry-run and report the message \
         above, which names the line codex could not read",
    )
}

/// `None` for anything that is not the expected array — including valid JSON of
/// another shape, which is what a foreign or future `codex` on `PATH` would
/// print.
fn parse_listing(stdout: &str) -> Option<Vec<ValidatedServer>> {
    let items = serde_json::from_str::<Value>(stdout).ok()?;
    let items = items.as_array()?.clone();
    items
        .iter()
        .map(|item| {
            Some(ValidatedServer {
                name: item.get("name")?.as_str()?.to_string(),
                transport_type: item.get("transport")?.get("type")?.as_str()?.to_string(),
                auth_status: item.get("auth_status")?.as_str()?.to_string(),
            })
        })
        .collect()
}

/// A private directory under the system temp dir, removed on every path out —
/// including the wedged one. `tempfile` is a dev-dependency, and a validator
/// that leaked a directory per run would be a slow disk leak on a machine that
/// syncs from a hook.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Option<Scratch> {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir =
            std::env::temp_dir().join(format!("a2a-sync-validate-{}-{unique}", std::process::id()));
        crate::home::create_dir_0700(&dir).ok()?;
        Some(Scratch(dir))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http(name: &str, headers: &[(&str, &str)]) -> SourceServer {
        SourceServer {
            name: name.to_string(),
            transport: SourceTransport::Http {
                url: "https://example.com/mcp".to_string(),
                headers: headers
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
        }
    }

    fn stdio(name: &str, env: &[(&str, &str)]) -> SourceServer {
        SourceServer {
            name: name.to_string(),
            transport: SourceTransport::Stdio {
                command: "uvx".to_string(),
                args: vec!["thing".to_string()],
                env: env
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
        }
    }

    /// The five branches of the pinned order, each reached by the input the
    /// order says should reach it.
    #[test]
    fn classification_follows_the_pinned_order() {
        let allow: Vec<String> = vec![];
        let bearer = http("neon", &[("Authorization", "Bearer aaaaaaaaaaaaaaaaaaaaaa")]);
        assert_eq!(
            classify(&bearer, false, false, true, &allow),
            McpClass::BearerIndirect {
                env_var: "A2A_MCP_NEON_TOKEN".to_string()
            }
        );
        assert_eq!(
            classify(&bearer, false, true, true, &allow),
            McpClass::Unowned,
            "unowned is checked before anything else"
        );
        assert_eq!(
            classify(&bearer, true, true, true, &allow),
            McpClass::BearerIndirect {
                env_var: "A2A_MCP_NEON_TOKEN".to_string()
            },
            "owned means a2a wrote it, so the ordinary verdict applies"
        );

        assert_eq!(
            classify(&http("plain", &[]), false, false, true, &allow),
            McpClass::SecretFree
        );
        assert_eq!(
            classify(&stdio("mx", &[("MX_MODE", "local")]), false, false, true, &allow),
            McpClass::SecretFree,
            "an innocent env value is writable even into a tracked file"
        );
        assert_eq!(
            classify(
                &SourceServer {
                    name: "broken".to_string(),
                    transport: SourceTransport::Unknown
                },
                false,
                false,
                true,
                &allow
            ),
            McpClass::Untranslatable {
                reason: UNTRANSLATABLE_SHAPE.to_string(),
                headers: vec![],
            }
        );
    }

    /// A non-bearer `Authorization` has no narrower indirection, so it is
    /// refused rather than written literally or silently dropped.
    #[test]
    fn a_non_bearer_authorization_is_untranslatable_not_secret_free() {
        let basic = http("legacy", &[("Authorization", "Basic ZmFrZTpmYWtl")]);
        assert_eq!(
            classify(&basic, false, false, true, &[]),
            McpClass::Untranslatable {
                reason: HEADER_POLICY_NOTE.to_string(),
                headers: vec!["Authorization".to_string()],
            }
        );
        // Allowlisted, it becomes an env indirection — never an http_headers
        // literal, because an Authorization is a credential by definition.
        assert_eq!(
            classify(&basic, false, false, true, &["legacy".to_string()]),
            McpClass::ExtraHeaders {
                literal: vec![],
                via_env: vec![(
                    "Authorization".to_string(),
                    "A2A_MCP_LEGACY_AUTHORIZATION".to_string()
                )],
            }
        );
    }

    /// The allowlisted split: a credential-shaped header goes through an env
    /// var, an opaque identifier goes in literally, and a bearer beside them
    /// stays a `bearer_token_env_var` rather than joining either list.
    #[test]
    fn allowlisted_headers_split_by_whether_they_are_credentials() {
        let server = http(
            "ghl",
            &[
                ("Authorization", "Bearer aaaaaaaaaaaaaaaaaaaaaa"),
                ("locationId", "opaque0000000000fake"),
                ("X-Api-Key", "also-opaque-but-named-like-a-key"),
                ("X-Trace", "xoxb-1234567890-abcdef"),
            ],
        );
        assert_eq!(
            classify(&server, false, false, true, &["ghl".to_string()]),
            McpClass::ExtraHeaders {
                literal: vec!["locationId".to_string()],
                via_env: vec![
                    ("X-Api-Key".to_string(), "A2A_MCP_GHL_X_API_KEY".to_string()),
                    ("X-Trace".to_string(), "A2A_MCP_GHL_X_TRACE".to_string()),
                ],
            },
            "secret by NAME and secret by VALUE SHAPE both land in env_http_headers"
        );
    }

    /// Both halves of the env rule, and the tracked/untracked switch that is
    /// the whole point of the migration.
    #[test]
    fn stdio_secrets_are_blocked_only_while_the_target_is_tracked() {
        let server = stdio(
            "atlassian",
            &[
                ("CONFLUENCE_URL", "https://example.atlassian.net"),
                ("CONFLUENCE_API_TOKEN", "opaque-matches-no-shape-at-all"),
                ("SESSION_BLOB", "xoxb-1234567890-abcdef"),
            ],
        );
        assert_eq!(
            classify(&server, false, false, true, &[]),
            McpClass::BlockedTrackedSecrets {
                env_keys: vec![
                    "CONFLUENCE_API_TOKEN".to_string(),
                    "SESSION_BLOB".to_string()
                ],
            }
        );
        assert_eq!(
            classify(&server, false, false, false, &[]),
            McpClass::SecretFree,
            "an untracked destination is a private file"
        );
    }

    #[test]
    fn env_var_names_collapse_runs_of_punctuation() {
        assert_eq!(bearer_env_var("Neon"), "A2A_MCP_NEON_TOKEN");
        assert_eq!(
            bearer_env_var("google-workspace"),
            "A2A_MCP_GOOGLE_WORKSPACE_TOKEN"
        );
        assert_eq!(bearer_env_var("a..b"), "A2A_MCP_A_B_TOKEN", "a RUN collapses");
        assert_eq!(header_env_var("ghl", "locationId"), "A2A_MCP_GHL_LOCATIONID");
        assert_eq!(header_env_var("ghl", "X-Api-Key"), "A2A_MCP_GHL_X_API_KEY");
    }

    /// The mask does not consult the classification, so a value the classifier
    /// got wrong is still masked.
    #[test]
    fn report_value_masks_unconditionally_and_literals_still_get_a_backstop() {
        assert_eq!(report_value("https://example.com/mcp"), "[REDACTED:23]");
        assert_eq!(report_value(""), "[REDACTED:0]");
        assert_eq!(report_value("sk-abc123456789012345678901"), "[REDACTED:27]");
        assert_eq!(
            report_literal("https://example.com/mcp"),
            "https://example.com/mcp",
            "a url is not a credential"
        );
        assert!(
            report_literal("--token=xoxb-1234567890-abcdef").contains("[REDACTED:"),
            "an arg that turns out to carry a token is still masked"
        );
    }

    #[test]
    fn each_class_details_itself_without_naming_a_value() {
        let detail = McpClass::BearerIndirect {
            env_var: "A2A_MCP_NEON_TOKEN".to_string(),
        }
        .detail();
        assert!(detail.contains("A2A_MCP_NEON_TOKEN"));
        assert!(detail.contains(BEARER_EXPORT_NOTE));

        let detail = McpClass::BlockedTrackedSecrets {
            env_keys: vec!["JIRA_API_TOKEN".to_string()],
        }
        .detail();
        assert!(detail.contains("JIRA_API_TOKEN") && detail.contains(MIGRATE_REMEDY));

        let detail = McpClass::Untranslatable {
            reason: HEADER_POLICY_NOTE.to_string(),
            headers: vec!["locationId".to_string()],
        }
        .detail();
        assert!(detail.starts_with("locationId: "), "{detail}");

        assert_eq!(
            McpClass::Untranslatable {
                reason: UNTRANSLATABLE_SHAPE.to_string(),
                headers: vec![],
            }
            .detail(),
            UNTRANSLATABLE_SHAPE
        );

        let detail = McpClass::ExtraHeaders {
            literal: vec!["locationId".to_string()],
            via_env: vec![("X-Api-Key".to_string(), "A2A_MCP_GHL_X_API_KEY".to_string())],
        }
        .detail();
        assert!(detail.contains("http_headers locationId"), "{detail}");
        assert!(detail.contains("X-Api-Key -> $A2A_MCP_GHL_X_API_KEY"), "{detail}");
    }

    /// Dispositions are the vocabulary every report and every AC quotes, so the
    /// mapping is asserted rather than inferred.
    #[test]
    fn the_class_to_disposition_mapping_is_the_pinned_one() {
        assert_eq!(McpClass::Unowned.disposition(), Disposition::SkippedUnowned);
        assert_eq!(
            McpClass::Untranslatable {
                reason: String::new(),
                headers: vec![]
            }
            .disposition(),
            Disposition::SkippedUntranslatable
        );
        assert_eq!(
            McpClass::BlockedTrackedSecrets { env_keys: vec![] }.disposition(),
            Disposition::SkippedSecret
        );
        for writable in [
            McpClass::SecretFree,
            McpClass::BearerIndirect {
                env_var: String::new(),
            },
            McpClass::ExtraHeaders {
                literal: vec![],
                via_env: vec![],
            },
        ] {
            assert_eq!(writable.disposition(), Disposition::Synced);
        }
    }

    /// Both states, against a real repository, because "is this file published"
    /// is the question the whole secret policy turns on.
    #[test]
    fn target_is_tracked_answers_for_a_temp_repo_in_both_states() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("codex");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join(TARGET_FILE), "# codex config\n").unwrap();

        assert!(
            !target_is_tracked(&repo),
            "a plain directory is not a repository"
        );
        assert!(!target_is_tracked(&tmp.path().join("nope")), "nor is nothing");

        let nowhere = tmp.path().join("no-such-gitconfig");
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(&repo)
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
        };
        git(&["init", "-q", "-b", "main"]);
        assert!(
            !target_is_tracked(&repo),
            "a repository that has not added the file yet"
        );
        git(&["add", TARGET_FILE]);
        git(&["commit", "-qm", "config"]);
        assert!(target_is_tracked(&repo), "tracked, and therefore published");

        git(&["rm", "-q", "--cached", TARGET_FILE]);
        assert!(
            !target_is_tracked(&repo),
            "untracked again, which is what --migrate-untrack will do"
        );
    }

    /// Drift is about content. Reformatting a table, commenting it or
    /// reordering its keys is not an edit; changing a value is.
    #[test]
    fn the_destination_hash_is_content_not_formatting() {
        let doc = |text: &str| text.parse::<DocumentMut>().expect("the fixture parses");
        let plain = doc("[mcp_servers.neon]\nurl = \"https://x/mcp\"\nbearer_token_env_var = \"V\"\n");
        let fussed = doc(
            "# a human's note\n[mcp_servers.neon]\nbearer_token_env_var   =   \"V\"   # why\n\n\
             url = 'https://x/mcp'\n",
        );
        let changed = doc("[mcp_servers.neon]\nurl = \"https://y/mcp\"\nbearer_token_env_var = \"V\"\n");

        let hash = |d: &DocumentMut| dest_hash(d, "neon").expect("the table is there");
        assert_eq!(
            hash(&plain),
            hash(&fussed),
            "comments, spacing, quoting and key order are not content"
        );
        assert_ne!(hash(&plain), hash(&changed), "a value IS content");
        assert_eq!(dest_hash(&doc("model = \"x\"\n"), "neon"), None);

        // The same projection whichever shape the destination uses, so a table
        // a human wrote inline is not reported as drift the moment a2a adopts
        // it.
        let inline_form = doc("[mcp_servers]\nneon = { url = \"https://x/mcp\", bearer_token_env_var = \"V\" }\n");
        assert_eq!(hash(&inline_form), hash(&plain));
        assert_eq!(
            canonical_table_hash(
                plain["mcp_servers"]["neon"]
                    .as_table()
                    .expect("a real table")
            ),
            hash(&plain),
            "the pinned public spelling agrees with the internal one"
        );
    }

    /// A validator that answered something else has not rejected anything — and
    /// must not be read as though it had.
    #[test]
    fn only_the_expected_array_parses_as_a_listing() {
        let good = r#"[{"name":"neon","transport":{"type":"streamable_http"},
                       "auth_status":"bearer_token"}]"#;
        assert_eq!(
            parse_listing(good),
            Some(vec![ValidatedServer {
                name: "neon".to_string(),
                transport_type: "streamable_http".to_string(),
                auth_status: "bearer_token".to_string(),
            }])
        );
        assert_eq!(parse_listing("[]"), Some(vec![]), "no servers is an answer");
        for other in [
            "",
            "codex-cli 0.140.0",
            "{\"servers\":[]}",
            r#"[{"type":"session.created"}]"#,
            r#"[{"name":"neon","transport":{},"auth_status":"x"}]"#,
        ] {
            assert_eq!(parse_listing(other), None, "not a listing: {other}");
        }
    }

    /// The reader carries what it cannot read, and normalizes the scalars it
    /// can, because the source file is somebody else's schema.
    #[test]
    fn the_projection_normalizes_scalars_and_never_drops_a_server() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(SOURCE_FILE);
        std::fs::write(
            &path,
            r#"{"mcpServers": {
                 "b": {"command": "run", "args": ["x", 8080], "env": {"PORT": 8080}},
                 "a": {"url": "https://a/mcp", "headers": {}},
                 "z": {"type": "future-transport"},
                 "both": {"command": "run", "url": "https://ignored/mcp"}
               }}"#,
        )
        .unwrap();
        let servers = read_source(&path).unwrap();
        let names: Vec<&str> = servers.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["a", "b", "both", "z"], "sorted, and all four kept");

        let SourceTransport::Stdio { args, env, .. } = &servers[1].transport else {
            panic!("b is stdio");
        };
        assert_eq!(args, &["x", "8080"], "a non-string arg is rendered, not lost");
        assert_eq!(env["PORT"], "8080");
        assert_eq!(servers[3].transport, SourceTransport::Unknown);
        assert!(
            matches!(&servers[2].transport, SourceTransport::Stdio { .. }),
            "command wins over url"
        );
    }
}
