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

use super::Disposition;
use crate::output::{AppError, ExitCode};
use crate::redact;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
                let (mut literal, mut via_env) = (Vec::new(), Vec::new());
                for name in extras {
                    let value = headers.get(&name).map(String::as_str).unwrap_or_default();
                    if header_is_secret(&name, value) {
                        let var = header_env_var(&server.name, &name);
                        via_env.push((name, var));
                    } else {
                        literal.push(name);
                    }
                }
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
