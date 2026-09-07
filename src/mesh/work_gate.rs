use crate::config::Config;
use crate::home::A2aHome;
use crate::mesh::registry;
use crate::mesh::render;
use crate::output::{AppError, ExitCode};
use crate::redact;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::process::{Command, Output};

const HOME_DIGEST_MAX_FILE_BYTES: u64 = 1_048_576;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultRange {
    pub base_sha: String,
    pub head_sha: String,
    pub closure_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GateHit {
    pub path: String,
    pub line: usize,
    pub rule: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanReport {
    pub hits: Vec<GateHit>,
    pub scanned_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct HomeDigests {
    sha256_hex: HashSet<String>,
}

impl HomeDigests {
    pub fn load(home: &A2aHome) -> Result<HomeDigests, AppError> {
        let mut digests = HomeDigests::default();
        let registry_env = registry::registry_env_path(home);
        digests.add_regular_file(&registry_env)?;

        let excluded = [home.work_dir(), home.workers_dir(), home.worktrees_dir()];
        let mut pending = vec![home.root().to_path_buf()];
        while let Some(dir) = pending.pop() {
            let entries = fs::read_dir(&dir)
                .map_err(|err| home_digest_io("reading a directory", err))?;
            for entry in entries {
                let entry = entry.map_err(|err| home_digest_io("reading a directory entry", err))?;
                let path = entry.path();
                if excluded.contains(&path) {
                    continue;
                }
                let file_type = entry
                    .file_type()
                    .map_err(|err| home_digest_io("reading an entry type", err))?;
                if file_type.is_dir() {
                    pending.push(path);
                } else if file_type.is_file() {
                    digests.add_regular_file(&path)?;
                }
            }
        }
        Ok(digests)
    }

    pub fn contains(&self, blob_sha256_hex: &str) -> bool {
        self.sha256_hex.contains(blob_sha256_hex)
    }

    fn add_regular_file(&mut self, path: &Path) -> Result<(), AppError> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(home_digest_io("reading file metadata", err)),
        };
        if !metadata.file_type().is_file() || metadata.len() > HOME_DIGEST_MAX_FILE_BYTES {
            return Ok(());
        }

        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(path)
            .map_err(|err| home_digest_io("opening a file", err))?
            .take(HOME_DIGEST_MAX_FILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|err| home_digest_io("reading a file", err))?;
        if bytes.len() as u64 <= HOME_DIGEST_MAX_FILE_BYTES {
            self.sha256_hex
                .insert(hex_lower(&Sha256::digest(&bytes)));
        }
        Ok(())
    }
}

impl ScanReport {
    pub fn digest(&self) -> String {
        let mut hits = self.hits.iter().collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            (&left.path, left.line, left.rule).cmp(&(&right.path, right.line, right.rule))
        });
        let mut digest = Sha256::new();
        for hit in hits {
            digest.update(hit.path.as_bytes());
            digest.update(b"\0");
            digest.update(hit.line.to_string().as_bytes());
            digest.update(b"\0");
            digest.update(hit.rule.as_bytes());
            digest.update(b"\n");
        }
        hex_lower(&digest.finalize())
    }
}

/// Build a git command whose process-wide and repository-local execution hooks are inert.
pub fn git_pinned(checkout: &Path) -> Command {
    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(checkout)
        .arg("--no-pager")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.attributesFile=/dev/null",
            "-c",
            "diff.external=",
            "-c",
            "core.fsmonitor=",
            "-c",
            "protocol.ext.allow=never",
        ])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0");
    cmd
}

/// Build the pinned git command used for result publication.
pub fn git_push_pinned(checkout: &Path) -> Command {
    let mut cmd = git_pinned(checkout);
    cmd.args(["-c", "push.followTags=false"]);
    cmd
}

/// Resolve the exact commit currently checked out without consulting worker-controlled helpers.
pub fn rev_parse_head(checkout: &Path) -> Result<String, AppError> {
    let output = git_pinned(checkout)
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .map_err(|err| {
            AppError::new(
                "WORK_STATE_INVALID",
                ExitCode::Precondition,
                format!("running pinned git rev-parse in {}: {err}", checkout.display()),
            )
            .remedy("re-clone the work item before approving it")
        })?;
    if !output.status.success() {
        return Err(AppError::new(
            "WORK_STATE_INVALID",
            ExitCode::Precondition,
            format!(
                "work checkout {} has no resolvable HEAD (git exited {})",
                checkout.display(),
                output.status
            ),
        )
        .remedy("re-clone the work item before approving it"));
    }
    let head = String::from_utf8(output.stdout).map_err(|_| {
        AppError::new(
            "WORK_STATE_INVALID",
            ExitCode::Precondition,
            format!("work checkout {} returned a non-UTF-8 HEAD", checkout.display()),
        )
        .remedy("re-clone the work item before approving it")
    })?;
    let head = head.trim();
    if head.is_empty() || !head.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AppError::new(
            "WORK_STATE_INVALID",
            ExitCode::Precondition,
            format!("work checkout {} returned an invalid HEAD", checkout.display()),
        )
        .remedy("re-clone the work item before approving it"));
    }
    Ok(head.to_string())
}

/// Freeze the linear commit range and the exact object closure reviewed for publication.
pub fn freeze_range(checkout: &Path, base_sha: &str) -> Result<ResultRange, AppError> {
    let head_sha = rev_parse_head(checkout).map_err(|err| {
        AppError::new("WORK_RESULT_NOT_LINEAR", ExitCode::Precondition, err.message)
            .remedy("restore the approved checkout base and use a linear result history")
    })?;
    let range = format!("{base_sha}..{head_sha}");
    let parents_output = git_output(
        checkout,
        &["rev-list", "--parents", "--full-history"],
        &range,
        "enumerating result commits",
    )?;
    let parents_text = String::from_utf8(parents_output.stdout).map_err(|_| {
        not_linear("git returned a non-UTF-8 commit graph", 0)
    })?;

    let mut parent_of: BTreeMap<String, String> = BTreeMap::new();
    for line in parents_text.lines() {
        let mut fields = line.split_whitespace();
        let sha = fields.next().unwrap_or_default().to_string();
        let parents: Vec<&str> = fields.collect();
        if sha.is_empty() || parents.len() != 1 {
            return Err(not_linear(&sha, parents.len()));
        }
        parent_of.insert(sha, parents[0].to_string());
    }

    let mut current = head_sha.clone();
    let mut walked = 0usize;
    while current != base_sha {
        let Some(parent) = parent_of.get(&current) else {
            return Err(base_not_ancestor(base_sha));
        };
        current.clone_from(parent);
        walked += 1;
        if walked > parent_of.len() {
            return Err(not_linear(&current, 1));
        }
    }
    if walked != parent_of.len() {
        return Err(not_linear("range holds commits off the HEAD chain", 0));
    }

    let objects_output = git_output(
        checkout,
        &["rev-list", "--objects"],
        &range,
        "enumerating result objects",
    )?;
    let mut object_ids = objects_output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter_map(|line| line.split(|byte| byte.is_ascii_whitespace()).next())
        .filter(|id| !id.is_empty())
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    if object_ids
        .iter()
        .any(|id| !id.iter().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err(not_linear("git returned an invalid object id", 0));
    }
    object_ids.sort_unstable();
    object_ids.dedup();
    let mut digest = Sha256::new();
    for id in object_ids {
        digest.update(id);
        digest.update(b"\n");
    }

    Ok(ResultRange {
        base_sha: base_sha.to_string(),
        head_sha,
        closure_digest: hex_lower(&digest.finalize()),
    })
}

/// Refuse publication if a worker changed HEAD after the review range was frozen.
pub fn assert_head_unmoved(checkout: &Path, held: &ResultRange) -> Result<(), AppError> {
    let current = rev_parse_head(checkout)?;
    if current == held.head_sha {
        return Ok(());
    }
    Err(AppError::new(
        "WORK_HEAD_MOVED",
        ExitCode::Precondition,
        format!(
            "work checkout HEAD moved after review (held {}, current {})",
            held.head_sha, current
        ),
    )
    .remedy("run `a2a mesh work diff <id>` again and review the newly frozen result"))
}

/// Scan the committed post-image blobs, never a rendered patch, for structural and secret holds.
pub fn scan_added_blobs(
    home: &A2aHome,
    cfg: &Config,
    checkout: &Path,
    range: &ResultRange,
) -> Result<ScanReport, AppError> {
    let home_digests = HomeDigests::load(home)?;
    let revision_range = format!("{}..{}", range.base_sha, range.head_sha);
    let names = git_pinned(checkout)
        .args([
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--text",
            "--no-renames",
            "--name-status",
            "-z",
        ])
        .arg(&revision_range)
        .arg("--")
        .output()
        .map_err(|err| binary_result_error(checkout, "enumerating changed blobs", err))?;
    if !names.status.success() {
        return Err(binary_result_status_error(
            checkout,
            "enumerating changed blobs",
            names.status,
        ));
    }

    let fields = names
        .stdout
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .collect::<Vec<_>>();
    if fields.len() % 2 != 0 {
        return Err(binary_result_message(
            "git returned a malformed changed-path list",
        ));
    }

    let mut hits = Vec::new();
    let mut scanned_bytes = 0u64;
    for pair in fields.chunks_exact(2) {
        if pair[0] != b"A" && pair[0] != b"M" {
            continue;
        }
        let path = String::from_utf8(pair[1].to_vec()).map_err(|_| {
            binary_result_message("result contains a non-UTF-8 added or modified path")
        })?;
        let object = format!("{}:{path}", range.head_sha);
        let blob = git_pinned(checkout)
            .args(["cat-file", "blob", "--"])
            .arg(&object)
            .output()
            .map_err(|err| binary_result_error(checkout, "reading a changed blob", err))?;
        if !blob.status.success() {
            return Err(binary_result_status_error(
                checkout,
                "reading a changed blob",
                blob.status,
            ));
        }
        scanned_bytes = scanned_bytes.saturating_add(blob.stdout.len() as u64);
        if scanned_bytes > cfg.mesh.work.diff_scan_max_bytes {
            return Err(AppError::new(
                "WORK_SCAN_OVERSIZE",
                ExitCode::Precondition,
                format!(
                    "result scan exceeded mesh.work.diff_scan_max_bytes={} after reading {path}",
                    cfg.mesh.work.diff_scan_max_bytes
                ),
            )
            .remedy(
                "reduce the committed result or raise [mesh.work] diff_scan_max_bytes and rescan",
            ));
        }
        let blob_sha256_hex = hex_lower(&Sha256::digest(&blob.stdout));
        if home_digests.contains(&blob_sha256_hex) {
            hits.push(GateHit {
                path: path.clone(),
                line: 0,
                rule: "a2a_home_file",
            });
        }
        let body = String::from_utf8(blob.stdout).map_err(|_| {
            AppError::new(
                "WORK_BINARY_IN_RESULT",
                ExitCode::Precondition,
                format!("result blob {path} is not UTF-8"),
            )
            .remedy("replace the binary result with reviewable UTF-8 text, then rerun the worker")
        })?;

        if Path::new(&path).file_name().and_then(|name| name.to_str()) == Some(".gitattributes") {
            let lower = body.to_ascii_lowercase();
            if lower.contains("diff") || lower.contains("textconv") {
                return Err(AppError::new(
                    "WORK_ATTRS_IN_RESULT",
                    ExitCode::Precondition,
                    format!("result {path} names diff or textconv behavior"),
                )
                .remedy("remove diff/textconv attribute changes and submit a reviewable result"));
            }
        }

        if let Some(rule) = redact::refuse_filename(&path) {
            hits.push(GateHit {
                path: path.clone(),
                line: 0,
                rule,
            });
        }
        hits.extend(redact::scan_text(&body).into_iter().map(|hit| GateHit {
            path: path.clone(),
            line: hit.line,
            rule: hit.rule,
        }));
        for (line_index, line) in body.lines().enumerate() {
            let Some(name) = env_assignment_name(line) else {
                continue;
            };
            if redact::is_secret_key(name) {
                hits.push(GateHit {
                    path: path.clone(),
                    line: line_index + 1,
                    rule: "secret_key_name",
                });
            }
        }
    }

    Ok(ScanReport {
        hits,
        scanned_bytes,
    })
}

/// Scan the frozen result in one blocking task and refuse any credential-bearing push.
pub async fn gate_push(
    home: &A2aHome,
    cfg: &Config,
    checkout: &Path,
    range: &ResultRange,
) -> Result<ScanReport, AppError> {
    let home = home.clone();
    let cfg = cfg.clone();
    let checkout = checkout.to_path_buf();
    let range = range.clone();
    tokio::task::spawn_blocking(move || {
        let report = scan_added_blobs(&home, &cfg, &checkout, &range)?;
        if report.hits.is_empty() {
            return Ok(report);
        }
        Err(AppError::new(
            "WORK_SECRET_IN_RESULT",
            ExitCode::Precondition,
            format!(
                "result scan found {} credential hit(s) in {}",
                report.hits.len(),
                checkout.display()
            ),
        )
        .remedy(format!(
            "inspect the checkout at {} and review with 'a2a mesh work diff <id> --raw'; --raw output must never be piped into agent context",
            checkout.display()
        )))
    })
    .await
    .map_err(|err| {
        AppError::new(
            "INTERNAL",
            ExitCode::Internal,
            format!("work result scan task failed: {err}"),
        )
        .remedy("retry the gateway work loop; inspect the checkout if the scan task fails again")
    })?
}

fn env_assignment_name(line: &str) -> Option<&str> {
    let (name, value) = line.split_once('=')?;
    if value.is_empty() {
        return None;
    }
    let mut bytes = name.bytes();
    if !matches!(bytes.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_')) {
        return None;
    }
    bytes
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        .then_some(name)
}

fn home_digest_io(operation: &str, err: std::io::Error) -> AppError {
    AppError::new(
        "IO",
        ExitCode::Internal,
        format!("{operation} while scanning $A2A_HOME for result byte matches: {err}"),
    )
    .remedy("repair unreadable files under $A2A_HOME and retry the result scan")
}

/// Render the frozen review surface with pinned git, masking secrets unless raw was requested.
pub fn render_diff(
    checkout: &Path,
    range: &ResultRange,
    raw: bool,
) -> Result<String, AppError> {
    let revision_range = format!("{}..{}", range.base_sha, range.head_sha);
    let stat = render_git(
        checkout,
        &[
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "--text",
            "--stat",
        ],
        &revision_range,
    )?;
    let numstat = render_git(
        checkout,
        &[
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "--text",
            "--numstat",
        ],
        &revision_range,
    )?;
    let patch = render_git(
        checkout,
        &[
            "log",
            "--no-color",
            "-p",
            "--full-history",
            "--no-ext-diff",
            "--no-textconv",
            "--text",
        ],
        &revision_range,
    )?;

    let mut body = String::with_capacity(stat.len() + numstat.len() + patch.len() + 2);
    append_section(&mut body, &stat);
    append_section(&mut body, &numstat);
    append_section(&mut body, &patch);
    let masked;
    let body = if raw {
        body.as_str()
    } else {
        masked = redact::redact_egress(&body);
        masked.as_str()
    };
    Ok(fence_worker_output(&escape_worker_lines(body)))
}

fn git_output(
    checkout: &Path,
    args: &[&str],
    range: &str,
    operation: &str,
) -> Result<Output, AppError> {
    let output = git_pinned(checkout)
        .args(args)
        .arg(range)
        .arg("--")
        .output()
        .map_err(|err| {
            AppError::new(
                "WORK_RESULT_NOT_LINEAR",
                ExitCode::Precondition,
                format!("{operation} in {}: {err}", checkout.display()),
            )
            .remedy("restore the approved checkout base and use a linear result history")
        })?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(AppError::new(
            "WORK_RESULT_NOT_LINEAR",
            ExitCode::Precondition,
            format!(
                "{operation} failed in {} (git exited {})",
                checkout.display(),
                output.status
            ),
        )
        .remedy("restore the approved checkout base and use a linear result history"))
    }
}

fn not_linear(commit: &str, parent_count: usize) -> AppError {
    let subject = if commit.is_empty() { "result range" } else { commit };
    AppError::new(
        "WORK_RESULT_NOT_LINEAR",
        ExitCode::Precondition,
        format!("result history is not linear at {subject} ({parent_count} parents)"),
    )
    .remedy("rebase the worker result into a single linear chain rooted at the approved base")
}

fn base_not_ancestor(base_sha: &str) -> AppError {
    AppError::new(
        "WORK_RESULT_NOT_LINEAR",
        ExitCode::Precondition,
        format!("approved base {base_sha} is not an ancestor of the result HEAD"),
    )
    .remedy("restore the approved checkout base and rebase the result onto it")
}

fn binary_result_error(
    checkout: &Path,
    operation: &str,
    err: std::io::Error,
) -> AppError {
    AppError::new(
        "WORK_BINARY_IN_RESULT",
        ExitCode::Precondition,
        format!("{operation} in {}: {err}", checkout.display()),
    )
    .remedy("restore the result checkout and submit reviewable UTF-8 text only")
}

fn binary_result_status_error(
    checkout: &Path,
    operation: &str,
    status: std::process::ExitStatus,
) -> AppError {
    AppError::new(
        "WORK_BINARY_IN_RESULT",
        ExitCode::Precondition,
        format!("{operation} failed in {} (git exited {status})", checkout.display()),
    )
    .remedy("restore the result checkout and submit reviewable UTF-8 text only")
}

fn binary_result_message(message: impl Into<String>) -> AppError {
    AppError::new("WORK_BINARY_IN_RESULT", ExitCode::Precondition, message)
        .remedy("replace the result with reviewable UTF-8 text and rerun the worker")
}

fn render_git(checkout: &Path, args: &[&str], range: &str) -> Result<String, AppError> {
    let output = git_pinned(checkout)
        .args(args)
        .arg(range)
        .arg("--")
        .output()
        .map_err(|err| {
            AppError::new(
                "WORK_STATE_INVALID",
                ExitCode::Precondition,
                format!("rendering work result in {}: {err}", checkout.display()),
            )
            .remedy("restore the approved checkout and rerun `a2a mesh work diff <id>`")
        })?;
    if !output.status.success() {
        return Err(AppError::new(
            "WORK_STATE_INVALID",
            ExitCode::Precondition,
            format!(
                "rendering work result failed in {} (git exited {})",
                checkout.display(),
                output.status
            ),
        )
        .remedy("restore the approved checkout and rerun `a2a mesh work diff <id>`"));
    }
    String::from_utf8(output.stdout).map_err(|_| {
        AppError::new(
            "WORK_BINARY_IN_RESULT",
            ExitCode::Precondition,
            "git rendered non-UTF-8 worker output",
        )
        .remedy("replace binary result content with reviewable UTF-8 text")
    })
}

fn append_section(body: &mut String, section: &str) {
    if section.is_empty() {
        return;
    }
    if !body.is_empty() && !body.ends_with('\n') {
        body.push('\n');
    }
    body.push_str(section);
    if !body.ends_with('\n') {
        body.push('\n');
    }
}

fn escape_worker_lines(body: &str) -> String {
    let mut escaped = String::with_capacity(body.len());
    let mut lines = body.split('\n').peekable();
    while let Some(line) = lines.next() {
        let rendered = render::escape_untrusted(line, 4_096);
        let added_line = line.starts_with('+') && !line.starts_with("+++");
        if added_line && (rendered.non_ascii || rendered.clamped) {
            escaped.push_str(&format!(
                "[a2a: added-line non_ascii={} clamped={}]\n",
                rendered.non_ascii, rendered.clamped
            ));
        }
        escaped.push_str(&lowercase_generated_escape_hex(
            line,
            &rendered.text,
            4_096,
        ));
        if lines.peek().is_some() {
            escaped.push('\n');
        }
    }
    escaped
}

fn lowercase_generated_escape_hex(original: &str, rendered: &str, max_chars: usize) -> String {
    let mut remaining = rendered;
    let mut out = String::with_capacity(rendered.len());
    for ch in original.chars().take(max_chars) {
        if remaining.starts_with(ch) {
            out.push(ch);
            remaining = &remaining[ch.len_utf8()..];
            continue;
        }
        let escape_len = if u32::from(ch) <= 0xffff { 6 } else { 12 };
        let Some(escape) = remaining.get(..escape_len) else {
            return rendered.to_string();
        };
        out.push_str(&escape.to_ascii_lowercase());
        remaining = &remaining[escape_len..];
    }
    out.push_str(remaining);
    out
}

fn fence_worker_output(body: &str) -> String {
    const BEGIN: &str = "===== BEGIN UNTRUSTED WORKER OUTPUT =====";
    const END: &str = "===== END UNTRUSTED WORKER OUTPUT =====";
    let mut fenced = String::with_capacity(BEGIN.len() + body.len() + END.len() + 2);
    fenced.push_str(BEGIN);
    fenced.push('\n');
    fenced.push_str(body);
    if !body.is_empty() && !body.ends_with('\n') {
        fenced.push('\n');
    }
    fenced.push_str(END);
    fenced
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
