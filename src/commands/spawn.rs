//! `a2a spawn` — the one command that brings a worker into existence
//! (spec §4.7, §4.9, §5).
//!
//! The order of the gates below is the contract, not an implementation
//! detail. Everything that can refuse the spawn runs **before** a single byte
//! is written under `$A2A_HOME/workers/`, so a refused spawn leaves no litter
//! and no half-worker for `ls`/`gc` to trip over:
//!
//! ```text
//! home+config → transport gate → elevation gate → cwd resolution
//!   → brief parse+validate → secret scan → id allocation
//!   ───────────────────────── first write ─────────────────────────
//!   → dir tree (0700) → context staging → brief.md + prompt.md
//!   → genesis event (WAL) → detached spawn → ProcessStarted (WAL)
//! ```
//!
//! Two failure boundaries matter:
//!
//! * **Before the genesis event** the worker does not exist yet, so a failure
//!   removes the directory outright — an id with no `state.json` is a ghost.
//! * **After the genesis event** the worker is real and readable; a failed
//!   `exec::spawn_detached` is recorded as `ProcessExited` so the snapshot
//!   says `failed{never_started}` instead of vanishing.
//!
//! Two rules from §5 are enforced here and nowhere else on this path: the
//! brief is scanned for secret shapes *before* the worker dir exists (a
//! refusal must not leave the credential sitting in `workers/<id>/brief.md`),
//! and `--cwd` is canonicalized on BOTH sides before it is compared to the
//! allowlist component-wise — `/x/a` must never satisfy a root of `/x/ab`.

use crate::brief::{self, Brief};
use crate::cli::SpawnArgs;
use crate::config::{self, Lane, LaneConfig, SandboxMode};
use crate::events::EventKind;
use crate::exec::{self, SpawnSpec};
use crate::fold::append_and_fold;
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::ids;
use crate::output::{AppError, ExitCode};
use crate::redact;
use crate::state::WorkerSnapshot;
use crate::store::EventLog;
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Total budget for everything staged into `context/` (spec §5 keeps the
/// worker's read-side context curated, not a repo dump).
const MAX_CONTEXT_BYTES: u64 = 10 * 1024 * 1024;

/// Id collision retries before we admit the generator is not helping.
const ID_ATTEMPTS: usize = 16;

/// The subdirectories every worker has from birth, all 0700.
const WORKER_DIRS: [&str; 5] = ["outbox", "inbox", "inbox/delivered", "scratch", "context"];

/// Where the worker will run. Research is resolved late, because its cwd is
/// inside the worker dir and the id does not exist yet at gate time.
#[derive(Debug)]
enum CwdPlan {
    /// `<worker>/scratch` — the research lane, always (spec §4.9).
    Scratch,
    /// A canonicalized, allowlisted path.
    Fixed(PathBuf),
}

pub fn run(
    home: &A2aHome,
    args: &SpawnArgs,
    json_mode: bool,
    quiet: bool,
) -> Result<Value, AppError> {
    let (cfg, _origins, warnings) = config::load(home)?;
    let warn = |msg: &str| {
        if !quiet {
            eprintln!("a2a: warning: {msg}");
        }
    };
    for w in &warnings {
        warn(w);
    }
    if args.worktree {
        warn("--worktree is not implemented in Phase 1 (worktrees land in Phase 4); spawning directly in --cwd");
    }
    if args.no_queue {
        warn("--no-queue is a no-op in Phase 1 (there is no broker, so there is no admission queue)");
    }

    // ---- gates: nothing below this block has touched the filesystem --------
    transport_gate(args.transport.as_deref())?;

    let lane = args.lane;
    let lane_cfg = cfg
        .lanes
        .get(&lane)
        .cloned()
        .expect("config::load seeds every lane");
    elevation_gate(lane, &lane_cfg)?;

    let cwd_plan = resolve_cwd(lane, args.cwd.as_deref(), &cfg.allowed_roots)?;

    let brief_text = std::fs::read_to_string(&args.brief).map_err(|e| {
        AppError::new(
            "BRIEF_UNREADABLE",
            ExitCode::Precondition,
            format!("--brief {}: {e}", args.brief.display()),
        )
    })?;
    let parsed = parse_and_validate_brief(&brief_text, &args.brief, lane, &warn)?;
    scan_brief(&brief_text, &args.brief)?;

    if let Some(schema) = args.schema.as_deref() {
        if !schema.is_file() {
            return Err(AppError::new(
                "SCHEMA_NOT_FOUND",
                ExitCode::Precondition,
                format!("--schema {}: not a readable file", schema.display()),
            ));
        }
    }

    let model = args.model.clone().unwrap_or_else(|| lane_cfg.model.clone());
    let effort = args.effort.clone().unwrap_or_else(|| lane_cfg.effort.clone());
    let brief_sha = brief::sha256_hex(&brief_text);
    let now = Utc::now();
    let id = allocate_id(home, lane, now)?;

    // ---- first write ------------------------------------------------------
    let wdir = home.worker_dir(&id);
    create_dir_0700(&wdir).map_err(|e| io_err("creating the worker dir", e))?;
    if let Err(e) = lay_out_worker(&wdir, args, &brief_text, &parsed) {
        // Pre-genesis: this id never became a worker, so leave nothing behind.
        let _ = std::fs::remove_dir_all(&wdir);
        return Err(e);
    }

    let cwd = match cwd_plan {
        CwdPlan::Fixed(p) => p,
        CwdPlan::Scratch => {
            let scratch = wdir.join("scratch");
            std::fs::canonicalize(&scratch).unwrap_or(scratch)
        }
    };

    // ---- genesis: from here the worker exists and is readable --------------
    let mut snap = WorkerSnapshot::blank(&id, &cwd);
    snap.writable_roots = vec![wdir.join("outbox")];
    snap.schema_path = args.schema.clone();
    let mut log = EventLog::open(&wdir).map_err(|e| io_err("opening events.jsonl", e))?;
    let snap = append_and_fold(
        &mut log,
        &wdir,
        snap,
        EventKind::Spawned,
        json!({
            "fsm": "Spawn",
            "id": id,
            "lane": lane.as_str(),
            "cwd": cwd,
            "brief_sha": brief_sha,
            "sandbox": lane_cfg.sandbox.as_str(),
            "network": lane_cfg.network,
            "model": model,
            "effort": effort,
            "transport": "exec",
            "schema_path": args.schema,
            "created_at": now.to_rfc3339(),
        }),
        now,
    )?;

    let codex_bin = cfg
        .codex_bin
        .clone()
        .unwrap_or_else(|| PathBuf::from("codex"));
    let prompt_file = wdir.join("prompt.md");
    let spawned = exec::spawn_detached(&SpawnSpec {
        codex_bin: &codex_bin,
        cwd: &cwd,
        worker_dir: &wdir,
        epoch: snap.epoch,
        sandbox: lane_cfg.sandbox,
        network: lane_cfg.network,
        model: &model,
        effort: &effort,
        schema_path: args.schema.as_deref(),
        prompt_file: &prompt_file,
        resume_session: None,
        env: &[],
    });
    let spawned = match spawned {
        Ok(p) => p,
        Err(e) => {
            // The worker is already durable, so record WHY it is dead rather
            // than deleting the evidence: `spawned` + ProcessExited ⇒
            // `failed{never_started}`.
            let _ = append_and_fold(
                &mut log,
                &wdir,
                snap,
                EventKind::StateChange,
                json!({"fsm": "ProcessExited", "code": null}),
                Utc::now(),
            );
            return Err(e);
        }
    };

    let started = Utc::now();
    let mut snap = snap;
    // The task deadline is armed the moment the process is observed running;
    // `reconcile` (Task 13) is what later fires it.
    snap.deadline_at =
        Some(started + chrono::Duration::seconds(lane_cfg.task_timeout_secs as i64));
    let snap = append_and_fold(
        &mut log,
        &wdir,
        snap,
        EventKind::StateChange,
        json!({
            "fsm": "ProcessStarted",
            "pid": spawned.pid,
            "start_witness": spawned.start_witness,
        }),
        started,
    )?;

    if !quiet {
        eprintln!("a2a: spawned {id} ({lane} lane) in {}", cwd.display());
    }
    // stdout carries the machine payload: the envelope's `data` under --json,
    // the bare id otherwise.
    Ok(if json_mode {
        json!({
            "id": id,
            "lane": lane.as_str(),
            "state": snap.state.as_str(),
            "transport": "exec",
            "epoch": snap.epoch,
            "pid": spawned.pid,
            "cwd": cwd,
            "worker_dir": wdir,
            "brief_sha": brief_sha,
            "sandbox": lane_cfg.sandbox.as_str(),
            "network": lane_cfg.network,
            "model": model,
            "effort": effort,
            "deadline_at": snap.deadline_at,
        })
    } else {
        Value::String(id)
    })
}

// --------------------------------------------------------------------- gates

/// Phase 1 speaks exec only; `auto` resolves to it rather than failing.
fn transport_gate(requested: Option<&str>) -> Result<(), AppError> {
    match requested.unwrap_or("auto") {
        "auto" | "exec" => Ok(()),
        "app-server" => Err(AppError::new(
            "TRANSPORT_UNAVAILABLE",
            ExitCode::Precondition,
            "the app-server transport is not available in Phase 1 (it arrives with the broker)",
        )
        .remedy("re-run with `--transport exec`, or drop the flag")),
        other => Err(AppError::new(
            "TRANSPORT_UNKNOWN",
            ExitCode::Precondition,
            format!("unknown transport `{other}` (expected auto|app-server|exec)"),
        )),
    }
}

/// `danger-full-access` needs the §5 elevation gate (lane permission AND an
/// explicit `--danger-full-access --reason <text>`), which lands with
/// worktrees in Phase 4. Until then a lane configured for it cannot spawn —
/// silently downgrading would be worse than refusing.
fn elevation_gate(lane: Lane, lane_cfg: &LaneConfig) -> Result<(), AppError> {
    if lane_cfg.sandbox == SandboxMode::DangerFullAccess {
        return Err(AppError::new(
            "ELEVATION_NOT_PERMITTED",
            ExitCode::Precondition,
            format!(
                "lane `{lane}` is configured `sandbox = \"danger-full-access\"`, but the elevation \
                 gate (`--danger-full-access --reason <text>`, spec §5) is not implemented in Phase 1"
            ),
        )
        .remedy("give the lane a non-elevated sandbox in $A2A_HOME/config.toml"));
    }
    Ok(())
}

/// Research runs in its own scratch dir, never a repo and never `$HOME`
/// (spec §4.9 — it is the exfiltration lane). Every other lane canonicalizes
/// `--cwd` and checks it against the (already canonicalized) allowlist.
fn resolve_cwd(
    lane: Lane,
    requested: Option<&Path>,
    roots: &[PathBuf],
) -> Result<CwdPlan, AppError> {
    if lane == Lane::Research {
        if requested.is_some() {
            return Err(AppError::new(
                "RESEARCH_CWD_FIXED",
                ExitCode::Precondition,
                "the research lane always runs in its own scratch dir, never a repo or $HOME \
                 (spec §4.9) — `--cwd` cannot be set",
            )
            .remedy("drop --cwd; the worker's cwd will be <worker>/scratch"));
        }
        return Ok(CwdPlan::Scratch);
    }
    let raw = requested.ok_or_else(|| {
        AppError::new(
            "CWD_REQUIRED",
            ExitCode::Precondition,
            format!("--cwd is required for the {lane} lane"),
        )
    })?;
    let canon = std::fs::canonicalize(raw).map_err(|e| {
        AppError::new(
            "CWD_NOT_FOUND",
            ExitCode::Precondition,
            format!("--cwd {}: {e}", raw.display()),
        )
    })?;
    if !roots.iter().any(|root| is_under(&canon, root)) {
        let listed = if roots.is_empty() {
            "none are configured".to_string()
        } else {
            roots
                .iter()
                .map(|r| r.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        return Err(AppError::new(
            "CWD_NOT_ALLOWED",
            ExitCode::Precondition,
            format!("--cwd {} is not under an allowed root ({listed})", canon.display()),
        )
        .remedy("add the root to `allowed_roots` in $A2A_HOME/config.toml (or A2A_ALLOWED_ROOTS)"));
    }
    Ok(CwdPlan::Fixed(canon))
}

/// Containment by path COMPONENT, never by string prefix: `/x/a` is not under
/// `/x/ab`. Both sides must already be canonicalized by the caller.
fn is_under(path: &Path, root: &Path) -> bool {
    path.starts_with(root)
}

fn parse_and_validate_brief(
    text: &str,
    path: &Path,
    lane: Lane,
    warn: &dyn Fn(&str),
) -> Result<Brief, AppError> {
    // A parse error can quote the offending line, so it goes out redacted —
    // an unparseable brief must not become a leak channel.
    let parsed = brief::parse(text).map_err(|e| {
        AppError::new(
            "BRIEF_INVALID",
            ExitCode::Precondition,
            format!("{}: {}", path.display(), redact::redact_egress(&e)),
        )
    })?;
    if let Some(declared) = parsed.lane {
        if declared != lane {
            warn(&format!(
                "brief front matter says `lane: {declared}` but --lane {lane} was requested; using {lane}"
            ));
        }
    }
    if let Err(missing) = brief::validate(&parsed, lane) {
        return Err(AppError::new(
            "BRIEF_INVALID",
            ExitCode::Precondition,
            format!(
                "{}: the {lane} lane requires front-matter key(s): {}",
                path.display(),
                missing.join(", ")
            ),
        )
        .remedy("add the missing keys to the brief's YAML front matter"));
    }
    Ok(parsed)
}

/// Ingress scan (spec §5): a hit fails the spawn naming file+line, and the
/// value never appears in the message — `IngressHit` carries no value by
/// construction.
fn scan_brief(text: &str, path: &Path) -> Result<(), AppError> {
    let hits = redact::scan_text(text);
    if hits.is_empty() {
        return Ok(());
    }
    let where_ = hits
        .iter()
        .take(5)
        .map(|h| format!("line {} ({})", h.line, h.rule))
        .collect::<Vec<_>>()
        .join(", ");
    Err(AppError::new(
        "SECRET_IN_BRIEF",
        ExitCode::Precondition,
        format!(
            "{} contains {} secret-shaped value(s): {where_} — refusing to spawn (nothing was written)",
            path.display(),
            hits.len()
        ),
    )
    .remedy("remove the credential from the brief; a2a never redacts a brief silently"))
}

// ------------------------------------------------------------------- id + IO

fn allocate_id(
    home: &A2aHome,
    lane: Lane,
    now: chrono::DateTime<Utc>,
) -> Result<String, AppError> {
    let mut taken: BTreeSet<String> = home
        .list_worker_ids()
        .map_err(|e| io_err("listing workers", e))?
        .into_iter()
        .collect();
    // Tombstones too: a gc'd id is never reused (spec §4.1).
    taken.extend(tombstone_ids(home));
    (0..ID_ATTEMPTS)
        .map(|_| ids::generate(lane, now))
        .find(|candidate| !taken.contains(candidate))
        .ok_or_else(|| {
            AppError::new(
                "ID_ALLOCATION_FAILED",
                ExitCode::Internal,
                format!("no free worker id for lane {lane} after {ID_ATTEMPTS} attempts"),
            )
        })
}

fn tombstone_ids(home: &A2aHome) -> Vec<String> {
    let Ok(text) = std::fs::read_to_string(home.root().join("index.jsonl")) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|v| {
            v.get("id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

/// Everything written under a freshly created worker dir, before genesis.
fn lay_out_worker(
    wdir: &Path,
    args: &SpawnArgs,
    brief_text: &str,
    parsed: &Brief,
) -> Result<(), AppError> {
    for d in WORKER_DIRS {
        create_dir_0700(&wdir.join(d)).map_err(|e| io_err("creating the worker dir tree", e))?;
    }
    stage_context(wdir, &args.context)?;
    write_0600(&wdir.join("brief.md"), brief_text.as_bytes())?;
    // The prompt is the brief BODY (front matter is metadata for a2a, not
    // instructions for the worker); a body-less brief falls back to the whole
    // file rather than handing codex an empty stdin.
    let prompt = if parsed.body.trim().is_empty() {
        brief_text
    } else {
        &parsed.body
    };
    write_0600(&wdir.join("prompt.md"), prompt.as_bytes())
}

/// Stage `--context` files into `<worker>/context/`, applying both halves of
/// the §5 ingress rule: the name alone can refuse the file, and the content is
/// scanned exactly like the brief.
fn stage_context(wdir: &Path, files: &[PathBuf]) -> Result<(), AppError> {
    let mut total: u64 = 0;
    for src in files {
        let name = src
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .ok_or_else(|| {
                AppError::new(
                    "CONTEXT_UNREADABLE",
                    ExitCode::Precondition,
                    format!("--context {}: not a file path", src.display()),
                )
            })?;
        if let Some(rule) = redact::refuse_filename(&name) {
            return Err(AppError::new(
                "SECRET_FILE_REFUSED",
                ExitCode::Precondition,
                format!("--context {}: matches the secret-file rule `{rule}`", src.display()),
            )
            .remedy("stage a redacted copy under a different name, or drop the file"));
        }
        let bytes = std::fs::read(src).map_err(|e| {
            AppError::new(
                "CONTEXT_UNREADABLE",
                ExitCode::Precondition,
                format!("--context {}: {e}", src.display()),
            )
        })?;
        total += bytes.len() as u64;
        if total > MAX_CONTEXT_BYTES {
            return Err(AppError::new(
                "CONTEXT_TOO_LARGE",
                ExitCode::Precondition,
                format!(
                    "--context totals more than {} MiB; `context/` is curated background, not a repo dump",
                    MAX_CONTEXT_BYTES / (1024 * 1024)
                ),
            ));
        }
        // Binary blobs are staged unscanned — the scanner is line-oriented and
        // a false "clean" on bytes it cannot read would be a lie.
        if let Ok(text) = std::str::from_utf8(&bytes) {
            let hits = redact::scan_text(text);
            if let Some(hit) = hits.first() {
                return Err(AppError::new(
                    "SECRET_IN_CONTEXT",
                    ExitCode::Precondition,
                    format!(
                        "--context {}: line {} looks like a secret ({}) — refusing to spawn",
                        src.display(),
                        hit.line,
                        hit.rule
                    ),
                )
                .remedy("remove the credential from the file, or drop it from --context"));
            }
        }
        write_0600(&wdir.join("context").join(&name), &bytes)?;
    }
    Ok(())
}

fn write_0600(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let mut f = write_new_0600(path).map_err(|e| io_err("creating a worker file", e))?;
    f.write_all(bytes)
        .map_err(|e| io_err("writing a worker file", e))
}

fn io_err(ctx: &str, e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, format!("{ctx}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_compares_whole_components() {
        assert!(is_under(Path::new("/x/ab/c"), Path::new("/x/ab")));
        assert!(is_under(Path::new("/x/ab"), Path::new("/x/ab")));
        // the byte-prefix bug this guards against
        assert!(!is_under(Path::new("/x/abc"), Path::new("/x/ab")));
        assert!(!is_under(Path::new("/x/a"), Path::new("/x/ab")));
        assert!(!is_under(Path::new("/y/ab"), Path::new("/x/ab")));
    }

    #[test]
    fn transport_gate_admits_only_exec_in_phase_one() {
        assert!(transport_gate(None).is_ok());
        assert!(transport_gate(Some("auto")).is_ok());
        assert!(transport_gate(Some("exec")).is_ok());
        let err = transport_gate(Some("app-server")).unwrap_err();
        assert_eq!(err.code, "TRANSPORT_UNAVAILABLE");
        assert_eq!(err.exit, ExitCode::Precondition);
        assert_eq!(
            transport_gate(Some("carrier-pigeon")).unwrap_err().code,
            "TRANSPORT_UNKNOWN"
        );
    }

    #[test]
    fn research_refuses_an_explicit_cwd_and_defaults_to_scratch() {
        let err = resolve_cwd(Lane::Research, Some(Path::new("/tmp")), &[]).unwrap_err();
        assert_eq!(err.code, "RESEARCH_CWD_FIXED");
        assert!(matches!(
            resolve_cwd(Lane::Research, None, &[]),
            Ok(CwdPlan::Scratch)
        ));
    }

    #[test]
    fn secret_scan_names_the_line_but_never_the_value() {
        let err = scan_brief(
            "clean\nkey: sk-abc123456789012345678901\n",
            Path::new("/tmp/b.md"),
        )
        .unwrap_err();
        assert_eq!(err.code, "SECRET_IN_BRIEF");
        assert!(err.message.contains("line 2"), "{}", err.message);
        assert!(!err.message.contains("sk-abc"), "{}", err.message);
    }
}
