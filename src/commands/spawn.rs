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
//!   → brief parse+validate → secret scan → TRANSPORT SELECTION → id allocation
//!   ───────────────────────── first write ─────────────────────────
//!   → dir tree (0700) → context staging → brief.md + prompt.md
//!   → genesis event (WAL) → detached spawn / broker Spawn frame
//!                         → ProcessStarted (WAL, exec only)
//! ```
//!
//! **Transport selection is a gate, not a fallback inside the write path**
//! (spec §4.4). It sits with the other gates, before the first byte is written,
//! so that:
//!
//! * `A2A_BROKER_DISABLE=1` takes *zero* socket syscalls — the Phase-1 path, bit
//!   for bit, which is what keeps the Phase-1 suite meaningful;
//! * `--transport auto` that cannot reach a broker degrades to exec and records
//!   a `transport_degraded` envelope, so a silently-downgraded worker does not
//!   exist;
//! * `--transport app-server` that cannot reach one **hard-fails with exit 7**
//!   rather than downgrading: exec has no true interrupt and no mid-turn steer,
//!   and a caller who named the transport asked for those.
//!
//! Exit 7 there is reachability, not a static precondition — exit 9 is reserved
//! for a transport string this build could never offer at all.
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
use crate::brokerclient::{self, BrokerClient};
use crate::cli::SpawnArgs;
use crate::config::{self, Lane, LaneConfig, SandboxMode};
use crate::events::{EventKind, Transport};
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
    let requested = transport_gate(args.transport.as_deref())?;

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

    // The last gate: which transport will own this worker. A refusal here still
    // leaves nothing behind, and the answer is what the genesis event records.
    let selected = select_transport(home.root(), requested)?;
    let transport = selected.transport();
    if let Selected::Exec {
        degraded: Some(err),
    } = &selected
    {
        warn(&format!(
            "the broker is not reachable ({}: {}) — falling back to the exec transport",
            err.code, err.message
        ));
    }

    let model = args.model.clone().unwrap_or_else(|| lane_cfg.model.clone());
    let effort = args.effort.clone().unwrap_or_else(|| lane_cfg.effort.clone());
    let brief_sha = brief::sha256_hex(&brief_text);
    let owner = resolve_owner();
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
    let mut snap = append_and_fold(
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
            "transport": transport,
            // Spec §4.4: both are recorded. `requested` is the explicit value
            // when one was given, and the resolved one otherwise — so it always
            // answers "what was this worker asked to be", never "nothing".
            "requested_transport": requested.recorded(transport),
            "transport_selection": requested.selection(),
            "owner": owner,
            "schema_path": args.schema,
            "created_at": now.to_rfc3339(),
        }),
        now,
    )?;

    // A degradation is an EVENT (spec §4.4), not a log line: an orchestrator
    // reading the worker's own stream must be able to see that it asked for the
    // live transport and got the fallback.
    if let Selected::Exec {
        degraded: Some(err),
    } = &selected
    {
        snap = append_and_fold(
            &mut log,
            &wdir,
            snap,
            EventKind::TransportDegraded,
            json!({
                "requested": "app-server",
                "selected": "exec",
                "code": err.code,
                "detail": redact::redact_egress(&err.message),
            }),
            Utc::now(),
        )?;
    }

    // ---- hand the worker to its transport ---------------------------------
    if let Selected::AppServer(client) = selected {
        let snap = attach_to_broker(client, &mut log, &wdir, snap, lane, &cwd)?;
        if !quiet {
            eprintln!(
                "a2a: spawned {id} ({lane} lane, app-server) in {}",
                cwd.display()
            );
        }
        return Ok(if json_mode {
            json!({
                "id": id,
                "lane": lane.as_str(),
                "state": snap.state.as_str(),
                "transport": Transport::AppServer,
                "epoch": snap.epoch,
                // The app-server child is the BROKER's process, shared by every
                // worker: no worker-scoped pid exists to report.
                "pid": Value::Null,
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
        });
    }

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
            "transport": Transport::Exec,
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

/// What `--transport` asked for, parsed. Every value this build can offer is
/// here; anything else is the one *static* transport failure left (exit 9).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Requested {
    /// The default: the live transport when there is one, exec when there is
    /// not.
    Auto,
    /// Named explicitly — and therefore never silently downgraded.
    AppServer,
    Exec,
}

impl Requested {
    /// The value recorded as `requested_transport`: the explicit ask, or the
    /// transport actually resolved when the caller named none.
    fn recorded(self, resolved: Transport) -> Transport {
        match self {
            Requested::Auto => resolved,
            Requested::AppServer => Transport::AppServer,
            Requested::Exec => Transport::Exec,
        }
    }

    /// `auto` | `explicit` — how the transport was chosen, which is not
    /// derivable from the transport itself.
    fn selection(self) -> &'static str {
        match self {
            Requested::Auto => "auto",
            _ => "explicit",
        }
    }
}

/// Parse `--transport`. Unknown values are the only remaining exit-9 transport
/// failure: they name something this build could never offer, whatever the
/// broker is doing.
fn transport_gate(requested: Option<&str>) -> Result<Requested, AppError> {
    match requested.unwrap_or("auto") {
        "auto" => Ok(Requested::Auto),
        "exec" => Ok(Requested::Exec),
        "app-server" => Ok(Requested::AppServer),
        other => Err(AppError::new(
            "TRANSPORT_UNKNOWN",
            ExitCode::Precondition,
            format!("unknown transport `{other}` (expected auto|app-server|exec)"),
        )
        .remedy("re-run with `--transport auto|app-server|exec`, or drop the flag")),
    }
}

/// The transport this spawn will use, and (for app-server) the connection it
/// will use it over.
enum Selected {
    Exec {
        /// `Some` when the caller asked for `auto`, a broker was attempted, and
        /// it could not be reached — the reason a `transport_degraded` envelope
        /// is about to be written.
        degraded: Option<AppError>,
    },
    AppServer(BrokerClient),
}

impl Selected {
    fn transport(&self) -> Transport {
        match self {
            Selected::Exec { .. } => Transport::Exec,
            Selected::AppServer(_) => Transport::AppServer,
        }
    }
}

/// Choose the transport — the ONE place a socket may be opened before the
/// worker exists.
///
/// The disable seam is checked first and short-circuits absolutely: with
/// `A2A_BROKER_DISABLE` set, neither branch reaches [`brokerclient`], so a
/// Phase-1 spawn performs no socket syscall at all.
fn select_transport(home: &Path, requested: Requested) -> Result<Selected, AppError> {
    match requested {
        Requested::Exec => Ok(Selected::Exec { degraded: None }),
        // A configured exec spawn is not a degradation: nothing was attempted,
        // so there is nothing to report as lost.
        Requested::Auto if brokerclient::disabled() => Ok(Selected::Exec { degraded: None }),
        Requested::Auto => match brokerclient::connect_or_start(home) {
            Ok(client) => Ok(Selected::AppServer(client)),
            Err(e) => Ok(Selected::Exec { degraded: Some(e) }),
        },
        // Explicit means explicit: exec has no true interrupt and no mid-turn
        // steer, so a caller who named app-server gets an error, not a quieter
        // worker (spec §4.4).
        Requested::AppServer if brokerclient::disabled() => Err(brokerclient::disabled_error()),
        Requested::AppServer => brokerclient::connect_or_start(home).map(Selected::AppServer),
    }
}

/// Who this worker belongs to.
///
/// Phase 2 records it at genesis so a rebuild is lossless; the `--owner` flag
/// and the cross-owner guards land in Task 15. `anon-<uid>` rather than `None`
/// because "unowned" already means something else on disk: a worker from before
/// owners existed, which every caller may touch.
fn resolve_owner() -> String {
    std::env::var("A2A_OWNER")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("anon-{}", nix::unistd::getuid().as_raw()))
}

/// Hand a laid-out, genesis-written worker to the broker.
///
/// The frame carries the resolved `cwd` and the lane **as well as** the
/// directory: the broker re-validates both against its own allowed roots
/// (spec §5 — the socket must not out-privilege the CLI), and it cannot do that
/// from a path alone.
///
/// A refusal here is the same failure boundary a failed `fork` is on the exec
/// path: the worker is already durable, so the reason is recorded
/// (`failed{never_started}`) rather than deleted.
fn attach_to_broker(
    mut client: BrokerClient,
    log: &mut EventLog,
    wdir: &Path,
    snap: WorkerSnapshot,
    lane: Lane,
    cwd: &Path,
) -> Result<WorkerSnapshot, AppError> {
    let answer = client.request(
        "spawn",
        json!({"dir": wdir, "cwd": cwd, "lane": lane.as_str()}),
    );
    let accepted = match answer {
        Ok(v) => v,
        Err(e) => {
            let _ = append_and_fold(
                log,
                wdir,
                snap,
                EventKind::StateChange,
                json!({"fsm": "ProcessExited", "code": null,
                       "detail": redact::redact_egress(&format!("{}: {}", e.code, e.message))}),
                Utc::now(),
            );
            return Err(e);
        }
    };
    // The broker answers with the snapshot it registered. It is the same file
    // this process just wrote, read back through the owner of the worker — so
    // it, not our copy, is what gets reported.
    Ok(serde_json::from_value(accepted).unwrap_or(snap))
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
    fn the_transport_gate_parses_all_three_and_refuses_only_the_unknown() {
        assert_eq!(transport_gate(None).unwrap(), Requested::Auto);
        assert_eq!(transport_gate(Some("auto")).unwrap(), Requested::Auto);
        assert_eq!(transport_gate(Some("exec")).unwrap(), Requested::Exec);
        assert_eq!(
            transport_gate(Some("app-server")).unwrap(),
            Requested::AppServer
        );
        // The one static transport failure left: a value no build can offer.
        let err = transport_gate(Some("carrier-pigeon")).unwrap_err();
        assert_eq!(err.code, "TRANSPORT_UNKNOWN");
        assert_eq!(err.exit, ExitCode::Precondition);
    }

    /// `requested_transport` always answers "what was this worker asked to
    /// be" — the explicit value when there was one, the resolved one otherwise.
    #[test]
    fn the_recorded_request_is_explicit_or_resolved() {
        assert_eq!(
            Requested::Auto.recorded(Transport::AppServer),
            Transport::AppServer
        );
        assert_eq!(Requested::Auto.recorded(Transport::Exec), Transport::Exec);
        assert_eq!(
            Requested::AppServer.recorded(Transport::Exec),
            Transport::AppServer
        );
        assert_eq!(
            Requested::Exec.recorded(Transport::AppServer),
            Transport::Exec
        );
        assert_eq!(Requested::Auto.selection(), "auto");
        assert_eq!(Requested::Exec.selection(), "explicit");
        assert_eq!(Requested::AppServer.selection(), "explicit");
    }

    /// An explicitly-named transport is never downgraded — with the socket
    /// switched off, `app-server` is a reachability failure (exit 7), while
    /// `auto` and `exec` are the pure Phase-1 path.
    #[test]
    fn the_disable_seam_short_circuits_selection_without_touching_a_socket() {
        let tmp = tempfile::tempdir().unwrap();
        // The home is empty: any attempt to reach a broker would autostart one,
        // and the assertions below would see its socket.
        temp_env(|| {
            assert!(matches!(
                select_transport(tmp.path(), Requested::Auto).unwrap(),
                Selected::Exec { degraded: None }
            ));
            assert!(matches!(
                select_transport(tmp.path(), Requested::Exec).unwrap(),
                Selected::Exec { degraded: None }
            ));
            let Err(err) = select_transport(tmp.path(), Requested::AppServer) else {
                panic!("an explicit app-server transport cannot resolve with the socket off");
            };
            assert_eq!(err.exit, ExitCode::BrokerUnavailable);
            assert!(err.remedy.is_some());
        });
        assert!(
            !tmp.path().join(crate::broker::BROKER_SOCK).exists(),
            "the disabled path never opened a socket"
        );
    }

    /// `A2A_BROKER_DISABLE=1` for the duration of `f`, restored afterwards.
    /// The environment is process-wide, so this test does its own scoping.
    fn temp_env(f: impl FnOnce()) {
        let key = brokerclient::DISABLE_ENV;
        let previous = std::env::var_os(key);
        std::env::set_var(key, "1");
        f();
        match previous {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
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
