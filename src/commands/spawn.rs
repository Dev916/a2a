//! `a2a spawn` — the one command that brings a worker into existence
//! (spec §4.7, §4.9, §5).
//!
//! The order of the gates below is the contract, not an implementation
//! detail. Everything that can refuse the spawn runs **before** a single byte
//! is written under `$A2A_HOME/workers/`, so a refused spawn leaves no litter
//! and no half-worker for `ls`/`gc` to trip over:
//!
//! ```text
//! arg parse → cwd resolve → brief parse+validate → secret scan (brief + reason)
//!   → ELEVATION GATE (lane permission + --reason, spec §5)
//!   → transport selection (elevation forces exec)
//!   → schema resolution (--schema > brief result_schema > impl-lane template)
//!   → id allocation
//!   → AUDIT APPEND ($A2A_HOME/audit.jsonl, elevated spawns only)
//!   → worker dir tree → context staging
//!   → WORKTREE CREATE (+ .git/info/exclude)
//!   → admission → genesis → spawn/attach
//! ```
//!
//! Two positions in there are deliberate and pinned. The **elevation gate** runs
//! *after* the brief parse, because `--reason` is ingress-scanned by the same
//! [`redact::scan_text`] pass the brief is, and *before* transport selection,
//! because an elevation forces the exec transport (spec §5 forbids the broker
//! socket from carrying one). The **audit append precedes the worker dir**: an
//! audit line for a spawn that then failed is the safe direction, and a spawn
//! that ran with no line is not.
//!
//! **The worktree is created here, CLI-side, and never by the broker**
//! (spec §4.8). Its two halves sit on opposite sides of the first write for a
//! reason: everything that can *refuse* — a cwd that is not a git work tree
//! under an explicit `--worktree`, a `--base-ref` that names no commit, a repo
//! with no commit to branch from — is decided in the gate, while the tree itself
//! cannot be created until the id exists, because the id is its name. A failure
//! in that window removes the worker dir *and* the half-made tree: it is the
//! pre-genesis boundary, and the rule there is that nothing is left behind.
//! From the create onwards the worker's `cwd` **is** the worktree — which is how
//! `codex -C` and the broker's spawn frame both land in it without either of
//! them knowing worktrees exist.
//!
//! **Admission is asked before genesis and answered by the broker** (spec §4.9),
//! because the answer is part of genesis: a worker born at the cap is born
//! `queued`, and the fold reads that from the spawn row alone. It sits after the
//! layout and before the WAL, so its two refusals (`QUEUE_FULL`, `AT_CAPACITY`)
//! land on the pre-genesis side of the failure boundary below — nothing is left
//! behind — while a "wait" answer produces a worker that is real, readable, and
//! doing nothing until a slot frees.
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
use crate::commands::owner;
use crate::config::{self, Lane, LaneConfig, SandboxMode};
use crate::events::{EventKind, Transport};
use crate::exec::{self, SpawnSpec};
use crate::fold::append_and_fold;
use crate::home::{create_dir_0700, write_new_0600, A2aHome};
use crate::ids;
use crate::output::{AppError, ExitCode};
use crate::redact;
use crate::state::{WorkerSnapshot, WorktreeRef};
use crate::store::EventLog;
use crate::worktree;
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Total budget for everything staged into `context/` (spec §5 keeps the
/// worker's read-side context curated, not a repo dump).
const MAX_CONTEXT_BYTES: u64 = 10 * 1024 * 1024;

/// The impl lane's default result schema (spec §4.9): devloop's
/// `sample-task-result.json` shape, so a devloop-dispatched worker's result is
/// parsed by devloop step 4.3.4 without an adapter. Compiled in, because a
/// worker's schema must not depend on the a2a checkout still being where it was
/// at spawn.
///
/// The bytes now live in [`crate::templates`] with every other compiled-in
/// artifact; this alias stays because it is the name the schema's own tests and
/// the rest of this module have always used.
pub const IMPL_RESULT_SCHEMA: &str = crate::templates::IMPL_RESULT_SCHEMA;

/// Where the chosen schema is materialized inside the worker directory.
pub const WORKER_SCHEMA_FILE: &str = "result-schema.json";

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

/// Where a respawned worker came from (spec §4.9).
///
/// Carried through the spawn path rather than patched on afterwards, because
/// the lineage belongs in *genesis*: a worker whose parent only `state.json`
/// knew about would lose its chain to the first rebuild, and the attempt guard
/// reads that chain to decide whether a fourth try is a retry or a quarantine.
#[derive(Clone, Debug)]
pub struct Lineage {
    pub parent_id: String,
    pub attempt: u32,
}

/// The ordinary spawn: no parent, and therefore a genesis row of exactly the
/// shape Phase 1 wrote.
pub fn run(
    home: &A2aHome,
    args: &SpawnArgs,
    json_mode: bool,
    quiet: bool,
) -> Result<Value, AppError> {
    run_with_lineage(home, args, None, json_mode, quiet)
}

/// [`run`], plus the retry lineage `a2a respawn` supplies.
///
/// One path, deliberately: a respawned worker goes through the same gates, the
/// same elevation check, the same admission door and the same worktree defaults
/// as any other spawn. A second, parallel spawn path is how a retry quietly
/// stops obeying the rules the first one enforces.
pub fn run_with_lineage(
    home: &A2aHome,
    args: &SpawnArgs,
    lineage: Option<Lineage>,
    json_mode: bool,
    quiet: bool,
) -> Result<Value, AppError> {
    // `--template <lane>`: print the scaffold, create nothing.
    //
    // FIRST, deliberately — before the config load, before every gate, and
    // long before [`select_transport`]. Printing a compiled-in document is not
    // a spawn: it must not read a config it will not use, must not leave a
    // worker directory behind, and above all must not autostart a broker to
    // answer a question that has never left this process.
    if let Some(lane) = args.template {
        return Ok(json!(crate::templates::brief_scaffold(lane)));
    }
    let (cfg, _origins, warnings) = config::load(home)?;
    let warn = |msg: &str| {
        if !quiet {
            eprintln!("a2a: warning: {msg}");
        }
    };
    for w in &warnings {
        warn(w);
    }

    // ---- gates: nothing below this block has touched the filesystem --------
    let requested = transport_gate(args.transport.as_deref())?;

    let lane = args.require_lane();
    let lane_cfg = cfg
        .lanes
        .get(&lane)
        .cloned()
        .expect("config::load seeds every lane");

    let cwd_plan = resolve_cwd(lane, args.cwd.as_deref(), &cfg.allowed_roots)?;
    let wt_gate = worktree_gate(args, &lane_cfg, &cwd_plan, &warn)?;

    let brief_path = args.require_brief();
    let brief_text = std::fs::read_to_string(brief_path).map_err(|e| {
        AppError::new(
            "BRIEF_UNREADABLE",
            ExitCode::Precondition,
            format!("--brief {}: {e}", brief_path.display()),
        )
    })?;
    let parsed = parse_and_validate_brief(&brief_text, brief_path, lane, &warn)?;
    scan_brief(&brief_text, brief_path)?;
    // The reason is INGRESS, exactly like the brief: it is operator text that
    // this process is about to write to two files, so it is scanned on the same
    // pass and refused on the same terms.
    scan_reason(args.reason.as_deref())?;

    // ---- the §5 elevation gate --------------------------------------------
    //
    // After the scan (see above) and before transport selection: an elevation
    // forces exec, so the answer here decides which path the next gate takes.
    let (sandbox, elevation_reason) = elevation_gate(
        lane,
        &lane_cfg,
        args.danger_full_access,
        args.reason.as_deref(),
        requested,
        &warn,
    )?;

    // The result schema, decided HERE with the other gates even though the copy
    // lands at layout time: a `--schema` — or a brief `result_schema` — naming a
    // file that is not there is a static precondition (exit 9), and a static
    // refusal must not first autostart a broker it is never going to use.
    let _ = plan_schema(lane, args.schema.as_deref(), brief_path, &parsed)?;

    // The last gate: which transport will own this worker. A refusal here still
    // leaves nothing behind, and the answer is what the genesis event records.
    //
    // An ELEVATED spawn never reaches [`select_transport`] at all: spec §5 says
    // the wire cannot request privilege, and Phase 4 may not teach the broker a
    // per-spawn elevation, so the elevation *is* the transport decision. The
    // short-circuit is why an elevated spawn performs no socket syscall — it
    // cannot autostart a daemon it is then forbidden to use.
    let mut selected = match &elevation_reason {
        Some(_) => Selected::Exec { degraded: None },
        None => select_transport(home.root(), requested)?,
    };
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
    // Admission control is the BROKER's (spec §4.9). An exec spawn takes no
    // slot and cannot be queued: its process is its own, and counting it
    // against a cap the broker cannot release is deferred to Phase 3.
    if args.no_queue && matches!(selected, Selected::Exec { .. }) {
        warn("--no-queue has no effect on the exec transport: there is no admission queue to refuse");
    }

    // The turn budget (spec §4.9). The lane owns the ceiling; the brief may only
    // lower it, and asking for more is a clamp plus a warning rather than a
    // refusal — a brief is a request, not a grant.
    let (max_turns, clamp) = effective_max_turns(lane, lane_cfg.max_turns, parsed.max_turns);
    if let Some(msg) = &clamp {
        warn(msg);
    }

    let model = args.model.clone().unwrap_or_else(|| lane_cfg.model.clone());
    let effort = args.effort.clone().unwrap_or_else(|| lane_cfg.effort.clone());
    let brief_sha = brief::sha256_hex(&brief_text);
    let owner = owner::resolve(args.owner.as_deref());
    let now = Utc::now();
    let id = allocate_id(home, lane, now)?;

    // ---- the audit line (spec §5) -----------------------------------------
    //
    // After the id, because the record names the worker, and BEFORE the worker
    // dir, because the safe direction of the two failure modes is asymmetric: an
    // audit line for a spawn that then failed is a question an operator can
    // answer, and an elevated worker that ran with no line is not.
    if let Some(reason) = &elevation_reason {
        append_audit(
            home,
            &json!({
                "ts": now.to_rfc3339(),
                "id": id,
                "lane": lane.as_str(),
                "reason": reason,
                "owner": owner,
            }),
        )?;
    }

    // ---- first write ------------------------------------------------------
    let wdir = home.worker_dir(&id);
    create_dir_0700(&wdir).map_err(|e| io_err("creating the worker dir", e))?;
    let (schema_path, schema_source) = match lay_out_worker(&wdir, args, &brief_text, &parsed) {
        Ok(schema) => schema,
        Err(e) => {
            // Pre-genesis: this id never became a worker, so leave nothing behind.
            let _ = std::fs::remove_dir_all(&wdir);
            return Err(e);
        }
    };

    let cwd = match cwd_plan {
        CwdPlan::Fixed(p) => p,
        CwdPlan::Scratch => {
            let scratch = wdir.join("scratch");
            std::fs::canonicalize(&scratch).unwrap_or(scratch)
        }
    };

    // ---- the worker's own tree (spec §4.8) ---------------------------------
    //
    // After the id (which names the branch and the directory) and before
    // genesis (which records it), so the fold is lossless from the first row.
    // Still pre-genesis: a failure here takes the worker dir AND the half-made
    // tree with it.
    let worktree = match wt_gate.create_from.as_deref() {
        Some(base) => match worktree::create(home, &id, lane, &cwd, Some(base)) {
            Ok(wt) => Some(wt),
            Err(e) => {
                discard_pre_genesis(home, &wdir, &id, worktree::repo_root(&cwd).as_deref());
                return Err(e);
            }
        },
        None => None,
    };
    let cwd = match &worktree {
        // The worktree IS the workspace from here on: `snap.cwd` is what
        // `exec::build_argv` turns into `-C`, and what the broker's spawn frame
        // carries as the worker's cwd.
        Some(wt) => {
            // Belt-and-braces (spec §4.8), and never fatal: a missing exclude
            // line has never made a worktree wrong.
            if let Err(e) = worktree::add_to_info_exclude(&wt.repo, &wt.path) {
                warn(&format!(
                    "could not add {} to the repo's info/exclude ({}): {}",
                    wt.path.display(),
                    e.code,
                    e.message
                ));
            }
            wt.path.clone()
        }
        None => cwd,
    };

    // ---- admission: the last thing that can refuse, and the first thing
    //      genesis has to record (spec §4.9) ---------------------------------
    //
    // Asked BEFORE genesis because the answer is *part* of genesis: a worker
    // born at the cap is born `queued`, and the fold reads that from the spawn
    // row alone. Still pre-genesis, so a refusal leaves nothing behind — the
    // directory laid out above is removed, exactly as any other failure in this
    // window is.
    let queued = match &mut selected {
        Selected::AppServer(client) => {
            match request_admission(client, &wdir, &cwd, lane, args.no_queue) {
                Ok(admitted) => admitted,
                Err(e) => {
                    discard_pre_genesis(
                        home,
                        &wdir,
                        &id,
                        worktree.as_ref().map(|w| w.repo.as_path()),
                    );
                    return Err(e);
                }
            }
        }
        Selected::Exec { .. } => false,
    };

    // ---- genesis: from here the worker exists and is readable --------------
    let mut snap = WorkerSnapshot::blank(&id, &cwd);
    snap.writable_roots = vec![wdir.join("outbox")];
    // The worker's OWN copy, never the caller's path: the schema has to outlive
    // the brief moving, the flag's file being edited, and this repo being
    // checked out somewhere else.
    snap.schema_path = schema_path.clone();
    snap.elevation_reason = elevation_reason.clone();
    // The diff anchor `a2a verify` needs — recorded whether or not there is a
    // worktree, and equal to the worktree's own base ref when there is one.
    snap.base_ref = wt_gate.base_ref.clone();
    snap.worktree = worktree.as_ref().map(|w| WorktreeRef {
        path: w.path.clone(),
        branch: w.branch.clone(),
        base_ref: w.base_ref.clone(),
        repo: w.repo.clone(),
    });
    let mut log = EventLog::open(&wdir).map_err(|e| io_err("opening events.jsonl", e))?;
    let mut genesis = json!({
            "fsm": "Spawn",
            "id": id,
            "lane": lane.as_str(),
            "cwd": cwd,
            "brief_sha": brief_sha,
            "sandbox": sandbox.as_str(),
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
            "schema_path": schema_path,
            // The EFFECTIVE turn budget, in genesis for the same reason the
            // model and the sandbox are: it is what this worker was judged
            // against, and an operator editing the lane table afterwards must
            // not retroactively change the verdict on a worker in flight.
            "max_turns": max_turns,
            "created_at": now.to_rfc3339(),
    });
    // WHICH rung of the precedence table produced that schema. In genesis, not
    // on the snapshot: a snapshot-only write is swallowed by a rebuild from the
    // WAL, and "this worker was judged against the lane's shipped default"
    // is exactly the fact an operator needs when the default later changes.
    if schema_path.is_some() {
        genesis["schema_source"] = json!(schema_source);
    }
    // The elevation, in genesis for the same reason the worktree is: a worker
    // that ran with full access must still say so after `state.json` is gone.
    // The `transport_selection` OVERRIDE goes here rather than on the snapshot
    // because the genesis fold is applied over the in-memory snapshot — a
    // post-fold insert would be overwritten now and lost on a rebuild later.
    // `Requested::selection()` still answers only `auto`/`explicit`; the third
    // value is a call-site override, not a third enum arm.
    if let Some(reason) = &elevation_reason {
        genesis["elevation_reason"] = json!(reason);
        if requested == Requested::Auto {
            genesis["transport_selection"] = json!("elevation");
        }
    }
    // The worktree, in genesis rather than only on the snapshot: `state.json` is
    // a derived checkpoint, and a worktree only `state.json` knew about would be
    // lost by a rebuild from zero — leaving a tree on disk that no `gc` could
    // ever address. All four keys or none (the fold refuses a partial object).
    if let Some(wt) = &worktree {
        genesis["worktree_path"] = json!(wt.path);
        genesis["worktree_branch"] = json!(wt.branch);
        genesis["worktree_base_ref"] = json!(wt.base_ref);
        genesis["worktree_repo"] = json!(wt.repo);
    }
    if let Some(base) = &wt_gate.base_ref {
        genesis["base_ref"] = json!(base);
    }
    // Why a worker the lane would have given a tree has none — same reasoning:
    // written where a rebuild can still read it (it reaches `snap.extra`).
    if let Some(reason) = wt_gate.skipped {
        genesis["worktree_skipped"] = json!(reason);
    }
    // The admission answer, and ONLY when it is "wait": genesis stays the
    // Phase-1 shape for every worker that was admitted straight away, and the
    // fold reads an absent marker exactly as it reads `false`.
    if queued {
        genesis["queued"] = json!(true);
    }
    // The retry lineage (spec §4.9), and ONLY when there is one: a plain
    // spawn's genesis keeps its exact Phase-1/2 shape, and the fold reads an
    // absent `attempt` exactly as it reads `1`.
    if let Some(lineage) = &lineage {
        genesis["parent_id"] = json!(lineage.parent_id);
        genesis["attempt"] = json!(lineage.attempt);
    }
    let mut snap = append_and_fold(
        &mut log,
        &wdir,
        snap,
        EventKind::Spawned,
        genesis,
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

    // The tree a caller can `git -C` into, in the same four fields
    // `status --json` reports — `null` for a worker that has none.
    let worktree_payload = json!(snap.worktree);
    // ` on a2a/<lane>/<id>`, appended to the human line: the branch IS the
    // deliverable, so the one place a human sees the spawn should name it.
    let on_branch = worktree
        .as_ref()
        .map(|w| format!(" on {}", w.branch))
        .unwrap_or_default();

    // ---- hand the worker to its transport ---------------------------------
    if let Selected::AppServer(client) = selected {
        let snap = attach_to_broker(client, &mut log, &wdir, snap, lane, &cwd)?;
        if !quiet {
            match snap.queue_position {
                Some(position) => eprintln!(
                    "a2a: queued {id} ({lane} lane, app-server) at position {position} — \
                     the broker is at its admission cap"
                ),
                None => eprintln!(
                    "a2a: spawned {id} ({lane} lane, app-server) in {}{on_branch}",
                    cwd.display()
                ),
            }
        }
        return Ok(if json_mode {
            let mut payload = json!({
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
                "sandbox": sandbox.as_str(),
                "network": lane_cfg.network,
                "model": model,
                "effort": effort,
                // `null` unless this worker is waiting: the 1-based place in the
                // admission queue (spec §4.9).
                "queue_position": snap.queue_position,
                "worktree": worktree_payload,
                "deadline_at": snap.deadline_at,
            });
            add_lineage(&mut payload, lineage.as_ref());
            payload
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
        sandbox,
        network: lane_cfg.network,
        model: &model,
        effort: &effort,
        schema_path: schema_path.as_deref(),
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
        eprintln!(
            "a2a: spawned {id} ({lane} lane) in {}{on_branch}",
            cwd.display()
        );
    }
    // stdout carries the machine payload: the envelope's `data` under --json,
    // the bare id otherwise.
    Ok(if json_mode {
        let mut payload = json!({
            "id": id,
            "lane": lane.as_str(),
            "state": snap.state.as_str(),
            "transport": Transport::Exec,
            "epoch": snap.epoch,
            "pid": spawned.pid,
            "cwd": cwd,
            "worker_dir": wdir,
            "brief_sha": brief_sha,
            "sandbox": sandbox.as_str(),
            "network": lane_cfg.network,
            "model": model,
            "effort": effort,
            "worktree": worktree_payload,
            "deadline_at": snap.deadline_at,
        });
        add_lineage(&mut payload, lineage.as_ref());
        payload
    } else {
        Value::String(id)
    })
}

/// The two lineage keys, added to a spawn payload ONLY when this spawn is a
/// retry — so `a2a spawn --json`'s shape is untouched for everybody else.
fn add_lineage(payload: &mut Value, lineage: Option<&Lineage>) {
    let (Some(lineage), Some(obj)) = (lineage, payload.as_object_mut()) else {
        return;
    };
    obj.insert("parent_id".to_string(), json!(lineage.parent_id));
    obj.insert("attempt".to_string(), json!(lineage.attempt));
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

/// Ask the broker whether this worker may run yet (spec §4.9).
///
/// Returns `true` when the answer is "wait" — which is the value genesis
/// records, and therefore the value the fold turns into `queued`. The two
/// refusals (`QUEUE_FULL`, `AT_CAPACITY`) come back as typed errors with their
/// exit code and their `data` intact, because an orchestrator meeting a full
/// queue branches on `retry_after_secs` rather than on the sentence.
///
/// A broker from before admission control existed answers `UNKNOWN_OP`. That is
/// a version skew the CLI does not have to lose a spawn over: an uncapped
/// broker admits everything anyway, so the spawn proceeds unqueued.
fn request_admission(
    client: &mut BrokerClient,
    wdir: &Path,
    cwd: &Path,
    lane: Lane,
    no_queue: bool,
) -> Result<bool, AppError> {
    let answer = client.request(
        "admit",
        json!({"dir": wdir, "cwd": cwd, "lane": lane.as_str(), "no_queue": no_queue}),
    );
    match answer {
        Ok(v) => Ok(v.get("decision").and_then(Value::as_str) == Some("queue")),
        Err(e) if e.code == crate::broker::wire::ERR_UNKNOWN_OP => Ok(false),
        Err(e) => Err(e),
    }
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

/// The §5 elevation gate. Returns the sandbox the worker will actually run
/// under, plus the audited reason when this spawn is an elevation.
///
/// Three things must line up before a worker gets `danger-full-access`, and
/// **no two of them are enough**:
///
/// * the **permission** — `allow_elevation = true` on the lane, which only an
///   operator editing config (or the matching `A2A_LANES_*` env var) can grant;
/// * the **intent** — `--danger-full-access` on *this* spawn, because a lane
///   configured for full access is a standing capability and elevation is a
///   per-spawn act (spec §5). A config that elevated silently would make every
///   future spawn on that lane an elevation nobody typed;
/// * the **reason** — free text, recorded verbatim in `audit.jsonl` and on the
///   worker, because an audit trail of *that it happened* without *why* is not
///   an audit trail.
///
/// Every refusal is a static precondition (exit 9) decided before the first
/// byte is written, so a refused elevation leaves no worker, no worktree and no
/// audit line.
fn elevation_gate(
    lane: Lane,
    lane_cfg: &LaneConfig,
    danger_full_access: bool,
    reason: Option<&str>,
    requested: Requested,
    warn: &dyn Fn(&str),
) -> Result<(SandboxMode, Option<String>), AppError> {
    if !danger_full_access {
        // A reason with nothing to explain is not an error — but it is never
        // silent either: the caller plainly *meant* to elevate.
        if reason.is_some() {
            warn("--reason has no effect without --danger-full-access");
        }
        if lane_cfg.sandbox == SandboxMode::DangerFullAccess {
            return Err(if lane_cfg.allow_elevation {
                // The permission is there; the per-spawn intent is not.
                AppError::new(
                    "ELEVATION_FLAG_REQUIRED",
                    ExitCode::Precondition,
                    format!(
                        "lane `{lane}` is configured `sandbox = \"danger-full-access\"`, but \
                         elevation is per-spawn intent (spec §5): a config alone never elevates"
                    ),
                )
                .remedy(
                    "re-run with `--danger-full-access --reason \"<why this needs full access>\"`",
                )
            } else {
                not_permitted(lane)
            });
        }
        return Ok((lane_cfg.sandbox, None));
    }

    // Intent given. Permission first: the flag can never grant itself one.
    if !lane_cfg.allow_elevation {
        return Err(not_permitted(lane));
    }
    let reason = reason.map(str::trim).filter(|r| !r.is_empty());
    let Some(reason) = reason else {
        return Err(AppError::new(
            "ELEVATION_REASON_REQUIRED",
            ExitCode::Precondition,
            format!(
                "--danger-full-access on lane `{lane}` needs a reason: every elevation is \
                 recorded in $A2A_HOME/audit.jsonl, and a record with no reason is not one"
            ),
        )
        .remedy("re-run with --reason \"<why this needs full access>\""));
    };
    // Spec §5: the wire cannot request privilege, and Phase 4 does not teach the
    // broker a per-spawn elevation — so an elevated spawn is an exec spawn, and
    // a caller who *named* the socket is told rather than quietly re-routed.
    if requested == Requested::AppServer {
        return Err(AppError::new(
            "ELEVATION_EXEC_ONLY",
            ExitCode::Precondition,
            "an elevated spawn runs on the exec transport: spec §5 forbids the broker socket \
             from carrying an elevation, and the daemon resolves its own sandbox server-side",
        )
        .remedy(
            "drop --transport app-server: spec §5 forbids the broker socket from carrying an \
             elevation",
        ));
    }
    Ok((SandboxMode::DangerFullAccess, Some(reason.to_string())))
}

/// The one refusal both un-permitted paths share: the lane has no `allow_elevation`.
fn not_permitted(lane: Lane) -> AppError {
    AppError::new(
        "ELEVATION_NOT_PERMITTED",
        ExitCode::Precondition,
        format!(
            "lane `{lane}` may not run with `danger-full-access`: elevation needs a lane \
             permission as well as the per-spawn flag (spec §5)"
        ),
    )
    .remedy(format!(
        "set `allow_elevation = true` under [lanes.{lane}] in $A2A_HOME/config.toml and re-run \
         with --danger-full-access --reason \"<why>\""
    ))
}

/// Ingress scan for `--reason` (spec §5) — the same pass, and the same refusal
/// shape, the brief gets. The reason is about to be written to `audit.jsonl`
/// *and* to the WAL, so a credential pasted into it would land in two files a
/// refusal could no longer take back.
fn scan_reason(reason: Option<&str>) -> Result<(), AppError> {
    let Some(reason) = reason else {
        return Ok(());
    };
    let hits = redact::scan_text(reason);
    if hits.is_empty() {
        return Ok(());
    }
    let rules = hits
        .iter()
        .take(5)
        .map(|h| h.rule)
        .collect::<Vec<_>>()
        .join(", ");
    Err(AppError::new(
        "SECRET_IN_REASON",
        ExitCode::Precondition,
        format!(
            "--reason contains {} secret-shaped value(s) ({rules}) — refusing to spawn \
             (nothing was written)",
            hits.len()
        ),
    )
    .remedy("remove the credential from --reason; it is recorded verbatim in the audit log"))
}

/// `$A2A_HOME/audit.jsonl` — the append-only record of every elevation.
pub const AUDIT_FILE: &str = "audit.jsonl";

/// Append one line to `$A2A_HOME/audit.jsonl` (0600, `O_APPEND`, fsync'd).
///
/// Same append-before-act discipline as `gc`'s `index.jsonl` and the WAL: the
/// line is complete and durable before the thing it records is allowed to
/// happen. 0600 because the reason is operator text about a privileged
/// operation, and this file outlives every worker it names.
pub fn append_audit(home: &A2aHome, entry: &Value) -> Result<(), AppError> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = home.root().join(AUDIT_FILE);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&path)
        .map_err(|e| io_err("opening audit.jsonl", e))?;
    let mut line = serde_json::to_vec(entry).map_err(|e| {
        AppError::new(
            "IO",
            ExitCode::Internal,
            format!("serializing an audit entry: {e}"),
        )
    })?;
    line.push(b'\n');
    file.write_all(&line)
        .map_err(|e| io_err("appending to audit.jsonl", e))?;
    file.sync_data()
        .map_err(|e| io_err("syncing audit.jsonl", e))?;
    Ok(())
}

/// The worktree decision, taken before the first byte is written.
///
/// Everything here is read-only on the workspace: the git probes are
/// `rev-parse`, and the only thing they can do is refuse.
#[derive(Debug, Default)]
struct WorktreeGate {
    /// The 40-hex commit to cut the branch from — `Some` exactly when a
    /// worktree is to be created.
    create_from: Option<String>,
    /// The diff anchor to record, worktree or not (spec §4.5): the target
    /// repo's `HEAD` at spawn, or `--base-ref` resolved.
    base_ref: Option<String>,
    /// Why a lane that defaults to a worktree did not get one.
    skipped: Option<&'static str>,
}

/// The turn budget this spawn will be judged against (spec §4.9), and the
/// warning to print when the brief asked for more than the lane allows.
///
/// **The brief may only LOWER it.** A budget is the operator's protection
/// against a worker spending an account's headroom on its own confusion, and a
/// brief is written by whoever dispatched the worker — often the worker's own
/// orchestrator. A brief that could raise its own ceiling would be a cap that
/// only stops the callers who were not going to exceed it anyway. Asking for
/// less is honoured silently: that is a caller volunteering a tighter bound.
///
/// The returned message carries **no** `a2a: warning: ` prefix — `run`'s `warn`
/// closure adds that, and a string that carried it would print it twice.
///
/// Takes the [`Lane`] as well as its ceiling, unlike the two-argument sketch in
/// the plan: the pinned wording names the lane ("the impl lane's 40"), and a
/// function that cannot see the lane cannot produce it.
pub fn effective_max_turns(
    lane: Lane,
    lane_max: u64,
    brief_max: Option<u32>,
) -> (u64, Option<String>) {
    let Some(asked) = brief_max.map(u64::from) else {
        return (lane_max, None);
    };
    if asked > lane_max {
        return (
            lane_max,
            Some(format!(
                "brief max_turns {asked} exceeds the {lane} lane's {lane_max} — clamped to \
                 {lane_max} (a brief may only lower the turn budget)"
            )),
        );
    }
    (asked, None)
}

/// `--worktree` ⇒ yes; `--no-worktree` ⇒ no; neither ⇒ the lane default.
///
/// The two-rule split on a cwd that is not a git work tree is deliberate and
/// pinned: an **explicit** `--worktree` is a request, and a request that cannot
/// be honoured is an error; a **lane default** is a policy, and refusing every
/// legitimate non-repo impl worker in its name would be worse than proceeding
/// without one. The skip is recorded (genesis, then `snap.extra`) rather than
/// left as a stderr line nobody keeps.
fn worktree_gate(
    args: &SpawnArgs,
    lane_cfg: &LaneConfig,
    cwd_plan: &CwdPlan,
    warn: &dyn Fn(&str),
) -> Result<WorktreeGate, AppError> {
    if args.no_worktree {
        warn("--no-worktree: spawning directly in --cwd");
    }
    let wanted = args.worktree || (!args.no_worktree && lane_cfg.worktree_default);
    // The research lane's cwd is inside the worker dir and does not exist yet;
    // it is never a git work tree, and `--worktree` on it is the same refusal a
    // non-repo `--cwd` gets.
    let target = match cwd_plan {
        CwdPlan::Fixed(p) => Some(p.as_path()),
        CwdPlan::Scratch => None,
    };
    let repo = target.and_then(worktree::repo_root);
    let Some(repo) = repo else {
        if args.worktree {
            let shown = target
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "the research lane's scratch dir".to_string());
            return Err(AppError::new(
                "WORKTREE_NOT_A_REPO",
                ExitCode::Precondition,
                format!("--worktree: {shown} is not inside a git work tree"),
            )
            .remedy("run `git init` in --cwd, or pass --no-worktree to spawn directly in it"));
        }
        if wanted {
            warn(&format!(
                "the {} lane defaults to a worktree, but {} is not inside a git work tree — \
                 spawning directly in it",
                args.require_lane(),
                target.map(|p| p.display().to_string()).unwrap_or_default()
            ));
            return Ok(WorktreeGate {
                skipped: Some("not-a-git-repo"),
                ..WorktreeGate::default()
            });
        }
        return Ok(WorktreeGate::default());
    };
    // Pinned here, in the gate, so an unresolvable `--base-ref` and a repo with
    // no commit both refuse before anything is written — and so the commit the
    // branch is cut from is the same one the snapshot records.
    let base = match args.base_ref.as_deref() {
        Some(r) => Some(worktree::resolve_commit(&repo, r)?),
        None if wanted => Some(worktree::head_ref(&repo)?),
        // No worktree and no explicit ref: the anchor is still worth recording,
        // but an empty repo is not this spawn's problem.
        None => worktree::head_ref(&repo).ok(),
    };
    Ok(WorktreeGate {
        create_from: base.clone().filter(|_| wanted),
        base_ref: base,
        skipped: None,
    })
}

/// Which schema this worker's result will be judged against — decided before
/// anything is copied, so the decision can be taken in the gate block and acted
/// on later.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SchemaChoice {
    /// `--schema <file>`: the caller named it, so it wins outright.
    Flag(PathBuf),
    /// The brief's `result_schema`, already resolved against the brief's dir.
    Brief(PathBuf),
    /// The lane's own compiled-in template
    /// ([`crate::templates::default_result_schema`]).
    LaneDefault,
}

impl SchemaChoice {
    /// The `schema_source` label recorded in genesis.
    fn label(&self) -> &'static str {
        match self {
            SchemaChoice::Flag(_) => "flag",
            SchemaChoice::Brief(_) => "brief",
            SchemaChoice::LaneDefault => "lane-default",
        }
    }
}

/// The precedence table (spec §4.9), decided without touching the worker dir:
/// **`--schema` > the brief's `result_schema` > the lane's own template**.
///
/// The middle rung is resolved **relative to the brief's own directory** — a
/// brief is a document that travels with its schema, and `./task.schema.json`
/// in it means "beside me", not "beside whatever the operator's shell happened
/// to be in". An absolute value is used as written.
///
/// Which lanes HAVE a default is [`crate::templates::default_result_schema`]'s
/// question, not this function's — `impl` (devloop's result shape) and `review`
/// (the findings schema) are the lanes whose output another machine consumes.
/// Giving a research or general worker a schema it was never briefed for would
/// fail it at the gate for something nobody asked of it.
fn plan_schema(
    lane: Lane,
    flag: Option<&Path>,
    brief_path: &Path,
    brief: &Brief,
) -> Result<Option<SchemaChoice>, AppError> {
    if let Some(schema) = flag {
        if !schema.is_file() {
            return Err(schema_not_found(format!(
                "--schema {}: not a readable file",
                schema.display()
            )));
        }
        return Ok(Some(SchemaChoice::Flag(schema.to_path_buf())));
    }
    if let Some(declared) = brief.result_schema.as_deref().map(str::trim) {
        let raw = Path::new(declared);
        let resolved = if raw.is_absolute() {
            raw.to_path_buf()
        } else {
            // `./x` and `x` are the same neighbour; joining the leading `.`
            // through would only make every message about this path uglier.
            brief_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(raw.strip_prefix(".").unwrap_or(raw))
        };
        if !resolved.is_file() {
            return Err(schema_not_found(format!(
                "the brief's `result_schema: {declared}` resolves to {}, which is not a readable \
                 file",
                resolved.display()
            ))
            .remedy(
                "point `result_schema` at a file beside the brief (or give an absolute path), or \
                 drop the key",
            ));
        }
        return Ok(Some(SchemaChoice::Brief(resolved)));
    }
    Ok(crate::templates::default_result_schema(lane).map(|_| SchemaChoice::LaneDefault))
}

fn schema_not_found(message: String) -> AppError {
    AppError::new("SCHEMA_NOT_FOUND", ExitCode::Precondition, message)
}

/// `--schema` > brief `result_schema` (relative to the brief's dir) > the
/// lane's own default template — resolved **and materialized** as
/// `<worker>/result-schema.json` (0600).
///
/// Returns the path recorded on the snapshot, in genesis and in
/// [`SpawnSpec::schema_path`], plus the `schema_source` label. The copy is the
/// point: a worker judged against a file outside its own directory is a worker
/// whose verdict changes when somebody edits that file, or moves the repo the
/// template shipped in.
fn resolve_schema(
    wdir: &Path,
    lane: Lane,
    flag: Option<&Path>,
    brief_path: &Path,
    brief: &Brief,
) -> Result<(Option<PathBuf>, &'static str), AppError> {
    let Some(choice) = plan_schema(lane, flag, brief_path, brief)? else {
        return Ok((None, "none"));
    };
    let bytes = match &choice {
        SchemaChoice::LaneDefault => crate::templates::default_result_schema(lane)
            .expect("plan_schema only chooses LaneDefault for a lane that has one")
            .as_bytes()
            .to_vec(),
        SchemaChoice::Flag(p) | SchemaChoice::Brief(p) => std::fs::read(p).map_err(|e| {
            schema_not_found(format!("reading the result schema {}: {e}", p.display()))
        })?,
    };
    let dest = wdir.join(WORKER_SCHEMA_FILE);
    write_0600(&dest, &bytes)?;
    Ok((Some(dest), choice.label()))
}

/// Pre-genesis cleanup: this id never became a worker, so the worker dir AND
/// any half-made tree go with it (an id with no `state.json` is a ghost, and a
/// tree with no worker is one `gc` can never reach).
///
/// Best-effort by construction — the error already on its way out is the one
/// worth reporting. The BRANCH is never touched: a2a does not delete branches.
fn discard_pre_genesis(home: &A2aHome, wdir: &Path, id: &str, repo: Option<&Path>) {
    let _ = std::fs::remove_dir_all(wdir);
    let path = home.worktrees_dir().join(id);
    if !path.exists() {
        return;
    }
    if let Some(repo) = repo {
        let _ = worktree::remove(repo, &path, true);
    }
    let _ = std::fs::remove_dir_all(&path);
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
///
/// Returns the materialized result schema and its `schema_source` label: the
/// schema is a *worker file*, laid out here with the brief and the prompt, so
/// that everything the worker is judged by lives in one directory.
fn lay_out_worker(
    wdir: &Path,
    args: &SpawnArgs,
    brief_text: &str,
    parsed: &Brief,
) -> Result<(Option<PathBuf>, &'static str), AppError> {
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
    write_0600(&wdir.join("prompt.md"), prompt.as_bytes())?;
    resolve_schema(
        wdir,
        args.require_lane(),
        args.schema.as_deref(),
        args.require_brief(),
        parsed,
    )
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

    /// The clamp, in all four shapes it has — and the pinned wording, WITHOUT
    /// the `a2a: warning: ` prefix `run`'s own closure prepends.
    #[test]
    fn a_brief_may_only_lower_the_turn_budget() {
        // No ask: the lane's number, silently.
        assert_eq!(effective_max_turns(Lane::Impl, 40, None), (40, None));
        // A lower ask is honoured, silently — a caller volunteering a bound.
        assert_eq!(effective_max_turns(Lane::Impl, 40, Some(5)), (5, None));
        // Equal is not "more".
        assert_eq!(effective_max_turns(Lane::Impl, 40, Some(40)), (40, None));
        // More is clamped, and says so.
        let (effective, warning) = effective_max_turns(Lane::Impl, 40, Some(99));
        assert_eq!(effective, 40);
        assert_eq!(
            warning.as_deref(),
            Some(
                "brief max_turns 99 exceeds the impl lane's 40 — clamped to 40 \
                 (a brief may only lower the turn budget)"
            )
        );
        assert!(
            !warning.unwrap().starts_with("a2a: warning: "),
            "`run`'s warn closure adds the prefix; carrying it here would double it"
        );
        // `0` is a lane that allows no turns, and a brief cannot talk its way up.
        assert_eq!(effective_max_turns(Lane::General, 0, Some(7)).0, 0);
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

    /// The published contract with devloop (spec §4.9 + the `sample-task-result.json`
    /// shape). `required` is what `reconcile::schema_gate` actually enforces, so
    /// its exact list — and its order, which is how a human reads the file — is
    /// the pin that keeps devloop step 4.3.4 parsing without an adapter.
    #[test]
    fn the_shipped_impl_schema_pins_the_devloop_result_shape() {
        let v: Value = serde_json::from_str(IMPL_RESULT_SCHEMA).expect("the template is JSON");
        assert_eq!(v["$schema"], json!("https://json-schema.org/draft/2020-12/schema"));
        assert_eq!(v["type"], json!("object"));
        assert_eq!(v["additionalProperties"], json!(false));
        let required: Vec<&str> = v["required"]
            .as_array()
            .expect("required is an array")
            .iter()
            .map(|k| k.as_str().expect("a key name"))
            .collect();
        assert_eq!(
            required,
            [
                "status",
                "iterations",
                "summary",
                "evidence",
                "files_changed",
                "next_step_hint"
            ]
        );
        let props = &v["properties"];
        assert_eq!(props["status"]["enum"], json!(["pass", "fail", "stuck"]));
        assert_eq!(props["iterations"]["type"], json!("integer"));
        assert_eq!(props["summary"]["type"], json!("string"));
        assert_eq!(props["evidence"]["type"], json!("object"));
        // Toolkit-dependent: the four playwright keys are DOCUMENTED, not
        // required, and anything else a toolkit produces is allowed through.
        assert_eq!(props["evidence"]["additionalProperties"], json!(true));
        assert_eq!(props["evidence"].get("required"), None);
        for key in [
            "screenshots",
            "console_errors",
            "network_failures",
            "logs_excerpts",
        ] {
            assert_eq!(props["evidence"]["properties"][key]["type"], json!("array"));
        }
        assert_eq!(props["files_changed"]["type"], json!("array"));
        assert_eq!(props["files_changed"]["items"]["type"], json!("string"));
        assert_eq!(props["next_step_hint"]["type"], json!(["string", "null"]));
        // Every required key is described; `additionalProperties: false` would
        // otherwise reject a key the schema itself asks for.
        for key in &required {
            assert!(props.get(key).is_some(), "`{key}` is required but undescribed");
        }
    }

    /// The precedence table, exercised as a table: flag beats brief beats the
    /// lane default, the brief's path is brief-relative, and a general-lane
    /// worker with none of the three gets no schema at all.
    #[test]
    fn the_schema_precedence_table_resolves_flag_then_brief_then_lane_default() {
        let tmp = tempfile::tempdir().unwrap();
        let briefs = tmp.path().join("briefs");
        std::fs::create_dir_all(&briefs).unwrap();
        let brief_path = briefs.join("task.md");
        std::fs::write(&brief_path, "x").unwrap();
        // The brief's schema is a SIBLING of the brief, not of the cwd.
        let sibling = briefs.join("task.schema.json");
        std::fs::write(&sibling, r#"{"required":["verdict"]}"#).unwrap();
        let flagged = tmp.path().join("flag.schema.json");
        std::fs::write(&flagged, r#"{"required":["flagged"]}"#).unwrap();

        let with_key = Brief {
            result_schema: Some("./task.schema.json".to_string()),
            ..Brief::default()
        };
        let without = Brief::default();

        let case = |lane, flag: Option<&Path>, brief: &Brief| {
            let wdir = tempfile::tempdir().unwrap();
            let (path, source) = resolve_schema(wdir.path(), lane, flag, &brief_path, brief)
                .expect("resolution succeeds");
            let body = path
                .as_ref()
                .map(|p| std::fs::read_to_string(p).expect("the worker's own copy"));
            // Whatever was chosen, the worker's copy is the recorded path.
            if let Some(p) = &path {
                assert_eq!(p, &wdir.path().join(WORKER_SCHEMA_FILE));
            }
            (source, body)
        };

        let (source, body) = case(Lane::Impl, Some(flagged.as_path()), &with_key);
        assert_eq!(source, "flag");
        assert_eq!(body.unwrap(), r#"{"required":["flagged"]}"#);

        let (source, body) = case(Lane::Impl, None, &with_key);
        assert_eq!(source, "brief");
        assert_eq!(body.unwrap(), r#"{"required":["verdict"]}"#);

        let (source, body) = case(Lane::Impl, None, &without);
        assert_eq!(source, "lane-default");
        assert_eq!(body.unwrap(), IMPL_RESULT_SCHEMA);

        // The review lane has a default too — the findings schema — and it
        // rides this same rung.
        let (source, body) = case(Lane::Review, None, &without);
        assert_eq!(source, "lane-default");
        assert_eq!(body.unwrap(), crate::templates::REVIEW_FINDINGS_SCHEMA);

        // The prose lanes have none; they keep `schema_path: null`.
        for lane in [Lane::General, Lane::Research] {
            let (source, body) = case(lane, None, &without);
            assert_eq!(source, "none", "{lane}");
            assert!(body.is_none(), "{lane}");
        }
        // …but an explicit flag still applies to them.
        let (source, _) = case(Lane::General, Some(flagged.as_path()), &without);
        assert_eq!(source, "flag");
    }

    /// Both rungs that name a file refuse the same way when it is not there:
    /// exit 9, `SCHEMA_NOT_FOUND`, decided before anything is written.
    #[test]
    fn a_named_schema_that_is_missing_is_a_static_precondition_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let brief_path = tmp.path().join("task.md");
        std::fs::write(&brief_path, "x").unwrap();

        let err = plan_schema(
            Lane::Impl,
            Some(&tmp.path().join("no-such.json")),
            &brief_path,
            &Brief::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, "SCHEMA_NOT_FOUND");
        assert_eq!(err.exit, ExitCode::Precondition);

        let err = plan_schema(
            Lane::Impl,
            None,
            &brief_path,
            &Brief {
                result_schema: Some("./no-such.json".to_string()),
                ..Brief::default()
            },
        )
        .unwrap_err();
        assert_eq!(err.code, "SCHEMA_NOT_FOUND");
        assert_eq!(err.exit, ExitCode::Precondition);
        // The message names the RESOLVED path, since that is the one the
        // operator has to go and look at.
        assert!(
            err.message.contains(&tmp.path().join("no-such.json").display().to_string()),
            "{}",
            err.message
        );
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
