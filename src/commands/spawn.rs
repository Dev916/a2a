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
//!   → WORKTREE CREATE (+ .git/info/exclude)
//!   → admission
//!   → seed_worker: worker dir tree → context staging → genesis
//!   → spawn/attach
//! ```
//!
//! The last three steps used to interleave — the directory was laid out before
//! the tree was cut and before admission answered. They were pulled apart when
//! [`seed_worker`] was extracted so the broker's `auto_review` (spec §4.8)
//! could seed a worker through the same code, and the order that fell out is
//! the stricter one: **every refusal now happens before the first byte is
//! written**, so "a refused spawn leaves no litter" stops depending on cleanup
//! and starts being true by construction. The one exception is the research
//! lane, whose cwd lives inside the worker dir and whose `scratch` is therefore
//! created early so the path can be canonicalized at all.
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
//! `config::load` appends the implicit `$A2A_HOME/work` root as well as the
//! worktrees root, which is what lets mesh work approval spawn with
//! `--no-worktree --cwd $A2A_HOME/work/<id>/repo` without a local config edit.

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
use std::io::{IsTerminal, Write};
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

/// Everything [`seed_worker`] needs to lay a worker out and commit its genesis.
///
/// This struct exists so that the **broker** can seed a worker (spec §4.8's
/// `auto_review`) through the CLI's own layout code rather than through a
/// second, parallel implementation that would drift key by key. Every field
/// genesis records is either here or derivable from `cfg` + [`Self::lane`] +
/// the worker directory; the derivations are named in [`seed_worker`]'s body
/// and each one is a decision, not an omission.
pub struct SeedPlan {
    /// Allocated by the CALLER, because both callers need it before they seed:
    /// the CLI writes the audit line against it, and the broker asks
    /// `decide_admission` about `<home>/workers/<id>` before a byte exists.
    pub id: String,
    pub lane: Lane,
    pub cwd: PathBuf,
    pub brief_text: String,
    /// `--context` files to stage into `<worker>/context/`; empty for the
    /// auto-review worker, whose whole context is its brief.
    pub context: Vec<PathBuf>,
    /// WHICH rung of the precedence table won, not the bytes: the copy into
    /// `<worker>/result-schema.json` happens here, and `schema_source` has to
    /// record the rung. The auto-review worker passes
    /// [`SchemaChoice::LaneDefault`] so it is judged by the review lane's
    /// shipped findings schema, exactly as `a2a spawn --lane review` is.
    pub schema: Option<SchemaChoice>,
    /// The named model/effort profile selected for this dispatch, if any.
    pub profile: Option<String>,
    pub model: String,
    pub effort: String,
    pub owner: Option<String>,
    pub max_turns: u64,
    /// NOT derived from `cfg.lanes[lane].sandbox`: the §5 elevation gate is
    /// what decides this, and a worker that ran with `danger-full-access` must
    /// record the mode it actually ran with.
    pub sandbox: SandboxMode,
    /// Genesis records BOTH the resolved transport and what was asked for, plus
    /// how it was chosen (`auto` | `explicit` | `elevation`).
    pub transport: Transport,
    pub requested_transport: Transport,
    pub transport_selection: &'static str,
    pub elevation_reason: Option<String>,
    /// The tree this worker owns, already created — creating it is the CLI's
    /// job (spec §4.8) and never the broker's.
    pub worktree: Option<WorktreeRef>,
    /// The diff anchor, recorded worktree or not.
    pub base_ref: Option<String>,
    /// Why a lane that defaults to a worktree has none.
    pub worktree_skipped: Option<&'static str>,
    pub lineage: Option<Lineage>,
    /// The impl worker this one is reviewing (spec §4.8) — **not** `parent_id`:
    /// a reviewer is not a retry of anything, and conflating the two would make
    /// `max_attempts` count reviews.
    pub auto_review_of: Option<String>,
    pub queued: bool,
}

/// The ordinary spawn: no parent, and therefore a genesis row of exactly the
/// shape Phase 1 wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpawnOverrides {
    pub network: Option<bool>,
}

pub fn effective_network(lane_network: bool, overrides: &SpawnOverrides) -> bool {
    overrides.network.unwrap_or(lane_network)
}

pub fn run(
    home: &A2aHome,
    args: &SpawnArgs,
    json_mode: bool,
    quiet: bool,
) -> Result<Value, AppError> {
    run_with_overrides(
        home,
        args,
        None,
        SpawnOverrides::default(),
        json_mode,
        quiet,
    )
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
    run_with_overrides(
        home,
        args,
        lineage,
        SpawnOverrides::default(),
        json_mode,
        quiet,
    )
}

pub fn run_with_overrides(
    home: &A2aHome,
    args: &SpawnArgs,
    lineage: Option<Lineage>,
    overrides: SpawnOverrides,
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
    let (mut cfg, _origins, warnings) = config::load(home)?;
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
        .get_mut(&lane)
        .expect("config::load seeds every lane");
    lane_cfg.network = effective_network(lane_cfg.network, &overrides);
    let lane_cfg = lane_cfg.clone();
    let profile_cfg = profile_gate(&cfg, args.profile.as_deref())?;

    let cwd_plan = resolve_cwd(
        home,
        lane,
        args.cwd.as_deref(),
        &mut cfg.allowed_roots,
        json_mode,
        quiet,
    )?;
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
    let schema_choice = plan_schema(lane, args.schema.as_deref(), brief_path, &parsed)?;

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
        warn(
            "--no-queue has no effect on the exec transport: there is no admission queue to refuse",
        );
    }

    // The turn budget (spec §4.9). The lane owns the ceiling; the brief may only
    // lower it, and asking for more is a clamp plus a warning rather than a
    // refusal — a brief is a request, not a grant.
    let (max_turns, clamp) = effective_max_turns(lane, lane_cfg.max_turns, parsed.max_turns);
    if let Some(msg) = &clamp {
        warn(msg);
    }

    let model = args
        .model
        .clone()
        .or_else(|| profile_cfg.as_ref().map(|profile| profile.model.clone()))
        .unwrap_or_else(|| lane_cfg.model.clone());
    let effort = args
        .effort
        .clone()
        .or_else(|| profile_cfg.as_ref().map(|profile| profile.effort.clone()))
        .unwrap_or_else(|| lane_cfg.effort.clone());
    let profile = args.profile.clone();
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

    // ---- the worker's own directory ----------------------------------------
    //
    // Its PATH, not its contents: everything that can still refuse — the tree,
    // admission — is asked first, and the layout happens once inside
    // [`seed_worker`], which is the one seam the broker shares (spec §4.8).
    let wdir = home.worker_dir(&id);

    let cwd = match cwd_plan {
        CwdPlan::Fixed(p) => p,
        CwdPlan::Scratch => {
            // The research lane's cwd lives INSIDE the worker dir, so this one
            // directory is created before `seed_worker` lays out the rest: a
            // path that does not exist cannot be canonicalized, and an
            // uncanonicalized `/var/…` cwd is one the broker's own `check_cwd`
            // would then compare against a `/private/var/…` scratch and refuse.
            // `create_dir_0700` is idempotent, so seeding it again is a no-op.
            let scratch = wdir.join("scratch");
            if let Err(e) = create_dir_0700(&scratch) {
                discard_pre_genesis(home, &wdir, &id, None);
                return Err(io_err("creating the worker dir tree", e));
            }
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

    // ---- layout + genesis: from here the worker exists and is readable -----
    //
    // Through [`seed_worker`], the ONE seam that writes a worker's directory
    // and its genesis row — so a broker-seeded auto-review worker (spec §4.8)
    // is the same shape as this one, key for key.
    let plan = SeedPlan {
        id: id.clone(),
        lane,
        cwd: cwd.clone(),
        brief_text: brief_text.clone(),
        context: args.context.clone(),
        schema: schema_choice,
        profile: profile.clone(),
        model: model.clone(),
        effort: effort.clone(),
        owner: Some(owner.clone()),
        max_turns,
        sandbox,
        transport,
        // Spec §4.4: both are recorded. `requested` is the explicit value when
        // one was given, and the resolved one otherwise — so it always answers
        // "what was this worker asked to be", never "nothing".
        requested_transport: requested.recorded(transport),
        // `Requested::selection()` answers only `auto`/`explicit`; an elevated
        // spawn never reached `select_transport` at all, so the elevation IS
        // how the transport was chosen and says so.
        transport_selection: match (&elevation_reason, requested) {
            (Some(_), Requested::Auto) => "elevation",
            _ => requested.selection(),
        },
        elevation_reason: elevation_reason.clone(),
        worktree: worktree.as_ref().map(|w| WorktreeRef {
            path: w.path.clone(),
            branch: w.branch.clone(),
            base_ref: w.base_ref.clone(),
            repo: w.repo.clone(),
        }),
        base_ref: wt_gate.base_ref.clone(),
        worktree_skipped: wt_gate.skipped,
        lineage: lineage.clone(),
        auto_review_of: None,
        queued,
    };
    let (id, wdir, mut snap) = match seed_worker(home, &cfg, &plan, now) {
        Ok(seeded) => seeded,
        Err(e) => {
            // Still pre-genesis: this id never became a worker, so the dir AND
            // the half-made tree go with it.
            discard_pre_genesis(
                home,
                &wdir,
                &id,
                worktree.as_ref().map(|w| w.repo.as_path()),
            );
            return Err(e);
        }
    };
    let schema_path = snap.schema_path.clone();
    // Re-opened rather than handed back: `EventLog::open` resumes `next_seq`
    // from the last parseable line, so the second open is the same log at the
    // same place — and `seed_worker`'s signature stays the one the broker's
    // `spawn_blocking` can call without owning a file handle across an await.
    let mut log = EventLog::open(&wdir).map_err(|e| io_err("opening events.jsonl", e))?;

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
                "profile": profile,
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
    snap.deadline_at = Some(started + chrono::Duration::seconds(lane_cfg.task_timeout_secs as i64));
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
            "profile": profile,
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

/// Lay a worker directory out and commit its genesis — the whole of what it
/// takes to make a worker *exist*, and nothing else.
///
/// Two callers, deliberately one function: `a2a spawn` (above) and the broker's
/// `auto_review` hook (spec §4.8, `broker::core`). A broker-seeded worker's
/// directory tree and genesis row are therefore the same shape as a
/// CLI-seeded one, key for key — a second implementation would agree on the day
/// it was written and drift on every one after.
///
/// **Everything that can refuse a spawn has already run** by the time this is
/// called: the brief is parsed and scanned, the elevation gate has answered,
/// the transport is chosen, the worktree exists and admission has said yes or
/// no. What is left is writing, and the only failure it has is IO — so a
/// failure here removes the directory it created and leaves nothing behind
/// (the caller owns any worktree cleanup: it created the tree, and this
/// function is not told which repo it belongs to).
///
/// Three genesis values are DERIVED here rather than carried on [`SeedPlan`],
/// each a decision and not a drop:
///
/// * `brief_sha` — [`brief::sha256_hex`] of `plan.brief_text`; a caller-supplied
///   digest of text this function also writes is a second source of truth.
/// * `network` — `cfg.lanes[plan.lane].network`; unlike the sandbox, no gate
///   can override it, so the lane table is the only answer there could be.
/// * `writable_roots` — `<worker>/outbox`, which is the worker directory this
///   function just chose.
///
/// Sync, and takes no `&mut` anything: the broker calls it inside
/// `spawn_blocking` (spec §4.2 — no blocking work on an actor task).
pub fn seed_worker(
    home: &A2aHome,
    cfg: &config::Config,
    plan: &SeedPlan,
    now: chrono::DateTime<Utc>,
) -> Result<(String, PathBuf, WorkerSnapshot), AppError> {
    let lane_cfg = cfg
        .lanes
        .get(&plan.lane)
        .cloned()
        .expect("config::load seeds every lane");
    let wdir = home.worker_dir(&plan.id);
    create_dir_0700(&wdir).map_err(|e| io_err("creating the worker dir", e))?;
    let (schema_path, schema_source) = match lay_out_worker(&wdir, plan) {
        Ok(schema) => schema,
        Err(e) => {
            // Pre-genesis: this id never became a worker, so leave nothing behind.
            let _ = std::fs::remove_dir_all(&wdir);
            return Err(e);
        }
    };

    let mut snap = WorkerSnapshot::blank(&plan.id, &plan.cwd);
    snap.writable_roots = vec![wdir.join("outbox")];
    // The worker's OWN copy, never the caller's path: the schema has to outlive
    // the brief moving, the flag's file being edited, and this repo being
    // checked out somewhere else.
    snap.schema_path = schema_path.clone();
    snap.elevation_reason = plan.elevation_reason.clone();
    // The diff anchor `a2a verify` needs — recorded whether or not there is a
    // worktree, and equal to the worktree's own base ref when there is one.
    snap.base_ref = plan.base_ref.clone();
    snap.worktree = plan.worktree.clone();
    let mut log = EventLog::open(&wdir).map_err(|e| io_err("opening events.jsonl", e))?;
    let mut genesis = json!({
            "fsm": "Spawn",
            "id": plan.id,
            "lane": plan.lane.as_str(),
            "cwd": plan.cwd,
            "brief_sha": brief::sha256_hex(&plan.brief_text),
            "sandbox": plan.sandbox.as_str(),
            "network": lane_cfg.network,
            "model": plan.model,
            "effort": plan.effort,
            "transport": plan.transport,
            "requested_transport": plan.requested_transport,
            "transport_selection": plan.transport_selection,
            "owner": plan.owner,
            "schema_path": schema_path,
            // The EFFECTIVE turn budget, in genesis for the same reason the
            // model and the sandbox are: it is what this worker was judged
            // against, and an operator editing the lane table afterwards must
            // not retroactively change the verdict on a worker in flight.
            "max_turns": plan.max_turns,
            "created_at": now.to_rfc3339(),
    });
    // WHICH rung of the precedence table produced that schema. In genesis, not
    // on the snapshot: a snapshot-only write is swallowed by a rebuild from the
    // WAL, and "this worker was judged against the lane's shipped default"
    // is exactly the fact an operator needs when the default later changes.
    if schema_path.is_some() {
        genesis["schema_source"] = json!(schema_source);
    }
    if let Some(profile) = &plan.profile {
        genesis["profile"] = json!(profile);
    }
    // The elevation, in genesis for the same reason the worktree is: a worker
    // that ran with full access must still say so after `state.json` is gone.
    if let Some(reason) = &plan.elevation_reason {
        genesis["elevation_reason"] = json!(reason);
    }
    // The worktree, in genesis rather than only on the snapshot: `state.json` is
    // a derived checkpoint, and a worktree only `state.json` knew about would be
    // lost by a rebuild from zero — leaving a tree on disk that no `gc` could
    // ever address. All four keys or none (the fold refuses a partial object).
    if let Some(wt) = &plan.worktree {
        genesis["worktree_path"] = json!(wt.path);
        genesis["worktree_branch"] = json!(wt.branch);
        genesis["worktree_base_ref"] = json!(wt.base_ref);
        genesis["worktree_repo"] = json!(wt.repo);
    }
    if let Some(base) = &plan.base_ref {
        genesis["base_ref"] = json!(base);
    }
    // Why a worker the lane would have given a tree has none — same reasoning:
    // written where a rebuild can still read it (it reaches `snap.extra`).
    if let Some(reason) = plan.worktree_skipped {
        genesis["worktree_skipped"] = json!(reason);
    }
    // The admission answer, and ONLY when it is "wait": genesis stays the
    // Phase-1 shape for every worker that was admitted straight away, and the
    // fold reads an absent marker exactly as it reads `false`.
    if plan.queued {
        genesis["queued"] = json!(true);
    }
    // The retry lineage (spec §4.9), and ONLY when there is one: a plain
    // spawn's genesis keeps its exact Phase-1/2 shape, and the fold reads an
    // absent `attempt` exactly as it reads `1`.
    if let Some(lineage) = &plan.lineage {
        genesis["parent_id"] = json!(lineage.parent_id);
        genesis["attempt"] = json!(lineage.attempt);
    }
    // The auto-review marker (spec §4.8), and ONLY on a worker the broker made
    // to review another's diff. Deliberately NOT `parent_id`: it is a different
    // relation, and sharing the key would make `max_attempts` count reviews.
    if let Some(reviewed) = &plan.auto_review_of {
        genesis["auto_review_of"] = json!(reviewed);
    }
    let snap = append_and_fold(&mut log, &wdir, snap, EventKind::Spawned, genesis, now)?;
    Ok((plan.id.clone(), wdir, snap))
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

/// Resolve a named model/effort pair before any spawn-side filesystem work.
/// The caller still applies explicit field flags over this value.
fn profile_gate(
    cfg: &config::Config,
    requested: Option<&str>,
) -> Result<Option<config::ProfileConfig>, AppError> {
    let Some(name) = requested else {
        return Ok(None);
    };
    if let Some(profile) = cfg.profiles.get(name) {
        return Ok(Some(profile.clone()));
    }
    let known: Vec<&str> = cfg.profiles.keys().map(String::as_str).collect();
    Err(AppError::new(
        "PROFILE_UNKNOWN",
        ExitCode::Precondition,
        format!("unknown spawn profile `{name}`"),
    )
    .remedy(format!(
        "choose a configured profile: {}",
        known.join(", ")
    ))
    .data(json!({"profile": name, "known_profiles": known})))
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
pub enum SchemaChoice {
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
    pub fn label(&self) -> &'static str {
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

/// Copy the schema [`plan_schema`] chose into `<worker>/result-schema.json`
/// (0600), returning the path recorded on the snapshot, in genesis and in
/// [`SpawnSpec::schema_path`], plus the `schema_source` label.
///
/// The **copy** is the point: a worker judged against a file outside its own
/// directory is a worker whose verdict changes when somebody edits that file,
/// or moves the repo the template shipped in.
///
/// Split from the *decision* ([`plan_schema`]) so that a caller with no brief
/// on disk — the broker's `auto_review` worker, whose brief it built in memory
/// — can still take a rung of the precedence table and get the same copy, the
/// same path and the same label as a CLI spawn.
fn materialize_schema(
    wdir: &Path,
    lane: Lane,
    choice: Option<&SchemaChoice>,
) -> Result<(Option<PathBuf>, &'static str), AppError> {
    let Some(choice) = choice else {
        return Ok((None, "none"));
    };
    let bytes = match choice {
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
    home: &A2aHome,
    lane: Lane,
    requested: Option<&Path>,
    roots: &mut Vec<PathBuf>,
    json_mode: bool,
    quiet: bool,
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
        .remedy(
            "the directory does not exist on this machine -- pass an existing project directory \
             (workers run inside it)",
        )
    })?;
    probe_cwd_writable(&canon)?;
    if !roots.iter().any(|root| is_under(&canon, root)) {
        return rescue_or_refuse_cwd(home, &canon, roots, json_mode, quiet);
    }
    Ok(CwdPlan::Fixed(canon))
}

fn rescue_or_refuse_cwd(
    home: &A2aHome,
    canon: &Path,
    roots: &mut Vec<PathBuf>,
    json_mode: bool,
    quiet: bool,
) -> Result<CwdPlan, AppError> {
    let stdin_tty = std::io::stdin().is_terminal();
    let stderr_tty = std::io::stderr().is_terminal();
    let config_writable = if stdin_tty && stderr_tty && !json_mode && !quiet {
        Some(crate::commands::config_roots::ensure_config_writable(home))
    } else {
        None
    };
    if rescue_eligibility(
        stdin_tty && stderr_tty,
        json_mode,
        quiet,
        matches!(config_writable, Some(Ok(()))),
    ) {
        let default = canon
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| canon.to_path_buf());
        let listed = allowed_roots_listing(home, roots);
        eprintln!(
            "a2a: --cwd {} is not under an allowed root ({}).",
            canon.display(),
            listed
        );
        eprintln!(
            "a2a: add an allowed root to {} so spawns can run here.",
            crate::commands::config_roots::config_path(home).display()
        );
        eprint!("a2a: root to add [{}]: ", default.display());
        let _ = std::io::stderr().flush();
        let mut answer = String::new();
        if let Err(e) = std::io::stdin().read_line(&mut answer) {
            return Err(cwd_not_allowed(
                home,
                canon,
                roots,
                Some(format!("reading stdin: {e}")),
            ));
        }
        let home_env = std::env::var("HOME").unwrap_or_default();
        let wanted = parse_root_answer(&answer, &default, &home_env);
        let root = crate::commands::config_roots::canonical_root(&wanted)?;
        if !canon.starts_with(&root) {
            eprintln!(
                "a2a: {} does not contain --cwd {}.",
                root.display(),
                canon.display()
            );
            return Err(cwd_not_allowed(home, canon, roots, None));
        }
        let written = crate::commands::config_roots::add_root(home, &root)?;
        eprintln!(
            "a2a: added allowed root {} to {}.",
            written.path.display(),
            crate::commands::config_roots::config_path(home).display()
        );
        if !roots.iter().any(|existing| canon.starts_with(existing)) {
            roots.push(root);
        }
        return Ok(CwdPlan::Fixed(canon.to_path_buf()));
    }

    let note = match config_writable {
        Some(Err(e)) => Some(format!(
            "also could not update {} automatically: {}",
            crate::commands::config_roots::config_path(home).display(),
            e.message
        )),
        _ => None,
    };
    Err(cwd_not_allowed(home, canon, roots, note))
}

pub(crate) fn rescue_eligibility(
    tty: bool,
    json_mode: bool,
    quiet: bool,
    config_writable: bool,
) -> bool {
    tty && !json_mode && !quiet && config_writable
}

pub(crate) fn parse_root_answer(input: &str, default: &Path, home_env: &str) -> PathBuf {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return default.to_path_buf();
    }
    if let Some(rest) = trimmed.strip_prefix("~/") {
        if !home_env.is_empty() {
            return PathBuf::from(home_env).join(rest);
        }
    }
    PathBuf::from(trimmed)
}

fn probe_cwd_writable(cwd: &Path) -> Result<(), AppError> {
    let probe = cwd.join(format!(".a2a-write-probe-{}", std::process::id()));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = std::fs::remove_file(&probe);
            Ok(())
        }
        Err(e) => Err(AppError::new(
            "CWD_NOT_WRITABLE",
            ExitCode::Precondition,
            format!("--cwd {}: {e}", cwd.display()),
        )
        .remedy("workers write into --cwd; fix permissions or choose another directory")),
    }
}

fn cwd_not_allowed(
    home: &A2aHome,
    canon: &Path,
    roots: &[PathBuf],
    note: Option<String>,
) -> AppError {
    let mut message = format!(
        "--cwd {} is not under an allowed root ({})",
        canon.display(),
        allowed_roots_listing(home, roots)
    );
    if let Some(note) = note {
        message.push_str("; ");
        message.push_str(&note);
    }
    AppError::new("CWD_NOT_ALLOWED", ExitCode::Precondition, message).remedy(
        "run `a2a config add-root <dir>` (or edit allowed_roots in $A2A_HOME/config.toml / \
         A2A_ALLOWED_ROOTS)",
    )
}

fn allowed_roots_listing(home: &A2aHome, roots: &[PathBuf]) -> String {
    let configured = config::non_implicit_allowed_roots(home, roots);
    if configured.is_empty() {
        "none are configured on this machine".to_string()
    } else {
        configured
            .iter()
            .map(|r| r.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Containment by path COMPONENT, never by string prefix: `/x/a` is not under
/// `/x/ab`. Both sides must already be canonicalized by the caller.
fn is_under(path: &Path, root: &Path) -> bool {
    path.starts_with(root)
}

#[cfg(test)]
mod cwd_rescue_tests {
    use super::*;

    #[test]
    fn rescue_requires_tty_human_output_and_writable_config() {
        assert!(rescue_eligibility(true, false, false, true));
        assert!(!rescue_eligibility(false, false, false, true));
        assert!(!rescue_eligibility(true, true, false, true));
        assert!(!rescue_eligibility(true, false, true, true));
        assert!(!rescue_eligibility(true, false, false, false));
    }

    #[test]
    fn parse_root_answer_accepts_default_and_expands_home_prefix() {
        let default = Path::new("/tmp/project-parent");
        assert_eq!(parse_root_answer("\n", default, "/Users/u"), default);
        assert_eq!(
            parse_root_answer("~/Development\n", default, "/Users/u"),
            PathBuf::from("/Users/u/Development")
        );
        assert_eq!(
            parse_root_answer("/opt/src\n", default, "/Users/u"),
            PathBuf::from("/opt/src")
        );
    }
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

/// The next free worker id on `lane`.
///
/// `pub` because both seeding callers need one BEFORE they seed: the CLI writes
/// the §5 audit line against it, and the broker asks `decide_admission` about
/// `<home>/workers/<id>` while that directory still does not exist.
pub fn allocate_id(
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
        .filter_map(|v| v.get("id").and_then(Value::as_str).map(str::to_string))
        .collect()
}

/// Everything written under a freshly created worker dir, before genesis.
///
/// Returns the materialized result schema and its `schema_source` label: the
/// schema is a *worker file*, laid out here with the brief and the prompt, so
/// that everything the worker is judged by lives in one directory.
fn lay_out_worker(
    wdir: &Path,
    plan: &SeedPlan,
) -> Result<(Option<PathBuf>, &'static str), AppError> {
    for d in WORKER_DIRS {
        create_dir_0700(&wdir.join(d)).map_err(|e| io_err("creating the worker dir tree", e))?;
    }
    stage_context(wdir, &plan.context)?;
    write_0600(&wdir.join("brief.md"), plan.brief_text.as_bytes())?;
    // The prompt is the brief BODY (front matter is metadata for a2a, not
    // instructions for the worker); a body-less brief falls back to the whole
    // file rather than handing codex an empty stdin.
    //
    // Re-parsed from the text this function has just written rather than taken
    // as a second argument: the brief that governs is the one on disk, and a
    // caller-supplied parse of different text would be a way for the two to
    // disagree. An unparseable brief cannot reach here from `a2a spawn` (the
    // gate refused it) and falls back to the whole file if it ever does.
    let parsed = brief::parse(&plan.brief_text).unwrap_or_default();
    let prompt = if parsed.body.trim().is_empty() {
        plan.brief_text.as_str()
    } else {
        parsed.body.as_str()
    };
    write_0600(&wdir.join("prompt.md"), prompt.as_bytes())?;
    materialize_schema(wdir, plan.lane, plan.schema.as_ref())
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
                format!(
                    "--context {}: matches the secret-file rule `{rule}`",
                    src.display()
                ),
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
    fn mesh_work_spawn_pins_network_off_regardless_of_the_lane() {
        assert!(!effective_network(
            true,
            &SpawnOverrides {
                network: Some(false),
            }
        ));
        assert!(effective_network(true, &SpawnOverrides::default()));

        let cwd = Path::new("/repo");
        let worker = Path::new("/worker");
        let prompt = Path::new("/worker/prompt.md");
        let spec = exec::SpawnSpec {
            codex_bin: Path::new("codex"),
            cwd,
            worker_dir: worker,
            epoch: 0,
            sandbox: SandboxMode::WorkspaceWrite,
            network: effective_network(
                true,
                &SpawnOverrides {
                    network: Some(false),
                },
            ),
            model: "gpt-5.5",
            effort: "high",
            schema_path: None,
            prompt_file: prompt,
            resume_session: None,
            env: &[],
        };
        assert!(exec::build_argv(&spec)
            .contains(&"sandbox_workspace_write.network_access=false".to_string()));
    }

    /// A fixed [`SeedPlan`] produces a genesis row with EXACTLY these keys and
    /// values — the pin behind the `auto_review` refactor's central claim, that
    /// a broker-seeded worker and a CLI-seeded one are the same worker.
    ///
    /// The key SET is asserted whole rather than key by key, because the
    /// failure this guards against is a key silently going missing when the
    /// spawn path is next reshaped, and an `assert` per key cannot see an
    /// absence it was not told to look for.
    #[test]
    fn seed_worker_writes_exactly_the_genesis_keys_the_fold_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        let (cfg, _, _) = config::load(&home).unwrap();
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-19T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let brief_text = "---\nlane: impl\nacceptance_criteria:\n  - it works\nverify:\n  - \"true\"\n---\nbody\n";
        let plan = SeedPlan {
            id: "impl-0819t1200-aaaa".to_string(),
            lane: Lane::Impl,
            cwd: PathBuf::from("/repo"),
            brief_text: brief_text.to_string(),
            context: vec![],
            schema: Some(SchemaChoice::LaneDefault),
            profile: None,
            model: "gpt-5.5".to_string(),
            effort: "high".to_string(),
            owner: Some("test-owner".to_string()),
            max_turns: 40,
            sandbox: SandboxMode::WorkspaceWrite,
            transport: Transport::Exec,
            requested_transport: Transport::Exec,
            transport_selection: "auto",
            elevation_reason: None,
            worktree: None,
            base_ref: None,
            worktree_skipped: None,
            lineage: None,
            auto_review_of: None,
            queued: false,
        };
        let (id, wdir, snap) = seed_worker(&home, &cfg, &plan, now).unwrap();
        assert_eq!(id, plan.id);
        assert_eq!(wdir, home.worker_dir(&plan.id));

        let line = std::fs::read_to_string(wdir.join("events.jsonl")).unwrap();
        let env: Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
        let raw = env["raw"].as_object().unwrap();
        let mut keys: Vec<&str> = raw.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "brief_sha",
                "created_at",
                "cwd",
                "effort",
                "fsm",
                "id",
                "lane",
                "max_turns",
                "model",
                "network",
                "owner",
                "requested_transport",
                "sandbox",
                "schema_path",
                "schema_source",
                "transport",
                "transport_selection",
            ],
            "the genesis key set for a plain impl spawn"
        );
        assert_eq!(raw["fsm"], "Spawn");
        assert_eq!(raw["lane"], "impl");
        assert_eq!(raw["cwd"], "/repo");
        assert_eq!(raw["sandbox"], "workspace-write");
        // DERIVED, not carried: `network` off the lane table, `brief_sha` off
        // the very text this call wrote to `brief.md`.
        assert_eq!(raw["network"], false);
        assert_eq!(raw["brief_sha"], json!(brief::sha256_hex(brief_text)));
        assert_eq!(raw["max_turns"], 40);
        assert_eq!(raw["owner"], "test-owner");
        assert_eq!(raw["created_at"], "2026-08-19T12:00:00+00:00");
        assert_eq!(raw["schema_source"], "lane-default");
        assert_eq!(
            raw["schema_path"],
            json!(wdir.join(WORKER_SCHEMA_FILE)),
            "the worker's OWN copy of the schema"
        );
        assert_eq!(
            std::fs::read_to_string(wdir.join(WORKER_SCHEMA_FILE)).unwrap(),
            crate::templates::IMPL_RESULT_SCHEMA
        );
        // ...and the derived third: the outbox is the one root this worker may
        // write to, chosen from the directory this call laid out.
        assert_eq!(snap.writable_roots, vec![wdir.join("outbox")]);
        assert_eq!(snap.state, crate::fsm::WorkerState::Spawned);
        // The prompt is the BODY, not the front matter.
        assert_eq!(
            std::fs::read_to_string(wdir.join("prompt.md")).unwrap(),
            "body\n"
        );
        for d in WORKER_DIRS {
            assert!(wdir.join(d).is_dir(), "{d}");
        }
    }

    /// The conditional half: every key that appears only when the fact behind
    /// it exists — and the auto-review marker, which is `auto_review_of` and
    /// never `parent_id`.
    #[test]
    fn seed_worker_adds_the_conditional_genesis_keys_only_when_they_are_true() {
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(tmp.path())).unwrap();
        let (cfg, _, _) = config::load(&home).unwrap();
        let now = Utc::now();
        let plan = SeedPlan {
            id: "review-0819t1200-bbbb".to_string(),
            lane: Lane::Review,
            cwd: home.worker_dir("review-0819t1200-bbbb"),
            brief_text: "---\nlane: review\nacceptance_criteria:\n  - look\n---\nbody\n"
                .to_string(),
            context: vec![],
            schema: Some(SchemaChoice::LaneDefault),
            profile: None,
            model: "gpt-5.5".to_string(),
            effort: "medium".to_string(),
            owner: Some("test-owner".to_string()),
            max_turns: 15,
            sandbox: SandboxMode::OutboxOnly,
            transport: Transport::AppServer,
            requested_transport: Transport::AppServer,
            transport_selection: "explicit",
            elevation_reason: None,
            worktree: None,
            base_ref: None,
            worktree_skipped: None,
            lineage: None,
            auto_review_of: Some("impl-0819t1200-aaaa".to_string()),
            queued: true,
        };
        let (_, wdir, snap) = seed_worker(&home, &cfg, &plan, now).unwrap();
        let line = std::fs::read_to_string(wdir.join("events.jsonl")).unwrap();
        let env: Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
        assert_eq!(env["raw"]["auto_review_of"], "impl-0819t1200-aaaa");
        assert_eq!(env["raw"]["queued"], true);
        assert!(
            env["raw"].get("parent_id").is_none(),
            "a review is not a retry"
        );
        assert!(env["raw"].get("attempt").is_none());
        // ...and the fold puts the marker where `ls --json` reads it.
        assert_eq!(
            snap.extra.get("auto_review_of"),
            Some(&json!("impl-0819t1200-aaaa"))
        );
        assert_eq!(snap.state, crate::fsm::WorkerState::Queued);
        // The review lane's shipped default, applied through the lane-default
        // rung — which is what makes the reviewer's `result.json` exist at all.
        assert_eq!(env["raw"]["schema_source"], "lane-default");
        assert_eq!(
            std::fs::read_to_string(wdir.join(WORKER_SCHEMA_FILE)).unwrap(),
            crate::templates::REVIEW_FINDINGS_SCHEMA
        );
    }

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
        let tmp = tempfile::tempdir().unwrap();
        let home = A2aHome::resolve(Some(&tmp.path().join("home"))).unwrap();
        let mut roots = Vec::new();
        let err = resolve_cwd(
            &home,
            Lane::Research,
            Some(Path::new("/tmp")),
            &mut roots,
            true,
            false,
        )
        .unwrap_err();
        assert_eq!(err.code, "RESEARCH_CWD_FIXED");
        assert!(matches!(
            resolve_cwd(&home, Lane::Research, None, &mut roots, true, false),
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
        assert_eq!(
            v["$schema"],
            json!("https://json-schema.org/draft/2020-12/schema")
        );
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
        // The model API's strict structured outputs require every object to be
        // CLOSED with a complete `required` list — an open `evidence` was
        // rejected live with `invalid_json_schema` (bug a2a-5n2). Evidence is
        // therefore exactly `logs_excerpts`; the playwright-only keys are
        // deliberately ABSENT (strict mode would force every worker to emit
        // them), and their absence is part of this pin.
        assert_eq!(props["evidence"]["additionalProperties"], json!(false));
        assert_eq!(props["evidence"]["required"], json!(["logs_excerpts"]));
        assert_eq!(
            props["evidence"]["properties"]["logs_excerpts"]["type"],
            json!("array")
        );
        for absent in ["screenshots", "console_errors", "network_failures"] {
            assert_eq!(
                props["evidence"]["properties"].get(absent),
                None,
                "`{absent}` must stay out of the strict schema"
            );
        }
        assert_eq!(props["files_changed"]["type"], json!("array"));
        assert_eq!(props["files_changed"]["items"]["type"], json!("string"));
        assert_eq!(props["next_step_hint"]["type"], json!(["string", "null"]));
        // Every required key is described; `additionalProperties: false` would
        // otherwise reject a key the schema itself asks for.
        for key in &required {
            assert!(
                props.get(key).is_some(),
                "`{key}` is required but undescribed"
            );
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
            let choice = plan_schema(lane, flag, &brief_path, brief).expect("a resolvable schema");
            let (path, source) = materialize_schema(wdir.path(), lane, choice.as_ref())
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
            err.message
                .contains(&tmp.path().join("no-such.json").display().to_string()),
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
