//! `a2a doctor [--json]` — the preflight an orchestrator runs before it trusts
//! the fleet (spec §4.1).
//!
//! Doctor's whole value is that it is **machine-readable and total**: every
//! check runs, every check reports, and one bad answer never hides the other
//! five. That shapes three decisions in this file.
//!
//! **The checks are data, not control flow.** [`CHECKS`] is a slice of
//! `fn(&Ctx) -> Check`; the runner maps over it and knows nothing about any
//! individual check. Phase 2's four (app-server handshake, codex-home
//! agreement, broker status, auth mode) were appended to that slice and the
//! runner was not touched. The mesh enrollment check and Phase 3's three
//! (context canon, the allowed-roots check, tracked-secret check, sync state)
//! were appended the same way. Phase 1's six, Phase 2's four, mesh enrollment,
//! Phase 3's three — and it is pinned by a test.
//!
//! **Only `fail` is fatal.** `fail` means "a worker spawned right now would
//! not work" and exits 9 (precondition); `warn` means "this will bite you
//! later" and exits 0. The version check is the sharp case: an unrecognizable
//! `codex --version` is a warn, never a fail, because refusing to run over a
//! string we could not parse would make every codex release a hard outage.
//! Phase 2 keeps that discipline everywhere it can: a handshake that does not
//! answer is a *warn*, because the exec transport still spawns workers, and an
//! absent codexHome is a warn rather than a fail — [`codex_home_match`] fails
//! only when it holds two known values that disagree.
//!
//! Phase 3 **widens that rule exactly once**, and only for
//! [`codex_config_secrets`]: a live credential in a file git publishes fails
//! whether or not a spawn would succeed. It violates the machine's hard rule on
//! its own — the key is already out, and every future clone republishes it —
//! and a preflight that reported it as a warning would be read as optional,
//! which for a leaked credential it is not. Every other check keeps the
//! spawn-blocker meaning of `fail`; the two sync checks Phase 3 added beside it
//! ([`context_canon`], [`sync_state`]) have no `fail` branch at all.
//!
//! **Doctor adopts a home that other commands refuse.** Every other command
//! goes through [`A2aHome::resolve`], which gates on `LAYOUT_VERSION` and errors
//! out on a mismatch — correct there, useless here, since a home the binary
//! will not touch is exactly the thing an operator ran `doctor` to be told
//! about. So a mismatch is caught, the root is adopted ungated, and it comes
//! back as the `layout_version` check with a remedy.

use crate::adapter::{self, InitInfo};
use crate::broker::core::BrokerHealth;
use crate::broker::wire;
use crate::brokerclient::{self, Handshake};
use crate::config::{self, Config};
use crate::exec::{self, Liveness};
use crate::home::{self, A2aHome, LAYOUT_VERSION};
use crate::mesh::keys;
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use crate::rpc::RpcError;
use crate::sync::{manifest, mcp};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

/// What one check concluded. `Fail` is the only status that changes the exit
/// code, and only `fail` may claim that spawning right now is impossible.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Status {
    Ok,
    Warn,
    Fail,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Warn => "warn",
            Status::Fail => "fail",
        }
    }
    /// The human-mode glyph. Deliberately three distinguishable shapes rather
    /// than three colours: doctor's output is read in pipes and CI logs too.
    fn glyph(self) -> char {
        match self {
            Status::Ok => '✓',
            Status::Warn => '⚠',
            Status::Fail => '✗',
        }
    }
}

/// One line of the report. `remedy` is what an operator would *type* — a check
/// that can fail without saying how to fix it is an alarm, not a diagnosis.
pub struct Check {
    pub id: &'static str,
    pub status: Status,
    pub detail: String,
    pub remedy: Option<String>,
}

impl Check {
    fn new(id: &'static str, status: Status, detail: impl Into<String>) -> Self {
        Self {
            id,
            status,
            detail: detail.into(),
            remedy: None,
        }
    }
    fn ok(id: &'static str, detail: impl Into<String>) -> Self {
        Self::new(id, Status::Ok, detail)
    }
    fn warn(id: &'static str, detail: impl Into<String>) -> Self {
        Self::new(id, Status::Warn, detail)
    }
    fn fail(id: &'static str, detail: impl Into<String>) -> Self {
        Self::new(id, Status::Fail, detail)
    }
    fn remedy(mut self, r: impl Into<String>) -> Self {
        self.remedy = Some(r.into());
        self
    }
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "status": self.status.as_str(),
            "detail": self.detail,
            "remedy": self.remedy,
        })
    }
}

/// Everything the checks are allowed to look at, gathered once.
///
/// The codex path in particular is resolved a single time and shared: two
/// checks asking the filesystem the same question independently could report
/// "present" and "cannot run it" about two different binaries. The handshake is
/// gathered here for the same reason and a sharper one — it spawns a child, and
/// two checks that each spawned their own app-server would be both slower and
/// capable of disagreeing.
pub struct Ctx {
    pub home: A2aHome,
    pub cfg: Config,
    pub codex: Option<PathBuf>,
    /// `None` = never attempted (there was no binary to probe), which is a
    /// different report from `Some(Err(_))` and must stay distinguishable.
    pub probe: Option<Result<InitInfo, RpcError>>,
    /// `CODEX_HOME`, else `$HOME/.codex`; `None` when neither is set.
    pub codex_home: Option<PathBuf>,
    pub now: DateTime<Utc>,
}

/// The check list, in report order. Append-only across phases: Phase 1's six,
/// the allowed-roots first-run check, Phase 2's four, mesh enrollment, then
/// Phase 3's three.
const CHECKS: &[fn(&Ctx) -> Check] = &[
    codex_present,
    codex_version,
    layout_version,
    home_perms,
    workers_health,
    allowed_roots,
    env_inheritance,
    app_server_handshake,
    codex_home_match,
    broker_status,
    gateway_enrollment,
    auth_mode,
    context_canon,
    codex_config_secrets,
    sync_state,
];

/// The whole handshake — spawn, `initialize`, response — not each call.
///
/// Two seconds is long enough for a cold app-server on a loaded machine and
/// short enough that `doctor` still answers like a command. The child is killed
/// when it expires ([`adapter::handshake_probe`]): a preflight that leaks a live
/// app-server is worse than one that fails.
pub const HANDSHAKE_BUDGET: Duration = Duration::from_secs(2);

/// The variable that decides which plan the *humans* in this shell bill to.
/// a2a's own children never see it (it is on the scrub list).
const API_KEY_ENV: &str = "OPENAI_API_KEY";

/// Where codex keeps the login every worker shares (spec §4.2).
const AUTH_JSON: &str = "auth.json";
const GATEWAY_TOKEN_REENROLL_REMEDY: &str =
    "re-enroll: `a2a mesh fleet enroll` on the root machine, `a2a mesh gateway install-token` here";

struct FixedClock(DateTime<Utc>);

impl keys::Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

pub fn run(home_flag: Option<&Path>, json_mode: bool, quiet: bool) -> Result<Outcome, AppError> {
    let home = match A2aHome::resolve(home_flag) {
        Ok(h) => h,
        // The one command that may proceed over a layout mismatch — see the
        // module docs. `resolve` has already created the tree by this point.
        Err(e) if e.code == "LAYOUT_VERSION_MISMATCH" => A2aHome::adopt(home::root_for(home_flag)),
        Err(e) => return Err(e),
    };
    let (cfg, _origins, warnings) = config::load(&home)?;
    if !quiet {
        for w in &warnings {
            eprintln!("a2a: warning: {w}");
        }
    }
    // Gathered before the runner, in report order's stead: the probe is the one
    // check input that costs a process, and two of the four Phase-2 checks read
    // its result.
    let codex = resolve_codex(&cfg);
    let probe = codex
        .as_deref()
        .map(|bin| adapter::handshake_probe(bin, HANDSHAKE_BUDGET));
    let ctx = Ctx {
        codex,
        probe,
        codex_home: effective_codex_home(),
        home,
        cfg,
        now: Utc::now(),
    };

    let checks: Vec<Check> = CHECKS.iter().map(|check| check(&ctx)).collect();
    let failed = checks.iter().filter(|c| c.status == Status::Fail).count();
    let warned = checks.iter().filter(|c| c.status == Status::Warn).count();
    let summary = format!(
        "{} check(s): {} ok, {warned} warn, {failed} fail",
        checks.len(),
        checks.len() - warned - failed,
    );

    // Human mode: one glyph line per check on stderr, the summary on stdout.
    // `--json` callers get the same facts in the envelope and are spared the
    // duplicate prose.
    if !json_mode && !quiet {
        for c in &checks {
            eprintln!("{} {}  {}", c.status.glyph(), c.id, c.detail);
            if let Some(r) = &c.remedy {
                eprintln!("    remedy: {r}");
            }
        }
    }

    // A doctor that found a fatal problem still *succeeded* — it answered the
    // question — so this is an Outcome with exit 9, not an error envelope: the
    // caller needs the full check list precisely when something failed.
    let exit = if failed > 0 {
        ExitCode::Precondition
    } else {
        ExitCode::Success
    };
    let data = if json_mode {
        json!({
            "ok": failed == 0,
            "home": ctx.home.root(),
            "summary": summary,
            "checks": checks.iter().map(Check::to_json).collect::<Vec<_>>(),
        })
    } else {
        Value::String(summary)
    };
    Ok(Outcome::with(data, exit))
}

// ------------------------------------------------------------- the six checks

/// Is there a codex to spawn at all? Fail: nothing else in Phase 1 works.
fn codex_present(ctx: &Ctx) -> Check {
    let configured = ctx
        .cfg
        .codex_bin
        .clone()
        .unwrap_or_else(|| PathBuf::from("codex"));
    match &ctx.codex {
        Some(p) => Check::ok(
            "codex_present",
            format!("codex resolves to {}", p.display()),
        ),
        None => Check::fail(
            "codex_present",
            format!(
                "no executable codex at `{}` (A2A_CODEX_BIN / config `codex_bin` / PATH)",
                configured.display()
            ),
        )
        .remedy("install codex, or point `A2A_CODEX_BIN` at the binary"),
    }
}

/// Is that codex one this build's transport contract was written against?
///
/// Never a fail (see the module docs): an out-of-range version warns, and so
/// does output we cannot parse — `codex --version` is somebody else's format
/// and it is free to change.
fn codex_version(ctx: &Ctx) -> Check {
    let Some(bin) = &ctx.codex else {
        return Check::warn(
            "codex_version",
            "not checked — no codex binary to interrogate",
        )
        .remedy("fix `codex_present` first");
    };
    let range = &ctx.cfg.codex_version_range;
    let out = match Command::new(bin).arg("--version").output() {
        Ok(o) => o,
        Err(e) => {
            return Check::warn(
                "codex_version",
                format!("could not run `{} --version`: {e}", bin.display()),
            )
            .remedy("check the binary is runnable on this platform")
        }
    };
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let Some(found) = Version::find(&text) else {
        return Check::warn(
            "codex_version",
            format!(
                "could not parse a version out of `{} --version` (first line: {}) — assuming it is in range",
                bin.display(),
                first_line(&text)
            ),
        )
        .remedy(format!("confirm by hand that codex satisfies {range}"));
    };
    match Constraint::parse_all(range) {
        None => Check::warn(
            "codex_version",
            format!("codex {found}, but `codex_version_range` (`{range}`) is not a version range"),
        )
        .remedy("write it as comma-separated constraints, e.g. `>=0.140, <0.150`"),
        Some(cs) if cs.iter().all(|c| c.admits(found)) => Check::ok(
            "codex_version",
            format!("codex {found} satisfies {range}"),
        ),
        Some(_) => Check::warn(
            "codex_version",
            format!("codex {found} is outside the pinned range {range} — the exec JSONL contract is version-coupled"),
        )
        .remedy("install a codex in range, or widen `codex_version_range` after re-checking the transport"),
    }
}

/// Was this tree built by a binary that means the same thing by it?
fn layout_version(ctx: &Ctx) -> Check {
    match ctx.home.recorded_layout_version() {
        Some(v) if v == LAYOUT_VERSION => Check::ok("layout_version", format!("home layout v{v}")),
        Some(v) => Check::fail(
            "layout_version",
            format!("home layout v{v}, binary expects v{LAYOUT_VERSION}"),
        )
        .remedy("run `a2a migrate` when it lands, or move the old home aside and re-spawn"),
        None => Check::fail(
            "layout_version",
            format!(
                "{}/{} is missing or unreadable",
                ctx.home.root().display(),
                home::LAYOUT_VERSION_FILE
            ),
        )
        .remedy(format!(
            "write `{LAYOUT_VERSION}` to it, or move the home aside and let a2a rebuild it"
        )),
    }
}

/// Briefs, prompts, results and session ids all live under this root; anything
/// looser than 0700 leaks every one of them to other local users. Fail.
fn home_perms(ctx: &Ctx) -> Check {
    let root = ctx.home.root();
    match std::fs::metadata(root) {
        Ok(m) => {
            let mode = m.permissions().mode() & 0o777;
            if mode == 0o700 {
                Check::ok("home_perms", format!("{} is 0700", root.display()))
            } else {
                Check::fail(
                    "home_perms",
                    format!("{} is {mode:04o}, must be 0700", root.display()),
                )
                .remedy(format!("chmod 700 {}", root.display()))
            }
        }
        Err(e) => Check::fail("home_perms", format!("{}: {e}", root.display()))
            .remedy("check the path exists and is readable by this user"),
    }
}

/// The fleet's own health, reconciled the way `ls` reconciles it — so a worker
/// that died unobserved is counted as what it *is*, not as what the last reader
/// happened to see.
///
/// Warn-only by contract: a parked or orphaned worker is a thing to act on, not
/// a reason to stop the next spawn. The `result.md` mode is reported here too
/// (spec §4.5 wants every worker file 0600, but codex creates that one under
/// the child's umask — 0644 in practice; see `reconcile::has_result`).
fn workers_health(ctx: &Ctx) -> Check {
    let ids = match ctx.home.list_worker_ids() {
        Ok(ids) => ids,
        Err(e) => {
            return Check::warn(
                "workers_health",
                format!("could not list {}: {e}", ctx.home.workers_dir().display()),
            )
            .remedy("check the workers/ directory is readable")
        }
    };
    let total = ids.len();
    let (mut orphaned, mut parked, mut unreadable, mut loose) = (0usize, 0, 0, 0);
    let mut names: Vec<String> = Vec::new();
    for id in &ids {
        let dir = ctx.home.worker_dir(id);
        if world_readable(&dir.join(crate::reconcile::RESULT_MD)) {
            loose += 1;
        }
        match reconcile(&ctx.home, &ctx.cfg, id, ctx.now) {
            Ok(snap) if snap.state == crate::fsm::WorkerState::Orphaned => {
                orphaned += 1;
                names.push(id.clone());
            }
            // Non-terminal with nothing running behind it: awaiting-input or
            // escalated, i.e. a worker waiting on a human who may not know.
            Ok(snap) if !snap.state.is_terminal() && exec::liveness(&dir) != Liveness::Alive => {
                parked += 1;
                names.push(id.clone());
            }
            Ok(_) => {}
            Err(_) => {
                unreadable += 1;
                names.push(id.clone());
            }
        }
    }

    let mut detail = format!(
        "{total} worker(s): {orphaned} orphaned, {parked} parked (non-terminal, no live process), {unreadable} unreadable"
    );
    if loose > 0 {
        detail.push_str(&format!(
            "; {loose} with a group/world-readable result.md (codex writes it under the child's umask)"
        ));
    }
    if orphaned + parked + unreadable + loose == 0 {
        return Check::ok("workers_health", detail);
    }
    names.sort();
    names.truncate(5);
    if !names.is_empty() {
        detail.push_str(&format!(" — {}", names.join(", ")));
    }
    Check::warn("workers_health", detail)
        .remedy("`a2a ls` for the fleet; `a2a resume <id>` an orphan, `a2a send <id>` a parked one")
}

/// A standing note, not a probe: Phase 1 has no allowlist to verify, so the
/// honest report is what the worker environment actually is today.
fn env_inheritance(_ctx: &Ctx) -> Check {
    Check::warn(
        "env_inheritance",
        "exec-transport workers inherit the CLI environment minus the scrub list \
         (OPENAI_API_KEY, AWS_*, DEPOT_TOKEN); app-server workers see only the \
         broker's allowlist (PATH, HOME, USER, TERM, LANG, TMPDIR, CODEX_HOME).",
    )
    .remedy(
        "keep secrets out of the environment a2a itself runs in, or spawn on the \
         app-server transport, whose children inherit nothing else",
    )
}

fn allowed_roots(ctx: &Ctx) -> Check {
    let roots = config::non_implicit_allowed_roots(&ctx.home, &ctx.cfg.allowed_roots);
    if roots.is_empty() {
        Check::warn(
            "allowed_roots",
            "only the implicit $A2A_HOME/worktrees root is configured; project directories are not configured",
        )
        .remedy("run `a2a config add-root <dir>` for the project parent directory")
    } else {
        Check::ok(
            "allowed_roots",
            format!(
                "{} configured project root(s): {}",
                roots.len(),
                roots
                    .iter()
                    .map(|r| r.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )
    }
}

// ------------------------------------------------------- the phase-2 checks

/// Does this codex answer the app-server protocol at all?
///
/// Warn, never fail, when it does not: Phase 2 demoted exec to a fallback, it
/// did not remove it, so a codex that cannot be driven over JSON-RPC still
/// spawns workers. What the operator needs to know is that every spawn will
/// quietly take the slower path — which is exactly what a warn says.
fn app_server_handshake(ctx: &Ctx) -> Check {
    let (Some(bin), Some(probe)) = (&ctx.codex, &ctx.probe) else {
        return Check::warn(
            "app_server_handshake",
            "not checked — no codex binary to probe",
        )
        .remedy("fix `codex_present` first");
    };
    match probe {
        Ok(info) => Check::ok(
            "app_server_handshake",
            format!(
                "`{} {}` completed initialize as {}",
                bin.display(),
                adapter::APP_SERVER_ARGS.join(" "),
                info.user_agent
            ),
        ),
        Err(e) => Check::warn(
            "app_server_handshake",
            format!(
                "`{} {}` did not complete initialize ({}s budget): {e} — spawns fall back to the exec transport",
                bin.display(),
                adapter::APP_SERVER_ARGS.join(" "),
                HANDSHAKE_BUDGET.as_secs(),
            ),
        )
        .remedy(format!(
            "run `{} {}` by hand and read its stderr; `a2a spawn --transport exec` keeps working meanwhile",
            bin.display(),
            adapter::APP_SERVER_ARGS.join(" ")
        )),
    }
}

/// Do the app-server and this CLI mean the same directory by `CODEX_HOME`?
///
/// It matters because the thread the broker starts is only resumable — by
/// `codex exec resume`, i.e. by the exec fallback — from the home the *server*
/// wrote its rollout into. Two homes means work that silently cannot be picked
/// up again, so a real disagreement is the one new `fail`.
///
/// Absence is never that: no handshake means no second value, and a check that
/// failed on "I could not look" would take every stub-codex, offline or
/// not-logged-in machine down with it.
fn codex_home_match(ctx: &Ctx) -> Check {
    let probed = ctx
        .probe
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .map(|info| info.codex_home.as_path());
    codex_home_verdict(probed, ctx.codex_home.as_deref())
}

fn codex_home_verdict(probed: Option<&Path>, configured: Option<&Path>) -> Check {
    let Some(probed) = probed else {
        return Check::warn(
            "codex_home_match",
            "not checked — no handshake, so there is no server-side codexHome to compare",
        )
        .remedy("fix `app_server_handshake` first — the exec transport is unaffected");
    };
    let Some(configured) = configured else {
        return Check::warn(
            "codex_home_match",
            format!(
                "not checked — the app-server reports {}, but this process has neither CODEX_HOME nor HOME set",
                probed.display()
            ),
        )
        .remedy("export CODEX_HOME (or HOME) so the two can be compared");
    };
    if same_dir(probed, configured) {
        return Check::ok(
            "codex_home_match",
            format!("app-server and CLI agree on {}", probed.display()),
        );
    }
    Check::fail(
        "codex_home_match",
        format!(
            "the app-server writes its rollouts to {}, but this CLI resolves CODEX_HOME to {} — a thread started over the broker could not be resumed by the exec fallback",
            probed.display(),
            configured.display()
        ),
    )
    .remedy(format!(
        "export CODEX_HOME={} for both, or start the broker from the environment the CLI runs in",
        probed.display()
    ))
}

/// Is there a broker, and is it one this binary can talk to?
///
/// **Asking never starts one.** `doctor` is the command an operator runs to find
/// out what is there; a preflight that autostarts a daemon answers a question
/// nobody asked and leaves it running.
fn broker_status(ctx: &Ctx) -> Check {
    broker_verdict(probe_broker(ctx.home.root()))
}

/// What the socket said, as a value — so the verdict below is a pure function
/// of it and can be tested at every branch without a daemon.
enum BrokerProbe {
    /// `A2A_BROKER_DISABLE` is set, so no socket was touched at all.
    Disabled,
    Down {
        startable: Result<PathBuf, String>,
    },
    Up {
        peer: wire::Hello,
        health: Option<Box<BrokerHealth>>,
        /// The health file is written by a task, so a broker that has just
        /// bound its socket may not have published one yet — the pidfile is
        /// there from before the listener existed, and a report of a live
        /// daemon should always be able to name it.
        pid: Option<u32>,
    },
    Skewed {
        peer: wire::Hello,
        mine: u32,
    },
    /// Something is listening and the handshake did not survive it.
    Unreachable(String),
}

fn probe_broker(home: &Path) -> BrokerProbe {
    // The disable switch is a hard one (see `brokerclient::DISABLE_ENV`): with
    // it set, no `a2a` command opens the socket — not even to look.
    if brokerclient::disabled() {
        return BrokerProbe::Disabled;
    }
    match brokerclient::try_handshake(home) {
        Ok(None) => BrokerProbe::Down {
            startable: brokerclient::broker_bin().map_err(|e| e.message),
        },
        Ok(Some(Handshake::Ready(client))) => {
            let health = BrokerHealth::read(home).map(Box::new);
            let pid = health
                .as_ref()
                .map(|h| h.pid)
                .or_else(|| read_pidfile(home));
            BrokerProbe::Up {
                peer: client.peer.clone(),
                health,
                pid,
            }
        }
        Ok(Some(Handshake::Skewed { client, .. })) => BrokerProbe::Skewed {
            peer: client.peer.clone(),
            mine: wire::protocol_version(),
        },
        Err(e) => BrokerProbe::Unreachable(e.message),
    }
}

fn broker_verdict(probe: BrokerProbe) -> Check {
    match probe {
        BrokerProbe::Disabled => Check::warn(
            "broker_status",
            format!(
                "{} is set: no a2a command will open the broker socket, so every spawn takes the exec transport",
                brokerclient::DISABLE_ENV
            ),
        )
        .remedy(format!(
            "unset {} to let spawns use the app-server transport",
            brokerclient::DISABLE_ENV
        )),
        BrokerProbe::Down { startable: Ok(bin) } => Check::ok(
            "broker_status",
            format!(
                "not running — start-on-demand from {} (a2a starts one on the first spawn)",
                bin.display()
            ),
        ),
        BrokerProbe::Down {
            startable: Err(why),
        } => Check::warn(
            "broker_status",
            format!("not running, and there is no a2a binary to start one with: {why}"),
        )
        .remedy(format!(
            "point {} at the installed a2a binary",
            brokerclient::BROKER_BIN_ENV
        )),
        BrokerProbe::Up { peer, health, pid } => {
            let who = describe_broker(&peer, health.as_deref(), pid);
            match health.as_deref() {
                // The latch, not a transient: the child's restart budget is
                // spent, so new spawns take the exec path until the promotion
                // probe wins. Live work is unaffected, hence warn.
                Some(h) if h.degraded.is_some() => Check::warn(
                    "broker_status",
                    format!(
                        "{who} is DEGRADED to `{}` since {} ({}) — new spawns take the exec transport",
                        h.degraded.clone().unwrap_or_default(),
                        h.degraded_since
                            .map(|t| t.to_rfc3339())
                            .unwrap_or_else(|| "an unrecorded time".into()),
                        h.degraded_reason.clone().unwrap_or_else(|| "no reason recorded".into()),
                    ),
                )
                .remedy("read broker.log for the child's exit, then `a2a broker restart`"),
                _ => Check::ok("broker_status", who),
            }
        }
        BrokerProbe::Skewed { peer, mine } => Check::warn(
            "broker_status",
            format!(
                "up, but it speaks protocol v{} and this binary speaks v{mine} (broker binary {}, this one {}) — a spawn will refuse rather than guess",
                peer.protocol_version,
                display_or(&peer.binary_version, "unknown"),
                env!("CARGO_PKG_VERSION"),
            ),
        )
        .remedy("`a2a broker restart` once its workers are done — an idle skewed broker is replaced automatically"),
        BrokerProbe::Unreachable(why) => Check::warn(
            "broker_status",
            format!("something is listening on the broker socket but the handshake failed: {why}"),
        )
        .remedy("read broker.log, then `a2a broker restart`"),
    }
}

/// Built from parts rather than by concatenation: every fact here is optional,
/// and a sentence assembled with `push_str` grows an empty slot the day one of
/// them is missing.
fn describe_broker(peer: &wire::Hello, health: Option<&BrokerHealth>, pid: Option<u32>) -> String {
    let mut parts = Vec::new();
    if let Some(pid) = pid {
        parts.push(format!("pid {pid}"));
    }
    parts.push(format!("protocol v{}", peer.protocol_version));
    parts.push(format!(
        "binary {}",
        display_or(&peer.binary_version, "unknown")
    ));
    if let Some(h) = health {
        parts.push(format!("{} worker(s)", h.workers));
    }
    format!("broker up ({})", parts.join(", "))
}

fn read_pidfile(home: &Path) -> Option<u32> {
    std::fs::read_to_string(crate::broker::pid_path(home))
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn display_or<'a>(s: &'a str, fallback: &'a str) -> &'a str {
    if s.is_empty() {
        fallback
    } else {
        s
    }
}

fn gateway_enrollment(ctx: &Ctx) -> Check {
    if !gateway_material_exists(&ctx.home) {
        return Check::ok(
            "gateway_enrollment",
            "no mesh gateway material present — enrollment check skipped",
        );
    }
    match gateway_enrollment_verdict(ctx) {
        Ok(check) => check,
        Err(err) => {
            let mut check = Check::warn(
                "gateway_enrollment",
                format!("{}: {}", err.code, err.message),
            );
            check.remedy = Some(
                err.remedy
                    .unwrap_or_else(|| GATEWAY_TOKEN_REENROLL_REMEDY.to_string()),
            );
            check
        }
    }
}

fn gateway_material_exists(home: &A2aHome) -> bool {
    let dir = home.root().join("mesh").join("gateway");
    match std::fs::read_dir(dir) {
        Ok(mut entries) => {
            entries.any(|entry| entry.map(|entry| entry.path().is_file()).unwrap_or(true))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
    }
}

fn gateway_enrollment_verdict(ctx: &Ctx) -> Result<Check, AppError> {
    let gateway_id = keys::gateway_id(&ctx.home)?;
    let pubkey_b64 = keys::gateway_public_key_b64(&ctx.home)?;
    let clock = FixedClock(ctx.now);
    let claims = keys::verify_gateway_token(&ctx.home, &gateway_id, &pubkey_b64, &clock)?;
    Ok(Check::ok(
        "gateway_enrollment",
        format!(
            "gateway token verifies for fleet {} gateway {} until {}",
            claims.fleet_id,
            claims.gateway_id,
            DateTime::from_timestamp(claims.exp, 0)
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| claims.exp.to_string())
        ),
    ))
}

/// Which plan the fleet bills to, from the two facts that decide it — and only
/// from facts this process can read without touching the network.
///
/// Spec §4.2 wants doctor to assert the expected auth mode; Phase 2 reports it
/// (warn) rather than refusing dispatch (fail), because "no auth.json" is also
/// what a brand-new machine looks like ten seconds before `codex login`.
fn auth_mode(ctx: &Ctx) -> Check {
    let exported = std::env::var_os(API_KEY_ENV).is_some_and(|v| !v.is_empty());
    auth_mode_verdict(
        exported,
        ctx.codex_home
            .as_deref()
            .map(|h| h.join(AUTH_JSON))
            .as_deref(),
    )
}

fn auth_mode_verdict(api_key_exported: bool, auth_json: Option<&Path>) -> Check {
    let mut problems: Vec<String> = Vec::new();
    if api_key_exported {
        problems.push(format!(
            "{API_KEY_ENV} is exported — a2a scrubs it from every child so workers bill the ChatGPT plan, but anything else run from this shell bills the key"
        ));
    }
    match auth_json {
        None => problems.push(
            "could not locate a codex home (neither CODEX_HOME nor HOME is set), so the login could not be checked".into(),
        ),
        Some(p) if !p.is_file() => problems.push(format!(
            "{} is missing — nothing is logged in, and every worker fails on its first turn",
            p.display()
        )),
        Some(_) => {}
    }
    if problems.is_empty() {
        let path = auth_json.map(Path::display);
        return Check::ok(
            "auth_mode",
            format!(
                "ChatGPT plan auth: {} present, {API_KEY_ENV} not exported",
                path.map(|d| d.to_string()).unwrap_or_default()
            ),
        );
    }
    Check::warn("auth_mode", problems.join("; ")).remedy(format!(
        "`codex login` for the shared login; `unset {API_KEY_ENV}` to keep billing on the plan"
    ))
}

// ------------------------------------------------------- the phase-3 checks

/// The file a Claude session reads, and the one line that makes a Codex worker
/// read the same thing (spec §6.3).
const CLAUDE_MD: &str = "CLAUDE.md";
const AGENTS_IMPORT: &str = "@AGENTS.md";

/// Does this repository's context reach a Codex worker at all?
///
/// Codex reads `AGENTS.md`; Claude reads `CLAUDE.md`. A repository whose
/// machine-shared instructions live only in the second one has workers running
/// without them and no symptom that says so, which is exactly what a preflight
/// is for. Evaluated against the process's cwd, because that is the repository
/// the operator is asking about.
fn context_canon(_ctx: &Ctx) -> Check {
    let cwd = std::env::current_dir().unwrap_or_default();
    let text = std::fs::read_to_string(cwd.join(CLAUDE_MD)).ok();
    context_canon_verdict(text.as_deref(), &cwd)
}

/// Never a `fail`. This is a migration nudge, and a fail would take the
/// preflight of every repository that has not migrated yet down with it — a
/// gate over a *documentation* fact, which is precisely how operators learn to
/// pass `--force` to a preflight.
fn context_canon_verdict(claude_md: Option<&str>, cwd: &Path) -> Check {
    let Some(text) = claude_md else {
        return Check::ok(
            "context_canon",
            format!("no {CLAUDE_MD} in {} — nothing to migrate", cwd.display()),
        );
    };
    // The line has to BE the import. A `CLAUDE.md` that merely mentions
    // `@AGENTS.md` in a sentence imports nothing, and reporting it as migrated
    // would be worse than not checking.
    if text.lines().any(|l| l.trim() == AGENTS_IMPORT) {
        return Check::ok(
            "context_canon",
            format!("{} in {} imports {AGENTS_IMPORT}", CLAUDE_MD, cwd.display()),
        );
    }
    Check::warn(
        "context_canon",
        format!(
            "{CLAUDE_MD} does not import {AGENTS_IMPORT} — Codex workers read AGENTS.md and will not see it"
        ),
    )
    .remedy(format!(
        "move machine-shared content into AGENTS.md and put `{AGENTS_IMPORT}` at the top of {CLAUDE_MD}"
    ))
}

/// Is a credential sitting in a file git publishes (spec §5, §6.2)?
///
/// The gathering reads the file and asks git one question about it; the verdict
/// below never sees a byte of what was in it. That split is the whole design:
/// this check's report travels into agent context and into CI logs, so it may
/// carry a path and a set of 1-based line numbers, and nothing else.
fn codex_config_secrets(ctx: &Ctx) -> Check {
    let Some(codex_home) = ctx.codex_home.as_deref() else {
        return codex_config_secrets_verdict(None, false, &[]);
    };
    let target = codex_home.join(mcp::TARGET_FILE);
    if !target.is_file() {
        return codex_config_secrets_verdict(None, false, &[]);
    }
    // False for "not a git repository" as well as for "tracked by nothing" —
    // both are the same answer to the only question that matters here, which is
    // whether this file is published.
    let tracked = mcp::target_is_tracked(codex_home);
    codex_config_secrets_verdict(Some(&target), tracked, &mcp::secret_lines(&target))
}

/// Doctor's first **security-class** `fail` (see the module docs): tracked AND
/// carrying a secret shape. Every other combination is `ok`, including the
/// untracked-with-secrets one, which is the state `--migrate-untrack` exists to
/// produce.
fn codex_config_secrets_verdict(
    path: Option<&Path>,
    tracked: bool,
    secret_lines: &[usize],
) -> Check {
    let Some(path) = path else {
        return Check::ok(
            "codex_config_secrets",
            format!(
                "no {} under the codex home — nothing to check",
                mcp::TARGET_FILE
            ),
        );
    };
    let count = secret_lines.len();
    if !tracked {
        let detail = if count == 0 {
            format!(
                "{} carries no secret shape and git does not track it",
                path.display()
            )
        } else {
            format!(
                "{count} secret-shaped line(s) in {}, which git does not track",
                path.display()
            )
        };
        return Check::ok("codex_config_secrets", detail);
    }
    if count == 0 {
        return Check::ok(
            "codex_config_secrets",
            format!(
                "{} is tracked by git and carries no secret shape",
                path.display()
            ),
        );
    }
    Check::fail(
        "codex_config_secrets",
        format!(
            "{} is tracked by git and carries a secret shape on {} {} — git has published it",
            path.display(),
            if count == 1 { "line" } else { "lines" },
            secret_lines
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        ),
    )
    .remedy(mcp::MIGRATE_REMEDY)
}

/// Is what `sync` mirrored still what is on disk?
///
/// Three input states, not two — see [`sync_state_verdict`]. The manifest file's
/// existence is asked separately from loading it because [`manifest::Manifest::load`]
/// answers "no file" with an empty ledger, which is the right answer for a run
/// and the wrong one for a report.
fn sync_state(ctx: &Ctx) -> Check {
    let path = manifest::Manifest::path(ctx.home.root());
    if !path.exists() {
        return sync_state_verdict(None);
    }
    match manifest::Manifest::load(ctx.home.root()) {
        Err(e) => sync_state_verdict(Some(Err(&e.message))),
        Ok(loaded) => {
            // A machine with a ledger but no resolvable codex home is a machine
            // whose destinations are all missing, which is what the summary will
            // say — an honest answer rather than a skipped check.
            let codex_home = ctx.codex_home.clone().unwrap_or_default();
            let summary = manifest::drift_summary(&loaded, &codex_home);
            sync_state_verdict(Some(Ok(&summary)))
        }
    }
}

/// `None` = no manifest, `Some(Err(detail))` = corrupt or wrong schema version,
/// `Some(Ok(_))` = loaded.
///
/// Folding the middle state into `None` would report a corrupt ledger as `ok`
/// with "no sync manifest", inverting the truth for the one state that costs
/// the user the ability to update anything a2a owns.
///
/// Never a `fail`: a stale mirror does not stop a spawn.
fn sync_state_verdict(summary: Option<Result<&manifest::DriftSummary, &str>>) -> Check {
    let Some(loaded) = summary else {
        return Check::ok(
            "sync_state",
            "no sync manifest — `a2a sync skills`/`a2a sync mcp` have not run",
        );
    };
    let summary = match loaded {
        Err(detail) => {
            return Check::warn("sync_state", format!("unreadable ledger: {detail}"))
                .remedy(manifest::CORRUPT_REMEDY)
        }
        Ok(s) => s,
    };

    let stale = summary.drifted > 0
        || summary.missing_dest > 0
        || summary.orphaned_source > 0
        || summary.ignore_block_uncovered;
    if !stale {
        return Check::ok(
            "sync_state",
            format!(
                "{} manifest entry(s), every destination as a2a left it; last synced {}",
                summary.entries,
                summary
                    .last_synced_at
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_else(|| "never".into()),
            ),
        );
    }
    Check::warn(
        "sync_state",
        format!(
            "{} manifest entry(s): {} drifted, {} missing, {} orphaned sources, ignore block {}",
            summary.entries,
            summary.drifted,
            summary.missing_dest,
            summary.orphaned_source,
            if summary.ignore_block_uncovered {
                "uncovered"
            } else {
                "covers every managed dir"
            },
        ),
    )
    .remedy(sync_remedy(&summary.kinds_affected))
}

/// Only the commands that would fix something. A remedy naming both syncs when
/// one kind is clean is a remedy an operator learns to skim.
fn sync_remedy(kinds: &[crate::sync::Kind]) -> String {
    let commands: Vec<&str> = kinds
        .iter()
        .map(|k| match k {
            crate::sync::Kind::Skill => "a2a sync skills",
            crate::sync::Kind::Mcp => "a2a sync mcp",
        })
        .collect();
    if commands.is_empty() {
        return "a2a sync skills / a2a sync mcp".into();
    }
    commands.join(" / ")
}

// -------------------------------------------------------------------- helpers

/// `CODEX_HOME`, else `$HOME/.codex` — codex's own resolution, and the one the
/// child env allowlist forwards.
pub(crate) fn effective_codex_home() -> Option<PathBuf> {
    if let Some(v) = std::env::var_os("CODEX_HOME") {
        if !v.is_empty() {
            return Some(PathBuf::from(v));
        }
    }
    std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(|h| PathBuf::from(h).join(".codex"))
}

/// Same directory, symlinks and `/private/var` vs `/var` included — but a path
/// that does not exist is compared as written rather than declared different,
/// so "both are `~/.codex` and neither exists yet" is agreement, not a fail.
fn same_dir(a: &Path, b: &Path) -> bool {
    let real = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    a == b || real(a) == real(b)
}

/// `A2A_CODEX_BIN` / config `codex_bin` / PATH, in that order — the same
/// resolution `spawn` performs, plus the executability check spawn learns the
/// hard way. A bare name is searched on PATH; anything with a separator is
/// taken literally.
pub(crate) fn resolve_codex(cfg: &Config) -> Option<PathBuf> {
    let raw = cfg
        .codex_bin
        .clone()
        .unwrap_or_else(|| PathBuf::from("codex"));
    if raw.components().count() > 1 {
        return is_executable(&raw).then_some(raw);
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|d| d.join(&raw))
        .find(|c| is_executable(c))
}

pub(crate) fn is_executable(p: &Path) -> bool {
    std::fs::metadata(p)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn world_readable(p: &Path) -> bool {
    std::fs::metadata(p)
        .map(|m| m.permissions().mode() & 0o077 != 0)
        .unwrap_or(false)
}

fn first_line(s: &str) -> String {
    let line = s
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim();
    if line.chars().count() > 60 {
        format!("{}…", line.chars().take(60).collect::<String>())
    } else {
        line.to_string()
    }
}

/// A dotted version, compared component-wise with absent components as zero, so
/// `0.147` and `0.147.0` are the same release.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
struct Version(u64, u64, u64);

impl Version {
    /// The first `N.N[.N]` anywhere in the output — `codex --version` has been
    /// spelled `0.147.0`, `codex-cli 0.147.0` and `codex 0.147.0 (abc123)`.
    fn find(text: &str) -> Option<Version> {
        let re = regex::Regex::new(r"(\d+)\.(\d+)(?:\.(\d+))?").expect("static regex");
        let c = re.captures(text)?;
        let n = |i: usize| c.get(i).and_then(|m| m.as_str().parse().ok()).unwrap_or(0);
        Some(Version(n(1), n(2), n(3)))
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.0, self.1, self.2)
    }
}

/// One `>=0.140`-shaped clause of `codex_version_range`.
#[derive(PartialEq, Clone, Copy, Debug)]
struct Constraint {
    op: Op,
    version: Version,
}

#[derive(PartialEq, Clone, Copy, Debug)]
enum Op {
    Ge,
    Gt,
    Le,
    Lt,
    Eq,
}

impl Constraint {
    /// Every clause must parse or the whole range is rejected: silently
    /// dropping the half of `>=0.140, <0.150` that had a typo would leave a
    /// check that reports "in range" while enforcing nothing.
    fn parse_all(spec: &str) -> Option<Vec<Constraint>> {
        let cs: Vec<Constraint> = spec
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(Constraint::parse)
            .collect::<Option<Vec<_>>>()?;
        (!cs.is_empty()).then_some(cs)
    }

    fn parse(s: &str) -> Option<Constraint> {
        let (op, rest) = match s {
            _ if s.starts_with(">=") => (Op::Ge, &s[2..]),
            _ if s.starts_with("<=") => (Op::Le, &s[2..]),
            _ if s.starts_with('>') => (Op::Gt, &s[1..]),
            _ if s.starts_with('<') => (Op::Lt, &s[1..]),
            _ if s.starts_with("==") => (Op::Eq, &s[2..]),
            _ if s.starts_with('=') => (Op::Eq, &s[1..]),
            _ => (Op::Eq, s),
        };
        let rest = rest.trim();
        // `Version::find` would happily read a version out of `>=lol 0.1`;
        // require the clause to BE a version and nothing else.
        if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_digit() || c == '.') {
            return None;
        }
        Version::find(rest).map(|version| Constraint { op, version })
    }

    fn admits(&self, v: Version) -> bool {
        match self.op {
            Op::Ge => v >= self.version,
            Op::Gt => v > self.version,
            Op::Le => v <= self.version,
            Op::Lt => v < self.version,
            Op::Eq => v == self.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::manifest::DriftSummary;

    #[test]
    fn the_check_list_is_exactly_fifteen_named_checks() {
        // The list is the contract `doctor --json` publishes; a phase that adds
        // a check adds it here and to the acceptance test in the same commit.
        // Six in Phase 1, the allowed-roots check, four in Phase 2, the mesh
        // enrollment check, and three in Phase 3 — the ids and their order are
        // pinned end-to-end by
        // `tests/cmd_doctor.rs`.
        assert_eq!(CHECKS.len(), 15);
    }

    #[test]
    fn versions_are_found_in_every_spelling_codex_has_used() {
        assert_eq!(Version::find("0.147.0"), Some(Version(0, 147, 0)));
        assert_eq!(Version::find("codex-cli 0.99.0\n"), Some(Version(0, 99, 0)));
        assert_eq!(
            Version::find("codex 0.147.0 (abc123)"),
            Some(Version(0, 147, 0))
        );
        assert_eq!(Version::find("codex 1.2"), Some(Version(1, 2, 0)));
        // the stub prints exec JSONL, which carries no version at all
        assert_eq!(
            Version::find(r#"{"type":"turn.completed","id":"t1"}"#),
            None
        );
        assert_eq!(Version::find(""), None);
    }

    #[test]
    fn missing_components_are_zero_so_0_147_equals_0_147_0() {
        assert_eq!(Version::find("0.147").unwrap(), Version(0, 147, 0));
        assert!(Version(0, 147, 0) > Version(0, 140, 9));
        assert!(Version(0, 99, 0) < Version(0, 140, 0));
    }

    #[test]
    fn the_default_range_admits_the_pinned_window_and_nothing_else() {
        let cs = Constraint::parse_all(crate::config::DEFAULT_CODEX_VERSION_RANGE).unwrap();
        assert_eq!(cs.len(), 2);
        let admits = |v: Version| cs.iter().all(|c| c.admits(v));
        assert!(admits(Version(0, 140, 0)), "the lower bound is inclusive");
        assert!(admits(Version(0, 147, 0)));
        assert!(admits(Version(0, 149, 99)));
        assert!(!admits(Version(0, 150, 0)), "the upper bound is exclusive");
        assert!(!admits(Version(0, 99, 0)));
        assert!(!admits(Version(1, 0, 0)));
    }

    #[test]
    fn every_operator_parses_and_a_typo_rejects_the_whole_range() {
        assert_eq!(Constraint::parse(">1.2").unwrap().op, Op::Gt);
        assert_eq!(Constraint::parse("<=1.2").unwrap().op, Op::Le);
        assert_eq!(Constraint::parse("=1.2").unwrap().op, Op::Eq);
        assert_eq!(Constraint::parse("==1.2").unwrap().op, Op::Eq);
        assert_eq!(Constraint::parse("1.2").unwrap().op, Op::Eq);
        assert!(Constraint::parse(">=lol").is_none());
        assert!(Constraint::parse("").is_none());
        // one bad clause invalidates the range rather than half-enforcing it
        assert!(Constraint::parse_all(">=0.140, latest").is_none());
        assert!(Constraint::parse_all("   ").is_none());
    }

    #[test]
    fn only_fail_is_fatal_and_every_status_has_a_distinct_glyph() {
        assert_eq!(Status::Ok.as_str(), "ok");
        assert_eq!(Status::Warn.as_str(), "warn");
        assert_eq!(Status::Fail.as_str(), "fail");
        let glyphs = [Status::Ok, Status::Warn, Status::Fail].map(Status::glyph);
        assert_eq!(glyphs, ['✓', '⚠', '✗']);
    }

    #[test]
    fn a_check_serializes_id_status_detail_and_remedy() {
        let c = Check::fail("home_perms", "0755, must be 0700").remedy("chmod 700 ~/.a2a");
        let v = c.to_json();
        assert_eq!(v["id"], json!("home_perms"));
        assert_eq!(v["status"], json!("fail"));
        assert_eq!(v["detail"], json!("0755, must be 0700"));
        assert_eq!(v["remedy"], json!("chmod 700 ~/.a2a"));
        // a check with no remedy still carries the key, as null
        assert_eq!(
            Check::ok("layout_version", "v1").to_json()["remedy"],
            Value::Null
        );
    }

    #[test]
    fn a_bare_name_is_searched_on_path_and_a_path_is_taken_literally() {
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("codex");
        std::fs::write(&bin, "#!/bin/sh\n").unwrap();
        assert!(
            !is_executable(&bin),
            "a non-executable file is not a binary"
        );
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_executable(&bin));
        assert!(!is_executable(tmp.path()), "a directory is not a binary");

        let mut cfg = crate::config::Config {
            codex_bin: Some(bin.clone()),
            codex_version_range: crate::config::DEFAULT_CODEX_VERSION_RANGE.to_string(),
            allowed_roots: vec![],
            lanes: Default::default(),
            profiles: crate::config::default_profiles(),
            broker: Default::default(),
            admission: Default::default(),
            sync: Default::default(),
            update: Default::default(),
            mesh: Default::default(),
        };
        assert_eq!(resolve_codex(&cfg), Some(bin));
        cfg.codex_bin = Some(PathBuf::from("/nonexistent/codex"));
        assert_eq!(resolve_codex(&cfg), None);
    }

    fn hello(protocol_version: u32, binary_version: &str) -> wire::Hello {
        wire::Hello {
            protocol_version,
            binary_version: binary_version.to_string(),
        }
    }

    /// Absence is reported as absence. Only two *known* homes that disagree may
    /// fail — anything else would take every machine without a working
    /// app-server down with it, which is the opposite of what doctor is for.
    #[test]
    fn codex_home_only_fails_on_two_known_values_that_disagree() {
        let tmp = tempfile::tempdir().unwrap();
        let theirs = tmp.path().join("a");
        let ours = tmp.path().join("b");

        assert_eq!(codex_home_verdict(None, None).status, Status::Warn);
        assert_eq!(codex_home_verdict(None, Some(&ours)).status, Status::Warn);
        assert!(codex_home_verdict(None, Some(&ours))
            .detail
            .contains("not checked"));
        assert_eq!(codex_home_verdict(Some(&theirs), None).status, Status::Warn);

        assert_eq!(
            codex_home_verdict(Some(&theirs), Some(&theirs)).status,
            Status::Ok
        );
        // ... including when neither exists yet: equal strings are agreement.
        assert!(!theirs.exists());
        let mismatch = codex_home_verdict(Some(&theirs), Some(&ours));
        assert_eq!(mismatch.status, Status::Fail);
        assert!(mismatch.remedy.is_some(), "a fail always says what to type");
    }

    /// `/var` and `/private/var` are the same directory on macOS, and a symlink
    /// is the ordinary way `CODEX_HOME` gets set to a second name for one home.
    #[test]
    fn the_same_directory_under_two_names_is_not_a_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        std::fs::create_dir_all(&real).unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(same_dir(&real, &link));
        assert!(same_dir(&real, &real.join(".").join("")));
        assert!(!same_dir(&real, &tmp.path().join("other")));
    }

    #[test]
    fn auth_mode_warns_on_the_api_key_on_a_missing_login_and_on_both() {
        let tmp = tempfile::tempdir().unwrap();
        let auth = tmp.path().join(AUTH_JSON);

        let missing = auth_mode_verdict(false, Some(&auth));
        assert_eq!(missing.status, Status::Warn);
        assert!(missing.detail.contains(AUTH_JSON));

        std::fs::write(&auth, "{}").unwrap();
        assert_eq!(auth_mode_verdict(false, Some(&auth)).status, Status::Ok);

        let keyed = auth_mode_verdict(true, Some(&auth));
        assert_eq!(keyed.status, Status::Warn);
        assert!(keyed.detail.contains(API_KEY_ENV));

        // both problems are reported in ONE check — a total report never makes
        // an operator run the command twice to see the second thing.
        std::fs::remove_file(&auth).unwrap();
        let both = auth_mode_verdict(true, Some(&auth));
        assert!(both.detail.contains(API_KEY_ENV) && both.detail.contains(AUTH_JSON));

        // no home at all is still a warn, never a fail
        assert_eq!(auth_mode_verdict(false, None).status, Status::Warn);
    }

    /// Every branch of the broker verdict, including the two an integration
    /// test cannot stage without corrupting a live daemon's own health file.
    #[test]
    fn the_broker_verdict_fails_on_nothing_and_warns_on_exactly_four_things() {
        let disabled = broker_verdict(BrokerProbe::Disabled);
        assert_eq!(disabled.status, Status::Warn);
        assert!(disabled.detail.contains(brokerclient::DISABLE_ENV));

        let down = broker_verdict(BrokerProbe::Down {
            startable: Ok(PathBuf::from("/usr/local/bin/a2a")),
        });
        assert_eq!(
            down.status,
            Status::Ok,
            "start-on-demand is the normal state"
        );
        assert!(down.detail.contains("not running"));

        let unstartable = broker_verdict(BrokerProbe::Down {
            startable: Err("no a2a next to the running executable".into()),
        });
        assert_eq!(unstartable.status, Status::Warn);

        let up = broker_verdict(BrokerProbe::Up {
            peer: hello(wire::PROTOCOL_VERSION, "0.1.0"),
            health: Some(Box::new(BrokerHealth::default())),
            pid: Some(4321),
        });
        assert_eq!(up.status, Status::Ok);
        assert!(up.detail.contains("up") && up.detail.contains("protocol"));
        assert!(up.detail.contains("pid 4321") && up.detail.contains("0 worker(s)"));

        // the latch: the child's restart budget is spent, new spawns take exec
        let degraded = broker_verdict(BrokerProbe::Up {
            peer: hello(wire::PROTOCOL_VERSION, "0.1.0"),
            health: Some(Box::new(BrokerHealth {
                degraded: Some("exec".into()),
                degraded_since: Some(Utc::now()),
                degraded_reason: Some("the app-server child died 3 times".into()),
                ..BrokerHealth::default()
            })),
            pid: Some(4321),
        });
        assert_eq!(degraded.status, Status::Warn);
        assert!(degraded.detail.contains("DEGRADED") && degraded.detail.contains("died 3 times"));

        let skewed = broker_verdict(BrokerProbe::Skewed {
            peer: hello(wire::PROTOCOL_VERSION + 1, "0.2.0"),
            mine: wire::PROTOCOL_VERSION,
        });
        assert_eq!(skewed.status, Status::Warn);
        assert!(skewed.detail.contains("0.2.0"));

        let unreachable = broker_verdict(BrokerProbe::Unreachable("connection reset".into()));
        assert_eq!(unreachable.status, Status::Warn);

        // A broker is never a `fail`: exec still spawns workers without one.
        for c in [
            disabled,
            down,
            unstartable,
            up,
            degraded,
            skewed,
            unreachable,
        ] {
            assert_ne!(c.status, Status::Fail, "{}", c.detail);
        }
    }

    /// A broker that has bound its socket but not yet published a health file
    /// (and a peer that names no binary version) still reads as a sentence —
    /// no empty slot where a missing fact would have gone.
    #[test]
    fn a_broker_with_nothing_but_a_hello_is_still_described_in_words() {
        let up = broker_verdict(BrokerProbe::Up {
            peer: hello(wire::PROTOCOL_VERSION, ""),
            health: None,
            pid: None,
        });
        assert!(up.detail.contains("unknown"), "{}", up.detail);
        assert!(!up.detail.contains("(,"), "{}", up.detail);
        assert!(!up.detail.contains(", )"), "{}", up.detail);
        assert_eq!(
            up.detail,
            format!(
                "broker up (protocol v{}, binary unknown)",
                wire::PROTOCOL_VERSION
            )
        );
    }

    /// A migration nudge, never a blocker: the check has no `fail` branch at
    /// all, because a `fail` here would take every un-migrated repository's
    /// preflight down over a file Codex workers merely cannot see.
    #[test]
    fn context_canon_is_ok_without_a_claude_md_and_warns_only_on_a_missing_import() {
        let cwd = Path::new("/work/proj");

        let none = context_canon_verdict(None, cwd);
        assert_eq!(none.status, Status::Ok);
        assert_eq!(
            none.detail,
            "no CLAUDE.md in /work/proj — nothing to migrate"
        );

        for body in [
            "@AGENTS.md\n\n# Project\n",
            "# Project\n\n  @AGENTS.md  \n",
            "@AGENTS.md",
        ] {
            let c = context_canon_verdict(Some(body), cwd);
            assert_eq!(c.status, Status::Ok, "{body:?}");
            assert!(c.detail.contains("@AGENTS.md"), "{}", c.detail);
        }

        // A mention is not an import: the line has to BE the import.
        for body in [
            "# Project\n\nsee AGENTS.md\n",
            "read @AGENTS.md when you can\n",
            "",
        ] {
            let c = context_canon_verdict(Some(body), cwd);
            assert_eq!(c.status, Status::Warn, "{body:?}");
            assert_eq!(
                c.detail,
                "CLAUDE.md does not import @AGENTS.md — Codex workers read AGENTS.md and will not see it"
            );
            assert_eq!(
                c.remedy.as_deref(),
                Some(
                    "move machine-shared content into AGENTS.md and put `@AGENTS.md` at the top of CLAUDE.md"
                )
            );
        }
    }

    /// Doctor's first security-class `fail` — and the one thing it may never
    /// print is the reason it fired.
    #[test]
    fn codex_config_secrets_fails_only_on_tracked_and_secret_bearing() {
        let path = Path::new("/home/mike/.codex/config.toml");

        // No file (or no codex home at all) is nothing to check.
        assert_eq!(
            codex_config_secrets_verdict(None, false, &[]).status,
            Status::Ok
        );
        assert_eq!(
            codex_config_secrets_verdict(None, true, &[23]).status,
            Status::Ok
        );

        // Not a work tree / not tracked, with secrets: the DESIRED end state.
        let private = codex_config_secrets_verdict(Some(path), false, &[23, 41]);
        assert_eq!(private.status, Status::Ok);
        assert_eq!(
            private.detail,
            "2 secret-shaped line(s) in /home/mike/.codex/config.toml, which git does not track"
        );

        // Tracked but clean, and untracked and clean, are both ok.
        assert_eq!(
            codex_config_secrets_verdict(Some(path), true, &[]).status,
            Status::Ok
        );
        assert_eq!(
            codex_config_secrets_verdict(Some(path), false, &[]).status,
            Status::Ok
        );

        let published = codex_config_secrets_verdict(Some(path), true, &[23, 41]);
        assert_eq!(published.status, Status::Fail);
        assert!(published.detail.contains("/home/mike/.codex/config.toml"));
        assert!(
            published.detail.contains("lines 23, 41"),
            "{}",
            published.detail
        );
        assert_eq!(
            published.remedy.as_deref(),
            Some(crate::sync::mcp::MIGRATE_REMEDY)
        );
        // One line is one line, not "lines 23".
        assert!(codex_config_secrets_verdict(Some(path), true, &[23])
            .detail
            .contains("line 23"));
    }

    /// Three input states, not two. Folding "corrupt" into "absent" would
    /// report the one state that costs the user every future update as `ok`.
    #[test]
    fn sync_state_reports_absent_corrupt_and_loaded_as_three_different_things() {
        let absent = sync_state_verdict(None);
        assert_eq!(absent.status, Status::Ok);
        assert_eq!(
            absent.detail,
            "no sync manifest — `a2a sync skills`/`a2a sync mcp` have not run"
        );

        let corrupt = sync_state_verdict(Some(Err(
            "/home/mike/.a2a/sync-manifest.json: expected value at line 1 column 1",
        )));
        assert_eq!(
            corrupt.status,
            Status::Warn,
            "a broken ledger is not a spawn blocker"
        );
        assert!(corrupt
            .detail
            .contains("/home/mike/.a2a/sync-manifest.json"));
        assert!(
            corrupt.detail.contains("expected value"),
            "{}",
            corrupt.detail
        );
        assert_eq!(
            corrupt.remedy.as_deref(),
            Some(crate::sync::manifest::CORRUPT_REMEDY),
            "one sentence, from the loader and from here"
        );

        let clean = DriftSummary {
            entries: 4,
            last_synced_at: Some(
                chrono::DateTime::parse_from_rfc3339("2026-08-18T10:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
            ..DriftSummary::default()
        };
        let ok = sync_state_verdict(Some(Ok(&clean)));
        assert_eq!(ok.status, Status::Ok);
        assert!(ok.detail.contains('4'), "{}", ok.detail);
        assert!(ok.detail.contains("2026-08-18T10:00:00"), "{}", ok.detail);
        assert!(ok.remedy.is_none());

        // An empty-but-present ledger is still `ok`, and says when it last ran.
        let never = sync_state_verdict(Some(Ok(&DriftSummary::default())));
        assert_eq!(never.status, Status::Ok);
        assert!(never.detail.contains("never"), "{}", never.detail);
    }

    /// Each of the four drift conditions warns on its own, the counts are named
    /// in the report, and the remedy names only the kinds that are affected.
    #[test]
    fn sync_state_warns_on_each_drift_condition_and_never_fails() {
        let base = DriftSummary {
            entries: 3,
            kinds_affected: vec![crate::sync::Kind::Skill],
            ..DriftSummary::default()
        };
        let cases = [
            DriftSummary {
                drifted: 2,
                missing_dest: 1,
                ..base.clone()
            },
            DriftSummary {
                missing_dest: 1,
                ..base.clone()
            },
            DriftSummary {
                orphaned_source: 1,
                ..base.clone()
            },
            DriftSummary {
                ignore_block_uncovered: true,
                ..base.clone()
            },
        ];
        for summary in &cases {
            let c = sync_state_verdict(Some(Ok(summary)));
            assert_eq!(c.status, Status::Warn, "{:?}", summary);
            assert_eq!(c.remedy.as_deref(), Some("a2a sync skills"));
        }

        let named = sync_state_verdict(Some(Ok(&DriftSummary {
            drifted: 2,
            missing_dest: 1,
            ignore_block_uncovered: true,
            kinds_affected: vec![crate::sync::Kind::Skill, crate::sync::Kind::Mcp],
            ..base
        })));
        assert!(
            named
                .detail
                .contains("2 drifted, 1 missing, 0 orphaned sources, ignore block uncovered"),
            "{}",
            named.detail
        );
        assert_eq!(
            named.remedy.as_deref(),
            Some("a2a sync skills / a2a sync mcp")
        );

        // A stale mirror never stops a spawn.
        for c in [
            sync_state_verdict(None),
            sync_state_verdict(Some(Err("broken"))),
            sync_state_verdict(Some(Ok(&cases[0]))),
        ] {
            assert_ne!(c.status, Status::Fail, "{}", c.detail);
        }
    }

    #[test]
    fn first_line_is_bounded_and_skips_leading_blanks() {
        assert_eq!(first_line("\n\n  codex 0.1  \n more"), "codex 0.1");
        assert_eq!(first_line(""), "");
        let long = "x".repeat(200);
        let shown = first_line(&long);
        assert_eq!(shown.chars().count(), 61, "60 chars plus the ellipsis");
        assert!(shown.ends_with('…'));
    }
}
