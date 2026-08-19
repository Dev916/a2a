//! Layered configuration: defaults < `$A2A_HOME/config.toml` < environment.
//!
//! The precedence is an explicit, testable contract (spec §4.9): every effective
//! value carries an [`Origin`] so `a2a config show --origin` can name the layer
//! that produced it. Unknown config keys warn (exit 0) rather than failing;
//! malformed *values* fail loudly with the offending dotted key.

use crate::home::A2aHome;
use crate::output::{AppError, ExitCode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

pub const DEFAULT_MODEL: &str = "gpt-5.5";
/// The codex releases Phase 1's transport contract was written against
/// (spec §9 risk table: the exec JSONL shape is version-coupled). `doctor`
/// warns outside it — never fails, because a drifted codex is a thing to tell
/// an operator about, not a reason to refuse to run.
pub const DEFAULT_CODEX_VERSION_RANGE: &str = ">=0.140, <0.150";
pub const DEFAULT_START_TIMEOUT_SECS: u64 = 120;
pub const DEFAULT_TASK_TIMEOUT_SECS: u64 = 3600;
pub const DEFAULT_BLOCKED_TIMEOUT_SECS: u64 = 1800;

/// How long a broker with nothing live may sit before it retires itself.
///
/// This is the **leaked-daemon safety net**, not a performance knob: a broker
/// autostarted by one `a2a spawn` in a directory nobody returns to would
/// otherwise hold a socket, a lock and an app-server child forever. Five
/// minutes is long enough that an operator working in bursts never notices a
/// restart, and short enough that a forgotten daemon is gone before it is
/// mysterious. `0` disables the timer entirely.
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 300;

/// How large `events.jsonl` may grow before [`crate::store::EventLog`] rotates
/// it to `events.jsonl.1` (spec §4.5).
///
/// Ten mebibytes is a *read* budget, not a disk budget: every append reopens
/// the log and scans it to resume `seq`, so the cap is really "how much a
/// worker's own bookkeeping may cost per event". A flooding transport reaches
/// it in minutes; a normal worker never does.
///
/// **`0` means no rotation** — the file grows. That is the deliberate escape
/// hatch for anyone debugging a fold, where a log split across two files (and,
/// after a second rotation, a *discarded* generation) is exactly the thing that
/// makes the bug harder to see. It is safe as an unset-to-zero reading for the
/// same reason a cap of zero is unsafe elsewhere: this number's job is to
/// DISCARD, and "discard nothing" is the conservative direction.
pub const DEFAULT_EVENTS_MAX_BYTES: u64 = 10_485_760;

/// Every `[broker]` key, in the order `config show --origin` reports them.
pub const BROKER_FIELDS: [&str; 2] = ["idle_timeout_secs", "events_max_bytes"];

/// How many workers one broker will run at once (spec §4.9 admission control).
///
/// Eight is a *concurrency* limit, not a licence: every admitted worker is a
/// live model conversation against one shared app-server child, and the thing
/// that breaks first when this is too high is not a2a but the account's rate
/// limit. Spawns past the cap queue rather than fail, because an orchestrator
/// dispatching a fleet should not have to implement backpressure itself.
pub const DEFAULT_ADMISSION_GLOBAL_CAP: u64 = 8;

/// How many spawns may WAIT at once. The queue is bounded for the same reason
/// every channel in this system is: an unbounded backlog is a slower way to run
/// out of memory, and a caller who is 33 deep needs to know now rather than in
/// an hour.
pub const DEFAULT_ADMISSION_QUEUE_CAP: u64 = 32;

/// Per-lane share of the global cap. Half of it by default: one lane's fleet
/// must not be able to starve every other lane, which is the failure a single
/// global cap has no way to prevent.
pub const DEFAULT_ADMISSION_LANE_CAP: u64 = 4;

/// How many turns one epoch of a worker on this lane may open (spec §4.9).
///
/// A turn is one model round trip, so this is the *quota* budget: the thing
/// that stops a worker which has misread its brief from spending an account's
/// headroom re-reading it forever. The four numbers are the lanes' shapes — an
/// impl worker legitimately iterates, a review worker reads a diff and answers,
/// a research worker follows a few threads, a general worker is somewhere in
/// between.
///
/// `0` means "no turns allowed" and trips on the FIRST one, exactly like every
/// other cap in this file: the value a mis-set environment variable produces
/// must be the safe reading. Uncapping is spelled with a large number.
pub const DEFAULT_MAX_TURNS_IMPL: u64 = 40;
pub const DEFAULT_MAX_TURNS_REVIEW: u64 = 15;
pub const DEFAULT_MAX_TURNS_RESEARCH: u64 = 20;
pub const DEFAULT_MAX_TURNS_GENERAL: u64 = 30;

/// How many `failed` settlements inside [`DEFAULT_BREAKER_WINDOW_SECS`] open the
/// broker's circuit breaker and stop it admitting new work (spec §4.9).
///
/// Five is "this is not one bad brief". A fleet whose workers fail one after
/// another is failing for a reason the fleet cannot see — a revoked token, a
/// repo that will not build, a model outage — and every further spawn spends
/// quota discovering the same thing. Stopping is the cheap answer; the operator
/// re-opens the door with `a2a broker reset-breaker` once the cause is fixed.
///
/// **`0` DISABLES the breaker** — the one documented exception to the "`0` is
/// the safe reading" rule this file otherwise keeps. A threshold whose job is to
/// *stop* work must not read "stop everything" when it is unset: a breaker that
/// trips at zero is not caution, it is an outage on a fresh daemon.
pub const DEFAULT_BREAKER_FAILURES: u64 = 5;

/// The window those failures have to fall inside. Five minutes is long enough
/// that a genuine cascade is still counted as one, and short enough that five
/// unrelated failures spread across an afternoon are not.
pub const DEFAULT_BREAKER_WINDOW_SECS: u64 = 300;

/// How many attempts one brief gets before `a2a respawn` quarantines it
/// (spec §4.9). The original spawn is attempt 1, so the default buys two
/// retries.
///
/// Three is the poison-brief signature: a brief that has failed three times in
/// a row is not failing for a reason a fourth identical worker will discover,
/// and a retry loop with no ceiling is how one bad brief spends a day's quota.
/// The refusal names the chain so the operator fixes the brief rather than the
/// counter.
pub const DEFAULT_MAX_ATTEMPTS: u64 = 3;

/// Every `[admission]` key, in the order `config show --origin` reports them.
/// `lanes` is a nested TABLE, not a value, and is handled separately.
///
/// The breaker keys and `max_attempts` are APPENDED, per the phase-append
/// discipline; every entry here is parsed through [`parse_u64_str`] into a
/// `u64`, which is why `AdmissionConfig`'s numbers are `u64` too — a narrowing
/// `as u32` would wrap `4294967296` to `0`, and `0` is precisely the value that
/// turns the breaker off.
pub const ADMISSION_FIELDS: [&str; 5] = [
    "global_cap",
    "queue_cap",
    "breaker_failures",
    "breaker_window_secs",
    "max_attempts",
];

/// Every `[admission.lanes.<lane>]` key.
pub const ADMISSION_LANE_FIELDS: [&str; 1] = ["cap"];

/// Every `[sync]` key, in the order `config show --origin` reports them.
pub const SYNC_FIELDS: [&str; 3] = [
    "allow_skills",
    "allow_http_header_servers",
    "claude_config_dir",
];

/// Every per-lane key, in the order `config show --origin` reports them.
pub const LANE_FIELDS: [&str; 11] = [
    "sandbox",
    "network",
    "worktree_default",
    "model",
    "effort",
    "start_timeout_secs",
    "task_timeout_secs",
    "blocked_timeout_secs",
    // APPENDED LAST on purpose: this array is the order `config show --origin`
    // reports its keys in, and a new key inserted mid-list would renumber a
    // surface other tools read.
    "allow_elevation",
    // APPENDED LAST, same discipline: the turn budget joins the end of the
    // reported order rather than displacing a key another tool reads by index.
    "max_turns",
    // APPENDED LAST, same discipline again (spec §4.8). The key exists on every
    // lane for surface uniformity; only `lanes.impl.auto_review` is ever read.
    "auto_review",
];

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Lane {
    Impl,
    Review,
    Research,
    General,
}

impl Lane {
    pub const ALL: [Lane; 4] = [Lane::Impl, Lane::Review, Lane::Research, Lane::General];
    pub fn as_str(self) -> &'static str {
        match self {
            Lane::Impl => "impl",
            Lane::Review => "review",
            Lane::Research => "research",
            Lane::General => "general",
        }
    }
}

impl fmt::Display for Lane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Lane {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "impl" => Ok(Lane::Impl),
            "review" => Ok(Lane::Review),
            "research" => Ok(Lane::Research),
            "general" => Ok(Lane::General),
            other => Err(format!(
                "unknown lane `{other}` (expected impl|review|research|general)"
            )),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Copy, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode {
    WorkspaceWrite,
    OutboxOnly,
    DangerFullAccess,
}

impl SandboxMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SandboxMode::WorkspaceWrite => "workspace-write",
            SandboxMode::OutboxOnly => "outbox-only",
            SandboxMode::DangerFullAccess => "danger-full-access",
        }
    }
}

impl fmt::Display for SandboxMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SandboxMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "workspace-write" => Ok(SandboxMode::WorkspaceWrite),
            "outbox-only" => Ok(SandboxMode::OutboxOnly),
            "danger-full-access" => Ok(SandboxMode::DangerFullAccess),
            other => Err(format!(
                "unknown sandbox mode `{other}` (expected workspace-write|outbox-only|danger-full-access)"
            )),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LaneConfig {
    pub sandbox: SandboxMode,
    pub network: bool,
    pub worktree_default: bool,
    /// NON-optional: spec §4.7 forbids inheriting codex's global config.
    pub model: String,
    pub effort: String,
    pub start_timeout_secs: u64,
    pub task_timeout_secs: u64,
    pub blocked_timeout_secs: u64,
    /// May a spawn on this lane ask for `danger-full-access` (spec §5)?
    ///
    /// **`false` for every lane by default, and it is a *permission*, not an
    /// action**: it grants nothing on its own. An actual elevation additionally
    /// needs the per-spawn intent (`--danger-full-access`) and an audited
    /// `--reason`, so neither the config nor the flag can elevate a worker
    /// alone — which is the whole shape of the §5 gate.
    #[serde(default)]
    pub allow_elevation: bool,
    /// How many turns ONE EPOCH of a worker on this lane may open (spec §4.9).
    ///
    /// Counted by the fold (a `turn_started` envelope increments the snapshot's
    /// `turns_this_epoch`, an epoch bump resets it) and enforced by the broker's
    /// `WorkerActor` at the one moment it already knows a turn opened. `0` trips
    /// on the first turn — see [`DEFAULT_MAX_TURNS_IMPL`] for why the safe
    /// reading has to be the one an unset value produces.
    ///
    /// Deliberately NOT `#[serde(default)]`: a `0` conjured by a missing key
    /// would be a worker that can never take a turn, and this struct is only
    /// ever built by [`default_lane`] plus the layered overrides below.
    pub max_turns: u64,
    /// Turn a finished worker's worktree diff into a review-lane worker whose
    /// findings land as `<impl-worker>/review.json` (spec §4.8).
    ///
    /// **Only `lanes.impl.auto_review` is ever read.** The key exists on every
    /// lane because `LANE_FIELDS` is one table and a per-lane surface with a
    /// hole in it is a surface an operator has to remember the shape of — but
    /// auto-review reviews an *implementation's diff*, so setting it on
    /// `review`, `research` or `general` changes nothing at all (pinned by
    /// `only_the_impl_lanes_auto_review_key_is_ever_read`). Reviewing a review
    /// is a loop, and reviewing a lane that has no worktree is a no-op with
    /// extra steps.
    ///
    /// `false` everywhere by default: spawning a second worker off the back of
    /// the first spends an account's headroom, and that is an opt-in.
    #[serde(default)]
    pub auto_review: bool,
}

/// The `[broker]` section: what the daemon does about itself.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Debug)]
pub struct BrokerConfig {
    /// Seconds with nothing live before the daemon self-exits; `0` disables it.
    /// See [`DEFAULT_IDLE_TIMEOUT_SECS`] for why this exists at all.
    pub idle_timeout_secs: u64,
    /// Bytes of `events.jsonl` before it rotates; `0` disables rotation.
    /// See [`DEFAULT_EVENTS_MAX_BYTES`].
    pub events_max_bytes: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            idle_timeout_secs: DEFAULT_IDLE_TIMEOUT_SECS,
            events_max_bytes: DEFAULT_EVENTS_MAX_BYTES,
        }
    }
}

/// One lane's slice of the admission cap.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Debug)]
pub struct LaneAdmission {
    pub cap: u64,
}

/// The `[admission]` section: how many workers may run, and how many may wait
/// (spec §4.9).
///
/// Every cap is a `u64` and `0` means "admit nothing" rather than "unbounded" —
/// the value an operator types to close the door is the same value a mis-set
/// environment variable produces, so it must be the SAFE reading. Uncapping is
/// spelled with a large number, which is a thing you can only do on purpose.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct AdmissionConfig {
    pub global_cap: u64,
    pub queue_cap: u64,
    /// Failed settlements inside `breaker_window_secs` that open the circuit
    /// breaker. **`0` disables it** — see [`DEFAULT_BREAKER_FAILURES`].
    pub breaker_failures: u64,
    pub breaker_window_secs: u64,
    /// Attempts one brief gets before `a2a respawn` refuses (spec §4.9). The
    /// original spawn is attempt 1 — see [`DEFAULT_MAX_ATTEMPTS`].
    pub max_attempts: u64,
    pub lanes: BTreeMap<Lane, LaneAdmission>,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        AdmissionConfig {
            global_cap: DEFAULT_ADMISSION_GLOBAL_CAP,
            queue_cap: DEFAULT_ADMISSION_QUEUE_CAP,
            breaker_failures: DEFAULT_BREAKER_FAILURES,
            breaker_window_secs: DEFAULT_BREAKER_WINDOW_SECS,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            lanes: Lane::ALL
                .iter()
                .map(|l| {
                    (
                        *l,
                        LaneAdmission {
                            cap: DEFAULT_ADMISSION_LANE_CAP,
                        },
                    )
                })
                .collect(),
        }
    }
}

impl AdmissionConfig {
    /// This lane's cap, falling back to the global one for a lane the map
    /// somehow does not carry (a config written by a newer binary).
    pub fn lane_cap(&self, lane: Lane) -> u64 {
        self.lanes.get(&lane).map(|l| l.cap).unwrap_or(self.global_cap)
    }
}

/// The `[sync]` section: what `a2a sync` is allowed to mirror, and where it
/// reads the Claude side from (spec §6).
///
/// Both lists are empty by default because both are *overrides of a refusal*:
/// a skill is only mirrored into approval-free workers once somebody says it is
/// safe to, and a server's literal headers only reach a git-published file once
/// somebody says that file may carry them.
#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Clone, Debug)]
pub struct SyncConfig {
    /// Skill directory names whose side-effecting classification is overridden.
    pub allow_skills: Vec<String>,
    /// SERVER names (e.g. `"ghl"`), **not** header names.
    ///
    /// The `_servers` suffix is deliberate: `header_env_var(server, header)`
    /// sits right beside this key, and an allowlist that matched header names
    /// would silently invert the decision — `["locationId"]` would allowlist
    /// nothing at all while reading as though it had allowlisted the header it
    /// names. A unit test pins both readings.
    pub allow_http_header_servers: Vec<String>,
    /// Where the Claude side lives (`~/.claude-revenium` for a second account).
    /// `None` means "the home", which is the default account.
    pub claude_config_dir: Option<PathBuf>,
}

impl SyncConfig {
    /// Whether `server` may carry extra HTTP headers into the codex config.
    /// Takes a SERVER name — see [`SyncConfig::allow_http_header_servers`].
    pub fn allows_http_headers(&self, server: &str) -> bool {
        self.allow_http_header_servers.iter().any(|s| s == server)
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    /// `A2A_CODEX_BIN` env / `codex_bin` config key; else `codex` on PATH.
    pub codex_bin: Option<PathBuf>,
    /// Comma-separated version constraints (`>=0.140, <0.150`) that `doctor`
    /// holds the resolved codex to.
    pub codex_version_range: String,
    /// CANONICALIZED at load; default `[~/dev]`.
    pub allowed_roots: Vec<PathBuf>,
    pub lanes: BTreeMap<Lane, LaneConfig>,
    pub broker: BrokerConfig,
    pub admission: AdmissionConfig,
    pub sync: SyncConfig,
}

#[derive(Serialize, PartialEq, Clone, Copy, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    Default,
    File,
    Env,
}

impl Origin {
    pub fn as_str(self) -> &'static str {
        match self {
            Origin::Default => "default",
            Origin::File => "file",
            Origin::Env => "env",
        }
    }
}

pub type Origins = BTreeMap<String, Origin>;

/// Spec §4.9 lane table. `worktree_default` is TRUE for the impl lane and false
/// for every other one: impl is the lane that writes, and now that
/// `src/worktree.rs` exists the protection the spec asks for is real rather than
/// a warning (Phase 1 shipped it false because nothing could create the tree).
fn default_lane(lane: Lane) -> LaneConfig {
    let (sandbox, network, effort) = match lane {
        Lane::Impl => (SandboxMode::WorkspaceWrite, false, "high"),
        Lane::Review => (SandboxMode::OutboxOnly, false, "medium"),
        Lane::Research => (SandboxMode::OutboxOnly, true, "medium"),
        Lane::General => (SandboxMode::WorkspaceWrite, false, "medium"),
    };
    let max_turns = match lane {
        Lane::Impl => DEFAULT_MAX_TURNS_IMPL,
        Lane::Review => DEFAULT_MAX_TURNS_REVIEW,
        Lane::Research => DEFAULT_MAX_TURNS_RESEARCH,
        Lane::General => DEFAULT_MAX_TURNS_GENERAL,
    };
    LaneConfig {
        sandbox,
        network,
        worktree_default: lane == Lane::Impl,
        model: DEFAULT_MODEL.to_string(),
        effort: effort.to_string(),
        start_timeout_secs: DEFAULT_START_TIMEOUT_SECS,
        task_timeout_secs: DEFAULT_TASK_TIMEOUT_SECS,
        blocked_timeout_secs: DEFAULT_BLOCKED_TIMEOUT_SECS,
        // No lane may elevate until an operator says so, in writing, per lane.
        allow_elevation: false,
        max_turns,
        // Nobody's quota is spent on a review nobody asked for.
        auto_review: false,
    }
}

fn default_allowed_roots() -> Vec<PathBuf> {
    match std::env::var_os("HOME") {
        Some(h) => vec![PathBuf::from(h).join("dev")],
        None => vec![],
    }
}

fn cfg_err(msg: String) -> AppError {
    AppError::new("CONFIG_INVALID", ExitCode::Precondition, msg)
        .remedy("fix the key in $A2A_HOME/config.toml or the A2A_* environment override")
}

/// defaults < file < env. Returns the effective config, the per-key origin map
/// (dotted paths), and non-fatal warnings the caller prints to stderr.
pub fn load(home: &A2aHome) -> Result<(Config, Origins, Vec<String>), AppError> {
    let mut warnings: Vec<String> = Vec::new();
    let mut origins: Origins = BTreeMap::new();
    origins.insert("codex_bin".to_string(), Origin::Default);
    origins.insert("codex_version_range".to_string(), Origin::Default);
    origins.insert("allowed_roots".to_string(), Origin::Default);
    for field in BROKER_FIELDS {
        origins.insert(format!("broker.{field}"), Origin::Default);
    }
    for field in ADMISSION_FIELDS {
        origins.insert(format!("admission.{field}"), Origin::Default);
    }
    for field in SYNC_FIELDS {
        origins.insert(format!("sync.{field}"), Origin::Default);
    }

    let mut lanes = BTreeMap::new();
    for lane in Lane::ALL {
        lanes.insert(lane, default_lane(lane));
        for field in LANE_FIELDS {
            origins.insert(format!("lanes.{lane}.{field}"), Origin::Default);
        }
        for field in ADMISSION_LANE_FIELDS {
            origins.insert(format!("admission.lanes.{lane}.{field}"), Origin::Default);
        }
    }
    let mut cfg = Config {
        codex_bin: None,
        codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
        allowed_roots: default_allowed_roots(),
        lanes,
        broker: BrokerConfig::default(),
        admission: AdmissionConfig::default(),
        sync: SyncConfig::default(),
    };

    let path = home.root().join("config.toml");
    if path.exists() {
        let text = std::fs::read_to_string(&path).map_err(|e| {
            AppError::new(
                "CONFIG_READ",
                ExitCode::Precondition,
                format!("{}: {e}", path.display()),
            )
        })?;
        let table: toml::Table = text.parse().map_err(|e| {
            AppError::new(
                "CONFIG_PARSE",
                ExitCode::Precondition,
                format!("{}: {e}", path.display()),
            )
        })?;
        apply_file(&mut cfg, &mut origins, &mut warnings, &table)?;
    }
    apply_env(&mut cfg, &mut origins)?;

    cfg.allowed_roots = canonicalize_roots(std::mem::take(&mut cfg.allowed_roots), &mut warnings);
    append_implicit_worktrees_root(home, &mut cfg.allowed_roots);
    Ok((cfg, origins, warnings))
}

/// `$A2A_HOME/worktrees` is an allowed root by construction — this is the ONE
/// thing that makes the worktree spawn work on the app-server transport without
/// touching `src/broker/**`.
///
/// A worker spawned into its own worktree runs with that path as its cwd, and
/// the broker re-validates the wire cwd against the roots *this* function
/// produced (`config::load` is the single source for both legs). A home outside
/// every configured root would otherwise make every worktree spawn unspawnable
/// over the socket.
///
/// Three properties are pinned: it is appended only when the directory exists,
/// only when no configured root already contains it (so a home *inside* an
/// allowed root leaves the list byte-identical to Phase 1), and always **last**.
/// `origins` gains no new key — the implicit root rides under `allowed_roots`.
///
/// Accepted consequence, recorded rather than papered over: a caller may now
/// `--cwd $A2A_HOME/worktrees/<other-id>`. Same uid, same tree — spec §5 already
/// documents cross-worker isolation as honestly absent.
fn append_implicit_worktrees_root(home: &A2aHome, roots: &mut Vec<PathBuf>) {
    let Ok(worktrees) = std::fs::canonicalize(home.worktrees_dir()) else {
        return;
    };
    // `starts_with` is component-wise, and a path starts with itself — so this
    // is both the containment check and the no-duplicate check.
    if roots.iter().any(|root| worktrees.starts_with(root)) {
        return;
    }
    roots.push(worktrees);
}

/// Canonicalize each root (resolving symlinks such as macOS `/tmp` →
/// `/private/tmp`); entries that do not exist are dropped with a warning.
pub(crate) fn canonicalize_roots(raw: Vec<PathBuf>, warnings: &mut Vec<String>) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    for p in raw {
        match std::fs::canonicalize(&p) {
            Ok(c) => {
                if !out.contains(&c) {
                    out.push(c);
                }
            }
            Err(e) => warnings.push(format!(
                "dropping allowed_roots entry {} ({e})",
                p.display()
            )),
        }
    }
    out
}

// ---------------------------------------------------------------- file layer

fn apply_file(
    cfg: &mut Config,
    origins: &mut Origins,
    warnings: &mut Vec<String>,
    table: &toml::Table,
) -> Result<(), AppError> {
    for (key, value) in table {
        match key.as_str() {
            "codex_bin" => {
                cfg.codex_bin = Some(PathBuf::from(want_str("codex_bin", value)?));
                origins.insert("codex_bin".to_string(), Origin::File);
            }
            "codex_version_range" => {
                cfg.codex_version_range = want_str("codex_version_range", value)?.to_string();
                origins.insert("codex_version_range".to_string(), Origin::File);
            }
            "allowed_roots" => {
                let arr = value.as_array().ok_or_else(|| {
                    cfg_err(format!(
                        "allowed_roots: expected an array of strings, got {}",
                        value.type_str()
                    ))
                })?;
                let mut roots = Vec::new();
                for v in arr {
                    roots.push(PathBuf::from(want_str("allowed_roots[]", v)?));
                }
                cfg.allowed_roots = roots;
                origins.insert("allowed_roots".to_string(), Origin::File);
            }
            "broker" => {
                let fields = value.as_table().ok_or_else(|| {
                    cfg_err(format!(
                        "broker: expected a table, got {}",
                        value.type_str()
                    ))
                })?;
                for (field, v) in fields {
                    let dotted = format!("broker.{field}");
                    if !BROKER_FIELDS.contains(&field.as_str()) {
                        warnings.push(format!("unknown config key: {dotted}"));
                        continue;
                    }
                    apply_broker_toml(&mut cfg.broker, &dotted, field, v)?;
                    origins.insert(dotted, Origin::File);
                }
            }
            "admission" => apply_admission_file(cfg, origins, warnings, value)?,
            "lanes" => {
                let lanes = value.as_table().ok_or_else(|| {
                    cfg_err(format!("lanes: expected a table, got {}", value.type_str()))
                })?;
                for (lane_name, lane_val) in lanes {
                    let lane = match Lane::from_str(lane_name) {
                        Ok(l) => l,
                        Err(_) => {
                            warnings.push(format!("unknown config key: lanes.{lane_name}"));
                            continue;
                        }
                    };
                    let fields = lane_val.as_table().ok_or_else(|| {
                        cfg_err(format!(
                            "lanes.{lane}: expected a table, got {}",
                            lane_val.type_str()
                        ))
                    })?;
                    let lc = cfg.lanes.get_mut(&lane).expect("all lanes seeded");
                    for (field, v) in fields {
                        let dotted = format!("lanes.{lane}.{field}");
                        if !LANE_FIELDS.contains(&field.as_str()) {
                            warnings.push(format!("unknown config key: {dotted}"));
                            continue;
                        }
                        apply_lane_toml(lc, &dotted, field, v)?;
                        origins.insert(dotted, Origin::File);
                    }
                }
            }
            "sync" => apply_sync_file(cfg, origins, warnings, value)?,
            other => warnings.push(format!("unknown config key: {other}")),
        }
    }
    Ok(())
}

/// `[sync]` — two allowlists and a path.
///
/// Walked by hand like every other section, for the same reason: a typo in an
/// allowlist warns and the keys beside it still apply, so a misspelled skill
/// name cannot take `a2a config show` (or a spawn) down with it.
fn apply_sync_file(
    cfg: &mut Config,
    origins: &mut Origins,
    warnings: &mut Vec<String>,
    value: &toml::Value,
) -> Result<(), AppError> {
    let fields = value
        .as_table()
        .ok_or_else(|| cfg_err(format!("sync: expected a table, got {}", value.type_str())))?;
    for (field, v) in fields {
        let dotted = format!("sync.{field}");
        if !SYNC_FIELDS.contains(&field.as_str()) {
            warnings.push(format!("unknown config key: {dotted}"));
            continue;
        }
        match field.as_str() {
            "allow_skills" => cfg.sync.allow_skills = want_str_array(&dotted, v)?,
            "allow_http_header_servers" => {
                cfg.sync.allow_http_header_servers = want_str_array(&dotted, v)?
            }
            "claude_config_dir" => {
                cfg.sync.claude_config_dir = Some(PathBuf::from(want_str(&dotted, v)?))
            }
            _ => unreachable!("field membership fixed by SYNC_FIELDS"),
        }
        origins.insert(dotted, Origin::File);
    }
    Ok(())
}

/// `[admission]` and its ONE nested table, `[admission.lanes.<lane>]`.
///
/// Walked by hand rather than deserialized so that an unknown key warns and the
/// rest of the section still applies — the same rule every other section obeys,
/// and the reason a typo in a cap cannot take the CLI down with it.
fn apply_admission_file(
    cfg: &mut Config,
    origins: &mut Origins,
    warnings: &mut Vec<String>,
    value: &toml::Value,
) -> Result<(), AppError> {
    let fields = value.as_table().ok_or_else(|| {
        cfg_err(format!(
            "admission: expected a table, got {}",
            value.type_str()
        ))
    })?;
    for (field, v) in fields {
        if field == "lanes" {
            let lanes = v.as_table().ok_or_else(|| {
                cfg_err(format!(
                    "admission.lanes: expected a table, got {}",
                    v.type_str()
                ))
            })?;
            for (lane_name, lane_val) in lanes {
                let Ok(lane) = Lane::from_str(lane_name) else {
                    warnings.push(format!("unknown config key: admission.lanes.{lane_name}"));
                    continue;
                };
                let lane_fields = lane_val.as_table().ok_or_else(|| {
                    cfg_err(format!(
                        "admission.lanes.{lane}: expected a table, got {}",
                        lane_val.type_str()
                    ))
                })?;
                for (lane_field, lv) in lane_fields {
                    let dotted = format!("admission.lanes.{lane}.{lane_field}");
                    if !ADMISSION_LANE_FIELDS.contains(&lane_field.as_str()) {
                        warnings.push(format!("unknown config key: {dotted}"));
                        continue;
                    }
                    let cap = want_u64(&dotted, lv)?;
                    cfg.admission
                        .lanes
                        .insert(lane, LaneAdmission { cap });
                    origins.insert(dotted, Origin::File);
                }
            }
            continue;
        }
        let dotted = format!("admission.{field}");
        if !ADMISSION_FIELDS.contains(&field.as_str()) {
            warnings.push(format!("unknown config key: {dotted}"));
            continue;
        }
        match field.as_str() {
            "global_cap" => cfg.admission.global_cap = want_u64(&dotted, v)?,
            "queue_cap" => cfg.admission.queue_cap = want_u64(&dotted, v)?,
            "breaker_failures" => cfg.admission.breaker_failures = want_u64(&dotted, v)?,
            "breaker_window_secs" => cfg.admission.breaker_window_secs = want_u64(&dotted, v)?,
            "max_attempts" => cfg.admission.max_attempts = want_u64(&dotted, v)?,
            _ => unreachable!("field membership fixed by ADMISSION_FIELDS"),
        }
        origins.insert(dotted, Origin::File);
    }
    Ok(())
}

fn want_str<'a>(key: &str, v: &'a toml::Value) -> Result<&'a str, AppError> {
    v.as_str()
        .ok_or_else(|| cfg_err(format!("{key}: expected a string, got {}", v.type_str())))
}

/// An array of strings, with the element that is not one named by the same
/// dotted key as the array itself — `sync.allow_skills` is what an operator has
/// to go and fix, not `sync.allow_skills[2]`.
fn want_str_array(key: &str, v: &toml::Value) -> Result<Vec<String>, AppError> {
    let arr = v.as_array().ok_or_else(|| {
        cfg_err(format!(
            "{key}: expected an array of strings, got {}",
            v.type_str()
        ))
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        out.push(
            item.as_str()
                .ok_or_else(|| {
                    cfg_err(format!(
                        "{key}: expected an array of strings, got {}",
                        item.type_str()
                    ))
                })?
                .to_string(),
        );
    }
    Ok(out)
}

fn want_bool(key: &str, v: &toml::Value) -> Result<bool, AppError> {
    v.as_bool()
        .ok_or_else(|| cfg_err(format!("{key}: expected a boolean, got {}", v.type_str())))
}

fn want_u64(key: &str, v: &toml::Value) -> Result<u64, AppError> {
    match v.as_integer() {
        Some(i) if i >= 0 => Ok(i as u64),
        Some(i) => Err(cfg_err(format!(
            "{key}: expected a non-negative integer, got {i}"
        ))),
        None => Err(cfg_err(format!(
            "{key}: expected an integer, got {}",
            v.type_str()
        ))),
    }
}

fn apply_broker_toml(
    bc: &mut BrokerConfig,
    dotted: &str,
    field: &str,
    v: &toml::Value,
) -> Result<(), AppError> {
    match field {
        "idle_timeout_secs" => bc.idle_timeout_secs = want_u64(dotted, v)?,
        "events_max_bytes" => bc.events_max_bytes = want_u64(dotted, v)?,
        _ => unreachable!("field membership checked by caller"),
    }
    Ok(())
}

fn apply_lane_toml(
    lc: &mut LaneConfig,
    dotted: &str,
    field: &str,
    v: &toml::Value,
) -> Result<(), AppError> {
    match field {
        "sandbox" => {
            lc.sandbox = SandboxMode::from_str(want_str(dotted, v)?)
                .map_err(|e| cfg_err(format!("{dotted}: {e}")))?
        }
        "network" => lc.network = want_bool(dotted, v)?,
        "worktree_default" => lc.worktree_default = want_bool(dotted, v)?,
        "model" => lc.model = want_str(dotted, v)?.to_string(),
        "effort" => lc.effort = want_str(dotted, v)?.to_string(),
        "start_timeout_secs" => lc.start_timeout_secs = want_u64(dotted, v)?,
        "task_timeout_secs" => lc.task_timeout_secs = want_u64(dotted, v)?,
        "blocked_timeout_secs" => lc.blocked_timeout_secs = want_u64(dotted, v)?,
        "allow_elevation" => lc.allow_elevation = want_bool(dotted, v)?,
        "max_turns" => lc.max_turns = want_u64(dotted, v)?,
        "auto_review" => lc.auto_review = want_bool(dotted, v)?,
        _ => unreachable!("field membership checked by caller"),
    }
    Ok(())
}

// ----------------------------------------------------------------- env layer

/// `A2A_BROKER_<FIELD>` (upper-cased) — the env spelling of one `[broker]` key.
pub fn broker_env_key(field: &str) -> String {
    format!("A2A_BROKER_{}", field.to_ascii_uppercase())
}

/// `A2A_ADMISSION_<FIELD>` — the env spelling of one `[admission]` key.
pub fn admission_env_key(field: &str) -> String {
    format!("A2A_ADMISSION_{}", field.to_ascii_uppercase())
}

/// `A2A_ADMISSION_LANES_<LANE>_<FIELD>` — one `[admission.lanes.<lane>]` key.
pub fn admission_lane_env_key(lane: Lane, field: &str) -> String {
    format!(
        "A2A_ADMISSION_LANES_{}_{}",
        lane.as_str().to_ascii_uppercase(),
        field.to_ascii_uppercase()
    )
}

/// `A2A_SYNC_<FIELD>` — the env spelling of one `[sync]` key.
///
/// Note what this does NOT cover: `A2A_SYNC_ALLOW_REAL_HOME` is a run-time
/// opt-in read by `sync` itself, not a config key, so it never appears in
/// `config show` and cannot be set from a config file.
pub fn sync_env_key(field: &str) -> String {
    format!("A2A_SYNC_{}", field.to_ascii_uppercase())
}

/// `A2A_LANES_<LANE>_<FIELD>` (upper-cased) plus top-level `A2A_ALLOWED_ROOTS`
/// (colon-separated) and `A2A_CODEX_BIN`.
pub fn lane_env_key(lane: Lane, field: &str) -> String {
    format!(
        "A2A_LANES_{}_{}",
        lane.as_str().to_ascii_uppercase(),
        field.to_ascii_uppercase()
    )
}

fn apply_env(cfg: &mut Config, origins: &mut Origins) -> Result<(), AppError> {
    if let Some(v) = std::env::var_os("A2A_CODEX_BIN") {
        cfg.codex_bin = Some(PathBuf::from(v));
        origins.insert("codex_bin".to_string(), Origin::Env);
    }
    if let Some(v) = std::env::var_os("A2A_CODEX_VERSION_RANGE") {
        cfg.codex_version_range = v.to_string_lossy().into_owned();
        origins.insert("codex_version_range".to_string(), Origin::Env);
    }
    if let Some(v) = std::env::var_os("A2A_ALLOWED_ROOTS") {
        let raw = v.to_string_lossy().into_owned();
        cfg.allowed_roots = raw
            .split(':')
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from)
            .collect();
        origins.insert("allowed_roots".to_string(), Origin::Env);
    }
    for field in BROKER_FIELDS {
        let key = broker_env_key(field);
        let Some(raw) = std::env::var_os(&key) else {
            continue;
        };
        let raw = raw.to_string_lossy().into_owned();
        match field {
            "idle_timeout_secs" => {
                cfg.broker.idle_timeout_secs = parse_u64_str(&key, raw.trim())?
            }
            "events_max_bytes" => {
                cfg.broker.events_max_bytes = parse_u64_str(&key, raw.trim())?
            }
            _ => unreachable!("field membership fixed by BROKER_FIELDS"),
        }
        origins.insert(format!("broker.{field}"), Origin::Env);
    }
    for field in ADMISSION_FIELDS {
        let key = admission_env_key(field);
        let Some(raw) = std::env::var_os(&key) else {
            continue;
        };
        let raw = raw.to_string_lossy().into_owned();
        let value = parse_u64_str(&key, raw.trim())?;
        match field {
            "global_cap" => cfg.admission.global_cap = value,
            "queue_cap" => cfg.admission.queue_cap = value,
            "breaker_failures" => cfg.admission.breaker_failures = value,
            "breaker_window_secs" => cfg.admission.breaker_window_secs = value,
            "max_attempts" => cfg.admission.max_attempts = value,
            _ => unreachable!("field membership fixed by ADMISSION_FIELDS"),
        }
        origins.insert(format!("admission.{field}"), Origin::Env);
    }
    for lane in Lane::ALL {
        for field in ADMISSION_LANE_FIELDS {
            let key = admission_lane_env_key(lane, field);
            let Some(raw) = std::env::var_os(&key) else {
                continue;
            };
            let raw = raw.to_string_lossy().into_owned();
            let cap = parse_u64_str(&key, raw.trim())?;
            cfg.admission.lanes.insert(lane, LaneAdmission { cap });
            origins.insert(format!("admission.lanes.{lane}.{field}"), Origin::Env);
        }
        for field in LANE_FIELDS {
            let key = lane_env_key(lane, field);
            let Some(raw) = std::env::var_os(&key) else {
                continue;
            };
            let raw = raw.to_string_lossy().into_owned();
            let lc = cfg.lanes.get_mut(&lane).expect("all lanes seeded");
            apply_lane_str(lc, &key, field, raw.trim())?;
            origins.insert(format!("lanes.{lane}.{field}"), Origin::Env);
        }
    }
    for field in SYNC_FIELDS {
        let key = sync_env_key(field);
        let Some(raw) = std::env::var_os(&key) else {
            continue;
        };
        match field {
            "allow_skills" => cfg.sync.allow_skills = comma_list(&raw.to_string_lossy()),
            "allow_http_header_servers" => {
                cfg.sync.allow_http_header_servers = comma_list(&raw.to_string_lossy())
            }
            "claude_config_dir" => {
                // An empty value is "unset", not "the current directory" —
                // the same reading `doctor` gives an empty `CODEX_HOME`, and
                // the one that cannot turn an exported-but-blank variable into
                // a sync against a relative path. Taken as an OsString: a path
                // is not required to be UTF-8.
                if raw.is_empty() {
                    continue;
                }
                cfg.sync.claude_config_dir = Some(PathBuf::from(raw));
            }
            _ => unreachable!("field membership fixed by SYNC_FIELDS"),
        }
        origins.insert(format!("sync.{field}"), Origin::Env);
    }
    Ok(())
}

/// `A2A_SYNC_*` list values: comma-separated, blanks trimmed, empties dropped.
///
/// A trailing comma is a typo rather than an allowlist entry named "", and an
/// allowlist with an empty name in it would match nothing while looking like it
/// matched something.
fn comma_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_bool_str(key: &str, s: &str) -> Result<bool, AppError> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => Err(cfg_err(format!("{key}: expected a boolean, got `{other}`"))),
    }
}

fn parse_u64_str(key: &str, s: &str) -> Result<u64, AppError> {
    s.parse::<u64>().map_err(|e| {
        cfg_err(format!(
            "{key}: expected a non-negative integer, got `{s}` ({e})"
        ))
    })
}

fn apply_lane_str(lc: &mut LaneConfig, key: &str, field: &str, s: &str) -> Result<(), AppError> {
    match field {
        "sandbox" => {
            lc.sandbox = SandboxMode::from_str(s).map_err(|e| cfg_err(format!("{key}: {e}")))?
        }
        "network" => lc.network = parse_bool_str(key, s)?,
        "worktree_default" => lc.worktree_default = parse_bool_str(key, s)?,
        "model" => lc.model = s.to_string(),
        "effort" => lc.effort = s.to_string(),
        "start_timeout_secs" => lc.start_timeout_secs = parse_u64_str(key, s)?,
        "task_timeout_secs" => lc.task_timeout_secs = parse_u64_str(key, s)?,
        "blocked_timeout_secs" => lc.blocked_timeout_secs = parse_u64_str(key, s)?,
        "allow_elevation" => lc.allow_elevation = parse_bool_str(key, s)?,
        "max_turns" => lc.max_turns = parse_u64_str(key, s)?,
        "auto_review" => lc.auto_review = parse_bool_str(key, s)?,
        _ => unreachable!("field membership fixed by LANE_FIELDS"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_roots_are_canonicalized() {
        // /tmp is a symlink to /private/tmp on macOS; after canonicalization both spell the same path
        let roots = super::canonicalize_roots(vec!["/tmp".into()], &mut vec![]);
        assert_eq!(roots, vec![std::fs::canonicalize("/tmp").unwrap()]);
    }

    #[test]
    fn missing_allowed_root_is_dropped_with_a_warning() {
        let mut warnings = vec![];
        let roots = canonicalize_roots(
            vec!["/definitely/not/a/real/root".into(), "/tmp".into()],
            &mut warnings,
        );
        assert_eq!(roots.len(), 1);
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("/definitely/not/a/real/root"),
            "{warnings:?}"
        );
    }

    #[test]
    fn lane_defaults_follow_the_spec_table() {
        assert_eq!(
            default_lane(Lane::Impl).sandbox,
            SandboxMode::WorkspaceWrite
        );
        assert_eq!(default_lane(Lane::Impl).effort, "high");
        // The impl lane — and only the impl lane — defaults to its own tree.
        assert!(default_lane(Lane::Impl).worktree_default);
        for lane in [Lane::Review, Lane::Research, Lane::General] {
            assert!(!default_lane(lane).worktree_default, "{lane} must not");
        }
        assert_eq!(default_lane(Lane::Review).sandbox, SandboxMode::OutboxOnly);
        assert_eq!(default_lane(Lane::Review).effort, "medium");
        assert!(!default_lane(Lane::Review).network);
        assert!(default_lane(Lane::Research).network);
        assert_eq!(
            default_lane(Lane::General).sandbox,
            SandboxMode::WorkspaceWrite
        );
        for lane in Lane::ALL {
            let d = default_lane(lane);
            assert_eq!(d.model, "gpt-5.5");
            assert_eq!(d.start_timeout_secs, 120);
            assert_eq!(d.task_timeout_secs, 3600);
            assert_eq!(d.blocked_timeout_secs, 1800);
        }
    }

    /// The leaked-daemon safety net is 300 seconds, and it is a *value* — not
    /// whatever the constant happens to say today.
    #[test]
    fn the_broker_idle_timeout_defaults_to_five_minutes() {
        assert_eq!(DEFAULT_IDLE_TIMEOUT_SECS, 300);
        assert_eq!(BrokerConfig::default().idle_timeout_secs, 300);
        assert_eq!(broker_env_key("idle_timeout_secs"), "A2A_BROKER_IDLE_TIMEOUT_SECS");
    }

    /// The `events.jsonl` cap (spec §4.5): ten mebibytes, one env spelling, and
    /// `0` meaning **no rotation** rather than "rotate everything away" — the
    /// one direction that is safe when a number whose job is to discard is
    /// unset to zero.
    #[test]
    fn the_events_log_cap_defaults_to_ten_mebibytes() {
        assert_eq!(DEFAULT_EVENTS_MAX_BYTES, 10_485_760);
        assert_eq!(BrokerConfig::default().events_max_bytes, 10_485_760);
        assert_eq!(
            broker_env_key("events_max_bytes"),
            "A2A_BROKER_EVENTS_MAX_BYTES"
        );
        // Appended, not inserted: `config show --origin` reports `[broker]` in
        // this order and an operator's eye is the only index into it.
        assert_eq!(BROKER_FIELDS, ["idle_timeout_secs", "events_max_bytes"]);
    }

    #[test]
    fn the_broker_section_walks_its_keys_and_warns_on_unknown() {
        let table: toml::Table = "[broker]\nidle_timeout_secs = 45\nevents_max_bytes = 4096\nbogus = 1\n"
            .parse()
            .unwrap();
        let mut cfg = Config {
            codex_bin: None,
            codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
            broker: BrokerConfig::default(),
            admission: AdmissionConfig::default(),
            sync: SyncConfig::default(),
        };
        let mut origins: Origins = BTreeMap::new();
        let mut warnings = vec![];
        apply_file(&mut cfg, &mut origins, &mut warnings, &table).unwrap();
        assert_eq!(cfg.broker.idle_timeout_secs, 45);
        assert_eq!(cfg.broker.events_max_bytes, 4096);
        assert_eq!(origins.get("broker.idle_timeout_secs"), Some(&Origin::File));
        assert_eq!(origins.get("broker.events_max_bytes"), Some(&Origin::File));
        assert!(
            warnings.iter().any(|w| w.contains("broker.bogus")),
            "{warnings:?}"
        );
    }

    /// The admission surface is PINNED (spec §4.9): three numbers, three env
    /// spellings, and a per-lane table that does not disturb `LANE_FIELDS`.
    #[test]
    fn the_admission_surface_is_the_one_the_spec_pins() {
        let a = AdmissionConfig::default();
        assert_eq!(a.global_cap, 8);
        assert_eq!(a.queue_cap, 32);
        for lane in Lane::ALL {
            assert_eq!(a.lane_cap(lane), 4, "{lane} lane");
        }
        // The circuit breaker's two keys, APPENDED in this order (spec §4.9).
        // `breaker_failures = 0` DISABLES the breaker — the one recorded
        // exception to this file's "0 is the safe reading" rule.
        assert_eq!(a.breaker_failures, 5);
        assert_eq!(a.breaker_window_secs, 300);
        // ...and the respawn ceiling, appended after THEM (spec §4.9). The
        // original spawn is attempt 1, so `3` buys two retries.
        assert_eq!(a.max_attempts, 3);
        assert_eq!(
            ADMISSION_FIELDS,
            [
                "global_cap",
                "queue_cap",
                "breaker_failures",
                "breaker_window_secs",
                "max_attempts"
            ]
        );
        assert_eq!(ADMISSION_LANE_FIELDS, ["cap"]);
        assert_eq!(admission_env_key("global_cap"), "A2A_ADMISSION_GLOBAL_CAP");
        assert_eq!(admission_env_key("queue_cap"), "A2A_ADMISSION_QUEUE_CAP");
        assert_eq!(
            admission_env_key("breaker_failures"),
            "A2A_ADMISSION_BREAKER_FAILURES"
        );
        assert_eq!(
            admission_env_key("breaker_window_secs"),
            "A2A_ADMISSION_BREAKER_WINDOW_SECS"
        );
        assert_eq!(
            admission_env_key("max_attempts"),
            "A2A_ADMISSION_MAX_ATTEMPTS"
        );
        assert_eq!(
            admission_lane_env_key(Lane::Impl, "cap"),
            "A2A_ADMISSION_LANES_IMPL_CAP"
        );
        // The layering test's key set is stable: admission is its own section.
        assert_eq!(LANE_FIELDS.len(), 11);
        assert!(!LANE_FIELDS.contains(&"cap"));
        // The §5 permission is a LANE key, appended after the Phase-1 block so
        // the reported order of every key before it is untouched.
        assert!(LANE_FIELDS.contains(&"allow_elevation"));
        // ...and the Phase-5 turn budget is appended after THAT, for the same
        // reason. Whatever is last is the newest key, always.
        assert!(LANE_FIELDS.contains(&"max_turns"));
        // ...and auto_review after THAT. Whatever is last is the newest key.
        assert_eq!(*LANE_FIELDS.last().unwrap(), "auto_review");
    }

    /// `auto_review`'s surface (spec §4.8): a per-lane bool that is `false`
    /// everywhere, with the ordinary env spelling — and a documented asymmetry,
    /// namely that only the impl lane's copy is ever consulted.
    #[test]
    fn the_auto_review_surface_is_the_one_the_spec_pins() {
        for lane in Lane::ALL {
            assert!(!default_lane(lane).auto_review, "{lane}");
        }
        assert_eq!(
            lane_env_key(Lane::Impl, "auto_review"),
            "A2A_LANES_IMPL_AUTO_REVIEW"
        );
        assert_eq!(
            lane_env_key(Lane::Review, "auto_review"),
            "A2A_LANES_REVIEW_AUTO_REVIEW"
        );
        let mut cfg = Config {
            codex_bin: None,
            codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
            broker: BrokerConfig::default(),
            admission: AdmissionConfig::default(),
            sync: SyncConfig::default(),
        };
        let mut origins: Origins = BTreeMap::new();
        let mut warnings = vec![];
        let table: toml::Table =
            "[lanes.impl]\nauto_review = true\n[lanes.review]\nauto_review = true\n"
                .parse()
                .unwrap();
        apply_file(&mut cfg, &mut origins, &mut warnings, &table).unwrap();
        assert!(cfg.lanes[&Lane::Impl].auto_review);
        // It PARSES on every lane (one table, one surface) — what it does not do
        // is mean anything anywhere but `impl`; that half is pinned in
        // `tests/auto_review.rs`, which is the only place behaviour lives.
        assert!(cfg.lanes[&Lane::Review].auto_review);
        assert_eq!(origins.get("lanes.impl.auto_review"), Some(&Origin::File));

        let bad: toml::Table = "[lanes.impl]\nauto_review = \"yes\"\n".parse().unwrap();
        let err = apply_file(&mut cfg, &mut origins, &mut warnings, &bad).unwrap_err();
        assert_eq!(err.code, "CONFIG_INVALID");
        assert!(
            err.message.contains("lanes.impl.auto_review"),
            "the failure names the dotted key: {}",
            err.message
        );
    }

    /// The turn-budget surface is PINNED (spec §4.9): four per-lane numbers,
    /// one env spelling, and `0` reading as "no turns allowed" rather than as
    /// "unbounded" — the same safe direction every other cap in this file has.
    #[test]
    fn the_turn_budget_surface_is_the_one_the_spec_pins() {
        assert_eq!(default_lane(Lane::Impl).max_turns, 40);
        assert_eq!(default_lane(Lane::Review).max_turns, 15);
        assert_eq!(default_lane(Lane::Research).max_turns, 20);
        assert_eq!(default_lane(Lane::General).max_turns, 30);
        assert_eq!(DEFAULT_MAX_TURNS_IMPL, 40);
        assert_eq!(DEFAULT_MAX_TURNS_REVIEW, 15);
        assert_eq!(DEFAULT_MAX_TURNS_RESEARCH, 20);
        assert_eq!(DEFAULT_MAX_TURNS_GENERAL, 30);
        assert_eq!(
            lane_env_key(Lane::Impl, "max_turns"),
            "A2A_LANES_IMPL_MAX_TURNS"
        );
        assert_eq!(
            lane_env_key(Lane::General, "max_turns"),
            "A2A_LANES_GENERAL_MAX_TURNS"
        );
    }

    /// The file layer walks the new key like every other one: a good value
    /// lands with a `lanes.<lane>.max_turns` origin, a malformed one fails
    /// `CONFIG_INVALID` naming the dotted key, and an unknown sibling still
    /// only warns.
    #[test]
    fn the_lane_turn_budget_is_layered_parsed_and_named_when_it_is_wrong() {
        let mut cfg = Config {
            codex_bin: None,
            codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
            broker: BrokerConfig::default(),
            admission: AdmissionConfig::default(),
            sync: SyncConfig::default(),
        };
        let mut origins: Origins = BTreeMap::new();
        let mut warnings = vec![];
        let table: toml::Table = "[lanes.impl]\nmax_turns = 7\nmax_turnz = 3\n"
            .parse()
            .unwrap();
        apply_file(&mut cfg, &mut origins, &mut warnings, &table).unwrap();
        assert_eq!(cfg.lanes[&Lane::Impl].max_turns, 7);
        assert_eq!(
            origins.get("lanes.impl.max_turns"),
            Some(&Origin::File),
            "the origin key is the dotted path `config show --origin` reports"
        );
        assert!(
            warnings.iter().any(|w| w.contains("lanes.impl.max_turnz")),
            "an unknown sibling only warns: {warnings:?}"
        );

        let bad: toml::Table = "[lanes.review]\nmax_turns = \"lots\"\n".parse().unwrap();
        let err = apply_file(&mut cfg, &mut origins, &mut warnings, &bad).unwrap_err();
        assert_eq!(err.code, "CONFIG_INVALID");
        assert!(
            err.message.contains("lanes.review.max_turns"),
            "the failure names the dotted key: {}",
            err.message
        );
    }

    /// A lane the map does not carry (a config from a newer binary) falls back
    /// to the global cap rather than to zero — "unknown lane" must not mean
    /// "admit nothing".
    #[test]
    fn a_lane_with_no_entry_falls_back_to_the_global_cap() {
        let mut a = AdmissionConfig::default();
        a.lanes.remove(&Lane::Review);
        assert_eq!(a.lane_cap(Lane::Review), a.global_cap);
    }

    #[test]
    fn the_admission_section_walks_its_keys_and_warns_on_unknown() {
        let table: toml::Table = "[admission]\nglobal_cap = 3\nqueue_cap = 5\nbogus = 1\n\
                                  [admission.lanes.impl]\ncap = 2\nnope = 1\n\
                                  [admission.lanes.nosuchlane]\ncap = 9\n"
            .parse()
            .unwrap();
        let mut cfg = Config {
            codex_bin: None,
            codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
            broker: BrokerConfig::default(),
            admission: AdmissionConfig::default(),
            sync: SyncConfig::default(),
        };
        let mut origins: Origins = BTreeMap::new();
        let mut warnings = vec![];
        apply_file(&mut cfg, &mut origins, &mut warnings, &table).unwrap();
        assert_eq!(cfg.admission.global_cap, 3);
        assert_eq!(cfg.admission.queue_cap, 5);
        assert_eq!(cfg.admission.lane_cap(Lane::Impl), 2);
        assert_eq!(
            cfg.admission.lane_cap(Lane::Review),
            DEFAULT_ADMISSION_LANE_CAP,
            "one lane's override leaves the others alone"
        );
        assert_eq!(origins.get("admission.global_cap"), Some(&Origin::File));
        assert_eq!(
            origins.get("admission.lanes.impl.cap"),
            Some(&Origin::File)
        );
        for unknown in [
            "admission.bogus",
            "admission.lanes.impl.nope",
            "admission.lanes.nosuchlane",
        ] {
            assert!(
                warnings.iter().any(|w| w.contains(unknown)),
                "{unknown} warns: {warnings:?}"
            );
        }
    }

    #[test]
    fn a_malformed_cap_names_its_dotted_key() {
        let table: toml::Table = "[admission]\nglobal_cap = \"lots\"\n".parse().unwrap();
        let mut cfg = Config {
            codex_bin: None,
            codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
            broker: BrokerConfig::default(),
            admission: AdmissionConfig::default(),
            sync: SyncConfig::default(),
        };
        let err = apply_file(&mut cfg, &mut BTreeMap::new(), &mut vec![], &table).unwrap_err();
        assert_eq!(err.code, "CONFIG_INVALID");
        assert!(err.message.contains("admission.global_cap"), "{}", err.message);
    }

    #[test]
    fn lane_env_keys_are_upper_cased_dotted_paths() {
        assert_eq!(
            lane_env_key(Lane::Impl, "task_timeout_secs"),
            "A2A_LANES_IMPL_TASK_TIMEOUT_SECS"
        );
        assert_eq!(
            lane_env_key(Lane::Research, "network"),
            "A2A_LANES_RESEARCH_NETWORK"
        );
    }

    #[test]
    fn lane_and_sandbox_round_trip_through_strings() {
        for lane in Lane::ALL {
            assert_eq!(Lane::from_str(lane.as_str()).unwrap(), lane);
            assert_eq!(lane.to_string(), lane.as_str());
        }
        for m in [
            SandboxMode::WorkspaceWrite,
            SandboxMode::OutboxOnly,
            SandboxMode::DangerFullAccess,
        ] {
            assert_eq!(SandboxMode::from_str(m.as_str()).unwrap(), m);
        }
        assert!(Lane::from_str("nope").is_err());
        assert!(SandboxMode::from_str("nope").is_err());
    }

    #[test]
    fn file_layer_walks_known_keys_and_warns_on_unknown() {
        let table: toml::Table = r#"
            sandbx = "oops"
            [lanes.impl]
            task_timeout_secs = 60
            bogus = 1
            [lanes.nosuchlane]
            model = "x"
        "#
        .parse()
        .unwrap();
        let mut cfg = Config {
            codex_bin: None,
            codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
            broker: BrokerConfig::default(),
            admission: AdmissionConfig::default(),
            sync: SyncConfig::default(),
        };
        let mut origins: Origins = BTreeMap::new();
        let mut warnings = vec![];
        apply_file(&mut cfg, &mut origins, &mut warnings, &table).unwrap();
        assert_eq!(cfg.lanes[&Lane::Impl].task_timeout_secs, 60);
        assert_eq!(
            origins.get("lanes.impl.task_timeout_secs"),
            Some(&Origin::File)
        );
        assert!(
            warnings.iter().any(|w| w.contains("sandbx")),
            "{warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| w.contains("lanes.impl.bogus")),
            "{warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| w.contains("lanes.nosuchlane")),
            "{warnings:?}"
        );
    }

    #[test]
    fn file_layer_rejects_a_malformed_value() {
        let table: toml::Table = "[lanes.impl]\ntask_timeout_secs = \"soon\"\n"
            .parse()
            .unwrap();
        let mut cfg = Config {
            codex_bin: None,
            codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
            broker: BrokerConfig::default(),
            admission: AdmissionConfig::default(),
            sync: SyncConfig::default(),
        };
        let err = apply_file(&mut cfg, &mut BTreeMap::new(), &mut vec![], &table).unwrap_err();
        assert_eq!(err.code, "CONFIG_INVALID");
        assert!(
            err.message.contains("lanes.impl.task_timeout_secs"),
            "{}",
            err.message
        );
    }

    /// The `[sync]` section walks its keys like every other section: an
    /// unknown one warns and the allowlist beside it still applies.
    #[test]
    fn the_sync_section_walks_its_keys_and_warns_on_unknown() {
        let table: toml::Table = "[sync]\nallow_skills = [\"slack-cli\", \"pr\"]\n\
                                  allow_http_header_servers = [\"ghl\"]\n\
                                  claude_config_dir = \"/home/mike/.claude-revenium\"\n\
                                  allow_http_headers = [\"locationId\"]\n"
            .parse()
            .unwrap();
        let mut cfg = Config {
            codex_bin: None,
            codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
            broker: BrokerConfig::default(),
            admission: AdmissionConfig::default(),
            sync: SyncConfig::default(),
        };
        let mut origins: Origins = BTreeMap::new();
        let mut warnings = vec![];
        apply_file(&mut cfg, &mut origins, &mut warnings, &table).unwrap();
        assert_eq!(cfg.sync.allow_skills, ["slack-cli", "pr"]);
        assert_eq!(cfg.sync.allow_http_header_servers, ["ghl"]);
        assert_eq!(
            cfg.sync.claude_config_dir,
            Some(PathBuf::from("/home/mike/.claude-revenium"))
        );
        assert_eq!(origins.get("sync.allow_skills"), Some(&Origin::File));
        assert_eq!(origins.get("sync.claude_config_dir"), Some(&Origin::File));
        assert!(
            warnings.iter().any(|w| w.contains("sync.allow_http_headers")),
            "the near-miss spelling warns rather than silently doing nothing: {warnings:?}"
        );
    }

    #[test]
    fn a_malformed_sync_allowlist_names_its_dotted_key() {
        for text in [
            "[sync]\nallow_skills = 3\n",
            "[sync]\nallow_skills = [\"ok\", 3]\n",
        ] {
            let table: toml::Table = text.parse().unwrap();
            let mut cfg = Config {
                codex_bin: None,
                codex_version_range: DEFAULT_CODEX_VERSION_RANGE.to_string(),
                allowed_roots: vec![],
                lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
                broker: BrokerConfig::default(),
                admission: AdmissionConfig::default(),
                sync: SyncConfig::default(),
            };
            let err = apply_file(&mut cfg, &mut BTreeMap::new(), &mut vec![], &table).unwrap_err();
            assert_eq!(err.code, "CONFIG_INVALID");
            assert!(
                err.message
                    .contains("sync.allow_skills: expected an array of strings, got integer"),
                "{}",
                err.message
            );
        }
    }

    #[test]
    fn sync_env_keys_are_upper_cased_field_names() {
        assert_eq!(SYNC_FIELDS.len(), 3);
        assert_eq!(sync_env_key("allow_skills"), "A2A_SYNC_ALLOW_SKILLS");
        assert_eq!(
            sync_env_key("allow_http_header_servers"),
            "A2A_SYNC_ALLOW_HTTP_HEADER_SERVERS"
        );
        assert_eq!(
            sync_env_key("claude_config_dir"),
            "A2A_SYNC_CLAUDE_CONFIG_DIR"
        );
        assert_eq!(comma_list(" x-cli , docusign ,"), ["x-cli", "docusign"]);
        assert!(comma_list("  ").is_empty());
    }

    /// The allowlist holds SERVER names, not header names. Both readings are
    /// plausible to somebody skimming the key, and they differ in the direction
    /// that matters: matching header names would allowlist nothing while
    /// reading as though it had allowlisted something.
    #[test]
    fn the_http_header_allowlist_holds_server_names_not_header_names() {
        let by_server = SyncConfig {
            allow_http_header_servers: vec!["ghl".to_string()],
            ..Default::default()
        };
        assert!(by_server.allows_http_headers("ghl"));
        assert!(!by_server.allows_http_headers("linear"));

        let by_header = SyncConfig {
            allow_http_header_servers: vec!["locationId".to_string()],
            ..Default::default()
        };
        assert!(
            !by_header.allows_http_headers("ghl"),
            "naming the HEADER allowlists no server at all"
        );
        assert!(SyncConfig::default().allow_http_header_servers.is_empty());
    }

    /// The implicit worktrees root: appended LAST when the home sits outside
    /// every configured root, and completely absent when a configured root
    /// already contains it — which is what keeps the two broker tests that pin
    /// `allowed_roots` green without an edit.
    #[test]
    fn the_implicit_worktrees_root_is_appended_only_when_nothing_covers_it() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = A2aHome::resolve(Some(&tmp.path().join("outside"))).unwrap();
        let canon_wt = std::fs::canonicalize(outside.worktrees_dir()).unwrap();
        let elsewhere = tmp.path().join("elsewhere");
        std::fs::create_dir_all(&elsewhere).unwrap();
        let elsewhere = std::fs::canonicalize(&elsewhere).unwrap();

        let mut roots = vec![elsewhere.clone()];
        append_implicit_worktrees_root(&outside, &mut roots);
        assert_eq!(roots, vec![elsewhere.clone(), canon_wt.clone()], "appended last");
        // Idempotent: a second pass adds nothing, because the root now covers it.
        append_implicit_worktrees_root(&outside, &mut roots);
        assert_eq!(roots.len(), 2, "never duplicated: {roots:?}");

        // A home INSIDE an allowed root leaves the list byte-identical.
        let covered = std::fs::canonicalize(tmp.path()).unwrap();
        let mut roots = vec![covered.clone()];
        append_implicit_worktrees_root(&outside, &mut roots);
        assert_eq!(roots, vec![covered]);

        // No worktrees directory (a home nothing has resolved) ⇒ no root.
        let unresolved = A2aHome::adopt(tmp.path().join("never-made"));
        let mut roots: Vec<PathBuf> = vec![];
        append_implicit_worktrees_root(&unresolved, &mut roots);
        assert!(roots.is_empty(), "{roots:?}");
    }
}
