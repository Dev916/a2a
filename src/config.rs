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
pub const DEFAULT_START_TIMEOUT_SECS: u64 = 120;
pub const DEFAULT_TASK_TIMEOUT_SECS: u64 = 3600;
pub const DEFAULT_BLOCKED_TIMEOUT_SECS: u64 = 1800;

/// Every per-lane key, in the order `config show --origin` reports them.
pub const LANE_FIELDS: [&str; 8] = [
    "sandbox",
    "network",
    "worktree_default",
    "model",
    "effort",
    "start_timeout_secs",
    "task_timeout_secs",
    "blocked_timeout_secs",
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
}

#[derive(Clone, Debug)]
pub struct Config {
    /// `A2A_CODEX_BIN` env / `codex_bin` config key; else `codex` on PATH.
    pub codex_bin: Option<PathBuf>,
    /// CANONICALIZED at load; default `[~/dev]`.
    pub allowed_roots: Vec<PathBuf>,
    pub lanes: BTreeMap<Lane, LaneConfig>,
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

/// Spec §4.9 lane table, with the Phase-1 exception that `worktree_default`
/// is false everywhere (worktrees land in Phase 4).
fn default_lane(lane: Lane) -> LaneConfig {
    let (sandbox, network, effort) = match lane {
        Lane::Impl => (SandboxMode::WorkspaceWrite, false, "high"),
        Lane::Review => (SandboxMode::OutboxOnly, false, "medium"),
        Lane::Research => (SandboxMode::OutboxOnly, true, "medium"),
        Lane::General => (SandboxMode::WorkspaceWrite, false, "medium"),
    };
    LaneConfig {
        sandbox,
        network,
        worktree_default: false,
        model: DEFAULT_MODEL.to_string(),
        effort: effort.to_string(),
        start_timeout_secs: DEFAULT_START_TIMEOUT_SECS,
        task_timeout_secs: DEFAULT_TASK_TIMEOUT_SECS,
        blocked_timeout_secs: DEFAULT_BLOCKED_TIMEOUT_SECS,
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
    origins.insert("allowed_roots".to_string(), Origin::Default);

    let mut lanes = BTreeMap::new();
    for lane in Lane::ALL {
        lanes.insert(lane, default_lane(lane));
        for field in LANE_FIELDS {
            origins.insert(format!("lanes.{lane}.{field}"), Origin::Default);
        }
    }
    let mut cfg = Config {
        codex_bin: None,
        allowed_roots: default_allowed_roots(),
        lanes,
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
    Ok((cfg, origins, warnings))
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
            other => warnings.push(format!("unknown config key: {other}")),
        }
    }
    Ok(())
}

fn want_str<'a>(key: &str, v: &'a toml::Value) -> Result<&'a str, AppError> {
    v.as_str()
        .ok_or_else(|| cfg_err(format!("{key}: expected a string, got {}", v.type_str())))
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
        _ => unreachable!("field membership checked by caller"),
    }
    Ok(())
}

// ----------------------------------------------------------------- env layer

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
    if let Some(v) = std::env::var_os("A2A_ALLOWED_ROOTS") {
        let raw = v.to_string_lossy().into_owned();
        cfg.allowed_roots = raw
            .split(':')
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from)
            .collect();
        origins.insert("allowed_roots".to_string(), Origin::Env);
    }
    for lane in Lane::ALL {
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
    Ok(())
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
        assert!(!default_lane(Lane::Impl).worktree_default);
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
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
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
            allowed_roots: vec![],
            lanes: Lane::ALL.iter().map(|l| (*l, default_lane(*l))).collect(),
        };
        let err = apply_file(&mut cfg, &mut BTreeMap::new(), &mut vec![], &table).unwrap_err();
        assert_eq!(err.code, "CONFIG_INVALID");
        assert!(
            err.message.contains("lanes.impl.task_timeout_secs"),
            "{}",
            err.message
        );
    }
}
