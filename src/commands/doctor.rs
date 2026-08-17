//! `a2a doctor [--json]` — the preflight an orchestrator runs before it trusts
//! the fleet (spec §4.1).
//!
//! Doctor's whole value is that it is **machine-readable and total**: every
//! check runs, every check reports, and one bad answer never hides the other
//! five. That shapes three decisions in this file.
//!
//! **The checks are data, not control flow.** [`CHECKS`] is a slice of
//! `fn(&Ctx) -> Check`; the runner maps over it and knows nothing about any
//! individual check. Phase 2's broker checks (socket reachable, version skew,
//! managed-daemon contention) are appended to that slice and the runner is not
//! touched. Six is the Phase-1 count, and it is pinned by a test.
//!
//! **Only `fail` is fatal.** `fail` means "a worker spawned right now would
//! not work" and exits 9 (precondition); `warn` means "this will bite you
//! later" and exits 0. The version check is the sharp case: an unrecognizable
//! `codex --version` is a warn, never a fail, because refusing to run over a
//! string we could not parse would make every codex release a hard outage.
//!
//! **Doctor adopts a home that other commands refuse.** Every other command
//! goes through [`A2aHome::resolve`], which gates on `LAYOUT_VERSION` and errors
//! out on a mismatch — correct there, useless here, since a home the binary
//! will not touch is exactly the thing an operator ran `doctor` to be told
//! about. So a mismatch is caught, the root is adopted ungated, and it comes
//! back as the `layout_version` check with a remedy.

use crate::config::{self, Config};
use crate::exec::{self, Liveness};
use crate::home::{self, A2aHome, LAYOUT_VERSION};
use crate::output::{AppError, ExitCode, Outcome};
use crate::reconcile::reconcile;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

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
/// "present" and "cannot run it" about two different binaries.
pub struct Ctx {
    pub home: A2aHome,
    pub cfg: Config,
    pub codex: Option<PathBuf>,
    pub now: DateTime<Utc>,
}

/// The Phase-1 check list, in report order. Append-only across phases.
const CHECKS: &[fn(&Ctx) -> Check] = &[
    codex_present,
    codex_version,
    layout_version,
    home_perms,
    workers_health,
    env_inheritance,
];

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
    let ctx = Ctx {
        codex: resolve_codex(&cfg),
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
        "Phase 1 exec workers inherit the CLI environment minus the scrub list \
         (OPENAI_API_KEY, AWS_*, DEPOT_TOKEN); the Phase 2 broker adds the full allowlist.",
    )
    .remedy("keep secrets out of the environment a2a itself runs in until the broker lands")
}

// -------------------------------------------------------------------- helpers

/// `A2A_CODEX_BIN` / config `codex_bin` / PATH, in that order — the same
/// resolution `spawn` performs, plus the executability check spawn learns the
/// hard way. A bare name is searched on PATH; anything with a separator is
/// taken literally.
fn resolve_codex(cfg: &Config) -> Option<PathBuf> {
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

fn is_executable(p: &Path) -> bool {
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

    #[test]
    fn the_phase_1_check_list_is_exactly_six_named_checks() {
        // The list is the contract `doctor --json` publishes; a phase that adds
        // a check adds it here and to the acceptance test in the same commit.
        assert_eq!(CHECKS.len(), 6);
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
        };
        assert_eq!(resolve_codex(&cfg), Some(bin));
        cfg.codex_bin = Some(PathBuf::from("/nonexistent/codex"));
        assert_eq!(resolve_codex(&cfg), None);
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
