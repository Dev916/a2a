//! The broker's private `codex app-server` child, and the policy that keeps it
//! alive (spec §4.2).
//!
//! One child serves every worker, so its death is not one worker's problem —
//! it is the transport's. Everything here exists to make that survivable and,
//! crucially, *bounded*:
//!
//! * **[`launch`]** spawns the child with the scrubbed env allowlist
//!   ([`crate::rpc::Client::spawn`]) and completes the handshake, so a
//!   `Launched` is a child that has already proved it speaks the protocol. A
//!   process that starts and then fails `initialize` is not a live transport,
//!   and treating it as one is how a broker ends up "up" with every worker
//!   silently wedged.
//! * **[`RestartPolicy`] / [`RestartBudget`]** bound how hard the broker tries.
//!   Three restarts in sixty seconds is a transport that is not coming back;
//!   past that the broker latches degraded rather than spinning a fork bomb.
//!   The backoff is exponential (500ms → 30s) with jitter, because N brokers
//!   restarting in lockstep against one rate-limited backend is a thundering
//!   herd.
//! * **[`ProbeTracker`]** is the liveness half. Stdout EOF catches a child that
//!   *died*; nothing catches a child that is alive and wedged, so the broker
//!   asks it a real question on an interval ([`PROBE_METHOD`], verified live to
//!   exist and to be free) and calls two consecutive misses a death.
//!
//! The scheduling types here are deliberately pure — no I/O, no child, no
//! clock reads other than `tokio::time::Instant` — so the backoff curve and the
//! probe streak are unit-testable under `tokio::time::pause` in milliseconds,
//! while the restart path itself is integration-tested against a real child in
//! real time (`tests/broker_lifecycle.rs`).

use crate::adapter::{self, AdapterRef, InitInfo, APP_SERVER_ARGS};
use crate::exec;
use crate::output::{AppError, ExitCode};
use crate::rpc::{Client, Incoming};
use rand::Rng;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// The liveness question. Verified live on 0.140.0: it exists, it answers
/// `{data:[],nextCursor:null}` on an empty home, and it costs no tokens.
pub const PROBE_METHOD: &str = "thread/loaded/list";

/// How often the child is asked [`PROBE_METHOD`], unless [`PROBE_ENV`] says
/// otherwise.
pub const DEFAULT_PROBE_MS: u64 = 30_000;

/// Test seam: the probe interval in milliseconds. Tests drive the real restart
/// path in real time, which is only affordable with a short interval.
pub const PROBE_ENV: &str = "A2A_TEST_PROBE_MS";

/// Consecutive missed probes that count as a death. One is a slow answer; two
/// in a row, on a channel that is otherwise silent, is a wedged child.
pub const PROBE_DEATH_STREAK: u32 = 2;

/// How long a latched `degraded: exec` broker waits before probing whether the
/// app-server transport can be promoted back (spec §4.2).
pub const DEFAULT_REPROMOTION_MS: u64 = 300_000;

/// Test seam for [`DEFAULT_REPROMOTION_MS`], for the same reason as
/// [`PROBE_ENV`].
pub const REPROMOTION_ENV: &str = "A2A_TEST_REPROMOTE_MS";

/// Budget for the handshake on a freshly spawned child. Generous enough for a
/// cold start, short enough that a wedged binary is a restart rather than a
/// hang.
pub const INIT_TIMEOUT: Duration = Duration::from_secs(30);

/// The env prefixes the broker forwards from its OWN environment into the
/// child (Global Constraints). This is the test seam that drives the scripted
/// fake; production spawns match nothing here, because production sets none of
/// these variables.
pub const FORWARDED_ENV_PREFIXES: [&str; 2] = ["FAKE_", "A2A_TEST_"];

// ---------------------------------------------------------------------------
// restart policy
// ---------------------------------------------------------------------------

/// How hard, and how fast, the broker retries a dead child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartPolicy {
    /// Restarts allowed inside [`RestartPolicy::window`] before the broker
    /// latches degraded.
    pub max_restarts: u32,
    pub window: Duration,
    /// The first backoff; each subsequent attempt doubles it.
    pub base_backoff: Duration,
    /// The ceiling the doubling saturates at.
    pub max_backoff: Duration,
}

impl Default for RestartPolicy {
    /// Spec §4.2: three restarts in sixty seconds, exponential 500ms → 30s.
    fn default() -> Self {
        RestartPolicy {
            max_restarts: 3,
            window: Duration::from_secs(60),
            base_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
        }
    }
}

/// The jitter band applied to every backoff: ±25% of the nominal delay.
///
/// Not decoration. Every `a2a` broker on a machine (and every machine in a
/// fleet) restarts against the same backend; without jitter their retries stay
/// phase-locked forever and arrive as one spike.
pub const JITTER_FRACTION: f64 = 0.25;

impl RestartPolicy {
    /// The nominal delay before attempt `attempt` (0-based), before jitter:
    /// `base * 2^attempt`, saturated at `max_backoff`.
    pub fn nominal_backoff(&self, attempt: u32) -> Duration {
        let factor = 2u64.saturating_pow(attempt.min(32));
        let millis = (self.base_backoff.as_millis() as u64).saturating_mul(factor);
        Duration::from_millis(millis).min(self.max_backoff)
    }

    /// The inclusive band [`nominal_backoff`] is jittered within.
    pub fn backoff_band(&self, attempt: u32) -> (Duration, Duration) {
        let nominal = self.nominal_backoff(attempt).as_secs_f64();
        (
            Duration::from_secs_f64(nominal * (1.0 - JITTER_FRACTION)),
            Duration::from_secs_f64(nominal * (1.0 + JITTER_FRACTION)),
        )
    }

    /// The actual delay to sleep before attempt `attempt`.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let (low, high) = self.backoff_band(attempt);
        let span = high.as_secs_f64() - low.as_secs_f64();
        let roll: f64 = rand::thread_rng().gen_range(0.0..=1.0);
        Duration::from_secs_f64(low.as_secs_f64() + span * roll)
    }
}

/// The sliding window of recent restarts. Owned by the core task — like every
/// other piece of broker state, it is reached by message, never by lock.
#[derive(Debug)]
pub struct RestartBudget {
    policy: RestartPolicy,
    restarts: VecDeque<Instant>,
}

impl RestartBudget {
    pub fn new(policy: RestartPolicy) -> Self {
        RestartBudget {
            policy,
            restarts: VecDeque::new(),
        }
    }

    pub fn policy(&self) -> RestartPolicy {
        self.policy
    }

    /// Record one restart attempt and report how many are now inside the
    /// window (which is also the 1-based attempt number).
    pub fn record(&mut self, now: Instant) -> u32 {
        self.prune(now);
        self.restarts.push_back(now);
        self.restarts.len() as u32
    }

    /// True once the window holds more restarts than the policy allows: the
    /// transport is not coming back on its own.
    pub fn exceeded(&self) -> bool {
        self.restarts.len() as u32 > self.policy.max_restarts
    }

    pub fn in_window(&self) -> u32 {
        self.restarts.len() as u32
    }

    /// The 0-based attempt index for [`RestartPolicy::backoff`]: the first
    /// restart in a quiet window sleeps `base_backoff`.
    pub fn attempt(&self) -> u32 {
        self.restarts.len().saturating_sub(1) as u32
    }

    /// A child that came back and stayed: the window starts clean, so an
    /// unlucky hour does not spend a healthy day's budget.
    pub fn clear(&mut self) {
        self.restarts.clear();
    }

    fn prune(&mut self, now: Instant) {
        while let Some(oldest) = self.restarts.front() {
            if now.duration_since(*oldest) > self.policy.window {
                self.restarts.pop_front();
            } else {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// liveness probe
// ---------------------------------------------------------------------------

/// Consecutive-miss counter for the liveness probe. Pure: the task that owns it
/// does the sleeping and the asking, so the *rule* is testable without either.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ProbeTracker {
    misses: u32,
}

impl ProbeTracker {
    pub fn new() -> Self {
        ProbeTracker::default()
    }

    /// A probe answered: the streak resets. One slow answer is not a death.
    pub fn hit(&mut self) {
        self.misses = 0;
    }

    /// A probe timed out. Returns true when the child is to be declared dead.
    pub fn miss(&mut self) -> bool {
        self.misses += 1;
        self.misses >= PROBE_DEATH_STREAK
    }

    pub fn misses(&self) -> u32 {
        self.misses
    }
}

/// The probe interval, from [`PROBE_ENV`] or [`DEFAULT_PROBE_MS`]. A value that
/// does not parse (or is zero) falls back to the default rather than
/// busy-looping.
pub fn probe_interval() -> Duration {
    interval_from_env(PROBE_ENV, DEFAULT_PROBE_MS)
}

/// How long the broker stays latched on `degraded: exec` before re-probing.
pub fn repromotion_interval() -> Duration {
    interval_from_env(REPROMOTION_ENV, DEFAULT_REPROMOTION_MS)
}

/// Every timing supervision runs on, in one value.
///
/// Grouped — rather than read from the environment where they are used —
/// because the daemon should resolve its configuration exactly once at
/// startup, and because a test that had to `set_var` to shorten an interval
/// would be mutating the environment it shares with every other test in the
/// binary. Tests construct this directly; `a2a broker run` uses
/// [`Supervision::from_env`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Supervision {
    pub policy: RestartPolicy,
    pub probe_interval: Duration,
    pub repromotion_interval: Duration,
}

impl Supervision {
    pub fn from_env() -> Self {
        Supervision {
            policy: RestartPolicy::default(),
            probe_interval: probe_interval(),
            repromotion_interval: repromotion_interval(),
        }
    }
}

impl Default for Supervision {
    fn default() -> Self {
        Supervision::from_env()
    }
}

fn interval_from_env(key: &str, default_ms: u64) -> Duration {
    let ms = std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(default_ms);
    Duration::from_millis(ms)
}

// ---------------------------------------------------------------------------
// spawning
// ---------------------------------------------------------------------------

/// Everything needed to (re)create the child. Held by the core so a restart is
/// a replay of the original spawn, never a re-derivation of it: the binary and
/// the env are pinned at broker start, so a `PATH` that changes under a
/// long-lived daemon cannot silently swap the model backend mid-flight.
#[derive(Clone, Debug)]
pub struct ChildSpec {
    pub bin: PathBuf,
    pub args: Vec<String>,
    /// Explicit pairs handed to the child on top of the allowlist. The scrub
    /// list still wins ([`crate::rpc::is_scrubbed_env`]).
    pub extra_env: Vec<(String, String)>,
    /// Where the child's stderr is appended. Never inherited.
    pub stderr_log: PathBuf,
}

impl ChildSpec {
    /// The production spec: `<bin> app-server`, with the broker's own
    /// `FAKE_*`/`A2A_TEST_*` variables forwarded (in production there are
    /// none — see [`forwarded_test_env`]).
    pub fn app_server(bin: impl Into<PathBuf>, stderr_log: impl Into<PathBuf>) -> Self {
        ChildSpec {
            bin: bin.into(),
            args: APP_SERVER_ARGS.iter().map(|a| (*a).to_string()).collect(),
            extra_env: forwarded_test_env(),
            stderr_log: stderr_log.into(),
        }
    }
}

/// The broker's own `FAKE_*` / `A2A_TEST_*` variables, to be forwarded into the
/// child.
///
/// This is the ONLY channel by which anything beyond the allowlist reaches the
/// child, and it is empty in production: no `a2a` deployment sets either
/// prefix. It exists because the scripted fake app-server is configured
/// entirely through its environment, and the broker — not the test — is what
/// spawns it.
pub fn forwarded_test_env() -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| FORWARDED_ENV_PREFIXES.iter().any(|p| k.starts_with(p)))
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// codex identity (the drift gate, spec §4.2)
// ---------------------------------------------------------------------------

/// How long `<codex> --version` is given before the answer is treated as
/// unknown. A version check is not worth hanging a spawn on.
///
/// Generous rather than tight, and deliberately so: an unknown version is read
/// by [`CodexIdentity::drift_from`] as **drift**, so a probe that merely ran out
/// of budget on a loaded machine refuses a spawn that had nothing wrong with
/// it. The bound exists to stop a *wedged* binary hanging the daemon, and ten
/// seconds is still that — a real `codex --version` answers in milliseconds.
pub const VERSION_TIMEOUT: Duration = Duration::from_secs(10);

/// Which codex binary the broker is serving, and which build of it.
///
/// Pinned at broker start and re-checked on every spawn. The *path* alone is not
/// the identity: an `nvm` switch or a `codex update` replaces the binary behind
/// a stable path, and mixing two builds against one session store is precisely
/// what spec §4.2 forbids.
///
/// `version: None` means the binary answered nothing parseable — recorded as a
/// value so that "no version" is compared to "no version" and does not read as
/// drift on every spawn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodexIdentity {
    pub path: PathBuf,
    pub version: Option<String>,
}

impl CodexIdentity {
    /// The identity of the binary a [`ChildSpec`] would launch, probed with the
    /// same environment that child would get.
    pub fn of(spec: &ChildSpec) -> CodexIdentity {
        CodexIdentity::resolve(&spec.bin, &spec.extra_env)
    }

    /// Ask the binary who it is. Synchronous and cheap; callers on the runtime
    /// go through `spawn_blocking`.
    ///
    /// Three properties are not incidental, and each is a way this could wedge
    /// the daemon it runs inside:
    ///
    /// * **stdin is `/dev/null`.** `codex` (and the scripted fake) read stdin
    ///   when they are not handed a flag they recognize, and a probe that
    ///   inherits a terminal is a hang.
    /// * **The output goes to a FILE, never a pipe.** `Command::output()` reads
    ///   the pipe to EOF, and EOF means *every* holder of the write end has
    ///   closed it — including any process that happened to inherit the
    ///   descriptor across a concurrent fork. A pipe read with no deadline is
    ///   the one way a version check can outlive the thing it was checking.
    /// * **It is bounded** ([`VERSION_TIMEOUT`]) and the child is killed on
    ///   expiry: an unknown version is a fine answer, a stuck broker is not.
    ///
    /// The environment is the **child's** ([`crate::rpc::child_env`]), not the
    /// daemon's — the allowlist governs every process spawned from this binary,
    /// not just the long-lived one (spec §4.2).
    pub fn resolve(bin: &Path, extra_env: &[(String, String)]) -> CodexIdentity {
        CodexIdentity {
            path: bin.to_path_buf(),
            version: exec::codex_version(bin, &crate::rpc::child_env(extra_env), VERSION_TIMEOUT),
        }
    }

    /// What changed since this identity was recorded, or `None` when nothing
    /// did. The message is the operator-facing half of the refusal.
    pub fn drift_from(&self, now: &CodexIdentity) -> Option<String> {
        if self.path != now.path {
            return Some(format!(
                "the broker is serving `{}`, but `{}` is what would run now",
                self.path.display(),
                now.path.display()
            ));
        }
        if self.version != now.version {
            let show = |v: &Option<String>| v.clone().unwrap_or_else(|| "unknown".to_string());
            return Some(format!(
                "`{}` was `{}` when the broker started and is `{}` now",
                self.path.display(),
                show(&self.version),
                show(&now.version)
            ));
        }
        None
    }
}

/// A child that is spawned, handshaken, and ready to serve workers.
pub struct Launched {
    /// The handle every worker multiplexes over, carrying this child's pid.
    pub adapter: AdapterRef,
    /// The two inbound streams the core demuxes. Must be drained.
    pub incoming: Incoming,
    /// The process handle. Kept by the core: dropping it kills the child
    /// (`kill_on_drop`), which is exactly what a restart wants for the corpse.
    pub process: tokio::process::Child,
    pub info: InitInfo,
    pub pid: u32,
}

/// Spawn the child and complete the handshake.
///
/// Failure at either step is one error: from the broker's point of view a
/// binary that will not start and a binary that will not answer `initialize`
/// are the same fact — there is no transport — and both are exit 7 territory.
pub async fn launch(spec: &ChildSpec) -> Result<Launched, AppError> {
    let args: Vec<&str> = spec.args.iter().map(String::as_str).collect();
    let (client, incoming, process) =
        Client::spawn(&spec.bin, &args, &spec.extra_env, &spec.stderr_log)
            .await
            .map_err(|e| {
                start_error(format!(
                    "could not start `{} {}`: {e}",
                    spec.bin.display(),
                    spec.args.join(" ")
                ))
            })?;
    let pid = process.id().unwrap_or(0);
    let witness = exec::start_witness(pid).unwrap_or_default();

    let info = match adapter::initialize(&client, INIT_TIMEOUT).await {
        Ok(info) => info,
        Err(e) => {
            // A child that cannot handshake must not be left running: it holds
            // a `CODEX_HOME` lock and answers nothing.
            let mut process = process;
            drop(client);
            let _ = process.kill().await;
            return Err(start_error(format!(
                "`{} {}` did not complete the handshake: {e}",
                spec.bin.display(),
                spec.args.join(" ")
            )));
        }
    };

    Ok(Launched {
        adapter: AdapterRef::with_child(Arc::new(client), pid, witness),
        incoming,
        process,
        info,
        pid,
    })
}

fn start_error(message: String) -> AppError {
    AppError::new("APP_SERVER_START", ExitCode::BrokerUnavailable, message).remedy(
        "`a2a doctor` checks the resolved codex binary; `a2a broker restart` retries the child",
    )
}

/// The stderr sink for the child, inside the broker's home.
pub fn child_stderr_log(home: &Path) -> PathBuf {
    home.join(super::BROKER_LOG)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- backoff scheduling (pure, paused clock, no child) -------------------

    #[test]
    fn the_policy_is_the_one_the_spec_pins() {
        let p = RestartPolicy::default();
        assert_eq!(p.max_restarts, 3);
        assert_eq!(p.window, Duration::from_secs(60));
        assert_eq!(p.base_backoff, Duration::from_millis(500));
        assert_eq!(p.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn the_backoff_doubles_from_500ms_and_saturates_at_30s() {
        let p = RestartPolicy::default();
        assert_eq!(p.nominal_backoff(0), Duration::from_millis(500));
        assert_eq!(p.nominal_backoff(1), Duration::from_secs(1));
        assert_eq!(p.nominal_backoff(2), Duration::from_secs(2));
        assert_eq!(p.nominal_backoff(6), Duration::from_secs(30), "32s clamps");
        // No overflow, no wraparound, however long the outage lasts.
        assert_eq!(p.nominal_backoff(64), Duration::from_secs(30));
        assert_eq!(p.nominal_backoff(u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn every_backoff_is_jittered_inside_a_quarter_of_its_nominal_delay() {
        let p = RestartPolicy::default();
        for attempt in 0..8 {
            let (low, high) = p.backoff_band(attempt);
            let nominal = p.nominal_backoff(attempt);
            assert!(low < nominal && nominal < high, "band brackets the nominal");
            for _ in 0..64 {
                let d = p.backoff(attempt);
                assert!(d >= low && d <= high, "attempt {attempt}: {d:?} outside the band");
            }
        }
        // ... and it is actually random: two draws in a row being identical is
        // possible, sixteen is not.
        let draws: Vec<Duration> = (0..16).map(|_| p.backoff(3)).collect();
        assert!(
            draws.iter().any(|d| *d != draws[0]),
            "the jitter is not jittering — a fleet would restart in lockstep"
        );
    }

    /// The schedule itself, under a paused clock: no child, no I/O, and the
    /// whole 60-second window elapses in microseconds.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_restart_schedule_is_bounded_and_shaped() {
        let policy = RestartPolicy::default();
        let mut budget = RestartBudget::new(policy);
        let start = Instant::now();
        let mut attempts = 0;

        loop {
            let n = budget.record(Instant::now());
            if budget.exceeded() {
                assert_eq!(n, policy.max_restarts + 1, "the 4th is the one that latches");
                break;
            }
            attempts += 1;
            tokio::time::sleep(policy.backoff(budget.attempt())).await;
        }

        assert_eq!(attempts, 3, "spec §4.2: three restarts, then degrade");
        let elapsed = start.elapsed();
        // 500ms + 1s + 2s = 3.5s nominal, ±25%.
        assert!(
            elapsed >= Duration::from_millis(2_600) && elapsed <= Duration::from_millis(4_400),
            "the three backoffs took {elapsed:?}"
        );
    }

    /// The window slides: restarts spread thinly over hours never latch.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn restarts_outside_the_window_do_not_count_against_the_budget() {
        let policy = RestartPolicy::default();
        let mut budget = RestartBudget::new(policy);
        for _ in 0..10 {
            assert_eq!(budget.record(Instant::now()), 1, "each is alone in its window");
            assert!(!budget.exceeded());
            tokio::time::sleep(policy.window + Duration::from_secs(1)).await;
        }
        // Inside the window they accumulate.
        for expected in 1..=4 {
            assert_eq!(budget.record(Instant::now()), expected);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(budget.exceeded());
        budget.clear();
        assert!(!budget.exceeded());
        assert_eq!(budget.in_window(), 0);
    }

    // -- probe scheduling ----------------------------------------------------

    #[test]
    fn two_consecutive_misses_are_a_death_and_one_is_not() {
        let mut probe = ProbeTracker::new();
        assert!(!probe.miss(), "one missed probe is a slow answer");
        assert_eq!(probe.misses(), 1);
        probe.hit();
        assert_eq!(probe.misses(), 0, "an answer resets the streak");
        assert!(!probe.miss());
        assert!(probe.miss(), "two in a row is a wedged child");
    }

    /// The probe cadence, paused: N intervals cost exactly N intervals, and the
    /// verdict lands on the second consecutive miss — never on the first.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_probe_fires_on_its_interval_and_declares_death_on_the_second_miss() {
        let interval = Duration::from_millis(250);
        let mut probe = ProbeTracker::new();
        let start = Instant::now();
        // answered, answered, missed, answered, missed, missed → dead
        let script = [true, true, false, true, false, false];
        let mut ticks = 0;
        let mut dead_at = None;
        for answered in script {
            tokio::time::sleep(interval).await;
            ticks += 1;
            if answered {
                probe.hit();
            } else if probe.miss() {
                dead_at = Some(ticks);
                break;
            }
        }
        assert_eq!(dead_at, Some(6), "the death is the SECOND consecutive miss");
        assert_eq!(start.elapsed(), interval * 6, "no extra sleeping, no drift");
    }

    #[test]
    fn the_probe_interval_is_30s_unless_the_test_seam_says_otherwise() {
        // The default is a claim about production, so it is asserted directly.
        assert_eq!(DEFAULT_PROBE_MS, 30_000);
        assert_eq!(PROBE_METHOD, "thread/loaded/list");
        assert_eq!(DEFAULT_REPROMOTION_MS, 300_000, "spec §4.2: a 5-minute probe");
        // Parsing, without touching the process environment (which is shared by
        // every test in this binary).
        assert_eq!(interval_from_env("A2A_TEST_NOT_SET_ANYWHERE", 1234), Duration::from_millis(1234));
    }

    // -- env forwarding ------------------------------------------------------

    #[test]
    fn only_the_two_test_prefixes_are_forwarded_into_the_child() {
        assert_eq!(FORWARDED_ENV_PREFIXES, ["FAKE_", "A2A_TEST_"]);
        // The broker's own configuration must NOT ride along: the allowlist
        // applies to the child, and A2A_HOME/A2A_CODEX_BIN are the daemon's.
        for key in ["A2A_HOME", "A2A_CODEX_BIN", "A2A_ALLOWED_ROOTS", "PATH"] {
            assert!(
                !FORWARDED_ENV_PREFIXES.iter().any(|p| key.starts_with(p)),
                "{key} must not be forwarded as an explicit pair"
            );
        }
    }

    /// The drift comparison, without a binary: a changed path OR a changed
    /// version is drift, and two unknowns are not (or every spawn against a
    /// binary with no `--version` would be refused forever).
    #[test]
    fn drift_is_a_changed_path_or_a_changed_version_and_never_two_unknowns() {
        let at_start = CodexIdentity {
            path: PathBuf::from("/usr/bin/codex"),
            version: Some("codex-cli 0.140.0".into()),
        };
        assert_eq!(at_start.drift_from(&at_start.clone()), None);

        let upgraded = CodexIdentity {
            version: Some("codex-cli 0.147.0".into()),
            ..at_start.clone()
        };
        let detail = at_start.drift_from(&upgraded).expect("an upgrade is drift");
        assert!(detail.contains("0.140.0") && detail.contains("0.147.0"), "{detail}");

        let moved = CodexIdentity {
            path: PathBuf::from("/opt/codex"),
            ..at_start.clone()
        };
        assert!(at_start.drift_from(&moved).is_some(), "a moved binary is drift");

        let unknown = CodexIdentity {
            path: at_start.path.clone(),
            version: None,
        };
        assert_eq!(unknown.drift_from(&unknown.clone()), None);
        assert!(unknown.drift_from(&at_start).is_some(), "unknown → known is drift");
    }

    /// A binary that answers `--version` is read (first non-empty line); one
    /// that is not there at all is `None` rather than an error.
    #[test]
    fn the_identity_reads_the_first_line_of_the_version_output() {
        let id = CodexIdentity::resolve(Path::new("/bin/echo"), &[]);
        assert_eq!(id.path, PathBuf::from("/bin/echo"));
        let version = id.version.expect("echo answers something");
        assert!(!version.is_empty() && !version.contains('\n'), "{version:?}");
        assert_eq!(
            CodexIdentity::resolve(Path::new("/definitely/not/a/binary"), &[]).version,
            None
        );
    }

    /// A fork that fails is not a version that is missing.
    ///
    /// The drift gate reads `None` as drift, so a probe that lost a race with a
    /// busy process table would refuse a spawn that had nothing wrong with it.
    /// Two transient failures in a row are re-asked; the third attempt answers.
    #[test]
    fn a_transient_probe_failure_is_retried_rather_than_read_as_drift() {
        let dir = tempfile::tempdir().unwrap();
        let counter = dir.path().join("attempts");
        let bin = dir.path().join("flaky-codex");
        std::fs::write(
            &bin,
            format!(
                "#!/bin/sh\nn=$(cat {c} 2>/dev/null || echo 0)\nn=$((n+1))\necho $n > {c}\n\
                 [ \"$n\" -lt 3 ] && exit 1\necho 'codex-cli 0.140.0'\n",
                c = counter.display()
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();

        let id = CodexIdentity::resolve(&bin, &[]);
        assert_eq!(id.version.as_deref(), Some("codex-cli 0.140.0"));
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap().trim(),
            crate::exec::VERSION_ATTEMPTS.to_string(),
            "exactly {} attempts — bounded, not a loop",
            crate::exec::VERSION_ATTEMPTS
        );
    }

    /// A binary that never answers is killed and reported as unknown. The probe
    /// runs inside the broker's own startup, so "it hangs" is not an option it
    /// is allowed to have.
    #[test]
    fn a_binary_that_will_not_answer_is_bounded_and_killed() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("wedged-codex");
        std::fs::write(&bin, "#!/bin/sh\nsleep 300\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();

        let started = std::time::Instant::now();
        let id = CodexIdentity::resolve(&bin, &[]);
        assert_eq!(id.version, None, "a silent binary has no version");
        assert!(
            started.elapsed() < VERSION_TIMEOUT * 2,
            "the probe is bounded: {:?}",
            started.elapsed()
        );
        // ... and it left nothing behind in TMPDIR.
        let leftovers: Vec<_> = std::fs::read_dir(std::env::temp_dir())
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(&format!("a2a-codex-version-{}-", std::process::id()))
            })
            .collect();
        assert!(leftovers.is_empty(), "the probe cleaned up: {leftovers:?}");
    }

    #[test]
    fn the_spec_builds_the_app_server_argv() {
        let spec = ChildSpec::app_server("/usr/bin/codex", "/tmp/broker.log");
        assert_eq!(spec.args, vec!["app-server"]);
        assert_eq!(spec.bin, PathBuf::from("/usr/bin/codex"));
    }
}
