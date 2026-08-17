use crate::config::Lane;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "a2a", version, about = "Claude-orchestrated Codex workers")]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(clap::Args)]
pub struct GlobalArgs {
    /// Emit exactly one JSON envelope on stdout
    #[arg(long, global = true, env = "A2A_JSON")]
    pub json: bool,
    /// Relocate the a2a home tree (default: $A2A_HOME or ~/.a2a)
    #[arg(long, global = true)]
    pub home: Option<PathBuf>,
    #[arg(long, global = true)]
    pub quiet: bool,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// (hidden) canned typed error for contract tests
    #[command(hide = true, name = "__selftest-error")]
    SelftestError,
    /// (hidden) detached exec-transport shim: holds run.lock, wires the child's
    /// streams to the worker dir, and lands the exit code in `exit_code`.
    #[command(hide = true, name = "__exec-shim")]
    ExecShim {
        worker_dir: PathBuf,
        epoch: u32,
        prompt_file: PathBuf,
        /// codex argv after `--` (never parsed by us, never shell-interpreted)
        #[arg(last = true, required = true, num_args = 1.., allow_hyphen_values = true)]
        argv: Vec<String>,
    },
    /// Dispatch a Codex worker; prints its id
    Spawn(SpawnArgs),
    /// Reconcile one worker and report its state (exit code IS the state)
    Status {
        /// Worker id, or any unambiguous prefix of one
        id: Option<String>,
        /// Report the broker daemon instead of a worker — the spec §4.1
        /// spelling of `a2a broker status` (exit 7 when there is none)
        #[arg(long, conflicts_with = "id")]
        broker: bool,
    },
    /// List every worker (reconciling each): id, lane, state, dwell, last-event age
    Ls,
    /// Print a worker's events: `<seq> <ts> <kind>`, lifecycle only by default
    Tail {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// Only events after this seq (the cursor from a previous tail)
        #[arg(long, value_name = "SEQ")]
        since: Option<u64>,
        /// Print full event envelopes as JSONL instead of the projection
        #[arg(long)]
        raw: bool,
    },
    /// Print a worker's final result (exit code encodes its state)
    Result {
        /// Worker id, or any unambiguous prefix of one
        id: String,
    },
    /// Block until a worker leaves the running states; exit code encodes the
    /// state reached (timeout expiry exits 6)
    Wait {
        /// One or more worker ids (or unambiguous prefixes); the first to
        /// reach the stop set wins and its id is printed
        #[arg(required = true, num_args = 1..)]
        ids: Vec<String>,
        /// Stop set: any (default) | blocked | terminal | done
        #[arg(long, value_enum, default_value_t = crate::commands::wait::Until::Any)]
        until: crate::commands::wait::Until,
        /// Give up after this long and exit 6 — `30s`, `5m`, `2h`
        #[arg(long, value_name = "DUR", value_parser = crate::commands::wait::parse_duration)]
        timeout: Option<std::time::Duration>,
    },
    /// Queue a message for a worker; delivered at the next turn boundary
    Send {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// The message text. Never appears in argv of the codex child — it
        /// travels to the worker through a 0600 prompt file on stdin.
        message: String,
        /// Queue without delivering or stepping the machine, so several
        /// messages can be batched into the next delivery
        #[arg(long)]
        queue_only: bool,
        /// Who the message is from (recorded in the message's front matter)
        #[arg(long, default_value = "orchestrator")]
        from: String,
        /// answer (responds to an outbox question) | steer (mid-flight guidance)
        #[arg(long, value_enum, default_value_t = MsgKindArg::Answer)]
        kind: MsgKindArg,
    },
    /// Stop a worker: witnessed group-kill, state → cancelled (never failed).
    /// Idempotent — a worker that is already terminal exits 0.
    Kill {
        /// Worker id, or any unambiguous prefix of one
        id: String,
    },
    /// Continue a parked or terminal worker in a NEW epoch, resuming its codex
    /// session
    Resume {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// Extra instruction for the resumed turn. Never appears in argv of the
        /// codex child — it travels through a 0600 prompt file on stdin.
        message: Option<String>,
    },
    /// Reclaim finished workers' directories, leaving a tombstone per removal.
    /// Terminal states only — a worker that is still actionable is never
    /// removed, and there is no flag that overrides that.
    Gc {
        /// Only reclaim workers idle this long, measured from their last event
        /// — `30s`, `10m`, `2h`, `7d`
        #[arg(long, value_name = "DUR", default_value = "7d",
              value_parser = crate::commands::wait::parse_duration)]
        age: std::time::Duration,
        /// List what would be reclaimed and exit, deleting nothing
        #[arg(long)]
        dry_run: bool,
    },
    /// Check the environment a worker would be spawned into: one line per
    /// check, exit 9 if any of them fails
    Doctor,
    /// Inspect effective configuration
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    /// Control the broker daemon (the app-server transport)
    Broker {
        #[command(subcommand)]
        cmd: BrokerCmd,
    },
    /// (hidden) one request over the broker socket, autostarting one if
    /// needed. The ops seam Task 8 tests the wire through; `broker status`
    /// (Task 9) is the user-facing shape of the `info` op.
    #[command(hide = true, name = "__broker-call")]
    BrokerCall {
        /// info|list|status|spawn|resume|send|interrupt|kill|shutdown
        op: String,
        /// The op's parameters, as one JSON object
        params: Option<String>,
    },
    // later tasks append variants here
}

#[derive(Subcommand)]
pub enum BrokerCmd {
    /// Run the broker daemon: take `broker.lock`, detach, and serve. A second
    /// one says `broker already running` and exits 0.
    Run {
        /// Stay attached instead of double-forking into the background
        #[arg(long)]
        foreground: bool,
    },
    /// Report the running broker: uptime, transport, pids, counts (exit 7 when
    /// there is none)
    Status,
    /// Ask a running broker to stop; exits 0 when there is none
    Shutdown,
    /// Stop the running broker, then start a fresh one
    Restart,
}

#[derive(clap::Args)]
pub struct SpawnArgs {
    /// Lane: impl|review|research|general (sets sandbox, model, effort)
    #[arg(long)]
    pub lane: Lane,
    /// Working directory; must be under an allowed root. Forbidden for
    /// `--lane research`, which always runs in the worker's own scratch dir.
    #[arg(long)]
    pub cwd: Option<PathBuf>,
    /// Brief file (Markdown + optional YAML front matter)
    #[arg(long)]
    pub brief: PathBuf,
    /// Extra file staged read-only into <worker>/context/ (repeatable)
    #[arg(long)]
    pub context: Vec<PathBuf>,
    /// JSON schema the worker's result must satisfy
    #[arg(long)]
    pub schema: Option<PathBuf>,
    /// Override the lane's model
    #[arg(long)]
    pub model: Option<String>,
    /// Override the lane's reasoning effort
    #[arg(long)]
    pub effort: Option<String>,
    /// auto|exec (Phase 1); app-server arrives with the broker
    #[arg(long)]
    pub transport: Option<String>,
    /// Run in a git worktree (Phase 4; warns and is ignored today)
    #[arg(long)]
    pub worktree: bool,
    /// Fail rather than queue at the admission cap (Phase 2; no-op today)
    #[arg(long)]
    pub no_queue: bool,
}

/// clap-facing mirror of [`crate::inbox::MsgKind`]; the domain enum stays free
/// of CLI derives so the file layer does not depend on the argument parser.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "lowercase")]
pub enum MsgKindArg {
    Answer,
    Steer,
}

impl From<MsgKindArg> for crate::inbox::MsgKind {
    fn from(k: MsgKindArg) -> Self {
        match k {
            MsgKindArg::Answer => crate::inbox::MsgKind::Answer,
            MsgKindArg::Steer => crate::inbox::MsgKind::Steer,
        }
    }
}

#[derive(Subcommand)]
pub enum ConfigCmd {
    /// Print the effective configuration
    Show {
        /// Annotate each value with the layer it came from
        #[arg(long)]
        origin: bool,
    },
}
