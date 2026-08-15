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
        id: String,
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
    /// Inspect effective configuration
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    // later tasks append variants here
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

#[derive(Subcommand)]
pub enum ConfigCmd {
    /// Print the effective configuration
    Show {
        /// Annotate each value with the layer it came from
        #[arg(long)]
        origin: bool,
    },
}
