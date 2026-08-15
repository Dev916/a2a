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
    /// Inspect effective configuration
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    // later tasks append variants here
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
