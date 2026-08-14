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
    // later tasks append variants here
}
