use a2a::cli::{Cli, Cmd, ConfigCmd};
use a2a::home::A2aHome;
use a2a::output::{emit_err, emit_ok, AppError, ExitCode};
use clap::Parser;

fn main() {
    let cli = Cli::parse(); // clap exits 2 on usage errors
    let cmd_name = command_name(&cli.cmd);
    let code = match run(&cli) {
        Ok(data) => {
            emit_ok(cli.global.json, cmd_name, data);
            0
        }
        Err(e) => emit_err(cli.global.json, cmd_name, &e),
    };
    std::process::exit(code);
}

fn command_name(cmd: &Cmd) -> &'static str {
    match cmd {
        Cmd::SelftestError => "__selftest-error",
        Cmd::Config { cmd } => match cmd {
            ConfigCmd::Show { .. } => "config show",
        },
    }
}

fn run(cli: &Cli) -> Result<serde_json::Value, AppError> {
    match &cli.cmd {
        Cmd::SelftestError => Err(AppError::new(
            "SELFTEST",
            ExitCode::Precondition,
            "canned error",
        )),
        Cmd::Config { cmd } => match cmd {
            // Temporary stub — replaced by the real layered config in Task 3.
            ConfigCmd::Show { .. } => {
                let home = A2aHome::resolve(cli.global.home.as_deref())?;
                Ok(serde_json::json!({ "home": home.root() }))
            }
        },
    }
}
