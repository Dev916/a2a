use a2a::cli::{Cli, Cmd, ConfigCmd};
use a2a::home::A2aHome;
use a2a::output::{emit_err, emit_ok, AppError, ExitCode};
use clap::Parser;

fn main() {
    let cli = Cli::parse(); // clap exits 2 on usage errors
    // The shim is not a user-facing command: it never emits an envelope, and it
    // owns the process until the codex child it supervises is done.
    if let Cmd::ExecShim {
        worker_dir,
        epoch,
        prompt_file,
        argv,
    } = &cli.cmd
    {
        a2a::exec::shim_main(worker_dir, *epoch, prompt_file, argv);
    }
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
        Cmd::ExecShim { .. } => "__exec-shim",
        Cmd::Spawn(_) => "spawn",
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
        Cmd::ExecShim { .. } => unreachable!("__exec-shim diverges before dispatch"),
        Cmd::Spawn(args) => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::spawn::run(&home, args, cli.global.json, cli.global.quiet)
        }
        Cmd::Config { cmd } => match cmd {
            ConfigCmd::Show { origin } => {
                let home = A2aHome::resolve(cli.global.home.as_deref())?;
                a2a::commands::config_show::run(&home, *origin, cli.global.quiet)
            }
        },
    }
}
