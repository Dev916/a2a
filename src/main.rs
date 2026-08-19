use a2a::cli::{BrokerCmd, Cli, Cmd, ConfigCmd};
use a2a::home::A2aHome;
use a2a::output::{emit_err, emit_ok, AppError, ExitCode, Outcome};
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
        // A command can succeed and still exit nonzero: `status` on a failed
        // worker answered the question correctly (exit 4, full payload).
        Ok(out) => {
            emit_ok(cli.global.json, cmd_name, out.data);
            out.exit as i32
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
        Cmd::Status { .. } => "status",
        Cmd::Ls { .. } => "ls",
        Cmd::Tail { .. } => "tail",
        Cmd::Result { .. } => "result",
        Cmd::Wait { .. } => "wait",
        Cmd::Send { .. } => "send",
        Cmd::Interrupt { .. } => "interrupt",
        Cmd::Kill { .. } => "kill",
        Cmd::Resume { .. } => "resume",
        Cmd::Gc { .. } => "gc",
        Cmd::Doctor => "doctor",
        Cmd::Config { cmd } => match cmd {
            ConfigCmd::Show { .. } => "config show",
        },
        Cmd::Broker { cmd } => match cmd {
            BrokerCmd::Run { .. } => "broker run",
            BrokerCmd::Status => "broker status",
            BrokerCmd::Shutdown => "broker shutdown",
            BrokerCmd::Restart => "broker restart",
        },
        Cmd::BrokerCall { .. } => "__broker-call",
        Cmd::Verify { .. } => "verify",
    }
}

fn run(cli: &Cli) -> Result<Outcome, AppError> {
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
                .map(Outcome::from)
        }
        Cmd::Status { id, broker } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::status::run(
                &home,
                id.as_deref(),
                *broker,
                cli.global.json,
                cli.global.quiet,
            )
        }
        Cmd::Ls { all } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::ls::run(&home, *all, cli.global.json, cli.global.quiet)
        }
        Cmd::Tail {
            id,
            since,
            raw,
            follow,
            timeout,
        } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            // The default bound is `wait`'s, for the same reason: a follow that
            // can block forever is one an orchestrator cannot supervise.
            let follow = follow.then(|| a2a::commands::tail::Follow {
                timeout: timeout.unwrap_or(a2a::commands::wait::DEFAULT_TIMEOUT),
            });
            a2a::commands::tail::run(
                &home,
                id,
                *since,
                *raw,
                follow,
                cli.global.json,
                cli.global.quiet,
            )
        }
        Cmd::Result { id } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::result::run(&home, id, cli.global.json, cli.global.quiet)
        }
        Cmd::Wait {
            ids,
            until,
            timeout,
        } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::wait::run(
                &home,
                ids,
                *until,
                timeout.unwrap_or(a2a::commands::wait::DEFAULT_TIMEOUT),
                cli.global.json,
                cli.global.quiet,
            )
        }
        Cmd::Send {
            id,
            message,
            queue_only,
            from,
            kind,
            force,
        } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::send::run(
                &home,
                id,
                message,
                from,
                (*kind).into(),
                *queue_only,
                *force,
                cli.global.json,
                cli.global.quiet,
            )
        }
        Cmd::Interrupt { id, force } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::interrupt::run(&home, id, *force, cli.global.json, cli.global.quiet)
        }
        Cmd::Kill { id, force } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::kill::run(&home, id, *force, cli.global.json, cli.global.quiet)
        }
        Cmd::Resume {
            id,
            message,
            force,
        } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::resume::run(
                &home,
                id,
                message.as_deref(),
                *force,
                cli.global.json,
                cli.global.quiet,
            )
        }
        Cmd::Gc {
            age,
            dry_run,
            all,
            force,
        } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::gc::run(
                &home,
                *age,
                *dry_run,
                *all,
                *force,
                cli.global.json,
                cli.global.quiet,
            )
        }
        // The one command that resolves its own home: a layout mismatch is a
        // finding doctor must report, not an error that stops it running.
        Cmd::Doctor => a2a::commands::doctor::run(
            cli.global.home.as_deref(),
            cli.global.json,
            cli.global.quiet,
        ),
        Cmd::Config { cmd } => match cmd {
            ConfigCmd::Show { origin } => {
                let home = A2aHome::resolve(cli.global.home.as_deref())?;
                a2a::commands::config_show::run(&home, *origin, cli.global.quiet)
                    .map(Outcome::from)
            }
        },
        Cmd::Broker { cmd } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            match cmd {
                // The ONE async command: everything else in this table is
                // synchronous, and stays that way (Global Constraints).
                BrokerCmd::Run { foreground } => {
                    a2a::commands::broker_cmd::run(&home, *foreground, cli.global.quiet)
                }
                BrokerCmd::Status => a2a::commands::broker_cmd::status(&home, cli.global.quiet),
                BrokerCmd::Shutdown => a2a::commands::broker_cmd::shutdown(&home),
                BrokerCmd::Restart => a2a::commands::broker_cmd::restart(&home, cli.global.quiet),
            }
        }
        Cmd::BrokerCall { op, params } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::broker_cmd::call(&home, op, params.as_deref())
        }
        Cmd::Verify {
            id,
            force_verify,
            timeout,
        } => {
            let home = A2aHome::resolve(cli.global.home.as_deref())?;
            a2a::commands::verify::run(
                &home,
                id,
                *force_verify,
                timeout.unwrap_or(a2a::commands::verify::DEFAULT_TIMEOUT),
                cli.global.json,
                cli.global.quiet,
            )
        }
    }
}
