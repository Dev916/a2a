use crate::home::{self, LAYOUT_VERSION, LAYOUT_VERSION_FILE};
use crate::observe::{self as observe_plane, AppState, Cursor, Observer, Plane};
use crate::output::{AppError, ExitCode, Outcome};
use serde_json::Value;
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

const TAIL_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub fn snapshot(home_arg: Option<&Path>, _json: bool) -> Result<Outcome, AppError> {
    let home = resolve_home(home_arg)?;
    let snapshot = Observer::open(home).snapshot();
    let value = serde_json::to_value(snapshot).map_err(serialize_err)?;
    Ok(Outcome::from(value))
}

pub fn tail(
    home_arg: Option<&Path>,
    from: Option<&str>,
    planes: Option<&str>,
) -> Result<Outcome, AppError> {
    let home = resolve_home(home_arg)?;
    let observer = Observer::open(home);
    let mut cursor = match from {
        Some(token) => Cursor::decode(token)?,
        None => observer.snapshot().cursor,
    };
    let planes = parse_planes(planes);
    let runtime = runtime()?;

    runtime.block_on(async move {
        loop {
            let (events, advanced) = observer.events_since(&cursor);
            let mut stdout = std::io::stdout().lock();
            for event in events
                .iter()
                .filter(|event| planes.as_ref().is_none_or(|p| p.contains(&event.plane)))
            {
                serde_json::to_writer(&mut stdout, event).map_err(serialize_err)?;
                writeln!(stdout).map_err(output_err)?;
            }
            stdout.flush().map_err(output_err)?;
            drop(stdout);
            cursor = advanced;

            tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(|error| {
                        AppError::new(
                            "SIGNAL",
                            ExitCode::Internal,
                            format!("waiting for Ctrl-C: {error}"),
                        )
                    })?;
                    return Ok(Outcome::from(Value::Null));
                }
                _ = tokio::time::sleep(TAIL_POLL_INTERVAL) => {}
            }
        }
    })
}

pub fn serve(
    home_arg: Option<&Path>,
    bind: IpAddr,
    port: u16,
    token: Option<&str>,
    token_file: Option<&Path>,
    tls: bool,
    _planes: Option<&str>,
) -> Result<Outcome, AppError> {
    let home = resolve_home(home_arg)?;
    let token = read_token(token, token_file)?;
    let state = AppState { home, token };
    let runtime = runtime()?;
    runtime.block_on(observe_plane::serve::run(state, bind, port, tls))?;
    Ok(Outcome::from(Value::Null))
}

fn resolve_home(home_arg: Option<&Path>) -> Result<PathBuf, AppError> {
    let root = home::root_for(home_arg);
    if !root.is_dir() || !root.join("workers").is_dir() {
        return Err(uninitialized_home(&root));
    }

    let version_path = root.join(LAYOUT_VERSION_FILE);
    let version = std::fs::read_to_string(&version_path)
        .map_err(|_| uninitialized_home(&root))?
        .trim()
        .parse::<u32>()
        .map_err(|_| uninitialized_home(&root))?;
    if version != LAYOUT_VERSION {
        return Err(AppError::new(
            "LAYOUT_VERSION_MISMATCH",
            ExitCode::Precondition,
            format!(
                "home at {} has layout v{version}, binary expects v{LAYOUT_VERSION}",
                root.display()
            ),
        )
        .remedy("run `a2a migrate` (or move the old home aside)"));
    }

    Ok(root)
}

fn uninitialized_home(root: &Path) -> AppError {
    AppError::new(
        "OBSERVE_HOME_UNINITIALIZED",
        ExitCode::Precondition,
        format!(
            "observability home is missing or uninitialized at {}",
            root.display()
        ),
    )
    .remedy(format!(
        "initialize the a2a home at {} before observing it",
        root.display()
    ))
}

fn read_token(token: Option<&str>, token_file: Option<&Path>) -> Result<Option<String>, AppError> {
    match (token, token_file) {
        (Some(_), Some(_)) => Err(AppError::new(
            "OBSERVE_TOKEN_CONFLICT",
            ExitCode::Precondition,
            "provide either --token or --token-file, not both",
        )),
        (Some(token), None) => Ok(Some(token.to_string())),
        (None, Some(path)) => std::fs::read_to_string(path)
            .map(|token| Some(token.trim().to_string()))
            .map_err(|error| {
                AppError::new(
                    "OBSERVE_TOKEN_FILE",
                    ExitCode::Precondition,
                    format!("reading token file {}: {error}", path.display()),
                )
            }),
        (None, None) => Ok(None),
    }
}

fn parse_planes(value: Option<&str>) -> Option<Vec<Plane>> {
    value.map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| match value {
                "mesh" => Plane::Mesh,
                "work" => Plane::Work,
                "worker" => Plane::Worker,
                "presence" => Plane::Presence,
                "health" => Plane::Health,
                _ => Plane::Unknown,
            })
            .collect()
    })
}

fn runtime() -> Result<tokio::runtime::Runtime, AppError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            AppError::new(
                "ASYNC",
                ExitCode::Internal,
                format!("starting runtime: {error}"),
            )
        })
}

fn serialize_err(error: serde_json::Error) -> AppError {
    AppError::new("SERIALIZE", ExitCode::Internal, error.to_string())
}

fn output_err(error: std::io::Error) -> AppError {
    AppError::new(
        "IO",
        ExitCode::Internal,
        format!("writing observe output: {error}"),
    )
}
