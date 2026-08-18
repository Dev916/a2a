use crate::config;
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode};
use serde_json::{json, Value};

/// `a2a config show [--origin]` — effective config on stdout, warnings on stderr.
pub fn run(home: &A2aHome, origin: bool, quiet: bool) -> Result<Value, AppError> {
    let (cfg, origins, warnings) = config::load(home)?;
    if !quiet {
        for w in &warnings {
            eprintln!("a2a: warning: {w}");
        }
    }
    let lanes = serde_json::to_value(&cfg.lanes).map_err(internal)?;
    let mut data = json!({
        "home": home.root(),
        "codex_bin": cfg.codex_bin,
        "codex_version_range": cfg.codex_version_range,
        "allowed_roots": cfg.allowed_roots,
        "lanes": lanes,
        "broker": serde_json::to_value(cfg.broker).map_err(internal)?,
        "admission": serde_json::to_value(&cfg.admission).map_err(internal)?,
        "sync": serde_json::to_value(&cfg.sync).map_err(internal)?,
    });
    if origin {
        data["origins"] = serde_json::to_value(&origins).map_err(internal)?;
    }
    Ok(data)
}

fn internal(e: serde_json::Error) -> AppError {
    AppError::new("SERIALIZE", ExitCode::Internal, e.to_string())
}
