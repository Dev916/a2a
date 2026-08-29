use crate::home::A2aHome;
use crate::output::{AppError, ExitCode};
use serde_json::{json, Value};
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use toml_edit::{Array, DocumentMut};

pub(crate) struct AddRoot {
    pub added: bool,
    pub path: PathBuf,
    pub allowed_roots: Vec<PathBuf>,
}

pub fn run(home: &A2aHome, path: &Path, json_mode: bool, _quiet: bool) -> Result<Value, AppError> {
    let out = add_root(home, path)?;
    if json_mode {
        Ok(json!({
            "added": out.added,
            "path": out.path,
            "allowed_roots": out.allowed_roots,
        }))
    } else {
        let verb = if out.added {
            "added"
        } else {
            "already covered"
        };
        Ok(Value::String(format!(
            "{verb}: {}\nconfig: {}",
            out.path.display(),
            config_path(home).display()
        )))
    }
}

pub(crate) fn add_root(home: &A2aHome, path: &Path) -> Result<AddRoot, AppError> {
    let canon = canonical_root(path)?;
    ensure_config_writable(home)?;
    let cfg_path = config_path(home);
    let mut doc = read_config_doc(&cfg_path)?;
    let existing = configured_roots(&doc)?;

    let already_covered = existing.iter().any(|root| canon.starts_with(root));
    if already_covered {
        return Ok(AddRoot {
            added: false,
            path: canon,
            allowed_roots: existing,
        });
    }

    if !doc.as_table().contains_key("allowed_roots") {
        doc["allowed_roots"] = toml_edit::value(Array::default());
    }
    let arr = doc["allowed_roots"].as_array_mut().ok_or_else(|| {
        AppError::new(
            "CONFIG_INVALID",
            ExitCode::Precondition,
            "allowed_roots must be an array of strings",
        )
        .remedy("fix allowed_roots in $A2A_HOME/config.toml")
    })?;
    let path_text = canon.to_string_lossy().to_string();
    arr.push(path_text);
    write_config_doc(&cfg_path, &doc)?;

    let mut allowed_roots = existing;
    allowed_roots.push(canon.clone());
    Ok(AddRoot {
        added: true,
        path: canon,
        allowed_roots,
    })
}

pub(crate) fn config_path(home: &A2aHome) -> PathBuf {
    home.root().join("config.toml")
}

pub(crate) fn canonical_root(path: &Path) -> Result<PathBuf, AppError> {
    let canon = std::fs::canonicalize(path).map_err(|e| {
        AppError::new(
            "ROOT_NOT_FOUND",
            ExitCode::Precondition,
            format!("root {}: {e}", path.display()),
        )
        .remedy("pass a directory that exists on this machine")
    })?;
    let meta = std::fs::metadata(&canon).map_err(|e| {
        AppError::new(
            "ROOT_NOT_FOUND",
            ExitCode::Precondition,
            format!("root {}: {e}", canon.display()),
        )
        .remedy("pass a directory that exists on this machine")
    })?;
    if !meta.is_dir() {
        return Err(AppError::new(
            "ROOT_NOT_FOUND",
            ExitCode::Precondition,
            format!("root {} is not a directory", canon.display()),
        )
        .remedy("pass a directory that exists on this machine"));
    }
    Ok(canon)
}

pub(crate) fn ensure_config_writable(home: &A2aHome) -> Result<(), AppError> {
    let path = config_path(home);
    if path.exists() {
        OpenOptions::new()
            .append(true)
            .open(&path)
            .map(|_| ())
            .map_err(|e| config_not_writable(&path, e))
    } else {
        let probe = home
            .root()
            .join(format!(".a2a-config-write-probe-{}", std::process::id()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&probe)
        {
            Ok(_) => {
                let _ = std::fs::remove_file(&probe);
                Ok(())
            }
            Err(e) => Err(config_not_writable(&path, e)),
        }
    }
}

fn read_config_doc(path: &Path) -> Result<DocumentMut, AppError> {
    if !path.exists() {
        return Ok(DocumentMut::new());
    }
    let text = std::fs::read_to_string(path).map_err(|e| {
        AppError::new(
            "CONFIG_READ",
            ExitCode::Precondition,
            format!("{}: {e}", path.display()),
        )
    })?;
    text.parse::<DocumentMut>().map_err(|e| {
        AppError::new(
            "CONFIG_PARSE",
            ExitCode::Precondition,
            format!("{}: {e}", path.display()),
        )
    })
}

fn write_config_doc(path: &Path, doc: &DocumentMut) -> Result<(), AppError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o644)
        .open(path)
        .map_err(|e| config_not_writable(path, e))?;
    file.write_all(doc.to_string().as_bytes())
        .map_err(|e| config_not_writable(path, e))
}

fn configured_roots(doc: &DocumentMut) -> Result<Vec<PathBuf>, AppError> {
    let Some(item) = doc.get("allowed_roots") else {
        return Ok(vec![]);
    };
    let arr = item.as_array().ok_or_else(|| {
        AppError::new(
            "CONFIG_INVALID",
            ExitCode::Precondition,
            "allowed_roots must be an array of strings",
        )
        .remedy("fix allowed_roots in $A2A_HOME/config.toml")
    })?;
    let mut roots = Vec::new();
    for v in arr {
        let Some(s) = v.as_str() else {
            return Err(AppError::new(
                "CONFIG_INVALID",
                ExitCode::Precondition,
                "allowed_roots must be an array of strings",
            )
            .remedy("fix allowed_roots in $A2A_HOME/config.toml"));
        };
        if let Ok(c) = std::fs::canonicalize(s) {
            if !roots.contains(&c) {
                roots.push(c);
            }
        }
    }
    Ok(roots)
}

fn config_not_writable(path: &Path, e: std::io::Error) -> AppError {
    AppError::new(
        "CONFIG_NOT_WRITABLE",
        ExitCode::Precondition,
        format!("{}: {e}", path.display()),
    )
}
