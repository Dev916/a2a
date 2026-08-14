use crate::output::{AppError, ExitCode};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub const LAYOUT_VERSION: u32 = 1;

pub struct A2aHome {
    root: PathBuf,
}

pub fn create_dir_0700(p: &Path) -> std::io::Result<()> {
    if p.exists() {
        return Ok(());
    }
    if let Some(parent) = p.parent() {
        if !parent.exists() {
            create_dir_0700(parent)?;
        }
    }
    fs::DirBuilder::new().mode(0o700).create(p)
}

/// Create-or-truncate, 0600. Snapshots and one-shot files ONLY (streams use append helpers).
pub fn write_new_0600(p: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(p)
}

impl A2aHome {
    pub fn resolve(flag: Option<&Path>) -> Result<Self, AppError> {
        let root = flag
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("A2A_HOME").map(PathBuf::from))
            .unwrap_or_else(|| dirs_home().join(".a2a"));
        create_dir_0700(&root).map_err(io_err)?;
        create_dir_0700(&root.join("workers")).map_err(io_err)?;
        create_dir_0700(&root.join("worktrees")).map_err(io_err)?;
        let vf = root.join("LAYOUT_VERSION");
        if vf.exists() {
            let v: u32 = fs::read_to_string(&vf)
                .map_err(io_err)?
                .trim()
                .parse()
                .unwrap_or(0);
            if v != LAYOUT_VERSION {
                return Err(AppError::new(
                    "LAYOUT_VERSION_MISMATCH",
                    ExitCode::Precondition,
                    format!("home layout v{v}, binary expects v{LAYOUT_VERSION}"),
                )
                .remedy("run `a2a migrate` (or move the old home aside)"));
            }
        } else {
            use std::io::Write;
            writeln!(write_new_0600(&vf).map_err(io_err)?, "{LAYOUT_VERSION}").map_err(io_err)?;
        }
        Ok(Self { root })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn workers_dir(&self) -> PathBuf {
        self.root.join("workers")
    }
    pub fn worker_dir(&self, id: &str) -> PathBuf {
        self.workers_dir().join(id)
    }
    pub fn worktrees_dir(&self) -> PathBuf {
        self.root.join("worktrees")
    }
    pub fn list_worker_ids(&self) -> std::io::Result<Vec<String>> {
        let mut out = vec![];
        for e in fs::read_dir(self.workers_dir())? {
            let e = e?;
            if e.file_type()?.is_dir() {
                out.push(e.file_name().to_string_lossy().into_owned());
            }
        }
        out.sort();
        Ok(out)
    }
}

fn dirs_home() -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").expect("HOME unset"))
}

fn io_err(e: std::io::Error) -> AppError {
    AppError::new("IO", ExitCode::Internal, e.to_string())
}
