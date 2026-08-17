use crate::output::{AppError, ExitCode};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub const LAYOUT_VERSION: u32 = 1;
/// The marker file that records the layout the tree was built at.
pub const LAYOUT_VERSION_FILE: &str = "LAYOUT_VERSION";

pub struct A2aHome {
    root: PathBuf,
}

/// The root this invocation would use — resolved, but neither created nor gated.
///
/// Split out of [`A2aHome::resolve`] because `doctor` has to be able to name the
/// tree it is about to *diagnose* even when adopting it would fail.
pub fn root_for(flag: Option<&Path>) -> PathBuf {
    flag.map(PathBuf::from)
        .or_else(|| std::env::var_os("A2A_HOME").map(PathBuf::from))
        .unwrap_or_else(|| dirs_home().join(".a2a"))
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
        let root = root_for(flag);
        create_dir_0700(&root).map_err(io_err)?;
        create_dir_0700(&root.join("workers")).map_err(io_err)?;
        create_dir_0700(&root.join("worktrees")).map_err(io_err)?;
        let vf = root.join(LAYOUT_VERSION_FILE);
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
    /// Adopt a root WITHOUT the layout gate.
    ///
    /// Exactly one caller is allowed to do this, and it is `doctor`: a home the
    /// binary refuses to touch is the thing an operator most needs diagnosed, so
    /// the mismatch has to come back as a *check* rather than as a command that
    /// cannot run. Every other path goes through [`A2aHome::resolve`], which
    /// gates first and hands out no handle at all on a mismatch.
    pub fn adopt(root: PathBuf) -> Self {
        Self { root }
    }

    /// The layout version recorded in the tree; `None` when the marker is
    /// absent or unreadable (which is itself a finding, not a default).
    pub fn recorded_layout_version(&self) -> Option<u32> {
        let text = std::fs::read_to_string(self.root.join(LAYOUT_VERSION_FILE)).ok()?;
        text.trim().parse().ok()
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
