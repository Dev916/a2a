//! Exec-transport process machinery (spec §4.4, §5).
//!
//! A worker is a **detached process group** whose liveness is a kernel-held
//! `flock` on `workers/<id>/run.lock` — never a PID we poll and never a pipe
//! the short-lived CLI must stay alive to drain. The layering is:
//!
//! ```text
//! a2a spawn ──fork/setsid──▶ a2a __exec-shim ──spawn──▶ codex exec --json
//!   (exits)                    holds run.lock            stdout → exec.<epoch>.jsonl
//!                              writes exit_code          stderr → stderr.log
//!                                                        stdin  ← prompt file ("-")
//! ```
//!
//! Invariants this module owns:
//!
//! * **Prompts never enter argv.** The brief/steer text is written to a 0600
//!   prompt file which becomes the child's stdin; argv ends with the `-`
//!   positional. [`build_argv`] is pure so a test can pin exactly that.
//! * **Streams are append-only.** `exec.<epoch>.jsonl` and `stderr.log` are
//!   opened `O_APPEND|O_CREAT` at 0600 and never truncated: a resume's second
//!   shim cycle must not destroy the prior epoch's stream (nor the ingest
//!   watermark that indexes into it).
//! * **Billing-critical env is scrubbed** in both hops — the shim spawn and the
//!   codex spawn — and the scrub is applied AFTER any caller-supplied pairs, so
//!   the scrub always wins.
//! * **Identity is witnessed before any signal.** PIDs get recycled; a kill
//!   compares the recorded start-time witness first and refuses on mismatch
//!   (supervisor holds identity; workers are disposable).
//! * **`--ephemeral` is never passed** (it would kill resumability).

use crate::config::SandboxMode;
use crate::output::{AppError, ExitCode};
use crate::store::write_atomic;
use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use nix::sys::signal::{kill, Signal};
use nix::unistd::{getpgid, getpid, setsid, Pid};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

/// Environment variables removed from every worker environment, even in
/// Phase 1: billing (`OPENAI_API_KEY` — a worker inheriting it bills the human
/// silently) and credential leak (AWS, depot) critical (spec §4.2, §5).
pub const SCRUBBED_ENV: &[&str] = &[
    "OPENAI_API_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "DEPOT_TOKEN",
];

/// Readiness poll: 20ms × 250 = 5s cap before `NEVER_STARTED`.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(20);
const READY_POLL_TRIES: u32 = 250;
/// SIGTERM → SIGKILL grace: 100ms × 20 = 2s.
const KILL_GRACE_INTERVAL: Duration = Duration::from_millis(100);
const KILL_GRACE_TRIES: u32 = 20;

/// Everything a spawn needs; borrowed so the caller owns the storage.
pub struct SpawnSpec<'a> {
    pub codex_bin: &'a Path,
    pub cwd: &'a Path,
    pub worker_dir: &'a Path,
    pub epoch: u32,
    pub sandbox: SandboxMode,
    pub network: bool,
    pub model: &'a str,
    pub effort: &'a str,
    pub schema_path: Option<&'a Path>,
    /// 0600; wired to the child's stdin — argv gets `-`, never the text.
    pub prompt_file: &'a Path,
    /// `Some(id)` → `exec resume <id> … -`.
    pub resume_session: Option<&'a str>,
    /// Extra env (also the test seam — no process-global `set_var` anywhere).
    /// Applied BEFORE [`SCRUBBED_ENV`], which always wins.
    pub env: &'a [(String, String)],
}

/// A live detached worker: the shim's pid (== its pgid) plus the start-time
/// witness that guards every later signal against PID reuse.
#[derive(Debug, Clone)]
pub struct SpawnedProc {
    pub pid: u32,
    pub start_witness: String,
}

/// Kernel-truth liveness derived from `run.lock`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liveness {
    Alive,
    Dead,
    /// `run.lock` absent — the shim has not reached its lock yet.
    NotYetStarted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillOutcome {
    Killed,
    AlreadyDead,
    WitnessMismatch,
}

/// `<worker_dir>/exec.<epoch>.jsonl` — one stream per epoch, so a resume never
/// overwrites the evidence (or the watermark) of the epoch before it.
pub fn exec_stream_path(worker_dir: &Path, epoch: u32) -> PathBuf {
    worker_dir.join(format!("exec.{epoch}.jsonl"))
}

/// Create-or-open for append at 0600. Never truncates.
pub fn open_append_0600(p: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(p)
}

/// The exact codex argv, in the order the spec pins (§4.4, §4.7). Pure — the
/// contract test asserts on this and nothing else.
///
/// For [`SandboxMode::OutboxOnly`] the PRIMARY workspace moves to the worker's
/// own `scratch/` dir: codex's `--add-dir` adds writable roots *alongside* the
/// always-writable `-C` workspace, so an outbox-only worker must be moved off
/// the repo, not merely denied extra roots. The intended repo path travels to
/// the worker inside the brief, as read-only context.
pub fn build_argv(spec: &SpawnSpec) -> Vec<String> {
    let w = spec.worker_dir;
    let workspace = match spec.sandbox {
        SandboxMode::OutboxOnly => w.join("scratch"),
        _ => spec.cwd.to_path_buf(),
    };
    let mut argv: Vec<String> = Vec::with_capacity(24);
    argv.push("exec".to_string());
    if let Some(session) = spec.resume_session {
        argv.push("resume".to_string());
        argv.push(session.to_string());
    }
    argv.push("--json".to_string());
    argv.push("-C".to_string());
    argv.push(path_arg(&workspace));
    argv.push("--output-last-message".to_string());
    argv.push(path_arg(&w.join("result.md")));
    argv.push("-s".to_string());
    argv.push(sandbox_flag(spec.sandbox).to_string());
    argv.push("--add-dir".to_string());
    argv.push(path_arg(&w.join("outbox")));
    argv.push("-c".to_string());
    argv.push("approval_policy=\"never\"".to_string());
    argv.push("-c".to_string());
    argv.push("features.hooks=false".to_string());
    argv.push("-c".to_string());
    argv.push(format!(
        "sandbox_workspace_write.network_access={}",
        spec.network
    ));
    argv.push("-m".to_string());
    argv.push(spec.model.to_string());
    argv.push("-c".to_string());
    argv.push(format!("model_reasoning_effort=\"{}\"", spec.effort));
    argv.push("--skip-git-repo-check".to_string());
    if let Some(schema) = spec.schema_path {
        argv.push("--output-schema".to_string());
        argv.push(path_arg(schema));
    }
    argv.push("-".to_string()); // prompt arrives on stdin — NEVER in argv
    argv
}

/// `-s <mode>`: outbox-only is expressed as workspace-write with the workspace
/// relocated (see [`build_argv`]).
fn sandbox_flag(mode: SandboxMode) -> &'static str {
    match mode {
        SandboxMode::DangerFullAccess => "danger-full-access",
        SandboxMode::WorkspaceWrite | SandboxMode::OutboxOnly => "workspace-write",
    }
}

fn path_arg(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

/// Fork the detached `a2a __exec-shim <worker-dir> <epoch> <prompt-file> --
/// <codex argv…>` and block until it holds `run.lock` (20ms × 250 = 5s cap).
///
/// Returning implies the worker is observably running (or has already
/// finished), so the caller may durably record `running` without inventing a
/// state the kernel cannot confirm. Expiry → `NEVER_STARTED`.
pub fn spawn_detached(spec: &SpawnSpec) -> Result<SpawnedProc, AppError> {
    let w = spec.worker_dir;
    for d in ["outbox", "scratch"] {
        mkdir_0700(&w.join(d))?;
    }
    // The sentinel is per-run: a stale value from the previous epoch would be
    // actively wrong, and would also fool the readiness poll below.
    let _ = fs::remove_file(w.join("exit_code"));

    let shim = shim_bin()?;
    let mut cmd = Command::new(&shim);
    cmd.arg("__exec-shim")
        .arg(w)
        .arg(spec.epoch.to_string())
        .arg(spec.prompt_file)
        .arg("--")
        .arg(spec.codex_bin)
        .args(build_argv(spec));
    cmd.current_dir(w)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (k, v) in spec.env {
        cmd.env(k, v);
    }
    scrub_env(&mut cmd); // AFTER spec.env — the scrub always wins
    unsafe {
        cmd.pre_exec(|| {
            // Own session + process group, so the whole worker tree can be
            // reaped with one signal and nothing races the CLI's own group.
            match setsid() {
                Ok(_) => Ok(()),
                Err(Errno::EPERM) => Ok(()), // already a session leader
                Err(e) => Err(io::Error::from(e)),
            }
        });
    }
    let child = cmd.spawn().map_err(|e| {
        AppError::new(
            "SPAWN_FAILED",
            ExitCode::Internal,
            format!("could not fork exec shim {}: {e}", shim.display()),
        )
    })?;
    let pid = child.id();

    for _ in 0..READY_POLL_TRIES {
        match liveness(w) {
            Liveness::Alive => {
                return Ok(SpawnedProc {
                    pid,
                    start_witness: start_witness(pid).unwrap_or_default(),
                })
            }
            // A very short worker can finish before we ever observe the lock;
            // the sentinel proves it ran, so that is readiness too.
            Liveness::Dead if w.join("exit_code").exists() => {
                return Ok(SpawnedProc {
                    pid,
                    start_witness: start_witness(pid).unwrap_or_default(),
                })
            }
            _ => std::thread::sleep(READY_POLL_INTERVAL),
        }
    }
    Err(AppError::new(
        "NEVER_STARTED",
        ExitCode::Precondition,
        format!(
            "exec shim (pid {pid}) never acquired {} within 5s",
            w.join("run.lock").display()
        ),
    )
    .remedy("check stderr.log in the worker dir and that the codex binary is executable"))
}

/// Kernel-truth liveness: try to take `run.lock` ourselves. Success means no
/// one holds it (the holder died — the kernel released it), contention means a
/// live holder.
pub fn liveness(worker_dir: &Path) -> Liveness {
    let path = worker_dir.join("run.lock");
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Liveness::NotYetStarted,
        Err(_) => return Liveness::Dead,
    };
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(lock) => {
            drop(lock); // unlocks; we were only probing
            Liveness::Dead
        }
        Err((_file, _errno)) => Liveness::Alive,
    }
}

/// Start-time witness for `pid` (`ps -o lstart=`). `None` ⇒ no such process.
/// Bare PIDs get recycled; the witness is what makes a kill identity-checked.
pub fn start_witness(pid: u32) -> Option<String> {
    let out = Command::new("ps")
        .args(["-o", "lstart=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let witness = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if witness.is_empty() {
        None
    } else {
        Some(witness)
    }
}

/// Signal the worker's process GROUP, but only after the start-time witness
/// matches — a recycled PID must never be signalled.
pub fn kill_worker(pid: u32, witness: &str) -> Result<KillOutcome, AppError> {
    let current = match start_witness(pid) {
        Some(w) => w,
        None => return Ok(KillOutcome::AlreadyDead),
    };
    if current.trim() != witness.trim() {
        return Ok(KillOutcome::WitnessMismatch);
    }
    let target = signal_target(pid);
    match kill(target, Signal::SIGTERM) {
        Ok(()) => {}
        Err(Errno::ESRCH) => return Ok(KillOutcome::AlreadyDead),
        Err(e) => {
            return Err(AppError::new(
                "KILL_FAILED",
                ExitCode::Internal,
                format!("SIGTERM to {target} failed: {e}"),
            ))
        }
    }
    for _ in 0..KILL_GRACE_TRIES {
        if !is_running(pid) {
            return Ok(KillOutcome::Killed);
        }
        std::thread::sleep(KILL_GRACE_INTERVAL);
    }
    let _ = kill(target, Signal::SIGKILL);
    Ok(KillOutcome::Killed)
}

/// Group target when the pid really leads its own group (the shim setsid'd);
/// otherwise the bare pid — never blast a group we do not own.
fn signal_target(pid: u32) -> Pid {
    let pid_t = Pid::from_raw(pid as i32);
    match getpgid(Some(pid_t)) {
        Ok(pgid) if pgid == pid_t => Pid::from_raw(-(pid as i32)),
        _ => pid_t,
    }
}

/// Alive ⇒ exists AND is not a zombie (an unreaped corpse is dead for our
/// purposes; its lock is already released).
fn is_running(pid: u32) -> bool {
    let out = match Command::new("ps")
        .args(["-o", "state=", "-p", &pid.to_string()])
        .output()
    {
        Ok(o) => o,
        Err(_) => return false,
    };
    if !out.status.success() {
        return false;
    }
    let state = String::from_utf8_lossy(&out.stdout).trim().to_string();
    !state.is_empty() && !state.starts_with('Z')
}

/// The `exit_code` sentinel written by the shim (a reparented child's status is
/// otherwise unreapable).
pub fn read_exit_code(worker_dir: &Path) -> Option<i32> {
    let raw = fs::read_to_string(worker_dir.join("exit_code")).ok()?;
    raw.trim().parse::<i32>().ok()
}

// ---------------------------------------------------------------------------
// shim (hidden `a2a __exec-shim` subcommand)
// ---------------------------------------------------------------------------

/// Shim entry point: `__exec-shim <worker-dir> <epoch> <prompt-file> -- <bin>
/// <args…>`. Owns `run.lock` for the child's whole life and never returns.
pub fn shim_main(worker_dir: &Path, epoch: u32, prompt_file: &Path, argv: &[String]) -> ! {
    std::process::exit(run_shim(worker_dir, epoch, prompt_file, argv))
}

/// Shim failure codes are private to the shim/parent pair — the parent reports
/// `NEVER_STARTED` when the lock is never taken, whatever the reason.
fn run_shim(worker_dir: &Path, epoch: u32, prompt_file: &Path, argv: &[String]) -> i32 {
    if !own_process_group() {
        eprintln!("a2a __exec-shim: refusing to run without our own process group");
        return 3;
    }
    let (bin, args) = match argv.split_first() {
        Some(split) => split,
        None => {
            eprintln!("a2a __exec-shim: empty child argv");
            return 2;
        }
    };
    let lock_file = match OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(worker_dir.join("run.lock"))
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("a2a __exec-shim: run.lock: {e}");
            return 4;
        }
    };
    // Exclusive + nonblocking: a second shim for the same worker aborts rather
    // than double-driving the session. Held until this process exits, at which
    // point the kernel releases it — that release IS the death notification.
    let _lock = match Flock::lock(lock_file, FlockArg::LockExclusiveNonblock) {
        Ok(l) => l,
        Err((_f, e)) => {
            eprintln!("a2a __exec-shim: run.lock already held ({e})");
            return 5;
        }
    };

    let stdout = match open_append_0600(&exec_stream_path(worker_dir, epoch)) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("a2a __exec-shim: exec stream: {e}");
            return 6;
        }
    };
    let stderr = match open_append_0600(&worker_dir.join("stderr.log")) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("a2a __exec-shim: stderr.log: {e}");
            return 6;
        }
    };
    let stdin = match File::open(prompt_file) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("a2a __exec-shim: prompt file: {e}");
            return 7;
        }
    };

    let mut cmd = Command::new(bin);
    cmd.args(args)
        .stdin(Stdio::from(stdin))
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    scrub_env(&mut cmd); // second hop: the codex child never sees them either
    let status = match cmd.status() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("a2a __exec-shim: exec {}: {e}", bin);
            let _ = write_atomic(&worker_dir.join("exit_code"), b"127\n");
            return 8;
        }
    };
    let code = status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(0));
    // Written while the lock is still held, so any observer that sees Dead also
    // sees the sentinel.
    let _ = write_atomic(&worker_dir.join("exit_code"), format!("{code}\n").as_bytes());
    0
}

/// True once this process leads its own group (so `kill(-pgid)` reaps exactly
/// the worker tree). The parent setsid's in `pre_exec`; this re-checks and
/// retries rather than trusting it.
fn own_process_group() -> bool {
    let me = getpid();
    if getpgid(None) == Ok(me) {
        return true;
    }
    let _ = setsid();
    getpgid(None) == Ok(me)
}

fn scrub_env(cmd: &mut Command) {
    for key in SCRUBBED_ENV {
        cmd.env_remove(key);
    }
}

fn mkdir_0700(p: &Path) -> Result<(), AppError> {
    if p.is_dir() {
        return Ok(());
    }
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(p)
        .map_err(|e| {
            AppError::new(
                "MKDIR_FAILED",
                ExitCode::Internal,
                format!("could not create {}: {e}", p.display()),
            )
        })
}

/// Locate the `a2a` binary that hosts `__exec-shim`. Normally that is us; under
/// `cargo test` the running executable is the test harness, so we look beside
/// it (`target/debug/deps/…` → `target/debug/a2a`).
fn shim_bin() -> Result<PathBuf, AppError> {
    if let Some(p) = std::env::var_os("A2A_SHIM_BIN").map(PathBuf::from) {
        if p.is_file() {
            return Ok(p);
        }
    }
    let exe = std::env::current_exe().map_err(|e| {
        AppError::new(
            "SHIM_NOT_FOUND",
            ExitCode::Internal,
            format!("cannot resolve the running executable: {e}"),
        )
    })?;
    if exe.file_name() == Some(OsStr::new("a2a")) {
        return Ok(exe);
    }
    let parent = exe.parent();
    let candidates = [
        parent.map(|d| d.join("a2a")),
        parent.and_then(|d| d.parent()).map(|d| d.join("a2a")),
    ];
    for candidate in candidates.into_iter().flatten() {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(AppError::new(
        "SHIM_NOT_FOUND",
        ExitCode::Precondition,
        format!("no `a2a` binary next to {}", exe.display()),
    )
    .remedy("set A2A_SHIM_BIN to the a2a binary path"))
}
