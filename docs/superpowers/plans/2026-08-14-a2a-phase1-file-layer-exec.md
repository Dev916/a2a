# A2A Phase 1: File Layer + Worker FSM + Exec Transport — Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a working `a2a` CLI that can dispatch, monitor, steer (at turn boundaries), and reap Codex workers via the exec transport, with the crash-safe file layer and worker state machine that every later phase builds on.

**Architecture:** Synchronous Rust CLI (no tokio in this phase — the broker arrives in Phase 2). All worker state lives in `$A2A_HOME/workers/<id>/` as a WAL (`events.jsonl` = source of truth) plus an atomic snapshot (`state.json`). A pure `step()` state machine governs every transition; CLI read paths run `reconcile()` under a per-worker lock to observe liveness/deadlines and commit transitions. Workers are detached `codex exec --json` process groups whose liveness is a kernel-held flock.

**Tech Stack:** Rust **edition 2021** (pinned — cargo 1.93 defaults to 2024, where `std::env::set_var` is unsafe), clap 4 (derive), serde/serde_json, serde_yaml (brief front matter), toml (config read), chrono, sha2, regex, **rand 0.8** (pinned — 0.10 removed `thread_rng`/`gen_range`), **nix 0.29** (pinned — the `fcntl::flock(RawFd, FlockArg)` API), tempfile + assert_cmd + predicates (tests).

**Spec:** `docs/superpowers/specs/2026-08-14-a2a-claude-codex-design.md` (v2). This plan implements spec §9 Phase 1. Phases 2–5 get their own plans.

**Compatible with:** devloop skill v0.1+

## Global Constraints

- Binary name `a2a`, installed via `cargo install --path . --root ~/.local` (spec §1).
- `A2A_HOME` env var + `--home` global flag relocate the whole tree; default `~/.a2a`. **Every test sets a temp `A2A_HOME` (and, where codex state matters, a temp `CODEX_HOME`) — tests must never touch `~/.a2a` or `~/.codex`.** The one exception: Task 20's `A2A_E2E=1` smoke reads (never writes) the real `~/.codex` for auth.
- All dirs 0700, all files 0600, modes set at creation, never chmod-after (spec §4.5, §5).
- `state.json`/`result.*`/index writes: temp-in-same-dir → fsync(file) → rename → fsync(dir). `events.jsonl`: O_APPEND single-writer, one complete line per write, `sync_data` after each append (WAL durability, spec §4.6); readers tolerate a torn trailing line; `EventLog::open` REPAIRS a torn tail (truncates to the last complete line) before appending.
- Exit codes are a versioned public contract pinned by a test: 0 success · 1 internal · 2 usage · 3 no such worker · 4 worker failed · 5 blocked/awaiting-input · 6 not terminal yet (**includes `wait --timeout` expiry**, spec §4.1) · 7 broker unavailable/version skew · 8 a2a-internal timeout (e.g. state.lock acquisition) · 9 precondition/config · 10 orphaned.
- Output discipline, stated once: **stdout always carries the machine payload** (the JSON envelope in `--json` mode; the plain payload otherwise); **stderr carries diagnostics, warnings, progress, and human decoration**. `--json` emits exactly one envelope: `{"schema_version":1,"ok":…,"command":…,"data":…}` / `{…,"error":{"code","message","retryable","remedy"}}`.
- Worker ids: `<lane>-<mmddthhmm>-<4 lowercase base32>`, CLI-allocated with a collision check against live dirs AND tombstones (never reused), prefix-resolvable.
- Every codex invocation is an argv array — never a shell string; **brief/steer text never appears in argv** (prompts travel via a 0600 prompt file wired to codex stdin with the `-` positional; spec §5).
- Secrets never through agent context: ingress scan fails spawn on hits; egress redaction on every printed stream AND at events-append time (spec §5). `OPENAI_API_KEY`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `DEPOT_TOKEN` are scrubbed from every worker's environment even in Phase 1 (billing + leak critical, spec §4.2).
- `--ephemeral` must never be passed to codex (kills resumability, spec §3).
- Worker state enum and transition table live in `docs/machines/worker.md`; the `step()` match has no catch-all arm over `(state, event)` pairs, enforced by a source-scan test (spec §4.6).
- Codex invocations pass security settings explicitly per spawn — sandbox, approvals, hooks, network, **model, and effort** (`-s <mode>`, `["-c","approval_policy=\"never\""]`, `["-c","features.hooks=false"]`, `["-c","sandbox_workspace_write.network_access=<bool>"]`, `-m <model>`, `["-c","model_reasoning_effort=\"<effort>\""]`, `--skip-git-repo-check`, `--output-last-message`, `--add-dir <worker>/outbox`). `-c` values are TOML, so inner double quotes are intentional and literal; the bracketed forms above are exact argv elements — no shell quoting anywhere.

## File Structure

```
Cargo.toml                       # edition = "2021"; pinned deps
src/
  main.rs                        # clap entry; single boundary mapping error → (exit code, JSON envelope)
  lib.rs                         # module exports (integration tests consume the crate)
  output.rs                      # ExitCode enum, envelope emit, output discipline
  home.rs                        # A2aHome resolution, 0700 tree, LAYOUT_VERSION gate
  config.rs                      # layered Config + LaneConfig (canonicalized allowed_roots) + origins
  ids.rs                         # WorkerId generate/validate/prefix-resolve
  events.rs                      # Envelope, EventKind/EventClass/Transport, exec-JSONL → envelope mapping
  store.rs                       # write_atomic, EventLog (append+sync, torn-tail repair), read_events
  redact.rs                      # ingress scan + filename refusal + egress redaction
  fsm.rs                         # WorkerState, FsmEvent, step(), Rejected, Transition
  fold.rs                        # WAL fold (genesis special-case, epoch fencing) + recover()
  state.rs                       # WorkerSnapshot serde schema (open enums, schema_version, sandbox fields)
  brief.rs                       # front-matter parse + per-lane validation table + sha256
  exec.rs                        # detached spawn (shim), per-epoch exec stream, flock liveness, witnessed kill
  inbox.rs                       # queue_message, undelivered, deliver_pending (prompt-file stdin)
  reconcile.rs                   # reconcile() under state.lock: ingest, session capture, liveness, deadlines
  cli.rs                         # clap structs + dispatch
  commands/
    mod.rs spawn.rs status.rs ls.rs tail.rs result.rs wait.rs send.rs kill.rs resume.rs gc.rs doctor.rs config_show.rs
docs/machines/worker.md
tests/
  common/mod.rs                  # shared helpers for ALL integration tests (each tests/*.rs is its own crate)
  fixtures/stub-codex            # executable stub emitting scripted --json output
  cli_contract.rs home_layout.rs config_layering.rs store_torture.rs fsm_table.rs fold_model.rs
  exec_lifecycle.rs cmd_spawn.rs cmd_status.rs cmd_ls_tail.rs cmd_result_wait.rs cmd_send.rs
  cmd_kill_resume.rs cmd_gc.rs cmd_doctor.rs e2e_exec.rs
```

Design rule carried from the spec: `step()` and `fold()` are pure (clock injected); all IO lives in store/exec/reconcile; commands compose the pure core with IO.

---

### Task 1: Crate scaffold, exit-code contract, output envelope

**Acceptance Criteria (user-visible, CLI-observable):**
- `a2a --version` exits 0 and prints `a2a <semver>` to stdout.
- `a2a definitely-not-a-command` exits 2 (clap usage error).
- `a2a --json __selftest-error` prints exactly one JSON object to stdout with `"ok":false` and `"error"."code":"SELFTEST"`, and exits 9.
- A cargo test pins every numeric exit code so a refactor cannot renumber them.
- `Cargo.toml` contains `edition = "2021"` and pins `rand` to 0.8.x and `nix` to 0.29.x (pinned by a test reading Cargo.toml).

**Verify via:** cli
**Apply:** docs/development/appendix-rust.md — domain errors as enums with one boundary mapping; docs/development/appendix-api-design.md — explicit versioned contracts.

**Files:**
- Create: `Cargo.toml`, `src/main.rs`, `src/lib.rs`, `src/output.rs`, `src/cli.rs`
- Test: `tests/cli_contract.rs`

**Interfaces:**
- Produces: `output::ExitCode` (repr(i32), the 11 pinned codes), `output::AppError { code: &'static str, message: String, retryable: bool, remedy: Option<String>, exit: ExitCode }` with `AppError::new(code, exit, msg)` and builder `.remedy(text)`, `output::emit_ok(json: bool, command: &str, data: serde_json::Value)`, `output::emit_err(json: bool, command: &str, err: &AppError) -> i32`, `cli::GlobalArgs { json: bool, home: Option<PathBuf>, quiet: bool }`. Every later command returns `Result<serde_json::Value, AppError>`; `main` does the only exit-code mapping.

- [ ] **Step 1: Scaffold with pinned toolchain assumptions**

```bash
cd ~/dev/a2a && cargo init --name a2a --edition 2021
cargo add clap@4 --features derive
cargo add serde --features derive
cargo add serde_json serde_yaml toml chrono@0.4 sha2 regex
cargo add rand@0.8
cargo add nix@0.29 --features process,fs,signal
cargo add --dev assert_cmd predicates tempfile
grep -q 'edition = "2021"' Cargo.toml || sed -i '' 's/edition = ".*"/edition = "2021"/' Cargo.toml
```

- [ ] **Step 2: Write the failing contract test**

```rust
// tests/cli_contract.rs
use assert_cmd::Command;

#[test]
fn version_prints_semver_and_exits_zero() {
    let out = Command::cargo_bin("a2a").unwrap().arg("--version").assert().success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    assert!(stdout.trim().starts_with("a2a "), "got: {stdout}");
}

#[test]
fn unknown_subcommand_exits_2() {
    Command::cargo_bin("a2a").unwrap().arg("definitely-not-a-command").assert().code(2);
}

#[test]
fn selftest_error_emits_single_json_envelope_and_exit_9() {
    let out = Command::cargo_bin("a2a").unwrap().args(["--json", "__selftest-error"]).assert().code(9);
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).expect("stdout must be exactly one JSON object");
    assert_eq!(v["schema_version"], 1);
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["code"], "SELFTEST");
    assert!(v["error"]["message"].is_string());
}

#[test]
fn exit_codes_are_pinned() {
    use a2a::output::ExitCode::*;
    for (code, n) in [(Success,0),(Internal,1),(Usage,2),(NoSuchWorker,3),(WorkerFailed,4),
                      (Blocked,5),(NotTerminal,6),(BrokerUnavailable,7),(CmdTimeout,8),
                      (Precondition,9),(Orphaned,10)] {
        assert_eq!(code as i32, n);
    }
}

#[test]
fn toolchain_is_pinned() {
    let manifest = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap();
    assert!(manifest.contains("edition = \"2021\""));
    assert!(manifest.contains("rand = \"0.8") || manifest.contains("rand = { version = \"0.8"));
    assert!(manifest.contains("nix = \"0.29") || manifest.contains("nix = { version = \"0.29"));
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test --test cli_contract`
Expected: compile failure — `a2a::output` does not exist.

- [ ] **Step 4: Implement output.rs, cli.rs, main.rs, lib.rs**

```rust
// src/output.rs
use serde_json::{json, Value};

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(i32)]
pub enum ExitCode {
    Success = 0, Internal = 1, Usage = 2, NoSuchWorker = 3, WorkerFailed = 4,
    Blocked = 5, NotTerminal = 6, BrokerUnavailable = 7, CmdTimeout = 8,
    Precondition = 9, Orphaned = 10,
}

#[derive(Debug)]
pub struct AppError {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
    pub remedy: Option<String>,
    pub exit: ExitCode,
}

impl AppError {
    pub fn new(code: &'static str, exit: ExitCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), retryable: false, remedy: None, exit }
    }
    pub fn remedy(mut self, r: impl Into<String>) -> Self { self.remedy = Some(r.into()); self }
}

/// stdout = machine payload only; stderr = diagnostics/decoration.
pub fn emit_ok(json_mode: bool, command: &str, data: Value) {
    if json_mode {
        println!("{}", json!({"schema_version":1,"ok":true,"command":command,"data":data}));
    } else if let Some(s) = data.as_str() {
        println!("{s}");
    } else if !data.is_null() {
        println!("{}", serde_json::to_string_pretty(&data).unwrap());
    }
}

pub fn emit_err(json_mode: bool, command: &str, err: &AppError) -> i32 {
    if json_mode {
        println!("{}", json!({"schema_version":1,"ok":false,"command":command,
            "error":{"code":err.code,"message":err.message,"retryable":err.retryable,"remedy":err.remedy}}));
    } else {
        eprintln!("a2a: {}: {}{}", err.code, err.message,
            err.remedy.as_deref().map(|r| format!(" (remedy: {r})")).unwrap_or_default());
    }
    err.exit as i32
}
```

```rust
// src/cli.rs
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
```

```rust
// src/main.rs
use a2a::cli::{Cli, Cmd};
use a2a::output::{emit_err, emit_ok, AppError, ExitCode};
use clap::Parser;

fn main() {
    let cli = Cli::parse(); // clap exits 2 on usage errors
    let cmd_name = command_name(&cli.cmd);
    let code = match run(&cli) {
        Ok(data) => { emit_ok(cli.global.json, cmd_name, data); 0 }
        Err(e) => emit_err(cli.global.json, cmd_name, &e),
    };
    std::process::exit(code);
}

fn command_name(cmd: &Cmd) -> &'static str {
    match cmd { Cmd::SelftestError => "__selftest-error" }
}

fn run(cli: &Cli) -> Result<serde_json::Value, AppError> {
    match &cli.cmd {
        Cmd::SelftestError => Err(AppError::new("SELFTEST", ExitCode::Precondition, "canned error")),
    }
}
```

`src/lib.rs`: `pub mod output; pub mod cli;` (grows with each task).

- [ ] **Step 5: Run tests to verify pass**

Run: `cargo test --test cli_contract`
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: scaffold a2a crate (edition 2021, pinned deps) with exit-code contract"
```

---

### Task 2: A2A_HOME resolution and 0700 tree

**Acceptance Criteria (user-visible, CLI-observable):**
- With `A2A_HOME=<tmp>`, `a2a config show` creates `<tmp>` with mode 0700 containing `workers/`, `worktrees/`, and `LAYOUT_VERSION` containing `1`.
- `--home <dir>` beats `A2A_HOME` env (running with both set uses the flag's dir; the env dir is not created).
- If `LAYOUT_VERSION` contains `999`, commands exit 9 with error code `LAYOUT_VERSION_MISMATCH` and a remedy mentioning `a2a migrate`.
- `stat -f %Lp` on the created root prints `700`.

**Verify via:** cli
**Apply:** docs/development/INFRA_CONFIG.md — configs 0600/0700 owner-only, created restrictive (never chmod-after).

**Files:**
- Create: `src/home.rs`
- Modify: `src/lib.rs`, `src/cli.rs`, `src/main.rs` (temporary `config show` stub returning `{"home": <path>}` — replaced by Task 3)
- Test: `tests/home_layout.rs`

**Interfaces:**
- Produces: `home::A2aHome` with `pub fn resolve(flag: Option<&Path>) -> Result<A2aHome, AppError>` (flag > `$A2A_HOME` > `~/.a2a`; creates tree; gates on layout version), `pub fn root(&self) -> &Path`, `pub fn workers_dir(&self) -> PathBuf`, `pub fn worker_dir(&self, id: &str) -> PathBuf`, `pub fn worktrees_dir(&self) -> PathBuf`, `pub fn list_worker_ids(&self) -> std::io::Result<Vec<String>>`, `pub const LAYOUT_VERSION: u32 = 1`, `pub fn create_dir_0700(p: &Path) -> std::io::Result<()>`, and `pub fn write_new_0600(p: &Path) -> std::io::Result<std::fs::File>` (create+write+**truncate** — used for snapshots and one-shot files ONLY; the exec transport uses `exec::open_append_0600`, Task 11, for its streams).

- [ ] **Step 1: Write the failing test**

```rust
// tests/home_layout.rs
use assert_cmd::Command;
use std::os::unix::fs::PermissionsExt;

fn cmd(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home);
    c
}

#[test]
fn home_tree_created_0700_with_layout_version() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("a2a-home");
    cmd(&home).args(["config", "show"]).assert().success();
    let mode = std::fs::metadata(&home).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    assert!(home.join("workers").is_dir());
    assert!(home.join("worktrees").is_dir());
    assert_eq!(std::fs::read_to_string(home.join("LAYOUT_VERSION")).unwrap().trim(), "1");
}

#[test]
fn home_flag_beats_env() {
    let tmp = tempfile::tempdir().unwrap();
    let (a, b) = (tmp.path().join("env-home"), tmp.path().join("flag-home"));
    let out = cmd(&a).arg("--home").arg(&b).args(["--json", "config", "show"]).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["home"], b.to_str().unwrap());
    assert!(b.exists() && !a.exists());
}

#[test]
fn layout_version_mismatch_exits_9() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    cmd(&home).args(["config", "show"]).assert().success();
    std::fs::write(home.join("LAYOUT_VERSION"), "999\n").unwrap();
    let out = cmd(&home).args(["--json", "config", "show"]).assert().code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "LAYOUT_VERSION_MISMATCH");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test home_layout` — FAIL (`config show` unknown / home.rs missing).

- [ ] **Step 3: Implement home.rs and wire the `config show` stub**

```rust
// src/home.rs
use crate::output::{AppError, ExitCode};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub const LAYOUT_VERSION: u32 = 1;

pub struct A2aHome { root: PathBuf }

pub fn create_dir_0700(p: &Path) -> std::io::Result<()> {
    if p.exists() { return Ok(()); }
    if let Some(parent) = p.parent() { if !parent.exists() { create_dir_0700(parent)?; } }
    fs::DirBuilder::new().mode(0o700).create(p)
}

/// Create-or-truncate, 0600. Snapshots and one-shot files ONLY (streams use append helpers).
pub fn write_new_0600(p: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).write(true).truncate(true).mode(0o600).open(p)
}

impl A2aHome {
    pub fn resolve(flag: Option<&Path>) -> Result<Self, AppError> {
        let root = flag.map(PathBuf::from)
            .or_else(|| std::env::var_os("A2A_HOME").map(PathBuf::from))
            .unwrap_or_else(|| dirs_home().join(".a2a"));
        create_dir_0700(&root).map_err(io_err)?;
        create_dir_0700(&root.join("workers")).map_err(io_err)?;
        create_dir_0700(&root.join("worktrees")).map_err(io_err)?;
        let vf = root.join("LAYOUT_VERSION");
        if vf.exists() {
            let v: u32 = fs::read_to_string(&vf).map_err(io_err)?.trim().parse().unwrap_or(0);
            if v != LAYOUT_VERSION {
                return Err(AppError::new("LAYOUT_VERSION_MISMATCH", ExitCode::Precondition,
                    format!("home layout v{v}, binary expects v{LAYOUT_VERSION}"))
                    .remedy("run `a2a migrate` (or move the old home aside)"));
            }
        } else {
            use std::io::Write;
            writeln!(write_new_0600(&vf).map_err(io_err)?, "{LAYOUT_VERSION}").map_err(io_err)?;
        }
        Ok(Self { root })
    }
    pub fn root(&self) -> &Path { &self.root }
    pub fn workers_dir(&self) -> PathBuf { self.root.join("workers") }
    pub fn worker_dir(&self, id: &str) -> PathBuf { self.workers_dir().join(id) }
    pub fn worktrees_dir(&self) -> PathBuf { self.root.join("worktrees") }
    pub fn list_worker_ids(&self) -> std::io::Result<Vec<String>> {
        let mut out = vec![];
        for e in fs::read_dir(self.workers_dir())? {
            let e = e?;
            if e.file_type()?.is_dir() { out.push(e.file_name().to_string_lossy().into_owned()); }
        }
        out.sort();
        Ok(out)
    }
}

fn dirs_home() -> PathBuf { PathBuf::from(std::env::var_os("HOME").expect("HOME unset")) }
fn io_err(e: std::io::Error) -> AppError { AppError::new("IO", ExitCode::Internal, e.to_string()) }
```

In `cli.rs` add `Config { #[command(subcommand)] cmd: ConfigCmd }` with `ConfigCmd::Show { #[arg(long)] origin: bool }`; in `main.rs` route it (for now) to `Ok(serde_json::json!({"home": home.root()}))` after `A2aHome::resolve(cli.global.home.as_deref())?`.

- [ ] **Step 4: Run tests** — `cargo test --test home_layout` — 3 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: A2A_HOME resolution with 0700 tree and layout-version gate"
```

---

### Task 3: Layered config and `a2a config show --origin`

**Acceptance Criteria (user-visible, CLI-observable):**
- With no config file, `a2a --json config show` succeeds; `data.lanes.impl` has `sandbox: "workspace-write"`, `model: "gpt-5.5"`, `effort: "high"`, `worktree_default: false` (Phase 1 — worktrees land in Phase 4; the spec default flips then); `data.lanes.review` has `sandbox: "outbox-only"`, `effort: "medium"`; `data.lanes.research.network` is `true` (spec §4.9).
- With `$A2A_HOME/config.toml` containing `[lanes.impl]\ntask_timeout_secs = 60`, `config show` reflects 60; with additionally `A2A_LANES_IMPL_TASK_TIMEOUT_SECS=90` in env, it reflects 90 (env beats file).
- `a2a config show --origin` labels those two values `file` and `env`; untouched values are labeled `default`.
- A config file containing an unknown key `sandbx = "x"` warns on stderr naming the key, exit 0.
- `allowed_roots` entries are canonicalized at load (a unit test pins that on macOS `/tmp` and `/private/tmp` compare equal after loading); entries that do not exist are dropped with a stderr warning.

**Verify via:** cli
**Apply:** docs/development/appendix-api-design.md — explicit contracts, not tribal knowledge (precedence stated and testable).

**Files:**
- Create: `src/config.rs`, `src/commands/mod.rs`, `src/commands/config_show.rs`
- Modify: `src/lib.rs`, `src/cli.rs`, `src/main.rs` (route `config show` through `commands::config_show::run`, deleting the Task 2 stub)
- Test: `tests/config_layering.rs` + unit tests in `src/config.rs`

**Interfaces:**
- Produces:
```rust
#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Lane { Impl, Review, Research, General }       // FromStr + Display: "impl" | "review" | "research" | "general"

#[derive(Serialize, Deserialize, PartialEq, Clone, Copy, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode { WorkspaceWrite, OutboxOnly, DangerFullAccess }

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LaneConfig {
    pub sandbox: SandboxMode, pub network: bool, pub worktree_default: bool,
    pub model: String, pub effort: String,               // NON-optional: spec §4.7 forbids inheriting codex's global config
    pub start_timeout_secs: u64, pub task_timeout_secs: u64, pub blocked_timeout_secs: u64,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub codex_bin: Option<PathBuf>,                      // A2A_CODEX_BIN env / config key; else "codex" on PATH
    pub allowed_roots: Vec<PathBuf>,                     // CANONICALIZED at load; default [~/dev]
    pub lanes: BTreeMap<Lane, LaneConfig>,
}

#[derive(Serialize, PartialEq, Clone, Copy, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Origin { Default, File, Env }

pub fn load(home: &A2aHome) -> Result<(Config, BTreeMap<String, Origin>, Vec<String> /*warnings*/), AppError>;
```
Lane defaults (spec §4.9, with the Phase-1 worktree exception): impl = workspace-write / network false / worktree_default **false** / gpt-5.5 / high; review = outbox-only / network false / gpt-5.5 / medium; research = outbox-only / network **true** / gpt-5.5 / medium; general = workspace-write / network false / gpt-5.5 / medium. Timeouts default 120/3600/1800. Env keys: `A2A_LANES_<LANE>_<FIELD>` (upper-cased) and top-level `A2A_ALLOWED_ROOTS` (colon-separated) / `A2A_CODEX_BIN`.

- [ ] **Step 1: Write the failing tests**

```rust
// tests/config_layering.rs
use assert_cmd::Command;

fn show(home: &std::path::Path, extra_env: &[(&str, &str)], origin: bool) -> serde_json::Value {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home).args(["--json", "config", "show"]);
    if origin { c.arg("--origin"); }
    for (k, v) in extra_env { c.env(k, v); }
    serde_json::from_slice(&c.assert().success().get_output().stdout).unwrap()
}

#[test]
fn defaults_match_spec_lane_table() {
    let tmp = tempfile::tempdir().unwrap();
    let v = show(tmp.path(), &[], false);
    assert_eq!(v["data"]["lanes"]["impl"]["sandbox"], "workspace-write");
    assert_eq!(v["data"]["lanes"]["impl"]["model"], "gpt-5.5");
    assert_eq!(v["data"]["lanes"]["impl"]["effort"], "high");
    assert_eq!(v["data"]["lanes"]["impl"]["worktree_default"], false);
    assert_eq!(v["data"]["lanes"]["review"]["sandbox"], "outbox-only");
    assert_eq!(v["data"]["lanes"]["review"]["effort"], "medium");
    assert_eq!(v["data"]["lanes"]["research"]["network"], true);
}

#[test]
fn env_beats_file_beats_default() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path()).unwrap();
    std::fs::write(tmp.path().join("config.toml"), "[lanes.impl]\ntask_timeout_secs = 60\n").unwrap();
    let v = show(tmp.path(), &[], false);
    assert_eq!(v["data"]["lanes"]["impl"]["task_timeout_secs"], 60);
    let v = show(tmp.path(), &[("A2A_LANES_IMPL_TASK_TIMEOUT_SECS", "90")], false);
    assert_eq!(v["data"]["lanes"]["impl"]["task_timeout_secs"], 90);
    let v = show(tmp.path(), &[("A2A_LANES_IMPL_TASK_TIMEOUT_SECS", "90")], true);
    assert_eq!(v["data"]["origins"]["lanes.impl.task_timeout_secs"], "env");
    assert_eq!(v["data"]["origins"]["lanes.review.sandbox"], "default");
}

#[test]
fn unknown_key_warns_on_stderr_but_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("config.toml"), "sandbx = \"oops\"\n").unwrap();
    let out = Command::cargo_bin("a2a").unwrap()
        .env("A2A_HOME", tmp.path()).args(["config", "show"]).assert().success();
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();
    assert!(stderr.contains("sandbx"), "stderr must name the unknown key: {stderr}");
}
```

```rust
// unit test in src/config.rs
#[cfg(test)]
mod tests {
    #[test]
    fn allowed_roots_are_canonicalized() {
        // /tmp is a symlink to /private/tmp on macOS; after canonicalization both spell the same path
        let roots = super::canonicalize_roots(vec!["/tmp".into()], &mut vec![]);
        assert_eq!(roots, vec![std::fs::canonicalize("/tmp").unwrap()]);
    }
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.

- [ ] **Step 3: Implement config.rs + commands/config_show.rs**

Mechanics (shape fixed by the Interfaces block): parse TOML into `toml::Value`, walk known keys into the typed struct collecting unknown key paths as warnings (no `deny_unknown_fields`); apply env overrides per key; track origins in a dotted-path map at each layer; `pub(crate) fn canonicalize_roots(raw: Vec<PathBuf>, warnings: &mut Vec<String>) -> Vec<PathBuf>` canonicalizes each entry and drops+warns on missing ones. `config_show::run` serializes `{home, lanes, allowed_roots, codex_bin}` (+ `origins` with `--origin`) and prints warnings to stderr as `a2a: warning: unknown config key: <key>`.

- [ ] **Step 4: Run tests** — `cargo test --test config_layering && cargo test config::` — passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: layered config with canonicalized roots, per-lane model/effort, origins"
```

---

### Task 4: Worker ids — generate, validate, prefix-resolve

**Acceptance Criteria (unit-verified via `cargo test ids`; becomes CLI-observable at Task 12's spawn):**
- Generated ids match `^(impl|review|research|general)-[0-9]{4}t[0-9]{4}-[a-z2-7]{4}$` (all lowercase, spec §4.1).
- Resolving the unique prefix of an existing worker dir returns the full id; an ambiguous prefix fails `AMBIGUOUS_WORKER` (exit 3) listing all candidates in the message; a miss fails `NO_SUCH_WORKER` (exit 3).

**Verify via:** cli

**Files:**
- Create: `src/ids.rs`
- Modify: `src/lib.rs`
- Test: unit tests in `src/ids.rs`

**Interfaces:**
- Produces: `ids::generate(lane: Lane, now: chrono::DateTime<chrono::Utc>) -> String`, `ids::is_valid(id: &str) -> bool` (also the gc traversal guard, Task 18), `ids::resolve(home: &A2aHome, prefix: &str) -> Result<String, AppError>`. Collision policy: `generate` is pure-random; **Task 12's spawn flow owns uniqueness** (retry loop against live dirs + tombstones).

- [ ] **Step 1: Write failing unit tests (no process-env mutation — pass the home explicitly)**

```rust
// in src/ids.rs
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn generate_matches_contract_shape() {
        let now = Utc.with_ymd_and_hms(2026, 8, 14, 13, 18, 0).unwrap();
        let id = generate(crate::config::Lane::Impl, now);
        let re = regex::Regex::new(r"^impl-0814t1318-[a-z2-7]{4}$").unwrap();
        assert!(re.is_match(&id), "got {id}");
        assert!(is_valid(&id));
    }

    #[test]
    fn resolve_prefix_unique_ambiguous_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let home = crate::home::A2aHome::resolve(Some(tmp.path())).unwrap();
        for id in ["impl-0814t1300-aaaa", "impl-0814t1300-aabb", "review-0814t1301-cccc"] {
            std::fs::create_dir_all(home.worker_dir(id)).unwrap();
        }
        assert_eq!(resolve(&home, "review").unwrap(), "review-0814t1301-cccc");
        let err = resolve(&home, "impl-0814t1300-aa").unwrap_err();
        assert_eq!(err.code, "AMBIGUOUS_WORKER");
        assert!(err.message.contains("aabb") && err.message.contains("aaaa"));
        assert_eq!(resolve(&home, "zzz").unwrap_err().code, "NO_SUCH_WORKER");
    }
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test ids` — FAIL.

- [ ] **Step 3: Implement**

```rust
// src/ids.rs
use crate::config::Lane;
use crate::home::A2aHome;
use crate::output::{AppError, ExitCode};
use rand::Rng;

const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";

pub fn generate(lane: Lane, now: chrono::DateTime<chrono::Utc>) -> String {
    let mut rng = rand::thread_rng();
    let suffix: String = (0..4).map(|_| B32[rng.gen_range(0..32)] as char).collect();
    format!("{}-{}-{}", lane, now.format("%m%dt%H%M"), suffix)
}

pub fn is_valid(id: &str) -> bool {
    regex::Regex::new(r"^(impl|review|research|general)-[0-9]{4}t[0-9]{4}-[a-z2-7]{4}$")
        .unwrap().is_match(id)
}

pub fn resolve(home: &A2aHome, prefix: &str) -> Result<String, AppError> {
    let ids = home.list_worker_ids().map_err(|e| AppError::new("IO", ExitCode::Internal, e.to_string()))?;
    let matches: Vec<_> = ids.into_iter().filter(|i| i.starts_with(prefix)).collect();
    match matches.len() {
        1 => Ok(matches.into_iter().next().unwrap()),
        0 => Err(AppError::new("NO_SUCH_WORKER", ExitCode::NoSuchWorker, format!("no worker matches '{prefix}'"))),
        _ => Err(AppError::new("AMBIGUOUS_WORKER", ExitCode::NoSuchWorker,
                 format!("'{prefix}' is ambiguous: {}", matches.join(", ")))),
    }
}
```

(`Lane::Display` from Task 3 emits the lowercase name.)

- [ ] **Step 4: Run tests** — `cargo test ids` — 2 passed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: worker id generation and prefix resolution"
```

---

### Task 5: Event envelope and the exec-JSONL mapping

**Acceptance Criteria (unit-verified here; CLI-observable via `a2a tail` in Task 14):**
- Every envelope serializes with `schema_version`, `seq`, `ts`, `epoch`, `class` (`lifecycle`|`trace`), `kind`, `transport`, `raw` (spec §4.5).
- Deserializing an envelope whose `kind` is unknown to this binary yields `EventKind::Unknown` — never a parse error (open enum).
- The exec mapping turns an agent-message line into `(Lifecycle, AgentMessage)`, a **session-id line into `(Lifecycle, SessionIdentified)`** (the id itself is extracted from `raw` by reconcile — Task 13), a token-delta line into `None` (dropped, spec §4.5), and an unrecognized-but-valid line into `(Trace, Unknown)`.

**Verify via:** cli
**Apply:** docs/development/appendix-api-design.md — open enums, readers ignore unknown fields.

**Files:**
- Create: `src/events.rs`
- Modify: `src/lib.rs`
- Test: unit tests in `src/events.rs`

**Interfaces:**
- Produces:
```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Envelope {
    pub schema_version: u32,          // 1
    pub seq: u64,                     // stamped by EventLog::append
    pub ts: chrono::DateTime<chrono::Utc>,
    pub epoch: u32,
    pub class: EventClass,
    pub kind: EventKind,
    pub transport: Transport,
    #[serde(default)] pub raw: serde_json::Value,
    #[serde(flatten)] pub extra: serde_json::Map<String, serde_json::Value>,
}
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum EventClass { Lifecycle, Trace, #[serde(other)] Unknown }
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum EventKind { Spawned, SessionIdentified, TurnStarted, AgentMessage, ToolCall, CommandExec,
    Blocked, OutboxNotice, StateChange, EventsDropped, TransportRestarted, Timeout, Interrupted,
    Killed, Orphaned, Resumed, Error, TurnCompleted, ResultWritten, #[serde(other)] Unknown }
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum Transport { AppServer, Exec, #[serde(other)] Unknown }
impl Envelope {
    pub fn new(epoch: u32, transport: Transport, class: EventClass, kind: EventKind,
               raw: serde_json::Value, now: chrono::DateTime<chrono::Utc>) -> Self;  // seq = 0 until appended
    pub fn synthetic(kind: EventKind) -> Self;   // epoch 1, exec transport, lifecycle, now — for tests
}
/// Maps one raw `codex exec --json` line by its "type" field. None = drop (token deltas).
pub fn map_exec_line(raw: &serde_json::Value) -> Option<(EventClass, EventKind)>;
```
Mapping table (single source; Task 20's live run corrects it against the real vocabulary): any type containing `delta` → `None`; `session.created` | `session_configured` | `thread.started` → `(Lifecycle, SessionIdentified)`; `agent_message` → `(Lifecycle, AgentMessage)`; `turn.started` | `task_started` → `(Lifecycle, TurnStarted)`; `turn.completed` | `task_complete` → `(Lifecycle, TurnCompleted)`; types starting `exec_command`/`command` → `(Trace, CommandExec)`; types containing `tool` → `(Trace, ToolCall)`; `error` → `(Lifecycle, Error)`; anything else → `(Trace, Unknown)`.

- [ ] **Step 1: Write failing unit tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_kind_roundtrips_as_unknown() {
        let line = r#"{"schema_version":1,"seq":9,"ts":"2026-08-14T00:00:00Z","epoch":1,
                       "class":"lifecycle","kind":"kind_from_the_future","transport":"exec","raw":{}}"#;
        let e: Envelope = serde_json::from_str(line).unwrap();
        assert!(matches!(e.kind, EventKind::Unknown));
        assert_eq!(e.seq, 9);
    }

    #[test]
    fn exec_mapping_covers_session_message_delta_unknown() {
        assert_eq!(map_exec_line(&json!({"type":"session.created","session_id":"s1"})),
                   Some((EventClass::Lifecycle, EventKind::SessionIdentified)));
        assert_eq!(map_exec_line(&json!({"type":"agent_message","message":"hi"})),
                   Some((EventClass::Lifecycle, EventKind::AgentMessage)));
        assert_eq!(map_exec_line(&json!({"type":"agent_message_delta","delta":"h"})), None);
        assert_eq!(map_exec_line(&json!({"type":"totally_new_thing"})),
                   Some((EventClass::Trace, EventKind::Unknown)));
    }
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test events` — FAIL.
- [ ] **Step 3: Implement** per the interface block.
- [ ] **Step 4: Run tests** — 2 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: normalized event envelope with open enums and exec-JSONL mapping"
```

---

### Task 6: Crash-safe store — atomic writes and the torn-tail-repairing event log

**Acceptance Criteria (verified by a torture test; underpins every later command):**
- `store::write_atomic` leaves either the old or the new complete content at the path, never a partial file, and litters no temp files.
- `EventLog::append` assigns strictly increasing `seq` from 1 and calls `sync_data` after each write (WAL durability, spec §4.6); `read_events` yields only complete lines — a file truncated mid-final-line yields all prior events and no error.
- `EventLog::open` on a torn-tailed file REPAIRS it (truncates to the last complete line) so subsequent appends are parseable — after repair+append, `read_events` returns ALL events including the new one.
- All files created by the store are mode 0600.

**Verify via:** cli
**Apply:** docs/development/appendix-streams.md — WAL append-before-apply, readers tolerate torn tails; docs/development/SHELL_SCRIPTING_GUIDE.md §3.4 — restrictive modes at creation.

**Files:**
- Create: `src/store.rs`
- Modify: `src/lib.rs`
- Test: `tests/store_torture.rs`

**Interfaces:**
- Consumes: `events::Envelope` (Task 5).
- Produces:
```rust
pub fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()>;   // 0600 temp in same dir → fsync → rename → fsync(dir)
pub struct EventLog { /* O_APPEND file + next_seq */ }
impl EventLog {
    pub fn open(worker_dir: &Path) -> io::Result<Self>;             // scans; repairs torn tail via set_len
    pub fn append(&mut self, env: &mut events::Envelope) -> io::Result<u64>; // stamps seq, one write, sync_data
}
pub fn read_events(worker_dir: &Path, since_seq: u64) -> io::Result<Vec<events::Envelope>>;
```

- [ ] **Step 1: Write the failing torture test**

```rust
// tests/store_torture.rs
use a2a::store;

#[test]
fn write_atomic_never_exposes_partial_content() {
    let tmp = tempfile::tempdir().unwrap();
    let p = tmp.path().join("state.json");
    store::write_atomic(&p, br#"{"v":1}"#).unwrap();
    store::write_atomic(&p, br#"{"v":2,"more":"data"}"#).unwrap();
    assert_eq!(std::fs::read_to_string(&p).unwrap(), r#"{"v":2,"more":"data"}"#);
    let leftovers: Vec<_> = std::fs::read_dir(tmp.path()).unwrap()
        .filter(|e| e.as_ref().unwrap().file_name() != "state.json").collect();
    assert!(leftovers.is_empty(), "temp files left behind: {leftovers:?}");
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(std::fs::metadata(&p).unwrap().permissions().mode() & 0o777, 0o600);
}

#[test]
fn event_log_seq_monotonic_and_torn_tail_tolerated() {
    let tmp = tempfile::tempdir().unwrap();
    let mut log = store::EventLog::open(tmp.path()).unwrap();
    for _ in 0..3 {
        let mut e = a2a::events::Envelope::synthetic(a2a::events::EventKind::StateChange);
        log.append(&mut e).unwrap();
    }
    let path = tmp.path().join("events.jsonl");
    let full = std::fs::read(&path).unwrap();
    let last_line_start = full[..full.len()-1].iter().rposition(|&b| b == b'\n').map(|i| i+1).unwrap();
    // torn strictly inside the final line (cut == full.len()-1 merely strips the trailing
    // newline, leaving a COMPLETE parseable line — asserted separately below)
    for cut in last_line_start+1..full.len()-1 {
        std::fs::write(&path, &full[..cut]).unwrap();
        let evs = store::read_events(tmp.path(), 0).unwrap();
        assert_eq!(evs.len(), 2, "torn tail at {cut} must drop only the torn line");
        assert_eq!(evs.last().unwrap().seq, 2);
    }
    // newline-less but complete final line IS returned
    std::fs::write(&path, &full[..full.len()-1]).unwrap();
    assert_eq!(store::read_events(tmp.path(), 0).unwrap().len(), 3);
}

#[test]
fn open_repairs_torn_tail_so_later_appends_stay_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let mut log = store::EventLog::open(tmp.path()).unwrap();
    for _ in 0..3 {
        let mut e = a2a::events::Envelope::synthetic(a2a::events::EventKind::StateChange);
        log.append(&mut e).unwrap();
    }
    drop(log);
    let path = tmp.path().join("events.jsonl");
    let full = std::fs::read(&path).unwrap();
    std::fs::write(&path, &full[..full.len()-3]).unwrap();   // torn mid-line-3
    let mut log = store::EventLog::open(tmp.path()).unwrap(); // must repair: truncate to end of line 2
    let mut e = a2a::events::Envelope::synthetic(a2a::events::EventKind::StateChange);
    assert_eq!(log.append(&mut e).unwrap(), 3);               // seq continues from last COMPLETE line
    let evs = store::read_events(tmp.path(), 0).unwrap();
    assert_eq!(evs.len(), 3, "post-repair append must be readable, not glued to the fragment");
    assert_eq!(evs.last().unwrap().seq, 3);
}
```

- [ ] **Step 2: Run to verify failure** — FAIL (store missing).

- [ ] **Step 3: Implement store.rs**

```rust
// src/store.rs
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path.parent().expect("write_atomic needs a parent dir");
    let tmp = dir.join(format!(".{}.tmp-{}", path.file_name().unwrap().to_string_lossy(), std::process::id()));
    {
        let mut f = OpenOptions::new().create(true).write(true).truncate(true).mode(0o600).open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

pub struct EventLog { file: File, next_seq: u64 }

impl EventLog {
    pub fn open(worker_dir: &Path) -> io::Result<Self> {
        let path = worker_dir.join("events.jsonl");
        let mut last_seq = 0u64;
        if path.exists() {
            let data = fs::read(&path)?;
            let mut end_of_complete = 0usize;
            for line in complete_lines(&data) {
                if let Ok(e) = serde_json::from_slice::<crate::events::Envelope>(line.bytes_) {
                    last_seq = e.seq;
                    end_of_complete = line.end;
                } else { break; }
            }
            if end_of_complete < data.len() {
                // torn tail (or trailing garbage): repair so future appends stay parseable
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(end_of_complete as u64)?;
                f.sync_all()?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).mode(0o600).open(&path)?;
        Ok(Self { file, next_seq: last_seq + 1 })
    }

    pub fn append(&mut self, env: &mut crate::events::Envelope) -> io::Result<u64> {
        env.seq = self.next_seq;
        let mut line = serde_json::to_vec(env)?;
        line.push(b'\n');
        self.file.write_all(&line)?;   // one write; O_APPEND
        self.file.sync_data()?;        // WAL durability before any side effect
        self.next_seq += 1;
        Ok(env.seq)
    }
}

pub fn read_events(worker_dir: &Path, since_seq: u64) -> io::Result<Vec<crate::events::Envelope>> {
    let path = worker_dir.join("events.jsonl");
    if !path.exists() { return Ok(vec![]); }
    let data = fs::read(&path)?;
    let mut out = vec![];
    for line in complete_lines(&data) {
        match serde_json::from_slice::<crate::events::Envelope>(line.bytes_) {
            Ok(e) if e.seq > since_seq => out.push(e),
            Ok(_) => {}
            Err(_) => break, // torn/garbled: drop this and everything after
        }
    }
    // a final line without a trailing newline may still be complete JSON — try it
    if let Some(tail) = trailing_fragment(&data) {
        if let Ok(e) = serde_json::from_slice::<crate::events::Envelope>(tail) {
            if e.seq > since_seq { out.push(e); }
        }
    }
    Ok(out)
}

struct Line<'a> { bytes_: &'a [u8], end: usize }
/// Newline-terminated lines only (offset-tracked); the unterminated tail is excluded.
fn complete_lines(data: &[u8]) -> Vec<Line<'_>> {
    let mut out = vec![]; let mut start = 0;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' { out.push(Line { bytes_: &data[start..i], end: i + 1 }); start = i + 1; }
    }
    out
}
fn trailing_fragment(data: &[u8]) -> Option<&[u8]> {
    match data.iter().rposition(|&b| b == b'\n') {
        Some(i) if i + 1 < data.len() => Some(&data[i + 1..]),
        None if !data.is_empty() => Some(data),
        _ => None,
    }
}
```

- [ ] **Step 4: Run tests** — `cargo test --test store_torture` — 3 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: crash-safe store (atomic writes; torn-tail-tolerant, self-repairing WAL)"
```

---

### Task 7: Redaction module — ingress refusal, egress masking

**Acceptance Criteria (unit-verified here; wired CLI-observably in Tasks 12–14):**
- `scan_text` on content containing `sk-abc123456789012345678901` reports the line number and rule name — and the hit struct never carries the matched value (pinned by a Debug-format assertion).
- `refuse_filename(".env.production")` returns `Some(rule)`; `refuse_filename("main.rs")` returns `None`. (The `context/` staging path that surfaces this as `SECRET_FILE_REFUSED` arrives in Task 12.)
- `redact_egress("token is xoxb-1234567890-abcdef")` returns `token is [REDACTED:22]` (the full 22-char token masked, length-only leak); a planted-token test proves every listed shape survives no output.

**Verify via:** cli
**Apply:** docs/development/appendix-rag.md — skip-file patterns + inline secret regexes (recipe adopted verbatim).

**Files:**
- Create: `src/redact.rs`
- Modify: `src/lib.rs`
- Test: unit tests in `src/redact.rs`

**Interfaces:**
- Produces:
```rust
#[derive(Debug)]
pub struct IngressHit { pub line: usize, pub rule: &'static str }   // NEVER carries the matched value
pub fn refuse_filename(name: &str) -> Option<&'static str>;
pub fn scan_text(text: &str) -> Vec<IngressHit>;
pub fn redact_egress(text: &str) -> String;                         // every match → [REDACTED:<len>]
```
Filename rules: `.env` + `.env.*`, `*.pem`, `*.key`, `*.pfx`, `*.p12`, `id_rsa*`, `credentials*`, `secrets*`. Value shapes (one compiled set shared by scan and redact): `sk-[A-Za-z0-9_-]{20,}`, `ghp_[A-Za-z0-9]{36}`, `xox[bcdp]-[A-Za-z0-9-]{10,}`, `AKIA[0-9A-Z]{16}`, `(?i)bearer\s+[A-Za-z0-9._~+/=-]{16,}`, `-----BEGIN [A-Z ]*PRIVATE KEY-----`.

- [ ] **Step 1: Write failing unit tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filenames_refused() {
        assert!(refuse_filename(".env.production").is_some());
        assert!(refuse_filename("server.pem").is_some());
        assert!(refuse_filename("id_rsa_1").is_some());
        assert!(refuse_filename("main.rs").is_none());
    }

    #[test]
    fn scan_reports_line_and_rule_never_value() {
        let hits = scan_text("line one\napi key: sk-abc123456789012345678901\n");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].line, 2);
        let debug = format!("{hits:?}");
        assert!(!debug.contains("sk-abc"), "hit must not carry the secret");
    }

    #[test]
    fn egress_masks_with_exact_length() {
        // "xoxb-1234567890-abcdef" is 22 chars; the mask leaks only the length
        assert_eq!(redact_egress("token is xoxb-1234567890-abcdef"),
                   "token is [REDACTED:22]");
    }

    #[test]
    fn planted_tokens_survive_no_egress() {
        for planted in ["sk-abc123456789012345678901", "xoxb-1234567890-abcdef",
                        "ghp_012345678901234567890123456789012345", "AKIAABCDEFGHIJKLMNOP",
                        "Bearer eyJhbGciOiJIUzI1NiJ9.x.y"] {
            let out = redact_egress(&format!("prefix {planted} suffix"));
            assert!(!out.contains(planted), "leaked: {planted}");
            assert!(out.contains("[REDACTED:"), "not masked: {out}");
        }
    }
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement** (`std::sync::LazyLock<Vec<(&'static str, Regex)>>`; `redact_egress` via `replace_all` with `|m: &regex::Captures| format!("[REDACTED:{}]", m[0].len())`).
- [ ] **Step 4: Run tests** — 4 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: redaction module (ingress scan, filename refusal, egress masking)"
```

---

### Task 8: Worker state machine — pure `step()`, full transition table, `docs/machines/worker.md`

**Acceptance Criteria (verified by the table artifact + tests; drives every later command):**
- `docs/machines/worker.md` exists with a transition table (`From | Event | Guard | To | Actions | Notes`) covering all states: spawned, queued, running, awaiting-input, escalated, done, failed, cancelled, orphaned — including genesis (`Spawn`), `ProcessStarted`, `SessionIdentified`, `SessionMissing`, and `Send`-while-running rows.
- A source-scan test proves `step()`'s match has no catch-all arm (`_ =>` / `(_, _)`) — the totality merge gate is enforceable, not vacuous.
- Terminal states absorb everything except `Resume` (which bumps the epoch); `Kill` from running/awaiting-input/**orphaned** produces `cancelled`, never `failed`.
- `Spawned + ProcessStarted → Running`; `Spawned + Timeout{Start} → Failed{NeverStarted}`; `Spawned + ProcessExited → Failed{NeverStarted}` (a worker whose process never came up is distinguishable from one that ran and died — spec §4.6).
- `Running + Send → Running` with no action (the message queues; delivery happens at the next turn boundary — spec §4.3).
- A reachability test proves every state reaches at least one terminal state via BFS **from that state**.

**Verify via:** cli
**Apply:** docs/development/appendix-fsm.md — C1 non-negotiables, C2 (time as events, terminal states), C6/C8 (checked-in table as merge gate).

**Files:**
- Create: `src/fsm.rs`, `docs/machines/worker.md`
- Modify: `src/lib.rs`
- Test: `tests/fsm_table.rs`

**Interfaces:**
- Produces:
```rust
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerState { Spawned, Queued, Running, AwaitingInput, Escalated,
                       Done, Failed, Cancelled, Orphaned, #[serde(other)] Unknown }
impl WorkerState { pub fn is_terminal(self) -> bool /* Done | Failed | Cancelled */ }

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum StateReason { Blocked, Interrupted, TurnEndedWithoutResult, BlockedTimeout, #[serde(other)] Unknown }

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass { RateLimit, AuthExpired, Network, ModelError, Timeout, SandboxDenied,
    SchemaViolation, NoResult, BudgetExhausted, NeverStarted, InternalPanic, SessionMissing,
    #[serde(other)] Unknown }

#[derive(Clone, Debug)]
pub enum FsmEvent {
    Spawn { id: String },                               // genesis ONLY — see fold's special-case (Task 9)
    Admit,                                              // queued → spawned (Phase 2 admission; Phase 1 never enters queued)
    ProcessStarted { pid: u32, start_witness: String },
    SessionIdentified { session_id: String, rollout_path: Option<String> }, // running → running (records ids)
    TurnEnded { has_result: bool, has_question: bool },
    ResultInvalid { detail: String },
    ProcessExited { code: Option<i32> },
    SessionMissing,                                     // emitted by reconcile: resumed process died pre-turn
    Timeout { kind: TimeoutKind },
    Interrupt,
    Kill,
    OrphanDetected,
    Resume { answer_queued: bool },                     // non-running (incl. terminal) → running, epoch+1
    Send,
}
#[derive(Clone, Copy, PartialEq, Debug)] pub enum TimeoutKind { Start, Task, Blocked }

#[derive(Debug)] pub struct Rejected { pub from: WorkerState, pub event: &'static str, pub reason: String }
#[derive(Debug, PartialEq)] pub enum Action { DeliverInbox, KillProcess, RecordEscalation }

pub struct Transition { pub state: WorkerState, pub reason: Option<StateReason>,
                        pub failure: Option<FailureClass>, pub epoch_bump: bool,
                        pub actions: Vec<Action> }
pub fn step(current: WorkerState, reason: Option<StateReason>, ev: &FsmEvent)
    -> Result<Transition, Rejected>;
```
Complete row set (mirrored 1:1 in `docs/machines/worker.md`; anything not listed for a state is an explicit `Rejected` with an actionable reason):
- `Spawn` is legal from NO state — the fold applies it only as the genesis event (Task 9); `step` rejects it from every state (reason: "genesis only").
- `Queued + Admit → Spawned` · `Queued + Kill → Cancelled`.
- `Spawned + ProcessStarted → Running` · `Spawned + Timeout{Start} → Failed{NeverStarted}` · `Spawned + ProcessExited → Failed{NeverStarted}` · `Spawned + Kill → Cancelled`.
- `Running + SessionIdentified → Running` · `Running + Send → Running` (no action; queue-only) · `Running + TurnEnded{result} → Done` · `Running + TurnEnded{question} → AwaitingInput{Blocked}` · `Running + TurnEnded{neither} → AwaitingInput{TurnEndedWithoutResult}` · `Running + ResultInvalid → Failed{SchemaViolation}` · `Running + ProcessExited → Failed{NoResult}` · `Running + SessionMissing → Failed{SessionMissing}` · `Running + Timeout{Task} → Failed{Timeout}` (action KillProcess) · `Running + Interrupt → AwaitingInput{Interrupted}` · `Running + Kill → Cancelled` (action KillProcess) · `Running + OrphanDetected → Orphaned`.
- `AwaitingInput + Send → Running` (action DeliverInbox) · `AwaitingInput + Timeout{Blocked} → Escalated` (action RecordEscalation) · `AwaitingInput + Kill → Cancelled` · `AwaitingInput + OrphanDetected → Orphaned` · `AwaitingInput + Resume → Running` (epoch+1).
- `Escalated + Send → Running` (DeliverInbox) · `Escalated + Kill → Cancelled` · `Escalated + Resume → Running` (epoch+1) · `Escalated + OrphanDetected → Orphaned`.
- `Orphaned + Resume → Running` (epoch+1) · `Orphaned + Kill → Cancelled` · `Orphaned + OrphanDetected → Orphaned` (idempotent).
- `Done|Failed|Cancelled + Resume → Running` (epoch+1); everything else on terminal → Rejected ("worker is <state> — use `a2a resume`").

- [ ] **Step 1: Write the failing table tests**

```rust
// tests/fsm_table.rs
use a2a::fsm::*;

fn all_states() -> Vec<WorkerState> {
    use WorkerState::*;
    vec![Spawned, Queued, Running, AwaitingInput, Escalated, Done, Failed, Cancelled, Orphaned]
}
fn all_events() -> Vec<FsmEvent> {
    use FsmEvent::*;
    vec![Spawn { id: "impl-0814t1300-aaaa".into() },
         Admit,
         ProcessStarted { pid: 1, start_witness: "w".into() },
         SessionIdentified { session_id: "s".into(), rollout_path: None },
         TurnEnded { has_result: true, has_question: false },
         TurnEnded { has_result: false, has_question: true },
         TurnEnded { has_result: false, has_question: false },
         ResultInvalid { detail: "d".into() },
         ProcessExited { code: Some(1) },
         SessionMissing,
         Timeout { kind: TimeoutKind::Start }, Timeout { kind: TimeoutKind::Task },
         Timeout { kind: TimeoutKind::Blocked },
         Interrupt, Kill, OrphanDetected, Resume { answer_queued: false }, Send]
}

#[test]
fn totality_no_catch_all_arm_in_source() {
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/fsm.rs")).unwrap();
    let body = src.split("pub fn step").nth(1).expect("step fn present");
    for forbidden in ["(_, _)", "_ =>"] {
        assert!(!body.contains(forbidden),
            "step() must enumerate every (state, event) pair explicitly; found `{forbidden}`");
    }
}

#[test]
fn every_pair_is_decided_without_panicking() {
    for s in all_states() {
        for e in all_events() {
            let _ = step(s, None, &e); // Ok or Rejected; a panic fails the test
        }
    }
}

#[test]
fn terminal_states_absorb_everything_but_resume() {
    for s in [WorkerState::Done, WorkerState::Failed, WorkerState::Cancelled] {
        for e in all_events() {
            let r = step(s, None, &e);
            match e {
                FsmEvent::Resume { .. } => {
                    let t = r.expect("resume must be legal on terminal");
                    assert_eq!(t.state, WorkerState::Running);
                    assert!(t.epoch_bump);
                }
                _ => assert!(r.is_err(), "{s:?} must reject {e:?}"),
            }
        }
    }
}

#[test]
fn spawn_lifecycle_rows() {
    let t = step(WorkerState::Spawned, None,
                 &FsmEvent::ProcessStarted { pid: 9, start_witness: "w".into() }).unwrap();
    assert_eq!(t.state, WorkerState::Running);
    let t = step(WorkerState::Spawned, None, &FsmEvent::Timeout { kind: TimeoutKind::Start }).unwrap();
    assert_eq!((t.state, t.failure), (WorkerState::Failed, Some(FailureClass::NeverStarted)));
    let t = step(WorkerState::Spawned, None, &FsmEvent::ProcessExited { code: Some(1) }).unwrap();
    assert_eq!((t.state, t.failure), (WorkerState::Failed, Some(FailureClass::NeverStarted)));
}

#[test]
fn kill_is_cancelled_never_failed_and_send_queues_while_running() {
    for s in [WorkerState::Running, WorkerState::AwaitingInput, WorkerState::Orphaned] {
        let t = step(s, None, &FsmEvent::Kill).unwrap();
        assert_eq!(t.state, WorkerState::Cancelled);
        assert!(t.failure.is_none());
    }
    let t = step(WorkerState::Running, None, &FsmEvent::Send).unwrap();
    assert_eq!(t.state, WorkerState::Running);
    assert!(t.actions.is_empty(), "send-while-running queues; no delivery action");
    let t = step(WorkerState::AwaitingInput, Some(StateReason::Blocked), &FsmEvent::Send).unwrap();
    assert!(t.actions.contains(&Action::DeliverInbox));
}

#[test]
fn blocked_timeout_escalates_and_session_missing_is_distinct() {
    let t = step(WorkerState::AwaitingInput, Some(StateReason::Blocked),
                 &FsmEvent::Timeout { kind: TimeoutKind::Blocked }).unwrap();
    assert_eq!(t.state, WorkerState::Escalated);
    let t = step(WorkerState::Running, None, &FsmEvent::SessionMissing).unwrap();
    assert_eq!((t.state, t.failure), (WorkerState::Failed, Some(FailureClass::SessionMissing)));
}

#[test]
fn every_state_reaches_terminal() {
    use std::collections::HashSet;
    for start in all_states() {
        let mut seen = HashSet::new();
        let mut stack = vec![start];
        let mut reached_terminal = false;
        while let Some(s) = stack.pop() {
            if !seen.insert(s) { continue; }
            if s.is_terminal() { reached_terminal = true; break; }
            for e in all_events() {
                if let Ok(t) = step(s, None, &e) { stack.push(t.state); }
            }
        }
        assert!(reached_terminal, "{start:?} cannot reach a terminal state");
    }
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement `step()`** as one exhaustive match over per-state blocks — every event named in each block, no catch-alls (the source-scan test enforces it).
- [ ] **Step 4: Write `docs/machines/worker.md`** — every Ok row + per-state Rejections section + Mermaid `stateDiagram-v2`. Header rule: *any PR changing `step()` updates this table and the tests; totality break = no merge.*
- [ ] **Step 5: Run tests** — `cargo test --test fsm_table` — 7 passed.
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: pure worker FSM with source-enforced totality + docs/machines/worker.md"
```

---

### Task 9: Snapshot schema, WAL fold, and recovery

**Acceptance Criteria (verified by fold tests; CLI-observable through status/ls after crashes):**
- `state.json` round-trips `WorkerSnapshot` with `schema_version: 1`; loading a snapshot with `schema_version: 2` yields `SnapshotError::TooNew`, which every mutating path maps to `AppError{code:"SCHEMA_TOO_NEW", exit: Precondition}` (spec §4.5).
- Model test: folding events 0..k at once equals folding them one at a time, for every prefix k.
- Epoch fencing: an event carrying `epoch: 1` applied to a snapshot at `epoch: 2` advances the seq watermark but does NOT change state.
- Idempotency: re-applying an already-applied seq is a no-op, not an error.
- `recover()` covers the spec §4.6 recovery table: dir-without-state-or-events → `Failed{NeverStarted}`; snapshot=running + dead process → `Orphaned`; snapshot corrupt + events present → rebuilt by folding from zero.

**Verify via:** cli
**Apply:** docs/development/appendix-streams.md — WAL-first, checkpoint+replay, sequence-number dedup; docs/development/appendix-fsm.md — C5 versioned persisted snapshots.

**Files:**
- Create: `src/state.rs`, `src/fold.rs`
- Modify: `src/lib.rs`
- Test: `tests/fold_model.rs`

**Interfaces:**
- Produces:
```rust
// state.rs
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerSnapshot {
    pub schema_version: u32, pub id: String, pub epoch: u32,
    pub lane: Lane, pub owner: Option<String>, pub cwd: PathBuf,
    pub transport: Transport, pub requested_transport: Transport,
    pub sandbox: SandboxMode, pub network: bool, pub writable_roots: Vec<PathBuf>,
    pub elevation_reason: Option<String>,
    pub session_id: Option<String>, pub rollout_path: Option<PathBuf>,
    pub pid: Option<u32>, pub pid_start_witness: Option<String>,
    pub model: String, pub effort: String,
    pub brief_sha: String, pub schema_path: Option<PathBuf>,
    pub state: WorkerState, pub state_reason: Option<StateReason>,
    pub failure_class: Option<FailureClass>,
    pub entered_state_at: DateTime<Utc>, pub deadline_at: Option<DateTime<Utc>>,
    pub last_event_seq: u64, pub last_applied_seq: u64, pub last_delivered_seq: u64,
    pub created_at: DateTime<Utc>, pub updated_at: DateTime<Utc>,
    #[serde(flatten)] pub extra: serde_json::Map<String, serde_json::Value>,
}
impl WorkerSnapshot {
    /// Plain pub fn (NOT #[cfg(test)] — integration tests are external crates).
    /// Seeds: schema_version 1, epoch 1, state Spawned, lane parsed from the id prefix
    /// (fallback General), cwd = given, transport/requested = Exec, sandbox WorkspaceWrite,
    /// network false, model "gpt-5.5", effort "medium", all seqs 0, timestamps Utc::now().
    pub fn new_for_test(id: &str, cwd: &Path) -> Self;
}
pub enum SnapshotError { Missing, Corrupt, TooNew(u32), Io(std::io::Error) }
pub fn load_snapshot(worker_dir: &Path) -> Result<WorkerSnapshot, SnapshotError>;
pub fn save_snapshot(worker_dir: &Path, snap: &WorkerSnapshot) -> std::io::Result<()>; // write_atomic

// fold.rs
/// Lifecycle envelopes carry their FsmEvent payload in raw as {"fsm":"<Variant>", ...fields}.
pub fn envelope_to_fsm_event(env: &events::Envelope) -> Option<fsm::FsmEvent>; // trace/unknown → None
/// GENESIS SPECIAL-CASE: an envelope with raw.fsm == "Spawn" does NOT go through step().
/// It seeds identity fields directly from raw (id, lane, cwd, brief_sha, sandbox, network,
/// model, effort, created_at, transport) and sets state = Spawned. Every other event:
/// skip if seq <= last_applied_seq (advance nothing); consume the seq; apply step() only
/// when epoch >= snap.epoch AND envelope_to_fsm_event is Some; a Rejected during fold is
/// recorded into extra["fold_rejections"] — the fold NEVER errors (recovery always
/// terminates in readable state). SessionIdentified also copies session_id/rollout_path
/// from raw into the snapshot.
pub fn fold(snap: WorkerSnapshot, events: &[events::Envelope], now: DateTime<Utc>) -> WorkerSnapshot;
/// The WAL helper every command uses: append (stamps seq) then fold that one event then save.
pub fn append_and_fold(log: &mut store::EventLog, worker_dir: &Path, snap: WorkerSnapshot,
    kind: events::EventKind, fsm_payload: serde_json::Value, now: DateTime<Utc>)
    -> Result<WorkerSnapshot, AppError>;
pub struct LivenessInput { pub process_alive: bool }
pub fn recover(worker_dir: &Path, live: LivenessInput, now: DateTime<Utc>)
    -> Result<WorkerSnapshot, AppError>;
```

- [ ] **Step 1: Write the failing tests**

```rust
// tests/fold_model.rs
use a2a::{events::*, fold::*, fsm::*, state::*};
use chrono::Utc;

fn lifecycle(kind: EventKind, fsm_payload: serde_json::Value, epoch: u32, seq: u64) -> Envelope {
    let mut e = Envelope::new(epoch, Transport::Exec, EventClass::Lifecycle, kind, fsm_payload, Utc::now());
    e.seq = seq;
    e
}
fn base(dir: &std::path::Path) -> WorkerSnapshot { WorkerSnapshot::new_for_test("impl-0814t1300-aaaa", dir) }

#[test]
fn fold_prefix_equals_incremental_apply() {
    let tmp = tempfile::tempdir().unwrap();
    let seq = vec![
        lifecycle(EventKind::StateChange, serde_json::json!({"fsm":"ProcessStarted","pid":1,"start_witness":"w"}), 1, 1),
        lifecycle(EventKind::StateChange, serde_json::json!({"fsm":"TurnEnded","has_result":false,"has_question":true}), 1, 2),
        lifecycle(EventKind::StateChange, serde_json::json!({"fsm":"Send"}), 1, 3),
        lifecycle(EventKind::StateChange, serde_json::json!({"fsm":"TurnEnded","has_result":true,"has_question":false}), 1, 4),
    ];
    for k in 0..=seq.len() {
        let all_at_once = fold(base(tmp.path()), &seq[..k], Utc::now());
        let mut inc = base(tmp.path());
        for e in &seq[..k] { inc = fold(inc, std::slice::from_ref(e), Utc::now()); }
        assert_eq!(all_at_once.state, inc.state, "prefix {k}");
        assert_eq!(all_at_once.last_applied_seq, inc.last_applied_seq, "prefix {k}");
    }
}

#[test]
fn stale_epoch_events_are_fenced() {
    let tmp = tempfile::tempdir().unwrap();
    let mut snap = base(tmp.path());
    snap.epoch = 2; snap.state = WorkerState::Running;
    let stale = lifecycle(EventKind::StateChange,
        serde_json::json!({"fsm":"TurnEnded","has_result":true,"has_question":false}), 1,
        snap.last_applied_seq + 1);
    let after = fold(snap.clone(), &[stale], Utc::now());
    assert_eq!(after.state, WorkerState::Running, "stale-epoch completion must not apply");
    assert_eq!(after.last_applied_seq, snap.last_applied_seq + 1, "seq watermark still advances");
}

#[test]
fn reapplying_applied_seq_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let e = lifecycle(EventKind::StateChange,
        serde_json::json!({"fsm":"ProcessStarted","pid":1,"start_witness":"w"}), 1, 1);
    let s1 = fold(base(tmp.path()), &[e.clone()], Utc::now());
    let s2 = fold(s1.clone(), &[e], Utc::now());
    assert_eq!((s1.state, s1.last_applied_seq), (s2.state, s2.last_applied_seq));
}

#[test]
fn snapshot_roundtrip_and_too_new() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("w"); std::fs::create_dir_all(&dir).unwrap();
    let snap = base(&dir);
    save_snapshot(&dir, &snap).unwrap();
    let loaded = load_snapshot(&dir).unwrap();
    assert_eq!(loaded.schema_version, 1);
    assert_eq!(loaded.id, snap.id);
    // newer schema is read-only
    let mut v: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.join("state.json")).unwrap()).unwrap();
    v["schema_version"] = 2.into();
    std::fs::write(dir.join("state.json"), v.to_string()).unwrap();
    assert!(matches!(load_snapshot(&dir), Err(SnapshotError::TooNew(2))));
}

#[test]
fn recovery_table_cases() {
    let now = Utc::now();
    let tmp = tempfile::tempdir().unwrap();
    // (a) empty dir → Failed{NeverStarted}
    let d1 = tmp.path().join("impl-0814t1300-bbbb"); std::fs::create_dir_all(&d1).unwrap();
    let r = recover(&d1, LivenessInput { process_alive: false }, now).unwrap();
    assert_eq!((r.state, r.failure_class), (WorkerState::Failed, Some(FailureClass::NeverStarted)));
    // (b) running snapshot + dead process → Orphaned
    let d2 = tmp.path().join("impl-0814t1300-cccc"); std::fs::create_dir_all(&d2).unwrap();
    let mut s = WorkerSnapshot::new_for_test("impl-0814t1300-cccc", &d2);
    s.state = WorkerState::Running;
    save_snapshot(&d2, &s).unwrap();
    let r = recover(&d2, LivenessInput { process_alive: false }, now).unwrap();
    assert_eq!(r.state, WorkerState::Orphaned);
    // (c) corrupt snapshot + events → rebuilt from the log
    let d3 = tmp.path().join("impl-0814t1300-dddd"); std::fs::create_dir_all(&d3).unwrap();
    std::fs::write(d3.join("state.json"), b"{not json").unwrap();
    let mut log = a2a::store::EventLog::open(&d3).unwrap();
    let mut g = Envelope::new(1, Transport::Exec, EventClass::Lifecycle, EventKind::Spawned,
        serde_json::json!({"fsm":"Spawn","id":"impl-0814t1300-dddd","lane":"impl",
            "cwd":d3.to_str().unwrap(),"brief_sha":"x","sandbox":"workspace-write",
            "network":false,"model":"gpt-5.5","effort":"high","transport":"exec"}), Utc::now());
    log.append(&mut g).unwrap();
    let mut p = Envelope::new(1, Transport::Exec, EventClass::Lifecycle, EventKind::StateChange,
        serde_json::json!({"fsm":"ProcessStarted","pid":1,"start_witness":"w"}), Utc::now());
    log.append(&mut p).unwrap();
    let r = recover(&d3, LivenessInput { process_alive: true }, now).unwrap();
    assert_eq!(r.state, WorkerState::Running, "rebuilt from the log, snapshot discarded");
    assert_eq!(r.lane, a2a::config::Lane::Impl, "identity seeded from the genesis event");
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement state.rs + fold.rs** per the Interfaces (the genesis special-case and rejection-recording are spelled out there; `recover` = `load_snapshot` (Corrupt → start from None) → fold events → if final state Running and `!process_alive` → apply `OrphanDetected`; truly empty dir → synthesize `Failed{NeverStarted}`).
- [ ] **Step 4: Run tests** — `cargo test --test fold_model` — 5 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: versioned snapshot + WAL fold (genesis seeding, epoch fencing, recovery)"
```

---

### Task 10: Brief front matter — parse, validate, hash

**Acceptance Criteria (unit-verified; wired CLI-observably at Task 12's spawn gate):**
- A valid brief parses lane, acceptance_criteria, verify, files_in_scope, non_goals, max_turns; a front-matter-free brief parses with all-empty fields.
- The validation table is explicit and pinned per lane: **impl** requires non-empty `acceptance_criteria` AND non-empty `verify`; **review** requires non-empty `acceptance_criteria` only; **research** and **general** require nothing. (`files_in_scope`/`non_goals` are recommended-not-required in Phase 1 — a recorded divergence from spec §4.9's full table, revisited in Phase 4.)
- `sha256_hex("abc")` equals the pinned NIST vector.

**Verify via:** cli

**Files:**
- Create: `src/brief.rs`
- Modify: `src/lib.rs`
- Test: unit tests in `src/brief.rs`

**Interfaces:**
- Produces:
```rust
pub struct Brief { pub lane: Option<Lane>, pub acceptance_criteria: Vec<String>,
    pub verify: Vec<String>, pub files_in_scope: Vec<String>, pub non_goals: Vec<String>,
    pub result_schema: Option<String>, pub max_turns: Option<u32>, pub body: String }
pub fn parse(text: &str) -> Result<Brief, String>;
pub fn validate(brief: &Brief, lane: Lane) -> Result<(), Vec<&'static str>>;  // the table above, exactly
pub fn sha256_hex(text: &str) -> String;
```

- [ ] **Step 1: Write failing unit tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Lane;

    const GOOD: &str = "---\nlane: impl\nacceptance_criteria:\n  - binary exits 0\nverify:\n  - cargo test\nfiles_in_scope:\n  - src/main.rs\nnon_goals:\n  - no refactors\nmax_turns: 30\n---\n# Task\nDo the thing.\n";

    #[test]
    fn parses_front_matter_and_body() {
        let b = parse(GOOD).unwrap();
        assert_eq!(b.lane, Some(Lane::Impl));
        assert_eq!(b.acceptance_criteria, vec!["binary exits 0"]);
        assert_eq!(b.verify, vec!["cargo test"]);
        assert_eq!(b.max_turns, Some(30));
        assert!(b.body.starts_with("# Task"));
    }

    #[test]
    fn validation_table_per_lane() {
        let empty = parse("just a body").unwrap();
        let missing = validate(&empty, Lane::Impl).unwrap_err();
        assert!(missing.contains(&"acceptance_criteria") && missing.contains(&"verify"));
        let ac_only = parse("---\nacceptance_criteria:\n  - x\n---\nb").unwrap();
        assert!(validate(&ac_only, Lane::Impl).is_err());     // impl also needs verify
        assert!(validate(&ac_only, Lane::Review).is_ok());    // review needs AC only
        assert!(validate(&empty, Lane::Research).is_ok());
        assert!(validate(&empty, Lane::General).is_ok());
    }

    #[test]
    fn sha_is_stable() {
        assert_eq!(sha256_hex("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement** (front matter = leading `---\n…\n---\n` split; `serde_yaml` into an all-optional intermediate; `sha2::Sha256`).
- [ ] **Step 4: Run tests** — 3 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: brief front-matter parsing with explicit per-lane validation table"
```

---

### Task 11: Exec process machinery — detached spawn, prompt-file stdin, per-epoch streams, flock liveness, witnessed kill, stub codex

**Acceptance Criteria (verified against the stub; CLI-observable via spawn/kill):**
- A spawned worker survives its parent CLI exiting; `exec::liveness(dir)` reports `Alive` **within 5s** of `spawn_detached` returning (readiness poll inside spawn_detached — it errors `NEVER_STARTED` if the shim never takes the lock) and `Dead` within 5s of the stub exiting; the exit code lands in `exit_code`.
- The codex child's stdout goes to **`exec.<epoch>.jsonl`** and stderr to `stderr.log`, both opened **append-only 0600** (never truncated — a resume's second shim cycle must not destroy the prior epoch's stream or the ingest watermark).
- **The prompt travels via a 0600 prompt file wired to the child's stdin with the `-` positional** — `build_argv` output contains `-` and NEVER any prompt text (pinned by test).
- `build_argv` pins every security flag, always emits `-m <model>` + `["-c","model_reasoning_effort=\"<effort>\""]`, and for `SandboxMode::OutboxOnly` sets the PRIMARY workspace to the worker's own `scratch/` dir (`-C <worker>/scratch`) with `--add-dir <worker>/outbox` — the target repo path appears in NO writable position (codex `--add-dir` adds writable roots *alongside* the always-writable primary workspace, so outbox-only must move the workspace, not clear extra roots).
- `spawn_detached` scrubs `OPENAI_API_KEY`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `DEPOT_TOKEN` from the child environment (pinned: parent exports `OPENAI_API_KEY`, the stub dumps its env to a file, assertion: absent) and applies `SpawnSpec.env` pairs (the test seam — no process-global `set_var` anywhere in tests).
- `exec::kill_worker` refuses on a start-time witness mismatch (PID reuse guard) and group-kills on match.

**Verify via:** cli
**Apply:** docs/development/appendix-actor-model.md §3 — supervisor holds identity; workers disposable (witnessed identity before any signal).

**Files:**
- Create: `src/exec.rs`, `tests/fixtures/stub-codex`
- Modify: `src/lib.rs`, `src/cli.rs`, `src/main.rs` (hidden `__exec-shim` subcommand)
- Test: `tests/exec_lifecycle.rs`

**Interfaces:**
- Produces:
```rust
pub struct SpawnSpec<'a> {
    pub codex_bin: &'a Path, pub cwd: &'a Path, pub worker_dir: &'a Path,
    pub epoch: u32,
    pub sandbox: SandboxMode, pub network: bool,
    pub model: &'a str, pub effort: &'a str,
    pub schema_path: Option<&'a Path>,
    pub prompt_file: &'a Path,                  // 0600; wired to child stdin; argv gets "-"
    pub resume_session: Option<&'a str>,        // Some(id) → `exec resume <id> -`
    pub env: &'a [(String, String)],            // extra env (test seam); applied AFTER the scrub
}
pub struct SpawnedProc { pub pid: u32, pub start_witness: String }
/// Fork the detached `a2a __exec-shim <worker-dir> <epoch> <prompt-file> -- <codex argv…>` and
/// poll liveness (20ms interval, 5s cap) until the shim holds run.lock; NEVER_STARTED on expiry.
pub fn spawn_detached(spec: &SpawnSpec) -> Result<SpawnedProc, AppError>;
pub enum Liveness { Alive, Dead, NotYetStarted }  // NotYetStarted = run.lock absent
pub fn liveness(worker_dir: &Path) -> Liveness;
pub fn start_witness(pid: u32) -> Option<String>; // `ps -o lstart= -p <pid>`
pub fn kill_worker(pid: u32, witness: &str) -> Result<KillOutcome, AppError>;
pub enum KillOutcome { Killed, AlreadyDead, WitnessMismatch }
pub fn read_exit_code(worker_dir: &Path) -> Option<i32>;
pub fn exec_stream_path(worker_dir: &Path, epoch: u32) -> PathBuf; // exec.<epoch>.jsonl
pub fn open_append_0600(p: &Path) -> std::io::Result<std::fs::File>; // create+append, never truncate
pub fn build_argv(spec: &SpawnSpec) -> Vec<String>;  // pure; pinned by tests
```
`build_argv` argv elements, in order: `exec` (+ `resume <session>` when resuming), `--json`, `-C <cwd-or-scratch>`, `--output-last-message <worker>/result.md`, `-s workspace-write`, `--add-dir <worker>/outbox`, `["-c","approval_policy=\"never\""]`, `["-c","features.hooks=false"]`, `["-c","sandbox_workspace_write.network_access=<true|false>"]`, `-m <model>`, `["-c","model_reasoning_effort=\"<effort>\""]`, `--skip-git-repo-check`, (`--output-schema <path>` when set), `-`. For OutboxOnly the `-C` value is `<worker>/scratch` (spawn creates it) and the intended repo path is conveyed to the worker inside the brief as read-only context. NEVER `--ephemeral`.
Shim contract (`__exec-shim <worker-dir> <epoch> <prompt-file> -- <bin> <args…>`): setsid'd group leader; opens+flocks `run.lock` (0600, exclusive, nonblock — a second shim for the same worker aborts); opens `exec.<epoch>.jsonl` and `stderr.log` via `open_append_0600`; opens the prompt file as the child's stdin; spawns the codex argv directly (`Command::arg`, no shell); waits; writes the exit code to `exit_code` via `write_atomic`; exits (kernel releases the lock).

- [ ] **Step 1: Create the stub codex fixture**

```bash
mkdir -p tests/fixtures && cat > tests/fixtures/stub-codex <<'EOF'
#!/bin/sh
# stub-codex: scripted `codex exec --json` stand-in.
# Env knobs: STUB_SLEEP, STUB_EXIT, STUB_LAST_MSG, STUB_NO_RESULT, STUB_LEAK,
#            STUB_EMIT_QUESTION, STUB_RECORD (dir for resume recordings), STUB_RESUME_FAIL
out=""; outbox=""; resume=0
[ "$1" = "exec" ] && shift
if [ "$1" = "resume" ]; then resume=1; shift; shift; fi   # consume "resume <session-id>"
while [ $# -gt 0 ]; do
  case "$1" in
    --output-last-message) shift; out="$1";;
    --add-dir)             shift; outbox="$1";;
  esac; shift
done
if [ "$resume" = 1 ] && [ -n "$STUB_RECORD" ]; then
  printf '%s\n' "$0 $*" > "$STUB_RECORD/resume-argv.txt"
  cat > "$STUB_RECORD/resume-stdin.txt"
fi
[ -n "$STUB_RESUME_FAIL" ] && [ "$resume" = 1 ] && exit "$STUB_RESUME_FAIL"
printf '%s\n' '{"type":"session.created","session_id":"stub-sess-1"}'
printf '%s\n' '{"type":"turn.started","id":"t1"}'
printf '%s\n' "{\"type\":\"agent_message\",\"message\":\"working ${STUB_LEAK:-}\"}"
printf '%s\n' '{"type":"agent_message_delta","delta":"x"}'
if [ -n "$STUB_DUMP_ENV" ]; then env > "$STUB_DUMP_ENV"; fi
[ -n "$STUB_SLEEP" ] && sleep "$STUB_SLEEP"
if [ -n "$STUB_EMIT_QUESTION" ] && [ -n "$outbox" ]; then
  printf '%s\n' '{"seq":1,"question":"A or B?","blocking":true}' > "$outbox/1.question.json"
fi
printf '%s\n' '{"type":"turn.completed","id":"t1"}'
if [ -n "$out" ] && [ -z "$STUB_NO_RESULT" ] && [ -z "$STUB_EMIT_QUESTION" ]; then
  printf '%s\n' "${STUB_LAST_MSG:-done}" > "$out"
fi
exit "${STUB_EXIT:-0}"
EOF
chmod +x tests/fixtures/stub-codex
```

- [ ] **Step 2: Write the failing lifecycle tests**

```rust
// tests/exec_lifecycle.rs
use a2a::exec::{self, SpawnSpec, Liveness, KillOutcome};
use a2a::config::SandboxMode;
use std::time::Duration;

fn stub() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/stub-codex")
}
fn mk_worker(tmp: &std::path::Path) -> std::path::PathBuf {
    let w = tmp.join("w");
    for d in ["outbox", "scratch"] { std::fs::create_dir_all(w.join(d)).unwrap(); }
    std::fs::write(w.join("prompt.md"), "do it\n").unwrap();
    w
}
fn spec<'a>(cwd: &'a std::path::Path, w: &'a std::path::Path, bin: &'a std::path::Path,
            env: &'a [(String, String)]) -> SpawnSpec<'a> {
    SpawnSpec { codex_bin: bin, cwd, worker_dir: w, epoch: 1,
        sandbox: SandboxMode::WorkspaceWrite, network: false,
        model: "gpt-5.5", effort: "high", schema_path: None,
        prompt_file: Box::leak(Box::new(w.join("prompt.md"))), resume_session: None, env }
}
fn wait_liveness(w: &std::path::Path, want_dead: bool, secs: u64) {
    for _ in 0..(secs * 10) {
        let dead = matches!(exec::liveness(w), Liveness::Dead);
        if dead == want_dead { return; }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("liveness did not converge to dead={want_dead} within {secs}s");
}

#[test]
fn argv_pins_security_flags_and_no_prompt_text() {
    let tmp = tempfile::tempdir().unwrap();
    let w = mk_worker(tmp.path());
    let b = stub();
    let s = spec(tmp.path(), &w, &b, &[]);
    let argv = exec::build_argv(&s);
    let joined = argv.join(" ");
    for must in ["--json", "approval_policy=\"never\"", "features.hooks=false",
                 "sandbox_workspace_write.network_access=false",
                 "-m gpt-5.5", "model_reasoning_effort=\"high\"",
                 "--skip-git-repo-check", "--output-last-message", "--add-dir"] {
        assert!(joined.contains(must), "missing {must} in {joined}");
    }
    assert_eq!(argv.last().unwrap(), "-", "prompt must be stdin, not argv");
    assert!(!joined.contains("do it"), "prompt text leaked into argv");
    assert!(!joined.contains("--ephemeral"));
    // outbox-only: primary workspace moves to scratch; repo cwd appears nowhere writable
    let mut s2 = spec(tmp.path(), &w, &b, &[]);
    s2.sandbox = SandboxMode::OutboxOnly;
    let argv2 = exec::build_argv(&s2);
    let cpos = argv2.iter().position(|a| a == "-C").unwrap();
    assert!(argv2[cpos + 1].ends_with("/scratch"));
    assert!(!argv2.iter().any(|a| a == tmp.path().to_str().unwrap()),
            "repo path must not appear in outbox-only argv");
}

#[test]
fn detached_worker_lifecycle_env_scrub_and_streams() {
    let tmp = tempfile::tempdir().unwrap();
    let w = mk_worker(tmp.path());
    let b = stub();
    let envdump = tmp.path().join("env.txt");
    let env = vec![("STUB_SLEEP".to_string(), "2".to_string()),
                   ("STUB_EXIT".to_string(), "7".to_string()),
                   ("STUB_DUMP_ENV".to_string(), envdump.to_str().unwrap().to_string()),
                   ("OPENAI_API_KEY".to_string(), "sk-should-be-scrubbed-000000".to_string())];
    // NOTE: OPENAI_API_KEY passed via SpawnSpec.env must ALSO be scrubbed (scrub wins)
    let proc_ = exec::spawn_detached(&spec(tmp.path(), &w, &b, &env)).unwrap();
    assert!(matches!(exec::liveness(&w), Liveness::Alive), "spawn_detached returns only after the shim holds run.lock");
    assert!(exec::start_witness(proc_.pid).is_some());
    wait_liveness(&w, true, 10);
    assert_eq!(exec::read_exit_code(&w), Some(7));
    let raw = std::fs::read_to_string(exec::exec_stream_path(&w, 1)).unwrap();
    assert!(raw.contains("session.created") && raw.contains("turn.completed"));
    let dumped = std::fs::read_to_string(&envdump).unwrap();
    assert!(!dumped.contains("OPENAI_API_KEY"), "billing-critical scrub failed");
}

#[test]
fn kill_refuses_on_witness_mismatch_and_kills_group_on_match() {
    let tmp = tempfile::tempdir().unwrap();
    let w = mk_worker(tmp.path());
    let b = stub();
    let env = vec![("STUB_SLEEP".to_string(), "30".to_string())];
    let proc_ = exec::spawn_detached(&spec(tmp.path(), &w, &b, &env)).unwrap();
    assert!(matches!(exec::kill_worker(proc_.pid, "wrong witness").unwrap(),
                     KillOutcome::WitnessMismatch));
    let wit = exec::start_witness(proc_.pid).unwrap();
    assert!(matches!(exec::kill_worker(proc_.pid, &wit).unwrap(), KillOutcome::Killed));
    wait_liveness(&w, true, 10);
}
```

- [ ] **Step 3: Run to verify failure** — FAIL.
- [ ] **Step 4: Implement exec.rs + `__exec-shim`** per the Interfaces contract. Scrub list applied via `cmd.env_remove` in BOTH `spawn_detached` (shim) and the shim (codex child), AFTER applying `spec.env` — the scrub always wins. `spawn_detached` polls `liveness` (20ms × 250) before returning `SpawnedProc`; expiry → `AppError::new("NEVER_STARTED", ExitCode::Precondition, …)`.
- [ ] **Step 5: Run tests** — `cargo test --test exec_lifecycle` — 3 passed.
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: detached exec transport (prompt-file stdin, per-epoch streams, env scrub, witnessed kill)"
```

---

### Task 12: `a2a spawn` + shared test helpers

**Acceptance Criteria (user-visible, CLI-observable):**
- `a2a spawn --lane general --cwd <dir> --brief <file>` (stub codex via `A2A_CODEX_BIN`) prints a worker id and exits 0; the worker dir contains `brief.md`, `prompt.md`, `outbox/`, `inbox/`, `scratch/`, `events.jsonl` (genesis + ProcessStarted lifecycle events), and `state.json` with `state: "running"`, `transport: "exec"`, `brief_sha` (64 hex), and the resolved `sandbox`/`network`/`model`/`effort` fields.
- Path validation canonicalizes BOTH sides (a `--cwd` under macOS `/var/...` passes against an allowed root given as the un-canonicalized tempdir path); comparison is by path components, so `/x/a` does not match root `/x/ab`.
- `--cwd` outside `allowed_roots` → exit 9 `CWD_NOT_ALLOWED`. `--lane research` with an explicit `--cwd` → exit 9 `RESEARCH_CWD_FIXED` (research always runs in `<worker>/scratch`, spec §4.9); without `--cwd` it spawns fine.
- A brief containing `sk-abc123456789012345678901` → exit 9 `SECRET_IN_BRIEF` naming the line; the worker dir is removed (no litter); the secret value appears in NO output. `--context <file>` staging: a file named `.env.production` → exit 9 `SECRET_FILE_REFUSED`; a clean file lands in `<worker>/context/` mode 0600; staged text files are scanned like the brief.
- `--lane impl` without acceptance criteria → exit 9 `BRIEF_INVALID`. `--transport app-server` → exit 9 `TRANSPORT_UNAVAILABLE` (Phase 1). A lane configured `danger-full-access` → exit 9 `ELEVATION_NOT_PERMITTED` (the §5 elevation gate lands with worktrees in Phase 4).

**Verify via:** cli

**Files:**
- Create: `src/commands/spawn.rs`, `tests/common/mod.rs`
- Modify: `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`
- Test: `tests/cmd_spawn.rs`

**Interfaces:**
- Consumes: everything prior.
- Produces: `SpawnArgs { lane: Lane, cwd: Option<PathBuf>, brief: PathBuf, context: Vec<PathBuf>, schema: Option<PathBuf>, model: Option<String>, effort: Option<String>, transport: Option<String>, worktree: bool /* warns: Phase 4 */, no_queue: bool }`.
- Spawn flow (in order): resolve home+config → transport gate → lane elevation gate → resolve cwd (research: reject explicit, use scratch; else: canonicalize + component-prefix check against canonicalized roots) → parse+validate brief → `redact::scan_text` brief (fail = no dir yet) → allocate id (retry `ids::generate` up to 16× against `list_worker_ids()` + `index.jsonl` tombstone ids; then `ID_ALLOCATION_FAILED`) → create dir tree (0700: outbox, inbox, inbox/delivered, scratch, context) → stage `--context` files (refuse_filename → `SECRET_FILE_REFUSED`; scan_text each; 10 MiB total cap → `CONTEXT_TOO_LARGE`) → write `brief.md` + `prompt.md` (brief body) → genesis event via `append_and_fold` (raw carries id/lane/cwd/brief_sha/sandbox/network/model/effort/transport) → `exec::spawn_detached` → `ProcessStarted` via `append_and_fold` (arms task deadline). On spawn failure after dir creation: append `ProcessExited` → snapshot shows `failed{never_started}` — readable, no litter removal needed at that point.
- **`tests/common/mod.rs`** (every later test file starts `mod common; use common::*;`):

```rust
// tests/common/mod.rs
#![allow(dead_code)]
use assert_cmd::Command;
use std::path::{Path, PathBuf};

pub fn stub_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/stub-codex")
}
/// Command with hermetic env: temp home, stub codex, tmp-wide allowlist.
pub fn a2a(home: &Path, allowed_root: &Path) -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env("A2A_HOME", home)
     .env("A2A_CODEX_BIN", stub_path())
     .env("A2A_ALLOWED_ROOTS", allowed_root);
    c
}
pub fn write_brief(dir: &Path, body: &str) -> PathBuf {
    let p = dir.join("the-brief.md");
    std::fs::write(&p, body).unwrap();
    p
}
/// Spawns a general-lane worker with the given STUB_* env; returns (home, tmp_keepalive, worker_id).
pub fn spawn_with(stub_env: &[(&str, &str)]) -> (PathBuf, tempfile::TempDir, String) {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = write_brief(tmp.path(), "do the thing\n");
    let mut c = a2a(&home, tmp.path());
    for (k, v) in stub_env { c.env(k, v); }
    let out = c.args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(&work)
        .arg("--brief").arg(&brief).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    (home, tmp, v["data"]["id"].as_str().unwrap().to_string())
}
pub fn read_snapshot(home: &Path, id: &str) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(
        home.join("workers").join(id).join("state.json")).unwrap()).unwrap()
}
pub fn wait_dead(home: &Path, id: &str, secs: u64) {
    let lock = home.join("workers").join(id).join("run.lock");
    for _ in 0..(secs * 10) {
        if !lock.exists() { return; }
        if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&lock) {
            use std::os::fd::AsRawFd;
            if nix::fcntl::flock(f.as_raw_fd(), nix::fcntl::FlockArg::LockExclusiveNonblock).is_ok() {
                return; // lock free → shim gone
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("worker {id} still alive after {secs}s");
}
pub fn rewrite_deadline_to_past(home: &Path, id: &str) {
    let dir = home.join("workers").join(id);
    let mut v = read_snapshot(home, id);
    v["deadline_at"] = serde_json::json!(chrono::Utc::now() - chrono::Duration::hours(1));
    a2a_lib_write_atomic(&dir.join("state.json"), v.to_string().as_bytes());
}
fn a2a_lib_write_atomic(p: &Path, b: &[u8]) { a2a::store::write_atomic(p, b).unwrap() }
```
(Note: `tests/common/mod.rs` is a module, not a test binary — cargo compiles it into each test crate that declares `mod common;`. The nix dev-dependency is needed: add `cargo add --dev nix@0.29 --features fs` in this task.)

- [ ] **Step 1: Write the failing tests**

```rust
// tests/cmd_spawn.rs
mod common;
use common::*;

#[test]
fn spawn_general_creates_worker_and_runs() {
    let (home, _tmp, id) = spawn_with(&[]);
    assert!(regex::Regex::new(r"^general-[0-9]{4}t[0-9]{4}-[a-z2-7]{4}$").unwrap().is_match(&id));
    let wdir = home.join("workers").join(&id);
    for f in ["brief.md", "prompt.md", "state.json", "events.jsonl"] { assert!(wdir.join(f).exists(), "{f}"); }
    for d in ["outbox", "inbox", "inbox/delivered", "scratch", "context"] { assert!(wdir.join(d).is_dir(), "{d}"); }
    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["state"], "running");
    assert_eq!(snap["transport"], "exec");
    assert_eq!(snap["sandbox"], "workspace-write");
    assert_eq!(snap["model"], "gpt-5.5");
    assert_eq!(snap["brief_sha"].as_str().unwrap().len(), 64);
}

#[test]
fn cwd_allowlist_canonicalizes_both_sides() {
    // spawn_with passes the UN-canonicalized tempdir as the allowed root while --cwd
    // canonicalizes to /private/var/... on macOS — success proves both sides are canonicalized.
    let (_home, _tmp, id) = spawn_with(&[]);
    assert!(!id.is_empty());
}

#[test]
fn cwd_outside_allowed_roots_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let brief = write_brief(tmp.path(), "x");
    let out = a2a(&tmp.path().join("home"), &tmp.path().join("only-here"))
        .args(["--json", "spawn", "--lane", "general", "--cwd", "/tmp", "--brief"]).arg(&brief)
        .assert().code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "CWD_NOT_ALLOWED");
}

#[test]
fn research_cwd_is_fixed_to_scratch() {
    let tmp = tempfile::tempdir().unwrap();
    let brief = write_brief(tmp.path(), "question\n");
    let home = tmp.path().join("home");
    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "research", "--cwd"]).arg(tmp.path())
        .arg("--brief").arg(&brief).assert().code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "RESEARCH_CWD_FIXED");
    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "research", "--brief"]).arg(&brief)
        .assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let snap = read_snapshot(&home, v["data"]["id"].as_str().unwrap());
    assert!(snap["cwd"].as_str().unwrap().ends_with("/scratch"));
}

#[test]
fn secret_in_brief_fails_with_no_litter_and_no_echo() {
    let tmp = tempfile::tempdir().unwrap();
    let brief = write_brief(tmp.path(), "key: sk-abc123456789012345678901\n");
    let home = tmp.path().join("home");
    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(tmp.path())
        .arg("--brief").arg(&brief).assert().code(9);
    let all = format!("{}{}", String::from_utf8_lossy(&out.get_output().stdout),
                              String::from_utf8_lossy(&out.get_output().stderr));
    assert!(all.contains("SECRET_IN_BRIEF") && all.contains("line 1"));
    assert!(!all.contains("sk-abc"), "secret value echoed");
    assert_eq!(std::fs::read_dir(home.join("workers")).map(|d| d.count()).unwrap_or(0), 0);
}

#[test]
fn context_staging_refuses_secret_files() {
    let tmp = tempfile::tempdir().unwrap();
    let brief = write_brief(tmp.path(), "x\n");
    let envfile = tmp.path().join(".env.production");
    std::fs::write(&envfile, "TOKEN=hunter2\n").unwrap();
    let out = a2a(&tmp.path().join("home"), tmp.path())
        .args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(tmp.path())
        .arg("--brief").arg(&brief).arg("--context").arg(&envfile)
        .assert().code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "SECRET_FILE_REFUSED");
}

#[test]
fn brief_transport_and_elevation_gates() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let no_ac = write_brief(tmp.path(), "no front matter\n");
    a2a(&home, tmp.path()).args(["spawn", "--lane", "impl", "--cwd"]).arg(tmp.path())
        .arg("--brief").arg(&no_ac).assert().code(9);
    a2a(&home, tmp.path()).args(["spawn", "--lane", "general", "--transport", "app-server", "--cwd"])
        .arg(tmp.path()).arg("--brief").arg(&no_ac).assert().code(9);
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("config.toml"), "[lanes.general]\nsandbox = \"danger-full-access\"\n").unwrap();
    let out = a2a(&home, tmp.path()).args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(tmp.path()).arg("--brief").arg(&no_ac).assert().code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "ELEVATION_NOT_PERMITTED");
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement `commands/spawn.rs`** following the flow in the Interfaces block.
- [ ] **Step 4: Run tests** — `cargo test --test cmd_spawn` — 7 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: a2a spawn (canonical allowlist, context staging, lane gates) + shared test helpers"
```

---

### Task 13: `reconcile()` + `a2a status`

**Acceptance Criteria (user-visible, CLI-observable):**
- After a stub worker finishes naturally, `a2a status <id>` (no broker exists in Phase 1 — reconcile-on-read owns transitions, spec §4.6) reports `done` and exits 0, having ingested the epoch's `exec.<epoch>.jsonl`, captured `session_id: "stub-sess-1"` into `state.json`, observed the released run.lock + `exit_code` 0 + `result.md`, and committed transitions through the WAL.
- A stub exiting 1 without a result → `failed`, `failure_class: "no_result"`, exit 4.
- A worker whose `deadline_at` is past while its process still runs → Task timeout fires → `failed{timeout}` and the process group is killed (converges dead within 10s).
- `a2a status nope` → exit 3, `NO_SUCH_WORKER` (deferred here from Task 1's AC).
- Eight concurrent `status` invocations on one worker cannot corrupt state (`state.lock` serializes reconcile; lock acquisition = nonblocking flock retried every 50ms for 10s, then exit 8 `LOCK_TIMEOUT`).
- A freshly spawned worker whose shim hasn't exited is NOT declared dead: reconcile treats a free/absent run.lock as death only when `exit_code` exists OR 5s grace has passed since `entered_state_at`.

**Verify via:** cli
**Apply:** docs/development/appendix-streams.md — checkpoint reads via watermarks; docs/development/appendix-fsm.md — C4 observability.

**Files:**
- Create: `src/reconcile.rs`, `src/commands/status.rs`
- Modify: `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`
- Test: `tests/cmd_status.rs`

**Interfaces:**
- Produces:
```rust
/// Under an exclusive flock on <worker>/state.lock (LockExclusiveNonblock retried 50ms×200,
/// then AppError{code:"LOCK_TIMEOUT", exit: CmdTimeout}):
/// 1. load snapshot (TooNew → SCHEMA_TOO_NEW; Corrupt → recover path)
/// 2. ingest exec.<snap.epoch>.jsonl from byte offset extra["exec_offset_<epoch>"]:
///    per line: redact_egress(raw) → map_exec_line → append envelope (skip None);
///    SessionIdentified lines also emit the FsmEvent with the extracted session_id;
///    clamp offset to file len (EventsDropped envelope when clamping fires)
/// 3. detect + WAL-apply, in order:
///    turn completion → TurnEnded{has_result: result.md exists, has_question: unanswered
///      outbox/*.question.json exists}   (question = file whose seq > last answer seq)
///    process death: liveness Dead AND (exit_code exists OR grace elapsed) → ProcessExited
///      — except when a turn already concluded this epoch
///    deadline expiry → Timeout{kind by state: Spawned→Start, Running→Task, AwaitingInput→Blocked}
///      (Task timeout also group-kills via the recorded pid+witness)
/// 4. save snapshot; return it
pub fn reconcile(home: &A2aHome, cfg: &Config, id: &str, now: DateTime<Utc>)
    -> Result<WorkerSnapshot, AppError>;
```
`status` exit codes: done→0, failed→4, awaiting-input/escalated→5, spawned/queued/running→0, orphaned→10, cancelled→0.

- [ ] **Step 1: Write the failing tests**

```rust
// tests/cmd_status.rs
mod common;
use common::*;

#[test]
fn finished_worker_reconciles_to_done_with_session_id() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let out = a2a(&home, tmp.path()).args(["--json", "status", &id]).assert().code(0);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["state"], "done");
    assert_eq!(read_snapshot(&home, &id)["session_id"], "stub-sess-1");
}

#[test]
fn failed_stub_is_failed_no_result_exit_4() {
    let (home, tmp, id) = spawn_with(&[("STUB_EXIT", "1"), ("STUB_NO_RESULT", "1")]);
    wait_dead(&home, &id, 10);
    let out = a2a(&home, tmp.path()).args(["--json", "status", &id]).assert().code(4);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["failure_class"], "no_result");
}

#[test]
fn expired_deadline_fires_task_timeout_and_kills() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "60")]);
    rewrite_deadline_to_past(&home, &id);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(4);
    assert_eq!(read_snapshot(&home, &id)["failure_class"], "timeout");
    wait_dead(&home, &id, 10);
}

#[test]
fn unknown_worker_exits_3() {
    let tmp = tempfile::tempdir().unwrap();
    let out = a2a(&tmp.path().join("home"), tmp.path())
        .args(["--json", "status", "nope"]).assert().code(3);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "NO_SUCH_WORKER");
}

#[test]
fn concurrent_status_calls_do_not_corrupt() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let root = tmp.path().to_path_buf();
    let handles: Vec<_> = (0..8).map(|_| {
        let (h, r, i) = (home.clone(), root.clone(), id.clone());
        std::thread::spawn(move || { a2a(&h, &r).args(["status", &i]).assert(); })
    }).collect();
    for h in handles { h.join().unwrap(); }
    assert_eq!(read_snapshot(&home, &id)["state"], "done");
}

#[test]
fn just_spawned_worker_is_not_declared_dead_by_grace_rule() {
    // STUB_SLEEP keeps the shim alive; immediately reconciling must keep state=running
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "3")]);
    let out = a2a(&home, tmp.path()).args(["--json", "status", &id]).assert().code(0);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["state"], "running");
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement reconcile.rs + status.rs** per the Interfaces contract.
- [ ] **Step 4: Run tests** — `cargo test --test cmd_status` — 6 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: reconcile-on-read (session capture, grace-guarded death, deadlines) + a2a status"
```

---

### Task 14: `a2a ls` + `a2a tail`

**Acceptance Criteria (user-visible, CLI-observable):**
- `a2a ls` prints one line per worker: `<id>  <lane>  <state>  <dwell>  <last-event-age>` (dwell = now − entered_state_at, humanized like `4m`); `a2a ls --json` returns `data.workers` as an array of objects with `id`, `lane`, `state`, `dwell_secs`, `last_event_age_secs`, `sandbox`, sorted by `created_at`.
- `a2a tail <id>` prints lifecycle events as `<seq> <ts> <kind>` one per line; `--since <seq>` filters to `seq > N`; `--raw` prints full envelope JSONL.
- Egress redaction on BOTH paths AND at rest: with a planted `STUB_LEAK` token, the token appears in neither `tail` output nor `tail --raw` output **nor the on-disk `events.jsonl`** (redaction happened at ingest).
- `ls` triggers reconcile per worker, so a fleet of finished workers shows terminal states without any prior `status` calls.

**Verify via:** cli

**Files:**
- Create: `src/commands/ls.rs`, `src/commands/tail.rs`
- Modify: `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`
- Test: `tests/cmd_ls_tail.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// tests/cmd_ls_tail.rs
mod common;
use common::*;

#[test]
fn ls_lists_reconciled_workers_with_columns() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let out = a2a(&home, tmp.path()).args(["--json", "ls"]).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let w = &v["data"]["workers"][0];
    assert_eq!(w["id"], id.as_str());
    assert_eq!(w["lane"], "general");
    assert_eq!(w["state"], "done", "ls must reconcile, not read stale snapshots");
    assert!(w["dwell_secs"].is_number() && w["last_event_age_secs"].is_number());
    let human = a2a(&home, tmp.path()).arg("ls").assert().success();
    let text = String::from_utf8_lossy(&human.get_output().stdout).to_string();
    assert!(text.contains(&id) && text.contains("done"));
}

#[test]
fn tail_formats_filters_and_redacts_everywhere() {
    let (home, tmp, id) = spawn_with(&[("STUB_LEAK", "xoxb-1234567890-abcdef")]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert(); // ingest
    let out = a2a(&home, tmp.path()).args(["tail", &id]).assert().success();
    let text = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    let first = text.lines().next().unwrap();
    let parts: Vec<_> = first.splitn(3, ' ').collect();
    assert_eq!(parts.len(), 3, "line format: <seq> <ts> <kind>, got {first}");
    assert!(parts[0].parse::<u64>().is_ok());
    // --since filters
    let out2 = a2a(&home, tmp.path()).args(["tail", &id, "--since", "1"]).assert().success();
    let text2 = String::from_utf8_lossy(&out2.get_output().stdout).to_string();
    assert!(text2.lines().count() < text.lines().count());
    assert!(text2.lines().all(|l| l.split(' ').next().unwrap().parse::<u64>().unwrap() > 1));
    // redaction: CLI outputs and the on-disk log
    let raw = a2a(&home, tmp.path()).args(["tail", &id, "--raw"]).assert().success();
    let rawtext = String::from_utf8_lossy(&raw.get_output().stdout).to_string();
    let disk = std::fs::read_to_string(home.join("workers").join(&id).join("events.jsonl")).unwrap();
    for s in [&text, &rawtext, &disk] {
        assert!(!s.contains("xoxb-1234567890"), "leak");
    }
    assert!(disk.contains("[REDACTED:"), "at-rest redaction must have happened at ingest");
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement ls.rs + tail.rs** (ls: reconcile each id, tolerate per-worker errors by showing `state: "unknown"` with the error in stderr; tail: `store::read_events` + kind filter unless `--raw`).
- [ ] **Step 4: Run tests** — 2 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: a2a ls (reconciled, dwell columns) and a2a tail (filtered, redacted, cursored)"
```

---

### Task 15: `a2a result` + `a2a wait`

**Acceptance Criteria (user-visible, CLI-observable):**
- `a2a result <id>` on a done worker prints the result (redacted) and exits 0; on a running worker exits 6; on a failed worker exits 4 with `failure_class` in the envelope.
- **Result derivation rule (spec §4.3):** with no `--schema`, `result.md` is the result. With `--schema <file>` at spawn: on turn completion reconcile parses `result.md` as JSON and checks the schema's top-level `required` keys (full JSON-Schema validation is a Phase 2+ upgrade — the limitation is a doctor note); on success the parsed JSON is written to `result.json` (rotating any prior to `result.<n>.json`) and `a2a result` prints `result.json`; on failure `result.md` is renamed `result.raw.md`, the worker goes `failed{schema_violation}`, and the session stays resumable.
- `a2a wait <id> --timeout 15s` blocks until the worker leaves {spawned, queued, running} and exits with the state-encoded code (0 done / 4 failed / 5 blocked / 10 orphaned); **timeout expiry exits 6** (spec §4.1 — 8 is reserved for a2a-internal timeouts).
- `--until` widens/narrows the stop set: `any` (default) = leaves running; `blocked` = stops ONLY on awaiting-input/escalated (runs through done→ exit per state reached); `terminal` = done/failed/cancelled; `done` = only done.
- `a2a wait <id1> <id2>` (multi-id) returns when the FIRST changes state, printing that id.

**Verify via:** cli

**Files:**
- Create: `src/commands/result.rs`, `src/commands/wait.rs`
- Modify: `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`, `src/reconcile.rs` (schema-gate + rotation on turn completion)
- Test: `tests/cmd_result_wait.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// tests/cmd_result_wait.rs
mod common;
use common::*;

#[test]
fn result_prints_when_done_exit_6_while_running() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "3"), ("STUB_LAST_MSG", "the answer")]);
    a2a(&home, tmp.path()).args(["result", &id]).assert().code(6);
    wait_dead(&home, &id, 10);
    let out = a2a(&home, tmp.path()).args(["result", &id]).assert().code(0);
    assert!(String::from_utf8_lossy(&out.get_output().stdout).contains("the answer"));
}

#[test]
fn schema_gate_rotates_or_fails() {
    // schema requires {"status": ...}; stub writes non-JSON → schema_violation
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj"); std::fs::create_dir_all(&work).unwrap();
    let schema = tmp.path().join("schema.json");
    std::fs::write(&schema, r#"{"type":"object","required":["status"]}"#).unwrap();
    let brief = write_brief(tmp.path(), "x\n");
    let out = a2a(&home, tmp.path())
        .env("STUB_LAST_MSG", "not json at all")
        .args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(&work)
        .arg("--brief").arg(&brief).arg("--schema").arg(&schema)
        .assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(4);
    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["failure_class"], "schema_violation");
    assert!(home.join("workers").join(&id).join("result.raw.md").exists());
    // valid JSON case → result.json written
    let out = a2a(&home, tmp.path())
        .env("STUB_LAST_MSG", r#"{"status":"pass"}"#)
        .args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(&work)
        .arg("--brief").arg(&brief).arg("--schema").arg(&schema)
        .assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id2 = v["data"]["id"].as_str().unwrap().to_string();
    wait_dead(&home, &id2, 10);
    a2a(&home, tmp.path()).args(["status", &id2]).assert().code(0);
    assert!(home.join("workers").join(&id2).join("result.json").exists());
}

#[test]
fn wait_blocks_until_done_and_times_out_with_6() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "2")]);
    let t0 = std::time::Instant::now();
    a2a(&home, tmp.path()).args(["wait", &id, "--timeout", "15s"]).assert().code(0);
    assert!(t0.elapsed() > std::time::Duration::from_secs(1));
    let (home2, tmp2, id2) = spawn_with(&[("STUB_SLEEP", "30")]);
    a2a(&home2, tmp2.path()).args(["wait", &id2, "--timeout", "1s"]).assert().code(6);
}

#[test]
fn wait_any_returns_first_changed_id() {
    let (home, tmp, fast) = spawn_with(&[("STUB_SLEEP", "1")]);
    // second worker in the SAME home so one wait sees both
    let brief = write_brief(tmp.path(), "slow\n");
    let work = tmp.path().join("proj");
    let out = a2a(&home, tmp.path()).env("STUB_SLEEP", "30")
        .args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(&work)
        .arg("--brief").arg(&brief).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let slow = v["data"]["id"].as_str().unwrap().to_string();
    let out = a2a(&home, tmp.path()).args(["wait", &fast, &slow, "--timeout", "20s"])
        .assert().code(0);
    let printed = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(printed.contains(&fast) && !printed.contains(&slow));
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement** (wait = 500ms reconcile poll via `std::thread::sleep`; duration parser for `Ns/Nm/Nh` unit-tested in wait.rs; the schema gate lives in reconcile's TurnEnded handling).
- [ ] **Step 4: Run tests** — 4 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: a2a result (schema gate + rotation) and a2a wait (state-encoded exits, multi-id)"
```

---

### Task 16: `a2a send` — inbox queue, turn-boundary delivery, BLOCKED round-trip

**Acceptance Criteria (user-visible, CLI-observable):**
- A stub worker that writes `outbox/1.question.json` and ends its turn reconciles to `awaiting-input` (`state_reason: "blocked"`, exit 5 from status) — the FILE drives the transition; no `BLOCKED:` text marker is needed (spec §4.7).
- `a2a send <id> "use option B"` on the blocked worker: queues `inbox/<seq>-<epoch_ms>-<rand8>.md` (front matter `{from, ts, kind: answer}`), launches a detached `codex exec resume stub-sess-1` delivery, moves the consumed message to `inbox/delivered/`, advances `last_delivered_seq`, and the worker converges back through running to done (`a2a wait` exits 0 within 15s).
- **ps-safety:** the stub's `resume-argv.txt` recording does NOT contain the message text; its `resume-stdin.txt` DOES (prompt-file stdin, spec §5).
- `a2a send` to a RUNNING worker exits 0 and only queues (delivery happens at the next turn boundary — FSM row `Running+Send→Running`, no delivery action).
- `a2a send` to a `done` worker → exit 9 `ILLEGAL_TRANSITION` with remedy naming `a2a resume`.
- Two rapid sends queue two lexicographically ordered files; one delivery carries both (concatenated in seq order), and both move to `delivered/`.

**Verify via:** cli
**Apply:** docs/development/appendix-streams.md — at-least-once + watermark dedup; per-sender ordering only.

**Files:**
- Create: `src/inbox.rs`, `src/commands/send.rs`
- Modify: `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`, `src/reconcile.rs` (question-file guard + pending-delivery trigger on turn end)
- Test: `tests/cmd_send.rs`

**Interfaces:**
- Produces:
```rust
// inbox.rs
pub enum MsgKind { Steer, Answer }
/// Writes inbox/<seq:06>-<epoch_ms>-<rand8 hex>.md with YAML front matter {from, ts, kind}.
pub fn queue_message(worker_dir: &Path, from: &str, kind: MsgKind, body: &str) -> std::io::Result<u64>;
pub fn undelivered(worker_dir: &Path, watermark: u64) -> std::io::Result<Vec<(u64, PathBuf, String)>>;
/// Concatenate all undelivered (seq order) into <worker>/prompt.md (overwrite via write_atomic),
/// spawn_detached with resume_session = snap.session_id (SESSION_UNKNOWN precondition error,
/// exit 9, when None), epoch unchanged (same-epoch continuation of delivery is Phase 1 policy:
/// resume-for-delivery reuses the CURRENT epoch's stream file — the shim appends), then move
/// each consumed file into inbox/delivered/ and return the new watermark.
pub fn deliver_pending(home: &A2aHome, cfg: &Config, snap: &WorkerSnapshot)
    -> Result<u64, AppError>;
```
`send` flow: resolve → reconcile → `step(Send)`; on `Rejected` map to `ILLEGAL_TRANSITION` (exit 9, remedy from the rejection); on Ok queue the message; if the transition carried `Action::DeliverInbox` (awaiting-input/escalated), call `deliver_pending` and WAL-append the Send event; while RUNNING, queue only. `--queue-only` skips reconcile and `step()` entirely — it is a pure enqueue (no state transition, no delivery); the NEXT plain `send` (or turn boundary) delivers the whole backlog. Reconcile addition: when a turn ends and `undelivered(last_delivered_seq)` is non-empty, deliver before computing TurnEnded (a queued steer keeps the worker moving without operator action).

- [ ] **Step 1: Write the failing tests**

```rust
// tests/cmd_send.rs
mod common;
use common::*;

fn spawn_blocked(record: &std::path::Path) -> (std::path::PathBuf, tempfile::TempDir, String) {
    let (home, tmp, id) = spawn_with(&[
        ("STUB_EMIT_QUESTION", "1"),
        ("STUB_RECORD", record.to_str().unwrap()),
    ]);
    wait_dead(&home, &id, 10);
    (home, tmp, id)
}

#[test]
fn question_file_drives_blocked_state() {
    let rec = tempfile::tempdir().unwrap();
    let (home, tmp, id) = spawn_blocked(rec.path());
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(5);
    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["state"], "awaiting-input");
    assert_eq!(snap["state_reason"], "blocked");
}

#[test]
fn send_answers_delivers_via_stdin_and_worker_converges() {
    let rec = tempfile::tempdir().unwrap();
    let (home, tmp, id) = spawn_blocked(rec.path());
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(5);
    // Delivery-env policy (also stated in inbox.rs Interfaces): the resume child inherits the
    // `a2a send` process's own STUB_*/A2A_* env (after the standard scrub) — spawn-time env is
    // not persisted. The stub only needs STUB_RECORD on the resume call, so set it here.
    a2a(&home, tmp.path())
        .env("STUB_RECORD", rec.path())
        .args(["send", &id, "use option B"]).assert().success();
    a2a(&home, tmp.path()).args(["wait", &id, "--timeout", "15s"]).assert().code(0);
    let argv = std::fs::read_to_string(rec.path().join("resume-argv.txt")).unwrap();
    let stdin = std::fs::read_to_string(rec.path().join("resume-stdin.txt")).unwrap();
    assert!(argv.contains("resume"), "delivery must be codex exec resume");
    assert!(!argv.contains("use option B"), "message leaked into argv");
    assert!(stdin.contains("use option B"), "message must arrive via stdin");
    // consumed message moved to delivered/
    let inbox = home.join("workers").join(&id).join("inbox");
    assert_eq!(std::fs::read_dir(&inbox).unwrap()
        .filter(|e| e.as_ref().unwrap().file_type().unwrap().is_file()).count(), 0);
    assert_eq!(std::fs::read_dir(inbox.join("delivered")).unwrap().count(), 1);
}

#[test]
fn send_while_running_queues_only() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "5")]);
    a2a(&home, tmp.path()).args(["send", &id, "fyi: keep going"]).assert().success();
    let inbox = home.join("workers").join(&id).join("inbox");
    assert_eq!(std::fs::read_dir(&inbox).unwrap()
        .filter(|e| e.as_ref().unwrap().file_type().unwrap().is_file()).count(), 1,
        "message queued, not delivered mid-turn");
}

#[test]
fn send_to_done_worker_is_rejected_with_resume_remedy() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(0);
    let out = a2a(&home, tmp.path()).args(["--json", "send", &id, "hello?"]).assert().code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "ILLEGAL_TRANSITION");
    assert!(v["error"]["remedy"].as_str().unwrap().contains("resume"));
}

#[test]
fn two_sends_one_delivery_in_order() {
    let rec = tempfile::tempdir().unwrap();
    let (home, tmp, id) = spawn_blocked(rec.path());
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(5);
    // queue two while blocked: first send triggers delivery; to test batching, queue the
    // second BEFORE the first delivery can be observed by using --queue-only
    a2a(&home, tmp.path()).args(["send", &id, "part one", "--queue-only"]).assert().success();
    a2a(&home, tmp.path()).env("STUB_RECORD", rec.path())
        .args(["send", &id, "part two"]).assert().success();
    let stdin = std::fs::read_to_string(rec.path().join("resume-stdin.txt")).unwrap();
    let (p1, p2) = (stdin.find("part one").unwrap(), stdin.find("part two").unwrap());
    assert!(p1 < p2, "delivery must concatenate in seq order");
}
```

(`--queue-only` is a real flag on `send` — queue without triggering delivery; the orchestrator uses it to batch. Add it to the CLI surface.)

- [ ] **Step 2: Extend the stub** — already done in Task 11 (question emission, resume recording, `--add-dir` parsing).
- [ ] **Step 3: Run to verify failure** — FAIL.
- [ ] **Step 4: Implement inbox.rs + send.rs + reconcile guard.**
- [ ] **Step 5: Run tests** — 5 passed.
- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: a2a send (file-authoritative BLOCKED, stdin delivery, delivered/ ack, batching)"
```

---

### Task 17: `a2a kill` and `a2a resume`

**Acceptance Criteria (user-visible, CLI-observable):**
- `a2a kill <id>` on a running stub: witnessed group-kill, state → `cancelled` (never `failed`), exit 0; a second `kill` on the now-terminal worker short-circuits BEFORE `step()` and exits 0 with `data.already_terminal == true` (an operator re-running kill is not an error).
- `a2a resume <id> "extra instruction"` on a done worker: `epoch` increments in `state.json`, a `Resumed` lifecycle event lands, a detached `codex exec resume stub-sess-1` starts with the instruction via stdin, and the worker converges to done again (`wait` exits 0). The new epoch writes `exec.2.jsonl` — the epoch-1 stream is untouched, and reconcile ingests ONLY the current epoch's file (pre-resume `turn.completed` lines are never re-applied: they live in the old file and old epoch).
- `a2a resume` on a RUNNING worker → exit 9 `ILLEGAL_TRANSITION`.
- `a2a resume` on a worker with no recorded session id → exit 9 `SESSION_UNKNOWN`. A resume whose codex process dies before any turn event (stub `STUB_RESUME_FAIL=64`) reconciles to `failed{session_missing}` — reconcile emits `SessionMissing` when a resumed epoch's process exits with no TurnStarted ingested (spec §7).

**Verify via:** cli

**Files:**
- Create: `src/commands/kill.rs`, `src/commands/resume.rs`
- Modify: `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`, `src/reconcile.rs` (SessionMissing detection for resumed epochs)
- Test: `tests/cmd_kill_resume.rs`

**Interfaces:**
- Consumes: `exec::kill_worker`, `inbox::deliver_pending`, FSM `Kill`/`Resume` rows, reconcile.
- Produces: `kill` = resolve → reconcile → if `snap.state.is_terminal()` return `Ok(json!({"already_terminal": true, "state": snap.state}))` → else `step(Kill)` → witnessed group-kill → WAL `Killed` event → snapshot. `resume` = resolve → reconcile → `session_id` presence gate (`SESSION_UNKNOWN`, exit 9) → `step(Resume)` (epoch_bump) → WAL `Resumed` (the fold applies the bump) → write the message (if any) to `prompt.md` → `spawn_detached` with `resume_session` + the NEW epoch → `ProcessStarted` event. Reconcile addition: for an epoch entered via Resume, `ProcessExited` with zero TurnStarted events ingested this epoch emits `SessionMissing` instead.

- [ ] **Step 1: Write the failing tests**

```rust
// tests/cmd_kill_resume.rs
mod common;
use common::*;

#[test]
fn kill_running_is_cancelled_second_kill_short_circuits() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "30")]);
    a2a(&home, tmp.path()).args(["kill", &id]).assert().code(0);
    wait_dead(&home, &id, 10);
    assert_eq!(read_snapshot(&home, &id)["state"], "cancelled");
    let out = a2a(&home, tmp.path()).args(["--json", "kill", &id]).assert().code(0);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["already_terminal"], true);
}

#[test]
fn resume_done_worker_bumps_epoch_streams_separately_and_converges() {
    let rec = tempfile::tempdir().unwrap();
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(0);
    let epoch1_stream = home.join("workers").join(&id).join("exec.1.jsonl");
    let before = std::fs::read_to_string(&epoch1_stream).unwrap();
    a2a(&home, tmp.path()).env("STUB_RECORD", rec.path())
        .args(["resume", &id, "one more thing"]).assert().success();
    a2a(&home, tmp.path()).args(["wait", &id, "--timeout", "15s"]).assert().code(0);
    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["epoch"], 2);
    assert!(home.join("workers").join(&id).join("exec.2.jsonl").exists());
    assert_eq!(std::fs::read_to_string(&epoch1_stream).unwrap(), before,
               "epoch-1 stream untouched by resume");
    let stdin = std::fs::read_to_string(rec.path().join("resume-stdin.txt")).unwrap();
    assert!(stdin.contains("one more thing"));
}

#[test]
fn resume_running_rejected_and_missing_session_gated() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "10")]);
    a2a(&home, tmp.path()).args(["--json", "resume", &id, "x"]).assert().code(9);
    // strip the session id → SESSION_UNKNOWN
    let (home2, tmp2, id2) = spawn_with(&[]);
    wait_dead(&home2, &id2, 10);
    a2a(&home2, tmp2.path()).args(["status", &id2]).assert().code(0);
    let dir2 = home2.join("workers").join(&id2);
    let mut v = read_snapshot(&home2, &id2);
    v["session_id"] = serde_json::Value::Null;
    a2a::store::write_atomic(&dir2.join("state.json"), v.to_string().as_bytes()).unwrap();
    let out = a2a(&home2, tmp2.path()).args(["--json", "resume", &id2, "x"]).assert().code(9);
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["error"]["code"], "SESSION_UNKNOWN");
}

#[test]
fn resume_that_dies_pre_turn_is_session_missing() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(0);
    a2a(&home, tmp.path()).env("STUB_RESUME_FAIL", "64")
        .args(["resume", &id, "x"]).assert().success();
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(4);
    assert_eq!(read_snapshot(&home, &id)["failure_class"], "session_missing");
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement kill.rs + resume.rs + the reconcile SessionMissing rule.**
- [ ] **Step 4: Run tests** — 4 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: a2a kill (idempotent, witnessed) and a2a resume (epoch streams, session gates)"
```

---

### Task 18: `a2a gc` with tombstones and safety rails

**Acceptance Criteria (user-visible, CLI-observable):**
- `a2a gc --dry-run` lists reclaimable workers (terminal states only) with byte counts and deletes nothing (before/after dir listing identical).
- `a2a gc --age 0s` removes done/failed/cancelled workers, appending one tombstone per removal to `$A2A_HOME/index.jsonl` (`{id, session_id, rollout_path, cwd, state, removed_at}`); default `--age` is `7d` anchored to the last event's `ts`.
- Non-terminal workers (running/awaiting-input/escalated/orphaned) are NEVER removed — there is no force-path for them in Phase 1; the remedy names `a2a kill` first (spec §7: orphans are exactly what resume needs).
- Traversal guard: real fixture dirs `workers/..evil/` and `workers/not-an-id/` (invalid names) plus a symlink `workers/evil-link -> <victim dir>` are all skipped with a stderr warning; the victim dir survives.
- With `CODEX_HOME=<tmp>/codex-home` containing `sessions/sentinel`, gc never touches it (sentinel survives) — a2a does not prune codex sessions (spec §7, stated choice).

**Verify via:** cli

**Files:**
- Create: `src/commands/gc.rs`
- Modify: `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`
- Test: `tests/cmd_gc.rs`

**Interfaces:**
- Consumes: reconcile (each candidate is reconciled first, so dead-but-unobserved workers become terminal before the decision), `ids::is_valid` (names failing it are skipped+warned; symlinked entries are skipped+warned via `symlink_metadata`), tombstone append O_APPEND to `index.jsonl`. `--age <dur>` uses the Task 15 duration parser.

- [ ] **Step 1: Write the failing tests**

```rust
// tests/cmd_gc.rs
mod common;
use common::*;

#[test]
fn dry_run_lists_but_deletes_nothing_and_age_zero_reaps_with_tombstone() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    a2a(&home, tmp.path()).args(["status", &id]).assert().code(0);
    let out = a2a(&home, tmp.path()).args(["--json", "gc", "--dry-run"]).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["reclaimable"][0]["id"], id.as_str());
    assert!(home.join("workers").join(&id).exists(), "dry-run must not delete");
    a2a(&home, tmp.path()).args(["gc", "--age", "0s"]).assert().success();
    assert!(!home.join("workers").join(&id).exists());
    let index = std::fs::read_to_string(home.join("index.jsonl")).unwrap();
    let t: serde_json::Value = serde_json::from_str(index.lines().next().unwrap()).unwrap();
    assert_eq!(t["id"], id.as_str());
    assert_eq!(t["session_id"], "stub-sess-1");
}

#[test]
fn non_terminal_workers_are_never_removed() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "20")]);
    a2a(&home, tmp.path()).args(["gc", "--age", "0s"]).assert().success();
    assert!(home.join("workers").join(&id).exists(), "running worker must survive gc");
}

#[test]
fn invalid_names_and_symlinks_are_skipped_victims_survive() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let workers = home.join("workers");
    std::fs::create_dir_all(workers.join("..evil")).unwrap();
    std::fs::create_dir_all(workers.join("not-an-id")).unwrap();
    let victim = tmp.path().join("victim"); std::fs::create_dir_all(&victim).unwrap();
    std::fs::write(victim.join("precious.txt"), "keep me").unwrap();
    std::os::unix::fs::symlink(&victim, workers.join("evil-link")).unwrap();
    let out = a2a(&home, tmp.path()).args(["gc", "--age", "0s"]).assert().success();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(stderr.contains("..evil") && stderr.contains("not-an-id") && stderr.contains("evil-link"));
    assert!(workers.join("..evil").exists() && workers.join("not-an-id").exists());
    assert!(victim.join("precious.txt").exists(), "symlink target must survive");
}

#[test]
fn codex_home_is_never_touched() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let codex_home = tmp.path().join("codex-home");
    std::fs::create_dir_all(codex_home.join("sessions")).unwrap();
    std::fs::write(codex_home.join("sessions/sentinel"), "x").unwrap();
    a2a(&home, tmp.path()).env("CODEX_HOME", &codex_home)
        .args(["gc", "--age", "0s"]).assert().success();
    assert!(codex_home.join("sessions/sentinel").exists());
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement gc.rs.**
- [ ] **Step 4: Run tests** — 4 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: a2a gc (terminal-only, tombstoned, traversal/symlink-guarded)"
```

---

### Task 19: `a2a doctor` (phase-1 checks)

**Acceptance Criteria (user-visible, CLI-observable):**
- `a2a doctor --json` emits `data.checks` as an array of `{id, status: "ok"|"warn"|"fail", detail, remedy}` and exits 0 when nothing fails, 9 on any fail (spec §4.1) — six checks, each with a test forcing its non-ok path:
  - `codex_present`: resolves `A2A_CODEX_BIN`/config/PATH → fail when missing.
  - `codex_version`: parses `codex --version` → warn outside the pinned range (config `codex_version_range`, default `>=0.140, <0.150`); unparseable output is also a warn, never a fail (the stub prints JSON, and the all-ok test still expects exit 0).
  - `layout_version`: fail on mismatch.
  - `home_perms`: fail when the home root is not 0700.
  - `workers_health`: warn listing counts of orphaned/stale workers (never fail).
  - `env_inheritance`: warn (static text): "Phase 1 exec workers inherit the CLI environment minus the scrub list (OPENAI_API_KEY, AWS_*, DEPOT_TOKEN); the Phase 2 broker adds the full allowlist."
- Human mode prints one glyph line per check (✓/⚠/✗) to stderr and a one-line summary to stdout.

**Verify via:** cli

**Files:**
- Create: `src/commands/doctor.rs`
- Modify: `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs`
- Test: `tests/cmd_doctor.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// tests/cmd_doctor.rs
mod common;
use common::*;
use std::os::unix::fs::PermissionsExt;

fn checks(home: &std::path::Path, root: &std::path::Path,
          f: impl FnOnce(&mut assert_cmd::Command)) -> (serde_json::Value, i32) {
    let mut c = a2a(home, root);
    c.args(["--json", "doctor"]);
    f(&mut c);
    let out = c.assert();
    let code = out.get_output().status.code().unwrap();
    (serde_json::from_slice(&out.get_output().stdout).unwrap(), code)
}
fn check<'a>(v: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    v["data"]["checks"].as_array().unwrap().iter().find(|c| c["id"] == id).unwrap()
}

#[test]
fn all_ok_with_stub_exits_zero_and_lists_six_checks() {
    let tmp = tempfile::tempdir().unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(), |_| {});
    assert_eq!(code, 0);
    assert_eq!(v["data"]["checks"].as_array().unwrap().len(), 6);
    assert_eq!(check(&v, "codex_present")["status"], "ok");
    assert_eq!(check(&v, "env_inheritance")["status"], "warn");
}

#[test]
fn missing_codex_fails_exit_9() {
    let tmp = tempfile::tempdir().unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(),
        |c| { c.env("A2A_CODEX_BIN", "/nonexistent/codex"); });
    assert_eq!(code, 9);
    assert_eq!(check(&v, "codex_present")["status"], "fail");
}

#[test]
fn out_of_range_version_warns_but_exits_zero() {
    // stub-codex has no --version handling; add a fake: a script printing "codex-cli 0.99.0"
    let tmp = tempfile::tempdir().unwrap();
    let fake = tmp.path().join("old-codex");
    std::fs::write(&fake, "#!/bin/sh\necho 'codex-cli 0.99.0'\n").unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (v, code) = checks(&tmp.path().join("h"), tmp.path(),
        |c| { c.env("A2A_CODEX_BIN", &fake); });
    assert_eq!(code, 0);
    assert_eq!(check(&v, "codex_version")["status"], "warn");
}

#[test]
fn bad_home_perms_fail() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("h");
    checks(&home, tmp.path(), |_| {}); // create the tree
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o755)).unwrap();
    let (v, code) = checks(&home, tmp.path(), |_| {});
    assert_eq!(code, 9);
    assert_eq!(check(&v, "home_perms")["status"], "fail");
}

#[test]
fn orphaned_worker_surfaces_in_workers_health() {
    let (home, tmp, id) = spawn_with(&[("STUB_SLEEP", "1")]);
    wait_dead(&home, &id, 10);
    // fake an orphan: rewrite state to running with a dead pid, older than grace
    let dir = home.join("workers").join(&id);
    let mut v = read_snapshot(&home, &id);
    v["state"] = "running".into();
    v["entered_state_at"] = serde_json::json!(chrono::Utc::now() - chrono::Duration::hours(1));
    a2a::store::write_atomic(&dir.join("state.json"), v.to_string().as_bytes()).unwrap();
    // events already show turn completion; doctor's reconcile pass will surface terminal or orphan —
    // either way workers_health reports a non-empty detail
    let (v, _) = checks(&home, tmp.path(), |_| {});
    assert!(check(&v, "workers_health")["detail"].as_str().unwrap().len() > 0);
}
```

- [ ] **Step 2: Run to verify failure** — FAIL.
- [ ] **Step 3: Implement doctor.rs** (checks as a `Vec<fn(&Ctx) -> Check>` so later phases append without touching the runner).
- [ ] **Step 4: Run tests** — 5 passed.
- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: machine-readable a2a doctor with six phase-1 checks"
```

---

### Task 20: End-to-end — full stub pipelines + gated real-codex smoke + README

**Acceptance Criteria (user-visible, CLI-observable):**
- Stub happy pipeline (always on): spawn general → `wait` 0 → `status` done → `result` prints the stub's message → `gc --dry-run` lists it — one test composing only public CLI surface.
- Stub BLOCKED pipeline (always on): spawn with question → `wait --until blocked` exits 5 → `send` the answer → `wait` exits 0 → `result` non-empty.
- `cargo test` is green from a clean checkout with NO real codex and NO network.
- Real-codex smoke (`#[ignore]`, run only when `A2A_E2E=1`): with `A2A_HOME=<tmp>/home`, `A2A_ALLOWED_ROOTS=<tmp>`, an un-set `A2A_CODEX_BIN` (real PATH codex), and a `git init`ed `<tmp>/repo`: spawn general with brief "Reply with exactly the text: A2A-OK" → `wait --timeout 300s` exits 0 → `result` contains `A2A-OK` → `state.json` has a non-null real `session_id`. This is the run that validates `map_exec_line` against the real vocabulary — fixing the mapping table is in-scope for this task, with the observed real type names recorded in a comment. **This test reads the real `~/.codex` (auth) and writes nothing under it** — the only sanctioned touch of real codex state in the suite.
- `README.md` has a Quick Start covering install, spawn, wait, result, send, doctor.

**Verify via:** cli

**Files:**
- Create: `tests/e2e_exec.rs`, `README.md`
- Modify: `src/events.rs` (mapping fixes from the live run, if any)
- Test: `tests/e2e_exec.rs`

- [ ] **Step 1: Write the two always-on pipeline tests** (compose `common::*` helpers; assertions per the criteria above — full command sequences, no new helpers).
- [ ] **Step 2: Write the gated smoke**

```rust
#[test]
#[ignore = "real codex + tokens; run with A2A_E2E=1 cargo test --test e2e_exec -- --ignored"]
fn real_codex_smoke() {
    if std::env::var("A2A_E2E").as_deref() != Ok("1") { return; }
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    assert!(std::process::Command::new("git").args(["init", "-q"]).current_dir(&repo)
        .status().unwrap().success());
    let brief = common::write_brief(tmp.path(), "Reply with exactly the text: A2A-OK\n");
    let mut c = assert_cmd::Command::cargo_bin("a2a").unwrap();
    // real codex from PATH; real ~/.codex auth is READ; nothing under it is written
    c.env("A2A_HOME", &home).env("A2A_ALLOWED_ROOTS", tmp.path()).env_remove("A2A_CODEX_BIN");
    let out = c.args(["--json", "spawn", "--lane", "general", "--cwd"]).arg(&repo)
        .arg("--brief").arg(&brief).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();
    let mut w = assert_cmd::Command::cargo_bin("a2a").unwrap();
    w.env("A2A_HOME", &home).env("A2A_ALLOWED_ROOTS", tmp.path());
    w.args(["wait", &id, "--timeout", "300s"]).assert().code(0);
    let mut r = assert_cmd::Command::cargo_bin("a2a").unwrap();
    r.env("A2A_HOME", &home).env("A2A_ALLOWED_ROOTS", tmp.path());
    let res = r.args(["result", &id]).assert().code(0);
    assert!(String::from_utf8_lossy(&res.get_output().stdout).contains("A2A-OK"));
    let snap = common::read_snapshot(&home, &id);
    assert!(snap["session_id"].is_string(), "real session id must be captured");
}
```

- [ ] **Step 3: Run `cargo test`** — all green, smoke skipped.
- [ ] **Step 4: Run the smoke for real:** `A2A_E2E=1 cargo test --test e2e_exec -- --ignored --nocapture`. Fix `events::map_exec_line` against the observed vocabulary; comment the real type names next to the table; re-run to green.
- [ ] **Step 5: Write README.md Quick Start.**
- [ ] **Step 6: Install and hand-verify:** `cargo install --path . --root ~/.local && a2a doctor` (real machine, default home created, doctor reports codex 0.140 in range).
- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: e2e pipelines (stub + gated real-codex smoke) and Quick Start docs"
```

---

## Plan self-review notes (kept for the executor)

- **Spec coverage (Phase 1 scope, task-mapped):** A2A_HOME + hermetic tests (§4/T1–2, enforced by `tests/common`), config layering + per-lane model/effort (§4.1, §4.7, §4.9/T3), ids (§4.1/T4), events envelope + delta drop + session row (§4.5/T5), atomic store + WAL durability + torn-tail repair (§4.5–4.6/T6), redaction ingress+egress+at-rest (§5/T7, T12, T13, T14), FSM + table artifact + source-enforced totality (§4.6/T8), WAL fold/epoch/recovery + snapshot sandbox fields (§4.5–4.6/T9), brief validation table (§4.9/T10), detached exec + prompt-file stdin + per-epoch streams + env scrub + outbox-only-as-moved-workspace (§4.4, §4.7, §5/T11), spawn + canonical allowlist + context staging + lane gates (§4.7, §4.9, §5/T12), reconcile + session capture + grace + deadlines (§4.6/T13), fleet observability columns (§4.8/T14), result schema gate + rotation + wait (§4.1, §4.3/T15), inbox/outbox + BLOCKED file authority + delivered/ ack + batching (§4.4, §4.7/T16), kill/resume + epoch streams + session gates (§4.6, §7/T17), gc + tombstones + guards (§7/T18), doctor (§4.1/T19), e2e + real-vocabulary validation (§8/T20).
- **Deliberately deferred (per spec §9):** broker/app-server + interrupt + admission/queued entry + env allowlist (Phase 2), sync tools + context canon (Phase 3), worktrees + `a2a verify` + owner scoping + the §5 elevation gate — Phase 1 REJECTS danger-full-access outright (Phase 4), budgets/circuit breaker/auto-review (Phase 5). The `queued`/`escalated` states exist in the FSM now (totality is cheaper to build than retrofit); Phase 1 enters `escalated` only via blocked-timeout in reconcile and never enters `queued`. `files_in_scope`/`non_goals` are recommended-not-required in brief validation (recorded divergence, revisit in Phase 4).
- **Known Phase-1 semantics to not "fix" during execution:** exec workers inherit the CLI env minus the scrub list (full allowlist is Phase 2); `interrupt` is absent from the CLI (needs app-server turn ids); impl lane runs without worktree isolation and `worktree_default` is false with a warning; schema validation checks only top-level `required` keys.
- **Devloop execution notes:** verify toolkit = `cli` for every task; acceptance criteria are converged-state assertions with explicit timeouts (`wait_dead`, liveness polls); every devloop task run must use a fresh `A2A_HOME` and never touch `~/.a2a`/`~/.codex`; real-codex steps appear ONLY in Task 20 and are `A2A_E2E`-gated.
