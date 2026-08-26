use crate::config::Lane;
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
    /// (hidden) detached exec-transport shim: holds run.lock, wires the child's
    /// streams to the worker dir, and lands the exit code in `exit_code`.
    #[command(hide = true, name = "__exec-shim")]
    ExecShim {
        worker_dir: PathBuf,
        epoch: u32,
        prompt_file: PathBuf,
        /// codex argv after `--` (never parsed by us, never shell-interpreted)
        #[arg(last = true, required = true, num_args = 1.., allow_hyphen_values = true)]
        argv: Vec<String>,
    },
    /// Dispatch a Codex worker; prints its id
    Spawn(SpawnArgs),
    /// Reconcile one worker and report its state (exit code IS the state)
    Status {
        /// Worker id, or any unambiguous prefix of one
        id: Option<String>,
        /// Report the broker daemon instead of a worker — the spec §4.1
        /// spelling of `a2a broker status` (exit 7 when there is none)
        #[arg(long, conflicts_with = "id")]
        broker: bool,
    },
    /// List every worker (reconciling each): id, lane, state, dwell, last-event age
    Ls {
        /// Include workers owned by somebody else, and print an OWNER column.
        /// Without it the listing is your own workers plus every unowned one
        /// (spec §4.8).
        #[arg(long)]
        all: bool,
    },
    /// Print a worker's events: `<seq> <ts> <kind>`, lifecycle only by default
    Tail {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// Only events after this seq (the cursor from a previous tail)
        #[arg(long, value_name = "SEQ")]
        since: Option<u64>,
        /// Print full event envelopes as JSONL instead of the projection
        #[arg(long)]
        raw: bool,
        /// Keep printing as the worker writes: live frames from the broker on
        /// the app-server transport, a 500ms poll of the log otherwise. Always
        /// bounded — it ends by itself when the worker reaches a terminal state
        /// (exit 0), when `--timeout` expires (exit 6), or when the broker that
        /// was feeding it shuts down (exit 0).
        #[arg(long)]
        follow: bool,
        /// Stop following after this long and exit 6 — `30s`, `5m`, `2h`
        /// (default: 1h, matching `a2a wait`)
        #[arg(long, value_name = "DUR", requires = "follow",
              value_parser = crate::commands::wait::parse_duration)]
        timeout: Option<std::time::Duration>,
    },
    /// Print a worker's final result (exit code encodes its state)
    Result {
        /// Worker id, or any unambiguous prefix of one
        id: String,
    },
    /// Block until a worker leaves the running states; exit code encodes the
    /// state reached (timeout expiry exits 6)
    Wait {
        /// One or more worker ids (or unambiguous prefixes); the first to
        /// reach the stop set wins and its id is printed
        #[arg(required = true, num_args = 1..)]
        ids: Vec<String>,
        /// Stop set: any (default) | blocked | terminal | done
        #[arg(long, value_enum, default_value_t = crate::commands::wait::Until::Any)]
        until: crate::commands::wait::Until,
        /// Give up after this long and exit 6 — `30s`, `5m`, `2h`
        #[arg(long, value_name = "DUR", value_parser = crate::commands::wait::parse_duration)]
        timeout: Option<std::time::Duration>,
    },
    /// Queue a message for a worker; delivered at the next turn boundary
    Send {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// The message text. Never appears in argv of the codex child — it
        /// travels to the worker through a 0600 prompt file on stdin.
        message: String,
        /// Queue without delivering or stepping the machine, so several
        /// messages can be batched into the next delivery
        #[arg(long)]
        queue_only: bool,
        /// Who the message is from (recorded in the message's front matter)
        #[arg(long, default_value = "orchestrator")]
        from: String,
        /// answer (responds to an outbox question) | steer (mid-flight guidance)
        #[arg(long, value_enum, default_value_t = MsgKindArg::Answer)]
        kind: MsgKindArg,
        /// Act on a worker owned by somebody else (spec §4.8)
        #[arg(long)]
        force: bool,
    },
    /// End the turn a worker is running right now, parking it in
    /// awaiting-input{interrupted}. App-server transport only (exec workers
    /// exit 9); a turn that already ended exits 0 with `data.noop`.
    Interrupt {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// Act on a worker owned by somebody else (spec §4.8)
        #[arg(long)]
        force: bool,
    },
    /// Stop a worker: witnessed group-kill, state → cancelled (never failed).
    /// Idempotent — a worker that is already terminal exits 0.
    Kill {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// Act on a worker owned by somebody else (spec §4.8)
        #[arg(long)]
        force: bool,
    },
    /// Continue a parked or terminal worker in a NEW epoch, resuming its codex
    /// session
    Resume {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// Extra instruction for the resumed turn. Never appears in argv of the
        /// codex child — it travels through a 0600 prompt file on stdin.
        message: Option<String>,
        /// Act on a worker owned by somebody else (spec §4.8)
        #[arg(long)]
        force: bool,
    },
    /// Reclaim finished workers' directories, leaving a tombstone per removal.
    /// Terminal states only — a worker that is still actionable is never
    /// removed, and there is no flag that overrides that.
    Gc {
        /// Only reclaim workers idle this long, measured from their last event
        /// — `30s`, `10m`, `2h`, `7d`
        #[arg(long, value_name = "DUR", default_value = "7d",
              value_parser = crate::commands::wait::parse_duration)]
        age: std::time::Duration,
        /// List what would be reclaimed and exit, deleting nothing
        #[arg(long)]
        dry_run: bool,
        /// Also reclaim workers owned by somebody else. Without it gc sweeps
        /// your own workers plus every unowned one (spec §4.8).
        #[arg(long)]
        all: bool,
        /// Also reclaim workers whose worktree has uncommitted or unmerged
        /// work — the worktree directory is deleted, the branch is not
        #[arg(long)]
        force: bool,
    },
    /// Check the environment a worker would be spawned into: one line per
    /// check, exit 9 if any of them fails
    Doctor,
    /// Inspect effective configuration
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    /// Control the broker daemon (the app-server transport)
    Broker {
        #[command(subcommand)]
        cmd: BrokerCmd,
    },
    /// (hidden) one request over the broker socket, autostarting one if
    /// needed. The ops seam Task 8 tests the wire through; `broker status`
    /// (Task 9) is the user-facing shape of the `info` op.
    #[command(hide = true, name = "__broker-call")]
    BrokerCall {
        /// info|list|status|spawn|resume|send|interrupt|kill|shutdown
        op: String,
        /// The op's parameters, as one JSON object
        params: Option<String>,
    },
    /// Mirror Claude-side capabilities into the Codex home (spec §6.1/§6.2)
    Sync {
        #[command(subcommand)]
        cmd: SyncCmd,
    },
    // later tasks append variants here
    /// Re-run the BRIEF's verify commands in the worker's workspace and write
    /// verification.json. Never runs commands the worker proposed.
    Verify {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// Run even when the diff touches execution-bearing paths (recorded
        /// either way)
        #[arg(long)]
        force_verify: bool,
        /// Kill any single verify command after this long — `30s`, `5m`, `2h`
        /// (default: 10m)
        #[arg(long, value_name = "DUR", value_parser = crate::commands::wait::parse_duration)]
        timeout: Option<std::time::Duration>,
    },
    /// Retry a worker: spawn a fresh one that inherits its lane, cwd, owner,
    /// model, effort, result schema and brief. Refused while the parent is
    /// still working, and after `admission.max_attempts` tries at one brief.
    Respawn {
        /// Worker id, or any unambiguous prefix of one
        id: String,
        /// Retry with this brief instead of the parent's (it goes through the
        /// same front-matter and secret gates any spawn's brief does)
        #[arg(long, value_name = "FILE")]
        brief: Option<std::path::PathBuf>,
        /// Respawn a worker owned by somebody else (spec §4.8). The child still
        /// belongs to the PARENT's owner — a forced retry is a repair, not a
        /// transfer.
        #[arg(long)]
        force: bool,
    },
    /// (hidden) render the generated skill artifacts from their in-repo sources
    #[command(hide = true, name = "__gen-skills")]
    GenSkills {
        #[arg(long, value_name = "DIR")]
        out: std::path::PathBuf,
        /// Compare instead of writing; exit 9 naming any drifted artifact
        #[arg(long)]
        check: bool,
    },
    /// Local agent mesh presence, feed and notes
    Mesh {
        #[command(subcommand)]
        cmd: MeshCmd,
    },
}

#[derive(Subcommand)]
pub enum BrokerCmd {
    /// Run the broker daemon: take `broker.lock`, detach, and serve. A second
    /// one says `broker already running` and exits 0.
    Run {
        /// Stay attached instead of double-forking into the background
        #[arg(long)]
        foreground: bool,
    },
    /// Report the running broker: uptime, transport, pids, counts (exit 7 when
    /// there is none)
    Status,
    /// Ask a running broker to stop; exits 0 when there is none
    Shutdown,
    /// Stop the running broker, then start a fresh one
    Restart,
    /// Close the circuit breaker again after the cause is fixed, so the broker
    /// admits new work (exit 7 when there is no broker — resetting a breaker a
    /// daemon never had is not a thing that can be autostarted into being)
    ResetBreaker,
}

#[derive(clap::Args)]
pub struct SpawnArgs {
    /// Lane: impl|review|research|general (sets sandbox, model, effort).
    /// Required unless `--template` is asking for a scaffold instead.
    #[arg(long, required_unless_present = "template")]
    pub lane: Option<Lane>,
    /// Working directory; must be under an allowed root. Forbidden for
    /// `--lane research`, which always runs in the worker's own scratch dir.
    #[arg(long)]
    pub cwd: Option<PathBuf>,
    /// Brief file (Markdown + optional YAML front matter).
    /// Required unless `--template` is asking for a scaffold instead.
    #[arg(long, required_unless_present = "template")]
    pub brief: Option<PathBuf>,
    /// Print this lane's brief scaffold to stdout and exit, creating nothing.
    /// The scaffolds are compiled into the binary: there is no install step and
    /// no runtime data dir, so `--template` answers the same on every machine.
    #[arg(long, value_name = "LANE", conflicts_with_all = ["lane", "brief"])]
    pub template: Option<Lane>,
    /// Extra file staged read-only into <worker>/context/ (repeatable)
    #[arg(long)]
    pub context: Vec<PathBuf>,
    /// JSON schema the worker's result must satisfy
    #[arg(long)]
    pub schema: Option<PathBuf>,
    /// Override the lane's model
    #[arg(long)]
    pub model: Option<String>,
    /// Override the lane's reasoning effort
    #[arg(long)]
    pub effort: Option<String>,
    /// auto|app-server|exec (default: auto — the broker when one is reachable,
    /// exec otherwise; `app-server` demands the broker and exits 7 without one)
    #[arg(long)]
    pub transport: Option<String>,
    /// Run in a git worktree at $A2A_HOME/worktrees/<id> on branch a2a/<lane>/<id>
    #[arg(long)]
    pub worktree: bool,
    /// Fail rather than queue at the admission cap (app-server transport only)
    #[arg(long)]
    pub no_queue: bool,
    /// Who this worker belongs to (default: `$A2A_OWNER`, else `anon-<uid>`).
    /// Recorded at genesis, so it survives losing `state.json` (spec §4.8).
    #[arg(long)]
    pub owner: Option<String>,
    /// Never create a worktree, even when the lane defaults to one
    #[arg(long, conflicts_with = "worktree")]
    pub no_worktree: bool,
    /// Branch the worktree from this ref instead of the target repo's HEAD
    #[arg(long, value_name = "REF")]
    pub base_ref: Option<String>,
    /// Run this ONE worker with `sandbox = danger-full-access` (spec §5).
    /// Needs `allow_elevation = true` on the lane and a `--reason`; forces the
    /// exec transport and appends a line to $A2A_HOME/audit.jsonl.
    #[arg(long)]
    pub danger_full_access: bool,
    /// Why this spawn needs full access — recorded verbatim in the audit log
    /// and on the worker. Required by `--danger-full-access`, ignored (with a
    /// warning) without it. NO clap `requires`: the pairing is validated in the
    /// §5 gate, so a missing reason is a typed envelope, not clap's exit 2.
    #[arg(long, value_name = "TEXT")]
    pub reason: Option<String>,
}

impl SpawnArgs {
    /// The lane this spawn is for.
    ///
    /// `Option` only because `--template` may stand alone; clap's
    /// `required_unless_present` guarantees one of the two, and the spawn path
    /// short-circuits on `--template` before it asks. A construction that
    /// reaches here with neither is a programming error inside this crate, not
    /// a user input, so it panics rather than growing a runtime refusal for a
    /// state the argument parser cannot produce.
    pub fn require_lane(&self) -> Lane {
        self.lane
            .expect("clap requires --lane unless --template short-circuited first")
    }
    /// The brief file this spawn reads. See [`SpawnArgs::require_lane`] for why
    /// the `Option` is not a runtime possibility here.
    pub fn require_brief(&self) -> &std::path::Path {
        self.brief
            .as_deref()
            .expect("clap requires --brief unless --template short-circuited first")
    }
}

/// clap-facing mirror of [`crate::inbox::MsgKind`]; the domain enum stays free
/// of CLI derives so the file layer does not depend on the argument parser.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "lowercase")]
pub enum MsgKindArg {
    Answer,
    Steer,
}

impl From<MsgKindArg> for crate::inbox::MsgKind {
    fn from(k: MsgKindArg) -> Self {
        match k {
            MsgKindArg::Answer => crate::inbox::MsgKind::Answer,
            MsgKindArg::Steer => crate::inbox::MsgKind::Steer,
        }
    }
}

#[derive(Subcommand)]
pub enum ConfigCmd {
    /// Print the effective configuration
    Show {
        /// Annotate each value with the layer it came from
        #[arg(long)]
        origin: bool,
    },
}

/// The two halves of `a2a sync`, both declared from the release that ships
/// either: a subcommand that appears in a later build is indistinguishable, to
/// a caller reading `--help`, from one they typed wrong.
#[derive(Subcommand)]
pub enum SyncCmd {
    /// Mirror ~/.claude/skills into $CODEX_HOME/skills (flat, manifest-tracked)
    Skills {
        /// Decide everything, write nothing
        #[arg(long)]
        dry_run: bool,
        /// Read the Claude side from this directory instead of $HOME
        #[arg(long, value_name = "DIR")]
        claude_config_dir: Option<PathBuf>,
    },
    /// Translate ~/.claude.json mcpServers into $CODEX_HOME/config.toml
    Mcp {
        /// Decide everything, write nothing
        #[arg(long)]
        dry_run: bool,
        /// Read the Claude side from this directory instead of $HOME
        #[arg(long, value_name = "DIR")]
        claude_config_dir: Option<PathBuf>,
        /// One-time: `git rm --cached config.toml` + gitignore it, so stdio
        /// secrets may be written to a file git no longer publishes
        #[arg(long)]
        migrate_untrack: bool,
    },
}

#[derive(Subcommand)]
pub enum MeshCmd {
    /// Register this session in mesh presence
    Register(MeshPeerArgs),
    /// Refresh this session's mesh presence
    Heartbeat {
        #[command(flatten)]
        peer: MeshPeerArgs,
        #[arg(long)]
        working_on: Option<String>,
        #[arg(long, value_name = "DIR")]
        cwd: Option<PathBuf>,
    },
    /// Mark this session retired in mesh presence
    Retire(MeshPeerArgs),
    /// Set this session's explicit mesh status; empty text clears it
    Status {
        #[command(flatten)]
        peer: MeshPeerArgs,
        text: String,
    },
    /// Append a shared mesh feed post
    Post {
        #[command(flatten)]
        peer: MeshPeerArgs,
        /// Message text, or `-` to read stdin
        text: Option<String>,
        /// Read message text from this file
        #[arg(long, value_name = "FILE")]
        message_file: Option<PathBuf>,
        /// Add a bead reference to the post
        #[arg(long = "ref-beads", value_name = "ID")]
        ref_beads: Vec<String>,
        /// Add a worker reference to the post
        #[arg(long = "ref-worker", value_name = "ID")]
        ref_worker: Vec<String>,
    },
    /// Read the shared mesh feed
    Feed {
        /// Only entries strictly after this seq
        #[arg(long, value_name = "SEQ")]
        since: Option<u64>,
        /// Emit full feed entries as JSON
        #[arg(long)]
        json: bool,
    },
    /// Send a directed mesh note to a peer or worker id
    Note {
        #[command(flatten)]
        peer: MeshPeerArgs,
        /// Target peer id, peer uuid/prefix, or worker id
        target: String,
        /// Use `-` to read the note from stdin
        stdin: Option<String>,
        /// Read note text from this file
        #[arg(long, value_name = "FILE")]
        message_file: Option<PathBuf>,
        /// Add a bead reference to the note
        #[arg(long = "ref-beads", value_name = "ID")]
        ref_beads: Vec<String>,
        /// Add a worker reference to the note
        #[arg(long = "ref-worker", value_name = "ID")]
        ref_worker: Vec<String>,
    },
    /// Read this peer's unread mesh notes
    Inbox {
        #[command(flatten)]
        peer: MeshPeerArgs,
        /// Emit full note entries as JSON
        #[arg(long)]
        json: bool,
        /// Move listed notes to inbox/read and advance the inbox cursor
        #[arg(long)]
        consume: bool,
    },
    /// List mesh peers and live codex workers; pending codex rows are not addressable
    Roster {
        /// Emit full roster rows as JSON
        #[arg(long)]
        json: bool,
    },
    /// Emit this peer's bounded ambient mesh digest
    Inject {
        #[command(flatten)]
        peer: MeshPeerArgs,
        /// Emit the digest block as a JSON string
        #[arg(long)]
        json: bool,
    },
    /// Print the Claude Code hooks settings fragment; never installs it
    Hooks {
        /// Emit the snippet to stdout
        #[arg(long)]
        print: bool,
    },
    /// Manage the layer-2 mesh fleet registry
    Fleet {
        #[command(subcommand)]
        cmd: MeshFleetCmd,
    },
    /// Run and inspect the layer-2 mesh gateway daemon
    Gateway {
        #[command(subcommand)]
        cmd: MeshGatewayCmd,
    },
}

#[derive(clap::Args)]
pub struct MeshPeerArgs {
    #[arg(long)]
    pub session: Option<String>,
    #[arg(long, value_enum, default_value_t = MeshProviderArg::Claude)]
    pub provider: MeshProviderArg,
}

#[derive(Subcommand)]
pub enum MeshFleetCmd {
    /// Mint the fleet root, store the registry URL, and apply the registry migration
    Init {
        /// Registry connection string source; use `-` to read from stdin
        #[arg(long, value_name = "SOURCE")]
        registry: Option<String>,
    },
    /// List gateway registry rows with reader-computed staleness
    Ls {
        /// Emit registry rows as JSON
        #[arg(long)]
        json: bool,
    },
    /// Mark a gateway revoked in the registry
    Revoke {
        gateway_id: String,
    },
    /// Mint root.pub.next and re-sign this machine's gateway token
    #[command(after_help = "After distributing root.pub.next and observing the fleet on the next root, manually remove root.pub.next or promote roots on each machine as appropriate for the rotation.")]
    RotateRoot,
}

#[derive(Subcommand)]
pub enum MeshGatewayCmd {
    /// Run the mesh gateway supervisor in the foreground
    Run,
    /// Report gateway health (exit 7 when it is not running)
    Status,
    /// Ask the running gateway to stop
    Stop,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
#[value(rename_all = "lowercase")]
pub enum MeshProviderArg {
    Claude,
    Codex,
}

impl From<MeshProviderArg> for crate::mesh::Provider {
    fn from(provider: MeshProviderArg) -> Self {
        match provider {
            MeshProviderArg::Claude => crate::mesh::Provider::Claude,
            MeshProviderArg::Codex => crate::mesh::Provider::Codex,
        }
    }
}
