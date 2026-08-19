// tests/sync_live.rs
//
// The one place in this repository that points `sync` at the developer's REAL
// home — and it may only ever READ it.
//
// **No live apply and no live migration is in this plan.** Every invocation
// below carries `--dry-run`; `a2a sync mcp --migrate-untrack` is never run
// against the real `~/.codex`, and `a2a sync skills`/`a2a sync mcp` are never
// run against it without `--dry-run`. Turning this machine's real config.toml
// into an untracked one is a post-merge human decision, not something a test
// suite does on somebody's behalf while they are looking the other way.
//
// This is also the only place `A2A_SYNC_ALLOW_REAL_HOME=1` is ever set. That
// variable is the runtime half of the hermeticity guard (Global Constraints):
// without it, a run that resolved the real `$HOME` refuses with
// `SYNC_REFUSES_REAL_HOME` and exit 9, which is exactly what protects every
// other `tests/sync_*.rs` from a forgotten `CODEX_HOME`. `common::sync_a2a`
// *removes* the variable, so [`Live::cmd`] has to put it back AFTER building
// the command — ordering that is load-bearing rather than stylistic.
//
// What makes reading somebody's real home acceptable is that the read is
// *proved*: every dry-run is bracketed by a [`Snapshot`] of the two git
// repositories involved (`~/.codex` and its `skills` submodule), the content
// hash of `config.toml` and of the skills tree, and the presence and content of
// `~/.a2a/sync-manifest.json`. The assertion is a before/after **diff**, never
// "the tree is clean": the real `~/.codex` is already dirty for reasons that
// have nothing to do with a2a (Ground truth found four modified tracked files
// and ~20 untracked entries), so "clean" would be a red test about somebody's
// unrelated edits. It turned out to be dirty *and moving* — see
// [`Snapshot::assert_unchanged_since`], which is where that observation and
// what it costs the porcelain comparison are recorded.
//
// The value of running here rather than against a fixture is that the fixtures
// are ours and this machine is not. Three things only a real machine can say:
//
//   1. **The classification survives contact with a real corpus.** Pinned by
//      MEMBERSHIP, never by totals — a machine gains and loses skills and MCP
//      servers weekly, and a test that pinned "19 entries" would rot into
//      something people delete rather than read.
//   2. **The redaction survives contact with real credentials.** The no-leak
//      sweep builds its needles by reading the real `~/.claude.json` itself, so
//      it covers values nobody thought to put in a fixture. Needles are held in
//      memory, and a failure prints a LENGTH — never the value it caught.
//   3. **`doctor` really does fail on this machine**, because `~/.codex/config.
//      toml` really is tracked and really does carry a live key on line 23.
//
// Gated twice over, like the Phase-2 tier-5 proofs in `tests/e2e_appserver.rs`:
// `#[ignore]`d so a plain `cargo test` never runs them, AND re-checking
// `A2A_SYNC_LIVE=1` at runtime so `--include-ignored` cannot reach the real home
// by accident.
//
//   A2A_SYNC_LIVE=1 cargo test --test sync_live -- --ignored

mod common;

use a2a::commands::sync_cmd::ALLOW_REAL_HOME_ENV;
use a2a::sync::manifest::{hash_path, Manifest};
use a2a::sync::mcp::TARGET_FILE;
use a2a::sync::skills::SEED_SIDE_EFFECTING;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

/// The environment gate. Distinct from Phase 2's `A2A_E2E` on purpose: these
/// cases spend no tokens and need no login, but they do touch a directory that
/// one wrong flag would damage, so opting into them is a separate decision.
const LIVE_ENV: &str = "A2A_SYNC_LIVE";

/// Every invocation here is an external process. A dry-run of both kinds takes
/// well under a second on this machine; the bound exists so a hung child fails
/// the case instead of the suite.
const STEP_BUDGET: Duration = Duration::from_secs(120);

/// The runtime half of the gate — `--ignored` alone would let
/// `--include-ignored` point a run at the real home.
fn gated(what: &str) -> bool {
    if std::env::var(LIVE_ENV).as_deref() != Ok("1") {
        eprintln!("{LIVE_ENV} != 1 — skipping {what}");
        return false;
    }
    true
}

fn real_home() -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").expect("a real $HOME is the subject of this file"))
}

/// The codex home as the binary itself resolves it: `$CODEX_HOME` when it is
/// set, else `~/.codex`. Spelled here rather than imported because
/// `doctor::effective_codex_home` is `pub(crate)`.
fn real_codex_home() -> PathBuf {
    match std::env::var_os("CODEX_HOME").filter(|v| !v.is_empty()) {
        Some(v) => PathBuf::from(v),
        None => real_home().join(".codex"),
    }
}

/// The real `~/.a2a` — never written by anything in this file, only watched.
fn real_a2a_home() -> PathBuf {
    real_home().join(".a2a")
}

/// The real machine, plus the temp `$A2A_HOME` this file's runs keep their
/// (never-saved) bookkeeping in.
///
/// The ledger is deliberately NOT the real `~/.a2a/sync-manifest.json`: a
/// dry-run never saves one, so pointing at the real home would prove nothing
/// extra, and reading an existing ledger would make every classification below
/// depend on whatever a previous apply had recorded. An empty ledger is the
/// state that makes the destination's own tables and directories foreign, which
/// is what the `skipped-collision` and `skipped-unowned` pins are about.
struct Live {
    tmp: tempfile::TempDir,
    home: PathBuf,
    codex_home: PathBuf,
    claude_dir: PathBuf,
    /// Everything every step printed, swept for real values at the end.
    transcript: std::cell::RefCell<Vec<String>>,
}

impl Live {
    fn new() -> Live {
        let tmp = tempfile::tempdir().unwrap();
        Live {
            home: tmp.path().join("a2a"),
            codex_home: real_codex_home(),
            // `$HOME` itself, so `skills_source_root` takes its HOME branch and
            // the source is the real `~/.claude/skills` (the other branch is
            // for fixture directories, which is what every hermetic test uses).
            claude_dir: real_home(),
            transcript: std::cell::RefCell::new(Vec::new()),
            tmp,
        }
    }

    fn config(&self) -> PathBuf {
        self.codex_home.join(TARGET_FILE)
    }
    fn skills_dest(&self) -> PathBuf {
        self.codex_home.join("skills")
    }
    fn skills_source(&self) -> PathBuf {
        self.claude_dir.join(".claude").join("skills")
    }
    fn claude_json(&self) -> PathBuf {
        self.claude_dir.join(".claude.json")
    }

    /// The ONE command builder, per Global Constraints — `common::sync_a2a`
    /// plus the three things this file needs on top of it.
    ///
    /// The opt-in is re-added AFTER the helper has run: `sync_a2a` REMOVES
    /// `A2A_SYNC_ALLOW_REAL_HOME` (that removal is what makes a forgotten
    /// `CODEX_HOME` in some other test a red test rather than a write into
    /// `~/.codex`), so setting it before the call would set nothing.
    fn cmd(&self) -> assert_cmd::Command {
        let nowhere = self.tmp.path().join("no-such-gitconfig");
        let mut c = common::sync_a2a(
            &self.home,
            &self.codex_home,
            &self.claude_dir,
            self.tmp.path(),
        );
        c.env(ALLOW_REAL_HOME_ENV, "1")
            // Hermetic git even here: the binary shells out to `git` to ask
            // whether config.toml is published, and the developer's own
            // gitconfig (hooks, templates, signing) must not reach it.
            .env("GIT_CONFIG_GLOBAL", &nowhere)
            .env("GIT_CONFIG_SYSTEM", &nowhere)
            // `doctor`'s `context_canon` reads the PROCESS cwd, and the
            // repository this suite runs in is not the subject of any assertion
            // here.
            .current_dir(self.tmp.path())
            .timeout(STEP_BUDGET);
        c
    }

    /// One dry-run, asserted successful, returning its `data` payload. Every
    /// caller passes `--dry-run`; there is no apply path in this file.
    fn dry_run(&self, args: &[&str]) -> Value {
        assert!(
            args.contains(&"--dry-run"),
            "this file only ever reads the real home: {args:?}"
        );
        let out = self.cmd().args(args).output().unwrap();
        self.transcript
            .borrow_mut()
            .push(format!("{}{}", stdout(&out), stderr(&out)));
        assert_eq!(
            out.status.code(),
            Some(0),
            "`a2a {}` failed: {}",
            args.join(" "),
            stderr(&out)
        );
        envelope(&out)["data"].clone()
    }

    /// Everything a2a must not have moved, captured immediately before and
    /// immediately after a dry-run.
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            codex_status: git_status(&self.codex_home),
            skills_status: git_status(&self.skills_dest()),
            config_hash: hash_of(&self.config()),
            // Hashes contents, and `hash_path` skips `.git` at every level —
            // so a `git status` refreshing an index cannot masquerade as a2a
            // having edited somebody's skill.
            skills_hash: hash_of(&self.skills_dest()),
            manifest: hash_of_optional(&Manifest::path(&real_a2a_home())),
        }
    }

    /// No value that lives in the real `~/.claude.json` reached any report.
    ///
    /// Needles are read from the source itself (so a credential nobody thought
    /// to put in a fixture is still covered), held only in memory, and a
    /// failure prints the LENGTH of what leaked and never the value.
    fn assert_nothing_leaked(&self) {
        let needles = live_secrets(&self.claude_json());
        assert!(
            !needles.is_empty(),
            "the real {} carries no header or env values — the sweep would prove nothing",
            self.claude_json().display()
        );
        let transcript = self.transcript.borrow();
        for secret in &needles {
            for (i, printed) in transcript.iter().enumerate() {
                assert!(
                    !printed.contains(secret.as_str()),
                    "step {i} leaked a {}-char value read from {}",
                    secret.len(),
                    self.claude_json().display()
                );
            }
        }
    }
}

/// The five facts that together say "a2a wrote nothing here".
#[derive(PartialEq, Eq, Debug)]
struct Snapshot {
    codex_status: BTreeSet<String>,
    skills_status: BTreeSet<String>,
    config_hash: String,
    skills_hash: String,
    /// `None` when `~/.a2a/sync-manifest.json` does not exist — so "did not
    /// appear" and "did not change" are one assertion rather than two.
    manifest: Option<String>,
}

/// The complete write surface `sync` has inside a codex home, as the leading
/// path components a `git status --porcelain` line can carry.
///
/// `config.toml` is the mcp destination, `skills/` the skills one (its own
/// `.gitignore` included), the bare `.gitignore` is the ignore line
/// `--migrate-untrack` appends, and `tmp/` is what `codex mcp list` leaves
/// behind when the applier validates a candidate. There is no fifth thing: a
/// path outside this list is one no code path in `src/sync/` can name.
const A2A_REACHABLE: [&str; 4] = ["config.toml", ".gitignore", "skills", "tmp"];

/// Whether a porcelain line is about something a2a could have written.
///
/// Porcelain v1 is `XY <path>`, with a rename carrying ` -> `; the first
/// component of the path is all this needs. Prefix matching is on whole path
/// components, so a `skills-backup/` of somebody's own is not mistaken for the
/// destination tree.
fn a2a_could_have_written(porcelain_line: &str) -> bool {
    let path = porcelain_line.get(3..).unwrap_or("").trim_matches('"');
    let head = path.split('/').next().unwrap_or("");
    A2A_REACHABLE.contains(&head)
}

impl Snapshot {
    /// The zero-writes proof: nothing a2a can reach moved between `before` and
    /// `self`.
    ///
    /// Content hashes, the ledger and the whole skills-submodule porcelain are
    /// compared as EXACT values — every one of them is inside a2a's write
    /// surface, and any movement at all is a2a's doing.
    ///
    /// The `~/.codex` porcelain is the one set that needs a word of
    /// explanation. **OBSERVED 2026-08-19, transcribed rather than papered
    /// over:** this machine's real `~/.codex` gains and loses UNTRACKED entries
    /// on its own while nothing a2a-related runs — a resident tool checkpointed
    /// its databases mid-test and four `goals_1.sqlite-wal`-shaped sidecars
    /// vanished between two snapshots taken 60 ms apart. That is the same class
    /// of fact as Ground truth's already-dirty tree, one step further: the tree
    /// is not merely dirty, it is *moving*. So the assertion over that set is
    /// scoped to [`A2A_REACHABLE`] — every difference involving `config.toml`,
    /// `skills`, `.gitignore` or `tmp` is a hard failure, and a difference
    /// involving anything else is reported on stderr so a reader can see the
    /// machine moved, and is not silently swallowed. Nothing a2a is capable of
    /// writing is exempt.
    fn assert_unchanged_since(&self, before: &Snapshot, what: &str) {
        let mut moved: Vec<String> = Vec::new();

        let appeared: Vec<&String> = self.codex_status.difference(&before.codex_status).collect();
        let vanished: Vec<&String> = before.codex_status.difference(&self.codex_status).collect();
        let (mine, theirs): (Vec<&&String>, Vec<&&String>) = appeared
            .iter()
            .chain(vanished.iter())
            .partition(|line| a2a_could_have_written(line));
        if !mine.is_empty() {
            moved.push(format!(
                "~/.codex `git status --porcelain` moved on a path a2a can write: {mine:?}"
            ));
        }
        if !theirs.is_empty() {
            eprintln!(
                "note: {what} ran while something else was writing to ~/.codex — untracked \
                 entries outside a2a's write surface moved: {theirs:?}"
            );
        }

        if self.skills_status != before.skills_status {
            moved.push(format!(
                "~/.codex/skills `git status --porcelain`: {:?} -> {:?}",
                before.skills_status, self.skills_status
            ));
        }
        for (name, after, prior) in [
            ("config.toml content", &self.config_hash, &before.config_hash),
            ("skills tree content", &self.skills_hash, &before.skills_hash),
        ] {
            if after != prior {
                moved.push(format!("{name}: {prior} -> {after}"));
            }
        }
        if self.manifest != before.manifest {
            moved.push(format!(
                "~/.a2a/sync-manifest.json: {:?} -> {:?}",
                before.manifest, self.manifest
            ));
        }

        assert!(
            moved.is_empty(),
            "{what} did not leave the real machine alone:\n  {}",
            moved.join("\n  ")
        );
    }
}

/// `git status --porcelain` as a SET (porcelain's ordering between index and
/// worktree entries is git's business), run hermetically.
///
/// A directory that is not a work tree yields one sentinel line rather than a
/// panic: the before/after comparison is still exactly as meaningful, and a
/// machine whose `~/.codex/skills` is a plain directory should get a real
/// verdict from this file rather than an infrastructure error.
fn git_status(repo: &Path) -> BTreeSet<String> {
    let nowhere = repo.join("no-such-gitconfig-a2a");
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["status", "--porcelain"])
        .env("GIT_CONFIG_GLOBAL", &nowhere)
        .env("GIT_CONFIG_SYSTEM", &nowhere)
        .output()
        .expect("git is on PATH");
    if !out.status.success() {
        return BTreeSet::from(["<not a git work tree>".to_string()]);
    }
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::to_string)
        .collect()
}

fn hash_of(p: &Path) -> String {
    hash_path(p).unwrap_or_else(|e| panic!("hashing {}: {e}", p.display()))
}
fn hash_of_optional(p: &Path) -> Option<String> {
    p.exists().then(|| hash_of(p))
}

fn envelope(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout must be one JSON envelope ({e}): {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}
fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}
fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// `name -> disposition`, read back out of a `--json` payload.
fn reported(data: &Value) -> BTreeMap<String, String> {
    data["entries"]
        .as_array()
        .expect("entries is an array")
        .iter()
        .map(|e| {
            (
                e["name"].as_str().unwrap().to_string(),
                e["disposition"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}
fn detail_of(data: &Value, name: &str) -> String {
    data["entries"]
        .as_array()
        .expect("entries is an array")
        .iter()
        .find(|e| e["name"] == name)
        .unwrap_or_else(|| panic!("no entry for {name}"))["detail"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Every header and env value in the real `~/.claude.json` long enough to be a
/// credential — the same shape `tests/sync_e2e.rs::fixture_secrets` builds from
/// the fixtures, pointed at the real file.
///
/// Deliberately NOT filtered by the classifier: the sweep must catch a value a
/// misclassification let through, so it treats every value as if it were
/// secret. (Eight characters is the floor that keeps `"1"`-shaped flags from
/// matching half the alphabet.)
fn live_secrets(claude_json: &Path) -> Vec<String> {
    let raw: Value = serde_json::from_str(
        &std::fs::read_to_string(claude_json)
            .unwrap_or_else(|e| panic!("reading {}: {e}", claude_json.display())),
    )
    .unwrap_or_else(|e| panic!("parsing {}: {e}", claude_json.display()));
    let mut out = Vec::new();
    let Some(servers) = raw["mcpServers"].as_object() else {
        return out;
    };
    for server in servers.values() {
        for group in ["headers", "env"] {
            if let Some(map) = server.get(group).and_then(|g| g.as_object()) {
                out.extend(
                    map.values()
                        .filter_map(|v| v.as_str())
                        .filter(|v| v.len() >= 8)
                        .map(str::to_string),
                );
            }
        }
    }
    out
}

/// Quoted literals long enough to be a credential, taken from the lines the
/// crate's own scanner flagged in a TOML file.
///
/// Used to prove `doctor` printed no VALUE from `~/.codex/config.toml` — the
/// check names a file and a line number and nothing else. Reusing
/// `redact::scan_text` (rather than a second regex here) is deliberate: the two
/// must agree about which lines carry a credential, or the line numbers doctor
/// prints would be untrustworthy.
fn quoted_secrets_in(toml_file: &Path) -> Vec<String> {
    let Ok(text) = std::fs::read_to_string(toml_file) else {
        return Vec::new();
    };
    let flagged: BTreeSet<usize> = a2a::redact::scan_text(&text)
        .into_iter()
        .map(|h| h.line)
        .collect();
    let mut out = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        if !flagged.contains(&(idx + 1)) {
            continue;
        }
        // Sixteen, not eight: a flagged line also carries flag names
        // (`--api-key`) and package names, and a needle short enough to be one
        // of those would fail this test for a reason that is not a leak.
        out.extend(
            line.split('"')
                .skip(1)
                .step_by(2)
                .filter(|v| v.len() >= 16)
                .map(str::to_string),
        );
    }
    out
}

// ---------------------------------------------------------------------------
// The read-only proof.
// ---------------------------------------------------------------------------

/// Both dry-runs against the real machine: nothing moves, the classification is
/// the one the plan predicted, and no credential reaches a report.
///
/// One test rather than three so the two dry-runs are executed once and the
/// zero-writes brackets sit as tightly around them as they can.
#[test]
#[ignore = "reads the developer's real ~/.codex and ~/.claude; run with A2A_SYNC_LIVE=1 cargo test --test sync_live -- --ignored"]
fn live_dry_runs_read_the_real_machine_and_write_nothing() {
    if !gated("live_dry_runs_read_the_real_machine_and_write_nothing") {
        return;
    }
    let live = Live::new();

    // ------------------------------------------------------ skills, read only
    let before = live.snapshot();
    let skills = live.dry_run(&["--json", "sync", "skills", "--dry-run"]);
    live.snapshot()
        .assert_unchanged_since(&before, "`a2a sync skills --dry-run`");
    assert_eq!(
        skills["dry_run"], true,
        "the payload must say it decided without writing"
    );

    // --------------------------------------------------------- skills, pinned
    // By MEMBERSHIP, never by totals: this machine gains and loses skills.
    let by_name = reported(&skills);
    assert_eq!(
        by_name.get("README.md").map(String::as_str),
        Some("skipped-not-a-skill"),
        "the curated tree's README is not a skill: {by_name:?}"
    );
    // OBSERVED 2026-08-19, transcribed rather than papered over: the real
    // `~/.claude/skills` also holds a `.gitignore` FILE, which Ground truth's
    // "20 entries" listing did not mention. It classifies the same way
    // README.md does — `skipped-not-a-skill`, "not a directory" — which is the
    // correct answer, so the pin below records the fact instead of hiding it.
    if let Some(disposition) = by_name.get(".gitignore") {
        assert_eq!(
            disposition, "skipped-not-a-skill",
            "a plain file in the source tree is never a skill"
        );
    }

    // Every seed name the source actually holds is refused. Scoped to what is
    // present because the seed list is a superset of any one machine's tree.
    let present = source_entries(&live.skills_source());
    let seeds: Vec<&str> = SEED_SIDE_EFFECTING
        .iter()
        .copied()
        .filter(|n| present.contains(*n))
        .collect();
    assert!(
        !seeds.is_empty(),
        "no seed-list skill is installed — the refusal pin would prove nothing"
    );
    for name in &seeds {
        assert_eq!(
            by_name.get(*name).map(String::as_str),
            Some("skipped-side-effecting"),
            // A seed skill that ALSO existed in the destination would report
            // `skipped-collision` instead (collision outranks the heuristic in
            // `skills::plan`'s pinned order). None does today; if one ever
            // does, transcribe that here rather than deleting the assertion.
            "{name} reaches outside the machine and must never be mirrored: {by_name:?}"
        );
    }

    // The curated ports in `~/.codex/skills` win over the Claude-side originals.
    for name in ["devloop", "writing-devloop-plans"] {
        assert_eq!(
            by_name.get(name).map(String::as_str),
            Some("skipped-collision"),
            "{name} exists in the curated submodule and a2a did not create it: {by_name:?}"
        );
    }

    // The front-matter-scoped heuristic is not a body scan: these two mention
    // plenty of verbs in their prose and none in their front matter.
    for name in ["techniques", "technique-research"] {
        assert_ne!(
            by_name.get(name).map(String::as_str),
            Some("skipped-side-effecting"),
            "{name} is a read-only skill: {by_name:?}"
        );
    }

    let synced: BTreeSet<&str> = by_name
        .iter()
        .filter(|(_, d)| d.as_str() == "synced")
        .map(|(n, _)| n.as_str())
        .collect();
    for name in SEED_SIDE_EFFECTING {
        assert!(
            !synced.contains(name),
            "a seed-list skill reached the synced set: {synced:?}"
        );
    }

    // --------------------------------------------------------- mcp, read only
    let before = live.snapshot();
    let mcp = live.dry_run(&["--json", "sync", "mcp", "--dry-run"]);
    live.snapshot()
        .assert_unchanged_since(&before, "`a2a sync mcp --dry-run`");
    assert_eq!(mcp["dry_run"], true);

    // ------------------------------------------------------------ mcp, pinned
    let by_name = reported(&mcp);
    // The two bearer-carrying HTTP servers are translated through the
    // indirection, never as literals — and the env var names are exact.
    for (server, env_var) in [
        ("Neon", "A2A_MCP_NEON_TOKEN"),
        ("linear", "A2A_MCP_LINEAR_TOKEN"),
    ] {
        let detail = detail_of(&mcp, server);
        assert!(
            detail.contains(&format!("bearer_token_env_var = \"{env_var}\"")),
            "{server} must translate through {env_var}: {detail}"
        );
    }

    // `locationId` is a policy skip, not a capability one (deviation 2): codex
    // 0.140.0 can express the header, and the detail names it.
    assert_eq!(
        by_name.get("ghl").map(String::as_str),
        Some("skipped-untranslatable"),
        "{by_name:?}"
    );
    assert!(
        detail_of(&mcp, "ghl").contains("locationId"),
        "the skip must name the header it is about: {}",
        detail_of(&mcp, "ghl")
    );

    // The `Disposition` spelling, not the prose one: `BlockedTrackedSecrets`
    // serializes as `skipped-secret`.
    assert!(
        target_is_tracked(&live.codex_home),
        "this pin is about a PUBLISHED destination; git no longer tracks {}",
        live.config().display()
    );
    for server in ["atlassian", "google-workspace"] {
        assert_eq!(
            by_name.get(server).map(String::as_str),
            Some("skipped-secret"),
            "{server} carries env credentials into a git-tracked file: {by_name:?}"
        );
    }

    // `mx` is the live case for "never touch a table the ledger does not own":
    // it exists on BOTH sides, and nobody synced the destination's copy.
    assert_eq!(
        by_name.get("mx").map(String::as_str),
        Some("skipped-unowned"),
        "{by_name:?}"
    );

    // ------------------------------------------------------------- no leaks
    // Every value a report DID print is masked, and the mask carries a length
    // and nothing else.
    let printed = live.transcript.borrow().join("");
    let masks: Vec<&str> = printed.match_indices("[REDACTED:").map(|(_, m)| m).collect();
    assert!(
        !masks.is_empty(),
        "no value was reported at all — the mask assertion would prove nothing"
    );
    for (at, _) in printed.match_indices("[REDACTED:") {
        let tail = &printed[at + "[REDACTED:".len()..];
        let end = tail.find(']').expect("every mask closes");
        assert!(
            !tail[..end].is_empty() && tail[..end].chars().all(|c| c.is_ascii_digit()),
            "a mask carried something other than a length: [REDACTED:{}]",
            &tail[..end]
        );
    }
    live.assert_nothing_leaked();
}

/// The real `a2a doctor --json` on this machine: thirteen checks in the pinned
/// order, and the security-class failure Phase 3 introduced.
///
/// Deliberately silent about every other check's status. `~/.codex` is a moving
/// target — a broker may or may not be up, `OPENAI_API_KEY` may or may not be
/// exported, a version bump may or may not have landed — and a gated case that
/// asserted the whole board would fail for reasons that say nothing about a2a.
#[test]
#[ignore = "reads the developer's real ~/.codex; run with A2A_SYNC_LIVE=1 cargo test --test sync_live -- --ignored"]
fn live_doctor_fails_on_the_tracked_config_and_lists_thirteen_checks() {
    if !gated("live_doctor_fails_on_the_tracked_config_and_lists_thirteen_checks") {
        return;
    }
    /// The thirteen, in report order — the same pin `tests/cmd_doctor.rs` holds.
    const CHECK_IDS: [&str; 13] = [
        "codex_present",
        "codex_version",
        "layout_version",
        "home_perms",
        "workers_health",
        "env_inheritance",
        "app_server_handshake",
        "codex_home_match",
        "broker_status",
        "auth_mode",
        "context_canon",
        "codex_config_secrets",
        "sync_state",
    ];

    let live = Live::new();
    assert!(
        target_is_tracked(&live.codex_home),
        "the failure below is about a PUBLISHED config; git no longer tracks {}",
        live.config().display()
    );

    // The real codex on PATH, not the fixture answerer: this case is about what
    // `doctor` says on this machine. `doctor` writes nothing under the codex
    // home, and the hash below is the proof rather than the promise.
    let before = hash_of(&live.config());
    let out = live
        .cmd()
        .env_remove("A2A_CODEX_BIN")
        .args(["--json", "doctor"])
        .output()
        .unwrap();
    assert_eq!(
        hash_of(&live.config()),
        before,
        "doctor edited {}",
        live.config().display()
    );
    let payload = envelope(&out);
    let checks = payload["data"]["checks"]
        .as_array()
        .expect("doctor reports an array of checks");
    let ids: Vec<&str> = checks.iter().map(|c| c["id"].as_str().unwrap()).collect();
    assert_eq!(ids, CHECK_IDS, "the report order is a public contract");

    let secrets = checks
        .iter()
        .find(|c| c["id"] == "codex_config_secrets")
        .expect("the check is in the list above");
    assert_eq!(
        secrets["status"], "fail",
        "this machine publishes a credential: {secrets}"
    );
    let detail = secrets["detail"].as_str().unwrap();
    assert!(
        detail.contains("line 23"),
        "Ground truth puts the context7 key on line 23: {detail}"
    );
    assert!(
        detail.contains(&live.config().to_string_lossy().to_string()),
        "the check names the file it is about: {detail}"
    );
    // A failing check is what makes `a2a doctor` exit 9 here — the consequence
    // the README states plainly, asserted rather than described.
    assert_eq!(out.status.code(), Some(9), "a failed check exits 9");

    // The line number is the whole payload: no value from that line, and none
    // from `~/.claude.json` either.
    let printed = format!("{}{}", stdout(&out), stderr(&out));
    for secret in quoted_secrets_in(&live.config())
        .into_iter()
        .chain(live_secrets(&live.claude_json()))
    {
        assert!(
            !printed.contains(&secret),
            "doctor printed a {}-char value; it may name lines and files only",
            secret.len()
        );
    }
}

/// The entry names of a source skills tree, so a pin can be scoped to what is
/// actually installed.
fn source_entries(root: &Path) -> BTreeSet<String> {
    std::fs::read_dir(root)
        .unwrap_or_else(|e| panic!("reading {}: {e}", root.display()))
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

/// Whether git publishes `<codex home>/config.toml` — the precondition two pins
/// above depend on, asked directly rather than inferred from a report.
fn target_is_tracked(codex_home: &Path) -> bool {
    let nowhere = codex_home.join("no-such-gitconfig-a2a");
    std::process::Command::new("git")
        .arg("-C")
        .arg(codex_home)
        .args(["ls-files", "--error-unmatch", "--", TARGET_FILE])
        .env("GIT_CONFIG_GLOBAL", &nowhere)
        .env("GIT_CONFIG_SYSTEM", &nowhere)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}
