//! The whole feature, driven through the public CLI only: both kinds, one codex
//! home, twice over.
//!
//! Every other `tests/sync_*.rs` file proves one command against a world built
//! for it. This one proves the thing an operator actually does — mirror the
//! skills, translate the servers, take `config.toml` out of git, ask `doctor`
//! whether it worked, then run the lot again — and the two properties that only
//! show up when those steps share a destination:
//!
//! **Containment survives the pipeline.** The skills stand-in is a git
//! repository and the codex home is another one, so at the end of a full run
//! each has an EXACT, named set of git-visible changes: the managed ignore block
//! in the first, and the migration's staged removal plus its own ignore line in
//! the second. Anything else — a stray `tmp/` from the validator, a managed
//! skill nobody covered, a rewritten table — turns those sets into a red test.
//!
//! **Losing the ledger costs updates, never content.** `sync-manifest.json` is
//! the only thing that tells a2a which destinations are its own; delete it and
//! every one of them becomes somebody else's, which is the safe direction. That
//! property has its own test below, because it is the one a manifest-only tool
//! is built around.
//!
//! Hermetic by construction, per Global Constraints: a temp `$A2A_HOME`, a temp
//! `CODEX_HOME` that is a fixture git repository, fixture Claude directories,
//! `GIT_CONFIG_GLOBAL`/`GIT_CONFIG_SYSTEM` pointed at a path that does not
//! exist, and every invocation built by `common::sync_a2a` — which leaves
//! `A2A_SYNC_ALLOW_REAL_HOME` unset, so a forgotten variable is a red test
//! rather than a write into the developer's real `~/.codex`. No network, no
//! tokens, no broker: sync opens no socket, and the one broker-staleness case
//! belongs to `tests/sync_mcp.rs`.

mod common;

use a2a::sync::manifest::{hash_path, Manifest};
use a2a::sync::{mcp, skills, Kind};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::{Duration, Instant};

/// Every `a2a` invocation below is an external process, and one that hung would
/// take the suite with it. `assert_cmd`'s timeout kills the child at this bound
/// and returns its (signalled, code-less) status, which every `expect_ok` below
/// reports as a failure naming the step.
const STEP_BUDGET: Duration = Duration::from_secs(60);

/// How long a converged-state assertion waits for a process table to settle.
const DRAIN_BUDGET: Duration = Duration::from_secs(10);

/// The codex home's hand-written, git-tracked `config.toml`: an untouched table
/// a2a must preserve, the live `mx` collision (present on BOTH sides, owned by
/// nobody) and two secret-shaped lines, so the migration this pipeline runs has
/// something true to say and `doctor` has something to refuse.
const E2E_CONFIG: &str = r#"# The codex home's own config.toml, hand written, and tracked by git.
model = "gpt-5-codex"

[tui]
notifications = true

[mcp_servers.mx]
# mx exists on BOTH sides. a2a did not write this table and must never touch it.
command = "mx"
args = ["mcp"]

[mcp_servers.context7]
url = "https://mcp.context7.com/mcp"
http_headers = { Authorization = "Bearer FAKEcontext7keyFAKEcontext7key" }

[mcp_servers.gh]
command = "gh-mcp"
env = { GITHUB_TOKEN = "ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
"#;

/// The literals [`E2E_CONFIG`] carries. Neither may appear in any report, in any
/// mode — including `doctor`'s, which names lines and never values.
const CONFIG_SECRETS: [&str; 2] = [
    "Bearer FAKEcontext7keyFAKEcontext7key",
    "ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
];

/// The outer repository's committed `.gitignore`. `skills/` is what keeps the
/// skills stand-in — an ordinary directory holding its own repository — from
/// showing up as `?? skills/` and turning this file's exact-set assertions into
/// a statement about git's submodule handling instead of about a2a.
const OUTER_GITIGNORE: &str = "auth.json\nhistory.jsonl\nsessions/\nskills/\n";

/// The skills repository's own committed `.gitignore`, which is the shape the
/// real `codex-skills` submodule has — and the reason its containment assertion
/// is `[" M .gitignore"]` rather than "empty": a tracked file cannot ignore
/// itself.
const INNER_GITIGNORE: &str = "*.log\n.DS_Store\n";

/// The Claude-side `devloop`, whose curated port already sits in the
/// destination — the live collision from the plan's Ground truth.
const SOURCE_DEVLOOP: &[u8] =
    b"---\nname: devloop\ndescription: The Claude-side original, which must never win.\n---\n\nsource\n";
/// The curated port, committed in the skills repository before any run.
const CURATED_DEVLOOP: &[u8] =
    b"---\nname: devloop\ndescription: The curated port, maintained in the submodule.\n---\n\ncurated\n";

/// The account labels the two fixture directories resolve to (`sync::account_label`
/// takes the basename). Skills are a different story: they land flat in one
/// shared tree and every skill entry is written under `"default"`, whichever
/// Claude directory a run was pointed at.
const ACCOUNT: &str = "claude";
const SECOND_ACCOUNT: &str = "claude-revenium";
const SKILL_ACCOUNT: &str = "default";

/// The three read-only skills the fixture tree yields, in ledger order.
const MANAGED_SKILLS: [&str; 3] = ["portable", "techniques", "wordy"];

/// The stdio servers whose env values look like credentials: blocked while
/// `config.toml` is published, written once it is not. The whole point of the
/// migration step in the pipeline below.
const BLOCKED_UNTIL_MIGRATED: [&str; 3] = ["atlassian", "google-workspace", "telegram"];

/// The seventeen checks `doctor` publishes, in report order — the same pin
/// `tests/cmd_doctor.rs` holds, restated here because this file asserts the
/// list mid-pipeline rather than in isolation.
const CHECK_IDS: [&str; 17] = [
    "codex_present",
    "codex_version",
    "layout_version",
    "home_perms",
    "workers_health",
    "allowed_roots",
    "env_inheritance",
    "app_server_handshake",
    "codex_home_match",
    "broker_status",
    "gateway_enrollment",
    "mesh_hardening",
    "work_awaiting_release",
    "auth_mode",
    "context_canon",
    "codex_config_secrets",
    "sync_state",
];

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sync")
}

/// One hermetic world: a temp `$A2A_HOME`, a temp `CODEX_HOME` that is a git
/// repository with a tracked `config.toml` and a skills stand-in that is a
/// repository of its own, and TWO writable Claude directories — the second is
/// what makes "two accounts, one destination" testable through the CLI.
struct World {
    tmp: tempfile::TempDir,
    home: PathBuf,
    codex_home: PathBuf,
    claude_dir: PathBuf,
    revenium_dir: PathBuf,
    /// Everything every step printed, swept for source values at the end of a
    /// test rather than at every call site.
    transcript: std::cell::RefCell<Vec<String>>,
}

impl World {
    fn new() -> World {
        let tmp = tempfile::tempdir().unwrap();
        let w = World {
            home: tmp.path().join("a2a"),
            codex_home: tmp.path().join("codex"),
            claude_dir: tmp.path().join("claude"),
            revenium_dir: tmp.path().join("claude-revenium"),
            transcript: std::cell::RefCell::new(Vec::new()),
            tmp,
        };
        w.claude_account(&w.claude_dir, "claude.json");
        w.claude_account(&w.revenium_dir, "claude-revenium.json");

        // The skills stand-in FIRST, and it commits its own `.gitignore` before
        // any run: the managed block a2a writes has to arrive as a modification
        // to a tracked file, which is the shape the real submodule has.
        let skills = w.dest();
        std::fs::create_dir_all(skills.join("devloop")).unwrap();
        std::fs::write(skills.join("devloop/SKILL.md"), CURATED_DEVLOOP).unwrap();
        std::fs::write(skills.join(".gitignore"), INNER_GITIGNORE).unwrap();
        w.git(&skills, &["init", "-q", "-b", "main"]);
        w.git(&skills, &["add", "-A"]);
        w.git(&skills, &["commit", "-qm", "curated skills"]);

        // Then the outer repository. No gitlink: a real submodule would make
        // these assertions a statement about git's version-dependent handling
        // of one, and the ignore line above buys everything it would.
        std::fs::write(w.config(), E2E_CONFIG).unwrap();
        std::fs::write(w.codex_home.join(".gitignore"), OUTER_GITIGNORE).unwrap();
        w.git(&w.codex_home, &["init", "-q", "-b", "main"]);
        w.git(&w.codex_home, &["add", ".gitignore", "config.toml"]);
        w.git(&w.codex_home, &["commit", "-qm", "codex home"]);
        w
    }

    /// A writable copy of the fixture Claude tree with `fixture_json` as its
    /// `.claude.json`, plus the Claude-side `devloop` that collides with the
    /// curated port. Both accounts get an identical `skills/` tree, which is
    /// what makes the second account's skills run report `unchanged` rather
    /// than rewriting the first account's copies.
    fn claude_account(&self, dir: &Path, fixture_json: &str) {
        copy_preserving(&fixture_root(), dir);
        std::fs::copy(fixture_root().join(fixture_json), dir.join(".claude.json")).unwrap();
        std::fs::create_dir_all(dir.join("skills/devloop")).unwrap();
        std::fs::write(dir.join("skills/devloop/SKILL.md"), SOURCE_DEVLOOP).unwrap();
    }

    fn source(&self) -> PathBuf {
        self.claude_dir.join("skills")
    }
    fn dest(&self) -> PathBuf {
        self.codex_home.join("skills")
    }
    fn config(&self) -> PathBuf {
        self.codex_home.join(mcp::TARGET_FILE)
    }
    fn config_text(&self) -> String {
        std::fs::read_to_string(self.config()).unwrap()
    }
    fn gitignore_text(&self) -> String {
        std::fs::read_to_string(self.dest().join(".gitignore")).unwrap()
    }
    fn manifest_path(&self) -> PathBuf {
        Manifest::path(&self.home)
    }
    fn manifest(&self) -> Manifest {
        Manifest::load(&self.home).unwrap()
    }
    fn manifest_keys(&self) -> Vec<String> {
        self.manifest().entries.keys().cloned().collect()
    }
    fn manifest_text(&self) -> String {
        std::fs::read_to_string(self.manifest_path()).unwrap_or_default()
    }
    /// The destination server names, read the way the crate reads them — never
    /// by grepping for a header, whose quoting is `toml_edit`'s business.
    fn dest_servers(&self) -> Vec<String> {
        mcp::dest_names(&mcp::read_target(&self.codex_home).unwrap())
    }

    /// `directory -> content hash` for every skill in the destination, the
    /// `.gitignore` beside them deliberately excluded: the ignore block is a2a's
    /// own bookkeeping and a run is allowed to rewrite it, so folding it into a
    /// hash of the whole tree would blur "the block moved" into "somebody's
    /// content moved".
    fn skill_hashes(&self) -> BTreeMap<String, String> {
        std::fs::read_dir(self.dest())
            .unwrap()
            .filter_map(|e| {
                let e = e.unwrap();
                let name = e.file_name().to_string_lossy().into_owned();
                // `.git` is the repository's own bookkeeping, which `git status`
                // itself rewrites; a hash over it would be a flake, not a fact.
                (e.path().is_dir() && !name.starts_with('.'))
                    .then(|| (name, hash_path(&e.path()).unwrap()))
            })
            .collect()
    }

    /// Rewrite a Claude source through a closure — how a test deletes a server.
    fn edit_source(&self, dir: &Path, f: impl FnOnce(&mut Value)) {
        let path = dir.join(".claude.json");
        let mut v: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        f(&mut v);
        std::fs::write(&path, serde_json::to_string_pretty(&v).unwrap()).unwrap();
    }

    /// The ONE command builder, per Global Constraints. Three things are added
    /// to `common::sync_a2a`: hermetic git (the binary shells out to it), a cwd
    /// inside the tempdir (`doctor`'s `context_canon` reads the PROCESS cwd, and
    /// the repository this suite runs in has a `CLAUDE.md` of its own), and the
    /// step budget.
    fn cmd(&self, claude_dir: &Path) -> assert_cmd::Command {
        let nowhere = self.tmp.path().join("no-such-gitconfig");
        let mut c = common::sync_a2a(&self.home, &self.codex_home, claude_dir, self.tmp.path());
        c.env("GIT_CONFIG_GLOBAL", &nowhere)
            .env("GIT_CONFIG_SYSTEM", &nowhere)
            .env_remove("OPENAI_API_KEY")
            .current_dir(self.tmp.path())
            .timeout(STEP_BUDGET);
        c
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_as(&self.claude_dir.clone(), args)
    }
    fn run_as(&self, claude_dir: &Path, args: &[&str]) -> Output {
        let out = self.cmd(claude_dir).args(args).output().unwrap();
        self.transcript
            .borrow_mut()
            .push(format!("{}{}", stdout(&out), stderr(&out)));
        out
    }

    /// One step of the pipeline, asserted successful, returning `data`.
    fn step(&self, args: &[&str]) -> Value {
        self.step_as(&self.claude_dir.clone(), args)
    }
    fn step_as(&self, claude_dir: &Path, args: &[&str]) -> Value {
        let out = self.run_as(claude_dir, args);
        assert_eq!(
            out.status.code(),
            Some(0),
            "`a2a {}` did not complete within {STEP_BUDGET:?}: {}",
            args.join(" "),
            stderr(&out)
        );
        data(&out)
    }

    /// `a2a --json doctor` over this world, as `(payload, exit code)`.
    fn doctor(&self) -> (Value, i32) {
        let out = self.run(&["--json", "doctor"]);
        let code = out.status.code().unwrap_or_else(|| {
            panic!(
                "doctor did not exit within {STEP_BUDGET:?}: {}",
                stderr(&out)
            )
        });
        (envelope(&out), code)
    }

    /// Hermetic git, per Global Constraints: the developer's real gitconfig
    /// (hooks, signing, templates) can never reach a test.
    fn git(&self, repo: &Path, args: &[&str]) -> String {
        let nowhere = self.tmp.path().join("no-such-gitconfig");
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args([
                "-c",
                "user.email=a2a@test",
                "-c",
                "user.name=a2a",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .env("GIT_CONFIG_GLOBAL", &nowhere)
            .env("GIT_CONFIG_SYSTEM", &nowhere)
            .output()
            .expect("git is on PATH");
        assert!(
            out.status.success(),
            "git {args:?} in {}: {}",
            repo.display(),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// `git status --porcelain` as a SET: porcelain's ordering between index
    /// entries and worktree ones is git's business, not this file's.
    fn status(&self, repo: &Path) -> BTreeSet<String> {
        self.git(repo, &["status", "--porcelain"])
            .lines()
            .map(str::to_string)
            .collect()
    }
    fn codex_status(&self) -> BTreeSet<String> {
        self.status(&self.codex_home)
    }
    fn skills_status(&self) -> BTreeSet<String> {
        self.status(&self.dest())
    }
    fn tracks_config(&self) -> bool {
        self.git(&self.codex_home, &["ls-files"])
            .lines()
            .any(|l| l == mcp::TARGET_FILE)
    }

    /// Nothing this world started is still running — bounded, never a bare
    /// sleep. `ps` is matched on the tempdir, which appears in the validator's
    /// argv and in a broker's `--home`, and in nothing else on the machine.
    fn assert_nothing_left_running(&self) {
        common::assert_no_broker_left(&self.home);
        let needle = self.tmp.path().to_string_lossy().into_owned();
        let deadline = Instant::now() + DRAIN_BUDGET;
        loop {
            let out = std::process::Command::new("ps")
                .args(["-axo", "pid=,command="])
                .output()
                .expect("ps runs");
            let left: Vec<String> = String::from_utf8_lossy(&out.stdout)
                .lines()
                .filter(|line| line.contains(&needle))
                .map(str::to_string)
                .collect();
            if left.is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "processes outlived their world {needle}: {left:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// No value that came from a source config reached any report. Run at the
    /// end of a test over everything every step printed, plus the ledger.
    fn assert_nothing_leaked(&self) {
        let mut needles = fixture_secrets("claude.json");
        needles.extend(fixture_secrets("claude-revenium.json"));
        needles.extend(CONFIG_SECRETS.iter().map(|s| s.to_string()));
        let ledger = self.manifest_text();
        let transcript = self.transcript.borrow();
        for secret in needles {
            assert!(
                !ledger.contains(&secret),
                "sync-manifest.json leaked a {}-char value",
                secret.len()
            );
            for (i, printed) in transcript.iter().enumerate() {
                assert!(
                    !printed.contains(&secret),
                    "step {i} leaked a {}-char value",
                    secret.len()
                );
            }
        }
    }
}

/// Copies a tree keeping symlinks as symlinks and modes as modes — `portable`
/// must still be a link and `bin/run.sh` must still be executable, or the
/// fixture no longer has the shape these tests are about.
fn copy_preserving(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let (from, to) = (e.path(), dst.join(e.file_name()));
        let meta = std::fs::symlink_metadata(&from).unwrap();
        if meta.file_type().is_symlink() {
            std::os::unix::fs::symlink(std::fs::read_link(&from).unwrap(), &to).unwrap();
        } else if meta.is_dir() {
            copy_preserving(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap(); // preserves the mode bits
        }
    }
}

fn envelope(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout must be one JSON envelope ({e}): {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}
fn data(out: &Output) -> Value {
    envelope(out)["data"].clone()
}
fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}
fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
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
        .unwrap_or_else(|| panic!("no entry for {name}: {data}"))["detail"]
        .as_str()
        .unwrap()
        .to_string()
}
/// `name -> (transport.type, auth_status)` as codex itself reported them.
fn validated(data: &Value) -> BTreeMap<String, (String, String)> {
    data["validated"]
        .as_array()
        .unwrap_or_else(|| panic!("codex validated nothing: {data}"))
        .iter()
        .map(|v| {
            (
                v["name"].as_str().unwrap().to_string(),
                (
                    v["transport_type"].as_str().unwrap().to_string(),
                    v["auth_status"].as_str().unwrap().to_string(),
                ),
            )
        })
        .collect()
}
fn check<'a>(v: &'a Value, id: &str) -> &'a Value {
    v["data"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == id)
        .unwrap_or_else(|| panic!("no check {id}: {v}"))
}
fn detail(v: &Value, id: &str) -> String {
    check(v, id)["detail"].as_str().unwrap().to_string()
}

/// Every value in a fixture the secret policy is about, enumerated from the raw
/// JSON — so a newly added fixture secret is covered by the sweeps here
/// automatically.
fn fixture_secrets(name: &str) -> Vec<String> {
    let raw: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture_root().join(name)).unwrap()).unwrap();
    let mut out = Vec::new();
    for (_, server) in raw["mcpServers"].as_object().unwrap() {
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

fn counts(synced: u64, unchanged: u64, removed: u64, skipped: u64, refused: u64) -> Value {
    json!({
        "synced": synced,
        "unchanged": unchanged,
        "removed": removed,
        "skipped": skipped,
        "refused": refused,
    })
}

/// What the fixture Claude config classifies to on a first run against a
/// git-TRACKED `config.toml` with a foreign `[mcp_servers.mx]` already in it.
fn first_mcp_run() -> BTreeMap<String, String> {
    BTreeMap::from(
        [
            ("Neon", "synced"),
            ("XcodeBuildMCP", "synced"),
            ("akiflow", "synced"),
            ("atlassian", "skipped-secret"),
            ("codegraph", "synced"),
            ("ghl", "skipped-untranslatable"),
            ("google-workspace", "skipped-secret"),
            ("hq", "synced"),
            ("linear", "synced"),
            ("malformed", "skipped-untranslatable"),
            ("mx", "skipped-unowned"),
            ("telegram", "skipped-secret"),
        ]
        .map(|(a, b)| (a.to_string(), b.to_string())),
    )
}

/// What the fixture skills tree classifies to, every run.
fn skills_run(managed: &str) -> BTreeMap<String, String> {
    BTreeMap::from(
        [
            ("README.md", "skipped-not-a-skill"),
            ("devloop", "skipped-collision"),
            ("portable", managed),
            ("provisioner", "skipped-side-effecting"),
            ("scratch", "skipped-not-a-skill"),
            ("slack-cli", "skipped-side-effecting"),
            ("techniques", managed),
            ("wordy", managed),
        ]
        .map(|(a, b)| (a.to_string(), b.to_string())),
    )
}

// ---------------------------------------------------------------------------
// The pipeline.
// ---------------------------------------------------------------------------

/// Both kinds, one codex home, twice: dry run, apply, migrate, diagnose, repeat
/// — and at the end each repository's git-visible change set is exactly the two
/// things a2a is allowed to have made.
#[test]
fn the_pipeline_syncs_both_kinds_into_one_home_and_converges() {
    let w = World::new();

    // (1) `sync skills --dry-run` decides everything and writes nothing.
    let before = hash_path(&w.codex_home).unwrap();
    let dry = w.step(&["--json", "sync", "skills", "--dry-run"]);
    assert_eq!(dry["dry_run"], true);
    assert_eq!(dry["counts"], counts(3, 0, 0, 5, 0));
    assert_eq!(reported(&dry), skills_run("synced"));
    assert_eq!(
        hash_path(&w.codex_home).unwrap(),
        before,
        "a dry run writes nothing anywhere in the codex home"
    );
    assert!(!w.manifest_path().exists(), "and records nothing");

    // (2) `sync skills` — the same plan, carried out.
    let skills = w.step(&["--json", "sync", "skills"]);
    assert_eq!(skills["account"], SKILL_ACCOUNT);
    assert_eq!(skills["counts"], counts(3, 0, 0, 5, 0));
    assert_eq!(reported(&skills), skills_run("synced"));
    for name in MANAGED_SKILLS {
        assert!(w.dest().join(name).is_dir(), "{name} landed");
    }
    assert_eq!(
        std::fs::read(w.dest().join("devloop/SKILL.md")).unwrap(),
        CURATED_DEVLOOP,
        "the curated port is untouched, bytes and all"
    );

    // (3) `sync mcp --dry-run` — same discipline on the other kind.
    let before = w.config_text();
    let dry = w.step(&["--json", "sync", "mcp", "--dry-run"]);
    assert_eq!(dry["account"], ACCOUNT);
    assert_eq!(dry["counts"], counts(6, 0, 0, 6, 0));
    assert_eq!(reported(&dry), first_mcp_run());
    assert_eq!(w.config_text(), before, "the target is untouched");

    // (4) `sync mcp` — the tracked config.toml blocks the credential-carrying
    //     stdio servers, loudly, and writes everything else.
    let mcp = w.step(&["--json", "sync", "mcp"]);
    assert_eq!(reported(&mcp), first_mcp_run());
    for name in BLOCKED_UNTIL_MIGRATED {
        assert_eq!(reported(&mcp)[name], "skipped-secret", "{name}");
        assert!(
            detail_of(&mcp, name).contains("a2a sync mcp --migrate-untrack"),
            "the block names its own way out: {}",
            detail_of(&mcp, name)
        );
        assert!(
            !w.dest_servers().contains(&name.to_string()),
            "{name} has no destination table at all"
        );
    }
    // The write was validated by the real binary, within the step budget, and
    // against a COPY: `codex mcp list` creates `<CODEX_HOME>/tmp/`, which would
    // otherwise show up in the containment assertion at the end.
    let validated = validated(&mcp);
    assert_eq!(
        validated["Neon"],
        ("streamable_http".to_string(), "bearer_token".to_string())
    );
    assert_eq!(
        validated["codegraph"],
        ("stdio".to_string(), "unsupported".to_string())
    );
    assert!(!w.codex_home.join("tmp").exists());

    // (5) `--migrate-untrack` takes config.toml out of git and the SAME run
    //     writes what the tracking was blocking.
    let migrated = w.step(&["--json", "sync", "mcp", "--migrate-untrack"]);
    assert_eq!(migrated["migration"]["performed"], json!(true));
    assert_eq!(migrated["migration"]["untracked"], json!(true));
    assert_eq!(migrated["counts"], counts(3, 6, 0, 3, 0));
    for name in BLOCKED_UNTIL_MIGRATED {
        assert_eq!(reported(&migrated)[name], "synced", "{name}");
        assert!(w.dest_servers().contains(&name.to_string()), "{name}");
    }
    assert!(!w.tracks_config(), "git no longer tracks config.toml");
    assert!(w.config().exists(), "`--cached`: the working file survives");

    // (6) `doctor` mid-pipeline: seventeen checks, and the two this phase added
    //     that a completed pipeline must satisfy.
    let (v, code) = w.doctor();
    let ids: Vec<&str> = v["data"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, CHECK_IDS, "the published check list, in report order");
    assert_eq!(code, 0, "nothing published, nothing fatal: {v}");
    assert_eq!(check(&v, "codex_config_secrets")["status"], "ok");
    assert!(
        detail(&v, "codex_config_secrets").contains("which git does not track"),
        "{}",
        detail(&v, "codex_config_secrets")
    );
    assert_eq!(check(&v, "sync_state")["status"], "ok");
    assert!(
        detail(&v, "sync_state").contains("every destination as a2a left it"),
        "{}",
        detail(&v, "sync_state")
    );
    let payload = v.to_string();
    for secret in CONFIG_SECRETS {
        assert!(!payload.contains(secret), "doctor named a value: {secret}");
    }

    // (7) Both re-runs are no-ops: everything a2a owns is `unchanged`, and not
    //     one byte of either destination is rewritten.
    let skills_tree = hash_path(&w.dest()).unwrap();
    let ignore_block = w.gitignore_text();
    let config_bytes = std::fs::read(w.config()).unwrap();
    let config_mtime = std::fs::metadata(w.config()).unwrap().modified().unwrap();

    let again = w.step(&["--json", "sync", "skills"]);
    assert_eq!(again["counts"], counts(0, 3, 0, 5, 0));
    assert_eq!(reported(&again), skills_run("unchanged"));
    let again = w.step(&["--json", "sync", "mcp"]);
    assert_eq!(again["counts"], counts(0, 9, 0, 3, 0));
    assert_eq!(
        hash_path(&w.dest()).unwrap(),
        skills_tree,
        "a second skills run writes no content"
    );
    assert_eq!(w.gitignore_text(), ignore_block, "nor a new ignore block");
    assert_eq!(
        std::fs::read(w.config()).unwrap(),
        config_bytes,
        "same bytes"
    );
    assert_eq!(
        std::fs::metadata(w.config()).unwrap().modified().unwrap(),
        config_mtime,
        "and config.toml was not even rewritten"
    );

    // (8) Containment, as EXACT sets in both repositories.
    let ignore = w.gitignore_text();
    for name in MANAGED_SKILLS {
        assert!(
            ignore.contains(&format!("/{name}/\n")),
            "the managed block covers {name}: {ignore}"
        );
        assert!(
            !w.skills_status()
                .iter()
                .any(|line| line.contains(&format!("{name}/"))),
            "nothing under a managed skill directory is visible to git: {:?}",
            w.skills_status()
        );
    }
    assert_eq!(
        w.skills_status(),
        BTreeSet::from([" M .gitignore".to_string()]),
        "the block this run wrote is the ONLY thing the skills repository sees \
         — a tracked .gitignore cannot ignore itself, so `empty` is unachievable"
    );
    assert_eq!(
        w.codex_status(),
        BTreeSet::from(["D  config.toml".to_string(), " M .gitignore".to_string()]),
        "the migration's staged index removal, plus the ignore line it appended"
    );

    w.assert_nothing_leaked();
    w.assert_nothing_left_running();
}

// ---------------------------------------------------------------------------
// Two accounts, one destination.
// ---------------------------------------------------------------------------

/// The one case where two legitimate sources contend for the same destination
/// table. It must not ping-pong: the account that synced a name first owns it,
/// the other is told so by name, and neither account's ledger entries are the
/// other's.
#[test]
fn two_accounts_share_one_config_toml_and_the_first_owner_wins() {
    let w = World::new();
    let first = w.step(&["--json", "sync", "mcp"]);
    assert_eq!(first["account"], ACCOUNT);
    assert_eq!(reported(&first)["Neon"], "synced");

    // The flag outranks the environment, which still points at the first
    // account — the second account is chosen for this run only.
    let revenium = w.revenium_dir.to_string_lossy().into_owned();
    let second = w.step(&["--json", "sync", "mcp", "--claude-config-dir", &revenium]);
    assert_eq!(second["account"], SECOND_ACCOUNT);
    assert_eq!(
        reported(&second),
        BTreeMap::from(
            [
                ("Neon", "skipped-unowned"),
                ("atlassian", "skipped-secret"),
                ("context-lake", "synced"),
                ("ghl", "skipped-untranslatable"),
                ("linear", "skipped-unowned"),
                ("mx", "skipped-unowned"),
            ]
            .map(|(a, b)| (a.to_string(), b.to_string())),
        )
    );

    // The detail NAMES the owner, resolved through the ledger rather than by
    // taking a manifest key apart.
    assert_eq!(
        w.manifest().owner_account(Kind::Mcp, "Neon"),
        Some(ACCOUNT),
        "the query the report line is composed from"
    );
    let owned = detail_of(&second, "Neon");
    assert!(
        owned.contains(&format!("`{ACCOUNT}`")),
        "the owning account is named: {owned}"
    );
    assert!(
        !owned.contains(SECOND_ACCOUNT),
        "and it is the OTHER account, not this one: {owned}"
    );
    // A table a2a never wrote has no owner to name, and says so differently.
    let foreign = detail_of(&second, "mx");
    assert!(
        foreign.contains("foreign") && !foreign.contains('`'),
        "an unowned table nobody synced names no account: {foreign}"
    );

    // Disjoint key sets, both accounts present, and the first account's tables
    // still standing.
    let keys = w.manifest_keys();
    assert!(keys.contains(&"mcp:claude:Neon".to_string()), "{keys:?}");
    assert!(
        keys.contains(&"mcp:claude-revenium:context-lake".to_string()),
        "{keys:?}"
    );
    assert!(
        !keys.contains(&"mcp:claude-revenium:Neon".to_string()),
        "the second account never adopted the first's table: {keys:?}"
    );
    let (mine, theirs): (Vec<&String>, Vec<&String>) = keys
        .iter()
        .filter(|k| k.starts_with("mcp:"))
        .partition(|k| k.starts_with(&format!("mcp:{ACCOUNT}:")));
    assert!(!mine.is_empty() && !theirs.is_empty());
    assert!(
        theirs
            .iter()
            .all(|k| k.starts_with(&format!("mcp:{SECOND_ACCOUNT}:"))),
        "every mcp key belongs to one account or the other: {keys:?}"
    );

    let servers = w.dest_servers();
    for name in [
        "Neon",
        "linear",
        "hq",
        "akiflow",
        "codegraph",
        "context-lake",
    ] {
        assert!(
            servers.contains(&name.to_string()),
            "{name} survived the second account's run: {servers:?}"
        );
    }
    assert!(
        w.config_text().contains("# mx exists on BOTH sides"),
        "and the foreign table kept its comment"
    );

    w.assert_nothing_leaked();
    w.assert_nothing_left_running();
}

/// Skills land flat in one shared tree under ONE account label, so a second
/// account's run must leave the managed block covering the first account's
/// directories — the regression `managed_dirs`' dropped `account` parameter
/// exists to prevent.
#[test]
fn a_second_account_leaves_the_first_accounts_skills_covered() {
    let w = World::new();
    let first = w.step(&["--json", "sync", "skills"]);
    assert_eq!(first["counts"], counts(3, 0, 0, 5, 0));
    let block_after_first = w.gitignore_text();
    let dirs: Vec<String> = MANAGED_SKILLS.iter().map(|s| s.to_string()).collect();
    assert!(skills::block_covers(&block_after_first, &dirs));

    let revenium = w.revenium_dir.to_string_lossy().into_owned();
    let second = w.step(&["--json", "sync", "skills", "--claude-config-dir", &revenium]);
    assert_eq!(
        second["account"], SKILL_ACCOUNT,
        "a skill entry carries one account label whichever directory it came from"
    );
    assert_eq!(
        second["counts"],
        counts(0, 3, 0, 5, 0),
        "the second account meets its own copies, not a parallel set"
    );
    assert_eq!(reported(&second), skills_run("unchanged"));

    assert_eq!(
        w.gitignore_text(),
        block_after_first,
        "the managed block still covers the first run's directories, byte for byte"
    );
    assert!(skills::block_covers(&w.gitignore_text(), &dirs));
    assert_eq!(
        w.manifest_keys(),
        MANAGED_SKILLS
            .iter()
            .map(|n| format!("skill:{SKILL_ACCOUNT}:{n}"))
            .collect::<Vec<_>>(),
        "and no parallel key set was created"
    );
    assert_eq!(
        w.skills_status(),
        BTreeSet::from([" M .gitignore".to_string()])
    );

    w.assert_nothing_leaked();
    w.assert_nothing_left_running();
}

// ---------------------------------------------------------------------------
// The load-bearing property of a manifest-only tool.
// ---------------------------------------------------------------------------

/// Delete the ledger and a2a forgets it owns anything — which is the SAFE
/// direction. Every destination becomes somebody else's, nothing is overwritten,
/// and the user's content is exactly where it was. Losing the ledger costs the
/// ability to update, never the content.
#[test]
fn losing_the_ledger_costs_the_ability_to_update_never_the_content() {
    let w = World::new();
    w.step(&["--json", "sync", "skills"]);
    w.step(&["--json", "sync", "mcp", "--migrate-untrack"]);
    // Per managed directory rather than over the whole destination tree: the
    // run below legitimately rewrites the `.gitignore` that lives beside them
    // (see the last assertion), and a tree hash would fold that into the
    // content claim this test is making.
    let skills_before = w.skill_hashes();
    let config_bytes = std::fs::read(w.config()).unwrap();
    let servers_before = w.dest_servers();

    std::fs::remove_file(w.manifest_path()).unwrap();

    let skills = w.step(&["--json", "sync", "skills"]);
    assert_eq!(
        reported(&skills),
        skills_run("skipped-collision"),
        "every managed destination is now somebody else's"
    );
    assert_eq!(skills["counts"], counts(0, 0, 0, 8, 0));

    let mcp = w.step(&["--json", "sync", "mcp"]);
    let mut expected = first_mcp_run();
    for name in [
        "Neon",
        "XcodeBuildMCP",
        "akiflow",
        "codegraph",
        "hq",
        "linear",
    ]
    .into_iter()
    .chain(BLOCKED_UNTIL_MIGRATED)
    {
        expected.insert(name.to_string(), "skipped-unowned".to_string());
    }
    assert_eq!(reported(&mcp), expected);
    assert_eq!(mcp["counts"], counts(0, 0, 0, 12, 0));

    // Nothing was overwritten and nothing was removed.
    assert_eq!(
        w.skill_hashes(),
        skills_before,
        "not one byte of skill content moved — including the curated port"
    );
    assert_eq!(std::fs::read(w.config()).unwrap(), config_bytes);
    assert_eq!(w.dest_servers(), servers_before);
    assert!(
        w.manifest().entries.is_empty(),
        "and a2a re-adopted nothing: what it cannot prove it wrote, it does not claim"
    );

    // The one consequence that IS visible: a2a stops covering directories it can
    // no longer prove it owns, so the skills repository sees them again. That is
    // the honest reading — an ignore block is a claim of ownership — and it is
    // recoverable by hand, unlike an overwrite.
    assert!(
        !skills::block_covers(&w.gitignore_text(), &["portable".to_string()]),
        "{}",
        w.gitignore_text()
    );
    let mut uncovered = BTreeSet::from([" M .gitignore".to_string()]);
    uncovered.extend(MANAGED_SKILLS.iter().map(|n| format!("?? {n}/")));
    assert_eq!(w.skills_status(), uncovered);

    w.assert_nothing_leaked();
    w.assert_nothing_left_running();
}

// ---------------------------------------------------------------------------
// Deletion, across both kinds.
// ---------------------------------------------------------------------------

/// A source that lost a skill and a server: both destinations go, both ledger
/// entries go, and everything else — including the curated port and the foreign
/// table — stays exactly where it was.
#[test]
fn a_deleted_skill_and_a_deleted_server_are_both_propagated() {
    let w = World::new();
    w.step(&["--json", "sync", "skills"]);
    w.step(&["--json", "sync", "mcp", "--migrate-untrack"]);
    assert!(w.dest().join("wordy").is_dir());
    assert!(w.dest_servers().contains(&"hq".to_string()));

    std::fs::remove_dir_all(w.source().join("wordy")).unwrap();
    let claude_dir = w.claude_dir.clone();
    w.edit_source(&claude_dir, |v| {
        v["mcpServers"].as_object_mut().unwrap().remove("hq");
    });

    let skills = w.step(&["--json", "sync", "skills"]);
    assert_eq!(reported(&skills)["wordy"], "removed");
    assert_eq!(skills["counts"], counts(0, 2, 1, 5, 0));
    let mcp = w.step(&["--json", "sync", "mcp"]);
    assert_eq!(reported(&mcp)["hq"], "removed");
    assert_eq!(mcp["counts"], counts(0, 8, 1, 3, 0));

    // Both destinations gone, both ledger entries dropped.
    assert!(!w.dest().join("wordy").exists());
    assert!(!w.dest_servers().contains(&"hq".to_string()));
    let keys = w.manifest_keys();
    assert!(
        !keys.contains(&"skill:default:wordy".to_string()),
        "{keys:?}"
    );
    assert!(!keys.contains(&"mcp:claude:hq".to_string()), "{keys:?}");
    // The ignore block still names `wordy`, and that is the containment ordering
    // working as designed: the block is written BEFORE any content, off a ledger
    // that still owned the directory, so a removal's line is dropped by the NEXT
    // run. Over-covering is harmless; under-covering is the failure the ordering
    // exists to prevent.
    assert!(w.gitignore_text().contains("/wordy/"));
    w.step(&["--json", "sync", "skills"]);
    assert!(
        !w.gitignore_text().contains("/wordy/"),
        "and the following run stops covering a directory a2a no longer owns: {}",
        w.gitignore_text()
    );

    // Everything else is untouched.
    for name in ["portable", "techniques"] {
        assert!(w.dest().join(name).is_dir(), "{name} survived");
        assert!(keys.contains(&format!("skill:default:{name}")), "{keys:?}");
    }
    assert_eq!(
        std::fs::read(w.dest().join("devloop/SKILL.md")).unwrap(),
        CURATED_DEVLOOP
    );
    for name in ["Neon", "linear", "atlassian"] {
        assert!(w.dest_servers().contains(&name.to_string()), "{name}");
    }
    assert!(w.config_text().contains("# mx exists on BOTH sides"));

    // And both repositories still see exactly what they saw before.
    assert_eq!(
        w.skills_status(),
        BTreeSet::from([" M .gitignore".to_string()])
    );
    assert_eq!(
        w.codex_status(),
        BTreeSet::from(["D  config.toml".to_string(), " M .gitignore".to_string()])
    );

    w.assert_nothing_leaked();
    w.assert_nothing_left_running();
}
