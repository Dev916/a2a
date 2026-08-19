//! `a2a sync mcp --migrate-untrack` — taking `config.toml` out of git on
//! purpose, and the order that makes it defensible.
//!
//! The migration is the one thing in this phase that mutates somebody's git
//! state, so almost every test below is about *when* it is allowed to happen
//! rather than about what it produces. Two rules carry the file:
//!
//! **Nothing is mutated until every static precondition has passed.** A run
//! that un-tracked `config.toml`, edited `.gitignore` and printed a rotation
//! reminder before discovering there is no `~/.claude.json` would have made a
//! git-visible change on its way to a "precondition failed" verdict. Exit 9 is
//! reserved for static preconditions, and "static" has to mean it.
//!
//! **The reminder is value-free.** Untracking removes a file from the index; it
//! does not un-publish a key that was already pushed. So the run says so, names
//! the lines that carried a secret shape — and never the values on them.
//!
//! Every fixture repository is created under a `tempfile::TempDir` with a
//! nonexistent `GIT_CONFIG_GLOBAL`/`GIT_CONFIG_SYSTEM`, and every command is
//! built by `common::sync_a2a`, so neither the developer's real `~/.codex` nor
//! their gitconfig can reach any of it.

mod common;

use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Output;

/// The codex home's hand-written, git-tracked `config.toml`: an untouched table
/// a2a must preserve, the live `mx` collision, and **two secret-shaped lines**
/// (14 and 18) so the rotation reminder has something true to report.
///
/// Task 7's `codex_config_secrets` doctor check is specified against this exact
/// shape — tracked, secret-carrying, flipping to `ok` once this command has
/// run — so the const is deliberately reusable rather than minimal.
const MIGRATE_CONFIG: &str = r#"# The codex home's own config.toml, hand written, and tracked by git.
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

/// The 1-based lines of [`MIGRATE_CONFIG`] that `redact::scan_text` finds, in
/// the order the reminder names them. Pinned rather than computed: the point of
/// the AC is that the numbers a user reads are the numbers in their file.
const SECRET_LINES: [usize; 2] = [14, 18];

/// The two literals on those lines. Neither may appear in stdout, stderr or the
/// ledger, in any mode.
const CONFIG_SECRETS: [&str; 2] = [
    "Bearer FAKEcontext7keyFAKEcontext7key",
    "ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
];

/// Verbatim, from the AC: the phrase that makes the reminder searchable.
const ROTATION_PHRASE: &str = "already-published secrets must still be rotated";

/// Verbatim, from the AC: the comment the ignore entry lands under.
const GITIGNORE_MARKER: &str = "# a2a: untracked by `a2a sync mcp --migrate-untrack`";

/// A `.gitignore` the fixture commits, so the post-migration `git status` shape
/// is ` M .gitignore` rather than `?? .gitignore`. Both are exercised.
const COMMITTED_GITIGNORE: &str = "auth.json\nhistory.jsonl\nsessions/\n";

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sync")
        .join(name)
}

/// How the fixture's codex home relates to git.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Repo {
    /// `git init` + `config.toml` committed — the state the whole secret policy
    /// turns on, and the one the migration exists for.
    Tracked,
    /// `git init`, `config.toml` present and never added — the success-shaped
    /// "already untracked" condition.
    Untracked,
    /// No repository at all — the one condition that is a refusal.
    Absent,
}

struct World {
    tmp: tempfile::TempDir,
    home: PathBuf,
    codex_home: PathBuf,
    claude_dir: PathBuf,
}

impl World {
    /// Tracked `config.toml`, no `.gitignore` at all.
    fn new() -> World {
        World::build(Repo::Tracked, None)
    }
    /// Tracked `config.toml` plus a committed `.gitignore` that does NOT cover
    /// it.
    fn with_gitignore() -> World {
        World::build(Repo::Tracked, Some(COMMITTED_GITIGNORE))
    }
    /// Tracked `config.toml` and a committed `.gitignore` that already covers
    /// it — the idempotence case.
    fn already_ignored() -> World {
        World::build(Repo::Tracked, Some("auth.json\nconfig.toml\nsessions/\n"))
    }
    fn untracked() -> World {
        World::build(Repo::Untracked, None)
    }
    fn not_a_repo() -> World {
        World::build(Repo::Absent, None)
    }

    fn build(repo: Repo, gitignore: Option<&str>) -> World {
        let tmp = tempfile::tempdir().unwrap();
        let w = World {
            home: tmp.path().join("a2a"),
            codex_home: tmp.path().join("codex"),
            claude_dir: tmp.path().join("claude"),
            tmp,
        };
        std::fs::create_dir_all(&w.codex_home).unwrap();
        std::fs::create_dir_all(&w.claude_dir).unwrap();
        std::fs::write(w.config(), MIGRATE_CONFIG).unwrap();
        std::fs::copy(fixture("claude.json"), w.source_json()).unwrap();
        if repo == Repo::Absent {
            if let Some(body) = gitignore {
                std::fs::write(w.gitignore(), body).unwrap();
            }
            return w;
        }
        w.git(&["init", "-q", "-b", "main"]);
        let mut staged = false;
        // `config.toml` is staged BEFORE the ignore file exists, because that is
        // the only order that reproduces the state this command exists for: a
        // file git already tracks stays tracked however emphatically a later
        // `.gitignore` covers it (`git add` refuses an ignored path outright).
        if repo == Repo::Tracked {
            w.git(&["add", "config.toml"]);
            staged = true;
        }
        if let Some(body) = gitignore {
            std::fs::write(w.gitignore(), body).unwrap();
            w.git(&["add", ".gitignore"]);
            staged = true;
        }
        if staged {
            w.git(&["commit", "-qm", "codex home"]);
        }
        w
    }

    fn config(&self) -> PathBuf {
        self.codex_home.join("config.toml")
    }
    fn config_text(&self) -> String {
        std::fs::read_to_string(self.config()).unwrap()
    }
    fn gitignore(&self) -> PathBuf {
        self.codex_home.join(".gitignore")
    }
    fn gitignore_text(&self) -> String {
        std::fs::read_to_string(self.gitignore()).unwrap_or_default()
    }
    fn source_json(&self) -> PathBuf {
        self.claude_dir.join(".claude.json")
    }
    fn manifest_text(&self) -> String {
        std::fs::read_to_string(self.home.join("sync-manifest.json")).unwrap_or_default()
    }
    /// Content hash of the whole codex home, `.git` excluded (`hash_path` skips
    /// it). Paired with `git status --porcelain` this is what "a dry run
    /// performs nothing" means: neither the working tree nor the index moved.
    fn tree_hash(&self) -> String {
        a2a::sync::manifest::hash_path(&self.codex_home).unwrap()
    }
    /// `git status --porcelain` as a SET, because porcelain's ordering between
    /// index entries and untracked ones is git's business, not this AC's.
    fn status(&self) -> BTreeSet<String> {
        self.git(&["status", "--porcelain"])
            .lines()
            .map(str::to_string)
            .collect()
    }
    fn tracks_config(&self) -> bool {
        self.git(&["ls-files"]).lines().any(|l| l == "config.toml")
    }

    fn cmd(&self) -> assert_cmd::Command {
        let mut c = common::sync_a2a(
            &self.home,
            &self.codex_home,
            &self.claude_dir,
            self.tmp.path(),
        );
        // The command shells out to git, so the same hermeticity `World::git`
        // keeps applies to the binary under test: no hooks, no templates, no
        // `core.excludesFile` from the developer's machine.
        let nowhere = self.tmp.path().join("no-such-gitconfig");
        c.env("GIT_CONFIG_GLOBAL", &nowhere)
            .env("GIT_CONFIG_SYSTEM", &nowhere);
        c
    }
    fn run(&self, args: &[&str]) -> Output {
        self.cmd().args(args).output().unwrap()
    }
    /// One `--migrate-untrack` run, asserted successful, returning `data`.
    fn migrate(&self) -> Value {
        let out = self.run(&["--json", "sync", "mcp", "--migrate-untrack"]);
        assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
        data(&out)
    }

    /// Hermetic git, per Global Constraints.
    fn git(&self, args: &[&str]) -> String {
        let nowhere = self.tmp.path().join("no-such-gitconfig");
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(&self.codex_home)
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
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
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
fn migration(out: &Output) -> Value {
    data(out)["migration"].clone()
}
fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}
fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}
fn error_code(out: &Output) -> String {
    envelope(out)["error"]["code"].as_str().unwrap().to_string()
}
fn error_message(out: &Output) -> String {
    envelope(out)["error"]["message"]
        .as_str()
        .unwrap()
        .to_string()
}
fn reported(data: &Value) -> std::collections::BTreeMap<String, String> {
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

/// Every literal in a Claude fixture that no report may echo — the same sweep
/// `tests/sync_mcp.rs` runs, so a newly added fixture secret is covered here
/// automatically.
fn fixture_secrets(name: &str) -> Vec<String> {
    let raw: Value =
        serde_json::from_str(&std::fs::read_to_string(fixture(name)).unwrap()).unwrap();
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
    out.extend(CONFIG_SECRETS.iter().map(|s| s.to_string()));
    out
}

/// The fixture really does carry the shapes this file's line numbers claim —
/// a guard against somebody editing the const above and silently invalidating
/// every reminder assertion below.
#[test]
fn the_fixture_config_carries_secret_shapes_on_the_pinned_lines() {
    let lines: Vec<&str> = MIGRATE_CONFIG.lines().collect();
    assert!(lines[SECRET_LINES[0] - 1].contains(CONFIG_SECRETS[0]));
    assert!(lines[SECRET_LINES[1] - 1].contains(CONFIG_SECRETS[1]));
    let hits: Vec<usize> = a2a::redact::scan_text(MIGRATE_CONFIG)
        .into_iter()
        .map(|h| h.line)
        .collect::<BTreeSet<usize>>()
        .into_iter()
        .collect();
    assert_eq!(hits, SECRET_LINES.to_vec(), "1-based, deduplicated, sorted");
}

// ---------------------------------------------------------------------------
// Preconditions, all of them, before a single git-visible byte moves.
// ---------------------------------------------------------------------------

/// A missing Claude source is a static precondition, so it must be decided
/// BEFORE the migration — otherwise exit 9 arrives after somebody's index has
/// already been rewritten.
#[test]
fn a_missing_claude_source_exits_nine_before_any_git_mutation() {
    let w = World::new();
    std::fs::remove_file(w.source_json()).unwrap();
    let before = w.status();

    let out = w.run(&["--json", "sync", "mcp", "--migrate-untrack"]);
    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(error_code(&out), "SYNC_CLAUDE_CONFIG_MISSING");

    assert!(w.tracks_config(), "git still tracks config.toml");
    assert_eq!(w.status(), before, "no index or worktree change");
    assert!(!w.gitignore().exists(), "no .gitignore was created");
    assert_eq!(w.config_text(), MIGRATE_CONFIG);
}

/// The same ordering, with the other unreadable-source verdict and a
/// `.gitignore` that must come back byte-identical.
#[test]
fn an_unparseable_claude_source_exits_nine_before_any_git_mutation() {
    let w = World::with_gitignore();
    std::fs::write(w.source_json(), "{ \"mcpServers\": ").unwrap();
    let before = w.status();

    let out = w.run(&["--json", "sync", "mcp", "--migrate-untrack"]);
    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(error_code(&out), "SYNC_CLAUDE_CONFIG_UNREADABLE");

    assert!(w.tracks_config());
    assert_eq!(w.status(), before);
    assert_eq!(
        w.gitignore_text(),
        COMMITTED_GITIGNORE,
        ".gitignore is byte-identical"
    );
}

/// "Not a git repository" and "already untracked" are different facts and get
/// different treatment: the first is a refusal, the second is a no-op. The
/// message has to say which one happened.
#[test]
fn a_codex_home_that_is_not_a_repository_refuses_with_a_distinguishing_detail() {
    let w = World::not_a_repo();
    let out = w.run(&["--json", "sync", "mcp", "--migrate-untrack"]);

    assert_eq!(out.status.code(), Some(9), "{}", stderr(&out));
    assert_eq!(error_code(&out), "SYNC_MIGRATE_NOT_APPLICABLE");
    let msg = error_message(&out);
    assert!(
        msg.contains("not a git repository"),
        "the detail names WHICH condition: {msg}"
    );
    assert!(
        !msg.contains("already untracked"),
        "and not the other one: {msg}"
    );
    assert!(msg.contains(&*w.codex_home.to_string_lossy()));
    assert_eq!(w.config_text(), MIGRATE_CONFIG, "nothing was touched");
    assert!(!w.gitignore().exists());
}

// ---------------------------------------------------------------------------
// The migration itself.
// ---------------------------------------------------------------------------

/// A dry run performs nothing at all: it prints the two git-visible changes it
/// would make and the rotation reminder, and the repository is bit-for-bit
/// where it was.
#[test]
fn dry_run_performs_nothing_and_prints_both_git_visible_changes() {
    let w = World::new();
    let (before_status, before_tree) = (w.status(), w.tree_hash());

    let out = w.run(&["--json", "sync", "mcp", "--migrate-untrack", "--dry-run"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let m = migration(&out);
    assert_eq!(m["performed"], json!(false));
    assert_eq!(m["untracked"], json!(false), "still tracked");
    assert_eq!(m["gitignore_appended"], json!(false));
    assert_eq!(m["secret_lines"], json!(SECRET_LINES.to_vec()));
    assert!(m["rotation_reminder"]
        .as_str()
        .unwrap()
        .contains(ROTATION_PHRASE));

    let err = stderr(&out);
    assert!(err.contains("rm --cached"), "the argv is printed: {err}");
    assert!(
        err.contains(&*w.gitignore().to_string_lossy()),
        "the .gitignore change is printed: {err}"
    );
    assert!(err.contains(GITIGNORE_MARKER), "{err}");
    assert!(err.contains(ROTATION_PHRASE), "{err}");
    assert!(!err.contains(" -f"), "`git rm` is never forced: {err}");

    assert!(w.tracks_config(), "still tracked");
    assert_eq!(w.status(), before_status);
    assert_eq!(w.tree_hash(), before_tree);
    assert!(!w.gitignore().exists());
}

/// The whole point, end to end: untrack, ignore, remind — and then keep going
/// into the sync, so the stdio servers the tracked file was blocking are
/// written in the SAME run.
#[test]
fn the_migration_untracks_then_the_same_run_writes_the_unblocked_stdio_literals() {
    let w = World::new();
    let out = w.run(&["--json", "sync", "mcp", "--migrate-untrack"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));

    let m = migration(&out);
    assert_eq!(m["performed"], json!(true));
    assert_eq!(m["untracked"], json!(true));
    assert_eq!(m["gitignore_appended"], json!(true));
    assert_eq!(m["secret_lines"], json!(SECRET_LINES.to_vec()));

    assert!(!w.tracks_config(), "git no longer tracks config.toml");
    assert!(w.config().exists(), "`--cached`: the working file survives");
    assert_eq!(
        w.status(),
        BTreeSet::from(["D  config.toml".to_string(), "?? .gitignore".to_string()]),
        "exactly the staged index removal and the new .gitignore"
    );

    let ignore = w.gitignore_text();
    assert!(ignore.contains(GITIGNORE_MARKER), "{ignore}");
    assert!(
        ignore.lines().any(|l| l.trim() == "config.toml"),
        "config.toml is its own line: {ignore}"
    );

    // The follow-on sync: previously `skipped-secret`, now written.
    let reported = reported(&data(&out));
    for name in ["atlassian", "google-workspace", "telegram"] {
        assert_eq!(
            reported[name], "synced",
            "{name} is writable once untracked"
        );
    }
    let config = w.config_text();
    assert!(
        config.contains("ghp_000000000000000000000000000000000000"),
        "the literal env value lands in a file git does not publish: {config}"
    );
    assert!(
        config.contains("[mcp_servers.context7]"),
        "an unowned table is preserved: {config}"
    );

    // …and nothing leaked on the way.
    for secret in fixture_secrets("claude.json") {
        for (what, text) in [
            ("stdout", stdout(&out)),
            ("stderr", stderr(&out)),
            ("sync-manifest.json", w.manifest_text()),
        ] {
            assert!(!text.contains(secret.as_str()), "{what} leaked {secret}");
        }
    }
}

/// The other fixture shape: a committed `.gitignore` becomes a modification
/// rather than an untracked file, and the status is still exactly two entries.
#[test]
fn a_committed_gitignore_is_modified_and_the_status_is_still_exactly_two_entries() {
    let w = World::with_gitignore();
    let m = w.migrate();

    assert_eq!(m["migration"]["gitignore_appended"], json!(true));
    assert_eq!(
        w.status(),
        BTreeSet::from(["D  config.toml".to_string(), " M .gitignore".to_string()])
    );
    let ignore = w.gitignore_text();
    assert!(
        ignore.starts_with(COMMITTED_GITIGNORE),
        "the human's lines are kept, the block is appended: {ignore}"
    );
    assert!(ignore.contains(GITIGNORE_MARKER));
}

/// `git rm --cached`, not `git rm`: with nothing for the sync to write, the
/// working file must come back byte-identical.
#[test]
fn the_working_config_toml_survives_the_migration_byte_identical() {
    let w = World::new();
    // A Claude config with no servers at all is a normal config, and it makes
    // the migration the only thing this run does.
    std::fs::write(w.source_json(), "{\"numStartups\": 1}").unwrap();

    let m = w.migrate();
    assert_eq!(m["migration"]["performed"], json!(true));
    assert_eq!(
        w.config_text(),
        MIGRATE_CONFIG,
        "the working file is untouched by the migration"
    );
    assert!(!w.tracks_config());
    assert_eq!(
        w.status(),
        BTreeSet::from(["D  config.toml".to_string(), "?? .gitignore".to_string()])
    );
}

/// Re-running the migration is a no-op, not an error — the second run finds an
/// already-untracked file and walks straight into the sync.
#[test]
fn a_second_migrate_run_is_a_no_op_and_the_sync_still_proceeds() {
    let w = World::new();
    let first = w.migrate();
    assert_eq!(first["migration"]["performed"], json!(true));
    let after_first = w.status();

    let second = w.migrate();
    assert_eq!(second["migration"]["performed"], json!(false));
    assert_eq!(second["migration"]["untracked"], json!(true));
    assert_eq!(second["migration"]["gitignore_appended"], json!(false));
    assert_eq!(
        second["counts"]["synced"],
        json!(0),
        "the second run has nothing left to write"
    );
    assert_eq!(w.status(), after_first, "no new git state");
}

/// A codex home whose `config.toml` was never tracked needs no migration at
/// all, and the run must not fail over it: it syncs.
#[test]
fn an_already_untracked_target_needs_no_migration_and_syncs() {
    let w = World::untracked();
    let d = w.migrate();

    assert_eq!(d["migration"]["performed"], json!(false));
    assert_eq!(d["migration"]["untracked"], json!(true));
    assert!(
        d["counts"]["synced"].as_u64().unwrap() > 0,
        "the sync ran: {d}"
    );
    assert_eq!(reported(&d)["atlassian"], "synced");
    assert!(!w.gitignore().exists(), "no git call, no .gitignore");
}

/// A `.gitignore` that already covers `config.toml` is left alone — the block
/// is appended once, ever.
#[test]
fn a_gitignore_that_already_covers_config_toml_is_not_appended_twice() {
    let w = World::already_ignored();
    let before = w.gitignore_text();

    let d = w.migrate();
    assert_eq!(d["migration"]["performed"], json!(true));
    assert_eq!(d["migration"]["gitignore_appended"], json!(false));
    assert_eq!(w.gitignore_text(), before, ".gitignore is byte-identical");
    assert_eq!(
        w.gitignore_text()
            .lines()
            .filter(|l| l.trim() == "config.toml")
            .count(),
        1
    );
    assert_eq!(
        w.status(),
        BTreeSet::from(["D  config.toml".to_string()]),
        "only the index removal: the ignore file needed no change"
    );
}

// ---------------------------------------------------------------------------
// The reminder.
// ---------------------------------------------------------------------------

/// Unmissable and value-free: the file, the line numbers, the phrase — and not
/// one byte of what is on those lines.
#[test]
fn the_rotation_reminder_names_lines_never_values() {
    let w = World::new();

    // Human mode: stderr.
    let out = w.run(&["sync", "mcp", "--migrate-untrack"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let err = stderr(&out);
    assert!(err.contains(ROTATION_PHRASE), "{err}");
    assert!(err.contains(&*w.config().to_string_lossy()), "{err}");
    for line in SECRET_LINES {
        assert!(
            err.contains(&line.to_string()),
            "line {line} is named: {err}"
        );
    }
    for secret in CONFIG_SECRETS {
        assert!(!err.contains(secret), "stderr leaked {secret}");
    }

    // `--json` mode: the same sentence in the payload, on a fresh world so the
    // migration is a real one again.
    let w = World::new();
    let out = w.run(&["--json", "sync", "mcp", "--migrate-untrack"]);
    let reminder = migration(&out)["rotation_reminder"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(reminder.contains(ROTATION_PHRASE), "{reminder}");
    assert!(
        reminder.contains(&*w.config().to_string_lossy()),
        "{reminder}"
    );
    for line in SECRET_LINES {
        assert!(reminder.contains(&line.to_string()), "{reminder}");
    }
    for secret in CONFIG_SECRETS {
        assert!(!stdout(&out).contains(secret), "stdout leaked {secret}");
        assert!(!stderr(&out).contains(secret), "stderr leaked {secret}");
    }
}

/// A run that did not ask for the migration reports `migration: null` rather
/// than an empty object — "not requested" and "requested, did nothing" are
/// different facts and a caller branches on them.
#[test]
fn a_run_without_the_flag_reports_no_migration_at_all() {
    let w = World::new();
    let out = w.run(&["--json", "sync", "mcp"]);
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(migration(&out), Value::Null);
    assert!(w.tracks_config(), "and nothing was migrated");
}
