mod common;

use a2a::home::A2aHome;
use a2a::mesh::keys::{
    enroll_gateway_token, fleet_id, fleet_init, gateway_identity, gateway_public_key_b64,
    install_gateway_token, SystemClock,
};
use a2a::mesh::registry::{
    FileRegistry, Registry, WorkRow, WorkStatus, REGISTRY_SCHEMA_VERSION,
};
use a2a::mesh::work::{
    sign_envelope, verify_envelope, UnsignedWorkEnvelope, WorkAddressing, WorkRepo,
};
use a2a::mesh::work_exec::{
    self, advance_approve_until, ApproveStopAfter, GitAdapter, WorkPhase, WorkSpawner,
};
use a2a::mesh::work_gate;
use a2a::mesh::work_store::{self, WorkSource};
use a2a::output::AppError;
use chrono::{TimeZone, Utc};
use predicates::prelude::*;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

#[test]
fn pinned_git_ignores_hooks_attributes_and_external_diff() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    init_repo(&repo);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    run_git(&repo, &["add", "README.md"]);
    run_git(&repo, &["commit", "-m", "base"]);

    let hook = repo.join(".git/hooks/pre-push");
    std::fs::write(&hook, "#!/bin/sh\ntouch \"$PWD/HOOK_RAN\"\n").unwrap();
    let mut permissions = std::fs::metadata(&hook).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&hook, permissions).unwrap();

    std::fs::write(repo.join(".gitattributes"), "* diff=evil\n").unwrap();
    std::fs::write(repo.join("payload.txt"), "the real added line\n").unwrap();
    run_git(&repo, &["add", ".gitattributes", "payload.txt"]);
    run_git(&repo, &["commit", "-m", "worker result"]);
    run_git(&repo, &["config", "diff.external", "/bin/false"]);
    run_git(&repo, &["config", "diff.evil.textconv", "/bin/false"]);

    let output = work_gate::git_pinned(&repo)
        .args([
            "log",
            "--no-ext-diff",
            "--no-textconv",
            "--text",
            "-p",
            "HEAD~1..HEAD",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("+the real added line"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(!repo.join("HOOK_RAN").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn approve_records_base_sha_remote_url_and_base_ref_before_spawn() {
    let tmp = tempfile::tempdir().unwrap();
    let root = home_at(&tmp.path().join("root"));
    let sender = home_at(&tmp.path().join("sender"));
    let receiver = home_at(&tmp.path().join("receiver"));
    fleet_init(&root).unwrap();
    copy_root_pub(root.root(), sender.root());
    copy_root_pub(root.root(), receiver.root());
    let sender_gw = enroll_joiner(&root, &sender);
    let receiver_gw = enroll_joiner(&root, &receiver);
    let work_id = "work-gate-approve";
    let remote_url = "file:///verified-origin.git";
    let base_ref = "release/1.2";
    let envelope = sign_envelope(
        &sender,
        UnsignedWorkEnvelope {
            work_id: work_id.to_string(),
            session: "user:test".to_string(),
            addressing: WorkAddressing::Pool,
            repo: WorkRepo {
                remote_url: remote_url.to_string(),
                base_ref: base_ref.to_string(),
            },
            title: "Record the reviewed base".to_string(),
            brief_text: "---\nlane: impl\nacceptance_criteria:\n  - pinned base recorded\nverify:\n  - cargo test --test mesh_work_gate\n---\n# Work\n".to_string(),
            acceptance: "pinned base recorded".to_string(),
            ttl_days: 7,
            max_bytes: 262_144,
        },
        &SystemClock,
    )
    .unwrap();
    let verified = verify_envelope(&receiver, &envelope, &SystemClock).unwrap();
    work_store::store_offer(&receiver, verified, WorkSource::Pool).unwrap();

    let registry = FileRegistry::new(tmp.path().join("registry"));
    registry.init_schema().unwrap();
    registry
        .work_insert(WorkRow {
            fleet_id: fleet_id(&receiver).unwrap(),
            work_id: work_id.to_string(),
            sender_gw,
            addressing: "pool".to_string(),
            status: WorkStatus::Offered,
            claimed_by: None,
            result_ref: None,
            result_delivery: None,
            swept_by: None,
            created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            updated_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            expires_at: Utc.timestamp_opt(1_700_086_400, 0).unwrap(),
            schema_version: REGISTRY_SCHEMA_VERSION,
        })
        .await
        .unwrap();

    let interrupted = advance_approve_until(
        &receiver,
        &registry,
        &MaterializingGit,
        &NoSpawn,
        work_id,
        Some(ApproveStopAfter::Cloned),
    )
    .await
    .unwrap();
    assert!(matches!(
        interrupted,
        work_exec::ApproveOutcome::Interrupted {
            phase: WorkPhase::Spawning,
            ..
        }
    ));

    let state = work_exec::load_state(&receiver, work_id)
        .unwrap()
        .unwrap();
    let checkout_head = git_stdout(&state.checkout, &["rev-parse", "HEAD"]);
    assert_eq!(state.base_sha.as_deref(), Some(checkout_head.as_str()));
    assert_eq!(state.remote_url.as_deref(), Some(remote_url));
    assert_eq!(state.base_ref.as_deref(), Some(base_ref));
    let row = registry
        .work_get(&fleet_id(&receiver).unwrap(), work_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.claimed_by.as_deref(), Some(receiver_gw.as_str()));
}

#[test]
fn merge_commit_in_range_is_refused_and_linear_range_freezes_a_stable_digest() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("linear");
    init_repo(&repo);
    commit_text(&repo, "result.txt", "base\n", "base");
    let base_sha = git_stdout(&repo, &["rev-parse", "HEAD"]);
    commit_text(&repo, "result.txt", "base\none\n", "one");
    commit_text(&repo, "result.txt", "base\none\ntwo\n", "two");

    let first = work_gate::freeze_range(&repo, &base_sha).unwrap();
    let second = work_gate::freeze_range(&repo, &base_sha).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.base_sha, base_sha);
    assert_eq!(first.head_sha, git_stdout(&repo, &["rev-parse", "HEAD"]));
    assert_eq!(first.closure_digest.len(), 64);
    assert!(first.closure_digest.bytes().all(|byte| byte.is_ascii_hexdigit()));

    run_git(&repo, &["switch", "-c", "side"]);
    commit_text(&repo, "side.txt", "side\n", "side");
    run_git(&repo, &["switch", "main"]);
    run_git(&repo, &["merge", "--no-ff", "-s", "ours", "side", "-m", "merge side"]);
    let merge_err = work_gate::freeze_range(&repo, &base_sha).unwrap_err();
    assert_eq!(merge_err.code, "WORK_RESULT_NOT_LINEAR", "{merge_err:?}");

    let unrelated = tmp.path().join("unrelated-base");
    init_repo(&unrelated);
    commit_text(&unrelated, "main.txt", "main root\n", "main root");
    run_git(&unrelated, &["switch", "--orphan", "other"]);
    if unrelated.join("main.txt").exists() {
        std::fs::remove_file(unrelated.join("main.txt")).unwrap();
    }
    commit_text(&unrelated, "other.txt", "other root\n", "other root");
    let unrelated_root = git_stdout(&unrelated, &["rev-parse", "HEAD"]);
    run_git(&unrelated, &["switch", "main"]);
    let unrelated_err = work_gate::freeze_range(&unrelated, &unrelated_root).unwrap_err();
    assert_eq!(
        unrelated_err.code, "WORK_RESULT_NOT_LINEAR",
        "{unrelated_err:?}"
    );
}

#[test]
fn non_utf8_blob_and_diff_touching_gitattributes_are_holds() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(&tmp.path().join("home"));
    let cfg = a2a::config::load(&home).unwrap().0;

    let binary_repo = tmp.path().join("binary");
    init_repo(&binary_repo);
    commit_text(&binary_repo, "README.md", "base\n", "base");
    let binary_base = git_stdout(&binary_repo, &["rev-parse", "HEAD"]);
    std::fs::write(binary_repo.join("bad.bin"), [0xff, 0xfe, 0x00]).unwrap();
    run_git(&binary_repo, &["add", "bad.bin"]);
    run_git(&binary_repo, &["commit", "-m", "binary result"]);
    let binary_range = work_gate::freeze_range(&binary_repo, &binary_base).unwrap();
    let binary_err =
        work_gate::scan_added_blobs(&home, &cfg, &binary_repo, &binary_range).unwrap_err();
    assert_eq!(binary_err.code, "WORK_BINARY_IN_RESULT", "{binary_err:?}");
    assert!(binary_err.message.contains("bad.bin"), "{binary_err:?}");

    let attrs_repo = tmp.path().join("attrs");
    init_repo(&attrs_repo);
    commit_text(&attrs_repo, "README.md", "base\n", "base");
    let attrs_base = git_stdout(&attrs_repo, &["rev-parse", "HEAD"]);
    commit_text(
        &attrs_repo,
        ".gitattributes",
        "*.md diff=doc\n",
        "attribute result",
    );
    let attrs_range = work_gate::freeze_range(&attrs_repo, &attrs_base).unwrap();
    let attrs_err =
        work_gate::scan_added_blobs(&home, &cfg, &attrs_repo, &attrs_range).unwrap_err();
    assert_eq!(attrs_err.code, "WORK_ATTRS_IN_RESULT", "{attrs_err:?}");
    assert!(attrs_err.message.contains(".gitattributes"), "{attrs_err:?}");

    let clean_repo = tmp.path().join("clean");
    init_repo(&clean_repo);
    commit_text(&clean_repo, "README.md", "base\n", "base");
    let clean_base = git_stdout(&clean_repo, &["rev-parse", "HEAD"]);
    commit_text(&clean_repo, "result.txt", "clean result\n", "clean result");
    let clean_range = work_gate::freeze_range(&clean_repo, &clean_base).unwrap();
    let report = work_gate::scan_added_blobs(&home, &cfg, &clean_repo, &clean_range).unwrap();
    assert!(report.hits.is_empty(), "{report:?}");
    assert_eq!(report.scanned_bytes, "clean result\n".len() as u64);
    assert_eq!(
        report.digest(),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn diff_body_is_fenced_escaped_and_masked_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("render");
    init_repo(&repo);
    commit_text(&repo, "README.md", "base\n", "base");
    let base_sha = git_stdout(&repo, &["rev-parse", "HEAD"]);
    let token = ["xoxb-", "1234567890-", "abcdef"].concat();
    let body = format!(
        "terminal \u{1b}[2J\nbidi \u{202e}worker text\nsecret {token}\nliteral \\uABCD\nlong {}\n",
        "x".repeat(4_200)
    );
    commit_text(&repo, "result.txt", &body, "render hostile output");
    let range = work_gate::freeze_range(&repo, &base_sha).unwrap();

    let masked = work_gate::render_diff(&repo, &range, false).unwrap();
    assert!(
        masked.starts_with("===== BEGIN UNTRUSTED WORKER OUTPUT =====\n"),
        "{masked}"
    );
    assert!(
        masked.ends_with("===== END UNTRUSTED WORKER OUTPUT ====="),
        "{masked}"
    );
    assert!(masked.contains("\\u001b"), "{masked}");
    assert!(masked.contains("\\u202e"), "{masked}");
    assert!(masked.contains("+literal \\uABCD"), "{masked}");
    assert!(!masked.contains('\u{1b}'), "{masked}");
    assert!(!masked.contains('\u{202e}'), "{masked}");
    assert!(masked.contains("[REDACTED:22]"), "{masked}");
    assert!(!masked.contains(&token), "{masked}");
    assert!(masked.contains("non_ascii=true"), "{masked}");
    assert!(masked.contains("clamped=true"), "{masked}");

    let raw = work_gate::render_diff(&repo, &range, true).unwrap();
    assert!(raw.starts_with("===== BEGIN UNTRUSTED WORKER OUTPUT =====\n"));
    assert!(raw.ends_with("===== END UNTRUSTED WORKER OUTPUT ====="));
    assert!(raw.contains(&token), "{raw}");
    assert!(raw.contains("\\u001b"), "{raw}");
    assert!(raw.contains("\\u202e"), "{raw}");
    assert!(!raw.contains('\u{1b}'), "{raw}");
    assert!(!raw.contains('\u{202e}'), "{raw}");
}

#[test]
fn work_diff_cli_prints_summary_patch_and_hit_set_digest() {
    let tmp = tempfile::tempdir().unwrap();
    let home = home_at(&tmp.path().join("home"));
    let work_id = "work-gate-diff-cli";
    let repo = home.work_dir().join(work_id).join("repo");
    init_repo(&repo);
    commit_text(&repo, "README.md", "base\n", "base");
    let base_sha = git_stdout(&repo, &["rev-parse", "HEAD"]);
    commit_text(&repo, "result.txt", "reviewed result\n", "reviewed result");
    let state = work_exec::WorkState {
        phase: WorkPhase::Running,
        work_id: work_id.to_string(),
        envelope_digest: "digest".to_string(),
        result_note_id: "result-note".to_string(),
        milestone_posted: false,
        worker_id: None,
        result_ref: None,
        checkout: repo,
        push_attempts: 0,
        next_push_after: None,
        result_summary: None,
        evidence_excerpt: None,
        result_status: None,
        base_sha: Some(base_sha),
        held_commit_sha: None,
        closure_digest: None,
        base_ref: Some("main".to_string()),
        remote_url: Some("file:///verified-origin.git".to_string()),
    };
    work_exec::save_state(&home, &state).unwrap();

    let masked = common::a2a(home.root(), tmp.path())
        .args(["mesh", "work", "diff", work_id])
        .assert()
        .success()
        .stdout(predicate::str::contains("BEGIN UNTRUSTED"))
        .stdout(predicate::str::contains("1\\u00090\\u0009result.txt"))
        .stdout(predicate::str::contains("hit_set_digest: "));
    let masked_stdout = String::from_utf8(masked.get_output().stdout.clone()).unwrap();
    let masked_digest = hit_set_digest(&masked_stdout);
    assert_eq!(masked_digest.len(), 64);
    assert!(masked_digest.bytes().all(|byte| byte.is_ascii_hexdigit()));

    let raw = common::a2a(home.root(), tmp.path())
        .args(["mesh", "work", "diff", work_id, "--raw"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "a2a: warning: --raw prints unmasked worker output; never pipe it into agent context",
        ));
    let raw_stdout = String::from_utf8(raw.get_output().stdout.clone()).unwrap();
    assert_eq!(hit_set_digest(&raw_stdout), masked_digest);
}

struct MaterializingGit;

#[allow(async_fn_in_trait)]
impl GitAdapter for MaterializingGit {
    async fn ls_remote(&self, _remote_url: &str, _base_ref: &str) -> Result<(), AppError> {
        Ok(())
    }

    async fn clone_checkout(
        &self,
        _remote_url: &str,
        _base_ref: &str,
        checkout: &Path,
    ) -> Result<(), AppError> {
        init_repo(checkout);
        std::fs::write(checkout.join("README.md"), "reviewed base\n").unwrap();
        run_git(checkout, &["add", "README.md"]);
        run_git(checkout, &["commit", "-m", "reviewed base"]);
        Ok(())
    }
}

struct NoSpawn;

impl WorkSpawner for NoSpawn {
    fn preflight(&self, _home: &A2aHome, _checkout: &Path) -> Result<(), AppError> {
        Ok(())
    }

    fn spawn(
        &self,
        _home: &A2aHome,
        _checkout: &Path,
        _brief: &Path,
    ) -> Result<String, AppError> {
        panic!("approve must stop before spawning")
    }
}

fn init_repo(repo: &Path) {
    std::fs::create_dir_all(repo).unwrap();
    run_git(repo, &["init", "-b", "main"]);
    run_git(repo, &["config", "user.email", "test@example.invalid"]);
    run_git(repo, &["config", "user.name", "Test User"]);
    run_git(repo, &["config", "commit.gpgsign", "false"]);
}

fn commit_text(repo: &Path, path: &str, body: &str, message: &str) {
    std::fs::write(repo.join(path), body).unwrap();
    run_git(repo, &["add", "--", path]);
    run_git(repo, &["commit", "-m", message]);
}

fn home_at(path: &Path) -> A2aHome {
    A2aHome::resolve(Some(path)).unwrap()
}

fn copy_root_pub(root: &Path, joiner: &Path) {
    std::fs::create_dir_all(joiner.join("mesh/fleet")).unwrap();
    std::fs::copy(
        root.join("mesh/fleet/root.pub"),
        joiner.join("mesh/fleet/root.pub"),
    )
    .unwrap();
}

fn enroll_joiner(root: &A2aHome, joiner: &A2aHome) -> String {
    let identity = gateway_identity(joiner, false).unwrap();
    let pubkey_b64 = gateway_public_key_b64(joiner).unwrap();
    let token = enroll_gateway_token(root, &identity.gateway_id, &pubkey_b64, &SystemClock).unwrap();
    install_gateway_token(joiner, &token, &SystemClock).unwrap();
    identity.gateway_id
}

fn git_stdout(cwd: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn hit_set_digest(output: &str) -> &str {
    output
        .lines()
        .find_map(|line| line.strip_prefix("hit_set_digest: "))
        .expect("work diff prints a hit-set digest")
}

fn run_git(cwd: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
