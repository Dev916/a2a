// tests/exec_lifecycle.rs
use a2a::config::SandboxMode;
use a2a::exec::{self, KillOutcome, Liveness, SpawnSpec};
use std::time::Duration;

fn stub() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/stub-codex")
}
fn mk_worker(tmp: &std::path::Path) -> std::path::PathBuf {
    let w = tmp.join("w");
    for d in ["outbox", "scratch"] {
        std::fs::create_dir_all(w.join(d)).unwrap();
    }
    std::fs::write(w.join("prompt.md"), "do it\n").unwrap();
    w
}
fn spec<'a>(
    cwd: &'a std::path::Path,
    w: &'a std::path::Path,
    bin: &'a std::path::Path,
    env: &'a [(String, String)],
) -> SpawnSpec<'a> {
    SpawnSpec {
        codex_bin: bin,
        cwd,
        worker_dir: w,
        epoch: 1,
        sandbox: SandboxMode::WorkspaceWrite,
        network: false,
        model: "gpt-5.5",
        effort: "high",
        schema_path: None,
        prompt_file: Box::leak(Box::new(w.join("prompt.md"))),
        resume_session: None,
        env,
    }
}
fn wait_liveness(w: &std::path::Path, want_dead: bool, secs: u64) {
    for _ in 0..(secs * 10) {
        let dead = matches!(exec::liveness(w), Liveness::Dead);
        if dead == want_dead {
            return;
        }
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
    for must in [
        "--json",
        "approval_policy=\"never\"",
        "features.hooks=false",
        "sandbox_workspace_write.network_access=false",
        "-m gpt-5.5",
        "model_reasoning_effort=\"high\"",
        "--skip-git-repo-check",
        "--output-last-message",
        "--add-dir",
    ] {
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
    assert!(
        !argv2.iter().any(|a| a == tmp.path().to_str().unwrap()),
        "repo path must not appear in outbox-only argv"
    );
}

#[test]
fn detached_worker_lifecycle_env_scrub_and_streams() {
    let tmp = tempfile::tempdir().unwrap();
    let w = mk_worker(tmp.path());
    let b = stub();
    let envdump = tmp.path().join("env.txt");
    let env = vec![
        ("STUB_SLEEP".to_string(), "2".to_string()),
        ("STUB_EXIT".to_string(), "7".to_string()),
        (
            "STUB_DUMP_ENV".to_string(),
            envdump.to_str().unwrap().to_string(),
        ),
        (
            "OPENAI_API_KEY".to_string(),
            "sk-should-be-scrubbed-000000".to_string(),
        ),
    ];
    // NOTE: OPENAI_API_KEY passed via SpawnSpec.env must ALSO be scrubbed (scrub wins)
    let proc_ = exec::spawn_detached(&spec(tmp.path(), &w, &b, &env)).unwrap();
    assert!(
        matches!(exec::liveness(&w), Liveness::Alive),
        "spawn_detached returns only after the shim holds run.lock"
    );
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
    assert!(matches!(
        exec::kill_worker(proc_.pid, "wrong witness").unwrap(),
        KillOutcome::WitnessMismatch
    ));
    let wit = exec::start_witness(proc_.pid).unwrap();
    assert!(matches!(
        exec::kill_worker(proc_.pid, &wit).unwrap(),
        KillOutcome::Killed
    ));
    wait_liveness(&w, true, 10);
}
