// tests/e2e_exec.rs
//
// End-to-end pipelines over the *public CLI surface only* — no crate internals,
// no reaching into the file layer except to prove the CLI's claims. Everything
// a Phase-1 orchestrator actually does, in the order it does it.
//
// Two tiers (spec §8):
//
//   * **Always on** (tiers 1/2 — hermetic): both pipelines run against the
//     scripted `tests/fixtures/stub-codex`, so `cargo test` is green from a
//     clean checkout with **no real codex on PATH and no network**.
//   * **Token-gated** (tier 5 — model e2e): `real_codex_smoke` is `#[ignore]`d
//     *and* re-checks `A2A_E2E=1` at runtime, because `--ignored` alone would
//     otherwise spend tokens the moment somebody runs the whole suite with
//     `--include-ignored`. It is the one test in the repo that touches the real
//     `~/.codex` — it READS the auth stored there and writes nothing under it.
mod common;
use common::*;

/// Happy path: spawn → wait → status → result → gc --dry-run.
///
/// The assertions are deliberately end-to-end rather than unit-shaped: the id
/// printed by `spawn` is the id `wait` blocks on, is the id `status` calls
/// `done`, is the id whose `result` carries the worker's own words, is the id
/// `gc` offers to reclaim. If any layer between the CLI and the WAL disagreed
/// about identity or terminality, one of those five would break.
#[test]
fn stub_happy_pipeline_spawn_wait_status_result_gc() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = write_brief(tmp.path(), "summarize the repository\n");

    // spawn → prints the new worker's id
    let out = a2a(&home, tmp.path())
        .env("STUB_LAST_MSG", "A2A-STUB-OK")
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();
    assert!(id.starts_with("general-"), "id carries its lane: {id}");

    // wait → exit 0 (the state reached IS the exit code)
    a2a(&home, tmp.path())
        .args(["wait", &id, "--timeout", "30s"])
        .assert()
        .code(0);

    // status → done, exit 0
    let st = a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .assert()
        .code(0);
    let sv: serde_json::Value = serde_json::from_slice(&st.get_output().stdout).unwrap();
    assert_eq!(sv["data"]["state"], "done");

    // result → the worker's final message on stdout
    let res = a2a(&home, tmp.path())
        .args(["result", &id])
        .assert()
        .code(0);
    let printed = String::from_utf8_lossy(&res.get_output().stdout).to_string();
    assert!(printed.contains("A2A-STUB-OK"), "result stdout was: {printed}");

    // gc --dry-run → lists the finished worker, deletes nothing
    let gc = a2a(&home, tmp.path())
        .args(["--json", "gc", "--dry-run"])
        .assert()
        .success();
    let gv: serde_json::Value = serde_json::from_slice(&gc.get_output().stdout).unwrap();
    assert_eq!(gv["data"]["reclaimable"][0]["id"], id.as_str());
    assert!(
        home.join("workers").join(&id).exists(),
        "--dry-run must not delete"
    );
}

/// The steering loop: a worker that asks a question parks, the orchestrator is
/// told *by an exit code* rather than by parsing prose, answers, and the same
/// worker converges.
///
/// `wait --until blocked` exiting 5 is the whole point — it is what lets an
/// orchestrator sleep on N workers and be woken only by the one that needs it.
#[test]
fn stub_blocked_pipeline_wait_blocked_send_wait_result() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = write_brief(tmp.path(), "pick an option, ask if unsure\n");

    // spawn a worker scripted to write an outbox question
    let out = a2a(&home, tmp.path())
        .env("STUB_EMIT_QUESTION", "1")
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();

    // wait --until blocked → exit 5
    a2a(&home, tmp.path())
        .args(["wait", &id, "--until", "blocked", "--timeout", "30s"])
        .assert()
        .code(5);

    // the question is readable before answering it
    let outbox = home.join("workers").join(&id).join("outbox");
    assert!(
        outbox.join("1.question.json").exists(),
        "the BLOCKED file is the authority"
    );

    // send the answer. The resume child inherits THIS process's env after the
    // scrub (inbox.rs delivery policy), so the stub's knobs are set here — with
    // no STUB_EMIT_QUESTION this time, the resumed turn produces a result.
    a2a(&home, tmp.path())
        .env("STUB_LAST_MSG", "A2A-ANSWERED-B")
        .args(["send", &id, "use option B"])
        .assert()
        .success();

    // wait → exit 0
    a2a(&home, tmp.path())
        .args(["wait", &id, "--timeout", "30s"])
        .assert()
        .code(0);

    // result → non-empty
    let res = a2a(&home, tmp.path())
        .args(["result", &id])
        .assert()
        .code(0);
    let printed = String::from_utf8_lossy(&res.get_output().stdout).to_string();
    assert!(!printed.trim().is_empty(), "answered worker must have a result");
    assert!(printed.contains("A2A-ANSWERED-B"), "result stdout was: {printed}");
}

/// Tier 5: the only test that runs a real `codex`.
///
/// This is what validates `events::map_exec_line` against the vocabulary codex
/// actually emits — the stub can only ever confirm the vocabulary we wrote into
/// it. A fresh `A2A_HOME` under a tempdir, an allowlist that is that tempdir,
/// and `A2A_CODEX_BIN` explicitly REMOVED so PATH's real codex is used.
#[test]
#[ignore = "real codex + tokens; run with A2A_E2E=1 cargo test --test e2e_exec -- --ignored"]
fn real_codex_smoke() {
    if std::env::var("A2A_E2E").as_deref() != Ok("1") {
        eprintln!("A2A_E2E != 1 — skipping the real-codex smoke");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    assert!(std::process::Command::new("git")
        .args(["init", "-q"])
        .current_dir(&repo)
        .status()
        .unwrap()
        .success());
    let brief = common::write_brief(tmp.path(), "Reply with exactly the text: A2A-OK\n");

    // Env shared by every call below. `A2A_SHIM_BIN` is set for the same reason
    // `tests/common` sets it: under the test harness `current_exe()` is the test
    // binary, not `a2a`, so the detached shim would be resolved from the wrong
    // directory. `A2A_CODEX_BIN` is *removed*, not overridden — PATH's codex is
    // the subject of this test.
    let envs = |c: &mut assert_cmd::Command| {
        c.env("A2A_HOME", &home)
            .env("A2A_ALLOWED_ROOTS", tmp.path())
            .env("A2A_SHIM_BIN", assert_cmd::cargo::cargo_bin("a2a"))
            .env_remove("A2A_CODEX_BIN");
    };

    let mut c = assert_cmd::Command::cargo_bin("a2a").unwrap();
    envs(&mut c);
    // real codex from PATH; real ~/.codex auth is READ; nothing under it is written
    let out = c
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(&repo)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();
    eprintln!("real-codex worker: {id}");

    let mut w = assert_cmd::Command::cargo_bin("a2a").unwrap();
    envs(&mut w);
    let waited = w.args(["wait", &id, "--timeout", "300s"]).assert().code(0);
    eprintln!(
        "wait stderr: {}",
        String::from_utf8_lossy(&waited.get_output().stderr)
    );

    let mut r = assert_cmd::Command::cargo_bin("a2a").unwrap();
    envs(&mut r);
    let res = r.args(["result", &id]).assert().code(0);
    let printed = String::from_utf8_lossy(&res.get_output().stdout).to_string();
    eprintln!("result: {printed}");
    assert!(printed.contains("A2A-OK"), "result was: {printed}");

    let snap = common::read_snapshot(&home, &id);
    assert!(
        snap["session_id"].is_string(),
        "real session id must be captured; snapshot was {snap}"
    );

    // The vocabulary this run actually observed — printed so a mapping-table
    // regression is diagnosable from the test output alone (`--nocapture`).
    let events = std::fs::read_to_string(home.join("workers").join(&id).join("events.jsonl"))
        .unwrap_or_default();
    let mut kinds: Vec<String> = events
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .map(|e| {
            format!(
                "{} <- {}",
                e["kind"].as_str().unwrap_or("?"),
                e["raw"]["type"].as_str().unwrap_or("(synthetic)")
            )
        })
        .collect();
    kinds.dedup();
    eprintln!("observed exec vocabulary:\n  {}", kinds.join("\n  "));

    // The drift gate. It asserts *presence of the load-bearing mappings* rather
    // than absence of `unknown`: unknown is the designed landing spot for a
    // trace line a newer codex invented (every enum here is open), so failing on
    // it would make this test break on additions that are explicitly fine. What
    // must never regress is that a real turn's session, boundaries, and the
    // worker's own message all arrive as *lifecycle* events — that is what
    // `a2a tail` shows and what an orchestrator reads.
    for expected in [
        "session_identified",
        "turn_started",
        "agent_message",
        "turn_completed",
    ] {
        assert!(
            kinds.iter().any(|k| k.starts_with(expected)),
            "map_exec_line has drifted: no `{expected}` in the real vocabulary {kinds:?}"
        );
    }
    // …and specifically that the agent message came from a real transport line,
    // not from a synthetic envelope we wrote ourselves.
    assert!(
        kinds.iter().any(|k| k.starts_with("agent_message <-") && !k.contains("(synthetic)")),
        "the worker's own message must map from a real exec line: {kinds:?}"
    );
}
