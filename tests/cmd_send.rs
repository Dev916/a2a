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
