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
    let _ = a2a(&home, tmp.path()).args(["status", &id]).assert(); // ingest
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
