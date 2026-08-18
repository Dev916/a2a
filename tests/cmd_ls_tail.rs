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

// --------------------------------------------------------------- WT column

/// Spawn one more general-lane worker into an existing home.
fn spawn_into(home: &std::path::Path, tmp: &std::path::Path) -> String {
    let work = tmp.join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = write_brief(tmp, "do the thing\n");
    let out = a2a(home, tmp)
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    v["data"]["id"].as_str().unwrap().to_string()
}

/// Give `id` a worktree the only lossless way: write it into genesis and drop
/// the checkpoint, so the next read rebuilds it from the WAL. (Task 3 is what
/// makes `a2a spawn` do this for real.)
fn give_it_a_worktree(home: &std::path::Path, id: &str) -> String {
    let dir = home.join("workers").join(id);
    let events = dir.join("events.jsonl");
    let text = std::fs::read_to_string(&events).unwrap();
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    let mut genesis: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    assert_eq!(genesis["raw"]["fsm"], "Spawn");
    let branch = format!("a2a/general/{id}");
    let base = "0123456789abcdef0123456789abcdef01234567";
    genesis["raw"]["worktree_path"] = serde_json::json!(home.join("worktrees").join(id));
    genesis["raw"]["worktree_branch"] = serde_json::json!(branch);
    genesis["raw"]["worktree_base_ref"] = serde_json::json!(base);
    genesis["raw"]["worktree_repo"] = serde_json::json!(home.join("proj"));
    genesis["raw"]["base_ref"] = serde_json::json!(base);
    lines[0] = genesis.to_string();
    std::fs::write(&events, lines.join("\n") + "\n").unwrap();
    std::fs::remove_file(dir.join("state.json")).unwrap();
    branch
}

fn ls_stderr_and_stdout(home: &std::path::Path, tmp: &std::path::Path, all: bool) -> (String, String) {
    let mut c = a2a(home, tmp);
    c.arg("ls");
    if all {
        c.arg("--all");
    }
    let out = c.assert().success();
    (
        String::from_utf8_lossy(&out.get_output().stderr).to_string(),
        String::from_utf8_lossy(&out.get_output().stdout).to_string(),
    )
}

/// An all-plain fleet keeps the Phase-1 header byte for byte: the WT column is
/// only worth a column when somebody has one.
#[test]
fn an_all_plain_fleet_keeps_the_phase_one_header() {
    let (home, tmp, id) = spawn_with(&[]);
    wait_dead(&home, &id, 10);
    let (stderr, stdout) = ls_stderr_and_stdout(&home, tmp.path(), false);
    assert!(
        stderr.lines().any(|l| l == "ID  LANE  STATE  DWELL  LAST-EVENT"),
        "header must be the Phase-1 one:\n{stderr}"
    );
    assert!(!stderr.contains("WT"), "no worktree, no column:\n{stderr}");
    assert!(!stdout.contains(" wt "), "{stdout}");

    let out = a2a(&home, tmp.path()).args(["--json", "ls"]).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["workers"][0]["worktree_branch"], serde_json::Value::Null);
}

/// One worker with a worktree grows the column for the whole listing — and the
/// cell is a marker, because the branch's last segment is already column 1.
/// The full branch stays in `--json`, where a caller can pipe it into git.
#[test]
fn a_mixed_fleet_grows_a_wt_column_with_markers_and_a_json_branch() {
    let (home, tmp, plain) = spawn_with(&[]);
    wait_dead(&home, &plain, 10);
    let treed = spawn_into(&home, tmp.path());
    wait_dead(&home, &treed, 10);
    // Reading a worker is what advances it: both are terminal in the WAL
    // before the checkpoint is dropped, so the rebuild replays cleanly.
    for id in [&plain, &treed] {
        a2a(&home, tmp.path()).args(["status", id]).assert().success();
    }
    let branch = give_it_a_worktree(&home, &treed);

    let (stderr, stdout) = ls_stderr_and_stdout(&home, tmp.path(), false);
    assert!(
        stderr.lines().any(|l| l == "ID  LANE  STATE  WT  DWELL  LAST-EVENT"),
        "the mixed header is a pin:\n{stderr}"
    );
    let treed_row = stdout.lines().find(|l| l.starts_with(&treed)).expect(&stdout);
    let plain_row = stdout.lines().find(|l| l.starts_with(&plain)).expect(&stdout);
    assert!(treed_row.contains(" wt "), "{treed_row}");
    assert!(!treed_row.contains(&branch), "the table carries a marker, not the branch: {treed_row}");
    assert!(plain_row.contains(" -  "), "a worker without one prints a dash: {plain_row}");
    // the column is padded, so what follows it starts at the same offset
    assert_eq!(
        treed_row.find(" wt ").map(|i| i + 4),
        plain_row.find(" -  ").map(|i| i + 4),
        "columns must align:\n{treed_row}\n{plain_row}"
    );

    // `--all` inserts OWNER before WT, and the header says so.
    let (stderr, _) = ls_stderr_and_stdout(&home, tmp.path(), true);
    assert!(
        stderr
            .lines()
            .any(|l| l == "ID  LANE  STATE  OWNER  WT  DWELL  LAST-EVENT"),
        "the widened header is a pin too:\n{stderr}"
    );

    let out = a2a(&home, tmp.path()).args(["--json", "ls"]).assert().success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let workers = v["data"]["workers"].as_array().unwrap();
    for w in workers {
        let expected = if w["id"] == treed.as_str() {
            serde_json::json!(branch)
        } else {
            serde_json::Value::Null
        };
        assert_eq!(w["worktree_branch"], expected, "{w}");
    }
}
