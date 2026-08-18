// tests/worktree_spawn.rs
//
// The spawn ⇄ worktree wiring. Task 2 owns exactly one case here: the snapshot
// is a *derived* checkpoint, so everything the worktree layer records at spawn
// has to be reconstructible from `events.jsonl` alone.
mod common;
use common::*;
use serde_json::json;
use std::path::Path;

/// Rewrite the genesis row's `raw` with `keys`, then throw the checkpoint away
/// so the next read has to rebuild from the WAL.
///
/// Task 3 is what makes `a2a spawn` write these keys; Task 2 owns the *fold*,
/// so the genesis row is staged here rather than produced by the spawn path.
fn stage_genesis_and_drop_the_snapshot(home: &Path, id: &str, keys: serde_json::Value) {
    let dir = home.join("workers").join(id);
    let events = dir.join("events.jsonl");
    let text = std::fs::read_to_string(&events).expect("events.jsonl");
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    let mut genesis: serde_json::Value = serde_json::from_str(&lines[0]).expect("genesis parses");
    assert_eq!(genesis["raw"]["fsm"], "Spawn", "line 1 is genesis");
    for (k, v) in keys.as_object().unwrap() {
        genesis["raw"][k] = v.clone();
    }
    lines[0] = genesis.to_string();
    std::fs::write(&events, lines.join("\n") + "\n").unwrap();
    std::fs::remove_file(dir.join("state.json")).unwrap();
}

/// The lossless pin. Genesis is the only writer of identity, so a worker's
/// worktree, its diff anchor and its result schema must all come back after the
/// derived checkpoint is deleted — otherwise `state.json` has quietly become
/// the source of truth for them.
#[test]
fn the_worktree_base_ref_and_schema_survive_losing_state_json() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = write_brief(tmp.path(), "do the thing\n");
    let schema = tmp.path().join("impl-result.schema.json");
    std::fs::write(&schema, br#"{"type":"object","required":["summary"]}"#).unwrap();

    let out = a2a(&home, tmp.path())
        .env("STUB_LAST_MSG", r#"{"summary":"ok"}"#)
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .arg("--schema")
        .arg(&schema)
        .assert()
        .success();
    let v: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();
    wait_dead(&home, &id, 10);
    // Reading a worker is what advances it: this commits the terminal
    // transition to the WAL, so the rebuild below replays a FINISHED worker.
    a2a(&home, tmp.path())
        .args(["status", &id])
        .assert()
        .success();

    let wt_path = home.join("worktrees").join(&id);
    let branch = format!("a2a/general/{id}");
    let base = "0123456789abcdef0123456789abcdef01234567";
    stage_genesis_and_drop_the_snapshot(
        &home,
        &id,
        json!({
            "worktree_path": wt_path,
            "worktree_branch": branch,
            "worktree_base_ref": base,
            "worktree_repo": work,
            "base_ref": base,
        }),
    );

    let out = a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .assert()
        .success();
    let env: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let data = &env["data"];
    assert_eq!(data["worktree"]["path"], json!(wt_path), "{env}");
    assert_eq!(data["worktree"]["branch"], json!(branch), "{env}");
    assert_eq!(data["worktree"]["base_ref"], json!(base), "{env}");
    assert_eq!(data["worktree"]["repo"], json!(work), "{env}");
    assert_eq!(data["base_ref"], json!(base), "{env}");

    // ...and the rebuild PERSISTED all of it, schema_path included — the key
    // genesis has recorded since Phase 1 and the fold never read.
    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["worktree"]["branch"], json!(branch), "{snap}");
    assert_eq!(snap["base_ref"], json!(base), "{snap}");
    assert_eq!(snap["schema_path"], json!(schema), "{snap}");
    assert!(
        snap.get("fold_rejections")
            .map(|r| r.as_array().is_some_and(|a| a.is_empty()))
            .unwrap_or(true),
        "a rebuild from zero must replay cleanly: {snap}"
    );
}
