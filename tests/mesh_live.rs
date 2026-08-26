// tests/mesh_live.rs
//
// Read-only live smoke for the mesh commands. This file performs no live
// registration and no live posting: `roster --json` and `feed --json` are the
// only real-home commands here, and a human opts into future hook exercises.
//
// Gated twice: `#[ignore]` keeps ordinary test runs away from the real home,
// and `A2A_MESH_LIVE=1` is re-checked at runtime so `--include-ignored` is not
// enough to touch `~/.a2a`.
//
//   A2A_MESH_LIVE=1 cargo test --test mesh_live -- --ignored

use assert_cmd::Command;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const LIVE_ENV: &str = "A2A_MESH_LIVE";

fn gated(what: &str) -> bool {
    if std::env::var(LIVE_ENV).as_deref() != Ok("1") {
        eprintln!("{LIVE_ENV} != 1 - skipping {what}");
        return false;
    }
    true
}

fn real_home() -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").expect("a real $HOME is the subject of this file"))
}

fn mesh_root() -> PathBuf {
    real_home().join(".a2a").join("mesh")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hash_mesh() -> Option<BTreeMap<String, String>> {
    fn walk(root: &Path, at: &Path, out: &mut BTreeMap<String, String>) {
        let mut entries: Vec<_> = fs::read_dir(at).unwrap().map(|e| e.unwrap()).collect();
        entries.sort_by_key(|e| e.path());
        for entry in entries {
            let path = entry.path();
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            let ty = entry.file_type().unwrap();
            if ty.is_dir() {
                out.insert(format!("{rel}/"), String::new());
                walk(root, &path, out);
            } else if ty.is_file() {
                let mut h = Sha256::new();
                h.update(fs::read(&path).unwrap());
                out.insert(rel, hex(&h.finalize()));
            }
        }
    }

    let root = mesh_root();
    if !root.exists() {
        return None;
    }
    let mut out = BTreeMap::new();
    walk(&root, &root, &mut out);
    Some(out)
}

fn live_cmd() -> Command {
    let mut c = Command::cargo_bin("a2a").unwrap();
    c.env_remove("A2A_HOME")
        .env_remove("A2A_BROKER_DISABLE")
        .env_remove("CLAUDE_SESSION_ID")
        .env_remove("BEADS_ACTOR")
        .env_remove("A2A_JSON");
    c
}

fn parsed_data(args: &[&str]) -> Value {
    let out = live_cmd().args(args).assert().success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    v.get("data").cloned().unwrap_or(v)
}

#[test]
#[ignore]
fn roster_and_feed_parse_without_writing_real_mesh_home() {
    if !gated("mesh live smoke") {
        return;
    }

    let before = hash_mesh();
    let roster = parsed_data(&["mesh", "roster", "--json"]);
    assert!(roster.is_array(), "{roster}");
    assert_eq!(hash_mesh(), before, "roster --json wrote under ~/.a2a/mesh");

    let feed = parsed_data(&["mesh", "feed", "--json"]);
    assert!(feed.is_array(), "{feed}");
    assert_eq!(hash_mesh(), before, "feed --json wrote under ~/.a2a/mesh");
}
