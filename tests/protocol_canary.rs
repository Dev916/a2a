//! Protocol drift canary for `schema/v2-snapshot/` (spec §8.3).
//!
//! The `codex app-server` protocol offers **no version handshake**, so the only
//! way `a2a` learns that the wire moved under it is to regenerate the schema
//! from the live binary and diff it against the checked-in subset.
//!
//! Three deliberate properties, each load-bearing:
//!
//! 1. **`codex` is resolved from `PATH` only.** `A2A_CODEX_BIN` is *deliberately
//!    ignored* here — that variable points at the binary workers are spawned
//!    with, which every other test overrides with `tests/fixtures/stub-codex`.
//!    This is a *protocol* gate, not a worker path: a stub could never answer
//!    `generate-json-schema`, and letting the stub win would silently turn the
//!    canary green forever. See `schema/v2-snapshot/REGEN.md`.
//! 2. **`CODEX_HOME` is a tempdir.** The regen never reads or writes `~/.codex`.
//! 3. **Absent `codex` SKIPs, it does not fail.** Drift is unverifiable on a
//!    machine without the binary, not violated.
//!
//! The regen costs **zero tokens and needs no auth** (verified against
//! codex-cli 0.140.0).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Every file byte-diffed against the live regen. Paths are relative to both
/// `schema/v2-snapshot/` and the `--out` directory (the layout is identical).
///
/// Two files are conspicuously absent and must stay absent, per REGEN.md:
/// the aggregate `codex_app_server_protocol*.schemas.json` (definition-map
/// ordering churns between runs) and `ClientRequest.json` /
/// `ServerNotification.json` (~150 KB each; any protocol addition would
/// permanently redden the gate). The latter two are compared as method *sets*.
const BYTE_SNAPSHOT_FILES: &[&str] = &[
    "v1/InitializeParams.json",
    "v1/InitializeResponse.json",
    "v2/ThreadStartParams.json",
    "v2/ThreadStartResponse.json",
    "v2/TurnStartParams.json",
    "v2/TurnStartResponse.json",
    "v2/TurnSteerParams.json",
    "v2/TurnInterruptParams.json",
    "v2/ThreadResumeParams.json",
    "v2/TurnCompletedNotification.json",
    "v2/ItemCompletedNotification.json",
    "v2/ThreadStartedNotification.json",
    "v2/TurnStartedNotification.json",
    "ExecCommandApprovalResponse.json",
    "ApplyPatchApprovalResponse.json",
    "CommandExecutionRequestApprovalResponse.json",
    "FileChangeRequestApprovalResponse.json",
    "PermissionsRequestApprovalResponse.json",
    "ToolRequestUserInputResponse.json",
    "McpServerElicitationRequestResponse.json",
    "DynamicToolCallResponse.json",
    "ChatgptAuthTokensRefreshResponse.json",
    "AttestationGenerateResponse.json",
];

/// `(snapshot method-set file, generated schema it is extracted from)`.
const METHOD_SET_FILES: &[(&str, &str)] = &[
    ("client_request_methods.txt", "ClientRequest.json"),
    ("server_notification_methods.txt", "ServerNotification.json"),
];

fn snapshot_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("schema/v2-snapshot")
}

/// Resolve `codex` from `PATH` **only**. Never consults `A2A_CODEX_BIN` (see
/// the module doc): a worker stub must not be able to answer the protocol gate.
fn codex_on_path() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|d| d.join("codex"))
        .find(|c| c.is_file())
}

/// `oneOf[].properties.method.enum[0]`, sorted and deduped — the same
/// extraction REGEN.md documents.
fn extract_methods(schema: &Path) -> BTreeSet<String> {
    let raw = std::fs::read_to_string(schema)
        .unwrap_or_else(|e| panic!("read {}: {e}", schema.display()));
    let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
    doc["oneOf"]
        .as_array()
        .unwrap_or_else(|| panic!("{} has no oneOf", schema.display()))
        .iter()
        .filter_map(|v| {
            v["properties"]["method"]["enum"][0]
                .as_str()
                .map(String::from)
        })
        .collect()
}

fn read_method_set(file: &Path) -> BTreeSet<String> {
    std::fs::read_to_string(file)
        .unwrap_or_else(|e| panic!("read {}: {e}", file.display()))
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(String::from)
        .collect()
}

/// Offline half of the gate: the checked-in subset is complete and well formed.
/// Runs on every machine, `codex` present or not.
#[test]
fn snapshot_subset_is_complete() {
    let dir = snapshot_dir();
    let mut missing = Vec::new();
    for f in BYTE_SNAPSHOT_FILES {
        if !dir.join(f).is_file() {
            missing.push(*f);
        }
    }
    for (set_file, _) in METHOD_SET_FILES {
        if !dir.join(set_file).is_file() {
            missing.push(set_file);
        }
    }
    if !dir.join("REGEN.md").is_file() {
        missing.push("REGEN.md");
    }
    assert!(missing.is_empty(), "missing snapshot files: {missing:?}");

    // The aggregate must never be snapshotted — its definition ordering churns.
    for forbidden in [
        "codex_app_server_protocol.schemas.json",
        "codex_app_server_protocol.v2.schemas.json",
        "ClientRequest.json",
        "ServerNotification.json",
    ] {
        assert!(
            !dir.join(forbidden).exists(),
            "{forbidden} must NOT be byte-snapshotted (see REGEN.md)"
        );
    }

    // Method sets are sorted, deduped and non-empty.
    for (set_file, _) in METHOD_SET_FILES {
        let raw = std::fs::read_to_string(dir.join(set_file)).unwrap();
        let lines: Vec<&str> = raw.lines().filter(|l| !l.trim().is_empty()).collect();
        assert!(!lines.is_empty(), "{set_file} is empty");
        let mut sorted = lines.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(lines, sorted, "{set_file} must be sorted and deduped");
    }
}

/// Live half: regenerate from the real `codex` on `PATH` and diff.
#[test]
fn schema_snapshot_matches_live_codex() {
    let Some(codex) = codex_on_path() else {
        eprintln!(
            "SKIP schema_snapshot_matches_live_codex: no `codex` on PATH \
             (protocol drift is unverifiable here, not violated)"
        );
        return;
    };

    let out = tempfile::tempdir().unwrap();
    let codex_home = tempfile::tempdir().unwrap();
    let status = Command::new(&codex)
        .args(["app-server", "generate-json-schema", "--out"])
        .arg(out.path())
        // Never `~/.codex`.
        .env("CODEX_HOME", codex_home.path())
        .output()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", codex.display()));
    assert!(
        status.status.success(),
        "generate-json-schema failed ({}): {}",
        status.status,
        String::from_utf8_lossy(&status.stderr)
    );

    let dir = snapshot_dir();
    let mut drifted: Vec<String> = Vec::new();

    for f in BYTE_SNAPSHOT_FILES {
        let want = std::fs::read(dir.join(f)).unwrap_or_else(|e| panic!("read snapshot {f}: {e}"));
        match std::fs::read(out.path().join(f)) {
            Ok(got) if got == want => {}
            Ok(_) => drifted.push(format!("{f} (bytes differ)")),
            Err(e) => drifted.push(format!("{f} (absent from regen: {e})")),
        }
    }

    for (set_file, source) in METHOD_SET_FILES {
        let want = read_method_set(&dir.join(set_file));
        let got = extract_methods(&out.path().join(source));
        if want != got {
            let added: Vec<&String> = got.difference(&want).collect();
            let removed: Vec<&String> = want.difference(&got).collect();
            drifted.push(format!(
                "{set_file} (from {source}): added={added:?} removed={removed:?}"
            ));
        }
    }

    // `SERVER_REQUEST_METHODS` is a hard pin: the responder (Task 3) owes an
    // answer to every one of these, and a dropped server request is a
    // permanent hang.
    let live_server_requests = extract_methods(&out.path().join("ServerRequest.json"));
    let pinned: BTreeSet<String> = a2a::protocol::SERVER_REQUEST_METHODS
        .iter()
        .map(|s| s.to_string())
        .collect();
    if pinned != live_server_requests {
        let added: Vec<&String> = live_server_requests.difference(&pinned).collect();
        let removed: Vec<&String> = pinned.difference(&live_server_requests).collect();
        drifted.push(format!(
            "SERVER_REQUEST_METHODS (from ServerRequest.json): added={added:?} removed={removed:?}"
        ));
    }

    assert!(
        drifted.is_empty(),
        "app-server protocol drifted from schema/v2-snapshot/ — regen per \
         schema/v2-snapshot/REGEN.md and reconcile src/protocol.rs:\n  {}",
        drifted.join("\n  ")
    );
}
