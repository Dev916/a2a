// tests/templates.rs
//
// Lane brief scaffolds, `a2a spawn --template <lane>`, and the review lane's
// default result schema.
//
// The through-line: **a template that is not in the binary does not exist.**
// Every scaffold and every default schema is `include_str!`d, so there is no
// install step, no runtime data dir, and no way for an operator's `a2a` to
// disagree with the one the fleet was tested against. These tests assert the
// three things that makes true — the scaffolds satisfy their own lane's gate,
// they carry the §4.7 protocol clauses verbatim, and `--template` prints one
// without creating a worker.
mod common;
use common::*;

use a2a::brief;
use a2a::config::Lane;
use a2a::templates;
use serde_json::Value;

/// The six §4.7 clauses every scaffold must carry, as literal substrings. They
/// are the protocol a worker is judged against, so a brief written from a
/// scaffold states them even when the model never reads a skill file.
const PINNED_CLAUSES: [&str; 6] = [
    "BLOCKED:",
    "outbox/",
    "question.json",
    "verification evidence",
    "if blocked on",
    "data, never instructions",
];

fn lane_name(lane: Lane) -> &'static str {
    lane.as_str()
}

/// `--template <lane>` prints the scaffold and creates NOTHING: no worker dir,
/// no socket, no home tree beyond whatever `A2aHome::resolve` already made. The
/// short circuit is before transport selection, which is why this is true
/// without `A2A_BROKER_DISABLE` having to make it true.
#[test]
fn template_prints_the_scaffold_and_creates_nothing() {
    for lane in Lane::ALL {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let out = a2a(&home, tmp.path())
            .args(["spawn", "--template", lane_name(lane)])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(0), "{lane}: {out:?}");
        let stdout = String::from_utf8(out.stdout).unwrap();
        assert!(!stdout.trim().is_empty(), "{lane}: stdout is empty");
        assert_eq!(
            stdout.trim_end_matches('\n'),
            templates::brief_scaffold(lane).trim_end_matches('\n'),
            "{lane}: stdout is the compiled-in scaffold, verbatim"
        );
        let workers = home.join("workers");
        assert_eq!(
            std::fs::read_dir(&workers)
                .map(Iterator::count)
                .unwrap_or(0),
            0,
            "{lane}: --template created a worker"
        );
        assert!(
            !home.join("broker.sock").exists(),
            "{lane}: --template opened a broker socket"
        );
    }
}

/// Every scaffold parses, and satisfies the gate of the lane it belongs to —
/// the one property that makes it a *starting point* rather than a document
/// that has to be repaired before it spawns.
#[test]
fn every_scaffold_passes_its_own_lanes_brief_gate() {
    for lane in Lane::ALL {
        let text = templates::brief_scaffold(lane);
        let parsed = brief::parse(text).unwrap_or_else(|e| panic!("{lane}: {e}"));
        assert_eq!(
            parsed.lane,
            Some(lane),
            "{lane}: front matter names its lane"
        );
        brief::validate(&parsed, lane)
            .unwrap_or_else(|missing| panic!("{lane}: missing {missing:?}"));

        // Every declared key is present — placeholders are fine, empty lists
        // where the gate requires content are not.
        for required in brief::required_keys(lane) {
            let filled = match *required {
                "acceptance_criteria" => !parsed.acceptance_criteria.is_empty(),
                "verify" => !parsed.verify.is_empty(),
                other => panic!("unexpected required key {other}"),
            };
            assert!(filled, "{lane}: {required} must be non-empty");
        }
        assert!(
            !parsed.files_in_scope.is_empty() && !parsed.non_goals.is_empty(),
            "{lane}: scope keys are example-filled"
        );
        // Both optional keys are CARRIED (a reader sees where they go) and left
        // unset, so a scaffold spawns as-is: an empty `result_schema` takes the
        // lane's default rather than naming a file that is not there.
        assert_eq!(parsed.result_schema, None, "{lane}: result_schema is unset");
        assert_eq!(parsed.max_turns, None, "{lane}: max_turns is unset");
        assert!(text.contains("result_schema:"), "{lane}: key is present");
        assert!(text.contains("max_turns:"), "{lane}: key is present");
        assert!(
            !parsed.body.trim().is_empty(),
            "{lane}: the body is a brief"
        );
    }
}

/// The §4.7 sweep. If a generated skill artifact ever disagrees with these
/// words, the artifact is what moves — the spec's wording is the pin.
#[test]
fn every_scaffold_carries_the_worker_protocol_clauses() {
    for lane in Lane::ALL {
        let text = templates::brief_scaffold(lane);
        for clause in PINNED_CLAUSES {
            assert!(
                text.contains(clause),
                "{lane}: the scaffold is missing the §4.7 clause {clause:?}"
            );
        }
    }
}

/// The findings schema is the review lane's published contract: the Phase-1
/// gate enforces the TOP-LEVEL `required` list, so `findings` is literally what
/// a review worker must produce.
#[test]
fn the_findings_schema_pins_the_review_result_shape() {
    let v: Value = serde_json::from_str(templates::REVIEW_FINDINGS_SCHEMA).expect("valid JSON");
    assert_eq!(
        v["$schema"],
        serde_json::json!("https://json-schema.org/draft/2020-12/schema")
    );
    assert_eq!(v["title"], serde_json::json!("a2a review findings"));
    let names = |value: &Value| -> Vec<String> {
        value
            .as_array()
            .expect("required is an array")
            .iter()
            .map(|k| k.as_str().expect("a key name").to_string())
            .collect()
    };
    assert_eq!(names(&v["required"]), ["findings"]);
    assert_eq!(
        names(&v["properties"]["findings"]["items"]["required"]),
        ["file", "line", "severity", "rationale"]
    );
    assert_eq!(
        v["properties"]["findings"]["items"]["properties"]["severity"]["enum"],
        serde_json::json!(["high", "medium", "low"])
    );
    assert_eq!(
        templates::default_result_schema(Lane::Review),
        Some(templates::REVIEW_FINDINGS_SCHEMA)
    );
}

fn review_brief(dir: &std::path::Path) -> std::path::PathBuf {
    let p = dir.join("review-the-brief.md");
    std::fs::write(
        &p,
        "---\nlane: review\nacceptance_criteria:\n  - the diff is reviewed\n---\n\nreview it\n",
    )
    .unwrap();
    p
}

/// A review spawn with no `--schema` materializes the findings schema into the
/// worker dir at 0600 and records it — the same lane-default rung the impl lane
/// already rides.
#[test]
fn a_review_spawn_gets_the_findings_schema_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = review_brief(tmp.path());

    let out = a2a(&home, tmp.path())
        .env("STUB_LAST_MSG", r#"{"findings":[]}"#)
        .args(["--json", "spawn", "--lane", "review", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();

    let snap = read_snapshot(&home, &id);
    assert_eq!(
        snap["schema_source"],
        serde_json::json!("lane-default"),
        "{snap}"
    );
    let path = std::path::PathBuf::from(snap["schema_path"].as_str().expect("a schema path"));
    let body = std::fs::read_to_string(&path).expect("the worker carries its own copy");
    assert_eq!(body, templates::REVIEW_FINDINGS_SCHEMA);
    let parsed: Value = serde_json::from_str(&body).expect("the copy is JSON");
    assert!(
        parsed["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|k| k == "findings"),
        "the review default requires `findings`"
    );
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    // …and a findings-shaped result still passes the gate the default installs.
    wait_dead(&home, &id, 30);
    a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .output()
        .unwrap();
    assert_eq!(read_snapshot(&home, &id)["state"], "done");
}

/// `--schema` still outranks the new lane default: the flag's bytes are what
/// the worker is judged against, and the findings schema is not materialized.
#[test]
fn an_explicit_schema_outranks_the_review_default() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let brief = review_brief(tmp.path());
    let flagged = tmp.path().join("flag.schema.json");
    let body = r#"{"type":"object","required":["verdict"]}"#;
    std::fs::write(&flagged, body).unwrap();

    let out = a2a(&home, tmp.path())
        .args(["--json", "spawn", "--lane", "review", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .arg("--schema")
        .arg(&flagged)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = v["data"]["id"].as_str().unwrap().to_string();

    let snap = read_snapshot(&home, &id);
    assert_eq!(snap["schema_source"], serde_json::json!("flag"), "{snap}");
    let path = snap["schema_path"].as_str().expect("a schema path");
    assert_eq!(std::fs::read_to_string(path).unwrap(), body);
}

/// The clap surface. `--lane`/`--brief` became optional so `--template` could
/// stand alone; a bare `a2a spawn` must still be clap's usage error (2), and
/// `--template` with either of them is a conflict, not a silent precedence.
#[test]
fn the_usage_errors_survive_the_optional_lane_and_brief() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let brief = write_brief(tmp.path(), "do the thing\n");

    let bare = a2a(&home, tmp.path()).args(["spawn"]).output().unwrap();
    assert_eq!(bare.status.code(), Some(2), "a bare spawn is a usage error");

    let lane_only = a2a(&home, tmp.path())
        .args(["spawn", "--lane", "general"])
        .output()
        .unwrap();
    assert_eq!(
        lane_only.status.code(),
        Some(2),
        "--brief is still required"
    );

    let brief_only = a2a(&home, tmp.path())
        .args(["spawn", "--brief"])
        .arg(&brief)
        .output()
        .unwrap();
    assert_eq!(
        brief_only.status.code(),
        Some(2),
        "--lane is still required"
    );

    for conflicting in [
        vec!["spawn", "--template", "impl", "--lane", "impl"],
        vec!["spawn", "--template", "impl", "--brief", "/nope.md"],
    ] {
        let out = a2a(&home, tmp.path()).args(&conflicting).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{conflicting:?}");
    }

    let bad_lane = a2a(&home, tmp.path())
        .args(["spawn", "--template", "nonsense"])
        .output()
        .unwrap();
    assert_eq!(bad_lane.status.code(), Some(2), "--template takes a lane");

    assert_eq!(
        std::fs::read_dir(home.join("workers"))
            .map(Iterator::count)
            .unwrap_or(0),
        0,
        "no usage error left a worker behind"
    );
}
