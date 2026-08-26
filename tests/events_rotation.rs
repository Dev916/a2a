//! `events.jsonl` rotation (spec §4.5) — the housekeeping that must not be a
//! data-loss bug.
//!
//! The log leads and the snapshot follows: a rotation that breaks that ordering
//! costs a worker its history the next time anything rebuilds from zero. So the
//! centre of this file is not "the file got smaller", it is the **crash-safety
//! proof** — the same events folded over a rotated log and over an unrotated
//! one produce the same machine-meaningful snapshot, and the only differences
//! are the seq watermarks the markers themselves moved.
//!
//! The two transport cases have their own parameters on purpose. The
//! app-server flood is driven by `FAKE_FLOOD`, a fake-app-server knob; the exec
//! stub has no flood knob at all (five fixed lines, ~2 KiB), so its cap is
//! sized to the run it actually produces rather than borrowed from the other
//! case.

mod common;

use a2a::events::{Envelope, EventClass, EventKind, Transport};
use a2a::fold::{self, LivenessInput};
use a2a::store::{self, EventLog, EVENTS_FILE, EVENTS_ROTATED_FILE};
use serde_json::{json, Value};
use std::path::Path;
use std::time::{Duration, Instant};

/// The convergence window every poll in this file shares (house style).
const CONVERGE: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

// ---------------------------------------------------------------------------
// the crash-safety proof
// ---------------------------------------------------------------------------

/// One worker's history, built once and appended to both logs, so "the same N
/// envelopes" is a fact about the data rather than about two loops agreeing.
fn history(id: &str, cwd: &Path, filler: usize) -> Vec<Envelope> {
    let ts = chrono::DateTime::parse_from_rfc3339("2026-08-19T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let lifecycle = |raw: Value| {
        Envelope::new(
            1,
            Transport::AppServer,
            EventClass::Lifecycle,
            EventKind::StateChange,
            raw,
            ts,
        )
    };
    let mut out = vec![
        lifecycle(json!({
            "fsm": "Spawn",
            "id": id,
            "lane": "general",
            "cwd": cwd.to_string_lossy(),
            "brief_sha": "abc123",
            "sandbox": "workspace-write",
            "model": "gpt-5.5",
            "effort": "medium",
            "transport": "app-server",
            "max_turns": 7,
            "schema_path": "/tmp/schema.json",
            "created_at": "2026-08-19T00:00:00Z",
        })),
        lifecycle(
            json!({"fsm": "ProcessStarted", "pid": 4242, "start_witness": "w-1",
                         "thread_id": "thread-of-record"}),
        ),
        lifecycle(json!({"fsm": "SessionIdentified", "session_id": "sess-1"})),
    ];
    // The bulk: trace rows big enough that a modest cap is crossed exactly once.
    for i in 0..filler {
        out.push(Envelope::new(
            1,
            Transport::AppServer,
            EventClass::Trace,
            EventKind::AgentMessage,
            json!({"item": {"type": "agentMessage", "text": format!("{i}: {}", "y".repeat(160))}}),
            ts,
        ));
    }
    out
}

fn write_history(dir: &Path, events: &[Envelope], cap: u64) -> Vec<Envelope> {
    std::fs::create_dir_all(dir).unwrap();
    let mut log = EventLog::open_capped(dir, cap).unwrap();
    let mut written = vec![];
    for e in events {
        let mut e = e.clone();
        log.append(&mut e).unwrap();
        written.push(e);
    }
    written
}

/// **The point of the whole design.** The same history, folded from zero over a
/// log that rotated and over one that did not, rebuilds the same worker.
///
/// "Same" is not an operator — `WorkerSnapshot` derives no `PartialEq` and this
/// test does not add one. It is the JSON of both snapshots minus the keys that
/// *must* differ, and those are named below with the reason each is removed.
#[test]
fn a_rotated_log_and_an_unrotated_one_rebuild_the_same_worker() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("work");
    std::fs::create_dir_all(&cwd).unwrap();
    let dir_a = tmp.path().join("workers/w-rotated");
    let dir_b = tmp.path().join("workers/w-whole");

    let events = history("w-rotated", &cwd, 24);
    let one = serde_json::to_vec(&events[4]).unwrap().len() as u64 + 1;
    // Sized for EXACTLY one rotation: ~2/3 of the way through the history, with
    // the remainder well inside the fresh generation. A second rotation would
    // discard the oldest generation, and the "gapless from 1" assertion below
    // would quietly become a tautology over whatever survived.
    let cap = one * 18;

    let written_a = write_history(&dir_a, &events, cap);
    let _written_b = write_history(&dir_b, &events, 0);

    assert!(
        dir_a.join(EVENTS_ROTATED_FILE).exists(),
        "the capped log rotated"
    );
    assert!(
        !dir_b.join(EVENTS_ROTATED_FILE).exists(),
        "the uncapped one did not"
    );

    // A rebuild from zero is what this proves, so there is no checkpoint to
    // read: remove any `state.json` before folding (neither run wrote one —
    // `EventLog::append` is the WAL, not the checkpoint — so this is the
    // statement, not the work).
    for dir in [&dir_a, &dir_b] {
        let _ = std::fs::remove_file(dir.join("state.json"));
        assert!(!dir.join("state.json").exists());
    }

    let now = chrono::Utc::now();
    let snap_a = fold::recover(
        &dir_a,
        LivenessInput {
            process_alive: true,
        },
        now,
    )
    .unwrap();
    let snap_b = fold::recover(
        &dir_b,
        LivenessInput {
            process_alive: true,
        },
        now,
    )
    .unwrap();

    let all_a = store::read_events(&dir_a, 0).unwrap();
    let markers: Vec<&Envelope> = all_a
        .iter()
        .filter(|e| e.kind == EventKind::EventsRotated)
        .collect();
    let m = markers.len() as u64;
    assert_eq!(m, 1, "exactly one rotation, or the proof below is weaker");
    assert_eq!(
        markers[0].raw["discarded_prior_generation"],
        json!(false),
        "nothing was lost, so nothing claims to have been"
    );
    assert_eq!(
        markers[0].raw["last_rotated_seq"],
        json!(markers[0].seq - 1),
        "the marker names the last seq that moved aside"
    );

    // The removed set, and why each one MUST differ rather than merely may:
    //   last_event_seq / last_applied_seq — the markers occupy their own seqs,
    //     so the rotated fold has consumed exactly `m` more of them (asserted
    //     below, not waived);
    //   updated_at — wall clock, stamped per fold.
    let strip = |snap: &a2a::state::WorkerSnapshot| {
        let mut v = serde_json::to_value(snap).unwrap();
        let o = v.as_object_mut().unwrap();
        for k in ["last_event_seq", "last_applied_seq", "updated_at"] {
            o.remove(k);
        }
        v
    };
    assert_eq!(
        strip(&snap_a),
        strip(&snap_b),
        "a rotation is invisible to the machine"
    );

    // The watermarks differ by EXACTLY the number of markers.
    assert_eq!(snap_a.last_event_seq, snap_b.last_event_seq + m);
    assert_eq!(snap_a.last_applied_seq, snap_b.last_applied_seq + m);

    // Spot-checks in the machine's own vocabulary, so a failure reads as a
    // worker fact rather than as a JSON diff.
    assert_eq!(snap_a.id, "w-rotated");
    assert_eq!(snap_a.session_id.as_deref(), Some("sess-1"));
    assert_eq!(snap_a.thread_id.as_deref(), Some("thread-of-record"));
    assert_eq!(snap_a.pid, Some(4242));
    assert_eq!(snap_a.max_turns, Some(7));
    assert_eq!(snap_a.epoch, snap_b.epoch);
    assert_eq!(snap_a.state, snap_b.state);

    // Neither fold rejected anything: the marker carries no `fsm` key, so it
    // consumes its seq and changes nothing.
    for (tag, snap) in [("rotated", &snap_a), ("whole", &snap_b)] {
        assert!(
            snap.extra.get("fold_rejections").is_none(),
            "{tag}: {:?}",
            snap.extra.get("fold_rejections")
        );
    }

    // And the log the rotated worker still has is one contiguous history.
    let seqs: Vec<u64> = all_a.iter().map(|e| e.seq).collect();
    assert_eq!(seqs.first(), Some(&1), "starts at 1: {seqs:?}");
    assert!(
        seqs.windows(2).all(|w| w[1] == w[0] + 1),
        "strictly increasing and gapless: {seqs:?}"
    );
    assert_eq!(
        seqs.len() as u64,
        written_a.len() as u64 + m,
        "N events + M markers, all of them readable"
    );
}

// ---------------------------------------------------------------------------
// app-server transport
// ---------------------------------------------------------------------------

fn a2a_bin() -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin("a2a")
}

/// `a2a --home <home> …` with the broker environment (no inherited `A2A_HOME`,
/// the scripted app-server as the codex binary).
fn a2a_appserver(home: &Path, allowed_root: &Path, args: &[&str]) -> std::process::Command {
    let mut c = std::process::Command::new(a2a_bin());
    c.arg("--home")
        .arg(home)
        .args(args)
        .env_remove("A2A_HOME")
        .env_remove("A2A_BROKER_DISABLE")
        .env("A2A_CODEX_BIN", common::fake_app_server_path())
        .env("A2A_ALLOWED_ROOTS", allowed_root)
        .env("A2A_OWNER", common::TEST_OWNER)
        .env("A2A_BROKER_BIN", a2a_bin())
        .env("A2A_SHIM_BIN", a2a_bin());
    c
}

fn run(mut cmd: std::process::Command) -> (i32, String, String) {
    let out = cmd.output().expect("the a2a binary runs");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn json_of(stdout: &str) -> Value {
    serde_json::from_str(stdout).unwrap_or_else(|e| panic!("one JSON envelope ({e}): {stdout:?}"))
}

fn until_state(home: &Path, id: &str, want: &str) {
    let path = home.join("workers").join(id).join("state.json");
    let deadline = Instant::now() + CONVERGE;
    let mut last = String::new();
    loop {
        if let Ok(text) = std::fs::read_to_string(&path) {
            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                last = v["state"].as_str().unwrap_or_default().to_string();
                if last == want {
                    return;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "worker {id} never reached {want} (last: {last})"
        );
        std::thread::sleep(POLL);
    }
}

fn marker_in(dir: &Path) -> Vec<Envelope> {
    store::read_events(dir, 0)
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == EventKind::EventsRotated)
        .collect()
}

/// The real thing, on the transport that can actually flood: a worker whose
/// app-server child emits a burst of agent messages crosses an 8 KiB cap
/// exactly once, and everything a reader can ask afterwards still answers.
///
/// `FAKE_FLOOD` items are `item/completed{agentMessage}` — **lifecycle**, not
/// trace — so each one is a `persist` round trip through the actor. Lifecycle
/// is what fills a log; the pair (8192, 30) is sized so total appended bytes
/// land between 1x and 2x the cap, which is what makes "exactly one rotation"
/// true and keeps the genesis row readable in `.1`.
#[test]
fn an_app_server_flood_rotates_once_and_every_reader_spans_the_split() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let guard = common::BrokerGuard::new(&home);

    let brief = common::write_brief(tmp.path(), "do the thing\n");
    let mut cmd = a2a_appserver(
        &home,
        tmp.path(),
        &[
            "--json",
            "spawn",
            "--lane",
            "general",
            "--transport",
            "app-server",
            "--cwd",
        ],
    );
    cmd.arg(&work)
        .arg("--brief")
        .arg(&brief)
        .env("FAKE_SCENARIO", "happy")
        .env("FAKE_FLOOD", "30")
        .env("A2A_BROKER_EVENTS_MAX_BYTES", "8192");
    let (code, stdout, stderr) = run(cmd);
    assert_eq!(code, 0, "spawn: {stderr}");
    let id = json_of(&stdout)["data"]["id"].as_str().unwrap().to_string();

    until_state(&home, &id, "done");
    let dir = home.join("workers").join(&id);

    // Exactly one rotation, and it says nothing was discarded.
    assert!(
        dir.join(EVENTS_ROTATED_FILE).exists(),
        "the flood crossed the cap"
    );
    let markers = marker_in(&dir);
    assert_eq!(
        markers.len(),
        1,
        "exactly one rotation — retune FAKE_FLOOD, never the assertions"
    );
    assert_eq!(markers[0].raw["discarded_prior_generation"], json!(false));
    assert_eq!(markers[0].raw["rotated_to"], json!("events.jsonl.1"));
    let split_at = markers[0].seq;
    assert_eq!(markers[0].raw["last_rotated_seq"], json!(split_at - 1));

    // `tail --raw` spans the split: strictly increasing, gapless, from 1.
    let (code, stdout, stderr) = run(a2a_appserver(
        &home,
        tmp.path(),
        &["--json", "tail", &id, "--raw"],
    ));
    assert_eq!(code, 0, "tail: {stderr}");
    let seqs: Vec<u64> = json_of(&stdout)["data"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs.first(), Some(&1), "the whole history: {seqs:?}");
    assert!(
        seqs.windows(2).all(|w| w[1] == w[0] + 1),
        "gapless across the split: {seqs:?}"
    );
    assert!(
        seqs.contains(&split_at) && *seqs.last().unwrap() > split_at,
        "the split is INSIDE the readable window: {seqs:?}"
    );

    // `--since <a seq before the split>` returns exactly what follows it.
    let since = split_at / 2;
    assert!(since >= 1, "there is a before-the-split seq to ask about");
    let (code, stdout, stderr) = run(a2a_appserver(
        &home,
        tmp.path(),
        &[
            "--json",
            "tail",
            &id,
            "--raw",
            "--since",
            &since.to_string(),
        ],
    ));
    assert_eq!(code, 0, "tail --since: {stderr}");
    let after: Vec<u64> = json_of(&stdout)["data"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(
        after,
        seqs.iter()
            .copied()
            .filter(|s| *s > since)
            .collect::<Vec<_>>(),
        "--since crosses the file boundary like it was never there"
    );

    // The genesis row is still readable — in `.1`, which is the whole reason a
    // single rotation is survivable.
    let rotated = std::fs::read_to_string(dir.join(EVENTS_ROTATED_FILE)).unwrap();
    let first: Value = serde_json::from_str(rotated.lines().next().unwrap()).unwrap();
    assert_eq!(first["seq"], json!(1));
    assert_eq!(first["raw"]["fsm"], json!("Spawn"), "genesis survived");
    assert!(
        !std::fs::read_to_string(dir.join(EVENTS_FILE))
            .unwrap()
            .contains(r#""fsm":"Spawn""#),
        "and it is not in the live file"
    );

    // The fold survived the split.
    let (code, stdout, stderr) = run(a2a_appserver(&home, tmp.path(), &["--json", "status", &id]));
    assert_eq!(code, 0, "status: {stderr}");
    assert_eq!(json_of(&stdout)["data"]["state"], json!("done"));

    drop(guard);
    common::assert_no_broker_left(&home);
}

// ---------------------------------------------------------------------------
// exec transport parity
// ---------------------------------------------------------------------------

/// The same rotation, on the transport that reaches the log through
/// `reconcile` instead of through the actor — pinned by test rather than by the
/// argument that "it is the same `store`".
///
/// Its parameters are its OWN: the stub codex has no flood knob (five fixed
/// lines for a whole run), so the cap is 1024 rather than the app-server case's
/// 8192.
///
/// Measured, because the estimate that picked 1024 was low: a stub run is
/// ~2.6 KiB of `events.jsonl`, so it crosses a 1 KiB cap **twice**, not once.
/// The cap is the pinned value and stays; the assertions are therefore written
/// as the single-generation contract itself — the readable window is one
/// contiguous run of seqs, and it either starts at 1 or a surviving marker says
/// `discarded_prior_generation: true`. Both are true statements about a log
/// that keeps one generation; only their disjunction is stable against a
/// fixture whose output size drifts.
#[test]
fn an_exec_worker_rotates_through_reconcile_and_stays_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let brief = common::write_brief(tmp.path(), "do the thing\n");

    let out = common::a2a(&home, tmp.path())
        .env("A2A_BROKER_EVENTS_MAX_BYTES", "1024")
        .args(["--json", "spawn", "--lane", "general", "--cwd"])
        .arg(&work)
        .arg("--brief")
        .arg(&brief)
        .assert()
        .success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(
        v["data"]["transport"],
        json!("exec"),
        "no broker, no socket"
    );
    let id = v["data"]["id"].as_str().unwrap().to_string();

    // `wait` is what drives `reconcile`, which is the capped writer here.
    common::a2a(&home, tmp.path())
        .env("A2A_BROKER_EVENTS_MAX_BYTES", "1024")
        .args(["--json", "wait", &id, "--timeout", "30s"])
        .assert()
        .success();

    let dir = home.join("workers").join(&id);
    assert!(
        dir.join(EVENTS_ROTATED_FILE).exists(),
        "the stub's own ~2 KiB crossed a 1 KiB cap"
    );
    let markers = marker_in(&dir);
    assert!(!markers.is_empty(), "and said so");
    assert_eq!(
        markers[0].transport,
        Transport::Exec,
        "the trigger's transport"
    );

    let out = common::a2a(&home, tmp.path())
        .args(["--json", "tail", &id, "--raw"])
        .assert()
        .success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let seqs: Vec<u64> = v["data"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["seq"].as_u64().unwrap())
        .collect();
    assert!(
        seqs.windows(2).all(|w| w[1] == w[0] + 1),
        "gapless and strictly increasing across the split: {seqs:?}"
    );
    assert!(
        seqs.contains(&markers[0].seq),
        "the marker is in the readable window: {seqs:?}"
    );
    // Single-generation retention, observed on the exec path: whatever is
    // readable is either the whole history or a window whose start a marker
    // accounts for. A window that just *begins* somewhere, with nothing saying
    // why, is the failure this rules out.
    let discarded = markers
        .iter()
        .any(|m| m.raw["discarded_prior_generation"] == json!(true));
    assert_eq!(
        seqs[0] == 1,
        !discarded,
        "either the log starts at 1 or a marker says a generation went: {seqs:?}"
    );
    for m in &markers {
        assert_eq!(m.raw["rotated_to"], json!("events.jsonl.1"));
        assert_eq!(m.raw["last_rotated_seq"], json!(m.seq - 1));
        assert_eq!(m.class, EventClass::Lifecycle);
        assert!(m.raw.get("fsm").is_none(), "bookkeeping, not an FSM edge");
    }

    // Coherent, not merely alive: the fold that read across the split reached a
    // terminal state and `status` agrees with it.
    let out = common::a2a(&home, tmp.path())
        .args(["--json", "status", &id])
        .assert()
        .success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(v["data"]["state"], json!("done"), "{v}");
    assert!(
        v["data"].get("fold_rejections").is_none(),
        "the marker drove nothing: {v}"
    );
}
