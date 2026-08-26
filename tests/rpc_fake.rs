// tests/rpc_fake.rs
//
// Round-trip tests for `a2a::rpc::Client` against `tests/fixtures/fake-app-server`,
// plus the fidelity contract (spec §8.3): every notification the fake emitted and
// every request the client sent is validated against `schema/v2-snapshot/` in the
// same run, so a drifted fake fails the suite instead of passing quietly.
//
// Async assertions here are converged-state assertions with explicit bounded
// timeouts — never sleeps-and-hope, and never `tokio::time::pause` (there is a
// real child process doing real I/O in every one of these).

#[path = "common/fidelity.rs"]
mod fidelity;

use a2a::adapter::{self, AdapterRef, SendPath, TurnSpec};
use a2a::config::{LaneConfig, SandboxMode};
use a2a::protocol::{
    InitializeParams, InitializeResponse, RpcNotification, SandboxModeParam, SandboxPolicy,
    ThreadResumeParams, ThreadStartParams, ThreadStartResponse, TurnInterruptParams,
    TurnStartParams, TurnStartResponse, TurnStatus, TurnSteerParams, UserInput,
};
use a2a::responder::{self, Disposition};
use a2a::rpc::{Client, Incoming, RpcError, UNPARSEABLE_METHOD};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::process::Child;

/// Per-request budget. Generous: these tests assert *which* error comes back,
/// never how fast a healthy call is.
const RPC: Duration = Duration::from_secs(10);
/// Bounded wait for a converged state (a notification arriving).
const WAIT: Duration = Duration::from_secs(10);
/// Window in which a scenario promises that *nothing* happens.
const QUIET: Duration = Duration::from_millis(400);

fn fake_bin() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-app-server")
}

fn state_dir(root: &Path) -> PathBuf {
    root.join("state")
}
fn transcript(root: &Path) -> PathBuf {
    root.join("transcript.jsonl")
}
fn work_dir(root: &Path) -> PathBuf {
    let p = root.join("work");
    std::fs::create_dir_all(&p).unwrap();
    p
}

struct Fake {
    client: Client,
    incoming: Incoming,
    child: Child,
}

impl Fake {
    /// Launches the fixture against a caller-owned `root` — relaunching with the
    /// same root is how the `$FAKE_STATE`-persistence scenarios are driven.
    async fn start(root: &Path, scenario: &str, extra: &[(&str, &str)]) -> Fake {
        std::fs::create_dir_all(state_dir(root)).unwrap();
        let mut env: Vec<(String, String)> = vec![
            ("FAKE_SCENARIO".into(), scenario.into()),
            (
                "FAKE_STATE".into(),
                state_dir(root).to_string_lossy().into_owned(),
            ),
            (
                "FAKE_TRANSCRIPT".into(),
                transcript(root).to_string_lossy().into_owned(),
            ),
        ];
        env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        let (client, incoming, child) =
            Client::spawn(&fake_bin(), &[], &env, &root.join("stderr.log"))
                .await
                .expect("spawn fake-app-server");
        Fake {
            client,
            incoming,
            child,
        }
    }

    async fn init(&self) -> InitializeResponse {
        self.client
            .call(
                "initialize",
                &InitializeParams::for_a2a("a2a", "0.1.0"),
                RPC,
            )
            .await
            .expect("initialize")
    }

    async fn start_thread(&self, cwd: &Path) -> ThreadStartResponse {
        self.client
            .call(
                "thread/start",
                &ThreadStartParams {
                    cwd: Some(cwd.to_path_buf()),
                    sandbox: Some(SandboxModeParam::WorkspaceWrite),
                    ..Default::default()
                },
                RPC,
            )
            .await
            .expect("thread/start")
    }

    async fn start_turn(&self, thread_id: &str, text: &str) -> TurnStartResponse {
        let mut p = TurnStartParams::new(thread_id, vec![UserInput::text(text)]);
        p.sandbox_policy = Some(SandboxPolicy::WorkspaceWrite {
            writable_roots: vec![],
            network_access: false,
            exclude_slash_tmp: false,
            exclude_tmpdir_env_var: false,
        });
        self.client
            .call("turn/start", &p, RPC)
            .await
            .expect("turn/start")
    }

    /// The next notification, or a panic naming the timeout — never an
    /// unbounded await.
    async fn next_note(&mut self) -> RpcNotification {
        match tokio::time::timeout(WAIT, self.incoming.notifications.recv()).await {
            Ok(Some(n)) => n,
            Ok(None) => panic!("notification stream closed while a notification was expected"),
            Err(_) => panic!("no notification within {WAIT:?}"),
        }
    }

    /// Collects notification methods until `terminal` (inclusive).
    async fn notes_until(&mut self, terminal: &str) -> Vec<String> {
        let mut seen = Vec::new();
        loop {
            let n = self.next_note().await;
            let m = n.method.clone();
            seen.push(m.clone());
            if m == terminal {
                return seen;
            }
        }
    }

    /// Asserts the scenario stays silent for [`QUIET`].
    async fn assert_quiet(&mut self) {
        if let Ok(got) = tokio::time::timeout(QUIET, self.incoming.notifications.recv()).await {
            panic!("expected silence, got {got:?}");
        }
    }

    async fn shutdown(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }

    /// Splits the fake into the handle the WorkerActor holds ([`AdapterRef`])
    /// and the halves the demux keeps. `Arc<Client>` with no interior lock is
    /// exactly what the actor discipline allows.
    fn into_adapter(self) -> (AdapterRef, Incoming, Child) {
        let Fake {
            client,
            incoming,
            child,
        } = self;
        (AdapterRef::new(Arc::new(client)), incoming, child)
    }
}

fn root() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

// ---------------------------------------------------------------------------
// lifecycle + fidelity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn happy_scenario_streams_the_lifecycle_in_order_and_matches_the_schemas() {
    let root = root();
    let mut f = Fake::start(root.path(), "happy", &[]).await;

    let init = f.init().await;
    assert_eq!(init.platform_os, "macos");

    let ts = f.start_thread(&work_dir(root.path())).await;
    assert_eq!(
        ts.thread.id, ts.thread.session_id,
        "thread.id must equal thread.sessionId"
    );

    let turn = f.start_turn(&ts.thread.id, "do the thing").await;
    assert_eq!(turn.turn.status, TurnStatus::InProgress);

    let seen = f.notes_until("turn/completed").await;
    assert_eq!(
        seen,
        vec![
            "thread/started",
            "turn/started",
            "item/completed",
            "turn/completed"
        ],
        "lifecycle notifications out of order"
    );
    f.shutdown().await;

    let audit = fidelity::audit(&transcript(root.path()));
    audit.assert_covers(
        &[
            "thread/started",
            "turn/started",
            "item/completed",
            "turn/completed",
        ],
        &["initialize", "thread/start", "turn/start"],
    );
}

#[tokio::test]
async fn item_completed_carries_the_agent_message_text() {
    let root = root();
    let mut f = Fake::start(root.path(), "happy", &[("FAKE_MESSAGE", "shipped it")]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;
    f.start_turn(&ts.thread.id, "go").await;

    let item = loop {
        let n = f.next_note().await;
        if n.method == "item/completed" {
            break n;
        }
    };
    assert_eq!(item.params["item"]["type"], "agentMessage");
    assert_eq!(item.params["item"]["text"], "shipped it");
    assert_eq!(item.thread_id(), Some(ts.thread.id.as_str()));
    f.shutdown().await;
}

// ---------------------------------------------------------------------------
// correlation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_in_flight_requests_correlate_by_id_not_by_arrival_order() {
    let root = root();
    // The fake holds the `thread/loaded/list` answer back until it has served
    // the NEXT request, so the two responses come back reversed on the wire.
    let f = Fake::start(
        root.path(),
        "happy",
        &[("FAKE_DEFER_METHODS", "thread/loaded/list")],
    )
    .await;
    f.init().await;

    let work = work_dir(root.path());
    let empty = json!({});
    let start_params = ThreadStartParams {
        cwd: Some(work.clone()),
        sandbox: Some(SandboxModeParam::ReadOnly),
        ..Default::default()
    };
    let probe = f.client.call::<_, Value>("thread/loaded/list", &empty, RPC);
    let start = f
        .client
        .call::<_, ThreadStartResponse>("thread/start", &start_params, RPC);
    let (probe, start) = tokio::join!(probe, start);

    let probe = probe.expect("thread/loaded/list");
    assert_eq!(probe, json!({"data": [], "nextCursor": null}));
    let start = start.expect("thread/start");
    assert_eq!(start.cwd, work);

    f.shutdown().await;

    // And prove the wire really did answer out of order.
    let raw = std::fs::read_to_string(transcript(root.path())).unwrap();
    let mut probe_id = None;
    let mut start_id = None;
    let mut response_order = Vec::new();
    for line in raw.lines() {
        let rec: Value = serde_json::from_str(line).unwrap();
        let frame = &rec["frame"];
        match (
            rec["dir"].as_str(),
            frame.get("method").and_then(|m| m.as_str()),
        ) {
            (Some("in"), Some("thread/loaded/list")) => probe_id = frame["id"].as_u64(),
            (Some("in"), Some("thread/start")) => start_id = frame["id"].as_u64(),
            (Some("out"), None) => {
                if let Some(id) = frame.get("id").and_then(|i| i.as_u64()) {
                    response_order.push(id);
                }
            }
            _ => {}
        }
    }
    let (probe_id, start_id) = (probe_id.unwrap(), start_id.unwrap());
    assert!(probe_id < start_id, "the probe was sent first");
    let positions: Vec<usize> = [probe_id, start_id]
        .iter()
        .map(|id| response_order.iter().position(|r| r == id).unwrap())
        .collect();
    assert!(
        positions[0] > positions[1],
        "expected the deferred response to arrive last, got order {response_order:?}"
    );
}

#[tokio::test]
async fn a_timed_out_request_does_not_poison_the_client() {
    let root = root();
    let f = Fake::start(
        root.path(),
        "happy",
        &[("FAKE_DEFER_METHODS", "thread/loaded/list")],
    )
    .await;
    f.init().await;

    let err = f
        .client
        .call::<_, Value>("thread/loaded/list", &json!({}), Duration::from_millis(200))
        .await
        .expect_err("deferred response must time out");
    assert_eq!(err, RpcError::Timeout);

    // The late answer arrives with an id nobody is waiting on; the client must
    // absorb it and keep serving.
    let ts = f.start_thread(&work_dir(root.path())).await;
    assert!(!ts.thread.id.is_empty());
    f.shutdown().await;
}

// ---------------------------------------------------------------------------
// framing edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unparseable_lines_surface_as_synthetic_notifications() {
    let root = root();
    let mut f = Fake::start(root.path(), "happy", &[("FAKE_EMIT_UNPARSEABLE", "1")]).await;

    let n = f.next_note().await;
    assert_eq!(n.method, UNPARSEABLE_METHOD);
    assert_eq!(
        n.params["line"].as_str().unwrap(),
        "this line is not json {{{"
    );
    assert_eq!(n.thread_id(), None);

    // Garbage is not fatal: the very next line is a real handshake.
    let init = f.init().await;
    assert_eq!(init.platform_family, "unix");
    f.shutdown().await;
}

#[tokio::test]
async fn unknown_notification_methods_are_delivered_not_dropped() {
    let root = root();
    let mut f = Fake::start(root.path(), "bad-vocabulary", &[]).await;
    f.init().await;

    let n = f.next_note().await;
    assert_eq!(n.method, "sorcery/unicornSummoned");
    assert_eq!(n.thread_id(), None);
    assert_eq!(n.params["horn"], "spiral");
    f.shutdown().await;
}

// ---------------------------------------------------------------------------
// child death
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crash_mid_turn_resolves_child_died_and_the_relaunch_resumes() {
    let root = root();
    let mut f = Fake::start(root.path(), "crash-mid-turn", &[]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;
    let thread_id = ts.thread.id.clone();
    f.start_turn(&thread_id, "go").await;

    assert_eq!(
        f.notes_until("turn/started").await.last().unwrap(),
        "turn/started"
    );
    // The child is gone: the stream ends rather than hanging.
    match tokio::time::timeout(WAIT, f.incoming.notifications.recv()).await {
        Ok(None) => {}
        other => panic!("expected the notification stream to close, got {other:?}"),
    }

    let t0 = Instant::now();
    let err = f
        .client
        .call::<_, Value>("thread/loaded/list", &json!({}), RPC)
        .await
        .expect_err("a call after the child died must fail");
    assert_eq!(err, RpcError::ChildDied);
    assert!(
        t0.elapsed() < RPC / 2,
        "ChildDied must resolve immediately, not by timeout ({:?})",
        t0.elapsed()
    );
    f.shutdown().await;

    // Second launch: same $FAKE_STATE, so the thread is still known and the
    // scenario behaves like `happy`.
    let mut f = Fake::start(root.path(), "crash-mid-turn", &[]).await;
    f.init().await;
    let resumed: ThreadStartResponse = f
        .client
        .call(
            "thread/resume",
            &ThreadResumeParams {
                thread_id: thread_id.clone(),
                cwd: None,
                sandbox: None,
                approval_policy: None,
                model: None,
                config: None,
                developer_instructions: None,
            },
            RPC,
        )
        .await
        .expect("thread/resume after crash");
    assert_eq!(resumed.thread.id, thread_id);

    f.start_turn(&thread_id, "again").await;
    let seen = f.notes_until("turn/completed").await;
    assert!(seen.contains(&"item/completed".to_string()), "got {seen:?}");
    f.shutdown().await;

    let audit = fidelity::audit(&transcript(root.path()));
    audit.assert_covers(&["turn/started", "turn/completed"], &["thread/resume"]);
}

#[tokio::test]
async fn always_crash_fails_the_next_call_with_child_died() {
    let root = root();
    let f = Fake::start(root.path(), "always-crash", &[]).await;
    f.init().await; // answered, then the child exits

    let err = f
        .client
        .call::<_, Value>("thread/loaded/list", &json!({}), RPC)
        .await
        .expect_err("the child is gone");
    assert_eq!(err, RpcError::ChildDied);
    f.shutdown().await;
}

// ---------------------------------------------------------------------------
// server→client requests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_requests_are_routed_and_the_turn_waits_for_the_answer() {
    let root = root();
    let mut f = Fake::start(root.path(), "server-request", &[]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;
    f.start_turn(&ts.thread.id, "rm -rf /").await;
    assert_eq!(
        f.notes_until("turn/started").await.last().unwrap(),
        "turn/started"
    );

    let req = tokio::time::timeout(WAIT, f.incoming.server_requests.recv())
        .await
        .expect("server request within the wait")
        .expect("server request channel open");
    assert_eq!(req.method, "execCommandApproval");
    assert_eq!(req.params["command"][0], "rm");
    assert!(!req.id.is_null());

    // The turn is blocked until a2a answers.
    f.assert_quiet().await;

    f.client
        .respond(req.id.clone(), Ok(json!({"decision": "denied"})))
        .await
        .expect("respond");

    let seen = f.notes_until("turn/completed").await;
    assert_eq!(seen, vec!["item/completed", "turn/completed"]);
    f.shutdown().await;
}

#[tokio::test]
async fn an_errored_unknown_server_request_keeps_the_connection_usable() {
    let root = root();
    let mut f = Fake::start(root.path(), "unknown-request", &[]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;
    f.start_turn(&ts.thread.id, "go").await;

    let req = tokio::time::timeout(WAIT, f.incoming.server_requests.recv())
        .await
        .expect("server request within the wait")
        .expect("server request channel open");
    assert_eq!(req.method, "sorcery/requestUnicorn");

    f.client
        .respond(
            req.id.clone(),
            Err((-32601, "unsupported request method".to_string())),
        )
        .await
        .expect("respond with a JSON-RPC error");

    let seen = f.notes_until("turn/completed").await;
    assert!(seen.contains(&"turn/completed".to_string()));

    // Still usable afterwards.
    let probe: Value = f
        .client
        .call("thread/loaded/list", &json!({}), RPC)
        .await
        .expect("connection still usable");
    assert_eq!(probe["data"], json!([]));
    f.shutdown().await;
}

/// The same exchange, driven by the real `responder` table rather than a
/// hand-written payload: the deny that unblocks the turn is the one production
/// will send, and it is produced well inside the 100ms budget.
#[tokio::test]
async fn the_responder_denies_the_exec_approval_and_only_then_does_the_turn_complete() {
    let root = root();
    let mut f = Fake::start(root.path(), "server-request", &[]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;
    f.start_turn(&ts.thread.id, "rm -rf /").await;
    assert_eq!(
        f.notes_until("turn/started").await.last().unwrap(),
        "turn/started"
    );

    let req = tokio::time::timeout(WAIT, f.incoming.server_requests.recv())
        .await
        .expect("server request within the wait")
        .expect("server request channel open");
    assert_eq!(req.method, "execCommandApproval");

    // Nothing moves until a2a answers.
    f.assert_quiet().await;

    let t0 = Instant::now();
    let answer = responder::respond_to(&req);
    f.client
        .respond(req.id.clone(), answer.payload.clone())
        .await
        .expect("respond");
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "answering must fit the 100ms budget, took {elapsed:?}"
    );

    assert_eq!(answer.method, "execCommandApproval");
    assert_eq!(answer.disposition, Disposition::Denied);
    assert_eq!(answer.payload, Ok(json!({"decision": "denied"})));
    // What the caller logs as a lifecycle envelope: the pair, not the command.
    assert_eq!(
        answer.raw(),
        json!({"method": "execCommandApproval", "disposition": "denied"})
    );

    let seen = f.notes_until("turn/completed").await;
    assert_eq!(seen, vec!["item/completed", "turn/completed"]);
    f.shutdown().await;
}

/// An unknown server→client request must still be answered — an error frame is
/// an answer, a dropped frame is a permanent hang — and the connection must
/// survive it.
#[tokio::test]
async fn the_responder_errors_an_unknown_server_request_and_the_connection_stays_usable() {
    let root = root();
    let mut f = Fake::start(root.path(), "unknown-request", &[]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;
    f.start_turn(&ts.thread.id, "go").await;

    let req = tokio::time::timeout(WAIT, f.incoming.server_requests.recv())
        .await
        .expect("server request within the wait")
        .expect("server request channel open");
    assert_eq!(req.method, "sorcery/requestUnicorn");

    let t0 = Instant::now();
    let answer = responder::respond_to(&req);
    assert_eq!(answer.disposition, Disposition::UnknownMethod);
    let (code, message) = answer
        .payload
        .clone()
        .expect_err("an unknown method has no payload to send");
    assert_eq!(code, responder::METHOD_NOT_FOUND);
    assert!(message.contains("sorcery/requestUnicorn"), "got {message}");
    f.client
        .respond(req.id.clone(), answer.payload.clone())
        .await
        .expect("respond with a JSON-RPC error");
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "an unknown method must be errored within 100ms, took {elapsed:?}"
    );

    let seen = f.notes_until("turn/completed").await;
    assert!(seen.contains(&"turn/completed".to_string()), "got {seen:?}");

    let probe: Value = f
        .client
        .call("thread/loaded/list", &json!({}), RPC)
        .await
        .expect("connection still usable after an errored server request");
    assert_eq!(probe["data"], json!([]));
    f.shutdown().await;
}

// ---------------------------------------------------------------------------
// scenario pins the later tasks depend on
// ---------------------------------------------------------------------------

#[tokio::test]
async fn blocked_scenario_writes_the_question_into_the_thread_cwd_before_completing() {
    let root = root();
    let mut f = Fake::start(root.path(), "blocked", &[]).await;
    f.init().await;
    let work = work_dir(root.path());
    let ts = f.start_thread(&work).await;
    f.start_turn(&ts.thread.id, "ask me").await;

    let seen = f.notes_until("turn/completed").await;
    // The file must already be there the moment the turn completes — the fake
    // writes it *before* turn/completed, which is what the blocked-worker fold
    // in later tasks relies on.
    let q = work.join("outbox/1.question.json");
    let body: Value = serde_json::from_str(&std::fs::read_to_string(&q).unwrap()).unwrap();
    assert_eq!(
        body,
        json!({"seq": 1, "question": "A or B?", "blocking": true})
    );
    assert!(seen.contains(&"turn/completed".to_string()));
    f.shutdown().await;

    fidelity::audit(&transcript(root.path())).assert_covers(&["turn/completed"], &["thread/start"]);
}

#[tokio::test]
async fn steer_race_rejects_the_first_steer_then_a_new_turn_succeeds() {
    let root = root();
    let mut f = Fake::start(root.path(), "steer-race", &[]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;
    let turn = f.start_turn(&ts.thread.id, "go").await;

    let err = f
        .client
        .call::<_, Value>(
            "turn/steer",
            &TurnSteerParams {
                thread_id: ts.thread.id.clone(),
                expected_turn_id: turn.turn.id.clone(),
                input: vec![UserInput::text("actually, stop")],
            },
            RPC,
        )
        .await
        .expect_err("the first steer loses the race");
    // The fake speaks the REAL 0.140.0 rejection (transcribed in Task 18), so
    // this assertion is a fidelity pin as much as a behaviour one: the message
    // is what `adapter::is_steer_precondition_failure` routes on, and the code
    // is the server's catch-all invalid-request code rather than a dedicated
    // one.
    assert_eq!(
        err,
        RpcError::Remote {
            code: adapter::STEER_PRECONDITION_CODE,
            message: "no active turn to steer".to_string(),
        }
    );
    assert!(
        adapter::is_steer_precondition_failure(&err),
        "the fake's rejection must be the one the predicate knows: {err:?}"
    );

    // A fresh turn still works.
    f.start_turn(&ts.thread.id, "try again").await;
    let seen = f.notes_until("turn/completed").await;
    assert!(seen.contains(&"turn/completed".to_string()));
    f.shutdown().await;

    fidelity::audit(&transcript(root.path())).assert_covers(&[], &["turn/steer"]);
}

#[tokio::test]
async fn hang_turn_holds_the_turn_open_until_interrupted() {
    let root = root();
    let mut f = Fake::start(root.path(), "hang-turn", &[]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;
    let turn = f.start_turn(&ts.thread.id, "spin forever").await;

    assert_eq!(
        f.notes_until("turn/started").await.last().unwrap(),
        "turn/started"
    );
    f.assert_quiet().await; // it never completes on its own

    let _: Value = f
        .client
        .call(
            "turn/interrupt",
            &TurnInterruptParams {
                thread_id: ts.thread.id.clone(),
                turn_id: turn.turn.id.clone(),
            },
            RPC,
        )
        .await
        .expect("turn/interrupt");

    let n = f.next_note().await;
    assert_eq!(n.method, "turn/completed");
    assert_eq!(n.params["turn"]["status"], "interrupted");
    f.shutdown().await;

    fidelity::audit(&transcript(root.path()))
        .assert_covers(&["turn/completed"], &["turn/interrupt"]);
}

#[tokio::test]
async fn hang_turn_completes_when_steered() {
    let root = root();
    let mut f = Fake::start(root.path(), "hang-turn", &[]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;
    let turn = f.start_turn(&ts.thread.id, "spin").await;
    assert_eq!(
        f.notes_until("turn/started").await.last().unwrap(),
        "turn/started"
    );

    let _: Value = f
        .client
        .call(
            "turn/steer",
            &TurnSteerParams {
                thread_id: ts.thread.id.clone(),
                expected_turn_id: turn.turn.id.clone(),
                input: vec![UserInput::text("do this instead")],
            },
            RPC,
        )
        .await
        .expect("turn/steer");

    let seen = f.notes_until("turn/completed").await;
    assert_eq!(seen, vec!["item/completed", "turn/completed"]);
    f.shutdown().await;
}

#[tokio::test]
async fn resume_fail_scenario_reports_a_remote_error() {
    let root = root();
    let f = Fake::start(root.path(), "resume-fail", &[]).await;
    f.init().await;
    let ts = f.start_thread(&work_dir(root.path())).await;

    let err = f
        .client
        .call::<_, Value>(
            "thread/resume",
            &ThreadResumeParams {
                thread_id: ts.thread.id.clone(),
                cwd: None,
                sandbox: None,
                approval_policy: None,
                model: None,
                config: None,
                developer_instructions: None,
            },
            RPC,
        )
        .await
        .expect_err("resume-fail must error");
    match err {
        RpcError::Remote { code, ref message } => {
            assert_eq!(code, -32001);
            assert!(message.contains(&ts.thread.id), "got {message}");
        }
        other => panic!("expected a remote error, got {other:?}"),
    }
    f.shutdown().await;
}

// ---------------------------------------------------------------------------
// child environment (spec §4.2 allowlist)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn child_env_is_the_allowlist_plus_extras_with_secrets_always_scrubbed() {
    let root = root();
    let dump = root.path().join("child-env.txt");
    let f = Fake::start(
        root.path(),
        "happy",
        &[
            ("FAKE_DUMP_ENV", dump.to_str().unwrap()),
            ("A2A_TEST_MARKER", "seam-ok"),
            // Planted secrets: the scrub list wins even when a caller passes them
            // explicitly.
            ("OPENAI_API_KEY", "sk-should-never-appear"),
            ("AWS_SECRET_ACCESS_KEY", "should-never-appear"),
            ("DEPOT_TOKEN", "should-never-appear"),
        ],
    )
    .await;
    f.init().await; // the dump is written before the first frame

    let dumped = std::fs::read_to_string(&dump).expect("child env dump");
    let keys: Vec<&str> = dumped.lines().filter_map(|l| l.split('=').next()).collect();

    assert!(keys.contains(&"PATH"), "PATH is in the allowlist");
    assert!(keys.contains(&"HOME"), "HOME is in the allowlist");
    assert!(
        keys.contains(&"A2A_TEST_MARKER"),
        "extra pairs pass through"
    );
    for scrubbed in ["OPENAI_API_KEY", "AWS_SECRET_ACCESS_KEY", "DEPOT_TOKEN"] {
        assert!(
            !keys.contains(&scrubbed),
            "{scrubbed} leaked into the child: {dumped}"
        );
    }
    assert!(
        !dumped.contains("should-never-appear") && !dumped.contains("sk-should-never-appear"),
        "a scrubbed value leaked under another name: {dumped}"
    );
    // env_clear really happened: the harness's own vars are not inherited.
    assert!(
        !keys.contains(&"CARGO_PKG_NAME"),
        "the child inherited the parent environment: {dumped}"
    );
    f.shutdown().await;
}

// ---------------------------------------------------------------------------
// the fidelity contract itself
// ---------------------------------------------------------------------------

#[test]
fn every_snapshot_schema_the_fidelity_helper_names_compiles() {
    for (_, rel) in fidelity::NOTIFICATION_SCHEMAS
        .iter()
        .chain(fidelity::REQUEST_PARAM_SCHEMAS.iter())
    {
        let _ = fidelity::validator(rel);
    }
}

#[test]
fn the_fidelity_helper_rejects_a_drifted_frame() {
    // A turn/completed whose status is not in the pinned TurnStatus enum: the
    // contract must catch it rather than wave it through.
    let drifted = json!({
        "threadId": "t",
        "turn": {"id": "u", "items": [], "status": "vibing"}
    });
    let v = fidelity::validator("v2/TurnCompletedNotification.json");
    assert!(
        v.iter_errors(&drifted).next().is_some(),
        "a drifted fake must fail the fidelity contract"
    );
    // ... while the real shape passes.
    let good = json!({
        "threadId": "t",
        "turn": {"id": "u", "items": [], "status": "completed"}
    });
    fidelity::assert_valid(&good, "v2/TurnCompletedNotification.json", "control frame");
}

// ---------------------------------------------------------------------------
// adapter: lifecycle calls, the steer decision table, AdapterRef, the probe
// ---------------------------------------------------------------------------

fn worker_dir(root: &Path) -> PathBuf {
    let p = root.join("worker");
    std::fs::create_dir_all(p.join("outbox")).unwrap();
    p
}

fn lane_cfg(sandbox: SandboxMode, network: bool, effort: &str) -> LaneConfig {
    LaneConfig {
        sandbox,
        network,
        worktree_default: false,
        model: "gpt-5.5".to_string(),
        effort: effort.to_string(),
        start_timeout_secs: 120,
        task_timeout_secs: 3600,
        blocked_timeout_secs: 1800,
        allow_elevation: false,
        max_turns: 30,
        auto_review: false,
    }
}

/// Every `params` object the client sent for `method`, in wire order — read
/// back off the fake's transcript, so these assertions are about what really
/// crossed the pipe rather than about what the adapter says it built.
fn sent_params(root: &Path, method: &str) -> Vec<Value> {
    let raw = std::fs::read_to_string(transcript(root)).expect("transcript");
    raw.lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|rec| rec["dir"] == "in")
        .filter_map(|rec| rec.get("frame").cloned())
        .filter(|f| f.get("method").and_then(|m| m.as_str()) == Some(method))
        .map(|f| f.get("params").cloned().unwrap_or(Value::Null))
        .collect()
}

async fn next_note_of(incoming: &mut Incoming) -> a2a::protocol::RpcNotification {
    match tokio::time::timeout(WAIT, incoming.notifications.recv()).await {
        Ok(Some(n)) => n,
        Ok(None) => panic!("notification stream closed while a notification was expected"),
        Err(_) => panic!("no notification within {WAIT:?}"),
    }
}

async fn notes_until_of(incoming: &mut Incoming, terminal: &str) -> Vec<String> {
    let mut seen = Vec::new();
    loop {
        let m = next_note_of(incoming).await.method;
        seen.push(m.clone());
        if m == terminal {
            return seen;
        }
    }
}

#[tokio::test]
async fn adapter_initialize_opts_out_of_the_delta_stream_and_the_voice_channel() {
    let root = root();
    let f = Fake::start(root.path(), "happy", &[]).await;

    let info = adapter::initialize(&f.client, RPC)
        .await
        .expect("initialize");
    assert!(
        info.user_agent.starts_with("fake-app-server/"),
        "got {}",
        info.user_agent
    );
    assert_eq!(info.codex_home, state_dir(root.path()));
    f.shutdown().await;

    let sent = sent_params(root.path(), "initialize");
    assert_eq!(sent.len(), 1, "exactly one handshake");
    let p = &sent[0];
    assert_eq!(p["clientInfo"]["name"], "a2a");
    assert_eq!(p["clientInfo"]["version"], env!("CARGO_PKG_VERSION"));
    // The v2 thread/turn surface rides behind the experimental flag; attestation
    // is never requested (a2a has no token to answer it with).
    assert_eq!(p["capabilities"]["experimentalApi"], true);
    assert_eq!(p["capabilities"]["requestAttestation"], false);

    let opts: Vec<String> =
        serde_json::from_value(p["capabilities"]["optOutNotificationMethods"].clone()).unwrap();
    for delta in a2a::protocol::SUPPRESSED_NOTIFICATION_METHODS {
        assert!(opts.contains(&delta.to_string()), "{delta} not suppressed");
    }
    for realtime in adapter::SUPPRESSED_REALTIME_METHODS {
        assert!(
            opts.contains(&realtime.to_string()),
            "{realtime} not suppressed"
        );
    }
    assert_eq!(opts, adapter::suppressed_notification_methods());

    fidelity::audit(&transcript(root.path())).assert_covers(&[], &["initialize"]);
}

#[tokio::test]
async fn adapter_thread_start_passes_the_worker_dir_as_cwd_for_outbox_only_lanes() {
    let root = root();
    let f = Fake::start(root.path(), "happy", &[]).await;
    adapter::initialize(&f.client, RPC).await.unwrap();

    let target = work_dir(root.path());
    let wd = worker_dir(root.path());
    let lane = lane_cfg(SandboxMode::OutboxOnly, false, "medium");
    let info = adapter::thread_start(&f.client, &lane, &target, &wd, RPC)
        .await
        .expect("thread/start");

    assert_eq!(
        info.thread_id, info.session_id,
        "thread.id must equal thread.sessionId — exec resume depends on it"
    );
    assert!(!info.thread_id.is_empty());
    f.shutdown().await;

    let sent = sent_params(root.path(), "thread/start");
    assert_eq!(sent.len(), 1);
    let p = &sent[0];
    assert_eq!(
        p["cwd"].as_str().unwrap(),
        wd.to_str().unwrap(),
        "an outbox-only lane is started on its WORKER DIR, never on the repo"
    );
    assert_eq!(p["sandbox"], "workspace-write");
    assert_eq!(p["approvalPolicy"], "never");
    assert_eq!(p["model"], "gpt-5.5");
    assert!(
        p.get("ephemeral").is_none(),
        "ephemeral must never be sent: it breaks resumability"
    );
    fidelity::audit(&transcript(root.path())).assert_covers(&[], &["thread/start"]);
}

#[tokio::test]
async fn adapter_thread_start_passes_the_target_cwd_for_workspace_write_lanes() {
    let root = root();
    let f = Fake::start(root.path(), "happy", &[]).await;
    adapter::initialize(&f.client, RPC).await.unwrap();

    let target = work_dir(root.path());
    let wd = worker_dir(root.path());
    let lane = lane_cfg(SandboxMode::WorkspaceWrite, false, "high");
    let info = adapter::thread_start(&f.client, &lane, &target, &wd, RPC)
        .await
        .expect("thread/start");
    // The echoed sandbox is the TYPED policy object, recorded as resolved.
    assert!(matches!(
        info.resolved_sandbox,
        a2a::protocol::SandboxPolicy::WorkspaceWrite { .. }
    ));
    f.shutdown().await;

    let p = &sent_params(root.path(), "thread/start")[0];
    assert_eq!(p["cwd"].as_str().unwrap(), target.to_str().unwrap());
}

#[tokio::test]
async fn adapter_turn_start_resupplies_the_sandbox_policy_and_output_schema_every_turn() {
    let root = root();
    let f = Fake::start(root.path(), "happy", &[]).await;
    adapter::initialize(&f.client, RPC).await.unwrap();
    let target = work_dir(root.path());
    let wd = worker_dir(root.path());
    let lane = lane_cfg(SandboxMode::OutboxOnly, true, "medium");
    let thread = adapter::thread_start(&f.client, &lane, &target, &wd, RPC)
        .await
        .unwrap();

    let schema = json!({"type": "object", "properties": {"answer": {"type": "string"}}});
    let spec = TurnSpec {
        thread_id: &thread.thread_id,
        text: "do the thing",
        lane: &lane,
        cwd: &target,
        worker_dir: &wd,
        output_schema: Some(&schema),
    };
    for _ in 0..2 {
        adapter::turn_start(&f.client, &spec, RPC)
            .await
            .expect("turn/start");
    }
    f.shutdown().await;

    let sent = sent_params(root.path(), "turn/start");
    assert_eq!(sent.len(), 2, "two turns");
    for (n, p) in sent.iter().enumerate() {
        assert_eq!(p["threadId"], thread.thread_id.as_str(), "turn {n}");
        assert_eq!(p["input"][0]["type"], "text", "turn {n}");
        assert_eq!(p["input"][0]["text"], "do the thing", "turn {n}");
        assert_eq!(p["effort"], "medium", "turn {n}");
        // Per-turn and NOT sticky: both must be on the wire every single time.
        assert_eq!(p["sandboxPolicy"]["type"], "workspaceWrite", "turn {n}");
        assert_eq!(
            p["sandboxPolicy"]["writableRoots"],
            json!([wd.join("outbox").to_str().unwrap()]),
            "an outbox-only lane may write to its outbox and nowhere else (turn {n})"
        );
        assert_eq!(p["sandboxPolicy"]["networkAccess"], true, "turn {n}");
        assert_eq!(p["outputSchema"], schema, "turn {n}");
        assert_eq!(p["approvalPolicy"], "never", "turn {n}");
    }
    fidelity::audit(&transcript(root.path())).assert_covers(&[], &["turn/start"]);
}

#[tokio::test]
async fn send_message_steers_the_open_turn_when_one_is_known() {
    let root = root();
    let mut f = Fake::start(root.path(), "hang-turn", &[]).await;
    adapter::initialize(&f.client, RPC).await.unwrap();
    let target = work_dir(root.path());
    let wd = worker_dir(root.path());
    let lane = lane_cfg(SandboxMode::WorkspaceWrite, false, "high");
    let thread = adapter::thread_start(&f.client, &lane, &target, &wd, RPC)
        .await
        .unwrap();
    let spec = TurnSpec {
        thread_id: &thread.thread_id,
        text: "go",
        lane: &lane,
        cwd: &target,
        worker_dir: &wd,
        output_schema: None,
    };
    let open = adapter::turn_start(&f.client, &spec, RPC).await.unwrap();
    assert_eq!(
        f.notes_until("turn/started").await.last().unwrap(),
        "turn/started"
    );

    let steer = TurnSpec {
        text: "actually, do this instead",
        ..spec
    };
    let outcome = adapter::send_message(&f.client, &steer, Some(&open.turn_id), RPC)
        .await
        .expect("steer");
    assert_eq!(outcome.path, SendPath::Steered);
    assert_eq!(outcome.turn_id, open.turn_id, "steering stays in the turn");

    let seen = f.notes_until("turn/completed").await;
    assert!(seen.contains(&"turn/completed".to_string()));
    f.shutdown().await;

    let sent = sent_params(root.path(), "turn/steer");
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["expectedTurnId"], open.turn_id.as_str());
    assert_eq!(sent[0]["input"][0]["text"], "actually, do this instead");
    assert_eq!(
        sent_params(root.path(), "turn/start").len(),
        1,
        "a successful steer must not also start a turn"
    );
    fidelity::audit(&transcript(root.path())).assert_covers(&[], &["turn/steer"]);
}

/// THE pin: the TOCTOU race at a turn boundary is expected, not exceptional.
#[tokio::test]
async fn send_message_retries_as_a_new_turn_when_the_steer_loses_the_race() {
    let root = root();
    let mut f = Fake::start(root.path(), "steer-race", &[]).await;
    adapter::initialize(&f.client, RPC).await.unwrap();
    let target = work_dir(root.path());
    let wd = worker_dir(root.path());
    let lane = lane_cfg(SandboxMode::WorkspaceWrite, false, "high");
    let thread = adapter::thread_start(&f.client, &lane, &target, &wd, RPC)
        .await
        .unwrap();
    let spec = TurnSpec {
        thread_id: &thread.thread_id,
        text: "go",
        lane: &lane,
        cwd: &target,
        worker_dir: &wd,
        output_schema: None,
    };
    let stale = adapter::turn_start(&f.client, &spec, RPC).await.unwrap();

    let outcome = adapter::send_message(&f.client, &spec, Some(&stale.turn_id), RPC)
        .await
        .expect("a lost steer race is retried, never surfaced");
    assert_eq!(outcome.path, SendPath::RetriedAsStart);
    assert_ne!(
        outcome.turn_id, stale.turn_id,
        "the retry must report the NEW turn's id"
    );

    let seen = f.notes_until("turn/completed").await;
    assert!(seen.contains(&"turn/completed".to_string()), "got {seen:?}");
    f.shutdown().await;

    assert_eq!(sent_params(root.path(), "turn/steer").len(), 1);
    assert_eq!(
        sent_params(root.path(), "turn/start").len(),
        2,
        "the rejected steer becomes a second turn/start"
    );
}

#[tokio::test]
async fn send_message_starts_a_turn_when_no_turn_is_known() {
    let root = root();
    let mut f = Fake::start(root.path(), "happy", &[]).await;
    adapter::initialize(&f.client, RPC).await.unwrap();
    let target = work_dir(root.path());
    let wd = worker_dir(root.path());
    let lane = lane_cfg(SandboxMode::WorkspaceWrite, false, "high");
    let thread = adapter::thread_start(&f.client, &lane, &target, &wd, RPC)
        .await
        .unwrap();
    let spec = TurnSpec {
        thread_id: &thread.thread_id,
        text: "first contact",
        lane: &lane,
        cwd: &target,
        worker_dir: &wd,
        output_schema: None,
    };

    let outcome = adapter::send_message(&f.client, &spec, None, RPC)
        .await
        .expect("turn/start");
    assert_eq!(outcome.path, SendPath::Started);
    assert!(!outcome.turn_id.is_empty());

    let seen = f.notes_until("turn/completed").await;
    assert!(seen.contains(&"turn/completed".to_string()));
    f.shutdown().await;

    assert!(sent_params(root.path(), "turn/steer").is_empty());
    assert_eq!(sent_params(root.path(), "turn/start").len(), 1);
}

/// Only a steer *precondition* failure is retried. Anything else — a timeout, a
/// different remote error — surfaces unchanged, because retrying it as a fresh
/// turn would double-drive the worker.
#[tokio::test]
async fn send_message_surfaces_a_non_precondition_failure_unchanged() {
    let root = root();
    let f = Fake::start(
        root.path(),
        "hang-turn",
        &[("FAKE_DEFER_METHODS", "turn/steer")],
    )
    .await;
    adapter::initialize(&f.client, RPC).await.unwrap();
    let target = work_dir(root.path());
    let wd = worker_dir(root.path());
    let lane = lane_cfg(SandboxMode::WorkspaceWrite, false, "high");
    let thread = adapter::thread_start(&f.client, &lane, &target, &wd, RPC)
        .await
        .unwrap();
    let spec = TurnSpec {
        thread_id: &thread.thread_id,
        text: "go",
        lane: &lane,
        cwd: &target,
        worker_dir: &wd,
        output_schema: None,
    };
    let open = adapter::turn_start(&f.client, &spec, RPC).await.unwrap();

    let err = adapter::send_message(
        &f.client,
        &spec,
        Some(&open.turn_id),
        Duration::from_millis(300),
    )
    .await
    .expect_err("a deferred steer times out");
    assert_eq!(err, RpcError::Timeout);
    f.shutdown().await;

    assert_eq!(
        sent_params(root.path(), "turn/start").len(),
        1,
        "a timeout must NOT be retried as a new turn"
    );
}

#[tokio::test]
async fn the_adapter_ref_clones_and_drives_turn_start_and_interrupt() {
    let root = root();
    let f = Fake::start(root.path(), "hang-turn", &[]).await;
    adapter::initialize(&f.client, RPC).await.unwrap();
    let target = work_dir(root.path());
    let wd = worker_dir(root.path());
    let lane = lane_cfg(SandboxMode::WorkspaceWrite, false, "high");
    let thread = adapter::thread_start(&f.client, &lane, &target, &wd, RPC)
        .await
        .unwrap();

    let (adapter_ref, mut incoming, mut child) = f.into_adapter();
    let cloned = adapter_ref.clone(); // one child, many worker actors
    let spec = TurnSpec {
        thread_id: &thread.thread_id,
        text: "spin forever",
        lane: &lane,
        cwd: &target,
        worker_dir: &wd,
        output_schema: None,
    };
    let open = adapter_ref
        .turn_start(&spec, RPC)
        .await
        .expect("turn/start");
    assert_eq!(
        notes_until_of(&mut incoming, "turn/started")
            .await
            .last()
            .unwrap(),
        "turn/started"
    );

    cloned
        .turn_interrupt(&thread.thread_id, &open.turn_id, RPC)
        .await
        .expect("turn/interrupt");

    let n = next_note_of(&mut incoming).await;
    assert_eq!(n.method, "turn/completed");
    assert_eq!(n.params["turn"]["status"], "interrupted");
    let _ = child.kill().await;

    let sent = sent_params(root.path(), "turn/interrupt");
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["threadId"], thread.thread_id.as_str());
    assert_eq!(sent[0]["turnId"], open.turn_id.as_str());
    fidelity::audit(&transcript(root.path())).assert_covers(&[], &["turn/interrupt"]);
}

#[tokio::test]
async fn adapter_thread_resume_carries_no_sandbox_policy() {
    let root = root();
    let f = Fake::start(root.path(), "happy", &[]).await;
    adapter::initialize(&f.client, RPC).await.unwrap();
    let target = work_dir(root.path());
    let wd = worker_dir(root.path());
    let lane = lane_cfg(SandboxMode::OutboxOnly, false, "medium");
    let thread = adapter::thread_start(&f.client, &lane, &target, &wd, RPC)
        .await
        .unwrap();

    let resumed = adapter::thread_resume(&f.client, &thread.thread_id, &lane, &target, &wd, RPC)
        .await
        .expect("thread/resume");
    assert_eq!(resumed.thread_id, thread.thread_id);
    f.shutdown().await;

    let sent = sent_params(root.path(), "thread/resume");
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["threadId"], thread.thread_id.as_str());
    assert_eq!(sent[0]["cwd"].as_str().unwrap(), wd.to_str().unwrap());
    assert!(
        sent[0].get("sandboxPolicy").is_none(),
        "resume takes no sandboxPolicy — it is re-supplied on the next turn"
    );
    fidelity::audit(&transcript(root.path())).assert_covers(&[], &["thread/resume"]);
}

// --- handshake_probe: the ONE sanctioned sync-CLI tokio exception -----------

#[test]
fn handshake_probe_completes_the_handshake_from_a_synchronous_caller() {
    // Deliberately NOT a #[tokio::test]: the probe owns its own current_thread
    // runtime, which is the whole point (doctor and transport selection are
    // sync commands).
    let info = adapter::handshake_probe(&fake_bin(), Duration::from_secs(10))
        .expect("the fake answers the handshake");
    assert!(
        info.user_agent.starts_with("fake-app-server/"),
        "got {}",
        info.user_agent
    );
}

#[test]
fn handshake_probe_times_out_and_kills_the_child() {
    let root = root();
    let pidfile = root.path().join("pid");
    let script = root.path().join("sleeper.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho $$ > {}\nexec sleep 30\n",
            pidfile.display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    // Long enough that the sleeper has certainly recorded its pid even under a
    // loaded parallel suite — the assertion is about the leak, not the latency.
    let budget = Duration::from_secs(3);
    let t0 = Instant::now();
    let err = adapter::handshake_probe(&script, budget)
        .expect_err("a child that never answers must not hang the CLI");
    assert_eq!(err, RpcError::Timeout);
    assert!(
        t0.elapsed() < budget * 5,
        "the probe must be bounded by its timeout, took {:?}",
        t0.elapsed()
    );

    let pid: u32 = std::fs::read_to_string(&pidfile)
        .expect("the sleeper wrote its pid")
        .trim()
        .parse()
        .unwrap();
    assert!(
        a2a::exec::start_witness(pid).is_none(),
        "the probe leaked child pid {pid} past its timeout"
    );
}

#[test]
fn handshake_probe_reports_a_missing_binary_rather_than_panicking() {
    let err = adapter::handshake_probe(
        Path::new("/definitely/not/a/binary"),
        Duration::from_secs(2),
    )
    .expect_err("there is no such binary");
    assert_eq!(err, RpcError::ChildDied);
}
