//! The broker core, end to end, against the scripted fake app-server.
//!
//! Everything here drives the REAL supervision path in REAL time — a child
//! process that really dies, a restart that really re-`initialize`s and
//! re-`thread/resume`s, an `AdapterRef` that is really swapped on the watch
//! channel. The scheduling *arithmetic* (backoff curve, probe streak) is
//! unit-tested under a paused clock in `src/broker/child.rs`; what cannot be
//! faked — and what this file exists for — is that a worker survives its
//! transport dying underneath it.
//!
//! Timings are short by injection, never by mutating the process environment:
//! every test in this binary shares one environment, and a `set_var` race is
//! exactly the kind of flake that gets a suite ignored.

mod common;

use a2a::adapter::SendPath;
use a2a::broker::child::{ChildSpec, RestartPolicy, Supervision};
use a2a::broker::core::{self, BrokerHealth, CoreHandle, CoreMsg};
use a2a::broker::sockserv::{self, ServeCtx};
use a2a::broker::wire;
use a2a::brokerclient;
use a2a::config::{self, Lane};
use a2a::events::{Envelope, EventKind};
use a2a::fold;
use a2a::fsm::{FailureClass, WorkerState};
use a2a::home::A2aHome;
use a2a::inbox::{self, MsgKind};
use a2a::rpc::ServerRequestFrame;
use a2a::state::WorkerSnapshot;
use a2a::store::{self, EventLog};
use chrono::Utc;
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Bounded wait for a converged state. These tests assert WHICH state a worker
/// (or the broker) converges on, never how fast.
const WAIT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(25);

fn fake_bin() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-app-server")
}

struct Harness {
    tmp: tempfile::TempDir,
    home: PathBuf,
    work: PathBuf,
    core: CoreHandle,
    transcript: PathBuf,
}

impl Harness {
    /// A broker over a fresh fake child. `supervision` is injected so a test
    /// can watch three restarts happen in seconds.
    async fn start(scenario: &str, extra: &[(&str, &str)], supervision: Supervision) -> Harness {
        let tmp = tempfile::tempdir().unwrap();
        let home_path = tmp.path().join("home");
        let home = A2aHome::resolve(Some(&home_path)).unwrap();
        let (cfg, _, _) = config::load(&home).unwrap();
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let fake_state = tmp.path().join("fake-state");
        std::fs::create_dir_all(&fake_state).unwrap();
        let transcript = tmp.path().join("transcript.jsonl");

        let mut env: Vec<(String, String)> = vec![
            ("FAKE_SCENARIO".into(), scenario.into()),
            (
                "FAKE_STATE".into(),
                fake_state.to_string_lossy().into_owned(),
            ),
            (
                "FAKE_TRANSCRIPT".into(),
                transcript.to_string_lossy().into_owned(),
            ),
        ];
        env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));

        let spec = ChildSpec {
            bin: fake_bin(),
            args: Vec::new(),
            extra_env: env,
            stderr_log: home.root().join("broker.log"),
        };
        let core = core::start(home.root().to_path_buf(), cfg, spec, supervision)
            .await
            .expect("the broker core starts");

        Harness {
            tmp,
            home: home.root().to_path_buf(),
            work,
            core,
            transcript,
        }
    }

    /// The default: a fast probe (so a wedged child is noticed inside a test's
    /// patience) and a re-promotion far enough out that it never fires mid-test.
    fn quick() -> Supervision {
        Supervision {
            policy: RestartPolicy::default(),
            probe_interval: Duration::from_millis(400),
            repromotion_interval: Duration::from_secs(3600),
        }
    }

    /// A worker directory laid out exactly as `a2a spawn` leaves one: the
    /// subdirectories, the genesis event, and the checkpoint it folds to.
    fn seed_worker(&self, lane: Lane) -> (String, PathBuf) {
        seed_worker_dir(
            &self.home,
            lane,
            &self.work,
            &format!("{}-0817t1200-aaaa", lane.as_str()),
        )
    }

    /// Put a socket server in front of this broker's core. Returns the token
    /// that stops it (`shutdown` over the wire cancels the same one).
    fn listen(&self) -> CancellationToken {
        let shutdown = CancellationToken::new();
        let listener = sockserv::bind(&self.home)
            .expect("the socket binds")
            .expect("nothing else is listening on this temp home");
        let ctx = ServeCtx::new(&self.home, self.core.clone(), shutdown.clone());
        tokio::spawn(sockserv::serve(listener, ctx));
        shutdown
    }

    async fn until_state(&self, id: &str, want: WorkerState) -> WorkerSnapshot {
        self.until(&format!("{id} to reach {}", want.as_str()), || async {
            self.core.status(id).await.ok().filter(|s| s.state == want)
        })
        .await
    }

    /// Poll `f` until it yields a value, or fail loudly with what was seen.
    async fn until<T, F, Fut>(&self, what: &str, f: F) -> T
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Option<T>>,
    {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            if let Some(value) = f().await {
                return value;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for {what}");
            }
            tokio::time::sleep(POLL).await;
        }
    }

    fn events(&self, dir: &Path) -> Vec<Envelope> {
        store::read_events(dir, 0).unwrap_or_default()
    }

    fn health(&self) -> Option<BrokerHealth> {
        BrokerHealth::read(&self.home)
    }

    fn broker_jsonl(&self) -> Vec<Value> {
        let Ok(text) = std::fs::read_to_string(self.home.join("broker.jsonl")) else {
            return Vec::new();
        };
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }

    /// Every frame the fake saw or sent, in order.
    fn transcript(&self) -> Vec<Value> {
        let Ok(text) = std::fs::read_to_string(&self.transcript) else {
            return Vec::new();
        };
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }

    async fn stop(self) {
        let _ = tokio::time::timeout(WAIT, self.core.shutdown()).await;
        drop(self.tmp);
    }
}

/// A worker directory laid out exactly as `a2a spawn` leaves one. Free-standing
/// so the socket tests — which drive a broker in another PROCESS — can seed one
/// without a [`Harness`].
fn seed_worker_dir(home: &Path, lane: Lane, cwd: &Path, id: &str) -> (String, PathBuf) {
    let dir = home.join("workers").join(id);
    for sub in ["outbox", "inbox", "inbox/delivered", "scratch", "context"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    let mut log = EventLog::open(&dir).unwrap();
    let snap = fold::append_and_fold(
        &mut log,
        &dir,
        WorkerSnapshot::new_for_test(id, cwd),
        EventKind::Spawned,
        json!({"fsm":"Spawn","id":id,"lane":lane.as_str(),"cwd":cwd,
               "transport":"app-server"}),
        Utc::now(),
    )
    .unwrap();
    assert_eq!(snap.state, WorkerState::Spawned);
    (id.to_string(), dir)
}

/// A client that speaks the wire by hand, so the FRAME SHAPE is pinned as bytes
/// rather than as whatever `wire::Frame` happens to serialize to today.
struct RawConn {
    stream: std::os::unix::net::UnixStream,
    reader: std::io::BufReader<std::os::unix::net::UnixStream>,
}

impl RawConn {
    fn open(home: &Path) -> RawConn {
        let stream = std::os::unix::net::UnixStream::connect(sockserv::socket_path(home))
            .expect("the broker socket accepts a peer of our own uid");
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let reader = std::io::BufReader::new(stream.try_clone().unwrap());
        RawConn { stream, reader }
    }

    /// Hello with an arbitrary protocol version — the skew tests' whole lever.
    fn hello(home: &Path, protocol_version: u32, binary_version: &str) -> (RawConn, Value) {
        let mut conn = RawConn::open(home);
        conn.send(&json!({"v": 1, "id": 0, "type": "hello",
                          "body": {"protocol_version": protocol_version,
                                   "binary_version": binary_version}}));
        let reply = conn.recv().expect("the broker answers a hello");
        (conn, reply)
    }

    fn send(&mut self, frame: &Value) {
        writeln!(self.stream, "{frame}").unwrap();
        self.stream.flush().unwrap();
    }

    fn send_raw(&mut self, line: &[u8]) {
        self.stream.write_all(line).unwrap();
        self.stream.write_all(b"\n").unwrap();
        self.stream.flush().unwrap();
    }

    fn req(&mut self, id: u64, op: &str, params: Value) -> Value {
        let mut body = json!({"op": op});
        merge(&mut body, params);
        self.send(&json!({"v": 1, "id": id, "type": "req", "body": body}));
        self.recv().expect("the broker answers a request")
    }

    fn recv(&mut self) -> Option<Value> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(serde_json::from_str(&line).expect("every frame is one JSON object")),
        }
    }
}

fn merge(into: &mut Value, from: Value) {
    if let (Some(a), Some(b)) = (into.as_object_mut(), from.as_object()) {
        for (k, v) in b {
            a.insert(k.clone(), v.clone());
        }
    }
}

/// Poll a condition on the calling (blocking) thread. The socket tests that run
/// a real daemon cannot await.
fn until_blocking(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    for _ in 0..(secs * 20) {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for {what}");
}

/// Requests the fake received, by method, after the LAST `initialize` — i.e.
/// everything the broker said to the CURRENT child.
fn requests_after_last_initialize(transcript: &[Value], method: &str) -> Vec<Value> {
    let last_init = transcript
        .iter()
        .rposition(|rec| {
            rec["dir"] == "in" && rec["frame"]["method"] == "initialize"
        })
        .unwrap_or(0);
    transcript[last_init..]
        .iter()
        .filter(|rec| rec["dir"] == "in" && rec["frame"]["method"] == method)
        .map(|rec| rec["frame"]["params"].clone())
        .collect()
}

// ---------------------------------------------------------------------------
// the demux
// ---------------------------------------------------------------------------

/// The whole path in one: register → the ACTOR opens the thread → the core
/// learns its id → every notification is demuxed onto that worker → the turn
/// concludes through the shared code and the worker is `done`.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_registered_with_the_core_is_demuxed_all_the_way_to_done() {
    let h = Harness::start("happy", &[("FAKE_MESSAGE", "THE ANSWER")], Harness::quick()).await;
    let (id, dir) = h.seed_worker(Lane::General);

    let snap = h.core.spawn_worker(&dir).await.unwrap();
    assert_eq!(snap.state, WorkerState::Spawned, "registered as spawned");
    let running = h.until_state(&id, WorkerState::Running).await;
    let thread_id = running.session_id.clone().expect("the actor opened a thread");

    assert_eq!(
        h.core.send(&id, "do the thing").await.unwrap(),
        SendPath::Started
    );
    let done = h.until_state(&id, WorkerState::Done).await;
    assert!(done.failure_class.is_none());
    assert_eq!(
        std::fs::read_to_string(dir.join("result.md")).unwrap(),
        "THE ANSWER"
    );

    // The demux proof: frames keyed three different ways all landed on this
    // worker's own event log.
    let events = h.events(&dir);
    let kinds: Vec<EventKind> = events.iter().map(|e| e.kind).collect();
    assert!(
        kinds.contains(&EventKind::TurnStarted),
        "turn/started keys on params.threadId"
    );
    assert!(
        kinds.contains(&EventKind::AgentMessage),
        "item/completed keys on params.threadId"
    );
    assert!(
        kinds.contains(&EventKind::TurnCompleted),
        "turn/completed keys on params.threadId"
    );
    // `thread/started` carries its id at params.thread.id and NOWHERE else:
    // routing it is the case a naive demux gets wrong, and losing it would look
    // like a worker that never started.
    let started = events
        .iter()
        .find(|e| e.raw.pointer("/thread/id").is_some())
        .expect("thread/started reached the worker");
    assert_eq!(started.raw["thread"]["id"], json!(thread_id));

    assert_eq!(h.core.list().await.unwrap().len(), 1);
    let idle = h.core.idle().await.unwrap();
    assert_eq!(idle.workers, 1);
    assert_eq!(idle.live, 0, "a `done` worker has settled");
    assert_eq!(idle.generation, 1, "the child never had to be replaced");
    h.stop().await;
}

/// Frames that belong to no worker end up in `broker.jsonl` — never dropped —
/// and the broker-scoped ones additionally surface in `broker-health.json`,
/// which is where `a2a doctor` looks.
#[tokio::test(flavor = "multi_thread")]
async fn broker_scoped_and_unroutable_frames_are_recorded_not_dropped() {
    // `bad-vocabulary` emits an unknown, unkeyed notification at startup.
    let h = Harness::start("bad-vocabulary", &[], Harness::quick()).await;

    // The three broker-scoped methods belong to the daemon, not to a worker.
    for (method, params) in [
        (
            "account/rateLimits/updated",
            json!({"rateLimits": {"primary": {"usedPercent": 12.5}}}),
        ),
        ("deprecationNotice", json!({"message": "v2 is coming"})),
        ("configWarning", json!({"message": "unknown key `foo`"})),
    ] {
        h.core
            .sender()
            .send(CoreMsg::Notification {
                generation: 1,
                method: method.to_string(),
                params,
            })
            .await
            .unwrap();
    }

    let health = h
        .until("the notices to reach broker-health.json", || async {
            h.health().filter(|health| health.notices.len() == 3)
        })
        .await;
    let methods: Vec<&str> = health.notices.iter().map(|n| n.method.as_str()).collect();
    assert_eq!(
        methods,
        vec![
            "account/rateLimits/updated",
            "deprecationNotice",
            "configWarning"
        ]
    );
    assert_eq!(health.notices[0].params["rateLimits"]["primary"]["usedPercent"], json!(12.5));
    assert_eq!(health.generation, 1);
    assert_eq!(health.degraded, None);
    assert!(health.child_pid.is_some());

    let lines = h
        .until("broker.jsonl to carry both kinds", || async {
            let lines = h.broker_jsonl();
            let scoped = lines.iter().filter(|l| l["kind"] == "broker_scoped").count();
            let unroutable = lines.iter().any(|l| l["kind"] == "unroutable");
            (scoped == 3 && unroutable).then_some(lines)
        })
        .await;
    let unroutable = lines
        .iter()
        .find(|l| l["kind"] == "unroutable")
        .expect("the frame nobody claimed was written down");
    assert_eq!(unroutable["method"], json!("sorcery/unicornSummoned"));
    assert_eq!(unroutable["reason"], json!("no_routing_key"));
    h.stop().await;
}

/// A server→client request is answered from the table and then recorded
/// against the worker that owns the thread — or, when it names no thread, in
/// `broker.jsonl`. Either way it is ANSWERED: a dropped one is a permanent hang.
#[tokio::test(flavor = "multi_thread")]
async fn server_requests_are_answered_and_logged_where_they_belong() {
    let h = Harness::start("server-request", &[("FAKE_MESSAGE", "APPROVED?")], Harness::quick()).await;
    let (id, dir) = h.seed_worker(Lane::General);
    h.core.spawn_worker(&dir).await.unwrap();
    h.until_state(&id, WorkerState::Running).await;

    // The fake's turn completes ONLY after the approval is answered, so
    // reaching `done` is itself the proof that the answer went out.
    h.core.send(&id, "run the command").await.unwrap();
    h.until_state(&id, WorkerState::Done).await;

    let logged = h
        .events(&dir)
        .into_iter()
        .find(|e| e.kind == EventKind::ServerRequest)
        .expect("the approval was recorded against its worker");
    assert_eq!(logged.raw["method"], json!("execCommandApproval"));
    assert_eq!(logged.raw["disposition"], json!("denied"));
    assert!(
        logged.raw.get("command").is_none(),
        "an approval's params can carry a command line: never in the event log"
    );

    // One that names no thread has no worker to belong to.
    h.core
        .sender()
        .send(CoreMsg::ServerRequest {
            generation: 1,
            frame: ServerRequestFrame {
                id: json!(999_001),
                method: "mcpServer/elicitation/request".into(),
                params: json!({"message": "who are you?"}),
            },
        })
        .await
        .unwrap();
    let line = h
        .until("the orphan request to reach broker.jsonl", || async {
            h.broker_jsonl()
                .into_iter()
                .find(|l| l["kind"] == "server_request")
        })
        .await;
    assert_eq!(line["method"], json!("mcpServer/elicitation/request"));
    assert_eq!(line["disposition"], json!("declined"));
    h.stop().await;
}

// ---------------------------------------------------------------------------
// supervision
// ---------------------------------------------------------------------------

/// The headline: the transport dies MID-TURN and the worker survives it.
///
/// The fake crashes on its first launch only, so the restart finds a child that
/// will answer `thread/resume` — and the post-restart `send` succeeding is the
/// proof that the watch-channel rebind reached the actor, because a stale
/// `AdapterRef` would be writing into a dead process's stdin.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_survives_its_transport_dying_mid_turn() {
    let h = Harness::start("crash-mid-turn", &[("FAKE_MESSAGE", "AFTER")], Harness::quick()).await;
    let (id, dir) = h.seed_worker(Lane::General);
    h.core.spawn_worker(&dir).await.unwrap();
    let running = h.until_state(&id, WorkerState::Running).await;
    let first_pid = h.core.idle().await.unwrap().child_pid.expect("a live child");
    assert_eq!(running.pid, Some(first_pid), "the SHARED child's pid");

    // The turn starts, and the child dies inside it.
    h.core.send(&id, "start working").await.unwrap();

    // Supervision restarts, re-initializes, and re-attaches this worker.
    let restarted = h
        .until("the worker to be re-attached to a new child", || async {
            h.events(&dir)
                .into_iter()
                .find(|e| e.kind == EventKind::TransportRestarted)
        })
        .await;
    let second_pid = restarted.raw["pid"].as_u64().expect("the NEW child's pid") as u32;
    assert_ne!(second_pid, first_pid, "a genuinely different process");
    assert_eq!(h.core.idle().await.unwrap().generation, 2);

    // The snapshot followed the pid, so a later kill aims at the right process.
    let snap = h
        .until("the snapshot to follow the new child", || async {
            h.core.status(&id).await.ok().filter(|s| s.pid == Some(second_pid))
        })
        .await;
    assert_eq!(snap.state, WorkerState::Running, "the worker never failed");

    // THE PIN: a send after the restart succeeds and runs the worker to done.
    assert_eq!(
        h.core.send(&id, "carry on").await.unwrap(),
        SendPath::Started,
        "the turn that died with the old child is gone: this opens a new one"
    );
    let done = h.until_state(&id, WorkerState::Done).await;
    assert!(
        done.extra.get("fold_rejections").is_none(),
        "a restart must not produce a single illegal transition: {:?}",
        done.extra.get("fold_rejections")
    );
    assert_eq!(
        std::fs::read_to_string(dir.join("result.md")).unwrap(),
        "AFTER"
    );

    // THE OTHER PIN: `thread/resume` cannot carry the per-turn overrides, so
    // the next turn/start re-supplies them IN FULL. Losing this is silent: the
    // worker keeps running, with the sandbox widened back to the mode default.
    let transcript = h.transcript();
    let resumed = requests_after_last_initialize(&transcript, "thread/resume");
    assert_eq!(resumed.len(), 1, "the thread was re-opened exactly once");
    assert!(
        resumed[0].get("sandboxPolicy").is_none(),
        "thread/resume has no sandboxPolicy — that is the whole problem"
    );
    let turns = requests_after_last_initialize(&transcript, "turn/start");
    let post_restart = turns.last().expect("a turn started after the restart");
    assert_eq!(post_restart["approvalPolicy"], json!("never"));
    assert_eq!(post_restart["sandboxPolicy"]["type"], json!("workspaceWrite"));
    assert!(
        post_restart["sandboxPolicy"]["writableRoots"]
            .as_array()
            .is_some_and(|roots| !roots.is_empty()),
        "the post-restart turn re-supplied its writable roots: {post_restart}"
    );
    assert_eq!(post_restart["effort"], json!("medium"));
    h.stop().await;
}

/// The other half of a re-attach: whatever the operator queued while the
/// transport was down goes out the moment it comes back. An outage the operator
/// has to notice — and re-`send` through — is an outage that lost a message.
#[tokio::test(flavor = "multi_thread")]
async fn work_queued_while_the_transport_was_down_is_re_delivered() {
    let h = Harness::start("crash-mid-turn", &[("FAKE_MESSAGE", "RESUMED")], Harness::quick()).await;
    let (id, dir) = h.seed_worker(Lane::General);
    h.core.spawn_worker(&dir).await.unwrap();
    h.until_state(&id, WorkerState::Running).await;
    h.core.send(&id, "start working").await.unwrap();

    // Wait for the transport to actually be gone, so the message is queued
    // while there is nothing that could deliver it.
    h.until("the child to die", || async {
        h.core
            .idle()
            .await
            .ok()
            .filter(|idle| idle.child_pid.is_none())
    })
    .await;
    let seq = inbox::queue_message(&dir, "orchestrator", MsgKind::Answer, "B, definitely").unwrap();
    assert_eq!(seq, 1);
    assert!(
        inbox::undelivered(&dir, 0).unwrap().len() == 1,
        "the message is sitting in the inbox with no transport to take it"
    );

    // Nobody sent anything after the restart: the re-attach did.
    let snap = h
        .until("the queued message to be re-delivered", || async {
            h.core
                .status(&id)
                .await
                .ok()
                .filter(|s| s.last_delivered_seq == seq)
        })
        .await;
    assert!(snap.pid.is_some());
    assert!(
        inbox::undelivered(&dir, snap.last_delivered_seq)
            .unwrap()
            .is_empty(),
        "the inbox was acknowledged, not just read"
    );

    // It went out as a real turn, carrying the operator's text and the full
    // per-turn overrides.
    let transcript = h.transcript();
    let turns = requests_after_last_initialize(&transcript, "turn/start");
    let turn = turns.last().expect("the re-delivery opened a turn");
    assert_eq!(turn["input"][0]["type"], json!("text"));
    assert_eq!(
        turn["input"][0]["text"].as_str().map(str::trim),
        Some("B, definitely"),
        "the operator's own words, not a synthetic nudge"
    );
    assert_eq!(turn["sandboxPolicy"]["type"], json!("workspaceWrite"));
    h.until_state(&id, WorkerState::Done).await;
    h.stop().await;
}

/// A child that dies every time is not restarted forever: three tries inside
/// the window, spaced by the exponential backoff, and then the broker latches
/// `degraded: exec` where the spawn path and `doctor` can see it.
#[tokio::test(flavor = "multi_thread")]
async fn a_child_that_always_dies_latches_degraded_after_three_restarts() {
    let started = tokio::time::Instant::now();
    let h = Harness::start("always-crash", &[], Harness::quick()).await;

    let health = h
        .until("the broker to give up on the app-server", || async {
            h.health().filter(|health| health.degraded.is_some())
        })
        .await;
    let elapsed = started.elapsed();

    assert_eq!(health.degraded.as_deref(), Some("exec"));
    assert_eq!(health.restarts, 3, "spec §4.2: three restarts, then degrade");
    assert!(health.degraded_since.is_some());
    assert!(
        health
            .degraded_reason
            .as_deref()
            .is_some_and(|r| r.contains("died")),
        "the reason names what happened: {:?}",
        health.degraded_reason
    );
    // ... and it will try again, rather than staying degraded forever.
    let next = health.next_promotion_probe_at.expect("a re-promotion probe is scheduled");
    assert!(next > Utc::now(), "the probe is in the future");

    // The backoff is real: 500ms + 1s + 2s nominal, jittered ±25%.
    assert!(
        elapsed >= Duration::from_millis(2_500),
        "three restarts took {elapsed:?} — that is not an exponential backoff"
    );

    let idle = h.core.idle().await.unwrap();
    assert!(idle.degraded);
    assert!(idle.child_pid.is_none(), "there is no live child to name");
    assert!(
        h.broker_jsonl().iter().any(|l| l["kind"] == "degraded"),
        "the degradation is in the broker log too"
    );
    h.stop().await;
}

/// A child that is alive but has stopped answering is still dead: the liveness
/// probe is the only thing that notices, because stdout never closes.
#[tokio::test(flavor = "multi_thread")]
async fn a_wedged_child_is_declared_dead_by_the_liveness_probe() {
    // The fake defers every probe response indefinitely: the process is up, the
    // pipe is open, and nothing ever comes back.
    let h = Harness::start(
        "happy",
        &[("FAKE_DEFER_METHODS", "thread/loaded/list")],
        Supervision {
            policy: RestartPolicy::default(),
            probe_interval: Duration::from_millis(300),
            repromotion_interval: Duration::from_secs(3600),
        },
    )
    .await;
    let first_pid = h.core.idle().await.unwrap().child_pid.expect("a live child");

    let idle = h
        .until("the probe to notice a wedged child", || async {
            h.core
                .idle()
                .await
                .ok()
                .filter(|idle| idle.generation > 1 || idle.degraded)
        })
        .await;
    assert!(idle.restarts >= 1, "the wedged child was replaced");
    assert_ne!(
        idle.child_pid,
        Some(first_pid),
        "the replacement is a different process"
    );
    h.stop().await;
}

/// The other half of a restart: a thread the server will NOT re-open. The
/// worker is `orphaned` — recoverable, but not by the broker — never silently
/// left `running` against a child that has never heard of it.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_whose_thread_cannot_be_resumed_is_orphaned() {
    let h = Harness::start("resume-fail", &[], Harness::quick()).await;
    let (id, dir) = h.seed_worker(Lane::General);
    h.core.spawn_worker(&dir).await.unwrap();
    h.until_state(&id, WorkerState::Running).await;

    // Kill the child out from under the broker: a real death, not a simulated
    // one, so the whole supervision path runs.
    let pid = h.core.idle().await.unwrap().child_pid.expect("a live child");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();

    let orphaned = h.until_state(&id, WorkerState::Orphaned).await;
    assert_eq!(orphaned.failure_class, None, "orphaned is not failed");
    let event = h
        .events(&dir)
        .into_iter()
        .rev()
        .find(|e| e.kind == EventKind::Orphaned)
        .expect("the orphaning went through the WAL");
    assert_eq!(event.raw["fsm"], json!("OrphanDetected"));
    assert!(
        event.raw["detail"]
            .as_str()
            .is_some_and(|d| d.contains("thread/resume")),
        "the detail names what refused: {:?}",
        event.raw["detail"]
    );
    // The transport itself came back — it is this worker that could not.
    assert!(h.core.idle().await.unwrap().child_pid.is_some());
    h.stop().await;
}

/// Expired credentials are a terminal, resumable failure with a class of their
/// own: an orchestrator must be able to tell "log in again" apart from "the
/// model failed", and must never auto-retry the first.
#[tokio::test(flavor = "multi_thread")]
async fn an_auth_expiry_error_fails_the_worker_as_auth_expired() {
    let h = Harness::start("hang-turn", &[], Harness::quick()).await;
    let (id, dir) = h.seed_worker(Lane::General);
    h.core.spawn_worker(&dir).await.unwrap();
    let running = h.until_state(&id, WorkerState::Running).await;
    let thread_id = running.session_id.clone().unwrap();

    h.core
        .sender()
        .send(CoreMsg::Notification {
            generation: 1,
            method: "error".into(),
            params: json!({
                "threadId": thread_id,
                "turnId": "turn_1",
                "error": {"message": "401 Unauthorized: your session token expired"},
                "willRetry": false,
            }),
        })
        .await
        .unwrap();

    let failed = h.until_state(&id, WorkerState::Failed).await;
    assert_eq!(
        failed.failure_class,
        Some(FailureClass::AuthExpired),
        "not `no_result`: the broker knew exactly why this died"
    );
    // Resumable, and the WAL says so — a snapshot rebuilt from zero reports the
    // same class.
    let rebuilt = fold::fold(
        WorkerSnapshot::new_for_test(&id, &h.work),
        &h.events(&dir),
        Utc::now(),
    );
    assert_eq!(rebuilt.failure_class, Some(FailureClass::AuthExpired));
    h.stop().await;
}

// ---------------------------------------------------------------------------
// the socket: one frame shape, one skew key (Task 8)
// ---------------------------------------------------------------------------

/// The socket is 0600 inside the 0700 home, and the handshake is a frame like
/// any other: `{v, id, type:"hello", body:{protocol_version, binary_version}}`
/// in both directions.
#[tokio::test(flavor = "multi_thread")]
async fn the_broker_socket_is_0600_and_the_handshake_is_one_frame_shape() {
    let h = Harness::start("happy", &[], Harness::quick()).await;
    let _serving = h.listen();
    let sock = sockserv::socket_path(&h.home);

    assert!(sock.exists(), "the daemon published its socket");
    assert_eq!(
        std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777,
        0o600,
        "a world-writable broker socket is a remote shell for every local user"
    );
    assert_eq!(
        std::fs::metadata(&h.home).unwrap().permissions().mode() & 0o777,
        0o700,
        "... inside a 0700 home"
    );

    let home = h.home.clone();
    let (hello, listed) = tokio::task::spawn_blocking(move || {
        let (mut conn, hello) = RawConn::hello(&home, wire::protocol_version(), "9.9.9");
        let listed = conn.req(1, "list", json!({}));
        (hello, listed)
    })
    .await
    .unwrap();

    assert_eq!(hello["v"], json!(1), "every frame carries the wire version");
    assert_eq!(hello["type"], json!("hello"));
    assert_eq!(hello["body"]["protocol_version"], json!(wire::protocol_version()));
    assert_eq!(hello["body"]["binary_version"], json!(wire::BINARY_VERSION));

    assert_eq!(listed["type"], json!("resp"));
    assert_eq!(listed["id"], json!(1));
    assert_eq!(listed["body"]["workers"], json!([]));
    h.stop().await;
}

/// A frame over the 1 MiB cap is a typed error and the end of the connection —
/// never an allocation the sender chose the size of.
#[tokio::test(flavor = "multi_thread")]
async fn a_frame_over_the_cap_is_refused_with_a_typed_error_and_the_connection_closes() {
    assert_eq!(
        wire::MAX_FRAME_BYTES,
        1024 * 1024,
        "the cap is 1 MiB — a value, not whatever the constant happens to say"
    );
    let h = Harness::start("happy", &[], Harness::quick()).await;
    let _serving = h.listen();

    let home = h.home.clone();
    let (err, after) = tokio::task::spawn_blocking(move || {
        let (mut conn, _) = RawConn::hello(&home, wire::protocol_version(), "9.9.9");
        let oversized = vec![b'x'; wire::MAX_FRAME_BYTES + 1];
        conn.send_raw(&oversized);
        let err = conn.recv().expect("the broker says why before it hangs up");
        (err, conn.recv())
    })
    .await
    .unwrap();

    assert_eq!(err["type"], json!("err"));
    assert_eq!(err["body"]["code"], json!(wire::ERR_FRAME_TOO_LARGE));
    assert_eq!(
        err["body"]["max_bytes"],
        json!(wire::MAX_FRAME_BYTES),
        "the error names the cap it hit"
    );
    assert!(after.is_none(), "the connection is closed, not merely scolded");
    h.stop().await;
}

/// Skew, the broker's half: the hello-err carries the LIVE-WORKER COUNT, which
/// is what tells the client whether it may retire this daemon. The refused peer
/// can do exactly one thing afterwards — drain it.
#[tokio::test(flavor = "multi_thread")]
async fn a_skewed_hello_is_refused_with_the_live_worker_count_and_can_only_drain() {
    let h = Harness::start("happy", &[], Harness::quick()).await;
    let (id, dir) = h.seed_worker(Lane::General);
    h.core.spawn_worker(&dir).await.unwrap();
    h.until_state(&id, WorkerState::Running).await;
    let _serving = h.listen();

    let home = h.home.clone();
    let skewed = wire::protocol_version() + 41;
    let (err, refused, drained) = tokio::task::spawn_blocking(move || {
        let (mut conn, err) = RawConn::hello(&home, skewed, "0.0.1");
        // Anything but a drain from a peer we cannot understand.
        let refused = conn.req(1, "list", json!({}));
        // ... and the drain itself, on a fresh connection.
        let (mut drain, _) = RawConn::hello(&home, skewed, "0.0.1");
        let drained = drain.req(1, "shutdown", json!({}));
        (err, refused, drained)
    })
    .await
    .unwrap();

    assert_eq!(err["type"], json!("err"));
    assert_eq!(err["body"]["code"], json!(wire::ERR_VERSION_SKEW));
    assert_eq!(
        err["body"]["live_workers"],
        json!(1),
        "the count is the whole point: a live worker forbids the respawn"
    );
    assert_eq!(err["body"]["broker_protocol_version"], json!(wire::protocol_version()));
    assert_eq!(err["body"]["client_protocol_version"], json!(skewed));
    assert_eq!(err["body"]["broker_binary_version"], json!(wire::BINARY_VERSION));
    assert_eq!(err["body"]["client_binary_version"], json!("0.0.1"));
    assert_eq!(err["body"]["exit"], json!(7));

    assert_eq!(refused["type"], json!("err"));
    assert_eq!(refused["body"]["code"], json!(wire::ERR_DRAIN_ONLY));
    assert_eq!(drained["type"], json!("resp"));
    assert_eq!(drained["body"]["shutting_down"], json!(true));

    until_blocking("the drained broker to take its socket with it", 10, || {
        !sockserv::socket_path(&h.home).exists()
    });
    h.stop().await;
}

/// One subscription per connection, and it carries a worker's envelopes as they
/// are folded — the `tail --follow` transport.
#[tokio::test(flavor = "multi_thread")]
async fn one_subscription_per_connection_streams_a_workers_envelopes() {
    let h = Harness::start("happy", &[("FAKE_MESSAGE", "STREAMED")], Harness::quick()).await;
    let (id, dir) = h.seed_worker(Lane::General);
    h.core.spawn_worker(&dir).await.unwrap();
    h.until_state(&id, WorkerState::Running).await;
    let _serving = h.listen();

    let home = h.home.clone();
    let worker = id.clone();
    let (second, path, kinds) = tokio::task::spawn_blocking(move || {
        let mut client = brokerclient::connect(&home).expect("the sync client handshakes");
        client.subscribe().expect("the first subscription is accepted");
        let second = client.subscribe().expect_err("the second is not");

        let path = client
            .request("send", json!({"id": worker, "text": "do the thing"}))
            .unwrap();

        let mut kinds: Vec<String> = Vec::new();
        let deadline = std::time::Instant::now() + WAIT;
        while std::time::Instant::now() < deadline {
            match client.next_event(Duration::from_millis(500)).unwrap() {
                Some(event) => {
                    assert_eq!(event.worker, worker, "events name the worker they belong to");
                    kinds.push(format!("{:?}", event.envelope.kind));
                    if event.envelope.kind == EventKind::TurnCompleted {
                        break;
                    }
                }
                None => continue,
            }
        }
        (second, path, kinds)
    })
    .await
    .unwrap();

    assert_eq!(second.code, wire::ERR_ALREADY_SUBSCRIBED);
    assert_eq!(path["path"], json!("started"));
    assert!(
        kinds.iter().any(|k| k == "TurnStarted") && kinds.iter().any(|k| k == "TurnCompleted"),
        "the subscription carried the turn, start to finish: {kinds:?}"
    );
    h.until_state(&id, WorkerState::Done).await;
    h.stop().await;
}

// ---------------------------------------------------------------------------
// the sync client, against a real daemon (Task 8)
// ---------------------------------------------------------------------------

/// Autostart, end to end: a CLI that finds no broker starts one, hands it the
/// environment it needs (its codex binary, its allowed roots), and talks to it —
/// without ever touching the operator's real `~/.a2a`.
#[test]
fn an_autostarted_broker_answers_the_client_and_never_touches_the_real_home() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let fake_home = tmp.path().join("fake-HOME");
    std::fs::create_dir_all(&fake_home).unwrap();
    let (mut cmd, guard) = common::a2a_broker(&home, tmp.path());

    let out = cmd
        .env("HOME", &fake_home)
        .args(["--json", "__broker-call", "info"])
        .assert()
        .success();
    let v: Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    let info = &v["data"];

    assert_eq!(v["ok"], json!(true));
    assert_eq!(info["protocol_version"], json!(wire::protocol_version()));
    assert_eq!(info["binary_version"], json!(wire::BINARY_VERSION));
    assert_eq!(info["workers"], json!(0));
    assert_eq!(info["clients"], json!(1), "the asking client counts itself");
    assert!(
        info["child_pid"].is_u64(),
        "the daemon got A2A_CODEX_BIN and owns a live app-server child: {info}"
    );
    assert_eq!(
        info["allowed_roots"],
        json!([tmp.path().canonicalize().unwrap()]),
        "the daemon got A2A_ALLOWED_ROOTS — it is NOT under the child allowlist"
    );
    assert!(
        !fake_home.join(".a2a").exists(),
        "an autostarted broker resolves --home, never $HOME/.a2a"
    );
    assert_eq!(
        std::fs::metadata(sockserv::socket_path(&home))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    drop(guard);
    assert!(
        !sockserv::socket_path(&home).exists(),
        "`broker shutdown` takes the socket with it"
    );
    common::assert_no_broker_left(&home);
}

/// Skew branch one — nothing in flight: the client DRAINS the old daemon and
/// respawns one from its own environment, then gets on with the job.
#[test]
fn a_version_skew_with_no_live_workers_drains_and_respawns_the_broker() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let (mut cmd, _guard) = common::a2a_broker(&home, tmp.path());
    let first: Value = serde_json::from_slice(
        &cmd.args(["--json", "__broker-call", "info"])
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .unwrap();
    let first_pid = first["data"]["pid"].as_u64().unwrap();

    // A client from a different generation: same socket, different protocol.
    let (mut skewed, _guard2) = common::a2a_broker(&home, tmp.path());
    let out: Value = serde_json::from_slice(
        &skewed
            .env(wire::PROTO_VERSION_ENV, "42")
            .args(["--json", "__broker-call", "info"])
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .unwrap();

    assert_eq!(out["ok"], json!(true), "the respawn made the call work: {out}");
    assert_eq!(
        out["data"]["protocol_version"],
        json!(42),
        "the respawned daemon came up from the CLIENT's environment"
    );
    assert_ne!(
        out["data"]["pid"].as_u64().unwrap(),
        first_pid,
        "a genuinely different daemon process"
    );

    // ... and exactly ONE of them survived the swap. A respawn that raced a
    // second autostart leaves a daemon holding an unlinked socket: alive,
    // unreachable, and outliving every guard in this file.
    drop(_guard2);
    drop(_guard);
    common::assert_no_broker_left(&home);
}

/// Skew branch two — work in flight: the client does NOT retire a broker that
/// is holding live workers. Exit 7, both binary versions named, and a remedy.
#[test]
fn a_version_skew_with_a_live_worker_exits_7_and_leaves_the_broker_alone() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    A2aHome::resolve(Some(&home)).unwrap();
    let (_, dir) = seed_worker_dir(&home, Lane::General, &work, "general-0817t1300-bbbb");

    let (mut cmd, _guard) = common::a2a_broker(&home, tmp.path());
    cmd.args(["--json", "__broker-call", "spawn"])
        .arg(json!({"dir": dir}).to_string())
        .assert()
        .success();
    let (mut probe, _g) = common::a2a_broker(&home, tmp.path());
    let before: Value = serde_json::from_slice(
        &probe
            .args(["--json", "__broker-call", "info"])
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .unwrap();
    assert_eq!(before["data"]["workers"], json!(1));
    let pid = before["data"]["pid"].as_u64().unwrap();

    let (mut skewed, _g2) = common::a2a_broker(&home, tmp.path());
    let out: Value = serde_json::from_slice(
        &skewed
            .env(wire::PROTO_VERSION_ENV, "42")
            .args(["--json", "__broker-call", "info"])
            .assert()
            .code(7)
            .get_output()
            .stdout,
    )
    .unwrap();

    assert_eq!(out["ok"], json!(false));
    assert_eq!(out["error"]["code"], json!(wire::ERR_VERSION_SKEW));
    let message = out["error"]["message"].as_str().unwrap();
    assert!(
        message.contains(wire::BINARY_VERSION),
        "the message names both binaries: {message}"
    );
    assert!(message.contains("42") && message.contains(&wire::protocol_version().to_string()));
    assert_eq!(out["error"]["remedy"], json!("`a2a broker restart`"));

    // Untouched: a broker with work in flight is never drained out from under it.
    let (mut after, _g3) = common::a2a_broker(&home, tmp.path());
    let still: Value = serde_json::from_slice(
        &after
            .args(["--json", "__broker-call", "info"])
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .unwrap();
    assert_eq!(still["data"]["pid"].as_u64().unwrap(), pid);
    assert_eq!(still["data"]["workers"], json!(1));

    drop(_g3);
    drop(_g2);
    drop(_g);
    drop(_guard);
    common::assert_no_broker_left(&home);
}
