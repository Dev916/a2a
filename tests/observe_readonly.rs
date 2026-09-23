//! The observability plane's hard invariant: observing a home never writes to it.
//!
//! The spec's read-only guarantee is tested by stamping the home's whole file
//! tree (paths + kinds + sizes + mtimes + content digests) before and after a
//! full read session, then asserting the two stamps are identical. Every read
//! path the plane exposes is exercised in between: the `Observer` snapshot and
//! tail, every HTTP route (in-process via `oneshot`), and a live `WS /stream`
//! session over a real bound port while a worker lifecycle is driven in a
//! COMPANION home.

use a2a::observe::{router, AppState, Cursor, Observer};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

const WORKER_ID: &str = "impl-0923t1200-readonly";
const WORK_ID: &str = "work-readonly-1";
const PEER_ID: &str = "gw-peer-readonly";

/// One file-tree entry, recorded finely enough that any write would change it.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Stamp {
    path: PathBuf,
    kind: &'static str,
    len: u64,
    modified: SystemTime,
    digest: String,
}

fn stamp_tree(root: &Path) -> Vec<Stamp> {
    let mut stamps = vec![stamp(root, root)];
    collect(root, root, &mut stamps);
    stamps.sort();
    stamps
}

fn collect(root: &Path, dir: &Path, out: &mut Vec<Stamp>) {
    let mut entries = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        out.push(stamp(root, &path));
        if fs::symlink_metadata(&path).unwrap().is_dir() {
            collect(root, &path, out);
        }
    }
}

fn stamp(root: &Path, path: &Path) -> Stamp {
    let metadata = fs::symlink_metadata(path).unwrap();
    let kind = if metadata.is_dir() {
        "dir"
    } else if metadata.is_file() {
        "file"
    } else {
        "other"
    };
    let digest = if metadata.is_file() {
        hex(&Sha256::digest(fs::read(path).unwrap()))
    } else {
        String::new()
    };
    Stamp {
        path: path.strip_prefix(root).unwrap().to_path_buf(),
        kind,
        len: metadata.len(),
        modified: metadata.modified().unwrap(),
        digest,
    }
}

/// Every entry that appeared, vanished, or changed between two stamps.
fn changes(before: &[Stamp], after: &[Stamp]) -> Vec<String> {
    let mut changes = Vec::new();
    for stamp in before {
        if !after.contains(stamp) {
            changes.push(format!("before-only: {stamp:?}"));
        }
    }
    for stamp in after {
        if !before.contains(stamp) {
            changes.push(format!(" after-only: {stamp:?}"));
        }
    }
    changes
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Seed a home with a worker (state + result + outbox question + events), a
/// peer, mesh logs, and a gateway health snapshot.
fn seed_home(root: &Path) -> PathBuf {
    let worker = root.join("workers").join(WORKER_ID);
    fs::create_dir_all(worker.join("outbox")).unwrap();
    fs::write(
        worker.join("state.json"),
        br#"{"state":"awaiting-input","lane":"impl","worktree":{"path":"/tmp/worktree","branch":"a2a/impl/readonly"},"max_turns":8,"turns_this_epoch":3,"last_delivered_seq":0}"#,
    )
    .unwrap();
    fs::write(
        worker.join("result.json"),
        br#"{"status":"fail","summary":"waiting for input"}"#,
    )
    .unwrap();
    fs::write(
        worker.join("outbox/1.question.json"),
        br#"{"seq":1,"question":"choose A or B","blocking":true}"#,
    )
    .unwrap();
    fs::write(
        worker.join("events.jsonl"),
        concat!(
            "{\"seq\":1,\"ts\":\"2026-09-23T12:00:00Z\",\"kind\":\"spawned\",\"raw\":{\"fsm\":\"Spawn\"}}\n",
            "{\"seq\":2,\"ts\":\"2026-09-23T12:00:01Z\",\"kind\":\"state_change\",\"raw\":{\"fsm\":\"ProcessStarted\"}}\n"
        )
        .as_bytes(),
    )
    .unwrap();

    fs::create_dir_all(root.join("mesh/gateway")).unwrap();
    fs::write(
        root.join("mesh/gateway/gw.key"),
        br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
    )
    .unwrap();
    fs::write(
        root.join("mesh/gateway-health.json"),
        format!(
            r#"{{"health_seq":4,"written_at":"2026-09-23T12:00:03Z","state":"running","links":[{{"peer":"{PEER_ID}","version":"1.2.3"}}],"limits":{{}}}}"#
        ),
    )
    .unwrap();
    fs::create_dir_all(root.join("mesh/peers").join(PEER_ID)).unwrap();
    fs::write(
        root.join("mesh/peers").join(PEER_ID).join("presence.json"),
        format!(
            r#"{{"presence_seq":2,"peer":"{PEER_ID}","last_seen":"2026-09-23T12:00:04Z","version":"1.2.3"}}"#
        ),
    )
    .unwrap();
    fs::write(
        root.join("mesh/events.jsonl"),
        format!(
            "{{\"seq\":5,\"ts\":\"2026-09-23T12:00:05Z\",\"kind\":\"work_transition\",\"work_id\":\"{WORK_ID}\",\"to\":\"offered\"}}\n"
        ),
    )
    .unwrap();
    fs::write(
        root.join("mesh/feed.jsonl"),
        b"{\"seq\":2,\"origin\":\"gw-local\",\"origin_seq\":2,\"ts\":\"2026-09-23T12:00:06Z\",\"text\":\"hello from feed\"}\n",
    )
    .unwrap();
    root.to_path_buf()
}

#[tokio::test]
async fn every_read_path_leaves_the_observed_home_byte_identical() {
    let tmp = tempfile::tempdir().unwrap();
    let home = seed_home(tmp.path());
    let before = stamp_tree(&home);

    // Library read paths: the fold and the cursor-based tail.
    let observer = Observer::open(&home);
    let snapshot = observer.snapshot();
    assert_eq!(snapshot.workers.len(), 1);
    assert_eq!(snapshot.workers[0].worker_id, WORKER_ID);
    assert_eq!(snapshot.work.len(), 1);
    assert_eq!(snapshot.peers.len(), 1);
    assert!(snapshot.health.is_some());

    let (replayed, cursor) = observer.events_since(&Cursor::default());
    assert!(
        replayed
            .iter()
            .any(|event| event.src.artifact == "mesh/feed.jsonl"),
        "genesis replay should include the seeded feed entry: {replayed:?}"
    );
    let (tail, _) = observer.events_since(&cursor);
    assert!(
        tail.iter()
            .all(|event| event.src.artifact != "mesh/feed.jsonl"),
        "an advanced cursor must not re-emit the feed: {tail:?}"
    );

    // Every HTTP read route, in-process (no bind).
    let app = router(AppState {
        home: home.clone(),
        token: None,
    });
    let worker_route = format!("/workers/{WORKER_ID}");
    let work_route = format!("/work/{WORK_ID}");
    let feed_route = format!("/feed?from={}", Cursor::default().encode());
    let routes = [
        "/healthz",
        "/snapshot",
        "/agents",
        "/workers",
        worker_route.as_str(),
        "/work",
        work_route.as_str(),
        "/feed",
        feed_route.as_str(),
    ];
    for route in routes {
        let response = app
            .clone()
            .oneshot(Request::get(route).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "GET {route}");
    }

    let changed = changes(&before, &stamp_tree(&home));
    assert!(
        changed.is_empty(),
        "observing the home must not write to it (paths, sizes, mtimes, digests):\n{}",
        changed.join("\n")
    );
}

#[tokio::test]
async fn a_live_stream_session_leaves_the_observed_home_byte_identical() {
    let observed_tmp = tempfile::tempdir().unwrap();
    let home = seed_home(observed_tmp.path());
    // The lifecycle the spec asks for is driven in a COMPANION home, so the
    // served home is untouched for the whole session.
    let companion_tmp = tempfile::tempdir().unwrap();
    let companion = seed_home(companion_tmp.path());

    let before = stamp_tree(&home);
    let companion_before = stamp_tree(&companion);

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState {
        home: home.clone(),
        token: None,
    });
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let url = format!("ws://{addr}/stream?from={}", Cursor::default().encode());
    let (mut socket, response) = connect_async(url).await.unwrap();
    assert_eq!(response.status().as_u16(), 101, "WS upgrade");

    // Replay from genesis is sent as soon as the socket is up.
    let replay = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("replay frame within 5s");
    assert!(
        matches!(replay, Some(Ok(Message::Text(_)))),
        "expected a replayed event frame, got {replay:?}"
    );

    // Serve a snapshot while the stream is still open — the spec's session is
    // streaming and snapshotting at the same time.
    let snapshot = router(AppState {
        home: home.clone(),
        token: None,
    })
    .oneshot(Request::get("/snapshot").body(Body::empty()).unwrap())
    .await
    .unwrap();
    assert_eq!(snapshot.status(), StatusCode::OK);

    // Drive a worker lifecycle in the companion home while the stream is live.
    let companion_worker = companion.join("workers").join(WORKER_ID);
    fs::write(
        companion_worker.join("state.json"),
        br#"{"state":"done","lane":"impl","max_turns":8,"turns_this_epoch":4,"last_delivered_seq":1}"#,
    )
    .unwrap();
    fs::write(
        companion_worker.join("result.json"),
        br#"{"status":"pass","summary":"companion finished"}"#,
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let _ = socket.close(None).await;
    server.abort();

    let changed = changes(&before, &stamp_tree(&home));
    assert!(
        changed.is_empty(),
        "a live serve + stream session must not write to the observed home:\n{}",
        changed.join("\n")
    );
    assert!(
        !changes(&companion_before, &stamp_tree(&companion)).is_empty(),
        "the companion home did change, so the assertion above is not vacuous"
    );
}
