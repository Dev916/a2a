//! Acceptance for the observability plane's serve surface.
//!
//! Two halves: the surface has no mutating route (proved in-process with
//! `oneshot`), and a REAL bound server on `127.0.0.1:0` answers `GET /snapshot`
//! and `/workers/{id}` and delivers live events over `WS /stream`, resuming
//! from an opaque cursor.

use a2a::observe::{router, AppState, Observer};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use futures_util::StreamExt;
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

const WORKER_ID: &str = "impl-0923t1300-serve";
const MARKER: &str = "live frame marker";

fn seed_home(root: &Path) -> PathBuf {
    let worker = root.join("workers").join(WORKER_ID);
    fs::create_dir_all(worker.join("outbox")).unwrap();
    fs::write(
        worker.join("state.json"),
        br#"{"state":"running","lane":"impl","worktree":{"path":"/tmp/worktree","branch":"a2a/impl/serve"},"max_turns":8,"turns_this_epoch":1,"last_delivered_seq":0}"#,
    )
    .unwrap();
    fs::write(
        worker.join("result.json"),
        br#"{"status":"pass","summary":"seeded"}"#,
    )
    .unwrap();
    fs::create_dir_all(root.join("mesh/gateway")).unwrap();
    fs::write(
        root.join("mesh/gateway/gw.key"),
        br#"{"gateway_id":"gw-local","secret_b64":"unused"}"#,
    )
    .unwrap();
    fs::write(
        root.join("mesh/events.jsonl"),
        b"{\"seq\":1,\"ts\":\"2026-09-23T13:00:00Z\",\"kind\":\"work_transition\",\"work_id\":\"work-serve-1\",\"to\":\"offered\"}\n",
    )
    .unwrap();
    fs::write(
        root.join("mesh/feed.jsonl"),
        b"{\"seq\":2,\"origin\":\"gw-local\",\"origin_seq\":2,\"ts\":\"2026-09-23T13:00:01Z\",\"text\":\"hello from feed\"}\n",
    )
    .unwrap();
    root.to_path_buf()
}

/// Bind `127.0.0.1:0` and serve the observe router exactly as `serve::run`
/// does, returning the port the OS actually handed out.
async fn serve_home(home: &Path) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(AppState {
        home: home.to_path_buf(),
        token: None,
    });
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, handle)
}

/// A minimal HTTP/1.1 GET over a raw socket — the crate has no HTTP client dep.
async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let response = String::from_utf8(raw).unwrap();
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed response for GET {path}: {response}"));
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line for GET {path}: {response}"));
    (status, decode_body(head, body))
}

fn decode_body(head: &str, body: &str) -> String {
    if !head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
        return body.to_string();
    }
    let mut rest = body;
    let mut decoded = String::new();
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let Ok(size) = usize::from_str_radix(size.trim(), 16) else {
            break;
        };
        if size == 0 || tail.len() < size {
            break;
        }
        decoded.push_str(&tail[..size]);
        rest = tail[size..].trim_start_matches("\r\n");
    }
    decoded
}

#[tokio::test]
async fn mutating_methods_find_no_route_on_the_serve_surface() {
    let tmp = tempfile::tempdir().unwrap();
    let home = seed_home(tmp.path());
    let app = router(AppState {
        home,
        token: None,
    });

    for route in ["/snapshot", "/workers", "/feed"] {
        for method in [Method::POST, Method::PUT, Method::DELETE, Method::PATCH] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method.clone())
                        .uri(route)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {route} reached a handler"
            );
        }
    }
}

#[tokio::test]
async fn a_bound_server_serves_the_snapshot_and_worker_detail() {
    let tmp = tempfile::tempdir().unwrap();
    let home = seed_home(tmp.path());
    let (addr, server) = serve_home(&home).await;

    let (status, body) = http_get(addr, "/snapshot").await;
    assert_eq!(status, 200, "{body}");
    let snapshot: Value = serde_json::from_str(&body).unwrap();
    assert!(snapshot["workers"].is_array(), "{snapshot}");
    assert_eq!(snapshot["workers"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["workers"][0]["worker_id"], WORKER_ID);
    assert!(snapshot["cursor"].is_string(), "{snapshot}");

    let (status, body) = http_get(addr, &format!("/workers/{WORKER_ID}")).await;
    assert_eq!(status, 200, "{body}");
    let worker: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(worker["worker_id"], WORKER_ID);
    assert_eq!(worker["state"], "running");
    assert_eq!(worker["last_result"]["status"], "pass");

    let (status, _) = http_get(addr, "/workers/unknown").await;
    assert_eq!(status, 404);

    server.abort();
}

#[tokio::test]
async fn a_live_stream_delivers_a_new_feed_event_and_replays_from_its_cursor() {
    let tmp = tempfile::tempdir().unwrap();
    let home = seed_home(tmp.path());
    let (addr, server) = serve_home(&home).await;

    let cursor = Observer::open(&home).snapshot().cursor.encode();
    let url = format!("ws://{addr}/stream?from={cursor}");
    let (mut socket, response) = connect_async(url.clone()).await.unwrap();
    assert_eq!(response.status().as_u16(), 101, "WS upgrade");

    // Append one feed entry to the SERVED home after the socket is up; the
    // tailer polls every second and must deliver it as a text frame.
    let mut feed = OpenOptions::new()
        .append(true)
        .open(home.join("mesh/feed.jsonl"))
        .unwrap();
    writeln!(
        feed,
        r#"{{"seq":3,"origin":"gw-local","origin_seq":3,"ts":"2026-09-23T13:00:02Z","text":"{MARKER}"}}"#
    )
    .unwrap();
    drop(feed);

    let live = next_marker_frame(&mut socket).await;
    assert_eq!(live["kind"], "feed_entry", "{live}");
    assert_eq!(live["src"]["artifact"], "mesh/feed.jsonl", "{live}");
    assert_eq!(live["payload"]["origin_seq"], 3, "{live}");
    let _ = socket.close(None).await;

    // Reconnecting with the SAME cursor replays the entry rather than losing it.
    let (mut resumed, response) = connect_async(url).await.unwrap();
    assert_eq!(response.status().as_u16(), 101, "WS re-upgrade");
    let replayed = next_marker_frame(&mut resumed).await;
    assert_eq!(replayed["payload"]["origin_seq"], 3, "{replayed}");
    assert_eq!(replayed["src"]["ref"], "origin_seq=3", "{replayed}");
    let _ = resumed.close(None).await;

    server.abort();
}

/// Read frames until the marker event arrives, tolerating any other event the
/// plane emits in the same window.
async fn next_marker_frame<S>(socket: &mut S) -> Value
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let search = async {
        while let Some(message) = socket.next().await {
            let Message::Text(frame) = message.unwrap() else {
                continue;
            };
            let event: Value = serde_json::from_str(&frame).unwrap();
            if event["payload"]["text"] == MARKER {
                return event;
            }
        }
        panic!("stream closed before the marker event arrived");
    };
    tokio::time::timeout(Duration::from_secs(10), search)
        .await
        .expect("marker event within 10s")
}
