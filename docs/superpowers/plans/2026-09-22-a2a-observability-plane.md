# a2a Observability Plane (Epic 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a federated, read-only, management-depth observability plane — a reusable `observe` module that normalizes an agent's on-disk artifacts into one versioned Event envelope + a folded Snapshot, plus `a2a observe serve|tail|snapshot` that expose it over HTTP snapshot + WS replay/live, so any system (the Tauri app, a CLI, a later collector) gets a uniform, subscribable view of one `$A2A_HOME` without touching agent state.

**Architecture:** Two layers. (1) `src/observe/` is pure and reusable: normalizers map each source artifact's records into the **Event** envelope `{v, ts, agent_id, plane, kind, subject, payload, src}`; a **fold** derives the current **Snapshot**; a **tailer** watches sources and emits new Events; an **opaque per-source cursor** encodes resume watermarks. (2) `a2a observe serve` is a thin **axum** HTTP+WS server over one home that exposes the module — read-only by hard invariant. The collector (Phase 2) links the same module; federated endpoints are just its upstreams, so the schema is identical end-to-end.

**Tech Stack:** Rust (tokio, serde, chrono), **axum 0.8** (+ transitive hyper/tower) for HTTP+WS, tokio-rustls for optional TLS (reusing `TlsIdentity::self_signed`). No new top-level binary — one `a2a observe` subcommand.

**Spec:** `docs/superpowers/specs/2026-09-17-a2a-observability-plane-design.md` (approved-shape; the plan argues from it — executors read both). The Event envelope, plane taxonomy, endpoints, cursor rationale, and read-only test all live there and are not re-derived here.

**Dispatch:** codex
**Compatible with:** devloop skill v0.1+

## Global Constraints

- **Read-only is a hard invariant.** The plane MUST NOT write to `$A2A_HOME`: open every artifact read-only, hold no write handle, add no mutating route. Any plane-owned bookkeeping lives outside the home (Phase 1 keeps none). Task 12's byte-identical-home test guards this and is non-negotiable.
- **Additive.** Read artifacts that already exist; never change how the broker/mesh writes them. Depth-2 capture is out of scope (a reserved seam only).
- **Forward-compatible contract.** Unknown `plane`, unknown `kind`, and unknown envelope fields are ignored by consumers; new kinds are additive; existing kinds' payloads only grow (never repurpose a field). The Event type serializes/deserializes accordingly (serde `#[serde(other)]` on enums, no `deny_unknown_fields`).
- **Repo hygiene:** commits use `--no-verify` (the bd pre-commit hook hangs in this repo). NEVER run `cargo fmt` — the repo is not rustfmt-clean. New code must be clippy-clean; the baseline is exactly the 4 pre-existing sites (`src/exec.rs:554`, `tests/fold_model.rs:23`, `tests/fold_model.rs:97`, `tests/cmd_doctor.rs:303` — line numbers drift; count, never assume).
- **New dependency: `axum = "0.8"`** (default features give routing + `ws`), which pulls hyper 1 + tower. The **orchestrator** adds it to `Cargo.toml` and warms the crate cache in Stage 0 — **codex workers cannot fetch/unpack crates**, so no task may be the first to introduce a dependency.
- **Split verification (the load-bearing constraint):** **codex workers cannot bind loopback listeners.** Route logic is therefore tested with `axum::Router::oneshot` (in-process, no bind) — worker-runnable. Any test that binds a real TCP port (`tokio::net::TcpListener`, a live WS client, `curl`) is **orchestrator-run only**. Each serve task's worker steps use `oneshot`; the real-bind e2e is an orchestrator gate. Workers run focused tests; the orchestrator runs the full suite (`cargo test --no-fail-fast`, backgrounded), clippy, and the bind e2e at every task gate.
- **Known pre-existing red test (ignore):** `cmd_update::version_pin_is_030` asserts `a2a 0.3.0` while the crate is `0.4.0` — fails independent of this work; the full suite needs `--no-fail-fast` to run past it. Do not "fix" it inside a plane task.
- **Workers never touch** real `~/.a2a`, `docs/superpowers/plans/`, or `.beads/`. All hermetic tests build a temp home with `A2aHome::resolve(Some(tmp))` and seed artifacts by hand.
- **Shared APPEND-ONLY files** (append only; the orchestrator reconciles at merge, array-length literals counted not assumed): the clap `Cmd` enum in `src/cli.rs`, the module list in `src/lib.rs`/`src/main.rs`, `KNOWN_CODES` in `src/broker/wire.rs` (each new error code + a one-line remedy). Every other file has one owning task.

## Execution Map

Single track, strictly sequential in one worktree (`observe/plane`); the plane is linear (contract → normalizers → fold/tail → serve → CLI → invariant), so no concurrent tracks. Each task ends committed. The orchestrator gates every task: worker returns focused-green → orchestrator runs the full suite + clippy, and for the serve tasks (T7–T10, T12) the real-bind e2e. Stage 0 (orchestrator, on the worktree base): add `axum` to Cargo.toml, `cargo build` once to warm the cache, commit.

## File Structure

- `src/observe/mod.rs` — module root; re-exports; the `Observer` entry type (opens a home read-only, produces snapshot/stream/cursor).
- `src/observe/event.rs` — the `Event` envelope, `Plane`, `Kind`, `Subject`, `Src`.
- `src/observe/snapshot.rs` — the folded `Snapshot` (agent, workers, work, peers, health, cursor).
- `src/observe/cursor.rs` — opaque `Cursor` (per-source watermarks; encode/decode; forward-only; gap flag).
- `src/observe/source.rs` — read-only jsonl/json readers over `$A2A_HOME` (trailing-partial-line safe, rotation-aware).
- `src/observe/normalize/{mesh,work,worker,presence,health}.rs` — per-plane normalizers (source record → `Event`).
- `src/observe/fold.rs` — sources → `Snapshot`.
- `src/observe/tail.rs` — watch sources from a cursor, emit new `Event`s, advance the cursor.
- `src/observe/serve.rs` — the axum `Router`, `AppState`, handlers, WS stream, auth layer, bind/TLS runner.
- `src/commands/observe.rs` — the `serve`/`tail`/`snapshot` command handlers.
- `src/cli.rs` — `Cmd::Observe { … }` subcommand (append to the enum).
- Tests: `src/observe/*` carry unit tests inline; `tests/observe_serve.rs` (orchestrator-run bind e2e) and `tests/observe_readonly.rs` (the invariant).

---

## Task 1: The contract — Event, Snapshot, Cursor types

**Files:**
- Create: `src/observe/mod.rs`, `src/observe/event.rs`, `src/observe/snapshot.rs`, `src/observe/cursor.rs`
- Modify: `src/lib.rs` (add `pub mod observe;`)

**Interfaces:**
- Produces: `observe::event::{Event, Plane, Kind, Subject, Src}`; `observe::snapshot::{Snapshot, AgentSummary, WorkerView, WorkView, PeerView, HealthView}`; `observe::cursor::Cursor`. These are the contract every later task consumes.

- [ ] **Step 1: Write the failing test** (`src/observe/event.rs` `#[cfg(test)]`)

```rust
#[test]
fn event_round_trips_and_tolerates_unknown_fields() {
    let e = Event {
        v: 1,
        ts: "2026-09-17T18:22:01.412Z".into(),
        agent_id: "gw-host-c726ba".into(),
        plane: Plane::Mesh,
        kind: "feed_entry".into(),
        subject: Subject { worker_id: None, work_id: None, peer: None },
        payload: serde_json::json!({"origin_seq": 42}),
        src: Src { artifact: "mesh/feed.jsonl".into(), reference: "origin_seq=42".into() },
    };
    let json = serde_json::to_string(&e).unwrap();
    let back: Event = serde_json::from_str(&json).unwrap();
    assert_eq!(back.plane, Plane::Mesh);
    // Forward-compat: an unknown plane and an unknown top-level field survive.
    let future = r#"{"v":2,"ts":"t","agent_id":"a","plane":"quantum","kind":"k",
        "subject":{},"payload":{},"src":{"artifact":"x","ref":"y"},"future_field":true}"#;
    let f: Event = serde_json::from_str(future).unwrap();
    assert!(matches!(f.plane, Plane::Unknown(_)));
}
```

- [ ] **Step 2: Run it, watch it fail** — `cargo test --lib observe::event` → fails (types absent).

- [ ] **Step 3: Implement** — `Plane` is `#[serde(rename_all="lowercase")] enum { Mesh, Work, Worker, Presence, Health, #[serde(other)] Unknown }` (a string-carrying `Unknown` needs `#[serde(untagged)]`-style handling; simplest: `Plane(String)` newtype with typed constants — prefer a plain enum with `#[serde(other)] Unknown` if the string value need not be preserved, else a newtype). `kind` is a plain `String` (open taxonomy). `Src` uses `#[serde(rename="ref")]` for `reference`. `Subject` fields are `#[serde(default, skip_serializing_if="Option::is_none")]`. No `deny_unknown_fields` anywhere.

- [ ] **Step 4: Run it, watch it pass.**

- [ ] **Step 5: Snapshot + Cursor tests + impl.** Add `snapshot.rs` (`Snapshot` with `agent: AgentSummary`, `workers: Vec<WorkerView>`, `work: Vec<WorkView>`, `peers: Vec<PeerView>`, `health: Option<HealthView>`, `cursor: Cursor`) and `cursor.rs` with:

```rust
#[test]
fn cursor_encodes_per_source_watermarks_and_round_trips() {
    let mut c = Cursor::default();
    c.set("feed", 42);
    c.set_line("broker.jsonl", /*gen*/ 1, /*pos*/ 900);
    let token = c.encode();                 // opaque base64url of a small JSON map
    let back = Cursor::decode(&token).unwrap();
    assert_eq!(back, c);
    assert!(Cursor::decode("!!not-base64!!").is_err());
}
```

Implement `Cursor` as a `BTreeMap<String, Watermark>` encoded as base64url(JSON). Add a `gap: bool` field defaulting false (set by the tailer when a cursor predates the retained window).

- [ ] **Step 6: Commit** — `git add src/observe src/lib.rs && git commit --no-verify -m "observe: the Event/Snapshot/Cursor contract (a2a-9x6 T1)"`

---

## Task 2: Read-only source readers + the mesh-plane normalizer

**Files:**
- Create: `src/observe/source.rs`, `src/observe/normalize/mod.rs`, `src/observe/normalize/mesh.rs`
- Modify: `src/observe/mod.rs` (`mod source; pub mod normalize;`)

**Interfaces:**
- Consumes: T1 types.
- Produces: `source::{read_jsonl(path) -> Vec<Value>, read_jsonl_from(path, byte_offset) -> (Vec<Value>, new_offset), read_json(path) -> Option<Value>}` — all open read-only, skip an unparseable trailing (mid-write) line, and never error on a missing file (return empty/None). `normalize::mesh::normalize(home, cursor) -> Vec<Event>` for the mesh plane (`feed.jsonl`→`feed_entry`, `events.jsonl` mesh rows→`note`, `mesh/gateway/gateway.jsonl`→`deferral`/`link_limit`).

- [ ] **Step 1: Failing test** — seed `mesh/feed.jsonl` with one entry line, assert `mesh::normalize` returns one `Event { plane: Mesh, kind: "feed_entry", src.artifact: "mesh/feed.jsonl", .. }` whose payload mirrors the source record and whose `ts` is the source ts. Include a trailing half-written line and assert it is skipped, not an error.
- [ ] **Step 2: Run, watch fail.**
- [ ] **Step 3: Implement** `source.rs` (mirror the trailing-line handling in `reconcile::ingest` — consume only up to the last `\n`) and `mesh.rs`. Read the real feed/events/gateway artifact shapes from `src/mesh/` (feed entries, `EventLog` envelopes, `GATEWAY_JSONL`) and map fields verbatim into `payload`; set `agent_id` from the home's `gateway_id`/identity.
- [ ] **Step 4: Run, watch pass** (+ a missing-file case returns `[]`).
- [ ] **Step 5: Commit** — `-m "observe: read-only source readers + mesh normalizer (a2a-9x6 T2)"`

---

## Task 3: Work + worker normalizers

**Files:**
- Create: `src/observe/normalize/work.rs`, `src/observe/normalize/worker.rs`
- Modify: `src/observe/normalize/mod.rs`

**Interfaces:**
- Consumes: T1, T2 (`source`).
- Produces: `normalize::work::normalize(home, cursor) -> Vec<Event>` (`work_offered/approved/released/done/failed/result_empty` from work-store transitions in `events.jsonl`/`broker.jsonl`); `normalize::worker::normalize(home, cursor) -> Vec<Event>` (`worker_spawned/status/awaiting_input/answered/result/interrupted/killed/budget_exhausted/respawned` from `workers/<id>/events.jsonl` + `result.json` + `broker.jsonl`), `subject.worker_id`/`work_id` set.

- [ ] **Step 1: Failing tests** — seed a worker dir (`workers/<id>/events.jsonl` with a spawn + a status envelope, a `result.json`) and assert `worker::normalize` yields the matching kinds with `subject.worker_id = <id>`. Seed a work transition and assert `work::normalize` yields `work_offered`.
- [ ] **Step 2–4:** fail → implement (mirror the FSM/broker event kinds from `src/fsm.rs` + `src/broker/wire.rs`; map each lifecycle kind to a `worker`-plane `Kind`) → pass.
- [ ] **Step 5: Commit** — `-m "observe: work + worker normalizers (a2a-9x6 T3)"`

---

## Task 4: Presence + health normalizers

**Files:**
- Create: `src/observe/normalize/presence.rs`, `src/observe/normalize/health.rs`
- Modify: `src/observe/normalize/mod.rs`

**Interfaces:**
- Consumes: T1, T2.
- Produces: `presence::normalize` (`peer_up/peer_down/peer_version` from presence + `gateway-health.json`); `health::normalize` (`limits/replication/exposure/rotation` from `gateway-health.json`). A **corrupt** `gateway-health.json` yields no health Events + a soft warning (never an error).

- [ ] **Step 1: Failing tests** — seed a `gateway-health.json` with a limits block + peer entries; assert `health::normalize` returns a `Health/limits` Event and `presence::normalize` returns `peer_version` per peer. Seed a corrupt health file; assert `[]` (no panic, no error).
- [ ] **Step 2–4:** fail → implement (read the real `GATEWAY_HEALTH` shape from `src/mesh/gateway/`) → pass.
- [ ] **Step 5: Commit** — `-m "observe: presence + health normalizers (a2a-9x6 T4)"`

---

## Task 5: The fold — sources → Snapshot + the Observer entry type

**Files:**
- Create: `src/observe/fold.rs`
- Modify: `src/observe/mod.rs` (add `pub struct Observer` + `pub fn snapshot()`)

**Interfaces:**
- Consumes: T1–T4.
- Produces: `Observer::open(home) -> Observer`; `Observer::snapshot(&self) -> Snapshot` (folds all sources into current state: the agent summary, one `WorkerView` per worker dir, work items, peers, health, and the `Cursor` current as of the fold).

- [ ] **Step 1: Failing test** — seed a home with one worker (spawned→running→result), one peer, a health block; assert `Observer::open(home).snapshot()` reports `workers.len()==1` with the right terminal status, `peers.len()==1`, `health.is_some()`, and a non-empty `cursor`.
- [ ] **Step 2–4:** fail → implement the fold (derive `WorkerView` from each `workers/<id>/state.json` + `result.json`, mirroring `reconcile`'s notion of state but READ-ONLY — never call reconcile, never write) → pass.
- [ ] **Step 5: Commit** — `-m "observe: fold sources into a Snapshot + Observer (a2a-9x6 T5)"`

---

## Task 6: The tailer — new Events from a cursor, cursor advance, rotation continuity

**Files:**
- Create: `src/observe/tail.rs`
- Modify: `src/observe/mod.rs` (add `Observer::events_since(&self, &Cursor) -> (Vec<Event>, Cursor)`)

**Interfaces:**
- Consumes: T1–T5.
- Produces: `Observer::events_since(cursor) -> (Vec<Event>, Cursor)` — all Events past the cursor across every plane, merged by `ts`, with the advanced cursor. A cursor older than the retained/compacted window returns from the earliest available record with `cursor.gap = true`.

- [ ] **Step 1: Failing test** — seed a home; take `c0 = snapshot().cursor`; append a new feed entry + a worker status; assert `events_since(&c0)` returns exactly those two (ts-ordered) and the returned cursor, fed back, returns nothing. Then force a rotation (`events.jsonl`→`.1`) and assert continuity (no drop, no double-emit).
- [ ] **Step 2–4:** fail → implement (each normalizer already takes a cursor; merge + advance per source; encode generation so rotation is detected — mirror `reconcile::ingest`'s watermark/generation handling) → pass.
- [ ] **Step 5: Commit** — `-m "observe: cursor-based tailer with rotation continuity (a2a-9x6 T6)"`

---

## Task 7: axum server skeleton + `/healthz`, `/snapshot`, `/agents`

**Files:**
- Create: `src/observe/serve.rs`
- Modify: `src/observe/mod.rs`; `Cargo.toml` already carries `axum` (Stage 0)

**Interfaces:**
- Consumes: T1–T6.
- Produces: `serve::router(state: AppState) -> axum::Router`; `AppState { home: PathBuf, token: Option<String> }` (clonable, cheap). `serve::run(state, bind, port, tls) -> Result<()>` (bind runner; used by the CLI, exercised only in the orchestrator e2e).

- [ ] **Step 1: Failing test (oneshot — no bind, worker-runnable)** —

```rust
use axum::{body::Body, http::{Request, StatusCode}};
use tower::ServiceExt; // oneshot
#[tokio::test]
async fn snapshot_and_healthz_render() {
    let tmp = seed_home_with_one_worker();
    let app = serve::router(AppState { home: tmp.path().into(), token: None });
    let hz = app.clone().oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(hz.status(), StatusCode::OK);
    let snap = app.oneshot(Request::get("/snapshot").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(snap.status(), StatusCode::OK);
    let v: serde_json::Value = body_json(snap).await;
    assert!(v["workers"].is_array() && v["cursor"].is_string());
}
```

- [ ] **Step 2: Run, watch fail.**
- [ ] **Step 3: Implement** `router()` with `GET /healthz` (plane liveness), `GET /snapshot` (`Observer::open(home).snapshot()` as JSON), `GET /agents` (the single local agent; shape is a list so the collector reuses it). Handlers open the `Observer` per request (read-only, cheap at management depth). `Content-Type: application/json`.
- [ ] **Step 4: Run, watch pass.**
- [ ] **Step 5: Commit** — `-m "observe: axum router + /healthz /snapshot /agents (a2a-9x6 T7)"`

---

## Task 8: `/workers`, `/workers/{id}`, `/work`, `/work/{id}`, `/feed`

**Files:** Modify: `src/observe/serve.rs`

**Interfaces:** Consumes T7. Produces the five read routes; `/feed?from=<cursor>&limit=` returns a recent feed slice; `/workers/{id}` and `/work/{id}` are 404 when absent.

- [ ] **Step 1: Failing tests (oneshot)** — assert `/workers` lists the seeded worker; `/workers/{id}` returns its detail incl. an `awaiting_input` question when parked; `/workers/unknown` → 404; `/feed?limit=1` returns one entry.
- [ ] **Step 2–4:** fail → implement (all derived from `Observer`; `{id}` via `axum::extract::Path`, query via `axum::extract::Query`) → pass.
- [ ] **Step 5: Commit** — `-m "observe: worker/work/feed read routes (a2a-9x6 T8)"`

---

## Task 9: `WS /stream?from=&planes=` — replay then live tail

**Files:** Modify: `src/observe/serve.rs`

**Interfaces:** Consumes T6–T8. Produces `GET /stream` (WS upgrade via `axum::extract::ws`): on connect, replay `events_since(from)` (or from the snapshot cursor if absent), then poll the home (reuse the ~1s feed-follow cadence) and push new Events; each WS text frame is one Event envelope; optional `planes=mesh,work,…` filter; server sends periodic pings; a slow/overflowing consumer is dropped with its last cursor (it reconnects `?from=`).

- [ ] **Step 1: Failing test (oneshot WS is limited; use a real bind here — ORCHESTRATOR-run).** Mark it `#[ignore]` for workers and note in the task: the orchestrator runs it. It binds `127.0.0.1:0`, connects a `tokio_tungstenite` client `?from=<snapshot.cursor>`, appends a feed entry, and asserts the client receives that Event as one frame. Workers instead unit-test the **frame-encoding + planes-filter** helper (`fn stream_frames(events, planes) -> Vec<String>`) with no socket.
- [ ] **Step 2–4:** worker: fail → implement `stream_frames` + the WS handler wired to it → worker unit tests pass; orchestrator: run the bind e2e.
- [ ] **Step 5: Commit** — `-m "observe: WS /stream replay + live tail (a2a-9x6 T9)"`

---

## Task 10: Transport & auth — bind/port, read token, optional TLS

**Files:** Modify: `src/observe/serve.rs`

**Interfaces:** Consumes T7–T9. Produces: `run(state, bind, port, tls)` binds `127.0.0.1` by default; a per-home **read token** gates HTTP (Bearer) and WS (query `?token=` or subprotocol) when `state.token.is_some()` (loopback with no token needs no auth); `--tls` wraps the listener with `tokio-rustls` using `TlsIdentity::self_signed` (reuse the mesh's identity), plaintext otherwise.

- [ ] **Step 1: Failing test (oneshot)** — with `token: Some("t")`, `/snapshot` without a bearer → 401; with `Authorization: Bearer t` → 200; with `token: None`, no header → 200. (An `axum` middleware layer, `oneshot`-testable, no bind.)
- [ ] **Step 2–4:** fail → implement the auth layer + the bind/TLS runner (runner exercised in the orchestrator e2e) → pass.
- [ ] **Step 5: Commit** — `-m "observe: read-token auth + bind/TLS runner (a2a-9x6 T10)"`

---

## Task 11: CLI — `a2a observe serve|tail|snapshot`

**Files:**
- Create: `src/commands/observe.rs`
- Modify: `src/cli.rs` (append `Cmd::Observe`), `src/commands/mod.rs`, `src/main.rs` (dispatch)

**Interfaces:** Consumes T1–T10. Produces the three subcommands per the spec's CLI surface: `serve [--home] [--bind] [--port] [--token|--token-file] [--tls] [--planes]`, `tail [--home] [--from] [--planes]` (prints Event envelopes — dogfood + zero-UI watch), `snapshot [--home] [--json]` (one-shot Snapshot to stdout).

- [ ] **Step 1: Failing test** — `a2a observe snapshot --home <seeded> --json` exits 0 and stdout parses to a Snapshot with the seeded worker. (`assert_cmd`, no bind — worker-runnable.)
- [ ] **Step 2–4:** fail → implement the handlers + clap wiring (missing/uninitialized home → clear precondition error naming the expected path) → pass.
- [ ] **Step 5: Commit** — `-m "observe: a2a observe serve|tail|snapshot CLI (a2a-9x6 T11)"`

---

## Task 12: The read-only invariant + acceptance

**Files:**
- Create: `tests/observe_readonly.rs`, `tests/observe_serve.rs`

**Interfaces:** Consumes the whole module. This task adds no production code — it proves the invariant and the end-to-end shape. **Orchestrator-run** (binds a port).

- [ ] **Step 1: The read-only test** — snapshot the home's file tree (paths + sizes + mtimes) before and after a serve session that: serves `/snapshot`, streams `WS /stream`, and (in a companion home) drives a worker lifecycle; assert the observed home is **byte-for-byte unchanged**.
- [ ] **Step 2: No-mutating-route test** — assert every route in `serve::router` answers only `GET`/WS; a `POST`/`PUT`/`DELETE` to any path returns 405 (there is no mutating handler to reach).
- [ ] **Step 3: Acceptance e2e** — `a2a observe serve` over a seeded home on `127.0.0.1:0`; `GET /snapshot` and `/workers/{id}` return the expected live shape; `WS /stream` delivers a lifecycle event driven in a companion home; a cursor round-trips (reconnect resumes with no gap).
- [ ] **Step 4: Run all three (orchestrator).**
- [ ] **Step 5: Commit** — `-m "observe: read-only invariant + serve acceptance (a2a-9x6 T12)"`

---

## Self-Review

- **Spec coverage:** module (normalizers/fold/tailer/cursor) = T1–T6; Event envelope + taxonomy = T1–T4; serve surface (all 8 GET routes + WS) = T7–T10; transport/auth/TLS = T10; CLI serve/tail/snapshot = T11; read-only invariant + edge cases (rotation, corrupt health, missing home, 404s) = distributed across T4/T6/T8/T11/T12. Central collector, depth-2, control spike are named seams — deliberately unbuilt.
- **Type consistency:** every task's Produces names types defined in T1 (`Event`, `Snapshot`, `Cursor`) or an earlier task; `Observer` (T5) is the single consumer entry all serve tasks use.
- **Placeholder scan:** none — each task carries a concrete failing test and the interface it produces. Source-artifact field shapes are referenced to their owning modules (`src/mesh/`, `src/fsm.rs`, `src/broker/wire.rs`) for the worker to mirror, exactly as the L4 train did.
- **Scope:** one subsystem, Phase 1 only. Right-sized for a single sequential codex track.
