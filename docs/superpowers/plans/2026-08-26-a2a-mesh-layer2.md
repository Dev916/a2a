# a2a Mesh Layer 2: Fleet Gateways + Endpoint Registry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the mesh off-machine — per-machine gateways replicate the feed, route notes, and gossip presence over fingerprint-pinned wss, discovered through a Neon Postgres registry, authenticated by fleet-issued ed25519 tokens with proof-of-possession.

**Architecture:** Registry first (`a2a-o4o`): keys/tokens + the sqlx client behind a fakeable trait + fleet CLI. Then the gateway (`a2a-90l`): a supervised five-loop tokio daemon (registry, server, dialer, replication watcher, anti-entropy) whose wire is NDJSON frames over pinned-TLS websockets, replicating per-origin grow-only feed logs, routing idempotent notes through the full Layer-1 ingress, and gossiping LWW-with-counter presence. The local mesh file layer stays the contract; agents never touch the network.

**Tech Stack:** Phase 1–5's PLUS four new dependencies (each added in the task that first needs it, appended at the END of `[dependencies]`, exact resolved version recorded in the commit): `ed25519-dalek 2` (+`rand_core`), `sqlx 0.8` (features `runtime-tokio-rustls,postgres,chrono` — **runtime query API only, never the compile-time `query!` macros**: builds must not need a database), `tokio-tungstenite 0.24` (+`rustls`, `rcgen` for certs), `ulid 1`.

**Spec:** `docs/superpowers/specs/2026-08-25-a2a-mesh-layer2-design.md` (rev 2.1) — §ns cited per task; every exact value there is a pin here.

**Compatible with:** devloop skill v0.1+
**Dispatch:** codex

## Ground truth (verified against merged main `07a515d`, 2026-08-26)

- 818+ tests green; zero-warning gate; clippy baseline EXACTLY 4 (src/exec.rs:554, tests/cmd_doctor.rs:147, tests/fold_model.rs:14,47). Repo NOT rustfmt-clean — never `cargo fmt`.
- Layer 1 mesh is live: `src/mesh/{mod,presence,feed,notes,routing}.rs`, `src/commands/mesh_cmd.rs`, `MESH_FIELDS: [&str; 3]`, `store::open_named/read_events_named` (parametrized rotated log), `mesh/peers/<provider>-<uuid>/`, `feed.jsonl`, `inject`, hooks. `Presence`/`Note`/`FeedEntry` are `#[serde(flatten)]`-tolerant — Layer 2's added fields (`origin`, `origin_seq`, `presence_seq`, `home_gateway`, `note_id`) extend them WITHOUT breaking Layer-1 files (absent = local/legacy, pinned by test).
- Config discipline: new `[mesh.gateway]` nested table gets its own `MESH_GATEWAY_FIELDS: [&str; 12]` + walk + env arm (`A2A_MESH_GATEWAY_<KEY>`), origin keys `mesh.gateway.<key>`, modeled on the `[sync]`/`[mesh]` walks. Append-only discipline everywhere (Cmd variants, arms, module lines LAST).
- `redact::scan_text`/`redact_egress`; Layer-1 ingress caps; `store` append discipline (O_APPEND complete lines, torn-tail tolerance) — remote feed appends go through the SAME store path (spec §5).
- gc's mesh sweep exists (Task 8 of L1) — origin retirement and remote-homed-peer cache rules EXTEND it, never rewrite it.
- Broker daemon idioms to copy: flock singleton (`broker.lock`), health json, `.jsonl` event log via a dedicated writer task, `CancellationToken`+`TaskTracker` shutdown. The gateway is a SECOND daemon with the same shapes (`mesh/gateway.lock` etc.) — read `src/broker/mod.rs`/`core.rs` for the house patterns, share nothing runtime with the broker.
- Actor discipline is source-scan-tested (no `Arc<Mutex` in broker dirs) — `src/mesh/gateway/` must satisfy the same scan; check `tests/` for the scan's path list and ADD the new dir to it.
- Codex-dispatch conventions (proven over 10 workers): worker does TDD + focused tests only (sandbox denies `ps` — NO full suite, NO commits by workers); the ORCHESTRATOR runs the full suite and commits `--no-verify` (beads pre-commit hook hangs in this repo). Plan file never committed by an executor. Load-flaky suites (isolate before believing): cmd_gc, admission, cmd_follow, cmd_verify, elevation, auto_review, templates, owner_scope, fleet_smoke, mesh_routing, cmd_spawn::spawn_general.
- Hermetic rule: tests never touch real `~/.a2a`, `~/.codex`, `~/.claude*`; loopback network only; never set `A2A_E2E`/`A2A_EVAL`/`A2A_SYNC_LIVE`/`A2A_MESH_LIVE`/`A2A_MESH_L2_LIVE`. **Time is a port** (spec §11): staleness/expiry/backoff computations take an injected clock; `tokio::time::pause`/`advance` where no real IO is in the loop.
- New-dependency protocol: `cargo add <crate>` must resolve offline-or-quickly; if resolution fails, return status "stuck" naming the crate — do not vendor, do not pin random versions.

## Global Constraints

- Exit codes: **7** reachability (registry unreachable, incl. timeout), **9** static precondition, **3** ambiguous prefix, **0** success. Typed `AppError` envelopes name codes and remedies, never secret values.
- Private keys (`root.key`, `gw.key`, TLS key) and `registry.env`: 0600, read only by CLI/gateway, NEVER printed/logged/passed through agent context. CLI prints fingerprints only.
- Frames forgiving (unknown kinds ignored), token claims strict (`deny_unknown_fields` + canonical re-serialization check). Max frame 2 MiB.
- Every `select!` branch in `src/mesh/gateway/` is cancellation-safe; frame writes via `SinkExt::send`/owned buffers, never `write_all` in a branch; no lock guard or permit across `.await`; link tasks in a `JoinSet`; bounded mpsc everywhere; raising channel capacity is never a fix.
- All retries/redials/heartbeats use FULL JITTER (`sleep(rand(0, min(cap, base·2^n)))`, ±10% on the heartbeat).
- FSM untouched; broker untouched (except gc extension named in T9); zero new warnings; clippy baseline stays 4.
- Commits: `--no-verify`, no co-author lines, exact messages from each task's Steps line.

## File Structure (additions)

```
src/mesh/
  keys.rs        # fleet root + gateway keys, TLS cert (rcgen), token claims/sign/verify, clock port
  registry.rs    # trait Registry + file fake + sqlx impl, migration constant
  wire.rs        # frame enum, NDJSON codec, handshake (challenge/PoP), pinned-TLS connectors
  gateway/mod.rs        # supervisor, five loops, lifecycle, health, gateway.jsonl
  gateway/link.rs       # dial/accept, liveness, flap damping, mailboxes, shed policy
  gateway/replicate.rs  # per-origin feed replication, cursors, catch-up, anti-entropy
  gateway/notes_wire.rs # outbound spool delivery, note_id dedup, remote ingress
  gateway/presence_wire.rs # gossip, tuple-LWW merge, origin retirement
  gateway/expose.rs     # trait Exposure: none | manual | ngrok (TCP tunnel)
migrations/mesh_l2_0001.sql
tests/
  mesh_keys.rs  mesh_registry.rs  mesh_wire.rs  mesh_gateway_lifecycle.rs
  mesh_gateway_links.rs  mesh_gateway_feed.rs  mesh_gateway_notes.rs
  mesh_gateway_presence.rs  mesh_gateway_expose.rs  mesh_gateway_e2e.rs  mesh_l2_live.rs (gated)
```

---

### Task 1: Keys, tokens, TLS certs, and the clock port (`a2a-o4o`)

**Acceptance Criteria:**
- Deps `ed25519-dalek 2` + `rcgen` + `ulid 1` added (END of `[dependencies]`; resolved versions in the commit body). `tests/cli_contract.rs::toolchain_is_pinned` untouched and green.
- `src/mesh/keys.rs`: fleet root gen (`root.key`/`root.pub`, 0600/0644) with id `fleet-<8 hex>`; gateway keypair (`gw.key`, 0600) with STABLE id `gw-<hostname>-<6 hex>` (regen preserves id when `gw.key` exists; `--fresh` mints new — pinned both ways); rcgen self-signed TLS cert + key beside `gw.key`, SPKI sha256 exposed as `cert_fpr()`.
- `TokenClaims { fleet_id, gateway_id, pubkey_b64, issued_at, exp }` with `#[serde(deny_unknown_fields)]`; `sign_token(claims, root)` and `verify_token(bytes, roots: &RootSet, now)` where verification CANONICALIZES (parse → deterministic re-serialize: sorted keys, no whitespace, integer seconds → byte-compare against signed bytes; mismatch = refusal — a claims-stuffed token with an extra field is REFUSED, pinned by test). `RootSet` = `root.pub` + optional `root.pub.next` (either verifies — pinned).
- Clock skew: `exp`/`issued_at` checked with ±300s tolerance against an injected `Clock` trait (`SystemClock` + `TestClock` with `advance`); a `TOKEN_EXPIRED` error's message carries both timestamps (pinned by substring, values are timestamps not secrets).
- `mint_ulid(clock)` for trace/note ids.
- Every function pure or fs-local; no network. Unit tests pin: sign/verify roundtrip, stuffed-claims refusal, skew boundaries (±299 ok, ±301 refused), root-set rotation acceptance, id stability, fingerprint format (64 lowercase hex), key files 0600, NO private key material in any error/Debug output (test greps a forced error corpus).

**Verify via:** cli
**Apply:** docs/development/appendix-api-design.md — strict claims at trust boundaries; canonicalize before verifying.

**Files:** Create `src/mesh/keys.rs`, `tests/mesh_keys.rs`; Modify `Cargo.toml`(+lock), `src/mesh/mod.rs` (`pub mod keys;` appended).

**Interfaces:**
```rust
pub trait Clock: Send + Sync { fn now(&self) -> chrono::DateTime<chrono::Utc>; }
pub struct FleetRoot { pub fleet_id: String }   // fleet_init(home) -> FleetRoot; loads/creates
pub struct GatewayIdentity { pub gateway_id: String, pub cert_fpr: String }
pub fn gateway_identity(home: &A2aHome, fresh: bool) -> Result<GatewayIdentity, AppError>;
pub struct RootSet(/* 1..=2 verifying keys */);
pub const SKEW_TOLERANCE_SECS: i64 = 300;
pub fn sign_token(claims: &TokenClaims, home: &A2aHome) -> Result<String, AppError>;   // b64
pub fn verify_token(token_b64: &str, roots: &RootSet, clock: &dyn Clock) -> Result<TokenClaims, AppError>;
pub fn mint_ulid(clock: &dyn Clock) -> String;
```

- [ ] Steps: failing tests → RED → implement (deps → keys/certs → claims/canonical verify → skew/clock) → GREEN → focused tests + zero warnings → orchestrator gates+commits `feat: mesh l2 keys — fleet root, gateway identity, strict tokens, pinned TLS certs`.

---

### Task 2: Registry — migration, sqlx client with the operational pins, fleet CLI (`a2a-o4o`)

**Acceptance Criteria:**
- Dep `sqlx 0.8` (`runtime-tokio-rustls,postgres,chrono`) added; **runtime query API only** — a build with no `DATABASE_URL` and no network compiles (pinned: CI-shaped `cargo build` needs nothing external).
- `migrations/mesh_l2_0001.sql` = the spec §3 schema verbatim (incl. `cert_fpr`, `revoked_at`, `schema_version`). Applied ONLY by `a2a mesh fleet init` (gateways never run DDL — pinned by the client having no DDL strings, asserted by source scan in the test).
- `trait Registry { async fn upsert_self(&self, row) ; heartbeat(&self, fleet, gw, now); discover(&self, fleet) -> Vec<GatewayRow>; retire_self(...); revoke(...); schema_version(&self) }` + `FileRegistry` fake (tempdir JSON; implements everything; used by ALL hermetic tests) + `PgRegistry` with the four pinned behaviors:
  1. `PgSslMode::VerifyFull` FORCED; a URL that would downgrade → startup refusal `REGISTRY_TLS_REQUIRED` (exit 9).
  2. host contains `-pooler` ⇒ `statement_cache_capacity(0)` (unit test pins the branch on URL parsing alone, no connection).
  3. any server-side bound via `SET LOCAL` in-transaction only (source scan: no bare `SET ` string).
  4. every call in `tokio::time::timeout(registry_timeout_secs)` (first acquire 2×); timeout ⇒ SAME error class as refusal: `REGISTRY_UNREACHABLE`, exit **7**. Pool: `max_connections(1)`, `min_connections(0)`, `idle_timeout(60s)` (pinned by options-construction unit test).
- CLI: `a2a mesh fleet init [--registry -]` (mint root via T1, read conn string from stdin → `mesh/fleet/registry.env` 0600, apply migration, print fleet id + root fingerprint ONLY); `fleet ls [--json]` (rows + reader-computed staleness: active < 3× heartbeat interval; exit 7 unreachable); `fleet revoke <gateway-id>`; `fleet rotate-root` (mint `root.pub.next`, re-sign own token, print both fingerprints; the drop-old-root step is documented as manual-per-machine in `--help`).
- `discover` skips `revoked_at IS NOT NULL` rows (pinned on the fake AND by the Pg impl's WHERE clause string).
- Hermetic tests all on `FileRegistry` + URL/options unit tests for Pg pins. NO live Postgres in this task (gated tier is T11).

**Verify via:** cli
**Apply:** docs/development/pgvector-rust-batch-embedding.md — pooler statement-cache and GUC leakage; bounded external calls.

**Files:** Create `src/mesh/registry.rs`, `migrations/mesh_l2_0001.sql`, `tests/mesh_registry.rs`; Modify `Cargo.toml`(+lock), `src/mesh/mod.rs`, `src/cli.rs` (`MeshCmd::Fleet{..}` appended), `src/commands/mesh_cmd.rs`, `src/main.rs`.

- [ ] Steps: failing tests → RED → implement (dep → migration → trait+fake → Pg pins → fleet CLI) → GREEN → focused + zero warnings → orchestrator gates+commits `feat: mesh l2 registry — migration, pinned sqlx client, fleet CLI`.

---

### Task 3: Wire — frames, codec, mutual PoP handshake, pinned TLS (`a2a-90l` begins)

**Acceptance Criteria:**
- Dep `tokio-tungstenite 0.24` + rustls stack added.
- `src/mesh/wire.rs`: `Frame` enum `{Challenge{nonce}, Hello{token, nonce_sig}, Feed{origin, entries}, Note{note}, Presence{peers}, Digest{origins}, Want{origins}, Ack{ref_id, outcome}, Err{code, detail}}` — serde-tagged NDJSON, **unknown frame kinds ignored by the reader** (pinned: a made-up kind is skipped, stream continues), 2 MiB frame cap (oversize = `WIRE_FRAME_TOO_LARGE`, connection closes cleanly).
- **Mutual handshake, exactly spec §2**: acceptor sends `Challenge`; dialer answers `Hello{token, nonce_sig}`; acceptor verifies token (T1 `verify_token`) AND `nonce_sig` against the token's `pubkey_b64`; roles reverse. Refusal paths each pinned by test: expired token (skew-aware), stuffed claims, wrong-key nonce_sig (stolen-token case — MUST refuse), unknown root.
- **Pinned TLS**: server side serves the T1 rcgen cert; `connect_pinned(url, expected_cert_fpr)` uses a custom rustls verifier accepting EXACTLY the SPKI fingerprint (hostname irrelevant — pinned by test connecting via `wss://127.0.0.1` to a cert with a random CN). Wrong fingerprint = refusal before any frame.
- Loopback pair helper `pub async fn test_link(clock) -> (Side, Side)` for later tasks.
- Cancellation safety pinned NOW: the reader/writer halves expose only cancel-safe awaits; a test cancels a writer mid-large-frame and asserts the peer's reader sees either the whole frame or a clean close — never a torn line.

**Verify via:** cli
**Apply:** docs/development/rust-async-cancellation-graceful-shutdown.md — cancel-safe wire IO; appendix-api-design.md — forgiving frames, strict credentials.

**Files:** Create `src/mesh/wire.rs`, `tests/mesh_wire.rs`; Modify `Cargo.toml`(+lock), `src/mesh/mod.rs`.

- [ ] Steps: failing tests → RED → implement (frames/codec → TLS plumbing → handshake) → GREEN → focused + zero warnings → orchestrator gates+commits `feat: mesh l2 wire — NDJSON frames, mutual PoP handshake, fingerprint-pinned TLS`.

---

### Task 4: Gateway daemon skeleton — supervisor, lifecycle, config, health

**Acceptance Criteria:**
- `[mesh.gateway]` config lands whole: `MESH_GATEWAY_FIELDS: [&str; 11] = ["listen_port","expose","endpoint_url","registry_heartbeat_secs","registry_timeout_secs","replication_poll_secs","outbound_retry_max_secs","ping_secs","anti_entropy_secs","feed_retain_days","shutdown_grace_secs"]` — exactly the 11 spec §8 keys, nothing else; an in-module pin test asserts the exact array. Defaults per §8; origins `mesh.gateway.<key>`; env `A2A_MESH_GATEWAY_*`; `config show --origin` lists all (pinned).
- `a2a mesh gateway run|status|stop`: flock singleton `mesh/gateway.lock`; `gateway-health.json` written by the SUPERVISOR ONLY with monotonic `health_seq` + `written_at` (pinned: no child-loop write path exists — source scan); `gateway.jsonl` via a dedicated log task (BrokerLog idiom).
- Supervisor owns five loop slots (registry/server/dialer/watcher/anti-entropy — stubs in this task that heartbeat health) with the pinned restart policy: one-for-one for registry/server/watcher (`max_restarts 5 within 60s`, jittered backoff), rest-for-one for dialer+links; escalation writes health, releases lock, exits non-zero (pinned with a deliberately-panicking stub loop).
- Shutdown exactly §4: SIGTERM AND ctrl-c → one `CancellationToken` + `TaskTracker` → stop accepting → `err{SHUTTING_DOWN}`+Close per link → `timeout(shutdown_grace_secs, tracker.wait())` → on expiry log `drain_timeout` + count, release lock, exit non-zero (pinned with a stub task that refuses to die: the process STILL exits inside grace+2s and a fresh `gateway run` acquires the lock).
- `gateway status` exits 7 when not running, prints health summary when running. Actor source-scan extended to `src/mesh/gateway/`.

**Verify via:** cli
**Apply:** docs/development/rust-async-cancellation-graceful-shutdown.md §6 — detect/tell/wait with bounded drain; appendix-actor-model.md §3 — supervision policy.

**Files:** Create `src/mesh/gateway/mod.rs`, `tests/mesh_gateway_lifecycle.rs`; Modify `src/config.rs`, `src/mesh/mod.rs`, `src/cli.rs` (`MeshCmd::Gateway{..}`), `src/commands/mesh_cmd.rs`, `src/main.rs`, the actor source-scan test.

- [ ] Steps: failing tests → RED → implement (config → lifecycle → supervisor/restart → shutdown) → GREEN → focused + zero warnings → orchestrator gates+commits `feat: mesh l2 gateway skeleton — supervised lifecycle, bounded shutdown, [mesh.gateway] config`.

---

### Task 5: Links — dial/accept, liveness, flap damping, mailboxes, shed

**Acceptance Criteria (loopback, two gateways in one test process where possible; spawned binaries where lifecycle demands):**
- Server loop accepts; dialer maintains links to discovered gateways (via `FileRegistry`); dedup by gateway-id ordering keeps ONE link per pair (pinned: both dial simultaneously, exactly one survives).
- **Flap damping**: 3 connect-drop cycles within 5 min ⇒ dial cooldown = `outbound_retry_max_secs`, `link_flapping` logged, health carries per-peer state; INBOUND still accepted during cooldown (pinned: the damped side accepts the peer's dial and the link recovers).
- **Liveness**: ws Ping every `ping_secs`; no read for 2× ⇒ close + jittered redial (pinned by a test that pauses one side's reads and asserts closure within budget on the injected-clock path or a short real interval).
- **Mailboxes per frame class**: note/feed bounded-with-backpressure (socket read pauses; ack deferred — pinned by flooding past capacity and asserting zero drops + eventual delivery); presence bounded drop-oldest cap 64 (pinned: flood 100, newest survive); past window ⇒ `err{BUSY}`.
- **Outbound shed**: per-link queue 1024; feed/presence drop-newest when full with `outbound_shed` counter+log at most 1/min (pinned); notes are NEVER queued from memory — they dequeue from the durable spool only (asserted by construction: the link writer has no note-enqueue API; notes enter via T7's spool pass).
- Every link task in the supervisor's `JoinSet`; per-link state (cursors etc.) handed in at spawn (error-kernel prep for T6). Jittered backoff pinned: two links recovering from one outage redial at timestamps differing by > RNG floor.

**Verify via:** cli
**Apply:** docs/development/appendix-streams.md — buffering/admission control, control & stability; appendix-actor-model.md §2 — mailbox strategies.

**Files:** Create `src/mesh/gateway/link.rs`, `tests/mesh_gateway_links.rs`; Modify `src/mesh/gateway/mod.rs`.

- [ ] Steps: failing tests → RED → implement (accept/dial/dedup → liveness → damping → mailboxes/shed) → GREEN → focused + zero warnings → orchestrator gates+commits `feat: mesh l2 links — liveness, flap damping, classed mailboxes, visible shed`.

---

### Task 6: Feed replication — per-origin logs, durability, cursors, compaction

**Acceptance Criteria:**
- `FeedEntry` gains `origin: Option<String>` + `origin_seq: Option<u64>` (flatten-tolerant; Layer-1 files load unchanged — pinned). `a2a mesh feed` renders the canonical citation `origin:origin_seq` (local entries: `<self-gateway-id>:<seq>` once a gateway identity exists, plain seq before — pinned both ways); `--json` carries both.
- **Durability ordering pinned**: remote entry → SAME store append path (complete-line O_APPEND) → in-memory cursor advance → `cursors.json` tmp+rename on interval. Startup validates each `(origin,last_seq)` against `feed.jsonl` max and repairs DOWNWARD; corrupt/missing cursors rebuild by scan (pinned: delete cursors.json → restart → no re-ingest, no loss).
- **Cursor ownership**: supervisor-owned map, single writer of `cursors.json`; link tasks receive starting cursors at spawn, report advances over their channel (pinned: kill a link task mid-stream, restart, dedup absorbs the replay, cursors never regress).
- Dedup: reject `origin_seq <= last_seq || origin_seq < compacted_before_seq` (pinned both clauses).
- **Compaction**: gc mesh sweep trims feed entries older than `feed_retain_days`, raises per-origin `compacted_before_seq`; peer requesting below watermark gets `err{COMPACTED, from_seq}` and resumes from it (pinned).
- Watcher pushes local deltas per link cursor; replication works A→B on loopback with attribution (pinned).

**Verify via:** cli
**Apply:** docs/development/appendix-fsm.md — the log leads, caches follow; appendix-streams.md — WAL-first, checkpoints split from state.

**Files:** Create `src/mesh/gateway/replicate.rs`, `tests/mesh_gateway_feed.rs`; Modify `src/mesh/mod.rs` (FeedEntry fields), `src/mesh/feed.rs` (render), `src/commands/gc.rs` (trim + watermark, extending the existing sweep), `src/mesh/gateway/mod.rs`.

- [ ] Steps: failing tests → RED → implement (shape → durability/cursors → dedup/compaction → watcher push) → GREEN → focused + zero warnings → orchestrator gates+commits `feat: mesh l2 feed replication — per-origin logs, downward-repairing cursors, compaction watermarks`.

---

### Task 7: Catch-up, anti-entropy, transitive relay

**Acceptance Criteria:**
- `Want{origins:[{origin, from_seq}]}` sent by the receiver after handshake and on new-origin discovery; sender streams contiguous ranges oldest-first, size-capped; receiver advances only across a contiguous prefix, above-gap entries in a bounded per-origin reorder buffer (pinned: out-of-order delivery converges; buffer cap eviction logged).
- **Transitive relay pinned**: gateways push ALL origins they hold (per-link cursors), relaying `origin`/`origin_seq` verbatim. Three-gateway loopback A—B—C with C outbound-only: A's entry reaches C, C's reaches A (both directions through B).
- **Anti-entropy loop live** (`anti_entropy_secs`): `Digest{origins:[{origin, max_seq, count}]}` exchanged per link; receiver `Want`s where behind OR where `count` disagrees over the same range (pinned: silently delete one mid-log entry on B, next digest cycle detects and repairs). Health gains `replication: {per_origin_lag, last_anti_entropy}`; sustained lag logs `drift`.
- Anti-entropy cycle also re-checks linked peers against discovery: a revoked peer's link closes within 2 cycles (pinned on FileRegistry).

**Verify via:** cli
**Apply:** docs/development/appendix-streams.md — anti-entropy/aging detectors; CRDT join is commutative/associative/idempotent.

**Files:** Modify `src/mesh/gateway/replicate.rs`, `src/mesh/gateway/mod.rs`, `src/mesh/wire.rs` (frames exist since T3 — handlers here); Test `tests/mesh_gateway_feed.rs` (extend).

- [ ] Steps: failing tests → RED → implement (want/catch-up → relay → digest loop → revocation re-check) → GREEN → focused + zero warnings → orchestrator gates+commits `feat: mesh l2 catch-up, anti-entropy, transitive relay`.

---

### Task 8: Notes over the wire — spool, idempotent delivery, remote ingress

**Acceptance Criteria:**
- `Note` gains `note_id` (= trace ULID, minted at creation; flatten-tolerant). Local Layer-1 note flow unchanged (pinned: existing mesh tests green unmodified except shape-tolerance additions).
- Remote target (peer whose `home_gateway` ≠ self): `a2a mesh note` writes to `mesh/outbound/<gateway-id>/<file>.note.json`; the spool pass dequeues, delivers over the link, receiving gateway runs the FULL Layer-1 ingress (scan → caps → write to target peer `inbox/` with `note_id` in the filename) and acks `{delivered|rejected}`; sender archives to `outbound/sent/` (delivered) or `outbound/rejected/` (rejected — non-retriable, pinned).
- **Idempotence pinned**: receiver dedups on `note_id` (bounded per-origin delivered-set persisted beside cursors) — a redelivery after a swallowed ack lands EXACTLY ONE inbox file (the e2e-critical test: drop the ack in a test hook, force retry, count files).
- Retries: reachability-class only, full-jitter capped at `outbound_retry_max_secs`; `TOKEN_EXPIRED`/`ingress_refused` acks archive immediately (pinned).
- `trace_id` carried through and logged both sides (`from_gateway,to_gateway,to_peer,action`) — the refused-secret-note case asserts the SAME trace_id in both gateways' `gateway.jsonl` (pinned).
- Offline target: spool holds; on gateway restart the spool pass resumes (pinned).

**Verify via:** cli
**Apply:** docs/development/appendix-actor-model.md §2 — at-least-once + idempotent receiver is the only honest delivery contract.

**Files:** Create `src/mesh/gateway/notes_wire.rs`, `tests/mesh_gateway_notes.rs`; Modify `src/mesh/mod.rs` (Note.note_id), `src/mesh/notes.rs` (remote spool branch), `src/mesh/gateway/mod.rs`.

- [ ] Steps: failing tests → RED → implement (shape → spool/deliver/ack → dedup → retries/refusals) → GREEN → focused + zero warnings → orchestrator gates+commits `feat: mesh l2 notes — durable spool, idempotent remote delivery through full ingress`.

---

### Task 9: Presence gossip, roster `via`, origin retirement

**Acceptance Criteria:**
- `Presence` gains `presence_seq: u64` (monotonic per gateway, persisted in gateway state) + `home_gateway: Option<String>` (flatten-tolerant; Layer-1 loads unchanged). Merge accepts iff `(presence_seq, last_seen, gateway_id)` tuple is lexicographically greater (pinned: a peer with a clock set 1h BACKWARD still supersedes its own older presence via the counter; deterministic winner across gateways).
- Gossip on connect + on change; remote-homed peers cached with `home_gateway`; **gc rules extend the L1 sweep**: remote-homed peers are never staleness-reclaimed locally, but their cache drops when the home gateway is registry-stale > `gc_after_days` (pinned both directions).
- `a2a mesh roster` renders `via <gateway-id>` for remote peers (pinned); `--json` carries `home_gateway`.
- **Origin retirement**: registry row revoked/gone AND stale > `gc_after_days` ⇒ cursors dropped, `outbound/<gw>/` archived with `origin_retired` log + count (pinned: never a silent forever-backoff — the spool for a dead gateway ends up in `outbound/retired/` with a summary line).

**Verify via:** cli
**Apply:** docs/development/appendix-streams.md — LWW only with justified semantics; per-replica counters beat wall clocks.

**Files:** Create `src/mesh/gateway/presence_wire.rs`, `tests/mesh_gateway_presence.rs`; Modify `src/mesh/mod.rs`, `src/mesh/presence.rs`, `src/mesh/notes.rs` (roster), `src/commands/gc.rs` (extend), `src/mesh/gateway/mod.rs`.

- [ ] Steps: failing tests → RED → implement (shape/counter → gossip/merge → roster → retirement/gc) → GREEN → focused + zero warnings → orchestrator gates+commits `feat: mesh l2 presence — counter-LWW gossip, via rendering, origin retirement`.

---

### Task 10: Exposure providers + registry re-upsert

**Acceptance Criteria:**
- `trait Exposure { async fn current_url(&self) -> Result<Option<String>, AppError> }` with `NoneExposure` (always None → registry row keeps `endpoint_url NULL`), `ManualExposure` (config `endpoint_url` verbatim), `NgrokExposure`: reads the local ngrok agent API (`http://127.0.0.1:4040/api/tunnels`) for a **TCP tunnel** to `listen_port`; if the agent is running without our tunnel, starts one via the API; if no agent, typed refusal `NGROK_UNAVAILABLE` naming the install step (never spawns arbitrary binaries silently). URL mapped to `wss://<host>:<port>`.
- The exposure loop re-checks every heartbeat interval; a CHANGED url (or first url) re-upserts the registry row incl. `cert_fpr` (pinned on FileRegistry + a fake ngrok API server: rotate the fake's URL, assert re-upsert).
- Dialers use `cert_fpr` from discovery for pinning (wired through T3's `connect_pinned` — pinned e2e-lite on loopback with a wrong-fpr row refusing).
- `expose = "none"` gateway: full replication through inbound-initiated links (pinned in T7's transitive test; asserted here that its registry row stays NULL-endpoint while it converges).

**Verify via:** cli

**Files:** Create `src/mesh/gateway/expose.rs`, `tests/mesh_gateway_expose.rs`; Modify `src/mesh/gateway/mod.rs`, `src/mesh/registry.rs` (row cert_fpr wiring if not already).

- [ ] Steps: failing tests → RED → implement (trait + none/manual → ngrok API client → re-upsert loop → pin wiring) → GREEN → focused + zero warnings → orchestrator gates+commits `feat: mesh l2 exposure — pluggable providers, ngrok TCP tunnels, cert-fpr publication`.

---

### Task 11: e2e, gated live tier, README

**Acceptance Criteria:**
- `tests/mesh_gateway_e2e.rs` (hermetic, loopback, FileRegistry, injected clocks where no socket IO is inside the paused region): fleet init → enroll ×3 → the spec §11 list VERBATIM: feed replication with `origin:seq` attribution + reconnect catch-up via `want{}`; three-gateway transitive relay with an outbound-only C; remote note through full ingress + `inject`; **lost-ack redelivery → exactly one inbox file**; offline-spool then deliver on restart; secret-note refusal with the same `trace_id` in both logs; token-expiry AND bad-PoP handshake refusals; mid-burst cancellation leaves no torn frame; jitter divergence on simultaneous recovery; skewed-clock presence (counter wins).
- `tests/mesh_l2_live.rs`, `#[ignore]` + `A2A_MESH_L2_LIVE=1` runtime re-check (sync_live pattern): real Neon — migration presence check, upsert/heartbeat/discover/retire OWN ROW only, and the pooler branch asserted against a real `-pooler` URL (statement cache 0 in effect — one prepared statement executed twice through the pooler without error). Header comment: own-row writes only; no gateway daemon started against the real home.
- README gains a **Mesh fleet (layer 2)** section: fleet init flow, enroll, gateway run, exposure options, the security model in five sentences (pinned TLS + PoP tokens + ingress re-scan + endpoints-only registry + revocation), the gated invocation, and the operational notes (Neon scale-to-zero pool shape, jittered everything, anti-entropy drift lines).
- Full `cargo test --no-fail-fast` green, zero warnings, clippy baseline 4, actor scan green over `src/mesh/gateway/`, drift gate green (skills untouched by L2 — pinned no-change).

**Verify via:** cli

**Files:** Create `tests/mesh_gateway_e2e.rs`, `tests/mesh_l2_live.rs`; Modify `README.md`.

- [ ] Steps: write the e2e → fix what it exposes (owning modules, never weakened assertions) → GREEN → gated tier written (run once locally by the ORCHESTRATOR against real Neon at the gate) → README → full suite, zero warnings → orchestrator gates+commits `feat: mesh l2 e2e on loopback fleets, gated Neon live tier, docs`.

---

## Plan self-review notes

**Spec coverage:** §2 → T1 (keys/tokens/skew/rotation-set/TLS certs) + T3 (PoP handshake) + T2 (revocation); §3 → T2 (schema/migration-ownership/client pins) + T11 (live tier); §4 → T4 (lifecycle/supervision/shutdown/health) + T5 (links/liveness/damping/mailboxes/shed) + T7 (anti-entropy loop) + T8 (trace_id); §5 → T6 (durability/cursors/dedup/compaction) + T7 (want/relay) + T8 (notes/note_id) + T9 (presence/retirement); §6 → T10; §7 → gates distributed (T3 handshake, T8 ingress re-scan, T5 caps); §8 → T4; §9 → T2/T4/T10 CLI; §10 → failure modes each land in the task owning the mechanism (pinned there); §11 → per-task + T11; §12 deferrals untouched.

**Type consistency:** `Clock`/`RootSet`/`TokenClaims`/`mint_ulid` (T1) consumed by T2/T3/T8; `Registry`+`FileRegistry` (T2) by T5/T7/T10/T11; `Frame` set complete in T3, handlers land in T5–T9; `GatewayIdentity.cert_fpr` (T1) published by T10, pinned by T3's `connect_pinned`.

**Known pin interactions:** `MESH_GATEWAY_FIELDS` is NEW (no existing array length changes); `MeshCmd` gains `Fleet` (T2) and `Gateway` (T4) variants appended; gc sweep EXTENDED in T6 (feed trim) and T9 (retirement) — both must keep every existing cmd_gc + mesh gc test green unmodified; FeedEntry/Note/Presence shape additions are flatten-tolerant with explicit Layer-1-file compatibility tests in the task that adds each field.
