# a2a Mesh Work Handoff (L3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move work — not just context — between fleet machines: signed work envelopes, human-approved claims, exactly-once pool claiming, crash-safe execution, idempotent result routing.

**Architecture:** Hybrid state (registry CAS rows + mesh notes/feed documents); root-signed-token trust anchor; local-state-first re-entrant approve; a new 6th supervised gateway loop for completion routing. Release = a2a 0.3.0 (wire handshake changes; fleet-wide lockstep upgrade via `a2a update`).

**Tech Stack:** Rust (existing crate; NO new dependencies — ed25519-dalek, sha2, ulid, sqlx, toml_edit all present), Postgres (Neon) via the pinned `Registry` trait, git via std::process.

**Spec:** docs/superpowers/specs/2026-09-01-a2a-mesh-work-handoff-design.md (rev 2 — BINDING; §ref in every task)

**Dispatch:** codex
**Compatible with:** devloop skill v0.1+

## Global Constraints

- Spec rev 2 is the contract; where this plan and the spec disagree, the spec wins and the disagreement is a stuck-report.
- NO new crate dependencies; do NOT `cargo add`; deps are already present with a warm cache.
- NEVER run `cargo fmt`; never run the full `cargo test` suite (sandbox denies `ps`) — focused `cargo test --test <file>` only; the orchestrator runs the full suite + commits.
- TDD: failing test observed (RED) before implementation, every task.
- Clippy baseline is exactly 4 pre-existing sites (src/exec.rs:554, tests/cmd_doctor.rs:168, tests/fold_model.rs:23,97); add nothing.
- Sandbox: no loopback TcpListener binds (use duplex IO / `A2A_TEST_MESH_GATEWAY_SERVER_BIND_OPTIONAL=1`); no network git (local `file://` bare repos and fixture clones are fine — filesystem only); never touch the real `~/.a2a`.
- All new error codes registered in `KNOWN_CODES` with exit class + remedy.
- Canonical JSON, key handling, token verification: reuse `src/mesh/keys.rs` primitives; do not invent parallel crypto plumbing.
- Version at the END of this plan is 0.3.0 (Task 10); do not bump earlier.

---

### Task 1: Work envelope — sign, verify, hygiene

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test mesh_work_envelope` green: a signed envelope round-trips; every row of the §2.1.1 verification matrix rejects with its distinct code.
- Domain separation is provable in-test: a signature produced over the raw canonical JSON (no prefix) does NOT verify as an envelope signature.
- Oversized, control-character-laden, and malformed-URL envelopes are rejected at construction, not at verify time.

**Verify via:** cli

**Files:**
- Create: `src/mesh/work.rs` (module root: envelope types + sign/verify), registered in `src/mesh/mod.rs`
- Test: `tests/mesh_work_envelope.rs`

**Interfaces (Produces — later tasks consume these exact names):**
- `pub struct WorkEnvelope { v: u8, work_id: String, fleet_id: String, sender: WorkSender, addressing: WorkAddressing, repo: WorkRepo, title: String, brief_text: String, acceptance: String, created_at: DateTime<Utc>, expires_at: DateTime<Utc>, sig_b64: String }` with `WorkSender { gateway_id, session, pubkey_b64, token }`, `WorkAddressing::{Directed(String), Pool}` (serde as `"directed:<gw>"` / `"pool"`), `WorkRepo { remote_url, base_ref }`. All structs `deny_unknown_fields`.
- `pub fn sign_envelope(home: &A2aHome, unsigned: UnsignedWorkEnvelope, clock: &dyn Clock) -> Result<WorkEnvelope, AppError>` — fills sender (identity, pubkey, gw.token read from disk), created/expires (ttl arg), signs per §2.1.1: `Sha256(b"a2a/work-envelope/v1\0" || canonical_json_without_sig)`.
- `pub fn verify_envelope(home: &A2aHome, env: &WorkEnvelope, clock: &dyn Clock) -> Result<VerifiedEnvelope, AppError>` — the §2.1.1 order: token vs RootSet (`keys::verify_token`), claim bindings (fleet/gateway/pubkey), envelope signature, local-fleet + expiry + addressing shape. Registry corroboration is NOT here (Task 4 does it at ingest with a registry handle).
- `pub fn validate_envelope_hygiene(env: &WorkEnvelope, max_bytes: usize) -> Result<(), AppError>` — §2.1.3: serialized size cap, remote_url shape (`ssh://`, `git@host:path`, `https://`; first char never `-`), control chars stripped-or-rejected in title/acceptance (reject; stripping happens only at display).
- Errors: `WORK_SIGNATURE_INVALID`, `WORK_SENDER_UNKNOWN` (token fails RootSet), `WORK_TOKEN_MISMATCH` (binding rows), `WORK_EXPIRED`, `WORK_TOO_LARGE`, `WORK_BAD_REPO_URL`, `WORK_WRONG_FLEET`.

- [ ] **Step 1:** Write tests/mesh_work_envelope.rs: happy round-trip (two tempdir homes sharing a root via the tests/mesh_enroll.rs fixture pattern — root home enrolls sender home; receiver holds root.pub); then the rejection matrix: tampered field, wrong-key signature, token from another fleet, expired token (keys::TestClock), expired envelope, oversize brief, `-oProxyCommand=...` remote_url, ESC bytes in title, unprefixed-signature cross-check. Run: `cargo test --test mesh_work_envelope` — expect FAIL (module absent).
- [ ] **Step 2:** Implement src/mesh/work.rs types + canonical JSON reuse (the same serde-canonical helper token claims use — extract from keys.rs into a shared fn if private today; note it in your result summary).
- [ ] **Step 3:** Implement sign/verify/hygiene until the matrix is green.
- [ ] **Step 4:** `cargo build` zero warnings; focused test green. No commit.

---

### Task 2: Close the handshake oracle (wire protocol, §2.1.2)

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test mesh_wire` green: a `Challenge` whose nonce is not exactly 44 chars of valid base64 decoding to 32 bytes is REFUSED before any signature is made; the hello signature is over `Sha256(b"a2a/hello/v1\0" || nonce_bytes)` and an old-format (raw-nonce) signature no longer verifies.
- Existing duplex handshake tests updated and green (both sides new format — this release is lockstep by design, spec §11).

**Verify via:** cli

**Files:**
- Modify: `src/mesh/wire.rs` (`hello_frame`, `handshake_acceptor`, `handshake_dialer`, nonce validation)
- Test: `tests/mesh_wire.rs` (extend)

**Interfaces:** unchanged public surface; wire behavior versioned by the release.

- [ ] **Step 1:** Tests first: malformed-nonce challenge (wrong length, non-base64, 31-byte payload) refused with `HANDSHAKE_BAD_NONCE` on BOTH dialer and acceptor paths before signing; signature-format test (sign raw nonce with the same key → handshake must reject). RED.
- [ ] **Step 2:** Implement: `fn require_nonce_shape(nonce: &str) -> Result<[u8;32], AppError>`; sign/verify over the domain-prefixed digest. Update the existing green-path tests' expectations.
- [ ] **Step 3:** Build clean, focused green. No commit.

---

### Task 3: Registry work methods, migration, `fleet migrate` (§2.2, §3, §11.2)

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test mesh_work_cas` green: double-claim (one winner), directed-claim by non-addressee refused in the WHERE clause, decline-after-claim reports real status, cancel races, owner-checked transitions, DB-clock expiry sweep (no client timestamp bound), abandoned sweep only touches own claims.
- FileRegistry passes the identical CAS suite under flock (same test fns, both backends via the trait).
- `a2a mesh fleet migrate` on a root home applies `mesh_l3_0001.sql` idempotently (runs twice, second is a no-op); on a joiner home it refuses with `FLEET_ALREADY_JOINED` semantics unchanged; SQLSTATE 42P01 from any work method maps to `WORK_SCHEMA_MISSING` with the migrate remedy.

**Verify via:** cli

**Files:**
- Create: `migrations/mesh_l3_0001.sql` (single `CREATE TABLE IF NOT EXISTS mesh_work_items` statement exactly per spec §2.2)
- Modify: `src/mesh/registry.rs` (trait + Pg + File impls), `src/commands/mesh_cmd.rs` (`MeshFleetCmd::Migrate`), `src/cli.rs`
- Test: `tests/mesh_work_cas.rs` (new), `tests/mesh_enroll.rs` (migrate CLI arm)

**Interfaces (Produces):**
- `pub struct WorkRow { fleet_id, work_id, sender_gw, addressing: String, status: WorkStatus, claimed_by: Option<String>, result_ref: Option<String>, result_delivery: Option<String>, swept_by: Option<String>, created_at, updated_at, expires_at, schema_version: i32 }`; `pub enum WorkStatus { Offered, Approved, Running, Done, Failed, Acked, Declined, Cancelled, Expired }` (serde snake_case strings in SQL).
- Trait methods (all async, all timeout-wrapped like existing ones): `work_insert(&self, row: WorkRow)`, `work_claim(&self, fleet_id, work_id, claimer_gw) -> Result<ClaimOutcome>` (`ClaimOutcome::{Won, Lost(WorkRow)}` — Lost carries the re-read row), `work_transition(&self, fleet_id, work_id, from: WorkStatus, to: WorkStatus, owner: WorkOwner, patch: WorkPatch) -> Result<TransitionOutcome>` (owner = `Sender(gw)`/`Claimer(gw)`/`Any`; patch sets result_ref/result_delivery/swept_by/updated_at), `work_get`, `work_list(&self, fleet_id, filter: WorkListFilter)`, `work_touch_running(&self, fleet_id, work_id, claimer)` (updated_at refresh), `work_sweep_expired(&self, fleet_id, swept_by) -> Vec<String>` (DB clock: `expires_at < now()`), `work_sweep_abandoned(&self, fleet_id, claimer_gw, stall: Duration) -> Vec<WorkRow>`.
- `a2a mesh fleet migrate`: root-only (reuses fleet_init's root.key check WITHOUT minting — call `keys::fleet_id`? no: require root.key present, error otherwise), reads registry.env, applies every `migrations/mesh_l*_*.sql` in order via the existing `apply_migration` path (one statement per file — assert this in a unit test that walks the migrations dir).

- [ ] **Step 1:** RED tests: the CAS suite written once as `fn cas_suite<R: Registry>(r: &R)` driven by both FileRegistry (flock the registry file for conditional updates — extend FileRegistry with an internal exclusive-lock guard around read-modify-write) and, hermetically, nothing else (Pg is exercised in the gated live tier, Task 11).
- [ ] **Step 2:** Implement SQL + trait + impls. Pg: every transition one conditional UPDATE with status/owner predicates; zero-rows ⇒ re-read and return the actual row.
- [ ] **Step 3:** Migrate command + 42P01 mapping (`sqlx::Error::Database` code check) + CLI test. Green, build clean, no commit.

---

### Task 4: Work store + carriage + ingest (§4.1, §7 skip-and-advance)

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test mesh_work_store` green: a `work_offer` note addressed `gateway:<my-gw>` is verified at ingest and filed to `$A2A_HOME/work/inbox/<work_id>.json` 0600; a failing envelope is acked `Rejected{code}`, logged `work_ingest_refused`, and NOT stored; work notes never appear in session inboxes or `mesh inbox`/`inject` output (only the one-line text ever would, and only for the addressed gateway store, which consumes it).
- Pool: a feed entry with `refs.work` materializes into `work/pool/<work_id>.json` after verification; an oversized/invalid `refs` feed entry produces `IngestDecision::Rejected` that ADVANCES the origin cursor (link session survives — extend the Task's feed test to prove the cursor moved past it).
- `refs` size caps enforced at note ingest and feed ingest (serialize refs, compare to `[mesh.work]` caps).

**Verify via:** cli

**Files:**
- Create: `src/mesh/work_store.rs` (inbox/pool file store: `store_offer`, `list`, `load`, `remove`), `tests/mesh_work_store.rs`
- Modify: `src/mesh/gateway/notes_wire.rs` (kind-aware ingest: `work_offer` → verify → store → ack; `gateway:<id>` addressing sentinel), `src/mesh/gateway/replicate.rs` (`IngestDecision::Rejected { reason }` + refs validation + work materialization hook), `src/mesh/gateway/link.rs` (no ingest error escapes the frame handler), `src/mesh/notes.rs` (note `kind` field if absent today — check; L1 notes have kind? if not, additive optional field, default "note"), `src/mesh/feed.rs` (refs size validation helper)
- Test: extend `tests/mesh_gateway_notes.rs`, `tests/mesh_gateway_feed.rs`

**Interfaces (Produces):** `work_store::store_offer(home, VerifiedEnvelope, WorkSource::{Directed, Pool})`, `work_store::list(home) -> Vec<StoredWork>`; note kind constant `WORK_OFFER_KIND = "work_offer"`, `WORK_RESULT_KIND = "work_result"`, `WORK_CANCELLED_KIND = "work_cancelled"`; gateway addressing sentinel `gateway:<gateway_id>` recognized in the note `to` field by ingest.

- [ ] **Step 1:** RED: store tests + ingest tests (build notes/feed entries carrying envelopes from Task 1 fixtures through the existing duplex-link test harnesses).
- [ ] **Step 2:** Implement store + ingest paths; Rejected advances cursors (mirror the dense-claiming test style in tests/mesh_gateway_feed.rs for the cursor assertion).
- [ ] **Step 3:** Green, build clean, no commit.

---

### Task 5: Sender/receiver CLI — send, offer, list, show, decline, cancel, status, ack (§4.2, §4.3, §4.6, §4.7, §5)

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test mesh_work_cli` green, hermetic (FileRegistry + two homes): `send` preflights addressee version (`WORK_PEER_TOO_OLD` when row version < 0.3.0 or missing; fixture rows), validates the brief like spawn does, inserts the row, spools the note, posts the milestone; `offer` does the feed variant with the 32 KiB cap; `list` merges local store + registry overlay and DEGRADES to store-only with a warning when the registry errors (fake erroring registry); `decline --reason` flips the row AND sends the reason note; `cancel` (sender, offered-only) + `work_cancelled` note; `ack` closes done/failed; `show` prints provenance + control-char-stripped strings; every error code carries a remedy.
- `--json` envelopes on all verbs.

**Verify via:** cli

**Files:**
- Create: `src/commands/work_cmd.rs` (all `a2a mesh work` verbs except approve/retry-push, which Task 6/7 add), `tests/mesh_work_cli.rs`
- Modify: `src/cli.rs`, `src/commands/mod.rs`, `src/commands/mesh_cmd.rs` (mount subcommand)

**Interfaces (Consumes):** Task 1 envelope fns, Task 3 registry methods, Task 4 store + note kinds. **(Produces):** `work_cmd::send_result_note(home, registry, work_id, status, reason_or_summary)` — shared by decline (here) and the Work loop (Task 7): builds the redacted, capped `work_result` note with a FIXED note_id argument.

- [ ] **Step 1:** RED CLI tests via the assert_cmd pattern of tests/mesh_enroll.rs.
- [ ] **Step 2:** Implement verbs; version preflight reads the addressee's `mesh_gateways.version` (semver compare ≥ 0.3.0; the plan runs pre-bump so tests use fixture rows, not the live constant).
- [ ] **Step 3:** Green, build clean, no commit.

---

### Task 6: Approve — local-state-first, re-entrant, allowed_roots carve-out (§4.4, §7)

**Apply:** docs/development appendix-streams.md — "log-before-apply for durability": every phase file write precedes its side effect; recovery = deterministic re-run from the log.

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test mesh_work_approve` green: the phase-resumption matrix — kill (simulated: the orchestrating fn returns early) after each of {state-written, claimed, cloned, spawned} and re-run `approve` → exactly-once side effects (fake git adapter + FileRegistry + fake spawner record call counts); CAS-lost with `claimed_by==me` resumes; CAS-lost to another gateway reports the winner and removes the claiming state file; `--dry-run` performs zero writes.
- `cargo test --test cmd_spawn` green with the new implicit work root: a spawn `--cwd` under `$A2A_HOME/work/...` is admitted in BOTH the CLI resolve and the broker admission path (extend the existing carve-out tests near the research-lane ones); `$A2A_HOME/work` is created by `A2aHome::open`.
- Preflight failures are typed: `WORK_REPO_UNREACHABLE` (fake ls-remote fail), `WORK_DAEMON_DOWN`, stale-broker probe message.

**Verify via:** cli

**Files:**
- Create: `src/mesh/work_exec.rs` (phase state machine: `WorkPhase::{Claiming, Cloning, Spawning, Running, Pushing, Reporting, Reported, Failed}`, `WorkState` file 0600 + flock, `GitAdapter` trait with `RealGit` + test fake, `advance_approve(home, registry, git, spawner, work_id) -> Result<ApproveOutcome>`), `tests/mesh_work_approve.rs`
- Modify: `src/commands/work_cmd.rs` (approve verb), `src/home.rs` (eager `work` dir in open), `src/config.rs` (`append_implicit_work_root` beside the worktrees one), `src/commands/spawn.rs` + `src/broker/core.rs` (admission carve-out, modeled on the research-lane carve-out), `tests/cmd_spawn.rs`, `tests/cmd_broker.rs`

**Interfaces (Produces for Task 7):** `WorkState { phase, work_id, envelope_digest, result_note_id, milestone_posted, worker_id: Option<String>, result_ref: Option<String>, checkout: PathBuf }`, `work_exec::load_states(home) -> Vec<WorkState>`, `work_exec::advance_completion(home, registry, git, work_state) -> Result<...>` (stub in this task; Task 7 implements the body).

- [ ] **Step 1:** RED: the kill-matrix tests with fakes; carve-out tests.
- [ ] **Step 2:** Implement state machine + approve + carve-outs. `result_note_id` = `keys::mint_ulid` at phase-1 write.
- [ ] **Step 3:** Green (mesh_work_approve, cmd_spawn, cmd_broker focused), build clean, no commit.

---

### Task 7: The Work loop — completion routing, sweeps, retry-push (§4.5, §3)

**Apply:** docs/development appendix-streams.md — "exactly-once is effectively-once: at-least-once delivery + idempotent sinks" (fixed note_id + status CAS are the idempotent sinks); poison-pill = capped jittered retries. docs/development rust-async-cancellation-graceful-shutdown.md — every `select!` branch in the loop must be cancel-safe: phase files advance only AFTER their side effect completes inside `spawn_blocking` (drop can only occur between phases, never mid-phase), bounded drain on shutdown.

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test mesh_gateway_lifecycle` green with SIX loops (health shows `work`; `HEALTH_SCHEMA_VERSION` bumped; panic/stall test hooks work for the new slot).
- `cargo test --test mesh_work_loop` green: with a fake git + FileRegistry + a terminal fake worker, one tick drives push→persist→CAS→spool→milestone; re-running the tick after a simulated crash at EACH step produces no duplicate side effects (fixed note_id, milestone_posted guard, push idempotent via fake); push failure retries with capped jittered backoff and `retry-push` CLI forces one attempt; result text passes `redact_egress` (a bearer-shaped string in fake evidence arrives `[REDACTED:n]`); expiry sweep (DB-clock semantics via the trait) + abandoned sweep (own claims only) run on the loop; a real `git push` to a LOCAL bare repo succeeds in the in-process leg (file:// — no network).
- The loop body never propagates `Err` (drive a fake registry that always errors: loop stays running, `registry_error` health pattern engages, zero restarts).

**Verify via:** cli

**Files:**
- Modify: `src/mesh/gateway/mod.rs` (LoopSlot::Work + spawn wiring + health), `src/mesh/work_exec.rs` (advance_completion body, sweep drivers, pool materialization call into work_store), `src/commands/work_cmd.rs` (`retry-push`)
- Test: `tests/mesh_work_loop.rs` (new), `tests/mesh_gateway_lifecycle.rs` (extend)

- [ ] **Step 1:** RED: lifecycle six-loop expectations; the tick idempotency matrix; never-Err test.
- [ ] **Step 2:** Implement slot (one-for-one, `work_poll_secs` interval, `spawn_blocking` for reconcile/git), completion pipeline in the §4.5 order, sweeps, `work_touch_running` refresh, `result_delivery` recording on Rejected acks (hook the ack path in notes_wire from Task 4).
- [ ] **Step 3:** Green, build clean, no commit.

---

### Task 8: gc work leg + holds (§9)

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test cmd_gc` green: a `reported`-phase work dir older than `checkout_retain_days` with a clean, remote-reachable checkout is reclaimed; dirty checkout ⇒ `kept` with remedy; unpushed HEAD ⇒ `kept`; a worker referenced by any non-terminal work state is never reclaimed by the worker leg (the work hold); gc remains registry-free (no Registry handle anywhere in the gc path).

**Verify via:** cli

**Files:**
- Modify: `src/commands/gc.rs`, `src/mesh/work_exec.rs` (state readers gc consumes)
- Test: `tests/cmd_gc.rs` (extend; fake git for reachability)

- [ ] **Step 1:** RED tests. **Step 2:** implement leg + holds. **Step 3:** green, clean, no commit.

---

### Task 9: Link observability — `mesh links` + status block (§10)

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test mesh_gateway_links` green: the daemon records per-link `{peer, direction, established_at, last_rx, last_tx, frames_rx, frames_tx}` INSIDE `gateway-health.json` (no second daemon file); sessions register/deregister entries; `a2a mesh links` renders them (and `[]` cleanly when the daemon is down, with a hint); `gateway status` human line gains `links N`.

**Verify via:** cli

**Files:**
- Modify: `src/mesh/gateway/mod.rs` (health struct additive `links` array + LoopEvent plumbing from sessions), `src/mesh/gateway/link.rs` (session hooks), `src/commands/mesh_cmd.rs` (`MeshCmd::Links`), `src/cli.rs`
- Test: `tests/mesh_gateway_links.rs`, `tests/mesh_gateway_lifecycle.rs` (health shape)

- [ ] **Step 1:** RED. **Step 2:** implement via LoopEvent (supervisor owns health writes; sessions emit LinkUp/LinkStat/LinkDown events). **Step 3:** green, clean, no commit.

---

### Task 10: Config, docs, skills, version 0.3.0, install-service noise (§8, §10, §11; ride-along a2a-0bn)

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test config_layering` green with `MESH_WORK_FIELDS` = ["ttl_days","max_directed_bytes","max_pool_bytes","result_note_max_bytes","work_poll_secs","claim_stall_secs","checkout_retain_days"] (defaults per spec §8; file/env layering like every other section).
- `cargo test --test skills_gen` green with a 10th ORCHESTRATE section `work-handoff` (send/approve/dry-run/result verbs + the milestone convention already in §mesh-milestones), artifacts regenerated in-tree.
- Version = 0.3.0 everywhere (Cargo.toml; version-pinned tests updated; the Task-5 `WORK_PEER_TOO_OLD` gate now compares against the real constant).
- `install-service` on a home with nothing loaded prints NO "Boot-out failed" noise (state-checked first); real bootout failures still surface.
- `docs/mesh-work.md` exists (operator guide: rollout steps §11, both flows, error table); `docs/mesh-join.md` gains the fleet-migrate note.

**Verify via:** cli

**Files:**
- Modify: `src/config.rs`, `skills/orchestrate-protocol.md` + `src/commands/gen_skills.rs` (+ regenerated artifacts), `Cargo.toml`, `src/mesh/gateway/mod.rs` (install-service), version-pinned tests
- Create: `docs/mesh-work.md`
- Test: `tests/config_layering.rs`, `tests/skills_gen.rs`, `tests/mesh_gateway_service.rs`

- [ ] **Step 1:** RED (config fields, 10-section pin, service-noise test with the reload-skip env). **Step 2:** implement all. **Step 3:** green, clean, no commit.

---

### Task 11: End-to-end integration + gated live tier (§12)

**Acceptance Criteria (Verify via: cli):**
- `cargo test --test mesh_work_e2e` green: two hermetic homes on one flock'd FileRegistry, duplex-linked gateways (in-process loop drivers, BIND_OPTIONAL): DIRECTED — send on A → ingest on B → list verified → approve (fake worker terminal) → Work-tick pushes to a local bare repo → result note back on A → ack; POOL — offer on A, both homes race approve, exactly one wins, loser sees the winner; declined-with-reason round trip.
- `tests/mesh_l3_live.rs` exists, `#[ignore]` + `A2A_MESH_L3_LIVE=1` gated: two independent `PgRegistry` handles contend one claim (exactly one `Won`); `fleet migrate` twice is idempotent. (Orchestrator runs this against real Neon at the gate.)

**Verify via:** cli

**Files:**
- Create: `tests/mesh_work_e2e.rs`, `tests/mesh_l3_live.rs`
- Modify: `tests/common/mod.rs` (shared two-home + bare-repo fixtures if needed)

- [ ] **Step 1:** RED e2e skeleton (directed leg). **Step 2:** drive to green; add pool + decline legs. **Step 3:** live-tier file (compiles; ignored). Build clean, no commit.

---

## Plan self-review

- Spec coverage: §1.1 (docs, Task 10 mesh-work.md trust note); §2.1 (T1); §2.1.2 (T2); §2.1.3 (T1 construct + T4 ingest + T5 send); §2.2+§11.2 (T3); §3 (T3 CAS + T7 sweeps); §4.1 (T4); §4.2/4.3 (T5); §4.4 (T6); §4.5 (T7); §4.6/4.7 (T5, T7 result path); §5 (T5/T6/T7 verbs); §6 (distributed across T4 ingest-refusal, T6 claim-keeps, T7 retries); §7 (T1 anchor, T2 oracle, T6 carve-out, T4 skip-and-advance, T7 redaction); §8 (T10); §9 (T8); §10 (T9, T10); §11 (T10 docs + the human rollout — orchestrator/exercise, not a code task); §12 (T11 + per-task tiers); §13 deferred — no tasks, correct.
- Type consistency: WorkStatus/WorkRow/ClaimOutcome names identical in T3/T5/T6/T7; WorkState fields in T6 = T7's consumption; note kinds defined once in T4.
- No placeholders; every step names its test file and expected RED.

**Exercise acceptance (human, after the train + §11 rollout):** Mike `work send`s a real item to gw-host-408f2f; John dry-runs, approves; result branch + note return; Mike acks. Then one pool round.
