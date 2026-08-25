# a2a Mesh Layer 2: Fleet Gateways + Endpoint Registry — Design

**Date:** 2026-08-25 (rev 2: mx-corpus hardening pass — 22 findings + 12 gaps folded in) · **Status:** approved-pending-review · **Epic:** beads `a2a-ecl`; issues `a2a-o4o` (registry, builds FIRST) and `a2a-90l` (gateway)
**Depends on:** Layer 1 (shipped 2026-08-24): the `$A2A_HOME/mesh/` file layer, `a2a mesh` CLI, hooks, worker-originated routing.

## §1 Purpose & scope

Take the mesh off-machine: every gateway-running machine in the fleet replicates the buzz feed, routes directed notes, and gossips presence — while agents keep using the exact same local commands. Turn-based sessions and sandboxed workers hold no sockets; the **per-machine gateway** is the network ambassador.

Decisions locked in brainstorm (2026-08-25, Mike):
- **Topology: fleet-first, public-ready.** V1 connects Mike's own machines/accounts, authenticated with fleet-issued keys; the registry schema and token mechanism are shaped so opening to other owners later is wider issuance + a `trust_tier` column, not a redesign.
- **Connection: replicating gateways.** Each machine runs `a2a mesh gateway run`, syndicating its local mesh file layer over **wss**. Local-first; offline-tolerant; eventually consistent; the file layer stays the contract.
- **Registry: Neon Postgres** (existing account). Endpoints only — never message content.
- **Auth: fleet root key + per-gateway tokens** (ed25519), with proof-of-possession (§2).
- Exposure: `endpoint_url` is whatever URL reaches the gateway — LAN/tailscale directly, or a managed **ngrok** tunnel behind a provider trait. ngrok is one provider, not a dependency.

Out of scope (recorded, §12): stranger registration/trust tiers, e2e payload encryption, multi-fleet federation, mid-turn push.

## §2 Identity, tokens, and key lifecycle

- **Fleet id**: `fleet-<8 hex>` minted by `a2a mesh fleet init`. One fleet per registry namespace in v1.
- **Gateway id**: `gw-<hostname>-<6 hex>` minted at first enroll and **stable across re-enrolls**: `a2a mesh gateway enroll` PRESERVES the existing id when `mesh/gateway/gw.key` exists (re-enroll = re-sign the same identity); `--fresh` deliberately mints a new id (a rebuilt machine is a new origin; §5 covers origin retirement). A gateway represents ONE machine's `$A2A_HOME`.
- **Peer ids are unchanged** (`provider:uuid`, Layer 1 §2). Presence gains `home_gateway`; `roster` shows `via <gateway-id>` for remote peers.
- **Keys**: ed25519. Fleet root at `$A2A_HOME/mesh/fleet/root.key` (0600) + `root.pub`; per-gateway `mesh/gateway/gw.key` (0600). Private keys are read only by the CLI/gateway process and NEVER printed, logged, or passed through agent context. CLI prints fingerprints only.
- **Gateway token**: claims `{fleet_id, gateway_id, pubkey_b64, issued_at, exp}` signed by the fleet root. **Claims are strict where frames are forgiving**: `#[serde(deny_unknown_fields)]`, and verification canonicalizes — parse, re-serialize with a deterministic writer (sorted keys, no whitespace, integer-second timestamps), refuse if the re-serialized bytes differ from the signed bytes (closes claims-stuffing). Default expiry 90 days; `enroll --renew` re-issues. **Clock skew tolerance ±300s** on `exp`/`issued_at`; a `TOKEN_EXPIRED` refusal's detail includes both clocks' timestamps so a skewed machine is diagnosable as a clock problem.
- **Proof-of-possession — the token is never a pure bearer credential.** Handshake: acceptor sends `challenge{nonce}`; dialer replies `hello{token, nonce_sig}` where `nonce_sig` is the gateway key's signature over the nonce; acceptor verifies the token against the root set AND the signature against the token's `pubkey_b64`, then the roles reverse (mutual). A stolen token file without `gw.key` is useless.
- **Root rotation**: verification accepts a SET — `root.pub` plus optional `root.pub.next`. `a2a mesh fleet rotate-root` mints the next root, distributes `root.pub.next`, re-signs gateway tokens; the old root is dropped once every registry row shows a token signed by the new one. No fleet-wide re-enroll flag day.
- **Revocation**: registry rows gain `revoked_at timestamptz`; discovery skips revoked rows, and every link re-verifies its peer against discovery each anti-entropy cycle (§4 loop 5), so a revoked gateway is dropped within ~2 cycles even on an established link. `a2a mesh fleet revoke <gateway-id>` sets it.
- **Audit provenance**: every handshake line in `gateway.jsonl` (accept AND refuse) records the peer's `gateway_id`, token `pubkey_fpr`, and which root pubkey verified it.

## §3 The registry (`a2a-o4o`, builds first)

**Schema (Neon Postgres; one migration file checked into the repo):**

```sql
CREATE TABLE IF NOT EXISTS mesh_gateways (
  fleet_id       text NOT NULL,
  gateway_id     text NOT NULL,
  owner          text NOT NULL,
  endpoint_url   text,                      -- wss://…; NULL = enrolled but unreachable (outbound-only)
  pubkey_fpr     text NOT NULL,
  version        text NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  last_heartbeat timestamptz NOT NULL DEFAULT now(),
  revoked_at     timestamptz,
  schema_version int  NOT NULL DEFAULT 1,
  PRIMARY KEY (fleet_id, gateway_id)
);
```

- **Migration ownership**: applied ONLY by `a2a mesh fleet init` (the root-holding act) — gateways never run DDL. A gateway whose client is behind the table's `schema_version` refuses to start with a typed error naming both versions; two gateways can never race migrations because only init migrates.
- **Staleness computed by readers** from `last_heartbeat` (`active` < 3× heartbeat interval, else `stale`); no reaper. `a2a mesh fleet ls` renders it.
- **Registry client** (`src/mesh/registry.rs`): trait `Registry { upsert_self, heartbeat, discover, retire_self }`; production impl = sqlx/Postgres; a file-backed fake for every hermetic test. Operational pins, each from verified corpus practice:
  - **TLS is verify-full or refuse**: the client forces `PgSslMode::VerifyFull`; a connection string that would downgrade is a startup refusal, not a warning (sqlx silently accepts `Prefer` otherwise).
  - **Pooler statement-cache**: if the URL host contains `-pooler` (Neon's PgBouncer transaction-mode endpoint), the client sets `statement_cache_capacity(0)` — sqlx's per-connection prepared-statement names collide across multiplexed backends and surface as intermittent `prepared statement "sqlx_s_N" already exists` under load, exactly the failure a 180s heartbeat hits rarely and inexplicably. Unit test pins the branch.
  - **Session GUCs**: any server-side bound (`statement_timeout`) is `SET LOCAL` inside the statement's transaction, never a bare `SET` on a pooled connection (leaks onto unrelated clients).
  - **Every call bounded**: `tokio::time::timeout(registry_timeout_secs, …)` (default 10s; the first acquire gets a 2× budget for a scale-to-zero cold start); a timeout maps to the SAME reachability error class as a refusal — `fleet ls` exits 7, the health file says `registry: unreachable since <ts>`, the heartbeat task never blocks on a black-holed socket.
  - **Pool shape for scale-to-zero**: `max_connections(1)`, `min_connections(0)`, `idle_timeout(60s)` — a daemon that heartbeats every 3 minutes must not pin a Neon compute awake around the clock; cold-start latency is absorbed by the timeout budget.
- Connection string in `$A2A_HOME/mesh/fleet/registry.env` (0600); `fleet init --registry -` reads it from stdin; never through agent context.
- The registry stores **endpoints only** — a compromise leaks topology, never content.

## §4 The gateway daemon (`a2a-90l`)

`a2a mesh gateway run` — a second daemon beside the broker: `mesh/gateway.lock` flock singleton, `mesh/gateway-health.json`, `mesh/gateway.jsonl` event log, tokio. NOT autostarted (explicit `run`; LaunchAgent is the operator's choice) — a network daemon must be a deliberate act.

**Five loops under one supervisor:**
1. **Registry**: heartbeat (±10% jitter on the 180s interval) + rediscovery; dial new gateways.
2. **Server**: accept wss; challenge/hello handshake (§2) before any frame.
3. **Client dialer**: maintain outbound links to discovered gateways; dedup by gateway-id ordering with **flap damping** — a link that connects-then-drops 3× in 5 minutes enters a dial cooldown of `outbound_retry_max_secs` (`kind: "link_flapping"` logged; per-peer state in health). A gateway in cooldown still ACCEPTS inbound from that peer, so one side always recovers the link.
4. **Replication watcher**: poll local mesh deltas (`replication_poll_secs`), push frames.
5. **Anti-entropy**: every `anti_entropy_secs` (300) exchange `digest{origins: [{origin, max_seq, count}]}` per link; the receiver issues `want{}` for any origin where it lags or where `count` disagrees over the same range (catches silently dropped entries a watermark alone cannot). Results surface as `replication: {per_origin_lag, last_anti_entropy}` in health; a `kind: "drift"` line logs sustained lag. This loop also re-checks each linked peer against discovery (revocation, §2).

**Supervision policy (pinned):** registry loop, server, and watcher restart **one-for-one** (`max_restarts: 5 within 60s`, jittered exponential backoff capped at `outbound_retry_max_secs`); the dialer and its link tasks are **rest-for-one** (a dialer restart tears down its links so cursors re-negotiate rather than resume half-dead). On escalation the daemon does NOT spin: it records the fact in health, releases the lock, exits non-zero.

**Wire discipline (pinned; every rule reviewer-checkable):**
- NDJSON frames over wss: `challenge`, `hello`, `feed`, `note`, `presence`, `digest`, `want`, `ack`, `err`. Unknown frame KINDS ignored (forward compat) — the forgiving policy applies to frames only, never token claims (§2). Max frame 2 MiB.
- **Every `select!` branch in the gateway is on tokio's cancellation-safe list** (`next()`, `recv()`, `cancelled()`, `accept()`); frame writes go through `SinkExt::send`/`write_all_buf` on caller-owned buffers, NEVER `write_all` inside a branch — a dropped `write_all` mid-frame is a torn NDJSON line the peer cannot resynchronize. Cancel-unsafe work (registry calls, ingress scan + file writes) runs in spawned tasks; loops select on the `JoinHandle`. A test cancels a link mid-burst and asserts no partial line reached the wire.
- **Futurelock rules**: one spawned task per link owned by a `JoinSet` (never a parked `&mut fut`, never `FuturesUnordered` of inline futures); supervisor↔link communication only over bounded `mpsc`; no lock guard or channel permit held across an `.await`; no `.await` inside a `select!` branch handler. If a link stalls, the remedy is the shed/backoff/escalation path — **raising channel capacity is explicitly not a fix**.
- **Mailbox policy per frame class**: `note`/`feed` inbound use bounded-with-backpressure (stop reading the socket / defer the ack rather than drop — these are the at-least-once payloads); `presence` uses bounded drop-oldest (cap 64; safe because presence is LWW); past the backpressure window the gateway answers `err{BUSY}`.
- **Outbound shed policy**: bounded per-link outbound queue (1024). Feed/presence frames drop-newest when full — cursors (§5) re-send feed on the next sweep and presence is superseded by LWW; **note frames never drop** — they stay durable in `mesh/outbound/` and simply aren't dequeued this pass. Every shed increments a health counter and logs `kind: "outbound_shed"` at most once/min: a wedged link is visible, never silent.
- **Liveness**: ws Ping every `ping_secs` (30); a link with no read activity for 2× `ping_secs` is closed and redialed — a half-open TCP (laptop sleep, NAT/ngrok idle-reap) must never sit "connected" while replication silently stalls. Health carries per-link `last_read`.
- **Tracing**: every `note` and `feed` frame carries a `trace_id` (ULID minted by the ORIGINATING gateway), preserved through relays, acks, and the ingress path, logged on both sides with `{from_gateway, to_gateway, to_peer, action}` — a note refused at the remote ingress produces linkable log lines on both machines.

**Health file**: written by the SUPERVISOR only (never a child loop — the loop that dies must not be the one reporting health), carrying a monotonic `health_seq` + `written_at` so staleness of the file itself is detectable.

**Shutdown (pinned sequence)**: detect SIGTERM **and** ctrl-c → one `CancellationToken` fans out to all loops and links (all owned by a `TaskTracker`) → stop accepting, send `err{SHUTTING_DOWN}` + ws Close per link → `tracker.close(); timeout(shutdown_grace_secs, tracker.wait())` (default 10s) → on timeout log `kind: "drain_timeout"` with outstanding count, release `gateway.lock`, exit non-zero rather than hang. `retire_self` is best-effort inside the same budget — shutdown never blocks on the registry (reader-computed staleness already covers a stale row).

## §5 Replication semantics

- **Feed — grow-only per-origin logs, transitive.** `FeedEntry` gains `origin` + `origin_seq` (absent = local, backfilled on read). Remote entries append with LOCAL seqs, origin attribution preserved; **the canonical, fleet-stable citation for a feed entry is `origin:origin_seq`** — `a2a mesh feed` renders it, refs use it, local seqs are display/cursor plumbing only. **Replication is transitive**: a gateway pushes ALL origins it holds (per-link cursor-scoped), relaying remote origins verbatim — the feed converges iff the link graph is connected (merge is commutative/associative/idempotent), which is what makes `expose = "none"` outbound-only gateways viable in a fleet where at least one gateway is reachable.
- **Durability ordering (pinned)**: (1) append the remote entry to `feed.jsonl` — through the SAME Layer-1 store append path every local writer uses (O_APPEND complete-line writes, torn-tail tolerance) — THEN (2) advance the in-memory cursor, THEN (3) persist `cursors.json` by tmp+rename on an interval. `feed.jsonl` is the source of truth; `cursors.json` is a cache — on startup each `(origin, last_seq)` is validated against the file and repaired DOWNWARD on mismatch (missing/corrupt cursors ⇒ rebuild by scan; never re-ingest from zero, never trust the cursor).
- **Cursor ownership**: the supervisor owns the cursor map and is the only writer of `cursors.json`; link tasks get their starting cursor at spawn and report advances over their channel. A crashed link task is a correctness no-op — it re-requests from the last durable cursor and dedup absorbs the replay (error-kernel pattern).
- **Catch-up**: after a successful handshake the receiver sends `want{origins: [{origin, from_seq}]}`; the sender streams contiguous ranges oldest-first, size-capped. The receiver advances `last_seq` only across a contiguous prefix; above-gap entries wait in a bounded per-origin reorder buffer.
- **Compaction (bounded growth)**: `[mesh.gateway] feed_retain_days` (30). gc's mesh sweep trims feed entries older than retention and raises a per-origin `compacted_before_seq` low-watermark in `cursors.json`; dedup reads `reject if origin_seq <= last_seq OR origin_seq < compacted_before_seq` so a trimmed entry can never re-ingest as new. A peer requesting below the watermark gets `err{COMPACTED, from_seq}` and resumes from the watermark.
- **Notes — routed, never broadcast, idempotent.** Every note carries `note_id` (= its `trace_id`). Local Layer-1 delivery is unchanged. Remote: write to `mesh/outbound/<gateway-id>/`, deliver, receiving gateway runs the FULL Layer-1 ingress path (scan → caps → write to the target peer's `inbox/` with `note_id` embedded in the filename) and acks; sender archives to `outbound/sent/`. **A lost ack redelivers; the receiver dedups on `note_id`** (bounded per-origin delivered-set within the retention window) so the target inbox never gets a second copy. Offline target: capped **full-jitter** backoff (`sleep(rand(0, min(cap, base·2^n)))`, cap = `outbound_retry_max_secs`) — deterministic doubling would have N gateways redial a recovering machine in the same instant. Non-retriable outcomes (`ingress_refused` ack, `TOKEN_EXPIRED`) archive immediately; only reachability-class errors retry. Fire-and-forget preserved: `roster`/`inbox` show undelivered counts, nothing errors.
- **Presence — gossip, LWW over a hybrid clock.** Presence gains `presence_seq: u64` (monotonic per gateway, persisted in gateway state). Merge accepts iff `(presence_seq, last_seen, gateway_id)` is lexicographically greater than the cached tuple — the counter first, so a skewed clock can never resurrect stale presence; `gateway_id` as the deterministic final tiebreak so all gateways converge. Remote-homed peers are cached with `home_gateway` set; gc skips them (their home owns reclamation) but drops the cache when the home gateway is registry-stale > `gc_after_days`.
- **Origin retirement**: when a gateway's registry row is revoked or gone and stale beyond `gc_after_days`, peers mark the origin retired — cursors dropped, its `outbound/<gateway-id>/` queue archived with a logged summary (`kind: "origin_retired"`, count surfaced) — never a silent forever-backoff.

## §6 Exposure

`[mesh.gateway] expose = "none" | "ngrok" | "manual"`; `manual` takes `endpoint_url` verbatim. `ngrok`: spawn/use the ngrok agent, read the public URL from its local API, upsert the registry, re-upsert on rotation. Behind `trait Exposure { fn current_url() }`. `expose = "none"` gateways are outbound-only (`endpoint_url NULL`) — viable because replication is transitive (§5) through any reachable gateway.

## §7 Security posture

- wss only; challenge/response mutual auth (§2) before any frame; failed handshakes log once and back off — never a retry storm.
- **Ingress re-scan at the receiving gateway**: every inbound note/feed/presence body passes `redact::scan_text` + Layer-1 caps BEFORE touching local files; refusals log (`ingress_refused`, with `trace_id`) and ack-as-rejected so the sender archives.
- Private keys + registry connection string: 0600, CLI/gateway-only, never in agent context or logs.
- Caps (forgiving backstops): frame 2 MiB, 64 links, 1000 inbound frames/min/link, outbound queue 1024 with the §4 shed policy.
- Replay: TLS on the wire; `(origin, origin_seq)` + `note_id` dedup make redelivery idempotent; nonces make handshakes non-replayable.

## §8 Config surface (`[mesh.gateway]`, appended per house discipline)

`listen_port` (7787), `expose` ("none"), `endpoint_url` (manual), `registry_heartbeat_secs` (180), `registry_timeout_secs` (10), `replication_poll_secs` (5), `outbound_retry_max_secs` (1920), `ping_secs` (30), `anti_entropy_secs` (300), `feed_retain_days` (30), `shutdown_grace_secs` (10). All in `MESH_GATEWAY_FIELDS`, `config show --origin`, env `A2A_MESH_GATEWAY_*`.

## §9 CLI surface (appended to `MeshCmd`)

- `a2a mesh fleet init [--registry -]` — mint fleet root, apply the migration (the ONLY migrator), write registry env, print fingerprint.
- `a2a mesh fleet ls [--json]` — rows + computed staleness (exit 7 unreachable). `a2a mesh fleet rotate-root` · `a2a mesh fleet revoke <gateway-id>`.
- `a2a mesh gateway enroll [--renew|--fresh]` — keypair + root-signed token (id-stable unless `--fresh`; enrollment happens on the root-holding machine or the token file is carried by the human; both documented).
- `a2a mesh gateway run|status|stop`.
- Existing commands unchanged; `roster`/`feed` learn `via`/`origin:seq` rendering only.

## §10 Failure modes (pinned behaviors)

- Registry unreachable (refusal OR timeout — same class): links keep working, discovery retries with full jitter, health says `registry: unreachable since <ts>`, `fleet ls` exits 7.
- Peer gateway unreachable: outbound holds with jittered capped backoff; its peers' presence ages to `stale` naturally.
- **Half-open link**: no read for 2× `ping_secs` ⇒ close + redial — replication must never stall behind a "connected" corpse.
- Token expired / clock skew: handshake `err{TOKEN_EXPIRED}` with both timestamps; own-token expiry warned in health 7 days ahead; ±300s tolerance.
- Revoked gateway: dropped from discovery immediately, from live links within ~2 anti-entropy cycles.
- ngrok URL rotates: next exposure poll re-upserts; peers redial on next discovery.
- **Crash between feed append and cursor flush**: startup validates cursors against `feed.jsonl` and repairs downward; dedup absorbs the replay window. Crash between note ack and archive: redelivery is dedup'd by `note_id` — at-least-once, idempotent, everywhere.
- **Wedged task at shutdown**: drain bounded by `shutdown_grace_secs`, forced exit, lock released — a subsequent `gateway run` never finds a stale flock.
- Link flapping on partition heal: dial cooldown after 3 drops/5min; inbound always accepted, so one side recovers the link.

## §11 Testing

- Hermetic throughout: `Registry` faked on files; two- and three-gateway harnesses on loopback; no real ngrok/Neon/network beyond loopback in the suite. **Time is a port**: staleness, token expiry, the 7-day warning, retention, and the backoff ladder all read an injected clock; tests use `tokio::time::pause`/`advance` — no wall-clock sleeps, and the backoff ladder is actually testable.
- e2e (`tests/mesh_gateway_e2e.rs`): fleet init → enroll ×2 → loopback gateways → feed replicates with `origin:seq` attribution and dedup on reconnect (incl. a `want{}` catch-up after a dropped link); three-gateway TRANSITIVE relay (A—B—C with C outbound-only, A's entry reaches C); directed note to a remote peer via the full ingress path + `inject`; **lost-ack redelivery dedup'd by `note_id`** (exactly one inbox file); offline-queue then deliver on restart; secret-note refusal acked+archived with the SAME `trace_id` in both gateways' logs; token-expiry and bad-PoP handshake refusals; mid-burst cancellation leaves no torn frame; two recovering gateways' redial timestamps differ (jitter proof); presence merge with a deliberately skewed clock (counter wins).
- Gated live tier (`A2A_MESH_L2_LIVE=1`): real Neon — upsert/heartbeat/discover/retire own row only; asserts the pooler branch (statement cache 0) against a real `-pooler` URL.
- Codex-dispatched devloop, split-verification pattern.

## §12 Deferred (recorded)

Stranger-facing registration + trust tiers (the public door); e2e payload encryption; multi-fleet federation; notify-based file watching (polling first); gateway autostart/LaunchAgent packaging; mid-turn push. Corpus-flagged and deliberately deferred with rationale: per-link outbound-queue PERSISTENT spill beyond notes (feed cursors already make loss free); registry-driven key transparency log (single-owner fleet trusts its root).
