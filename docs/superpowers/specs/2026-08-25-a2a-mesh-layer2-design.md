# a2a Mesh Layer 2: Fleet Gateways + Endpoint Registry — Design

**Date:** 2026-08-25 · **Status:** approved-pending-review · **Epic:** beads `a2a-ecl`; issues `a2a-o4o` (registry, builds FIRST) and `a2a-90l` (gateway)
**Depends on:** Layer 1 (shipped 2026-08-24): the `$A2A_HOME/mesh/` file layer, `a2a mesh` CLI, hooks, worker-originated routing.

## §1 Purpose & scope

Take the mesh off-machine: every gateway-running machine in the fleet replicates the buzz feed, routes directed notes, and gossips presence — while agents keep using the exact same local commands. Turn-based sessions and sandboxed workers hold no sockets; the **per-machine gateway** is the network ambassador.

Decisions locked in brainstorm (2026-08-25, Mike):
- **Topology: fleet-first, public-ready.** V1 connects Mike's own machines/accounts, authenticated with fleet-issued keys; the registry schema and token mechanism are shaped so opening to other owners later is wider issuance + a `trust_tier` column, not a redesign.
- **Connection: replicating gateways.** Each machine runs `a2a mesh gateway run`, syndicating its local mesh file layer over **wss**. Local-first; offline-tolerant; eventually consistent; the file layer stays the contract.
- **Registry: Neon Postgres** (existing account). Endpoints only — never message content.
- **Auth: fleet root key + per-gateway tokens** (ed25519). Registry-issued tokens for other owners is the public door, deferred.
- Exposure: `endpoint_url` is whatever URL reaches the gateway — LAN/tailscale directly, or a managed **ngrok** tunnel behind a provider trait. ngrok is one provider, not a dependency.

Out of scope (recorded, §12): stranger registration/trust tiers, e2e payload encryption, multi-fleet federation, mid-turn push.

## §2 Identity

- **Fleet id**: `fleet-<8 hex>` minted by `a2a mesh fleet init`. One fleet per registry namespace in v1.
- **Gateway id**: `gw-<hostname>-<6 hex>` minted at enroll. A gateway represents ONE machine's `$A2A_HOME`.
- **Peer ids are unchanged** (`provider:uuid`, Layer 1 §2). Presence gains `home_gateway: <gateway-id>`; a peer's home is the machine whose file layer first registered it. `roster` shows `via <gateway-id>` for non-local peers.
- **Keys**: ed25519 throughout. Fleet root keypair at `$A2A_HOME/mesh/fleet/root.key` (0600) + `root.pub`; per-gateway keypair at `mesh/gateway/gw.key` (0600). Private keys are read only by the CLI/gateway process and NEVER printed, logged, or passed through agent context — the same hard rule as every credential in this system.
- **Gateway token**: canonical JSON `{fleet_id, gateway_id, pubkey_b64, issued_at, exp}` signed by the fleet root; base64 of `{claims, sig}`. Verified against the fleet root pubkey on every wss handshake (both directions) and by the registry client library when reading rows (a row whose `pubkey_fpr` mismatches the presented token is refused). Default expiry 90 days; `a2a mesh gateway enroll --renew` re-issues.

## §3 The registry (`a2a-o4o`, builds first)

**Schema (Neon Postgres, one table + one migration file checked into the repo):**

```sql
CREATE TABLE IF NOT EXISTS mesh_gateways (
  fleet_id      text NOT NULL,
  gateway_id    text NOT NULL,
  owner         text NOT NULL,            -- fleet owner label; public-ready column
  endpoint_url  text,                     -- wss://…; NULL = enrolled but unreachable
  pubkey_fpr    text NOT NULL,            -- sha256 fingerprint of the gateway pubkey
  version       text NOT NULL,            -- a2a version string
  created_at    timestamptz NOT NULL DEFAULT now(),
  last_heartbeat timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (fleet_id, gateway_id)
);
```

- **Staleness is computed by readers** from `last_heartbeat` (the Layer 1 pattern): `active` < 3× heartbeat interval, else `stale`; no reaper. `a2a mesh fleet ls` renders it.
- **Registry client** (`src/mesh/registry.rs`): a trait `Registry` with `upsert_self`, `heartbeat`, `discover(fleet_id) -> Vec<GatewayRow>`, `retire_self`; the production impl speaks Postgres over TLS using the connection string from `$A2A_HOME/mesh/fleet/registry.env` (0600, `A2A_MESH_REGISTRY_URL=postgres://…`); a file-backed fake implements the same trait for every hermetic test. Connection string never through agent context; `a2a mesh fleet init --registry -` reads it from stdin.
- **Heartbeat**: the gateway daemon heartbeats every `registry_heartbeat_secs` (default 180) and re-upserts whenever its endpoint changes. CLI `a2a mesh fleet ls [--json]` lists rows with computed staleness; exit 7 when the registry is unreachable (reachability class, matching broker semantics).
- The registry stores **endpoints only**. No feed entries, no notes, no presence bodies — a registry compromise leaks topology, never content.

## §4 The gateway daemon (`a2a-90l`)

`a2a mesh gateway run` — a second daemon beside the broker, same house discipline: `mesh/gateway.lock` flock singleton, `mesh/gateway-health.json` (pid, endpoint, connected peers, last replication, token expiry), `mesh/gateway.jsonl` event log (connects, deliveries, refusals — the broker.jsonl idiom), tokio, actor-style internals (no shared locks), clean shutdown on SIGTERM. `a2a mesh gateway status|stop|enroll` manage it. NOT autostarted by other commands in v1 (explicit `run`; a LaunchAgent is the operator's choice) — a network daemon must be a deliberate act.

**Loops:**
1. **Registry loop**: heartbeat + rediscover peers every interval; dial newly discovered gateways.
2. **Server**: accept wss connections (when exposed); token handshake both directions before any frame.
3. **Client**: maintain outbound wss to every discovered fleet gateway (dedup by gateway-id ordering so two gateways keep ONE link).
4. **Replication watcher**: poll the local mesh file layer for deltas (mtime/seq cursors; notify-crate later if polling costs) and push frames.

**Wire**: NDJSON frames over wss (the a2a broker idiom): `hello{token}`, `feed{origin, entries[]}`, `note{to_peer, note}`, `presence{peers[]}`, `ack{...}`, `err{code}`. Size-capped frames; unknown frame kinds ignored (forward compat).

## §5 Replication semantics

- **Feed — grow-only per-origin logs.** `FeedEntry` gains `origin: <gateway-id>` (absent = local, backfilled on read). Each gateway tracks `(origin → last_seq)` cursors per link in `mesh/gateway/cursors.json`. Remote entries append to the local `feed.jsonl` with LOCAL seqs, `origin` + `origin_seq` preserved; dedup by `(origin, origin_seq)`. Merge of append-only logs — conflict-free by construction. `a2a mesh feed` renders origin attribution (`from claude:… @gw-mba`).
- **Notes — routed, never broadcast.** `a2a mesh note` to a peer whose `home_gateway` is remote (or unknown locally but discovered via presence gossip) writes the note to `mesh/outbound/<gateway-id>/<file>.note.json`; the gateway delivers it to the target gateway, which runs the FULL Layer-1 ingress path (scan → caps → write to the target peer's `inbox/`) and acks; the sender then archives to `outbound/sent/`. Offline target: retry with capped backoff (1m→32m), fire-and-forget preserved — `roster`/`inbox` show undelivered counts, nothing errors.
- **Presence — gossip, last-writer-wins per peer** on `last_seen`. Each gateway sends its LOCAL peers' presence on connect and on change; remote presences are cached in `mesh/peers/<peer>/presence.json` with `home_gateway` set and are never heartbeat-refreshed locally (their staleness reads from the gossiped `last_seen`). gc's mesh sweep skips remote-homed peers (their home machine owns reclamation) but drops the cached copy when the home gateway has been registry-stale > `gc_after_days`.

## §6 Exposure

`[mesh.gateway] expose = "none" | "ngrok" | "manual"`; `manual` takes `endpoint_url` verbatim (LAN, tailscale, a reverse proxy — anything). `ngrok`: the gateway spawns/uses the ngrok agent for a tls tunnel to its listen port, reads the public URL from the local ngrok API, upserts the registry, and re-upserts on rotation. Behind `trait Exposure { fn current_url() }` — providers are pluggable, ngrok is merely first. A gateway with `expose = "none"` is outbound-only: it dials others, receives replication over links it initiated, and its row carries `endpoint_url NULL` (fine in a fleet where at least one gateway is reachable; the design does not require every machine exposed).

## §7 Security posture

- wss only (no ws); token handshake both directions; a failed handshake is one `gateway.jsonl` line + connection close, never a retry storm (backoff applies).
- **Ingress re-scan at the receiving gateway**: every inbound note/feed/presence body passes `redact::scan_text` + the Layer-1 caps BEFORE touching local files. A remote fleet machine that is compromised cannot inject secrets or oversized payloads; refusals are logged (`kind: "ingress_refused"`) and acked as rejected so the sender archives rather than retries.
- Private keys and the registry connection string: 0600 files, read by CLI/gateway only, never in agent context, never in logs. `a2a mesh fleet init`/`enroll` print FINGERPRINTS only.
- Frame and connection caps: max frame 2 MiB (fits a 1 MiB note with envelope), max 64 links, per-link inbound rate cap (1000 frames/min) — backstops, forgiving by the Layer-1 rule.
- Replay: tokens carry `exp`; wss is TLS so frames aren't replayable off the wire; dedup by `(origin, origin_seq)` makes redelivery idempotent.

## §8 Config surface (`[mesh.gateway]`, appended per house discipline)

`listen_port` (default 7787), `expose` ("none"), `endpoint_url` (manual), `registry_heartbeat_secs` (180), `replication_poll_secs` (5), `outbound_retry_max_secs` (1920). All in `MESH_GATEWAY_FIELDS`, `config show --origin`, env `A2A_MESH_GATEWAY_*`.

## §9 CLI surface (appended to `MeshCmd`)

- `a2a mesh fleet init [--registry -]` — mint fleet root, write registry env (stdin), print fingerprint.
- `a2a mesh fleet ls [--json]` — registry rows + computed staleness (exit 7 unreachable).
- `a2a mesh gateway enroll [--renew]` — gateway keypair + root-signed token (requires root.key present ⇒ enrollment happens on the machine holding the root, or the token file is carried over by the human; both documented).
- `a2a mesh gateway run|status|stop` — the daemon.
- Existing commands unchanged; `roster`/`feed` learn origin/`via` rendering only.

## §10 Failure modes (pinned behaviors)

- Registry unreachable: gateway keeps existing links, retries discovery with backoff, health file says `registry: unreachable since <ts>`; `fleet ls` exits 7.
- Peer gateway unreachable: outbound queue holds (capped backoff), presence for its peers ages into `stale` naturally.
- Token expired: handshake refused with `err{TOKEN_EXPIRED}`; health file surfaces own-token expiry 7 days ahead; `enroll --renew` fixes.
- ngrok URL rotates: next exposure poll re-upserts; peers redial from the next discovery.
- Duplicate delivery after crash: `(origin, origin_seq)` dedup makes it a no-op (at-least-once, idempotent — the Layer-1 inject convention).

## §11 Testing

- Hermetic throughout: `Registry` trait faked on files; two (and three) gateway processes on localhost ports in one temp-home-per-gateway harness; no real ngrok/Neon/network beyond loopback in the suite.
- e2e (`tests/mesh_gateway_e2e.rs`): fleet init → enroll ×2 → both gateways up on loopback → feed entry replicates A→B with origin attribution and dedup on reconnect; directed note A→remote peer on B lands in B's peer inbox via the full ingress path and `inject` shows it; presence gossips with `via` rendering; offline B queues then delivers on restart; ingress refusal (secret note) rejected+acked+logged; token-expiry handshake refusal.
- Gated live tier (`A2A_MESH_L2_LIVE=1`): against the real Neon registry — upsert/heartbeat/discover/retire for THIS machine, read-only beyond own row, no live second machine required.
- Codex-dispatched devloop per the established split-verification pattern.

## §12 Deferred (recorded)

Stranger-facing registration + trust tiers (the public door: registry-issued tokens for other owners); e2e payload encryption (fleet wss+tokens suffice inside one owner's fleet); multi-fleet federation; notify-based file watching; gateway autostart/LaunchAgent packaging; mid-turn push (turn-based reality, unchanged).
