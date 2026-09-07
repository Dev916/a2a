# Joining an a2a mesh fleet

How to enroll a second (third, …) machine into an existing fleet. Two roles:

- **Fleet owner** — holds `~/.a2a/mesh/fleet/root.key` (created by `a2a mesh fleet init`).
  The root key NEVER leaves this machine.
- **Joiner** — the machine being enrolled. It ends up with its own gateway identity,
  a root-signed token, and two fleet files.

Posting from a plain terminal works without passing a session id: `a2a mesh post "hello"`
mints one stable human identity at `~/.a2a/mesh/user-identity.json` and reuses it
for later terminal posts. Agent sessions still carry their own ids through
`--session`, `CLAUDE_SESSION_ID`, or `BEADS_ACTOR`, and those always take precedence.

## What the joiner needs from the owner

| File / value | What it is | Sensitivity |
|---|---|---|
| enrollment token | root-signed claims for YOUR pubkey (90-day expiry) | low — useless for any other keypair |
| `root.pub` | the fleet root public key | public |
| `registry.env` | one line: the fleet registry connection string | **secret** — it is a database credential; send over a secure channel |

## Steps

### 1. Joiner: install a2a and mint an identity

Build from the repo (`cargo build --release`, copy `target/release/a2a` onto your
`PATH`) or use a binary the owner sends you. Then:

```sh
a2a mesh gateway identity --json
```

Send the `gateway_id` and `pubkey_b64` values to the fleet owner. (This also mints
your TLS material; nothing secret is printed.)

For later upgrades on an enrolled machine, run:

```sh
a2a update
```

That fetches the latest release, installs it atomically, and restarts the gateway
service when one is installed.

For the 0.3.0 work-handoff rollout, the fleet owner must run
`a2a mesh fleet migrate` on the root machine after every member upgrades. Joiners
do not run fleet migrations; they only upgrade and restart their gateway service.

### 2. Owner: mint the token

```sh
a2a mesh fleet enroll --gateway-id <their-gateway-id> --pubkey <their-pubkey-b64> --out token.txt
```

Send back: `token.txt`, `~/.a2a/mesh/fleet/root.pub`, and `~/.a2a/mesh/fleet/registry.env`
(the last one over a secure channel).

### 3. Joiner: install the fleet files and the token

```sh
mkdir -p ~/.a2a/mesh/fleet
cp root.pub registry.env ~/.a2a/mesh/fleet/
a2a mesh gateway install-token --file token.txt
```

`install-token` verifies the token against `root.pub` and your local identity before
writing anything — a token minted for a different key is rejected.

### 4. Joiner: reachability (ngrok recommended)

Gateways dial each other directly; at least one side of every link needs a reachable
endpoint. The reliable setup is an ngrok agent on every machine:

```sh
ngrok config add-authtoken <your-token>   # once
ngrok start --none                        # leave running (launchd/systemd is fine)
```

Then in `~/.a2a/config.toml`:

```toml
[mesh.gateway]
expose = "ngrok"
```

a2a asks the local ngrok agent for a TCP tunnel itself — you never configure the
tunnel manually. Alternatives: `expose = "manual"` + `endpoint_url = "wss://host:7787"`
if the machine is directly reachable (LAN, Tailscale), or `expose = "none"` if you
are content to rely on the other side's endpoint (links form only when the dial
direction works out).

### 5. Joiner: run the gateway

```sh
a2a mesh gateway run --daemon
```

To survive reboot, install the per-user service instead:

```sh
a2a mesh gateway install-service
```

Verify from either machine:

```sh
a2a mesh fleet ls       # both gateways listed, staleness "active"
a2a mesh feed           # the other machine's feed entries appear within seconds
a2a mesh gateway status # loops all "running", endpoint_url populated
```

## Ingest limits

The connection/session bounds ship enabled at permissive defaults. The six
metered traffic buckets ship at `0`, which means OFF rather than deny; enable
them only after every member reports a version that understands deferred
frames. The calibrated deployment values are filled during the fleet rollout.

| `[mesh.limits]` key | Shipped default | Calibrated value | Basis |
|---|---:|---|---|
| `notes_per_min` | 0 (off) | TBD — set from measured fleet traffic before §7 step 5 | Measure only after L1's spool backoff has landed. Before that fix, every note was resent every `replication_poll_secs` (5 seconds), so arrival counts were meaningless. |
| `feed_entries_per_min` | 0 (off) | TBD — set from measured fleet traffic before §7 step 5 | Set above `replicate::DEFAULT_WANT_ENTRY_CAP` (128 entries, one want-reply catch-up frame), or explicitly exempt catch-up traffic. |
| `feed_bytes_per_min` | 0 (off) | TBD — set from measured fleet traffic before §7 step 5 | Measure complete catch-up frames and ordinary deltas; the one-minute bucket must absorb a reconnect burst. |
| `work_offers_per_hour` | 0 (off) | TBD — set from measured fleet traffic before §7 step 5 | Use the full hourly allowance as bucket depth, so a nonzero hourly rate can always admit at least one offer. |
| `wants_per_min` | 0 (off) | TBD — set from measured fleet traffic before §7 step 5 | Measure per-peer reconnect and anti-entropy request bursts after unknown origins are filtered. |
| `digests_per_min` | 0 (off) | TBD — set from measured fleet traffic before §7 step 5 | Measure per-peer reconnect and scheduled anti-entropy bursts over a full minute. |
| `max_unauthenticated_sessions` | 32 | TBD — set from measured fleet traffic before §7 step 5 | Ships enabled and deliberately permissive so an honest reconnect storm cannot self-DoS; tighten from observed concurrent handshakes. |
| `handshake_deadline_secs` | 10 | TBD — set from measured fleet traffic before §7 step 5 | Ships enabled and deliberately permissive so slow honest reconnects survive; tighten from observed handshake latency. |
| `connects_per_min_per_source` | 60 | TBD — set from measured fleet traffic before §7 step 5 | Ships enabled and deliberately permissive so an honest reconnect storm behind one source cannot self-DoS. |
| `handshakes_per_min_per_peer` | 12 | TBD — set from measured fleet traffic before §7 step 5 | Ships enabled with a one-minute authenticated-peer burst allowance; calibrate from reconnect frequency. |

Use `a2a --json mesh gateway status | jq '.data.health.limits'` as the
measurement surface for the calibration pass.

## Housekeeping

- **Revoke a machine** (owner): `a2a mesh fleet revoke <gateway-id>` — peers drop it
  on their next registry refresh.
- **Token expiry**: tokens live 90 days; re-run steps 1–3 to renew (identity is
  reused, so only `enroll` + `install-token` are needed).
- **Root rotation** (owner): `a2a mesh fleet rotate-root --stage --out announcement.txt`
  emits a current-root-signed announcement. Distribute that announcement out of
  band; each joiner runs `a2a mesh gateway install-root-next --file announcement.txt`
  and restarts. Never overwrite `root.pub`, and never install a bare `root.pub.next`.

## Reader errors

| Code | Meaning | Remedy |
|---|---|---|
| `SESSION_LIMIT` | A linked peer opened a surplus directional session or exceeded its authenticated handshake rate. | Keep the incumbent link and retry after the one-minute handshake window. |
| `PREAUTH_LIMIT` | The source exceeded a pre-auth bound or missed the handshake deadline. | Verify TLS/authentication reachability and retry after the source window clears. |
| `WORK_POOL_FULL` | This gateway already holds 512 pool offers. | Approve, decline, or expire an offer before accepting another. |
| `FEED_ORIGIN_UNBOUND` | A feed frame claimed an origin other than its authenticated link peer. | The peer sent entries for another member's origin; v1 pins feed origin to the authenticated link peer — relay is deferred until entries are signed end-to-end. |
| `ROOTS_UNREADABLE` | This reader could not load its local `mesh/fleet/root.pub` or `root.pub.next`. | Repair the local root files and permissions before judging any peer row. |
| `ENROLLMENT_REBIND_REQUIRED` | The requested gateway id is already bound to a different key in the owner ledger or live row. | Verify both printed fingerprints out of band, then rerun `fleet enroll` with `--rebind`. |
| `REVOCATION_INVALID` | A revocation record or local cached tombstone is malformed or does not verify under a fleet root. | Preserve the suspect row for evidence, then mint a fresh revocation on the fleet root machine. |
| `ROOT_ROTATION_STAGED` | A staged next-root pair already exists. | Inspect the staged fingerprint, then use `rotate-root --abort` or deliberately restage with `--stage --force`. |
| `ROOT_ROTATION_ABORT_REFUSED` | At least one enrollment was issued under the staged root. | Re-enroll the named gateways under the current root, or use `--abort --force` only after confirming they may be stranded. |
| `ROOT_NEXT_MISSING` | The requested operation needs a staged next-root pair, but it is absent. | Run `a2a mesh fleet rotate-root --stage` on the root machine. |
| `ROOT_PREV_MISSING` | Retirement was requested before a previous-root pair existed. | Promote a staged root before retiring the prior root. |
| `ROOT_NEXT_FLEET_MISMATCH` | A held next or previous root names a different fleet than `root.pub`. | Remove the mismatched file and install only roots from this fleet's signed rotation flow. |
| `ROOT_ANNOUNCEMENT_INVALID` | The rotation announcement is malformed, non-canonical, or not current-root-signed. | Obtain the signed announcement from the root operator; never substitute a bare `root.pub.next`. |
| `ROOT_ANNOUNCEMENT_FLEET_MISMATCH` | A validly signed announcement names a different fleet. | Obtain the announcement for the fleet named by this gateway's `root.pub`. |
| `ROTATION_PHASE_UNMET` | Verified member rows do not yet prove every ledgered gateway is on the target root. | Re-enroll, install, restart, and re-sign the named members before retrying promotion or retirement. |
| `ROTATION_SOAK_UNMET` | Post-promotion handshakes or the configured soak interval are incomplete. | Wait for the missing gateways and interval; `--retire --force` skips only this soak check. |
| `ROTATION_STATUS_UNAVAILABLE` | The owner enrollment ledger or fleet registry credential is absent. | Restore `enrollments.json` and `registry.env` in the fleet owner home before inspecting rotation state. |
| `REGISTRY_SET_REFUSED` | The proposed registry credential exposes a fleet id different from the locally installed root. | Use a credential scoped to the existing fleet; `set-registry` deliberately never re-homes a machine. |
