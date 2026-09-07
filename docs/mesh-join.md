# Joining an a2a mesh fleet

This is the authoritative rollout and enrollment guide for an a2a mesh fleet.
It covers adding a second (third, …) machine and moving an existing fleet onto
the L4 hardening posture. There are two roles:

- **Fleet owner** — holds `~/.a2a/mesh/fleet/root.key` (created by
  `a2a mesh fleet init`). The root key NEVER leaves this machine.
- **Joiner** — the machine being enrolled. It ends up with its own gateway
  identity, a root-signed token, and two fleet files.

Posting from a plain terminal works without passing a session id:
`a2a mesh post "hello"` mints one stable human identity at
`~/.a2a/mesh/user-identity.json` and reuses it for later terminal posts. Agent
sessions still carry their own ids through `--session`, `CLAUDE_SESSION_ID`, or
`BEADS_ACTOR`, and those always take precedence.

## What the joiner needs from the owner

| File / value | What it is | Sensitivity |
|---|---|---|
| enrollment token | root-signed claims for YOUR pubkey (90-day expiry) | low — useless for any other keypair |
| `root.pub` | the fleet root public key | public |
| `registry.env` | one line: the fleet registry connection string | **secret** — it is a database credential; send over a secure channel |

## Steps

### 1. Joiner: install a2a and mint an identity

Build from the repo (`cargo build --release`, copy `target/release/a2a` onto
your `PATH`) or use a binary the owner sends you. Then:

```sh
a2a mesh gateway identity --json
```

Send the `gateway_id` and `pubkey_b64` values to the fleet owner. (This also
mints your TLS material; nothing secret is printed.)

For later upgrades on an enrolled machine, use `a2a update`, but follow the
[fleet rollout ordering](#rollout-ordering): the owner migrates before any
member updates.

### 2. Owner: mint the token

```sh
a2a mesh fleet enroll --gateway-id <their-gateway-id> --pubkey <their-pubkey-b64> --out token.txt
```

Send back: `token.txt`, `~/.a2a/mesh/fleet/root.pub`, and
`~/.a2a/mesh/fleet/registry.env` (the last one over a secure channel).

### 3. Joiner: install the fleet files and the token

```sh
mkdir -p ~/.a2a/mesh/fleet
cp root.pub registry.env ~/.a2a/mesh/fleet/
a2a mesh gateway install-token --file token.txt
```

`install-token` verifies the token against `root.pub` and your local identity
before writing anything — a token minted for a different key is rejected.

### 4. Joiner: reachability (ngrok recommended)

Gateways dial each other directly; at least one side of every link needs a
reachable endpoint. The reliable setup is an ngrok agent on every machine:

```sh
ngrok config add-authtoken <your-token>   # once
ngrok start --none                        # leave running (launchd/systemd is fine)
```

Then in `~/.a2a/config.toml`:

```toml
[mesh.gateway]
expose = "ngrok"
```

a2a asks the local ngrok agent for a TCP tunnel itself — you never configure
the tunnel manually. Alternatives: `expose = "manual"` plus
`endpoint_url = "wss://host:7787"` if the machine is directly reachable (LAN,
Tailscale), or `expose = "none"` if you are content to rely on the other
side's endpoint (links form only when the dial direction works out).

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

## Version floor

Every fleet member must run and report a version greater than or equal to
**0.3.0**. The work-handoff compatibility floor is the literal `0.3.0`; it is
not derived from the sender's current build version. A gateway's registry
version refreshes only when that gateway restarts.

During the mixed-version rollout window, discovery, `fleet ls`, notes, feed
ingest, pool offers, and directed work send/result return continue to work with
a 0.3.0-floor peer. The permitted degradations are metered limits remaining
off, unsigned rows being warn-only when `require_signed_rows=false`, and the
envelope-corroboration pubkey-fingerprint check being skipped with a warning
for a usable unsigned row.

## Rollout ordering

Use this order for every existing fleet; do not update members first.

1. On the **root machine**, install the new binary so it carries the embedded
   migrations, then run `a2a mesh fleet migrate` **BEFORE any member runs
   `a2a update`**. The reverse order makes upgraded readers report
   `REGISTRY_SCHEMA_STALE`; its remedy is to run `a2a mesh fleet migrate` on
   the root machine.
2. Run `a2a update` fleet-wide and restart every gateway. Keep every member at
   or above the literal 0.3.0 floor.
3. Confirm restarted daemons publish signed rows with `a2a mesh fleet ls`.
   Renew any enrollment token within 14 days of expiry before continuing.
4. Add the third member by following [Steps](#steps), then prove feed and pool
   reachability among all three members.
5. Enable the six metered buckets only after every member understands deferred
   frames, using the values calibrated through the [limits table](#limits).
6. Set `require_signed_rows=true` while keeping `auto_push=false`, restart each
   gateway, and verify both effective start-time snapshots as described under
   [Posture flags](#posture-flags). Before a deliberate downgrade, run
   `a2a mesh fleet clear-signature <gateway-id>` on each reader.
7. Run the [root-rotation drill](mesh-rotation.md), followed by its registry
   credential-rotation drill.

## Posture flags

The two rollout posture controls live in different config sections:

```toml
[mesh.gateway]
require_signed_rows = true

[mesh.work]
auto_push = false
```

The compiled default for `require_signed_rows` is `false`, which is the
upgrade-transition value; `a2a mesh fleet init` writes `true` for a new fleet.
The default for `auto_push` is `false`, so completed work stops for review at
`awaiting_release`.

Both values are snapshots taken when the gateway starts. Editing
`config.toml` does not change a running daemon: restart it, then verify the
effective values from the **RUNNING daemon** with
`a2a mesh gateway status --json`. Do not infer the live posture from the file
alone.

## Root rotation

On the root machine, staging emits a current-root-signed announcement:

```sh
a2a mesh fleet rotate-root --stage --out announcement.txt
```

Distribute `announcement.txt` out of band. Each member installs the
announcement with:

```sh
a2a mesh gateway install-root-next --file announcement.txt
```

`install-root-next` verifies the announcement against the root already held by
the member. It is the only supported next-root installation path: **never overwrite `root.pub`, and never install a bare `root.pub.next`**.
Restart the gateway after installation. Follow
[docs/mesh-rotation.md](mesh-rotation.md) for phase-1/phase-2 gates, promotion,
soak, retirement, and stranded-member recovery.

## Row verdicts

This is the single authoritative registry-row verdict table. The
`first-signature bit` is reader-local history, never a capability inferred
from the row's unverified `version`. `flag=false` and `flag=true` refer to the
reader's own `[mesh.gateway] require_signed_rows` value. Rows remain visible in
`fleet ls`; “refused” means they cannot be used for dialing or work preflight.

| verdict | shape | first-signature bit | flag=false | flag=true | remedy |
|---|---|---|---|---|---|
| `valid-signed` | All four signature columns are present; token, signature, canonical bindings, epoch, and replay checks pass. | unset → set, or already set | usable | usable | No action is required. |
| `UNSIGNED` | `row_msg`, `row_token`, `row_sig`, and `signed_at` are all NULL. | unset | usable + warning | refused | Enable signed-row publishing on the peer, then observe a valid signature before enabling strict enforcement. |
| `UNSIGNED (stripped)` | All four signature columns are NULL after this reader previously accepted a signature. | set | refused | refused | Capture the row for evidence and restore the signed publisher; only for a deliberate downgrade, run `a2a mesh fleet clear-signature <gw>` locally before accepting unsigned rows. |
| `ROW_SIG_INCOMPLETE` | Any partial-NULL combination of the four signature columns. | either | refused | refused | Capture the row for evidence, then have the peer republish all four signature columns atomically. |
| `BAD_SIG` | The signed shape is malformed, its token/bindings/epoch/pin disagree, or signature content disagrees after first trust. | either for malformed rows; set for signature disagreement | refused | refused | Capture the row for evidence first; only after proving a deliberate downgrade may this reader run `a2a mesh fleet clear-signature <gw>`. |
| `UNSIGNED-STALE` | All four columns are present, but signature or column binding disagrees on a cold reader. | unset | usable + warning | refused | Capture the row, then have the peer publish a fresh, fully signed row. |
| `UNVERIFIED` | The signed shape is coherent, but its token verifies under no locally held root; an existing pinned-key check still passes. | either | usable + warning | refused | Verify the issuing root fingerprint out of band, then install that fleet root and restart the reader. |
| `ROW_REPLAYED` | `signed_at` is below the accepted high-water, or equals it with a different row-message digest. | set | refused | refused | Capture the row for evidence; use `a2a mesh fleet forget-signed <gw>` only for an intentional local replay-mark reset. |
| `ROW_CLOCK_AHEAD` | `signed_at` is more than the allowed clock skew ahead of this reader. | either | refused | refused | Check the reader and peer clocks before accepting any newer row. |
| `ROW_KEY_SUPERSEDED` | The token's verified key epoch is below this reader's highest accepted epoch. | set | refused | refused | Install the peer's current enrollment token and make it publish with the latest key epoch. |
| `ROW_SIG_UNKNOWN_VERSION` | `row_msg` uses a message version this reader does not understand. | either | refused | refused | Upgrade this reader before trusting or using the row. |
| `REVOKED` | A correctly bound, root-signed revocation covers this token epoch. | either | refused | refused | Keep the row as evidence; only the root operator may intentionally re-enroll the member at a higher key epoch. |
| `ROW_REVOCATION_MISPLACED` | A valid root-signed revocation is carried by a different gateway's row. | either | refused | refused | Capture the carrying row and revocation for evidence; do not move or copy the record. |
| `STALE_ROW` | An otherwise valid signed row is older than twice its declared-and-clamped re-sign cadence. | set | usable + warning | usable + warning | Check the peer gateway, then run `a2a mesh gateway resign` on that machine. |
| `ROW_ENDPOINT_CONFLICT` | This later row claims an endpoint already held by the incumbent row. | either | visible, but refused for dial/preflight | visible, but refused for dial/preflight | Keep the incumbent endpoint and investigate the later row before dialing it. |
| `TOKEN_EXPIRED` | The row signature and token bindings verify, but the enrollment token is past `exp`. | either | usable + warning | usable + warning | Renew the token on the fleet root, install it on the gateway, and restart. |
| `TOKEN_NOT_YET_VALID` | The row signature and token bindings verify, but the token is before `nbf`. | either | usable + warning | usable + warning | Check local and fleet-root clocks before installing or renewing the token. |
| `row_key_rebound` | A root-attested, strictly higher token epoch intentionally replaces the pinned gateway pubkey. | set | usable + warning | usable + warning | Confirm out of band that the higher epoch matches an owner-approved `fleet enroll --rebind`. |

Verdict precedence is:

```text
REVOKED > ROW_REVOCATION_MISPLACED > ROW_SIG_UNKNOWN_VERSION
  > ROW_SIG_INCOMPLETE > malformation-BAD_SIG > ROW_KEY_SUPERSEDED
  > ROW_REPLAYED / ROW_CLOCK_AHEAD > disagreement
  > UNVERIFIED > UNSIGNED > valid-signed
```

## `gateway_id` derivation

For a key-derived gateway id, base64-decode the enrollment token's
`pubkey_b64`, compute `Sha256` over those public-key bytes, encode the digest as
lowercase hex, and use its first **N characters, where N is at least 16**, as
the suffix. Existing enrollments keep the suffix width with which they were
shipped; do not silently widen and rename them during an upgrade.

## Limits

The connection/session bounds ship enabled at permissive defaults. The six
metered traffic buckets ship at `0`, which means OFF rather than deny; enable
them only at [rollout step 5](#rollout-ordering), after every member reports a
version that understands deferred frames. This is the one calibration table;
other rollout and operations docs should link here rather than duplicate it.

| `[mesh.limits]` key | Shipped default | Calibrated value | Basis |
|---|---:|---|---|
| `notes_per_min` | 0 (off) until §7 step 5 | TBD — set from measured fleet traffic before §7 step 5 | Measure only after the spool-backoff fix; pre-fix five-second retransmits make arrival counts meaningless. |
| `feed_entries_per_min` | 0 (off) until §7 step 5 | TBD — set from measured fleet traffic before §7 step 5 | Set above `replicate::DEFAULT_WANT_ENTRY_CAP` (128 entries, one want-reply catch-up frame), or explicitly exempt catch-up traffic. |
| `feed_bytes_per_min` | 0 (off) until §7 step 5 | TBD — set from measured fleet traffic before §7 step 5 | Measure complete catch-up frames and ordinary deltas; the one-minute bucket must absorb a reconnect burst. |
| `work_offers_per_hour` | 0 (off) until §7 step 5 | TBD — set from measured fleet traffic before §7 step 5 | Use the full hourly allowance as bucket depth, so a nonzero hourly rate can always admit at least one offer. |
| `wants_per_min` | 0 (off) until §7 step 5 | TBD — set from measured fleet traffic before §7 step 5 | Measure per-peer reconnect and anti-entropy request bursts after unknown origins are filtered. |
| `digests_per_min` | 0 (off) until §7 step 5 | TBD — set from measured fleet traffic before §7 step 5 | Measure per-peer reconnect and scheduled anti-entropy bursts over a full minute. |
| `max_unauthenticated_sessions` | 32 | TBD — tighten from measured fleet traffic before §7 step 5 | Ships enabled and deliberately permissive so an honest reconnect storm cannot self-DoS; tighten from observed concurrent handshakes. |
| `handshake_deadline_secs` | 10 | TBD — tighten from measured fleet traffic before §7 step 5 | Ships enabled and deliberately permissive so slow honest reconnects survive; tighten from observed handshake latency. |
| `connects_per_min_per_source` | 60 | TBD — tighten from measured fleet traffic before §7 step 5 | Ships enabled and deliberately permissive so an honest reconnect storm behind one source cannot self-DoS. |
| `handshakes_per_min_per_peer` | 12 | TBD — tighten from measured fleet traffic before §7 step 5 | Ships enabled with a one-minute authenticated-peer burst allowance; calibrate from reconnect frequency. |

Use `a2a mesh gateway status --json` and its per-peer limit counters as the
measurement surface for the calibration pass.

## Revocation and registry access

`a2a mesh fleet revoke <gateway-id>` revokes that member's fleet identity for
dialing, handshakes, and work, but it **does not remove registry access**. The
shared credential in `registry.env` is independent of enrollment revocation.
Only registry credential rotation removes that access: use
`a2a mesh fleet set-registry --url <source>` and follow the ordered
credential-rotation runbook in [docs/mesh-rotation.md](mesh-rotation.md).

Enrollment tokens live 90 days. Renew one by repeating the owner `enroll` and
joiner `install-token` steps; the joiner's identity is reused.

## Reader errors

| Code | Meaning | Remedy |
|---|---|---|
| `REGISTRY_SCHEMA_STALE` | The registry is missing columns required by this binary. | On the root machine, run `a2a mesh fleet migrate` before updating members. |
| `SESSION_LIMIT` | A linked peer opened a surplus directional session or exceeded its authenticated handshake rate. | Keep the incumbent link and retry after the one-minute handshake window. |
| `PREAUTH_LIMIT` | The source exceeded a pre-auth bound or missed the handshake deadline. | Verify TLS/authentication reachability and retry after the source window clears. |
| `WORK_POOL_FULL` | This gateway already holds 512 pool offers. | Approve, decline, or expire an offer before accepting another. |
| `FEED_ORIGIN_UNBOUND` | A feed frame claimed an origin other than its authenticated link peer. | The peer sent entries for another member's origin; v1 pins feed origin to the authenticated link peer until entries are signed end to end. |
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
