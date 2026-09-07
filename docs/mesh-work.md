# Mesh work handoff

Mesh work handoff moves a signed implementation brief through the fleet, lets a
human approve the claim on the receiver, and routes the completed result back to
the sender. Use it only after every member is on a2a 0.3.0 and the fleet
registry has the work schema.

## Rollout

Before the first `a2a mesh work send` or `offer`, follow the authoritative
[fleet rollout ordering](mesh-join.md#rollout-ordering). In particular, migrate
before updating any member:

1. On the fleet root machine, install the new binary, then migrate the registry:

   ```sh
   a2a mesh fleet migrate
   ```

   The command applies every migration idempotently. Joiners do not run it.
   Updating a member first can make that upgraded reader report
   `REGISTRY_SCHEMA_STALE` until the root machine migrates.

2. Upgrade a2a on every fleet member and restart every gateway daemon:

   ```sh
   a2a update
   a2a mesh gateway stop
   ```

   launchd or systemd relaunches an installed service. You can also use the
   platform service manager directly, for example `launchctl kickstart -k`.
   Upgrade every member before any receiver begins holding work. A 0.3.0-floor
   sender that reads an `awaiting_release` row reports `WORK_STATUS_INVALID`.

3. Verify the rollout:

   ```sh
   a2a mesh fleet ls
   a2a mesh gateway status
   ```

   `fleet ls` must show version 0.3.0 for every target gateway, and `gateway
   status` must show the `work` loop running.

## Directed Flow

Use directed work when one gateway should receive the brief.

```sh
a2a mesh work send --to <gateway-id> \
  --repo <git-remote-url> \
  --base-ref <branch-or-commit> \
  --title "Short title" \
  --brief brief.md \
  --acceptance "Focused acceptance criteria"
```

The sender preflights the addressee row and refuses gateways below 0.3.0. On
success it inserts the registry row, sends a durable `work_offer` note to the
gateway, and posts a feed milestone shaped like `work sent: <title> ->
<gateway> (<work_id>)`.

On the receiver:

```sh
a2a mesh work list
a2a mesh work show <work_id>
a2a mesh work approve <work_id> --dry-run
a2a mesh work approve <work_id>
```

The dry run verifies the envelope and prints the sender, repository, base ref,
title, acceptance text, brief and preflight results without claiming or
checking out the repository. Approval claims the registry row, checks out the
repo under `$A2A_HOME/work/<work_id>`, spawns the impl worker, and leaves
completion routing to the gateway work loop.

## Pool Flow

Use pool work when any upgraded fleet member may claim it.

```sh
a2a mesh work offer \
  --repo <git-remote-url> \
  --base-ref <branch-or-commit> \
  --title "Short title" \
  --brief brief.md \
  --acceptance "Focused acceptance criteria"
```

The sender publishes the signed envelope as a feed entry and inserts the
registry row. Each 0.3.0 gateway materializes the offer locally; the first
successful `approve` wins the registry CAS claim. Losing approvers get a typed
already-claimed error naming the winner.

## Results

Senders monitor and finish work with:

```sh
a2a mesh work status <work_id>
a2a mesh work diff <work_id>
a2a mesh work release <work_id>
a2a mesh work reject <work_id> --reason "local review reason"
a2a mesh work ack <work_id>
```

With the default `[mesh.work] auto_push = false`, the work loop reconciles a
terminal worker and stops at `awaiting_release` without pushing. It emits one
machine-generated `work_held` note. Review the frozen result with `diff`, then
use `release` to push the recorded SHA, mark the row `done` or `failed`, send the
`work_result` note, and post the milestone. Use `reject` to leave the row
`rejected` with no result ref. The rejection reason stays local by default;
`--notify-sender` deliberately transmits it and should never describe a secret
or where one was found. `retry-push` is only for a transient push failure
outside a hold; it refuses `awaiting_release`. `ack` closes a delivered result.

## Holds and exits

`decline` is the pre-approval verb and is valid only while a row is `offered`.
`reject --reason` is the post-review exit and is valid only from
`awaiting_release`. A decline reason is sent before work begins; a reject reason
is local unless the receiver explicitly adds `--notify-sender`.
`a2a mesh work retry-push <id>` refuses a held item; choose `release` or
`reject` after reviewing it.
Items held longer than `[mesh.work] release_stall_days` (default 7) become
`failed{release_stalled}`; `a2a mesh work reopen <id>` restores the item to
`awaiting_release` from exactly that state, using its local hold record. The
ordinary abandoned-claim sweep never changes `awaiting_release`. `rejected`
and `failed` checkouts become eligible for gc after `checkout_retain_days`; a
held checkout never does.

## Security

Review a completed checkout with `a2a mesh work diff <work_id>`. The command
pins git against checkout-controlled hooks, attributes and external diff
drivers and masks secret-shaped values. Diff bodies are attacker-authored and
always render inside the clearly marked `UNTRUSTED` fence; scanner hits and
non-ASCII/over-long markers render from a2a-owned structures outside that
body. It also displays the exact outgoing result-note payload and scans its
summary and evidence excerpt as part of the same hit-set digest. `--raw` is an
operator-only escape hatch: its output is unmasked.
**`work diff --raw` output must never be piped into agent context.**

Every mesh work spawn pins `network=false` (`network_access=false` in the codex
spawn arguments), even if the impl lane normally enables network access. The
approve dry run prints the resolved sandbox (normally `workspace-write`) and
network posture before the brief.

**Worker READ scoping is not confined in v1** (spec §3.7 finding,
codex 0.152.1): the documented `sandbox_permissions` config key is rejected
by actual config resolution (`unknown configuration field`), and the only
working read-narrowing route — beta permission profiles — would replace the
entire `-s workspace-write` spawn path rather than compose with it. A worker
can therefore READ anything the operating user can; the effective
mitigations are exactly the gates above: exfiltration must transit the
reviewed push (linear-range diff gate, blob-level secret scan, note-payload
scan, network=false), and `--allow-secrets` is out-of-band. Revisit when
permission profiles leave beta (tracked as a follow-up issue).

`release --allow-secrets` never treats text inside the reviewed diff as
authorization. It requires the one-time token printed only by an interactive
`work diff`; the token is bound to the current hit-set digest, single-use,
non-delegable between principals, and recorded in `audit.jsonl` before the
push.

Before every push, the result scanner checks added post-image blobs, added or
renamed filenames, anchored `NAME=value` assignments, and byte-identical copies
of regular files under `$A2A_HOME`. The scan stops with `WORK_SCAN_OVERSIZE`
after `[mesh.work] diff_scan_max_bytes` bytes (default 5,242,880); an oversize
scan is a hold, never a clean result.

The scanner is an accidental-disclosure backstop, not a proof that a result is
secret-free. It deliberately does not claim coverage for encoded credentials,
values split so no individual line matches a rule, or entropy-only values with
no secret-bearing key name. Review the complete fenced diff even when the hit
set is empty.

## Errors

| Code | Meaning | Remedy |
|---|---|---|
| `RATE_LIMITED` | The peer is over an ingest limit. | Nothing to do — the note stays spooled and retries with backoff; check `a2a mesh gateway status --json` for their limit counters if it persists. |
| `SESSION_LIMIT` | An authenticated peer already has a session in that direction, or exceeded its handshake rate. | Keep the incumbent link, close surplus clients, and retry after the one-minute handshake window. |
| `PREAUTH_LIMIT` | A source exceeded the pre-auth connection bounds or did not finish its handshake before the deadline. | Verify the peer can complete TLS and mesh authentication, then retry after the source window clears. |
| `WORK_POOL_FULL` | The local pool already holds 512 work offers. | Approve, decline, or expire a pool offer before accepting another. |
| `WORK_PEER_ROW_INVALID` | The target gateway row's flag-resolved disposition refuses directed work. | Capture the row for evidence first with `a2a mesh fleet ls --json`; do not ask the peer to restart or republish. |
| `WORK_SENDER_ROW_INVALID` | The sender's cached row is absent, refused, revoked, or its verified key binding differs from the work envelope. | Capture the sender row with `a2a mesh fleet ls --json`; approve only after its disposition and verified key binding are usable. |
| `WORK_PEER_TOO_OLD` | Target gateway has no version or records a version below 0.3.0. | Ask the operator to upgrade and restart the gateway; the registry version refreshes on restart. |
| `WORK_SCHEMA_MISSING` | The registry has not been migrated for work rows. | Run `a2a mesh fleet migrate` on the fleet root machine. |
| `REGISTRY_SCHEMA_STALE` | The gateway registry is missing columns required by this build. | Run `a2a mesh fleet migrate` on the fleet root machine. |
| `WORK_ALREADY_CLAIMED` | Another gateway won the pool claim. | Use the named claimer, or send a directed item if one machine must run it. |
| `WORK_REPO_UNREACHABLE` | The receiver could not fetch the requested repository or base ref. | Fix repo URL, credentials or base ref, then send fresh work. |
| `WORK_DAEMON_DOWN` | Approval needs the gateway daemon for completion routing. | Run `a2a mesh gateway run --daemon` or install the service. |
| `WORK_RECONCILE_FAILED` | The gateway could not reconcile the terminal worker or freeze its result range. | Inspect the worker with `a2a status <worker_id>` and retry the gateway tick. |
| `WORK_PUSH_FAILED` | The result branch could not be pushed. | Fix git credentials or remote access, then run `a2a mesh work retry-push <id>`. |
| `WORK_EXPIRED` | The envelope expired before approval. | Ask the sender to issue a fresh work envelope. |
| `WORK_TOO_LARGE` | The envelope or result note exceeds the configured cap. | Shorten the brief/result or raise the relevant `[mesh.work]` cap. |
| `WORK_SIGNATURE_INVALID` | The signed envelope did not verify. | Discard it and ask the sender to re-send. |
| `WORK_SENDER_UNKNOWN` | The sender is not trusted by the local fleet root. | Install the sender fleet root or re-enroll the sender. |
| `WORK_TOKEN_MISMATCH` | The sender token and gateway identity do not match. | Re-enroll the gateway and retry with a fresh envelope. |
| `WORK_WRONG_FLEET` | The envelope is for a different fleet. | Only ingest work from the local fleet. |
| `WORK_BAD_REPO_URL` | The repo URL failed hygiene checks. | Use a canonical git remote URL. |
| `WORK_BAD_BASE_REF` | The base ref failed option, control-character, length, or git ref-shape hygiene. | Use a branch name such as `main` or `release/1.2`. |
| `WORK_CONTROL_CHARS` | Work text contained terminal control characters. | Remove the control characters before sending. |
| `WORK_BRIEF_INVALID` | Brief front matter is missing or malformed. | Fix the brief YAML before sending work. |
| `WORK_STATE_CONFLICT` | The local work state does not allow the requested transition. | Re-check `a2a mesh work status <id>`: `decline` is valid only while `offered`, while `reject` is reachable only from `awaiting_release`. |
| `WORK_HELD` | A push retry was requested for an item still in `awaiting_release`. | Review with `a2a mesh work diff <id>`, then use `release` or `reject`. |
| `WORK_ID_INVALID` | A work id is not a 26-character Crockford ULID or uses a reserved work-store name. | Use the generated id from `a2a mesh work send` or `offer`. |
| `WORK_TERMINAL_OBSERVED` | This receiver previously observed cancellation or another terminal state for the id. | Do not approve it; ask the sender for a fresh work id. |
| `WORK_NOT_HELD` | `release`, `reject`, or `reopen` was requested without the required local hold record and registry state. | Check `a2a mesh work status <id>` and act only on a locally held item. |
| `WORK_NOT_FOUND` | The work id is not present locally or in the registry. | Check `a2a mesh work list` or ask the sender to re-send. |
| `WORK_RESULT_NOT_LINEAR` | The reviewed commit range contains a merge, an unrelated base, or commits outside the HEAD chain. | Rebase the result into one linear chain rooted at the approved base. |
| `WORK_BINARY_IN_RESULT` | An added or modified result blob/path is not reviewable UTF-8. | Replace binary output with reviewable UTF-8 text and rerun the worker. |
| `WORK_ATTRS_IN_RESULT` | An added or modified `.gitattributes` names diff or textconv behavior. | Remove the diff/textconv attribute change and resubmit the result. |
| `WORK_HEAD_MOVED` | Checkout HEAD changed after the result range was frozen. | Review the new range before attempting release again. |
| `WORK_SECRET_IN_RESULT` | The result scanner found a credential shape, secret-bearing path/key, or byte match to an `$A2A_HOME` file. | Inspect the checkout named in the error and review with `a2a mesh work diff <id> --raw`; `--raw` output must never be piped into agent context. |
| `WORK_SCAN_OVERSIZE` | Added result blobs exceed `[mesh.work] diff_scan_max_bytes`. | Reduce the committed result or raise the configured scan cap, then rescan before release. |
| `WORK_ALLOW_SECRETS_REQUIRED` | A secret-bearing release lacks the out-of-band one-time confirmation token. | Review `a2a mesh work diff <id>` in a terminal and pass its token with `release --allow-secrets --confirm-token <token>`. |
| `WORK_ALLOW_SECRETS_STALE` | The current scanner hit set differs from the one reviewed when the token was minted. | Rerun `a2a mesh work diff <id>`, review the current hit set, and use the new token. |
