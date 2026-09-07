# Mesh work handoff

Mesh work handoff moves a signed implementation brief through the fleet, lets a
human approve the claim on the receiver, and routes the completed result back to
the sender. Use it only after every member is on a2a 0.3.0 and the fleet
registry has the work schema.

## Rollout

Run these steps before the first `a2a mesh work send` or `offer`:

1. Upgrade a2a on every fleet member:

   ```sh
   a2a update
   ```

2. On the fleet root machine only, migrate the registry:

   ```sh
   a2a mesh fleet migrate
   ```

   The command applies every migration idempotently. Joiners do not run it.

3. Restart every gateway daemon so the registry records version 0.3.0:

   ```sh
   a2a mesh gateway stop
   ```

   launchd or systemd relaunches an installed service. You can also use the
   platform service manager directly, for example `launchctl kickstart -k`.

4. Verify the rollout:

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
a2a mesh work retry-push <work_id>
a2a mesh work ack <work_id>
```

The work loop reconciles a terminal worker, pushes the result branch, marks the
registry row `done` or `failed`, and sends a `work_result` note back to the
sender with text such as `work done: <title> (<work_id>) -> <result_ref>`.
`retry-push` is safe to re-run after a transient git failure. `ack` closes a
result after the sender has handled it.

## Errors

| Code | Meaning | Remedy |
|---|---|---|
| `WORK_PEER_ROW_INVALID` | The target gateway row's flag-resolved disposition refuses directed work. | Capture the row for evidence first with `a2a mesh fleet ls --json`; do not ask the peer to restart or republish. |
| `WORK_SENDER_ROW_INVALID` | The sender's cached row is absent, refused, revoked, or its verified key binding differs from the work envelope. | Capture the sender row with `a2a mesh fleet ls --json`; approve only after its disposition and verified key binding are usable. |
| `WORK_PEER_TOO_OLD` | Target gateway has no version or records a version below 0.3.0. | Ask the operator to upgrade and restart the gateway; the registry version refreshes on restart. |
| `WORK_SCHEMA_MISSING` | The registry has not been migrated for work rows. | Run `a2a mesh fleet migrate` on the fleet root machine. |
| `REGISTRY_SCHEMA_STALE` | The gateway registry is missing columns required by this build. | Run `a2a mesh fleet migrate` on the fleet root machine. |
| `WORK_ALREADY_CLAIMED` | Another gateway won the pool claim. | Use the named claimer, or send a directed item if one machine must run it. |
| `WORK_REPO_UNREACHABLE` | The receiver could not fetch the requested repository or base ref. | Fix repo URL, credentials or base ref, then send fresh work. |
| `WORK_DAEMON_DOWN` | Approval needs the gateway daemon for completion routing. | Run `a2a mesh gateway run --daemon` or install the service. |
| `WORK_PUSH_FAILED` | The result branch could not be pushed. | Fix git credentials or remote access, then run `a2a mesh work retry-push <id>`. |
| `WORK_EXPIRED` | The envelope expired before approval. | Ask the sender to issue a fresh work envelope. |
| `WORK_TOO_LARGE` | The envelope or result note exceeds the configured cap. | Shorten the brief/result or raise the relevant `[mesh.work]` cap. |
| `WORK_SIGNATURE_INVALID` | The signed envelope did not verify. | Discard it and ask the sender to re-send. |
| `WORK_SENDER_UNKNOWN` | The sender is not trusted by the local fleet root. | Install the sender fleet root or re-enroll the sender. |
| `WORK_TOKEN_MISMATCH` | The sender token and gateway identity do not match. | Re-enroll the gateway and retry with a fresh envelope. |
| `WORK_WRONG_FLEET` | The envelope is for a different fleet. | Only ingest work from the local fleet. |
| `WORK_BAD_REPO_URL` | The repo URL failed hygiene checks. | Use a canonical git remote URL. |
| `WORK_CONTROL_CHARS` | Work text contained terminal control characters. | Remove the control characters before sending. |
| `WORK_BRIEF_INVALID` | Brief front matter is missing or malformed. | Fix the brief YAML before sending work. |
| `WORK_STATE_CONFLICT` | The local work state does not allow the requested transition. | Re-check `a2a mesh work status <id>` and use the next valid command. |
| `WORK_NOT_FOUND` | The work id is not present locally or in the registry. | Check `a2a mesh work list` or ask the sender to re-send. |
