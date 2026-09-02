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

## Housekeeping

- **Revoke a machine** (owner): `a2a mesh fleet revoke <gateway-id>` — peers drop it
  on their next registry refresh.
- **Token expiry**: tokens live 90 days; re-run steps 1–3 to renew (identity is
  reused, so only `enroll` + `install-token` are needed).
- **Root rotation** (owner): `a2a mesh fleet rotate-root` stages `root.pub.next`;
  distribute the new `root.pub` to joiners before retiring the old root.
