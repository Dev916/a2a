# a2a — Claude-orchestrated Codex workers

`a2a` dispatches [Codex](https://github.com/openai/codex) workers, watches them,
steers them at turn boundaries, and reaps them — from one small CLI whose **exit
code is the answer**. It is built to be driven by another agent (Claude, a
script, a Makefile) rather than watched by a human: every command answers in one
call, so an orchestrator never burns its context window polling.

All worker state lives on disk under `$A2A_HOME` (default `~/.a2a`) as a
write-ahead log (`events.jsonl`, the source of truth) plus an atomic snapshot
(`state.json`). Nothing is lost if the CLI, your shell, or the machine goes away
mid-turn — the log is the truth and the snapshot is a rebuildable projection of
it.

There are **two transports**, and the CLI is the same on both
([Transports](#transports)): the **app-server** transport runs workers as threads
inside a small broker daemon (mid-turn `interrupt`, live `tail --follow`,
admission control), and the **exec** transport runs each worker as a detached
`codex exec` process group with no daemon at all. `--transport auto` — the
default — takes the first and falls back to the second.

This is **Phase 2** (broker + app-server transport). Worktrees, guardian-denial
parking and `a2a approve` arrive later.

---

## Quick Start

### 1. Install

```bash
cargo install --path . --root ~/.local     # → ~/.local/bin/a2a
a2a --version
```

You also need `codex` on `PATH`, already authenticated (`codex login`). `a2a`
reads the auth in `~/.codex` and never writes there.

### 2. Check the environment a worker would be spawned into

```bash
a2a doctor
```

```
✓ codex_present  codex resolves to /Users/you/.nvm/versions/node/v23.10.0/bin/codex
✓ codex_version  codex 0.140.0 satisfies >=0.140, <0.150
✓ layout_version  home layout v1
✓ home_perms  /Users/you/.a2a is 0700
✓ workers_health  0 worker(s): 0 orphaned, 0 parked (non-terminal, no live process), 0 unreadable
⚠ env_inheritance  exec-transport workers inherit the CLI environment minus the scrub list …
✓ app_server_handshake  `…/codex app-server` completed initialize as a2a/0.140.0 (…)
✓ codex_home_match  app-server and CLI agree on /Users/you/.codex
✓ broker_status  not running — start-on-demand from /Users/you/.local/bin/a2a
⚠ auth_mode  OPENAI_API_KEY is exported — a2a scrubs it from every child …
✓ context_canon  CLAUDE.md in /Users/you/dev/proj imports @AGENTS.md
✓ codex_config_secrets  2 secret-shaped line(s) in /Users/you/.codex/config.toml, which git does not track
✓ sync_state  12 manifest entry(s), every destination as a2a left it; last synced 2026-08-18T10:04:11+00:00
13 check(s): 11 ok, 2 warn, 0 fail
```

One line per check; **exit 9 if any check fails** (warnings do not fail). Run it
first — it creates the home tree (0700), performs a real `codex app-server`
handshake, and catches the version-coupled transport mismatch before a worker
does. `app_server_handshake` failing is a warning, not an error: it means every
spawn will quietly take the exec fallback.

On a machine whose `~/.codex/config.toml` is **tracked by git** and carries a
credential, `codex_config_secrets` fails and `a2a doctor` **exits 9** until
`a2a sync mcp --migrate-untrack` has been run — it names the file and the line
numbers, never the values.

### 3. Spawn a worker

A worker is a **brief** (Markdown, optional YAML front matter) plus a **lane**
(which fixes the sandbox, model and reasoning effort) and a **cwd**:

```bash
cat > /tmp/brief.md <<'EOF'
---
task: Fix the flaky test in foo_test.rs
acceptance:
  - cargo test passes 10 runs in a row
EOF

a2a spawn --lane general --cwd ~/dev/myproject --brief /tmp/brief.md
```

```
a2a: spawned general-0817t0237-7x6h (general lane) in /Users/you/dev/myproject
general-0817t0237-7x6h
```

**stdout is the worker id** (stderr is decoration) — so `ID=$(a2a spawn …)` just
works. Add `--json` to any command for a single machine-readable envelope
instead.

| lane | sandbox | network | for |
|---|---|---|---|
| `impl` | workspace-write | off | writing code in a repo |
| `review` | outbox-only | off | reading a repo, answering in `outbox/` |
| `research` | outbox-only | **on** | web research (no `--cwd`; own scratch dir) |
| `general` | workspace-write | off | everything else |

`--cwd` must live under an allowed root (default `~/dev`; see
[Configuration](#configuration)). Useful extras: `--context <file>` stages a
read-only copy into the worker's `context/`, and `--schema <file.json>` makes the
worker's result gate on a JSON schema.

### 4. Wait for it

```bash
a2a wait "$ID" --timeout 10m ; echo "exit=$?"
```

`wait` blocks and returns **the moment the worker changes state** — one call per
state change, no polling loop in your orchestrator. The **exit code encodes the
state reached**:

| exit | meaning |
|---|---|
| 0 | done (or cancelled) |
| 4 | failed |
| 5 | blocked — it asked a question |
| 6 | timeout expired; still running |
| 10 | orphaned (its process vanished) |

Variants worth knowing:

```bash
a2a wait "$ID" --until blocked      # return only when it needs you
a2a wait "$A" "$B" "$C"             # first one to change wins; its id is printed
```

### 5. Read the result

```bash
a2a result "$ID"
```

```
a2a: general-0817t0237-7x6h done — ~/.a2a/workers/general-0817t0237-7x6h/result.md (markdown)
fixed it
```

The result is printed on stdout (redacted); the exit code again encodes state, so
`result` on a still-running worker exits 6 rather than lying with an empty
answer. For the play-by-play instead of the conclusion:

```bash
a2a ls                       # every worker: id, lane, state, dwell, last-event age
a2a status "$ID"             # reconcile one worker; exit code IS the state
a2a tail "$ID"               # lifecycle events; --raw for full envelopes, --since N to resume
```

### 6. Answer a blocked worker

A worker that cannot proceed writes a question into its `outbox/` and parks in
`awaiting-input`. That is what `wait --until blocked` (exit 5) reports. Answer it
with `send`:

```bash
a2a wait "$ID" --until blocked ; [ $? -eq 5 ] && cat ~/.a2a/workers/$ID/outbox/*.question.json
a2a send "$ID" "use option B — keep the existing public API"
a2a wait "$ID" --timeout 10m ; echo "exit=$?"
```

`send` delivers at the next turn boundary. On the **exec** transport that means
resuming the worker's codex session in a new epoch; on the **app-server**
transport the broker delivers it over the live connection and `--json` adds a
`send_path` saying how — `steered` (the open turn took it mid-flight),
`started` (there was no open turn), or `retried_as_start` (the steer lost the
turn-boundary race and became a new turn). Either way the text travels through a
0600 prompt file on the child's stdin — **never through argv**. `--queue-only`
batches several messages into one delivery; `--kind steer` marks guidance rather
than an answer.

### 7. Clean up

```bash
a2a kill "$ID"               # witnessed group-kill → cancelled (never "failed"); idempotent
a2a resume "$ID" "also update the changelog"   # continue a parked or finished worker
a2a gc --age 7d --dry-run    # list what is reclaimable, delete nothing
a2a gc --age 7d              # reclaim terminal workers, leaving a tombstone per removal
```

`gc` removes **terminal workers only** — there is no flag that overrides that —
and every removal leaves a tombstone in `index.jsonl` so an id is never reused.

---

## Transports

Every command above works the same on both transports. What differs is who runs
the worker.

| | `app-server` | `exec` |
|---|---|---|
| worker is | a thread in a `codex app-server` the broker owns | a detached `codex exec --json` process group |
| needs a daemon | yes (autostarted) | no |
| mid-turn `send` | steers the open turn | queued, delivered at the next turn boundary |
| `a2a interrupt` | yes | no (exit 9) |
| `a2a tail --follow` | live frames from the broker | 500ms poll of the log |
| admission control | yes (caps + queue) | no |
| survives the daemon dying | adopted by the next one | never had one |

```bash
a2a spawn --lane impl --cwd ~/dev/p --brief b.md                        # auto (default)
a2a spawn --transport app-server --lane impl --cwd ~/dev/p --brief b.md # demand the broker
a2a spawn --transport exec       --lane impl --cwd ~/dev/p --brief b.md # demand no daemon
A2A_BROKER_DISABLE=1 a2a spawn --lane impl --cwd ~/dev/p --brief b.md   # no socket, at all
```

* **`auto`** takes the app-server transport when a broker is reachable (starting
  one if needed) and falls back to exec otherwise, recording a
  `transport_degraded` event so a downgrade is never silent.
* **`app-server`** is an ask, not a preference: if the broker cannot be reached
  it **exits 7** rather than quietly running somewhere else.
* **`A2A_BROKER_DISABLE=1`** is the hard switch back to Phase 1 — with it set no
  `a2a` command opens the socket at all, not even to probe. It is the answer to
  "the broker is misbehaving, get work moving now".

There is exactly one place a worker changes transport, and it only goes one way:
`a2a resume` always forks a `codex exec resume <session>` child, so an
app-server worker can be continued **after its broker is gone** — same
conversation, new epoch, no daemon. That works because the app-server thread id
*is* the codex session id, which is proved against the real binary by the gated
smoke ([Development](#development)).

## The broker

The broker is a daemon-lite: one process per `$A2A_HOME`, owning one private
`codex app-server` child that every worker on that home multiplexes over.

```bash
a2a broker status            # uptime, pids, worker/queued/client counts (exit 7 if none)
a2a broker run               # take broker.lock, detach, serve (--foreground to stay attached)
a2a broker shutdown          # graceful and bounded; exit 0 when there is none
a2a broker restart           # stop, then start a fresh one
```

You rarely run any of them: the first `spawn` that wants the live transport
**autostarts** one, and a second `broker run` on the same home says
`broker already running` and exits 0. In `$A2A_HOME` it keeps `broker.sock`
(0600, peer-uid checked), `broker.lock` (flock held for the daemon's lifetime),
`broker.pid`, `broker.log`, `broker.jsonl` and `broker-health.json`.

Nothing is lost when it dies. A `kill -9`d broker leaves its workers behind;
the next `a2a status`/`ls` commits them `orphaned` (exit 10), and the next
daemon **adopts** them — reaping the previous instance's child, re-opening each
thread (`thread/resume`) and continuing on a fresh epoch. `a2a broker status
--json` reports what the last reconcile adopted and reaped.

### Interrupt: ending a turn that is already running

```bash
a2a interrupt "$ID"          # → awaiting-input{interrupted}
```

The one command with no exec spelling. The broker knows the open turn id and
sends `turn/interrupt`; the worker parks in `awaiting-input{interrupted}`, which
`a2a wait --until blocked` reports as exit 5, and a `send` (or `resume`) starts
it again from there. Two behaviours worth knowing:

* an exec worker is refused **statically** (exit 9) — `a2a kill` stops it, `a2a
  send` reaches it at the next boundary;
* interrupting a turn that has already ended is **exit 0 with `data.noop:true`**,
  not an error: racing a turn's last millisecond is not a mistake.

A missing broker is exit 7, never a quiet success.

### Following a worker live

```bash
a2a tail "$ID" --follow                 # ends by itself when the worker is terminal (exit 0)
a2a tail "$ID" --follow --timeout 5m    # …or when the window expires (exit 6)
a2a tail "$ID" --since 42 --raw         # resume from a cursor; full envelopes as JSONL
```

On the app-server transport `--follow` subscribes to live frames from the broker;
on exec it polls the log every 500ms. It is always bounded — a terminal state, an
expired `--timeout`, or the broker shutting down all end it.

## Admission control

The broker caps how many workers run at once; the rest **queue** rather than
fail, and drain in FIFO order as slots free.

```toml
# $A2A_HOME/config.toml
[admission]
global_cap = 8       # live workers across all lanes
queue_cap  = 32      # waiting workers before a spawn is refused

[admission.lanes.impl]
cap = 4              # per-lane, so one lane cannot starve the others
```

A queued worker is a real worker with an id, an event log and a
`queue_position`; `a2a ls` shows it in state `queued`, and `a2a broker status`
counts the fleet's `live` and `queued` next to each other, because "8 live, 3
queued" is one fact about a fleet and is exactly what an operator asking "why
has my spawn not started" wants. `spawn --no-queue` refuses instead of queueing
(exit 9), and every cap is a `u64` where **`0` means "admit nothing"** — the safe
reading of a mis-set value. Uncapping is spelled with a large number, which is
something you can only do on purpose.

## Fleet survival

Caps stop a fleet running too *wide*. These stop it running too *long*, too
often, or unreviewed — the four things that let a fleet burn quota on its own
mistakes, plus the two file-layer leftovers.

### Turn budgets — `failed{budget_exhausted}`

Every lane has a **turn budget**: how many turns one worker may take in one
epoch before the broker stops it.

```toml
[lanes.impl]
max_turns = 40       # impl 40, general 30, research 20, review 15
```

A brief may **only lower** it (`max_turns:` in the front matter); asking for
more than the lane allows is clamped, loudly, at spawn. Spending it lands the
worker in `failed` with `failure_class = "budget_exhausted"` — a real failure
with a real diagnosis, not a silent stop. It is **resumable**: `a2a resume`
opens a new epoch, and a new epoch is a fresh budget.

Counted on both transports (the fold counts `turn_started` envelopes), but
**enforced broker-side only** — an exec worker's `turns_this_epoch` shows up in
`a2a ls --json` and nothing acts on it, because the exec transport has no
mid-turn interrupt to enforce with.

### The circuit breaker — `BREAKER_OPEN`

A fleet that is failing is usually failing for one reason, and the cheapest
thing to do about it is stop.

```toml
[admission]
breaker_failures   = 5     # failed settlements inside the window that open it
breaker_window_secs = 300  # the window
```

At the threshold the broker stops admitting **new** work: `a2a spawn` exits 9
with `BREAKER_OPEN`, and `error.data` carries `{failures, window_secs,
opened_at}`. Adoption is never refused — workers that were already running are
re-attached, because refusing them strands the fleet the daemon exists to
recover. `a2a broker status` says `breaker OPEN (…)` on a daemon that otherwise
looks perfectly healthy, which is the point.

```bash
a2a broker reset-breaker     # close it again (exit 7 if no broker is running)
a2a broker restart           # the other reset path: a fresh daemon starts closed
```

`breaker_failures = 0` **disables** the breaker. That is the one cap in this
tree where `0` does not mean "admit nothing" — a threshold whose job is to stop
work must not read "stop everything" when it is left unset.

### `a2a respawn` — retry with lineage, quarantine without

```bash
a2a respawn "$ID"                      # same brief, new worker
a2a respawn "$ID" --brief /tmp/fix.md  # a different brief, same lineage
```

The child records `parent_id` and `attempt`; `max_attempts` (default 3) is what
stops a poison brief being retried forever — the fourth try exits 9 with
`ATTEMPTS_EXHAUSTED` and `error.data` naming `{parent_id, attempt,
max_attempts}`. Respawning a parent that is still `spawned`/`queued`/`running`
is `PARENT_ACTIVE` (exit 9): kill it first, deliberately.

```toml
[admission]
max_attempts = 3
```

### `auto_review` — a reviewer per finished impl worker

```toml
[lanes.impl]
auto_review = true       # OFF by default
```

When an **impl** worker with a worktree reaches `done`, the broker diffs its
branch (`git diff <base_ref>..<branch>`), builds a review brief from that diff
plus the impl brief's acceptance criteria, and spawns a **review-lane worker**
for it. The reviewer's findings land in the impl worker's directory as
`review.json`.

**What it costs:** one extra worker per completed impl worker, and it goes
through the same admission door as anything else — it is counted against the
caps, it can queue behind them, and it can be refused by them. Turning this on
roughly doubles the workers a busy impl lane creates. An impl worker without a
worktree is skipped with a recorded `auto_review_skipped{reason:"no_worktree"}`
line in `broker.jsonl`; reviewing a diff that does not exist is worse than not
reviewing. Reviewers are never themselves reviewed, and a worker that already
has a `review.json` is not reviewed twice.

### `a2a spawn --template <lane>` — the brief scaffold

```bash
a2a spawn --template impl > /tmp/brief.md   # then fill it in and spawn it
```

Prints that lane's brief scaffold — front matter, acceptance criteria, verify
commands — and creates nothing. The scaffolds are compiled into the binary, so
there is no install step and no runtime data dir, and every machine answers the
same.

### `broker.jsonl` — the transition log

Every state transition in the fleet is one line, so a post-mortem is a `grep`:

```jsonc
{"ts":"…","kind":"state_change","worker":"impl-…","from":"queued","to":"spawned","event":"admit","epoch":1}
{"ts":"…","kind":"state_change","worker":"impl-…","from":"spawned","to":"running","event":"process_started","epoch":1}
{"ts":"…","kind":"state_change","worker":"impl-…","from":"running","to":"done","event":"turn_ended","epoch":1}
```

`from`/`to` are the same kebab-case spellings `a2a ls` prints; `event` names the
write that carried the transition. One line per transition, and none for a
self-loop.

### `events.jsonl` rotation

```toml
[broker]
events_max_bytes = 10485760   # 10 MiB
```

A worker whose transport floods it cannot grow its WAL without bound: at the
threshold `events.jsonl` is rotated to `events.jsonl.1` and a fresh one is
started, with an `events_rotated` marker recording the cut. Reads span both
generations, so `a2a tail`, `status` and the fold do not notice.

**One generation is kept.** A *second* rotation discards the oldest, and its
marker says `discarded_prior_generation: true` — so a rebuild-from-zero that
starts mid-log is visible rather than inferred. Multi-generation retention is
deliberately deferred.

### The two eval tiers

The worker protocol is testable rather than merely written down:

```bash
# Deterministic, always on: fixtures + the BLOCKED matcher, no tokens, no codex.
cargo test --test prompt_contract

# Statistical: k=3 real-codex runs against adversarial briefs. Gated, #[ignore]d,
# and it copies ~/.codex/auth.json into a temp CODEX_HOME (your real one is read,
# never written).
A2A_E2E=1 A2A_EVAL=1 cargo test --test prompt_contract -- --ignored --nocapture
```

Both gates must be `1`; either one missing skips the tier with a line saying so.

## Owner scoping

Every worker records an **owner** at genesis: `$A2A_OWNER`, or `anon-<uid>`.

```bash
a2a ls                       # your workers, plus every unowned one
a2a ls --all                 # the whole machine, with an OWNER column
a2a kill "$ID" --force       # act on somebody else's worker, deliberately
```

Mutating commands (`send`, `kill`, `interrupt`, `resume`) refuse a worker owned
by somebody else unless `--force` is given; `gc` skips them unless `--all` is
given; read-only commands never refuse. Legacy workers with no recorded owner
stay open to everyone, so upgrading does not strand them. The owner is recorded
at **genesis**, so it survives losing `state.json`.

---

## The whole loop, in one script

```bash
set -u
ID=$(a2a spawn --lane impl --cwd ~/dev/myproject --brief /tmp/brief.md)

while :; do
  a2a wait "$ID" --timeout 30m
  case $? in
    0)  a2a result "$ID"; break ;;                       # done
    5)  a2a send "$ID" "$(decide_somehow "$ID")" ;;      # blocked → answer, loop
    4)  echo "failed:"; a2a result "$ID" >&2; exit 1 ;;
    6)  echo "still running after 30m" >&2 ;;
    10) echo "orphaned" >&2; exit 1 ;;
  esac
done
```

## Exit codes

A versioned public contract, pinned by a test:

| code | meaning | code | meaning |
|---|---|---|---|
| 0 | success | 6 | not terminal yet (incl. `wait --timeout` expiry) |
| 1 | internal error | 7 | broker unavailable / version skew |
| 2 | usage error | 8 | a2a-internal timeout (e.g. `state.lock`) |
| 3 | no such worker | 9 | precondition / config |
| 4 | worker failed | 10 | orphaned |
| 5 | blocked / awaiting input | | |

## Configuration

Layered: **defaults < `$A2A_HOME/config.toml` < environment**. `a2a config show`
prints the effective configuration as JSON, and `--origin` annotates each value
with the layer it came from:

```bash
a2a config show --origin
```

The middle layer is a TOML file you write:

```toml
# $A2A_HOME/config.toml
allowed_roots = ["/Users/you/dev", "/Users/you/work"]
codex_version_range = ">=0.140, <0.150"

[lanes.impl]
model = "gpt-5.5"
effort = "high"
task_timeout_secs = 7200
max_turns = 40           # per-lane turn budget: impl 40, general 30,
                         # research 20, review 15. A brief may only lower it.
auto_review = false      # impl only: review every finished impl worktree

[admission]              # app-server transport only
global_cap = 8
queue_cap = 32
breaker_failures = 5     # failed settlements in the window that open the
                         # circuit breaker; 0 DISABLES it
breaker_window_secs = 300
max_attempts = 3         # `a2a respawn` attempts before ATTEMPTS_EXHAUSTED

[broker]
events_max_bytes = 10485760   # 10 MiB, then events.jsonl rotates to .1
```

Every key has an `A2A_`-prefixed environment override
(`A2A_ALLOWED_ROOTS`, `A2A_CODEX_VERSION_RANGE`,
`A2A_LANES_IMPL_TASK_TIMEOUT_SECS`, `A2A_LANES_IMPL_MAX_TURNS`,
`A2A_LANES_IMPL_AUTO_REVIEW`, `A2A_ADMISSION_GLOBAL_CAP`,
`A2A_ADMISSION_LANES_IMPL_CAP`, `A2A_ADMISSION_BREAKER_FAILURES`,
`A2A_ADMISSION_MAX_ATTEMPTS`, `A2A_BROKER_EVENTS_MAX_BYTES`, …). `A2A_HOME` (or `--home`) relocates the
entire tree — that is how the test suite stays hermetic. Two variables are
switches rather than settings: `A2A_BROKER_DISABLE=1`
([Transports](#transports)) and `A2A_OWNER` ([Owner scoping](#owner-scoping)).

## Mesh

The mesh is the same-machine coordination plane for Claude Code sessions and
Codex workers. A peer id is always `<provider>:<uuid>`, where provider is
`claude` or `codex`; on disk that becomes `mesh/peers/<provider>-<uuid>/`.
Codex peers also surface their a2a `worker_id` when the session belongs to a
known worker, so a note target may be either `codex:<session>` or the worker id.

The everyday loop is five commands:

```bash
a2a mesh roster --json              # who is present, stale, retired, and unread counts
a2a mesh post -                     # append to the shared feed from stdin
a2a mesh note <peer-or-worker> -    # send a directed note from stdin
a2a mesh inbox --consume            # read your notes and archive them
a2a mesh status -                   # set or clear your short status from stdin
```

Claude Code hooks are opt-in. Print the snippet, then merge it into your
`~/.claude/settings.json` yourself:

```bash
a2a mesh hooks --print
```

The snippet wires SessionStart to `register` + `inject`, UserPromptSubmit to
`heartbeat` + `inject`, and SessionEnd to `retire`. a2a never edits your hook
settings because installing a live hook changes when agent context is augmented;
that stays a human-controlled step.

Mesh text limits are forgiving backstops, not style rules. They stop runaway
loops from filling disk and refuse oversized input with exit 9 rather than
truncating it.

| plane | cap | refusal |
|---|---:|---|
| note | 1,048,576 bytes | `NOTE_TOO_LARGE` |
| post | 262,144 bytes | `POST_TOO_LARGE` |
| status | 4,096 bytes | `STATUS_TOO_LARGE` |

Every post, note and status is scanned on ingress; secret-shaped text refuses
with `SECRET_IN_POST`, `SECRET_IN_NOTE` or `SECRET_IN_STATUS` and the value is
not written. Roster, feed, inbox and inject output is redacted on egress.

Workers can originate notes without writing outside their worker directory by
dropping this JSON in `outbox/<seq>.note.json`:

```json
{"to":"claude:0a1ec845-4394-449e-a098-e0e01cb19887","text":"handoff for the next turn","refs":{"worker":["impl-0824t2201-q4ag"]}}
```

On reconcile, a note to a Claude peer lands in that peer's mesh inbox; a note to
a Codex peer or worker id rides the existing `a2a send` queue. The broker uses
the same shape at turn end for app-server workers.

The live smoke is read-only and ignored by default:

```bash
A2A_MESH_LIVE=1 cargo test --test mesh_live -- --ignored
```

It parses `roster --json` and `feed --json` against your real `~/.a2a` and
checks a before/after hash of `~/.a2a/mesh` (or its absence) so those reads prove
they wrote nothing. It does not register, post or install hooks.

## Mesh Fleet (Layer 2)

Layer 2 links multiple a2a homes into one mesh fleet through a registry and a
gateway daemon. Initialize the fleet once with a registry URL; file registries
are useful for loopback tests, and Postgres/Neon is the intended shared
registry:

```bash
printf '%s\n' 'postgres://user:pass@ep-name.region.aws.neon.tech/a2a?sslmode=verify-full' |
  a2a mesh fleet init --registry -
```

Enrollment means giving a gateway the fleet root public material plus the same
`mesh/fleet/registry.env`, then minting that gateway's local key material under
`mesh/gateway/` with 0600 private-key files. A reachable gateway publishes a
registry row with `endpoint_url` and its TLS certificate fingerprint; an
outbound-only gateway keeps `endpoint_url = NULL` and can still dial peers that
are exposed. `a2a mesh fleet ls --json` reads the registry and computes active
or stale state from heartbeats, while `a2a mesh fleet revoke <gateway-id>` marks
a gateway untrusted without deleting its history.

Run the gateway in the foreground under a process manager:

```bash
a2a mesh gateway run
a2a mesh gateway status --json
a2a mesh gateway stop
```

Exposure is controlled by `[mesh.gateway]`. `expose = "none"` keeps the gateway
outbound-only, `expose = "manual"` publishes `endpoint_url`, and
`expose = "ngrok"` asks a local ngrok API for a TCP tunnel and converts it to a
`wss://` endpoint. Neon pooler URLs are supported; when the host contains
`-pooler`, a2a disables sqlx's statement cache so PgBouncer transaction pooling
does not collide on prepared statement names after scale-to-zero reconnects.

Security model: gateways connect with pinned TLS, using the SPKI fingerprint in
the registry rather than trusting hostnames alone. Each gateway also presents a
fleet-root-signed PoP token and proves possession of the token key during the
wire handshake. Remote notes pass through the same ingress scanner and size caps
as local notes before any inbox file is written. The registry stores gateway
endpoints and public fingerprints only, never private keys, note text or feed
payloads. Revocation sets `revoked_at`, discovery hides the row immediately, and
anti-entropy re-checks existing links so revoked gateways are dropped.

The layer-2 live tier is gated twice and writes only its own generated registry
row; it does not start a gateway daemon against the real home:

```bash
A2A_MESH_L2_LIVE=1 \
A2A_MESH_L2_REGISTRY_URL='postgres://...' \
A2A_MESH_L2_POOLER_URL='postgres://...-pooler...' \
cargo test --test mesh_l2_live -- --ignored
```

Operationally, assume every timer is jittered: registry heartbeats, redials,
outbound note retries and anti-entropy all spread recovery load instead of
thundering at the same instant. The gateway health file reports
`replication.per_origin_lag` and `last_anti_entropy`; sustained lag is also
written as `kind: "drift"` lines in `mesh/gateway/gateway.jsonl`. For Neon,
expect a cold first acquire after scale-to-zero and keep the gateway's registry
pool small; layer-2 uses one connection, idle timeout, transaction-local
statement timeouts and the pooler statement-cache branch above.

## Sync: giving Codex workers the same capability layer

Two one-way mirrors from the Claude side into the Codex home, both governed by a
single ownership ledger:

```bash
a2a sync skills --dry-run    # decide everything, write nothing
a2a sync skills              # ~/.claude/skills      → $CODEX_HOME/skills/<name>/
a2a sync mcp --dry-run
a2a sync mcp                 # ~/.claude.json        → $CODEX_HOME/config.toml
```

`sync skills` copies curated skill directories **flat** into
`$CODEX_HOME/skills/<name>/`, and first writes — then re-reads from disk — a
managed `# BEGIN a2a-sync` … `# END a2a-sync` block in that directory's
`.gitignore`, refusing to copy anything the block does not cover. That
destination is usually a git submodule, and a skill tree landing in it uncovered
turns somebody's clean checkout into hundreds of untracked files.

`sync mcp` translates each `mcpServers` entry into an `[mcp_servers.*]` table
with `toml_edit`, so the comments, ordering and blank lines of a hand-maintained
`config.toml` survive the write. The candidate is rendered, handed to
`CODEX_HOME=<temp copy> codex mcp list --json` for a real load check, and only
then replaces the file — a config codex refuses never reaches your home.

**The ownership rule, in one sentence:** `$A2A_HOME/sync-manifest.json` records
every destination a2a created, and a2a only ever updates or removes a
destination that ledger says it created — everything else is reported and left
byte-for-byte alone.

Two consequences worth knowing. Losing the manifest costs you the ability to
**update**, never your content: every destination becomes somebody else's and is
skipped. And `--dry-run` is side-effect-free *structurally* rather than by
discipline — it runs the planner, and the planner only reads.

Every completed run exits **0**, including runs full of skips and refusals: the
verdict travels in `data.counts`, because a fleet-wide sync that exited nonzero
whenever one destination had drifted would be re-run with a bypass by every
caller. Exit **9** is reserved for static preconditions (no codex home, an
unreadable ledger, a missing source) that touched nothing.

### What a run reports

| disposition | meaning |
|---|---|
| `synced` / `unchanged` / `removed` | a2a owns it: written, already current, or dropped because its source is gone |
| `skipped-not-a-skill` | a plain file, or a directory with no `SKILL.md` |
| `skipped-side-effecting` | the skill acts on the world — see below |
| `skipped-collision` | the destination already exists and the ledger does not own it |
| `skipped-unowned` | an `[mcp_servers]` table a2a did not write **for this account** |
| `skipped-secret` | a credential that would land in a git-tracked file |
| `skipped-untranslatable` | translatable in principle, skipped by policy — see headers below |
| `refused-dest-drift` | a2a made it, somebody edited it since; it is neither overwritten nor removed |

`skipped-unowned` has two shapes, because "nobody synced this" and "the other
account synced this" are different problems: a table no ledger entry mentions
reports `a foreign [mcp_servers] table wins; a2a only ever updates what it
created`, while one another account owns names the owner — ``account `claude`
synced this server first; a2a only ever updates what it created for the account
it was run for`` — so you are not left guessing which of your Claude configs got
there first. (`--claude-config-dir ~/.claude-revenium` is how the second account
syncs; MCP entries are namespaced per account, skills are shared.)

### Which skills get mirrored

A worker that never asks for approval will happily run any dangerous skill it
can reach, so classification is deny-first:

| rule | effect |
|---|---|
| name on the built-in seed list (`slack-send`, `docusign`, `dmv-keys`, … 15 names) | `skipped-side-effecting`, reason `seed list` |
| one of `credential, send, post, deploy, password, token, aws` in the directory name or the **YAML front matter** | `skipped-side-effecting`, reason names the word |
| anything else | mirrored |

The heuristic reads the front matter only, never the body: a guide that mentions
`deploy` in an example is not a deploy tool, and scanning bodies flagged nearly
the whole corpus on a real machine. Matching is on word boundaries, so `send`
does not fire inside "sender".

The only way past a refusal is a human writing the name down:

```toml
# $A2A_HOME/config.toml
[sync]
allow_skills = ["docusign"]                 # skill directory names
allow_http_header_servers = ["ghl"]         # SERVER names, not header names
claude_config_dir = "/Users/you/.claude-revenium"
```

### The secret policy

**a2a never writes a literal credential into a file git publishes.** What that
means per source shape:

- **`Authorization: Bearer …` on an HTTP server** is translated through codex's
  indirection rather than copied: the table gets
  `bearer_token_env_var = "A2A_MCP_<NAME>_TOKEN"` and you export the value
  yourself. The naming is exact and mechanical — `Neon` →
  `A2A_MCP_NEON_TOKEN`, `linear` → `A2A_MCP_LINEAR_TOKEN`,
  `google-workspace` → `A2A_MCP_GOOGLE_WORKSPACE_TOKEN`. Export the token
  **without** the `Bearer ` prefix (codex supplies the scheme); if the server
  rejects that, export the whole header value instead.
- **Any other header** (`locationId`, `X-Api-Key`, …) is `skipped-untranslatable`
  by default, with the exact TOML you could paste yourself printed as a remedy —
  header values masked. Opt in per server with
  `[sync] allow_http_header_servers = ["ghl"]`; the key holds **server** names,
  and a list of header names would allowlist nothing while reading as though it
  had.
- **Credential-shaped `env` values on a stdio server** are `skipped-secret` for
  as long as the destination `config.toml` is tracked by git. Untrack it (below)
  and the same run writes them.

Every value read from a source config is printed as `[REDACTED:<len>]` in every
mode, whatever the classifier decided — a misclassification cannot leak, because
the mask does not consult the classification. Errors, warnings and `doctor`
details name **keys, header names, env var names, files and line numbers**, and
never a value.

### Taking `config.toml` out of git (one time)

If `$CODEX_HOME` is a git repository that tracks `config.toml` — a published
dotfiles repo — stdio servers with credentials stay blocked until you run:

```bash
a2a sync mcp --migrate-untrack        # git rm --cached config.toml + gitignore it, then sync
```

It removes the file from the **index only** (your copy stays), appends an ignore
line marked ``# a2a: untracked by `a2a sync mcp --migrate-untrack` ``, and then
proceeds with the sync that the migration just unblocked. Commit the staged
removal yourself.

**It is not a fix for what is already out.** Untracking changes nothing about
the copies on the remote, in forks, or in CI caches — the reminder it prints
says so verbatim (*already-published secrets must still be rotated*) and names
the lines to rotate, never their contents.

### The three checks `doctor` gained

| check | says |
|---|---|
| `context_canon` | whether the `CLAUDE.md` in the current directory imports `@AGENTS.md`, so a Codex worker reads the same instructions a Claude session does |
| `codex_config_secrets` | whether `$CODEX_HOME/config.toml` is git-tracked **and** carries a secret shape — the only security-class `fail` in the list |
| `sync_state` | whether every destination the ledger records is still as a2a left it (and warns on a ledger that will not parse) |

On a machine whose `~/.codex/config.toml` is tracked by git and carries a
credential, `codex_config_secrets` **fails and `a2a doctor` exits 9** — it names
the file and the line numbers, never the values. Clear it by running
`a2a sync mcp --migrate-untrack`, committing the staged removal, and rotating the
key that was published; the check flips to `ok` as soon as git stops tracking the
file.

### Three deviations from the design doc, recorded

1. **`--strict-config` is not the post-write validator.** It is nondeterministic
   on codex 0.140.0 — the identical argv returns either an unknown-field error
   or "`--strict-config` is not supported for this subcommand", both exit 1 and
   indistinguishable to a caller. `codex mcp list --json` replaces it and also
   yields structured `transport.type`/`auth_status`. The cost: unknown fields are
   tolerated, so the check proves the config **loads**, not that it is
   schema-complete.
2. **Codex *can* express arbitrary HTTP headers.** `http_headers` (literal
   values) and `env_http_headers` (header → env var name) both round-trip, so
   skipping `ghl`'s `locationId` is a **policy** choice about copying
   account-scoping data into a shared config — not a capability limit. Hence the
   `allow_http_header_servers` opt-in above; the default is still to skip,
   loudly.
3. **The generated global `~/.codex/AGENTS.md` is deferred.** This phase ships
   only the `context_canon` check that reports on it; generating the file belongs
   with the worker-protocol templates.

### Proving it against a real machine

The suite is hermetic — fixture Claude directories, a temp `CODEX_HOME`, and a
runtime guard that refuses the real `$HOME` with `SYNC_REFUSES_REAL_HOME` (exit
9) unless `A2A_SYNC_ALLOW_REAL_HOME=1` is set. One gated file opts in, and only
to **read**:

```bash
A2A_SYNC_LIVE=1 cargo test --test sync_live -- --ignored
```

It runs both syncs with `--dry-run` against your real `~/.claude.json`,
`~/.claude/skills` and `$CODEX_HOME`, and proves they wrote nothing by diffing
`git status --porcelain` in `~/.codex` and in `~/.codex/skills`, the content
hashes of `config.toml` and the skills tree, and `~/.a2a/sync-manifest.json`,
before against after. It then sweeps both transcripts for every value in your
real `~/.claude.json` and reports only lengths if it finds one. **There is no
live apply and no live migration in it** — pointing `sync` at your machine for
real is a decision you make, not one a test suite makes for you.

## Safety properties

- **Secrets never reach a worker's context.** Briefs and context files are scanned
  on ingress (spawn fails on a hit); every printed stream is redacted on egress;
  events are redacted at append time, so the WAL itself is clean.
  `OPENAI_API_KEY`, `AWS_*` and `DEPOT_TOKEN` are scrubbed from every worker's
  environment. The broker's app-server child gets an **allowlist**, not the
  ambient environment: `PATH` (pinned at broker start), `HOME`, `USER`, `TERM`,
  `LANG`, `TMPDIR`, `CODEX_HOME` — nothing else crosses.
- **The socket cannot out-privilege the CLI.** `broker.sock` is 0600, accepts
  only a peer whose uid is the daemon's own, and the broker **re-validates**
  every wire-supplied `cwd` against the allowed roots it was started with.
- **No shell, ever.** Every codex invocation is an argv array, and no brief or
  message text appears in argv — a `ps` on this machine reveals nothing.
- **Crash-safe by construction.** `events.jsonl` is append-only with `sync_data`
  per line and a torn-tail repair on open; `state.json` is a rebuildable
  projection written temp→fsync→rename→fsync-dir.
- **Terminal states are absorbing.** Only `a2a resume` leaves one, in a new epoch.
- **`--ephemeral` is never passed to codex** (it would kill resumability), and
  `danger-full-access` is rejected outright.

## Development

```bash
cargo test                                     # hermetic: no real codex, no network

# The gated proofs. Real codex, real ~/.codex auth (read, never written), tokens.
A2A_E2E=1 cargo test --test e2e_exec      -- --ignored --nocapture
A2A_E2E=1 cargo test --test e2e_appserver -- --ignored --nocapture

# Read-only, no tokens: both syncs dry-run against this machine ([Sync](#sync-giving-codex-workers-the-same-capability-layer)).
A2A_SYNC_LIVE=1 cargo test --test sync_live -- --ignored
```

The suite never touches `~/.a2a` or `~/.codex`; every test gets a temp
`A2A_HOME`, the scripted stub at `tests/fixtures/stub-codex` (exec) and the
scripted `tests/fixtures/fake-app-server` (live transport), so `cargo test` is
green from a clean checkout with no codex on `PATH` and no network.

The two `A2A_E2E`-gated smokes are the exception, and they are the only thing
that can catch **vocabulary drift** — a fake can only ever confirm the
vocabulary we wrote into it. `e2e_exec` validates `events::map_exec_line`
against what `codex exec --json` really emits; `e2e_appserver` validates
`events::map_appserver_notification` and
`adapter::is_steer_precondition_failure` against a real `codex app-server`,
proves `a2a interrupt` really ends a turn, and proves the **cross-transport
resume**: an app-server worker, finished, its broker shut down, continued by
`codex exec resume <session>` on the same conversation.

- Design: `docs/superpowers/specs/2026-08-14-a2a-claude-codex-design.md`
- Worker state machine: `docs/machines/worker.md`
