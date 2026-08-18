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
10 check(s): 8 ok, 2 warn, 0 fail
```

One line per check; **exit 9 if any check fails** (warnings do not fail). Run it
first — it creates the home tree (0700), performs a real `codex app-server`
handshake, and catches the version-coupled transport mismatch before a worker
does. `app_server_handshake` failing is a warning, not an error: it means every
spawn will quietly take the exec fallback.

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

[admission]              # app-server transport only
global_cap = 8
queue_cap = 32
```

Every key has an `A2A_`-prefixed environment override
(`A2A_ALLOWED_ROOTS`, `A2A_CODEX_VERSION_RANGE`,
`A2A_LANES_IMPL_TASK_TIMEOUT_SECS`, `A2A_ADMISSION_GLOBAL_CAP`,
`A2A_ADMISSION_LANES_IMPL_CAP`, …). `A2A_HOME` (or `--home`) relocates the
entire tree — that is how the test suite stays hermetic. Two variables are
switches rather than settings: `A2A_BROKER_DISABLE=1`
([Transports](#transports)) and `A2A_OWNER` ([Owner scoping](#owner-scoping)).

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
