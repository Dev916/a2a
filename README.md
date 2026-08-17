# a2a — Claude-orchestrated Codex workers

`a2a` dispatches [Codex](https://github.com/openai/codex) workers, watches them,
steers them at turn boundaries, and reaps them — from one small CLI whose **exit
code is the answer**. It is built to be driven by another agent (Claude, a
script, a Makefile) rather than watched by a human: every command answers in one
call, so an orchestrator never burns its context window polling.

All worker state lives on disk under `$A2A_HOME` (default `~/.a2a`) as a
write-ahead log (`events.jsonl`, the source of truth) plus an atomic snapshot
(`state.json`). Workers are detached `codex exec --json` process groups whose
liveness is a kernel-held `flock` — **no daemon**, and nothing is lost if the
CLI, your shell, or the machine goes away mid-turn.

This is **Phase 1** (file layer + worker state machine + exec transport). The
broker, the app-server transport, and mid-turn interrupts arrive in Phase 2.

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
⚠ env_inheritance  Phase 1 exec workers inherit the CLI environment minus the scrub list …
6 check(s): 5 ok, 1 warn, 0 fail
```

One line per check; **exit 9 if any check fails** (warnings do not fail). Run it
first — it creates the home tree (0700) and catches the version-coupled
transport mismatch before a worker does.

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

`send` queues the message and delivers it at the next turn boundary, resuming the
worker's codex session in a new epoch. The text travels through a 0600 prompt
file on the child's stdin — **never through argv**. `--queue-only` batches several
messages into one delivery; `--kind steer` marks guidance rather than an answer.

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
```

Every key has an `A2A_`-prefixed environment override
(`A2A_ALLOWED_ROOTS`, `A2A_CODEX_VERSION_RANGE`,
`A2A_LANES_IMPL_TASK_TIMEOUT_SECS`, …). `A2A_HOME` (or `--home`) relocates the
entire tree — that is how the test suite stays hermetic.

## Safety properties

- **Secrets never reach a worker's context.** Briefs and context files are scanned
  on ingress (spawn fails on a hit); every printed stream is redacted on egress;
  events are redacted at append time, so the WAL itself is clean.
  `OPENAI_API_KEY`, `AWS_*` and `DEPOT_TOKEN` are scrubbed from every worker's
  environment.
- **No shell, ever.** Every codex invocation is an argv array, and no brief or
  message text appears in argv — a `ps` on this machine reveals nothing.
- **Crash-safe by construction.** `events.jsonl` is append-only with `sync_data`
  per line and a torn-tail repair on open; `state.json` is a rebuildable
  projection written temp→fsync→rename→fsync-dir.
- **Terminal states are absorbing.** Only `a2a resume` leaves one, in a new epoch.
- **`--ephemeral` is never passed to codex** (it would kill resumability), and
  Phase 1 rejects `danger-full-access` outright.

## Development

```bash
cargo test                                    # hermetic: no real codex, no network
A2A_E2E=1 cargo test --test e2e_exec -- --ignored --nocapture   # real codex, spends tokens
```

The suite never touches `~/.a2a` or `~/.codex`; every test gets a temp
`A2A_HOME` and the scripted stub at `tests/fixtures/stub-codex`. The single
exception is the `A2A_E2E`-gated smoke, which reads (and never writes) the real
`~/.codex` auth — it is what validates the `codex exec --json` mapping table in
`src/events.rs` against the vocabulary codex actually emits.

- Design: `docs/superpowers/specs/2026-08-14-a2a-claude-codex-design.md`
- Worker state machine: `docs/machines/worker.md`
