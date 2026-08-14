# A2A: Claude Code orchestrators + Codex workers — design spec

**Date:** 2026-08-14
**Status:** Approved design, pre-implementation
**Repo:** `~/dev/a2a` → private GitHub (Dev916), Python CLI `a2a` installed globally via uv tool

## 1. Purpose

Cross-agent orchestration between Claude Code (orchestrator) and OpenAI Codex CLI
(workers), using each model where it is strongest. Claude Code plans, dispatches,
steers, and verifies; Codex executes discrete tasks. Alongside the dispatch
protocol, a shared-capability layer keeps skills, MCP servers, and project
context available to both runners without double maintenance.

Everything targets this machine's existing conventions: own repo + global uv-tool
CLI, devloop-driven builds, file-based state that a human or agent can inspect.

## 2. Decisions made during brainstorming

| Question | Decision |
|----------|----------|
| Worker roles | All four: devloop task executors, cross-model review, parallel research, general dispatch. General protocol is the foundation; lanes are consumers. |
| Skills sync policy | Auto-sync Claude skills → Codex, with the 4 hand-curated Codex ports (`brainstorming`, `devloop`, `writing-plans`, `writing-devloop-plans`) kept as overrides — name collision means the Codex-native version wins. Supersedes (partially) the "never sync" decision in the codex-skills README, which must be updated. |
| Communication model | Live bidirectional: orchestrator can steer/interrupt a running worker; workers can raise questions mid-task. |
| Architecture | Approach A: broker on `codex app-server` (JSON-RPC), with the file layer from Approach B built in and `codex exec` as an automatic fallback transport. |

## 3. Environment facts (verified 2026-08-14)

- Codex CLI **0.140.0** at `~/.nvm/versions/node/v23.10.0/bin/codex`; config
  `~/.codex/config.toml` (gpt-5.5, xhigh reasoning, 7 MCP servers).
- Codex natively supports skills at `~/.codex/skills/` (SKILL.md format,
  `.system/` built-ins git-ignored). `~/.codex/skills` is the
  `nyvorin/codex-skills` git repo containing the 4 curated ports.
- `codex exec`: `--json` (JSONL events), `--output-last-message <file>`,
  `--output-schema <file>` (JSON Schema for the final response), `-C <dir>`,
  `--add-dir`, sandbox modes (`read-only`, `workspace-write`,
  `danger-full-access`), `exec resume <session-id>`. Sessions persist on disk.
- `codex app-server`: experimental JSON-RPC-over-stdio programmatic interface
  (thread lifecycle, mid-turn message injection, interrupts, event streaming).
  `codex mcp-server` exists but was rejected for orchestration (blocking calls,
  MCP timeouts, no steer).
- Codex MCP config supports stdio (`command`/`args`/`env`) and streamable HTTP
  (`url`) servers.
- Claude side: 17 user-scoped MCP servers in `~/.claude.json`; personal skills
  in `~/.claude/skills/`; plugin skills under `~/.claude/plugins/cache/`.
- No global `~/.codex/AGENTS.md` exists yet.

## 4. Architecture

```
Claude Code (orchestrator)
  └─ a2a-orchestrate skill  ──uses──▶  a2a CLI
                                        │ unix socket ~/.a2a/broker.sock
                                        ▼
                                      broker (daemon-lite)
                                        │ JSON-RPC stdio
                                        ▼
                                      codex app-server ──▶ worker threads
                                        │
                        fallback:  codex exec --json (per worker)
                                        │
                            ~/.a2a/workers/<id>/…  (file layer, both transports)
```

### 4.1 a2a CLI

```
a2a spawn --lane impl|review|research|general --cwd <dir> [--worktree]
          --brief <file> [--schema <file>] [--model <m>] [--transport auto|app-server|exec]
                                              → prints worker-id
a2a send <id> "<msg>"        # steer a running worker, or answer its question
a2a interrupt <id>           # stop the current turn (app-server transport only)
a2a tail <id> [--follow]     # human/agent-readable event stream
a2a status [id]              # running | awaiting-input | done | failed | orphaned
a2a result <id>              # final message; prints result.json path when schema was used
a2a ls                       # all workers + states
a2a resume <id> ["<msg>"]    # continue an orphaned/finished worker's session
a2a kill <id>                # terminate
a2a gc [--age <dur>]         # remove finished workspaces
a2a doctor                   # codex present, app-server handshake, transport selection,
                             # context-canon warnings
a2a sync skills [--dry-run]
a2a sync mcp    [--dry-run]
```

Config: `~/.a2a/config.toml` — default model, per-lane defaults (sandbox,
network, timeout), skill/MCP exclusion lists, transport preference.

### 4.2 Broker

- Daemon-lite: first CLI call that needs it starts it (lockfile-guarded);
  idle-shutdown after a configurable period with no live workers.
- Owns exactly one `codex app-server` child; multiplexes workers as app-server
  threads (one thread per worker).
- Appends every app-server event to the worker's `events.jsonl`; maintains
  `state.json` transitions.
- **Adapter isolation:** all knowledge of the app-server JSON-RPC protocol lives
  in a single adapter module. Codex updates that shift the experimental API are
  patched there and nowhere else. The broker pins the tested Codex version range
  and `a2a doctor` verifies the handshake before selecting the transport.
- CLI ↔ broker: newline-delimited JSON over unix socket `~/.a2a/broker.sock`.
- Crash handling: stale socket detected → broker restarts → previously-running
  workers are marked `orphaned` in their `state.json`. Because Codex persists
  sessions on disk, an orphaned worker's conversation is resumable; `a2a spawn
  --resume <id>` (or the orchestrator) can continue it on either transport.

### 4.3 Exec transport (fallback)

- `codex exec --json --output-last-message result.md -C <cwd>` per worker, run
  as a detached background process; PID + session-id recorded in `state.json`.
- Steering: messages queue in `inbox/`; delivery happens at turn boundaries via
  `codex exec resume <session-id>` with the queued content. `a2a interrupt`
  degrades to SIGINT (best-effort).
- Selected automatically when `a2a doctor` fails the app-server handshake, or
  explicitly via `--transport exec` / config.

### 4.4 File layer (identical on both transports)

```
~/.a2a/
  config.toml
  broker.sock / broker.lock / broker.log
  workers/<id>/
    brief.md          # orchestrator-authored task brief (lane template)
    context/          # files the orchestrator explicitly shares
    events.jsonl      # full event stream
    inbox/            # steer messages in (always recorded, even on app-server)
    outbox/           # worker questions + progress notices
    result.md         # worker's final message
    result.json       # present when --schema was given
    state.json        # {lane, cwd, transport, session_id, state, timestamps}
```

The file layer is the contract. Transports are implementation detail; anything
reading `~/.a2a/workers/` (human, orchestrator, tests) sees the same shapes.

### 4.5 Worker protocol

Delivered as an `a2a-worker` skill in `~/.codex/skills/` plus a section in the
new global `~/.codex/AGENTS.md`:

1. Read `brief.md` fully before acting; treat `context/` as curated background.
2. Work only in the assigned cwd/worktree.
3. When blocked on a decision only the orchestrator can make: write the
   question to `outbox/`, end the turn with a `BLOCKED:` marker as the final
   line. The orchestrator answers via `a2a send`; the reply arrives as the next
   user message.
4. Post significant findings as `outbox/` notices as they happen.
5. Finish by writing the result in the brief's required format, including
   verification evidence (commands run + output) — claims without evidence are
   incomplete work.

Sandboxing defaults: `workspace-write`, approvals never (headless). Network
access and `danger-full-access` are per-lane opt-ins in config, never global.

### 4.6 Orchestrator skill (Claude side)

`a2a-orchestrate` skill in `~/.claude/skills/`:

- When to dispatch to Codex vs a Claude subagent.
- Brief-writing guidance per lane (self-contained, acceptance criteria,
  verification commands, context file selection).
- Monitor loop: background `a2a tail --follow` / `a2a status` polling, steering
  with `a2a send`, answering `BLOCKED` workers.
- Collect + verify: the orchestrator re-verifies worker evidence before
  accepting a result (trust but verify).
- Parallel dispatch patterns and worktree hygiene.

**Devloop integration:** devloop (both runners' versions) gains a dispatch
option — a plan task may be executed by `a2a spawn --lane impl --worktree`
instead of a Claude subagent, with the same acceptance-criteria verification
step afterward.

### 4.7 Lanes

Lanes are brief templates + spawn defaults only — no separate machinery.

| Lane | Sandbox | Brief template requires | Result |
|------|---------|------------------------|--------|
| `impl` | workspace-write, `--worktree` default | acceptance criteria, files in scope, verify commands | schema: files changed, tests run, evidence, done/blocked |
| `review` | read-only | diff or branch ref, review dimensions | schema: findings (file, line, severity, rationale) |
| `research` | read-only + network | question, source hints, report format | `result.md` report with citations |
| `general` | workspace-write | freeform | freeform `result.md` |

## 5. Shared-capability layer

### 5.1 `a2a sync skills`

- One-way mirror: `~/.claude/skills/*` → `~/.codex/skills/*`.
- Collision rule: if the name exists in `~/.codex/skills` and is **not** a prior
  sync product, skip it (curated port wins). Sync products are tracked in a
  manifest at `~/.a2a/sync-manifest.json` (shared with `sync mcp`), so re-syncs
  update managed entries but never touch curated ones.
- Exclusion list in `~/.a2a/config.toml` for Claude-only skills.
- Plugin-provided skills (superpowers, sc:*, etc.) excluded by default.
- No content transform in v1; SKILL.md format is shared. The worker AGENTS.md
  notes that MCP tool naming differs between runners (`mcp__server__tool` vs
  Codex naming) so skills referencing tools by exact name may degrade.
- Respect the codex-skills git repo: sync writes land as normal files inside it;
  the README's "never merged" section is updated to describe this policy.
- Runnable manually and from the existing SessionStart hook chain.

### 5.2 `a2a sync mcp`

- Translates `~/.claude.json` `mcpServers` → `[mcp_servers.*]` in
  `~/.codex/config.toml` using tomlkit (preserves comments/formatting).
  stdio → `command`/`args`/`env`; HTTP → `url`.
- Never modifies entries that exist only on the Codex side (mx, laravel-boost,
  mono, chrome-devtools, puppeteer, playwright, context7).
- Managed-entry rule mirrors skills sync: only entries the tool created (tracked
  in `~/.a2a/sync-manifest.json`) are updated/removed on re-sync.
- Exclusion list for non-portable servers.
- `--dry-run` prints the TOML diff. Output flags OAuth HTTP servers (linear,
  akiflow) as needing one-time `codex mcp login <name>`.

### 5.3 Context canon

- Per-project `AGENTS.md` is the single source of truth; `CLAUDE.md` in those
  repos becomes `@AGENTS.md` plus Claude-specific extras.
- New global `~/.codex/AGENTS.md` (versioned in the `.codex` home repo):
  machine conventions + worker protocol summary.
- `a2a doctor` warns when a project has `CLAUDE.md` without the `@AGENTS.md`
  import. Migration of existing projects is manual and incremental.
- Task-level context always flows through `brief.md` + `context/` — the
  orchestrator curates what each worker sees; no assumption of shared memory.

## 6. Error handling

- Broker crash → stale-lock/socket detection → clean restart → running workers
  marked `orphaned`, resumable via persisted Codex sessions.
- Worker turn/task timeouts per lane (config); timeout → `failed` with reason in
  `state.json`, session still resumable.
- app-server handshake failure at any point → transport falls back to exec for
  new spawns; existing app-server workers are orphaned-and-resumable.
- Every failure mode terminates in on-disk state that a human or the
  orchestrator can read. No invisible states.

## 7. Testing

- **Unit:** broker state machine + both transports against a fake app-server
  (scripted JSON-RPC); sync tools against fixture configs asserting the
  no-clobber rules (curated skills, Codex-only MCP entries).
- **E2E smoke (real codex):** trivial spawn→result; BLOCKED→`a2a send`→resume
  round-trip; mid-turn interrupt (app-server); forced exec-transport fallback;
  `a2a sync * --dry-run` on the live machine producing sane diffs.
- Built via a devloop plan with per-task acceptance criteria (CLI toolkit).

## 8. Build order (each phase independently usable)

1. **File layer + exec transport** + `spawn/status/result/ls/kill/gc` — workers
   function day one, degraded liveness.
2. **Broker + app-server transport** — `send`/`interrupt`/`tail --follow` go
   live; `doctor` gains handshake + auto-selection.
3. **Sync tooling** (`sync skills`, `sync mcp`) + global `~/.codex/AGENTS.md` +
   context-canon checks in `doctor`.
4. **Skills**: `a2a-orchestrate` (Claude) + `a2a-worker` (Codex) + devloop
   dispatch integration.
5. **Lane polish**: review/research templates + result schemas.

## 9. Risks & mitigations

| Risk | Mitigation |
|------|-----------|
| app-server API is experimental; Codex updates may break it | Single adapter module; `doctor` handshake gate; exec transport always available; pin tested version range |
| Worker briefs under-specified → garbage results | Lane templates force acceptance criteria + verify commands; orchestrator skill teaches brief-writing; BLOCKED escalation path |
| Sync clobbers curated content | Manifest-tracked managed entries; never touch non-managed files; `--dry-run`; unit-tested no-clobber rules |
| Two runners drift on skill semantics (tool names) | Documented degradation note in worker AGENTS.md; exclusion list for tightly-coupled skills |
| Codex quota burn from parallel workers | Per-lane concurrency cap in config |

## 10. Out of scope (v1)

- Codex orchestrating Claude workers (reverse direction).
- Cross-machine workers; everything is local.
- Automatic CLAUDE.md→AGENTS.md migration of existing projects.
- Syncing plugin-provided Claude skills.
- Live token-level streaming into the Claude UI (events.jsonl + tail is the v1
  visibility story).
