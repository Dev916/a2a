# `schema/v2-snapshot/` — regeneration and drift policy

This directory is a **checked-in subset** of the JSON Schema that
`codex app-server` emits for its own wire protocol. It exists because the
app-server protocol offers **no version handshake** — the only way `a2a` can
learn that the protocol moved under it is to diff a snapshot (spec §8.3, and
`docs/development/appendix-api-design.md` on versioned contracts).

`tests/protocol_canary.rs` is the gate. It resolves `codex` **from `PATH` only**
(deliberately ignoring `A2A_CODEX_BIN`: this is a protocol gate, not a worker
path), regenerates into a tempdir with `CODEX_HOME` pointed at another tempdir,
and byte-diffs every file below plus set-compares the two method lists. The
regen costs **zero tokens and needs no auth**.

## Captured against

- `codex-cli 0.152.1` (`codex --version`), 2026-09-02.
- Version range the binary accepts lives in `config::DEFAULT_CODEX_VERSION_RANGE`
  (`>=0.140, <0.150`). This file does not duplicate it.

## Regen command

```sh
CODEX_HOME="$(mktemp -d)" codex app-server generate-json-schema --out "$(mktemp -d)"
```

then copy exactly the subset below into `schema/v2-snapshot/`, preserving the
`v1/` and `v2/` subdirectories, and re-extract the two method lists:

```sh
python3 - <<'EOF'
import json, os
OUT = "<the --out dir>"; DST = "schema/v2-snapshot"
def methods(f):
    d = json.load(open(os.path.join(OUT, f)))
    return sorted({v["properties"]["method"]["enum"][0]
                   for v in d.get("oneOf", [])
                   if v.get("properties", {}).get("method", {}).get("enum")})
for src, dst in [("ClientRequest.json", "client_request_methods.txt"),
                 ("ServerNotification.json", "server_notification_methods.txt")]:
    open(os.path.join(DST, dst), "w").write("\n".join(methods(src)) + "\n")
EOF
```

## The captured subset

Byte-snapshotted, paths relative to the `--out` dir (identical here):

| file | why |
|---|---|
| `v1/InitializeParams.json` | handshake request — note `initialize` lives in **v1/**, not v2/ |
| `v1/InitializeResponse.json` | handshake reply (`userAgent`, `codexHome`, `platformFamily`, `platformOs` — no version field) |
| `v2/ThreadStartParams.json` | worker spawn (`sandbox` is the kebab-case `SandboxMode` string) |
| `v2/ThreadStartResponse.json` | echoed `sandbox` is the **typed** `SandboxPolicy` object; `thread.id == thread.sessionId` |
| `v2/TurnStartParams.json` | per-turn `sandboxPolicy` / `outputSchema` (re-supply every turn) |
| `v2/TurnStartResponse.json` | `{turn}` |
| `v2/TurnSteerParams.json` | mid-turn steering (`expectedTurnId` — the race guard) |
| `v2/TurnInterruptParams.json` | true interrupt |
| `v2/ThreadResumeParams.json` | resume (no `sandboxPolicy` — re-supply on the next `turn/start`) |
| `v2/TurnStartedNotification.json` | `{threadId, turn}` |
| `v2/TurnCompletedNotification.json` | `{threadId, turn}`; `TurnStatus` enum lives here |
| `v2/ItemCompletedNotification.json` | `{threadId, turnId, item, completedAtMs}`; `ThreadItem` discriminants live here |
| `v2/ThreadStartedNotification.json` | `{thread}` — **no top-level `threadId`**; routing key is `params.thread.id` |
| `ExecCommandApprovalResponse.json` | `{"decision":"denied"}` (snake_case `ReviewDecision` family) |
| `ApplyPatchApprovalResponse.json` | `{"decision":"denied"}` |
| `CommandExecutionRequestApprovalResponse.json` | `{"decision":"decline"}` (camelCase family) |
| `FileChangeRequestApprovalResponse.json` | `{"decision":"decline"}` |
| `McpServerElicitationRequestResponse.json` | `{"action":"decline"}` — generated name confirmed **exactly** as listed |
| `DynamicToolCallResponse.json` | `{"success":false,"contentItems":[…]}` |
| `PermissionsRequestApprovalResponse.json` | requires `permissions` — **no deny shape**, so a2a answers with a JSON-RPC error |
| `ToolRequestUserInputResponse.json` | requires `answers` — no decline shape → JSON-RPC error |
| `ChatgptAuthTokensRefreshResponse.json` | requires real tokens → JSON-RPC error (lets the server self-refresh) |
| `AttestationGenerateResponse.json` | requires a token; unreachable (a2a never sets `requestAttestation:true`) → JSON-RPC error |

Extracted method sets (set-compared, not byte-diffed):

| file | source | count at capture |
|---|---|---|
| `client_request_methods.txt` | `ClientRequest.json` → `oneOf[].properties.method.enum[0]`, sorted+deduped | 98 |
| `server_notification_methods.txt` | `ServerNotification.json`, same extraction | 81 |

## The two rules

1. **Never snapshot the aggregate files.** `codex_app_server_protocol.v2.schemas.json`
   (and its non-v2 sibling) are *not* byte-stable across runs — the definition
   map ordering churns between invocations of the same binary, so a byte-diff
   would flap red for no protocol reason. They are also where the **snake_case
   `SandboxWorkspaceWrite` decoy** lives (`writable_roots`, `network_access`, …),
   which is an unrelated config type — grep-and-copy from it produces silent
   deserialization failure against the real camelCase `SandboxPolicy`.
2. **Never byte-snapshot `ClientRequest.json` / `ServerNotification.json`.**
   They are ~150 KB each and any protocol *addition* — even one entirely
   irrelevant to a2a — would permanently redden the gate. Snapshot their
   extracted **method sets** instead, which is what actually binds a2a.

## When the canary goes red

Drift is signal, not noise: read the diff, decide whether a2a's typed subset in
`src/protocol.rs` must change, update both, and re-run. Do **not** silence the
canary. If `codex` is absent from `PATH` the canary SKIPs with an `eprintln`
rather than failing — protocol drift is unverifiable, not violated, on a machine
without the binary.
