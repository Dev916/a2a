# Agent Command Channel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the ACC v1 — an authenticated, imperative directive channel that carries a "do X now" instruction to a remote agent's Claude orchestrator over the mesh, is executed at the orchestrator's next turn only when the sender is capability-granted AND signature-verified, and reports its outcome back — a narrow, audited exception to a2a's "observed content is data, never instructions" rule.

**Architecture:** A new signed **directive** message class rides the existing fleet-scoped, spooled, replicated mesh (reusing L4 signing from `row_sig`/`keys`). Receipt is gated twice — a receiver-owned **capability allowlist** and a **signature check** — before the inject path renders a directive as an **imperative block** distinct from the ambient "data, not instructions" note block. Outcomes route back as notes; every directive+verdict is audited locally. Session-class directives only; idle-wake + harness actions (compact/model) via pane injection are a named seam (a2a-ota).

**Tech Stack:** Rust (serde, ed25519 via existing `keys`/`row_sig`), the a2a mesh (`src/mesh/`), clap CLI.

**Spec:** docs/superpowers/specs/2026-09-25-a2a-agent-command-channel-design.md (the plan argues from it — executors read both).

**Dispatch:** codex
**Compatible with:** devloop skill v0.1+

## Global Constraints

- **The imperative exception is gated TWICE, always** (spec Global Constraints): a directive renders imperatively ONLY when the sender is on the receiver's capability allowlist AND its signature verifies against the fleet RootSet. Fail either → NOT imperative (refused + audited, never a silent ambient nudge). Hard invariant with a dedicated test.
- **Ambient rendering is never weakened.** Ordinary notes/feed keep the exact "data, not instructions" `inject_block` rendering (`src/mesh/notes.rs:414`). The imperative block is a SEPARATE render used only for granted+verified directives.
- **Reuse the mesh + L4 signing.** A directive is a new message class, not a new transport; sign/verify reuse `row_sig::{sign_row-style canonical bytes, verify}` + `keys::canonical_json_fields` + the gateway `SigningKey`/RootSet. Do not invent crypto or a new network path.
- **Every directive is audited** (who/when/directive_id/verdict) to `$A2A_HOME/mesh/command-audit.jsonl`.
- **Repo hygiene:** commits `--no-verify`, NEVER `cargo fmt`; clippy at the established baseline (exactly 4 pre-existing sites — count, don't assume). Split verification: codex workers CANNOT bind loopback listeners → hermetic/file-based tests are worker-run; a live-socket A→B acceptance is orchestrator/subagent-run. Workers never `git commit` (orchestrator commits), never touch `docs/superpowers/plans/**`, the spec, or `.beads/`.
- **Shared APPEND-ONLY files** (orchestrator reconciles at merge; array-length literals counted, never assumed): `KNOWN_CODES` in `src/broker/wire.rs` (currently `[&str; 77]`), the clap `MeshCmd` enum in `src/cli.rs`, the module list in `src/mesh/mod.rs`, `src/commands/mod.rs`, `src/main.rs` dispatch + cmd_name.
- **Every new error code** gets a `KNOWN_CODES` entry + a one-line remedy + a `docs/mesh-command.md` row.
- **Instruction text stays off argv** (world-readable via `ps`): `send` takes `--instruction-file` (or stdin), mirroring the note/answer discipline.
- **No idle-wake in v1.** Delivery is next-turn via the existing inject cadence. `class: "harness"` is representable but refused with a clear code (seam for a2a-ota).

## File Structure

- `src/mesh/directive.rs` — the `Directive` message, canonical bytes, sign/verify, the two-gate evaluator, verdicts, dedup.
- `src/mesh/command_grants.rs` — the capability allowlist store (grant/revoke/list/is_granted; 0600).
- `src/mesh/notes.rs` — MODIFY: hook the evaluator into the inject path; add the imperative-block render (leave `inject_block` ambient render untouched).
- `src/mesh/notes_wire.rs` (or a directive routing seam) — MODIFY: route/spool/dedup directives over the mesh alongside notes.
- `src/commands/mesh_cmd.rs` — MODIFY: `mesh command grant|revoke|list|send|log` handlers.
- `src/cli.rs`, `src/commands/mod.rs`, `src/main.rs` — MODIFY: wire the subcommands (append).
- `src/broker/wire.rs` — MODIFY: append directive `KNOWN_CODES`.
- `docs/mesh-command.md` — new operator doc.
- Tests: inline unit tests per module; `tests/mesh_command.rs` (hermetic routing + evaluator + render); a live-socket acceptance (orchestrator/subagent-run).

## Execution Map

Single sequential codex track (`acc/channel`), one worktree, each task committed by the orchestrator after its gate (focused-green from the worker → orchestrator full-suite + clippy; the live-socket acceptance is the final orchestrator/subagent step). Order: contract+signing → grant store → routing → evaluator+audit → imperative render → outcome → CLI → codes+docs → acceptance.

---

## Task 1: The Directive message + canonical bytes + signing

**Files:** Create `src/mesh/directive.rs`; Modify `src/mesh/mod.rs` (`pub mod directive;`)

**Interfaces:**
- Produces: `Directive { directive_id: String (ULID), from: String, target: String, ts: DateTime<Utc>, class: DirectiveClass (Session|Harness|Unknown), instruction: String, sig: String }`; `fn canonical_bytes(&Directive) -> Result<String>`; `fn sign(&mut Directive, &SigningKey)`; `fn verify(&Directive, &RootSet) -> Result<VerifiedSender, AppError>`.

- [ ] **Step 1: Write the failing test** — sign a directive with a test gateway key, `verify` against a RootSet containing it → Ok; flip one instruction byte → verify Err; canonical_bytes is deterministic across field order. (Mirror `tests`/patterns in `src/mesh/row_sig.rs`.)
- [ ] **Step 2: Run it, watch it fail.**
- [ ] **Step 3: Implement** reusing `row_sig`'s canonical-JSON + ed25519 sign/verify (`keys::canonical_json_fields`, the gateway `SigningKey`, RootSet verification, `gateway_id_binds_key`). `class` is `#[serde(rename_all="lowercase")]` with `#[serde(other)] Unknown`.
- [ ] **Step 4: Run it, watch it pass.**
- [ ] **Step 5: Commit** — `-m "mesh: signed directive message + verify (a2a-byd T1)"`

---

## Task 2: The capability grant store

**Files:** Create `src/mesh/command_grants.rs`; Modify `src/mesh/mod.rs`

**Interfaces:**
- Consumes: T1.
- Produces: `CommandGrants::load(home)`, `.grant(peer)`, `.revoke(peer)`, `.list()`, `.is_granted(peer, self_id)` — persisted at `$A2A_HOME/mesh/command-grants.json` (0600, owner-writable). `is_granted` returns true for the agent's own id (implicit self-grant) and for any listed peer.

- [ ] **Step 1: Failing test** — grant a peer → `is_granted` true; revoke → false; `list` reflects state; the agent's own gateway_id is always granted; the file is 0600.
- [ ] **Step 2–4:** fail → implement (atomic write via the repo's `store::write_atomic`, 0600) → pass.
- [ ] **Step 5: Commit** — `-m "mesh: capability grant allowlist for directives (a2a-byd T2)"`

---

## Task 3: Directive routing over the mesh (send + receive/spool)

**Files:** Modify `src/mesh/directive.rs`, `src/mesh/notes_wire.rs` (or the mesh routing seam)

**Interfaces:**
- Consumes: T1. Produces: a routing path that spools + sends a signed directive to `target` and dedups incoming directives by `directive_id` (reuse the note spool/dedup/replication machinery — grep `note_id` dedup, outbound spool, want/catch-up).

- [ ] **Step 1: Failing test (hermetic, no socket)** — mint+sign a directive, route it through the file-layer mesh from home A to home B; assert B has it once; re-deliver the same directive_id → still once (idempotent).
- [ ] **Step 2–4:** fail → implement, reusing the notes routing/spool/dedup (a directive is a sibling message class) → pass.
- [ ] **Step 5: Commit** — `-m "mesh: route + dedup directives over the mesh (a2a-byd T3)"`

---

## Task 4: The two-gate evaluator + audit log

**Files:** Modify `src/mesh/directive.rs`; Create the audit path (`$A2A_HOME/mesh/command-audit.jsonl`)

**Interfaces:**
- Consumes: T1–T3. Produces: `evaluate(home, grants, roots, directives) -> Vec<Verdict>` where `Verdict = Imperative(Directive) | Refused{directive_id, reason: Ungranted|BadSignature|HarnessClass|Duplicate}`; every verdict is appended to the audit log. Only `class: Session` + granted + verified → `Imperative`.

- [ ] **Step 1: Failing tests** — granted+valid Session → `Imperative`; ungranted → `Refused{Ungranted}`; bad sig → `Refused{BadSignature}`; `class: Harness` → `Refused{HarnessClass}`; a second eval of the same directive_id → `Refused{Duplicate}` (acts at most once). Each writes an audit line.
- [ ] **Step 2–4:** fail → implement the evaluator + audit append → pass.
- [ ] **Step 5: Commit** — `-m "mesh: two-gate directive evaluator + audit log (a2a-byd T4)"`

---

## Task 5: Imperative inject rendering

**Files:** Modify `src/mesh/notes.rs`

**Interfaces:**
- Consumes: T4. Produces: the inject path runs the evaluator BEFORE building the digest; `Imperative` verdicts render as a distinct **imperative block** (names the granted sender; states it is an authenticated instruction the operator consented to receive; carries the instruction); `Refused`/ambient content is unaffected. The ambient `inject_block` render (the "data, not instructions" block) is unchanged.

- [ ] **Step 1: Failing tests** — with a granted+verified directive pending, the inject digest contains the imperative block naming the sender + instruction; with an UNGRANTED directive pending, the digest contains NO imperative content anywhere; ordinary notes in the same digest still render under the "data, not instructions" comment. (This is the security invariant — assert both the presence for granted and the absence for ungranted.)
- [ ] **Step 2–4:** fail → implement (a new `format_imperative_block`, called from `inject`/`format_inject_block`; keep `inject_block` ambient render byte-for-byte as is) → pass.
- [ ] **Step 5: Commit** — `-m "mesh: imperative inject render for granted directives (a2a-byd T5)"`

---

## Task 6: Outcome reporting

**Files:** Modify `src/mesh/directive.rs` / `src/mesh/notes.rs`

**Interfaces:**
- Consumes: T3–T5. Produces: after an imperative directive is surfaced/acted, an outcome note (`{directive_id, verdict: acted|failed|refused, reason?, summary?}`) is routed back to `from` reusing the note transport; the sender correlates by directive_id.

- [ ] **Step 1: Failing test (hermetic)** — acting on a directive posts an outcome note addressed to `from` with the directive_id; a refused directive posts a `refused` outcome. Route it back A←B and assert A sees it.
- [ ] **Step 2–4:** fail → implement (mint an outcome note via the existing note post/route path) → pass.
- [ ] **Step 5: Commit** — `-m "mesh: directive outcome reporting back to the sender (a2a-byd T6)"`

---

## Task 7: CLI — `a2a mesh command grant|revoke|list|send|log`

**Files:** Modify `src/cli.rs` (append `MeshCmd::Command`), `src/commands/mesh_cmd.rs`, `src/commands/mod.rs`, `src/main.rs`

**Interfaces:**
- Consumes: T1–T6. Produces: `mesh command grant <peer>` / `revoke <peer>` / `list`; `mesh command send --to <agent> --instruction-file <f>` (mint+sign+route; instruction off argv); `mesh command log` (print the audit log).

- [ ] **Step 1: Failing test** — `a2a mesh command grant <peer>` then `list` shows it; `send --to B --instruction-file <f>` exits 0 and routes a directive (assert via the file-layer store); `log` prints an audit entry. (assert_cmd; hermetic; no socket.)
- [ ] **Step 2–4:** fail → implement + clap wiring (match the existing `MeshCmd` subcommand pattern) → pass. Missing home / not-enrolled → a clear precondition error, never a panic.
- [ ] **Step 5: Commit** — `-m "mesh: a2a mesh command grant|revoke|list|send|log CLI (a2a-byd T7)"`

---

## Task 8: Error codes + operator docs

**Files:** Modify `src/broker/wire.rs` (KNOWN_CODES), create `docs/mesh-command.md`; regenerate skills if the orchestrate skill documents directives (drift gate)

**Interfaces:** Consumes T1–T7.

- [ ] **Step 1:** Add each new directive error code to `KNOWN_CODES` (append; UPDATE the array length literal by COUNTING) with a one-line remedy; write `docs/mesh-command.md` (the grant model, the imperative-exception safety note, the CLI). If `skills/orchestrate-protocol.md` should mention directives, edit the source + regenerate (`a2a __gen-skills --out skills/generated`) so the drift gate stays green.
- [ ] **Step 2:** Run `cargo test --test skills_gen` (if skills touched) + `cargo build`.
- [ ] **Step 3: Commit** — `-m "mesh: directive error codes + mesh-command docs (a2a-byd T8)"`

---

## Task 9: Integration acceptance

**Files:** Create `tests/mesh_command.rs`

**Interfaces:** Consumes the whole channel. The hermetic parts are worker-run; the live-socket A→B is ORCHESTRATOR/subagent-run.

- [ ] **Step 1 (hermetic):** B grants A; A `send`s a directive; route it into B; assert B's inject digest renders the imperative block; assert an outcome note routes back to A. Then a directive from an UNGRANTED A → refused audit + NO imperative render.
- [ ] **Step 2 (live, orchestrator/subagent-run):** stand up two loopback-linked gateways (reuse the mesh e2e harness), grant, `send` over the live link, and assert the directive arrives + is evaluated. (Codex workers can't bind — dispatch this leg to a Claude subagent or run it as the orchestrator.)
- [ ] **Step 3: Commit** — `-m "mesh: agent command channel acceptance (a2a-byd T9)"`

## Self-Review

- **Spec coverage:** directive+signing = T1; grant model = T2; transport = T3; two-gate evaluator + audit = T4; imperative render (the exception) = T5; outcome = T6; CLI = T7; codes/docs = T8; acceptance + the security invariant = T5/T9. Deferred seams (harness class, wake) are refused-with-a-code, not built.
- **The security invariant** (imperative ONLY for granted+verified; ambient unchanged) is asserted in T4 (evaluator verdicts) AND T5 (render presence/absence) AND T9 (end-to-end) — three layers.
- **Type consistency:** `Directive`/`Verdict`/`DirectiveClass`/`CommandGrants` defined in T1–T2 and consumed by name thereafter.
- **No placeholders:** each task has a concrete failing test and a named interface; crypto + routing reuse named existing modules (`row_sig`, `keys`, the note spool).
