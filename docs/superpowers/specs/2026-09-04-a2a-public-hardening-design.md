# a2a mesh public hardening — design (rev 1)

The third act of the mesh roadmap ("my fleet first, **public-ready**"): harden
the fleet so membership beyond a fully-trusted pair is survivable. Anchor
decision (Mike, 2026-09-04): **signed registry rows** for registry write
authorization — no new infrastructure, tamper-evident to every reader,
composing with the root-anchored trust chain L3 established.

## 1. Threat model delta and goals

Today's standing assumptions (documented in the L3 spec §1.1): every enrolled
member is a trusted operator holding the shared registry DB credential. The
two known consequences, filed during the L3 review:
- **a2a-kii**: any member can rewrite any `mesh_gateways` row (endpoint_url /
  cert_fpr / pubkey_fpr) — in-fleet MITM and impersonation-shaped mischief.
- **a2a-06n**: an approved malicious work brief can copy any worker-readable
  file into the checkout; the result push exfiltrates it.

Goals (v1): make registry rows tamper-evident (not tamper-proof — the DB
credential is still shared; detection + refusal is the v1 bar); give the
approver a diff gate before anything leaves their machine; drill root
rotation for real; bound ingest abuse; prove 3-member topology.

Non-goals (v1): per-member DB credentials/RLS; a hosted registry service;
revoking the shared credential's raw DELETE/UPDATE power (a hostile member
can still vandalize rows — vandalism is detectable and attributable-by-
elimination in a small fleet, and enrollment control remains the outer wall);
cross-fleet anything; multi-tenancy.

## 2. Signed registry rows (a2a-kii)

### 2.1 Shape

`mesh_gateways` gains three columns (migration `mesh_l4_0001.sql`, one
statement, applied via `fleet migrate`):

```sql
ALTER TABLE mesh_gateways
  ADD COLUMN IF NOT EXISTS row_token text,
  ADD COLUMN IF NOT EXISTS row_sig   text,
  ADD COLUMN IF NOT EXISTS signed_at timestamptz;
```

- `row_token`: the gateway's enrollment token envelope, verbatim (public
  document; puts the root-vouched pubkey binding IN the row so readers need
  no side channel).
- `row_sig`: ed25519 by the gateway key over
  `Sha256(b"a2a/registry-row/v1\0" || canonical_json{fleet_id, gateway_id,
  endpoint_url, pubkey_fpr, cert_fpr, version, signed_at})`. Domain-separated
  like every other signature in the system.
- `signed_at`: set by the WRITER at signing time; part of the signed content.

`last_heartbeat` stays UNSIGNED and server-relevant only for staleness — a
forged heartbeat can make a row look alive but cannot change where peers
dial or which keys they trust.

### 2.2 Writer behavior

The exposure publisher signs on every UPSERT (row content changed) and
re-signs on a heartbeat only when `signed_at` is older than
`row_resign_secs` (config, default 86400) — freshness without per-heartbeat
signing cost. `fleet enroll`/`gateway identity` unchanged.

### 2.3 Reader behavior (the actual enforcement)

One shared verifier, used by every consumer of registry rows (dialer
discover, anti-entropy refresh, `fleet ls`, work send preflight, envelope
corroboration):

1. Parse `row_token`; verify vs local `RootSet`; require claims.gateway_id
   == row.gateway_id and claims.fleet_id == row.fleet_id.
2. Verify `row_sig` under claims.pubkey_b64 over the 2.1 message.
3. On failure: the row is **invalid** — excluded from dialing, peer
   discovery, and preflights; `fleet ls` shows it flagged `UNSIGNED` /
   `BAD_SIG`; one state-change log event per row (no spam).
4. Replay defense: if multiple reads of the same gateway_id disagree over
   time, newest `signed_at` wins in caches; a row whose signed_at moves
   BACKWARD is logged (`row_replay_suspected`) and the cached newer row is
   kept until it exceeds staleness.
5. Transition flag `[mesh] require_signed_rows` (default **false** at ship):
   false = verify-and-warn (unsigned rows still usable, invalid-SIGNED rows
   are always refused); true = unsigned rows are refused too. The rollout
   flips it fleet-wide after every member publishes signed rows.

### 2.4 Explicitly out of scope

Signing `mesh_work_items`: work rows are CAS coordination mutated by
multiple parties (sweeps, transitions) and are **never authorization** —
L3's envelope signatures already carry the trust. Revisit only if a
concrete attack through work-row tampering is shown (the known one —
flipping status — is a denial/vandalism nuisance, not a code-execution
path, because approve re-verifies the envelope).

## 3. Result-diff review gate + worker confinement (a2a-06n)

- New config `[mesh.work] auto_push` (default **true** — current behavior
  for trusted fleets; the hardened posture sets false). With auto_push
  false, the Work loop stops the completion pipeline at a new phase
  `awaiting_release` after the worker is terminal: `a2a mesh work diff
  <id>` shows the checkout's diff vs base_ref (stat + full, pager-friendly,
  control-chars stripped); `a2a mesh work release <id>` resumes the
  pipeline (push → CAS → note → milestone); `a2a mesh work reject <id>
  [--reason]` transitions to `failed` with the reason note. The registry
  `running` state persists throughout (the stall sweep's updated_at refresh
  continues in awaiting_release).
- **Push-time secret scan regardless of auto_push**: before any push, run
  the redact scanner over the diff-to-base; secret-shaped content ⇒ the
  push is refused with `WORK_SECRET_IN_RESULT` (remedy names the file:line
  hits and `a2a mesh work diff`), the item enters `awaiting_release` even
  under auto_push=true, and a `work_result`-shaped progress note tells the
  sender the result is held. Release requires `--allow-secrets` (explicit,
  logged) after human inspection.
- **Codex read confinement**: investigate what codex 0.152.x
  `sandbox_permissions` actually offers for read scoping; if read
  confinement to the checkout is expressible, set it on work-item spawns;
  if not, document the limitation in mesh-work.md §security (the diff gate
  + secret scan above are the effective mitigations, since exfiltration
  must transit the push). Finding goes in the implementing task's result
  either way.

## 4. Root rotation drill + visibility

- The signed row (2.1) additionally publishes `roots_held`: the fprs of
  root.pub [+ root.pub.next] this gateway currently trusts (inside the
  signed content). `a2a mesh fleet rotation-status` reads them: which
  members have the next root installed, which would strand.
- `rotate-root` output gains the checklist; docs/mesh-rotation.md runbook:
  stage next → distribute root.pub.next (mesh note `root_pub_next` kind,
  auto-installed? NO — v1 keeps installation manual per joiner, the note
  just carries the file + instructions; auto-install of trust roots over
  the channel they secure is circular) → rotation-status all-green → cut
  over → retire old.
- **The drill** (human + orchestrator, after the tooling lands): rotate the
  live fleet root with JD, verified end-to-end, runbook corrected from
  experience.

## 5. Ingest rate limits

Per-peer token buckets at gateway ingest, config under `[mesh.limits]`:
`notes_per_min` (60), `feed_entries_per_min` (120), `work_offers_per_hour`
(20), all 0-disables. Over-limit ⇒ counted, one state-change log event,
`Rejected{RATE_LIMITED}` ack for notes / skip-and-advance for feed (cursor
still advances — a flooding peer cannot wedge replication, they just lose
excess entries at this receiver). Health exposes per-peer drop counters.

## 6. Third member

Enroll a third machine (Mike's spare or a small VM) via docs/mesh-join.md
unchanged — the point is proving the docs + transitive relay + 3-way pool
claims on real infrastructure, and giving the rotation drill a quorum.
Process item, not code (any code gaps it finds get filed).

## 7. Rollout

1. Ship + `a2a update` fleet-wide (signed-row columns tolerated absent
   until migrate).
2. Root machine: `a2a mesh fleet migrate` (adds columns).
3. Daemon restarts publish signed rows; `fleet ls` shows signature state.
4. When all members show signed: flip `require_signed_rows=true` in each
   member's config (documented as the hardening switch).
5. Rotation drill (§4) once, live.

## 8. Testing tiers

- Unit: row sign/verify matrix (tamper each signed field, wrong key, token
  from another fleet, signed_at replay ordering); verifier shared-path
  tests; rate-limit buckets; diff-gate state machine (awaiting_release
  resume/reject/secret-hold paths, kill-and-resume at the new phase).
- Integration: two-home duplex — a tampered row (endpoint swapped by the
  "other" writer) is refused by the reader and the dialer never dials it;
  require_signed_rows=false vs true behavior; flooding peer loses excess
  without wedging replication; full work flow with auto_push=false through
  diff → release.
- Live tier additions (`A2A_MESH_L4_LIVE=1`): signed-row round-trip on
  Neon; a hand-tampered row (SQL UPDATE of endpoint_url) observed refused
  by a live reader.
- Drill: §4, human-in-the-loop with JD.

## 9. Deferred (beyond v1 hardening)

Per-member DB credentials / RLS or a registry service (removes the shared
credential's vandalism surface — the natural v2 if the fleet grows past
people who share a Slack channel); auto-claim policy; work-row integrity
records; audit log shipping.
