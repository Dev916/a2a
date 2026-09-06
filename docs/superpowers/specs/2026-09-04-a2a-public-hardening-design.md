# a2a mesh public hardening — design (rev 2)

The third act of the mesh roadmap ("my fleet first, **public-ready**"): harden
the fleet so membership beyond a fully-trusted pair is survivable. Anchor
decision (Mike, 2026-09-04): **signed registry rows** for registry write
authorization — no new infrastructure, tamper-evident to every reader,
composing with the root-anchored trust chain L3 established.

Rev 2 (2026-09-06) folds the adversarial panel review of rev 1: ~550 agents
across two passes, 104 confirmed findings collapsed into 28 edits. The four
structural corrections: the signed row is now a pinned, versioned *document*
rather than a field list; the reader is now a *decision procedure* with a
published verdict table rather than "verify and refuse"; the §3 gate now
reviews the object closure the push actually transmits, through pinned
tooling, on behalf of a named (usually agent) principal; and the operational
sections (§4 rotation, §5 limits, §7 rollout) are rewritten to be executable
against the real mixed-version fleet.

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
approver a real review gate before anything leaves their machine — noting
that in this fleet the reviewing principal is routinely an orchestrator
AGENT, so the gate must be safe to operate from inside an agent context;
drill root rotation for real; bound ingest abuse; prove 3-member topology.

Non-goals and **stated residual risks** (v1) — each named here so no later
section quietly claims it is covered:
- Per-member DB credentials/RLS or a hosted registry service (the natural v2
  once the fleet outgrows people who share a Slack channel).
- Raw vandalism: a hostile member can still DELETE/NULL rows. Vandalism is
  detectable (§2.3) and attributable-by-elimination in a small fleet.
- `fleet revoke` does NOT remove registry access — only §4.5's credential
  rotation does. mesh-join.md must say this too.
- Work-row integrity records: `mesh_work_items` rows stay unsigned. §2.4
  constrains what unsigned fields may do; residual risk: a member can
  un-cancel a cancelled row (sender-signed cancellation is deferred, §9).
- End-to-end feed entry signing (and with it transitive relay) is deferred;
  v1 pins feed origin to the authenticated link peer (§5).
- Cross-fleet anything; multi-tenancy.

## 2. Signed registry rows (a2a-kii)

### 2.1 Shape

`mesh_gateways` gains four columns plus a revocation column (migration
`mesh_l4_0001.sql`, additive, applied via `fleet migrate`):

```sql
ALTER TABLE mesh_gateways
  ADD COLUMN IF NOT EXISTS row_token  text,
  ADD COLUMN IF NOT EXISTS row_sig    text,
  ADD COLUMN IF NOT EXISTS signed_at  timestamptz,
  ADD COLUMN IF NOT EXISTS roots_held text,
  ADD COLUMN IF NOT EXISTS revocation text;
```

- `row_token`: the gateway's enrollment token envelope, verbatim (public
  document; puts the root-vouched pubkey binding IN the row so readers need
  no side channel).
- `row_sig`: ed25519 by the gateway key over the **signed message** below.
- `signed_at`: set by the WRITER at signing time, **truncated to
  microseconds before signing AND before storage** (Postgres timestamptz is
  microsecond; nanosecond-precision signing makes every row unverifiable
  after the round-trip).
- `roots_held`: sorted JSON array of lowercase-hex root fingerprints this
  gateway currently trusts (root.pub [+ root.pub.next]); always present,
  `[]` when none. §4's rotation gate reads it (advisory — see §4).
- `revocation`: a ROOT-signed record `{fleet_id, gateway_id, revoked_at}`
  (same envelope construction as an enrollment token), or NULL. See §2.3
  step 0 and the eviction rules below.

**The signed message** — the single authoritative definition; §8 pins it
with a byte-exact golden vector:

```
Sha256( b"a2a/registry-row/v1\0" || canonical_json{
  msg_ver: 1,
  fleet_id, gateway_id, endpoint_url, pubkey_fpr, cert_fpr, version,
  owner, created_at, schema_version,
  roots_held,               // the JSON array, as a string, sorted
  row_token_sha256,         // hex sha256 of the row_token text
  signed_at                 // RFC3339, microsecond precision
} )
```

Canonicalization is `keys::canonical_json` (the L3 implementation), named
by field: keys sorted, no whitespace, NULL columns encoded as explicit
`null` with the key PRESENT, fingerprints and version as strings. The
domain prefix carries the message version; a reader seeing an unknown
`msg_ver` reports `ROW_SIG_UNKNOWN_VERSION` and treats the row as UNSIGNED
— never BAD_SIG — so future field additions are not a mutual partition.

Signing `row_token_sha256` (not the token bytes) binds the signature to the
exact token, so swapping in an archived older token for the same key is
tamper-evident.

Excluded from the signature, each with its reason:
- `last_heartbeat` — liveness only; a forged heartbeat can make a row look
  alive but cannot change where peers dial or which keys they trust.
- `revocation` — carries its own root signature; a gateway must not be able
  to sign (or refuse to sign) its own tombstone.
- `row_sig` — the signature itself.

**Identity binding.** gateway_id is a key commitment: the verifier refuses
any row whose gateway_id key-derived suffix does not match the derivation
over claims.pubkey_b64 (the derivation is pinned in mesh-join.md; NEW
gateway ids widen the suffix to ≥16 hex chars). `fleet enroll` maintains an
owner-side ledger ({gateway_id, pubkey, issuing root fpr, issued_at, exp})
and refuses to enroll a gateway_id already bound to a different pubkey
without `--rebind` (printing both fingerprints). Readers pin first-seen
gateway_id→pubkey and flag `row_key_rebound` on change. The verifier
rejects a row whose (endpoint_url, cert_fpr) duplicates another verified
row's — earliest signed_at wins, the later row is flagged
`row_endpoint_conflict` — and the dialer asserts that the TLS handshake's
gateway identity equals the dialed row's gateway_id.

### 2.2 Writer behavior

What the publisher writes, and when:

- The re-sign path performs a **full upsert of every signed field** plus
  row_token/row_sig/signed_at. `revoked_at`-era fields and `revocation` are
  NEVER in the publisher's SET list (a member must not clear its own
  tombstone).
- Freshness is judged by reading `signed_at` and `row_sig` back FROM the
  registry, not from memory — so a cleared or rewritten signature is
  self-repaired on the next heartbeat tick. The maximum tamper-persistence
  window is therefore `row_resign_secs` (config `[mesh.gateway]`, default
  86400) — a deliberate, stated choice.
- Re-sign triggers: daemon start; ANY signed field changing (endpoint churn,
  version bump from `a2a update`, token renewal, roots_held change after
  installing root.pub.next); `signed_at` older than `row_resign_secs`. The
  publisher re-reads gw.token and the root files each tick, so renewals and
  root installs publish without a restart. `install-token` forces an
  immediate re-sign.
- `a2a mesh gateway resign` is the manual lever (also §8's live-test undo).
- The publisher verifies its OWN row each registry tick and logs
  `row_self_tamper_detected` on mismatch before repairing it.

### 2.3 Reader behavior (the actual enforcement)

**Chokepoint.** The registry handle is constructed WITH a RootSet and
fleet_id (every open_registry call site has a home in scope); `discover`
returns verified rows plus a flagged-invalid list, and bypassing the
verifier requires adding a new method — there is no unverified read path.

**Verification order** (per row):
0. `revocation` present and verifying under the local RootSet ⇒ the row is
   REVOKED: excluded from dialing and preflights, shown flagged in
   `fleet ls`, cached locally forever (a revoked gateway_id stays revoked
   even if the column is later cleared). Revocation checks also run at the
   gateway accept path and at work `approve`.
1. Parse `row_token`; verify vs local RootSet; require claims.gateway_id ==
   row.gateway_id, claims.fleet_id == row.fleet_id, AND
   fpr(claims.pubkey_b64) == row.pubkey_fpr. Temporal validity: row reads
   verify the root signature and bindings but NOT exp/nbf (the
   verify-token-no-clock entry point) — expiry is a lifecycle event, not
   tampering; revocation is the kill switch. `fleet ls`, `rotation-status`,
   `gateway status` and health all surface token_expires_at with T-30/14/7
   warnings (see §2.5).
2. Verify gateway_id's key commitment (2.1) and `row_sig` under
   claims.pubkey_b64 over the 2.1 message.
3. Replay defense (see below).
4. Verdict + visibility (see below).

**Replay defense** (replaces rev 1's "newest signed_at wins in caches"):
the daemon persists per-gateway {highest accepted signed_at, digest of
signed content} under `$A2A_HOME/mesh/row-marks/`, written by the daemon
and READ by every CLI and cold reader. A row below the mark is refused
unconditionally — `last_heartbeat` and staleness are NEVER replay inputs.
The mark is cleared only by `a2a mesh fleet forget-signed <gw>` or
re-enrollment. Reader bounds: signed_at more than SKEW_TOLERANCE_SECS
(default 300) ahead of local time ⇒ `ROW_CLOCK_AHEAD` (refused, never
cached — one future-dated row must not pin the fleet); signed_at older than
2× the row's published resign period ⇒ `STALE_ROW` (warn). `row_resign_secs`
and `registry_heartbeat_secs` are published INSIDE the signed content so
reader windows derive from the writer's own declared cadence. Rows with
NULL signed_at are never cache-ordered (latest read wins; a valid signature
always beats unsigned). If MANY rows fail only the clock check, the reader
reports local-clock suspicion — never fleet-wide forgery.

**Verdict table.** "Unsigned" is DEFINED: row_token, row_sig and signed_at
ALL NULL. Any other combination is invalid-signed. The full matrix — token
{missing | unverifiable under local RootSet | valid} × signature {ok | bad}
× require_signed_rows {false | true} — ships in mesh-join.md with a label
and usability per cell. The distinct verdicts:
- `UNSIGNED` (all three NULL): usable + warn under flag=false; refused
  under flag=true.
- `UNVERIFIED` (token verifies under NO local root): warn-only under
  false; refused under true; the log names the unknown root fpr — an
  unknown root is a rotation-in-progress signal, not forgery.
- `UNSIGNED-STALE` (bad signature from a writer whose `version` predates
  signed rows — e.g. a rollback rewrote content under an old binary):
  warn-only under false.
- `BAD_SIG` (bad signature from a signed-row-capable writer): ALWAYS
  refused, both flag states. This is the tamper alarm.
- `ROW_CLOCK_AHEAD`, `STALE_ROW`, `ROW_SIG_UNKNOWN_VERSION`, `REVOKED`,
  `TOKEN_EXPIRED` (warn label; see 2.5): as defined above.

**Trust on first signature.** Once a gateway_id has been seen validly
signed (recorded in the row-mark), later UNSIGNED rows for it are refused
regardless of the flag — stripping row_sig+row_token is not a usable
downgrade. The escape lever ships in the same release: root-only
`a2a mesh fleet clear-signature <gw>`, named in the BAD_SIG remedy and in
§7's pre-downgrade step.

**Verdict vs visibility.** The verifier returns (row, verdict); ONLY the
dialer, the work-send preflight and envelope corroboration FILTER on it.
`refresh_registry_cache`, origin retirement and `fleet ls` see every row
present in the table. A present-but-invalid row records `invalid_since`
and can NEVER trigger origin retirement — no dropped replication cursors,
no archived outbound spool from a verdict. Peer-written fields
(last_heartbeat, revocation) never drive local destruction: retirement
uses the reader's own `missing_since` on a gc-scale grace period, never
`registry_timeout_secs`. A parse or verify failure of ONE row never fails
the enclosing read (per-row degradation lives in the verifier layer). A
RootSet LOAD failure is `ROOTS_UNREADABLE` naming the LOCAL file — never
BAD_SIG on a peer row, never fatal to a supervised loop. `work send`
against an invalid addressee row returns `WORK_PEER_ROW_INVALID` — never
WORK_PEER_TOO_OLD — with a remedy that says capture the row for evidence
FIRST, do not ask the peer to restart/republish. Every flag in `fleet ls`
carries a one-line remedy; the verdict column renders FIRST so truncated
lines still show it.

**Rendering contract** (replaces "control-chars stripped"): every remote-
or worker-authored string rendered to a terminal — diff bodies, and
`fleet ls`'s endpoint_url/owner/version/cert_fpr — is escaped as \uXXXX
for ALL Cc AND Cf codepoints (bidi overrides and zero-width included),
length-clamped, rendered with no color/pager control sequences. This is
its own named function; `work_cmd::clean` (which preserves \n \r \t) is
explicitly NOT it.

**Envelope corroboration** is NEW work, not a call-site edit: it runs at
work ingest (after envelope signature verification) and at `approve`,
reads the CACHED verified rows (no live query per note), and refuses
senders whose row is absent, REVOKED, or whose signed pubkey_fpr disagrees
with the envelope key. Degrade rule, pinned: revocation state is cached
locally (root-signed records), so "registry unreachable" is never a
revocation bypass; row ABSENCE while unreachable is warn-and-proceed at
ingest but refuse-at-approve.

### 2.4 Work rows: constrained, still unsigned

Signing `mesh_work_items` stays out of scope (multi-party CAS coordination,
never authorization) — but rev 1's "only a nuisance" argument was wrong as
stated. The binding rule: **no unsigned work-row field may be a routing or
authorization input.** Named consequences:
- The result-note destination and result_ref derive from local WorkState
  and the VERIFIED envelope's sender.gateway_id (work_exec.rs result path,
  work_cmd.rs approve path) — never from row fields. result_ref is
  validated against `^a2a/work/<work_id>$` and cleaned like title.
- work_id is validated as a 26-char Crockford ULID at send AND at ingest
  (`WORK_ID_INVALID`, refused before storage); store and state paths key on
  the exact id (no lossy safe_file_part); `inbox`/`pool`/`acked` are
  reserved; approve/diff/release assert stored.envelope.work_id == the
  requested id and that it resolves in exactly one bucket.
- Residual v1 risk (stated in §1): un-cancelling a cancelled row re-arms
  approve; sender-signed cancellation is deferred (§9). approve re-checks
  the CAS row's status and refuses non-`approved`-eligible states, which
  bounds the window to deliberate row tampering.

## 3. Result gate + worker confinement (a2a-06n)

**Posture decision: `[mesh.work] auto_push` defaults to FALSE.** The human
(or supervising-agent-with-human) gate is the control; the secret scanner
is a backstop for ACCIDENTS, not the defense against an adversary who
authored the brief. (Rev 1 shipped auto_push=true, which left §1's headline
gate never running; a trusted fleet may still set true, documented as the
convenience posture.)

### 3.1 The reviewed unit: the pushed object closure, frozen at hold

- At checkout (before spawn), record `base_sha` (`git rev-parse HEAD`) in
  WorkState. At hold, record the exact commit sha and an object-closure
  digest.
- Both the review surface and the scan run over
  `git log -p --full-history <base_sha>..HEAD` — patches AND commit
  messages/authors — plus `git rev-list --objects <base_sha>..HEAD`.
  ("diff vs base_ref" is gone: for the shipped clone layout it is empty for
  exactly the committed work that ships.)
- Refuse any commit with a parent unreachable from base_sha (defeats
  `-s ours` merge smuggling) and any non-ancestor base.
- Scan BLOBS via `git cat-file`, never rendered diff text. A non-UTF-8
  added blob is an automatic hold (`WORK_BINARY_IN_RESULT`), never an
  implicit pass. An added/modified `.gitattributes` touching diff/textconv
  is a hold.
- `release` pushes the RECORDED sha by explicit refspec and refuses if
  HEAD moved since the hold.

### 3.2 The gate's tooling is inside the threat model

Every input to the gate — .git/config, .gitattributes, hooks, textconv,
the `origin` remote — is worker-writable, so `work diff` and `release` are
themselves execution and redirection vectors unless pinned. Invariant: **no
worker-writable file may influence the push target, refspec, or diff
rendering.** Every git invocation against a worker-controlled repo runs
with `--no-pager -c core.hooksPath=/dev/null -c core.attributesFile=/dev/null
-c diff.external= -c core.fsmonitor= -c protocol.ext.allow=never
--no-ext-diff --no-textconv --text`, plus GIT_CONFIG_NOSYSTEM=1,
GIT_ATTR_NOSYSTEM=1, GIT_TERMINAL_PROMPT=0; push adds `--no-verify
-c push.followTags=false --atomic`. The push target is the literal
repo.remote_url from the VERIFIED envelope, passed after `--`, never the
name `origin`. base_ref joins L3 §2.1.3 field hygiene: no control chars or
whitespace, no leading `-`, length cap, check-ref-format shape, passed
after `--`.

### 3.3 The reviewing principal, named

In this fleet the reviewer is routinely an orchestrator agent, and the
diff body is attacker-authored text entering its context. Therefore:
- `work diff` renders worker text inside a clearly fenced UNTRUSTED block;
  the scanner's hit list renders from a2a's own structures, never from
  content inside the diff.
- `work diff` output is redact_egress-masked BY DEFAULT; `--raw` prints
  unmasked with a one-line warning, and mesh-work.md §security states
  --raw output must never be piped into agent context. The
  WORK_SECRET_IN_RESULT remedy points at the checkout path and --raw, not
  the plain command.
- `release --allow-secrets` is NON-DELEGABLE: it requires an interactive
  TTY confirmation, or a one-time token that `work diff` prints only to a
  terminal (unreachable from --json and non-TTY callers), bound to the
  hit-set digest `work diff` printed. A fixture diff instructing the
  reviewer to release still requires the out-of-band confirmation (§8).

### 3.4 The scanner, made real

The scan names its entrypoints — scan_text over added content,
refuse_filename over every added/renamed path, is_secret_key over
NAME=value lines — and adds rules for the shapes THIS system creates:
`(postgres|postgresql|mysql|mongodb(\+srv)?|redis|amqp)://user:pass@` DSNs,
`"secret_b64"`/`"private_key"` JSON keys, `ASIA[0-9A-Z]{16}`, gw.token
envelope shapes, `*.env` filenames — and refuses any added blob whose bytes
match a file under `$A2A_HOME`. Hits carry a real `path` and post-image
line numbers (hunk offsets mapped), and never echo the matched value.
Scanning streams under spawn_blocking with `[mesh.work]
diff_scan_max_bytes`; oversize is a HOLD, never a pass. Documented misses
(encodings, splits, entropy-only) are asserted as misses in §8's corpus —
the scanner is a backstop, per the posture decision above.

### 3.5 The other channels (the premise, fixed)

Rev 1's "exfiltration must transit the push" was false. Covered now:
- `work diff` also renders the exact note payload release will transmit
  (status, result_ref, summary, evidence_excerpt), and the scan runs over
  summary and evidence_excerpt too.
- The hold note carries ONLY `{work_id, status: held, hit_count}` — never
  worker-authored text, matched content, or filenames.
- `reject --reason` is LOCAL by default (the sender authored the brief);
  the sender receives a fixed status, and free text rides only behind
  `--notify-sender`, whose help says "do not describe what you found".
- Mesh work spawns pin `network=false` regardless of lanes.impl.network
  via a spawn-path override; a unit test asserts build_argv for a work
  spawn always emits network_access=false; the approve banner and
  --dry-run print the resolved sandbox posture.

### 3.6 Hold lifecycle: survival contract and exits

**Survival.** `awaiting_release` gets its own registry status, EXCLUDED
from the abandoned-claims sweep, with a separate long `release_stall`
horizon (default 7 days) — an updated_at refresh cannot protect an item
across a stopped daemon, and a reviewer taking 15 minutes must not have
their own gateway fail the item. `release` re-checks the row BEFORE the
push and refuses if the item is no longer held; a swept/expired hold is
re-openable. diff/release/reject act on locally stored, already-approved
state and MUST NOT re-run envelope expiry or sender-token checks — the
approve-time verification is recorded (base_ref and remote_url are copied
into state.json at approve). Readers (Work loop, gc, sweeps) skip-and-warn
on an unparseable state.json, naming the work_id — never abort the sweep.

**Exits.** `Rejected` is a first-class WorkResultStatus/WorkStatus — not
`failed` (whose remedy says "re-send a fresh work_id"). The sender-visible
status is fixed text; result_ref stays NULL. The hold announcement is its
own note kind (`work_held`) with its OWN minted note_id — never
result_note_id (reusing it poisons the dedupe and can permanently swallow
the real result) — carved out of record_result_delivery_ack, emitted on
EVERY entry to awaiting_release, addressed to the RECEIVER's operator
surface first, naming diff/release/reject. It carries `held_since` so the
sender's `work status` prints "running (held on gw-X since …)".
`retry-push` refuses a held item, remedy naming release/reject. gc gains a
leg for rejected/failed checkouts on `checkout_retain_days`, reporting
anything it keeps.

## 4. Root rotation: staged, signed, gated on proof

### 4.1 Command family

`rotate-root` splits (today's single command re-signs the local token at
stage time — keys.rs — which partitions the root machine before the new
root can be distributed; that is gone):
- `--stage`: mints root.key.next/root.pub.next and the **signed rotation
  announcement** ONLY. Does NOT touch the local token. Refuses when a next
  root exists without `--force`; prints the staged fpr.
- `--status`: staged fpr, per-member issuer state (4.3), phase.
- `--abort`: removes the staged pair, re-mints the local token under the
  current root.
- `--promote`: cuts over (gated, 4.3). `--retire`: retires the old root
  (gated on phase 2).
- `fleet enroll --sign-with next`: mints member tokens under the staged
  root — without this, cutover is unimplementable.

### 4.2 Distribution: authenticated, out-of-band in v1

`--stage` emits a rotation announcement signed by the CURRENT root over
{fleet_id, next_pubkey_b64, next_fpr, issued_at} — the same construction
as an enrollment token. A bare root.pub.next file, however delivered, must
NEVER be installed. Joiners run
`a2a mesh gateway install-root-next --file <announcement>`: verifies the
announcement against the held root, refuses a differing fleet_id, prints
the fingerprint, refuses to touch root.pub, re-verifies the local gw.token
after writing, and instructs a gateway restart (the listener snapshots the
RootSet at server-loop start; only the dialer reloads per dial).
RootSet::load rejects a next root whose fleet_id differs. Distribution is
**out-of-band in v1** (the announcement file over the operators' secure
channel); a mesh-note carrier is deferred (§9) — the announcement is
self-authenticating, so a carrier is an ergonomic add, not a trust change.
mesh-join.md's "overwrite root.pub" line is corrected.

### 4.3 The gate: verified issuers, not self-reports

`rotation-status` does NOT gate on roots_held (an unfalsifiable
self-report; it remains published as advisory telemetry). The gate is the
fingerprint of the root that VERIFIED each row's row_token:
- Phase 1 (safe to re-enroll): every ledgered member's row verifies, and
  every member's roots_held includes the staged fpr (advisory readiness).
- Phase 2 (safe to retire): NO row verifies under the OLD root — every
  member has re-enrolled under next. `--retire` is gated on phase 2.
- The status diffs against the enrollment ledger (2.1) and REFUSES to show
  all-green while a ledgered member is unaccounted for — members must not
  be able to shrink the row set out of the gate.

### 4.4 Runbook (docs/mesh-rotation.md)

stage → distribute announcement (out-of-band) → all members
install-root-next + restart → **quiesce**: `work list` shows no
non-terminal items on any member (stored envelopes signed under a retired
root are permanently unapprovable — drain first) → owner re-enrolls every
member `--sign-with next`; each runs install-token (publisher republishes
per 2.2) → `rotation-status` phase 2 → promote → retire.

**Stranded member** section: symptoms (peers show your row BAD_SIG /
UNVERIFIED; handshakes fail TOKEN_INVALID), diagnosis (`rotation-status`
names your token's issuing root; repeated TOKEN_INVALID refusals surface
as `root_mismatch_suspected` in gateway status/health), fix (out-of-band
re-enroll → install-token → restart) — recovery cannot use the mesh, and
the doctor remedy for token-invalid branches to "install the fleet root
that signed this token", not re-enroll. `a2a doctor` is a §4 deliverable:
roots held with fprs, local token's issuer vs installed roots, effective
require_signed_rows, items awaiting release.

**The drill** (human + orchestrator, after the tooling lands AND §8's
hermetic rotation cases pass): rotate the live fleet root with JD,
end-to-end, runbook corrected from experience.

### 4.5 Registry-credential rotation

The shared registry credential is the one secret `fleet revoke` cannot
touch. Ship `fleet set-registry --url` (updates registry.env atomically,
verifies connectivity, prints the fleet state read under the new
credential) plus an ordered runbook: provision new credential → owner
set-registry → distribute new registry.env out-of-band → members
set-registry → revoke old credential at the provider. A second live drill
covers it.

## 5. Ingest rate limits

**Ship disabled.** All defaults are 0 (off) at release; §7 enables them
only after every member reports a version that understands the deferral
ack. Never emit RATE_LIMITED to a peer whose version predates it.

**Bucket mechanics.** Bucket state lives on the shared link-state handle
keyed by peer gateway_id — explicitly NOT per session — with a per-peer
concurrent-session cap (1 inbound + 1 outbound; surplus refused with a
distinct code, never a silent registration overwrite) and a per-peer
handshake-rate bucket. Pre-auth bounds precede the per-peer buckets: a cap
on un-handshaken inbound sessions, a handshake deadline, a
connections-per-minute bucket keyed by source address, and a health
counter — the public TCP surface is bounded even for strangers. Depth =
one minute's rate (reconnect bursts absorb). Config under `[mesh.limits]`:
`notes_per_min`, `feed_entries_per_min`, `feed_bytes_per_min`,
`work_offers_per_hour`, `wants_per_min`, `digests_per_min`.

**Over-limit is DEFERRAL, not rejection.** RATE_LIMITED gets its own ack
class (`Deferred`) — L3's Rejected is terminal and must not be overloaded.
Sender: schedule_retry with jittered backoff capped at
outbound_retry_max_secs, never archive; push_spooled_notes honours
next_attempt_after (today it re-sends every spooled note each 5s poll with
NO backoff — that is fixed BEFORE defaults are calibrated, since buckets
count wire arrivals). Feed: over-limit applies BACKPRESSURE — stop
draining that link for the window and let anti-entropy redeliver; NEVER
advance the cursor or raise compacted_before_seq on a rate-limit skip —
entries are deferred, not lost. The ack is emitted with try_send (or once
per bucket-state-change); a slow or non-reading peer never blocks another
peer's push loop.

**What is metered.** The feed bucket charges only entries reaching
append_entry (Duplicate/Compacted exempt — reconnect replays don't burn
the bucket); its key is the entry origin, notes key on the authenticated
link peer. work_offers_per_hour charges BOTH note-carried and feed-carried
offers, before verify-and-store, and the pool store is capped — pool
addressing is not a bypass. Want/Digest request frames are metered
(wants_per_min/digests_per_min) with a hard cap on origins per frame,
honoring only known origins; cursor state is per-link, not one
process-wide mutex.

**Feed origin binding (v1).** A Frame::Feed's declared origin is
peer-chosen and entries are unsigned — so until entries are signed
end-to-end by the origin gateway key (§9), the receiver REQUIRES
frame_origin == the handshaked peer id. This forfeits transitive relay in
v1, deliberately (§6's proof shifts to 3-way pool claims + docs; in a
fully-linked small fleet relay is not needed for reachability). A
fabricated or unknown origin never creates a persisted cursor or a
reorder-buffer slot; unknown origins are capped per link, and total
buffered reorder entries are capped per link.

**Calibration.** Defaults are set against measured fleet traffic and the
spec states the basis: feed_entries_per_min above the 128-entry want-reply
catch-up frame (or catch-up exempted, with a comment tying the numbers);
notes defaults set AFTER the spool backoff fix. Because per-entry ingest
currently re-scans the feed, either scan_origin_sequences is hoisted to
once per batch / seq sets live in the CursorStore, or the limit is
denominated in work with a per-peer ingest CPU budget — §8 carries an
O(1)-per-entry benchmark. Health exposes per-peer drop/defer counters.

## 6. Third member

Enroll a third machine (Mike's spare or a small VM) via mesh-join.md — now
a DELIVERABLE, not "unchanged": version floor, migrate ordering, both
posture flags, install-root-next, the verdict table, and what
UNSIGNED/BAD_SIG mean to a joiner. The exercise proves the docs, 3-way
pool claims, and gives the rotation drill a quorum. Its §7 position is
explicit: before the flag flip. Any code gaps get filed.

## 7. Rollout

1. **Migrate FIRST**: root machine runs `a2a mesh fleet migrate`
   (mesh_l4_0001.sql is additive; every pre-L4 statement names its
   columns, so old binaries are unaffected). Migrations are embedded
   (include_str!), applied names recorded, and the SQL is printed before
   apply. (Rev 1 had update-then-migrate: upgraded binaries would have run
   against the unmigrated schema and reported REGISTRY_UNREACHABLE.)
2. Ship + `a2a update` fleet-wide. SQLSTATE 42703 on mesh_gateways maps to
   typed `REGISTRY_SCHEMA_STALE` whose remedy names `fleet migrate`
   (mirroring L3's 42P01 ⇒ WORK_SCHEMA_MISSING); the exposure loop still
   heartbeats when the upsert fails. MIN_WORK_PEER_VERSION becomes the
   LITERAL floor "0.3.0" (today it is the sender's own build version,
   which would make directed work one-way every release) with a test
   asserting it is a literal. A peer's `version` refreshes only on gateway
   restart — the sequencing below accounts for that.
3. Daemon restarts publish signed rows; `fleet ls` shows signature state.
   Token-expiry precondition: no member's token inside T-14 (renew first —
   both live members' tokens currently expire within a day of each other).
4. Third member joins (§6).
5. Enable §5 limits (flip from 0) once every member's reported version
   understands Deferred.
6. Flip `[mesh.gateway] require_signed_rows=true` AND
   `[mesh.work] auto_push=false` in each member's config, restart the
   gateway, and VERIFY the effective values from the RUNNING daemon —
   both are start-time snapshots; `gateway status`/health expose them, the
   signed row publishes the effective enforcement mode, and
   `a2a mesh fleet hardening-status` reads the fleet. Config keys live in
   their named sections, section field arrays include them (unit-tested),
   and `a2a mesh …` prints config warnings instead of discarding unknown
   keys. Pre-downgrade step: `fleet clear-signature` before any deliberate
   member downgrade. `fleet init` writes require_signed_rows=true for NEW
   fleets — false is only the upgrade-transition setting.
7. Rotation drill (§4), then registry-credential drill (§4.5).

Docs deliverables ride the train: mesh-join.md (floor, ordering, flags,
verdict table), mesh-work.md (new codes in the remedy table), new
mesh-rotation.md, and the a2a-orchestrate skill's work-handoff section
regenerated with diff/release/reject/awaiting_release.

## 8. Testing tiers

- **Unit**: byte-pinned golden vector for the 2.1 message + a tamper case
  per signed field; verdict-table matrix legs (every cell); strip-to-
  unsigned refused after first-signature; replay marks (below-mark
  refusal, ROW_CLOCK_AHEAD, STALE_ROW, NULL signed_at ordering);
  gateway_id key-commitment; scanner corpus — registry.env/root.key/gw.key
  shapes as TRUE positives, documented misses asserted AS misses, benign
  fixtures clean; rendering (CR-overwrite line, U+202E line, a row whose
  endpoint_url contains ESC/CR still prints its BAD_SIG flag first);
  bucket mechanics incl. depth and Deferred semantics; hold state machine
  (kill-and-resume at awaiting_release, release after claim_stall_secs,
  after daemon restart, after ttl_days; double release; release racing
  reject); config keys present in section arrays.
- **Integration (two-home duplex)**: tampered row (endpoint swapped by the
  other writer) refused and never dialed; one invalid row among many —
  the rest of the result set unaffected, no cursor reaped; root-state
  variations (missing root.pub.next, malformed, wrong fleet_id);
  require_signed_rows false vs true; MIXED-VERSION legs — old-style
  unsigned writer + new reader (both flags), pre-migration reader
  (REGISTRY_SCHEMA_STALE path), previous-release peer for work send;
  token expired / not-yet-valid / re-keyed member rows; full work flow
  with auto_push=false through diff → release AND diff → reject; `-s
  ours` merge refused; planted pre-push hook, textconv driver, rewritten
  pushurl and followTags all inert; hold-note dedupe does not swallow the
  real result; a peer reconnecting with a 300-entry backlog eventually
  receives ALL of it (replaces rev 1's flooding test, which asserted the
  lossy behavior); spoofed feed origin refused, spoofed origin_seq does
  not advance the victim's cursor; hermetic rotation cases (stage/abort/
  promote/retire, stranded-member detection) — these GATE the live drill;
  per-entry ingest O(1) benchmark.
- **Live tier** (`A2A_MESH_L4_LIVE=1`): signed-row round-trip on Neon; a
  hand-tampered row (SQL UPDATE of endpoint_url) observed refused by a
  live reader, repaired by the writer's next tick (`gateway resign` as
  undo); replay of a captured older row refused by the mark.
- **Drills**: §4 rotation, then §4.5 credential rotation — human in the
  loop with JD, runbooks corrected from experience.

## 9. Deferred (beyond v1 hardening)

Per-member DB credentials / RLS or a registry service (removes the shared
credential's vandalism surface — the natural v2); end-to-end feed entry
signing (restores transitive relay); sender-signed cancellation for work
rows; an in-band (mesh-note) carrier for the rotation announcement;
work-row integrity records; auto-claim policy; audit log shipping.
