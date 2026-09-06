# a2a mesh public hardening — design (rev 2)

The third act of the mesh roadmap ("my fleet first, **public-ready**"): harden
the fleet so membership beyond a fully-trusted pair is survivable. Anchor
decision (Mike, 2026-09-04): **signed registry rows** for registry write
authorization — no new infrastructure, tamper-evident to every reader,
composing with the root-anchored trust chain L3 established.

Rev 2 (2026-09-06) folds the adversarial panel review of rev 1 (~550 agents
across two passes, 104 confirmed findings collapsed into 28 edits) plus two
audit passes (54 + 36 issues). The structural corrections: the signed row is
a pinned, versioned *document* (stored verbatim, not reconstructed); the
reader is a *decision procedure* whose trust decisions key on READER-LOCAL
state (the row-mark), never on unverified row content; the §3 gate reviews
the object closure the push actually transmits, through pinned tooling, on
behalf of a named (usually agent) principal; and the operational sections
(§4 rotation, §5 limits, §7 rollout) are executable against the real
mixed-version fleet.

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
  un-cancel a cancelled row BEFORE the receiver has locally observed the
  cancellation (once observed, §2.4's terminal-state marker makes approve
  fail closed; sender-signed cancellation is deferred, §9).
- End-to-end feed entry signing (and with it transitive relay) is deferred;
  v1 pins feed origin to the authenticated link peer (§5).
- Cross-fleet anything; multi-tenancy.

## 2. Signed registry rows (a2a-kii)

### 2.1 Shape

`mesh_gateways` gains five columns (migration `mesh_l4_0001.sql`, additive,
applied via `fleet migrate`):

```sql
ALTER TABLE mesh_gateways
  ADD COLUMN IF NOT EXISTS row_msg    text,
  ADD COLUMN IF NOT EXISTS row_token  text,
  ADD COLUMN IF NOT EXISTS row_sig    text,
  ADD COLUMN IF NOT EXISTS signed_at  timestamptz,
  ADD COLUMN IF NOT EXISTS revocation text;
```

- `row_msg`: the exact canonical-JSON message that was signed, stored
  VERBATIM. Readers verify the signature over these stored bytes and then
  check the message's fields against the column projection (below) — the
  message is never re-canonicalized or reconstructed from columns.
- `row_token`: the gateway's enrollment token envelope, verbatim (public
  document; puts the root-vouched pubkey binding IN the row so readers need
  no side channel).
- `row_sig`: ed25519 by the gateway key over
  `Sha256(b"a2a/registry-row/v<msg_ver>\0" || row_msg)`, where `<msg_ver>`
  renders as unpadded decimal ASCII.
- `signed_at`: mirror column of the in-message field, for SQL-side
  ordering/inspection only; the signed value in row_msg is authoritative.
- `revocation`: a ROOT-signed record `{fleet_id, gateway_id, key_epoch,
  revoked_at}` (same envelope construction as an enrollment token), or
  NULL. Its ONLY producer is `a2a mesh fleet revoke <gw>`, run on the root
  machine, gated behind require_fleet_root_key exactly as `fleet migrate`
  is; it mints the record, writes this column, and (transition only)
  mirrors the legacy `revoked_at` column so pre-L4 readers still drop the
  member (`fleet enroll --rebind` clears that mirror as part of its
  root-gated write — §2.5). A revocation naming key_epoch N revokes epochs
  ≤ N. See §2.3 step 0 and §2.3's origin-retirement rules (Verdict vs
  visibility).

**The signed message (row_msg)** — the single authoritative definition; §8
pins it with a golden vector (see Testing for exactly what is pinned):

```
canonical_json{
  msg_ver: 1,
  fleet_id, gateway_id, endpoint_url, pubkey_fpr, cert_fpr, version,
  owner, created_at, schema_version,
  key_epoch,                    // integer, MUST equal the token claims'
                                //   key_epoch (§2.3 step 1)
  roots_held,                   // sorted JSON array of lowercase-hex fprs
                                //   AS A STRING, always present, "[]" when
                                //   none (kept flat for the canonicalizer)
  row_token_sha256,             // hex sha256 of the row_token text
  row_resign_secs,              // writer's effective re-sign cadence (int)
  registry_heartbeat_secs,      // writer's effective heartbeat cadence (int)
  enforce_require_signed_rows,  // bool — writer's EFFECTIVE running value
  enforce_auto_push,            // bool — writer's EFFECTIVE running value
  signed_at                     // RFC3339, microsecond precision
}
```

Canonicalization is `keys::canonical_json_fields` (the L3 implementation,
src/mesh/keys.rs), named by field: TOP-LEVEL keys sorted, no whitespace,
NULL values encoded as explicit `null` with the key PRESENT, fingerprints
and version as strings. The message is deliberately FLAT — no nested
objects (the enforcement pair is two boolean fields; roots_held is a
pre-sorted JSON-array string) — because the canonicalizer orders top-level
keys only. ALL timestamptz fields in the signed set (`signed_at`,
`created_at`) are truncated to microseconds before signing and before
storage and encoded RFC3339 with microsecond precision; the writer supplies
`created_at` explicitly on insert and NEVER updates it on conflict (it is
insert-only, and the §2.1 conflict tie-break depends on that).

**Reader-side parsing rule.** The reader parses row_msg STRICTLY (duplicate
keys ⇒ BAD_SIG), verifies the signature over the stored bytes, and binds
fields from the parsed value. Canonical FORM is a writer obligation, not a
reader check — a validly signed but non-canonically-formatted row_msg is
accepted (the signature covers the actual bytes, so there is no
ambiguity); §8 carries both cases.

**Versioning.** `msg_ver` is read by PARSING row_msg — before any signature
check. A msg_ver above the reader's maximum is `ROW_SIG_UNKNOWN_VERSION`:
the row's columns are UNVERIFIED and the row is refused by all three
verdict filters (§2.3) under BOTH flag states — "treat it as usable" would
hand an attacker a one-UPDATE downgrade. It is never BAD_SIG (not a tamper
alarm — the remedy is "upgrade this reader", shown in `fleet ls`). Forward
compatibility is a ROLLOUT property, not a trust property: a msg_ver bump
ships behind a fleet version floor exactly like §7 step 5's limits —
readers that understand v(N+1) deploy fleet-wide before any writer emits
it, so the refusal window is a deliberate, sequenced step, not a mutual
partition.

**Column binding.** After signature verification, the reader requires the
row_msg fields {fleet_id, gateway_id, endpoint_url, pubkey_fpr, cert_fpr,
version, owner, created_at, schema_version, signed_at} to EQUAL the
corresponding columns; any mismatch ⇒ BAD_SIG (the columns are the
SQL-queryable projection, the message is the truth). Fields with no column
(key_epoch, roots_held, row_token_sha256, the cadence pair, the
enforcement pair) live only in row_msg — and every row_msg-only field
renders as `unknown` for any verdict other than valid-signed:
`hardening-status` and `rotation-status` count ONLY verified rows and
print the unverified/unknown count alongside (a member must not be able to
APPEAR to enforce by writing bytes). The reader clamps a row's declared
row_resign_secs to a locally configured maximum (default 7 days) before
computing STALE_ROW, so a writer cannot declare itself never-stale.

Signing `row_token_sha256` (not the token bytes) binds the signature to the
exact token, so swapping in an archived older token for the same key is
tamper-evident.

Excluded from the signature, each with its reason:
- `last_heartbeat` — liveness only; a forged heartbeat can make a row look
  alive but cannot change where peers dial or which keys they trust.
- `revocation` — carries its own ROOT signature; a gateway must not be able
  to sign (or refuse to sign) its own tombstone.
- `revoked_at` (legacy, pre-L4) — deprecated to advisory display only:
  never signed, never a filter, never an eviction input for L4 readers
  (§2.3); written only by `fleet revoke` (set) and `fleet enroll --rebind`
  (clear) as a transition mirror for pre-L4 readers.
- `row_sig`, `row_msg`, `row_token` — the signature machinery itself
  (row_token is bound via row_token_sha256).

**Write invariant.** No write path may modify a signature-bound column
without re-signing in the same statement. Disposition of every existing
raw write against mesh_gateways: `heartbeat` stays (last_heartbeat is
excluded); `retire_self` (UPDATE endpoint_url = NULL) violates the
invariant and is replaced by a signed upsert with endpoint_url = null;
`fleet revoke`'s legacy revoked_at mirror is fine (excluded column,
root-gated command).

**Identity binding.** gateway_id is a key commitment at first enrollment:
the id's key-derived suffix is the first N lowercase-hex chars of
Sha256(the token claims' pubkey raw bytes) — N ≥ 16 for NEW gateway ids;
existing enrollments keep their shipped width and are verified against the
same derivation prefix. The verifier enforces the derivation for
key_epoch-0 tokens; for re-keyed members (token claims key_epoch > 0,
§2.5) the binding authority is the root-signed token plus the enrollment
ledger — root attestation supersedes derivation — and readers flag
`row_key_rebound`. `fleet enroll` consults BOTH the live registry row for
the gateway_id AND an owner-side ledger ({gateway_id, pubkey, key_epoch,
issuing root fpr, issued_at, exp}) and refuses a gateway_id already bound
to a different pubkey from EITHER source without `--rebind`, printing both
fingerprints. Readers pin first-seen gateway_id→pubkey in the row-mark
(§2.3) — replaced ONLY by a root-signed token with a strictly higher
key_epoch, never by `forget-signed` or `clear-signature`.

**Endpoint conflict.** The verifier flags `ROW_ENDPOINT_CONFLICT` when two
verified rows claim the same (endpoint_url, cert_fpr) — the rule is
SKIPPED when either component is NULL (NULL means unexposed/outbound-only
by design; two unexposed members are not a conflict). The incumbent keeps
the endpoint: tie-break on the signed, insert-only `created_at` (earliest
wins — never signed_at, which every re-sign rewrites and a squatter can
backdate); the later row is refused for dialing and preflights and flagged
in `fleet ls` (the flag names both rows). The dialer also asserts that the
TLS handshake's gateway identity equals the dialed row's gateway_id.

### 2.2 Writer behavior

What the publisher writes, and when:

- The re-sign path builds a fresh row_msg and performs a **full upsert of
  row_msg, row_sig, row_token, signed_at and every projected column except
  created_at** (insert-only, §2.1). `revocation` and legacy `revoked_at`
  are NEVER in the publisher's SET list (a member must not clear its own
  tombstone).
- **Self-verification each tick**: the publisher runs the FULL §2.3
  verification over its own row every registry tick and repairs (full
  re-sign upsert) on any failure, logging `row_self_tamper_detected`. The
  tamper-persistence window is therefore ONE `registry_heartbeat_secs`
  (plus readers' cache staleness) — not row_resign_secs.
  `row_resign_secs` (config `[mesh.gateway]`, default 86400) is only the
  no-change re-sign cadence: how often an untouched row gets a fresh
  signed_at, and the basis of §2.3's STALE_ROW window.
- Re-sign triggers: daemon start; ANY signed field changing (endpoint
  churn, version bump from `a2a update`, token renewal, roots_held change
  after installing root.pub.next, an enforcement flag change taking
  effect); self-verification failure; `signed_at` older than
  `row_resign_secs`. The publisher re-reads gw.token and the root files
  each tick, so renewals and root installs publish without a restart.
  `install-token` forces an immediate re-sign.
- `a2a mesh gateway resign` is the manual lever (also §8's live-test undo).

### 2.3 Reader behavior (the actual enforcement)

**Chokepoint.** The registry handle is constructed WITH a RootSet and
fleet_id (every open_registry call site has a home in scope); `discover`
returns verified rows plus a flagged-invalid list, and bypassing the
verifier requires adding a new method — there is no unverified read path.
The L4 reader's SQL drops the `revoked_at IS NULL` predicate entirely:
the revocation determination moves into the verifier as step 0 over the
root-signed `revocation` column, and legacy revoked_at becomes advisory
display only — so revoked and invalid rows are visible-and-flagged rather
than silently absent.

**Reader-local state (the row-mark).** The daemon persists ONE record per
gateway_id under `$A2A_HOME/mesh/row-marks/`, written by the daemon and
READ by every CLI and cold reader, with exactly five fields:

1. highest accepted signed_at (the replay high-water),
2. digest of the row_msg accepted at that mark,
3. the first-signature bit (has this gateway_id ever been seen validly
   signed),
4. the pinned pubkey (first seen),
5. the highest key_epoch verified (from TOKEN claims, never row_msg).

What mutates each: a root-attested re-enrollment at a strictly higher
key_epoch raises (5), replaces (4) (the pin's ONLY exception, flagged
`row_key_rebound`), clears (1)+(2), and NEVER clears (3).
`a2a mesh fleet forget-signed <gw>` clears (1)+(2) only.
`a2a mesh fleet clear-signature <gw>` clears (3) only — the pre-downgrade
lever (§7 step 6). Both are deliberate local-state commands: explicit,
loudly logged, named in the relevant remedies (they are per-reader local
state, so a root.key gate is unimplementable on member machines —
loud-and-explicit is the applied substitute). A reader with NO mark for a
gateway is in first-trust and `fleet ls` says so (an operator can tell a
cold reader from a cleared one).

The revocation cache lives beside it at `$A2A_HOME/mesh/revocations/` —
root-signed records keyed by the RECORD's own gateway_id, daemon-written,
CLI-read, never cleared except by supersession (a root-signed token with
key_epoch strictly greater than the record's).

**Verification order** (per row):
0. `revocation` present, verifying under the local RootSet, AND matching
   the carrying row (record.fleet_id == row.fleet_id AND
   record.gateway_id == row.gateway_id — checked BEFORE honoring or
   caching; a verifying-but-misplaced record is `ROW_REVOCATION_MISPLACED`,
   a tamper flag on the carrying row, never a revocation of anyone) ⇒ the
   row is REVOKED for token key_epochs ≤ the record's: excluded from
   dialing and preflights, shown flagged in `fleet ls`, cached (see
   above). A root-signed token with a HIGHER key_epoch supersedes the
   record — the §2.5 re-key path. Revocation checks also run at the
   gateway accept path and at work `approve`, against the local cache, so
   "registry unreachable" is never a revocation bypass.
1. Parse `row_msg` strictly (§2.1); check msg_ver (unknown ⇒
   ROW_SIG_UNKNOWN_VERSION, §2.1). Parse `row_token`; verify vs local
   RootSet; require claims.gateway_id == row.gateway_id, claims.fleet_id
   == row.fleet_id, fpr(claims.pubkey_b64) == row.pubkey_fpr,
   sha256(row_token) == row_msg.row_token_sha256, AND
   claims.key_epoch == row_msg.key_epoch (mismatch ⇒ BAD_SIG). EVERY epoch
   comparison in this spec — revocation supersession, superseded-key
   refusal, mark reset, row_key_rebound — reads the TOKEN CLAIMS' epoch;
   row_msg.key_epoch is only ever checked for equality against it. An
   absent key_epoch claim reads as 0 (serde default — safe because token
   signatures verify the stored bytes and are never re-serialized; epoch 0
   is exactly §2.1's derivation-checked case). Temporal validity: row
   reads verify the root signature and bindings but NOT exp/nbf (the
   verify-token-no-clock entry point) — expiry is a lifecycle event
   (§2.5), not tampering; revocation is the kill switch.
2. Verify gateway_id's key commitment (2.1), `row_sig` under
   claims.pubkey_b64 over `Sha256(prefix || row_msg)`, the column binding
   (2.1), the pinned-pubkey and epoch rules (row-mark: a token epoch below
   field 5 ⇒ `ROW_KEY_SUPERSEDED`, refused; a pubkey differing from field
   4 without a higher-epoch token ⇒ BAD_SIG).
3. Replay bounds: a row whose signed_at is below the mark's high-water is
   `ROW_REPLAYED` (refused unconditionally — last_heartbeat and staleness
   are NEVER replay inputs); a row AT the high-water whose row_msg digest
   differs from field 2 is refused as a fork (same label); signed_at more
   than SKEW_TOLERANCE_SECS (default 300) ahead of local time ⇒
   `ROW_CLOCK_AHEAD` (refused, never cached); signed_at older than 2× the
   row's declared-and-clamped row_resign_secs ⇒ `STALE_ROW` (warn). Rows
   with NULL signed_at are never cache-ordered (latest read wins; a valid
   signature always beats unsigned). If MANY rows fail only the clock
   check, the reader reports local-clock suspicion — never fleet-wide
   forgery.
4. Verdict + visibility (below).

**Verdict table.** The inputs: signature {absent | ok | bad} × token
{missing | unverifiable under local RootSet | valid} × the reader's OWN
first-signature bit {unset | set} × require_signed_rows {false | true} —
writer capability is ALWAYS derived from the local first-signature bit,
NEVER from the row's own `version` or any other unverified content (an
attacker sets columns freely on a row whose signature already fails).
"Unsigned" is DEFINED: row_msg, row_token, row_sig and signed_at ALL NULL;
any other combination is NOT unsigned — the verdicts below grade it. The
full enumerated table ships in mesh-join.md; §8 tests every cell. The
verdicts:
- `UNSIGNED` (all four NULL, first-signature bit unset): usable + warn
  under flag=false; refused under flag=true.
- `UNSIGNED-STALE` (all four NULL is impossible here — this is a bad or
  absent signature where the first-signature bit is UNSET, i.e. the reader
  has never seen this gateway signed; the honest cause is a rollback or a
  pre-L4 writer): warn-only + usable under false; refused under true.
- `UNVERIFIED` (token present but verifies under NO local root): warn-only
  under false; refused under true; the log names the unknown root fpr — an
  unknown root is a rotation-in-progress signal, not forgery.
- `ROW_SIG_INCOMPLETE` (any partial-NULL combination of the four signature
  columns): refused under BOTH flags, like BAD_SIG — unreachable for a
  compliant writer, since §2.2's upsert always sets all four together.
- `BAD_SIG` (first-signature bit SET and the signature fails; or
  column-binding mismatch, token-digest mismatch, key_epoch mismatch,
  duplicate-key row_msg, pubkey-pin violation): ALWAYS refused, both flag
  states. This is the tamper alarm.
- `ROW_REPLAYED`, `ROW_CLOCK_AHEAD`, `ROW_KEY_SUPERSEDED`,
  `ROW_SIG_UNKNOWN_VERSION`, `REVOKED`, `ROW_REVOCATION_MISPLACED`:
  refused under both flags, per steps 0-3. `STALE_ROW`,
  `ROW_ENDPOINT_CONFLICT` (later row refused for dial/preflight),
  `row_key_rebound`, `TOKEN_EXPIRED` / `TOKEN_NOT_YET_VALID` (§2.5):
  warn/display labels as defined at their sections.

**Trust on first signature.** Once the first-signature bit is set for a
gateway_id, UNSIGNED and UNSIGNED-STALE rows for it are refused regardless
of the flag — stripping or downgrading the signature columns is not a
usable path (ROW_SIG_UNKNOWN_VERSION is already refused independently, so
no verdict escapes the bit). The escape lever ships in the same release:
`clear-signature` as above, named in the BAD_SIG remedy and §7 step 6.

**Verdict vs visibility.** The verifier returns (row, verdict); ONLY the
dialer, the work-send preflight and envelope corroboration FILTER on it.
`refresh_registry_cache`, origin retirement and `fleet ls` see every row
present in the table. A present-but-invalid row records `invalid_since`
and can NEVER trigger origin retirement — no dropped replication cursors,
no archived outbound spool from a verdict. Unsigned peer-written fields
(last_heartbeat, legacy revoked_at) never drive local destruction; a
root-VERIFIED, correctly-bound `revocation` is the one peer-carried record
that does gate behavior (dial, preflight, accept, approve). Retirement
uses the reader's own `missing_since` on a gc-scale grace period, never
`registry_timeout_secs`. A parse or verify failure of ONE row never fails
the enclosing read (per-row degradation lives in the verifier layer). A
RootSet LOAD failure is `ROOTS_UNREADABLE` naming the LOCAL file — never
BAD_SIG on a peer row, never fatal to a supervised loop. Log discipline:
one state-change log event per row per verdict transition (no spam);
repeated reads of an unchanged verdict do not re-log. `work send`
against an invalid addressee row returns `WORK_PEER_ROW_INVALID` — never
WORK_PEER_TOO_OLD — with a remedy that says capture the row for evidence
FIRST, do not ask the peer to restart/republish. Every flag in `fleet ls`
carries a one-line remedy; the verdict column renders FIRST so truncated
lines still show it.

**Rendering contract** (replaces "control-chars stripped"): every remote-
or worker-authored string rendered to a terminal — diff bodies, and
`fleet ls`'s endpoint_url/owner/version/cert_fpr — is escaped as \uXXXX
for ALL Cc AND Cf codepoints (bidi overrides and zero-width included),
length-clamped, rendered with no color/pager control sequences; added
diff lines that are non-ASCII or exceed the clamp are FLAGGED to the
reviewer with a per-line marker rendered from a2a's own structures (§3.3)
— never silently escaped or truncated out of view. This is its own named
function; `work_cmd::clean` (which preserves \n \r \t) is explicitly NOT
it.

**Envelope corroboration** is NEW work, not a call-site edit: it runs at
work ingest (after envelope signature verification) and at `approve`,
reads the CACHED verified rows (no live query per note). It refuses a
sender whose row verdict is anything other than valid-signed, whose row is
REVOKED (from the local cache), or whose row_msg.pubkey_fpr — read from a
VERIFIED row only, never from the bare column — disagrees with the
envelope key. Degrade split (applies to both registry-unreachable and
present-but-invalid): warn-and-proceed at ingest, refuse at `approve`; row
ABSENCE follows the same split. Revocation is the exception — refused at
BOTH, from the cache.

### 2.4 Work rows: constrained, still unsigned

Signing `mesh_work_items` stays out of scope (multi-party CAS coordination,
never authorization) — but rev 1's "only a nuisance" argument was wrong as
stated. The binding rule: **no unsigned work-row field may be a routing or
authorization input**; unsigned row status may be used ONLY as a
fail-closed refusal input, never to grant an action. The two call sites:
`approve` re-checks the CAS row's status and refuses
non-approve-eligible states, and `release` re-checks that the row is
still held before pushing (§3.6) — a row flipped back to a releasable
state does not by itself authorize a push; the local hold record does.
Further consequences:
- **Terminal-state marker**: when the receiver locally observes a
  cancellation (or any terminal state) for a work_id — by note or by row
  read — it persists a marker in the work store; `approve` fails closed on
  it thereafter, regardless of the row's current status. This bounds §1's
  un-cancel residual risk to flips the receiver never observed.
- The result-note destination and result_ref derive from local WorkState
  and the VERIFIED envelope's sender.gateway_id (work_exec.rs:802-855
  result path, work_cmd.rs:390-440 approve path) — never from row fields.
  result_ref is validated against `^a2a/work/<work_id>$` and cleaned like
  title.
- work_id is validated as a 26-char Crockford ULID at send AND at ingest
  (`WORK_ID_INVALID`, refused before storage); store and state paths key on
  the exact id (no lossy safe_file_part); `inbox`/`pool`/`acked` are
  reserved; approve/diff/release assert stored.envelope.work_id == the
  requested id and that it resolves in exactly one bucket.

### 2.5 Token lifecycle and re-key

- **Expiry surfacing.** `fleet ls`, `fleet rotation-status`,
  `gateway status` and health all surface token_expires_at AND a
  days-remaining figure, with T-30/14/7 warnings. Because row verification
  ignores exp/nbf (§2.3.1), an expired or not-yet-valid token attaches the
  warn labels `TOKEN_EXPIRED` / `TOKEN_NOT_YET_VALID` to the row — never
  BAD_SIG, and never refusal by themselves. Renewal: owner re-runs
  `fleet enroll` for the member (same gateway_id, same key, same epoch),
  member runs `install-token`; the publisher republishes immediately
  (§2.2).
- **Re-key.** gateway_id stays STABLE across a re-key. The token claims
  carry an integer `key_epoch` (0 at first enrollment; an absent claim in
  a pre-L4 token reads as 0). The operator sequence:
  `fleet revoke <gw>` (mints the revocation record naming the current
  epoch) → owner runs `fleet enroll <gw> --rebind [--sign-with next]` —
  `--rebind` is the same flag §2.1's dual-source refusal names: it prints
  both fingerprints, increments key_epoch to max(ledger, live row) + 1,
  records the rebind in the ledger, and clears the legacy revoked_at
  mirror in the same root-gated write (so pre-L4 readers re-admit the
  member) → member installs the new token and republishes. The
  higher-epoch token supersedes the revocation record (§2.3 step 0) and
  updates the reader row-marks per §2.3 (pin replaced, epoch raised,
  high-water cleared, first-signature bit KEPT, `row_key_rebound`
  flagged). Rows signed under a superseded key are `ROW_KEY_SUPERSEDED`,
  refused.

## 3. Result gate + worker confinement (a2a-06n)

**Posture decision: `[mesh.work] auto_push` defaults to FALSE.** The human
(or supervising-agent-with-human) gate is the control; the secret scanner
is a backstop for ACCIDENTS, not the defense against an adversary who
authored the brief. (Rev 1 shipped auto_push=true, which left §1's headline
gate never running; a trusted fleet may still set true, documented as the
convenience posture — the push-time scan below runs regardless.)

### 3.1 The reviewed unit: the pushed object closure, frozen at hold

- At checkout (before spawn), record `base_sha` (`git rev-parse HEAD`) in
  WorkState. At hold, record the exact commit sha and an object-closure
  digest.
- Both the review surface and the scan run over
  `git log -p --full-history <base_sha>..HEAD` — patches AND commit
  messages/authors — plus `git rev-list --objects <base_sha>..HEAD`.
  ("diff vs base_ref" is gone: for the shipped clone layout it is empty for
  exactly the committed work that ships.) `work diff` renders a
  --stat/--numstat summary ahead of the full patch, under the same pinned
  flags (§3.2).
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
repo.remote_url recorded in state.json at approve (copied there from the
VERIFIED envelope — §3.6), passed after `--`, never the name `origin`.
base_ref joins L3 §2.1.3 field hygiene: no control chars or whitespace, no
leading `-`, length cap, check-ref-format shape, passed after `--`.

### 3.3 The reviewing principal, named

In this fleet the reviewer is routinely an orchestrator agent, and the
diff body is attacker-authored text entering its context. Therefore:
- `work diff` renders worker text inside a clearly fenced UNTRUSTED block;
  the scanner's hit list and the §2.3 non-ASCII/over-long line markers
  render from a2a's own structures, never from content inside the diff.
- `work diff` output is redact_egress-masked BY DEFAULT; `--raw` prints
  unmasked with a one-line warning, and mesh-work.md §security states
  --raw output must never be piped into agent context. The
  WORK_SECRET_IN_RESULT remedy points at the checkout path and --raw, not
  the plain command.
- `work diff` prints the hit-set digest in both its masked and --raw
  output. `release --allow-secrets` is bound to that digest — refused if
  the current hit set's digest differs — non-delegable, and LOGGED
  (work_id, hit-set digest, confirming principal). Confirmation takes one
  of two forms: an interactive TTY confirmation, or a one-time token that
  `work diff` prints only to a terminal (unreachable from --json and
  non-TTY callers). A fixture diff instructing the reviewer to release
  still requires the out-of-band confirmation (§8 carries this leg).

### 3.4 The scanner, made real

**The scan runs before ANY push regardless of auto_push**; a hit refuses
the push with `WORK_SECRET_IN_RESULT` and puts the item into
awaiting_release even under auto_push=true. The scan names its entrypoints
— scan_text over added content, refuse_filename over every added/renamed
path, is_secret_key over NAME=value lines — and adds rules for the shapes
THIS system creates: connection-string DSNs matching
`(postgres|postgresql|mysql|mongodb(\+srv)?|redis|amqp)://[^:@/\s]+:[^@/\s]+@`
(the character classes are the rule — a literal user:pass shape matches
nothing real), `"secret_b64"`/`"private_key"` JSON keys,
`ASIA[0-9A-Z]{16}`, gw.token envelope shapes, `*.env` filenames — and
refuses any added blob whose bytes match a file under `$A2A_HOME`. Hits
carry a real `path` and post-image line numbers (hunk offsets mapped), and
never echo the matched value. Scanning streams under spawn_blocking with
`[mesh.work] diff_scan_max_bytes`; oversize is a HOLD, never a pass.
Documented misses (encodings, splits, entropy-only) are asserted as misses
in §8's corpus — the scanner is a backstop, per the posture decision.

### 3.5 The other channels (the premise, fixed)

Rev 1's "exfiltration must transit the push" was false. Covered now:
- `work diff` also renders the exact note payload release will transmit
  (status, result_ref, summary, evidence_excerpt), and the scan runs over
  summary and evidence_excerpt too.
- The hold note carries the closed field set `{work_id, status: held,
  hit_count, held_since}` — every field machine-generated locally; never
  worker-authored text, matched content, or filenames.
- `reject --reason` is LOCAL by default (the sender authored the brief);
  the sender receives fixed status text with no branch suffix (never the
  `a2a/work/<work_id>` ref the done path appends), and free text rides
  only behind `--notify-sender`, whose help says "do not describe what you
  found".
- Mesh work spawns pin `network=false` regardless of lanes.impl.network
  via a spawn-path override; a unit test asserts build_argv for a work
  spawn always emits network_access=false; the approve banner and
  --dry-run print the resolved sandbox posture.

### 3.6 Hold lifecycle: survival contract and exits

**Entry and resumption.** With auto_push=false, the Work loop halts the
completion pipeline at `awaiting_release` once the worker is terminal —
BEFORE the push. `release` resumes the pipeline in order: push (the
recorded sha, §3.1) → CAS transition → result note (§3.5 payload) →
milestone. (Under auto_push=true the only entry to awaiting_release is a
scanner hold, §3.4; release resumes identically.)

**Survival.** `awaiting_release` gets its own registry status, EXCLUDED
from the abandoned-claims sweep, with a separate long horizon
`[mesh.work] release_stall_days` (default 7) — an updated_at refresh
cannot protect an item across a stopped daemon, and a reviewer taking 15
minutes must not have their own gateway fail the item. An item past the
horizon is swept to `failed{release_stalled}`; `a2a mesh work reopen <id>`
restores it to awaiting_release from exactly that state, using the local
hold record. `release` re-checks the row BEFORE the push and refuses if
the item is no longer held (fail-closed only — §2.4). diff/release/reject
act on locally stored, already-approved state and MUST NOT re-run envelope
expiry or sender-token checks — the approve-time verification is recorded
(base_ref and remote_url are copied into state.json at approve). Readers
(Work loop, gc, sweeps) skip-and-warn on an unparseable state.json, naming
the work_id — never abort the sweep.

**Exits.** `Rejected` is a first-class WorkResultStatus/WorkStatus — not
`failed` (whose remedy says "re-send a fresh work_id"). The verb boundary:
`decline` remains the pre-approval verb, valid ONLY while `offered`, and
is refused past that with a remedy naming reject; `reject` is the
post-hold exit, reachable only from `awaiting_release`. Both appear in
mesh-work.md's remedy table. The sender-visible status is fixed text;
result_ref stays NULL. The hold announcement is its own note kind
(`work_held`) with its OWN minted note_id — never result_note_id (reusing
it poisons the dedupe and can permanently swallow the real result) —
carved out of record_result_delivery_ack, emitted on EVERY entry to
awaiting_release, addressed to the RECEIVER's operator surface first,
naming diff/release/reject; its `held_since` field (§3.5's closed set)
lets the sender's `work status` print "running (held on gw-X since …)".
`retry-push` refuses a held item, remedy naming release/reject. gc gains a
leg for rejected/failed checkouts on `checkout_retain_days`, reporting
anything it keeps.

### 3.7 Codex read confinement (investigation deliverable)

Investigate what codex `sandbox_permissions` (0.152.x line) offers for
READ scoping: if read confinement to the checkout is expressible, set it
on work-item spawns; if not, document the limitation in mesh-work.md
§security (the §3.1-3.5 gates are then the effective mitigations). The
finding goes in the implementing task's result either way.

## 4. Root rotation: staged, signed, gated on proof

### 4.1 Command family

`rotate-root` splits (today's single command re-signs the local token at
stage time — keys.rs — which partitions the root machine before the new
root can be distributed; that is gone):
- `--stage`: mints root.key.next/root.pub.next and the **signed rotation
  announcement** ONLY. Does NOT touch the local token. Refuses when a next
  root exists without `--force`; prints the staged fpr.
- `--abort`: removes the staged pair ONLY (it never touches the local
  token — there is nothing to revert, since --stage no longer re-signs);
  refuses when any ledgered member has been enrolled under the staged root
  unless `--force`.
- `--promote`: cuts over — gated on phase 2 (4.3). `--retire`: retires the
  old root — gated on phase 2 PLUS the soak predicate (4.3).
- `fleet enroll --sign-with next`: mints member tokens under the staged
  root — without this, cutover is unimplementable. `fleet enroll --rebind`
  (§2.5) composes with it.
- Status lives in ONE command: `a2a mesh fleet rotation-status` — staged
  fpr, per-member verified-issuer state, phase, soak state. (rotate-root
  has no --status flag; every section and doc names `rotation-status`.)

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

Phase computation uses ONLY verified issuer fingerprints — the fpr of the
root that VERIFIED each row's row_token. roots_held (from row_msg,
verified rows only per §2.1) renders as a separate advisory readiness
column that never blocks or unblocks a phase (it is an unfalsifiable
self-report; useful telemetry, not a gate).
- Phase 1 (safe to re-enroll): every ledgered member's row verifies under
  a held root.
- Phase 2 (safe to promote): NO row verifies under the OLD root — every
  member has re-enrolled under next. `--promote` requires phase 2.
- Soak (required for `--retire`, on top of phase 2): every ledgered member
  has completed ≥ 1 gateway handshake whose token verified under the NEW
  root since promote, AND `[mesh.gateway] rotation_soak_secs` (default
  3600) has elapsed since promote. `rotation-status` renders the soak
  state per member; `--retire --force` skips ONLY the soak predicate
  (never phase 2) and says so.
- The status diffs against the enrollment ledger (2.1) and REFUSES to show
  all-green while a ledgered member is unaccounted for — members must not
  be able to shrink the row set out of the gate.

### 4.4 Runbook (docs/mesh-rotation.md)

stage → distribute announcement (out-of-band) → all members
install-root-next + restart → `rotation-status` phase 1 → **quiesce**:
`work list` shows no non-terminal items on any member (stored envelopes
signed under a retired root are permanently unapprovable — drain first) →
owner re-enrolls every member `--sign-with next`; each runs install-token
(publisher republishes per 2.2) → `rotation-status` phase 2 → promote →
soak (rotation-status all-green) → retire.

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

**What ships enabled vs disabled.** Two enabled-at-release groups, neither
of which emits a protocol ack (so mixed-version safety is structural):
- **Pre-auth bounds**, keyed on source address, active before any
  identity exists: a cap on un-handshaken inbound sessions, a handshake
  deadline, a connections-per-minute bucket. Refusal = the socket is
  closed; a health counter records each.
- **Post-handshake session bounds**, keyed on the authenticated peer: a
  concurrent-session cap (1 inbound + 1 outbound; a surplus session is
  refused with the locally-logged code `SESSION_LIMIT` and a closed
  connection — never a silent registration overwrite; an older peer
  simply observes the close, which every shipped version already
  tolerates) and a per-peer handshake-rate bucket.

The six METERED buckets ship with **shipped default 0 (off)**; §7 step 5
flips them to their calibrated enabled values only after every member
reports a version that understands the deferral ack. Never emit
RATE_LIMITED to a peer whose version predates it.

**Bucket mechanics.** Metered-bucket state lives on the shared link-state
handle keyed by peer gateway_id — explicitly NOT per session — beginning
after handshake. Depth = one minute's rate (reconnect bursts absorb).
Config under `[mesh.limits]`: `notes_per_min`, `feed_entries_per_min`,
`feed_bytes_per_min`, `work_offers_per_hour`, `wants_per_min`,
`digests_per_min`.

**Over-limit is DEFERRAL, not rejection.** RATE_LIMITED gets its own ack
class (`Deferred`) — L3's Rejected is terminal and must not be overloaded.
Over-limit traffic is counted, with one state-change log event per bucket
transition (no spam). Sender: schedule_retry with jittered backoff capped
at outbound_retry_max_secs, never archive; push_spooled_notes honours
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
peer-chosen and entries are unsigned — so a linked member can publish
under another member's identity and, by asserting a high origin_seq,
permanently censor that member's real entries as duplicates. Until entries
are signed end-to-end by the origin gateway key (same domain-separated
construction as §2.1's signed message, verified against the pubkey in that
origin's row_token — §9), the receiver REQUIRES frame_origin == the
handshaked peer id. This forfeits transitive relay in v1, deliberately
(§6's proof shifts to 3-way pool claims + docs; in a fully-linked small
fleet relay is not needed for reachability). A fabricated or unknown
origin never creates a persisted cursor or a reorder-buffer slot; unknown
origins are capped per link, and total buffered reorder entries are capped
per link.

**Calibration.** The calibrated enabled values are set against measured
fleet traffic before §7 step 5 (deliverable: the values land in
mesh-join.md's limits table), and the spec states the basis:
feed_entries_per_min above the 128-entry want-reply catch-up frame (or
catch-up exempted, with a comment tying the numbers); notes values set
AFTER the spool backoff fix. Because per-entry ingest currently re-scans
the feed, either scan_origin_sequences is hoisted to once per batch / seq
sets live in the CursorStore, or the limit is denominated in work with a
per-peer ingest CPU budget — §8 carries an O(1)-per-entry benchmark.
Health exposes per-peer drop/defer counters.

## 6. Third member

Enroll a third machine (Mike's spare or a small VM) via mesh-join.md — now
a DELIVERABLE, not "unchanged": version floor, migrate ordering, both
posture flags, install-root-next, the verdict table, the gateway_id
derivation, and what UNSIGNED/BAD_SIG mean to a joiner. The exercise
proves the docs, 3-way pool claims, and gives the rotation drill a quorum.
Its §7 position is explicit: before the flag flip. Any code gaps get
filed.

## 7. Rollout

1. **Migrate FIRST**: the root machine installs the new build (it carries
   the embedded migrations — include_str!), then runs
   `a2a mesh fleet migrate` (mesh_l4_0001.sql is additive; every pre-L4
   statement names its columns, so old binaries are unaffected). Applied
   migration names are recorded, and the SQL is printed before apply.
   (Rev 1 had update-then-migrate: upgraded binaries would have run
   against the unmigrated schema and reported REGISTRY_UNREACHABLE.)
2. Ship + `a2a update` fleet-wide. SQLSTATE 42703 on mesh_gateways maps to
   typed `REGISTRY_SCHEMA_STALE` whose remedy names `fleet migrate`
   (mirroring L3's 42P01 ⇒ WORK_SCHEMA_MISSING); the exposure loop still
   heartbeats when the upsert fails. MIN_WORK_PEER_VERSION becomes the
   LITERAL floor "0.3.0" (today it is the sender's own build version,
   which would make directed work one-way every release) with a test
   asserting it is a literal. A peer's `version` refreshes only on gateway
   restart. **Cross-version contract for the window**: discovery and
   `fleet ls`, notes and feed ingest, pool offers, and directed work
   send + result return MUST keep working against a 0.3.0-floor peer;
   the permitted degradations are exactly: metered limits off, unsigned
   rows warn-only under flag=false. (Any future msg_ver bump rides the
   same mechanism: readers that understand it deploy fleet-wide first —
   §2.1 Versioning.)
3. Daemon restarts publish signed rows; `fleet ls` shows signature state.
   Token-expiry precondition: no member's token inside T-14 (renew first —
   both live members' tokens currently expire within a day of each other).
4. Third member joins (§6).
5. Enable §5's metered buckets (flip from 0 to the calibrated values) once
   every member's reported version understands Deferred. Pre-auth and
   session bounds have been live since step 2.
6. Flip `[mesh.gateway] require_signed_rows=true` AND keep
   `[mesh.work] auto_push=false` (the compiled-in defaults are
   require_signed_rows=false — the upgrade-transition value — and
   auto_push=false; `fleet init` writes require_signed_rows=true for NEW
   fleets) in each member's config, restart the gateway, and VERIFY the
   effective values from the RUNNING daemon — both are start-time
   snapshots; `gateway status`/health expose them, the signed row's
   enforcement fields publish them (§2.1), and
   `a2a mesh fleet hardening-status` reads the fleet (verified rows only,
   unknowns counted). Config keys live in their named sections, section
   field arrays include them (unit-tested), and `a2a mesh …` prints config
   warnings instead of discarding unknown keys. Pre-downgrade step:
   `fleet clear-signature` before any deliberate member downgrade.
7. Rotation drill (§4), then registry-credential drill (§4.5).

Docs deliverables ride the train: mesh-join.md (floor, ordering, flags,
verdict table, gateway_id derivation, limits table), mesh-work.md (new
codes in the remedy table; decline/reject boundary; §security), new
mesh-rotation.md, and the a2a-orchestrate skill's work-handoff section
regenerated with diff/release/reject/awaiting_release.

## 8. Testing tiers

- **Unit — signed rows**: golden vector pinned as a TUPLE {row_msg bytes,
  prefix bytes (msg_ver as unpadded decimal), sha256 digest hex, row_sig
  base64, signing pubkey} covering created_at, the cadence pair, both
  enforcement fields, key_epoch and roots_held; a tamper case per signed
  field; a column-binding mismatch case; a verifier-side vector with a
  validly signed NON-canonical row_msg (accepted, bound from parse) and a
  duplicate-key row_msg (BAD_SIG); verdict-table legs for EVERY enumerated
  cell including every partial-NULL combination (ROW_SIG_INCOMPLETE) and
  the version-downgraded bad-sig row (first-signature bit set + version
  column rewritten low ⇒ still BAD_SIG); strip-to-unsigned refused after
  first-signature; clear-signature clears the bit and a subsequent
  UNSIGNED row is usable under flag=false; forget-signed clears the
  high-water but NOT the bit; replay marks (below-mark ROW_REPLAYED,
  at-mark digest fork refused, ROW_CLOCK_AHEAD, STALE_ROW from the row's
  own declared-and-clamped cadence, NULL signed_at ordering, cold-reader
  first-trust shown in ls); row_msg claiming a higher key_epoch than its
  token ⇒ BAD_SIG and no revocation lift; a planted valid revocation from
  gw-A in gw-B's row ⇒ ROW_REVOCATION_MISPLACED, gw-B not revoked, cache
  untouched; gateway_id key-commitment (epoch 0), root-attested rebind
  (epoch > 0 replaces the pin; same-epoch pubkey change refused); a
  pre-L4 token with no key_epoch claim verifies as epoch 0 and is
  superseded by epoch 1; endpoint-conflict legs (two NULL-endpoint rows
  not in conflict; a backdated squatting row loses to the incumbent's
  created_at); unexpose-then-read yields a valid signed row with null
  endpoint, never BAD_SIG (the retire_self replacement).
- **Unit — gate, scanner, limits, holds**: scanner corpus —
  registry.env/root.key/gw.key shapes as TRUE positives, documented misses
  asserted AS misses, benign fixtures clean; secret-hold entry under
  auto_push=true → release --allow-secrets / reject; rendering
  (CR-overwrite line, U+202E line, a confusable non-ASCII line and an
  over-clamp line each FLAGGED, a row whose endpoint_url contains ESC/CR
  still prints its BAD_SIG flag first); metered-bucket mechanics incl.
  depth and Deferred semantics; pre-auth/session-bound legs (un-handshaken
  cap reached ⇒ close without ack, deadline expiry closes the socket,
  per-source connects/min, surplus session refused with SESSION_LIMIT and
  no registration overwrite, health counters increment); hold state
  machine (kill-and-resume at awaiting_release, release after
  claim_stall_secs, after daemon restart, after ttl_days, after
  release_stall_days via `work reopen`; double release; release racing
  reject); decline valid while offered, refused afterward with the
  reject-naming remedy; one-state-change-log-per-transition; config keys
  present in section arrays; the verifier chokepoint (every registry read
  returns (row, verdict); a read path that skips verification fails the
  build or the call-site enumeration test).
- **Integration (two-home duplex)**: tampered row (endpoint swapped by the
  other writer) refused and never dialed, repaired within one heartbeat by
  the owner's self-verification; one invalid row among many — the rest of
  the result set unaffected, no cursor reaped; root-state variations
  (missing root.pub.next, malformed, wrong fleet_id);
  require_signed_rows false vs true; MIXED-VERSION legs — old-style
  unsigned writer + new reader (both flags), pre-migration reader
  (REGISTRY_SCHEMA_STALE path), previous-release peer for work send;
  token expired / not-yet-valid (§2.5 warn labels, usable rows) /
  re-keyed member rows (higher epoch supersedes revocation, superseded
  key refused, pre-L4 reader re-admits after --rebind clears the mirror);
  full work flow with auto_push=false through diff → release AND diff →
  reject; a work row whose sender_gw is tampered — the result note still
  derives from the verified envelope; an invalid-verdict sender row —
  warn at ingest, refused at approve; two work_ids colliding under
  safe_file_part resolve in exactly one bucket, and a non-ULID work_id is
  refused WORK_ID_INVALID at send and ingest; a cancelled row flipped
  back to offered is refused at approve WHEN the local terminal-state
  marker exists (and proceeds when it does not — §1's residual risk,
  asserted as such); `-s ours` merge refused; planted pre-push hook,
  textconv driver, rewritten pushurl and followTags all inert; a fixture
  diff whose body instructs the reviewer to release with --allow-secrets
  is still refused without the TTY confirmation / digest-bound token, the
  token is unreachable from --json and non-TTY callers and refused when
  the hit-set digest differs, and the instruction text renders inside the
  UNTRUSTED fence and never appears in the scanner hit list; hold-note
  dedupe does not swallow the real result; a peer reconnecting with a
  300-entry backlog eventually receives ALL of it (replaces rev 1's
  flooding test, which asserted the lossy behavior); a stranger flooding
  un-handshaken connections is bounded by the pre-auth caps; spoofed feed
  origin refused, spoofed origin_seq does not advance the victim's
  cursor; hermetic rotation cases (stage/abort — including
  abort-refused-after-staged-enrollment/promote/retire, phase-1/phase-2
  gating, retire blocked by soak, stranded-member detection) — these GATE
  the live drill; per-entry ingest O(1) benchmark.
- **Live tier** (`A2A_MESH_L4_LIVE=1`): signed-row round-trip on Neon; a
  hand-tampered row (SQL UPDATE of endpoint_url) observed refused by a
  live reader, repaired by the writer's next tick (`gateway resign` as
  undo); replay of a captured older row refused by the mark.
- **Drills**: §4 rotation, then §4.5 credential rotation — human in the
  loop with JD, runbooks corrected from experience.

## 9. Deferred (beyond v1 hardening)

Per-member DB credentials / RLS or a registry service (removes the shared
credential's vandalism surface — the natural v2); end-to-end feed entry
signing (same domain-separated construction as §2.1, verified against the
origin's row_token — restores transitive relay); sender-signed
cancellation for work rows; an in-band (mesh-note) carrier for the
rotation announcement; work-row integrity records; auto-claim policy;
audit log shipping.
