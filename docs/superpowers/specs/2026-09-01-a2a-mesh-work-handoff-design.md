# a2a mesh work handoff — design (rev 2)

Layer 3 of the agent mesh: moving **work**, not just context, between fleet
machines. Approved brainstorm decisions (2026-09-01): human approves each
claim; code context = shared git remote; **both** directed handoff and open
pool in v1; state home = **hybrid** (registry rows for claim/status atomicity,
mesh notes/feed for everything human-facing).

Rev 2 folds in the adversarial review panel (22 confirmed findings, 11
blockers). The headline changes versus rev 1: the trust anchor is the
root-signed enrollment token, not the registry row; envelope signatures are
domain-separated and the handshake oracle is closed; approve is local-state-
first and re-entrant; completion is a new supervised daemon loop with
idempotent, locally-driven routing; a real migration/rollout path exists.
This release is **a2a 0.3.0**.

## 1. Goals and non-goals

Goals (v1):
- `a2a mesh work send` hands a unit of work to a specific gateway;
  `a2a mesh work offer` publishes it to the fleet pool.
- The receiving human sees **root-verified** sender identity and the full
  brief before anything runs; `approve` is the only path to execution.
- Exactly-once claiming for pool work; race-free approve/cancel for directed.
- Results return as a pushed branch + a signed result note + a feed milestone,
  delivered at-least-once and deduplicated.
- Every step survives a crash at any point (local state first, resumable).

Non-goals (v1): no auto-execution policy; no repo-free work; no code over
the mesh; no cross-fleet work; no negotiation beyond offer → decision →
result.

### 1.1 Trust assumption (state it, do not hide it)

v1 assumes enrolled fleet members are **trusted operators**. A malicious
brief approved by a human can direct the worker to copy any file the
worker's sandbox can read into the checkout, and the result push carries it
to the sender's remote. Human approval + the fleet's enrollment control are
the v1 defenses. Public-hardening backlog (§12): worker filesystem read
confinement, and a result-diff review gate before push.

## 2. Work item

### 2.1 Envelope (human-facing document; travels the mesh)

```json
{
  "v": 1,
  "work_id": "<ULID>",
  "fleet_id": "fleet-...",
  "sender": {
    "gateway_id": "gw-...",
    "session": "<provider:session of the originating session>",
    "pubkey_b64": "<sender gateway ed25519 pubkey>",
    "token": "<the sender gateway's enrollment token envelope, verbatim>"
  },
  "addressing": "directed:gw-..." | "pool",
  "repo": { "remote_url": "git@github.com:org/repo.git", "base_ref": "main" },
  "title": "<one line>",
  "brief_text": "<the full brief, same contract codex workers consume>",
  "acceptance": "<criteria text>",
  "created_at": "<RFC3339>",
  "expires_at": "<RFC3339, default created_at + ttl_days>",
  "sig_b64": "<ed25519, see 2.1.1>"
}
```

#### 2.1.1 Signing and verification (the trust anchor)

- **Message**: `Sha256(b"a2a/work-envelope/v1\0" || canonical_json(all fields
  except sig_b64))`. Signing a fixed 32-byte, domain-prefixed digest means no
  other signing path in the system (the handshake signs nonces) can be coaxed
  into producing this signature. Canonical JSON = the same canonicalization
  `keys.rs` uses for token claims; `deny_unknown_fields` on parse.
- **Verification order** (all must pass; any failure ⇒ the item is refused
  at ingest, never listed as approvable):
  1. `sender.token` verifies via `keys::verify_token` against the local
     `RootSet` (root.pub + root.pub.next) — this is the ONLY root-vouched
     binding of a gateway_id to a pubkey in the system.
  2. `claims.fleet_id == fleet_id(home)`, `claims.gateway_id ==
     sender.gateway_id`, `claims.pubkey_b64 == sender.pubkey_b64`, token
     unexpired (existing skew tolerance).
  3. `sig_b64` verifies under `sender.pubkey_b64` over the 2.1.1 message.
  4. `fleet_id` field == local fleet; `expires_at` in the future;
     `addressing` matches the receiver (directed) or is `pool`.
  5. Registry corroboration (non-authorizing): the sender's `mesh_gateways`
     row must exist with `revoked_at IS NULL` (revocation is the kill
     switch); a `pubkey_fpr` mismatch is logged as a warning only — the
     registry row is peer-writable and is never an authorization input.
- **Root rotation caveat**: `rotate-root` re-signs the local token with
  `root.key.next`; peers verify only against roots they hold. `root.pub.next`
  must be distributed to all members before any sender re-signs, or their
  envelopes verify nowhere. `fleet rotate-root` output says so.

#### 2.1.2 Closing the handshake oracle (L2 change, same release)

`wire.rs` today signs the peer's raw `Challenge.nonce` string. In 0.3.0:
the receiver rejects any `Challenge` whose nonce is not exactly the shape
`nonce()` produces (44 chars, base64 of 32 bytes) before signing, and the
signed handshake message becomes `Sha256(b"a2a/hello/v1\0" || nonce_bytes)`.
This is a wire-protocol change: both ends must run 0.3.0 (see §11 rollout).

#### 2.1.3 Hygiene

- `remote_url` must parse as an `ssh://`, `git@host:path`, or `https://`
  git URL, must not begin with `-`, and is always passed to git after `--`.
- Remote-authored strings (`title`, `brief_text`, `acceptance`) are
  control-character-stripped before terminal display (no ANSI smuggling into
  the approve banner).
- Size caps (runaway backstops, enforced at send AND at ingest, measured on
  the serialized envelope): directed ≤ `max_directed_bytes` (256 KiB, rides
  a note), pool ≤ `max_pool_bytes` (32 KiB, rides a feed entry).
- The brief is validated at `send`/`offer` exactly as `spawn` validates a
  brief (`brief::parse` + lane validate + `redact::scan_text` over
  title/brief/acceptance) so a sender is refused by their own error, never
  by a peer's ingest.

### 2.2 Registry row (atomic truth; thin, no prose)

New table `mesh_work_items`, migration `mesh_l3_0001.sql`:

```sql
CREATE TABLE IF NOT EXISTS mesh_work_items (
  fleet_id        text NOT NULL,
  work_id         text NOT NULL,
  sender_gw       text NOT NULL,
  addressing      text NOT NULL,          -- 'directed:<gw>' | 'pool'
  status          text NOT NULL,          -- §3
  claimed_by      text,                   -- gateway_id set by the claim CAS
  result_ref      text,                   -- branch name once done
  result_delivery text,                   -- 'delivered' | 'rejected:<code>' | NULL
  swept_by        text,                   -- gateway that expired/abandoned it
  created_at      timestamptz NOT NULL,
  updated_at      timestamptz NOT NULL,
  expires_at      timestamptz NOT NULL,
  schema_version  integer NOT NULL,
  PRIMARY KEY (fleet_id, work_id)
);
```

Registry trait gains: `work_insert`, `work_claim` (CAS), `work_transition`
(CAS with status precondition + owner check), `work_get`, `work_list`,
`work_sweep_expired`, `work_sweep_abandoned`. Pg pins unchanged (timeouts,
VerifyFull, pooler statement cache, one statement per call — the migration
is one `CREATE TABLE` statement; no index in v1). **FileRegistry** implements
the conditional updates faithfully under an exclusive `flock` on the
registry file, so hermetic double-claim tests are real tests.

**Missing table** (fleet not yet migrated): SQLSTATE `42P01` maps to
`WORK_SCHEMA_MISSING` with remedy "the fleet root machine must run
`a2a mesh fleet migrate`" — never surfaced as `REGISTRY_UNREACHABLE`.

## 3. Lifecycle

```
offered ──approve(CAS)──▶ approved ──▶ running ──▶ done ──ack──▶ acked
   │  │                        │            │
   │  │                        └────────────┴──▶ failed ──ack──▶ acked
   │  ├─decline(CAS)──▶ declined
   ├──cancel(CAS, sender, while offered)──▶ cancelled
   ├──sweep(expires_at < now(), offered)──▶ expired
   └──(approved/running stalled, no live worker)──sweep──▶ failed
```

- **Every transition is one conditional UPDATE** with the status precondition
  and (where applicable) the owner in the WHERE clause; zero rows affected
  ⇒ the command re-reads the row and reports the actual current status
  (`WORK_ALREADY_CLAIMED` names `claimed_by`; cancelled/expired name
  themselves).
- **Claim CAS**: `... SET status='approved', claimed_by=$gw, updated_at=now()
  WHERE fleet_id=$f AND work_id=$id AND status='offered' AND (addressing='pool'
  OR addressing=$directed_to_me)`.
- **Expiry sweep uses the database clock**: `... WHERE status='offered' AND
  expires_at < now()`, no client timestamp bound, `swept_by=$gw`. Every
  gateway runs it (idempotent by the CAS); a skewed local clock cannot expire
  anyone's offers early.
- **Abandoned-claim sweep**: `approved`/`running` rows whose `updated_at` is
  older than `claim_stall_secs` and whose claimer is this gateway with no
  live worker recorded locally ⇒ `failed` with a "claim abandoned" result
  note. (Only the claiming gateway reaps its own claims; a foreign gateway
  never decides another machine's worker is dead.) The claimer refreshes
  `updated_at` on each Work-loop tick while running, so a healthy long job
  never trips it.

## 4. Flows

### 4.1 Receiver-side work store (gateway-level, not session-level)

Work items are gateway documents, not session messages. The daemon's note
ingest recognizes `kind = work_offer` addressed to `gateway:<my gateway_id>`
and, after full §2.1.1 verification, files the envelope at
`$A2A_HOME/work/inbox/<work_id>.json` (0600) and acks `delivered`; an
envelope that fails verification is acked `Rejected{code}` and logged
(`work_ingest_refused`) — it never reaches the store and is never listed.
Pool envelopes arrive as feed entries; the Work loop materializes verified
ones into `$A2A_HOME/work/pool/<work_id>.json`. Session inboxes, `inject`,
and `mesh inbox --consume` never see work envelopes (only the one-line
human text of the carrying note/entry).

### 4.2 Directed: `a2a mesh work send`

```
a2a mesh work send --to gw-host-408f2f \
  --repo git@github.com:org/repo.git --base-ref main \
  --title "..." --brief <file|-> --acceptance "..." [--ttl-days N]
```

1. Preflight: addressee row exists, not revoked, `version >= 0.3.0` (else
   `WORK_PEER_TOO_OLD` naming the recorded version, remedy "ask them to
   upgrade and **restart their gateway** — the registry version refreshes
   only on gateway restart"; fail closed on a missing version).
2. Validate the brief (§2.1.3); build + sign the envelope.
3. `work_insert` the registry row (`offered`).
4. Send a mesh **note** to the addressee gateway: `kind=work_offer`,
   `to=gateway:<gw>`, `text="work offer: <title> (<work_id>)"`,
   `refs.work=<envelope>` — existing durable spool, fixed note_id, dedup,
   transitive routing.
5. Post the feed milestone `work sent: <title> → <gw> (<work_id>)`.

### 4.3 Pool: `a2a mesh work offer`

Same flags minus `--to`. Step 1 preflights nothing peer-specific; step 4
becomes a feed entry `text="work offered: <title> (<work_id>)"`,
`refs.work=<envelope>` (≤ 32 KiB). Any 0.3.0 gateway materializes it (§4.1);
`approve` runs the same CAS — first claimer wins, losers get
`WORK_ALREADY_CLAIMED` naming the winner.

### 4.4 Approve and execute (local-state-first, re-entrant)

```
a2a mesh work approve <work_id> [--dry-run]
```

`--dry-run`: print verified sender (gateway, session, token fleet), repo,
base_ref, title, acceptance, full brief, and the preflight results below;
touch nothing.

Real run — every step is recorded in `$A2A_HOME/work/<work_id>/state.json`
**before** the side effect it precedes, under a per-item `flock`:

0. Preflight (no side effects): envelope re-verified from the store;
   `git ls-remote -- <remote_url> <base_ref>` succeeds with the approver's
   git config (else `WORK_REPO_UNREACHABLE`, remedy names the URL and
   "check credentials for this remote"); the work checkout root is admitted
   (see §7); the gateway daemon is running (else refuse: `WORK_DAEMON_DOWN`,
   remedy `a2a mesh gateway run --daemon` — completion routing lives there).
1. Write `state.json {phase:"claiming", envelope_digest, result_note_id:<ULID
   minted now>, milestone_posted:false}`.
2. Claim CAS. Zero rows: re-read; if `claimed_by == me` and status is
   `approved`/`running` ⇒ **resume from the recorded phase** (re-entrant);
   otherwise report the real status and delete the `claiming` state file.
3. `phase:"cloning"` → clone `remote_url` at `base_ref` into
   `<work>/repo` (approver's credentials; `git clone --depth 50 -- <url>`,
   then checkout `base_ref`). Failure ⇒ `phase:"failed"` locally + the Work
   loop reports it (§4.5); the claim is NOT released (a failed handoff is
   information; the sender re-sends a fresh work_id).
4. `phase:"spawning"` → write `<work>/brief.md`; `a2a spawn --lane impl
   --no-worktree --cwd <work>/repo --brief <work>/brief.md`; record
   `worker_id` in state (`phase:"running"`); transition registry to
   `running`.
5. Return `{work_id, worker_id, checkout}`. Execution and completion are the
   Work loop's job from here.

### 4.5 Completion routing — the Work loop (new, 6th supervised loop)

`LoopSlot::Work`, policy one-for-one, own `work_poll_secs` interval (15s
default). `HEALTH_SCHEMA_VERSION` bumps; `health.loops.work` appears. Rules:
- The loop body **never returns `Err`**: every per-item failure is caught,
  recorded in that item's state file and the jsonl log, and the loop
  continues (mirrors the registry-degrade pattern; registry calls report
  `registry_error`).
- Blocking work (worker `reconcile()`, `git push`) runs under
  `spawn_blocking` so a hung git cannot stall the loop.
- Per tick, for each local `state.json` in `running`: reconcile the recorded
  worker; refresh registry `updated_at` (stall guard). When the worker is
  terminal, drive the item through these **locally-recorded** phases:
  1. `pushing`: `git push -- origin HEAD:refs/heads/a2a/work/<work_id>`
     (force-with-lease against an empty ref; a pre-existing branch from a
     retried push is fast-forwarded or reported). Failure ⇒ `WORK_PUSH_FAILED`
     recorded; the item stays `pushing` and is retried each tick with
     full-jitter backoff (cap `outbound_retry_max_secs`); `a2a mesh work
     retry-push <id>` from an interactive shell (with a full login env) is
     the escape hatch when the daemon's environment lacks the credential.
  2. `reporting`: persist `{result_ref}`; CAS registry to `done`/`failed`
     (owner-checked); then spool the `work_result` note with the **fixed**
     `result_note_id` from step 1 (§4.4) — dedup at the receiver collapses
     any retry; then post the feed milestone once (`milestone_posted`
     guard). Order: push → persist → CAS → spool → milestone, each step
     idempotent and resumable from the state file, so a crash anywhere
     re-runs safely on the next tick.
  3. `reported`: terminal locally; gc eligibility clock starts here.
- Result note content: `text="work done|failed: <title> (<work_id>) →
  a2a/work/<work_id>"`, `refs.result={status, result_ref, summary,
  evidence_excerpt}` with summary/evidence passed through
  `redact::redact_egress` (secret-shaped bytes become `[REDACTED:n]`, never
  a delivery refusal), capped at `result_note_max_bytes`. A `Rejected` ack
  from the sender's ingest is recorded on the row as
  `result_delivery='rejected:<code>'` so the sender's `work status` shows
  "done on gw-X but the result note was refused" instead of silence.
- The Work loop is also where pool envelopes are materialized (§4.1) and
  where the expiry / abandoned-claim sweeps run (registry leg).

### 4.6 Decline, cancel, ack

- `decline <id> [--reason]`: CAS to `declined`, then a signed
  `work_result`-shaped note (status `declined`, reason, capped) to the sender
  + feed milestone `work declined: ...`. Same path as done/failed.
- `cancel <id>` (sender, `offered` only): CAS to `cancelled`, then a
  `work_cancelled` note to the addressee (directed) so their store updates;
  pool cancellations are learned from the registry at list/approve time.
- `ack <id>` (sender, `done|failed → acked`): CAS + local marker; closes the
  loop and starts the sender-side retention clock.

### 4.7 Sender side

`work status [<id>]` reads the registry (sender's own items) and merges
local knowledge; `work_result` notes land as one-line inbox/digest entries
with the full result in `work show <id>`.

## 5. Command surface (`a2a mesh work …`)

`send`, `offer`, `list` (addressed-to-me + open pool + mine; from the LOCAL
store first, registry status overlaid — a registry blip degrades to a
warning, never a failure), `show <id>`, `approve <id> [--dry-run]`,
`decline <id> [--reason]`, `cancel <id>`, `status [<id>]`, `ack <id>`,
`retry-push <id>`. All honor `--json` and `--home`. Every new error code
has an exit class, retryability, and a remedy (registered in `KNOWN_CODES`).

## 6. Failure rules (consolidated)

- Verification failure ⇒ refused at ingest; nothing transitions; the
  sender learns via the note's `Rejected` ack (recorded, visible in
  `status`).
- Post-claim failure (clone/spawn/push) ⇒ the claim stays with the claimer;
  outcome `failed` with the error as the result note. No release-to-offered.
- Worker budget exhaustion / kill ⇒ `failed` with evidence.
- Registry unreachable during approve ⇒ hard error before any side effect
  (state file removed); during the Work loop ⇒ `registry_error` degrade +
  retry on cadence.
- Daemon down mid-execution ⇒ worker continues under the broker; the Work
  loop resumes from state files on next start (all phases idempotent).
- Approving machine disappears for good ⇒ the row stalls in
  `approved/running` until its own daemon returns (the abandoned sweep is
  claimer-only); the sender can observe staleness in `status` and re-send.
  Deferred: a fleet-level reaper with a much longer horizon (§12).

## 7. Security

- Trust anchor = root-signed enrollment token carried in the envelope
  (§2.1.1); the registry never authorizes, only corroborates and revokes.
- Domain-separated signatures; handshake oracle closed (§2.1.2).
- **allowed_roots carve-out, done explicitly**: `$A2A_HOME/work` is created
  eagerly in `A2aHome::open` and appended as an implicit allowed root in
  BOTH admission paths (`resolve_cwd` in the CLI and the broker's
  admission), exactly like the worktrees root. Upgrade note: the broker
  must be restarted after upgrading for the new implicit root to apply
  (`approve` preflight detects a stale broker via a `CWD_NOT_ALLOWED` probe
  and says so).
- Secret handling: briefs scanned at send; envelope contents are not
  re-scanned as note text (they live in `refs`); result summaries are
  redacted on egress; ingest scans the one-line text only.
- Ingest caps (`refs` size, envelope validity) **skip-and-advance**: a
  rejected feed entry is a new `IngestDecision::Rejected` that counts,
  advances the origin cursor, and logs — no ingest-side check may error out
  of a link session.
- v1 trust assumption per §1.1.

## 8. Config

`[mesh.work]` (new `MESH_WORK_FIELDS`): `ttl_days` (7), `max_directed_bytes`
(262144), `max_pool_bytes` (32768), `result_note_max_bytes` (65536),
`work_poll_secs` (15), `claim_stall_secs` (900), `checkout_retain_days`
(30). `checkout_root` is fixed at `$A2A_HOME/work` in v1 (a configurable
root would need its own allowed_roots reconciliation — deferred).

## 9. gc

`a2a gc` gains a work leg decided from **local state only** (gc stays
registry-free and offline-capable): a `$A2A_HOME/work/<id>` directory is
reclaimable when its state is `reported`/`acked`-marked locally, the
recorded worker is terminal, and it is idle past `checkout_retain_days`;
checkouts with a dirty `git status --porcelain` or a HEAD not reachable on
the remote are **held** (reported as `kept` with a remedy). gc also gains a
**work hold**: any worker id referenced by a non-terminal work state file is
never reclaimed by the worker-dir leg.

## 10. Observability + ride-alongs (same build)

- `a2a mesh links` and a `links` block in `gateway status`: the daemon
  records live link state in `gateway-health.json` itself (peer, direction,
  established_at, last rx/tx, frame counts) — no second daemon-written
  file.
- `install-service` boot-out noise (`a2a-0bn`); crate version → **0.3.0**
  (supersedes `a2a-lmf`).
- Docs + skills deliverables: `docs/mesh-work.md` (operator guide with the
  exercise as its worked example), a `work-handoff` section (10th) in the
  generated `a2a-orchestrate` skill so orchestrator sessions know the verbs,
  and a `mesh-join.md` note on `fleet migrate`.

## 11. Migration and rollout (mandatory before the first `send`)

1. Upgrade the binary on every member (0.3.0 changes the handshake).
2. Fleet root machine: `a2a mesh fleet migrate` — new command that applies
   every migration file idempotently (`CREATE TABLE IF NOT EXISTS`) to the
   configured registry; joiners never run it (`FLEET_ALREADY_JOINED` rules
   unchanged). `fleet init` keeps applying migrations on first init.
3. Restart every gateway daemon (`a2a mesh gateway stop` — launchd/systemd
   relaunch on the new binary; or `launchctl kickstart -k`). Old daemons are
   not inert: they ack work notes `delivered` without work support.
4. Verify: `fleet ls` shows `version 0.3.0` for every member; `gateway
   status` shows the `work` loop running.
`send`/`offer` enforce step 4 for their addressee (§4.2 preflight).

## 12. Testing tiers

- Unit/hermetic: envelope sign/verify matrix (tamper, wrong key, wrong
  fleet, expired token, expired envelope, oversize, forged-by-handshake
  message shape); nonce-shape rejection in wire; CAS transitions incl.
  double-claim, decline-after-claim, cancel races, abandoned sweep; state
  machine table tests; state-file phase resumption (kill between every
  phase, resume, assert exactly-once side effects with a fake git + fake
  registry + fixed note_id); gc work leg/hold; version-gate preflight.
- In-process integration: directed and **pool** flows end-to-end across two
  hermetic homes with a flock'd FileRegistry and duplex-linked gateways
  (send → note ingested → store → list shows verified → approve → fake
  worker → Work loop pushes to a LOCAL bare repo → result note back →
  ack). Pool: two homes race the claim; exactly one wins.
- Gated live tier (`A2A_MESH_L3_LIVE=1`): CAS contention against real Neon
  with two independent registry handles (two claimants, exactly one wins);
  `fleet migrate` idempotency against the live schema.
- Exercise acceptance (human): rollout §11 on both machines; Mike `work
  send`s a real item to `gw-host-408f2f`; John `--dry-run`s then approves;
  his machine executes and pushes `a2a/work/<id>`; result note + milestone
  arrive on Mike's side; Mike `ack`s. Then the pool variant once.

## 13. Explicitly deferred (→ public-hardening spec)

Registry write authorization (rows are peer-writable today: `cert_fpr` /
`pubkey_fpr` / endpoints — needs per-row ownership proofs or per-gateway DB
roles; this spec routes around it by never authorizing from the registry);
worker filesystem read confinement + result-diff review gate; fleet-level
abandoned-claim reaper; auto-claim policy; repo-free work; configurable
checkout root; pool fairness; progress streaming; cross-fleet.
