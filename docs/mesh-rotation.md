# Mesh root rotation

Root rotation is staged and gated by the enrollment ledger plus verified
registry rows. The `roots_held_advisory` field is useful rollout telemetry, but
it is a signed self-report and never advances a phase.

## Root runbook

Follow this order. Do not promote or retire early.

1. On the fleet owner, stage the new root and save its current-root-signed
   announcement:

   ```text
   a2a mesh fleet rotate-root --stage --out rotation-announcement.txt
   ```

2. Distribute `rotation-announcement.txt` to every member through a trusted
   out-of-band channel. Never distribute a bare `root.pub.next` as a substitute.
3. On every member, verify and install the announcement, then restart its
   gateway:

   ```text
   a2a mesh gateway install-root-next --file rotation-announcement.txt
   a2a mesh gateway stop
   a2a mesh gateway run --daemon
   ```

4. Run `a2a mesh fleet rotation-status`. Continue only after it reports phase
   1 and accounts for every ledgered member.
5. **Quiesce the entire fleet.** `a2a mesh work list` must show no non-terminal
   item on any member. An envelope stored under a retired root becomes
   permanently unapprovable, so drain or terminate every item before cutover.
6. On the owner, re-enroll every member with the staged root. Deliver each new
   token out of band; each member installs it, which immediately republishes its
   signed row:

   ```text
   a2a mesh fleet enroll --gateway-id <gw> --pubkey <key> --sign-with next
   a2a mesh gateway install-token --file <token-file>
   ```

7. Run `a2a mesh fleet rotation-status` again. Continue only at phase 2, when
   every ledgered row token verifies under the next root.
8. Promote with `a2a mesh fleet rotate-root --promote`.
9. Wait for `rotation-status` to report a satisfied soak for every member and
   for the configured `rotation_soak_secs` interval to elapse.
10. Retire the previous root with `a2a mesh fleet rotate-root --retire`.

In v1, soak evidence is row-attested: the member's current signed registry row
must carry a row token verified by the promoted root, and local gateway link
health must show a link to that member established after the promote timestamp.
When the wire carries the verified token issuer in L2, this predicate upgrades
to handshake-witnessed evidence without changing the status or retire commands.
`--retire --force` skips only the soak predicate; it never skips phase 2.

## Stranded member

Symptoms:

- Peers render the member's registry row as `BAD_SIG` or `UNVERIFIED`.
- Gateway handshakes repeatedly fail with `TOKEN_INVALID` or
  `GATEWAY_TOKEN_INVALID`.

Diagnosis:

- `a2a mesh fleet rotation-status` names the root that verified each usable
  member token and lists unaccounted members.
- `a2a mesh gateway status --json` surfaces repeated token refusals under
  `health.root_mismatch_suspected`, keyed by peer with the consecutive count.
- `a2a doctor --json` lists locally held roots, the local token issuer, whether
  that issuer is installed, and the effective `require_signed_rows` setting.

Fix the member out of band: the owner re-enrolls it, an operator transfers the
new token through a trusted external channel, then the member runs
`a2a mesh gateway install-token --file <token-file>` and restarts the gateway.
Recovery cannot use the mesh that the mismatched root has already partitioned.
If the token is valid but its issuer root is absent, install the fleet root that
signed this token; do not treat re-enrollment as the first remedy.

## Registry credential rotation

The registry credential is shared access, not a member enrollment token.
Rotate it in this order:

1. Provision a new credential at the registry provider without revoking the old
   credential yet.
2. On the owner, run `a2a mesh fleet set-registry --url <SOURCE>`. The command
   verifies connectivity and fleet identity before atomically replacing
   `mesh/fleet/registry.env`.
3. Distribute the new `registry.env` out of band.
4. On every member, run `a2a mesh fleet set-registry --url <SOURCE>` and confirm
   the fleet rows printed through the new credential.
5. Revoke the old credential at the provider only after every member has moved.

A failed connectivity or fleet check leaves the previous `registry.env`
byte-identical.
