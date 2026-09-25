# Mesh agent commands

The Agent Command Channel sends a signed, imperative session directive from one
enrolled mesh gateway to another gateway's orchestrator. It is for requests the
orchestrator can act on during a turn, such as starting a brainstorm or checking
a build. Delivery is durable, but it happens at the target orchestrator's next
turn; a successful `send` means the directive was queued, not that it was acted
on.

Both gateways must be enrolled in the same fleet. Follow [Joining an a2a mesh
fleet](mesh-join.md) before using this channel, and use `a2a mesh fleet ls` to
find the exact sender and target gateway ids.

## Safety boundary

**This channel is a narrow, audited, twice-gated exception to the mesh rule that
peer content is data, not instructions.** A directive becomes imperative only
when the receiving operator has granted its sender command capability **and**
the receiver verifies the directive's signature against its fleet roots. A
failure at either gate is refused and audited; it is never rendered as an
ambient note.

Ordinary notes and feed entries remain data, not instructions. A grant only
means that the named gateway may ask this orchestrator to act: it does not
bypass the orchestrator's normal safety checks. In v1, the receiver also accepts
only the `session` directive class.

## Consent and grants

Command capability is receiver-owned and default-deny. Cross-gateway
directives cannot become imperative until an operator runs `grant` on the
receiving machine. Grants are stored owner-only under
`$A2A_HOME/mesh/command-grants.json` and are checked when a pending directive is
evaluated, so revocation applies to directives that have not yet been surfaced.
It cannot undo a directive already delivered to an orchestrator.

An agent's own gateway id is implicitly self-granted. It does not appear in
`list`, and `revoke` cannot remove that implicit consent. Every other sender
needs an explicit grant:

```sh
# Run these on the receiving machine.
a2a mesh command grant <sender-gateway-id>
a2a mesh command list
a2a mesh command revoke <sender-gateway-id>
```

Verify the gateway id and its owner out of band before granting it. A grant is
coarse in v1: it covers all session directives from that gateway until revoked.

## Send a directive

Prefer a file or stdin so instruction text does not appear in the process table:

```sh
a2a mesh command send \
  --to <target-gateway-id> \
  --instruction-file directive.txt

a2a mesh command send \
  --to <target-gateway-id> \
  --instruction-file -
```

`send` accepts exactly one instruction source. `--instruction <text>` is also
available for non-sensitive one-line input, and `--instruction -` reads stdin,
but inline text is visible in argv while the command runs. The sender must have
a valid local gateway key and enrollment token. a2a mints an id, signs the
directive, and places it in the existing durable mesh spool.

On the receiving machine, the next inject pass checks the signature, the grant,
the directive class, and duplicate history. Only a granted, verified, new
session directive is rendered in the distinct `AUTHENTICATED DIRECTIVE` block.
The receiver records its verdict and sends a correlated outcome note back to
the sender. In v1, an `acted` outcome means the directive was surfaced to the
orchestrator; it is not proof that the requested work completed successfully.

## Audit log

Inspect the receiver's local command audit with:

```sh
a2a mesh command log
```

The command prints `$A2A_HOME/mesh/command-audit.jsonl`. Each row identifies the
time, directive id, sender, verdict, and refusal reason when present. Keep this
log when investigating an unexpected instruction, then revoke the sender while
you verify its enrollment and key ownership.

## Deferred actions

The v1 channel cannot wake an idle orchestrator. It delivers only on the next
turn. Harness actions that require user-input or pane injection, including
`/compact` and `/model` or effort changes, are also refused rather than
approximated by a prompt. Idle-wake and harness actions are deferred to
`a2a-ota`.

## Errors and refusal reasons

The directive transport and receiver verdict codes are registered wire codes.
The last two rows are local command preflight codes and deliberately do not
cross the broker wire.

| Code | Meaning | Remedy |
|---|---|---|
| `DIRECTIVE_SCHEMA_INVALID` | The signed payload or its transport metadata is malformed or inconsistent. | Preserve the receiver audit, then issue a fresh directive with `a2a mesh command send`; investigate the sender if a fresh CLI-generated directive is also refused. |
| `DIRECTIVE_SIGNATURE_INVALID` | Direct signature verification could not bind the sender to a trusted fleet key. | Verify fleet roots and the sender's enrollment, re-enroll or rotate the sender key if needed, then send a fresh directive. |
| `DIRECTIVE_UNGRANTED` | The signature passed, but the receiving operator has not granted this sender. | Verify the sender gateway id out of band, run `grant` on the receiver if it should be trusted, then send a fresh directive. |
| `DIRECTIVE_BAD_SIGNATURE` | The receiver's delivery gate rejected the directive signature. | Treat the directive as untrusted; verify the sender enrollment and fleet roots before asking the sender to retry. |
| `DIRECTIVE_HARNESS_CLASS` | The directive requested a harness action, which v1 does not execute. | Perform the action locally or send a session-class instruction; `/compact`, `/model` or effort changes, and idle-wake await `a2a-ota`. |
| `DIRECTIVE_DUPLICATE` | This directive id has already been surfaced, so it will not act twice. | Check `a2a mesh command log` and the original outcome; issue a new directive only if new work is required. |
| `DIRECTIVE_INSTRUCTION_SOURCE` | The local `send` invocation did not select exactly one instruction source. | Pass exactly one of `--instruction-file <file>`, `--instruction-file -`, `--instruction <text>`, or `--instruction -`. |
| `GATEWAY_NOT_ENROLLED` | The local sender has no usable fleet enrollment token. | Complete the owner/joiner enrollment flow in [mesh-join.md](mesh-join.md), then retry `send`. |
