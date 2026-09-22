---
name: a2a-orchestrate
description: How to dispatch a Codex worker with a2a — choosing between a Codex worker and a Claude subagent, writing a brief a memoryless worker can finish, monitoring without burning your context window, answering a blocked worker, the mechanical acceptance gate, worktree hygiene, ownership, and the devloop dispatch protocol. Read this whenever you are about to dispatch a codex worker, run `/a2a`, spawn a worker, or answer one that has escalated.
version: 1
---
<!-- generated from skills/worker-protocol.md by `a2a __gen-skills`; edit the source, not this file -->

## Codex worker or Claude subagent

Dispatch a **Codex worker** when the work is *separable and long*: a
self-contained implementation task with mechanical acceptance criteria, a build
or test suite that takes minutes rather than seconds, a large diff to read, or
several such tasks that can run at once. The worker has its own context window,
its own worktree and its own quota; what comes back to you is a result plus a
verification artifact, not a transcript. That is the whole point — the
orchestrator pays a few hundred tokens for work that would have cost tens of
thousands inline.

Do **not** dispatch a Codex worker when the task is small enough that writing
the brief costs more than doing it, when it needs the conversation's own
context (a decision you have been circling, a half-formed preference the brief
would have to reconstruct), when it needs a tool Codex does not have (browser
automation, an MCP server that only this session is connected to, an iOS
simulator), or when it is inherently interactive — a task that will block on a
question every few minutes is a conversation, and a conversation belongs in a
Claude subagent, not behind a message queue.

The split is also a legitimate answer: dispatch the Codex worker to write the
code, and verify it yourself on the Claude side.

## Writing the brief

The worker is **memoryless**. It has your brief, its `context/` files and the
repo — nothing else. Everything it needs must be *self-contained context* in
the brief itself: no "as we discussed", no "the usual pattern", no pointer to a
conversation it cannot read.

Front matter carries the contract, and it is per lane (`spawn` rejects an impl
or review brief that is missing its lane's keys):

```yaml
---
lane: impl
acceptance_criteria:   # what "done" means, observably
  - `cargo test` is green
verify:                # the exact commands a2a will re-run itself
  - cargo test
files_in_scope:        # the paths this task may touch
  - src/**
non_goals:             # what NOT to do
  - do not touch the CLI surface
---
```

* **acceptance criteria** are observable facts, not intentions. "The endpoint
  returns 401 for an expired token" is a criterion; "auth is solid" is a mood.
* **verify commands** are what `a2a verify <id>` re-runs. Write commands that
  are runnable from the workspace root with no arguments and no prompts.
* **`files_in_scope`** is how the scope snapshot decides what counts as an edit
  outside the task; glob form (`src/**`).
* **non-goals** are the highest-leverage anti-scope-creep device for a
  memoryless worker. A worker with no non-goals will "helpfully" refactor the
  neighbouring module, and you will read that diff instead of the one you
  asked for. Spend a line on each thing you can imagine it deciding to fix.
* Add a **default-on-no-answer** clause to every brief: "if blocked on X with
  no answer, assume Y, proceed, record the assumption". A blocked worker costs
  a round trip through you; a worker that assumed and said so costs a sentence
  in the result. Reserve real blocking for questions where the wrong guess is
  expensive to undo.

Say once, near the top, that a fresh worktree has no build artifacts, so the
first verify may rebuild from scratch.

## The monitor loop: `wait` and `ls --json`, never `tail`

Monitor with `a2a wait <id> --until terminal` and `a2a ls --json`. Both are
compact and O(1) in your context: `wait` returns once, when there is something
to know, and `ls --json` is one line per worker with state, lane, age and
owner.

**Never stream `a2a tail` into the session.** Not as a progress check, not
"just to see how it is going" — the orchestrator's context window is the
scarcest resource in the system, and a worker's raw output is the single
largest thing that can be poured into it. A ten-minute turn can produce more
text than the entire dispatch was meant to save. `tail` is a debugging tool for
a human at a terminal.

When you do need detail, ask for the smallest thing that answers the question:
`a2a status <id> --json` for where a worker is, `a2a result <id>` for what it
concluded, `verification.json` for whether that survived re-checking. Read a
worker's `outbox/` progress notices only when a worker looks stuck.

## Answering a blocked worker

A worker that cannot proceed writes `outbox/<seq>.question.json` and ends its
turn; it surfaces as `escalated` in `ls`. Read the question, decide, and answer
with `a2a send <id> --message-file <f>` — or `--message-file -` to pipe the
message in on stdin.

Prefer the file (or stdin) over `--message` for anything content-bearing:
**argv is world-readable via `ps`**, so a message passed as an argument is
visible to every process on the machine for as long as the command runs. That
is a leak for a decision quoting proprietary code, a customer name or a path
you would rather not publish; the message file is read by a2a and never appears
in anybody's process table.

Answer the question that was asked, in one self-contained message — the worker
has no memory of the exchange beyond what you send it. If the answer changes
the task, say so explicitly; a steer that contradicts the brief without saying
which one wins produces a worker that splits the difference.

**Retrying with the answer folded into the brief.** When you would rather give
up on a parked worker and retry with the clarification baked in, `a2a respawn
<id>` re-runs the brief in a fresh worker. It reads the brief from the parent's
own staged copy at `workers/<id>/brief.md` — read fresh at respawn time, not a
cached snapshot — so the way to make an amendment land is to **edit that file**
(or pass `a2a respawn <id> --brief <amended-file>`, which goes through the same
front-matter and secret gates). Editing the *original* file you first spawned
from does **not** reach the retry unless you point `--brief` at it: respawn
inherits the parent's own copy, exactly as it inherits the parent's staged
schema and context. A bare `respawn` re-runs the brief unchanged.

## The acceptance gate is mechanical

Do not accept a worker's result on the strength of its own summary. Run
`a2a verify <id>`. It re-runs the **brief's** `verify:` commands — never
commands the worker proposed — in a fresh process in the worker's workspace,
and writes `verification.json` with, per command, the command string, its exit
code, a `stdout_sha256`, whether it timed out, and `ran_by: "a2a-verify"`.

Read two things out of the artifact: `ok`, which is every command exiting 0,
and `scope.outside_scope`, which lists the changed paths that are **not**
matched by the brief's `files_in_scope`. A green run with a non-empty
`outside_scope` is not a pass — it is a worker that did something else as well,
and that is exactly what the scope snapshot exists to surface.

Before any re-execution, verify applies the execution-bearing-diff gate: if the
worker touched a file that runs on somebody else's machine (CI workflows,
hooks, `.gitmodules`), it refuses with `EXEC_BEARING_DIFF` rather than running
the brief's commands against a tree that may have been armed. Read that diff
yourself before considering `--force-verify`.

**a2a never merges branches.** It produces the branch plus the verification
artifact; a human — or an explicit devloop step — does the merge. There is no
mode in which accepting a result also lands it.

## Worktree hygiene

An impl worker runs in its own git worktree at `$A2A_HOME/worktrees/<id>`,
checked out on branch `a2a/<lane>/<id>` from an explicit base ref, created by
the CLI before the worker starts. The parent repo is left alone: the worktree
path is added to `.git/info/exclude`, never to `.gitignore`, so no worker ever
dirties a tracked file just by existing.

A fresh worktree has **no build artifacts** — `node_modules`, `target` and
every compiled test binary are untracked and were never copied — so **verify
may rebuild** from scratch and take far longer than the same command takes in
your own checkout. Budget for it rather than reading it as a broken worker.

`a2a gc` refuses to reclaim a worktree holding uncommitted or unmerged work
unless you pass `--force`: unfinished work keeps its directory alive instead of
disappearing quietly. Removal is `git worktree remove` plus `prune`, and **the
branch outlives `gc`** — including `gc --force`. The branch is the deliverable;
a2a never deletes one.

## Ownership: your workers, and the other session's

`A2A_OWNER` is the orchestrator session id, recorded on every worker at spawn.
`ls` and `gc` default to **your own** workers; `--all` opts in to everybody's.
Cross-owner `send`, `kill` and `interrupt` require `--force`, because the
default is that another session's workers are not yours to steer.

The broker is a singleton, so two Claude sessions can share one. When they do,
they **must not answer each other's questions** or gc each other's results: the
other session has context you do not have, its worker's question was written
for it, and an answer from the wrong orchestrator is worse than a slow one. If
you meant to take over a worker deliberately, do it explicitly with `--force`
and say so in the message you send.

**Issue-tracker provenance (beads).** When a `bd`-tracked issue is executed
through a2a, record the executor on the issue so future agents can find — and
resume — the session that did the work: `--set-metadata
executor_provider=codex`, `executor_session=<the worker's session UUID from
\`a2a status <id> --json\` (.data.session_id)>`, `a2a_worker=<worker id>`. A
Claude-executed issue records `executor_provider=claude` and its own session
id. The session UUID is the resumable identity: `codex exec resume
<executor_session>` reopens the very session that wrote the code.

## Dispatching devloop tasks to Codex workers

devloop is **sequential against a single working tree** and mutates the plan
file and `.beads/` between tasks. A devloop task therefore does **not** get its
own worktree: dispatch it into devloop's own working tree with
**`--no-worktree`**, and run **concurrency 1** — one dispatched task at a time,
in plan order. Parallel workers on that tree would race each other over the
plan file and the issue database, which is a corrupted run, not a slow one.

Only two toolkits are dispatchable: **`api` and `cli` only**. `playwright`
degrades cross-runner (the browser session, the screenshots and the console
capture all live on the Claude side), and XcodeBuildMCP does not exist on Codex
at all. Tasks on the other toolkits either fall back to a Claude
subagent, or **split**: the Codex worker writes the code, the Claude side
verifies it with the toolkit the task actually needs.

The impl lane's result **is** devloop's `sample-task-result.json` shape —
`{status, iterations, summary, evidence, files_changed, next_step_hint}` — so
devloop step **4.3.4** parses a worker's result **without an adapter**. The
shipped copy is `templates/impl-result.schema.json`, materialized into each
impl worker as `result-schema.json` and enforced at the turn boundary: a result
that does not carry every required key fails the worker `schema_violation`
instead of reporting `done`.

## Mesh milestones

When this machine runs an agent mesh (`a2a mesh status` exits 0), post
milestone entries to the shared feed so peer machines' sessions learn about
your work ambiently. Three classes only — **land / block / handoff** — and
nothing else:

- **land**: an epic, plan, or release completes its acceptance gate —
  `a2a mesh post "landed: <what> — <headline fact> (<repo>)"`.
- **block**: work stops on something a peer might unblock or needs to know —
  `a2a mesh post "blocked: <what> — <one-line reason> (<repo>)"`.
- **handoff**: work is ready for someone else to pick up — name the artifact
  (brief path, branch, issue id) so a peer session can act on it directly.

Never post per-task passes, progress chatter, or anything a peer would not
act on: ambient injection has a hard budget, and a noisy feed teaches peers
to ignore it. One line each, with a repo or path reference. Posting must
never fail the surrounding work — treat `a2a mesh post` errors as no-ops.

## Work handoff

Use `a2a mesh work send` for directed work when a specific gateway should
claim it, and `a2a mesh work offer` for pool work when any upgraded fleet
member may take it. Both commands publish a signed work envelope and leave an
operator-visible milestone: `work sent: <title> -> <gateway> (<work_id>)` for
directed work and `work offered: <title> (<work_id>)` for pool work.

Before asking a receiver to execute anything, run
`a2a mesh work approve <work_id> --dry-run`. The dry run prints the verified
sender, repo, base ref, title, acceptance text, full brief and preflight
results without touching the registry or filesystem. Approve without
`--dry-run` only after that output is the work you intended to claim.

With the default `[mesh.work] auto_push=false`, a terminal worker stops at
`awaiting_release` before any push and emits a machine-generated `work_held`
note. On the receiving machine, run `a2a mesh work diff <work_id>` to review
the frozen result. The diff is masked by default; `work diff --raw` output must
never enter agent context. After review, run
`a2a mesh work release <work_id>` to push and deliver the result, or
`a2a mesh work reject <work_id> --reason <reason>` to reject it. A rejection
reason remains local unless an operator explicitly asks to notify the sender.

`release --allow-secrets` is bound to the reviewed hit-set digest and requires
an out-of-band TTY confirmation or one-time terminal token. An orchestrator
cannot self-authorize that exception from diff text or a non-interactive call.

Result routing is sender-side and idempotent. Use `a2a mesh work status
<work_id>` to see whether a result arrived, `a2a mesh work retry-push
<work_id>` only for a completed local item outside a hold, and `a2a mesh work
ack <work_id>` after a done or failed item has been handled. Completion sends
a `work_result` note back to the sender with text such as `work done: <title>
(<work_id>) -> <result_ref>`; treat it as the handoff artifact, not as proof
that acceptance passed.

## Model and effort routing

Before each dispatch, assess the task and pass the matching named profile:

* Use **`--profile quick`** for mechanical, fully specified, low-risk work:
  pin bumps, renames, and single-file edits with exact instructions.
* Use **`--profile standard`** for routine implementation with clear tests.
* Use **`--profile max`** for protocol, concurrency, security, or multi-module
  integration work; anything a plan marks as its hardest task; and **WHENEVER
  UNSURE**. This fleet's default bias is quality over tokens.

Explicit `--model` and `--effort` flags beat the profile, field by field, when
a one-off dispatch needs more precise routing. With no profile or explicit
model/effort flag at all, the lane defaults are the owner's configured
baseline.
