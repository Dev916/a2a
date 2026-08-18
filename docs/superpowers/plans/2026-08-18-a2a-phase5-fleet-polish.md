# A2A Phase 5: Lane Polish + Fleet Features — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a *fleet* survivable. Phase 2 gave the broker admission caps; Phase 5 gives it the four things that stop a fleet burning quota or memory on its own mistakes — per-lane **turn budgets** (`failed{budget_exhausted}`, resumable), a **circuit breaker** that stops admitting after K failures in a window, **respawn lineage** (`parent_id`/`attempt`/`max_attempts`) so a poison brief is quarantined instead of retried forever, and **`auto_review`**, which turns a finished impl worktree into a review-lane worker whose findings land as `review.json`. Plus the two leftovers the file layer has been carrying since §4.5: **`events.jsonl` rotation** and **`broker.jsonl` as the transition log**. Plus the lane templates and the **prompt-contract eval fixtures** (spec §8.9) that make the worker protocol testable rather than merely written down.

**Architecture:** No new subsystem. Every feature here is a hook on machinery Phase 2 already built and Phases 3/4 already extended:

- Turn budgets ride the **fold** (a `TurnStarted` envelope increments a counter, an epoch bump resets it) and are *enforced* by the `WorkerActor` at the one moment it already knows a turn opened.
- The circuit breaker rides `CoreMsg::WorkerSettled` — the single message every terminal transition already funnels through — and is read at `decide_admission`, the single door every new spawn already passes.
- `auto_review` rides the same `WorkerSettled` message and re-uses the CLI's own worker-layout code through a newly-extracted `spawn::seed_worker`, so a broker-seeded worker's genesis is byte-identical in shape to a CLI-seeded one.
- Rotation rides `store::EventLog` — the single writer *and* the single reader of `events.jsonl` for every transport — so the broker, the CLI reconcile path, the exec reconcile path and `tail` all inherit it without knowing it happened.

**Tech Stack:** Phase 1+2+3+4's, unchanged. No new dependencies (the auto-review diff is `git diff` via an awaited `tokio::process::Command` in `spawn_blocking`; `regex` is already a dependency and is what the BLOCKED matcher uses).

**Spec:** `docs/superpowers/specs/2026-08-14-a2a-claude-codex-design.md` (v2), §9 Phase 5 scope (§4.9 lanes/budgets/breaker/lineage, §4.5 events rotation + broker.jsonl, §4.8 auto_review, §8.9 prompt-contract tier).

**Compatible with:** devloop skill v0.1+

## Concurrent-build partition contract (READ THIS FIRST)

Phases 3 and 4 were **executed concurrently** in separate git worktrees off HEAD `29265b6` and merged before this plan runs. Phase 5 is **post-merge and sequential**, and therefore owns everything either of them touched. The contract that governed them is recorded here because this plan's ACs depend on their artifacts:

- **Phase 3 owned (created):** `src/sync/{mod,skills,mcp,manifest}.rs`, `src/commands/sync_cmd.rs`, `tests/sync_*.rs`. Append-only touches: `src/cli.rs`, `src/main.rs`, `src/commands/mod.rs` (a `Sync` subcommand), `src/config.rs` (`[sync]` keys), `src/commands/doctor.rs` (appended checks), `Cargo.toml` (`toml_edit`).
- **Phase 4 owned (created):** `src/worktree.rs`, `src/commands/verify.rs`, `skills/**` (in-repo skill sources + generator), `templates/impl-result.schema.json`, `tests/cmd_verify.rs`, `tests/worktree_*.rs`. Modified: `src/commands/spawn.rs` (worktree default + elevation gate), `src/commands/gc.rs`, `src/state.rs` (`+worktree` fields, serde-default), `src/fold.rs` (genesis worktree keys), `src/config.rs` (`[lanes] allow_elevation` etc.), `src/cli.rs`/`src/main.rs` (`Verify` + spawn flags), `src/commands/ls.rs` (WT marker).
- **Both** appended to `config.rs`/`cli.rs`/`main.rs`/`commands/mod.rs` at the END of the relevant blocks so the merge stayed trivial. **Phase 5 keeps that discipline** — every new config key, CLI variant and `*_FIELDS` entry is appended at the end of its list.
- **Phase 5 owns:** `src/broker/core.rs` + `src/broker/worker_actor.rs` (budgets, breaker, auto_review, transition log), `src/commands/respawn.rs`, `src/templates.rs`, `templates/**` (except `impl-result.schema.json`, which is P4's and is only *read* here), events rotation in `src/store.rs`, and the eval fixtures.

**Three named Phase-4 dependencies.** Where a task below depends on a P4 artifact it says so and names it. Read the merged code before assuming a shape — the names below are what this plan expects, not what P4 necessarily chose:

| # | Dependency | Where used | If it is missing / differently named |
|---|---|---|---|
| D1 | `WorkerSnapshot.worktree: Option<…{path, branch, base_ref}>` (spec §4.5) | Task 5 (auto_review needs a diff to review) | Auto-review is a no-op with a `broker.jsonl` `auto_review_skipped{reason:"no_worktree"}` line — pinned behaviour, so the feature ships either way |
| D2 | The lane→default-result-schema mechanism P4 added for `impl`. **The filename here is a guess** — `templates/impl-result.schema.json` — recorded once so the plan uses one name; the merged tree's name wins wherever they disagree | Task 4 (review lane's default = the findings schema), Task 5 (the auto-review worker gets the *review* lane's default through the same mechanism) | Task 4 implements the mechanism itself in `src/templates.rs`, for BOTH lanes, and P4's impl default is folded into it |
| D3 | `skills/**` — the in-repo generated worker-protocol artifacts (AGENTS.md source + `a2a-worker` skill) | Task 7 (deterministic prompt-contract tier) | That half of the tier SKIPs with an eprintln naming the dependency; the template half is self-contained and always runs |

## Ground truth (verified against HEAD `29265b6` on this machine, 2026-08-18)

Every item was read out of the tree, not remembered, and each one BINDS a task below.

- **`FailureClass::BudgetExhausted` already exists** (`src/fsm.rs:88`) and **`src/fsm.rs` needs no change to reach it.** `fold::refine_failure_class` (`src/fold.rs:320`) honours a lifecycle row's `"failure_class": "<snake_case>"` on any transition that already lands in `failed`, and `Running + Timeout{Task}` → `Transition::failed(FailureClass::Timeout)` is an existing row. So a budget trip is the payload `{"fsm":"Timeout","kind":"task","failure_class":"budget_exhausted"}` — an existing edge with a refined diagnosis. **The FSM stays frozen in Phase 5** (`tests/fsm_table.rs` and `docs/machines/worker.md` are untouched).
- **`fold::envelope_to_fsm_event` dispatches on `raw["fsm"]` alone** (`src/fold.rs:43`), independent of `Envelope::kind`. A NEW `EventKind` may therefore carry an EXISTING FSM payload — which is how `budget_exhausted` gets a readable `tail` line without an FSM edge, and how `events_rotated`/`auto_review_*` stay pure bookkeeping (no `fsm` key ⇒ the fold consumes the seq and changes nothing ⇒ `fold_rejections` stays empty).
- **`store::EventLog` is the single append point and `store::read_events` the single read point for `events.jsonl`, across every transport.** Verified by grep: the only `read_events` callers are `fold::recover` (`src/fold.rs:449`), `reconcile.rs:643`, `commands/tail.rs:95` and `:309`, plus tests; every writer (`WorkerActor::persist`, `reconcile.rs:136`, `reconcile.rs:293`, `broker/core.rs:980/1640/1700`, `commands/*`) goes through `EventLog::open` + `append`. Rotation implemented inside `store.rs` is therefore transparent to the broker, the CLI reconcile path, the **exec** reconcile path and `tail` — no call-site sweep.
- **`EventLog::open` already scans for the last parseable line to resume `next_seq`, and repairs a torn tail with `set_len` before the first append** (`src/store.rs:60-90`). Rotation extends that scan across `events.jsonl.1`; it does not replace it.
- **`broker::core::BrokerLog` is `#[derive(Clone)]` over an `mpsc::Sender<LogMsg>`** (`src/broker/core.rs:866`) with `pub async fn line(&self, Value)`. A `WorkerActor` can hold one, so `broker.jsonl` gains per-worker transition lines with no new channel and no new writer (the dedicated log task stays the sole writer of the file).
- **`broker.jsonl` today carries NO state transitions.** Its whole vocabulary is `admission_expired`, `admission_failed`, `worker_panic`, `broker_scoped`, `server_request` and `degraded` (grep of `log.line(`). Spec §4.5 defines it as "one line per state transition `{worker,from,to,event,ts}` — fleet post-mortems". Task 8 closes that gap; nothing before Task 8 may assume transition lines exist.
- **`Settled { id, state }`** (`src/broker/worker_actor.rs:149`) is announced by `WorkerActor::announce_settled` on every first entry into a terminal state or `escalated`, and `CoreMsg::WorkerSettled` (`src/broker/core.rs:1105-1113`) is the ONLY way it reaches the registry. Both the breaker (Task 2) and auto_review (Task 5) hook exactly there. `Settled` carries no failure class — and does not need one: the breaker counts `state == Failed`, whatever the class.
- **`decide_admission`** (`src/broker/core.rs:1441`) is the single door for new work: `gates(req)` → `drain_queue()` → lane/global room → `no_queue`/`queue_cap` → enqueue. **`register(dir, resume, capped)`** (`src/broker/core.rs:1753`) is the resume/adoption door and already refuses `capped` requests at the cap with `AT_CAPACITY`. The breaker is checked in both, right after the gates.
- **`gates(req)` → `check_cwd(req)` (`src/broker/core.rs:1333`) refuses with `"CWD_NOT_ALLOWED"` any `req.cwd` that is `Some(_)` and not under `cfg.allowed_roots`.** It has exactly **two** escapes, both read out of the tree: `req.cwd == None` returns `Ok(())` immediately (`core.rs:1334-1336`, commented *"an adoption: the record on disk is already the truth"*), and `Lane::Research` is checked against `<dir>/scratch` instead of the roots. **There is no `Lane::Review` exemption and Phase 5 does not add one** — Task 5's broker-seeded reviewer therefore passes `cwd: None` over the wire. `$A2A_HOME/workers/<id>` is NOT under the default allowed root, so any other choice ships a feature that is green in a temp home and refused on a real one.
- **Two state transitions are committed by the CORE, not by `WorkerActor::persist`:** `queued → spawned` in `admit_queued` (`src/broker/core.rs:1642`, `append_and_fold(… {"fsm":"Admit"})`) and `queued → cancelled` in `cancel_queued` (`core.rs:1703`, `{"fsm":"Kill"}`). Both already hold `self.log` (the `BrokerLog`). Task 8's transition log must therefore have **two** writers, or a worker born `queued` has no `from:"queued"` line at all.
- **The `what` labels passed to `WorkerActor::persist` are exactly these twelve** (grep of `self.persist(` / `.persist(` in `src/broker/worker_actor.rs`): `readoption` (:400), `process_started` (:444), `session_identified` (:464), `start_failed` (:536), `turn_ended` (:647), `send` (:692), `inbox_delivered` (:726), `interrupt` (:762), `kill` (:797), `timeout` (:846), `lifecycle` (:925), `trace` (:948). **There is no `"resume"` label.** Task 1 adds a thirteenth (`trip_budget`'s commit rides `lifecycle` unless the executor names it otherwise) — so the set is OPEN and no test may pin it closed.
- **`tests/fixtures/stub-codex` has no flood knob.** Its knobs are `STUB_SLEEP, STUB_EXIT, STUB_LAST_MSG, STUB_NO_RESULT, STUB_LEAK, STUB_EMIT_QUESTION, STUB_RECORD, STUB_RESUME_FAIL, STUB_DUMP_ENV`, and it emits five fixed JSONL lines (one of which, `agent_message_delta`, is dropped by the mapper) ⇒ ~2 KiB of `events.jsonl` for a whole run. `FAKE_FLOOD`/`FAKE_SCENARIO`/`FAKE_MESSAGE` are **fake-app-server knobs only** and do nothing to the stub. Task 6's exec case sizes its cap to the stub, rather than borrowing the app-server case's numbers.
- **`FAKE_MESSAGE` is one env var on ONE app-server child shared by every worker** (`tests/fixtures/fake-app-server:83`) — there is no per-thread message. Any broker test that wants two workers to say different things must arrange it some other way (a schema that only one of them carries, or two separate assertions on the same text).
- **`WorkerSnapshot` already carries `thread_id`/`queue_position` as `#[serde(default)] Option<…>`** and preserves unknown siblings through `#[serde(flatten)] pub extra` (`src/state.rs:53-79`), with a regression test (`a_snapshot_without_the_phase_two_fields_still_loads`) proving an older snapshot is not `Corrupt`. Every field Phase 5 adds follows that exact pattern and extends that test.
- **`brief::Brief` already parses `max_turns: Option<u32>` and `result_schema: Option<String>`** (`src/brief.rs:54-56`) and **nothing outside `brief.rs` reads either of them** (verified by grep). Task 1 and Task 4 are what finally consume them; no parser change is needed.
- **`AppError.data: Option<Value>` exists and serializes as `error.data`** (`src/output.rs:33`, `emit_err`), and survives the broker socket via `wire::WireError` — the transport for `BREAKER_OPEN`'s `{failures, window_secs, opened_at}` and `ATTEMPTS_EXHAUSTED`'s `{parent_id, attempt, max_attempts}` already exists.
- **`events::EventKind` is `#[serde(rename_all="snake_case")]` with `#[serde(other)] Unknown`**, and `src/events.rs:853` holds a `Phase1EventKind` mirror marked *"the vocabulary a deployed older binary knows. **Do not add to it**"* with a test that new variants degrade to `Unknown`. Phase 5's new variants are exactly **two** — `budget_exhausted` (Task 1) and `events_rotated` (Task 6) — and they go in `EventKind` and **never** in that mirror. **There is no `auto_review_spawned` variant:** Task 5's bookkeeping is `broker.jsonl` lines plus one existing `EventKind::OutboxNotice`, and `src/events.rs` is deliberately absent from Task 5's Files list.
- **`config::LANE_FIELDS` is `[&str; 8]`, `ADMISSION_FIELDS` `[&str; 2]`, `BROKER_FIELDS` `[&str; 1]`** at HEAD, each with an in-module test asserting the array's contents/length; `tests/config_layering.rs` contains **no** length assertion (grep) so only the `src/config.rs` unit tests move. Phases 3/4 may have appended to these — **read the merged arrays and append after whatever is there.**
- **No `BLOCKED:` matcher exists anywhere in `src/`** (grep). Spec §4.7 defines one (`^BLOCKED:[ \t]*(.*)$` on the last non-empty line of the final agent message, mid-message occurrences ignored) and Task 7 is what finally writes it — as an *observational* scorer for the eval tier, not as a driver of the machine (the outbox file drives the transition, per §4.7; keeping it that way is a recorded deferral).
- **The fake app-server fixture** (`tests/fixtures/fake-app-server`) answers `--version` as `codex-cli 0.140.0` without touching state, persists per-thread state in `$FAKE_STATE`, and offers the scenarios `happy | blocked | steer-race | hang-turn | server-request | unknown-request | crash-mid-turn | always-crash | resume-fail | bad-vocabulary` plus the knobs `FAKE_MESSAGE`, `FAKE_FLOOD`, `FAKE_DEFER_METHODS`, `FAKE_DUMP_ENV`, `FAKE_TRANSCRIPT`. **`hang-turn` holds the turn open until steered or interrupted** — that is what makes every budget/breaker assertion in this plan race-free.
- `git version 2.50.1` is on PATH (auto_review's diff).

## Global Constraints (Phase 1+2+3+4's all still bind; these are the additions)

- **The FSM is not modified.** No new `FsmEvent`, no new `TimeoutKind`, no new `WorkerState`. Every Phase-5 state outcome is an existing edge, optionally with a refined `failure_class` (see Ground truth). `tests/fsm_table.rs` and `docs/machines/worker.md` are untouched by every task in this plan.
- **The CLI stays synchronous** except `a2a broker run` and the one sanctioned `adapter::handshake_probe` exception. `a2a respawn` (Task 3) and `a2a broker reset-breaker` (Task 2) are ordinary sync commands.
- **Actor discipline continues:** `Arc<Mutex`/`Arc<RwLock` remain forbidden in `src/broker/` + `src/rpc.rs` (source-scan-tested); no `write_all` inside `select!`; no `std::thread::sleep` in async modules. The breaker is a **value owned by the core task**, not a shared lock. The auto-review diff runs in `spawn_blocking`/an awaited `Command`, never on the registry task (spec §4.2: "no blocking work on actor tasks").
- **Zero compiler warnings.** `cargo build` and `cargo test` must both be warning-free at every commit; a task is not done while `cargo test 2>&1 | grep -c warning` is nonzero.
- **Tests never touch real `~/.a2a`, `~/.codex`, `~/.claude.json` or `~/.claude/skills`.** Temp `A2A_HOME`/`CODEX_HOME`/fixture dirs only. The ONLY real-machine touch in this plan is Task 7's `A2A_E2E=1 A2A_EVAL=1` statistical tier, and even that is **read-only against the real home**: it copies `~/.codex/auth.json` into a temp `CODEX_HOME` and exports that for the run, so real codex writes its rollouts into the temp dir (a real `codex exec` DOES write sessions into `$CODEX_HOME` — that is what `a2a resume` depends on, so "reads auth and writes nothing" is only true with the copy). Explicitly gated, `#[ignore]`d, and off by default.
- **Exit-code table unchanged and pinned:** 7 = broker/reachability/skew, 9 = static precondition. Every new refusal in this plan is **9** (`BREAKER_OPEN`, `ATTEMPTS_EXHAUSTED`, `PARENT_ACTIVE`) — they are decided from records and config, and no broker appearing or disappearing changes the answer. `a2a broker reset-breaker` against a broker that is not running is **7**.
- **`tests/common::a2a()` keeps setting `A2A_BROKER_DISABLE=1` + `A2A_OWNER=test-owner`;** broker tests opt in via `common::a2a_broker()` and **every broker in every test is torn down by its `BrokerGuard`**, with `common::assert_no_broker_left(home)` at the end of any test that asserts about strays.
- **Commit messages carry no co-author lines** and **the plan file itself is never committed by a task executor** (the orchestrator ticks the checkboxes).
- **Config additions append at the end of their `*_FIELDS` array** and their `config show --origin` key is pinned in the task that adds them. `0` keeps meaning the SAFE reading for every cap (`0` = admit nothing) — with **one recorded exception**, `admission.breaker_failures = 0`, which DISABLES the breaker, because a threshold whose job is to *stop* work must not read "stop everything" when unset-to-zero; a breaker that trips at zero is not caution, it is an outage on a fresh daemon.
- **Deliberate deviations from the spec, recorded:**
  - Turn budgets are **counted on both transports** (the fold counts `turn_started` envelopes regardless of transport) but **enforced broker-side only**. An exec worker's `turns_this_epoch` is observable in `ls --json` and nothing acts on it — the exec transport has no mid-turn interrupt to enforce with. Recorded, not silent.
  - The circuit breaker **counts every `failed` settlement regardless of class**. A per-class exclusion list (e.g. "schema violations are the brief's fault, not the fleet's") is deliberately not in v1: every failure class in the table is a reason to stop and look, and a breaker with exemptions is one that does not trip when it matters.
  - The breaker's persistence in `broker-health.json` is **observational**: a fresh daemon starts with the breaker closed. `a2a broker restart` is the documented second reset path precisely because of that.
  - `events.jsonl` rotation keeps a **single** generation. A *second* rotation discards the oldest one, so a rebuild-from-zero after two rotations starts mid-log. The rotation marker records `discarded_prior_generation: true` so the gap is visible rather than inferred. Multi-generation retention is deferred.
  - The `BLOCKED:` marker (§4.7) is added as an **observational** matcher for the eval tier only. The `outbox/<seq>.question.json` file remains the sole driver of the `awaiting-input{blocked}` transition, exactly as §4.7 specifies.
  - `auto_review` fires **only for the `impl` lane and only when the worker has a worktree** (D1). An impl worker without one produces a `broker.jsonl` `auto_review_skipped` line and nothing else — reviewing a diff that does not exist is worse than not reviewing.
  - Rate-limit-headroom-fed admission (spec §4.9, deferred from Phase 2) stays deferred: `account/rateLimits/updated` notices are recorded in `broker-health.json` (`BrokerHealth.notices`, `core.rs:817/850`) and **nothing reads them yet** — `doctor`'s ten `CHECKS` touch `BrokerHealth` only for pid/degraded/child identity, never `notices` (verified). Turning a vendor's headroom number into an admission decision needs data this build has never collected.

**Test-specification policy (unchanged from Phases 1/2):** tasks specify tests by precise acceptance criteria + Interfaces contracts rather than verbatim bodies. **Every exact value an AC names is a pin** — error codes, exit codes, config keys, file names, defaults, JSON key names. When an AC is silent, match the surrounding codebase's conventions (read the neighbouring tests). **Async ACs are ALWAYS converged-state assertions**: poll with an explicit bounded timeout (the house style is ≤10s at 50ms), never an instant assertion on an async transition; `tokio::time::pause` only in `current_thread`-flavour tests with no real child I/O.

## File Structure (additions)

```
src/
  templates.rs           # include_str! of templates/**, lane scaffolds, default result schemas
  commands/respawn.rs    # a2a respawn <id> [--brief <f>]
  store.rs               # (+) events.jsonl rotation: open_capped, rotate-on-append, .1-aware reads
  broker/core.rs         # (+) Breaker, ResetBreaker, auto_review hook, transition log plumbing
  broker/worker_actor.rs # (+) turn-budget enforcement, BrokerLog transition lines
templates/
  impl-brief.md  review-brief.md  research-brief.md  general-brief.md
  review-findings.schema.json
tests/
  fixtures/adversarial-briefs/{unresolvable-decision,missing-file,ambiguous-criteria,compaction-forcing}.md
  budgets.rs  breaker.rs  cmd_respawn.rs  templates.rs
  auto_review.rs  events_rotation.rs  prompt_contract.rs  fleet_smoke.rs
```

---

### Task 1: Turn budgets — `[lanes.<lane>] max_turns`, fold-counted, broker-enforced

**Acceptance Criteria:**

- **Config surface PINNED.** `LANE_FIELDS` gains `"max_turns"` **appended at the end** of whatever the merged array holds. Defaults per lane: **`impl 40`, `review 15`, `research 20`, `general 30`** (`LaneConfig.max_turns: u64`). Origin key `lanes.<lane>.max_turns` (e.g. `lanes.impl.max_turns`), env key `A2A_LANES_IMPL_MAX_TURNS` via the existing `config::lane_env_key`. `a2a config show --origin` reports it. A malformed value fails `CONFIG_INVALID` naming the dotted key; an unknown sibling still only warns.
- **`max_turns = 0` means "no turns allowed"** and trips on the first `turn/started` — the same safe reading every other cap in `config.rs` has. Uncapping is spelled with a large number.
- **The brief may only lower it.** `spawn` computes `effective = min(lane_cfg.max_turns, brief.max_turns as u64)` when the brief's front matter carries one. A brief asking for MORE is clamped and warns to stderr, verbatim shape: `a2a: warning: brief max_turns 99 exceeds the impl lane's 40 — clamped to 40 (a brief may only lower the turn budget)`. **The `a2a: warning: ` prefix is added by `spawn::run`'s existing `warn` closure (`src/commands/spawn.rs:105-109`)** — so the `Option<String>` `effective_max_turns` returns is the message WITHOUT that prefix (`brief max_turns 99 exceeds …`), and the pinned line above is what stderr shows after `warn()` has prepended it. A returned string that carries the prefix itself would print it twice. `brief::Brief.max_turns` is already parsed (Ground truth); no parser change.
- **Genesis records the effective budget** as `"max_turns": <u64>`, and `fold::seed_from_genesis` seeds a new `WorkerSnapshot.max_turns: Option<u64>` (`#[serde(default)]`). Rebuild-lossless: deleting `state.json` and running `a2a status` restores it (pinned by test).
- **The counter is fold-derived, not writer-derived.** `WorkerSnapshot.turns_this_epoch: Option<u64>` (`#[serde(default)]`), seeded `Some(0)` at genesis. Inside `fold::fold`, in the non-genesis `env.epoch >= snap.epoch` branch and BEFORE the FSM dispatch: `env.kind == EventKind::TurnStarted` increments it. In `fold::apply_event`, a transition with `epoch_bump` resets it to `Some(0)`. Consequences pinned by unit tests in `src/fold.rs`: (a) three `turn_started` envelopes ⇒ `turns_this_epoch == Some(3)`; (b) a `Resume` row after them ⇒ `Some(0)` and `epoch == 2`; (c) a stale-epoch `turn_started` does NOT increment (fencing); (d) replaying the same log twice is idempotent (seq dedup).
- **Enforcement is broker-side and lands on the existing edge.** In `WorkerActor::on_envelope`, immediately AFTER the `EventKind::TurnStarted` envelope has been durably appended (so the count that trips is the count on disk), if `self.snap.turns_this_epoch.unwrap_or(0) > self.budget()` — where `budget()` is `snap.max_turns` falling back to the lane config — the actor:
  1. best-effort `turn/interrupt`s the open turn (ignoring the RPC result: a child that will not answer must not keep a worker running past its budget) and clears `open_turn`;
  2. persists ONE lifecycle envelope, `EventKind::BudgetExhausted`, payload exactly `{"fsm":"Timeout","kind":"task","failure_class":"budget_exhausted","turns_this_epoch":<n>,"max_turns":<m>}`.
  The FSM sees `Running + Timeout{Task}` → `failed`, refined to **`failed{budget_exhausted}`**. `fold_rejections` stays empty (pinned). `a2a status <id>` exits **4** with `data.failure_class == "budget_exhausted"`.
- **Resumable.** `a2a resume <id>` on a `failed{budget_exhausted}` worker succeeds, bumps the epoch, and resets `turns_this_epoch` to 0 — the resumed worker gets a fresh budget (pinned by test with `max_turns = 1`).
- **New `EventKind::BudgetExhausted`** (serde `budget_exhausted`). It is **NOT** added to the `Phase1EventKind` mirror in `src/events.rs` (Ground truth) — the existing forward-compat test proving new kinds degrade to `Unknown` is extended to cover it.
- **Surfaced in `ls --json`:** each worker row gains `"turns_this_epoch"` and `"max_turns"` (both nullable). The human table is unchanged.
- **Integration, on the fake, race-free:** with `A2A_LANES_GENERAL_MAX_TURNS=0` and `FAKE_SCENARIO=hang-turn` (the turn is held open, so nothing races the trip), one `a2a spawn --lane general` converges (poll ≤10s at 50ms) to `state == failed`, `failure_class == budget_exhausted`, `turns_this_epoch == 1`, `max_turns == 0`, and its `events.jsonl` contains exactly one `budget_exhausted` envelope. With `A2A_LANES_GENERAL_MAX_TURNS=1` and `FAKE_SCENARIO=blocked`, turn 1 parks `awaiting-input{blocked}`, `a2a send <id> "go on"` opens turn 2, and the worker converges to `failed{budget_exhausted}` — proving the budget counts turns across a park, not just within one.

**Verify via:** cli
**Apply:** docs/development/appendix-fsm.md — the diagnosis is data on the row, the edge is the machine's.

**Files:** Modify `src/config.rs`, `src/state.rs`, `src/fold.rs`, `src/events.rs`, `src/commands/spawn.rs`, `src/commands/ls.rs`, `src/broker/worker_actor.rs`; Test `tests/budgets.rs` + unit tests in `config.rs`/`fold.rs`/`state.rs`.

**Interfaces:**
```rust
// src/config.rs
pub const DEFAULT_MAX_TURNS_IMPL: u64 = 40;
pub const DEFAULT_MAX_TURNS_REVIEW: u64 = 15;
pub const DEFAULT_MAX_TURNS_RESEARCH: u64 = 20;
pub const DEFAULT_MAX_TURNS_GENERAL: u64 = 30;
// LaneConfig gains: pub max_turns: u64,

// src/state.rs — WorkerSnapshot gains (both #[serde(default)]):
//   pub max_turns: Option<u64>,
//   pub turns_this_epoch: Option<u64>,

// src/commands/spawn.rs — the clamp, pure and unit-testable
pub fn effective_max_turns(lane_max: u64, brief_max: Option<u32>) -> (u64, Option<String>);
//                                                     effective ^      ^ warning body, WITHOUT the
//                                                        `a2a: warning: ` prefix — `spawn::run`'s
//                                                        existing `warn` closure adds that.

// src/broker/worker_actor.rs — private, but its contract is pinned:
//   fn budget(&self) -> u64                      // snap.max_turns else lane config
//   async fn trip_budget(&mut self)              // interrupt + the one lifecycle commit
```

- [ ] Steps: failing tests → RED → implement (config → state → fold → spawn → actor → ls) → GREEN → full suite, zero warnings → commit `feat: per-lane turn budgets (fold-counted, broker-enforced, resumable)`.

---

### Task 2: Circuit breaker — `BREAKER_OPEN` + `a2a broker reset-breaker`

**Acceptance Criteria:**

- **Config surface PINNED.** `ADMISSION_FIELDS` gains `"breaker_failures"` and `"breaker_window_secs"`, **appended in that order** after whatever the merged array holds. Defaults: **`breaker_failures = 5`, `breaker_window_secs = 300`**. Origins `admission.breaker_failures`, `admission.breaker_window_secs`; env `A2A_ADMISSION_BREAKER_FAILURES`, `A2A_ADMISSION_BREAKER_WINDOW_SECS`.
- **Both are `u64`, deliberately.** Every `ADMISSION_FIELDS` key is parsed through `parse_u64_str` into a `u64` and then matched on the field name (`src/config.rs:654-666` for env, `:518-525` for TOML), each arm ending in `_ => unreachable!("field membership fixed by ADMISSION_FIELDS")`. A `u32` field would need a narrowing cast with no defined behaviour for an out-of-range value — and a bare `as u32` silently wraps `4294967296` to `0`, which by this task's own rule means *breaker disabled*, i.e. a typo that turns the safety feature off. `u64` throughout (config field, `Breaker`'s threshold argument, `BreakerHealth.failures`, and the refusal's `data.failures`) removes the question. **Three edit sites, all pinned:** the `apply_env` arm, the `apply_admission_file` (TOML) arm, and — because it is an **array-length compile error, not a test failure** — `assert_eq!(ADMISSION_FIELDS, ["global_cap", "queue_cap"])` at `src/config.rs:820`, which must be extended in the same commit or the crate does not build.
- **`breaker_failures = 0` DISABLES the breaker** (the one recorded exception to the "0 is the safe reading" rule — see Global Constraints). Pinned by a unit test that pushes 50 failures at a 0-threshold breaker and asserts it stays closed.
- **The breaker is a value owned by the core task**, `BrokerCore.breaker: Breaker` — no `Arc`, no lock (the source-scan gate still passes). It counts **`CoreMsg::WorkerSettled` with `state == WorkerState::Failed`**, whatever the failure class. Pure unit tests (no IO, injected `Instant`s): failures outside the window are pruned; the threshold is `>=`; a second failure after opening does not move `opened_at`.
- **Refusal shape PINNED.** With the breaker open, a new spawn's `admit` gets:
  `code "BREAKER_OPEN"`, exit **9**, `data == {"failures": <u64>, "window_secs": <u64>, "opened_at": "<rfc3339>"}`, remedy `` `a2a broker reset-breaker` once the cause is fixed, or `a2a broker restart` ``. The CLI surfaces it verbatim (`a2a --json spawn …` → `error.code == "BREAKER_OPEN"`, `error.data.failures == …`).
- **`BREAKER_OPEN` is added to `wire::KNOWN_CODES`** (`src/broker/wire.rs:86`, `[&str; 22]` at HEAD ⇒ `[&str; 23]`), appended after `"AT_CAPACITY"` with the other admission refusals it is a sibling of. It crosses the socket exactly like `QUEUE_FULL`/`AT_CAPACITY` do; an unlisted code still round-trips, but only through `wire::static_code`'s `Box::leak` fallback (`wire.rs:332`), which is the path for codes from a *newer* daemon, not for ones this build emits. **`ATTEMPTS_EXHAUSTED` and `PARENT_ACTIVE` (Task 3) correctly stay OUT** — `respawn` decides both CLI-side and neither ever crosses the socket.
- **Checked at both doors, after the gates.** In `decide_admission`, the order is `gates(req)` → **breaker** → `drain_queue()` → capacity (so a cwd outside the allowlist is still reported as a cwd problem, not as a breaker problem). In `register(dir, resume, capped)`, the breaker is checked at the top of the `capped` branch — a socket `resume` is new work and is refused identically. **Adoption (`capped == false`) is never refused**: those workers were already running, and refusing to re-attach them strands the fleet the daemon exists to recover.
- **Queued workers keep draining.** The breaker refuses NEW admission requests only; `drain_queue`/`admit_queued` are untouched, so a worker that was already waiting still gets its slot when one frees. Pinned by the construction in the tests below.
- **Persisted for observation.** `BrokerHealth` gains `#[serde(default)] pub breaker: BreakerHealth { pub open: bool, pub failures: u64, pub window_secs: u64, pub opened_at: Option<DateTime<Utc>> }`. **The type is named `BreakerHealth` everywhere** — in this AC, in the Interfaces block below, and in the code. A `broker-health.json` written by the Phase-2 binary (no `breaker` key) still loads (pinned). Opening the breaker also writes one `broker.jsonl` line: `{"ts":…,"kind":"breaker_open","failures":<n>,"window_secs":<w>}`; a reset writes `{"ts":…,"kind":"breaker_reset"}`.
- **A fresh daemon starts closed** — the persisted state is observational, and `a2a broker restart` is the documented second reset path (pinned by test: trip it, restart the broker, spawn succeeds).
- **The registry surface test is updated, not left to rot.** `CoreMsg::ResetBreaker` is the fifteenth registry message, and `the_registry_is_reached_by_exactly_these_fourteen_messages` (`src/broker/core.rs:2686`) has a `_ => "internal"` catch-all in its `name()` fn, so a new variant neither breaks the build nor fails the assertion — the invariant would silently go stale. **Pinned:** rename it to `…_fifteen_messages`, add the `CoreMsg::ResetBreaker => "ResetBreaker"` arm, and add the corresponding entry to both the `surface` vec and the expected-names list (and update the doc comment's count).
- **`a2a broker reset-breaker`:** new `BrokerCmd::ResetBreaker`, `command_name` → `"broker reset-breaker"`, socket op `"reset-breaker"`, `CoreMsg::ResetBreaker`. Exit **0** with `data == {"reset": true, "was_open": <bool>}`. It uses `brokerclient::connect` (NOT `connect_or_start`) so **resetting a breaker on a broker that is not running exits 7** `BROKER_UNAVAILABLE` — autostarting a daemon to reset a breaker it never had is a lie about what happened.
- **`broker status` reports it.** The `info` op's payload gains `"breaker"` (the `BreakerHealth` object); `a2a broker status --json` carries it through as `data.breaker`, and the human stderr line appends `, breaker OPEN (<n> failures in <w>s)` when open — loud, per spec §4.9.
- **Integration (`tests/breaker.rs`), all on the fake with a `BrokerGuard`:**
  - **trip + refuse:** `A2A_ADMISSION_BREAKER_FAILURES=2`, `A2A_LANES_GENERAL_MAX_TURNS=0`, `FAKE_SCENARIO=hang-turn`. Two spawns each trip Task 1's budget and settle `failed` (poll ≤10s). A third spawn exits **9** with `error.code == "BREAKER_OPEN"` and `error.data.failures == 2`; `broker-health.json`'s `breaker.open == true`. **No worker directory is left behind by the refused spawn** (the pre-genesis cleanup rule from Phase 2 still holds).
  - **reset:** `a2a broker reset-breaker` exits 0 with `was_open == true`; a subsequent spawn succeeds; a second `reset-breaker` exits 0 with `was_open == false`.
  - **no broker:** `a2a broker reset-breaker` against a home with no daemon exits **7**.
  - **queued workers still drain while open** (deterministic construction, pinned): `A2A_ADMISSION_GLOBAL_CAP=2`, `A2A_ADMISSION_LANES_GENERAL_CAP=1`, `A2A_ADMISSION_LANES_REVIEW_CAP=1`, `A2A_ADMISSION_BREAKER_FAILURES=1`, `FAKE_SCENARIO=hang-turn`. **A** (`--lane general`, `max_turns` 5) runs and holds the general slot; **B** (`--lane general`) queues at position 1 (poll `ls --json`); **C** (`--lane review`, `A2A_LANES_REVIEW_MAX_TURNS=0`) is admitted under the review lane, trips its budget, fails ⇒ the breaker opens. **C's brief is not `common::write_brief`'s one-liner:** `brief::required_keys(Lane::Review)` is `["acceptance_criteria"]` (`src/brief.rs:69`) and `spawn` **rejects** rather than warns, so C's brief must carry `lane: review` front matter with a non-empty `acceptance_criteria` list or it dies at the brief gate before admission is ever consulted. Write it inline in the test (or, once Task 4 has landed, scaffold it from `a2a spawn --template review`). Then `a2a kill <A>` settles A `cancelled`, and **B leaves `queued`** within 10s (poll `ls --json` for `state != "queued"`) **while a fresh spawn D still exits 9 `BREAKER_OPEN`**.

**Verify via:** cli
**Apply:** docs/development/appendix-actor-model.md §3 — the supervisor decides admission; the failure counter is its state, not a shared one.

**Files:** Modify `src/config.rs`, `src/broker/core.rs`, `src/broker/wire.rs` (`KNOWN_CODES` += `"BREAKER_OPEN"`), `src/broker/sockserv.rs`, `src/brokerclient.rs` (if a typed helper is warranted), `src/commands/broker_cmd.rs`, `src/cli.rs`, `src/main.rs`; Test `tests/breaker.rs` + unit tests in `config.rs`/`core.rs`.

**Interfaces:**
```rust
// src/config.rs — AdmissionConfig gains (both u64, like every other
// ADMISSION_FIELDS key, because they all go through `parse_u64_str`):
//   pub breaker_failures: u64,     // 0 disables
//   pub breaker_window_secs: u64,
pub const DEFAULT_BREAKER_FAILURES: u64 = 5;
pub const DEFAULT_BREAKER_WINDOW_SECS: u64 = 300;

// src/broker/core.rs
#[derive(Default)]
pub struct Breaker { /* VecDeque<Instant> of in-window failures + opened_at */ }
impl Breaker {
    /// Record one failed settlement; returns true if THIS one opened the breaker.
    pub fn record_failure(&mut self, now: Instant, at: DateTime<Utc>, threshold: u64, window: Duration) -> bool;
    pub fn is_open(&self) -> bool;
    /// Clears the window and `opened_at`; returns whether it had been open.
    pub fn reset(&mut self) -> bool;
    pub fn health(&self, window_secs: u64) -> BreakerHealth;
}
// CoreMsg gains: ResetBreaker { reply: oneshot::Sender<bool> },
impl CoreHandle { pub async fn reset_breaker(&self) -> Result<bool, AppError>; }

// src/commands/broker_cmd.rs
pub fn reset_breaker(home: &A2aHome, quiet: bool) -> Result<Outcome, AppError>;
```

- [ ] Steps: failing tests → RED → implement (config → Breaker unit → core hook + admission doors → health/log → op + CLI) → GREEN → full suite, zero warnings → commit `feat: broker circuit breaker (BREAKER_OPEN, health persistence, reset-breaker op)`.

---

### Task 3: `a2a respawn <id>` — lineage, `max_attempts`, poison-brief quarantine

**Acceptance Criteria:**

- **Config:** `ADMISSION_FIELDS` gains `"max_attempts"` (appended after Task 2's two). Default **`max_attempts = 3`**, type `u64` for the same reason Task 2's two are (`parse_u64_str` is the only parser the admission section has), and the same three edit sites apply — `apply_env`, `apply_admission_file`, and the `assert_eq!(ADMISSION_FIELDS, …)` array literal at `src/config.rs:820`, which is a compile error until it is extended. Origin `admission.max_attempts`, env `A2A_ADMISSION_MAX_ATTEMPTS`.
- **Snapshot fields:** `WorkerSnapshot` gains `#[serde(default)] pub parent_id: Option<String>` and `#[serde(default)] pub attempt: Option<u32>`. `fold::seed_from_genesis` reads `"parent_id"` (string) and `"attempt"` (integer). **Absent `attempt` ≡ attempt 1** — every worker that existed before this task is attempt 1, and nothing about it changes on disk. Rebuild-lossless (delete `state.json` → `a2a status` → both survive; pinned).
- **CLI:** `a2a respawn <id> [--brief <f>] [--force]`. `command_name` → `"respawn"`. `<id>` accepts an unambiguous prefix, the same as every other command.
- **Semantics, in this order** (each an independent refusal, each exit **9** unless noted):
  1. Resolve + reconcile the parent (prefix ambiguity → exit **3** with candidates, as elsewhere).
  2. **Owner guard:** `commands::owner::guard(id, parent.owner.as_deref(), force)` — cross-owner without `--force` is `NOT_OWNER`.
  3. **Parent-active guard:** a parent in `{spawned, queued, running}` is refused with `PARENT_ACTIVE` and remedy `` wait for it, or `a2a kill` it first ``. `awaiting-input`, `escalated`, `orphaned` and all three terminal states are respawnable — a parked worker is a legitimate "give up and retry".
  4. **Attempt guard:** `next = parent.attempt.unwrap_or(1) + 1`; if `next > max_attempts` → `ATTEMPTS_EXHAUSTED`, `data == {"parent_id": "<id>", "attempt": <parent's>, "max_attempts": <n>}`, remedy `` fix the brief — three identical retries is the poison-brief signature (spec §4.9) ``.
- **What the child inherits:** `lane`, `cwd`, `owner`, `model`, `effort`, `schema_path` from the parent; the brief is the parent's `brief.md` unless `--brief <f>` replaces it (in which case the new brief goes through the SAME §5 ingress gates every spawn's brief does — front-matter validation, secret scan). **Research-lane exception:** the parent's `cwd` is its own `scratch` dir, so a research respawn passes `cwd: None` and gets a fresh scratch (pinned).
- **The child is an ordinary spawn.** It goes through the whole existing path — transport selection, admission (including Task 2's breaker), the elevation gate, worktree defaults (P4) — by way of a refactor that is itself an AC: `spawn::run` keeps its signature and delegates to a new `spawn::run_with_lineage(home, args, lineage: Option<Lineage>, json_mode, quiet)`. **Every existing `tests/cmd_spawn.rs` case stays green, unmodified.**
- **Genesis carries the lineage** as `"parent_id"` and `"attempt"` keys, present ONLY when lineage is present (a plain spawn's genesis keeps its exact Phase-1/2 shape — the fold reads an absent `attempt` exactly as it reads `1`).
- **Output:** the new worker's usual spawn payload plus `"parent_id"` and `"attempt"`. `ls --json` rows gain `"parent_id"` and `"attempt"`.
- **Tests (`tests/cmd_respawn.rs`), broker-free** (they run under `common::a2a()`, i.e. `A2A_BROKER_DISABLE=1`, on the exec transport with the stub codex; `a2a kill` is how a parent is put into a respawnable terminal state):
  - spawn → `kill` → `respawn` prints a NEW id; `ls --json` shows the child with `parent_id == <parent>`, `attempt == 2`, same lane/cwd/owner, and `brief_sha` **equal** to the parent's (same brief, byte-identical);
  - `respawn --brief <other>` ⇒ `brief_sha` **differs**;
  - chain to attempt 3 — that is spawn (attempt 1) → respawn #1 (attempt 2) → respawn #2 (attempt 3) — and then the **third** `respawn` exits **9** `ATTEMPTS_EXHAUSTED` with the pinned `data` (`next = 3 + 1 = 4 > max_attempts 3`);
  - `respawn` on a `running` parent exits **9** `PARENT_ACTIVE`;
  - cross-owner (`A2A_OWNER=other`) exits **9** `NOT_OWNER`; with `--force` it succeeds and the child's owner is the **parent's** owner, not the forcing caller's (pinned — a forced respawn is a repair, not a transfer);
  - lineage survives losing `state.json` (delete it, `a2a status`, both fields back).

**Verify via:** cli

**Files:** Create `src/commands/respawn.rs`; Modify `src/config.rs`, `src/state.rs`, `src/fold.rs`, `src/commands/spawn.rs`, `src/commands/ls.rs`, `src/commands/mod.rs`, `src/cli.rs`, `src/main.rs`; Test `tests/cmd_respawn.rs`.

**Interfaces:**
```rust
// src/commands/spawn.rs
#[derive(Clone, Debug)]
pub struct Lineage { pub parent_id: String, pub attempt: u32 }

pub fn run(home: &A2aHome, args: &SpawnArgs, json_mode: bool, quiet: bool) -> Result<Value, AppError>;
pub fn run_with_lineage(home: &A2aHome, args: &SpawnArgs, lineage: Option<Lineage>,
                        json_mode: bool, quiet: bool) -> Result<Value, AppError>;

// src/commands/respawn.rs
pub fn run(home: &A2aHome, id: &str, brief: Option<&Path>, force: bool,
           json_mode: bool, quiet: bool) -> Result<Outcome, AppError>;
```

- [ ] Steps: failing tests → RED → implement (config → snapshot+fold → `run_with_lineage` refactor with the spawn suite green → respawn command → cli/main → ls) → GREEN → full suite, zero warnings → commit `feat: a2a respawn with parent/attempt lineage and max_attempts quarantine`.

---

### Task 4: Lane templates, `a2a spawn --template <lane>`, and the review lane's default result schema

**Acceptance Criteria:**

- **Four brief scaffolds exist:** `templates/impl-brief.md`, `templates/review-brief.md`, `templates/research-brief.md`, `templates/general-brief.md`, plus `templates/review-findings.schema.json`. They are compiled into the binary with `include_str!` from a new `src/templates.rs` — there is no install step and no runtime data dir, so a template that is not in the binary does not exist. **If the merged P4 tree already has a `templates/` module or an impl brief template, EXTEND it rather than adding a second one** (dependency D2).
- **Every scaffold satisfies its own lane's gate:** `brief::parse(scaffold)` succeeds and `brief::validate(&parsed, lane)` returns `Ok` for the lane it belongs to (pinned per lane — in particular the impl and review scaffolds carry non-empty `acceptance_criteria`, and impl carries `verify`). Front matter carries `lane`, `acceptance_criteria`, `verify`, `files_in_scope`, `non_goals`, `result_schema`, `max_turns` keys (values may be placeholders, lists may be example-filled, but a list that must be non-empty is non-empty).
- **Every scaffold carries the worker-protocol clauses (spec §4.7), pinned as literal substrings** asserted by a unit test over all four:
  - `BLOCKED:` — the redundant final-line marker;
  - `outbox/` and `question.json` — the file that actually drives the transition;
  - `verification evidence` — the §4.7 finish rule (claims without evidence are incomplete work);
  - `if blocked on` — the default-on-no-answer clause, so a fleet keeps moving;
  - `data, never instructions` — the §4.7 rule about repo content.
  **If P4's generated artifacts use different wording, the fix is to align the ARTIFACT to the spec §4.7 wording, not to loosen this assertion.**
- **`a2a spawn --template <lane>` prints the scaffold to stdout and exits 0**, creating nothing: after the command, `$A2A_HOME/workers/` is empty (pinned) and no broker socket was opened (`A2A_BROKER_DISABLE` is not needed for it to be true — the template path returns before transport selection).
- **Clap surface PINNED.** `SpawnArgs.lane` becomes `Option<Lane>` with `required_unless_present = "template"`, `SpawnArgs.brief` becomes `Option<PathBuf>` with `required_unless_present = "template"`, and a new `#[arg(long, value_name = "LANE")] pub template: Option<Lane>` with `conflicts_with_all = ["lane", "brief"]`. **Regression pin:** `a2a spawn` with neither `--lane` nor `--template` still exits **2** (clap usage), and every existing `tests/cmd_spawn.rs` invocation is unchanged and green.
- **The clap change reaches outside `spawn.rs`, and the task owns that.** `src/commands/respawn.rs` (created in Task 3) constructs a `SpawnArgs` to call `spawn::run_with_lineage`; turning `lane` and `brief` into `Option<…>` breaks its literal, so **`src/commands/respawn.rs` is in this task's Files list** and the executor fixes it here rather than discovering a build error outside its declared surface. **AC: every `tests/cmd_respawn.rs` case stays green, unmodified**, exactly as the `tests/cmd_spawn.rs` pin above requires.
- **The review lane gets a default result schema.** Spawning `--lane review` without `--schema` materializes the findings schema into the worker directory at 0600 and records it: `state.json`'s `schema_path` points at a readable file whose parsed JSON has `"findings"` in its top-level `required`. This uses **the same default mechanism P4 added for the impl lane** (D2); if P4 named the destination file something other than `result-schema.json`, **use P4's name** — the AC is behavioural, not about the filename.
- **The findings schema content is PINNED** (`templates/review-findings.schema.json`, exactly this shape):
  ```json
  {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "a2a review findings",
    "type": "object",
    "required": ["findings"],
    "properties": {
      "findings": {
        "type": "array",
        "items": {
          "type": "object",
          "required": ["file", "line", "severity", "rationale"],
          "properties": {
            "file": {"type": "string"},
            "line": {"type": "integer", "minimum": 0},
            "severity": {"type": "string", "enum": ["high", "medium", "low"]},
            "rationale": {"type": "string"}
          }
        }
      }
    }
  }
  ```
  A unit test parses it and asserts the top-level `required` is `["findings"]` and the item `required` is `["file","line","severity","rationale"]` in that order — the Phase-1 schema gate checks top-level `required` keys, so `findings` is what a review worker must produce.
- **`--schema` still wins:** an explicit `--schema` on a review spawn is recorded unchanged and the default is not materialized (pinned).
- **Tests (`tests/templates.rs`):** one case per lane for `--template` (exit 0, stdout non-empty, nothing created); the parse+validate round trip; the pinned-phrase sweep; the review default-schema spawn; the `--schema` override; and the usage-error regressions.

**Verify via:** cli
**Apply:** docs/development/appendix-api-design.md — the schema is the contract; shipping it in the binary is what makes it one.

**Files:** Create `src/templates.rs`, `templates/{impl,review,research,general}-brief.md`, `templates/review-findings.schema.json`; Modify `src/lib.rs`, `src/cli.rs`, `src/commands/spawn.rs`, `src/commands/respawn.rs` (its `SpawnArgs` literal follows the clap change); Test `tests/templates.rs` + unit tests in `templates.rs`.

**Interfaces:**
```rust
// src/templates.rs
pub fn brief_scaffold(lane: crate::config::Lane) -> &'static str;
/// The lane's default result schema, when it has one. `review` is the findings
/// schema below. `impl` is P4's — this plan's best guess at its filename is
/// `templates/impl-result.schema.json` (the name used in the header's D2 row),
/// but that is a GUESS: read the merged tree and use whatever P4 actually
/// shipped. The AC is behavioural, never about the filename.
pub fn default_result_schema(lane: crate::config::Lane) -> Option<&'static str>;
pub const REVIEW_FINDINGS_SCHEMA: &str = include_str!("../templates/review-findings.schema.json");
```

- [ ] Steps: failing tests → RED → write the templates → implement (`templates.rs` → clap surface → spawn's `--template` short circuit → review default schema) → GREEN → full suite, zero warnings → commit `feat: lane brief templates, spawn --template, review findings schema default`.

---

### Task 5: `auto_review` — an impl worktree's diff becomes a review worker, findings become `review.json`

**Acceptance Criteria:**

- **Config:** `LANE_FIELDS` gains `"auto_review"` (appended after Task 1's `max_turns`), `LaneConfig.auto_review: bool`, **default `false` on every lane**. Origin `lanes.<lane>.auto_review`, env `A2A_LANES_IMPL_AUTO_REVIEW`. The key exists on every lane for surface uniformity, but **only `lanes.impl.auto_review` is ever read** — documented in the field's doc comment and pinned by a test that setting `lanes.review.auto_review = true` changes no behaviour.
- **The trigger is `CoreMsg::WorkerSettled`, but the WORK is not done there.** `CoreMsg::WorkerSettled`'s arm runs inline on the registry task (`src/broker/core.rs:1283`; the core loop is `handle(msg).await`), and `src/broker/core.rs:19-24` states the contract: *"the core must never block on anything a worker does: every command is handed to a short-lived task"*. A `git diff` awaited from that arm stalls status/admit/kill/send/notification-demux for its whole duration. **The shape is therefore PINNED as two hops:**
  1. the `WorkerSettled` arm does only the **cheap, in-memory guards** below (lane, config flag, worktree presence, recursion marker, `review.json` presence) and then `self.tracker.spawn`s a detached task carrying `{impl_id, impl_dir, worktree}`;
  2. that task runs the diff and builds the brief, and sends the outcome **back to the core as a new message**, `CoreMsg::AutoReviewReady { impl_id, impl_dir, brief_text }`, whose handler does the admission decision and the `spawn_blocking(seed_worker)`.
  **AC: the `WorkerSettled` arm performs no `await` on a subprocess and no `await` on `spawn_blocking`** — asserted by reading the arm, and the source-scan gate in `tests/` that already polices actor discipline is the place to add a check if one is cheap. `CoreMsg::AutoReviewReady` is the **sixteenth** registry message: rename Task 2's `…_fifteen_messages` surface test to `…_sixteen_messages` and extend it at the same three points (the `name()` arm, the `surface` vec, the expected-names list) — its `_ => "internal"` catch-all means a new variant is otherwise invisible to it.
  **Ordering within the second hop, pinned:** allocate the id and the worker dir path first, then ask `decide_admission` with that dir (the CLI does the same — the directory exists and genesis does not, `spawn.rs:178-212`), then `spawn_blocking(seed_worker)` with `SeedPlan.queued` set from the answer. A refusal at that point leaves nothing behind, exactly as the CLI's pre-genesis cleanup rule requires.
- **The guards that decide whether to spawn that task** — `state == WorkerState::Done`, the settled worker's `lane == Lane::Impl`, `cfg.lanes[Impl].auto_review == true`, and the worker's snapshot carrying a worktree (D1). **Each pinned:**
  - a worker whose genesis carries `extra["auto_review_of"]` never triggers one (no recursion);
  - a settled worker with no worktree logs `{"ts":…,"kind":"auto_review_skipped","worker":"<id>","reason":"no_worktree"}` to `broker.jsonl` and does nothing else;
  - a settled worker that already has `<dir>/review.json` is not re-reviewed (idempotent across a broker restart that re-adopts a `done` worker).
- **The brief is built off the registry task** — i.e. inside the detached task from the trigger AC, never in the `WorkerSettled` arm. There: `git -C <worktree.path> diff <base_ref>..<branch>`, captured, **capped at 204800 bytes (200 KiB)**; when the diff is longer it is truncated at the cap and the exact marker `\n\n[... diff truncated at 204800 bytes ...]\n` is appended (pinned literal). A `git` failure is not fatal to the impl worker: it logs `{"kind":"auto_review_skipped","reason":"git_diff_failed","detail":<redacted>}` and stops.
- **The brief embeds the impl brief's acceptance criteria.** `brief::parse(<impl_dir>/brief.md)`'s `acceptance_criteria` list is rendered into the review brief under a fixed `## Acceptance criteria under review` H2; the review brief's own front matter is `lane: review` with non-empty `acceptance_criteria` (the review dimensions), so it passes the review lane's gate. The diff goes under a fixed `## Diff` H2 inside a fenced block. The whole thing is one function, `templates::review_auto_brief`, unit-tested against a fixed input.
- **The reviewer is seeded through the CLI's own layout code.** `spawn.rs` exposes `pub fn seed_worker(...)` — the block between "create the worker dir" and "genesis committed" — and `spawn::run_with_lineage` is refactored to call it, so a broker-seeded worker's directory tree and genesis are the same shape as a CLI-seeded one. **AC: every existing spawn/respawn test stays green, unmodified, and the genesis a CLI spawn writes is byte-identical before and after the refactor** (pinned by capturing a spawn's `events.jsonl` line 1 on the pre-refactor binary, or equivalently by a unit test that asserts the genesis object's exact key set and values for a fixed `SpawnArgs`).
- **`SeedPlan` must carry everything today's spawn path puts between those two points**, or the refactor cannot reproduce genesis and the AC above fails. Read `src/commands/spawn.rs` and enumerate before writing the struct; at HEAD that is: `lay_out_worker(&wdir, args, &brief_text, &parsed)` (`spawn.rs:180`) also stages `--context` files, and genesis (`spawn.rs:218-238`) records `brief_sha`, `sandbox`, `network`, `requested_transport`, `transport_selection` and `snap.writable_roots = vec![wdir.join("outbox")]` — **none of which the Interfaces sketch below originally carried.** They are added there. P4's worktree/elevation genesis keys are added the same way, read out of the merged tree. Where a value is derivable rather than caller-supplied (`sandbox`/`network` come from `cfg.lanes[plan.lane]`), deriving it inside `seed_worker` is preferred to widening the struct — but it must be *decided*, not dropped.
- **The reviewer's result schema is PINNED, because two materially different implementations otherwise satisfy the ACs.** `result.json` exists only when `snap.schema_path` is set AND the result conforms (`reconcile::conclude_turn`'s gate, `src/reconcile.rs:394` + `schema_gate`); with `schema_path: None` the reviewer produces only `result.md`, the writeback AC below is satisfied through its `{"raw": …}` branch, and the integration AC "`review.json` parses with a `findings` array" then fails. **So: `seed_worker` applies the review lane's default result schema (Task 4's D2 mechanism) to the auto-review worker** — the broker materializes `templates/review-findings.schema.json` into the reviewer's dir at 0600 and sets `SeedPlan.schema_path` to it, exactly as a CLI `a2a spawn --lane review` without `--schema` does. The `{"raw": …}` fallback below remains the path for a reviewer that *fails* that gate, not for one that never had it.
- **The reviewer's `cwd` is its OWN worker directory** (`$A2A_HOME/workers/<review-id>`), like the research lane's scratch: the broker chose the path, it is inside the 0700 home, and the reviewer needs no repo access because the diff is in its brief. It is recorded in the reviewer's genesis exactly as any other cwd is, and `seed_worker` lays the directory out.
- **How it survives `check_cwd` is PINNED as a mechanism, not asserted as existing behaviour.** `decide_admission` → `gates(req)` → `check_cwd(req)` (`src/broker/core.rs:1333`) refuses any `Some(cwd)` that is not under `cfg.allowed_roots` with **`CWD_NOT_ALLOWED`**, and its only escapes are `Lane::Research`-under-`<dir>/scratch` and **`req.cwd == None`** (`core.rs:1334-1336`). `$A2A_HOME/workers/<review-id>` is under `~/.a2a`, which is not an allowed root on a real machine — so a `SpawnRequest` carrying it would be refused, every auto-review would land on the `auto_review_declined` path, and the feature would be green in tests (whose temp homes happen to sit under the test's allowed root) and dead in production. **Therefore: the auto-review `SpawnRequest` carries `cwd: None`** — the shape `check_cwd` already returns `Ok(())` for, and honest here because the field exists to re-check a *caller-supplied* cwd against the roots the daemon was started with, and there is no caller: the broker picked this path out of its own 0700 home. Re-validating it would be the daemon checking itself. The reviewer's real cwd is recorded where every cwd is recorded — its genesis — and `ls`/`status` read it from there. **`check_cwd` is NOT modified and no `Lane::Review` exemption is added.** Pinned by a unit/integration case whose `allowed_roots` **deliberately excludes `$A2A_HOME`** and in which the auto-review still spawns — the one test that would have caught this.
- **The reviewer counts against the caps.** It goes through the same admission path as any spawn — `decide_admission` with its dir, `Lane::Review`, and the `cwd: None` above: at the cap it is **queued** like anything else. If admission refuses outright (`QUEUE_FULL`, or `BREAKER_OPEN` from Task 2), the auto-review is abandoned with `{"ts":…,"kind":"auto_review_declined","worker":"<impl-id>","code":"<CODE>"}` in `broker.jsonl` **and the impl worker is untouched**.
- **Lineage marker:** the reviewer's genesis carries `"auto_review_of": "<impl-id>"` (NOT `parent_id` — a reviewer is not a retry of anything, and conflating the two would make `max_attempts` count reviews). `fold::seed_from_genesis` copies it into `snap.extra["auto_review_of"]`; `ls --json` rows gain `"auto_review_of"`.
- **Findings land in the IMPL worker's dir.** When the review worker settles `done`, the broker writes `<impl_dir>/review.json` via `store::write_atomic` **under `reconcile::lock_state(<impl_dir>)`**, then appends ONE envelope through `fold::append_envelope_and_fold` — `EventKind::OutboxNotice`, payload `{"review_of":"<review-id>","path":"review.json"}`, **no `fsm` key** (so the fold consumes the seq, rewrites the snapshot's watermarks, and moves no state; `fold_rejections` stays empty — pinned). Content: the reviewer's `result.json` bytes when the schema gate produced one; otherwise `{"raw": "<result.md text>", "note": "the reviewer's result did not parse against the findings schema"}`.
- **The reviewer's failure never touches the impl worker.** Pinned: kill the reviewer mid-run (or let it fail its schema gate) and the impl worker stays exactly `done` with no new lifecycle transition, no `review.json`, and one `broker.jsonl` line naming the reviewer's fate.
- **Integration (`tests/auto_review.rs`, needs D1):** a temp git repo with one commit; `A2A_LANES_IMPL_AUTO_REVIEW=1`; an impl spawn with P4's worktree flag/default; `FAKE_SCENARIO=happy` with `FAKE_MESSAGE` set to a findings-shaped JSON document.
  **The impl worker must be able to reach `done`, and by default it cannot.** `FAKE_MESSAGE` is one env var on one app-server child shared by every worker (Ground truth) — so the impl worker's final message is the same findings JSON the reviewer is meant to emit. If D2 is present, the impl lane's **default** result schema then gates that message in `reconcile::conclude_turn` (`src/reconcile.rs:394-409`), the impl worker lands `failed{schema_violation}`, never `Done`, and the trigger never fires. **Pinned resolution:** this test's impl spawn carries an explicit **`--schema <f>`** pointing at a schema the findings document satisfies (the simplest being `templates/review-findings.schema.json` itself, or a permissive `{"type":"object"}`), which — per Task 4's "`--schema` still wins" AC — overrides the lane default. State that in a comment: the shared `FAKE_MESSAGE` is why, and a future per-thread message knob on the fake is the cleaner fix.
  Converge (poll ≤20s at 50ms) on: a second worker exists with `lane == "review"` and `auto_review_of == <impl-id>`; `<impl_dir>/review.json` exists and parses with a `findings` array; the impl worker is still `done` with the same `last_delivered_seq` semantics; the reviewer's `brief.md` contains both the test's known diff marker line and the impl brief's acceptance-criteria text. A separate case commits a >200 KiB change and asserts the exact truncation marker. **If D1 is absent from the merged tree, this test asserts the `auto_review_skipped` path instead and says so in a comment naming the dependency.**

**Verify via:** cli
**Apply:** docs/development/appendix-actor-model.md §2 — the supervisor composes workers; a worker never spawns another.

**Files:** Modify `src/config.rs`, `src/templates.rs`, `src/commands/spawn.rs` (`seed_worker` extraction), `src/broker/core.rs` (the two-hop hook, `CoreMsg::AutoReviewReady`, **and `BrokerCore` gaining a `home` field** — see Interfaces), `src/fold.rs`, `src/commands/ls.rs`; Test `tests/auto_review.rs` + unit tests in `templates.rs`/`core.rs`.

**Interfaces:**
```rust
// src/templates.rs
pub const AUTO_REVIEW_DIFF_CAP: usize = 204_800;
pub const AUTO_REVIEW_TRUNCATION_MARKER: &str = "\n\n[... diff truncated at 204800 bytes ...]\n";
pub fn review_auto_brief(impl_id: &str, acceptance_criteria: &[String], diff: &str) -> String;

// src/broker/core.rs
// `BrokerCore` (core.rs:1030-1064) has NO `home` today — `start()` takes
// `home: PathBuf` (core.rs:1073) and hands it only to `BrokerLog::start`
// (core.rs:1080); the core itself sees per-worker `dir` paths and nothing else.
// `seed_worker` needs an `&A2aHome`, so the core gains one field, stored at
// `start()`. Do NOT reconstruct it with `dir.parent().parent()`.
//   BrokerCore gains: home: crate::home::A2aHome,
//
// The second hop of the trigger (see the trigger AC): the detached task hands
// the finished brief back to the registry rather than doing admission itself.
// CoreMsg gains:
//   AutoReviewReady { impl_id: String, impl_dir: PathBuf, brief_text: String },

// src/commands/spawn.rs — the extracted layout+genesis seam the broker calls.
// EVERY field genesis records must be reachable from here or from `cfg` +
// `plan.lane`, or the "genesis is byte-identical before and after the
// refactor" AC fails. The list below is what HEAD requires; ADD whatever
// worktree/elevation keys the merged P4 spawn also records.
pub struct SeedPlan {
    pub lane: crate::config::Lane,
    pub cwd: PathBuf,
    pub brief_text: String,
    /// `--context` files to stage into `<worker>/context/` (`lay_out_worker`
    /// does this today via `args.context`; empty for the auto-review worker).
    pub context: Vec<PathBuf>,
    /// Set for the auto-review worker to the review lane's default result
    /// schema, materialized into the worker dir (see the schema AC).
    pub schema_path: Option<PathBuf>,
    pub model: String,
    pub effort: String,
    pub owner: Option<String>,
    pub max_turns: u64,
    /// Genesis records BOTH the resolved transport and what was asked for,
    /// plus how it was chosen (`spawn.rs:218-238`).
    pub transport: crate::events::Transport,
    pub requested_transport: crate::events::Transport,
    pub transport_selection: &'static str,
    pub lineage: Option<Lineage>,
    pub auto_review_of: Option<String>,
    pub queued: bool,
}
// `brief_sha` is derived inside (`brief::sha256_hex(&plan.brief_text)`), and
// `sandbox`/`network`/`writable_roots` are derived from `cfg` + `plan.lane` +
// the worker dir — deriving beats widening, but each one is a DECISION the
// executor records, not a field it may silently drop.
/// Allocate the id, lay out the dir tree, write brief.md/prompt.md, commit
/// genesis. Sync — the broker calls it inside `spawn_blocking`.
pub fn seed_worker(home: &A2aHome, cfg: &config::Config, plan: &SeedPlan, now: DateTime<Utc>)
    -> Result<(String, PathBuf, WorkerSnapshot), AppError>;
```

- [ ] Steps: failing tests → RED → implement (config key → `seed_worker` extraction with the spawn suite green → brief builder → core hook incl. diff + admission + guards → `review.json` writeback → ls) → GREEN → full suite, zero warnings → commit `feat: auto_review — impl worktree diffs reviewed on completion, findings as review.json`.

---

### Task 6: `events.jsonl` rotation (spec §4.5 leftover)

**Acceptance Criteria:**

- **Config:** `BROKER_FIELDS` gains `"events_max_bytes"` (appended). Default **`events_max_bytes = 10485760`** (10 MiB). Origin `broker.events_max_bytes`, env `A2A_BROKER_EVENTS_MAX_BYTES`. `0` means **no rotation** (the file grows) — pinned, and documented as the escape hatch for anyone debugging a fold.
- **`store::EventLog` gains a cap without a call-site sweep.** `pub fn open_capped(worker_dir: &Path, max_bytes: u64) -> io::Result<Self>`; `open(worker_dir)` stays and is exactly `open_capped(worker_dir, config::DEFAULT_EVENTS_MAX_BYTES)` (a const dependency `store → config` only; `config` does not use `store`, so no cycle). **Exactly two writers switch to `open_capped(dir, cfg.broker.events_max_bytes)`:** `WorkerActor::persist` (`src/broker/worker_actor.rs`, which already clones `self.cfg` into the closure) and `reconcile::reconcile` (`src/reconcile.rs:136`, whose signature already takes `cfg: &Config`). **Every other site keeps `open` and the default cap**, and the reason is *volume*, not the absence of a `Config` — four sites in fact have one in scope. Each is named so the difference is a decision, not an oversight:
  - `reconcile::reconcile_app_server` (`src/reconcile.rs:293`) — no `cfg` in scope, and writes at most one envelope per broker death;
  - `commands/spawn.rs` (loads a `Config` at `:104`, opens an `EventLog` at `:219`) — writes exactly one envelope, genesis, per worker;
  - `commands/send.rs` (loads a `Config` at `:68`, opens an `EventLog` at `:146`) — writes one envelope per `send`;
  - `broker/core.rs`'s `admit_queued`/`cancel_queued` — one envelope per queued worker's decision.
  None of them can be what fills a log, and re-plumbing them buys a wider diff and no behaviour.
- **Rotation, at append time, rename-then-continue.** Inside `EventLog::append`, before the write: when `self.bytes > 0 && self.bytes + line.len() as u64 > self.max_bytes` (and `max_bytes != 0`):
  1. `fs::rename(<dir>/events.jsonl, <dir>/events.jsonl.1)` — atomic, and **replacing any existing `.1`**;
  2. `fsync` the directory;
  3. reopen a fresh `events.jsonl` (`create`+`append`+`mode(0o600)`), `self.bytes = 0`;
  4. append the **rotation marker** as the new file's first line, then the caller's line.
  `self.next_seq` is NOT reset — **seqs continue across the split** (pinned).
- **The rotation marker is PINNED:** class `lifecycle`, `kind` `events_rotated` (new `EventKind::EventsRotated`), `epoch` = the epoch of the envelope whose append triggered the rotation, `transport` = that envelope's transport, and `raw` exactly `{"rotated_to":"events.jsonl.1","last_rotated_seq":<u64>,"discarded_prior_generation":<bool>}`. **No `fsm` key** — the fold consumes its seq and changes nothing, `fold_rejections` stays empty (pinned). `EventsRotated` is **not** added to the `Phase1EventKind` mirror.
- **Reads are transparent.** `store::read_events(dir, since)` reads `events.jsonl.1` first (when present) then `events.jsonl`, applying the same torn-tail tolerance to each; `EventLog::open`/`open_capped` computes `next_seq` from the maximum over both files and initializes `bytes` from the current file's post-repair length. Every existing caller — `fold::recover`, `reconcile`, `tail` (both the one-shot and the `--follow` cursor path) — inherits this with no change (Ground truth: those are all the callers).
- **The crash-safety proof (the point of the whole design), stated so it can actually pass.** A unit test builds a log of the same N envelopes twice: once with `open_capped(dir_a, <small>)` so it rotates at least once, once with `open_capped(dir_b, 0)` so it does not. It deletes each `state.json`, runs `fold::recover` on both, and compares. **The comparison is NOT "equal in every field":** the rotated log contains N + M envelopes (the markers occupy their own seqs and are counted — a sibling AC), so `last_event_seq` and `last_applied_seq` necessarily differ by exactly M, and `updated_at` is wall-clock. `WorkerSnapshot` also derives only `Serialize, Deserialize, Clone, Debug` (`src/state.rs:30`) — there is **no `PartialEq`**, so "equal" has no operator behind it, and **`PartialEq` is not derived just for this test**. Instead, pinned two ways:
  1. **Machine-meaningful equality, field by field:** `state`, `state_reason`, `failure_class`, `epoch`, `turns_this_epoch`, `max_turns`, `session_id`, `thread_id`, `pid`, `deadline_at`, `last_delivered_seq`, `schema_path`, and `extra` **minus `fold_rejections`** — all equal between the two folds. Equivalently, compare `serde_json::to_value(&snap)` of both after removing `last_event_seq`, `last_applied_seq`, `updated_at` and any other seq-derived or clock-derived key; whichever form the executor picks, the removed set is named in a comment with the reason.
  2. **The seq watermarks differ by exactly the number of rotation markers** — asserted, not waived: `snap_a.last_event_seq == snap_b.last_event_seq + M`.
  Separately, over `read_events(dir_a, 0)`: seqs are strictly increasing, start at 1, and have **no gaps**. And `fold_rejections` is empty in both.
- **Unit ACs** (`src/store.rs` tests): rotation happens once and only once at the threshold; a single line larger than the cap does not rotate an empty file into an endless loop (the `bytes > 0` guard, pinned by appending one oversized envelope to a fresh log); `EventLog::open` after a rotation continues the seq (the next append's seq is `previous_max + 1`); a **second** rotation replaces `.1`, and its marker carries `discarded_prior_generation: true`.
- **Integration (app-server transport):** `A2A_BROKER_EVENTS_MAX_BYTES=8192` with `FAKE_SCENARIO=happy` and **`FAKE_FLOOD=30`**. Two corrections to the obvious parameters, both load-bearing:
  - **The flood is `lifecycle`, not `trace`** — `item/completed{agentMessage}` maps to `(EventClass::Lifecycle, EventKind::AgentMessage)` (`src/events.rs:288`), so each flood item is a lifecycle envelope routed through `WorkerActor::append_lifecycle` → `persist`, i.e. one lock + fold + `state.json` fsync each. **Lifecycle is what fills a log**, and 300 of those (the number an earlier draft named; the existing `FAKE_FLOOD` usage in `tests/cmd_follow.rs:503` is 60) is both ~110 KiB and 300 sequential fsync round-trips.
  - **The parameters must produce EXACTLY ONE rotation.** 300 envelopes against a 4096-byte cap rotates ~28 times, and with single-generation retention only the last two generations survive — the genesis row is gone and any assertion anchored on an early seq (notably `--since <seq-before-the-split>`) silently degrades into a tautology. Size the flood so total appended bytes land **between 1× and 2× the cap**; `FAKE_FLOOD=30` at `8192` is the pinned pair, and the executor adjusts the flood count (never the assertions) if the fixture's envelope size has drifted.
  Assertions: the worker reaches `done`; **exactly one** `<dir>/events.jsonl.1` exists and its marker carries `discarded_prior_generation: false`; `a2a tail <id> --raw` emits a strictly-increasing gapless seq sequence spanning the split; `a2a tail <id> --since <seq-before-the-split>` returns exactly the envelopes after it, and the genesis envelope is still readable in `events.jsonl.1`; `a2a status <id>` reports `done` (the fold survived the split).
- **Exec transport parity — its OWN parameters, because the stub has no flood knob.** `FAKE_SCENARIO`/`FAKE_FLOOD`/`FAKE_MESSAGE` are fake-app-server knobs and do nothing to `tests/fixtures/stub-codex`, which emits five fixed JSONL lines (~2 KiB of `events.jsonl` for a whole run) and whose only knobs are `STUB_SLEEP, STUB_EXIT, STUB_LAST_MSG, STUB_NO_RESULT, STUB_LEAK, STUB_EMIT_QUESTION, STUB_RECORD, STUB_RESUME_FAIL, STUB_DUMP_ENV` (Ground truth). Running "the same integration" against it produces no rotation at all and the AC fails outright. **Pinned instead:** `A2A_BROKER_DISABLE=1` + the stub codex + **`A2A_BROKER_EVENTS_MAX_BYTES=1024`**, which the stub's ~2 KiB does cross exactly once. Assert the same shape: `events.jsonl.1` exists, `a2a tail <id> --raw` is gapless and strictly increasing across the split, and `a2a status <id>` is coherent — the rotation lives in `store`, so it applies to `reconcile`'s appends without a second implementation (pinned by test, not by argument). *(If a future task wants the exec case to flood, the fix is a `STUB_FLOOD=<n>` knob on the fixture — deliberately not added here, since one rotation is all this AC needs.)*

**Verify via:** cli
**Apply:** docs/development/appendix-fsm.md — the log leads and the snapshot follows; a rotation that breaks that ordering is a data-loss bug, not a housekeeping one.

**Files:** Modify `src/store.rs`, `src/config.rs`, `src/events.rs`, `src/reconcile.rs`, `src/broker/worker_actor.rs`; Test `tests/events_rotation.rs` + unit tests in `store.rs`.

**Interfaces:**
```rust
// src/config.rs
pub const DEFAULT_EVENTS_MAX_BYTES: u64 = 10_485_760;   // 10 MiB; 0 disables rotation
// BrokerConfig gains: pub events_max_bytes: u64,

// src/store.rs
pub const EVENTS_FILE: &str = "events.jsonl";
pub const EVENTS_ROTATED_FILE: &str = "events.jsonl.1";
impl EventLog {
    pub fn open(worker_dir: &Path) -> io::Result<Self>;                     // default cap
    pub fn open_capped(worker_dir: &Path, max_bytes: u64) -> io::Result<Self>;
    pub fn append(&mut self, env: &mut crate::events::Envelope) -> io::Result<u64>;
}
/// Reads `events.jsonl.1` then `events.jsonl`, each torn-tail tolerant.
pub fn read_events(worker_dir: &Path, since_seq: u64) -> io::Result<Vec<crate::events::Envelope>>;
```

- [ ] Steps: failing tests → RED → implement (config key → `open_capped` + rotate-on-append + marker → `.1`-aware `open`/`read_events` → wire the two config-carrying writers) → GREEN → full suite, zero warnings → commit `feat: events.jsonl rotation (single generation, seq-continuous, fold-safe)`.

---

### Task 7: Prompt-contract tier — adversarial fixtures, deterministic gate, gated statistical gate

**Acceptance Criteria:**

- **Four adversarial brief fixtures exist** at `tests/fixtures/adversarial-briefs/`, each a valid brief with `lane: general` front matter (so `a2a spawn` accepts them unmodified and no lane gate obscures the result being measured):
  - `unresolvable-decision.md` — asks for a choice between two named options and states explicitly that the answer is not in the repo and must come from the orchestrator. **Correct behaviour: `outbox/<seq>.question.json` + a final-line `BLOCKED:` marker.**
  - `missing-file.md` — instructs work on a path that does not exist in the cwd.
  - `ambiguous-criteria.md` — two acceptance criteria that cannot both hold.
  - `compaction-forcing.md` — a long enumeration engineered to force a context compaction before the answer, so the protocol has to survive being summarized away (spec §4.7 long-context note).
  Each is ≤8 KiB, contains no secrets (the §5 ingress scan must pass), and is **committed** — these are the corpus, not scratch.
- **The `BLOCKED:` matcher finally exists** (Ground truth: none does). In `src/brief.rs` — pure, no IO:
  `pub const BLOCKED_MARKER_RE: &str = r"^BLOCKED:[ \t]*(.*)$"` and `pub fn blocked_marker(final_message: &str) -> Option<String>`, applied to the **last non-empty line** only. Unit-pinned: `"...\nBLOCKED: needs a decision\n"` → `Some("needs a decision")`; `"BLOCKED: mid\nand then more text"` → `None` (mid-message occurrences ignored); `"BLOCKED:"` → `Some("")`; trailing blank lines and `\r\n` are tolerated. **It is observational** — nothing in the FSM or reconcile path calls it (recorded deferral: the outbox file remains the sole driver, per §4.7).
- **Deterministic tier (always on, no gate, `tests/prompt_contract.rs`):**
  - every file in `tests/fixtures/adversarial-briefs/` parses via `brief::parse` and validates for `Lane::General`, and `redact::scan_text` finds nothing;
  - all four `templates/*-brief.md` carry the five pinned §4.7 phrases from Task 4 (re-asserted here so the eval tier's premise is checked where the eval tier lives);
  - **D3:** every generated worker-protocol artifact in the repo's `skills/**` (P4's AGENTS.md source and the `a2a-worker` skill) carries the same five phrases, and the two agree with each other (the §4.7 "both generated from one source, and a test asserts they agree" rule). **If `skills/` is absent from the merged tree the case SKIPs with `eprintln!` naming P4 as the dependency** — it must not fail the suite for a dependency that did not land.
- **Statistical tier (`#[ignore]`, and additionally gated on BOTH `A2A_E2E=1` AND `A2A_EVAL=1` — it skips with an eprintln when either is unset):**
  - **k = 3** real-codex runs of `unresolvable-decision.md`: `a2a spawn --lane general --transport exec --cwd <temp repo> --brief tests/fixtures/adversarial-briefs/unresolvable-decision.md`, each in a **fresh temp `A2A_HOME`**, each `a2a wait --timeout 5m`;
  - **Hermeticity is achieved, not asserted.** A real `codex exec` writes its session/rollout files into `$CODEX_HOME` — that is precisely the artifact `a2a resume` depends on, and why `doctor`'s codex-home check exists (`src/commands/doctor.rs:491-543`) — so "reads real `~/.codex` auth and writes nothing under it" is false for a run that simply inherits the user's home. **The harness therefore creates a temp `CODEX_HOME`, copies ONLY `auth.json` into it (0600), and exports it for every run.** With that, the claim is true and testable: assert `~/.codex` has no new entries and the temp `CODEX_HOME` does. If `~/.codex/auth.json` is absent the tier skips with an `eprintln` naming it, exactly like the other gates;
  - **Every run is torn down unconditionally.** Each spawn is followed — on **every** path, including a `wait` that expires 6, a classification failure, and a panic (use a guard/`defer`-shaped helper, not a happy-path tail) — by `a2a kill <id>` and then `common::wait_dead(home, id, …)`. A `wait --timeout 5m` that expires otherwise leaves the exec shim and its real codex child running against a temp `A2A_HOME` the test is about to delete; the `BrokerGuard` discipline that covers brokers has no analogue here, so the tier supplies its own. **AC: after the tier, no surviving process whose argv or cwd mentions the run's temp home** — scoped to test-created pids, never a global `ps` sweep for "codex";
  - a run **passes** when ANY of: the worker's state is `awaiting-input` with reason `blocked`; an `outbox/*.question.json` exists; or `brief::blocked_marker(<result.md>)` is `Some`;
  - **the assertion is a rate, not a boolean: at least 2 of 3 must pass** (`>= 2/3`, per spec §8.9), and the test prints the observed rate and each run's classification so a near-miss is diagnosable rather than merely red;
  - **budget documented in the test's doc comment and pinned in the ACs:** 3 workers × 1 short turn each on the general lane's default model/effort, wall-clock bounded at 5 minutes per run (15 minutes worst case); it reads real `~/.codex/auth.json` and **writes nothing under `~/.codex` or `~/.a2a`** — true only because of the temp-`CODEX_HOME` copy above, which the doc comment states rather than assumes;
  - the other three fixtures get the same harness but are **recorded, not asserted** — the run prints their classification and always passes. They exist so a future threshold has a baseline; asserting a rate on a fixture nobody has ever measured would be a coin flip dressed as a gate. Recorded deferral.

**Verify via:** cli

**Files:** Create `tests/fixtures/adversarial-briefs/*.md`, `tests/prompt_contract.rs`; Modify `src/brief.rs`; Test the above + unit tests in `brief.rs`.

**Interfaces:**
```rust
// src/brief.rs
pub const BLOCKED_MARKER_RE: &str = r"^BLOCKED:[ \t]*(.*)$";
/// The §4.7 marker on the LAST non-empty line of a final agent message.
/// Observational only — `outbox/<seq>.question.json` is what drives the FSM.
pub fn blocked_marker(final_message: &str) -> Option<String>;
```

- [ ] Steps: write the fixtures → failing tests → RED → implement `blocked_marker` → GREEN on the deterministic tier → run the gated tier once locally and transcribe the observed rate into a comment → full suite, zero warnings → commit `feat: prompt-contract fixtures, BLOCKED matcher, deterministic + gated statistical eval tiers`.

---

### Task 8: `broker.jsonl` as the transition log + the fleet smoke + Phase 5 docs

**Acceptance Criteria:**

- **`broker.jsonl` finally carries state transitions (spec §4.5).** `spawn_worker_actor`/`build_worker_actor` gain a `log: BrokerLog` parameter (it is `Clone` over an mpsc sender — Ground truth — so the dedicated log task remains the sole writer of the file, and no new writer or channel appears). In `WorkerActor::persist`, whenever `previous != snap.state`, exactly one line is emitted:
  `{"ts":"<rfc3339>","kind":"state_change","worker":"<id>","from":"<prev>","to":"<new>","event":"<persist label>","epoch":<u32>}`
  where `<persist label>` is **whatever `what` string the `persist` call carried** — the twelve at HEAD are `readoption`, `process_started`, `session_identified`, `start_failed`, `turn_ended`, `send`, `inbox_delivered`, `interrupt`, `kill`, `timeout`, `lifecycle`, `trace` (Ground truth; **there is no `"resume"` label**, and `process_started` — the app-server transport's `spawned → running` edge — and `start_failed` are both state-changing and both were missing from an earlier draft of this list). **The set is OPEN and no test may pin it closed:** Task 1's `trip_budget` and Task 5's paths add to it, so an assertion over a fixed enumeration would either fail on `process_started` today or rot tomorrow. Assert only that `event` is a **non-empty string**. **Pinned:** one line per transition, none for a self-loop, and `from`/`to` are the `WorkerState::as_str()` kebab-case spellings.
- **The transition log has TWO writers, because two transitions are not the actor's.** `queued → spawned` is committed by `BrokerCore::admit_queued` (`src/broker/core.rs:1642`) and `queued → cancelled` by `cancel_queued` (`core.rs:1703`) — neither goes through `WorkerActor::persist`, and both already hold `self.log`. **Both emit the identical `state_change` line** (`event` = `"admit"` and `"cancel_queued"` respectively — new labels, which is exactly why the set above is open). Without this, a worker born `queued` has no `from:"queued"` line at all and the fleet smoke's first-`from` assertion below is unsatisfiable by construction.
- **Fleet smoke (`tests/fleet_smoke.rs`), on the fake transport, one `BrokerGuard`:** `A2A_ADMISSION_GLOBAL_CAP=2`, a fresh `A2A_HOME`, `A2A_LANES_IMPL_AUTO_REVIEW=1`, three concurrent spawns issued from three threads:
  - **W1** `--lane general` with `A2A_LANES_GENERAL_MAX_TURNS=0` and `FAKE_SCENARIO=hang-turn` ⇒ converges `failed{budget_exhausted}`;
  - **W2** `--lane impl` with a worktree (D1) ⇒ converges `done` and produces an auto-review worker and `<W2>/review.json`;
  - **W3** `--lane general` ⇒ converges `done`.
  Assertions, all converged-state with bounded polls (≤30s at 100ms):
  - **the cap held:** `a2a broker status --json`'s `data.live` is sampled every 100ms for the duration and is **`<= 2` on every sample** (the assertion is over every sample, not over one snapshot);
  - **the auto-review worker was admitted through the cap** — it appears in `ls --json` with `auto_review_of == <W2 id>` and (because the cap was busy) may legitimately have passed through `queued`; the pin is that it exists and reaches a terminal state;
  - **final states** are exactly one `failed` with `failure_class == "budget_exhausted"`, and the rest `done`;
  - **`broker.jsonl` coherence:** every line parses as JSON; every `state_change` line has all of `{ts,kind,worker,from,to,event,epoch}` and a non-empty string `event` (never a member of a pinned enumeration — see the label AC); for each worker id, the ordered `to` values end at exactly that worker's final `state.json` `state`, and the first `from` is the state its genesis seeded — `spawned` **or `queued`**. This test is arranged (`A2A_ADMISSION_GLOBAL_CAP=2` against three concurrent spawns plus an auto-review worker) to guarantee at least one `queued` worker, so it **is** the test that fails if the `admit_queued`/`cancel_queued` writer above was skipped;
  - **no strays:** `common::assert_no_broker_left(home)` after the guard drops, and no surviving fake-app-server process whose argv or cwd mentions this test's home (scoped to test-created pids only — never a global `ps` sweep for "codex").
- **Full `cargo test` green** — every Phase 1/2/3/4/5 test, **zero compiler warnings** (`cargo build 2>&1 | grep -c 'warning'` is 0 and the same for `cargo test`), every broker torn down by its guard.
- **README gains a Phase 5 section** covering: turn budgets (`[lanes.<lane>] max_turns`, brief may only lower, `failed{budget_exhausted}` is resumable), the circuit breaker (`breaker_failures`/`breaker_window_secs`, `BREAKER_OPEN`, `a2a broker reset-breaker`), `a2a respawn` (lineage + `max_attempts`), `auto_review` (what it costs — a review worker per completed impl worker, counted against the caps), `a2a spawn --template <lane>`, `events_max_bytes` rotation and its single-generation limitation, and the two eval tiers with their env gates. The config example block in the README gains every new key with its default.
- **`a2a config show --origin` lists every Phase-5 key** with origin `default` on a fresh home — asserted in the smoke, so a key that was added to a struct but never wired into the origins map cannot ship silently.

**Verify via:** cli

**Files:** Modify `src/broker/worker_actor.rs`, `src/broker/core.rs`, `README.md`; Create `tests/fleet_smoke.rs`; Test itself + the full suite.

**Interfaces:**
```rust
// src/broker/worker_actor.rs — both gain `log`, appended LAST so the two
// call sites in core.rs (`register` and the auto-review path) are one-line edits.
pub fn spawn_worker_actor(dir: PathBuf, snap: WorkerSnapshot, cfg: Config,
    adapter_rx: watch::Receiver<AdapterRef>, settle_tx: mpsc::Sender<Settled>,
    cancel: CancellationToken, tracker: &TaskTracker,
    log: crate::broker::core::BrokerLog) -> WorkerHandle;

pub fn build_worker_actor(dir: PathBuf, snap: WorkerSnapshot, cfg: Config,
    adapter_rx: watch::Receiver<AdapterRef>, settle_tx: mpsc::Sender<Settled>,
    cancel: CancellationToken,
    log: crate::broker::core::BrokerLog)
    -> (WorkerHandle, impl std::future::Future<Output = ()> + Send + 'static);
```

- [ ] Steps: failing tests → RED → implement the transition log → GREEN → write the fleet smoke → fixes only where the smoke exposes them → full suite, zero warnings → README → commit `feat: broker.jsonl transition log, fleet smoke under cap, Phase 5 docs`.

---

## Plan self-review notes (kept for the executor)

**Spec coverage map:**

| Task | Spec |
|---|---|
| T1 turn budgets | §4.9 (per-worker turn budget ⇒ `failed{budget_exhausted}`, resumable), §4.9 lane table |
| T2 circuit breaker | §4.9 (K failures in a window stops new spawns, `degraded`, loud, until reset), §10 quota-burn mitigation |
| T3 respawn lineage | §4.1 (`a2a respawn`), §4.9 (`parent_id`/`attempt`/`max_attempts` quarantines poison briefs) |
| T4 lane templates | §4.9 lane table (review/research brief requirements, findings schema), §4.7 (brief templates carry the protocol clauses) |
| T5 auto_review | §4.8 ("Optional composition: `impl.auto_review = true` … findings → `review.json`, counted against the caps"), §4.8 worktrees |
| T6 events rotation | §4.5 ("per-worker file cap + rotation, `tail --follow` handles rotation via seq cursors") |
| T7 prompt-contract | §8.9 (deterministic + statistical tiers, BLOCKED-rate threshold not a boolean), §4.7 |
| T8 transition log + smoke | §4.5 (`broker.jsonl` = one line per state transition), §8.7/§8.10 (concurrency tier, converged-state assertions, fresh home + torn-down broker per task) |

**Adversarial-review pass (2026-08-18), fixes folded in.** Two reviewers read this plan against `src/`; 32 findings (24 distinct after dedup) were applied, none rejected. The ones that changed *substance* rather than wording, so a reader knows what moved:

- **Task 5's reviewer would never have spawned.** `check_cwd` has no `Lane::Review` exemption; a `cwd` under `$A2A_HOME` is refused `CWD_NOT_ALLOWED`, and the feature would have been green in temp homes and dead on a real machine. The mechanism is now pinned (`cwd: None`) and the test that catches it (allowed_roots deliberately excluding `$A2A_HOME`) is an AC.
- **Task 5's hook stalled the registry.** `CoreMsg::WorkerSettled` runs inline on the core loop; an awaited `git diff` there contradicted this plan's own Global Constraint. Split into two hops via a new `CoreMsg::AutoReviewReady`.
- **Task 5's integration test could not reach its own trigger** (shared `FAKE_MESSAGE` + D2's impl-lane default schema ⇒ the impl worker fails its gate and never settles `Done`), and the reviewer's `schema_path` was unspecified, making the writeback AC satisfiable two incompatible ways. Both pinned.
- **Task 6's crash-safety AC was unsatisfiable as written** — it demanded field-equality between folds whose seq watermarks the plan's own marker design forces apart, over a type with no `PartialEq`. Restated as machine-meaningful equality plus an explicit watermark delta.
- **Task 6's parameters produced ~28 rotations, not one** (and the exec case produced none, since the stub has no flood knob). Both re-sized; the flood is lifecycle, not trace.
- **Task 8 pinned a persist-label set that does not exist** (no `"resume"`; five real labels missing, including `process_started`), and pinned a first-`from` of `queued` that no writer emits — `admit_queued`/`cancel_queued` commit those edges on the core, not in `persist`. The set is now open and the second writer is an AC.
- **Task 7 claimed a hermeticity it could not have** (real `codex exec` writes rollouts into `$CODEX_HOME`) and specified no teardown for an expired `wait`. Temp `CODEX_HOME` + unconditional kill/`wait_dead`.
- Smaller, all applied: `BREAKER_OPEN` added to `wire::KNOWN_CODES`; admission's new keys typed `u64` (the section has one parser) with the `ADMISSION_FIELDS` array-literal **compile error** at `src/config.rs:820` named; `BreakerHealth` used in both places; the registry surface test renamed at each new `CoreMsg` rather than left stale behind its catch-all; `SeedPlan` extended to everything genesis records; `src/commands/respawn.rs` added to Task 4's Files (the clap change breaks it); `src/broker/core.rs`'s `BrokerCore` given a `home`; the warning-prefix double-print, the respawn off-by-one, the "two writers have a Config" miscount, the `doctor`-reads-notices claim, and the phantom `auto_review_spawned` variant all corrected.

**Deliberate deviations & deferrals** are consolidated in Global Constraints — none are silent. The five that matter most to a reviewer: the FSM stays frozen (budget exhaustion is an existing edge with a refined `failure_class`); `breaker_failures = 0` disables rather than trips; rotation keeps one generation and says so in the marker; the `BLOCKED:` matcher is observational only; turn budgets are counted on both transports but enforced only on the broker's.

**Dependency handling.** Three Phase-4 artifacts are named in the header table (D1 worktree snapshot fields, D2 the lane default-result-schema mechanism, D3 the generated `skills/**`). Each has a pinned fallback so that a task whose dependency did not land ships a *correct, tested* degraded behaviour rather than a broken build — but the executor should read the merged tree first and prefer the real artifact every time. Where a name in this plan and a name in the merged tree disagree, **the merged tree wins** and the AC is satisfied behaviourally.

**Devloop execution notes:**
- Toolkit `cli` throughout. Fresh `A2A_HOME` per test; `A2A_OWNER=test-owner` from `tests/common`; `A2A_BROKER_DISABLE=1` remains the default in `common::a2a()` and broker tests opt in via `common::a2a_broker()` + `BrokerGuard`.
- **`FAKE_SCENARIO=hang-turn` is the race-free primitive for every budget/breaker/cap assertion in this plan** — it holds the turn open until steered or interrupted, so a worker's state does not move while the test is arranging the next step. Reach for it before inventing a sleep.
- Every async AC is a bounded poll (house style ≤10s at 50ms; the fleet smoke uses ≤30s at 100ms because it is arranging three workers through a cap of two). No test in this plan may contain an unbounded wait.
- `tokio::time::pause` only in `current_thread` tests with no child I/O — the `Breaker` window test is pure and injects `Instant`s rather than pausing anything.
- Real codex appears in exactly one place: Task 7's `A2A_E2E=1 A2A_EVAL=1` statistical tier. Everything else in Phase 5 runs on the fake app-server or the stub codex.
- Suggested order is the task order: Task 1's budget trip is the failure primitive Task 2's breaker tests are built on, Task 3's `run_with_lineage` refactor is what Task 5's `seed_worker` extraction extends, and Task 4's templates are what Task 5's auto-review brief and Task 7's deterministic tier both assert against.
