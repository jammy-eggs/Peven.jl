# Peven.jl

A colored Petri net engine for multi-agent evaluations, written in Julia.

Named after Peven of Aum from Patricia A. McKillip's Riddle-Master trilogy. Riddles have three parts: questions, answers, and strictures. Peven helps you explore evaluations the same way -- ask a question, iterate based on the stricture, and eventually get to an answer. "Beware the unanswered Riddle."

## Bets

1. Petri nets are the best way to express multi-agent evaluations. Every agent loop, adversarial interaction, and multi-turn evaluation is a concurrent stateful system with shared resources. That's what Petri nets were invented to model.

2. Structure where you want it, chaos where you need it. Acyclic nets give you deterministic parallel experiments. Cyclic nets give you dynamic agent loops. One engine, one formalism.

3. What matters in serious evaluations is the shape of interaction: who sees what, in what order, what actions they can take, and how we judge those actions across individual or shared states. Peven favors radical explicitness: evaluation work should never hide inside implicit assumptions.

## Install

```julia
using Pkg
Pkg.add("Peven")
```

## Quickstart

```julia
using Peven

register_executor!(:agent, FunctionExecutor((_, tokens) ->
    Token(:response, run_key(tokens[1]), string(tokens[1].payload, " -> draft"))
))
register_executor!(:judge, FunctionExecutor((_, tokens) ->
    Token(:score, run_key(tokens[1]), length(String(tokens[1].payload)))
))

# Define the net: prompt -> generate -> response -> judge -> scored
places = Dict(
    :prompt   => Place(:prompt),
    :response => Place(:response),
    :scored   => Place(:scored),
)

transitions = Dict(
    :gen  => Transition(:gen, :agent),
    :jdg  => Transition(:jdg, :judge),
)

arcsfrom = [ArcFrom(:gen, :prompt), ArcFrom(:jdg, :response)]
arcsto   = [ArcTo(:gen, :response), ArcTo(:jdg, :scored)]

net = Net(places, transitions, arcsfrom, arcsto)

# Seed the marking with a token
marking = Marking(Dict(
    :prompt => Token[Token(:default, "run1", "Write about testing")],
))

# Validate
issues = validate(net, marking)
isempty(issues) || error(issues[1].message)

# Run
results = fire(net, marking; max_concurrency=1)
@assert results[1].status === :completed
@assert length(results[1].final_marking.tokens_by_place[:scored]) == 1
```

## Concepts

- **Places** -- containers that hold tokens. States or buffers.
- **Transitions** -- actions that consume tokens from input places and produce tokens in output places. In Peven, transitions are executor calls (agents, judges).
- **Arcs** -- directed edges connecting places to transitions and back. Always bipartite. Each arc has a weight (default 1).
- **Tokens** -- colored data flowing through the net. In v0.4, the practical schema is:
  - `run_key` = the batch or rollout boundary
  - `join_by` field in the payload = the per-item correlation key for keyed joins
  - `color` = routing / debugging / token-type signal
  - `payload` = the content you want guards and executors to inspect
- **Marking** -- the distribution of tokens across places at a point in time.
- **Guards** -- functions on transitions that gate firing. Under bundle-first scheduling, a guard that returns `false` blocks that bundle, not the whole run.
- **Bundles** -- the engine's first-class firing unit. A `BundleRef` is `(transition_id, run_key, selected_key, ordinal)`. `selected_key` is `nothing` for unkeyed transitions.
- **Firing rule** -- a transition is hot when at least one bundle is ready. Firing grabs one concrete bundle eagerly, spawns the executor, and drops outputs on completion. Overlap is defined on disjoint bundles, not just on `(transition, run_key)`. Only transitions affected by the marking change are rechecked (incremental enablement).

## Core Patterns

- **`tee`** -- the canonical one-to-many fork idiom. One transition with multiple output arcs emits explicit per-place outputs so downstream branches can consume distinct tokens eagerly.
- **Self-loop** -- a transition deposits back into one of its input Places, which is how Peven expresses retry loops and iterative refinement cycles.
- **Keyed join** -- `Transition(...; join_by=(place_id, token) -> key)` correlates multi-place inputs within a single `run_key`, so one batch can contain many independently joined items.

For keyed pipelines, the default schema is: one `run_key` per batch, one `join_by` field per item, and `color` only for routing/debugging.

### Rubric Pipeline Pattern

This is the canonical eval shape for `tee` plus keyed join:

- `:tee` emits one token to `:problem` and one token to `:rubric_seed`
- `:make_rubric` turns `:rubric_seed` into `:rubric`
- `:judge` consumes from `:problem` and `:rubric` with `join_by=(pid, token) -> token.payload.problem_id`

That keeps problem/rubric pairing correct even when rubric generation completes out of order.

## Engine API

| Function | What it does |
|---|---|
| `run_keys(marking)` | Collect the distinct `run_key`s present in a marking |
| `run_marking(marking, run_key)` | Slice a marking down to one `run_key` |
| `enablement(net, marking)` | Return bundle-level readiness records with `bundle`, `status`, `inputs`, and `error` |
| `hot(net, marking)` | Which concrete `BundleRef`s are ready to fire |
| `cold(net, marking)` | Which transitions have no ready bundle — the inverse of `hot` |
| `grab(marking, net, bundle)` | Return `(new_marking, grabbed, grabbed_by_place)` or `nothing` if that bundle is stale/unavailable |
| `take(marking, net, bundle)` | Same as `grab`, but throws `ArgumentError("stale or unavailable bundle")` |
| `fire(net, marking; ...)` | Run the engine to completion and return `Vector{RunResult}` |
| `drop(marking, net, tid, token)` | Deposit one output token for a single-output transition |
| `drop(marking, net, tid, outputs)` | Deposit many output tokens for a single-output transition |
| `drop(marking, net, tid, outputs_by_place)` | Deposit explicit per-place outputs for a multi-output transition |
| `misfire(marking, consumed)` | Return consumed tokens after executor failure |
| `completed_firings(result)` | Project completed `TransitionResult` rows from a `RunResult` |
| `failed_firings(result)` | Project ordinary failed `TransitionResult` rows from a `RunResult` |
| `fuse_blocked_firings(result)` | Project `TransitionResult` rows that stopped because fuse exhaustion blocked a retry |
| `firing_result(result, firing_id)` | Look up a launched firing by durable `firing_id` |
| `firing_status(result, firing_id)` | Look up the terminal status for one launched firing |

`BundleRef` is snapshot-scoped. Using it against a later marking is stale by definition:

- `grab(...)` returns `nothing`
- `take(...)` throws

Peven keeps scheduler identity internally with admission ids, but the public `BundleRef.ordinal` is intentionally only meaningful for the snapshot that produced it. For durable post-launch inspection, use `firing_id`, not `BundleRef`.
`run_keys(marking)` preserves first-seen order. `fire(...)` returns `RunResult`s ordered lexicographically by `run_key`.

## Performance

Peven uses precomputed per-transition influence sets (LoLA 2's incremental enablement technique) to avoid re-scanning all transitions after every firing. After transition `t` fires, only `recheck[t] = t ∪ upstream[t] ∪ downstream[t]` is re-scanned, where `upstream[t]` means the input-side contender transitions that may have lost enablement and `downstream[t]` means transitions that may have gained enablement from `t`'s outputs. Cost per firing is bounded by local connectivity, not net size.

## Executors

Transitions look up executors by their `executor::Symbol` in a process-global registry.
For custom executors, subtype `AbstractExecutor`, extend `Peven.execute`, and register an instance with `register_executor!`.
The registry itself is private. Use `unregister_executor!` for cleanup, or pass `fire(...; executors=Dict(...))` to inject executors without touching global registry state.
Executors for single-output transitions may return one token or a vector of tokens.
Executors for multi-output transitions must return outputs keyed by destination place, for example `Dict(:left => Token[t1], :right => Token[t2])`.
Tokens are linear: if two downstream branches need the same data, the executor must emit two distinct tokens, even if they share payload.
Executors and guards still receive `Vector{T}` inputs. Under keyed joins that vector is now the selected bundle.
Executors must keep outputs inside the initial `run_key` set for v0.4. Emitting a token with a new `run_key` is treated as a launched-firing failure.
`fire` treats retries as new launches for fuse accounting, and event / trace records expose `bundle`, `firing_id`, plus `attempt` metadata for each launched firing.

## Events and traces

`TransitionStarted`, `TransitionCompleted`, and `TransitionFailed` are emitted through `on_event` for launched firings only, and each now carries a `bundle::BundleRef`.
Guard exceptions emit `GuardErrored(bundle, error)`. Selector exceptions emit `SelectionErrored(transition_id, run_key, error)`.
Completed events and `TransitionResult`s carry committed outputs keyed by destination place.
Each launched firing has a stable `firing_id`, and retries increment `attempt` while keeping that same `firing_id`.
`TransitionResult.trace` contains launched firings only; guard and selection observations do not allocate firing ids or create fake trace rows. Each trace row is one terminal lifecycle row per launched `firing_id`, with `status === :completed`, `:failed`, or `:fuse_blocked`.

`hot` and `cold` remain stateless snapshot helpers. Their optional `on_guard_error` callback is per-call and non-deduplicated.
`fire` owns guard-error lifecycle state and emits `GuardErrored` only when a bundle enters an active guard-error state. `on_event` is observational only; ordinary hook exceptions are swallowed and never fail the scheduler.

## Validation

`validate(net)` checks structural integrity: key consistency, place endpoint references, orphan places, keyed-join structure, duplicate input arcs, duplicate output arcs, and input arc weights that exceed bounded place capacity.
`validate(net, marking)` adds marking checks (unknown places, capacity) and reachability (dead transitions).

Structural keyed-join issues are reported by `validate(...)`; `Net(...)` construction keeps only local field invariants.

v0.4 validation rules include:

- `join_by` is valid only on transitions with at least two unique input Places
- duplicate input arcs for the same `(transition, place)` are rejected; use `weight` instead
- duplicate output arcs for the same `(transition, place)` are rejected; use one output arc per destination place
- input arc weights may not exceed the capacity of a bounded connected Place
- `ArcFrom` declaration order is no longer semantic for joins; bundle ordering is canonicalized by Place id and token order

```julia
issues = validate(net, marking)
# Returns Vector{ValidationIssue} -- empty means valid
```

## Run outcomes

- `completed` -- all tokens reached terminal places
- `failed` -- a selector errored, an executor failed after retries were exhausted, or an active guard exception remained at shutdown
- `incomplete` -- tokens remain in nonterminal places because no transition is enabled or the fuse budget was exhausted

`RunResult.terminal_reason` is one of `:selection_error`, `:executor_failed`, `:guard_error`, `:fuse_exhausted`, or `:no_enabled_transition`.

Under keyed joins, `RunResult.status` is intentionally a pessimistic run-level roll-up. A single failed bundle can make the run `:failed` even if many sibling bundles completed successfully. The trace is the per-firing source of truth:

- `result.trace` tells you what actually launched and how each launched firing ended
- `result.terminal_bundle` points at the bundle-level cause for `:executor_failed` or `:guard_error`
- `result.terminal_transition` points at the transition that raised `:selection_error`
- `completed_firings`, `failed_firings`, `fuse_blocked_firings`, `firing_result`, and `firing_status` are the fast way to inspect keyed runs without replaying the trace yourself

## Keyed-Join Guide

`join_by` is classification, not evaluation. It groups tokens from multiple input Places into correlated bundles inside one `run_key`.

- Keep one `run_key` per batch
- Put the per-item correlation key in the token payload
- Use `join_by=(place_id, token) -> token.payload.item_id` or the equivalent field in your token type
- Keep the selector pure, deterministic, and non-`nothing`; `nothing` is reserved for unkeyed transitions and becomes a selection error at runtime

Canonical ordering for keyed bundles does not depend on `ArcFrom` declaration order. Surviving ready bundles keep queue order across rescans, and newly enabled bundles append at the tail in canonical bundle order.

## Deferred

Read arcs are explicitly out of scope for v0.4. The next design pass needs to answer:

- whether read arcs participate in bundle identity
- whether read-arc consumers share or duplicate bundle selection
- whether read-arc reads are snapshot-consistent with claimed write arcs
- how read arcs affect fairness and overlap when the same tokens are simultaneously observed and claimed

## Tests

```bash
julia --project -e 'using Pkg; Pkg.test()'
```

## License

MIT. See [LICENSE](LICENSE).
