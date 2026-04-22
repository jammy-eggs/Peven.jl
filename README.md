# Peven.jl

A colored Petri net execution engine in Julia.

This README covers the engine layer: net construction, token movement, scheduler semantics, validation, and the public API.

## Install

```julia
using Pkg
Pkg.add("Peven")
```

## Quickstart

```julia
using Peven

register_executor!(:transform, FunctionExecutor((_, tokens) ->
    Token(:intermediate, run_key(tokens[1]), string(tokens[1].payload, " -> processed"))
))
register_executor!(:measure, FunctionExecutor((_, tokens) ->
    Token(:output, run_key(tokens[1]), length(String(tokens[1].payload)))
))

# Define the net: input -> transform -> intermediate -> measure -> output
places = Dict(
    :input        => Place(:input),
    :intermediate => Place(:intermediate),
    :output       => Place(:output),
)

transitions = Dict(
    :step    => Transition(:step, :transform),
    :measure => Transition(:measure, :measure),
)

arcsfrom = [ArcFrom(:step, :input), ArcFrom(:measure, :intermediate)]
arcsto   = [ArcTo(:step, :intermediate), ArcTo(:measure, :output)]

net = Net(places, transitions, arcsfrom, arcsto)

# Seed the marking with a token
marking = Marking(Dict(
    :input => Token[Token(:default, "run1", "payload")],
))

# Validate
issues = validate(net, marking)
isempty(issues) || error(issues[1].message)

# Run
results = fire(net, marking; max_concurrency=1)
@assert results[1].status === :completed
@assert length(results[1].final_marking.tokens_by_place[:output]) == 1
```

## Engine Model

Peven is a colored Petri net engine where Places hold state, transitions do work, and tokens carry data through the graph.

- **Places** are states or buffers.
- **Transitions** consume tokens from input Places and produce tokens into output Places. In practice, a firing is an executor call.
- **Arcs** connect Places to transitions and transitions back to Places. The graph is always bipartite, and each arc has a weight.
- **Marking** is the current distribution of tokens across Places.
- **Bundle** is the unit the scheduler actually fires. A `BundleRef` is `(transition_id, run_key, selected_key, ordinal)`, with `selected_key === nothing` for unkeyed transitions.

A practical token schema is:

- `run_key` = the batch or rollout boundary
- payload field used by `join_by` = the per-item correlation key inside a batch
- `color` = routing / debugging / token-type signal
- `payload` = the content guards and executors inspect

A transition is hot when at least one bundle is ready. Firing claims one concrete bundle eagerly, runs its executor, and commits outputs on completion. Guards are bundle-scoped: a guard returning `false` blocks that bundle, not the whole run. Overlap is defined on disjoint bundles, not just on `(transition, run_key)`.

## Net Construction Patterns

- **Branch / `tee`** -- the canonical one-to-many fork idiom. A transition with multiple output Places must emit explicit per-place outputs, typically `Dict(place => Token[...])`. Nothing is duplicated implicitly; if two downstream branches need the same information, the executor must emit two tokens.
- **Self-loop** -- deposit back into an input Place when the graph itself should keep evolving, such as revise-until-good loops and iterative refinement.
- **Retries** -- use transition `retries` when you mean "retry the same firing after failure", not "send the work back around the graph".
- **Keyed join** -- use `Transition(...; join_by=(place_id, token) -> key)` to correlate multi-place inputs within one `run_key`. `join_by` is classification, not evaluation.

For keyed joins:

- keep one `run_key` per batch
- store the per-item key in the token payload
- keep the selector pure, deterministic, and non-`nothing`
- treat `ArcFrom` declaration order as non-semantic; bundle ordering is canonicalized by Place id and token order

## Scheduler Semantics

Transitions look up executors by `executor::Symbol` in a process-global registry. For custom executors, subtype `AbstractExecutor`, extend `Peven.execute`, and register an instance with `register_executor!`. Use `unregister_executor!` for cleanup, or pass `fire(...; executors=Dict(...))` to inject executors without touching global registry state.

Executor outputs follow these rules:

- single-output transitions may return one token or a vector of tokens
- multi-output transitions must return outputs keyed by destination place, covering every declared output Place exactly once
- returning `nothing` is valid only for transitions with no output Places
- tokens are linear: if two downstream branches need the same data, the executor must emit two distinct tokens, even if they share payload
- executors and guards receive `Vector{T}` inputs; under keyed joins, that vector is the selected bundle
- outputs must stay inside the initial seeded `run_key` set; emitting a token with a new `run_key` is a launched-firing failure

Runtime behavior:

- only transitions affected by the marking change are rechecked
- `BundleRef` is snapshot-scoped, so using it against a later marking is stale by definition
- `grab(...)` returns `nothing` for a stale bundle
- `take(...)` throws `ArgumentError("stale or unavailable bundle")` for a stale bundle
- `fire` treats retries as new launches for fuse accounting
- `hot` and `cold` are stateless snapshot helpers
- `on_event` is observational only; ordinary hook exceptions are swallowed and never fail the scheduler

Events and traces:

- `TransitionStarted`, `TransitionCompleted`, and `TransitionFailed` are emitted only for launched firings
- `GuardErrored(bundle, error)` and `SelectionErrored(transition_id, run_key, error)` report scheduler-time failures before or outside launched firings
- completed events and `TransitionResult`s carry outputs keyed by destination place
- each launched firing has a stable `firing_id`; retries increment `attempt` while keeping that same `firing_id`
- `TransitionResult.trace` contains launched firings only; guard and selection observations do not create fake trace rows

## Result Semantics

`fire(...)` returns `RunResult`s ordered lexicographically by `run_key`. `run_keys(marking)` preserves first-seen order inside a marking.

Run statuses:

- `completed` -- all tokens reached terminal Places
- `failed` -- a selector errored, an executor failed after retries were exhausted, or an active guard exception remained at shutdown
- `incomplete` -- tokens remain in nonterminal Places because no transition is enabled or the fuse budget was exhausted

`RunResult.terminal_reason` is one of `:selection_error`, `:executor_failed`, `:guard_error`, `:fuse_exhausted`, or `:no_enabled_transition`.

Under keyed joins, `RunResult.status` is intentionally a pessimistic run-level roll-up. Use `result.trace`, `result.terminal_bundle`, `result.terminal_transition`, and the firing helper functions for the per-firing source of truth.

## Public API

### Markings and readiness

`run_keys(marking)`, `run_marking(marking, run_key)`, `enablement(net, marking)`, `hot(net, marking)`, `cold(net, marking)`, `grab(marking, net, bundle)`, `take(marking, net, bundle)`

### Running and token movement

`fire(net, marking; ...)`, `drop(marking, net, tid, token)`, `drop(marking, net, tid, outputs)`, `drop(marking, net, tid, outputs_by_place)`, `misfire(marking, consumed)`

### Result helpers

`completed_firings(result)`, `failed_firings(result)`, `fuse_blocked_firings(result)`, `firing_result(result, firing_id)`, `firing_status(result, firing_id)`

## Performance

Peven uses precomputed per-transition influence sets (LoLA 2's incremental enablement technique) to avoid re-scanning all transitions after every firing. After transition `t` fires, only `recheck[t] = t ∪ upstream[t] ∪ downstream[t]` is re-scanned, where `upstream[t]` means the input-side contender transitions that may have lost enablement and `downstream[t]` means transitions that may have gained enablement from `t`'s outputs. Cost per firing is bounded by local connectivity, not net size.

## Validation

`validate(net)` checks structural integrity: key consistency, place endpoint references, orphan places, keyed-join structure, duplicate input arcs, duplicate output arcs, and input arc weights that exceed bounded place capacity.
`validate(net, marking)` adds marking checks (unknown places, capacity) and reachability (dead transitions).

Structural keyed-join issues are reported by `validate(...)`; `Net(...)` construction keeps only local field invariants.

Current validation rules include:

- `join_by` is valid only on transitions with at least two unique input Places
- duplicate input arcs for the same `(transition, place)` are rejected; use `weight` instead
- duplicate output arcs for the same `(transition, place)` are rejected; use one output arc per destination place
- input arc weights may not exceed the capacity of a bounded connected Place

```julia
issues = validate(net, marking)
# Returns Vector{ValidationIssue} -- empty means valid
```

## Tests

```bash
julia --project -e 'using Pkg; Pkg.test()'
```

## License

MIT. See [LICENSE](LICENSE).
