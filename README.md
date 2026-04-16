# Peven.jl

A colored Petri net engine for multi-agent evaluations, written in Julia.

Named after Peven of Aum from Patricia A. McKillip's Riddle-Master trilogy. Riddles have three parts: questions, answers, and strictures. Peven helps you explore evaluations the same way -- ask a question, iterate based on the stricture, and eventually get to an answer. "Beware the unanswered Riddle."

## Bets

1. Petri nets are the best way to express multi-agent evaluations. Every agent loop, adversarial interaction, and multi-turn evaluation is a concurrent stateful system with shared resources. That's what Petri nets were invented to model.

2. Structure where you want it, chaos where you need it. Acyclic nets give you deterministic parallel experiments. Cyclic nets give you dynamic agent loops. One engine, one formalism.

3. What matters in serious evaluations is the shape of interaction: who sees what, in what order, what actions they can take, and how we judge those actions across individual or shared states. Peven favors radical explicitness: evaluation work should never hide inside implicit assumptions.

This Julia port is a ground-up rewrite of my original python package optimized around concrete-token fast paths, copy-on-write marking semantics, and precomputed indexes from the Petri net model checking literature. It is the core of Peven, the engine, no extra fat.

## Install

```julia
using Pkg
Pkg.add("Peven")
```

## Quickstart

```julia
using Peven

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
```

## Concepts

- **Places** -- containers that hold tokens. States or buffers.
- **Transitions** -- actions that consume tokens from input places and produce tokens in output places. In Peven, transitions are executor calls (agents, judges).
- **Arcs** -- directed edges connecting places to transitions and back. Always bipartite. Each arc has a weight (default 1).
- **Tokens** -- colored data flowing through the net. Each token has a `color` label and a `run_key` for batch isolation.
- **Marking** -- the distribution of tokens across places at a point in time.
- **Guards** -- functions on transitions that gate firing. A guard that returns false blocks the transition for that run_key.
- **Firing rule** -- a transition is hot when every input place has enough tokens of the same run_key for the arc weight. Firing grabs those tokens eagerly (reservation), spawns the executor, and drops outputs on completion. Only transitions affected by the marking change are rechecked (incremental enablement).

## Engine API

| Function | What it does |
|---|---|
| `fuses(marking)` | Collect unique run_keys from the initial marking |
| `hot(net, marking)` | Which (transition, run_key) pairs are ready to fire |
| `cold(net, marking)` | Which transitions cannot fire — the inverse of `hot` |
| `grab(marking, net, tid, rk)` | Return `(new_marking, grabbed, grabbed_by_place)` or `nothing` if not enabled |
| `take(marking, net, tid, rk)` | Same as `grab`, but throws `ArgumentError` instead of returning `nothing` |
| `fire(net, marking; ...)` | Run the engine to completion and return `Vector{RunResult}` |
| `drop(marking, net, tid, token)` | Deposit output token into output places |
| `misfire(marking, consumed)` | Return consumed tokens after executor failure |

## Performance

Peven uses precomputed per-transition influence sets (LoLA 2's incremental enablement technique) to avoid re-scanning all transitions after every firing. After transition `t` fires, only `recheck[t] = t ∪ upstream[t] ∪ downstream[t]` is re-scanned, where `upstream[t]` means the input-side contender transitions that may have lost enablement and `downstream[t]` means transitions that may have gained enablement from `t`'s outputs. Cost per firing is bounded by local connectivity, not net size.

## Executors

Transitions look up executors by their `executor::Symbol` in a process-global registry.
For custom executors, subtype `AbstractExecutor`, extend `Peven.execute`, and register an instance with `register_executor!`.

## Validation

`validate(net)` checks structural integrity: key consistency, place endpoint references, and orphan places.
`validate(net, marking)` adds marking checks (unknown places, capacity) and reachability (dead transitions).

```julia
issues = validate(net, marking)
# Returns Vector{ValidationIssue} -- empty means valid
```

## Run outcomes

- `completed` -- all tokens reached terminal places
- `failed` -- an executor failed after retries were exhausted
- `incomplete` -- tokens remain in nonterminal places because no transition is enabled or the fuse budget was exhausted

## Tests

```bash
julia --project -e 'using Pkg; Pkg.test()'
```

## License

MIT. See [LICENSE](LICENSE).
