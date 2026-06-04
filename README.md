# Peven.jl

A colored Petri net engine for multi-agent evaluations, written in Julia.

Peven models evaluation runs as explicit places, transitions, arcs, and tokens. It is for workflows where the shape of interaction matters: who sees what, what can run in parallel, where state waits, and what counts as completion.

## Install

```julia
using Pkg
Pkg.add("Peven")
```

## Quickstart

```julia
using Peven

places = Dict(
    :prompt => Place(:prompt),
    :draft => Place(:draft),
    :scored => Place(:scored),
)

transitions = Dict(
    :write => Transition(:write, :agent),
    :score => Transition(:score, :judge),
)

net = Net(
    places,
    transitions,
    [ArcFrom(:write, :prompt), ArcFrom(:score, :draft)],
    [ArcTo(:write, :draft), ArcTo(:score, :scored)],
)

marking = Marking(Dict(
    :prompt => Token[Token(:default, "run1", "Write a test plan")],
))

registerExec!(:agent, FunctionExecutor(ctx -> begin
    input = only(ctx.inputs[:prompt])
    Token(color(input), ctx.bundle.runKey, "draft: $(input.payload)")
end))

registerExec!(:judge, FunctionExecutor(ctx -> begin
    input = only(ctx.inputs[:draft])
    Token(color(input), ctx.bundle.runKey, (score=1.0, text=input.payload))
end))

results = fire(net, marking)
```

## Concepts

- `Place` stores tokens. Optional capacity bounds how many tokens fit.
- `Transition` consumes input tokens and produces output tokens through an executor.
- `ArcFrom` connects a place to a transition. `ArcTo` connects a transition to a place.
- `Token` has `color`, `runKey`, and `payload`.
- `Marking` is the current token distribution by place.
- `Bundle` identifies a logical firing group: `transitionId`, `runKey`, and `selectedKey`.
- `ExecutionContext` is what executors receive: `bundle`, `firingId`, `attempt`, and `inputs`.

## Engine API

| Function | Contract |
|---|---|
| `hot(net, marking)` | Return enabled `Bundle`s. |
| `cold(net, marking)` | Return transitions with no enabled bundle. |
| `grab(marking, net, bundle)` | Reserve the next guard-passing chunk for that bundle and return `(newMarking, inputs)`, or `nothing`. |
| `take(marking, net, bundle)` | Same as `grab`, but throws when the bundle is not enabled. |
| `drop(marking, net, tid, output)` | Commit executor output into transition output places. |
| `misfire(marking, inputs)` | Return grabbed inputs after a failed firing. |
| `fire(net, marking; fuse=1000, maxConcurrency=10, onEvent=nothing)` | Run until each seeded run completes, fails, or becomes incomplete. |
| `registerExec!(name, executor)` | Register an executor for `Transition(..., name)`. |
| `getExec(name)` | Fetch a registered executor. |

## Bundles

Bundles are the engine's scheduling unit. `hot` returns logical bundles, `grab` reserves the next guard-passing token chunk for a bundle, executors receive `ctx.bundle`, and events/results report that same bundle.

```julia
Bundle(:score, "run1", nothing)
```

The fields mean:

- `transitionId`: which transition fires
- `runKey`: which run the firing belongs to
- `selectedKey`: which keyed join group this bundle selected, or `nothing`

Plain transitions have `selectedKey === nothing`:

```julia
hot(net, marking) == [Bundle(:score, "run1", nothing)]
```

Overlapping same-run firings share the same logical bundle in events and results:

```julia
[
    Bundle(:score, "run1", nothing),
    Bundle(:score, "run1", nothing),
]
```

Those are the same logical bundle. They become distinct launched firings through `firingId`:

```julia
(bundle=Bundle(:score, "run1", nothing), firingId=1)
(bundle=Bundle(:score, "run1", nothing), firingId=2)
```

The scheduler gets concurrency by reserving a chunk, launching it, refreshing enablement, and seeing the same logical bundle hot again if more disjoint input tokens remain.

Keyed joins differ by `selectedKey`:

```julia
[
    Bundle(:join, "run1", :itemA),
    Bundle(:join, "run1", :itemB),
]
```

A bundle can reserve tokens from multiple input places. Weighted arcs reserve multiple tokens from one place. Optional arcs add tokens to `ctx.inputs` when present without blocking the bundle when absent.

## Inputs

Required input arcs must be satisfied for a transition to fire:

```julia
ArcFrom(:score, :draft)
ArcFrom(:join, :left, 2)
```

Optional input arcs are consumed when present, but do not block firing:

```julia
ArcFrom(:score, :rubric; optional=true)
```

Tokens in optional input places are still live. If an initial marking contains only an optional-place token for a run, that run is seeded and remains incomplete until something can consume it.

If a transition has only optional input arcs, it is hot only when at least one optional input token is present.

Duplicate input arcs from the same place to the same transition are invalid. Use one weighted arc.

## Keyed Joins

`joinBy` selects matching tokens across input places inside a run:

```julia
Transition(:join, :default; joinBy = (place, token) -> token.payload.item)
```

If `joinBy` throws or returns `nothing`, the affected run fails with a `SelectionErrored` event.

## Outputs

Executors return one of these shapes:

```julia
Token(:default, ctx.bundle.runKey, payload)
Token[...]
Dict(:left => tokenOrVector, :right => tokenOrVector)
```

Scalar and vector outputs are valid only when the transition has exactly one output arc. Dict outputs are the multi-place form: every output place must be present, keys must be `Symbol`s, and each place must receive exactly its `ArcTo` weight.

Use dict outputs to dispatch different tokens to different places:

```julia
FunctionExecutor(ctx -> Dict(
    :agent => Token(:default, ctx.bundle.runKey, "next prompt"),
    :trace => Token(:default, ctx.bundle.runKey, (step=ctx.firingId,)),
))
```

Use the same token in more than one place only when that is the intended model:

```julia
FunctionExecutor(ctx -> begin
    token = Token(:default, ctx.bundle.runKey, "shared")
    Dict(:left => token, :right => token)
end)
```

Executor outputs must keep the firing run key:

```julia
runKey(output) == ctx.bundle.runKey
```

Unknown output places, wrong output counts, capacity failures, and wrong run keys fail the firing without crashing the scheduler.

## Executors

Use `FunctionExecutor` for simple cases:

```julia
registerExec!(:agent, FunctionExecutor(ctx -> Token(:default, ctx.bundle.runKey, "ok")))
```

For custom executors, subtype `AbstractExecutor` and extend:

```julia
Peven.execute(executor::MyExecutor, ctx::ExecutionContext)
```

Retries are transition retries. Retry attempts keep the same `firingId` and `Bundle`; `attempt` increments.

## Events and Results

Pass `onEvent = event -> ...` to `fire`.

Events:

- `TransitionStarted(ctx)`
- `TransitionCompleted(bundle, firingId, attempt, outputs)`
- `TransitionFailed(bundle, firingId, attempt, error, retrying)`
- `GuardErrored(bundle, error)`
- `SelectionErrored(transitionId, runKey, error)`
- `RunFinished(result)`

`RunResult` reports `runKey`, `status`, `error`, `reason`, `trace`, and `finalMarking`.

Statuses:

- `:completed`
- `:failed`
- `:incomplete`

## Validation

`validate(net)` checks net structure. `validate(net, marking)` also checks marking capacity and reachability.

Validation rejects:

- mismatched dict keys
- unknown arc endpoints
- duplicate same transition/place input or output arcs
- transitions with no input arcs
- orphan places
- unknown marking places
- capacity overflow
- unreachable transitions from the current marking

## Performance

Peven precomputes local influence sets. After transition `t` fires, only `recheck[t] = t ∪ upstream[t] ∪ downstream[t]` is scanned for changed enablement.

## Tests

```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

## License

MIT. See [LICENSE](LICENSE).
