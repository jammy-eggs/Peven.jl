# Changelog

## v0.5.0

This release rebuilds the engine from the v0.2.0 base.

Breaking changes:

- Make `Bundle(transitionId, runKey, selectedKey)` the scheduling unit.
- Use `(bundle, firingId)` as the concrete launched firing identity.
- Route executors through `execute(executor, ctx)` with `ExecutionContext(bundle, firingId, attempt, inputs)`.
- Keep `misfire(marking, inputs)` for returning reserved inputs after a failed firing.
- Rename executor registry helpers to `registerExec!` and `getExec`.
- Keep `unregisterExec!` internal and unexported.

Engine semantics:

- Add scheduler-time `GuardErrored` without synthetic firing records.
- Add keyed joins with `Transition(...; joinBy=(place, token) -> key)`.
- Add `SelectionErrored` for `joinBy` failures.
- Add optional input arcs with `ArcFrom(...; optional=true)`.
- Keep tokens in optional input places live for run accounting.
- Let repeated enablement after reservation handle same-run concurrency.
- Return reserved inputs when a firing terminally fails before output commit.
- Validate executor output run keys against the firing bundle run key.

Input and output shape:

- Keep executor inputs grouped by place in `ctx.inputs`.
- Add explicit per-place dict executor outputs.
- Keep scalar and vector executor outputs as single-output-place forms.
- Require dict outputs to provide every output place with exactly its `ArcTo` weight.
- Allow explicit multi-place dispatch by returning the same token under multiple output places.

Validation and tests:

- Reject duplicate same transition/place input arcs; use one weighted `ArcFrom`.
- Reject duplicate same transition/place output arcs; use one weighted `ArcTo`.
- Cover bundle identity, keyed joins, optional inputs, duplicate arcs, output shape, wrong run keys, failure restoration, and same-run concurrency.

## v0.2.0

- Allow overlapping firings of the same `(transition, runKey)` when enough input tokens exist.
- Add vector executor outputs for transitions with exactly one output arc of weight `1`.
- Track `firingId` and `attempt` across events and terminal trace records.
- Keep retry inputs reserved across attempts, while restoring the marking if fuse exhaustion blocks a retry.
- Clarify fuse semantics: it counts launches, including retries, while letting in-flight work drain.

## v0.1.0

- Colored Petri net core: places, transitions, arcs, tokens, and markings.
- Incremental enablement with precomputed influence sets.
- Validation for key consistency, arc endpoints, orphan places, capacity, and reachability.
- Executor registry with `FunctionExecutor`.
- Event hooks for transition start, transition completion, transition failure, and run finish.
- Guard-based routing on transitions.
- Copy-on-write marking operations.
- `runKey` partitioning for batch isolation.
