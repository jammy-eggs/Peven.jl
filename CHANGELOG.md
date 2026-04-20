# Changelog

## v0.2.0

- Allow overlapping firings of the same `(transition, run_key)` when enough input tokens exist
- Add vector executor outputs for transitions with exactly one output arc of weight `1`
- Track `firing_id` and `attempt` across events and terminal trace records
- Keep retry inputs reserved across attempts, while restoring the marking if fuse exhaustion blocks a retry
- Clarify fuse semantics: it now counts launches, including retries, while letting in-flight work drain

## v0.1.0

- Colored Petri net core: Places, Transitions, Arcs, Tokens, Markings
- Engine with incremental enablement (LoLA 2 influence-set technique)
- Precomputed indexes: input_arcs, output_arcs, affected_transitions, upstream, downstream
- Validation: key consistency, arc endpoints, orphan places, capacity, reachability
- Executor registry with FunctionExecutor convenience wrapper
- Event hooks: TransitionStarted, TransitionCompleted, TransitionFailed, RunFinished
- Guard-based routing on transitions
- Copy-on-write marking semantics (grab/drop/misfire)
- run_key partitioning for batch isolation
