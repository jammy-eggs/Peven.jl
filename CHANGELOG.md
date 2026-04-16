# Changelog

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
