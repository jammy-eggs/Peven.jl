# Changelog

## v0.4.0

- Make scheduling bundle-first: the engine now reasons in concrete `BundleRef`s instead of plain `(transition, run_key)` pairs
- Add keyed joins with `Transition(...; join_by=(place_id, token) -> key)` while keeping `run_key` as the outer run/result boundary
- Add `enablement(net, marking)` plus bundle-scoped `hot`, `grab`, and `take`
- Add bundle-aware observability: launched-firing events and `TransitionResult` now carry `bundle::BundleRef`
- Add `SelectionErrored` for selector failures that happen before bundle formation
- Keep `TransitionResult.trace` launched-firings-only, while `RunResult` now exposes `terminal_bundle` and `terminal_transition`
- Make `emit` best-effort so `on_event` hook exceptions never fail the scheduler
- Harden the scheduler lifecycle so task launch, commit, retry, and shutdown stay supervisor-owned and rollback-safe
- Convert post-executor commit errors like invalid vector outputs, capacity failures, and unknown run keys into launched-firing failures instead of crashing `fire()`
- Replace bundle-centric post-run helpers with firing-centric helpers: `completed_firings`, `failed_firings`, `fuse_blocked_firings`, `firing_result`, and `firing_status`
- Add `TransitionResult(status=:fuse_blocked)` so fuse-blocked retries are visible in the public trace and distinct from ordinary failures
- Change overlap semantics from “same `(transition, run_key)`” to “disjoint bundles may overlap”
- Report duplicate input arcs and invalid keyed-join structure through `validate(...)`; `Net(...)` construction now keeps only local field invariants
- Reject duplicate input arcs for the same `(transition, place)`; use `weight` instead
- Make `ArcFrom` declaration order non-semantic for joins by canonicalizing bundle selection order
- Add validation for arc weights that exceed bounded place capacity
- Reject executor-emitted run keys outside the initial run set in v0.4
- Keep executor failure isolation bundle-scoped for scheduling, while run status remains a pessimistic roll-up per `run_key`
- Defer read arcs out of scope; the design questions around bundle identity, selection sharing, snapshot consistency, and fairness are tracked in README for a later release

## v0.3.0

- Add `GuardErrored` for scheduler-time guard exceptions
- Stop allocating firing ids or synthetic `TransitionFailed` / trace rows for guard exceptions
- Make `TransitionResult.trace` represent launched firings only
- Finalize unresolved guard exceptions as `RunResult(status=:failed, terminal_reason=:guard_error)`
- Close fuse-blocked retries with a real failed firing record, `TransitionFailed(retrying=false)`, and restored inputs

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
