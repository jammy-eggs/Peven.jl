"""
    Events emitted during engine execution via the on_event hook
    Pass `on_event = e -> push!(log, e)` to `fire()` to capture them
    Event hooks are observational only; ordinary hook exceptions are swallowed
"""
abstract type EngineEvent end

"""
    Emitted when a transition begins executing
    bundle identifies the launched bundle and inputs contains the reserved tokens for that firing
"""
struct TransitionStarted{T<:AbstractToken} <: EngineEvent
    bundle::BundleRef
    firing_id::Int
    attempt::Int
    inputs::Vector{T}
end

"""
    Emitted when a transition completes successfully
    bundle identifies the launched bundle and outputs contains the executor's return tokens
"""
struct TransitionCompleted{T<:AbstractToken} <: EngineEvent
    bundle::BundleRef
    firing_id::Int
    attempt::Int
    outputs::Vector{T}
end

"""
    Emitted when a launched bundle fails
    retrying is true if the firing will be re-attempted, false if retries are exhausted or fuse blocked
"""
struct TransitionFailed <: EngineEvent
    bundle::BundleRef
    firing_id::Int
    attempt::Int
    error::String
    retrying::Bool
end

"""
    Emitted when a bundle's guard throws during enablement evaluation
    Guard exceptions are scheduler observations, not launched firings
"""
struct GuardErrored <: EngineEvent
    bundle::BundleRef
    error::String
end

"""
    Emitted when join_by classification throws before a bundle can be formed
"""
struct SelectionErrored <: EngineEvent
    transition_id::Symbol
    run_key::String
    error::String
end

@inline emit(::Nothing, _) = nothing
@inline function emit(hook, event)
    try
        hook(event)
    catch e
        e isa InterruptException && rethrow()
        nothing
    end
end

"""
    Record of one launched firing's terminal lifecycle outcome kept in a run trace
    status is :completed, :failed, or :fuse_blocked
    bundle identifies the launched bundle
    outputs holds the executor's return tokens on success, error holds the message on failure
    retries are summarized into attempts; trace stores one row per launched firing_id
"""
struct TransitionResult{T<:AbstractToken}
    bundle::BundleRef
    firing_id::Int
    status::Symbol
    outputs::Vector{T}
    error::Union{Nothing,String}
    attempts::Int
end

"""
    Final result for one run_key after the engine finishes
    status is :completed, :failed, or :incomplete
    terminal_reason explains why:
      :selection_error, :executor_failed, :guard_error, :fuse_exhausted, :no_enabled_transition
    terminal_bundle is populated for bundle-scoped terminal reasons like :executor_failed or :guard_error
    terminal_transition is populated for pre-classification failures like :selection_error
    trace contains launched firings only; it does not include guard or selection observations
    final_marking keeps only this run_key's tokens when fire() stopped
"""
struct RunResult{T<:AbstractToken}
    run_key::String
    status::Symbol
    error::Union{Nothing,String}
    terminal_reason::Union{Nothing,Symbol}
    terminal_bundle::Union{Nothing,BundleRef}
    terminal_transition::Union{Nothing,Symbol}
    trace::Vector{TransitionResult{T}}
    final_marking::Marking{T}
end

"""
    Emitted once per run_key when the engine finishes processing it
"""
struct RunFinished{T<:AbstractToken} <: EngineEvent
    result::RunResult{T}
end

completed_firings(result::RunResult) = TransitionResult[
    step for step in result.trace if step.status === :completed
]

failed_firings(result::RunResult) = TransitionResult[
    step for step in result.trace if step.status === :failed
]

fuse_blocked_firings(result::RunResult) = TransitionResult[
    step for step in result.trace if step.status === :fuse_blocked
]

function firing_result(result::RunResult, firing_id::Int)
    for step in result.trace
        step.firing_id == firing_id && return step
    end
    return nothing
end

firing_status(result::RunResult, firing_id::Int) = begin
    step = firing_result(result, firing_id)
    isnothing(step) ? nothing : step.status
end
