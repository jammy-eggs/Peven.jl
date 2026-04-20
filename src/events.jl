"""
    Events emitted during engine execution via the on_event hook
    Pass on_event = e -> push!(log, e) to fire() to capture them
"""
abstract type EngineEvent end

"""
    Emitted when a transition begins executing
    inputs contains the tokens reserved for this firing
"""
struct TransitionStarted{T<:AbstractToken} <: EngineEvent
    transition_id::Symbol
    run_key::String
    firing_id::Int
    attempt::Int
    inputs::Vector{T}
end

"""
    Emitted when a transition completes successfully
    outputs contains the executor's pre-fan-out outputs
"""
struct TransitionCompleted{T<:AbstractToken} <: EngineEvent
    transition_id::Symbol
    run_key::String
    firing_id::Int
    attempt::Int
    outputs::Vector{T}
end

"""
    Emitted when a transition fails
    retrying is true if the firing will be re-attempted, false if retries are exhausted
"""
struct TransitionFailed <: EngineEvent
    transition_id::Symbol
    run_key::String
    firing_id::Int
    attempt::Int
    error::String
    retrying::Bool
end

"""
    Emitted when a guard throws during enablement evaluation
    Guard exceptions are scheduler observations, not launched firings
"""
struct GuardErrored <: EngineEvent
    transition_id::Symbol
    run_key::String
    error::String
end

@inline emit(::Nothing, _) = nothing
@inline emit(hook, event) = hook(event)

"""
    Record of a terminal transition outcome kept in a run trace
    status is :completed or :failed
    outputs holds the executor's pre-fan-out outputs on success, error holds the message on failure
    Failed entries capture launched firings that ended in terminal failure
"""
struct TransitionResult{T<:AbstractToken}
    transition_id::Symbol
    run_key::String
    firing_id::Int
    status::Symbol
    outputs::Vector{T}
    error::Union{Nothing, String}
    attempts::Int

    function TransitionResult(
        transition_id::Symbol,
        run_key::String,
        firing_id::Int,
        status::Symbol,
        outputs::Vector{T},
        error::Union{Nothing, String},
        attempts::Int,
    ) where {T<:AbstractToken}
        new{T}(transition_id, run_key, firing_id, status, outputs, error, attempts)
    end
end

"""
    Final result for one run_key after the engine finishes
    status is :completed, :failed, or :incomplete
    terminal_reason explains why: :executor_failed, :guard_error, :fuse_exhausted, :no_enabled_transition
    trace holds the recorded outcomes for this run_key in engine order
    trace contains terminal outcomes for launched firings only
    retry attempts that will relaunch are emitted as events only
    terminal failures, including retries blocked by fuse exhaustion, are retained in trace
    final_marking keeps only this run_key's tokens when fire() stopped
"""
struct RunResult{T<:AbstractToken}
    run_key::String
    status::Symbol
    error::Union{Nothing, String}
    terminal_reason::Union{Nothing, Symbol}
    trace::Vector{TransitionResult{T}}
    final_marking::Marking{T}

    function RunResult(
        run_key::String,
        status::Symbol,
        error::Union{Nothing, String},
        terminal_reason::Union{Nothing, Symbol},
        trace::Vector{TransitionResult{T}},
        final_marking::Marking{T},
    ) where {T<:AbstractToken}
        new{T}(run_key, status, error, terminal_reason, trace, final_marking)
    end
end

"""
    Emitted once per run_key when the engine finishes processing it
"""
struct RunFinished{T<:AbstractToken} <: EngineEvent
    result::RunResult{T}
end
