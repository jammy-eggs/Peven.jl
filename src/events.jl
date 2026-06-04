"""
    Events emitted during engine execution via the onEvent hook
    Pass onEvent = e -> push!(log, e) to fire() to capture them
"""
abstract type EngineEvent end

"""
    Emitted when a transition begins executing
    inputs contains the tokens reserved for this firing
"""
struct TransitionStarted{T<:AbstractToken} <: EngineEvent
    ctx::ExecutionContext{T}
end

"""
    Emitted when a transition completes successfully
    outputs contains the executor's returned tokens
"""
struct TransitionCompleted{T<:AbstractToken} <: EngineEvent
    bundle::Bundle
    firingId::Int
    attempt::Int
    outputs::Vector{T}
end

"""
    Emitted when a transition fails
    retrying is true if the firing will be re-attempted, false if retries are exhausted
"""
struct TransitionFailed <: EngineEvent
    bundle::Bundle
    firingId::Int
    attempt::Int
    error::String
    retrying::Bool
end

"""
    Emitted when a guard throws during enablement evaluation.
    Guard exceptions are scheduler observations, not launched firings.
"""
struct GuardErrored <: EngineEvent
    bundle::Bundle
    error::String
end

"""
    Emitted when joinBy throws or returns nothing before a bundle can be formed.
"""
struct SelectionErrored <: EngineEvent
    transitionId::Symbol
    runKey::String
    error::String
end

@inline emit(::Nothing, _) = nothing
@inline emit(hook, event) = hook(event)

"""
    Record of a terminal transition outcome kept in a run trace
    status is :completed or :failed
    outputs holds the executor's returned tokens on success, error holds the message on failure
    Failed entries capture launched firings that ended in terminal failure
"""
struct TransitionResult{T<:AbstractToken}
    bundle::Bundle
    firingId::Int
    status::Symbol
    outputs::Vector{T}
    error::Union{Nothing, String}
    attempts::Int

    function TransitionResult(
        bundle::Bundle,
        firingId::Int,
        status::Symbol,
        outputs::Vector{T},
        error::Union{Nothing, String},
        attempts::Int,
    ) where {T<:AbstractToken}
        new{T}(bundle, firingId, status, outputs, error, attempts)
    end
end

"""
    Final result for one runKey after the engine finishes
    status is :completed, :failed, or :incomplete
    reason explains why: :executorFailed, :guardError, :fuseExhausted, :noEnabledTransition
    trace holds the recorded outcomes for this runKey in engine order
    retrying failures are emitted as events but are not retained in trace
    terminal failures, including retries blocked by fuse exhaustion, are retained in trace
    finalMarking keeps only this runKey's tokens when fire() stopped
"""
struct RunResult{T<:AbstractToken}
    runKey::String
    status::Symbol
    error::Union{Nothing, String}
    reason::Union{Nothing, Symbol}
    trace::Vector{TransitionResult{T}}
    finalMarking::Marking{T}

    function RunResult(
        runKey::String,
        status::Symbol,
        error::Union{Nothing, String},
        reason::Union{Nothing, Symbol},
        trace::Vector{TransitionResult{T}},
        finalMarking::Marking{T},
    ) where {T<:AbstractToken}
        new{T}(runKey, status, error, reason, trace, finalMarking)
    end
end

"""
    Emitted once per runKey when the engine finishes processing it
"""
struct RunFinished{T<:AbstractToken} <: EngineEvent
    result::RunResult{T}
end
