"""
    Base type for events emitted via `fire(...; on_event=...)`.
"""
abstract type EngineEvent end

"""
    Event emitted when a transition begins executing.
"""
struct TransitionStarted{T<:AbstractToken} <: EngineEvent
    bundle::BundleRef
    firing_id::Int
    attempt::Int
    inputs::Vector{T}
end

"""
    Event emitted when a transition completes successfully.
"""
struct TransitionCompleted{T<:AbstractToken} <: EngineEvent
    bundle::BundleRef
    firing_id::Int
    attempt::Int
    outputs::Dict{Symbol,Vector{T}}
end

"""
    Event emitted when a launched bundle fails.
"""
struct TransitionFailed <: EngineEvent
    bundle::BundleRef
    firing_id::Int
    attempt::Int
    error::String
    retrying::Bool
end

"""
    Event emitted when a bundle guard throws during enablement evaluation.
"""
struct GuardErrored <: EngineEvent
    bundle::BundleRef
    error::String
end

"""
    Event emitted when `join_by` throws before a bundle can be formed.
"""
struct SelectionErrored <: EngineEvent
    transition_id::Symbol
    run_key::String
    error::String
end

"""
    Terminal outcome for one launched firing recorded in a run trace.
"""
struct TransitionResult{T<:AbstractToken}
    bundle::BundleRef
    firing_id::Int
    status::Symbol
    outputs::Dict{Symbol,Vector{T}}
    error::Union{Nothing,String}
    attempts::Int
end

"""
    Final result for one `run_key` after the engine stops.
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
    Event emitted once per `run_key` when processing finishes.
"""
struct RunFinished{T<:AbstractToken} <: EngineEvent
    result::RunResult{T}
end

completed_firings(result::RunResult{T}) where {T<:AbstractToken} = TransitionResult{T}[
    step for step in result.trace if step.status === :completed
]

failed_firings(result::RunResult{T}) where {T<:AbstractToken} = TransitionResult{T}[
    step for step in result.trace if step.status === :failed
]

fuse_blocked_firings(result::RunResult{T}) where {T<:AbstractToken} = TransitionResult{T}[
    step for step in result.trace if step.status === :fuse_blocked
]

function firing_result(result::RunResult{T}, firing_id::Int) where {T<:AbstractToken}
    for step in result.trace
        step.firing_id == firing_id && return step
    end
    return nothing
end

firing_status(result::RunResult{T}, firing_id::Int) where {T<:AbstractToken} = begin
    step = firing_result(result, firing_id)
    isnothing(step) ? nothing : step.status
end
