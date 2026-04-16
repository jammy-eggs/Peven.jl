"""
    Events emitted during engine execution via the on_event hook
    Pass on_event = e -> push!(log, e) to fire() to capture them
"""
abstract type EngineEvent end

"""
    Emitted when a transition begins executing
    inputs contains the tokens that were grabbed from input places
"""
struct TransitionStarted <: EngineEvent
    transition_id::Symbol
    run_key::String
    inputs::Vector{<:AbstractToken}
end

"""
    Emitted when a transition completes successfully
    output is the token that will be dropped into output places
"""
struct TransitionCompleted <: EngineEvent
    transition_id::Symbol
    run_key::String
    output::AbstractToken
end

"""
    Emitted when a transition fails
    retrying is true if the transition will be re-attempted, false if retries are exhausted
"""
struct TransitionFailed <: EngineEvent
    transition_id::Symbol
    run_key::String
    error::String
    retrying::Bool
end

@inline emit(::Nothing, _) = nothing
@inline emit(hook, event) = hook(event)

"""
    Record of a terminal transition outcome kept in a run trace
    status is :completed or :failed
    output holds the produced token on success, error holds the message on failure
    Failed entries capture terminal executor failures and guard errors
"""
struct TransitionResult
    transition_id::Symbol
    run_key::String
    status::Symbol
    output::Union{Nothing, AbstractToken}
    error::Union{Nothing, String}

    function TransitionResult(
        transition_id::Symbol,
        run_key::String,
        status::Symbol,
        output::Union{Nothing, AbstractToken},
        error::Union{Nothing, String},
    )
        new(transition_id, run_key, status, output, error)
    end
end

"""
    Final result for one run_key after the engine finishes
    status is :completed, :failed, or :incomplete
    terminal_reason explains why: :executor_failed, :fuse_exhausted, :no_enabled_transition
    trace holds the recorded outcomes for this run_key in engine order
    retrying failures are emitted as events but are not retained in trace
    final_marking keeps only this run_key's tokens when fire() stopped
"""
struct RunResult
    run_key::String
    status::Symbol
    error::Union{Nothing, String}
    terminal_reason::Union{Nothing, Symbol}
    trace::Vector{TransitionResult}
    final_marking::Marking

    function RunResult(
        run_key::String,
        status::Symbol,
        error::Union{Nothing, String},
        terminal_reason::Union{Nothing, Symbol},
        trace::Vector{TransitionResult},
        final_marking::Marking,
    )
        new(run_key, status, error, terminal_reason, trace, final_marking)
    end
end

"""
    Emitted once per run_key when the engine finishes processing it
"""
struct RunFinished <: EngineEvent
    result::RunResult
end
