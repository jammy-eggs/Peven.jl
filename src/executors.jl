"""
    Base type for transition executors.
    Extend `execute(executor, tid, tokens)` or `execute(executor, ctx)` for custom executors.
"""
abstract type AbstractExecutor end

"""
    ExecutionContext{T<:AbstractToken}

Public executor context carrying the firing metadata known by the engine.
"""
struct ExecutionContext{T<:AbstractToken}
    transition_id::Symbol
    bundle::BundleRef
    firing_id::Int
    attempt::Int
    tokens::Vector{T}
end

execute(e::AbstractExecutor, tid::Symbol, tokens::Vector{<:AbstractToken}) =
    error("implement execute(::$(typeof(e)), ...)")
execute(e::AbstractExecutor, ctx::ExecutionContext) = execute(e, ctx.transition_id, ctx.tokens)

function register_executor! end
function unregister_executor! end
function resolve_executor end

"""
    Wrap a `(tid, tokens) -> token_or_tokens` callable as an executor.
"""
struct FunctionExecutor <: AbstractExecutor
    fn::Function
end
execute(e::FunctionExecutor, tid::Symbol, tokens::Vector{<:AbstractToken}) = e.fn(tid, tokens)

let registry = Dict{Symbol,AbstractExecutor}()
    """
        Register an executor under a name.
        The registry is process-global, so callers should clean up reused names.
    """
    global function register_executor!(name::Symbol, executor::AbstractExecutor)
        registry[name] = executor
        return nothing
    end

    """
        Remove a registered executor by name.
    """
    global unregister_executor!(name::Symbol) = !isnothing(pop!(registry, name, nothing))

    global function resolve_executor(name::Symbol, ::Nothing)
        haskey(registry, name) || throw(KeyError("no executor registered for :$name"))
        return registry[name]
    end

    global function resolve_executor(
        name::Symbol,
        executors::AbstractDict{Symbol,E},
    ) where {E<:AbstractExecutor}
        haskey(executors, name) || throw(KeyError("no executor provided for :$name"))
        return executors[name]
    end
end
