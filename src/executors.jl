"""
    Executors run the actual work for a Transition (LLM call, tool, judge)
    Subtype AbstractExecutor and extend Peven.execute(executor, ctx) to use your own.
    Executors may return one token or a vector of tokens.
"""
abstract type AbstractExecutor end

struct ExecutionContext{T<:AbstractToken}
    bundle::Bundle
    firingId::Int
    attempt::Int
    inputs::Dict{Symbol, Vector{T}}
end

execute(e::AbstractExecutor, ctx::ExecutionContext) =
    error("implement execute(::$(typeof(e)), ...)")

const registry = Dict{Symbol, AbstractExecutor}()

"""
    Register an executor under a name so Transitions can reference it
    registerExec!(:agent, MyAgentExecutor()) makes Transition(:gen, :agent) use it
    The registry is process-global, so tests and applications should clean up names they reuse
"""
registerExec!(name::Symbol, executor::AbstractExecutor) =
    (registry[name] = executor; nothing)

"""
    Remove a registered executor by name, returning true when one was present
"""
unregisterExec!(name::Symbol) = !isnothing(pop!(registry, name, nothing))

"""
    Look up a registered executor by name, throws KeyError if not found
"""
function getExec(name::Symbol)
    haskey(registry, name) || throw(KeyError("no executor registered for :$name"))
    return registry[name]
end

"""
    Wraps any ctx -> token callable as an executor
    Useful for simple inline executors without defining a struct
"""
struct FunctionExecutor <: AbstractExecutor
    fn::Function
end
execute(e::FunctionExecutor, ctx::ExecutionContext) = e.fn(ctx)
