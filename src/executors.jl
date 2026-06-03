"""
    Executors run the actual work for a Transition (LLM call, tool, judge)
    Subtype AbstractExecutor and extend Peven.execute(executor, tid, tokens) to use your own.
    Executors may return one token or a vector of tokens.
"""
abstract type AbstractExecutor end

execute(e::AbstractExecutor, tid::Symbol, tokens::Vector{<:AbstractToken}) =
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
    Look up a registered executor by name, throws KeyError if not found
"""
function getExec(name::Symbol)
    haskey(registry, name) || throw(KeyError("no executor registered for :$name"))
    return registry[name]
end

"""
    Wraps any (tid, tokens) -> token callable as an executor
    Useful for tests and simple inline executors without defining a struct
"""
struct FunctionExecutor <: AbstractExecutor
    fn::Function
end
execute(e::FunctionExecutor, tid::Symbol, tokens::Vector{<:AbstractToken}) = e.fn(tid, tokens)
