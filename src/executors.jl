"""
    Executors run the actual work for a Transition (LLM call, tool, judge)
    Subtype AbstractExecutor and extend Peven.execute(executor, tid, tokens) to use your own.
    Executors may return one token or a vector of tokens.
"""
abstract type AbstractExecutor end

execute(e::AbstractExecutor, tid::Symbol, tokens::Vector{<:AbstractToken}) =
    error("implement execute(::$(typeof(e)), ...)")

const EXECUTOR_REGISTRY = Dict{Symbol, AbstractExecutor}()

"""
    Register an executor under a name so Transitions can reference it
    register_executor!(:agent, MyAgentExecutor()) makes Transition(:gen, :agent) use it
    The registry is process-global, so tests and applications should clean up names they reuse
"""
register_executor!(name::Symbol, executor::AbstractExecutor) =
    (EXECUTOR_REGISTRY[name] = executor; nothing)

"""
    Look up a registered executor by name, throws KeyError if not found
"""
function get_executor(name::Symbol)
    haskey(EXECUTOR_REGISTRY, name) || throw(KeyError("no executor registered for :$name"))
    return EXECUTOR_REGISTRY[name]
end

"""
    Wraps any (tid, tokens) -> token callable as an executor
    Useful for tests and simple inline executors without defining a struct
"""
struct FunctionExecutor <: AbstractExecutor
    fn::Function
end
execute(e::FunctionExecutor, tid::Symbol, tokens::Vector{<:AbstractToken}) = e.fn(tid, tokens)
