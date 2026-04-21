"""
    Symbol label carried by a token.
"""
const Color = Symbol

"""
    Dispatch boundary for engine-compatible token types.
    Custom token types must implement `color` and `run_key`.
"""
abstract type AbstractToken end
color(t::AbstractToken)::Color = error("implement color(::$(typeof(t)))")
run_key(t::AbstractToken)::String = error("implement run_key(::$(typeof(t)))")

"""
    Default engine token type.
    `run_key` partitions tokens into independent runs.
"""
struct Token <: AbstractToken
    color::Color
    run_key::String
    payload::Any
end
color(t::Token) = t.color
run_key(t::Token) = t.run_key

Token(run_key::String) = Token(:default, run_key, nothing)
Token(color::Symbol, run_key::String) = Token(color, run_key, nothing)

"""
    Distribution of tokens across places at one point in time.
"""
struct Marking{T<:AbstractToken}
    tokens_by_place::Dict{Symbol, Vector{T}}
end

Marking{T}() where {T<:AbstractToken} = Marking{T}(Dict{Symbol, Vector{T}}())
Marking() = Marking{Token}()

"""
    Distinct `run_key` values present in a marking, in first-seen order.
"""
function run_keys(marking::Marking)
    seen = Set{String}()
    ordered = String[]
    for tokens in values(marking.tokens_by_place)
        for token in tokens
            rk = run_key(token)
            rk in seen && continue
            push!(seen, rk)
            push!(ordered, rk)
        end
    end
    return ordered
end

"""
    Extract the marking slice for one `run_key`.
"""
function run_marking(marking::Marking{T}, rk::String) where T<:AbstractToken
    filtered = Dict{Symbol,Vector{T}}()
    sizehint!(filtered, length(marking.tokens_by_place))
    for (pid, tokens) in marking.tokens_by_place
        kept = T[]
        for token in tokens
            run_key(token) == rk && push!(kept, token)
        end
        isempty(kept) || (filtered[pid] = kept)
    end
    return Marking(filtered)
end
