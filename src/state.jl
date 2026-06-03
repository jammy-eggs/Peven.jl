"""
    Color is a Symbol label on a Token — this is a colored Petri net
    Colors let guards and executors distinguish between token types in the same Place
    e.g. :redteam, :baseline, :gold
"""
const Color = Symbol

"""
    AbstractToken is the dispatch seam for custom token types
    Subtype it and implement color() + runKey() to use your own token struct
    with guards, executors, and the engine — everything dispatches on AbstractToken
"""
abstract type AbstractToken end
color(t::AbstractToken)::Color = error("implement color(::$(typeof(t)))")
runKey(t::AbstractToken)::String = error("implement runKey(::$(typeof(t)))")

"""
    Token is the default colored token — concrete and type-stable in Vector{Token}
    color:    Symbol label for this token (e.g. :redteam, :baseline)
    runKey:  String that partitions tokens into independent runs for batch execution
    payload:  Any data the token carries between Places (prompt, score, response, etc.)
"""
struct Token <: AbstractToken
    color::Color
    runKey::String
    payload::Any
end
color(t::Token) = t.color
runKey(t::Token) = t.runKey

# Convenience constructors for tokens that don't need color or payload
Token(runKey::String) = Token(:default, runKey, nothing)
Token(color::Symbol, runKey::String) = Token(color, runKey, nothing)

"""
    Marking is the distribution of Tokens across Places at a point in time
    tokensByPlace maps each Place id to the Tokens currently sitting in it

    Marking{Token} is the fast path — all tokens are the same concrete type
    Marking{AbstractToken} is the flexible path for nets with mixed token types
"""
struct Marking{T<:AbstractToken}
    tokensByPlace::Dict{Symbol, Vector{T}}
end

Marking{T}() where {T<:AbstractToken} = Marking{T}(Dict{Symbol, Vector{T}}())
Marking() = Marking{Token}()
