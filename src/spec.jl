
"""
    Places are buckets that hold Tokens
    Each Place has a unique id and optionally a capacity that limits how many Tokens it can hold
"""
struct Place
    id::Symbol
    capacity::Union{Nothing,Int}

    function Place(id::Symbol, capacity::Union{Nothing,Int}=nothing)
        isnothing(capacity) || capacity > 0 || throw(ArgumentError("Capacity must be positive or nothing"))
        new(id, capacity)
    end
end

"""
    Transitions transform data passing between Places
    Executors are registered arbitrarily by a researcher — common executors include Agents, Sub-Agents, and Judges

    Guards are the backbone for cycles in Peven
    A guard says "data from Place A must satisfy this condition to proceed to Place B"
    If a guard fails, the Net might return to Place A (a 1-shot success task) or transition to Place C
    (e.g. a score of 70-79 gets different instructions than a score of 80-89)
    Place C might transition back to Place B, or branch into its own condition path

    Retries are transition retries, not API retries, encoded into the Net
    They can be paired with Guards to provide n chances to transition from Place A to Place B
    Retries are optional — certain experiments might favor a total rollout budget governing all steps
"""
struct Transition
    id::Symbol
    executor::Symbol
    guard::Union{Nothing,Function}
    retries::Int
    joinBy::Union{Nothing,Function}

    function Transition(id::Symbol, executor::Symbol=:default; guard=nothing, retries::Int=0, joinBy=nothing)
        retries >= 0 || throw(ArgumentError("retries must be non-negative"))
        new(id, executor, guard, retries, joinBy)
    end
end

struct Bundle
    transitionId::Symbol
    runKey::String
    selectedKey::Any
end

Base.:(==)(a::Bundle, b::Bundle) =
    a.transitionId === b.transitionId &&
    a.runKey == b.runKey &&
    isequal(a.selectedKey, b.selectedKey)
Base.isequal(a::Bundle, b::Bundle) = a == b
Base.hash(b::Bundle, h::UInt) =
    hash((b.transitionId, b.runKey, b.selectedKey), h)

"""
    Arcs connect Places and Transitions
    ArcFrom: Place -> Transition (input arc, tokens flow into a Transition)
    ArcTo: Transition -> Place (output arc, tokens flow out of a Transition)
    Weight determines how many Tokens an Arc consumes or produces per firing
"""
abstract type AbstractArc end

struct ArcFrom <: AbstractArc
    transition::Symbol
    from::Symbol
    weight::Int
    optional::Bool

    function ArcFrom(transition::Symbol, from::Symbol, weight::Int=1; optional::Bool=false)
        weight > 0 || throw(ArgumentError("Arc weight must be positive"))
        new(transition, from, weight, optional)
    end
end

struct ArcTo <: AbstractArc
    transition::Symbol
    to::Symbol
    weight::Int

    function ArcTo(transition::Symbol, to::Symbol, weight::Int=1)
        weight > 0 || throw(ArgumentError("Arc weight must be positive"))
        new(transition, to, weight)
    end
end


"""
    Constructs an adjacency list of all Places and Transitions
    ArcsFrom maps Places to a list of Transitions
    ArcsTo maps Transitions to a list of Places
"""
function buildChildren(arcsfrom::Vector{ArcFrom}, arcsto::Vector{ArcTo})
    children = Dict{Symbol,Vector{Symbol}}()
    sizehint!(children, length(arcsfrom) + length(arcsto)) # perf: pre-allocate space
    for arc in arcsfrom
        push!(get!(() -> Symbol[], children, arc.from), arc.transition)
    end
    for arc in arcsto
        push!(get!(() -> Symbol[], children, arc.transition), arc.to)
    end
    return children
end

"""
    Build index of each Transition to the Places it consumes from and how many tokens it needs from the Place
    :judge => [(:ready, 2)] means :judge needs 2 tokens from :ready to fire
"""
function buildInputArcs(transitions, arcsfrom)
    inputs = Dict{Symbol,Vector{Tuple{Symbol,Int,Bool}}}()
    for tid in keys(transitions)
        inputs[tid] = Tuple{Symbol,Int,Bool}[]
    end
    for arc in arcsfrom
        push!(
            get!(() -> Tuple{Symbol,Int,Bool}[], inputs, arc.transition),
            (arc.from, arc.weight, arc.optional),
        )
    end
    return inputs
end

"""
    Build index of each Transition to the Places it deposits into and how many tokens it deposits into each Place
    :judge => [(:done, 1)] means :judge deposits 1 token into :done after firing
"""
function buildOutputArcs(transitions, arcsto)
    outputs = Dict{Symbol,Vector{Tuple{Symbol,Int}}}()
    for tid in keys(transitions)
        outputs[tid] = Tuple{Symbol,Int}[]
    end
    for arc in arcsto
        push!(get!(() -> Tuple{Symbol,Int}[], outputs, arc.transition), (arc.to, arc.weight))
    end
    return outputs
end

"""
    Build a reverse index of each Place to the Transitions that consume from it
    :ready => [:judge, :score] means changes to :ready could affect :judge and :score
    Only built from ArcFrom (input arcs) because only consumption determines enablement —
    ArcTo arcs affect where tokens land after firing, not whether a transition can fire
    Used as a building block for the precomputed upstream/downstream indexes
"""
function buildAffectedTransitions(arcsfrom)
    affected = Dict{Symbol,Vector{Symbol}}()
    for arc in arcsfrom
        push!(get!(() -> Symbol[], affected, arc.from), arc.transition)
    end
    return affected
end

"""
    Precompute which Transitions to re-check after a given Transition fires
    Follows LoLA 2's influence-set technique (Wolf 2018, github.com/hlisdero/lola)

    upstream[t]:     Transitions on the input side of t's firing
                     Concretely: transitions sharing input Places with t
                     These might have LOST enablement because t consumed tokens they needed
                     This is not "all graph predecessors of t" — it is the input-side
                     contender set affected by t's consumption step
                     :judge => [:score] means after :judge fires, :score might no longer be enabled

    downstream[t]:   Transitions whose input Places are t's output Places
                     These might have GAINED enablement because t deposited tokens they need
                     :judge => [:review] means after :judge fires, :review might now be enabled
                     Includes t itself when t's output loops back to its own input (cycles)

    After firing t, re-check: t ∪ upstream[t] ∪ downstream[t]
    Net precomputes that full set as recheck[t] so fire() can reuse it directly
"""
function buildUpstream(transitions, inputArcs, affected)
    rivals = Dict{Symbol,Vector{Symbol}}()
    for tid in keys(transitions)
        seen = Set{Symbol}()
        for (pid, _, _) in inputArcs[tid]
            for rival in get(affected, pid, Symbol[])
                rival == tid || push!(seen, rival)
            end
        end
        rivals[tid] = sort!(collect(seen))
    end
    return rivals
end

function buildDownstream(transitions, outputArcs, affected)
    successors = Dict{Symbol,Vector{Symbol}}()
    for tid in keys(transitions)
        seen = Set{Symbol}()
        for (pid, _) in outputArcs[tid]
            for neighbor in get(affected, pid, Symbol[])
                push!(seen, neighbor)  # include self — cycles re-enable the same transition
            end
        end
        successors[tid] = sort!(collect(seen))
    end
    return successors
end

"""
    Net is the complete Petri net graph — Places, Transitions, and Arcs
    Caches indexes at construction for fast runtime access:
      children:               adjacency list for reachability checks
      inputArcs:             Transition -> [(Place, weight, optional)] for consumption
      outputArcs:            Transition -> [(Place, weight)] for deposit
      affectedTransitions:   Place -> [Transitions] reverse index (building block)
      upstream:               Transition -> [input-side contender transitions] that might lose
                              enablement after firing
      downstream:             Transition -> [Transitions] that might gain enablement after firing
      recheck:                Transition -> [Transitions] precomputed union of tid ∪ upstream ∪ downstream
      fromPlaces:            Set of Places with outgoing arcs — tokens here haven't reached a sink
"""
struct Net
    places::Dict{Symbol,Place}
    transitions::Dict{Symbol,Transition}
    arcsfrom::Vector{ArcFrom}
    arcsto::Vector{ArcTo}
    children::Dict{Symbol,Vector{Symbol}}
    inputArcs::Dict{Symbol,Vector{Tuple{Symbol,Int,Bool}}}
    outputArcs::Dict{Symbol,Vector{Tuple{Symbol,Int}}}
    affectedTransitions::Dict{Symbol,Vector{Symbol}}
    upstream::Dict{Symbol,Vector{Symbol}}
    downstream::Dict{Symbol,Vector{Symbol}}
    recheck::Dict{Symbol,Vector{Symbol}}
    fromPlaces::Set{Symbol}

    function Net(
        places::Dict{Symbol,Place},
        transitions::Dict{Symbol,Transition},
        arcsfrom::Vector{ArcFrom},
        arcsto::Vector{ArcTo},
    )
        input = buildInputArcs(transitions, arcsfrom)
        output = buildOutputArcs(transitions, arcsto)
        affected = buildAffectedTransitions(arcsfrom)
        up = buildUpstream(transitions, input, affected)
        down = buildDownstream(transitions, output, affected)
        # Precompute the full recheck set per transition: tid ∪ upstream ∪ downstream
        rchk = Dict{Symbol,Vector{Symbol}}()
        for tid in keys(transitions)
            rchk[tid] = sort!(collect(union(Set([tid]), up[tid], down[tid])))
        end
        new(places, transitions, arcsfrom, arcsto,
            buildChildren(arcsfrom, arcsto),
            input, output, affected, up, down, rchk,
            Set{Symbol}(arc.from for arc in arcsfrom))
    end
end
