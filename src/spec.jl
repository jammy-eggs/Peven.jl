
"""
    A place in the net.
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
    A transition in the net.
    `guard` gates one firing's reserved inputs.
    `retries` counts engine-level transition retries.
    `join_by` classifies tokens for keyed joins and should be pure and deterministic.
"""
struct Transition
    id::Symbol
    executor::Symbol
    guard::Union{Nothing,Function}
    retries::Int
    join_by::Union{Nothing,Function}

    function Transition(
        id::Symbol,
        executor::Symbol=:default;
        guard=nothing,
        retries::Int=0,
        join_by=nothing,
    )
        retries >= 0 || throw(ArgumentError("retries must be non-negative"))
        new(id, executor, guard, retries, join_by)
    end
end

"""
    Identifies one concrete firing bundle in a marking snapshot.
"""
struct BundleRef
    transition_id::Symbol
    run_key::String
    selected_key::Any
    ordinal::Int

    function BundleRef(
        transition_id::Symbol,
        run_key::String,
        selected_key,
        ordinal::Int,
    )
        ordinal > 0 || throw(ArgumentError("bundle ordinal must be positive"))
        new(transition_id, run_key, selected_key, ordinal)
    end
end

Base.:(==)(a::BundleRef, b::BundleRef) = (
    a.transition_id === b.transition_id &&
    a.run_key == b.run_key &&
    isequal(a.selected_key, b.selected_key) &&
    a.ordinal == b.ordinal
)
Base.isequal(a::BundleRef, b::BundleRef) = a == b
Base.hash(bundle::BundleRef, h::UInt) = hash(
    (bundle.transition_id, bundle.run_key, bundle.selected_key, bundle.ordinal),
    h,
)

"""
    Bundle-level readiness record returned by `enablement`.
"""
struct BundleEnablement{T<:AbstractToken}
    bundle::BundleRef
    status::Symbol
    inputs::Vector{T}
    error::Union{Nothing,String}
end

"""
    Base type for net arcs.
"""
abstract type AbstractArc end

struct ArcFrom <: AbstractArc
    transition::Symbol
    from::Symbol
    weight::Int

    function ArcFrom(transition::Symbol, from::Symbol, weight::Int=1)
        weight > 0 || throw(ArgumentError("Arc weight must be positive"))
        new(transition, from, weight)
    end
end

struct ArcTo <: AbstractArc
    transition::Symbol
    to::Symbol
end


"""
    Net graph plus the runtime indexes derived from it.
    Structural problems are reported by `validate`, not by the constructor.
"""
struct Net
    places::Dict{Symbol,Place}
    transitions::Dict{Symbol,Transition}
    arcsfrom::Vector{ArcFrom}
    arcsto::Vector{ArcTo}
    children::Dict{Symbol,Vector{Symbol}}
    input_arcs::Dict{Symbol,Vector{Tuple{Symbol,Int}}}
    output_arcs::Dict{Symbol,Vector{Symbol}}
    affected_transitions::Dict{Symbol,Vector{Symbol}}
    upstream::Dict{Symbol,Vector{Symbol}}
    downstream::Dict{Symbol,Vector{Symbol}}
    recheck::Dict{Symbol,Vector{Symbol}}
    from_places::Set{Symbol}

    function Net(
        places::Dict{Symbol,Place},
        transitions::Dict{Symbol,Transition},
        arcsfrom::Vector{ArcFrom},
        arcsto::Vector{ArcTo},
        children::Dict{Symbol,Vector{Symbol}},
        input_arcs::Dict{Symbol,Vector{Tuple{Symbol,Int}}},
        output_arcs::Dict{Symbol,Vector{Symbol}},
        affected_transitions::Dict{Symbol,Vector{Symbol}},
        upstream::Dict{Symbol,Vector{Symbol}},
        downstream::Dict{Symbol,Vector{Symbol}},
        recheck::Dict{Symbol,Vector{Symbol}},
        from_places::Set{Symbol},
    )
        new(
            places,
            transitions,
            arcsfrom,
            arcsto,
            children,
            input_arcs,
            output_arcs,
            affected_transitions,
            upstream,
            downstream,
            recheck,
            from_places,
        )
    end
end

let
    build_children(arcsfrom::Vector{ArcFrom}, arcsto::Vector{ArcTo}) = begin
        children = Dict{Symbol,Vector{Symbol}}()
        sizehint!(children, length(arcsfrom) + length(arcsto))
        for arc in arcsfrom
            push!(get!(() -> Symbol[], children, arc.from), arc.transition)
        end
        for arc in arcsto
            push!(get!(() -> Symbol[], children, arc.transition), arc.to)
        end
        children
    end

    build_input_arcs(transitions, arcsfrom) = begin
        inputs = Dict{Symbol,Vector{Tuple{Symbol,Int}}}()
        for tid in keys(transitions)
            inputs[tid] = Tuple{Symbol,Int}[]
        end
        for arc in arcsfrom
            push!(get!(() -> Tuple{Symbol,Int}[], inputs, arc.transition), (arc.from, arc.weight))
        end
        inputs
    end

    build_output_arcs(transitions, arcsto) = begin
        outputs = Dict{Symbol,Vector{Symbol}}()
        for tid in keys(transitions)
            outputs[tid] = Symbol[]
        end
        for arc in arcsto
            push!(get!(() -> Symbol[], outputs, arc.transition), arc.to)
        end
        outputs
    end

    build_affected_transitions(arcsfrom) = begin
        affected = Dict{Symbol,Vector{Symbol}}()
        for arc in arcsfrom
            push!(get!(() -> Symbol[], affected, arc.from), arc.transition)
        end
        affected
    end

    build_upstream(transitions, input_arcs, affected) = begin
        rivals = Dict{Symbol,Vector{Symbol}}()
        for tid in keys(transitions)
            seen = Set{Symbol}()
            for (pid, _) in input_arcs[tid]
                for rival in get(affected, pid, Symbol[])
                    rival == tid || push!(seen, rival)
                end
            end
            rivals[tid] = sort!(collect(seen))
        end
        rivals
    end

    build_downstream(transitions, output_arcs, affected) = begin
        successors = Dict{Symbol,Vector{Symbol}}()
        for tid in keys(transitions)
            seen = Set{Symbol}()
            for pid in output_arcs[tid]
                for neighbor in get(affected, pid, Symbol[])
                    push!(seen, neighbor)
                end
            end
            successors[tid] = sort!(collect(seen))
        end
        successors
    end

    global function Net(
        places::Dict{Symbol,Place},
        transitions::Dict{Symbol,Transition},
        arcsfrom::Vector{ArcFrom},
        arcsto::Vector{ArcTo},
    )
        input = build_input_arcs(transitions, arcsfrom)
        output = build_output_arcs(transitions, arcsto)
        affected = build_affected_transitions(arcsfrom)
        up = build_upstream(transitions, input, affected)
        down = build_downstream(transitions, output, affected)
        rchk = Dict{Symbol,Vector{Symbol}}()
        for tid in keys(transitions)
            rchk[tid] = sort!(collect(union(Set([tid]), up[tid], down[tid])))
        end
        return Net(
            places,
            transitions,
            arcsfrom,
            arcsto,
            build_children(arcsfrom, arcsto),
            input,
            output,
            affected,
            up,
            down,
            rchk,
            Set{Symbol}(arc.from for arc in arcsfrom),
        )
    end
end
