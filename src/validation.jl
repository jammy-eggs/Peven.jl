"""
    A single validation problem found in a Net or Marking
    code is a machine-readable category like :key_mismatch, :unknown_place, :capacity_exceeded
    object_id identifies which Place, Transition, or Arc the issue is about
    message is a human-readable description of the problem
"""
struct ValidationIssue
    code::Symbol
    object_id::Symbol
    message::String
end

# ── Public API ───────────────────────────────────────────────────────────────

"""
    validate(net) checks structural integrity of the Net only
    validate(net, marking) checks structure, then marking, then reachability
    Returns a Vector{ValidationIssue} where empty means valid
"""
validate(net::Net) = validate!(new_issues(net), net)

function validate(net::Net, marking::Marking)
    validate!(new_issues(net, marking), net, marking)
end

# ── Validation pipeline ─────────────────────────────────────────────────────

"""
    Net-only validation runs dict key consistency, arc endpoint existence, and orphan place checks
"""
function validate!(issues::Vector{ValidationIssue}, net::Net)
    validate_keys!(issues, net)
    validate_arcs!(issues, net)
    validate_keyed_join_structure!(issues, net)
    validate_arc_capacity!(issues, net)
    validate_orphan_places!(issues, net)
    return issues
end

"""
    Net + Marking validation runs structure first, then marking checks, then reachability
    Reachability is skipped if earlier checks already found problems
"""
function validate!(issues::Vector{ValidationIssue}, net::Net, marking::Marking)
    nissues = length(issues)
    validate!(issues, net)
    validate_marking!(issues, marking, net)
    length(issues) == nissues && validate_reachability!(issues, net, marking)
    return issues
end

# ── Internal helpers ─────────────────────────────────────────────────────────

# Pre-allocate issue vector based on worst case — every element could have a problem
issue_capacity(net::Net) = (
    length(net.places) +
    length(net.transitions) +
    length(net.arcsfrom) +
    3 * length(net.arcsfrom) +
    3 * length(net.arcsto)
)

issue_capacity(net::Net, marking::Marking) = (
    issue_capacity(net) +
    length(marking.tokens_by_place) +
    1
)

new_issues(x) = sizehint!(ValidationIssue[], issue_capacity(x))
new_issues(net::Net, marking::Marking) = sizehint!(ValidationIssue[], issue_capacity(net, marking))

push_issue!(
    issues::Vector{ValidationIssue},
    code::Symbol,
    object_id::Symbol,
    message::String,
) = push!(issues, ValidationIssue(code, object_id, message))

push_issue!(
    issues::Vector{ValidationIssue},
    code::Symbol,
    object_id::Symbol,
    message::AbstractString,
) = push!(issues, ValidationIssue(code, object_id, String(message)))

# Check that a referenced id exists in a dict, push an issue if not
validate_ref!(
    issues::Vector{ValidationIssue},
    items,
    id::Symbol,
    code::Symbol,
    message::String,
) = haskey(items, id) || push_issue!(issues, code, id, message)

# ── Key consistency ──────────────────────────────────────────────────────────

"""
    Dict keys must match the .id field of the struct they point to
    e.g. net.places[:ready].id must be :ready
"""
function validate_keys!(issues::Vector{ValidationIssue}, net::Net)
    for (id, place) in net.places
        id == place.id || push_issue!(
            issues, :key_mismatch, id,
            "place dict key does not match place.id",
        )
    end
    for (id, transition) in net.transitions
        id == transition.id || push_issue!(
            issues, :key_mismatch, id,
            "transition dict key does not match transition.id",
        )
    end
    return issues
end

# ── Arc endpoint existence ───────────────────────────────────────────────────

"""
    Every Arc must reference Places and Transitions that actually exist in the Net
"""
function validate_arcs!(issues::Vector{ValidationIssue}, net::Net)
    for arc in net.arcsfrom
        validate!(issues, arc, net)
    end
    for arc in net.arcsto
        validate!(issues, arc, net)
    end
    return issues
end

# ── Keyed-join structure ─────────────────────────────────────────────────────

"""
    Keyed joins require at least two unique input places
    Duplicate input arcs for the same (transition, place) are rejected; use weight instead
"""
function validate_keyed_join_structure!(issues::Vector{ValidationIssue}, net::Net)
    seen_inputs = Set{Tuple{Symbol,Symbol}}()
    unique_inputs = Dict{Symbol,Set{Symbol}}()

    for arc in net.arcsfrom
        key = (arc.transition, arc.from)
        key in seen_inputs && push_issue!(
            issues,
            :duplicate_input_arc,
            arc.transition,
            "transition :$(arc.transition) has duplicate input arcs from place :$(arc.from); use weight instead",
        )
        push!(seen_inputs, key)
        push!(get!(()->Set{Symbol}(), unique_inputs, arc.transition), arc.from)
    end

    for (tid, transition) in net.transitions
        isnothing(transition.join_by) && continue
        nunique = length(get(unique_inputs, tid, Set{Symbol}()))
        nunique >= 2 || push_issue!(
            issues,
            :invalid_keyed_join,
            tid,
            "transition :$tid uses join_by but has fewer than 2 unique input places",
        )
    end

    return issues
end

"""
    Arc weights must not exceed the bounded capacity of their connected place
    Otherwise the transition can never consume or deposit enough tokens successfully
"""
function validate_arc_capacity!(issues::Vector{ValidationIssue}, net::Net)
    for arc in net.arcsfrom
        place = get(net.places, arc.from, nothing)
        if !isnothing(place) && !isnothing(place.capacity) && arc.weight > place.capacity
            push_issue!(
                issues,
                :weight_exceeds_capacity,
                arc.transition,
                "input arc from :$(arc.from) requires weight $(arc.weight) but place capacity is $(place.capacity)",
            )
        end
    end

    for arc in net.arcsto
        place = get(net.places, arc.to, nothing)
        if !isnothing(place) && !isnothing(place.capacity) && arc.weight > place.capacity
            push_issue!(
                issues,
                :weight_exceeds_capacity,
                arc.transition,
                "output arc to :$(arc.to) deposits weight $(arc.weight) but place capacity is $(place.capacity)",
            )
        end
    end

    return issues
end

function validate!(issues::Vector{ValidationIssue}, arc::ArcFrom, net::Net)
    validate_ref!(issues, net.places, arc.from, :unknown_place, "arc references unknown input place")
    validate_ref!(issues, net.transitions, arc.transition, :unknown_transition, "arc references unknown transition")
    return issues
end

function validate!(issues::Vector{ValidationIssue}, arc::ArcTo, net::Net)
    validate_ref!(issues, net.places, arc.to, :unknown_place, "arc references unknown output place")
    validate_ref!(issues, net.transitions, arc.transition, :unknown_transition, "arc references unknown transition")
    return issues
end

# ── Orphan places ────────────────────────────────────────────────────────────

"""
    A Place with no arcs at all is dead weight — nothing can consume from it or deposit into it
    This is almost always a bug in the net definition
"""
function validate_orphan_places!(issues::Vector{ValidationIssue}, net::Net)
    # Places that appear in any arc (as input or output)
    connected = Set{Symbol}()
    for arc in net.arcsfrom
        push!(connected, arc.from)
    end
    for arc in net.arcsto
        push!(connected, arc.to)
    end
    for id in keys(net.places)
        id ∈ connected || push_issue!(issues, :orphan_place, id, "place has no arcs — not connected to any transition")
    end
    return issues
end

# ── Marking validation ───────────────────────────────────────────────────────

"""
    Tokens must sit in Places that exist in the Net
    Token count in each Place must not exceed that Place's capacity
"""
function validate_marking!(issues::Vector{ValidationIssue}, marking::Marking, net::Net)
    for (id, tokens) in marking.tokens_by_place
        place = get(net.places, id, nothing)
        if isnothing(place)
            push_issue!(issues, :unknown_place, id, "marking references unknown place")
            continue
        end
        capacity = place.capacity
        if !isnothing(capacity) && length(tokens) > capacity
            push_issue!(issues, :capacity_exceeded, id, "marking exceeds place capacity")
        end
    end
    return issues
end

# ── Reachability ─────────────────────────────────────────────────────────────

"""
    Iterative DFS from Places that hold Tokens, following the precomputed children adjacency list
    Any Transition not visited is unreachable — it can never fire from this Marking
"""
const NO_CHILDREN = Symbol[]

function validate_reachability!(issues::Vector{ValidationIssue}, net::Net, marking::Marking)
    isempty(net.transitions) && return issues

    seen = Set{Symbol}()
    sizehint!(seen, length(net.places) + length(net.transitions))
    stack = Symbol[]
    sizehint!(stack, length(marking.tokens_by_place))

    # Seed the DFS with Places that currently hold Tokens
    for (id, tokens) in marking.tokens_by_place
        if haskey(net.places, id) && !isempty(tokens)
            push!(stack, id)
        end
    end

    # Walk the graph using the precomputed children adjacency list
    # Only push unseen children to keep the stack small
    while !isempty(stack)
        id = pop!(stack)
        id in seen && continue
        push!(seen, id)
        for child in get(net.children, id, NO_CHILDREN)
            child in seen || push!(stack, child)
        end
    end

    # Any Transition not reached is dead from this Marking
    for id in sort!(collect(keys(net.transitions)))
        id in seen || push_issue!(issues, :unreachable_transition, id, "transition :$id is unreachable from the current marking")
    end

    return issues
end
