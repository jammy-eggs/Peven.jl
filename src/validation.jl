"""
    A single validation problem found in a Net or Marking
    code is a machine-readable category like :keyMismatch, :unknownPlace, :capacityExceeded
    objectId identifies which Place, Transition, or Arc the issue is about
    message is a human-readable description of the problem
"""
struct ValidationIssue
    code::Symbol
    objectId::Symbol
    message::String
end

# ── Public API ───────────────────────────────────────────────────────────────

"""
    validate(net) checks structural integrity of the Net only
    validate(net, marking) checks structure, then marking, then reachability
    Returns a Vector{ValidationIssue} where empty means valid
"""
validate(net::Net) = validate!(newIssues(net), net)

function validate(net::Net, marking::Marking)
    validate!(newIssues(net, marking), net, marking)
end

# ── Validation pipeline ─────────────────────────────────────────────────────

"""
    Net-only validation runs dict key consistency, arc endpoint existence, and orphan place checks
"""
function validate!(issues::Vector{ValidationIssue}, net::Net)
    validateKeys!(issues, net)
    validateArcs!(issues, net)
    validateOrphanPlaces!(issues, net)
    return issues
end

"""
    Net + Marking validation runs structure first, then marking checks, then reachability
    Reachability is skipped if earlier checks already found problems
"""
function validate!(issues::Vector{ValidationIssue}, net::Net, marking::Marking)
    nissues = length(issues)
    validate!(issues, net)
    validateMarking!(issues, marking, net)
    length(issues) == nissues && validateReachability!(issues, net, marking)
    return issues
end

# ── Internal helpers ─────────────────────────────────────────────────────────

# Pre-allocate issue vector based on worst case — every element could have a problem
issueCapacity(net::Net) = (
    length(net.places) +
    length(net.transitions) +
    2 * length(net.arcsfrom) +
    2 * length(net.arcsto)
)

issueCapacity(net::Net, marking::Marking) = (
    issueCapacity(net) +
    length(marking.tokensByPlace) +
    1
)

newIssues(x) = sizehint!(ValidationIssue[], issueCapacity(x))
newIssues(net::Net, marking::Marking) = sizehint!(ValidationIssue[], issueCapacity(net, marking))

pushIssue!(
    issues::Vector{ValidationIssue},
    code::Symbol,
    objectId::Symbol,
    message::String,
) = push!(issues, ValidationIssue(code, objectId, message))

pushIssue!(
    issues::Vector{ValidationIssue},
    code::Symbol,
    objectId::Symbol,
    message::AbstractString,
) = push!(issues, ValidationIssue(code, objectId, String(message)))

# Check that a referenced id exists in a dict, push an issue if not
validateRef!(
    issues::Vector{ValidationIssue},
    items,
    id::Symbol,
    code::Symbol,
    message::String,
) = haskey(items, id) || pushIssue!(issues, code, id, message)

# ── Key consistency ──────────────────────────────────────────────────────────

"""
    Dict keys must match the .id field of the struct they point to
    e.g. net.places[:ready].id must be :ready
"""
function validateKeys!(issues::Vector{ValidationIssue}, net::Net)
    for (id, place) in net.places
        id == place.id || pushIssue!(
            issues, :keyMismatch, id,
            "place dict key does not match place.id",
        )
    end
    for (id, transition) in net.transitions
        id == transition.id || pushIssue!(
            issues, :keyMismatch, id,
            "transition dict key does not match transition.id",
        )
    end
    return issues
end

# ── Arc endpoint existence ───────────────────────────────────────────────────

"""
    Every Arc must reference Places and Transitions that actually exist in the Net
"""
function validateArcs!(issues::Vector{ValidationIssue}, net::Net)
    for arc in net.arcsfrom
        validate!(issues, arc, net)
    end
    for arc in net.arcsto
        validate!(issues, arc, net)
    end
    return issues
end

function validate!(issues::Vector{ValidationIssue}, arc::ArcFrom, net::Net)
    validateRef!(issues, net.places, arc.from, :unknownPlace, "arc references unknown input place")
    validateRef!(issues, net.transitions, arc.transition, :unknownTransition, "arc references unknown transition")
    return issues
end

function validate!(issues::Vector{ValidationIssue}, arc::ArcTo, net::Net)
    validateRef!(issues, net.places, arc.to, :unknownPlace, "arc references unknown output place")
    validateRef!(issues, net.transitions, arc.transition, :unknownTransition, "arc references unknown transition")
    return issues
end

# ── Orphan places ────────────────────────────────────────────────────────────

"""
    A Place with no arcs at all is dead weight — nothing can consume from it or deposit into it
    This is almost always a bug in the net definition
"""
function validateOrphanPlaces!(issues::Vector{ValidationIssue}, net::Net)
    # Places that appear in any arc (as input or output)
    connected = Set{Symbol}()
    for arc in net.arcsfrom
        push!(connected, arc.from)
    end
    for arc in net.arcsto
        push!(connected, arc.to)
    end
    for id in keys(net.places)
        id ∈ connected || pushIssue!(issues, :orphanPlace, id, "place has no arcs — not connected to any transition")
    end
    return issues
end

# ── Marking validation ───────────────────────────────────────────────────────

"""
    Tokens must sit in Places that exist in the Net
    Token count in each Place must not exceed that Place's capacity
"""
function validateMarking!(issues::Vector{ValidationIssue}, marking::Marking, net::Net)
    for (id, tokens) in marking.tokensByPlace
        place = get(net.places, id, nothing)
        if isnothing(place)
            pushIssue!(issues, :unknownPlace, id, "marking references unknown place")
            continue
        end
        capacity = place.capacity
        if !isnothing(capacity) && length(tokens) > capacity
            pushIssue!(issues, :capacityExceeded, id, "marking exceeds place capacity")
        end
    end
    return issues
end

# ── Reachability ─────────────────────────────────────────────────────────────

"""
    Iterative DFS from Places that hold Tokens, following the precomputed children adjacency list
    Any Transition not visited is unreachable — it can never fire from this Marking
"""
const noChildren = Symbol[]

function validateReachability!(issues::Vector{ValidationIssue}, net::Net, marking::Marking)
    isempty(net.transitions) && return issues

    seen = Set{Symbol}()
    sizehint!(seen, length(net.places) + length(net.transitions))
    stack = Symbol[]
    sizehint!(stack, length(marking.tokensByPlace))

    # Seed the DFS with Places that currently hold Tokens
    for (id, tokens) in marking.tokensByPlace
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
        for child in get(net.children, id, noChildren)
            child in seen || push!(stack, child)
        end
    end

    # Any Transition not reached is dead from this Marking
    for id in sort!(collect(keys(net.transitions)))
        id in seen || pushIssue!(issues, :unreachableTransition, id, "transition :$id is unreachable from the current marking")
    end

    return issues
end
