"""
    One validation problem found in a `Net` or `Marking`.
"""
struct ValidationIssue
    code::Symbol
    object_id::Symbol
    message::String
end

function validate end
function validate_fire_inputs end

let
    # Public API

    """
        Validate a net or a net plus marking.
        Returns a `Vector{ValidationIssue}`; empty means valid.
    """
    global validate(net::Net) = validate!(new_issues(net), net)

    global function validate(net::Net, marking::Marking)
        validate!(new_issues(net, marking), net, marking)
    end

    """
        Validation preflight used by `fire` before execution starts.
        Checks structure and marking correctness, but skips reachability.
    """
    global function validate_fire_inputs(net::Net, marking::Marking)
        issues = new_issues(net, marking)
        validate!(issues, net)
        validate_marking!(issues, marking, net)
        return issues
    end

    # Validation pipeline

    """
        Validate net structure only.
    """
    function validate!(issues::Vector{ValidationIssue}, net::Net)
        validate_keys!(issues, net)
        validate_arcs!(issues, net)
        validate_keyed_join_structure!(issues, net)
        validate_output_arc_structure!(issues, net)
        validate_arc_capacity!(issues, net)
        validate_orphan_places!(issues, net)
        return issues
    end

    """
        Validate net structure, then marking, then reachability.
    """
    function validate!(issues::Vector{ValidationIssue}, net::Net, marking::Marking)
        nissues = length(issues)
        validate!(issues, net)
        validate_marking!(issues, marking, net)
        length(issues) == nissues && validate_reachability!(issues, net, marking)
        return issues
    end

    # Internal helpers

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

    validate_ref!(
        issues::Vector{ValidationIssue},
        items,
        id::Symbol,
        code::Symbol,
        message::String,
    ) = haskey(items, id) || push_issue!(issues, code, id, message)

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

    function validate_arcs!(issues::Vector{ValidationIssue}, net::Net)
        for arc in net.arcsfrom
            validate!(issues, arc, net)
        end
        for arc in net.arcsto
            validate!(issues, arc, net)
        end
        return issues
    end

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

    function validate_output_arc_structure!(issues::Vector{ValidationIssue}, net::Net)
        seen_outputs = Set{Tuple{Symbol,Symbol}}()

        for arc in net.arcsto
            key = (arc.transition, arc.to)
            key in seen_outputs && push_issue!(
                issues,
                :duplicate_output_arc,
                arc.transition,
                "transition :$(arc.transition) has duplicate output arcs to place :$(arc.to); use one output arc per destination place",
            )
            push!(seen_outputs, key)
        end

        return issues
    end

    function validate_arc_capacity!(issues::Vector{ValidationIssue}, net::Net)
        input_weights = Dict{Tuple{Symbol,Symbol},Int}()
        for arc in net.arcsfrom
            key = (arc.transition, arc.from)
            input_weights[key] = get(input_weights, key, 0) + arc.weight
        end
        for ((transition_id, place_id), total_weight) in input_weights
            place = get(net.places, place_id, nothing)
            if !isnothing(place) && !isnothing(place.capacity) && total_weight > place.capacity
                push_issue!(
                    issues,
                    :weight_exceeds_capacity,
                    transition_id,
                    "input arcs from :$place_id require total weight $total_weight but place capacity is $(place.capacity)",
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

    function validate_orphan_places!(issues::Vector{ValidationIssue}, net::Net)
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

    no_children = Symbol[]

    function validate_reachability!(issues::Vector{ValidationIssue}, net::Net, marking::Marking)
        isempty(net.transitions) && return issues

        seen = Set{Symbol}()
        sizehint!(seen, length(net.places) + length(net.transitions))
        stack = Symbol[]
        sizehint!(stack, length(marking.tokens_by_place))

        for (id, tokens) in marking.tokens_by_place
            if haskey(net.places, id) && !isempty(tokens)
                push!(stack, id)
            end
        end

        while !isempty(stack)
            id = pop!(stack)
            id in seen && continue
            push!(seen, id)
            for child in get(net.children, id, no_children)
                child in seen || push!(stack, child)
            end
        end

        for id in sort!(collect(keys(net.transitions)))
            id in seen || push_issue!(issues, :unreachable_transition, id, "transition :$id is unreachable from the current marking")
        end

        return issues
    end
end
