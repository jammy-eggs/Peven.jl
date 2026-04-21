"""
    Check whether a transition's guard allows firing
    Nothing guard always passes, Function guard is called with the bundle's input tokens
"""
@inline evaluate_guard(::Nothing, _) = true
@inline evaluate_guard(guard::Function, tokens) = guard(tokens)

"""
    Drop a token into the output places of a fired transition
    Copy-on-write: returns a new Marking, original is unchanged
    Throws ArgumentError if any output place would exceed its capacity
"""
function drop(marking::Marking{T}, net::Net, tid::Symbol, token::T) where T<:AbstractToken
    tokens = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
    for (pid, weight) in net.output_arcs[tid]
        bucket = get!(()->T[], tokens, pid)
        cap = net.places[pid].capacity
        for _ in 1:weight
            !isnothing(cap) && length(bucket) >= cap &&
                throw(ArgumentError("capacity exceeded at :$pid"))
            push!(bucket, token)
        end
    end
    return Marking(tokens)
end

"""
    Drop many tokens into a transition's sole output place
    Copy-on-write: returns a new Marking, original is unchanged
    Valid only for transitions with exactly one output arc of weight 1
    Validates the full deposit before committing so capacity failures stay atomic
"""
function drop(
    marking::Marking{T},
    net::Net,
    tid::Symbol,
    outputs::AbstractVector{S},
) where {T<:AbstractToken,S<:T}
    isempty(outputs) && throw(ArgumentError("executor output vector must be non-empty"))

    arcs = get(net.output_arcs, tid, Tuple{Symbol,Int}[])
    length(arcs) == 1 || throw(ArgumentError("executor output vector requires exactly one output arc"))
    pid, weight = only(arcs)
    weight == 1 || throw(ArgumentError("executor output vector requires a weight-1 output arc"))

    bucket = get(marking.tokens_by_place, pid, T[])
    cap = net.places[pid].capacity
    !isnothing(cap) && length(bucket) + length(outputs) > cap &&
        throw(ArgumentError("capacity exceeded at :$pid"))

    tokens = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
    deposited = copy(bucket)
    append!(deposited, outputs)
    tokens[pid] = deposited
    return Marking(tokens)
end

"""
    Put grabbed tokens back after a failed firing when callers want restoration
    Restores the marking to its pre-grab state
    No capacity check needed because these tokens were already in these places
"""
function misfire(marking::Marking{T}, grabbed_by_place::Dict{Symbol,Vector{T}}) where T<:AbstractToken
    restored = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
    for (pid, returned) in grabbed_by_place
        append!(get!(()->T[], restored, pid), returned)
    end
    return Marking(restored)
end

"""
    Collect the unique run_keys from the initial marking in lexicographic order
    These are the fuses the engine will light
"""
function fuses(marking::Marking)
    n = sum(length, values(marking.tokens_by_place); init=0)
    seen = Set{String}()
    sizehint!(seen, n)
    ordered = String[]
    sizehint!(ordered, n)
    for tokens in values(marking.tokens_by_place)
        for token in tokens
            rk = run_key(token)
            if rk ∉ seen
                push!(seen, rk)
                push!(ordered, rk)
            end
        end
    end
    sort!(ordered)
    return ordered
end

"""
    Check if a run_key's tokens have all left from-places (places with outgoing arcs)
    Returns true when no tokens for this run_key remain in any from-place
"""
function run_completed(marking::Marking, rk::String, from_places::Set{Symbol})
    for (pid, tokens) in marking.tokens_by_place
        pid ∈ from_places || continue
        any(t -> run_key(t) == rk, tokens) && return false
    end
    return true
end

"""
    Extract the final marking snapshot for one run_key
    Keeps only that run_key's tokens across all places and drops empty places
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

function _normalize_outputs(output, ::Type{T}) where T<:AbstractToken
    if output isa T
        return T[output]
    end

    output isa AbstractVector || throw(ArgumentError("executor output must be a token or vector of tokens"))
    isempty(output) && throw(ArgumentError("executor output vector must be non-empty"))

    outputs = T[]
    sizehint!(outputs, length(output))
    for token in output
        token isa T || throw(ArgumentError("executor output vector must match the marking token type"))
        push!(outputs, token)
    end
    return outputs
end

const _BundleTriple = Tuple{Symbol,String,Any}

struct _TripleScan{T<:AbstractToken}
    triple::_BundleTriple
    bundles::Vector{BundleEnablement{T}}
end

struct _SelectionFailure
    transition_id::Symbol
    run_key::String
    error::String
end

struct _GuardErrorEpisode
    message::String
    first_seen_epoch::Int
    first_seen_order::Int
end

struct _SelectionErrorEpisode
    transition_id::Symbol
    message::String
end

mutable struct _RunProgress{T<:AbstractToken}
    trace::Vector{TransitionResult{T}}
    active_guard_errors::Dict{Int,_GuardErrorEpisode}
    selection_error::Union{Nothing,_SelectionErrorEpisode}
    commit_blocked::Bool
    fuse_blocked::Bool
end

mutable struct _PendingFiring{T<:AbstractToken}
    bundle::BundleRef
    admission_id::Int
    firing_id::Int
    attempt::Int
    grabbed::Vector{T}
    grabbed_by_place::Dict{Symbol,Vector{T}}
end

mutable struct _SchedulerState{T<:AbstractToken}
    marking::Marking{T}
    runs::Dict{String,_RunProgress{T}}
    available::Dict{Int,BundleEnablement{T}}
    available_triples::Dict{_BundleTriple,Vector{Int}}
    ready::Vector{Int}
    ready_set::Set{Int}
    pending::Dict{Task,_PendingFiring{T}}
    fired::Int
    next_firing_id::Int
    next_admission_id::Int
    scan_epoch::Int
end

function _scheduler_state(marking::Marking{T}) where T<:AbstractToken
    runs = Dict{String,_RunProgress{T}}()
    for rk in fuses(marking)
        runs[rk] = _RunProgress{T}(
            TransitionResult{T}[],
            Dict{Int,_GuardErrorEpisode}(),
            nothing,
            false,
            false,
        )
    end
    return _SchedulerState{T}(
        marking,
        runs,
        Dict{Int,BundleEnablement{T}}(),
        Dict{_BundleTriple,Vector{Int}}(),
        Int[],
        Set{Int}(),
        Dict{Task,_PendingFiring{T}}(),
        0,
        1,
        1,
        0,
    )
end

function _get_or_create_run_progress!(state::_SchedulerState{T}, rk::String) where T<:AbstractToken
    return get!(state.runs, rk) do
        _RunProgress{T}(TransitionResult{T}[], Dict{Int,_GuardErrorEpisode}(), nothing, false, false)
    end
end

@inline _bundle_triple(bundle::BundleRef) = (bundle.transition_id, bundle.run_key, bundle.selected_key)
@inline _bundle_transition(triple::_BundleTriple) = triple[1]
@inline _bundle_run_key(triple::_BundleTriple) = triple[2]
@inline _bundle_selected_key(triple::_BundleTriple) = triple[3]

function _input_specs_for_transition(net::Net, transition::Transition)
    input_specs = get(net.input_arcs, transition.id, Tuple{Symbol,Int}[])
    isnothing(transition.join_by) && return input_specs
    return sort!(copy(input_specs); by=first)
end

function _input_specs_for_bundle(net::Net, bundle::BundleRef)
    return _input_specs_for_transition(net, net.transitions[bundle.transition_id])
end

function _collect_run_buckets(marking::Marking{T}, pid::Symbol) where T<:AbstractToken
    buckets = Dict{String,Vector{Tuple{Int,T}}}()
    for (idx, token) in pairs(get(marking.tokens_by_place, pid, T[]))
        push!(get!(()->Tuple{Int,T}[], buckets, run_key(token)), (idx, token))
    end
    return buckets
end

function _candidate_run_keys(input_specs::Vector{Tuple{Symbol,Int}}, by_place)
    seen = Set{String}()
    ordered = String[]
    for (pid, _) in input_specs
        for rk in keys(get(by_place, pid, Dict{String,Vector}()))
            rk ∈ seen && continue
            push!(seen, rk)
            push!(ordered, rk)
        end
    end
    sort!(ordered)
end

function _normalize_check(net::Net, check::Union{Nothing,Vector{Symbol}})
    if check === nothing
        return sort!(collect(keys(net.transitions)))
    end

    live = sort!(collect(Set(check)))
    for tid in live
        haskey(net.transitions, tid) || throw(ArgumentError("unknown transition in check: :$tid"))
    end
    return live
end

function _safe_showerror(err)
    try
        return sprint(showerror, err)
    catch e
        e isa InterruptException && rethrow()
        return sprint(show, err)
    end
end

function _selector_key(
    selector::Function,
    tid::Symbol,
    pid::Symbol,
    rk::String,
    token,
)
    key = selector(pid, token)
    isnothing(key) && throw(ArgumentError(
        "join_by for transition :$tid returned nothing on place :$pid for run_key=$rk",
    ))
    return key
end

function _safe_put_completed!(completed::Channel{Task}, task::Task)
    try
        put!(completed, task)
    catch e
        e isa InvalidStateException || rethrow()
    end
    return nothing
end

function _evaluate_bundle(
    transition::Transition,
    bundle::BundleRef,
    inputs::Vector{T};
    on_guard_error = nothing,
) where T<:AbstractToken
    status = :ready
    error = nothing
    try
        evaluate_guard(transition.guard, inputs) || (status = :guard_blocked)
    catch e
        status = :guard_errored
        error = _safe_showerror(e)
        on_guard_error !== nothing && on_guard_error(bundle, e)
    end
    return BundleEnablement(bundle, status, inputs, error)
end

function _unkeyed_bundles_for_run(
    tid::Symbol,
    rk::String,
    transition::Transition,
    input_specs::Vector{Tuple{Symbol,Int}},
    by_place::Dict{Symbol,Dict{String,Vector{Tuple{Int,T}}}};
    in_flight::Set{BundleRef},
    on_guard_error = nothing,
) where T<:AbstractToken
    chunks = Vector{Vector{Tuple{Int,T}}}()
    sizehint!(chunks, length(input_specs))
    for (pid, weight) in input_specs
        run_bucket = get(get(by_place, pid, Dict{String,Vector{Tuple{Int,T}}}()), rk, Tuple{Int,T}[])
        length(run_bucket) >= weight || return BundleEnablement{T}[]
        push!(chunks, run_bucket)
    end

    bundle_count = minimum(div(length(chunks[i]), input_specs[i][2]) for i in eachindex(input_specs))
    bundles = BundleEnablement{T}[]
    sizehint!(bundles, bundle_count)

    for ordinal in 1:bundle_count
        bundle = BundleRef(tid, rk, nothing, ordinal)
        bundle ∈ in_flight && continue
        inputs = T[]
        for (index, (_, weight)) in pairs(input_specs)
            start = (ordinal - 1) * weight + 1
            stop = start + weight - 1
            for (_, token) in @view chunks[index][start:stop]
                push!(inputs, token)
            end
        end
        push!(bundles, _evaluate_bundle(transition, bundle, inputs; on_guard_error=on_guard_error))
    end

    return bundles
end

function _keyed_bundles_for_run(
    tid::Symbol,
    rk::String,
    transition::Transition,
    input_specs::Vector{Tuple{Symbol,Int}},
    by_place::Dict{Symbol,Dict{String,Vector{Tuple{Int,T}}}};
    in_flight::Set{BundleRef},
    on_guard_error = nothing,
) where T<:AbstractToken
    selector = something(transition.join_by)
    first_pid = input_specs[1][1]
    first_run_bucket = get(get(by_place, first_pid, Dict{String,Vector{Tuple{Int,T}}}()), rk, Tuple{Int,T}[])

    first_groups = Dict{Any,Vector{T}}()
    ordered_keys = Any[]
    for (idx, token) in first_run_bucket
        key = _selector_key(selector, tid, first_pid, rk, token)
        if !haskey(first_groups, key)
            first_groups[key] = T[]
            push!(ordered_keys, key)
        end
        push!(first_groups[key], token)
    end

    grouped = Dict{Symbol,Dict{Any,Vector{T}}}(first_pid => first_groups)
    for (pid, _) in input_specs[2:end]
        run_bucket = get(get(by_place, pid, Dict{String,Vector{Tuple{Int,T}}}()), rk, Tuple{Int,T}[])
        keyed = Dict{Any,Vector{T}}()
        for (_, token) in run_bucket
            key = _selector_key(selector, tid, pid, rk, token)
            push!(get!(()->T[], keyed, key), token)
        end
        grouped[pid] = keyed
    end

    bundles = BundleEnablement{T}[]
    for key in ordered_keys
        counts = Int[]
        for (pid, weight) in input_specs
            place_groups = get(grouped, pid, Dict{Any,Vector{T}}())
            key_bucket = get(place_groups, key, T[])
            length(key_bucket) >= weight || (empty!(counts); break)
            push!(counts, div(length(key_bucket), weight))
        end
        isempty(counts) && continue
        bundle_count = minimum(counts)
        for ordinal in 1:bundle_count
            bundle = BundleRef(tid, rk, key, ordinal)
            bundle ∈ in_flight && continue
            inputs = T[]
            for (pid, weight) in input_specs
                place_bucket = grouped[pid][key]
                start = (ordinal - 1) * weight + 1
                stop = start + weight - 1
                for token in @view place_bucket[start:stop]
                    push!(inputs, token)
                end
            end
            push!(bundles, _evaluate_bundle(transition, bundle, inputs; on_guard_error=on_guard_error))
        end
    end

    return bundles
end

"""
    Internal bundle enumerator shared by enablement/hot/cold/fire
    Returns bundle-level status grouped by `(transition, run_key, selected_key)`
"""
function _scan_enablement(
    net::Net,
    marking::Marking{T};
    failed::Set{String} = Set{String}(),
    in_flight::Set{BundleRef} = Set{BundleRef}(),
    check::Union{Nothing,Vector{Symbol}} = nothing,
    on_guard_error = nothing,
    swallow_selection_errors::Bool = false,
) where T<:AbstractToken
    scans = _TripleScan{T}[]
    selection_failures = _SelectionFailure[]
    live = _normalize_check(net, check)

    for tid in live
        transition = net.transitions[tid]
        input_specs = _input_specs_for_transition(net, transition)
        isempty(input_specs) && continue
        by_place = Dict{Symbol,Dict{String,Vector{Tuple{Int,T}}}}()
        for (pid, _) in input_specs
            by_place[pid] = _collect_run_buckets(marking, pid)
        end

        for rk in _candidate_run_keys(input_specs, by_place)
            rk ∈ failed && continue
            bundles = BundleEnablement{T}[]
            if isnothing(transition.join_by)
                bundles = _unkeyed_bundles_for_run(
                    tid,
                    rk,
                    transition,
                    input_specs,
                    by_place;
                    in_flight=in_flight,
                    on_guard_error=on_guard_error,
                )
            else
                try
                    bundles = _keyed_bundles_for_run(
                        tid,
                        rk,
                        transition,
                        input_specs,
                        by_place;
                        in_flight=in_flight,
                        on_guard_error=on_guard_error,
                    )
                catch e
                    if swallow_selection_errors
                        push!(selection_failures, _SelectionFailure(tid, rk, _safe_showerror(e)))
                        continue
                    end
                    rethrow()
                end
            end
            isempty(bundles) && continue
            push!(scans, _TripleScan{T}((tid, rk, bundles[1].bundle.selected_key), bundles))
        end
    end

    return scans, selection_failures
end

"""
    Public bundle-level readiness view
    `on_guard_error` receives `(bundle, exception)` for each errored bundle in this snapshot
    Selector exceptions propagate to the caller
"""
function enablement(
    net::Net,
    marking::Marking{T};
    failed::Set{String} = Set{String}(),
    in_flight::Set{BundleRef} = Set{BundleRef}(),
    check::Union{Nothing,Vector{Symbol}} = nothing,
    on_guard_error = nothing,
) where T<:AbstractToken
    scans, _ = _scan_enablement(
        net,
        marking;
        failed=failed,
        in_flight=in_flight,
        check=check,
        on_guard_error=on_guard_error,
        swallow_selection_errors=false,
    )

    records = BundleEnablement{T}[]
    for scan in scans
        append!(records, scan.bundles)
    end
    return records
end

"""
    Which bundles are hot — ready to fire from the current marking?
"""
function hot(
    net::Net,
    marking::Marking{T};
    failed::Set{String} = Set{String}(),
    in_flight::Set{BundleRef} = Set{BundleRef}(),
    check::Union{Nothing,Vector{Symbol}} = nothing,
    on_guard_error = nothing,
) where T<:AbstractToken
    return BundleRef[
        entry.bundle for entry in enablement(
            net,
            marking;
            failed=failed,
            in_flight=in_flight,
            check=check,
            on_guard_error=on_guard_error,
        ) if entry.status === :ready
    ]
end

"""
    Which transitions have no ready bundle — the inverse of hot()
"""
function cold(
    net::Net,
    marking::Marking{T};
    failed::Set{String} = Set{String}(),
    in_flight::Set{BundleRef} = Set{BundleRef}(),
    check::Union{Nothing,Vector{Symbol}} = nothing,
    on_guard_error = nothing,
) where T<:AbstractToken
    ready = Set{Symbol}(bundle.transition_id for bundle in hot(
        net,
        marking;
        failed=failed,
        in_flight=in_flight,
        check=check,
        on_guard_error=on_guard_error,
    ))
    scanned = _normalize_check(net, check)
    return [tid for tid in scanned if tid ∉ ready]
end

function _claim_positions(marking::Marking{T}, net::Net, bundle::BundleRef) where T<:AbstractToken
    transition = net.transitions[bundle.transition_id]
    input_specs = _input_specs_for_bundle(net, bundle)
    claimed_positions = Dict{Symbol,Vector{Int}}()
    claimed_by_place = Dict{Symbol,Vector{T}}()

    for (pid, weight) in input_specs
        bucket = get(marking.tokens_by_place, pid, T[])
        matched_positions = Int[]
        matched_tokens = T[]
        for (idx, token) in pairs(bucket)
            run_key(token) == bundle.run_key || continue
            if !isnothing(transition.join_by)
                key = _selector_key(
                    something(transition.join_by),
                    bundle.transition_id,
                    pid,
                    bundle.run_key,
                    token,
                )
                isequal(key, bundle.selected_key) || continue
            end
            push!(matched_positions, idx)
            push!(matched_tokens, token)
        end

        start = (bundle.ordinal - 1) * weight + 1
        stop = start + weight - 1
        length(matched_positions) >= stop || return nothing
        claimed_positions[pid] = collect(@view matched_positions[start:stop])
        claimed_by_place[pid] = collect(@view matched_tokens[start:stop])
    end

    return claimed_positions, claimed_by_place
end

"""
    Try to grab the exact bundle from the supplied marking snapshot
    Returns `(new_marking, grabbed, grabbed_by_place)` or nothing if the bundle is stale/unavailable
"""
function grab(marking::Marking{T}, net::Net, bundle::BundleRef) where T<:AbstractToken
    claimed = _claim_positions(marking, net, bundle)
    isnothing(claimed) && return nothing
    claimed_positions, grabbed_by_place = claimed

    tokens = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
    grabbed = T[]

    for (pid, _) in _input_specs_for_bundle(net, bundle)
        bucket = get(tokens, pid, T[])
        claimed_set = Set(get(claimed_positions, pid, Int[]))
        leftover = T[]
        for (idx, token) in pairs(bucket)
            if idx ∈ claimed_set
                push!(grabbed, token)
            else
                push!(leftover, token)
            end
        end
        tokens[pid] = leftover
    end

    return Marking(tokens), grabbed, grabbed_by_place
end

"""
    Grab the exact bundle or throw if it is stale/unavailable in the supplied marking
"""
function take(marking::Marking{T}, net::Net, bundle::BundleRef) where T<:AbstractToken
    result = grab(marking, net, bundle)
    isnothing(result) && throw(ArgumentError("stale or unavailable bundle"))
    return result
end

@inline _allocate_firing_id!(state::_SchedulerState) =
    (firing_id = state.next_firing_id; state.next_firing_id += 1; firing_id)

@inline _allocate_admission_id!(state::_SchedulerState) =
    (admission_id = state.next_admission_id; state.next_admission_id += 1; admission_id)

function _remove_ready_ids!(state::_SchedulerState, doomed::Set{Int})
    isempty(doomed) && return nothing
    filter!(state.ready) do id
        keep = id ∉ doomed
        keep || delete!(state.ready_set, id)
        keep
    end
    return nothing
end

function _drop_available_id!(state::_SchedulerState{T}, id::Int) where T<:AbstractToken
    record = get(state.available, id, nothing)
    isnothing(record) && return nothing

    run = _get_or_create_run_progress!(state, record.bundle.run_key)
    delete!(run.active_guard_errors, id)

    triple = _bundle_triple(record.bundle)
    if haskey(state.available_triples, triple)
        ids = state.available_triples[triple]
        filter!(existing -> existing != id, ids)
        isempty(ids) && delete!(state.available_triples, triple)
    end

    if id ∈ state.ready_set
        _remove_ready_ids!(state, Set([id]))
    end
    delete!(state.available, id)
    return nothing
end

function _drop_available_for_run!(state::_SchedulerState, rk::String)
    doomed = Int[]
    for (id, record) in state.available
        record.bundle.run_key == rk && push!(doomed, id)
    end
    for id in doomed
        _drop_available_id!(state, id)
    end
    return nothing
end

function _set_ready_membership!(
    state::_SchedulerState,
    id::Int,
    should_be_ready::Bool,
)
    if should_be_ready
        if id ∉ state.ready_set
            push!(state.ready, id)
            push!(state.ready_set, id)
        end
    elseif id ∈ state.ready_set
        _remove_ready_ids!(state, Set([id]))
    end
    return nothing
end

function _reconcile_guard_error!(
    state::_SchedulerState{T},
    id::Int,
    old_record::Union{Nothing,BundleEnablement{T}},
    new_record::Union{Nothing,BundleEnablement{T}},
    order::Int;
    on_event = nothing,
) where T<:AbstractToken
    bundle = isnothing(new_record) ? old_record.bundle : new_record.bundle
    run = _get_or_create_run_progress!(state, bundle.run_key)
    old_guard = !isnothing(old_record) && old_record.status === :guard_errored
    new_guard = !isnothing(new_record) && new_record.status === :guard_errored

    if old_guard && !new_guard
        delete!(run.active_guard_errors, id)
    elseif new_guard
        if haskey(run.active_guard_errors, id)
            episode = run.active_guard_errors[id]
            run.active_guard_errors[id] = _GuardErrorEpisode(
                something(new_record.error),
                episode.first_seen_epoch,
                episode.first_seen_order,
            )
        else
            run.active_guard_errors[id] = _GuardErrorEpisode(
                something(new_record.error),
                state.scan_epoch,
                order,
            )
            emit(on_event, GuardErrored(new_record.bundle, something(new_record.error)))
        end
    end
    return nothing
end

function _blocked_run_keys(state::_SchedulerState)
    blocked = Set{String}()
    for (rk, run) in state.runs
        (!isnothing(run.selection_error) || run.commit_blocked) && push!(blocked, rk)
    end
    return blocked
end

function _block_run_for_selection!(
    state::_SchedulerState{T},
    failure::_SelectionFailure;
    on_event = nothing,
) where T<:AbstractToken
    run = _get_or_create_run_progress!(state, failure.run_key)
    isnothing(run.selection_error) || return nothing

    run.selection_error = _SelectionErrorEpisode(failure.transition_id, failure.error)
    empty!(run.active_guard_errors)
    emit(on_event, SelectionErrored(failure.transition_id, failure.run_key, failure.error))

    doomed = Set{Int}()
    for (id, record) in state.available
        record.bundle.run_key == failure.run_key && push!(doomed, id)
    end
    for id in doomed
        _drop_available_id!(state, id)
    end
    return nothing
end

function _block_run_for_commit_failure!(state::_SchedulerState{T}, rk::String) where T<:AbstractToken
    run = _get_or_create_run_progress!(state, rk)
    run.commit_blocked && return nothing
    run.commit_blocked = true
    empty!(run.active_guard_errors)
    _drop_available_for_run!(state, rk)
    return nothing
end

function _refresh_enablement!(
    state::_SchedulerState{T},
    net::Net,
    check::Vector{Symbol};
    on_event = nothing,
) where T<:AbstractToken
    state.scan_epoch += 1
    latest_scans, selection_failures = _scan_enablement(
        net,
        state.marking;
        failed=_blocked_run_keys(state),
        in_flight=Set{BundleRef}(),
        check=check,
        swallow_selection_errors=true,
    )

    for failure in selection_failures
        _block_run_for_selection!(state, failure; on_event=on_event)
    end

    blocked = _blocked_run_keys(state)
    filtered_scans = _TripleScan{T}[]
    for scan in latest_scans
        _bundle_run_key(scan.triple) ∈ blocked || push!(filtered_scans, scan)
    end

    affected = Set(check)
    latest_map = Dict{_BundleTriple,Vector{BundleEnablement{T}}}()
    for scan in filtered_scans
        latest_map[scan.triple] = scan.bundles
    end

    old_triples = _BundleTriple[]
    for triple in keys(state.available_triples)
        _bundle_transition(triple) ∈ affected && push!(old_triples, triple)
    end

    seen = Set{_BundleTriple}()
    ordered_triples = _BundleTriple[]
    for scan in filtered_scans
        scan.triple ∈ seen && continue
        push!(seen, scan.triple)
        push!(ordered_triples, scan.triple)
    end
    for triple in old_triples
        triple ∈ seen && continue
        push!(ordered_triples, triple)
    end

    scan_order = 0
    for triple in ordered_triples
        old_ids = [id for id in get(state.available_triples, triple, Int[]) if haskey(state.available, id)]
        new_bundles = get(latest_map, triple, BundleEnablement{T}[])
        keep_count = min(length(old_ids), length(new_bundles))
        new_ids = Int[]
        sizehint!(new_ids, length(new_bundles))

        for i in 1:keep_count
            id = old_ids[i]
            old_record = state.available[id]
            new_record = new_bundles[i]
            scan_order += 1
            _reconcile_guard_error!(state, id, old_record, new_record, scan_order; on_event=on_event)
            state.available[id] = new_record
            _set_ready_membership!(state, id, new_record.status === :ready)
            push!(new_ids, id)
        end

        for i in (keep_count + 1):length(old_ids)
            id = old_ids[i]
            old_record = state.available[id]
            _reconcile_guard_error!(state, id, old_record, nothing, scan_order; on_event=on_event)
            _drop_available_id!(state, id)
        end

        for i in (keep_count + 1):length(new_bundles)
            new_record = new_bundles[i]
            id = _allocate_admission_id!(state)
            state.available[id] = new_record
            scan_order += 1
            _reconcile_guard_error!(state, id, nothing, new_record, scan_order; on_event=on_event)
            _set_ready_membership!(state, id, new_record.status === :ready)
            push!(new_ids, id)
        end

        if isempty(new_ids)
            delete!(state.available_triples, triple)
        else
            state.available_triples[triple] = new_ids
        end
    end

    return nothing
end

function _pop_ready!(state::_SchedulerState)
    while !isempty(state.ready)
        id = popfirst!(state.ready)
        delete!(state.ready_set, id)
        record = get(state.available, id, nothing)
        isnothing(record) && continue
        record.status === :ready || continue
        return id
    end
    return nothing
end

function _spawn_execution_task(
    completed::Channel{Task},
    firing::_PendingFiring{T},
    run_executor,
) where T<:AbstractToken
    inputs = copy(firing.grabbed)
    task = Threads.@spawn begin
        local result
        try
            result = run_executor(firing.bundle.transition_id, inputs)
        catch
            _safe_put_completed!(completed, current_task())
            rethrow()
        end
        _safe_put_completed!(completed, current_task())
        result
    end
    return task, inputs
end

function _register_pending_firing!(
    state::_SchedulerState{T},
    task::Task,
    firing::_PendingFiring{T},
    inputs::Vector{T},
    on_event,
) where T<:AbstractToken
    state.pending[task] = firing
    emit(on_event, TransitionStarted(firing.bundle, firing.firing_id, firing.attempt, inputs))
    return nothing
end

function _launch_ready_bundle!(
    state::_SchedulerState{T},
    net::Net,
    completed::Channel{Task},
    admission_id::Int,
    record::BundleEnablement{T},
    run_executor,
    on_event,
) where T<:AbstractToken
    result = grab(state.marking, net, record.bundle)
    if isnothing(result)
        _drop_available_id!(state, admission_id)
        return false
    end

    new_marking, grabbed, grabbed_by_place = result
    firing = _PendingFiring(
        record.bundle,
        admission_id,
        _allocate_firing_id!(state),
        1,
        grabbed,
        grabbed_by_place,
    )

    task, inputs = try
        _spawn_execution_task(completed, firing, run_executor)
    catch
        _set_ready_membership!(state, admission_id, record.status === :ready)
        rethrow()
    end

    state.marking = new_marking
    _drop_available_id!(state, admission_id)
    _register_pending_firing!(state, task, firing, inputs, on_event)
    state.fired += 1
    _refresh_enablement!(state, net, net.recheck[record.bundle.transition_id]; on_event=on_event)
    return true
end

function _launch_ready!(
    state::_SchedulerState{T},
    net::Net,
    completed::Channel{Task},
    max_concurrency::Int,
    fuse::Int,
    run_executor,
    on_event,
) where T<:AbstractToken
    while length(state.pending) < max_concurrency && state.fired < fuse
        id = _pop_ready!(state)
        isnothing(id) && break
        record = get(state.available, id, nothing)
        isnothing(record) && continue
        _launch_ready_bundle!(state, net, completed, id, record, run_executor, on_event)
    end
    return nothing
end

function _ensure_known_output_run_keys!(state::_SchedulerState, outputs)
    for token in outputs
        rk = run_key(token)
        haskey(state.runs, rk) || throw(ArgumentError(
            "executor emitted token with unknown run_key=$rk; outputs must stay within the initial run set",
        ))
    end
    return nothing
end

function _commit_firing_outputs!(
    state::_SchedulerState{T},
    net::Net,
    firing::_PendingFiring{T},
    output,
) where T<:AbstractToken
    outputs = _normalize_outputs(output, T)
    _ensure_known_output_run_keys!(state, outputs)
    state.marking = output isa AbstractVector ?
        drop(state.marking, net, firing.bundle.transition_id, outputs) :
        drop(state.marking, net, firing.bundle.transition_id, only(outputs))
    return outputs
end

function _retry_firing!(
    state::_SchedulerState{T},
    completed::Channel{Task},
    firing::_PendingFiring{T},
    msg::String,
    run_executor,
    on_event,
) where T<:AbstractToken
    failed_attempt = firing.attempt
    emit(on_event, TransitionFailed(
        firing.bundle,
        firing.firing_id,
        failed_attempt,
        msg,
        true,
    ))

    firing.attempt = failed_attempt + 1

    try
        task, inputs = _spawn_execution_task(completed, firing, run_executor)
        _register_pending_firing!(state, task, firing, inputs, on_event)
    catch e
        firing.attempt = failed_attempt
        e isa InterruptException && rethrow()
        return _safe_showerror(e)
    end

    state.fired += 1
    return nothing
end

function _close_failed_firing!(
    state::_SchedulerState{T},
    net::Net,
    firing::_PendingFiring{T},
    msg::String,
    status::Symbol,
    on_event,
    restore_inputs::Bool = false,
) where T<:AbstractToken
    run = _get_or_create_run_progress!(state, firing.bundle.run_key)
    push!(run.trace, TransitionResult(
        firing.bundle,
        firing.firing_id,
        status,
        T[],
        msg,
        firing.attempt,
    ))
    emit(on_event, TransitionFailed(
        firing.bundle,
        firing.firing_id,
        firing.attempt,
        msg,
        false,
    ))

    if restore_inputs || status === :fuse_blocked
        state.marking = misfire(state.marking, firing.grabbed_by_place)
    end
    if restore_inputs && status === :failed
        _block_run_for_commit_failure!(state, firing.bundle.run_key)
    end
    if status === :fuse_blocked
        run.fuse_blocked = true
    end

    _refresh_enablement!(state, net, net.recheck[firing.bundle.transition_id]; on_event=on_event)
    return nothing
end

function _handle_firing_failure!(
    state::_SchedulerState{T},
    net::Net,
    firing::_PendingFiring{T},
    msg::String,
    completed::Channel{Task},
    fuse::Int,
    run_executor,
    on_event,
    restore_inputs::Bool = false,
) where T<:AbstractToken
    transition = net.transitions[firing.bundle.transition_id]

    if firing.attempt <= transition.retries
        if state.fired < fuse
            retry_error = _retry_firing!(state, completed, firing, msg, run_executor, on_event)
            isnothing(retry_error) && return nothing
            msg = string(msg, "\nretry launch failed: ", retry_error)
            return _close_failed_firing!(
                state,
                net,
                firing,
                msg,
                :failed,
                on_event,
                restore_inputs,
            )
        end
        return _close_failed_firing!(state, net, firing, msg, :fuse_blocked, on_event)
    end

    return _close_failed_firing!(
        state,
        net,
        firing,
        msg,
        :failed,
        on_event,
        restore_inputs,
    )
end

function _handle_task_success!(
    state::_SchedulerState{T},
    net::Net,
    firing::_PendingFiring{T},
    done::Task,
    completed::Channel{Task},
    fuse::Int,
    run_executor,
    on_event,
) where T<:AbstractToken
    output = fetch(done)
    outputs = try
        _commit_firing_outputs!(state, net, firing, output)
    catch e
        _handle_firing_failure!(
            state,
            net,
            firing,
            _safe_showerror(e),
            completed,
            fuse,
            run_executor,
            on_event,
            true,
        )
        return nothing
    end

    run = _get_or_create_run_progress!(state, firing.bundle.run_key)
    push!(run.trace, TransitionResult(
        firing.bundle,
        firing.firing_id,
        :completed,
        outputs,
        nothing,
        firing.attempt,
    ))
    emit(on_event, TransitionCompleted(
        firing.bundle,
        firing.firing_id,
        firing.attempt,
        outputs,
    ))
    _refresh_enablement!(state, net, net.recheck[firing.bundle.transition_id]; on_event=on_event)
    return nothing
end

function _handle_task_failure!(
    state::_SchedulerState{T},
    net::Net,
    firing::_PendingFiring{T},
    done::Task,
    completed::Channel{Task},
    fuse::Int,
    run_executor,
    on_event,
) where T<:AbstractToken
    msg = _safe_showerror(done.result)
    return _handle_firing_failure!(
        state,
        net,
        firing,
        msg,
        completed,
        fuse,
        run_executor,
        on_event,
    )
end

function _run_has_ready_bundle(state::_SchedulerState, rk::String)
    for record in values(state.available)
        record.bundle.run_key == rk && record.status === :ready && return true
    end
    return false
end

function _earliest_guard_error(state::_SchedulerState{T}, run::_RunProgress{T}) where T<:AbstractToken
    admission_id, episode = argmin(
        entry -> (entry[2].first_seen_epoch, entry[2].first_seen_order),
        run.active_guard_errors,
    )
    return state.available[admission_id].bundle, episode.message
end

function _earliest_failed_firing(run::_RunProgress)
    failed = [step for step in run.trace if step.status === :failed]
    isempty(failed) && return nothing
    return first(sort!(failed; by=step -> step.firing_id))
end

function _finalize_results(
    net::Net,
    state::_SchedulerState{T},
    seeded::Vector{String};
    fuse::Int,
    on_event = nothing,
) where T<:AbstractToken
    results = RunResult{T}[]
    for rk in seeded
        run = _get_or_create_run_progress!(state, rk)
        terminal_bundle = nothing
        terminal_transition = nothing
        selection_error = run.selection_error

        if !isnothing(selection_error)
            status = :failed
            reason = :selection_error
            err = selection_error.message
            terminal_transition = selection_error.transition_id
        else
            earliest_failed = _earliest_failed_firing(run)
            if !isnothing(earliest_failed)
                status = :failed
                reason = :executor_failed
                err = something(earliest_failed.error)
                terminal_bundle = earliest_failed.bundle
            elseif run_completed(state.marking, rk, net.from_places)
                status = :completed
                reason = nothing
                err = nothing
            elseif !isempty(run.active_guard_errors)
                terminal_bundle, err = _earliest_guard_error(state, run)
                status = :failed
                reason = :guard_error
            elseif run.fuse_blocked || (state.fired >= fuse && _run_has_ready_bundle(state, rk))
                status = :incomplete
                reason = :fuse_exhausted
                err = nothing
            else
                status = :incomplete
                reason = :no_enabled_transition
                err = nothing
            end
        end

        result = RunResult(
            rk,
            status,
            err,
            reason,
            terminal_bundle,
            terminal_transition,
            run.trace,
            run_marking(state.marking, rk),
        )
        emit(on_event, RunFinished(result))
        push!(results, result)
    end
    return results
end

"""
    Run the engine to completion
    fuse limits total transition launches before stopping, including retries
    Once fuse is exhausted the engine launches no new firings, but lets in-flight work drain
    max_concurrency caps how many transitions run in parallel
    RunResult.terminal_reason is one of
      :selection_error, :executor_failed, :guard_error, :fuse_exhausted, :no_enabled_transition
    on_event receives EngineEvent instances as they happen
"""
function fire(
    net::Net,
    marking::Marking{T};
    fuse::Int = 1000,
    max_concurrency::Int = 10,
    on_event = nothing,
) where T<:AbstractToken
    max_concurrency >= 1 || throw(ArgumentError("max_concurrency must be at least 1"))
    fuse >= 0 || throw(ArgumentError("fuse must be at least 0"))

    seeded = fuses(marking)
    if isempty(seeded)
        issues = new_issues(net, marking)
        validate!(issues, net)
        validate_marking!(issues, marking, net)
        isempty(issues) || throw(ArgumentError("invalid net/marking: $(issues[1].message)"))
        return RunResult{T}[]
    end

    issues = validate(net, marking)
    isempty(issues) || throw(ArgumentError("invalid net/marking: $(issues[1].message)"))

    state = _scheduler_state(marking)

    run_executor(tid::Symbol, tokens::Vector{T}) =
        execute(get_executor(net.transitions[tid].executor), tid, tokens)

    completed = Channel{Task}(max_concurrency)
    interrupted = false
    try
        _refresh_enablement!(state, net, sort!(collect(keys(net.transitions))); on_event=on_event)
        _launch_ready!(state, net, completed, max_concurrency, fuse, run_executor, on_event)

        while !isempty(state.pending)
            done = take!(completed)
            try
                wait(done)
            catch e
                e isa InterruptException && rethrow()
            end
            firing = get(state.pending, done, nothing)
            isnothing(firing) && continue
            delete!(state.pending, done)

            if istaskfailed(done)
                _handle_task_failure!(state, net, firing, done, completed, fuse, run_executor, on_event)
            else
                _handle_task_success!(state, net, firing, done, completed, fuse, run_executor, on_event)
            end

            _launch_ready!(state, net, completed, max_concurrency, fuse, run_executor, on_event)
        end

        return _finalize_results(net, state, seeded; fuse=fuse, on_event=on_event)
    catch e
        interrupted = e isa InterruptException
        rethrow()
    finally
        close(completed)
        if !interrupted
            for task in keys(state.pending)
                try
                    wait(task)
                catch e
                    e isa InterruptException && rethrow()
                end
            end
        end
    end
end
