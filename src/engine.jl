# Shared runtime core

@inline evaluate_guard(::Nothing, _) = true
@inline evaluate_guard(guard::Function, tokens) = guard(tokens)

@inline emit(::Nothing, _) = nothing
@inline function emit(hook, event)
    try
        hook(event)
    catch e
        e isa InterruptException && rethrow()
        nothing
    end
end

@inline _seeded_run_keys(marking::Marking) = sort!(run_keys(marking))

function run_completed(marking::Marking, rk::String, from_places::Set{Symbol})
    for (pid, tokens) in marking.tokens_by_place
        pid ∈ from_places || continue
        for token in tokens
            run_key(token) == rk && return false
        end
    end
    return true
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

struct _ClaimSelectionError <: Exception
    failure::_SelectionFailure
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


# Enablement

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
    bucket_cache = Dict{Symbol,Dict{String,Vector{Tuple{Int,T}}}}()

    for tid in live
        transition = net.transitions[tid]
        input_specs = _input_specs_for_transition(net, transition)
        isempty(input_specs) && continue
        by_place = Dict{Symbol,Dict{String,Vector{Tuple{Int,T}}}}()
        for (pid, _) in input_specs
            by_place[pid] = get!(bucket_cache, pid) do
                _collect_run_buckets(marking, pid)
            end
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
    Bundle-level readiness view for the current marking.
    `on_guard_error` receives `(bundle, exception)` for each guard error.
    Selector exceptions propagate.
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
    Bundles that are ready to fire from the current marking.
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
    Transitions with no ready bundle in the current marking.
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


# Firing and token movement

const _NO_OUTPUT_PLACES = Symbol[]
const _ReadyEntry = Tuple{Int,Int}

@inline _output_places(net::Net, tid::Symbol) = get(net.output_arcs, tid, _NO_OUTPUT_PLACES)

@inline function _has_output_place(places::Vector{Symbol}, pid::Symbol)
    for place in places
        place === pid && return true
    end
    return false
end

function _normalize_output_batch(output, ::Type{T}) where T<:AbstractToken
    if output isa T
        return T[output]
    end

    output isa AbstractVector || throw(ArgumentError("executor output must be a token or vector of tokens"))

    outputs = T[]
    sizehint!(outputs, length(output))
    for token in output
        token isa T || throw(ArgumentError("executor output vector must match the marking token type"))
        push!(outputs, token)
    end
    return outputs
end

function _normalize_output_deposits(
    net::Net,
    tid::Symbol,
    output,
    ::Type{T},
) where T<:AbstractToken
    places = _output_places(net, tid)

    if output === nothing
        isempty(places) || throw(ArgumentError(
            "executor returned nothing for transition :$tid, but output places are defined",
        ))
        return Dict{Symbol,Vector{T}}()
    end

    if output isa AbstractDict
        expected_count = length(places)
        deposits = Dict{Symbol,Vector{T}}()
        sizehint!(deposits, expected_count)

        for (pid, value) in pairs(output)
            pid isa Symbol || throw(ArgumentError("executor output keys must be destination place Symbols"))
            _has_output_place(places, pid) || throw(ArgumentError(
                "executor output for transition :$tid references unknown output place :$pid",
            ))
            deposits[pid] = _normalize_output_batch(value, T)
        end

        length(deposits) == expected_count || throw(ArgumentError(
            "executor output for transition :$tid must provide outputs for exactly $places",
        ))
        return deposits
    end

    outputs = _normalize_output_batch(output, T)
    isempty(outputs) && throw(ArgumentError("executor output vector must be non-empty"))
    length(places) == 1 || throw(ArgumentError(
        "executor output for transition :$tid must be keyed by destination place when multiple output places are defined",
    ))
    return Dict(only(places) => outputs)
end

function _drop_outputs(
    marking::Marking{T},
    net::Net,
    deposits::Dict{Symbol,Vector{T}},
) where T<:AbstractToken
    tokens = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)

    for (pid, deposited) in deposits
        bucket = get(marking.tokens_by_place, pid, nothing)
        cap = net.places[pid].capacity
        bucket_len = isnothing(bucket) ? 0 : length(bucket)
        !isnothing(cap) && bucket_len + length(deposited) > cap &&
            throw(ArgumentError("capacity exceeded at :$pid"))
    end

    for (pid, deposited) in deposits
        append!(get!(()->T[], tokens, pid), deposited)
    end

    return Marking(tokens)
end

"""
    Drop one output token through a transition's output arcs.
    Returns a new marking and leaves the original unchanged.
"""
function drop(marking::Marking{T}, net::Net, tid::Symbol, token::T) where T<:AbstractToken
    deposits = _normalize_output_deposits(net, tid, token, T)
    return _drop_outputs(marking, net, deposits)
end

"""
    Drop many output tokens through a transition's output arcs.
    Returns a new marking and leaves the original unchanged.
"""
function drop(
    marking::Marking{T},
    net::Net,
    tid::Symbol,
    outputs::AbstractVector{S},
) where {T<:AbstractToken,S<:T}
    deposits = _normalize_output_deposits(net, tid, outputs, T)
    return _drop_outputs(marking, net, deposits)
end

"""
    Drop explicit per-place outputs through a transition's output arcs.
    Returns a new marking and leaves the original unchanged.
"""
function drop(
    marking::Marking{T},
    net::Net,
    tid::Symbol,
    outputs_by_place::AbstractDict,
) where T<:AbstractToken
    deposits = _normalize_output_deposits(net, tid, outputs_by_place, T)
    return _drop_outputs(marking, net, deposits)
end

"""
    Restore grabbed tokens to their original places.
"""
function misfire(marking::Marking{T}, grabbed_by_place::Dict{Symbol,Vector{T}}) where T<:AbstractToken
    restored = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
    for (pid, returned) in grabbed_by_place
        append!(get!(()->T[], restored, pid), returned)
    end
    return Marking(restored)
end

function _claim_positions(marking::Marking{T}, net::Net, bundle::BundleRef) where T<:AbstractToken
    transition = net.transitions[bundle.transition_id]
    input_specs = _input_specs_for_bundle(net, bundle)
    claimed_positions = Dict{Symbol,Vector{Int}}()
    claimed_by_place = Dict{Symbol,Vector{T}}()
    selector = transition.join_by

    for (pid, weight) in input_specs
        bucket = get(marking.tokens_by_place, pid, T[])
        start = (bundle.ordinal - 1) * weight + 1
        stop = start + weight - 1
        selected_positions = Int[]
        selected_tokens = T[]
        sizehint!(selected_positions, weight)
        sizehint!(selected_tokens, weight)
        matched = 0
        for (idx, token) in pairs(bucket)
            run_key(token) == bundle.run_key || continue
            if !isnothing(selector)
                key = try
                    _selector_key(
                        something(selector),
                        bundle.transition_id,
                        pid,
                        bundle.run_key,
                        token,
                    )
                catch e
                    e isa InterruptException && rethrow()
                    throw(_ClaimSelectionError(
                        _SelectionFailure(bundle.transition_id, bundle.run_key, _safe_showerror(e)),
                    ))
                end
                isequal(key, bundle.selected_key) || continue
            end
            matched += 1
            matched < start && continue
            push!(selected_positions, idx)
            push!(selected_tokens, token)
            matched == stop && break
        end

        matched >= stop || return nothing
        claimed_positions[pid] = selected_positions
        claimed_by_place[pid] = selected_tokens
    end

    return claimed_positions, claimed_by_place
end

"""
    Try to reserve the exact bundle from a marking snapshot.
    Returns `(new_marking, grabbed, grabbed_by_place)` or `nothing`.
"""
function grab(marking::Marking{T}, net::Net, bundle::BundleRef) where T<:AbstractToken
    claimed = _claim_positions(marking, net, bundle)
    isnothing(claimed) && return nothing
    claimed_positions, grabbed_by_place = claimed

    tokens = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
    grabbed = T[]
    sizehint!(grabbed, sum(length, values(grabbed_by_place)))

    for (pid, _) in _input_specs_for_bundle(net, bundle)
        bucket = get(marking.tokens_by_place, pid, T[])
        claimed = get(claimed_positions, pid, Int[])
        leftover = T[]
        sizehint!(leftover, length(bucket) - length(claimed))
        next_claim = 1
        nclaimed = length(claimed)
        for (idx, token) in pairs(bucket)
            if next_claim <= nclaimed && idx == claimed[next_claim]
                push!(grabbed, token)
                next_claim += 1
            else
                push!(leftover, token)
            end
        end
        tokens[pid] = leftover
    end

    return Marking(tokens), grabbed, grabbed_by_place
end

"""
    Reserve the exact bundle or throw if it is stale or unavailable.
"""
function take(marking::Marking{T}, net::Net, bundle::BundleRef) where T<:AbstractToken
    result = grab(marking, net, bundle)
    isnothing(result) && throw(ArgumentError("stale or unavailable bundle"))
    return result
end


# Runtime scheduler

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
    blocked_bundles::Set{BundleRef}
    available::Dict{Int,BundleEnablement{T}}
    available_triples::Dict{_BundleTriple,Vector{Int}}
    ready::Vector{_ReadyEntry}
    ready_head::Int
    ready_versions::Dict{Int,Int}
    ready_set::Set{Int}
    pending::Dict{Task,_PendingFiring{T}}
    fired::Int
    next_firing_id::Int
    next_admission_id::Int
    scan_epoch::Int

    function _SchedulerState{T}(
        marking::Marking{T},
        runs::Dict{String,_RunProgress{T}},
        blocked_bundles::Set{BundleRef},
        available::Dict{Int,BundleEnablement{T}},
        available_triples::Dict{_BundleTriple,Vector{Int}},
        ready::Vector{_ReadyEntry},
        ready_head::Int,
        ready_versions::Dict{Int,Int},
        ready_set::Set{Int},
        pending::Dict{Task,_PendingFiring{T}},
        fired::Int,
        next_firing_id::Int,
        next_admission_id::Int,
        scan_epoch::Int,
    ) where T<:AbstractToken
        new(
            marking,
            runs,
            blocked_bundles,
            available,
            available_triples,
            ready,
            ready_head,
            ready_versions,
            ready_set,
            pending,
            fired,
            next_firing_id,
            next_admission_id,
            scan_epoch,
        )
    end
end

function _scheduler_state(marking::Marking{T}) where T<:AbstractToken
    runs = Dict{String,_RunProgress{T}}()
    for rk in _seeded_run_keys(marking)
        runs[rk] = _RunProgress{T}(
            TransitionResult{T}[],
            Dict{Int,_GuardErrorEpisode}(),
            nothing,
            false,
        )
    end
    return _SchedulerState{T}(
        marking,
        runs,
        Set{BundleRef}(),
        Dict{Int,BundleEnablement{T}}(),
        Dict{_BundleTriple,Vector{Int}}(),
        _ReadyEntry[],
        1,
        Dict{Int,Int}(),
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
        _RunProgress{T}(TransitionResult{T}[], Dict{Int,_GuardErrorEpisode}(), nothing, false)
    end
end

function _safe_put_completed!(completed::Channel{Task}, task::Task)
    try
        put!(completed, task)
    catch e
        e isa InvalidStateException || rethrow()
    end
    return nothing
end

@inline _allocate_firing_id!(state::_SchedulerState) =
    (firing_id = state.next_firing_id; state.next_firing_id += 1; firing_id)

@inline _allocate_admission_id!(state::_SchedulerState) =
    (admission_id = state.next_admission_id; state.next_admission_id += 1; admission_id)

function _remove_ready_id!(state::_SchedulerState, doomed::Int)
    doomed ∈ state.ready_set || return nothing
    delete!(state.ready_set, doomed)
    return nothing
end

@inline function _enqueue_ready!(state::_SchedulerState, id::Int)
    version = get(state.ready_versions, id, 0) + 1
    state.ready_versions[id] = version
    push!(state.ready, (id, version))
    push!(state.ready_set, id)
    return nothing
end

function _maybe_compact_ready!(state::_SchedulerState)
    if state.ready_head > length(state.ready)
        empty!(state.ready)
        state.ready_head = 1
    elseif state.ready_head > 32 && state.ready_head * 2 > length(state.ready)
        state.ready = state.ready[state.ready_head:end]
        state.ready_head = 1
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

    _remove_ready_id!(state, id)
    delete!(state.ready_versions, id)
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
            _enqueue_ready!(state, id)
        end
    elseif id ∈ state.ready_set
        _remove_ready_id!(state, id)
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
        !isnothing(run.selection_error) && push!(blocked, rk)
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
    _drop_available_for_run!(state, failure.run_key)
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
        in_flight=state.blocked_bundles,
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
    while state.ready_head <= length(state.ready)
        id, version = state.ready[state.ready_head]
        state.ready_head += 1
        id ∈ state.ready_set || continue
        version == get(state.ready_versions, id, 0) || continue
        delete!(state.ready_set, id)
        record = get(state.available, id, nothing)
        isnothing(record) && continue
        record.status === :ready || continue
        _maybe_compact_ready!(state)
        return id
    end
    _maybe_compact_ready!(state)
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
    result = try
        grab(state.marking, net, record.bundle)
    catch e
        if e isa _ClaimSelectionError
            _block_run_for_selection!(state, e.failure; on_event=on_event)
            _refresh_enablement!(state, net, net.recheck[record.bundle.transition_id]; on_event=on_event)
            return false
        end
        rethrow()
    end
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
    for deposited in values(outputs)
        for token in deposited
            rk = run_key(token)
            haskey(state.runs, rk) || throw(ArgumentError(
                "executor emitted token with unknown run_key=$rk; outputs must stay within the initial run set",
            ))
        end
    end
    return nothing
end

function _commit_firing_outputs!(
    state::_SchedulerState{T},
    net::Net,
    firing::_PendingFiring{T},
    output,
) where T<:AbstractToken
    deposits = _normalize_output_deposits(net, firing.bundle.transition_id, output, T)
    _ensure_known_output_run_keys!(state, deposits)
    state.marking = _drop_outputs(state.marking, net, deposits)
    return deposits
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
        Dict{Symbol,Vector{T}}(),
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
        push!(state.blocked_bundles, firing.bundle)
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

function _earliest_failed_firing(run::_RunProgress{T}) where T<:AbstractToken
    earliest = nothing
    for step in run.trace
        step.status === :failed || continue
        if isnothing(earliest) || step.firing_id < earliest.firing_id
            earliest = step
        end
    end
    return earliest
end

function _finalize_results(
    net::Net,
    state::_SchedulerState{T},
    seeded::Vector{String};
    fuse::Int,
    on_event = nothing,
) where T<:AbstractToken
    results = RunResult{T}[]
    sizehint!(results, length(seeded))
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
    Run the engine to completion.
    `fuse` limits total launches, including retries.
    `max_concurrency` caps concurrently executing transitions.
"""
function fire(
    net::Net,
    marking::Marking{T};
    fuse::Int = 1000,
    max_concurrency::Int = 10,
    on_event = nothing,
    executors::Union{Nothing,AbstractDict{Symbol,<:AbstractExecutor}} = nothing,
) where T<:AbstractToken
    max_concurrency >= 1 || throw(ArgumentError("max_concurrency must be at least 1"))
    fuse >= 0 || throw(ArgumentError("fuse must be at least 0"))

    seeded = _seeded_run_keys(marking)
    if isempty(seeded)
        issues = validate_fire_inputs(net, marking)
        isempty(issues) || throw(ArgumentError("invalid net/marking: $(issues[1].message)"))
        return RunResult{T}[]
    end

    issues = validate(net, marking)
    isempty(issues) || throw(ArgumentError("invalid net/marking: $(issues[1].message)"))

    state = _scheduler_state(marking)

    run_executor(tid::Symbol, tokens::Vector{T}) =
        execute(resolve_executor(net.transitions[tid].executor, executors), tid, tokens)

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
