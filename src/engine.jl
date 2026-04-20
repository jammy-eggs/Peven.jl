"""
    Check whether a transition's guard allows firing
    Nothing guard always passes, Function guard is called with the peeked tokens
"""
@inline evaluate_guard(::Nothing, _) = true
@inline evaluate_guard(guard::Function, tokens) = guard(tokens)

"""
    Drop a token into the output places of a fired transition
    Copy-on-write: returns a new Marking, original is unchanged
    Throws ArgumentError if any output place would exceed its capacity
"""
function drop(marking::Marking{T}, net::Net, tid::Symbol, token::T) where T<:AbstractToken
    tokens = Dict{Symbol, Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
    for (pid, weight) in net.output_arcs[tid]
        bucket = get!(() -> T[], tokens, pid)
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
) where {T<:AbstractToken, S<:T}
    isempty(outputs) && throw(ArgumentError("executor output vector must be non-empty"))

    arcs = get(net.output_arcs, tid, Tuple{Symbol, Int}[])
    length(arcs) == 1 || throw(ArgumentError("executor output vector requires exactly one output arc"))
    pid, weight = only(arcs)
    weight == 1 || throw(ArgumentError("executor output vector requires a weight-1 output arc"))

    bucket = get(marking.tokens_by_place, pid, T[])
    cap = net.places[pid].capacity
    !isnothing(cap) && length(bucket) + length(outputs) > cap &&
        throw(ArgumentError("capacity exceeded at :$pid"))

    tokens = Dict{Symbol, Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
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
function misfire(marking::Marking{T}, grabbed_by_place::Dict{Symbol, Vector{T}}) where T<:AbstractToken
    restored = Dict{Symbol, Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
    for (pid, returned) in grabbed_by_place
        append!(get!(() -> T[], restored, pid), returned)
    end
    return Marking(restored)
end

"""
    Collect the unique run_keys from the initial marking in first-seen order
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
    filtered = Dict{Symbol, Vector{T}}()
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

"""
    Grab tokens for a transition or throw if not enough are available
    Convenience wrapper around grab for callers who want the exception
"""
function take(marking::Marking{T}, net::Net, tid::Symbol, rk::String) where T<:AbstractToken
    result = grab(marking, net, tid, rk)
    isnothing(result) && throw(ArgumentError("transition :$tid not enabled for run_key=$rk"))
    return result
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

struct _EnablementStatus
    status::Symbol
    error::Union{Nothing, String}
end

"""
    Internal enablement collector shared by hot/cold/fire
    Returns one entry per candidate (transition, run_key) pair
    status is :ready, :guard_blocked, or :guard_errored
"""
function _collect_enablement(
    net::Net,
    marking::Marking{T};
    failed::Set{String} = Set{String}(),
    in_flight::Set{Tuple{Symbol, String}} = Set{Tuple{Symbol, String}}(),
    check::Union{Nothing, Vector{Symbol}} = nothing,
    on_guard_error = nothing,
) where T<:AbstractToken

    entries = Dict{Tuple{Symbol, String}, _EnablementStatus}()
    live = check === nothing ? sort!(collect(keys(net.transitions))) : check

    for tid in live
        arcs = net.input_arcs[tid]
        isempty(arcs) && continue

        candidates = nothing
        for (pid, weight) in arcs
            toks = get(marking.tokens_by_place, pid, T[])
            counts = Dict{String, Int}()
            for token in toks
                rk = run_key(token)
                counts[rk] = get(counts, rk, 0) + 1
            end
            can_fire = Set(rk for (rk, cnt) in counts if cnt >= weight)
            if candidates === nothing
                candidates = can_fire
            else
                intersect!(candidates, can_fire)
            end
        end

        candidates === nothing && continue
        isempty(candidates) && continue

        for rk in sort!(collect(candidates))
            rk ∈ failed && continue
            (tid, rk) ∈ in_flight && continue

            peeked = T[]
            for (pid, weight) in arcs
                claimed = 0
                for token in get(marking.tokens_by_place, pid, T[])
                    run_key(token) == rk || continue
                    push!(peeked, token)
                    claimed += 1
                    claimed >= weight && break
                end
            end

            status = :ready
            error = nothing
            try
                evaluate_guard(net.transitions[tid].guard, peeked) || (status = :guard_blocked)
            catch e
                status = :guard_errored
                error = sprint(showerror, e)
                on_guard_error !== nothing && on_guard_error(tid, rk, e)
            end

            entries[(tid, rk)] = _EnablementStatus(status, error)
        end
    end

    return entries
end

"""
    Which (transition, run_key) pairs are hot — ready to fire from the current marking?
    Returns a sorted list of primed (transition, run_key) pairs, filtered by failed/in_flight/guards
    check=nothing considers all transitions, check=[tids] considers only those with live tokens
    on_guard_error receives (tid, rk, exception) when a guard crashes — nothing means silent
"""
function hot(
    net::Net,
    marking::Marking{T};
    failed::Set{String} = Set{String}(),
    in_flight::Set{Tuple{Symbol, String}} = Set{Tuple{Symbol, String}}(),
    check::Union{Nothing, Vector{Symbol}} = nothing,
    on_guard_error = nothing,
) where T<:AbstractToken
    entries = _collect_enablement(
        net,
        marking;
        failed=failed,
        in_flight=in_flight,
        check=check,
        on_guard_error=on_guard_error,
    )
    return sort!(Tuple{Symbol, String}[pair for (pair, entry) in entries if entry.status === :ready])
end

"""
    Which transitions have no enabled run_key — the inverse of hot()
    Returns a sorted list of transition ids that cannot fire from the current marking
    Respects the same check subset and filters as hot()
    Useful for diagnosing deadlocks and incomplete runs
"""
function cold(
    net::Net,
    marking::Marking{T};
    failed::Set{String} = Set{String}(),
    in_flight::Set{Tuple{Symbol, String}} = Set{Tuple{Symbol, String}}(),
    check::Union{Nothing, Vector{Symbol}} = nothing,
    on_guard_error = nothing,
) where T<:AbstractToken
    enabled = Set{Symbol}(tid for (tid, _) in hot(
        net, marking;
        failed=failed,
        in_flight=in_flight,
        check=check,
        on_guard_error=on_guard_error,
    ))
    scanned = check === nothing ? sort!(collect(keys(net.transitions))) : sort!(collect(Set(check)))
    return [tid for tid in scanned if tid ∉ enabled]
end

"""
    Try to grab tokens from input places for a transition and run_key
    Returns (new_marking, grabbed, grabbed_by_place) or nothing if not enough tokens
    Copy-on-write: original marking is unchanged
    Multi-place selection is keyed by run_key only: when several eligible tokens exist
    in multiple input places for the same run, grab claims the first available tokens
    per place. A keyed-join extension for correlated multi-place inputs is intentionally
    deferred.
"""
function grab(
    marking::Marking{T},
    net::Net,
    tid::Symbol,
    rk::String,
) where T<:AbstractToken

    tokens = Dict{Symbol, Vector{T}}(k => copy(v) for (k, v) in marking.tokens_by_place)
    grabbed = T[]
    grabbed_by_place = Dict{Symbol, Vector{T}}()

    for (pid, weight) in net.input_arcs[tid]
        bucket = get(tokens, pid, T[])
        available = count(t -> run_key(t) == rk, bucket)
        available < weight && return nothing

        claimed = T[]
        leftover = T[]
        for token in bucket
            if run_key(token) == rk && length(claimed) < weight
                push!(claimed, token)
            else
                push!(leftover, token)
            end
        end

        append!(grabbed, claimed)
        grabbed_by_place[pid] = claimed
        tokens[pid] = leftover
    end

    return (Marking(tokens), grabbed, grabbed_by_place)
end

mutable struct _GuardErrorEpisode
    message::String
    first_seen_pass::Int
end

mutable struct _RunProgress{T<:AbstractToken}
    trace::Vector{TransitionResult{T}}
    active_guard_errors::Dict{Symbol, _GuardErrorEpisode}
    executor_failed::Bool
    executor_error::Union{Nothing, String}
    fuse_blocked::Bool
end

mutable struct _PendingFiring{T<:AbstractToken}
    transition_id::Symbol
    run_key::String
    firing_id::Int
    attempt::Int
    grabbed::Vector{T}
    grabbed_by_place::Dict{Symbol, Vector{T}}
end

mutable struct _SchedulerState{T<:AbstractToken}
    marking::Marking{T}
    runs::Dict{String, _RunProgress{T}}
    enablement::Dict{Tuple{Symbol, String}, _EnablementStatus}
    ready::Vector{Tuple{Symbol, String}}
    ready_set::Set{Tuple{Symbol, String}}
    pending::Dict{Task, _PendingFiring{T}}
    fired::Int
    next_firing_id::Int
    reconcile_pass::Int

    function _SchedulerState{T}(
        marking::Marking{T},
        runs::Dict{String, _RunProgress{T}},
        enablement::Dict{Tuple{Symbol, String}, _EnablementStatus},
        ready::Vector{Tuple{Symbol, String}},
        ready_set::Set{Tuple{Symbol, String}},
        pending::Dict{Task, _PendingFiring{T}},
        fired::Int,
        next_firing_id::Int,
        reconcile_pass::Int,
    ) where T<:AbstractToken
        new{T}(marking, runs, enablement, ready, ready_set, pending, fired, next_firing_id, reconcile_pass)
    end
end

function _scheduler_state(marking::Marking{T}) where T<:AbstractToken
    runs = Dict{String, _RunProgress{T}}()
    for rk in fuses(marking)
        runs[rk] = _RunProgress{T}(
            TransitionResult{T}[],
            Dict{Symbol, _GuardErrorEpisode}(),
            false,
            nothing,
            false,
        )
    end
    return _SchedulerState{T}(
        marking,
        runs,
        Dict{Tuple{Symbol, String}, _EnablementStatus}(),
        Tuple{Symbol, String}[],
        Set{Tuple{Symbol, String}}(),
        Dict{Task, _PendingFiring{T}}(),
        0,
        1,
        0,
    )
end

function _get_or_create_run_progress!(state::_SchedulerState{T}, rk::String) where T<:AbstractToken
    return get!(state.runs, rk) do
        _RunProgress{T}(
            TransitionResult{T}[],
            Dict{Symbol, _GuardErrorEpisode}(),
            false,
            nothing,
            false,
        )
    end
end

function _remove_ready_for_transitions!(state::_SchedulerState, affected::Set{Symbol})
    filter!(state.ready) do pair
        keep = pair[1] ∉ affected
        keep || delete!(state.ready_set, pair)
        keep
    end
    return nothing
end

function _reconcile_enablement!(
    state::_SchedulerState{T},
    latest::Dict{Tuple{Symbol, String}, _EnablementStatus},
    check::Vector{Symbol};
    on_event = nothing,
) where T<:AbstractToken
    state.reconcile_pass += 1
    affected = Set(check)
    _remove_ready_for_transitions!(state, affected)

    previous = Dict{Tuple{Symbol, String}, _EnablementStatus}()
    for (pair, entry) in state.enablement
        pair[1] ∈ affected || continue
        previous[pair] = entry
    end
    for pair in keys(previous)
        delete!(state.enablement, pair)
    end

    pairs = sort!(collect(union(Set(keys(previous)), Set(keys(latest)))))
    for pair in pairs
        tid, rk = pair
        run = _get_or_create_run_progress!(state, rk)
        previous_entry = get(previous, pair, nothing)
        current_entry = get(latest, pair, nothing)
        old_guard = _is_guard_errored(previous_entry)
        new_guard = _is_guard_errored(current_entry)

        if run.executor_failed
            old_guard && delete!(run.active_guard_errors, tid)
            continue
        end

        if old_guard && !new_guard
            delete!(run.active_guard_errors, tid)
        elseif new_guard
            if haskey(run.active_guard_errors, tid)
                run.active_guard_errors[tid].message = something(current_entry.error)
            else
                episode = _GuardErrorEpisode(
                    something(current_entry.error),
                    state.reconcile_pass,
                )
                run.active_guard_errors[tid] = episode
                emit(on_event, GuardErrored(tid, rk, episode.message))
            end
        end

        isnothing(current_entry) || (state.enablement[pair] = current_entry)
        if !isnothing(current_entry) &&
           current_entry.status === :ready &&
           !run.executor_failed &&
           pair ∉ state.ready_set
            push!(state.ready, pair)
            push!(state.ready_set, pair)
        end
    end

    return nothing
end

@inline _is_guard_errored(entry) = !isnothing(entry) && entry.status === :guard_errored

function _earliest_guard_error(run::_RunProgress)
    tid, episode = first(sort!(
        collect(run.active_guard_errors);
        by=entry -> (entry[2].first_seen_pass, entry[1]),
    ))
    return tid, episode.message
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
        has_ready = any(
            pair -> pair[2] == rk && get(state.enablement, pair, nothing) !== nothing &&
                state.enablement[pair].status === :ready,
            keys(state.enablement),
        )

        if run.executor_failed
            status = :failed
            reason = :executor_failed
            err = run.executor_error
        elseif run_completed(state.marking, rk, net.from_places)
            status = :completed
            reason = nothing
            err = nothing
        elseif !isempty(run.active_guard_errors)
            _, err = _earliest_guard_error(run)
            status = :failed
            reason = :guard_error
        elseif run.fuse_blocked || (state.fired >= fuse && has_ready)
            status = :incomplete
            reason = :fuse_exhausted
            err = nothing
        else
            status = :incomplete
            reason = :no_enabled_transition
            err = nothing
        end

        result = RunResult(rk, status, err, reason, run.trace, run_marking(state.marking, rk))
        emit(on_event, RunFinished(result))
        push!(results, result)
    end
    return results
end

@inline _allocate_firing_id!(state::_SchedulerState) =
    (firing_id = state.next_firing_id; state.next_firing_id += 1; firing_id)

function _clear_ready_for_run!(state::_SchedulerState, rk::String)
    filter!(state.ready) do pair
        keep = pair[2] != rk
        keep || delete!(state.ready_set, pair)
        keep
    end
    return nothing
end

function _pop_ready!(state::_SchedulerState)
    while !isempty(state.ready)
        pair = popfirst!(state.ready)
        delete!(state.ready_set, pair)
        run = get(state.runs, pair[2], nothing)
        !isnothing(run) && run.executor_failed && continue
        entry = get(state.enablement, pair, nothing)
        isnothing(entry) && continue
        entry.status === :ready || continue
        return pair
    end
    return nothing
end

function _refresh_enablement!(state::_SchedulerState{T}, net::Net, check::Vector{Symbol}; on_event = nothing) where T<:AbstractToken
    latest = _collect_enablement(net, state.marking; check=check)
    _reconcile_enablement!(state, latest, check; on_event=on_event)
    return nothing
end

function _spawn_task!(
    state::_SchedulerState{T},
    completed::Channel{Task},
    firing::_PendingFiring{T},
    run_executor,
    on_event,
) where T<:AbstractToken
    inputs = copy(firing.grabbed)
    emit(on_event, TransitionStarted(
        firing.transition_id,
        firing.run_key,
        firing.firing_id,
        firing.attempt,
        inputs,
    ))
    task = Threads.@spawn begin
        local result
        try
            result = run_executor(firing.transition_id, inputs)
        catch
            put!(completed, current_task())
            rethrow()
        end
        put!(completed, current_task())
        result
    end
    state.pending[task] = firing
    return nothing
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
        pair = _pop_ready!(state)
        isnothing(pair) && break

        tid, rk = pair
        result = grab(state.marking, net, tid, rk)
        isnothing(result) && continue

        state.marking, grabbed, grabbed_by_place = result
        firing = _PendingFiring(
            tid,
            rk,
            _allocate_firing_id!(state),
            1,
            grabbed,
            grabbed_by_place,
        )
        state.fired += 1
        _spawn_task!(state, completed, firing, run_executor, on_event)
        _refresh_enablement!(state, net, net.recheck[tid]; on_event=on_event)
    end
    return nothing
end

function _handle_task_success!(
    state::_SchedulerState{T},
    net::Net,
    firing::_PendingFiring{T},
    done::Task,
    on_event,
) where T<:AbstractToken
    output = fetch(done)
    outputs = _normalize_outputs(output, T)
    state.marking = output isa AbstractVector ?
        drop(state.marking, net, firing.transition_id, outputs) :
        drop(state.marking, net, firing.transition_id, only(outputs))

    run = _get_or_create_run_progress!(state, firing.run_key)
    push!(run.trace, TransitionResult(
        firing.transition_id,
        firing.run_key,
        firing.firing_id,
        :completed,
        outputs,
        nothing,
        firing.attempt,
    ))
    emit(on_event, TransitionCompleted(
        firing.transition_id,
        firing.run_key,
        firing.firing_id,
        firing.attempt,
        outputs,
    ))
    _refresh_enablement!(state, net, net.recheck[firing.transition_id]; on_event=on_event)
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
    msg = sprint(showerror, done.result)
    transition = net.transitions[firing.transition_id]
    run = _get_or_create_run_progress!(state, firing.run_key)

    if !run.executor_failed && firing.attempt <= transition.retries && state.fired < fuse
        emit(on_event, TransitionFailed(
            firing.transition_id,
            firing.run_key,
            firing.firing_id,
            firing.attempt,
            msg,
            true,
        ))
        firing.attempt += 1
        state.fired += 1
        _spawn_task!(state, completed, firing, run_executor, on_event)
        return nothing
    end

    push!(run.trace, TransitionResult(
        firing.transition_id,
        firing.run_key,
        firing.firing_id,
        :failed,
        T[],
        msg,
        firing.attempt,
    ))
    emit(on_event, TransitionFailed(
        firing.transition_id,
        firing.run_key,
        firing.firing_id,
        firing.attempt,
        msg,
        false,
    ))

    if !run.executor_failed && firing.attempt <= transition.retries
        state.marking = misfire(state.marking, firing.grabbed_by_place)
        run.fuse_blocked = true
    else
        if !run.executor_failed
            run.executor_failed = true
            run.executor_error = msg
        end
        _clear_ready_for_run!(state, firing.run_key)
    end

    _refresh_enablement!(state, net, net.recheck[firing.transition_id]; on_event=on_event)
    return nothing
end

"""
    Run the engine to completion
    fuse limits total transition launches before stopping, including retries
    Once fuse is exhausted the engine launches no new firings, but lets in-flight work drain
    max_concurrency caps how many transitions run in parallel
    RunResult.terminal_reason is one of :executor_failed, :guard_error, :fuse_exhausted, :no_enabled_transition
    on_event receives EngineEvent instances as they happen
"""
function fire(
    net::Net,
    marking::Marking{T};
    fuse::Int = 1000,
    max_concurrency::Int = 10,
    on_event = nothing,
) where T <: AbstractToken

    issues = validate(net, marking)
    isempty(issues) || throw(ArgumentError("invalid net/marking: $(issues[1].message)"))

    seeded = fuses(marking)
    isempty(seeded) && return RunResult{T}[]
    state = _scheduler_state(marking)

    run_executor(tid::Symbol, tokens::Vector{T}) =
        execute(get_executor(net.transitions[tid].executor), tid, tokens)
    completed = Channel{Task}(max_concurrency)
    _refresh_enablement!(state, net, sort!(collect(keys(net.transitions))); on_event=on_event)
    _launch_ready!(state, net, completed, max_concurrency, fuse, run_executor, on_event)

    # take! blocks until a task puts itself on the channel — no spinning
    while !isempty(state.pending)
        done = take!(completed)
        try; wait(done); catch; end  # ensure task has fully exited
        firing = state.pending[done]
        delete!(state.pending, done)

        if istaskfailed(done)
            _handle_task_failure!(state, net, firing, done, completed, fuse, run_executor, on_event)
        else
            _handle_task_success!(state, net, firing, done, on_event)
        end

        _launch_ready!(state, net, completed, max_concurrency, fuse, run_executor, on_event)
    end

    return _finalize_results(net, state, seeded; fuse=fuse, on_event=on_event)
end
