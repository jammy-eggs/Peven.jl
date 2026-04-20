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

"""
    Which (transition, run_key) pairs are hot — ready to fire from the current marking?
    Returns a sorted list of primed (transition, run_key) pairs, filtered by failed/in_flight/guards
    check=nothing scans all transitions, check=[tids] scans only those with live tokens
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

    result = Tuple{Symbol, String}[]

    # check=nothing → full scan, check=[tids] → only transitions with live tokens
    live = check === nothing ? sort!(collect(keys(net.transitions))) : check

    for tid in live
        arcs = net.input_arcs[tid]
        isempty(arcs) && continue

        # For each input place, count tokens per run_key.
        # Keep only run_keys with enough tokens, then intersect across all input places.
        # First input place seeds candidates, each subsequent place narrows via intersect
        candidates = nothing
        for (pid, weight) in arcs
            toks = get(marking.tokens_by_place, pid, T[])
            counts = Dict{String, Int}()
            for t in toks
                rk = run_key(t)
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

            # Peek tokens that would be grabbed (read-only, no marking change)
            peeked = T[]
            for (pid, weight) in arcs
                n = 0
                for t in get(marking.tokens_by_place, pid, T[])
                    run_key(t) == rk || continue
                    push!(peeked, t)
                    n += 1
                    n >= weight && break
                end
            end

            # Guard check — false or exception means skip
            try
                evaluate_guard(net.transitions[tid].guard, peeked) || continue
            catch e
                on_guard_error !== nothing && on_guard_error(tid, rk, e)
                continue
            end

            push!(result, (tid, rk))
        end
    end

    return result
end

"""
    Which transitions have no enabled run_key — the inverse of hot()
    Returns a sorted list of transition ids that cannot fire from the current marking
    Respects the same scan subset and filters as hot()
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

mutable struct _PendingFiring{T<:AbstractToken}
    transition_id::Symbol
    run_key::String
    firing_id::Int
    attempt::Int
    grabbed::Vector{T}
    grabbed_by_place::Dict{Symbol, Vector{T}}
end

"""
    Run the engine to completion
    fuse limits total transition launches before stopping, including retries
    Once fuse is exhausted the engine launches no new firings, but lets in-flight work drain
    max_concurrency caps how many transitions run in parallel
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

    trace = TransitionResult{T}[]
    failed_runs = Set{String}()
    fired = 0
    next_firing_id = 1

    run_executor(tid::Symbol, tokens::Vector{T}) =
        execute(get_executor(net.transitions[tid].executor), tid, tokens)

    allocate_firing_id!() = (firing_id = next_firing_id; next_firing_id += 1; firing_id)

    function guard_error(tid, rk, e)
        msg = sprint(showerror, e)
        firing_id = allocate_firing_id!()
        push!(trace, TransitionResult(tid, rk, firing_id, :failed, T[], msg, 1))
        emit(on_event, TransitionFailed(tid, rk, firing_id, 1, msg, false))
    end

    ready = Tuple{Symbol, String}[]
    ready_set = Set{Tuple{Symbol, String}}()
    pending = Dict{Task, _PendingFiring{T}}()
    completed = Channel{Task}(max_concurrency)

    function rebuild_ready!()
        empty!(ready)
        empty!(ready_set)
        for pair in hot(net, marking; failed=failed_runs, on_guard_error=guard_error)
            push!(ready, pair)
            push!(ready_set, pair)
        end
        return nothing
    end

    function refresh_ready!(check::Vector{Symbol})
        affected = Set(check)
        filter!(ready) do pair
            keep = pair[1] ∉ affected && pair[2] ∉ failed_runs
            keep || delete!(ready_set, pair)
            keep
        end
        for pair in hot(net, marking; failed=failed_runs, check=check, on_guard_error=guard_error)
            pair ∈ ready_set && continue
            push!(ready, pair)
            push!(ready_set, pair)
        end
        return nothing
    end

    function pop_ready!()
        while !isempty(ready)
            pair = popfirst!(ready)
            delete!(ready_set, pair)
            pair[2] ∈ failed_runs && continue
            return pair
        end
        return nothing
    end

    function spawn_task!(firing::_PendingFiring{T})
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
        pending[task] = firing
        return nothing
    end

    function spawn_ready!()
        while length(pending) < max_concurrency && fired < fuse
            pair = pop_ready!()
            isnothing(pair) && break

            tid, rk = pair
            result = grab(marking, net, tid, rk)
            isnothing(result) && continue

            marking, grabbed, grabbed_by_place = result
            firing = _PendingFiring(
                tid,
                rk,
                allocate_firing_id!(),
                1,
                grabbed,
                grabbed_by_place,
            )
            fired += 1
            spawn_task!(firing)
            refresh_ready!(net.recheck[tid])
        end
        return nothing
    end

    rebuild_ready!()
    spawn_ready!()

    # take! blocks until a task puts itself on the channel — no spinning
    while !isempty(pending)
        done = take!(completed)
        try; wait(done); catch; end  # ensure task has fully exited
        firing = pending[done]
        delete!(pending, done)

        if istaskfailed(done)
            msg = sprint(showerror, done.result)
            transition = net.transitions[firing.transition_id]
            if firing.attempt <= transition.retries && fired < fuse
                emit(on_event, TransitionFailed(
                    firing.transition_id,
                    firing.run_key,
                    firing.firing_id,
                    firing.attempt,
                    msg,
                    true,
                ))
                firing.attempt += 1
                fired += 1
                spawn_task!(firing)
            elseif firing.attempt <= transition.retries
                marking = misfire(marking, firing.grabbed_by_place)
            else
                push!(trace, TransitionResult(
                    firing.transition_id,
                    firing.run_key,
                    firing.firing_id,
                    :failed,
                    T[],
                    msg,
                    firing.attempt,
                ))
                push!(failed_runs, firing.run_key)
                emit(on_event, TransitionFailed(
                    firing.transition_id,
                    firing.run_key,
                    firing.firing_id,
                    firing.attempt,
                    msg,
                    false,
                ))
                refresh_ready!(net.recheck[firing.transition_id])
            end
        else
            output = fetch(done)
            outputs = _normalize_outputs(output, T)
            marking = output isa AbstractVector ?
                drop(marking, net, firing.transition_id, outputs) :
                drop(marking, net, firing.transition_id, only(outputs))
            push!(trace, TransitionResult(
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
            refresh_ready!(net.recheck[firing.transition_id])
        end

        spawn_ready!()
    end

    traces = Dict{String, Vector{TransitionResult{T}}}()
    for r in trace
        push!(get!(() -> TransitionResult{T}[], traces, r.run_key), r)
    end

    results = RunResult{T}[]
    for rk in seeded
        steps = get(traces, rk, TransitionResult{T}[])

        # Order matters: failed > completed > fuse_exhausted > deadlocked
        if rk ∈ failed_runs
            status = :failed
            reason = :executor_failed
            err = last(r.error for r in steps if r.status === :failed)
        elseif run_completed(marking, rk, net.from_places)
            status = :completed
            reason = nothing
            err = nothing
        elseif fired >= fuse
            status = :incomplete
            reason = :fuse_exhausted
            err = nothing
        else
            status = :incomplete
            reason = :no_enabled_transition
            err = nothing
        end

        result = RunResult(rk, status, err, reason, steps, run_marking(marking, rk))
        emit(on_event, RunFinished(result))
        push!(results, result)
    end

    return results
end
