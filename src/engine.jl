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
    Put grabbed tokens back after an executor failure (retry path)
    Restores the marking to its pre-grab state so the transition can re-attempt
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

"""
    Run the engine to completion
    fuse limits total transitions fired before stopping
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
    isempty(seeded) && return RunResult[]

    trace = TransitionResult[]
    failed_runs = Set{String}()
    in_flight = Set{Tuple{Symbol, String}}()
    retries = Dict{Tuple{Symbol, String}, Int}()
    fired = 0

    run_executor(tid::Symbol, tokens::Vector{T}) =
        execute(get_executor(net.transitions[tid].executor), tid, tokens)

    function guard_error(tid, rk, e)
        msg = sprint(showerror, e)
        push!(trace, TransitionResult(tid, rk, :failed, nothing, msg))
        emit(on_event, TransitionFailed(tid, rk, msg, false))
    end

    check = nothing
    pending = Dict{Task, Tuple{Symbol, String, Dict{Symbol, Vector{T}}}}()
    completed = Channel{Task}(max_concurrency)

    function spawn_ready!()
        primed = hot(net, marking; failed=failed_runs, in_flight=in_flight, check=check, on_guard_error=guard_error)
        slots = max_concurrency - length(pending)
        for (tid, rk) in primed
            slots <= 0 && break
            fired >= fuse && break
            # grab can fail even though hot() said ready — another primed pair grabbed first
            result = grab(marking, net, tid, rk)
            isnothing(result) && continue
            marking, grabbed, grabbed_by_place = result
            fired += 1
            push!(in_flight, (tid, rk))
            emit(on_event, TransitionStarted(tid, rk, grabbed))
            task = Threads.@spawn begin
                local r
                try
                    r = run_executor(tid, grabbed)
                catch
                    put!(completed, current_task())
                    rethrow()
                end
                put!(completed, current_task())
                r
            end
            pending[task] = (tid, rk, grabbed_by_place)
            slots -= 1
        end
    end

    spawn_ready!()

    # take! blocks until a task puts itself on the channel — no spinning
    while !isempty(pending)
        done = take!(completed)
        try; wait(done); catch; end  # ensure task has fully exited
        tid, rk, grabbed_by_place = pending[done]
        delete!(pending, done)
        delete!(in_flight, (tid, rk))

        if istaskfailed(done)
            msg = sprint(showerror, done.result)
            key = (tid, rk)
            retries[key] = get(retries, key, 0) + 1
            t = net.transitions[tid]
            if retries[key] <= t.retries
                marking = misfire(marking, grabbed_by_place)
                emit(on_event, TransitionFailed(tid, rk, msg, true))
            else
                push!(trace, TransitionResult(tid, rk, :failed, nothing, msg))
                push!(failed_runs, rk)
                emit(on_event, TransitionFailed(tid, rk, msg, false))
            end
        else
            token = fetch(done)
            marking = drop(marking, net, tid, token)
            push!(trace, TransitionResult(tid, rk, :completed, token, nothing))
            emit(on_event, TransitionCompleted(tid, rk, token))
        end

        # Re-check the precomputed influence set for this transition:
        # the fired transition itself, transitions that might have lost
        # enablement, and transitions that might have gained enablement.
        check = net.recheck[tid]
        spawn_ready!()
    end

    traces = Dict{String, Vector{TransitionResult}}()
    for r in trace
        push!(get!(() -> TransitionResult[], traces, r.run_key), r)
    end

    results = RunResult[]
    for rk in seeded
        steps::Vector{TransitionResult} = get(traces, rk, TransitionResult[])

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
