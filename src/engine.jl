"""
    Check whether a transition's guard allows firing
    Nothing guard always passes, Function guard is called with the peeked tokens
"""
@inline evaluateGuard(::Nothing, _) = true
@inline evaluateGuard(guard::Function, tokens) = guard(tokens)

"""
    Drop a token into the output places of a launched transition
    Copy-on-write: returns a new Marking, original is unchanged
    Throws ArgumentError if any output place would exceed its capacity
"""
function drop(marking::Marking{T}, net::Net, tid::Symbol, token::T) where T<:AbstractToken
    tokens = Dict{Symbol, Vector{T}}(k => copy(v) for (k, v) in marking.tokensByPlace)
    for (pid, weight) in net.outputArcs[tid]
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

    arcs = get(net.outputArcs, tid, Tuple{Symbol, Int}[])
    length(arcs) == 1 || throw(ArgumentError("executor output vector requires exactly one output arc"))
    pid, weight = only(arcs)
    weight == 1 || throw(ArgumentError("executor output vector requires a weight-1 output arc"))

    bucket = get(marking.tokensByPlace, pid, T[])
    cap = net.places[pid].capacity
    !isnothing(cap) && length(bucket) + length(outputs) > cap &&
        throw(ArgumentError("capacity exceeded at :$pid"))

    tokens = Dict{Symbol, Vector{T}}(k => copy(v) for (k, v) in marking.tokensByPlace)
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
function misfire(marking::Marking{T}, grabbedByPlace::Dict{Symbol, Vector{T}}) where T<:AbstractToken
    restored = Dict{Symbol, Vector{T}}(k => copy(v) for (k, v) in marking.tokensByPlace)
    for (pid, returned) in grabbedByPlace
        append!(get!(() -> T[], restored, pid), returned)
    end
    return Marking(restored)
end

"""
    Collect the unique runKeys from the initial marking in first-seen order
    These are the fuses the engine will light
"""
function fuses(marking::Marking)
    n = sum(length, values(marking.tokensByPlace); init=0)
    seen = Set{String}()
    sizehint!(seen, n)
    ordered = String[]
    sizehint!(ordered, n)
    for tokens in values(marking.tokensByPlace)
        for token in tokens
            rk = runKey(token)
            if rk ∉ seen
                push!(seen, rk)
                push!(ordered, rk)
            end
        end
    end
    return ordered
end

"""
    Check if a runKey's tokens have all left from-places (places with outgoing arcs)
    Returns true when no tokens for this runKey remain in any from-place
"""
function isComplete(marking::Marking, rk::String, fromPlaces::Set{Symbol})
    for (pid, tokens) in marking.tokensByPlace
        pid ∈ fromPlaces || continue
        any(t -> runKey(t) == rk, tokens) && return false
    end
    return true
end

"""
    Extract the final marking snapshot for one runKey
    Keeps only that runKey's tokens across all places and drops empty places
"""
function finalMarking(marking::Marking{T}, rk::String) where T<:AbstractToken
    filtered = Dict{Symbol, Vector{T}}()
    sizehint!(filtered, length(marking.tokensByPlace))
    for (pid, tokens) in marking.tokensByPlace
        kept = T[]
        for token in tokens
            runKey(token) == rk && push!(kept, token)
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
    isnothing(result) && throw(ArgumentError("transition :$tid not enabled for runKey=$rk"))
    return result
end

function normalize(output, ::Type{T}) where T<:AbstractToken
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
    Which (transition, runKey) pairs are hot — ready to fire from the current marking?
    Returns a sorted list of primed (transition, runKey) pairs, filtered by failed/inFlight/guards
    check=nothing scans all transitions, check=[tids] scans only those with live tokens
    onGuardError receives (tid, rk, exception) when a guard crashes — nothing means silent
"""
function hot(
    net::Net,
    marking::Marking{T};
    failed::Set{String} = Set{String}(),
    inFlight::Set{Tuple{Symbol, String}} = Set{Tuple{Symbol, String}}(),
    check::Union{Nothing, Vector{Symbol}} = nothing,
    onGuardError = nothing,
) where T<:AbstractToken

    result = Tuple{Symbol, String}[]

    # check=nothing → full scan, check=[tids] → only transitions with live tokens
    live = check === nothing ? sort!(collect(keys(net.transitions))) : check

    for tid in live
        arcs = net.inputArcs[tid]
        isempty(arcs) && continue

        # For each input place, count tokens per runKey.
        # Keep only runKeys with enough tokens, then intersect across all input places.
        # First input place seeds candidates, each subsequent place narrows via intersect
        candidates = nothing
        for (pid, weight) in arcs
            toks = get(marking.tokensByPlace, pid, T[])
            counts = Dict{String, Int}()
            for t in toks
                rk = runKey(t)
                counts[rk] = get(counts, rk, 0) + 1
            end
            canFire = Set(rk for (rk, cnt) in counts if cnt >= weight)
            if candidates === nothing
                candidates = canFire
            else
                intersect!(candidates, canFire)
            end
        end

        candidates === nothing && continue
        isempty(candidates) && continue

        for rk in sort!(collect(candidates))
            rk ∈ failed && continue
            (tid, rk) ∈ inFlight && continue

            # Peek tokens that would be grabbed (read-only, no marking change)
            peeked = T[]
            for (pid, weight) in arcs
                n = 0
                for t in get(marking.tokensByPlace, pid, T[])
                    runKey(t) == rk || continue
                    push!(peeked, t)
                    n += 1
                    n >= weight && break
                end
            end

            # Guard check — false or exception means skip
            try
                evaluateGuard(net.transitions[tid].guard, peeked) || continue
            catch e
                onGuardError !== nothing && onGuardError(tid, rk, e)
                continue
            end

            push!(result, (tid, rk))
        end
    end

    return result
end

"""
    Which transitions have no enabled runKey — the inverse of hot()
    Returns a sorted list of transition ids that cannot fire from the current marking
    Respects the same scan subset and filters as hot()
    Useful for diagnosing deadlocks and incomplete runs
"""
function cold(
    net::Net,
    marking::Marking{T};
    failed::Set{String} = Set{String}(),
    inFlight::Set{Tuple{Symbol, String}} = Set{Tuple{Symbol, String}}(),
    check::Union{Nothing, Vector{Symbol}} = nothing,
    onGuardError = nothing,
) where T<:AbstractToken
    enabled = Set{Symbol}(tid for (tid, _) in hot(
        net, marking;
        failed=failed,
        inFlight=inFlight,
        check=check,
        onGuardError=onGuardError,
    ))
    scanned = check === nothing ? sort!(collect(keys(net.transitions))) : sort!(collect(Set(check)))
    return [tid for tid in scanned if tid ∉ enabled]
end

"""
    Try to grab tokens from input places for a transition and runKey
    Returns (newMarking, grabbed, grabbedByPlace) or nothing if not enough tokens
    Copy-on-write: original marking is unchanged
    Multi-place selection is keyed by runKey only: when several eligible tokens exist
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

    tokens = Dict{Symbol, Vector{T}}(k => copy(v) for (k, v) in marking.tokensByPlace)
    grabbed = T[]
    grabbedByPlace = Dict{Symbol, Vector{T}}()

    for (pid, weight) in net.inputArcs[tid]
        bucket = get(tokens, pid, T[])
        available = count(t -> runKey(t) == rk, bucket)
        available < weight && return nothing

        claimed = T[]
        leftover = T[]
        for token in bucket
            if runKey(token) == rk && length(claimed) < weight
                push!(claimed, token)
            else
                push!(leftover, token)
            end
        end

        append!(grabbed, claimed)
        grabbedByPlace[pid] = claimed
        tokens[pid] = leftover
    end

    return (Marking(tokens), grabbed, grabbedByPlace)
end

mutable struct Fired{T<:AbstractToken}
    transitionId::Symbol
    runKey::String
    firingId::Int
    attempt::Int
    grabbed::Vector{T}
    grabbedByPlace::Dict{Symbol, Vector{T}}
end

"""
    Run the engine to completion
    fuse limits total transition launches before stopping, including retries
    Once fuse is exhausted the engine launches no new firings, but lets in-flight work drain
    maxConcurrency caps how many transitions run in parallel
    onEvent receives EngineEvent instances as they happen
"""
function fire(
    net::Net,
    marking::Marking{T};
    fuse::Int = 1000,
    maxConcurrency::Int = 10,
    onEvent = nothing,
) where T <: AbstractToken

    issues = validate(net, marking)
    isempty(issues) || throw(ArgumentError("invalid net/marking: $(issues[1].message)"))

    seeded = fuses(marking)
    isempty(seeded) && return RunResult{T}[]

    trace = TransitionResult{T}[]
    failedRuns = Set{String}()
    guardErrors = Dict{Tuple{Symbol, String}, String}()
    launched = 0
    nextId = 1

    runExec(tid::Symbol, tokens::Vector{T}) =
        execute(getExec(net.transitions[tid].executor), tid, tokens)

    nextId!() = (id = nextId; nextId += 1; id)

    function guardError(tid, rk, e)
        msg = sprint(showerror, e)
        guardErrors[(tid, rk)] = msg
        emit(onEvent, GuardErrored(tid, rk, msg))
    end

    readyQ = Tuple{Symbol, String}[]
    readySet = Set{Tuple{Symbol, String}}()
    pending = Dict{Task, Fired{T}}()
    done = Channel{Task}(maxConcurrency)

    function reset!()
        empty!(readyQ)
        empty!(readySet)
        empty!(guardErrors)
        for pair in hot(net, marking; failed=failedRuns, onGuardError=guardError)
            push!(readyQ, pair)
            push!(readySet, pair)
        end
        return nothing
    end

    function refresh!(check::Vector{Symbol})
        affected = Set(check)
        for pair in collect(keys(guardErrors))
            pair[1] ∈ affected && delete!(guardErrors, pair)
        end
        filter!(readyQ) do pair
            keep = pair[1] ∉ affected && pair[2] ∉ failedRuns
            keep || delete!(readySet, pair)
            keep
        end
        for pair in hot(net, marking; failed=failedRuns, check=check, onGuardError=guardError)
            pair ∈ readySet && continue
            push!(readyQ, pair)
            push!(readySet, pair)
        end
        return nothing
    end

    function popNext!()
        while !isempty(readyQ)
            pair = popfirst!(readyQ)
            delete!(readySet, pair)
            pair[2] ∈ failedRuns && continue
            return pair
        end
        return nothing
    end

    function spawnTask!(fired::Fired{T})
        inputs = copy(fired.grabbed)
        emit(onEvent, TransitionStarted(
            fired.transitionId,
            fired.runKey,
            fired.firingId,
            fired.attempt,
            inputs,
        ))
        task = Threads.@spawn begin
            local result
            try
                result = runExec(fired.transitionId, inputs)
            catch
                put!(done, current_task())
                rethrow()
            end
            put!(done, current_task())
            result
        end
        pending[task] = fired
        return nothing
    end

    function launchNext!()
        while length(pending) < maxConcurrency && launched < fuse
            pair = popNext!()
            isnothing(pair) && break

            tid, rk = pair
            result = grab(marking, net, tid, rk)
            isnothing(result) && continue

            marking, grabbed, grabbedByPlace = result
            fired = Fired(
                tid,
                rk,
                nextId!(),
                1,
                grabbed,
                grabbedByPlace,
            )
            launched += 1
            spawnTask!(fired)
            refresh!(net.recheck[tid])
        end
        return nothing
    end

    reset!()
    launchNext!()

    # take! blocks until a task puts itself on the channel — no spinning
    while !isempty(pending)
        task = take!(done)
        try; wait(task); catch; end  # ensure task has fully exited
        fired = pending[task]
        delete!(pending, task)

        if istaskfailed(task)
            msg = sprint(showerror, task.result)
            transition = net.transitions[fired.transitionId]
            if fired.attempt <= transition.retries && launched < fuse
                emit(onEvent, TransitionFailed(
                    fired.transitionId,
                    fired.runKey,
                    fired.firingId,
                    fired.attempt,
                    msg,
                    true,
                ))
                fired.attempt += 1
                launched += 1
                spawnTask!(fired)
            elseif fired.attempt <= transition.retries
                marking = misfire(marking, fired.grabbedByPlace)
                push!(trace, TransitionResult(
                    fired.transitionId,
                    fired.runKey,
                    fired.firingId,
                    :failed,
                    T[],
                    msg,
                    fired.attempt,
                ))
                emit(onEvent, TransitionFailed(
                    fired.transitionId,
                    fired.runKey,
                    fired.firingId,
                    fired.attempt,
                    msg,
                    false,
                ))
            else
                push!(trace, TransitionResult(
                    fired.transitionId,
                    fired.runKey,
                    fired.firingId,
                    :failed,
                    T[],
                    msg,
                    fired.attempt,
                ))
                push!(failedRuns, fired.runKey)
                emit(onEvent, TransitionFailed(
                    fired.transitionId,
                    fired.runKey,
                    fired.firingId,
                    fired.attempt,
                    msg,
                    false,
                ))
                refresh!(net.recheck[fired.transitionId])
            end
        else
            output = fetch(task)
            outputs = normalize(output, T)
            marking = output isa AbstractVector ?
                drop(marking, net, fired.transitionId, outputs) :
                drop(marking, net, fired.transitionId, only(outputs))
            push!(trace, TransitionResult(
                fired.transitionId,
                fired.runKey,
                fired.firingId,
                :completed,
                outputs,
                nothing,
                fired.attempt,
            ))
            emit(onEvent, TransitionCompleted(
                fired.transitionId,
                fired.runKey,
                fired.firingId,
                fired.attempt,
                outputs,
            ))
            refresh!(net.recheck[fired.transitionId])
        end

        launchNext!()
    end

    traces = Dict{String, Vector{TransitionResult{T}}}()
    for r in trace
        push!(get!(() -> TransitionResult{T}[], traces, r.runKey), r)
    end

    results = RunResult{T}[]
    for rk in seeded
        steps = get(traces, rk, TransitionResult{T}[])

        # Order matters: failed > completed > fuseExhausted > deadlocked
        if rk ∈ failedRuns
            status = :failed
            reason = :executorFailed
            err = last(r.error for r in steps if r.status === :failed)
        elseif isComplete(marking, rk, net.fromPlaces)
            status = :completed
            reason = nothing
            err = nothing
        else
            erroredGuards = sort!(Symbol[
                tid for ((tid, key), _) in guardErrors if key == rk
            ])
            if !isempty(erroredGuards)
                tid = first(erroredGuards)
                status = :failed
                reason = :guardError
                err = guardErrors[(tid, rk)]
            elseif launched >= fuse
                status = :incomplete
                reason = :fuseExhausted
                err = nothing
            else
                status = :incomplete
                reason = :noEnabledTransition
                err = nothing
            end
        end

        result = RunResult(rk, status, err, reason, steps, finalMarking(marking, rk))
        emit(onEvent, RunFinished(result))
        push!(results, result)
    end

    return results
end
