"""
    Check whether a transition's guard allows firing
    Nothing guard always passes, Function guard is called with the peeked tokens
"""
@inline evaluateGuard(::Nothing, _) = true
@inline evaluateGuard(guard::Function, tokens) = guard(tokens)

"""
    Drop one token through a transition's output arc
    Copy-on-write: returns a new Marking, original is unchanged
    Throws ArgumentError if any output place would exceed its capacity
"""
function drop(marking::Marking{T}, net::Net, tid::Symbol, token::T) where {T<:AbstractToken}
    return drop(marking, net, tid, T[token])
end

"""
    Drop many tokens into a transition's sole output place
    Copy-on-write: returns a new Marking, original is unchanged
    Valid only for transitions with exactly one output arc
    Output count must match that arc's weight
    Validates the full deposit before committing so capacity failures stay atomic
"""
function drop(
    marking::Marking{T}, net::Net, tid::Symbol, outputs::AbstractVector{S}
) where {T<:AbstractToken,S<:T}
    isempty(outputs) && throw(ArgumentError("executor output vector must be non-empty"))

    arcs = get(net.outputArcs, tid, Tuple{Symbol,Int}[])
    length(arcs) == 1 ||
        throw(ArgumentError("executor output vector requires exactly one output arc"))
    pid, weight = only(arcs)
    length(outputs) == weight ||
        throw(ArgumentError("executor output count must match output weight at :$pid"))

    bucket = get(marking.tokensByPlace, pid, T[])
    cap = net.places[pid].capacity
    !isnothing(cap) &&
        length(bucket) + length(outputs) > cap &&
        throw(ArgumentError("capacity exceeded at :$pid"))

    tokens = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokensByPlace)
    deposited = copy(bucket)
    append!(deposited, outputs)
    tokens[pid] = deposited
    return Marking(tokens)
end

function drop(marking::Marking{T}, net::Net, tid::Symbol, outputsByPlace::AbstractDict) where {T<:AbstractToken}
    expected = Dict(pid => weight for (pid, weight) in net.outputArcs[tid])
    deposits = Dict{Symbol,Vector{T}}()

    for (pid, value) in pairs(outputsByPlace)
        pid isa Symbol ||
            throw(ArgumentError("executor output keys must be destination place Symbols"))
        haskey(expected, pid) ||
            throw(ArgumentError("executor output for transition :$tid references unknown output place :$pid"))
        deposits[pid] = flatten(value, T)
    end

    Set(keys(deposits)) == Set(keys(expected)) ||
        throw(ArgumentError("executor output for transition :$tid must provide every output place"))

    for (pid, outputs) in deposits
        length(outputs) == expected[pid] ||
            throw(ArgumentError("executor output count must match output weight at :$pid"))
        bucket = get(marking.tokensByPlace, pid, T[])
        cap = net.places[pid].capacity
        !isnothing(cap) &&
            length(bucket) + length(outputs) > cap &&
            throw(ArgumentError("capacity exceeded at :$pid"))
    end

    tokens = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokensByPlace)
    for (pid, outputs) in deposits
        bucket = get!(() -> T[], tokens, pid)
        append!(bucket, outputs)
    end
    return Marking(tokens)
end

"""
    Return input tokens after a failed firing
    Appends returned tokens to their places
    No capacity check needed because these tokens were already in these places
"""
function misfire(
    marking::Marking{T}, inputs::Dict{Symbol,Vector{T}}
) where {T<:AbstractToken}
    restored = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokensByPlace)
    for (pid, returned) in inputs
        append!(get!(() -> T[], restored, pid), returned)
    end
    return Marking(restored)
end

"""
    Collect the unique runKeys from the initial marking in first-seen order
    These are the fuses the engine will light
"""
function fuses(marking::Marking, places=nothing)
    n = sum(length, values(marking.tokensByPlace); init=0)
    seen = Set{String}()
    sizehint!(seen, n)
    ordered = String[]
    sizehint!(ordered, n)
    for (pid, tokens) in marking.tokensByPlace
        isnothing(places) || pid ∈ places || continue
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
function finalMarking(marking::Marking{T}, rk::String) where {T<:AbstractToken}
    filtered = Dict{Symbol,Vector{T}}()
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
    Grab tokens for a bundle or throw if not enough are available
    Convenience wrapper around grab for callers who want the exception
"""
function take(marking::Marking{T}, net::Net, bundle::Bundle) where {T<:AbstractToken}
    result = grab(marking, net, bundle)
    isnothing(result) && throw(ArgumentError("bundle not enabled: $bundle"))
    return result
end

function flatten(output, ::Type{T}) where {T<:AbstractToken}
    if output isa T
        return T[output]
    end

    output isa AbstractVector ||
        throw(ArgumentError("executor output must be a token or vector of tokens"))
    isempty(output) && throw(ArgumentError("executor output vector must be non-empty"))

    outputs = T[]
    sizehint!(outputs, length(output))
    for token in output
        token isa T ||
            throw(ArgumentError("executor output vector must match the marking token type"))
        push!(outputs, token)
    end
    return outputs
end

function flattenOutput(output, ::Type{T}) where {T<:AbstractToken}
    if output isa AbstractDict
        outputs = T[]
        for (pid, value) in pairs(output)
            pid isa Symbol ||
                throw(ArgumentError("executor output keys must be destination place Symbols"))
            append!(outputs, flatten(value, T))
        end
        return outputs
    end
    return flatten(output, T)
end

function validateOutputRunKeys(outputs, expected::String)
    for token in outputs
        rk = runKey(token)
        rk == expected ||
            throw(ArgumentError("executor output runKey=$rk does not match bundle runKey=$expected"))
    end
    return nothing
end

function bundleKey(transition::Transition, pid::Symbol, token)
    isnothing(transition.joinBy) && return nothing
    key = transition.joinBy(pid, token)
    isnothing(key) && throw(ArgumentError("joinBy returned nothing"))
    return key
end

function chunk(marking::Marking{T}, net::Net, bundle::Bundle, n::Int) where {T<:AbstractToken}
    transition = net.transitions[bundle.transitionId]
    inputs = Dict{Symbol,Vector{T}}()
    indexes = Dict{Symbol,Vector{Int}}()

    for (pid, weight, optional) in net.inputArcs[bundle.transitionId]
        group = 0
        tokens = T[]
        tokenIndexes = Int[]

        for (idx, token) in pairs(get(marking.tokensByPlace, pid, T[]))
            runKey(token) == bundle.runKey || continue
            isequal(bundleKey(transition, pid, token), bundle.selectedKey) || continue
            push!(tokens, token)
            push!(tokenIndexes, idx)

            length(tokens) == weight || continue
            group += 1
            group == n && break
            empty!(tokens)
            empty!(tokenIndexes)
        end

        if length(tokens) < weight
            optional || return nothing
            inputs[pid] = T[]
            indexes[pid] = Int[]
            continue
        end
        inputs[pid] = tokens
        indexes[pid] = tokenIndexes
    end

    return inputs, indexes
end

function flatten(net::Net, bundle::Bundle, inputs::Dict{Symbol,Vector{T}}) where {T<:AbstractToken}
    list = T[]
    for (pid, _, _) in net.inputArcs[bundle.transitionId]
        append!(list, inputs[pid])
    end
    return list
end

function claim(
    marking::Marking{T},
    net::Net,
    bundle::Bundle;
    onGuardError=nothing,
    onSelectionError=nothing,
) where {T<:AbstractToken}
    n = 1
    guardError = nothing

    while true
        candidate = try
            chunk(marking, net, bundle, n)
        catch e
            e isa InterruptException && rethrow()
            onSelectionError === nothing && rethrow()
            onSelectionError(bundle.transitionId, bundle.runKey, e)
            return nothing
        end
        isnothing(candidate) && break

        inputs = first(candidate)
        try
            evaluateGuard(
                net.transitions[bundle.transitionId].guard,
                flatten(net, bundle, inputs),
            ) && return candidate
        catch e
            e isa InterruptException && rethrow()
            guardError = e
        end

        n += 1
    end

    guardError !== nothing &&
        onGuardError !== nothing &&
        onGuardError(bundle, guardError)
    return nothing
end

function selectedKeys(
    marking::Marking{T}, net::Net, tid::Symbol; onSelectionError=nothing
) where {T<:AbstractToken}
    transition = net.transitions[tid]
    keys = Dict{String,Vector{Any}}()

    for (pid, _, _) in net.inputArcs[tid]
        for token in get(marking.tokensByPlace, pid, T[])
            rk = runKey(token)
            key = try
                bundleKey(transition, pid, token)
            catch e
                e isa InterruptException && rethrow()
                onSelectionError === nothing && rethrow()
                onSelectionError(tid, rk, e)
                continue
            end
            bucket = get!(() -> Any[], keys, rk)
            any(existing -> isequal(existing, key), bucket) || push!(bucket, key)
        end
    end

    return keys
end

"""
    Which bundles are hot — ready to fire from the current marking?
    check=nothing scans all transitions, check=[tids] scans only those with live tokens
    onGuardError receives (bundle, exception) when a guard crashes — nothing means silent
"""
function hot(
    net::Net,
    marking::Marking{T};
    failed::Set{String}=Set{String}(),
    check::Union{Nothing,Vector{Symbol}}=nothing,
    onGuardError=nothing,
    onSelectionError=nothing,
) where {T<:AbstractToken}
    hotBundles = Bundle[]

    # check=nothing → full scan, check=[tids] → only transitions with live tokens
    live = check === nothing ? sort!(collect(keys(net.transitions))) : check

    for tid in live
        isempty(net.inputArcs[tid]) && continue

        for (rk, keys) in sort!(
            collect(selectedKeys(marking, net, tid; onSelectionError=onSelectionError));
            by=first,
        )
            rk ∈ failed && continue
            for key in keys
                bundle = Bundle(tid, rk, key)
                isnothing(
                    claim(
                        marking,
                        net,
                        bundle;
                        onGuardError=onGuardError,
                        onSelectionError=onSelectionError,
                    ),
                ) && continue
                push!(hotBundles, bundle)
            end
        end
    end

    return hotBundles
end

"""
    Which transitions have no enabled runKey — the inverse of hot()
    Returns a sorted list of transition ids that cannot fire from the current marking
    Respects the same scan subset and filters as hot()
    Useful for diagnosing incomplete runs
"""
function cold(
    net::Net,
    marking::Marking{T};
    failed::Set{String}=Set{String}(),
    check::Union{Nothing,Vector{Symbol}}=nothing,
    onGuardError=nothing,
) where {T<:AbstractToken}
    enabled = Set{Symbol}(
        bundle.transitionId for bundle in hot(
            net,
            marking;
            failed=failed,
            check=check,
            onGuardError=onGuardError,
        )
    )
    scanned = if check === nothing
        sort!(collect(keys(net.transitions)))
    else
        sort!(collect(Set(check)))
    end
    return [tid for tid in scanned if tid ∉ enabled]
end

"""
    Try to grab tokens from input places for a bundle
    Returns (newMarking, inputs) or nothing if not enough tokens
    Copy-on-write: original marking is unchanged
"""
function grab(marking::Marking{T}, net::Net, bundle::Bundle) where {T<:AbstractToken}
    reservation = claim(marking, net, bundle)
    isnothing(reservation) && return nothing
    inputs, indexes = reservation
    tokens = Dict{Symbol,Vector{T}}(k => copy(v) for (k, v) in marking.tokensByPlace)

    for (pid, _, _) in net.inputArcs[bundle.transitionId]
        bucket = get(marking.tokensByPlace, pid, T[])
        tokenIndexes = indexes[pid]
        leftover = T[]
        next = 1
        for (idx, token) in pairs(bucket)
            if next <= length(tokenIndexes) && idx == tokenIndexes[next]
                next += 1
                continue
            end
            push!(leftover, token)
        end
        tokens[pid] = leftover
    end

    return (Marking(tokens), inputs)
end

mutable struct Fired{T<:AbstractToken}
    bundle::Bundle
    firingId::Int
    attempt::Int
    inputs::Dict{Symbol,Vector{T}}
end

"""
    Run the engine to completion
    fuse limits total transition launches before stopping, including retries
    Once fuse is exhausted the engine launches no new firings, but lets in-flight work drain
    maxConcurrency caps how many transitions run in parallel
    onEvent receives EngineEvent instances as they happen
"""
function fire(
    net::Net, marking::Marking{T}; fuse::Int=1000, maxConcurrency::Int=10, onEvent=nothing
) where {T<:AbstractToken}
    issues = validate(net, marking)
    isempty(issues) || throw(ArgumentError("invalid net/marking: $(issues[1].message)"))

    seeded = fuses(marking, net.fromPlaces)
    isempty(seeded) && return RunResult{T}[]

    trace = TransitionResult{T}[]
    failedRuns = Set{String}()
    guardErrors = Dict{Bundle,String}()
    selectionErrors = Dict{String,String}()
    launched = 0
    nextId = 1

    function runExec(ctx::ExecutionContext{T})
        return execute(getExec(net.transitions[ctx.bundle.transitionId].executor), ctx)
    end

    nextId!() = (id = nextId; nextId += 1; id)

    function guardError(bundle, e)
        msg = sprint(showerror, e)
        guardErrors[bundle] = msg
        return emit(onEvent, GuardErrored(bundle, msg))
    end

    function selectionError(tid, rk, e)
        isempty(rk) && return nothing
        haskey(selectionErrors, rk) && return nothing
        msg = sprint(showerror, e)
        selectionErrors[rk] = msg
        push!(failedRuns, rk)
        emit(onEvent, SelectionErrored(tid, rk, msg))
        return nothing
    end

    readyQ = Bundle[]
    readySet = Set{Bundle}()
    pending = Dict{Task,Fired{T}}()
    done = Channel{Task}(maxConcurrency)

    function reset!()
        empty!(readyQ)
        empty!(readySet)
        empty!(guardErrors)
        for pair in hot(
            net,
            marking;
            failed=failedRuns,
            onGuardError=guardError,
            onSelectionError=selectionError,
        )
            push!(readyQ, pair)
            push!(readySet, pair)
        end
        return nothing
    end

    function refresh!(check::Vector{Symbol})
        affected = Set(check)
        for bundle in collect(keys(guardErrors))
            bundle.transitionId ∈ affected && delete!(guardErrors, bundle)
        end
        filter!(readyQ) do bundle
            keep = bundle.transitionId ∉ affected && bundle.runKey ∉ failedRuns
            keep || delete!(readySet, bundle)
            keep
        end
        for bundle in hot(
            net,
            marking;
            failed=failedRuns,
            check=check,
            onGuardError=guardError,
            onSelectionError=selectionError,
        )
            bundle ∈ readySet && continue
            push!(readyQ, bundle)
            push!(readySet, bundle)
        end
        return nothing
    end

    function popNext!()
        while !isempty(readyQ)
            bundle = popfirst!(readyQ)
            delete!(readySet, bundle)
            bundle.runKey ∈ failedRuns && continue
            return bundle
        end
        return nothing
    end

    function spawnTask!(fired::Fired{T})
        inputs = Dict{Symbol,Vector{T}}(
            pid => copy(tokens) for (pid, tokens) in fired.inputs
        )
        ctx = ExecutionContext(fired.bundle, fired.firingId, fired.attempt, inputs)
        emit(onEvent, TransitionStarted(ctx))
        task = Threads.@spawn begin
            local result
            try
                result = runExec(ctx)
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

    function failFiring!(fired::Fired{T}, msg::String)
        bundle = fired.bundle
        transition = net.transitions[bundle.transitionId]
        if fired.attempt <= transition.retries && launched < fuse
            emit(
                onEvent,
                TransitionFailed(bundle, fired.firingId, fired.attempt, msg, true),
            )
            fired.attempt += 1
            launched += 1
            spawnTask!(fired)
        elseif fired.attempt <= transition.retries
            marking = misfire(marking, fired.inputs)
            push!(
                trace,
                TransitionResult(bundle, fired.firingId, :failed, T[], msg, fired.attempt),
            )
            emit(
                onEvent,
                TransitionFailed(bundle, fired.firingId, fired.attempt, msg, false),
            )
        else
            marking = misfire(marking, fired.inputs)
            push!(
                trace,
                TransitionResult(bundle, fired.firingId, :failed, T[], msg, fired.attempt),
            )
            push!(failedRuns, bundle.runKey)
            emit(
                onEvent,
                TransitionFailed(bundle, fired.firingId, fired.attempt, msg, false),
            )
            refresh!(net.recheck[bundle.transitionId])
        end
        return nothing
    end

    function launchNext!()
        while length(pending) < maxConcurrency && launched < fuse
            bundle = popNext!()
            isnothing(bundle) && break

            grabbed = grab(marking, net, bundle)
            isnothing(grabbed) && continue

            marking, inputs = grabbed
            fired = Fired(bundle, nextId!(), 1, inputs)
            launched += 1
            spawnTask!(fired)
            refresh!(net.recheck[bundle.transitionId])
        end
        return nothing
    end

    reset!()
    launchNext!()

    # take! blocks until a task puts itself on the channel — no spinning
    while !isempty(pending)
        task = take!(done)
        try
            wait(task)
        catch
        end  # ensure task has fully exited
        fired = pending[task]
        delete!(pending, task)

        if istaskfailed(task)
            msg = sprint(showerror, task.result)
            failFiring!(fired, msg)
        else
            bundle = fired.bundle
            local outputs
            try
                output = fetch(task)
                outputs = flattenOutput(output, T)
                validateOutputRunKeys(outputs, bundle.runKey)
                marking = drop(marking, net, bundle.transitionId, output)
            catch e
                e isa InterruptException && rethrow()
                failFiring!(fired, sprint(showerror, e))
                launchNext!()
                continue
            end
            push!(
                trace,
                TransitionResult(
                    bundle, fired.firingId, :completed, outputs, nothing, fired.attempt
                ),
            )
            emit(
                onEvent, TransitionCompleted(bundle, fired.firingId, fired.attempt, outputs)
            )
            refresh!(net.recheck[bundle.transitionId])
        end

        launchNext!()
    end

    traces = Dict{String,Vector{TransitionResult{T}}}()
    for r in trace
        push!(get!(() -> TransitionResult{T}[], traces, r.bundle.runKey), r)
    end

    results = RunResult{T}[]
    for rk in seeded
        steps = get(traces, rk, TransitionResult{T}[])

        # Order matters: failed > completed > fuseExhausted > noEnabledTransition
        if haskey(selectionErrors, rk)
            status = :failed
            reason = :selectionError
            err = selectionErrors[rk]
        elseif rk ∈ failedRuns
            status = :failed
            reason = :executorFailed
            err = last(r.error for r in steps if r.status === :failed)
        elseif isComplete(marking, rk, net.fromPlaces)
            status = :completed
            reason = nothing
            err = nothing
        else
            erroredGuards = sort!(
                Bundle[bundle for (bundle, _) in guardErrors if bundle.runKey == rk];
                by=bundle -> bundle.transitionId,
            )
            if !isempty(erroredGuards)
                bundle = first(erroredGuards)
                status = :failed
                reason = :guardError
                err = guardErrors[bundle]
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
