module SrcEngineTests

using Test
using Main.Peven

# ── Test fixtures ─────────────────────────────────────────────────────────────

# Simple chain: ready --(1)--> judge --(1)--> done
function chainNet(; guard=nothing, retries=0)
    places = Dict(:ready => Place(:ready), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge; guard=guard, retries=retries))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Fork/join: left --> join --> done, right --> join
function joinNet()
    places = Dict(:left => Place(:left), :right => Place(:right), :done => Place(:done))
    transitions = Dict(:join => Transition(:join))
    arcsfrom = [ArcFrom(:join, :left), ArcFrom(:join, :right)]
    arcsto = [ArcTo(:join, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Branch: ready --> judge --> left, right
function branchNet()
    places = Dict(:ready => Place(:ready), :left => Place(:left), :right => Place(:right))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :left), ArcTo(:judge, :right)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Weighted: ready --(2)--> judge --(1)--> done
function weightedNet()
    places = Dict(:ready => Place(:ready), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready, 2)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Bounded: ready --> judge --> done (capacity 1)
function boundedNet()
    places = Dict(:ready => Place(:ready), :done => Place(:done, 1))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Weighted output: ready --> judge --(2)--> done
function weightedOutputNet()
    places = Dict(:ready => Place(:ready), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done, 2)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Two independent transitions for check/filter tests
function splitNet()
    places = Dict(:left => Place(:left), :right => Place(:right), :done => Place(:done))
    transitions = Dict(:leftT => Transition(:leftT), :rightT => Transition(:rightT))
    arcsfrom = [ArcFrom(:leftT, :left), ArcFrom(:rightT, :right)]
    arcsto = [ArcTo(:leftT, :done), ArcTo(:rightT, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

passthrough(tid, tokens) = Token(color(tokens[1]), Peven.runKey(tokens[1]), (transition=tid,))

function withExec(f, name::Symbol, executor)
    registerExec!(name, executor)
    try f() finally delete!(Peven.registry, name) end
end

# ── Guard dispatch ────────────────────────────────────────────────────────────

@testset "src/engine" begin
    @testset "evaluateGuard" begin
        @test Peven.evaluateGuard(nothing, []) == true
        @test Peven.evaluateGuard(ts -> length(ts) > 0, [Token("r1")]) == true
        @test Peven.evaluateGuard(ts -> length(ts) > 5, [Token("r1")]) == false
    end

    # ── drop ───────────────────────────────────────────────────────────────

    @testset "drop: basic" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token(:red, "r1", 1)]))
        output = Token(:red, "r1", "result")
        newMarking = drop(marking, net, :judge, output)
        @test length(newMarking.tokensByPlace[:done]) == 1
        @test newMarking.tokensByPlace[:done][1].payload == "result"
    end

    @testset "drop: weighted" begin
        net = weightedNet()
        marking = Marking()
        output = Token(:red, "r1", "x")
        # drop doesn't check inputs, just outputs — weight on output arc is 1
        newMarking = drop(marking, net, :judge, output)
        @test length(newMarking.tokensByPlace[:done]) == 1
    end

    @testset "drop: capacity exceeded" begin
        net = boundedNet()
        marking = Marking(Dict(:done => Token[Token(:red, "r1", "existing")]))
        output = Token(:red, "r1", "overflow")
        @test_throws ArgumentError drop(marking, net, :judge, output)
    end

    @testset "drop: many outputs" begin
        net = chainNet()
        marking = Marking()
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        newMarking = drop(marking, net, :judge, outputs)
        @test [token.payload for token in newMarking.tokensByPlace[:done]] == ["a", "b"]
    end

    @testset "drop: many outputs is atomic on capacity failure" begin
        net = boundedNet()
        marking = Marking(Dict(:done => Token[Token(:red, "r1", "existing")]))
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        @test_throws ArgumentError drop(marking, net, :judge, outputs)
        @test [token.payload for token in marking.tokensByPlace[:done]] == ["existing"]
    end

    @testset "drop: many outputs rejects branching fan-out" begin
        net = branchNet()
        marking = Marking()
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        @test_throws ArgumentError drop(marking, net, :judge, outputs)
    end

    @testset "drop: many outputs rejects weighted output arc" begin
        net = weightedOutputNet()
        marking = Marking()
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        @test_throws ArgumentError drop(marking, net, :judge, outputs)
    end

    @testset "drop: many outputs accepts concrete vectors for Marking{AbstractToken}" begin
        net = chainNet()
        marking = Marking{AbstractToken}(Dict(:done => AbstractToken[]))
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        newMarking = drop(marking, net, :judge, outputs)
        @test [token.payload for token in newMarking.tokensByPlace[:done]] == ["a", "b"]
    end

    # ── misfire ─────────────────────────────────────────────────────────────

    @testset "misfire" begin
        marking = Marking(Dict(:ready => Token[]))
        consumed = Dict(:ready => Token[Token(:red, "r1", 1)])
        restored = misfire(marking, consumed)
        @test length(restored.tokensByPlace[:ready]) == 1
        @test Peven.runKey(restored.tokensByPlace[:ready][1]) == "r1"
    end

    # ── fuses ─────────────────────────────────────────────────────────

    @testset "fuses" begin
        marking = Marking(Dict(
            :a => Token[Token(:red, "r1", 1), Token(:blue, "r2", 2)],
            :b => Token[Token(:red, "r1", 3)],
        ))
        keys = Peven.fuses(marking)
        @test Set(keys) == Set(["r1", "r2"])
    end

    @testset "fuses: empty" begin
        @test isempty(Peven.fuses(Marking()))
    end

    # ── isComplete ─────────────────────────────────────────────────────────

    @testset "isComplete" begin
        from = Set([:ready])
        m1 = Marking(Dict(:ready => Token[Token(:red, "r1", 1)]))
        @test Peven.isComplete(m1, "r1", from) == false

        m2 = Marking(Dict(:done => Token[Token(:red, "r1", 1)]))
        @test Peven.isComplete(m2, "r1", from) == true

        m3 = Marking()
        @test Peven.isComplete(m3, "r1", from) == true
    end

    # ── executor registry ─────────────────────────────────────────────────────

    @testset "executor registry" begin
        fnExec = FunctionExecutor((tid, tokens) -> Token(Peven.runKey(tokens[1]), tid, nothing))
        registerExec!(:testExec, fnExec)
        @test getExec(:testExec) === fnExec
        @test_throws KeyError getExec(:nonexistent)

        # Clean up
        delete!(Peven.registry, :testExec)
    end

    # ── emit ──────────────────────────────────────────────────────────────────

    @testset "emit" begin
        # emit with nothing hook does nothing
        Peven.emit(nothing, TransitionStarted(:t, "r1", 1, 1, Token[]))

        # emit with hook calls it
        events = EngineEvent[]
        hook = e -> push!(events, e)
        Peven.emit(hook, TransitionStarted(:t, "r1", 1, 1, Token[]))
        @test length(events) == 1
        @test events[1] isa TransitionStarted
        @test events[1].transitionId === :t
        @test events[1].firingId == 1
        @test events[1].attempt == 1
    end

    # ── hot ───────────────────────────────────────────────────────────────

    @testset "hot: basic chain" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        result = hot(net, marking)
        @test result == [(:judge, "r1")]
    end

    @testset "hot: no tokens" begin
        net = chainNet()
        @test isempty(hot(net, Marking()))
    end

    @testset "hot: join requires both places" begin
        net = joinNet()
        # Only left has a token — join needs both
        m1 = Marking(Dict(:left => Token[Token("r1")]))
        @test isempty(hot(net, m1))

        # Both places have a token — join is hot
        m2 = Marking(Dict(:left => Token[Token("r1")], :right => Token[Token("r1")]))
        @test hot(net, m2) == [(:join, "r1")]
    end

    @testset "hot: skips failed" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(hot(net, marking; failed=Set(["r1"])))
    end

    @testset "hot: skips inFlight" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(hot(net, marking; inFlight=Set([(:judge, "r1")])))
    end

    @testset "hot: guard blocks" begin
        net = chainNet(guard = _ -> false)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(hot(net, marking))
    end

    @testset "hot: guard exception" begin
        net = chainNet(guard = _ -> error("boom"))
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        # Silent by default — just skips
        @test isempty(hot(net, marking))

        # With callback — records the error
        errors = []
        hot(net, marking; onGuardError = (tid, rk, e) -> push!(errors, (tid, rk, e)))
        @test length(errors) == 1
        @test errors[1][1] === :judge
        @test errors[1][2] == "r1"
    end

    @testset "hot: weighted arc" begin
        net = weightedNet()
        # 1 token, weight 2 — not hot
        m1 = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(hot(net, m1))

        # 2 tokens, weight 2 — hot
        m2 = Marking(Dict(:ready => Token[Token("r1"), Token("r1")]))
        @test hot(net, m2) == [(:judge, "r1")]
    end

    @testset "hot: check parameter" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        # check=[:judge] — includes the transition, finds it
        @test hot(net, marking; check=[:judge]) == [(:judge, "r1")]
        # check=[] — empty scan, finds nothing
        @test isempty(hot(net, marking; check=Symbol[]))
    end

    # ── cold ──────────────────────────────────────────────────────────────

    @testset "cold: all enabled" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(cold(net, marking))
    end

    @testset "cold: no tokens" begin
        net = chainNet()
        @test cold(net, Marking()) == [:judge]
    end

    @testset "cold: join missing one input" begin
        net = joinNet()
        m = Marking(Dict(:left => Token[Token("r1")]))
        @test cold(net, m) == [:join]
    end

    @testset "cold: join fully enabled" begin
        net = joinNet()
        m = Marking(Dict(:left => Token[Token("r1")], :right => Token[Token("r1")]))
        @test isempty(cold(net, m))
    end

    @testset "cold: guard blocks" begin
        net = chainNet(guard = _ -> false)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test cold(net, marking) == [:judge]
    end

    @testset "cold: failed runKey blocks transition" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test cold(net, marking; failed=Set(["r1"])) == [:judge]
    end

    @testset "cold: inFlight blocks transition" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test cold(net, marking; inFlight=Set([(:judge, "r1")])) == [:judge]
    end

    @testset "cold: check respects scan subset" begin
        net = splitNet()
        marking = Marking(Dict(:left => Token[Token("r1")]))
        @test cold(net, marking) == [:rightT]
        @test isempty(cold(net, marking; check=[:leftT]))
        @test cold(net, marking; check=[:rightT]) == [:rightT]
    end

    # ── grab ──────────────────────────────────────────────────────────────

    @testset "grab: basic" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token(:red, "r1", 42)]))
        result = grab(marking, net, :judge, "r1")
        @test result !== nothing
        newMarking, grabbed, grabbedByPlace = result
        @test length(grabbed) == 1
        @test grabbed[1].payload == 42
        @test isempty(get(newMarking.tokensByPlace, :ready, Token[]))
        @test haskey(grabbedByPlace, :ready)
    end

    @testset "grab: insufficient tokens" begin
        net = chainNet()
        @test grab(Marking(), net, :judge, "r1") === nothing
    end

    @testset "grab: weight > 1" begin
        net = weightedNet()
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r1"), Token("r1")]))
        result = grab(marking, net, :judge, "r1")
        @test result !== nothing
        _, grabbed, _ = result
        @test length(grabbed) == 2
    end

    @testset "grab: preserves other runKeys" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r2")]))
        result = grab(marking, net, :judge, "r1")
        @test result !== nothing
        newMarking, grabbed, _ = result
        @test length(grabbed) == 1
        @test Peven.runKey(grabbed[1]) == "r1"
        remaining = newMarking.tokensByPlace[:ready]
        @test length(remaining) == 1
        @test Peven.runKey(remaining[1]) == "r2"
    end

    @testset "grab: copy-on-write" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        result = grab(marking, net, :judge, "r1")
        @test result !== nothing
        # Original marking unchanged
        @test length(marking.tokensByPlace[:ready]) == 1
    end

    @testset "grab: multiple input places" begin
        net = joinNet()
        marking = Marking(Dict(
            :left => Token[Token("r1")],
            :right => Token[Token("r1")],
        ))
        result = grab(marking, net, :join, "r1")
        @test result !== nothing
        _, grabbed, grabbedByPlace = result
        @test length(grabbed) == 2
        @test haskey(grabbedByPlace, :left)
        @test haskey(grabbedByPlace, :right)
    end

    # ── take ──────────────────────────────────────────────────────────────

    @testset "take: success" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        newMarking, grabbed, _ = take(marking, net, :judge, "r1")
        @test length(grabbed) == 1
    end

    @testset "take: failure" begin
        net = chainNet()
        @test_throws ArgumentError take(Marking(), net, :judge, "r1")
    end

    # ── fire ──────────────────────────────────────────────────────────────

    @testset "fire: simple chain" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        withExec(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; maxConcurrency=1)
            @test length(results) == 1
            @test results[1].status === :completed
            @test results[1].runKey == "r1"
            @test length(results[1].trace) == 1
            @test results[1].trace[1].status === :completed
            @test length(results[1].trace[1].outputs) == 1
            @test results[1].trace[1].attempts == 1
        end
    end

    @testset "fire: multiple runKeys" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r2")]))
        withExec(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; maxConcurrency=1)
            @test length(results) == 2
            rks = Set(r.runKey for r in results)
            @test rks == Set(["r1", "r2"])
            @test all(r.status === :completed for r in results)
        end
    end

    @testset "fire: guard routing" begin
        # Guard only allows r1
        net = chainNet(guard = ts -> Peven.runKey(ts[1]) == "r1")
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r2")]))
        withExec(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; maxConcurrency=1)
            r1 = first(r for r in results if r.runKey == "r1")
            r2 = first(r for r in results if r.runKey == "r2")
            @test r1.status === :completed
            @test r2.status === :incomplete
            @test r2.reason === :noEnabledTransition
            @test get(r1.finalMarking.tokensByPlace, :done, Token[])[1] |> Peven.runKey == "r1"
            @test !haskey(r1.finalMarking.tokensByPlace, :ready)
            @test get(r2.finalMarking.tokensByPlace, :ready, Token[])[1] |> Peven.runKey == "r2"
            @test !haskey(r2.finalMarking.tokensByPlace, :done)
        end
    end

    @testset "fire: guard exceptions emit GuardErrored without fake firings" begin
        net = chainNet(guard = _ -> error("boom"))
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        events = EngineEvent[]
        withExec(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; maxConcurrency=1, onEvent = event -> push!(events, event))
            @test results[1].status === :failed
            @test results[1].reason === :guardError
            @test results[1].error == "boom"
            @test isempty(results[1].trace)
            @test length([event for event in events if event isa GuardErrored]) == 1
            @test isempty([event for event in events if event isa TransitionStarted])
            @test isempty([event for event in events if event isa TransitionFailed])
        end
    end

    @testset "fire: guard exceptions do not create firing-id gaps" begin
        net = chainNet(guard = ts -> begin
            runKey(ts[1]) == "r1" && error("boom")
            true
        end)
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r2")]))
        events = EngineEvent[]
        withExec(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; maxConcurrency=1, onEvent = event -> push!(events, event))
            started = [event for event in events if event isa TransitionStarted]
            r1 = only([result for result in results if result.runKey == "r1"])
            r2 = only([result for result in results if result.runKey == "r2"])

            @test length(started) == 1
            @test started[1].firingId == 1
            @test isempty(r1.trace)
            @test r2.trace[1].firingId == 1
        end
    end

    @testset "fire: retry recovers" begin
        attempts = Ref(0)
        net = chainNet(retries=1)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((tid, tokens) -> begin
            attempts[] += 1
            attempts[] == 1 && error("transient")
            passthrough(tid, tokens)
        end)
        withExec(:default, executor) do
            results = fire(net, marking; maxConcurrency=1)
            @test length(results) == 1
            @test results[1].status === :completed
            @test attempts[] == 2
            @test results[1].trace[1].attempts == 2
        end
    end

    @testset "fire: retry exhausted" begin
        net = chainNet(retries=0)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, _) -> error("permanent"))
        withExec(:default, executor) do
            results = fire(net, marking; maxConcurrency=1)
            @test length(results) == 1
            @test results[1].status === :failed
            @test results[1].reason === :executorFailed
            @test results[1].error !== nothing
        end
    end

    @testset "fire: retry blocked by fuse records the launched failure" begin
        net = chainNet(retries=1)
        marking = Marking(Dict(:ready => Token[Token(:default, "r1", "work")]))
        executor = FunctionExecutor((_, _) -> error("transient"))
        events = EngineEvent[]
        withExec(:default, executor) do
            results = fire(net, marking; maxConcurrency=1, fuse=1, onEvent = event -> push!(events, event))
            @test length(results) == 1
            @test results[1].status === :incomplete
            @test results[1].reason === :fuseExhausted
            @test length(results[1].trace) == 1
            @test results[1].trace[1].status === :failed
            @test results[1].trace[1].firingId == 1
            @test only([event for event in events if event isa TransitionFailed]).retrying == false
            @test get(results[1].finalMarking.tokensByPlace, :ready, Token[])[1].payload == "work"
            @test !haskey(results[1].finalMarking.tokensByPlace, :done)
        end
    end

    @testset "fire: retry gets a fresh input vector" begin
        attempts = Ref(0)
        net = chainNet(retries=1)
        marking = Marking(Dict(:ready => Token[Token(:default, "r1", "work")]))
        executor = FunctionExecutor((tid, tokens) -> begin
            attempts[] += 1
            if attempts[] == 1
                empty!(tokens)
                error("transient")
            end
            return Token(color(tokens[1]), Peven.runKey(tokens[1]), length(tokens))
        end)
        withExec(:default, executor) do
            results = fire(net, marking; maxConcurrency=1)
            @test results[1].status === :completed
            @test results[1].trace[1].outputs[1].payload == 1
        end
    end

    @testset "fire: fuse exhaustion" begin
        # Cycle: ready -> judge -> ready (loop forever without fuse)
        places = Dict(:ready => Place(:ready))
        transitions = Dict(:judge => Transition(:judge))
        arcsfrom = [ArcFrom(:judge, :ready)]
        arcsto = [ArcTo(:judge, :ready)]
        net = Net(places, transitions, arcsfrom, arcsto)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        withExec(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; maxConcurrency=1, fuse=3)
            @test length(results) == 1
            @test results[1].status === :incomplete
            @test results[1].reason === :fuseExhausted
        end
    end

    @testset "fire: onEvent hook" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        events = EngineEvent[]
        withExec(:default, FunctionExecutor(passthrough)) do
            fire(net, marking; maxConcurrency=1, onEvent = e -> push!(events, e))
        end
        @test any(e -> e isa TransitionStarted, events)
        @test any(e -> e isa TransitionCompleted, events)
        @test any(e -> e isa RunFinished, events)
        started = only([e for e in events if e isa TransitionStarted])
        completed = only([e for e in events if e isa TransitionCompleted])
        finished = only([e for e in events if e isa RunFinished])
        @test started.firingId == 1
        @test started.attempt == 1
        @test completed.firingId == 1
        @test completed.attempt == 1
        @test length(completed.outputs) == 1
        @test finished.result.trace[1].firingId == 1
        @test length(finished.result.trace[1].outputs) == 1
    end

    @testset "fire: vector output emits many tokens to one place" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "a"),
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "b"),
        ])
        withExec(:default, executor) do
            results = fire(net, marking; maxConcurrency=1)
            @test results[1].status === :completed
            @test [token.payload for token in results[1].trace[1].outputs] == ["a", "b"]
            @test [token.payload for token in results[1].finalMarking.tokensByPlace[:done]] == ["a", "b"]
        end
    end

    @testset "fire: plain return keeps fan-out semantics" begin
        net = branchNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        withExec(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; maxConcurrency=1)
            outputs = results[1].trace[1].outputs
            @test length(outputs) == 1
            @test get(results[1].finalMarking.tokensByPlace, :left, Token[])[1].payload == (transition=:judge,)
            @test get(results[1].finalMarking.tokensByPlace, :right, Token[])[1].payload == (transition=:judge,)
        end
    end

    @testset "fire: vector output rejects branching transitions" begin
        net = branchNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "a"),
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "b"),
        ])
        withExec(:default, executor) do
            @test_throws ArgumentError fire(net, marking; maxConcurrency=1)
        end
    end

    @testset "fire: singleton vector output rejects branching transitions" begin
        net = branchNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "a"),
        ])
        withExec(:default, executor) do
            @test_throws ArgumentError fire(net, marking; maxConcurrency=1)
        end
    end

    @testset "fire: vector output rejects weighted output arcs" begin
        net = weightedOutputNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "a"),
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "b"),
        ])
        withExec(:default, executor) do
            @test_throws ArgumentError fire(net, marking; maxConcurrency=1)
        end
    end

    @testset "fire: singleton vector output rejects weighted output arcs" begin
        net = weightedOutputNet()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "a"),
        ])
        withExec(:default, executor) do
            @test_throws ArgumentError fire(net, marking; maxConcurrency=1)
        end
    end

    @testset "fire: vector output works with Marking{AbstractToken}" begin
        net = chainNet()
        marking = Marking{AbstractToken}(Dict(:ready => AbstractToken[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "a"),
            Token(color(tokens[1]), Peven.runKey(tokens[1]), "b"),
        ])
        withExec(:default, executor) do
            results = fire(net, marking; maxConcurrency=1)
            @test results[1].status === :completed
            @test [token.payload for token in results[1].trace[1].outputs] == ["a", "b"]
            @test [token.payload for token in results[1].finalMarking.tokensByPlace[:done]] == ["a", "b"]
        end
    end

    @testset "fire: overlaps same transition and runKey" begin
        net = chainNet()
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r1")]))
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            sleep(0.1)
            passthrough(tid, tokens)
        end)
        withExec(:default, executor) do
            results = fire(net, marking; maxConcurrency=2, onEvent=event -> push!(events, event))
            @test length(results) == 1
            @test length(results[1].trace) == 2
            @test length(Set(step.firingId for step in results[1].trace)) == 2
            started = findall(event -> event isa TransitionStarted, events)
            completed = findall(event -> event isa TransitionCompleted, events)
            @test length(started) == 2
            @test length(completed) == 2
            @test started[2] < completed[1]
        end
    end

    @testset "fire: retries keep firing identity" begin
        net = chainNet(retries=1)
        marking = Marking(Dict(:ready => Token[
            Token(:default, "r1", "a"),
            Token(:default, "r1", "b"),
        ]))
        events = EngineEvent[]
        stateLock = ReentrantLock()
        attemptsByPayload = Dict{Any, Int}()
        executor = FunctionExecutor((_, tokens) -> begin
            payload = only(tokens).payload
            attempt = Base.lock(stateLock) do
                next = get(attemptsByPayload, payload, 0) + 1
                attemptsByPayload[payload] = next
                next
            end
            if payload == "a" && attempt == 1
                sleep(0.05)
                error("transient")
            end
            sleep(0.1)
            return Token(color(tokens[1]), Peven.runKey(tokens[1]), payload)
        end)
        withExec(:default, executor) do
            results = fire(net, marking; maxConcurrency=2, onEvent = e -> push!(events, e))
            startedEvents = [event for event in events if event isa TransitionStarted]
            @test length(Set(event.firingId for event in startedEvents)) == 2
            attemptsByFiring = Dict{Int, Vector{Int}}()
            for event in startedEvents
                push!(get!(attemptsByFiring, event.firingId, Int[]), event.attempt)
            end
            observed = sort([sort(attempts) for attempts in values(attemptsByFiring)], by=length)
            @test observed == [[1], [1, 2]]
            @test results[1].status === :completed
        end
    end

    @testset "fire: fuse exhaustion drains started firings" begin
        places = Dict(:ready => Place(:ready))
        transitions = Dict(:judge => Transition(:judge))
        arcsfrom = [ArcFrom(:judge, :ready)]
        arcsto = [ArcTo(:judge, :ready)]
        net = Net(places, transitions, arcsfrom, arcsto)
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r1")]))
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            sleep(0.1)
            passthrough(tid, tokens)
        end)
        withExec(:default, executor) do
            results = fire(net, marking; maxConcurrency=2, fuse=2, onEvent = e -> push!(events, e))
            startedEvents = [event for event in events if event isa TransitionStarted]
            @test length(startedEvents) == 2
            @test results[1].status === :incomplete
            @test results[1].reason === :fuseExhausted
        end
    end

    @testset "fire: invalid net" begin
        net = Net(
            Dict(:ready => Place(:other)),
            Dict(:judge => Transition(:judge)),
            ArcFrom[], ArcTo[],
        )
        marking = Marking()
        @test_throws ArgumentError fire(net, marking)
    end
end

end
