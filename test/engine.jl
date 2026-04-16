module SrcEngineTests

using Test
using Main.Peven

# ── Test fixtures ─────────────────────────────────────────────────────────────

# Simple chain: ready --(1)--> judge --(1)--> done
function chain_net(; guard=nothing, retries=0)
    places = Dict(:ready => Place(:ready), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge; guard=guard, retries=retries))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Fork/join: left --> join --> done, right --> join
function join_net()
    places = Dict(:left => Place(:left), :right => Place(:right), :done => Place(:done))
    transitions = Dict(:join => Transition(:join))
    arcsfrom = [ArcFrom(:join, :left), ArcFrom(:join, :right)]
    arcsto = [ArcTo(:join, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Weighted: ready --(2)--> judge --(1)--> done
function weighted_net()
    places = Dict(:ready => Place(:ready), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready, 2)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Bounded: ready --> judge --> done (capacity 1)
function bounded_net()
    places = Dict(:ready => Place(:ready), :done => Place(:done, 1))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

# Two independent transitions for check/filter tests
function split_net()
    places = Dict(:left => Place(:left), :right => Place(:right), :done => Place(:done))
    transitions = Dict(:left_t => Transition(:left_t), :right_t => Transition(:right_t))
    arcsfrom = [ArcFrom(:left_t, :left), ArcFrom(:right_t, :right)]
    arcsto = [ArcTo(:left_t, :done), ArcTo(:right_t, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

passthrough(tid, tokens) = Token(color(tokens[1]), Peven.run_key(tokens[1]), (transition=tid,))

function with_executor(f, name::Symbol, executor)
    register_executor!(name, executor)
    try f() finally delete!(Peven.EXECUTOR_REGISTRY, name) end
end

# ── Guard dispatch ────────────────────────────────────────────────────────────

@testset "src/engine" begin
    @testset "evaluate_guard" begin
        @test evaluate_guard(nothing, []) == true
        @test evaluate_guard(ts -> length(ts) > 0, [Token("r1")]) == true
        @test evaluate_guard(ts -> length(ts) > 5, [Token("r1")]) == false
    end

    # ── drop ───────────────────────────────────────────────────────────────

    @testset "drop: basic" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token(:red, "r1", 1)]))
        output = Token(:red, "r1", "result")
        new_marking = drop(marking, net, :judge, output)
        @test length(new_marking.tokens_by_place[:done]) == 1
        @test new_marking.tokens_by_place[:done][1].payload == "result"
    end

    @testset "drop: weighted" begin
        net = weighted_net()
        marking = Marking()
        output = Token(:red, "r1", "x")
        # drop doesn't check inputs, just outputs — weight on output arc is 1
        new_marking = drop(marking, net, :judge, output)
        @test length(new_marking.tokens_by_place[:done]) == 1
    end

    @testset "drop: capacity exceeded" begin
        net = bounded_net()
        marking = Marking(Dict(:done => Token[Token(:red, "r1", "existing")]))
        output = Token(:red, "r1", "overflow")
        @test_throws ArgumentError drop(marking, net, :judge, output)
    end

    # ── misfire ─────────────────────────────────────────────────────────────

    @testset "misfire" begin
        marking = Marking(Dict(:ready => Token[]))
        consumed = Dict(:ready => Token[Token(:red, "r1", 1)])
        restored = misfire(marking, consumed)
        @test length(restored.tokens_by_place[:ready]) == 1
        @test Peven.run_key(restored.tokens_by_place[:ready][1]) == "r1"
    end

    # ── fuses ─────────────────────────────────────────────────────────

    @testset "fuses" begin
        marking = Marking(Dict(
            :a => Token[Token(:red, "r1", 1), Token(:blue, "r2", 2)],
            :b => Token[Token(:red, "r1", 3)],
        ))
        keys = fuses(marking)
        @test Set(keys) == Set(["r1", "r2"])
    end

    @testset "fuses: empty" begin
        @test isempty(fuses(Marking()))
    end

    # ── run_completed ─────────────────────────────────────────────────────────

    @testset "run_completed" begin
        from = Set([:ready])
        m1 = Marking(Dict(:ready => Token[Token(:red, "r1", 1)]))
        @test Peven.run_completed(m1, "r1", from) == false

        m2 = Marking(Dict(:done => Token[Token(:red, "r1", 1)]))
        @test Peven.run_completed(m2, "r1", from) == true

        m3 = Marking()
        @test Peven.run_completed(m3, "r1", from) == true
    end

    # ── executor registry ─────────────────────────────────────────────────────

    @testset "executor registry" begin
        fn_exec = FunctionExecutor((tid, tokens) -> Token(Peven.run_key(tokens[1]), tid, nothing))
        register_executor!(:test_exec, fn_exec)
        @test get_executor(:test_exec) === fn_exec
        @test_throws KeyError get_executor(:nonexistent)

        # Clean up
        delete!(Peven.EXECUTOR_REGISTRY, :test_exec)
    end

    # ── emit ──────────────────────────────────────────────────────────────────

    @testset "emit" begin
        # emit with nothing hook does nothing
        emit(nothing, TransitionStarted(:t, "r1", Token[]))

        # emit with hook calls it
        events = EngineEvent[]
        hook = e -> push!(events, e)
        emit(hook, TransitionStarted(:t, "r1", Token[]))
        @test length(events) == 1
        @test events[1] isa TransitionStarted
        @test events[1].transition_id === :t
    end

    # ── hot ───────────────────────────────────────────────────────────────

    @testset "hot: basic chain" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        result = hot(net, marking)
        @test result == [(:judge, "r1")]
    end

    @testset "hot: no tokens" begin
        net = chain_net()
        @test isempty(hot(net, Marking()))
    end

    @testset "hot: join requires both places" begin
        net = join_net()
        # Only left has a token — join needs both
        m1 = Marking(Dict(:left => Token[Token("r1")]))
        @test isempty(hot(net, m1))

        # Both places have a token — join is hot
        m2 = Marking(Dict(:left => Token[Token("r1")], :right => Token[Token("r1")]))
        @test hot(net, m2) == [(:join, "r1")]
    end

    @testset "hot: skips failed" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(hot(net, marking; failed=Set(["r1"])))
    end

    @testset "hot: skips in_flight" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(hot(net, marking; in_flight=Set([(:judge, "r1")])))
    end

    @testset "hot: guard blocks" begin
        net = chain_net(guard = _ -> false)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(hot(net, marking))
    end

    @testset "hot: guard exception" begin
        net = chain_net(guard = _ -> error("boom"))
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        # Silent by default — just skips
        @test isempty(hot(net, marking))

        # With callback — records the error
        errors = []
        hot(net, marking; on_guard_error = (tid, rk, e) -> push!(errors, (tid, rk, e)))
        @test length(errors) == 1
        @test errors[1][1] === :judge
        @test errors[1][2] == "r1"
    end

    @testset "hot: weighted arc" begin
        net = weighted_net()
        # 1 token, weight 2 — not hot
        m1 = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(hot(net, m1))

        # 2 tokens, weight 2 — hot
        m2 = Marking(Dict(:ready => Token[Token("r1"), Token("r1")]))
        @test hot(net, m2) == [(:judge, "r1")]
    end

    @testset "hot: check parameter" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        # check=[:judge] — includes the transition, finds it
        @test hot(net, marking; check=[:judge]) == [(:judge, "r1")]
        # check=[] — empty scan, finds nothing
        @test isempty(hot(net, marking; check=Symbol[]))
    end

    # ── cold ──────────────────────────────────────────────────────────────

    @testset "cold: all enabled" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test isempty(cold(net, marking))
    end

    @testset "cold: no tokens" begin
        net = chain_net()
        @test cold(net, Marking()) == [:judge]
    end

    @testset "cold: join missing one input" begin
        net = join_net()
        m = Marking(Dict(:left => Token[Token("r1")]))
        @test cold(net, m) == [:join]
    end

    @testset "cold: join fully enabled" begin
        net = join_net()
        m = Marking(Dict(:left => Token[Token("r1")], :right => Token[Token("r1")]))
        @test isempty(cold(net, m))
    end

    @testset "cold: guard blocks" begin
        net = chain_net(guard = _ -> false)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test cold(net, marking) == [:judge]
    end

    @testset "cold: failed run_key blocks transition" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test cold(net, marking; failed=Set(["r1"])) == [:judge]
    end

    @testset "cold: in_flight blocks transition" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test cold(net, marking; in_flight=Set([(:judge, "r1")])) == [:judge]
    end

    @testset "cold: check respects scan subset" begin
        net = split_net()
        marking = Marking(Dict(:left => Token[Token("r1")]))
        @test cold(net, marking) == [:right_t]
        @test isempty(cold(net, marking; check=[:left_t]))
        @test cold(net, marking; check=[:right_t]) == [:right_t]
    end

    # ── grab ──────────────────────────────────────────────────────────────

    @testset "grab: basic" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token(:red, "r1", 42)]))
        result = grab(marking, net, :judge, "r1")
        @test result !== nothing
        new_marking, grabbed, grabbed_by_place = result
        @test length(grabbed) == 1
        @test grabbed[1].payload == 42
        @test isempty(get(new_marking.tokens_by_place, :ready, Token[]))
        @test haskey(grabbed_by_place, :ready)
    end

    @testset "grab: insufficient tokens" begin
        net = chain_net()
        @test grab(Marking(), net, :judge, "r1") === nothing
    end

    @testset "grab: weight > 1" begin
        net = weighted_net()
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r1"), Token("r1")]))
        result = grab(marking, net, :judge, "r1")
        @test result !== nothing
        _, grabbed, _ = result
        @test length(grabbed) == 2
    end

    @testset "grab: preserves other run_keys" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r2")]))
        result = grab(marking, net, :judge, "r1")
        @test result !== nothing
        new_marking, grabbed, _ = result
        @test length(grabbed) == 1
        @test Peven.run_key(grabbed[1]) == "r1"
        remaining = new_marking.tokens_by_place[:ready]
        @test length(remaining) == 1
        @test Peven.run_key(remaining[1]) == "r2"
    end

    @testset "grab: copy-on-write" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        result = grab(marking, net, :judge, "r1")
        @test result !== nothing
        # Original marking unchanged
        @test length(marking.tokens_by_place[:ready]) == 1
    end

    @testset "grab: multiple input places" begin
        net = join_net()
        marking = Marking(Dict(
            :left => Token[Token("r1")],
            :right => Token[Token("r1")],
        ))
        result = grab(marking, net, :join, "r1")
        @test result !== nothing
        _, grabbed, grabbed_by_place = result
        @test length(grabbed) == 2
        @test haskey(grabbed_by_place, :left)
        @test haskey(grabbed_by_place, :right)
    end

    # ── take ──────────────────────────────────────────────────────────────

    @testset "take: success" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        new_marking, grabbed, _ = take(marking, net, :judge, "r1")
        @test length(grabbed) == 1
    end

    @testset "take: failure" begin
        net = chain_net()
        @test_throws ArgumentError take(Marking(), net, :judge, "r1")
    end

    # ── fire ──────────────────────────────────────────────────────────────

    @testset "fire: simple chain" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1)
            @test length(results) == 1
            @test results[1].status === :completed
            @test results[1].run_key == "r1"
            @test length(results[1].trace) == 1
            @test results[1].trace[1].status === :completed
        end
    end

    @testset "fire: multiple run_keys" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r2")]))
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1)
            @test length(results) == 2
            rks = Set(r.run_key for r in results)
            @test rks == Set(["r1", "r2"])
            @test all(r.status === :completed for r in results)
        end
    end

    @testset "fire: guard routing" begin
        # Guard only allows r1
        net = chain_net(guard = ts -> Peven.run_key(ts[1]) == "r1")
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r2")]))
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1)
            r1 = first(r for r in results if r.run_key == "r1")
            r2 = first(r for r in results if r.run_key == "r2")
            @test r1.status === :completed
            @test r2.status === :incomplete
            @test r2.terminal_reason === :no_enabled_transition
            @test get(r1.final_marking.tokens_by_place, :done, Token[])[1] |> Peven.run_key == "r1"
            @test !haskey(r1.final_marking.tokens_by_place, :ready)
            @test get(r2.final_marking.tokens_by_place, :ready, Token[])[1] |> Peven.run_key == "r2"
            @test !haskey(r2.final_marking.tokens_by_place, :done)
        end
    end

    @testset "fire: retry recovers" begin
        attempts = Ref(0)
        net = chain_net(retries=1)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((tid, tokens) -> begin
            attempts[] += 1
            attempts[] == 1 && error("transient")
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1)
            @test length(results) == 1
            @test results[1].status === :completed
            @test attempts[] == 2
        end
    end

    @testset "fire: retry exhausted" begin
        net = chain_net(retries=0)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, _) -> error("permanent"))
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1)
            @test length(results) == 1
            @test results[1].status === :failed
            @test results[1].terminal_reason === :executor_failed
            @test results[1].error !== nothing
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
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1, fuse=3)
            @test length(results) == 1
            @test results[1].status === :incomplete
            @test results[1].terminal_reason === :fuse_exhausted
        end
    end

    @testset "fire: on_event hook" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        events = EngineEvent[]
        with_executor(:default, FunctionExecutor(passthrough)) do
            fire(net, marking; max_concurrency=1, on_event = e -> push!(events, e))
        end
        @test any(e -> e isa TransitionStarted, events)
        @test any(e -> e isa TransitionCompleted, events)
        @test any(e -> e isa RunFinished, events)
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
