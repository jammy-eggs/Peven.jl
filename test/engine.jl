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

# Branch: ready --> judge --> left, right
function branch_net()
    places = Dict(:ready => Place(:ready), :left => Place(:left), :right => Place(:right))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :left), ArcTo(:judge, :right)]
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

# Weighted output: ready --> judge --(2)--> done
function weighted_output_net()
    places = Dict(:ready => Place(:ready), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done, 2)]
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

    @testset "drop: many outputs" begin
        net = chain_net()
        marking = Marking()
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        new_marking = drop(marking, net, :judge, outputs)
        @test [token.payload for token in new_marking.tokens_by_place[:done]] == ["a", "b"]
    end

    @testset "drop: many outputs is atomic on capacity failure" begin
        net = bounded_net()
        marking = Marking(Dict(:done => Token[Token(:red, "r1", "existing")]))
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        @test_throws ArgumentError drop(marking, net, :judge, outputs)
        @test [token.payload for token in marking.tokens_by_place[:done]] == ["existing"]
    end

    @testset "drop: many outputs rejects branching fan-out" begin
        net = branch_net()
        marking = Marking()
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        @test_throws ArgumentError drop(marking, net, :judge, outputs)
    end

    @testset "drop: many outputs rejects weighted output arc" begin
        net = weighted_output_net()
        marking = Marking()
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        @test_throws ArgumentError drop(marking, net, :judge, outputs)
    end

    @testset "drop: many outputs accepts concrete vectors for Marking{AbstractToken}" begin
        net = chain_net()
        marking = Marking{AbstractToken}(Dict(:done => AbstractToken[]))
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        new_marking = drop(marking, net, :judge, outputs)
        @test [token.payload for token in new_marking.tokens_by_place[:done]] == ["a", "b"]
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
        emit(nothing, TransitionStarted(:t, "r1", 1, 1, Token[]))

        # emit with hook calls it
        events = EngineEvent[]
        hook = e -> push!(events, e)
        emit(hook, TransitionStarted(:t, "r1", 1, 1, Token[]))
        @test length(events) == 1
        @test events[1] isa TransitionStarted
        @test events[1].transition_id === :t
        @test events[1].firing_id == 1
        @test events[1].attempt == 1
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

    # ── _collect_enablement ──────────────────────────────────────────────────

    @testset "_collect_enablement: classifies ready blocked and errored pairs" begin
        places = Dict(:ready => Place(:ready), :done => Place(:done))
        transitions = Dict(
            :ready_t => Transition(:ready_t),
            :blocked_t => Transition(:blocked_t; guard=_ -> false),
            :errored_t => Transition(:errored_t; guard=_ -> error("boom")),
        )
        arcsfrom = [ArcFrom(:ready_t, :ready), ArcFrom(:blocked_t, :ready), ArcFrom(:errored_t, :ready)]
        arcsto = [ArcTo(:ready_t, :done), ArcTo(:blocked_t, :done), ArcTo(:errored_t, :done)]
        net = Net(places, transitions, arcsfrom, arcsto)
        marking = Marking(Dict(:ready => Token[Token(:red, "r1", 1)]))

        errors = Any[]
        entries = Peven._collect_enablement(
            net,
            marking;
            on_guard_error=(tid, rk, e) -> push!(errors, (tid, rk, e)),
        )

        @test entries[(:ready_t, "r1")].status === :ready
        @test entries[(:blocked_t, "r1")].status === :guard_blocked
        @test entries[(:errored_t, "r1")].status === :guard_errored
        @test entries[(:errored_t, "r1")].error == "boom"
        @test length(errors) == 1
        @test errors[1][1] === :errored_t
        @test errors[1][2] == "r1"
    end

    @testset "_collect_enablement: does not mutate marking" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token(:red, "r1", "work")]))
        original = [token.payload for token in marking.tokens_by_place[:ready]]

        entries = Peven._collect_enablement(net, marking)

        @test [token.payload for token in marking.tokens_by_place[:ready]] == original
        @test entries[(:judge, "r1")].status === :ready
    end

    @testset "_collect_enablement: hot and cold remain projections" begin
        places = Dict(:ready => Place(:ready), :done => Place(:done))
        transitions = Dict(
            :pass_t => Transition(:pass_t),
            :guarded_t => Transition(:guarded_t; guard=_ -> false),
            :other_t => Transition(:other_t),
        )
        arcsfrom = [ArcFrom(:pass_t, :ready), ArcFrom(:guarded_t, :ready), ArcFrom(:other_t, :done)]
        arcsto = [ArcTo(:pass_t, :done), ArcTo(:guarded_t, :done), ArcTo(:other_t, :ready)]
        net = Net(places, transitions, arcsfrom, arcsto)
        marking = Marking(Dict(:ready => Token[Token("r1")]))

        entries = Peven._collect_enablement(net, marking)
        ready_pairs = sort!(Tuple{Symbol, String}[pair for (pair, entry) in entries if entry.status === :ready])
        cold_tids = sort!(Symbol[
            tid for tid in sort!(collect(keys(net.transitions))) if all(
                !(pair[1] === tid && entry.status === :ready) for (pair, entry) in entries
            )
        ])

        @test hot(net, marking) == ready_pairs
        @test cold(net, marking) == cold_tids
    end

    # ── _reconcile_enablement! ───────────────────────────────────────────────

    @testset "_reconcile_enablement!: guard error is edge-triggered across rescans" begin
        mode = Ref(:errored)
        net = chain_net(guard=_ -> begin
            mode[] === :errored && error("boom")
            mode[] === :blocked && return false
            return true
        end)
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        state = Peven._scheduler_state(marking)
        events = EngineEvent[]
        hook = event -> push!(events, event)

        Peven._reconcile_enablement!(
            state,
            Peven._collect_enablement(net, marking),
            [:judge];
            on_event=hook,
        )
        @test [event for event in events if event isa Peven.GuardErrored] |> length == 1
        @test haskey(state.runs["r1"].active_guard_errors, :judge)
        @test isempty(state.ready)

        Peven._reconcile_enablement!(
            state,
            Peven._collect_enablement(net, marking),
            [:judge];
            on_event=hook,
        )
        @test [event for event in events if event isa Peven.GuardErrored] |> length == 1
        @test haskey(state.runs["r1"].active_guard_errors, :judge)

        mode[] = :ready
        Peven._reconcile_enablement!(
            state,
            Peven._collect_enablement(net, marking),
            [:judge];
            on_event=hook,
        )
        @test !haskey(state.runs["r1"].active_guard_errors, :judge)
        @test state.ready == [(:judge, "r1")]

        mode[] = :blocked
        Peven._reconcile_enablement!(
            state,
            Peven._collect_enablement(net, marking),
            [:judge];
            on_event=hook,
        )
        @test !haskey(state.runs["r1"].active_guard_errors, :judge)
        @test isempty(state.ready)

        mode[] = :errored
        Peven._reconcile_enablement!(
            state,
            Peven._collect_enablement(net, marking),
            [:judge];
            on_event=hook,
        )
        guard_events = [event for event in events if event isa Peven.GuardErrored]
        @test length(guard_events) == 2
        @test haskey(state.runs["r1"].active_guard_errors, :judge)
    end

    # ── _finalize_results ────────────────────────────────────────────────────

    @testset "_finalize_results: active guard error fails the run" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        state = Peven._scheduler_state(marking)
        state.runs["r1"].active_guard_errors[:judge] = Peven._GuardErrorEpisode("boom", 1)

        results = Peven._finalize_results(net, state, ["r1"]; fuse=10)

        @test length(results) == 1
        @test results[1].status === :failed
        @test results[1].terminal_reason === :guard_error
        @test results[1].error == "boom"
        @test isempty(results[1].trace)
    end

    @testset "_finalize_results: terminal reasons stay per-run" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[
            Token("r1"),
            Token("r2"),
            Token("r3"),
            Token("r4"),
        ]))
        state = Peven._scheduler_state(marking)
        state.fired = 3
        state.runs["r1"].active_guard_errors[:judge] = Peven._GuardErrorEpisode("guard boom", 1)
        state.runs["r2"].fuse_blocked = true
        state.runs["r3"].executor_failed = true
        state.runs["r3"].executor_error = "executor boom"

        results = Dict(result.run_key => result for result in Peven._finalize_results(net, state, ["r1", "r2", "r3", "r4"]; fuse=3))

        @test results["r1"].status === :failed
        @test results["r1"].terminal_reason === :guard_error
        @test results["r1"].error == "guard boom"

        @test results["r2"].status === :incomplete
        @test results["r2"].terminal_reason === :fuse_exhausted
        @test isnothing(results["r2"].error)

        @test results["r3"].status === :failed
        @test results["r3"].terminal_reason === :executor_failed
        @test results["r3"].error == "executor boom"

        @test results["r4"].status === :incomplete
        @test results["r4"].terminal_reason === :no_enabled_transition
        @test isnothing(results["r4"].error)
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
            @test length(results[1].trace[1].outputs) == 1
            @test results[1].trace[1].attempts == 1
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

    @testset "fire: guard exceptions emit GuardErrored without fake firings" begin
        net = chain_net(guard = _ -> error("boom"))
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        events = EngineEvent[]
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1, on_event = event -> push!(events, event))
            @test length(results) == 1
            @test results[1].status === :failed
            @test results[1].terminal_reason === :guard_error
            @test results[1].error == "boom"
            @test isempty(results[1].trace)
            @test length([event for event in events if event isa GuardErrored]) == 1
            @test isempty([event for event in events if event isa TransitionStarted])
            @test isempty([event for event in events if event isa TransitionFailed])
        end
    end

    @testset "fire: guard exceptions do not create firing-id gaps" begin
        net = chain_net(guard = ts -> begin
            Peven.run_key(ts[1]) == "r1" && error("boom")
            true
        end)
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r2")]))
        events = EngineEvent[]
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1, on_event = event -> push!(events, event))
            started = [event for event in events if event isa TransitionStarted]
            completed = [event for event in events if event isa TransitionCompleted]
            guard_events = [event for event in events if event isa GuardErrored]
            r1 = only([result for result in results if result.run_key == "r1"])
            r2 = only([result for result in results if result.run_key == "r2"])

            @test length(guard_events) == 1
            @test length(started) == 1
            @test started[1].firing_id == 1
            @test length(completed) == 1
            @test completed[1].firing_id == 1
            @test isempty(r1.trace)
            @test r2.trace[1].firing_id == 1
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
            @test results[1].trace[1].attempts == 2
        end
    end

    @testset "fire: retry does not re-evaluate the guard" begin
        guard_calls = Ref(0)
        guard_open = Ref(true)
        net = chain_net(
            guard=_ -> begin
                guard_calls[] += 1
                guard_open[]
            end,
            retries=1,
        )
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        attempts = Ref(0)
        executor = FunctionExecutor((tid, tokens) -> begin
            attempts[] += 1
            if attempts[] == 1
                guard_open[] = false
                error("transient")
            end
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1)
            @test results[1].status === :completed
            @test guard_calls[] == 1
            @test results[1].trace[1].attempts == 2
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

    @testset "fire: executor failure suppresses sibling ready work for the run" begin
        net = split_net()
        marking = Marking(Dict(
            :left => Token[Token("r1")],
            :right => Token[Token("r1")],
        ))
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            tid === :left_t && error("permanent")
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1, on_event = event -> push!(events, event))
            @test length(results) == 1
            @test results[1].status === :failed
            @test results[1].terminal_reason === :executor_failed
            @test [step.transition_id for step in results[1].trace] == [:left_t]
            @test [event.transition_id for event in events if event isa TransitionStarted] == [:left_t]
            @test get(results[1].final_marking.tokens_by_place, :right, Token[])[1] |> Peven.run_key == "r1"
        end
    end

    @testset "fire: executor failure suppresses later guard errors for the run" begin
        guard_mode = Ref(:open)
        net = chain_net(
            guard=_ -> begin
                guard_mode[] === :errored && error("guard boom")
                true
            end,
            retries=0,
        )
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r1")]))
        events = EngineEvent[]
        executor = FunctionExecutor((_, _) -> begin
            guard_mode[] = :errored
            error("permanent")
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1, on_event = event -> push!(events, event))
            @test results[1].status === :failed
            @test results[1].terminal_reason === :executor_failed
            @test isempty([event for event in events if event isa GuardErrored])
        end
    end

    @testset "fire: retry blocked by fuse restores marking" begin
        net = chain_net(retries=1)
        marking = Marking(Dict(:ready => Token[Token(:default, "r1", "work")]))
        executor = FunctionExecutor((_, _) -> error("transient"))
        with_executor(:default, executor) do
            events = EngineEvent[]
            results = fire(net, marking; max_concurrency=1, fuse=1, on_event = event -> push!(events, event))
            @test length(results) == 1
            @test results[1].status === :incomplete
            @test results[1].terminal_reason === :fuse_exhausted
            @test length(results[1].trace) == 1
            @test results[1].trace[1].status === :failed
            @test results[1].trace[1].firing_id == 1
            @test results[1].trace[1].attempts == 1
            @test get(results[1].final_marking.tokens_by_place, :ready, Token[])[1].payload == "work"
            @test !haskey(results[1].final_marking.tokens_by_place, :done)
            failed = only([event for event in events if event isa TransitionFailed])
            @test failed.firing_id == 1
            @test failed.retrying == false
        end
    end

    @testset "fire: fuse-blocked retry can coexist with a successful sibling firing" begin
        net = chain_net(retries=1)
        marking = Marking(Dict(:ready => Token[
            Token(:default, "r1", "retry"),
            Token(:default, "r1", "done"),
        ]))
        attempts_by_payload = Dict{Any, Int}()
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            payload = only(tokens).payload
            attempt = get(attempts_by_payload, payload, 0) + 1
            attempts_by_payload[payload] = attempt
            if payload == "retry" && attempt == 1
                error("transient")
            end
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=2, fuse=2, on_event = event -> push!(events, event))
            @test length(results) == 1
            @test results[1].status === :incomplete
            @test results[1].terminal_reason === :fuse_exhausted
            @test Set(step.status for step in results[1].trace) == Set([:completed, :failed])
            @test length([event for event in events if event isa TransitionCompleted]) == 1
            @test length([event for event in events if event isa TransitionFailed && !event.retrying]) == 1
            @test [token.payload for token in get(results[1].final_marking.tokens_by_place, :done, Token[])] == [(transition=:judge,)]
            @test [token.payload for token in get(results[1].final_marking.tokens_by_place, :ready, Token[])] == ["retry"]
        end
    end

    @testset "fire: retry gets a fresh input vector" begin
        attempts = Ref(0)
        net = chain_net(retries=1)
        marking = Marking(Dict(:ready => Token[Token(:default, "r1", "work")]))
        executor = FunctionExecutor((tid, tokens) -> begin
            attempts[] += 1
            if attempts[] == 1
                empty!(tokens)
                error("transient")
            end
            return Token(color(tokens[1]), Peven.run_key(tokens[1]), length(tokens))
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1)
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
        started = only([e for e in events if e isa TransitionStarted])
        completed = only([e for e in events if e isa TransitionCompleted])
        finished = only([e for e in events if e isa RunFinished])
        @test started.firing_id == 1
        @test started.attempt == 1
        @test completed.firing_id == 1
        @test completed.attempt == 1
        @test length(completed.outputs) == 1
        @test finished.result.trace[1].firing_id == 1
        @test length(finished.result.trace[1].outputs) == 1
    end

    @testset "fire: vector output emits many tokens to one place" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "a"),
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "b"),
        ])
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1)
            @test results[1].status === :completed
            @test [token.payload for token in results[1].trace[1].outputs] == ["a", "b"]
            @test [token.payload for token in results[1].final_marking.tokens_by_place[:done]] == ["a", "b"]
        end
    end

    @testset "fire: plain return keeps fan-out semantics" begin
        net = branch_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1)
            outputs = results[1].trace[1].outputs
            @test length(outputs) == 1
            @test get(results[1].final_marking.tokens_by_place, :left, Token[])[1].payload == (transition=:judge,)
            @test get(results[1].final_marking.tokens_by_place, :right, Token[])[1].payload == (transition=:judge,)
        end
    end

    @testset "fire: vector output rejects branching transitions" begin
        net = branch_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "a"),
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "b"),
        ])
        with_executor(:default, executor) do
            @test_throws ArgumentError fire(net, marking; max_concurrency=1)
        end
    end

    @testset "fire: singleton vector output rejects branching transitions" begin
        net = branch_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "a"),
        ])
        with_executor(:default, executor) do
            @test_throws ArgumentError fire(net, marking; max_concurrency=1)
        end
    end

    @testset "fire: vector output rejects weighted output arcs" begin
        net = weighted_output_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "a"),
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "b"),
        ])
        with_executor(:default, executor) do
            @test_throws ArgumentError fire(net, marking; max_concurrency=1)
        end
    end

    @testset "fire: singleton vector output rejects weighted output arcs" begin
        net = weighted_output_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "a"),
        ])
        with_executor(:default, executor) do
            @test_throws ArgumentError fire(net, marking; max_concurrency=1)
        end
    end

    @testset "fire: vector output works with Marking{AbstractToken}" begin
        net = chain_net()
        marking = Marking{AbstractToken}(Dict(:ready => AbstractToken[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "a"),
            Token(color(tokens[1]), Peven.run_key(tokens[1]), "b"),
        ])
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1)
            @test results[1].status === :completed
            @test [token.payload for token in results[1].trace[1].outputs] == ["a", "b"]
            @test [token.payload for token in results[1].final_marking.tokens_by_place[:done]] == ["a", "b"]
        end
    end

    @testset "fire: overlaps same transition and run_key" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1"), Token("r1")]))
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            sleep(0.1)
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=2, on_event=event -> push!(events, event))
            @test length(results) == 1
            @test length(results[1].trace) == 2
            @test length(Set(step.firing_id for step in results[1].trace)) == 2
            started = findall(event -> event isa TransitionStarted, events)
            completed = findall(event -> event isa TransitionCompleted, events)
            @test length(started) == 2
            @test length(completed) == 2
            @test started[2] < completed[1]
        end
    end

    @testset "fire: retries keep firing identity" begin
        net = chain_net(retries=1)
        marking = Marking(Dict(:ready => Token[
            Token(:default, "r1", "a"),
            Token(:default, "r1", "b"),
        ]))
        events = EngineEvent[]
        state_lock = ReentrantLock()
        attempts_by_payload = Dict{Any, Int}()
        executor = FunctionExecutor((_, tokens) -> begin
            payload = only(tokens).payload
            attempt = Base.lock(state_lock) do
                next = get(attempts_by_payload, payload, 0) + 1
                attempts_by_payload[payload] = next
                next
            end
            if payload == "a" && attempt == 1
                sleep(0.05)
                error("transient")
            end
            sleep(0.1)
            return Token(color(tokens[1]), Peven.run_key(tokens[1]), payload)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=2, on_event = e -> push!(events, e))
            started_events = [event for event in events if event isa TransitionStarted]
            @test length(Set(event.firing_id for event in started_events)) == 2
            attempts_by_firing = Dict{Int, Vector{Int}}()
            for event in started_events
                push!(get!(attempts_by_firing, event.firing_id, Int[]), event.attempt)
            end
            observed = sort([sort(attempts) for attempts in values(attempts_by_firing)], by=length)
            @test observed == [[1], [1, 2]]
            @test results[1].status === :completed
        end
    end

    @testset "fire: guard error recovery and re-entry emit edge events" begin
        places = Dict(:ready => Place(:ready), :pulse => Place(:pulse))
        mode = Ref(:errored)
        transitions = Dict(
            :a_judge => Transition(:a_judge; guard=_ -> begin
                mode[] === :errored && error("boom")
                mode[] === :blocked && return false
                true
            end),
            :z_flip => Transition(:z_flip),
        )
        arcsfrom = [ArcFrom(:a_judge, :ready), ArcFrom(:a_judge, :pulse), ArcFrom(:z_flip, :pulse)]
        arcsto = [ArcTo(:a_judge, :ready), ArcTo(:a_judge, :pulse), ArcTo(:z_flip, :pulse)]
        net = Net(places, transitions, arcsfrom, arcsto)
        marking = Marking(Dict(:ready => Token[Token("r1")], :pulse => Token[Token("r1")]))
        flips = Ref(0)
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            if tid === :z_flip
                flips[] += 1
                mode[] = flips[] == 1 ? :ready : :errored
            elseif tid === :a_judge
                mode[] = :blocked
            end
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1, fuse=3, on_event = event -> push!(events, event))
            guard_events = [event for event in events if event isa GuardErrored]

            @test length(guard_events) == 2
            @test all(event.transition_id === :a_judge for event in guard_events)
            @test results[1].status === :failed
            @test results[1].terminal_reason === :guard_error
            @test results[1].error == "boom"
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
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=2, fuse=2, on_event = e -> push!(events, e))
            started_events = [event for event in events if event isa TransitionStarted]
            @test length(started_events) == 2
            @test results[1].status === :incomplete
            @test results[1].terminal_reason === :fuse_exhausted
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
