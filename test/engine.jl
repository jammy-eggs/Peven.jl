module SrcEngineTests

using Random
using Test
using Main.Peven

item_id(token::AbstractToken) = token.payload.item
batch_token(color::Symbol, rk::String, item; payload=item) = Token(color, rk, (item=item, payload=payload))
batch_token(color::Symbol, rk::String, item, payload) = batch_token(color, rk, item; payload=payload)

function chain_net(; guard=nothing, retries=0)
    places = Dict(:ready => Place(:ready), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge; guard=guard, retries=retries))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

function weighted_net()
    places = Dict(:ready => Place(:ready), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready, 2)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

function branch_net()
    places = Dict(:ready => Place(:ready), :left => Place(:left), :right => Place(:right))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :left), ArcTo(:judge, :right)]
    return Net(places, transitions, arcsfrom, arcsto)
end

function weighted_output_net()
    places = Dict(:ready => Place(:ready), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done, 2)]
    return Net(places, transitions, arcsfrom, arcsto)
end

function unkeyed_join_net(; guard=nothing, retries=0, arc_order=[:left, :right])
    places = Dict(:left => Place(:left), :right => Place(:right), :done => Place(:done))
    transitions = Dict(:join => Transition(:join; guard=guard, retries=retries))
    arcsfrom = ArcFrom[ArcFrom(:join, pid) for pid in arc_order]
    arcsto = [ArcTo(:join, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

function keyed_join_net(; guard=nothing, retries=0, arc_order=[:left, :right], weight=1, join_by=(pid, token) -> token.payload.item)
    places = Dict(:left => Place(:left), :right => Place(:right), :done => Place(:done))
    transitions = Dict(:join => Transition(:join; guard=guard, retries=retries, join_by=join_by))
    arcsfrom = ArcFrom[ArcFrom(:join, pid, weight) for pid in arc_order]
    arcsto = [ArcTo(:join, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

function keyed_loop_net()
    places = Dict(:left => Place(:left), :right => Place(:right))
    transitions = Dict(:join => Transition(:join; join_by=(pid, token) -> token.payload.item))
    arcsfrom = [ArcFrom(:join, :left), ArcFrom(:join, :right)]
    arcsto = [ArcTo(:join, :left), ArcTo(:join, :right)]
    return Net(places, transitions, arcsfrom, arcsto)
end

function rubric_pipeline_net(; join_guard=nothing)
    places = Dict(
        :seed => Place(:seed),
        :problem => Place(:problem),
        :rubric_seed => Place(:rubric_seed),
        :rubric => Place(:rubric),
        :done => Place(:done),
    )
    transitions = Dict(
        :tee => Transition(:tee),
        :make_rubric => Transition(:make_rubric),
        :judge => Transition(:judge; guard=join_guard, join_by=(pid, token) -> token.payload.item),
    )
    arcsfrom = [
        ArcFrom(:tee, :seed),
        ArcFrom(:make_rubric, :rubric_seed),
        ArcFrom(:judge, :problem),
        ArcFrom(:judge, :rubric),
    ]
    arcsto = [
        ArcTo(:tee, :problem),
        ArcTo(:tee, :rubric_seed),
        ArcTo(:make_rubric, :rubric),
        ArcTo(:judge, :done),
    ]
    return Net(places, transitions, arcsfrom, arcsto)
end

function with_executor(f, name::Symbol, executor)
    register_executor!(name, executor)
    try
        f()
    finally
        delete!(Peven.EXECUTOR_REGISTRY, name)
    end
end

passthrough(tid, tokens) = Token(color(tokens[1]), run_key(tokens[1]), (item=item_id(tokens[1]), transition=tid))

@testset "src/engine" begin
    @testset "evaluate_guard" begin
        @test evaluate_guard(nothing, []) == true
        @test evaluate_guard(ts -> length(ts) > 0, [Token("r1")]) == true
        @test evaluate_guard(ts -> length(ts) > 5, [Token("r1")]) == false
    end

    @testset "emit swallows ordinary hook errors but rethrows interrupts" begin
        @test emit(_ -> error("boom"), :event) === nothing
        @test_throws InterruptException emit(_ -> throw(InterruptException()), :event)
    end

    @testset "drop and misfire basics" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token(:red, "r1", 1)]))
        output = Token(:red, "r1", "result")
        new_marking = drop(marking, net, :judge, output)
        @test new_marking.tokens_by_place[:done][1].payload == "result"

        restored = misfire(Marking(Dict(:ready => Token[])), Dict(:ready => Token[Token("r1")]))
        @test length(restored.tokens_by_place[:ready]) == 1
    end

    @testset "drop many outputs keeps vector semantics" begin
        net = chain_net()
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        marking = drop(Marking(), net, :judge, outputs)
        @test [token.payload for token in marking.tokens_by_place[:done]] == ["a", "b"]
    end

    @testset "fuses and run_completed" begin
        marking = Marking(Dict(
            :a => Token[Token(:red, "r2", 1), Token(:blue, "r10", 2)],
            :b => Token[Token(:red, "r1", 3)],
        ))
        @test fuses(marking) == ["r1", "r10", "r2"]
        @test !Peven.run_completed(Marking(Dict(:ready => Token[Token("r1")])), "r1", Set([:ready]))
        @test Peven.run_completed(Marking(Dict(:done => Token[Token("r1")])), "r1", Set([:ready]))
    end

    @testset "enablement: unkeyed chain returns bundle records" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        entries = enablement(net, marking)
        @test length(entries) == 1
        @test entries[1].status === :ready
        @test entries[1].bundle == BundleRef(:judge, "r1", nothing, 1)
        @test hot(net, marking) == BundleRef[BundleRef(:judge, "r1", nothing, 1)]
        @test isempty(cold(net, marking))
    end

    @testset "enablement: unkeyed multi-input transitions preserve ArcFrom declaration order" begin
        net = unkeyed_join_net(arc_order=[:right, :left])
        marking = Marking(Dict(
            :left => Token[batch_token(:left, "r1", :left, "L")],
            :right => Token[batch_token(:right, "r1", :right, "R")],
        ))

        entries = enablement(net, marking)
        @test length(entries) == 1
        @test [token.payload.payload for token in only(entries).inputs] == ["R", "L"]

        bundle = only(hot(net, marking))
        _, grabbed, _ = take(marking, net, bundle)
        @test [token.payload.payload for token in grabbed] == ["R", "L"]
    end

    @testset "enablement/hot/cold reject unknown check transitions" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        @test_throws ArgumentError enablement(net, marking; check=[:ghost])
        @test_throws ArgumentError hot(net, marking; check=[:ghost])
        @test_throws ArgumentError cold(net, marking; check=[:ghost])
    end

    @testset "enablement: keyed join mismatched keys is cold" begin
        net = keyed_join_net()
        marking = Marking(Dict(
            :left => Token[batch_token(:left, "batch_1", :a)],
            :right => Token[batch_token(:right, "batch_1", :b)],
        ))
        @test isempty(hot(net, marking))
        @test cold(net, marking) == [:join]
    end

    @testset "enablement: guard callback is bundle-scoped" begin
        net = chain_net(guard=_ -> error("boom"))
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        seen = Any[]
        entries = enablement(net, marking; on_guard_error=(bundle, e) -> push!(seen, (bundle, e)))
        @test length(entries) == 1
        @test entries[1].status === :guard_errored
        @test entries[1].error == "boom"
        @test seen[1][1] == BundleRef(:judge, "r1", nothing, 1)
        @test isempty(hot(net, marking))
    end

    @testset "enablement: multiple bundles for one selected_key" begin
        net = keyed_join_net()
        marking = Marking(Dict(
            :left => Token[
                batch_token(:left, "batch_1", :a, "l1"),
                batch_token(:left, "batch_1", :a, "l2"),
            ],
            :right => Token[
                batch_token(:right, "batch_1", :a, "r1"),
                batch_token(:right, "batch_1", :a, "r2"),
            ],
        ))
        entries = enablement(net, marking)
        @test [entry.bundle for entry in entries] == [
            BundleRef(:join, "batch_1", :a, 1),
            BundleRef(:join, "batch_1", :a, 2),
        ]
    end

    @testset "enablement: ArcFrom order is non-semantic for keyed joins" begin
        left_first = keyed_join_net(arc_order=[:left, :right])
        right_first = keyed_join_net(arc_order=[:right, :left])
        marking = Marking(Dict(
            :left => Token[
                batch_token(:left, "batch_1", :a, "l_a"),
                batch_token(:left, "batch_1", :b, "l_b"),
            ],
            :right => Token[
                batch_token(:right, "batch_1", :a, "r_a"),
                batch_token(:right, "batch_1", :b, "r_b"),
            ],
        ))

        @test [entry.bundle for entry in enablement(left_first, marking)] ==
            [entry.bundle for entry in enablement(right_first, marking)]
    end

    @testset "enablement: queue preservation across rescans keeps survivors and appends new bundles" begin
        net = keyed_join_net()
        initial = Marking(Dict(
            :left => Token[
                batch_token(:left, "batch_1", :a),
                batch_token(:left, "batch_1", :b),
            ],
            :right => Token[
                batch_token(:right, "batch_1", :a),
                batch_token(:right, "batch_1", :b),
            ],
        ))
        state = Peven._scheduler_state(initial)
        Peven._refresh_enablement!(state, net, [:join])
        @test [state.available[id].bundle.selected_key for id in state.ready] == [:a, :b]

        state.marking = Marking(Dict(
            :left => Token[
                batch_token(:left, "batch_1", :a),
                batch_token(:left, "batch_1", :b),
                batch_token(:left, "batch_1", :c),
            ],
            :right => Token[
                batch_token(:right, "batch_1", :a),
                batch_token(:right, "batch_1", :b),
                batch_token(:right, "batch_1", :c),
            ],
        ))
        Peven._refresh_enablement!(state, net, [:join])
        @test [state.available[id].bundle.selected_key for id in state.ready] == [:a, :b, :c]
    end

    @testset "grab/take: exact bundle reservation and stale bundle behavior" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[
            Token(:red, "r1", "first"),
            Token(:red, "r1", "second"),
        ]))
        bundles = hot(net, marking)
        first_bundle = bundles[1]
        second_bundle = bundles[2]

        grabbed = grab(marking, net, first_bundle)
        @test grabbed !== nothing
        new_marking, tokens, grabbed_by_place = grabbed
        @test [token.payload for token in tokens] == ["first"]
        @test haskey(grabbed_by_place, :ready)
        @test grab(new_marking, net, second_bundle) === nothing
        @test_throws ArgumentError take(new_marking, net, second_bundle)
    end

    @testset "hot: in_flight filters BundleRef" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        bundle = only(hot(net, marking))
        @test isempty(hot(net, marking; in_flight=Set([bundle])))
    end

    @testset "fire: simple chain emits bundle-aware trace and events" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        events = EngineEvent[]
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1, on_event=e -> push!(events, e))
            @test length(results) == 1
            @test results[1].status === :completed
            @test results[1].run_key == "r1"
            @test length(results[1].trace) == 1
            @test results[1].trace[1].bundle == BundleRef(:judge, "r1", nothing, 1)
            @test results[1].trace[1].status === :completed
            started = only([e for e in events if e isa TransitionStarted])
            completed = only([e for e in events if e isa TransitionCompleted])
            @test started.bundle == completed.bundle == results[1].trace[1].bundle
            @test started.firing_id == completed.firing_id == results[1].trace[1].firing_id
        end
    end

    @testset "fire: bundle-level guard errors are edge-triggered and do not alias across siblings" begin
        net = keyed_join_net(guard=tokens -> begin
            item = item_id(tokens[1])
            item == :a && error("boom")
            true
        end)
        marking = Marking(Dict(
            :left => Token[
                batch_token(:left, "batch_1", :a),
                batch_token(:left, "batch_1", :b),
            ],
            :right => Token[
                batch_token(:right, "batch_1", :a),
                batch_token(:right, "batch_1", :b),
            ],
        ))
        events = EngineEvent[]
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1, on_event=e -> push!(events, e))
            guards = [e for e in events if e isa GuardErrored]
            @test length(guards) == 1
            @test guards[1].bundle == BundleRef(:join, "batch_1", :a, 1)
            @test [step.bundle.selected_key for step in results[1].trace if step.status === :completed] == [:b]
            @test results[1].status === :failed
            @test results[1].terminal_reason === :guard_error
            @test results[1].terminal_bundle == BundleRef(:join, "batch_1", :a, 1)
        end
    end

    @testset "fire: earliest active guard error wins terminal tie-breaker" begin
        net = keyed_join_net(guard=tokens -> error(string("boom_", item_id(tokens[1]))))
        marking = Marking(Dict(
            :left => Token[
                batch_token(:left, "batch_1", :a),
                batch_token(:left, "batch_1", :b),
            ],
            :right => Token[
                batch_token(:right, "batch_1", :a),
                batch_token(:right, "batch_1", :b),
            ],
        ))
        events = EngineEvent[]
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1, on_event=e -> push!(events, e))
            guards = [e for e in events if e isa GuardErrored]
            @test length(guards) == 2
            @test results[1].status === :failed
            @test results[1].terminal_reason === :guard_error
            @test results[1].terminal_bundle == BundleRef(:join, "batch_1", :a, 1)
            @test occursin("boom_a", something(results[1].error))
        end
    end

    @testset "fire: selector exceptions emit SelectionErrored and fail only the affected run" begin
        selector = (pid, token) -> getproperty(token.payload, :item)
        net = keyed_join_net(join_by=selector)
        marking = Marking(Dict(
            :left => Token[
                Token(:left, "bad_run", (payload=1,)),
                Token(:left, "good_run", (item=:ok,)),
            ],
            :right => Token[
                Token(:right, "bad_run", (item=:bad,)),
                Token(:right, "good_run", (item=:ok,)),
            ],
        ))
        events = EngineEvent[]
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = Dict(result.run_key => result for result in fire(
                net,
                marking;
                max_concurrency=1,
                on_event=e -> push!(events, e),
            ))
            @test results["bad_run"].status === :failed
            @test results["bad_run"].terminal_reason === :selection_error
            @test results["bad_run"].terminal_transition === :join
            @test isnothing(results["bad_run"].terminal_bundle)
            @test isempty(results["bad_run"].trace)
            @test results["good_run"].status === :completed
            selection = only([e for e in events if e isa SelectionErrored])
            @test selection.transition_id === :join
            @test selection.run_key == "bad_run"
        end
    end

    @testset "fire: join_by returning nothing becomes a selection error" begin
        net = keyed_join_net(join_by=(_, _) -> nothing)
        marking = Marking(Dict(
            :left => Token[batch_token(:left, "batch_1", :a)],
            :right => Token[batch_token(:right, "batch_1", :a)],
        ))
        events = EngineEvent[]
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1, on_event=e -> push!(events, e))
            @test length(results) == 1
            @test results[1].status === :failed
            @test results[1].terminal_reason === :selection_error
            @test occursin("returned nothing", something(results[1].error))
            @test only([e for e in events if e isa SelectionErrored]).transition_id === :join
        end
    end

    @testset "selection errors clear already-available work for the same run" begin
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        state = Peven._scheduler_state(marking)
        bundle = BundleRef(:judge, "r1", nothing, 1)
        record = BundleEnablement(bundle, :ready, Token[batch_token(:ready, "r1", :a)], nothing)

        state.available[1] = record
        state.available_triples[Peven._bundle_triple(bundle)] = [1]
        push!(state.ready, 1)
        push!(state.ready_set, 1)
        run = Peven._get_or_create_run_progress!(state, "r1")
        run.active_guard_errors[1] = Peven._GuardErrorEpisode("old", 1, 1)

        Peven._block_run_for_selection!(
            state,
            Peven._SelectionFailure(:join, "r1", "selector boom"),
        )

        @test isnothing(get(state.available, 1, nothing))
        @test !haskey(state.available_triples, Peven._bundle_triple(bundle))
        @test isempty(state.ready)
        @test isempty(state.ready_set)
        @test isempty(run.active_guard_errors)
        @test !isnothing(run.selection_error)
        @test run.selection_error.transition_id === :join
        @test run.selection_error.message == "selector boom"
    end

    @testset "fire: retries keep the same bundle and firing identity" begin
        attempts = Dict{Any,Int}()
        net = chain_net(retries=1)
        marking = Marking(Dict(:ready => Token[
            batch_token(:ready, "r1", :a, "a"),
            batch_token(:ready, "r1", :b, "b"),
        ]))
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            key = tokens[1].payload.payload
            attempts[key] = get(attempts, key, 0) + 1
            if key == "a" && attempts[key] == 1
                error("transient")
            end
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=2, on_event=e -> push!(events, e))
            started = [e for e in events if e isa TransitionStarted]
            attempts_by_firing = Dict{Int,Vector{Int}}()
            for event in started
                push!(get!(attempts_by_firing, event.firing_id, Int[]), event.attempt)
            end
            observed = sort([sort(attempts) for attempts in values(attempts_by_firing)], by=length)
            @test observed == [[1], [1, 2]]
            @test results[1].status === :completed
        end
    end

    @testset "fire: post-executor commit failures use retry machinery instead of crashing" begin
        attempts = Ref(0)
        net = Net(
            Dict(:ready => Place(:ready), :left => Place(:left), :right => Place(:right)),
            Dict(:judge => Transition(:judge; retries=1)),
            [ArcFrom(:judge, :ready)],
            [ArcTo(:judge, :left), ArcTo(:judge, :right)],
        )
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        events = EngineEvent[]
        executor = FunctionExecutor((_, tokens) -> begin
            attempts[] += 1
            if attempts[] == 1
                return Token[
                    Token(color(tokens[1]), run_key(tokens[1]), "bad_a"),
                    Token(color(tokens[1]), run_key(tokens[1]), "bad_b"),
                ]
            end
            return Token(color(tokens[1]), run_key(tokens[1]), "good")
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1, on_event=e -> push!(events, e))
            @test results[1].status === :completed
            @test length(results[1].trace) == 1
            @test results[1].trace[1].status === :completed
            @test results[1].trace[1].attempts == 2
            @test [e.attempt for e in events if e isa TransitionStarted] == [1, 2]
            failed = only([e for e in events if e isa TransitionFailed])
            @test failed.retrying == true
            @test failed.firing_id == results[1].trace[1].firing_id
            failed_index = only(findall(e -> e isa TransitionFailed && e.retrying, events))
            retry_start_index = only(findall(e -> e isa TransitionStarted && e.attempt == 2, events))
            @test failed_index < retry_start_index
        end
    end

    @testset "fire: executor failure is isolated per bundle but rolls up per run" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[
            batch_token(:ready, "r1", :a, "bad"),
            batch_token(:ready, "r1", :b, "good"),
        ]))
        executor = FunctionExecutor((tid, tokens) -> begin
            tokens[1].payload.payload == "bad" && error("permanent")
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=2)
            @test length(results) == 1
            @test results[1].status === :failed
            @test results[1].terminal_reason === :executor_failed
            @test length(results[1].trace) == 2
            @test Set(step.status for step in results[1].trace) == Set([:completed, :failed])
            failed_step = only([step for step in results[1].trace if step.status === :failed])
            completed_step = only([step for step in results[1].trace if step.status === :completed])
            @test results[1].terminal_bundle == failed_step.bundle
            @test completed_firings(results[1]) == [completed_step]
            @test failed_firings(results[1]) == [failed_step]
            @test firing_status(results[1], completed_step.firing_id) === :completed
            @test firing_result(results[1], failed_step.firing_id) == failed_step
        end
    end

    @testset "fire: fuse-blocked retry restores inputs and closes the firing lifecycle" begin
        net = chain_net(retries=1)
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a, "work")]))
        events = EngineEvent[]
        executor = FunctionExecutor((_, _) -> error("transient"))
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1, fuse=1, on_event=e -> push!(events, e))
            @test results[1].status === :incomplete
            @test results[1].terminal_reason === :fuse_exhausted
            @test length(results[1].trace) == 1
            @test results[1].trace[1].status === :fuse_blocked
            @test results[1].trace[1].bundle == BundleRef(:judge, "r1", nothing, 1)
            @test only(results[1].final_marking.tokens_by_place[:ready]).payload.payload == "work"
            @test isempty(failed_firings(results[1]))
            @test length(fuse_blocked_firings(results[1])) == 1
            failed = only([e for e in events if e isa TransitionFailed])
            @test failed.retrying == false
        end
    end

    @testset "fire: fully blocked runs end as no_enabled_transition" begin
        net = chain_net(guard=_ -> false)
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1)
            @test length(results) == 1
            @test results[1].status === :incomplete
            @test results[1].terminal_reason === :no_enabled_transition
            @test isempty(results[1].trace)
            @test isnothing(results[1].terminal_bundle)
            @test isnothing(results[1].terminal_transition)
            @test only(results[1].final_marking.tokens_by_place[:ready]).payload.payload == :a
        end
    end

    @testset "fire: overlap works across multiple bundles of the same run" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[
            batch_token(:ready, "r1", :a, "a"),
            batch_token(:ready, "r1", :b, "b"),
        ]))
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            sleep(0.05)
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=2, on_event=e -> push!(events, e))
            started = findall(e -> e isa TransitionStarted, events)
            completed = findall(e -> e isa TransitionCompleted, events)
            @test length(results[1].trace) == 2
            @test length(Set(step.firing_id for step in results[1].trace)) == 2
            @test length(started) == 2
            @test length(completed) == 2
            @test started[2] < completed[1]
        end
    end

    @testset "fire: fairness rotates surviving keyed bundles before repeats" begin
        net = keyed_loop_net()
        marking = Marking(Dict(
            :left => Token[
                batch_token(:left, "batch_1", :a),
                batch_token(:left, "batch_1", :b),
                batch_token(:left, "batch_1", :c),
            ],
            :right => Token[
                batch_token(:right, "batch_1", :a),
                batch_token(:right, "batch_1", :b),
                batch_token(:right, "batch_1", :c),
            ],
        ))
        events = EngineEvent[]
        executor = FunctionExecutor((_, tokens) -> Token(:loop, run_key(tokens[1]), (item=item_id(tokens[1]),)))
        with_executor(:default, executor) do
            fire(net, marking; max_concurrency=1, fuse=9, on_event=e -> push!(events, e))
            started = [e.bundle.selected_key for e in events if e isa TransitionStarted]
            @test started == [:a, :b, :c, :a, :b, :c, :a, :b, :c]
        end
    end

    @testset "fire: firing-centric helpers stay unambiguous when the same BundleRef recurs" begin
        net = keyed_loop_net()
        marking = Marking(Dict(
            :left => Token[batch_token(:left, "batch_1", :a)],
            :right => Token[batch_token(:right, "batch_1", :a)],
        ))
        executor = FunctionExecutor((_, tokens) -> Token(:loop, run_key(tokens[1]), (item=item_id(tokens[1]),)))
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1, fuse=2)
            @test length(results[1].trace) == 2
            @test results[1].trace[1].bundle == results[1].trace[2].bundle
            @test results[1].trace[1].firing_id != results[1].trace[2].firing_id
            @test firing_result(results[1], results[1].trace[1].firing_id) == results[1].trace[1]
            @test firing_result(results[1], results[1].trace[2].firing_id) == results[1].trace[2]
            @test firing_status(results[1], 99999) === nothing
        end
    end

    @testset "fire: vector outputs still work" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), run_key(tokens[1]), "a"),
            Token(color(tokens[1]), run_key(tokens[1]), "b"),
        ])
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1)
            @test [token.payload for token in results[1].trace[1].outputs] == ["a", "b"]
            @test [token.payload for token in results[1].final_marking.tokens_by_place[:done]] == ["a", "b"]
        end
    end

    @testset "fire: invalid vector outputs become launched-firing failures" begin
        branch = branch_net()
        weighted = weighted_output_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), run_key(tokens[1]), "a"),
            Token(color(tokens[1]), run_key(tokens[1]), "b"),
        ])
        with_executor(:default, executor) do
            branch_results = fire(branch, marking; max_concurrency=1)
            weighted_results = fire(weighted, marking; max_concurrency=1)
            @test branch_results[1].status === :failed
            @test branch_results[1].terminal_reason === :executor_failed
            @test occursin("exactly one output arc", something(branch_results[1].error))
            @test only(branch_results[1].final_marking.tokens_by_place[:ready]).run_key == "r1"
            @test weighted_results[1].status === :failed
            @test weighted_results[1].terminal_reason === :executor_failed
            @test occursin("weight-1 output arc", something(weighted_results[1].error))
            @test only(weighted_results[1].final_marking.tokens_by_place[:ready]).run_key == "r1"
        end
    end

    @testset "fire: output capacity commit failures restore inputs on terminal failure" begin
        net = Net(
            Dict(:ready => Place(:ready), :done => Place(:done, 1)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :ready)],
            [ArcTo(:judge, :done)],
        )
        marking = Marking(Dict(
            :ready => Token[batch_token(:ready, "r1", :a, "work")],
            :done => Token[Token(:done, "occupied", "full")],
        ))
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = Dict(result.run_key => result for result in fire(net, marking; max_concurrency=1))
            @test results["r1"].status === :failed
            @test results["r1"].terminal_reason === :executor_failed
            @test occursin("capacity exceeded", something(results["r1"].error))
            @test only(results["r1"].final_marking.tokens_by_place[:ready]).payload.payload == "work"
            @test isempty(get(results["r1"].final_marking.tokens_by_place, :done, Token[]))
        end
    end

    @testset "fire: rubric pipeline joins per item regardless of rubric arrival order" begin
        net = rubric_pipeline_net()
        marking = Marking(Dict(:seed => Token[
            batch_token(:seed, "eval_batch_1", :a),
            batch_token(:seed, "eval_batch_1", :b),
            batch_token(:seed, "eval_batch_1", :c),
        ]))
        rng = MersenneTwister(1234)
        delays = Dict(item => 0.01 + 0.01 * rand(rng) for item in [:a, :b, :c])
        executor = FunctionExecutor((tid, tokens) -> begin
            if tid === :tee
                return Token(:seed, run_key(tokens[1]), (item=item_id(tokens[1]),))
            elseif tid === :make_rubric
                sleep(delays[item_id(tokens[1])])
                return Token(:rubric, run_key(tokens[1]), (item=item_id(tokens[1]), rubric=true))
            end
            return Token(:judgment, run_key(tokens[1]), (item=item_id(tokens[1]), judged=true))
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=3)
            done_items = sort([token.payload.item for token in results[1].final_marking.tokens_by_place[:done]])
            judge_bundles = [step.bundle.selected_key for step in results[1].trace if step.bundle.transition_id === :judge]
            @test done_items == [:a, :b, :c]
            @test sort(judge_bundles) == [:a, :b, :c]
        end
    end

    @testset "fire: rubric failure variant still lets sibling keyed bundles finish" begin
        net = rubric_pipeline_net()
        marking = Marking(Dict(:seed => Token[
            batch_token(:seed, "eval_batch_1", :a),
            batch_token(:seed, "eval_batch_1", :b),
            batch_token(:seed, "eval_batch_1", :c),
        ]))
        executor = FunctionExecutor((tid, tokens) -> begin
            if tid === :tee
                return Token(:seed, run_key(tokens[1]), (item=item_id(tokens[1]),))
            elseif tid === :make_rubric
                item_id(tokens[1]) == :b && error("rubric boom")
                return Token(:rubric, run_key(tokens[1]), (item=item_id(tokens[1]), rubric=true))
            end
            return Token(:judgment, run_key(tokens[1]), (item=item_id(tokens[1]), judged=true))
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=3)
            judge_steps = [step for step in results[1].trace if step.bundle.transition_id === :judge]
            rubric_failures = [step for step in results[1].trace if step.bundle.transition_id === :make_rubric && step.status === :failed]
            @test results[1].status === :failed
            @test results[1].terminal_reason === :executor_failed
            @test length(judge_steps) == 2
            @test length(rubric_failures) == 1
            @test sort([step.bundle.selected_key for step in judge_steps]) == [:a, :c]
        end
    end

    @testset "fire: unknown run_keys from executors fail clearly instead of disappearing" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        executor = FunctionExecutor((_, tokens) -> Token(color(tokens[1]), "forked_run", tokens[1].payload))
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1)
            @test results[1].status === :failed
            @test results[1].terminal_reason === :executor_failed
            @test occursin("unknown run_key=forked_run", something(results[1].error))
            @test isempty(get(results[1].final_marking.tokens_by_place, :done, Token[]))
            @test only(results[1].final_marking.tokens_by_place[:ready]).payload.item == :a
            @test length(results[1].trace) == 1
        end
    end

    @testset "fire: on_event hook failures are swallowed" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        seen_started = Ref(false)
        hook(event) = begin
            if event isa TransitionStarted && !seen_started[]
                seen_started[] = true
                error("hook boom")
            end
        end
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1, on_event=hook)
            @test seen_started[]
            @test results[1].status === :completed
            @test length(results[1].trace) == 1
        end
    end

    @testset "fire: interrupts do not wait for pending executors in finally" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        executor = FunctionExecutor((tid, tokens) -> begin
            sleep(0.5)
            passthrough(tid, tokens)
        end)
        hook(event) = event isa TransitionStarted ? throw(InterruptException()) : nothing
        with_executor(:default, executor) do
            task = @async fire(net, marking; max_concurrency=1, on_event=hook)
            @test timedwait(() -> istaskdone(task), 0.2) == :ok
            err = try
                fetch(task)
                nothing
            catch e
                e
            end
            if err isa TaskFailedException
                @test err.task.exception isa InterruptException
            else
                @test err isa InterruptException
            end
            sleep(0.6)
        end
    end

    @testset "fire: interrupting the retry start hook aborts the scheduler" begin
        net = chain_net(retries=1)
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        attempts = Ref(0)
        executor = FunctionExecutor((tid, tokens) -> begin
            attempts[] += 1
            attempts[] == 1 && error("retry me")
            sleep(0.5)
            passthrough(tid, tokens)
        end)
        hook(event) = event isa TransitionStarted && event.attempt == 2 ? throw(InterruptException()) : nothing
        with_executor(:default, executor) do
            task = @async fire(net, marking; max_concurrency=1, on_event=hook)
            @test timedwait(() -> istaskdone(task), 0.2) == :ok
            err = try
                fetch(task)
                nothing
            catch e
                e
            end
            if err isa TaskFailedException
                @test err.task.exception isa InterruptException
            else
                @test err isa InterruptException
            end
            sleep(0.6)
        end
    end

    @testset "fire: invalid scheduler boundary inputs throw" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        with_executor(:default, FunctionExecutor(passthrough)) do
            @test_throws ArgumentError fire(net, marking; max_concurrency=0)
            @test_throws ArgumentError fire(net, marking; fuse=-1)
        end
    end

    @testset "fire: empty markings return no runs without reachability failure" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[]))
        with_executor(:default, FunctionExecutor(passthrough)) do
            @test fire(net, marking; max_concurrency=1) == RunResult{Token}[]
        end
    end

    @testset "fire: run results are ordered lexicographically by run_key" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[
            batch_token(:ready, "r2", :two),
            batch_token(:ready, "r10", :ten),
            batch_token(:ready, "r1", :one),
        ]))
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1)
            @test [result.run_key for result in results] == ["r1", "r10", "r2"]
        end
    end

    @testset "fire: bundle-aware events replay the launched trace" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[
            batch_token(:ready, "r1", :a, "bad"),
            batch_token(:ready, "r1", :b, "good"),
        ]))
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            tokens[1].payload.payload == "bad" && error("permanent")
            passthrough(tid, tokens)
        end)
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=2, on_event=e -> push!(events, e))
            trace_projection = sort(
                [(step.bundle, step.firing_id, step.status, step.attempts) for step in results[1].trace],
                by=x -> x[2],
            )
            event_projection = sort(vcat(
                [(e.bundle, e.firing_id, :completed, e.attempt) for e in events if e isa TransitionCompleted],
                [(e.bundle, e.firing_id, :failed, e.attempt) for e in events if e isa TransitionFailed && !e.retrying],
            ); by=x -> x[2])
            @test trace_projection == event_projection
            finished = only([e for e in events if e isa RunFinished])
            @test finished.result.terminal_reason === :executor_failed
            @test finished.result.terminal_bundle == results[1].terminal_bundle
        end
    end
end

end
