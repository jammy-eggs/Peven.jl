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

function optional_advice_net()
    places = Dict(:ready => Place(:ready), :planner_advice => Place(:planner_advice), :done => Place(:done))
    transitions = Dict(:judge => Transition(:judge))
    arcsfrom = [ArcFrom(:judge, :ready), ArcFrom(:judge, :planner_advice; optional=true)]
    arcsto = [ArcTo(:judge, :done)]
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
        unregister_executor!(name)
    end
end

mutable struct RecordingExecutor <: AbstractExecutor
    contexts::Vector{Any}
end

Peven.execute(executor::RecordingExecutor, ctx::ExecutionContext) = begin
    push!(executor.contexts, ctx)
    passthrough(ctx.transition_id, ctx.tokens)
end

passthrough(tid, tokens) = Token(color(tokens[1]), run_key(tokens[1]), (item=item_id(tokens[1]), transition=tid))

queued_ready_ids(state) = Int[
    id for (id, version) in state.ready[state.ready_head:end]
    if id in state.ready_set && get(state.ready_versions, id, 0) == version
]

@testset "src/engine" begin
    @testset "drop and misfire basics" begin
        net = chain_net()
        marking = Marking(Dict(
            :ready => Token[Token(:red, "r1", 1)],
            :untouched => Token[Token(:blue, "r1", "keep")],
        ))
        output = Token(:red, "r1", "result")
        new_marking = drop(marking, net, :judge, output)
        @test new_marking.tokens_by_place[:done][1].payload == "result"
        push!(new_marking.tokens_by_place[:untouched], Token(:blue, "r1", "new"))
        @test length(marking.tokens_by_place[:untouched]) == 1

        restored = misfire(Marking(Dict(:ready => Token[])), Dict(:ready => Token[Token("r1")]))
        @test length(restored.tokens_by_place[:ready]) == 1
    end

    @testset "drop many outputs keeps vector semantics" begin
        net = chain_net()
        outputs = Token[Token(:red, "r1", "a"), Token(:red, "r1", "b")]
        marking = drop(Marking(), net, :judge, outputs)
        @test [token.payload for token in marking.tokens_by_place[:done]] == ["a", "b"]
    end

    @testset "drop explicit per-place outputs routes tokens separately" begin
        net = branch_net()
        outputs = Dict(
            :left => Token[Token(:red, "r1", "a")],
            :right => Token[Token(:red, "r1", "b")],
        )
        marking = drop(Marking(), net, :judge, outputs)
        @test [token.payload for token in marking.tokens_by_place[:left]] == ["a"]
        @test [token.payload for token in marking.tokens_by_place[:right]] == ["b"]
    end

    @testset "run_keys ordering" begin
        marking = Marking(Dict(
            :a => Token[Token(:red, "r2", 1), Token(:blue, "r10", 2)],
            :b => Token[Token(:red, "r1", 3)],
        ))
        @test sort(run_keys(marking)) == ["r1", "r10", "r2"]
    end

    @testset "fire ignores ordinary on_event hook errors" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        with_executor(:default, FunctionExecutor(passthrough)) do
            results = fire(net, marking; max_concurrency=1, on_event=_ -> error("boom"))
            @test length(results) == 1
            @test results[1].status === :completed
        end
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
        @test [state.available[id].bundle.selected_key for id in queued_ready_ids(state)] == [:a, :b]

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
        @test [state.available[id].bundle.selected_key for id in queued_ready_ids(state)] == [:a, :b, :c]
    end

    @testset "ready queue re-enabled bundles append at the tail" begin
        state = Peven._scheduler_state(Marking())
        first = BundleEnablement(BundleRef(:join, "r1", :a, 1), :ready, Token[], nothing)
        second = BundleEnablement(BundleRef(:join, "r1", :b, 1), :ready, Token[], nothing)
        state.available[1] = first
        state.available[2] = second

        Peven._set_ready_membership!(state, 1, true)
        Peven._set_ready_membership!(state, 2, true)
        Peven._set_ready_membership!(state, 1, false)
        Peven._set_ready_membership!(state, 1, true)

        @test Peven._pop_ready!(state) == 2
        @test Peven._pop_ready!(state) == 1
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

    @testset "optional inputs: absent tokens do not block required bundles" begin
        net = optional_advice_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a, "work")]))

        entries = enablement(net, marking)
        @test length(entries) == 1
        @test entries[1].status === :ready
        @test [token.payload.payload for token in entries[1].inputs] == ["work"]

        _, grabbed, grabbed_by_place = take(marking, net, entries[1].bundle)
        @test [token.payload.payload for token in grabbed] == ["work"]
        @test [token.payload.payload for token in grabbed_by_place[:ready]] == ["work"]
        @test isempty(grabbed_by_place[:planner_advice])
    end

    @testset "optional inputs: present tokens are consumed and passed" begin
        net = optional_advice_net()
        marking = Marking(Dict(
            :ready => Token[batch_token(:ready, "r1", :a, "work")],
            :planner_advice => Token[batch_token(:planner_advice, "r1", :a, "advice")],
        ))

        entries = enablement(net, marking)
        @test length(entries) == 1
        @test [token.payload.payload for token in entries[1].inputs] == ["work", "advice"]

        new_marking, grabbed, grabbed_by_place = take(marking, net, entries[1].bundle)
        @test [token.payload.payload for token in grabbed] == ["work", "advice"]
        @test [token.payload.payload for token in grabbed_by_place[:planner_advice]] == ["advice"]
        @test isempty(new_marking.tokens_by_place[:ready])
        @test isempty(new_marking.tokens_by_place[:planner_advice])
    end

    @testset "optional inputs: required input absent still blocks" begin
        net = optional_advice_net()
        marking = Marking(Dict(:planner_advice => Token[batch_token(:planner_advice, "r1", :a, "advice")]))

        @test isempty(hot(net, marking))
        @test grab(marking, net, BundleRef(:judge, "r1", nothing, 1)) === nothing
    end

    @testset "optional inputs: stale optional token does not stale the bundle" begin
        net = optional_advice_net()
        initial = Marking(Dict(
            :ready => Token[batch_token(:ready, "r1", :a, "work")],
            :planner_advice => Token[batch_token(:planner_advice, "r1", :a, "advice")],
        ))
        bundle = only(hot(net, initial))
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a, "work")]))

        result = grab(marking, net, bundle)
        @test result !== nothing
        _, grabbed, grabbed_by_place = something(result)
        @test [token.payload.payload for token in grabbed] == ["work"]
        @test isempty(grabbed_by_place[:planner_advice])
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
            @test collect(keys(completed.outputs)) == [:done]
            @test completed.outputs == results[1].trace[1].outputs
        end
    end

    @testset "fire: public ExecutionContext carries firing metadata" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a, "work")]))
        events = EngineEvent[]
        executor = RecordingExecutor(Any[])
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1, on_event=e -> push!(events, e))
            started = only([e for e in events if e isa TransitionStarted])
            step = only(results[1].trace)
            ctx = only(executor.contexts)

            @test ctx.transition_id === :judge
            @test ctx.bundle == BundleRef(:judge, "r1", nothing, 1)
            @test ctx.bundle == started.bundle == step.bundle
            @test ctx.firing_id == started.firing_id == step.firing_id
            @test ctx.attempt == started.attempt == 1
            @test ctx.tokens === started.inputs
            @test ctx.inputs_by_place === started.inputs_by_place
            @test length(ctx.tokens) == 1
            @test ctx.tokens[1].payload.payload == "work"
            @test collect(keys(ctx.inputs_by_place)) == [:ready]
            @test length(ctx.inputs_by_place[:ready]) == 1
            @test ctx.inputs_by_place[:ready][1].payload.payload == "work"
        end
    end

    @testset "fire: public input grouping is exposed by place" begin
        net = keyed_join_net()
        marking = Marking(Dict(
            :left => Token[batch_token(:left, "r1", :a, "left-work")],
            :right => Token[batch_token(:right, "r1", :a, "right-work")],
        ))
        events = EngineEvent[]
        executor = RecordingExecutor(Any[])
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1, on_event=e -> push!(events, e))
            started = only([e for e in events if e isa TransitionStarted])
            step = only(results[1].trace)
            ctx = only(executor.contexts)

            @test ctx.bundle == BundleRef(:join, "r1", :a, 1)
            @test ctx.bundle == started.bundle == step.bundle
            @test sort(collect(keys(ctx.inputs_by_place))) == [:left, :right]
            @test ctx.inputs_by_place === started.inputs_by_place
            @test [token.payload.payload for token in ctx.inputs_by_place[:left]] == ["left-work"]
            @test [token.payload.payload for token in ctx.inputs_by_place[:right]] == ["right-work"]
        end
    end

    @testset "fire: optional input grouping records absent places" begin
        net = optional_advice_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a, "work")]))
        events = EngineEvent[]
        executor = RecordingExecutor(Any[])
        with_executor(:default, executor) do
            results = fire(net, marking; max_concurrency=1, on_event=e -> push!(events, e))
            started = only([e for e in events if e isa TransitionStarted])
            ctx = only(executor.contexts)

            @test results[1].status === :completed
            @test sort(collect(keys(ctx.inputs_by_place))) == [:planner_advice, :ready]
            @test ctx.inputs_by_place === started.inputs_by_place
            @test [token.payload.payload for token in ctx.inputs_by_place[:ready]] == ["work"]
            @test isempty(ctx.inputs_by_place[:planner_advice])
        end
    end

    @testset "fire: optional inputs can be re-emitted as ordinary tokens" begin
        net = Net(
            Dict(:ready => Place(:ready), :planner_advice => Place(:planner_advice), :done => Place(:done)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :ready), ArcFrom(:judge, :planner_advice; optional=true)],
            [ArcTo(:judge, :planner_advice), ArcTo(:judge, :done)],
        )
        marking = Marking(Dict(
            :ready => Token[batch_token(:ready, "r1", :a, "work")],
            :planner_advice => Token[batch_token(:planner_advice, "r1", :a, "advice")],
        ))
        executor = FunctionExecutor((_, tokens) -> Dict(
            :planner_advice => Token[token for token in tokens if color(token) === :planner_advice],
            :done => Token[Token(:done, run_key(tokens[1]), "done")],
        ))

        with_executor(:default, executor) do
            events = EngineEvent[]
            results = fire(net, marking; max_concurrency=1, on_event=e -> push!(events, e))
            started = only([e for e in events if e isa TransitionStarted])
            @test results[1].status === :completed
            @test [token.payload.payload for token in started.inputs_by_place[:planner_advice]] == ["advice"]
            @test [token.payload.payload for token in results[1].final_marking.tokens_by_place[:planner_advice]] == ["advice"]
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
        Peven._set_ready_membership!(state, 1, true)
        run = Peven._get_or_create_run_progress!(state, "r1")
        run.active_guard_errors[1] = Peven._GuardErrorEpisode("old", 1, 1)

        Peven._block_run_for_selection!(
            state,
            Peven._SelectionFailure(:join, "r1", "selector boom"),
        )

        @test isnothing(get(state.available, 1, nothing))
        @test !haskey(state.available_triples, Peven._bundle_triple(bundle))
        @test isnothing(Peven._pop_ready!(state))
        @test isempty(state.ready_set)
        @test isempty(run.active_guard_errors)
        @test !isnothing(run.selection_error)
        @test run.selection_error.transition_id === :join
        @test run.selection_error.message == "selector boom"
    end

    @testset "fire: claim-time selector exceptions emit SelectionErrored and spare sibling runs" begin
        selector = (pid, token) -> getproperty(token.payload, :item)
        net = Net(
            Dict(
                :seed => Place(:seed),
                :left => Place(:left),
                :right => Place(:right),
                :done => Place(:done),
            ),
            Dict(
                :a_poison => Transition(:a_poison),
                :join => Transition(:join; join_by=selector),
            ),
            [
                ArcFrom(:a_poison, :seed),
                ArcFrom(:join, :left),
                ArcFrom(:join, :right),
            ],
            [
                ArcTo(:a_poison, :left),
                ArcTo(:join, :done),
            ],
        )
        marking = Marking(Dict(
            :seed => Token[Token(:seed, "bad_run", (seed=true,))],
            :left => Token[
                Token(:left, "bad_run", (item=:ok, left=1)),
                Token(:left, "good_run", (item=:ok, left=2)),
            ],
            :right => Token[
                Token(:right, "bad_run", (item=:ok, right=10)),
                Token(:right, "good_run", (item=:ok, right=20)),
            ],
        ))
        events = EngineEvent[]
        executor = FunctionExecutor((tid, tokens) -> begin
            if tid === :a_poison
                return Token(:left, run_key(tokens[1]), (payload=1,))
            end
            left, right = tokens
            return Token(:done, run_key(left), (item=left.payload.item, total=left.payload.left + right.payload.right))
        end)
        with_executor(:default, executor) do
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
            @test results["good_run"].status === :completed
            selection = only([e for e in events if e isa SelectionErrored])
            @test selection.transition_id === :join
            @test selection.run_key == "bad_run"
        end
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
            return Dict(
                :left => Token[Token(color(tokens[1]), run_key(tokens[1]), "good_left")],
                :right => Token[Token(color(tokens[1]), run_key(tokens[1]), "good_right")],
            )
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
        executor = FunctionExecutor((_, tokens) -> Dict(
            :left => Token[Token(:loop, run_key(tokens[1]), (item=item_id(tokens[1]),))],
            :right => Token[Token(:loop, run_key(tokens[1]), (item=item_id(tokens[1]),))],
        ))
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
        executor = FunctionExecutor((_, tokens) -> Dict(
            :left => Token[Token(:loop, run_key(tokens[1]), (item=item_id(tokens[1]),))],
            :right => Token[Token(:loop, run_key(tokens[1]), (item=item_id(tokens[1]),))],
        ))
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
            @test [token.payload for token in results[1].trace[1].outputs[:done]] == ["a", "b"]
            @test [token.payload for token in results[1].final_marking.tokens_by_place[:done]] == ["a", "b"]
        end
    end

    @testset "fire: explicit per-place outputs drive branched transitions" begin
        branch = branch_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Dict(
            :left => Token[Token(color(tokens[1]), run_key(tokens[1]), "a")],
            :right => Token[Token(color(tokens[1]), run_key(tokens[1]), "b")],
        ))
        with_executor(:default, executor) do
            branch_results = fire(branch, marking; max_concurrency=1)
            @test branch_results[1].status === :completed
            @test [token.payload for token in branch_results[1].trace[1].outputs[:left]] == ["a"]
            @test [token.payload for token in branch_results[1].trace[1].outputs[:right]] == ["b"]
            @test [token.payload for token in branch_results[1].final_marking.tokens_by_place[:left]] == ["a"]
            @test [token.payload for token in branch_results[1].final_marking.tokens_by_place[:right]] == ["b"]
        end
    end

    @testset "fire: branched transitions reject unplaced vector outputs" begin
        branch = branch_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))
        executor = FunctionExecutor((_, tokens) -> Token[
            Token(color(tokens[1]), run_key(tokens[1]), "a"),
            Token(color(tokens[1]), run_key(tokens[1]), "b"),
        ])
        with_executor(:default, executor) do
            branch_results = fire(branch, marking; max_concurrency=1)
            @test branch_results[1].status === :failed
            @test branch_results[1].terminal_reason === :executor_failed
            @test occursin("must be keyed by destination place", something(branch_results[1].error))
            @test only(branch_results[1].final_marking.tokens_by_place[:ready]).run_key == "r1"
        end
    end

    @testset "fire: branched transitions reject malformed per-place outputs" begin
        branch = branch_net()
        marking = Marking(Dict(:ready => Token[Token("r1")]))

        cases = [
            (
                "missing destination",
                FunctionExecutor((_, tokens) -> Dict(
                    :left => Token[Token(color(tokens[1]), run_key(tokens[1]), "a")],
                )),
                "must provide outputs for exactly",
            ),
            (
                "extra destination",
                FunctionExecutor((_, tokens) -> Dict(
                    :left => Token[Token(color(tokens[1]), run_key(tokens[1]), "a")],
                    :right => Token[Token(color(tokens[1]), run_key(tokens[1]), "b")],
                    :ghost => Token[Token(color(tokens[1]), run_key(tokens[1]), "c")],
                )),
                "unknown output place",
            ),
            (
                "non-symbol key",
                FunctionExecutor((_, tokens) -> Dict{Any,Any}(
                    "left" => Token[Token(color(tokens[1]), run_key(tokens[1]), "a")],
                    :right => Token[Token(color(tokens[1]), run_key(tokens[1]), "b")],
                )),
                "keys must be destination place Symbols",
            ),
            (
                "nothing output",
                FunctionExecutor((_, _) -> nothing),
                "returned nothing",
            ),
        ]

        for (label, executor, needle) in cases
            with_executor(:default, executor) do
                results = fire(branch, marking; max_concurrency=1)
                @testset "$label" begin
                    @test results[1].status === :failed
                    @test results[1].terminal_reason === :executor_failed
                    @test occursin(needle, something(results[1].error))
                    @test only(results[1].final_marking.tokens_by_place[:ready]).run_key == "r1"
                end
            end
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

    @testset "fire: commit-phase failures do not block sibling bundles in the same run" begin
        net = Net(
            Dict(
                :bad_ready => Place(:bad_ready),
                :good_ready => Place(:good_ready),
                :done_full => Place(:done_full, 1),
                :done_ok => Place(:done_ok),
            ),
            Dict(
                :a_bad => Transition(:a_bad),
                :z_good => Transition(:z_good),
            ),
            [
                ArcFrom(:a_bad, :bad_ready),
                ArcFrom(:z_good, :good_ready),
            ],
            [
                ArcTo(:a_bad, :done_full),
                ArcTo(:z_good, :done_ok),
            ],
        )
        marking = Marking(Dict(
            :bad_ready => Token[Token(:bad_ready, "r1", (label=:bad,))],
            :good_ready => Token[Token(:good_ready, "r1", (label=:good,))],
            :done_full => Token[Token(:done_full, "occupied", (full=true,))],
        ))
        executor = FunctionExecutor((tid, tokens) -> begin
            tid === :a_bad && return Token(:done_full, run_key(tokens[1]), (label=:bad,))
            return Token(:done_ok, run_key(tokens[1]), (label=:good,))
        end)
        with_executor(:default, executor) do
            result = only([item for item in fire(net, marking; max_concurrency=1) if item.run_key == "r1"])
            @test result.status === :failed
            @test result.terminal_reason === :executor_failed
            @test [step.bundle.transition_id for step in result.trace] == [:a_bad, :z_good]
            @test [step.status for step in result.trace] == [:failed, :completed]
            @test only(result.final_marking.tokens_by_place[:bad_ready]).payload.label === :bad
            @test only(result.final_marking.tokens_by_place[:done_ok]).payload.label === :good
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
                item = item_id(tokens[1])
                return Dict(
                    :problem => Token[Token(:seed, run_key(tokens[1]), (item=item,))],
                    :rubric_seed => Token[Token(:seed, run_key(tokens[1]), (item=item,))],
                )
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
                item = item_id(tokens[1])
                return Dict(
                    :problem => Token[Token(:seed, run_key(tokens[1]), (item=item,))],
                    :rubric_seed => Token[Token(:seed, run_key(tokens[1]), (item=item,))],
                )
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

    @testset "fire: on_event_error=:throw rethrows hook failures" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        hook(event) = event isa TransitionStarted ? error("hook boom") : nothing
        with_executor(:default, FunctionExecutor(passthrough)) do
            err = try
                fire(net, marking; max_concurrency=1, on_event=hook, on_event_error=:throw)
                nothing
            catch e
                e
            end
            @test err isa ErrorException
            @test err.msg == "hook boom"
        end
    end

    @testset "fire: invalid on_event_error throws" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        @test_throws ArgumentError fire(net, marking; max_concurrency=1, on_event_error=:boom)
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

    @testset "fire: optional-only transitions fail validation preflight" begin
        net = Net(
            Dict(:planner_advice => Place(:planner_advice), :done => Place(:done)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :planner_advice; optional=true)],
            [ArcTo(:judge, :done)],
        )
        marking = Marking(Dict(:planner_advice => Token[batch_token(:planner_advice, "r1", :a, "advice")]))

        with_executor(:default, FunctionExecutor(passthrough)) do
            @test_throws ArgumentError fire(net, marking; max_concurrency=1)
        end
    end

    @testset "fire: keyed joins with optional arcs fail validation preflight" begin
        net = Net(
            Dict(:left => Place(:left), :right => Place(:right), :done => Place(:done)),
            Dict(:join => Transition(:join; join_by=(pid, token) -> token.payload.item)),
            [ArcFrom(:join, :left), ArcFrom(:join, :right; optional=true)],
            [ArcTo(:join, :done)],
        )
        marking = Marking(Dict(
            :left => Token[batch_token(:left, "r1", :a)],
            :right => Token[batch_token(:right, "r1", :a)],
        ))

        with_executor(:default, FunctionExecutor(passthrough)) do
            @test_throws ArgumentError fire(net, marking; max_concurrency=1)
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

    @testset "unregister_executor!" begin
        name = :unregister_test
        executor = FunctionExecutor(passthrough)
        @test unregister_executor!(name) == false
        register_executor!(name, executor)
        @test unregister_executor!(name) == true
        @test unregister_executor!(name) == false
    end

    @testset "fire: injected executors bypass the global registry" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))

        executors = Dict{Symbol,AbstractExecutor}(:default => FunctionExecutor(passthrough))
        results = fire(net, marking; max_concurrency=1, executors=executors)
        @test length(results) == 1
        @test results[1].status === :completed
    end

    @testset "fire: missing injected executor surfaces as an executor failure" begin
        net = chain_net()
        marking = Marking(Dict(:ready => Token[batch_token(:ready, "r1", :a)]))
        executors = Dict{Symbol,AbstractExecutor}()
        results = fire(net, marking; max_concurrency=1, executors=executors)
        @test length(results) == 1
        @test results[1].status === :failed
        @test results[1].terminal_reason === :executor_failed
    end
end

end
