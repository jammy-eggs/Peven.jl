module ReadmeTests

using Test
using Main.Peven

function with_readme_executors(f)
    register_executor!(:agent, FunctionExecutor((_, tokens) ->
        Token(:response, run_key(tokens[1]), string(tokens[1].payload, " -> draft"))
    ))
    register_executor!(:judge, FunctionExecutor((_, tokens) ->
        Token(:score, run_key(tokens[1]), length(String(tokens[1].payload)))
    ))
    try
        f()
    finally
        delete!(Peven.EXECUTOR_REGISTRY, :agent)
        delete!(Peven.EXECUTOR_REGISTRY, :judge)
    end
end

@testset "README quickstart" begin
    places = Dict(
        :prompt => Place(:prompt),
        :response => Place(:response),
        :scored => Place(:scored),
    )

    transitions = Dict(
        :gen => Transition(:gen, :agent),
        :jdg => Transition(:jdg, :judge),
    )

    arcsfrom = [ArcFrom(:gen, :prompt), ArcFrom(:jdg, :response)]
    arcsto = [ArcTo(:gen, :response), ArcTo(:jdg, :scored)]
    net = Net(places, transitions, arcsfrom, arcsto)

    marking = Marking(Dict(
        :prompt => Token[Token(:default, "run1", "Write about testing")],
    ))

    issues = validate(net, marking)
    @test isempty(issues)

    with_readme_executors() do
        results = fire(net, marking; max_concurrency=1)
        @test length(results) == 1
        @test results[1].status === :completed
        @test length(results[1].final_marking.tokens_by_place[:scored]) == 1
    end
end

end
