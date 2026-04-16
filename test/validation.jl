module SrcValidationTests

using Test
using Main.Peven

function toy_net(; extra_transitions=Dict{Symbol, Transition}())
    places = Dict(
        :ready => Place(:ready, 2),
        :done  => Place(:done),
    )
    transitions = Dict(:judge => Transition(:judge))
    merge!(transitions, extra_transitions)
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

@testset "src/validation" begin
    @testset "valid net and state" begin
        net = toy_net()
        marking = Marking(Dict(
            :ready => Token[Token(:redteam, "r1", 1)],
        ))

        @test isempty(validate(net))
        @test isempty(validate(net, marking))
    end

    @testset "key mismatch" begin
        net = Net(
            Dict(:ready => Place(:other)),
            Dict(:judge => Transition(:judge)),
            ArcFrom[],
            ArcTo[],
        )

        issues = validate(net)
        @test any(i -> i.code === :key_mismatch && i.object_id === :ready, issues)
    end

    @testset "arc endpoint checks" begin
        net = Net(
            Dict(:ready => Place(:ready)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:ghost, :missing)],
            [ArcTo(:phantom, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :unknown_place && i.object_id === :missing, issues)
        @test any(i -> i.code === :unknown_transition && i.object_id === :ghost, issues)
        @test any(i -> i.code === :unknown_place && i.object_id === :done, issues)
        @test any(i -> i.code === :unknown_transition && i.object_id === :phantom, issues)
    end

    @testset "marking checks accumulate" begin
        net = toy_net()
        marking = Marking(Dict(
            :ghost => Token[Token(:redteam, "r1", 1)],
            :ready => Token[Token(:redteam, "r1", 1), Token(:baseline, "r2", 2), Token(:gold, "r3", 3)],
        ))

        issues = validate(net, marking)
        @test any(i -> i.code === :unknown_place && i.object_id === :ghost, issues)
        @test any(i -> i.code === :capacity_exceeded && i.object_id === :ready, issues)
    end

    @testset "orphan places" begin
        net = Net(
            Dict(:ready => Place(:ready), :lonely => Place(:lonely), :done => Place(:done)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :ready)],
            [ArcTo(:judge, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :orphan_place && i.object_id === :lonely, issues)
        # :ready and :done have arcs, so they should not be flagged
        @test !any(i -> i.code === :orphan_place && i.object_id === :ready, issues)
        @test !any(i -> i.code === :orphan_place && i.object_id === :done, issues)
    end

    @testset "reachability" begin
        net = toy_net(extra_transitions=Dict(:score => Transition(:score)))
        marking = Marking(Dict(
            :ready => Token[Token(:redteam, "r1", 1)],
        ))

        issues = validate(net, marking)
        @test any(i -> i.code === :unreachable_transition && i.object_id === :score, issues)
    end
end

end
