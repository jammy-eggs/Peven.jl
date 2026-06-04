module SrcValidationTests

using Test
using Main.Peven

function toyNet(; extraTransitions=Dict{Symbol, Transition}())
    places = Dict(
        :ready => Place(:ready, 2),
        :done  => Place(:done),
    )
    transitions = Dict(:judge => Transition(:judge))
    merge!(transitions, extraTransitions)
    arcsfrom = [ArcFrom(:judge, :ready)]
    arcsto = [ArcTo(:judge, :done)]
    return Net(places, transitions, arcsfrom, arcsto)
end

@testset "src/validation" begin
    @testset "valid net and state" begin
        net = toyNet()
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
        @test any(i -> i.code === :keyMismatch && i.objectId === :ready, issues)
    end

    @testset "arc endpoint checks" begin
        net = Net(
            Dict(:ready => Place(:ready)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:ghost, :missing)],
            [ArcTo(:phantom, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :unknownPlace && i.objectId === :missing, issues)
        @test any(i -> i.code === :unknownTransition && i.objectId === :ghost, issues)
        @test any(i -> i.code === :unknownPlace && i.objectId === :done, issues)
        @test any(i -> i.code === :unknownTransition && i.objectId === :phantom, issues)
    end

    @testset "marking checks accumulate" begin
        net = toyNet()
        marking = Marking(Dict(
            :ghost => Token[Token(:redteam, "r1", 1)],
            :ready => Token[Token(:redteam, "r1", 1), Token(:baseline, "r2", 2), Token(:gold, "r3", 3)],
        ))

        issues = validate(net, marking)
        @test any(i -> i.code === :unknownPlace && i.objectId === :ghost, issues)
        @test any(i -> i.code === :capacityExceeded && i.objectId === :ready, issues)
    end

    @testset "orphan places" begin
        net = Net(
            Dict(:ready => Place(:ready), :lonely => Place(:lonely), :done => Place(:done)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :ready)],
            [ArcTo(:judge, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :orphanPlace && i.objectId === :lonely, issues)
        # :ready and :done have arcs, so they should not be flagged
        @test !any(i -> i.code === :orphanPlace && i.objectId === :ready, issues)
        @test !any(i -> i.code === :orphanPlace && i.objectId === :done, issues)
    end

    @testset "reachability" begin
        net = Net(
            Dict(
                :ready => Place(:ready, 2),
                :done => Place(:done),
                :other => Place(:other),
                :sink => Place(:sink),
            ),
            Dict(:judge => Transition(:judge), :score => Transition(:score)),
            [ArcFrom(:judge, :ready), ArcFrom(:score, :other)],
            [ArcTo(:judge, :done), ArcTo(:score, :sink)],
        )
        marking = Marking(Dict(
            :ready => Token[Token(:redteam, "r1", 1)],
        ))

        issues = validate(net, marking)
        @test any(i -> i.code === :unreachableTransition && i.objectId === :score, issues)
    end

    @testset "transitions with no input arcs are invalid" begin
        net = Net(
            Dict(:done => Place(:done)),
            Dict(:judge => Transition(:judge)),
            ArcFrom[],
            [ArcTo(:judge, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :missingInputArc && i.objectId === :judge, issues)
    end

    @testset "duplicate input arcs are invalid" begin
        net = Net(
            Dict(:ready => Place(:ready), :done => Place(:done)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :ready), ArcFrom(:judge, :ready)],
            [ArcTo(:judge, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :duplicateInputArc && i.objectId === :judge, issues)
    end

    @testset "duplicate output arcs are invalid" begin
        net = Net(
            Dict(:ready => Place(:ready), :done => Place(:done)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :ready)],
            [ArcTo(:judge, :done), ArcTo(:judge, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :duplicateOutputArc && i.objectId === :judge, issues)
    end
end

end
