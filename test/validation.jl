module SrcValidationTests

using Test
using Main.Peven

function toy_net(; extra_transitions=Dict{Symbol,Transition}())
    places = Dict(
        :ready => Place(:ready, 2),
        :done => Place(:done),
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

    @testset "keyed-join structure" begin
        join_by = (pid, token) -> token.payload
        net = Net(
            Dict(:left => Place(:left), :right => Place(:right), :done => Place(:done)),
            Dict(:join => Transition(:join; join_by=join_by)),
            [ArcFrom(:join, :left), ArcFrom(:join, :right)],
            [ArcTo(:join, :done)],
        )

        @test isempty(validate(net))
    end

    @testset "duplicate input arcs are reported by validate" begin
        join_by = (pid, token) -> token.payload
        net = Net(
            Dict(:left => Place(:left), :done => Place(:done)),
            Dict(:join => Transition(:join; join_by=join_by)),
            [ArcFrom(:join, :left), ArcFrom(:join, :left, 2)],
            [ArcTo(:join, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :duplicate_input_arc && i.object_id === :join, issues)
        @test any(i -> i.code === :invalid_keyed_join && i.object_id === :join, issues)
    end

    @testset "invalid keyed joins with fewer than two unique inputs are reported by validate" begin
        join_by = (pid, token) -> token.payload
        net = Net(
            Dict(:left => Place(:left), :done => Place(:done)),
            Dict(:join => Transition(:join; join_by=join_by)),
            [ArcFrom(:join, :left)],
            [ArcTo(:join, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :invalid_keyed_join && i.object_id === :join, issues)
    end

    @testset "arc weights that exceed place capacity are reported" begin
        net = Net(
            Dict(:ready => Place(:ready, 1), :done => Place(:done, 1)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :ready, 2)],
            [ArcTo(:judge, :done)],
        )

        issues = validate(net)
        @test count(i -> i.code === :weight_exceeds_capacity, issues) == 1
    end

    @testset "duplicate input arcs are aggregated before capacity validation" begin
        net = Net(
            Dict(:ready => Place(:ready, 1), :done => Place(:done, 1)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :ready), ArcFrom(:judge, :ready)],
            [ArcTo(:judge, :done)],
        )

        issues = validate(net)
        @test count(i -> i.code === :weight_exceeds_capacity, issues) == 1
        @test any(i -> i.code === :duplicate_input_arc && i.object_id === :judge, issues)
    end

    @testset "duplicate output arcs are reported by validate" begin
        net = Net(
            Dict(:ready => Place(:ready), :done => Place(:done)),
            Dict(:judge => Transition(:judge)),
            [ArcFrom(:judge, :ready)],
            [ArcTo(:judge, :done), ArcTo(:judge, :done)],
        )

        issues = validate(net)
        @test any(i -> i.code === :duplicate_output_arc && i.object_id === :judge, issues)
    end
end

end
