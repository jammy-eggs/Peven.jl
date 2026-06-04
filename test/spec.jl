module SrcSpecTests

using Test
using Main.Peven

@testset "src/spec" begin
    @testset "Place" begin
        place = Place(:ready)
        @test place.id === :ready
        @test isnothing(place.capacity)

        bounded = Place(:done, 2)
        @test bounded.id === :done
        @test bounded.capacity == 2

        @test_throws ArgumentError Place(:invalid, 0)
        @test_throws ArgumentError Place(:invalid, -1)
    end

    @testset "Transition" begin
        t = Transition(:score)
        @test t.id === :score
        @test t.executor === :default
        @test isnothing(t.guard)
        @test t.retries == 0
        @test isnothing(t.joinBy)

        t2 = Transition(:judge, :agent; retries=2)
        @test t2.executor === :agent
        @test t2.retries == 2

        guardFn = tokens -> length(tokens) > 0
        joinFn = (pid, token) -> token.payload.item
        t3 = Transition(:gate, :default; guard=guardFn, joinBy=joinFn)
        @test t3.guard === guardFn
        @test t3.joinBy === joinFn

        @test_throws ArgumentError Transition(:bad; retries=-1)
    end

    @testset "Bundle" begin
        a = Bundle(:judge, "r1", :item1)
        b = Bundle(:judge, "r1", :item1)
        c = Bundle(:judge, "r1", :item2)

        @test a == b
        @test a != c
        @test length(Set([a, b, c])) == 2
    end

    @testset "ArcFrom" begin
        arc = ArcFrom(:judge, :ready)
        @test arc.transition === :judge
        @test arc.from === :ready
        @test arc.weight == 1
        @test arc.optional == false

        weighted = ArcFrom(:judge, :ready, 3)
        @test weighted.weight == 3
        @test weighted.optional == false

        optional = ArcFrom(:judge, :advice; optional=true)
        @test optional.weight == 1
        @test optional.optional == true

        weightedOptional = ArcFrom(:judge, :advice, 2; optional=true)
        @test weightedOptional.weight == 2
        @test weightedOptional.optional == true

        @test_throws ArgumentError ArcFrom(:judge, :ready, 0)
        @test_throws ArgumentError ArcFrom(:judge, :ready, -1)
    end

    @testset "ArcTo" begin
        arc = ArcTo(:judge, :done)
        @test arc.transition === :judge
        @test arc.to === :done
        @test arc.weight == 1

        weighted = ArcTo(:judge, :done, 2)
        @test weighted.weight == 2

        @test_throws ArgumentError ArcTo(:judge, :done, 0)
        @test_throws ArgumentError ArcTo(:judge, :done, -1)
    end

    @testset "Net" begin
        places = Dict(
            :ready => Place(:ready),
            :done  => Place(:done, 3),
        )
        transitions = Dict(:judge => Transition(:judge))
        arcsfrom = [ArcFrom(:judge, :ready)]
        arcsto = [ArcTo(:judge, :done)]

        net = Net(places, transitions, arcsfrom, arcsto)

        @test net.places === places
        @test net.transitions === transitions
        @test net.arcsfrom === arcsfrom
        @test net.arcsto === arcsto
        @test net.children[:ready] == [:judge]
        @test net.children[:judge] == [:done]
        @test net.fromPlaces == Set([:ready])
    end

    @testset "Net cached arc indexes" begin
        places = Dict(:a => Place(:a), :b => Place(:b), :c => Place(:c))
        transitions = Dict(:t1 => Transition(:t1), :t2 => Transition(:t2))
        arcsfrom = [ArcFrom(:t1, :a), ArcFrom(:t1, :b, 2)]
        arcsto = [ArcTo(:t1, :c), ArcTo(:t2, :c)]

        net = Net(places, transitions, arcsfrom, arcsto)

        # inputArcs: t1 has two input arcs, t2 has none
        @test length(net.inputArcs[:t1]) == 2
        @test isempty(net.inputArcs[:t2])

        # Check tuples contain (placeId, weight, optional)
        t1Inputs = Set(net.inputArcs[:t1])
        @test (:a, 1, false) ∈ t1Inputs
        @test (:b, 2, false) ∈ t1Inputs

        # outputArcs: t1 and t2 each have one output to :c
        @test net.outputArcs[:t1] == [(:c, 1)]
        @test net.outputArcs[:t2] == [(:c, 1)]
    end

    @testset "Net affectedTransitions (reverse index)" begin
        places = Dict(:a => Place(:a), :b => Place(:b), :c => Place(:c))
        transitions = Dict(:t1 => Transition(:t1), :t2 => Transition(:t2))
        arcsfrom = [ArcFrom(:t1, :a), ArcFrom(:t1, :b, 2), ArcFrom(:t2, :a)]
        arcsto = [ArcTo(:t1, :c), ArcTo(:t2, :c)]

        net = Net(places, transitions, arcsfrom, arcsto)

        # :a feeds both :t1 and :t2
        @test Set(net.affectedTransitions[:a]) == Set([:t1, :t2])
        # :b feeds only :t1
        @test net.affectedTransitions[:b] == [:t1]
        # :c is an output place — no transitions consume from it
        @test !haskey(net.affectedTransitions, :c)
    end

    @testset "Net upstream / downstream (LoLA 2 indexes)" begin
        # Net: :a -> :t1 -> :c, :a -> :t2 -> :c, :c -> :t3 -> :d
        places = Dict(:a => Place(:a), :c => Place(:c), :d => Place(:d))
        transitions = Dict(:t1 => Transition(:t1), :t2 => Transition(:t2), :t3 => Transition(:t3))
        arcsfrom = [ArcFrom(:t1, :a), ArcFrom(:t2, :a), ArcFrom(:t3, :c)]
        arcsto = [ArcTo(:t1, :c), ArcTo(:t2, :c), ArcTo(:t3, :d)]

        net = Net(places, transitions, arcsfrom, arcsto)

        # :t1 and :t2 share input place :a — they are upstream of each other
        @test net.upstream[:t1] == [:t2]
        @test net.upstream[:t2] == [:t1]
        # :t3 has no upstream transitions (only :t3 consumes from :c)
        @test isempty(net.upstream[:t3])

        # :t1 and :t2 output to :c, which :t3 consumes from — :t3 is downstream
        @test net.downstream[:t1] == [:t3]
        @test net.downstream[:t2] == [:t3]
        # :t3 outputs to :d, nothing consumes from :d
        @test isempty(net.downstream[:t3])

        @test net.recheck[:t1] == [:t1, :t2, :t3]
        @test net.recheck[:t2] == [:t1, :t2, :t3]
        @test net.recheck[:t3] == [:t3]
    end

    @testset "Net downstream includes self on cycle" begin
        # Cycle: :a -> :t1 -> :a (output loops back to input)
        places = Dict(:a => Place(:a))
        transitions = Dict(:t1 => Transition(:t1))
        arcsfrom = [ArcFrom(:t1, :a)]
        arcsto = [ArcTo(:t1, :a)]

        net = Net(places, transitions, arcsfrom, arcsto)

        # :t1 deposits back to :a, which :t1 consumes from — self-loop
        @test :t1 ∈ net.downstream[:t1]
    end
end

end
