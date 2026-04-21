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
        @test isnothing(t.join_by)

        t2 = Transition(:judge, :agent; retries=2)
        @test t2.executor === :agent
        @test t2.retries == 2

        guard_fn = tokens -> length(tokens) > 0
        join_fn = (pid, token) -> token.payload
        t3 = Transition(:gate, :default; guard=guard_fn, join_by=join_fn)
        @test t3.guard === guard_fn
        @test t3.join_by === join_fn

        @test_throws ArgumentError Transition(:bad; retries=-1)
    end

    @testset "BundleRef" begin
        a = BundleRef(:judge, "r1", :user_1, 1)
        b = BundleRef(:judge, "r1", :user_1, 1)
        c = BundleRef(:judge, "r1", :user_1, 2)
        @test a == b
        @test isequal(a, b)
        @test hash(a) == hash(b)
        @test a != c
        @test_throws ArgumentError BundleRef(:judge, "r1", nothing, 0)
    end

    @testset "ArcFrom" begin
        arc = ArcFrom(:judge, :ready)
        @test arc.transition === :judge
        @test arc.from === :ready
        @test arc.weight == 1

        weighted = ArcFrom(:judge, :ready, 3)
        @test weighted.weight == 3

        @test_throws ArgumentError ArcFrom(:judge, :ready, 0)
        @test_throws ArgumentError ArcFrom(:judge, :ready, -1)
    end

    @testset "ArcTo" begin
        arc = ArcTo(:judge, :done)
        @test arc.transition === :judge
        @test arc.to === :done
        @test !hasmethod(ArcTo, Tuple{Symbol,Symbol,Int})
    end

    @testset "Net" begin
        places = Dict(
            :ready => Place(:ready),
            :done => Place(:done, 3),
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
        @test net.from_places == Set([:ready])
    end

    @testset "Net cached arc indexes" begin
        places = Dict(:a => Place(:a), :b => Place(:b), :c => Place(:c))
        transitions = Dict(:t1 => Transition(:t1), :t2 => Transition(:t2))
        arcsfrom = [ArcFrom(:t1, :b, 2), ArcFrom(:t1, :a)]
        arcsto = [ArcTo(:t1, :c), ArcTo(:t2, :c)]

        net = Net(places, transitions, arcsfrom, arcsto)

        @test net.input_arcs[:t1] == [(:b, 2), (:a, 1)]
        @test isempty(net.input_arcs[:t2])
        @test net.output_arcs[:t1] == [:c]
        @test net.output_arcs[:t2] == [:c]
    end

    @testset "Net allows structural validation to happen later" begin
        places = Dict(:left => Place(:left), :right => Place(:right), :done => Place(:done))
        join_by = (pid, token) -> token.payload

        duplicate_arc_net = Net(
            places,
            Dict(:join => Transition(:join; join_by=join_by)),
            [ArcFrom(:join, :left), ArcFrom(:join, :left, 2)],
            [ArcTo(:join, :done)],
        )
        @test duplicate_arc_net.input_arcs[:join] == [(:left, 1), (:left, 2)]

        invalid_keyed_join_net = Net(
            places,
            Dict(:judge => Transition(:judge; join_by=join_by)),
            [ArcFrom(:judge, :left)],
            [ArcTo(:judge, :done)],
        )
        @test invalid_keyed_join_net.input_arcs[:judge] == [(:left, 1)]

        net = Net(
            places,
            Dict(:join => Transition(:join; join_by=join_by)),
            [ArcFrom(:join, :left), ArcFrom(:join, :right)],
            [ArcTo(:join, :done)],
        )
        @test net.input_arcs[:join] == [(:left, 1), (:right, 1)]
    end

end

end
