module SrcStateTests

using Test
using Main.Peven

@testset "src/state" begin
    @testset "Token" begin
        @test Color === Symbol

        tok = Token(:redteam, "run1", 42)
        @test tok isa Token
        @test tok isa AbstractToken
        @test color(tok) === :redteam
        @test run_key(tok) == "run1"
        @test tok.payload == 42

        str_tok = Token(:baseline, "run2", "draft")
        @test color(str_tok) === :baseline
        @test run_key(str_tok) == "run2"
        @test str_tok.payload == "draft"
    end

    @testset "Token convenience" begin
        bare = Token("run1")
        @test color(bare) === :default
        @test run_key(bare) == "run1"
        @test bare.payload === nothing

        colored = Token(:gold, "run2")
        @test color(colored) === :gold
        @test colored.payload === nothing
    end

    @testset "Marking" begin
        m = Marking(Dict(
            :ready => Token[Token(:red, "r1", 1)],
            :done  => Token[],
        ))
        @test m isa Marking{Token}
        @test length(m.tokens_by_place[:ready]) == 1
        @test isempty(m.tokens_by_place[:done])

        empty_m = Marking()
        @test empty_m isa Marking{Token}
        @test isempty(empty_m.tokens_by_place)

        typed_empty = Marking{AbstractToken}()
        @test typed_empty isa Marking{AbstractToken}
    end

    @testset "run_keys" begin
        @test run_keys(Marking()) == String[]

        m = Marking(Dict(
            :ready => Token[Token(:red, "r1", 1), Token(:red, "r2", 2), Token(:red, "r1", 3)],
            :done  => Token[Token(:gold, "r3", 4)],
        ))
        keys_out = run_keys(m)
        @test keys_out == ["r1", "r2", "r3"]

        single_place = Marking(Dict(
            :ready => Token[
                Token(:red, "r2", 1),
                Token(:red, "r1", 2),
                Token(:red, "r2", 3),
                Token(:red, "r3", 4),
            ],
        ))
        @test run_keys(single_place) == ["r2", "r1", "r3"]
    end

    @testset "run_marking" begin
        m = Marking(Dict(
            :ready => Token[Token(:red, "r1", 1), Token(:red, "r2", 2)],
            :done  => Token[Token(:gold, "r1", 3)],
        ))

        sliced = run_marking(m, "r1")
        @test sliced isa Marking{Token}
        @test length(sliced.tokens_by_place[:ready]) == 1
        @test sliced.tokens_by_place[:ready][1].payload == 1
        @test length(sliced.tokens_by_place[:done]) == 1
        @test sliced.tokens_by_place[:done][1].payload == 3

        only_ready = run_marking(m, "r2")
        @test haskey(only_ready.tokens_by_place, :ready)
        @test !haskey(only_ready.tokens_by_place, :done)

        @test isempty(run_marking(m, "missing").tokens_by_place)
    end
end

end
