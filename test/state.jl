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
end

end
