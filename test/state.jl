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
        @test runKey(tok) == "run1"
        @test tok.payload == 42

        strTok = Token(:baseline, "run2", "draft")
        @test color(strTok) === :baseline
        @test runKey(strTok) == "run2"
        @test strTok.payload == "draft"
    end

    @testset "Token convenience" begin
        bare = Token("run1")
        @test color(bare) === :default
        @test runKey(bare) == "run1"
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
        @test length(m.tokensByPlace[:ready]) == 1
        @test isempty(m.tokensByPlace[:done])

        emptyM = Marking()
        @test emptyM isa Marking{Token}
        @test isempty(emptyM.tokensByPlace)

        typedEmpty = Marking{AbstractToken}()
        @test typedEmpty isa Marking{AbstractToken}
    end
end

end
