using Test
using Peven

@testset "Peven" begin
    include("state.jl")
    include("spec.jl")
    include("validation.jl")
    include("engine.jl")
    include("readme.jl")
    include("jet.jl")
    include("aqua.jl")
end
