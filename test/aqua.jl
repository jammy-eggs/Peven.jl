module SrcAquaTests

using Test
using Main.Peven

@testset "src/aqua" begin
    if Base.find_package("Aqua") === nothing
        @info "Skipping Aqua tests; run via Pkg.test() to activate test extras."
    else
        using Aqua
        Aqua.test_all(Peven)
    end
end

end
