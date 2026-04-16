module SrcJETTests

using Test
using Main.Peven

@testset "src/jet" begin
    if Base.find_package("JET") === nothing
        @info "Skipping JET tests; run via Pkg.test() to activate test extras."
    else
        using JET
        JET.test_package(Peven; toplevel_logger=nothing)
    end
end

end
