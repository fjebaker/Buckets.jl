using Test
using Buckets

@testset "simple" begin
    # generate known data
    X = collect(range(1.0, 10.0, 20))
    y = ones(Float64, length(X))
    bins = 1:10
    y_binned = bucket(X, y, bins; reduction=sum)
    @test y_binned == [1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 3.0]
    y_binned = bucket(X, y, bins)
    @test y_binned == [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
end

# little bit of aqua
using Aqua
Aqua.test_undefined_exports(Buckets)
Aqua.test_unbound_args(Buckets)
Aqua.test_stale_deps(Buckets)
