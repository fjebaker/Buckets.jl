using Test
using Buckets

@testset "simple" begin
    # generate known data
    X = collect(range(1.0, 10.0, 20))
    y = ones(Float64, length(X))
    bins = 1:10
    y_binned = bucket(X, y, bins; reduction=sum)
    @test y_binned == [1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 3.0]
    y_binned = bucket(X, y, bins; reduction=mean)
    @test y_binned == [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
end

@testset "down-sample" begin
    # generate known data
    X = collect(range(1.0, 10.0, 20))
    y = ones(Float64, length(X)-1)
    bins = 1:1.5:11.5
    y_binned = bucket(DownSample(), X, y, bins)
    @test y_binned == [0.0, 3.1666666666666665, 3.166666666666667, 3.166666666666667, 3.1666666666666665, 3.166666666666666, 3.1666666666666674, 0.0]
    @test sum(y_binned) == sum(y)

    y = ones(Float64, 20)
    @test_throws DimensionMismatch bucket(DownSample(), X, y, bins)
end

# little bit of aqua
using Aqua
Aqua.test_undefined_exports(Buckets)
Aqua.test_unbound_args(Buckets)
Aqua.test_stale_deps(Buckets)
