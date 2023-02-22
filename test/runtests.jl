using Test
using Buckets
using Random

@testset "simple" begin
    # generate known data
    X = collect(range(1.1, 10.6, step = 0.5))
    y = ones(Float64, length(X))
    bins = 1:11
    y_binned = bucket(X, y, bins; reduction = sum)
    @test y_binned == [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 0.0]
    y_binned = bucket(X, y, bins; reduction = mean)
    @test y_binned == [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0]
end

@testset "simple-count" begin
    X = collect(range(1.1, 10.6, step = 0.5)) 
    bins = 1:11
    y_binned = bucket(X, bins)
    @test y_binned == [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 0.0]
end

@testset "down-sample" begin
    # generate known data
    X = collect(range(1.0, 10.0, 20))
    y = ones(Float64, length(X) - 1)
    bins = 1:11
    y_binned = bucket(DownSample(), X, y, bins)
    @test y_binned == [
        0.0,
        2.111111111111111,
        2.111111111111111,
        2.111111111111111,
        2.1111111111111107,
        2.1111111111111125,
        2.1111111111111103,
        2.111111111111112,
        2.1111111111111107,
        2.1111111111111107,
        0.0,
    ]
    @test sum(y_binned) ≈ sum(y)

    y = ones(Float64, 20)
    @test_throws DimensionMismatch bucket(DownSample(), X, y, bins)
end

@testset "race-conditions" begin
    Random.seed!(1)
    X = collect(range(1.0, 10.0, 10_000))
    shuffle!(X)
    y = ones(Float64, length(X))
    bins = 1:10

    # default
    y_binned = bucket(X, y, bins; reduction = sum)
    expected = [1112.0, 1111.0, 1111.0, 1111.0, 1111.0, 1111.0, 1111.0, 1111.0, 1111.0, 0.0]
    @test y_binned == expected


    # threaded sum
    cache = ThreadBuckets(AggregateBucket, Float64, 10)
    bucket!(cache, Simple(), X, y, bins)
    @test unpack_bucket(cache) == expected

    y_binned = bucket(ThreadedSimple(), X, y, bins; reduction = sum)
    @test y_binned == expected
end

# little bit of aqua
using Aqua
Aqua.test_undefined_exports(Buckets)
Aqua.test_unbound_args(Buckets)
Aqua.test_stale_deps(Buckets)
