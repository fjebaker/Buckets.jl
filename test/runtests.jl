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

@testset "simple-index" begin
    # generate known data
    X = collect(range(1.1, 10.6, step = 0.5))
    y = ones(Float64, length(X))
    bins = 1:11

    outbucket = Buckets.IndexBucket(Int64, size(X), length(bins))
    bucket!(outbucket, Simple(), X, bins)
    @test outbucket.indices ==
          [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10]

    groups = unpack_bucket(outbucket)
    @test groups == [
        [1, 2],
        [3, 4],
        [5, 6],
        [7, 8],
        [9, 10],
        [11, 12],
        [13, 14],
        [15, 16],
        [17, 18],
        [19, 20],
        Int64[],
    ]
    @test length(groups) == length(bins)
end

@testset "simple-index-out-of-bin" begin
    # generate known data that is out of range of the bins
    X = collect(range(-1.0, 12.6, step = 0.5))
    y = ones(Float64, length(X))
    bins = 1:11

    outbucket = Buckets.IndexBucket(Int64, size(X), length(bins))
    bucket!(outbucket, Simple(), X, bins)
    @test outbucket.indices == [
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        2,
        3,
        3,
        4,
        4,
        5,
        5,
        6,
        6,
        7,
        7,
        8,
        8,
        9,
        9,
        10,
        10,
        11,
        11,
        11,
    ]

    groups = unpack_bucket(outbucket)
    @test groups == [
        [1, 2, 3, 4, 5, 6, 7],
        [8, 9],
        [10, 11],
        [12, 13],
        [14, 15],
        [16, 17],
        [18, 19],
        [20, 21],
        [22, 23],
        [24, 25],
        [26, 27, 28],
    ]
    @test length(groups) == length(bins)
end

@testset "simple-bin-bigger-than-indexes" begin
    X = collect(range(-1.0, 4.0, step = 0.5))
    y = ones(Float64, length(X))
    bins = 1:11

    outbucket = Buckets.IndexBucket(Int64, size(X), length(bins))
    bucket!(outbucket, Simple(), X, bins)
    @test outbucket.indices == [1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 3]

    groups = unpack_bucket(outbucket)
    @test groups == [
        [1, 2, 3, 4, 5, 6, 7],
        [8, 9],
        [10, 11],
        Int64[],
        Int64[],
        Int64[],
        Int64[],
        Int64[],
        Int64[],
        Int64[],
        Int64[],
    ]
    @test length(groups) == length(bins)
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

@testset "#13" begin
    X = [58120.5, 58121.5, 58123.5, 58124.5, 58125.5]
    y = [
        -0.00013285555791675628,
        0.0007222660213649527,
        0.00027540938576390173,
        0.00011658070990990752,
        -2.691830320163596e-6,
    ]
    bins = [
        58120.5,
        58140.64473684211,
        58160.78947368421,
        58180.93421052631,
        58201.07894736842,
    ]
    y_binned = bucket(X, y, bins; reduction = mean)
    @test y_binned ≈ [0.00019574174576036845, 0, 0, 0, 0] rtol = 1e-3
end

@testset "mean" begin
    # generate known data
    X = collect(range(1.1, 10.6, step = 0.5))
    y = collect(1.0:length(X))
    bins = 1:11
    y_binned = bucket(X, y, bins; reduction = sum)
    @test y_binned ≈ [3.0, 7.0,11.0,15.0,19.0,23.0,27.0,31.0,35.0,39.0, 0.0] rtol = 1e-3
    y_binned = bucket(X, y, bins; reduction = mean)
    @test y_binned ≈ [1.5, 3.5, 5.5, 7.5, 9.5,11.5,13.5,15.5,17.5,19.5, 0] rtol = 1e-3
end

# little bit of aqua
using Aqua
Aqua.test_undefined_exports(Buckets)
Aqua.test_unbound_args(Buckets)
Aqua.test_stale_deps(Buckets)
