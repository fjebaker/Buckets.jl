_algorithm(T::Type{<:AbstractThreadedBucketAlgorithm}) = error("Not defined for $T")
_algorithm(::T) where {T<:AbstractThreadedBucketAlgorithm} = _algorithm(T)
_algorithm(B::AbstractBucketAlgorithm) = B

struct Simple <: AbstractBucketAlgorithm end
struct ThreadedSimple <: AbstractThreadedBucketAlgorithm end
_algorithm(::Type{<:ThreadedSimple}) = Simple()

macro optionally_threaded(bucket, expr)
    quote
        if typeof($(bucket)) <: ThreadBuckets
            @inbounds Threads.@threads $(expr)
        else
            @inbounds $(expr)
        end
    end |> esc
end

function bucket!(out_bucket, ::Simple, X1, X2, y::AbstractMatrix, bins1, bins2)
    _check_bin_args(out_bucket, X1, X2, y, bins1, bins2)
    last_bin1 = lastindex(bins1)
    last_bin2 = lastindex(bins2)

    @optionally_threaded out_bucket for i in eachindex(X1)
        bin_row = find_bin_index(X1[i], bins1, last_bin1)
        for j in eachindex(X2)
            bin_column = find_bin_index(X2[j], bins2, last_bin2)

            upsert!(
                out_bucket,
                CartesianIndex(bin_column, bin_row),
                CartesianIndex(i, j),
                y[i, j],
            )
        end
    end

    out_bucket
end

function bucket!(out_bucket, ::Simple, X1, X2, y::AbstractVector, bins1, bins2)
    _check_bin_args(out_bucket, X1, X2, y, bins1, bins2)

    last_bin1 = lastindex(bins1)
    last_bin2 = lastindex(bins2)

    @optionally_threaded out_bucket for i in eachindex(X1)
        bin_column = find_bin_index(X1[i], bins1, last_bin1)
        bin_row = find_bin_index(X2[i], bins2, last_bin2)

        upsert!(out_bucket, CartesianIndex(bin_column, bin_row), i, y[i])
    end

    out_bucket
end

function bucket!(out_bucket, ::Simple, X, y, bins)
    _check_bin_args(out_bucket, X, y, bins)
    last_bin = lastindex(bins)

    @optionally_threaded out_bucket for i in eachindex(X)
        bin_index = find_bin_index(X[i], bins, last_bin)

        upsert!(out_bucket, bin_index, i, y[i])
    end

    out_bucket
end


"""
    bucket!(::DownSample, output, X, y, bins)

Down-sample the values in `y` in current bins `X` to fewer bins in `bins` by linearly
splitting bins where they overlap a new bin edge.
```
            x???   ??x   x???        
  |         |---??x----|         |
 .                .                .
            |--B--|-C-|
```

```math
\\gamma = \\min \\left( \\frac{B}{\\Delta x}, 1 \\right)
```
"""
struct DownSample <: AbstractBucketAlgorithm end
bucket_type(::Type{<:DownSample}) = AggregateBucket
struct ThreadedDownSample <: AbstractThreadedBucketAlgorithm end
_algorithm(::Type{<:ThreadedDownSample}) = DownSample()

function bucket!(out_bucket, ::DownSample, X, y, bins)
    _check_X_y_delta(X, y, 1)
    _check_bin_output_args(out_bucket, bins)
    _check_sorted(X)
    # additionally need to make sure fewer bins than X
    if length(X) < length(bins)
        error("DownSample requires length of bins to be less than or equal to length of X.")
    end

    last_bin = lastindex(bins)
    last_x = lastindex(X)

    start = find_bin_index(first(bins), X)
    if start >= last_x
        #??nothing to rebin
        return out_bucket
    end

    # special case for first bin
    # use underscore to avoid boxed variables in the @floop
    if start > 1
        _x??? = X[start-1]
        _x??? = X[start]
        _b??? = bins[1]

        #??find ratio of lengths
        _??x = _x??? - _x???
        _??b = _x??? - _b???
        _?? = min(_??b / _??x, 1)
        upsert!(out_bucket, 1, start - 1, y[start-1] * (1 - _??))
    end

    @optionally_threaded out_bucket for i = start:(last_x-1)
        x??? = X[i]
        x??? = X[i+1]

        j = find_bin_index(x???, bins)
        if j > last_bin
            # ... hang around and wait for other threads
        else
            b??? = bins[j]

            #??find ratio of lengths
            ??x = x??? - x???
            ??b = b??? - x???
            ?? = min(??b / ??x, 1)

            upsert!(out_bucket, j, i, y[i] * ??)
            # carry over if not at end
            if j + 1 <= last_bin
                upsert!(out_bucket, j + 1, i, y[i] * (1 - ??))
            end
        end
    end
    out_bucket
end

export bucket!, Simple, ThreadedSimple, DownSample, ThreadedDownSample
