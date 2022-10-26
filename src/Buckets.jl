module Buckets

import Base.size
import Base.merge!

using Statistics
export mean

abstract type AbstractBucketAlgorithm end
abstract type AbstractThreadedBucketAlgorithm <: AbstractBucketAlgorithm end

include("valuebucket.jl")
include("algorithms.jl")


@inline function find_bin_index(x, bins)
    searchsortedfirst(bins, x)
end

@inline function find_bin_index(x, bins, last_bin)
    _bin_low = find_bin_index(x, bins)
    min(_bin_low, last_bin)
end

function _check_X_y_delta(X, y, Δ)
    if length(X) != length(y) + Δ
        throw(
            DimensionMismatch(
                "For X with $(length(X)) elements, y must have $(length(X) - Δ) elements.",
            ),
        )
    end
end

function _check_X_y(X, y)
    if size(X) != size(y)
        throw(DimensionMismatch("Dimensions mismatch for X and y."))
    end
end

function _check_X_y(X1, X2, y::AbstractVector)
    if (size(X1) != size(X2)) || (size(X1) != size(y))
        throw(DimensionMismatch("Dimensions mismatch for X1, X2 and y."))
    end
end

function _check_X_y(X1, X2, y::AbstractMatrix)
    if (size(X1, 1), size(X2, 1)) != size(y)
        throw(DimensionMismatch("Dimensions mismatch for X1, X2 and y."))
    end
end

function _check_bin_output_args(out_bucket, bins1, bins2)
    _check_sorted(bins1)
    _check_sorted(bins2)
    if ((size(bins1, 1), size(bins2, 1)) != size(out_bucket))
        throw(DimensionMismatch("Dimension mistmatch between output arrays and bins."))
    end
end

function _check_bin_output_args(out_bucket, bins)
    _check_sorted(bins)
    if (size(out_bucket) != size(bins))
        throw(DimensionMismatch("Dimension mistmatch between output arrays and bins."))
    end
end

function _check_bin_args(out_bucket, X1, X2, y, bins1, bins2)
    _check_X_y(X1, X2, y)
    _check_bin_output_args(out_bucket, bins1, bins2)
end

function _check_bin_args(out_bucket, X, y, bins)
    _check_X_y(X, y)
    _check_bin_output_args(out_bucket, bins)
end

function _check_sorted(X)
    if !issorted(X)
        error(
            "Input `X` is not sorted. `X` and `y` should be sorted together (e.g. with `sortperm`).",
        )
    end
end

function bucket(alg::AbstractBucketAlgorithm, args...; reduction = nothing, kwargs...)
    out_bucket = allocate_output(alg, reduction, args...; kwargs...)
    bucket!(out_bucket, _algorithm(alg), args...; kwargs...)
    unpack_bucket(out_bucket)
end

"""
    bucket(X, y, bins; kwargs...)
    bucket(alg::AbstractBucketAlgorithm, args...; kwargs...)

Defaults to the [`Simple`](@ref) algorithm if `alg` unspecified.

Bin data in `y` by `X` into `bins`, that is to say, reduce the `y` data corresponding to coordinates `X` over
domain ranges given by `bins`. 

The contiguous requirement is that `bins` describes the bin edges, such that the minimal value 
of bin ``i`` is the maximal value of bin ``(i-1)``. This function will bin all `y` with
`X < minimum(bins)` into the first bin, and all `y` with `X > maximum(bins)` into the last bin.
"""
bucket(X, y, bins; kwargs...) = bucket(Simple(), X, y, bins; kwargs...)

"""
    bucket(X1, X2, y, bins1, bins2, alg=Simple(); kwargs...)

Two dimensional contiguous binning, where `y` can either be

- `AbstractMatrix`: in this case, `X1` and `X2` are assumed to be the columns and rows respectively of the data in `y`,
and `bins1` (`bins2`) the bin edges for `X1` (`X2`).
- `AbstractVector`: `X1` and `X2` are effectively the coordinates of `y`
"""
bucket(X1, X2, y, bins1, bins2; kwargs...) =
    bucket(Simple(), X1, X2, y, bins1, bins2; kwargs...)

export bucket, AbstractBucketAlgorithm

end # module Buckets
