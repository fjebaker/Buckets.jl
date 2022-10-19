module Buckets

using Statistics

export mean

using Base.Threads: @threads 

abstract type AbstractBucketAlgorithm end
struct Simple <: AbstractBucketAlgorithm end
struct DownSample <: AbstractBucketAlgorithm end

@inline function find_bin_index(x, bins)
    searchsortedfirst(bins, x) 
end

@inline function find_bin_index(x, bins, last_bin)
    _bin_low = find_bin_index(x, bins)
    min(_bin_low, last_bin)
end

function _check_X_y_delta(X, y, Δ)
    if length(X) != length(y) + Δ
        throw(DimensionMismatch("For X with $(length(X)) elements, y must have $(length(X) - Δ) elements."))
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

function _check_bin_output_args(output, noutput, bins1, bins2)
    _check_sorted(bins1)
    _check_sorted(bins2)
    if ((size(bins1, 1), size(bins2, 1)) != size(output)) || size(output) != size(noutput)
        throw(DimensionMismatch("Dimension mistmatch between output arrays and bins."))
    end
end

function _check_bin_output_args(output, noutput, bins)
    _check_sorted(bins)
    if (size(output) != size(bins)) || (size(output) != size(noutput))
        throw(DimensionMismatch("Dimension mistmatch between output arrays and bins."))
    end
end

function _check_bin_output_args(output, bins)
    _check_sorted(bins)
    if (size(output) != size(bins))
        throw(DimensionMismatch("Dimension mistmatch between output arrays and bins."))
    end
end

function _check_bin_args(output, noutput, X1, X2, y, bins1, bins2)
    _check_X_y(X1, X2, y)
    _check_bin_output_args(output, noutput, bins1, bins2)
end

function _check_bin_args(output, noutput, X, y, bins)
    _check_X_y(X, y)
    _check_bin_output_args(output, noutput, bins)
end

function _check_sorted(X)
    if !issorted(X)
        error("Input `X` is not sorted. `X` and `y` should be sorted together (e.g. with `sortperm`).")
    end
end

_apply_reduction!(output, _, ::typeof(sum)) = 
    output

_apply_reduction!(output, noutput, ::typeof(mean)) =
    @. output = output / noutput

# Algorithms

function bucket!(::Simple, output, noutput, X1, X2, y::AbstractMatrix, bins1, bins2; reduction=sum)
    _check_bin_args(output, noutput, X1, X2, y, bins1, bins2)
    last_bin1 = lastindex(bins1)
    last_bin2 = lastindex(bins2)

    for i in eachindex(X1)
        bin_row = find_bin_index(X1[i], bins1, last_bin1)
        for j in eachindex(X2)
            bin_column = find_bin_index(X2[j], bins2, last_bin2)

            output[bin_column, bin_row] += y[bin_column, bin_row]
            noutput[bin_column, bin_row] += 1
        end
    end  
    _apply_reduction!(output, noutput, reduction)
    output
end

function bucket!(::Simple, output, noutput, X1, X2, y::AbstractVector, bins1, bins2; reduction=sum)
    _check_bin_args(output, noutput, X1, X2, y, bins1, bins2)

    last_bin1 = lastindex(bins1)
    last_bin2 = lastindex(bins2)

    for i in eachindex(X1)
        bin_column = find_bin_index(X1[i], bins1, last_bin1)
        bin_row = find_bin_index(X2[i], bins2, last_bin2)

        output[bin_column, bin_row] += y[i]
        noutput[bin_column, bin_row] += 1
    end

    _apply_reduction!(output, noutput, reduction)
    output
end

function bucket!(::Simple, output, noutput, X, y, bins; reduction=sum)
    _check_bin_args(output, noutput, X, y, bins)
    last_bin = lastindex(bins)

    for i in eachindex(X)
        bin_index = find_bin_index(X[i], bins, last_bin)

        output[bin_index] += y[i]
        noutput[bin_index] += 1
    end

    _apply_reduction!(output, noutput, reduction)
    output
end

"""
    bucket!(::DownSample, output, X, y, bins)

Down-sample the values in `y` in current bins `X` to fewer bins in `bins` by linearly
splitting bins where they overlap a new bin edge.
```
            x₁   Δx   x₂        
  |         |---Δx----|         |
 .                .                .
            |--B--|-C-|
```

```math
\\gamma = \\min \\left( \\frac{B}{\\Delta x}, 1 \\right)
```
"""
function bucket!(::DownSample, output, X, y, bins)
    _check_X_y_delta(X, y, 1)
    _check_bin_output_args(output, bins)
    _check_sorted(X)
    # additionally need to make sure fewer bins than X
    if length(X) < length(bins)
        error("DownSample requires length of bins to be less than or equal to length of X.")
    end

    last_bin = lastindex(bins)
    last_x = lastindex(X)

    start = find_bin_index(first(bins), X)
    if start >= last_x
        # nothing to rebin
        return output
    end

    # special case for first bin
    if start > 1
        x₁= X[start-1]
        x₂ = X[start]
        b₁ = bins[1]

        # find ratio of lengths
        Δx = x₂ - x₁
        Δb = x₂ - b₁
        γ = min(Δb / Δx, 1)
        output[1] += y[start-1] * (1 - γ)
    end

    for i in start:(last_x-1)
        x₁= X[i]
        x₂ = X[i+1]

        j = find_bin_index(x₁, bins)
        if j > last_bin
            break
        end

        b₂ = bins[j]

        # find ratio of lengths
        Δx = x₂ - x₁
        Δb = b₂ - x₁
        γ = min(Δb / Δx, 1)

        output[j] = y[i] * γ + output[j]
        # carry over if not at end
        if j + 1 <= last_bin
            output[j+1] = y[i] * (1 - γ) + output[j+1]
        end
    end
    output
end

# Allocations

function allocate_output(::DownSample, X, y::AbstractArray{T}, bins) where {T}
    output = zeros(T, length(bins))
    (output,)
end

function allocate_output(::AbstractBucketAlgorithm, X, y::AbstractArray{T}, bins; kwargs...) where {T}
    output = zeros(T, length(bins))
    noutput = similar(output)
    (output, noutput)
end

function allocate_output(::AbstractBucketAlgorithm, X1, X2, y::AbstractArray{T}, bins1, bins2; kwargs...) where {T}
    output = zeros(T, (length(bins1), length(bins2)))
    noutput = similar(output)
    (output, noutput)
end

# Allocating interface

function bucket(
    alg::AbstractBucketAlgorithm,
    args...; kwargs...
)
    allocated_outputs = allocate_output(alg, args...; kwargs...)
    bucket!(alg, allocated_outputs..., args...; kwargs...)
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
bucket(X1, X2, y, bins1, bins2; kwargs...) = bucket(Simple(), X1, X2, y, bins1, bins2; kwargs...)

export bucket, bucket!, Simple, DownSample, AbstractBucketAlgorithm

end # module Buckets
