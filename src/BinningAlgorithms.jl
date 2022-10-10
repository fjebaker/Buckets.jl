module BinningAlgorithms

using Statistics
using LoopVectorization


@inline function _get_bin_index(x, bins, last_bin)
    _bin_low = searchsortedfirst(bins, x)
    _bin_low > last_bin ? last_bin : _bin_low
end

function _check_bin_args(output, noutput, X, y, bins)
    if size(X) != size(y)
        error("Dimensions mismatch for X and y.")
    end
    if (size(output) != size(bins)) || (size(output) != size(noutput))
        error("Dimension mistmatch between output arrays and bins.")
    end
end

function _check_bin_args(output, noutput, X1, X2, y::AbstractVector, bins1, bins2)
    if (size(X1) != size(X2)) || (size(X1) != size(y))
        error("Dimensions mismatch for X1, X2 and y.")
    end
    if ((size(bins1, 1), size(bins2, 1)) != size(output)) || size(output) != size(noutput)
        error("Dimension mistmatch between output arrays and bins.")
    end
end

function _check_bin_args(output, noutput, X1, X2, y::AbstractMatrix, bins1, bins2)
    if (size(X1, 1), size(X2, 1)) != size(y)
        error("Dimensions mismatch for X1, X2 and y.")
    end
    if ((size(bins1, 1), size(bins2, 1)) != size(output)) || size(output) != size(noutput)
        error("Dimension mistmatch between output arrays and bins.")
    end
end

function _fast_sorted_single_dim_binning!(output, noutput, X, y, bins)
    _check_bin_args(output, noutput, X, y, bins)

    last_bin = lastindex(bins)
    @tturbo warn_check_args = true for i in eachindex(X)
        bin_index = _get_bin_index(X[i], bins, last_bin)

        output[bin_index] += y[i]
        noutput[bin_index] += 1
    end
end

function _fast_2d_contiguous_binning!(output, noutput, X1, X2, y::AbstractMatrix, bins1, bins2)
    _check_bin_args(output, noutput, X1, X2, y, bins1, bins2)

    last_bin1 = lastindex(bins1)
    last_bin2 = lastindex(bins2)

    @tturbo warn_check_args = true for i in eachindex(X1)
        bin_row = _get_bin_index(X1[i], bins1, last_bin1)
        for j in eachindex(X2)
            bin_column = _get_bin_index(X2[j], bins2, last_bin2)

            output[bin_column, bin_row] += y[bin_column, bin_row]
            noutput[bin_column, bin_row] += 1
        end
    end
end

function _fast_2d_contiguous_binning!(output, noutput, X1, X2, y::AbstractVector, bins1, bins2)
    _check_bin_args(output, noutput, X1, X2, y, bins1, bins2)

    last_bin1 = lastindex(bins1)
    last_bin2 = lastindex(bins2)

    @tturbo warn_check_args = true for i in eachindex(X1)
        bin_column = _get_bin_index(X1[i], bins1, last_bin1)
        bin_row = _get_bin_index(X2[i], bins2, last_bin2)

        output[bin_column, bin_row] += y[i]
        noutput[bin_column, bin_row] += 1
    end
end

@inline function _apply_reduction!(output, noutput, ::typeof(sum))
    output
end

@inline function _apply_reduction!(output, noutput, ::typeof(mean))
    @. output = output / noutput
end

"""
    bincontiguous(X, y, bins; kwargs...)

Bin data in `y` by `X` into `bins`, that is to say, reduce the `y` data corresponding to coordinates `X` over
domain ranges given by `bins`. 

The contiguous requirement here is that `bins` describes the bin edges, such that the minimal value 
of bin ``i`` is the maximal value of bin ``(i-1)``. This function will bin all `y` with
`X < minimum(bins)` into the first bin, and all `y` with `X > maximum(bins)` into the last bin.

This function, and its dispatches, accept the following keyword arguments

- `reduction=sum`: a statistical function used to reduce all `y` in a given bin. 
"""
function bincontiguous(
    X::AbstractVector{T},
    y::AbstractVector{T},
    bins;
    reduction = sum,
) where {T}
    output = zeros(T, size(bins))
    noutput = zeros(Int, size(output))
    _fast_contiguous_binning!(output, noutput, X, y, bins)
    _apply_reduction!(output, noutput, reduction)
    output
end

"""
    bincontiguous(X1, X2, y, bins1, bins2; kwargs...)

Two dimensional contiguous binning, where `y` can either be

- `AbstractMatrix`: in this case, `X1` and `X2` are assumed to be the columns and rows respectively of the data in `y`,
and `bins1` (`bins2`) the bin edges for `X1` (`X2`).
- `AbstractVector`: `X1` and `X2` are effectively the coordinates of `y`
"""
function bincontiguous(
    X1::AbstractVector{T},
    X2::AbstractVector{T},
    y::AbstractArray{T},
    bins1,
    bins2;
    reduction = sum,
) where {T}
    output = zeros(T, (length(bins1), length(bins2)))
    noutput = zeros(Int, size(output))
    _fast_2d_contiguous_binning!(output, noutput, X1, X2, y, bins1, bins2)
    _apply_reduction!(output, noutput, reduction)
    output
end

export bincontiguous

end # module BinningAlgorithms
