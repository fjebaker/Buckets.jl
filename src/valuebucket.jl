abstract type AbstractValueBucket{T} end

function allocate_output(alg::AbstractBucketAlgorithm, ::Nothing, args...)
    B = bucket_type(alg)
    _allocate_output(B, args...)
end

allocate_output(alg::AbstractBucketAlgorithm, ::typeof(sum), args...) =
    _allocate_output(SumBucket, args...)

allocate_output(alg::AbstractBucketAlgorithm, ::typeof(mean), args...) =
    _allocate_output(CountBucket, args...)

function _allocate_output(B, X, y::AbstractArray{T}, bins) where {T}
    dims = length(bins)
    B(T, dims)
end

function _allocate_output(B, X1, X2, y::AbstractArray{T}, bins1, bins2) where {T}
    dims = (length(bins1), length(bins2))
    B(T, dims)
end

upsert!(b::AbstractValueBucket, _, _, _) = error("Not implemented for $(typeof(b))")
Base.size(b::AbstractValueBucket) = error("Not implemented for $(typeof(b))")
unpack_result(b::AbstractValueBucket; kwargs...) = error("Not implemented for $(typeof(b))")

"""
Count as well as summing output
"""
struct CountBucket{T} <: AbstractValueBucket{T}
    ncounts::Vector{Int}
    output::Vector{T}
end

CountBucket(::Type{T}, dims) where {T} = CountBucket(zeros(Int, dims), zeros(T, dims))

@inline @inbounds function upsert!(b::CountBucket, bin, i, yᵢ)
    b.output[bin] += yᵢ
    b.ncounts[bin] += 1
end

Base.size(b::CountBucket) = size(b.output)
unpack_result(b::CountBucket) = @. b.output / b.ncounts

"""
Just sum the output
"""
struct SumBucket{T} <: AbstractValueBucket{T}
    output::Vector{T}
end

SumBucket(::Type{T}, dims) where {T} = SumBucket(zeros(T, dims))

@inline @inbounds function upsert!(b::SumBucket, bin, i, yᵢ)
    b.output[bin] += yᵢ
end

Base.size(b::SumBucket) = size(b.output)
unpack_result(b::SumBucket) = b.output

"""
Track which index the elements should go into
"""
struct IndexBucket{T} <: AbstractValueBucket{T}
    indices::Vector{Int}
end

IndexBucket(::Type{T}, dims) where {T} = IndexBucket{T}(zeros(Int, dims))

@inline @inbounds function upsert!(b::IndexBucket, bin, i, _)
    b.indices[i] = bin
end

Base.size(b::IndexBucket) = size(b.indices)
unpack_result(b::IndexBucket) = b.indices

bucket_type(::Type{<:AbstractBucketAlgorithm}) = CountBucket
bucket_type(alg::A) where {A<:AbstractBucketAlgorithm} = bucket_type(A)
