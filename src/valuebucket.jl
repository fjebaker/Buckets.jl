abstract type AbstractValueBucket{T} end

_bucket_for_reduction(::AbstractBucketAlgorithm, ::typeof(sum)) = AggregateBucket
_bucket_for_reduction(::AbstractBucketAlgorithm, ::typeof(mean)) = CountBucket
_bucket_for_reduction(alg::AbstractBucketAlgorithm, ::Nothing) = bucket_type(alg)

allocate_output(alg::AbstractBucketAlgorithm, reduction, args...) =
    _allocate_output(_bucket_for_reduction(alg, reduction), args...)
allocate_output(alg::AbstractThreadedBucketAlgorithm, reduction, args...) =
    _allocate_output(ThreadBuckets{_bucket_for_reduction(alg, reduction)}, args...)

function _allocate_output(B, X::AbstractArray{T}, bins) where {T}
    dims = length(bins)
    B(T, dims)
end

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
unpack_bucket(b::AbstractValueBucket; kwargs...) = error("Not implemented for $(typeof(b))")
function Base.merge!(b::T, bs::Vararg{T}) where {T}
    fields = fieldnames(T)
    for item in bs
        for f in fields
            getproperty(b, f) .+= getproperty(item, f)
        end
    end
    b
end

struct ThreadBuckets{B,T} <: AbstractValueBucket{T}
    buckets::Vector{B}
    @inbounds function ThreadBuckets(
        B::Type{<:AbstractValueBucket},
        args...;
        nthreads = Threads.nthreads(),
    )
        bucket = B(args...)
        T = typeof(bucket)
        # allocate thread local storage
        buckets = Vector{T}(undef, nthreads)
        buckets[1] = bucket
        for i = 2:nthreads
            buckets[i] = B(args...)
        end
        new{T,T.parameters[1]}(buckets)
    end
end

@inline function ThreadBuckets{B}(args...; kwargs...) where {B}
    ThreadBuckets(B, args...; kwargs...)
end

@inline @inbounds function upsert!(b::ThreadBuckets, bin, i, yᵢ)
    upsert!(b.buckets[Threads.threadid()], bin, i, yᵢ)
end

Base.size(b::ThreadBuckets) = @inbounds size(b.buckets[1])
@inbounds function unpack_bucket(b::ThreadBuckets)
    out_bucket = b.buckets[1]
    for i = 2:length(b.buckets)
        merge!(out_bucket, b.buckets[i])
    end
    unpack_bucket(out_bucket)
end

"""
Count as well as summing output
"""
struct CountBucket{T,Tcount} <: AbstractValueBucket{T}
    output::T
    ncounts::Tcount
end

CountBucket(::Type{T}, dims) where {T} = CountBucket(zeros(Int, dims), zeros(T, dims))

@inline @inbounds function upsert!(b::CountBucket, bin, i, yᵢ)
    b.output[bin] += yᵢ
    b.ncounts[bin] += 1
end

Base.size(b::CountBucket) = size(b.output)
function unpack_bucket(b::CountBucket) 
    [n > 0 ? o / n : 0 for (n, o) in zip(b.output, b.ncounts)]
end

"""
Just sum the output
"""
struct AggregateBucket{T} <: AbstractValueBucket{T}
    output::T
end

AggregateBucket(::Type{T}, dims) where {T} = AggregateBucket(zeros(T, dims))

@inline @inbounds function upsert!(b::AggregateBucket, bin, i, yᵢ)
    b.output[bin] += yᵢ
end

Base.size(b::AggregateBucket) = size(b.output)
unpack_bucket(b::AggregateBucket) = b.output

"""
Track which index the elements should go into
"""
struct IndexBucket{T} <: AbstractValueBucket{T}
    indices::T
end

IndexBucket(::Type{T}, dims) where {T} = IndexBucket{T}(zeros(Int, dims))

@inline @inbounds function upsert!(b::IndexBucket, bin, i, _)
    b.indices[i] = bin
end

Base.size(b::IndexBucket) = size(b.indices)
unpack_bucket(b::IndexBucket) = b.indices

bucket_type(::Type{<:AbstractBucketAlgorithm}) = AggregateBucket
bucket_type(::A) where {A<:AbstractBucketAlgorithm} = bucket_type(A)
bucket_type(B::AbstractThreadedBucketAlgorithm) = bucket_type(_algorithm(B))


export ThreadBuckets, AggregateBucket, CountBucket, IndexBucket, unpack_bucket
