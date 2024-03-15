# 
# This module is greatly inspired by the implementation of the HLL algorithm in the Julia library:
# 
# Below is the header from this file
# From Flajolet, Philippe; Fusy, Éric; Gandouet, Olivier; Meunier, Frédéric (2007)
# DOI: 10.1.1.76.4286
# With algorithm improvements by Google (https://ai.google/research/pubs/pub40671)

# Principle:
# When observing N distinct uniformly distributed integers, the expected maximal
# number of leading zeros in the integers is log(2, N), with large variation.
# To cut variation, we keep 2^P counters, each keeping track of N/2^P
# observations. The estimated N for each counter is averaged using harmonic mean.
# Last, corrections for systematic bias are added, one multiplicative and one
# additive factor.
# To make the observations uniformly distributed integers, we hash them.

# We made sugnificant changes to the original implementation:
# - We use a BitVector instead of a UInt8 for the counters
# - We implemented additional operators to support set operations, like union (union), intersection(intersect), difference(diff), 
#   and equality (isequal). Now they work the same way as they work for sets
# - we added a function to convert the BitVector to a UInt64 (dump) and vice versa (restore)
# - We added a function to calculate the delta between two HLL sets (delta)
# - We added a function to calculate SHA1 of the counts as a string
# - We also renamed some of the original operators to be more consistent with HyperLogLog terminology
"""
MIT License

Copyright (c) 2023: Jakob Nybo Nissen.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

https://github.com/jakobnissen/Probably.jl/blob/master/src/hyperloglog/hyperloglog.jl

I borrowed a lot from this project, but also made a lot of changes, 
so, for all errors do not blame the original author but me.
"""

module SetCore

    include("constants.jl")
    using SHA
    using DataFrames
    using CSV
    using Arrow
    using Tables

    export HllSet, add!, count, union, intersect, diff, 
        isequal, isempty, id, delta, getbin, getzeros, maxidx

    struct HllSet{P}
        counts::Vector{BitVector}

        function HllSet{P}() where {P}
            validate_P(P)
            n = calculate_n(P)
            counts = init(n, P)
            return new(counts)
        end

        function validate_P(P)
            isa(P, Integer) || throw(ArgumentError("P must be integer"))
            (P < 4 || P > 18) && throw(ArgumentError("P must be between 4 and 18"))
        end

        function calculate_n(P)
            return 1 << P
        end

        function init(n, P)
            return [falses(64 - 1) for _ in 1:n]
        end
    end

    Base.show(io::IO, x::HllSet{P}) where {P} = print(io, "HllSet{$(P)}()")

    Base.sizeof(::Type{HllSet{P}}) where {P} = 1 << P
    Base.sizeof(x::HllSet{P}) where {P} = sizeof(typeof(x))

    function Base.union!(dest::HllSet{P}, src::HllSet{P}) where {P}
        length(dest.counts) == length(src.counts) || throw(ArgumentError("HllSet{P} must have same size"))
        for i in 1:length(dest.counts)
            dest.counts[i] = dest.counts[i] .| src.counts[i]
        end
        return dest
    end

    function Base.copy!(dest::HllSet{P}, src::HllSet{P}) where {P}
        length(dest.counts) == length(src.counts) || throw(ArgumentError("HllSet{P} must have same size"))
        for i in 1:length(dest.counts)
            dest.counts[i] = src.counts[i]
        end
        return dest
    end

    function Base.union(x::HllSet{P}, y::HllSet{P}) where {P} 
        length(x.counts) == length(y.counts) || throw(ArgumentError("HllSet{P} must have same size"))
        z = HllSet{P}()
        for i in 1:length(x.counts)
            z.counts[i] = x.counts[i] .| y.counts[i]
        end
        return z
    end

    function Base.intersect(x::HllSet{P}, y::HllSet{P}) where {P} 
        length(x.counts) == length(y.counts) || throw(ArgumentError("HllSet{P} must have same size"))
        z = HllSet{P}()
        for i in 1:length(x.counts)
            z.counts[i] = x.counts[i] .& y.counts[i]
        end
        return z
    end

    function Base.diff(x::HllSet{P}, y::HllSet{P}) where {P} 
        length(x.counts) == length(y.counts) || throw(ArgumentError("HllSet{P} must have same size"))
        z = HllSet{P}()
        for i in 1:length(x.counts)
            z.counts[i] = x.counts[i] .& .~(y.counts[i])
        end
        return z
    end

    function Base.isequal(x::HllSet{P}, y::HllSet{P}) where {P} 
        length(x.counts) == length(y.counts) || throw(ArgumentError("HllSet{P} must have same size"))
        for i in 1:length(x.counts)
            x.counts[i] == y.counts[i] || return false
        end
        return true
    end

    Base.isempty(x::HllSet{P}) where {P} = all(x -> x == 0,  x.counts)

    function getbin(hll::HllSet{P}, x::Int) where {P} 
        # println("P = ", P)
        # Increasing P by 1 to compensate BitVector size that is of size 64
        x = x >>> (8 * sizeof(UInt) - (P + 1)) + 1
        str = replace(string(x, base = 16), "0x" => "")
        return parse(Int, str, base = 16)
    end

    function getzeros(hll::HllSet{P}, x::Int) where {P}
        or_mask = ((UInt(1) << P) - 1) << (8 * sizeof(UInt) - P)
        return trailing_zeros(x | or_mask) + 1
    end

    function add!(hll::HllSet{P}, x::Any; seed::Int = 0) where {P}
        # println("seed = ", seed, "; P = ", P, "; x = ", x)
        h = u_hash(x; seed=seed)
        bin = getbin(hll, h)
        idx = getzeros(hll, h)
        hll.counts[bin][idx] = true
        # return hll
    end

    function add!(hll::HllSet{P}, values::Union{Set, Vector}) where {P}
        for value in values
            add!(hll, value)
        end
        # return hll
    end

    α(x::HllSet{P}) where {P} =
        if P == 4
            return 0.673
        elseif P == 5
            return 0.697
        elseif P == 6
            return 0.709
        else
            return 0.7213 / (1 + 1.079 / sizeof(x))
        end
    
    
        function bias(::HllSet{P}, biased_estimate) where {P}
        # For safety - this is also enforced in the HLL constructor
        if P < 4 || P > 18
            error("We only have bias estimates for P ∈ 4:18")
        end
        rawarray = @inbounds RAW_ARRAYS[P - 3]
        biasarray = @inbounds BIAS_ARRAYS[P - 3]
        firstindex = searchsortedfirst(rawarray, biased_estimate)
        # Raw count large, no need for bias correction
        if firstindex == length(rawarray) + 1
            return 0.0
            # Raw count too small, cannot be corrected. Maybe raise error?
        elseif firstindex == 1
            return @inbounds biasarray[1]
            # Else linearly approximate the right value for bias
        else
            x1, x2 = @inbounds rawarray[firstindex - 1], @inbounds rawarray[firstindex]
            y1, y2 = @inbounds biasarray[firstindex - 1], @inbounds biasarray[firstindex]
            delta = @fastmath (biased_estimate - x1) / (x2 - x1) # relative distance of raw from x1
            return y1 + delta * (y2 - y1)
        end
    end

    function maxidx(vec::BitVector)        
        for i in length(vec):-1:1
            if vec[i]
                return i
            end
        end
        return 0
    end

    function Base.count(x::HllSet{P}) where {P}
        # Harmonic mean estimates cardinality per bin. There are 2^P bins
        harmonic_mean = sizeof(x) / sum(1 / 1 << maxidx(i) for i in x.counts)
        biased_estimate = α(x) * sizeof(x) * harmonic_mean
        return round(Int, biased_estimate - bias(x, biased_estimate))
    end

    function jaccard(hll_1::HllSet{P}, hll_2::HllSet{P}) where {P}
        x = count(union(hll_1, hll_2))
        n = count(intersect(hll_1, hll_2))
        return round(Int64, ((n / x) * 100))
    end

    function delta(hll_1::HllSet{P}, hll_2::HllSet{P}) where {P}
        x = count(intersect(hll_1, hll_2))
        d = count(hll_1) - x
        r = x
        n = count(hll_2) - x
        return (D = d, R = r, N = n)
    end

    function id(x::HllSet{P}) where {P}
        # Convert the Vector{BitVector} to a byte array
        bytearray = UInt8[]
        for bv in x.counts
            append!(bytearray, reinterpret(UInt8, bv))
        end
        # Calculate the SHA1 hash
        hash_value = SHA.sha1(bytearray)
        return SHA.bytes2hex(hash_value)
    end

    # dump and restor functions
    #--------------------------------------------------
    function Base.dump(x::SetCore.HllSet{P}) where {P}
        # For safety - this is also enforced in the HLL constructor
        if P < 4 || P > 18
            error("We only have bias estimates for P ∈ 4:18")
        end
        z = Vector{UInt64}(undef, length(x.counts))
        for i in 1:length(x.counts)
            n = bits_to_ints(x.counts[i])
            z[i] = n
        end
        return z
    end

    function restore(z::SetCore.HllSet{P}, x::Vector{UInt64}) where {P} 
        # For safety - this is also enforced in the HLL constructor
        if P < 4 || P > 18
            error("We only have bias estimates for P ∈ 4:18")
        end
        if length(x) != length(z.counts)
            error("The length of the vector must be equal to the length of the HllSet")
        end
        for i in 1:length(x)
            y = ints_to_bits(x[i])
            z.counts[i] = z.counts[i] .| y
        end
        return z
    end

    # The following functions are used to convert between UInt64 and BitVector
    #--------------------------------------------------
    function ints_to_bits(integer::UInt64)
        # Convert the integer to a binary string
        binary_string = string(integer, base=2)
        # Pad the binary string to 64 bits
        binary_string = lpad(binary_string, 64 -1 , '0')
        # Create a BitVector from the binary string
        return BitVector(parse.(Bool, collect(binary_string)))
    end

    function bits_to_ints(vec::BitVector)
        # Convert the BitVector to a binary string
        binary_string = join(Int.(vec))
        # Convert the binary string to an integer
        return parse(UInt64, binary_string, base=2)
    end

    function u_hash(x; seed::Int=0) 
        if seed == 0
            abs_hash = abs(hash(x))
        else
            abs_hash = abs(hash(hash(x) + seed))
        end         
        return Int(abs_hash % typemax(Int64))
    end

    function bit_indices(n)
        binary_representation = reverse(digits(n, base=2))
        return findall(x -> x == 1, binary_representation)
    end

    function apply_fn(x, fn)
        return fn(x)
    end
end

