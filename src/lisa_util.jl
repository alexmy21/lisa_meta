# include("lisa_mycore.jl")
module Util
    using SHA
    using DataFrames

    export sha1, ints_to_bits, bits_to_ints, l_hash

    # SHA1 hash function for Vector{UInt64}
    #--------------------------------------------------
    function sha1(x::Vector{UInt64})
        # Create a SHA1 hash object
        h = SHA1()
        # Update the hash object with the input
        for i in 1:length(x)
            update!(h, reinterpret(UInt8, x[i]))
        end
        # Return the hash
        return digest(h)
    end 

    function sha1(x::Vector{String})
        # Create a SHA1 hash object
        h = SHA1()
        # Update the hash object with the input
        for i in 1:length(x)
            update!(h, x[i])
        end
        # Return the hash
        return digest(h)
    end

    # Support for calculating sha1 for union and intersection of strings
    #--------------------------------------------------
    function char_to_bin(c::Char)
        return string(UInt8(c), base=2)
    end

    function string_to_bin(str)
        return join([char_to_bin(c) for c in str])
    end

    function bin_to_string(bin_str)
        return join([Char(parse(UInt8, bin_str[i:min(i+7, end)] , base=2)) for i in 1:8:length(bin_str)])
    end

    function sha1_union(strings::Array{String, 1})
        bin_strings = [string_to_bin(str) for str in strings]
        bin_union = bin_strings[1]
        for i in 2:length(bin_strings)
            bin_union = string(parse(BigInt, "0b" * bin_union) | parse(BigInt, "0b" * bin_strings[i]), base=2)
        end
        str_union = bin_to_string(bin_union)
        new_sha1_hash = bytes2hex(SHA.sha1(str_union))

        return new_sha1_hash
    end

    function sha1_intersect(strings::Array{String, 1})
        bin_strings = [string_to_bin(str) for str in strings]
        bin_intersect = bin_strings[1]
        for i in 2:length(bin_strings)
            bin_intersect = string(parse(BigInt, "0b" * bin_intersect) & parse(BigInt, "0b" * bin_strings[i]), base=2)
        end
        str_intersect = bin_to_string(bin_intersect)
        new_sha1_hash = bytes2hex(SHA.sha1(str_intersect))

        return new_sha1_hash
    end

    function sha1_comp(sha_1::String, sha_2::String)  
        bin_1 = string_to_bin(sha_1)
        bin_2 = string_to_bin(sha_2)
        bin_comp = string(parse(BigInt, "0b" * bin_1) & ~parse(BigInt, "0b" * bin_2), base=2)
        str_comp = bin_to_string(bin_comp)
        new_sha1_hash = bytes2hex(SHA.sha1(str_comp))
        
        return new_sha1_hash
    end

    function sha1_xor(sha_1::String, sha_2::String)  
        bin_1 = string_to_bin(sha_1)
        bin_2 = string_to_bin(sha_2)
        bin_xor = string(xor(parse(BigInt, "0b" * bin_1), parse(BigInt, "0b" * bin_2)), base=2)
        str_xor = bin_to_string(bin_xor)
        new_sha1_hash = bytes2hex(SHA.sha1(str_xor))
        
        return new_sha1_hash
    end

    function sha1_union(strings::Set)
        arr = collect(strings)
        return sha1_union(arr)
    end

    function sha1_intersect(strings::Set)
        arr = collect(strings)
        return sha1_intersect(arr)
    end
end # module