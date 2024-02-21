# include("lisa_mycore.jl")
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
