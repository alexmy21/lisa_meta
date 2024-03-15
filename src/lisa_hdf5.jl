include("lisa_sets.jl")
include("lisa_util.jl")

using HDF5
using DataFrames
using SQLite
using FilePathsBase
using CSV
using Arrow
using Tables
using Dates

SetCore.HllSet{P}(::Type{T}) where {P, T} = SetCore.HllSet{10}()

# Working with HDF5 files
#--------------------------------------------------
function save(file_name::String, group_name::String, dataset_name::String, 
    dataset::Vector{UInt64}; attributes::Dict = Dict()) 
    
    h5open(file_name, "w") do file
        if haskey(file, group_name) 
            g = file[group_name]                
        else
            # Create a new group in the file
            g = create_group(file, group_name)
        end
        g[dataset_name] = dataset
        if isempty(attributes)
            return
        end
        for(key, value) in attributes
            attrs(g[dataset_name])[key] = value
        end 
    end     
end

function save(file_name::String, group_name::String, dataset_name::String, 
    dataset::Vector{String}; attributes::Dict = Dict())

    h5open(file_name, "w") do file
        # Check if the group already exists in the file
        if haskey(file, group_name) 
            g = file[group_name]                
        else
            # Create a new group in the file, if it doesn't exist
            g = create_group(file, group_name)
        end
        g[dataset_name] = dataset
        # Create attributes
        if isempty(attributes)
            return
        end
        for(key, value) in attributes
            attrs(g[dataset_name])[key] = value
        end 
    end    
end

function save(file_name::String, group_name::String, dataset_name::String, 
    dataset::String; attributes::Dict = Dict())

    h5open(file_name, "w") do file
        # Check if the group already exists in the file
        if haskey(file, group_name) 
            g = file[group_name]                
        else
            # Create a new group in the file, if it doesn't exist
            g = create_group(file, group_name)
        end
        g[dataset_name] = dataset
        # Create attributes
        if isempty(attributes)
            return
        end
        for(key, value) in attributes
            attrs(g[dataset_name])[key] = value
        end 
    end    
end

# Function to recursively read datasets from an HDF5 file or group that match a wildcard
function read_datasets(file_or_group, wildcard)
    for name in keys(file_or_group)
        item = file_or_group[name]
        if isa(item, HDF5.Dataset) && occursin(wildcard, string(item))
            data = read(item)
            println("Read dataset '$name' with data: $data")
        elseif isa(item, HDF5.Group)
            read_datasets(item, wildcard)
        end
    end
end

function retrieve(hdf5_file::String, hdf5_path::String)    
    h5open(hdf5_file, "r") do file
        hll_d = read(file[hdf5_path])        
        return hll_d
    end
end