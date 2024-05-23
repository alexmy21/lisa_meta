include("lisa_neo4j.jl")

module LisaMeta
    using ..SetCore
    using ..Store
    using ..Graph
    using ..LisaNeo4j

    using SQLite
    using DBInterface
    using MurmurHash3
    using TextAnalysis
    using JSON3
    using PooledArrays
    using HTTP, JSON3
    using Base64
    using DataFrames
    using EasyConfig

    # Getting emails by date
    function get_emails_by_date(db, date)
        return DBInterface.execute(db, "SELECT * FROM emails WHERE Date LIKE '$date%'") |> DataFrame
    end

    function get_emails_by_date(db, date, fields)
        return DBInterface.execute(db, "SELECT $fields FROM emails WHERE Date LIKE '$date%'") |> DataFrame
    end

    function get_emails_by_date(db, date, fields, limit)
        return DBInterface.execute(db, "SELECT $fields FROM emails WHERE Date LIKE '$date%' LIMIT $limit") |> DataFrame
    end

    # Ingesting email data into the store column by column
    # db::Graph.DB, file::Graph.Assignment, lock_uuid::String; p::Int=10, limit::Int=-1, offset::Int=0, seed::Int=0
    function ingest_df_by_column(db::Graph.DB, df::DataFrame, parent_sha1::String; p::Int = 10)
        # Create a new HLL set
        hll = SetCore.HllSet{p}()
        for col_name in names(df)
            col = df[!, col_name]
            col_props = Config()
            col_props["column_name"] = col_name
            col_props["column_type"] = eltype(col)
            # col_props["parent_sha1"] = parent_sha1
            
            node_sha1 = bytes2hex(Store.sha1(string(col_name, col_props["column_type"], parent_sha1)))
            
            col_hll = Store.ingest_dataset(db, Set(col), node_sha1, ["column"], col_props, p)
            # Accumulate the column into the HLL set for current day
            hll = SetCore.union!(hll, col_hll)
        end

        return hll
    end

    # Ingesting email data into the store row by row
    # db::Graph.DB, file::Graph.Assignment, lock_uuid::String; p::Int=10, limit::Int=-1, offset::Int=0, seed::Int=0
    function ingest_df_by_row(db::Graph.DB, df::DataFrame, parent_sha1::String; p::Int = 10)
        # Create a new HLL set
        hll = SetCore.HllSet{p}()
        for i in 1:size(df, 1)
            row = df[i, :]
            row_props = Config()
            # row_props["Message_ID"] = df[i, :Message-ID]
            # row_props["parent_sha1"] = parent_sha1
            row_props["Date"] = df[i, :Date]
            row_props["From"] = df[i, :From]
            row_props["To"] = df[i, :To]
            mess_id = ""
            try
                mess_id = df[i, Symbol("message_id")]
            catch
                mess_id = df[i, Symbol("Message-ID")]
            end
            node_sha1 = bytes2hex(Store.sha1(string(mess_id, row_props["Date"], row_props["From"], row_props["To"])))

            row_hll = Store.ingest_dataset(db, Set(row), node_sha1, ["row"], row_props, p)
            # Accumulate the row into the HLL set for current day
            hll = SetCore.union!(hll, row_hll)
        end

        return hll
    end

    function select_nodes_by_label(db::Graph.DB, label::String, table::String, n::Int)
        refs = Set()
        query = ""
        if n == -1
            query = "SELECT * FROM $table WHERE labels LIKE '%$label%'"
        else
            query = "SELECT * FROM $table WHERE labels LIKE '%$label%' ORDER BY RANDOM() LIMIT $n"
        end
        
        results = DBInterface.execute(db, query) |> DataFrame
        for result in eachrow(results)
            if length(result) > 0
                push!(refs, result.sha1)
                refs = union(refs, Set([result.sha1]))
            end
        end
        return refs
    end

    function select_nodes_by_query(db::Graph.DB, query::String, n::Int)
        refs = Set()
        # query = ""
        if n > 0
            query = query * " LIMIT $n"
        end
        
        results = DBInterface.execute(db, query) |> DataFrame
        for result in eachrow(results)
            if length(result) > 0
                push!(refs, result.sha1)
                refs = union(refs, Set([result.sha1]))
            end
        end
        return refs
    end

    function print_hdf5_tree(obj, indent="", limit = 100)
        i = 0
        for name in names(obj)
            if i > limit
                println(indent, "...")
                break
            end
            child = obj[name]
            println(indent, name)
            if isa(child, HDF5Group)
                print_hdf5_tree(child, indent * "    ")
            end
            i += 1
        end
    end

    function merge_edge(db::Graph.DB, from::String, to::String, edge::String, props::Dict, url::String, headers::Dict)
        props = JSON3.write(props)
        dict = Dict{String, Any}("source" => "$from", "target" => "$to", "r_type" => "$edge", "props" => props)
        df_row = LisaNeo4j.dict_to_dfrow(dict)
        query = LisaNeo4j.add_neo4j_edge(df_row)
        data = LisaNeo4j.request(url, headers, query)
    end

end # module
