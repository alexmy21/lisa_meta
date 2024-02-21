include("lisa_sets.jl")
include("lisa_util.jl")
include("lisa_graph.jl")

module Store

    using ..Graph
    using ..SetCore

    using SQLite
    using DataFrames
    using FilePathsBase
    using SHA
    using CSV
    using EasyConfig
    using TextAnalysis
    using JSON3
    using PooledArrays
    using UUIDs
    using HDF5

    export book_file, book_file, ingest_csv

    function has_header(filename::String)
        first_row = first(CSV.File(filename), 1)
        return all(x -> isa(x, String), values(first_row))
    end

    """
        intake_csv(db::Graph.DB, start_dir::String, hll::SetCore.HllSet; ext::String="csv") 
        It takes a database, a directory and a hyperloglog set and processes all the csv files in the directory.
        1. It updates the tokens table with the data from the csv files
        2. It books the columns in the csv files in the assignments table
        3. It books the csv files in the assignments table
    """
    function book_file(db::Graph.DB, start_dir::String, hll::SetCore.HllSet; ext::String="csv") 
        # Update tokens table 
        df = DataFrame(:id=>Int[], :bin=>Int[], :zeros=>Int[], :token=>String[], :refs=>String[])
        # Walk through the directory and its subdirectories
        for (root, dirs, files) in walkdir(Path(start_dir))
            # Check each file
            for file in files                
                # If the file is a CSV file, add its path to the array
                if extension(Path(file)) == ext
                    # Register (book) the file in the assignmnets table
                    f_name = joinpath(root, file)
                    sha_1 = bytes2hex(sha1(f_name))
                    assign = Graph.Assignment(sha_1, root, f_name, ext, "book_file", "", "waiting")
                    Graph.replace!(db, assign)

                    dataset = split(f_name,"/")
                    update_tokens(db, hll, dataset, sha_1)
                    # Book (register) the csv file columns in the assignments table
                    book_column(db, hll, f_name, sha_1)
                end
            end
        end
    end

    function book_column(db::Graph.DB, hll::SetCore.HllSet, csv_filename::String, file_sha1::String)        
        file_name = abspath(csv_filename)
        # get file extentions
        ext = extension(Path(file_name))
        # Read the csv file
        df_csv = CSV.read(file_name, DataFrame; limit=10)
        column_names = names(df_csv)
        column_types = eltype.(eachcol(df_csv))
        
        # Iterate over the columns
        for i in eachindex(column_names)
            # Get the column name and type
            column_name = string(column_names[i])
            column_type = string(column_types[i])
            
            sha_1 = bytes2hex(sha1(file_name * column_name * column_type))
            assign = Graph.Assignment(sha_1, file_sha1, column_name, column_type, "book_column", "", "waiting")
            Graph.replace!(db, assign)
            dataset = [column_name, column_type]
            # Update tokens table
            update_tokens(db, hll, dataset, sha_1)            
        end
    end
    
    function update_tokens(db::Graph.DB, hll::SetCore.HllSet{P}, dataset, sha_1::String) where {P}
        # println("Updating tokens table")
        i = 0
        dataset = collect(skipmissing(dataset))
        df = DataFrame(:id=>Int[], :bin=>Int[], :zeros=>Int[], :token=>String[], :refs=>String[])
        for item in dataset
            if is_number(item)
                continue
            end
            tokens = tokenize(item)
            for token in tokens
                try                    
                    if isempty(token) || is_number(token) || length(token) < 3
                        continue
                    end                    
                    SetCore.add!(hll, token)    
                    update_token(db, df, hll, token, sha_1)
                catch e
                    i += 1
                    if i < 20
                        println("update_tokens ERROR on $item, Error msg: $e")
                    end    
                end
            end
        end
        SQLite.load!(df, db.sqlitedb, "tokens", replace=true)
        
        return hll
    end

    function update_token(db::Graph.DB, df::DataFrame, hll::SetCore.HllSet{P}, item, sha_1::String; table_name::String="tokens") where {P}        
        h = SetCore.u_hash(item)
        # Retrieve the set from the table
        row = DBInterface.execute(db.sqlitedb, "SELECT * FROM $table_name WHERE id = $h") |> DataFrame
        item_set = Set([item])
        sha1_set = Set([sha_1])        
        if isempty(row)
            # Do nothing
        else
            try
                retrieved_items = Graph.json_to_set(row[1, "token"])
                retrieved_sha1 = Graph.json_to_set(row[1, "refs"])
                item_set = retrieved_items ∪ item_set
                sha1_set = retrieved_sha1 ∪ sha1_set
            catch e
                println("update_token ERROR on $item_set or $sha1_set, Error msg: $e")
            end
        end
        # Token(id::Int, bin::Int, zeros::Int; token::Set{String}, refs::Set{String})
        token = Graph.Token(h, SetCore.getbin(hll, h), SetCore.getzeros(hll, h), 
            JSON3.write(collect(item_set)), JSON3.write(collect(sha1_set)))        
        dict = Graph.getdict(token)
        push!(df, dict)
        return df  
    end

    """
        This function reads a whole CSV file to SQLite DB . May cause memory overflow with very big tables,
        but it is slightly more performant.

        ingest_csv(db::Graph.DB, assign::Assignmnet, p::Int) 
        It takes a database, an assignment that points to csv file and a precision parameter.
        1. query all references to columns from the csv file
        2. for each column, it processes the data and updates the tokens table and the nodes table
        3. At the end of the process, it updates the status of the assignment to "completed" and
        create a new node in the nodes with the SHA1 hash of the csv file as the id, and edges in edges tables.

        Arguments limit and offset are used to read the CSV file in chunks. 
        Can be used to simulate different loads with the same files.
    """    
    function _ingest_csv(db::Graph.DB, file::Graph.Assignment, lock_uuid::String; p::Int=10, limit::Int=-1, offset::Int=0)         
        # db::DB, parent::String, type::String, processor_id::String, status::String, lock_uuid::String; result::Bool=false
        assign_df = Graph.set_lock!(db, file.id, :, "book_column", "ingest_csv", "waiting", "waiting", lock_uuid; result=true)
        # Create initial csv file record in t_nodes table.
        # We need it to be able to create edges between the file and its columns
        file_node = Graph.Node(file.id, [file.type], "", 0, Vector(), Config())
        Graph.replace!(db, file_node, table_name="t_nodes")

        if isempty(assign_df) 
            println("The file $file.item is already being processed")
            return
        end        
        file_name = abspath(file.item)
        file_df = CSV.read(file_name, DataFrame; missingstring = "", skipto=offset+1, limit=limit, silencewarnings=true)
        file_hll = SetCore.HllSet{p}()
        file_props = Config()
        file_props["sha1_arg"] = file_name
        file_props["labels"] = file.type
        # Iterate over the columns
        i = 0
        for assign in eachrow(assign_df)
            col_props = Config()
            col_props["column_name"] = assign.item
            col_props["file_sha1"] = file.id
            col_props["column_type"] = assign.type
            # Process the column data depending on the column type
            if assign.type == Missing
                # println("Processing integer data for column $assign.item")
            elseif assign.type == Int64
                # println("Processing real number data for column $assign.item")
            elseif assign.type == "String"
                column_name = assign.item
                # 
                # dataset = df[!, column_name]
                dataset = Set(file_df[!, column_name])         
                col_hll = ingest_column(db, dataset, assign.id, [assign.item], col_props, p)
                SetCore.union!(file_hll, col_hll)
                # Create and add to t_edge table a new edge between the file and the column
                edge_props = Config()
                edge_props["source"] = file.item
                edge_props["target"] = assign.item
                edge = Graph.Edge(file.id, assign.id, "has_column", edge_props)
                # Update "t_edge" table
                Graph.replace!(db, edge; table_name="t_edges")
                i = i + 1
            else
                # Process other types of data
            end
            Graph.unset_lock!(db, assign.id, :, "completed")
        end
        println("Processed column: $i")
        card = SetCore.count(file_hll)
        file_dump = SetCore.dump(file_hll)
        sha1_d = SetCore.id(file_hll)
        file_node = Graph.Node(file.id, [file.type], sha1_d, card, file_dump, file_props)
        Graph.replace!(db, file_node, table_name="t_nodes")

        Graph.unset_lock!(db, file.id, :, "completed")
    end

    """
        This function is almost the same as _ingest_csv, but reads a CSV file to SQLite DB column by column. 
        Suppose to save some memory.
    """
    function ingest_csv(db::Graph.DB, file::Graph.Assignment, lock_uuid::String; p::Int=10, limit::Int=-1, offset::Int=0) 
        assign_df = Graph.set_lock!(db, file.id, :, "book_column", "ingest_csv", "waiting", "waiting", lock_uuid; result=true)
        if isempty(assign_df) 
            println("The file $file.item is already being processed")
            return
        end
        # Create initial csv file record in t_nodes table.
        # We need it to be able to create edges between the file and its columns
        file_node = Graph.Node(file.id, [file.a_type], "", 0, Vector(), Config())
        Graph.replace!(db, file_node, table_name="t_nodes")
        file_name = abspath(file.item)
        csv_data = CSV.File(file_name, skipto=offset+1, limit=limit, silencewarnings=true) # Read the CSV file into a Tables.jl compatible object
        db_memory = SQLite.DB(":memory:") # Create an SQLite in-memory database
        SQLite.load!(csv_data, db_memory, "file_df") # Load the CSV data into the in-memory database
        file_hll = SetCore.HllSet{p}()
        file_props = Config()
        file_props["file_name"] = file_name
        file_props["file_type"] = file.a_type
        i = 0
        for assign in eachrow(assign_df)
            col_props = Config()
            col_props["column_name"] = assign.item
            col_props["file_sha1"] = file.id
            col_props["column_type"] = assign.a_type
            if assign.a_type == Missing
                # println("Processing integer data for column $assign.item")
            elseif assign.a_type == Int64
                # println("Processing real number data for column $assign.item")
            elseif assign.a_type == "String"
                try
                    column_name = assign.item
                    dataset_df = DBInterface.execute(db_memory, """SELECT DISTINCT "$column_name" FROM file_df""") |> DataFrame
                    dataset = Set(dataset_df[!, column_name])
                    """
                        Here is the place where we can make decisions about the labels of future graph node
                        representing column. For now we are using a constant "csv_column" label.
                    """
                    col_hll = ingest_column(db, dataset, assign.id, ["csv_column"], col_props, p)
                    # Update the file hyperloglog set with the column hyperloglog set
                    SetCore.union!(file_hll, col_hll)
                    # Create and add to t_edge table a new edge between the file and the column
                    edge_props = Config()
                    edge_props["source"] = file.item
                    edge_props["target"] = assign.item
                    edge_props["source_label"] = "csv_file"
                    edge_props["target_label"] = "csv_column"
                    edge = Graph.Edge(file.id, assign.id, "has_column", edge_props)
                    # Update "t_edge" table
                    Graph.replace!(db, edge; table_name="t_edges")
                    i = i + 1
                catch e
                    column_name = assign.item
                    println("ingest_csv ERROR on $column_name, Error msg: $e")
                end
            else
                # Process other types of data
            end
            Graph.unset_lock!(db, assign.id, :, "completed")
        end
        println("Processed column: $i")
        card = SetCore.count(file_hll)
        file_dump = SetCore.dump(file_hll)
        sha1_d = SetCore.id(file_hll)
        """
            Here is the place where we can make desicions about the labels of the future graph node
            representing file. For now we are using a constant "csv_file" label.
        """
        file_node = Graph.Node(file.id, ["csv_file"], sha1_d, card, file_dump, file_props)
        Graph.replace!(db, file_node, table_name="t_nodes")

        Graph.unset_lock!(db, file.id, :, "completed")
    end

    # Utility functions
    #--------------------------------------------------
    function ingest_column(db::Graph.DB, dataset::Union{Vector, PooledVector, Set}, col_sha1::String, labels, props::Config, p::Int64)
        # Update tokens table 
        # df = DataFrame(:id=>Int[], :bin=>Int[], :zeros=>Int[], :token=>String[], :refs=>String[])
        hll = SetCore.HllSet{p}()
        hll = update_tokens(db, hll, dataset, col_sha1)
        # Update nodes table
        card = SetCore.count(hll)
        _dump = SetCore.dump(hll)
        sha1_d = SetCore.id(hll)
        # Node(sha1::String, labels::String...; d_sha1::String="", dataset::Vector{Int}=Vector{Int}(), props...)
        node = Graph.Node(col_sha1, labels, sha1_d, card, _dump, props)
        Graph.replace!(db, node, table_name="t_nodes")

        return hll
    end
    
    # id::String, committer_name::String, committer_email::String, message::String, props...
    function commit(db::Graph.DB, hdf5_filename::String, committer_name::String, committer_email::String, message::String, props::Config)
        commit_id = string(uuid4())
        props = JSON3.write(props)
        commit = Graph.Commit(commit_id, committer_name, committer_email, message, props)
        Graph.replace!(db, commit)

        commit_node(db, hdf5_filename, commit_id)
        commit_edge(db, hdf5_filename, commit_id)
    end

    function commit_node(db::Graph.DB, hdf5_filename::String, commit_id::String)
        # Get all nodes from t_nodes
        t_nodes = DBInterface.execute(db.sqlitedb, "SELECT * FROM t_nodes") |> DataFrame

        for row in eachrow(t_nodes)
            t_sha1 = string(row.sha1)
            row = update_props!(row, commit_id)
            # Check if the node exists in nodes
            nodes = DBInterface.execute(db.sqlitedb, """SELECT * FROM nodes WHERE sha1 ='$t_sha1'""") |> DataFrame
            if !isempty(nodes)
                node = nodes[1, :]
                # Compare the props fields
                if JSON3.read(row.props) != JSON3.read(node.props)
                    export_node(db, node, hdf5_filename)
                    # Remove the node from nodes
                    DBInterface.execute(db.sqlitedb, """DELETE FROM nodes WHERE sha1 ='$t_sha1'""")
                end
            end
            # Remove the node from t_nodes
            DBInterface.execute(db.sqlitedb, """DELETE FROM t_nodes WHERE sha1 ='$t_sha1'""") 
            # Delete the record from the nodes table
            DBInterface.execute(db.sqlitedb, "DELETE FROM assignments WHERE id = '$t_sha1'")
            # Load node tp nodes           
            SQLite.load!(DataFrame(row), db.sqlitedb, "nodes")
        end        
    end

    function export_node(db::Graph.DB, node::DataFrameRow, hdf5_filename::String)
        labels = node.labels
        labels = replace(string(labels), ";" => "_")
        dataset = JSON3.read(node.dataset, Vector{Int})
        sha1 = node.sha1
        
        props = JSON3.read(node.props, Dict{String, Any})
        type_props = typeof(node.props)
        println("props: $props", type_props)
        commit_id = props["commit_id"]
        
        save_node(hdf5_filename, "/$commit_id/nodes/$sha1", labels, dataset, attributes=props)
    end

    function commit_edge(db::Graph.DB, hdf5_filename::String, commit_id::String)
        # Get all edges from t_edges
        t_edges = DBInterface.execute(db, "SELECT * FROM t_edges") |> DataFrame

        for row in eachrow(t_edges)
            t_source = string(row.source)
            t_target = string(row.target)
            t_type = string(row.r_type)
            
            # println("t_source: $t_source, t_target: $t_target")
            row = update_props!(row, commit_id)
            # Check if the edge exists in edges
            edges = DBInterface.execute(db.sqlitedb, """SELECT * FROM edges WHERE
                    source ='$t_source' AND target = '$t_target' AND r_type = '$t_type'""") |> DataFrame
            if !isempty(edges)
                edge = edges[1, :]
                # Compare the props fields
                if JSON3.read(row.props) != JSON3.read(edge.props)
                    export_edge(db, edge, hdf5_filename)
                    # Remove the edge from edges
                    DBInterface.execute(db.sqlitedb, """DELETE FROM edges WHERE 
                            source ='$t_source' AND target = '$t_target' AND r_type = '$t_type'""")
                end
            end
            # Remove the edge from t_edges
            DBInterface.execute(db.sqlitedb, """DELETE FROM t_edges WHERE 
                    source ='$t_source' AND target = '$t_target' AND r_type = '$t_type'""") 
            # Delete the record from the nodes table
            # DBInterface.execute(db, "DELETE FROM assignments WHERE id = '$id'")
            # Load edge tp edges           
            SQLite.load!(DataFrame(row), db.sqlitedb, "edges")
        end        
    end

    function export_edge(db::Graph.DB, edge::DataFrameRow, hdf5_filename::String)
        json = string(edge.props)
        props = JSON3.read(edge.props, Dict{String, Any})
        commit_id = props["commit_id"]
        source = edge.source
        target = edge.target        
        edge_type = edge.r_type

        save_edge(hdf5_filename, "/$commit_id/edges/$source/$edge_type", target, json, attributes=props)
    end

    # Working with HDF5 files
    #--------------------------------------------------
    """
        save function to handle Nodes. 
            - group_name: /nodes/commit_id/type/uid or abs path to the source file
            - dataset_name: name of the part of the source or sha1_d (in case of csv file: sha1_d for file; column name - for column)
            - dataset: SetCore.dump(hll_set) that is Vector{UInt64} made from HllSet.counts Vector{BitVector} 
            - attributes: Dict of attributes
    """ 
    function save_node(file_name::String, group_name::String, dataset_name::String, dataset::Vector{Int}; attributes::Dict = Dict())    
        println("save_node: $group_name, $dataset_name")
        h5open(file_name, "r+") do file
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

    """
        save function to handle Edges. Dataset is JSON string that represents edge row from SQLite table edges (t_edges)
            - group_name: /edges/commit_id/source/type
            - dataset_name: target
            - dataset: JSON string represent edge row from SQLite table edges (t_edges)
            - attributes: Dict of attributes
    """ 
    function save_edge(file_name::String, group_name::String, dataset_name::String, dataset::String; attributes::Dict = Dict())  
        println("save_node: $group_name, $dataset_name")  
        h5open(file_name, "r+") do file
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
    function read_datasets(file_or_group, data_out::Dict, wildcard)        
        for name in keys(file_or_group)
            item = file_or_group[name]
            if isa(item, HDF5.Dataset) && occursin(wildcard, string(item))
                data = read(item)
                data_out[name] = data
                # println("Read dataset '$name' with data: $data")
            elseif isa(item, HDF5.Group)
                read_datasets(item, data_out, wildcard)
            end
        end
        return data_out
    end

    function retrieve(hdf5_file::String, hdf5_path::String)    
        h5open(hdf5_file, "r") do file
            hll_d = read(file[hdf5_path])        
            return hll_d
        end
    end

    # Misc functions
    #--------------------------------------------------

    function is_sha1_hash(s)
        return length(s) == 40 && all(isxdigit, s)
    end

    function is_number(str)
        try
            number = tryparse(Float64, str)
            return !isnothing(number)
        catch e
            return false
        end
    end

    function update_props!(row::DataFrameRow, commit_id::String)
        try
            # Update props with commit_id
            props = JSON3.read(row[:props])
            config = Config(props)
            config["commit_id"] = commit_id
            props_str = JSON3.write(config)
            row[:props] = props_str
        catch e
            println("update_node error: $e")
        end
        return row
    end

end # module