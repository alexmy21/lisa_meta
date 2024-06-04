# include("lisa_sets.jl")
# include("lisa_util.jl")
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
    import TextAnalysis as TEXT
    import WordTokenizers as WT
    using JSON3
    using PooledArrays
    using UUIDs
    using HDF5
    using SparseArrays

    export book_file, ingest_csv, _ingest_csv_by_column, ingest_csv_by_row, # ingest functions
        commit, commit_node, commit_edge,                                   # commit functions 
        collect_tokens, update_props!,                                      # utility functions
        save_node, save_edge, read_datasets, retrieve,                      # HDF5 functions
        is_sha1_hash, is_number, is_quoted, set_quotes,                     # misc functions
        get_card_matrix, get_node_matrix                                    # matrix functions

    g_seed::Int = 0

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
    function book_file(db::Graph.DB, start_dir::String; 
        ext::String="csv", seed::Int=0, column::Bool=true, P::Int=10) 

        global g_seed = seed
        P = P
        # Update tokens table 
        # df = DataFrame(:id=>Int[], :bin=>Int[], :zeros=>Int[], :token=>String[], :refs=>String[])
        # Walk through the directory and its subdirectories
        for (root, dirs, files) in walkdir(Path(start_dir))
            # Check each file
            for file in files                
                # If the file is a CSV file, add its path to the array
                if extension(Path(file)) == ext
                    # Register (book) the file in the assignmnets table
                    f_name = joinpath(root, file)
                    sha_1 = bytes2hex(sha1(f_name))
                    # println("sha1: ", sha_1)
                    
                    assign = Graph.Assignment(sha_1, root, f_name, ext, "book_file", "", "waiting")
                    Graph.replace!(db, assign)

                    dataset = [f_name, ext, root]
                    # println("dataset: ", dataset)
                    update_tokens(db, dataset, sha_1, P)

                    if column
                        # Book (register) the csv file columns in the assignments table
                        book_column(db, f_name, sha_1, P)
                    end
                end
            end
        end
    end

    function book_column(db::Graph.DB, csv_filename::String, file_sha1::String, P::Int)         
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
            update_tokens(db, dataset, sha_1, P)            
        end
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
    function ingest_csv_by_column(db::Graph.DB, file::Graph.Assignment, lock_uuid::String; 
        p::Int=10, limit::Int=-1, offset::Int=0, seed::Int=0)

        global g_seed = seed        
        assign_df = Graph.set_lock!(db, file.id, :, "book_column", "ingest_csv", "waiting", "waiting", lock_uuid; result=true)        
        if isempty(assign_df) 
            println("The file $file.item is already being processed")
            return
        end 
        # println("Processing file: ", file.item)
        file_node = Graph.Node(file.id, ["csv_file"], "", 0, [], Config())
        Graph.replace!(db, file_node, table_name="t_nodes")

        file_name = abspath(file.item)
        file_df = CSV.read(file_name, DataFrame; missingstring = "", skipto=offset+1, limit=limit, silencewarnings=true)
        file_hll = SetCore.HllSet{p}()
        file_props = Config()
        file_props["file_name"] = file_name
        file_props["file_type"] = file.a_type
        # Iterate over the columns
        i = 0
        for assign in eachrow(assign_df)
            col_props = Config()
            col_props["column_name"] = assign.item
            col_props["file_sha1"] = file.id
            col_props["column_type"] = assign.a_type
            # Process the column data depending on the column type
            if assign.a_type == Missing
                # println("Processing integer data for column $assign.item")
            elseif assign.a_type == Int64
                # println("Processing real number data for column $assign.item")
            elseif assign.a_type == "String"
                column_name = string(assign.item)                
                dataset = Set(file_df[!, column_name])         
                col_hll = ingest_dataset(db, dataset, assign.id, ["csv_column"], col_props, p)
                file_hll = SetCore.union!(file_hll, col_hll)
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
            else
                # Process other types of data
            end
            Graph.unset_lock!(db, assign.id, :, "completed")
        end
        println("Processed column: $i")
        card = SetCore.count(file_hll)
        file_dump = SetCore.dump(file_hll)
        sha1_d = SetCore.id(file_hll)
        file_node = Graph.Node(file.id, ["csv_file"], sha1_d, card, file_dump, file_props)
        Graph.replace!(db, file_node, table_name="t_nodes")

        Graph.unset_lock!(db, file.id, :, "completed")
    end

    """
        This function reads a whole CSV file to SQLite DB . May cause memory overflow with very big tables,
        but it is slightly more performant.

        ingest_csv_by_row
        It takes a database, an assignment that points to csv file and a precision parameter.
        1. reads file row by row
        3. At the end of the process, it updates the status of the assignment to "completed" and
        create a new node in the nodes with the SHA1 hash of the csv file as the id, and edges in edges tables.

        Arguments limit and offset are used to read the CSV file in chunks. 
        Can be used to simulate different loads with the same files.

        IMPORTANT! This function can be run only after the ingest_csv_by_column function has been run. 
        It assumes that the nodes for files already created.
    """
    function ingest_csv_by_row(db::Graph.DB, file::Graph.Assignment; 
        p::Int=10, limit::Int=-1, offset::Int=0, seed::Int=0)

        global g_seed = seed 

        file_name = abspath(file.item)
        file_hll = SetCore.HllSet{p}()
        file_props = Config()
        file_props["file_name"] = file_name
        file_props["file_type"] = file.a_type

        rows = CSV.Rows(file_name)
        i = 0
        for row in rows
            i = i + 1
            if(i < offset)
                continue
            end
            if(i > (offset + limit) && limit != -1)
                break
            end
            dataset = collect(skipmissing(row))
            # println("row: ", dataset)
            row_sha1 = bytes2hex(sha1(join(dataset)))
            row_props = Config()
            row_props["file_sha1"] = file.id     
            row_hll = ingest_dataset(db, dataset, row_sha1, ["csv_row"], row_props, p)
            file_hll = SetCore.union!(file_hll, row_hll)
            # Create and add to t_edge table a new edge between the file and the row
            edge_props = Config()
            edge_props["source"] = file.item
            edge_props["target"] = row_sha1
            edge_props["source_label"] = "csv_file"
            edge_props["target_label"] = "csv_row"
            edge = Graph.Edge(file.id, row_sha1, "has_row", edge_props)
            # Update "t_edge" table
            Graph.replace!(db, edge; table_name="t_edges")
        end
        Graph.unset_lock!(db, file.id, :, "completed")
    end

    function ingest_dataset(db::Graph.DB, dataset::Union{Vector, PooledVector, Set}, node_sha1::String, labels, props::Config, 
        p::Int64)
        # println("dataset SHA1: ", col_sha1)
        hll = update_tokens(db, dataset, node_sha1, p)        
        # Update nodes table
        card = SetCore.count(hll)
        _dump = SetCore.dump(hll)
        sha1_d = SetCore.id(hll)        
        node = Graph.Node(node_sha1, labels, sha1_d, card, _dump, props)
        Graph.replace!(db, node, table_name="t_nodes")
        return hll
    end
    
    function update_tokens(db::Graph.DB, dataset, sha_1::String, p::Int)
        
        ds_hll = SetCore.HllSet{p}()
        
        dataset = collect(skipmissing(dataset))
        df = DataFrame(:id=>Int[], :bin=>Int[], :zeros=>Int[], :token=>String[], :tf=>Int[], :refs=>String[])
        
        function update_token(token; table_name::String="tokens")    
            
            h = SetCore.u_hash(token, seed=g_seed) 
            # Retrieve the set from the table
            row = DBInterface.execute(db.sqlitedb, "SELECT * FROM $table_name WHERE id = $h") |> DataFrame
            item_set = Set([token])
            sha1_set = Set([sha_1]) 
            tf = 0       
            if isempty(row)
                tf = 1
            else
                retrieved_items = Graph.json_to_set(row[1, "token"])
                retrieved_sha1 = Graph.json_to_set(row[1, "refs"])
                item_set = retrieved_items ∪ item_set
                sha1_set = retrieved_sha1 ∪ sha1_set
                tf = row[1, "tf"] + 1
            end
            SetCore.add!(ds_hll, token; seed=g_seed)            
            token_node = Graph.Token(h, SetCore.getbin(ds_hll, h), SetCore.getzeros(ds_hll, h),
                item_set, tf, sha1_set) 
            
            dict = Graph.getdict(token_node)
            # println("dict: ", dict)
            push!(df, dict)
            
            return df  
        end
        
        i = 0        
        for item in dataset
            if !isa(item, String)
                continue
            end
            tokens = WT.tokenize(item)
            for token in tokens       
                if isempty(token) || is_number(token) || length(token) < 3
                    continue
                end                    
                # SetCore.add!(hll, token)    
                update_token(token)
            end
        end
        SQLite.load!(df, db.sqlitedb, "tokens", replace=true)
        
        return ds_hll
    end

    function get_card_matrix(db::Graph.DB, source_id::String)    
        row_nodes = get_joined_nodes(db, source_id, "has_row")
        row_sets = [SetCore.restore(SetCore.HllSet{10}(), 
                JSON3.read(row.dataset, Vector{UInt64})) for row in eachrow(row_nodes)]
        # Select column nodes
        column_nodes = get_joined_nodes(db, source_id, "has_column")
        column_sets = [SetCore.restore(SetCore.HllSet{10}(), 
                JSON3.read(row.dataset, Vector{UInt64})) for row in eachrow(column_nodes)]
        # Create a matrix where each cell is the cardinality of the intersection 
        # of the corresponding row and column HllSet
        matrix = spzeros(length(row_sets), length(column_sets))
        for (i, row_set) in enumerate(row_sets)
            for (j, column_set) in enumerate(column_sets)
                matrix[i, j] = SetCore.count(SetCore.intersect(row_set, column_set))
            end
        end
        return matrix
    end

    # This  function returns a matrix where each cell is the node of the intersection
    function get_node_matrix(db::Graph.DB, source_id::String)        
        row_nodes = get_joined_nodes(db, source_id, "has_row")
        row_sets = [SetCore.restore(SetCore.HllSet{10}(), 
                JSON3.read(row.dataset, Vector{UInt64})) for row in eachrow(row_nodes)]
        # Select column nodes
        column_nodes = get_joined_nodes(db, source_id, "has_column")
        column_sets = [SetCore.restore(SetCore.HllSet{10}(), 
                JSON3.read(row.dataset, Vector{UInt64})) for row in eachrow(column_nodes)]
        # Create a matrix where each cell is the cardinality of the intersection 
        # of the corresponding row and column HllSet
        # matrix = spzeros(length(row_sets), length(column_sets))
        matrix = Array{Graph.Node}(undef, length(row_sets), length(column_sets))
        for (i, row_set) in enumerate(row_sets)
            row_id = row_nodes[i, "sha1"]
            for (j, column_set) in enumerate(column_sets)
                column_id = column_nodes[j, "sha1"]
                d_set = SetCore.intersect(row_set, column_set)
                card = SetCore.count(d_set)
                dump = SetCore.dump(d_set)
                sha1_d = SetCore.id(d_set)
                sha1_d = SetCore.id(d_set)

                props = Config()
                props.source = row_id
                props.target = column_id
                node = Graph.Node(sha1_d, [row_id * "_" * column_id], sha1_d, card, dump, props)

                matrix[i, j] = node
            end
        end
        return matrix
    end

    function get_value_matrix(db::Graph.DB, source_id::String)    
        row_nodes = get_joined_nodes(db, source_id, "has_row")
        row_sets = [SetCore.restore(SetCore.HllSet{10}(), 
                JSON3.read(row.dataset, Vector{UInt64})) for row in eachrow(row_nodes)]
        # Select column nodes
        column_nodes = get_joined_nodes(db, source_id, "has_column")
        column_sets = [SetCore.restore(SetCore.HllSet{10}(), 
                JSON3.read(row.dataset, Vector{UInt64})) for row in eachrow(column_nodes)]
        # Create a matrix where each cell is the cardinality of the intersection 
        # of the corresponding row and column HllSet
        matrix = fill("", length(row_sets), length(column_sets))
        for (i, row_set) in enumerate(row_sets)
            row_id = row_nodes[i, "sha1"]
            for (j, column_set) in enumerate(column_sets)
                column_id = column_nodes[j, "sha1"]
                result = Graph.gettokens(db, row_id, column_id)
                tokens = Store.collect_tokens(db, result)
                matrix[i, j] = JSON3.write(tokens)
            end
        end
        return matrix
    end

    function get_joined_nodes(db::Graph.DB, source_id::String, r_type::String)
        col_query = """
            SELECT *
            FROM t_nodes
            INNER JOIN t_edges ON t_edges.target = t_nodes.sha1
            WHERE t_edges.source LIKE '%$source_id%' AND t_edges.r_type LIKE '%$r_type%';
        """
        return DBInterface.execute(db.sqlitedb, col_query) |> DataFrame
    end

    # Commit functions
    #--------------------------------------------------
    """
        commit function to handle commits. 
            - db::Graph.DB
            - hdf5_filename::String
            - committer_name::String
            - committer_email::String
            - message::String
            - props::Config
    """    
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
                if row.card != node.card || JSON3.read(row.props) != JSON3.read(node.props)                    
                    # Remove diff edges (HEW, RET, DEL) for if they exist
                    DBInterface.execute(db.sqlitedb, """DELETE FROM edges WHERE target ='$t_sha1' AND r_type IN ('NEW', 'RET', 'DEL')""")
                    # Remove the node from nodes
                    DBInterface.execute(db.sqlitedb, """DELETE FROM nodes WHERE sha1 ='$t_sha1'""")
                    # Remove diff nodes (NEW, RET, DEL) for if they exist
                    DBInterface.execute(db.sqlitedb, raw"DELETE FROM nodes WHERE json_extract(props, '$.this')" * " = '$t_sha1'")
                    # println("Exporting node: ", t_sha1)
                    export_node(db, node, hdf5_filename)

                    # Calculate difference between new and old version of the node and 
                    # Generate 3 nodes (NEW, RET, and DEL) and 3 edges (NEW, RET, and DEL)
                    # to represent the difference
                    #--------------------------------------------------
                    Graph.node_diff(db, row, node)
                    #--------------------------------------------------
                    # Load node tp nodes 

                    dataset = JSON3.read(row.dataset, Vector{Int})
                    props = JSON3.read(row.props, Dict{String, Any})
                    labels = row.labels
                    labels = JSON3.read(labels, Array{String, 1})
                    g_node = Graph.Node(row.sha1, labels, row.d_sha1, row.card, dataset, props)
                    Graph.replace!(db, g_node, table_name="nodes")
                end
            end
            # Remove the node from t_nodes
            DBInterface.execute(db.sqlitedb, """DELETE FROM t_nodes WHERE sha1 ='$t_sha1'""") 
            # Delete the record from the nodes table
            DBInterface.execute(db.sqlitedb, "DELETE FROM assignments WHERE id = '$t_sha1'")
            
        end        
    end

    function export_node(db::Graph.DB, node::DataFrameRow, hdf5_filename::String)
        labels = node.labels
        labels = replace(string(labels), ";" => "_")
        dataset = JSON3.read(node.dataset, Vector{Int})
        sha1 = node.sha1
        
        props = JSON3.read(node.props, Dict{String, Any})
        type_props = typeof(node.props)
        # println("props: $props", type_props)
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

    # Utility functions
    #--------------------------------------------------
    function collect_tokens(db::Graph.DB, result)
        tokens = Set{String}()
        # result = Graph.gettokens(db, refs, :, :, :)
        for row in result
            token = JSON3.read(row.token)
            for tok in token
                # println("token:     ", tok)
                push!(tokens, tok)
            end
        end
        return tokens
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

    # Working with HDF5 files
    #--------------------------------------------------
    """
        save function to handle Nodes. 
            - group_name: /nodes/commit_id/type/uid or abs path to the source file
            - dataset_name: name of the part of the source or sha1_d (in case of csv file: sha1_d for file; column name - for column)
            - dataset: SetCore.dump(hll_set) that is Vector{UInt64} made from HllSet.counts Vector{BitVector} 
            - attributes: Dict of attributes
    """ 
    function save_node(file_name::String, group_name::String, dataset_name::String, 
        dataset::Union{Vector{Int}, String}; attributes::Dict = Dict())

        # println("save_node: $group_name, $dataset_name")
        try
            h5open(file_name, "r+") do file
                # println("file: ", isempty(file))
                # # Check if the group already exists in the file
                # if HDF5.exist(file, group_name) 
                #     g = file[group_name]                
                # else
                # Create a new group in the file
                g = create_group(file, group_name)
                # end
                g[dataset_name] = dataset
                if isempty(attributes)
                    return
                end
                for(key, value) in attributes
                    attrs(g[dataset_name])[key] = value
                end
            end        
        catch e
            println("save_node error: $e")
        end      
    end

    """
        save function to handle Edges. Dataset is JSON string that represents edge row from SQLite table edges (t_edges)
            - group_name: /edges/commit_id/source/type
            - dataset_name: target
            - dataset: JSON string represent edge row from SQLite table edges (t_edges)
            - attributes: Dict of attributes
    """ 
    function save_edge(file_name::String, group_name::String, dataset_name::String, 
        dataset::String; attributes::Dict = Dict())
        
        try
            h5open(file_name, "r+") do file
                # Check if the group already exists in the file
                # if HDF5.exist(file, group_name) 
                #     g = file[group_name]                
                # else
                # Create a new group in the file, if it doesn't exist
                g = create_group(file, group_name)
                # end
                g[dataset_name] = dataset
                # Create attributes
                if isempty(attributes)
                    return
                end
                for(key, value) in attributes
                    attrs(g[dataset_name])[key] = value
                end 
            end 
        catch e
            println("save_edge error: $e")
        end   
    end

    # Function to recursively read datasets from an HDF5 file or group that match a wildcard
    # function read_datasets(file_or_group, data_out::Dict, wildcard) 
    #     if isempty(file_or_group)
    #         return
    #     end

    #     for name in names(file_or_group)
    #         item = file_or_group[name]
    #         if isa(item, HDF5Dataset) && occursin(wildcard, string(item))
    #             data = read(item)
    #             data_out[name] = data
    #             # println("Read dataset '$name' with data: $data")
    #         elseif isa(item, HDF5Group)
    #             read_datasets(item, data_out, wildcard)
    #         end
    #     end
    #     return data_out
    # end

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

    function is_quoted(s::String)
        return length(s) >= 2 && s[1] == '"' && s[end] == '"'
    end

    function set_quotes(s::String)
        if is_quoted(s)
            return s
        end
        return "\"$s\""
    end

    function remove_quotes(s::String)
        if is_quoted(s)
            return s[2:end-1]
        end
        return s
    end
    
end # module