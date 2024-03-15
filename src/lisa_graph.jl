"""
MIT License

Copyright (c) 2022: Julia Computing Inc. All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Source code is on github
https://github.com/JuliaComputing/SQLiteGraph.jl.git

I borrowed a lot from this project, but also made a lot of changes, 
so, for all errors do not blame the original author but me.
"""

include("lisa_util.jl")

module Graph

    using SQLite: SQLite
    using DBInterface
    import DBInterface: execute
    using JSON3: JSON3
    using EasyConfig
    using DataFrames: DataFrame
    using DataFrames: DataFrameRow

    export DB, Node, Edge, Assignment, Token, 
        set_to_json, json_to_set, assign_lock!, assign_unlock!

    abstract type AbstractGraphType end
    
    #=============================================================================#
    # Assignment, Commit, Token
    #=============================================================================#
    struct Assignment <: AbstractGraphType
        id::String
        parent::String
        item::String
        a_type::String
        processor_id::String
        lock_uuid::String
        status::String
    end

    Assignmnent(id::String, parent::String, item::String, a_type::String, processor_id::String, lock_uuid::String, status::String) =
        Assignment(id, parent, item, a_type, processor_id, lock_uuid, status)
    Assignment(row::SQLite.Row) =
        Assignment(row.id, row.parent, row.item, row.a_type, row.processor_id, row.lock_uuid, row.status)
    Assignment(row::DataFrameRow) =
        Assignment(row.id, row.parent, row.item, row.a_type, row.processor_id, row.lock_uuid, row.status)

    function Base.show(io::IO, o::Assignment)
        print(io, "Assignment($(o.id), $(o.parent), $(o.item), $(o.a_type), $(o.processor_id), $(o.lock_uuid), $(o.status))")
    end

    args(b::Assignment) = (b.id, b.parent, b.item, b.a_type, b.processor_id, b.lock_uuid, b.status)

    #-----------------------------------------------------------------------------# Commit
    struct Commit <: AbstractGraphType
        id::String
        committer_name::String
        committer_email::String
        message::String
        props::Config
    end

    Commit(id::String, committer_name::String, committer_email::String, message::String, props...) = 
        Commit(id, committer_name, committer_email, message, Config(props))
    Commit(row::SQLite.Row) = 
        Commit(row.id, row.committer_name, row.committer_email, row.message, JSON3.read(row.props, Config))

    function Base.show(io::IO, o::Commit)
        print(io, "Commit($(o.id), $(o.committer_name), $(o.committer_email), ", repr(o.message))
        !isempty(o.props) && print(io, "; "); print_props(io, o.props)
        print(io, ')')
    end

    args(c::Commit) = (c.id, c.committer_name, c.committer_email, c.message, JSON3.write(c.props))
        
    #---------------------------------------------------------------------------- Token #
    struct Token <: AbstractGraphType
        id::Int
        bin::Int
        zeros::Int
        token::Set{String}
        tf::Int
        refs::Set{String}
    end

    Token(id::Int, bin::Int, zeros::Int; token::String, tf::Int, refs::String) = 
        Token(id, bin, zeros, token, tf, refs)
    Token(row::SQLite.Row) = Token(row.id, row.bin, row.zeros, JSON3.read(row.token), row.tf, JSON3.read(row.refs))

    function Base.show(io::IO, o::Token)
        print(io, "Token($(o.id), $(o.bin), $(o.zeros), $(o.token), $(o.tf), $(o.refs))")
    end

    args(g::Token) = (g.id, g.bin, g.zeros, JSON3.write(collect(g.token)), g.tf, JSON3.write(collect(g.refs)))

    #=============================================================================#
    # Node, Edge
    #=============================================================================#
    #-----------------------------------------------------------------------------# Node
    struct Node <: AbstractGraphType
        sha1::String
        labels::Vector{String}
        d_sha1::String
        card::Int
        dataset::Vector{Int}
        props::Config
    end

    Node(sha1::String, labels::Vector{String}=Vector{String}(); d_sha1::String="", card::Int, dataset::Vector{Int}=Vector{Int}(), props...) = 
        Node(sha1, collect(labels), d_sha1, card, dataset, Config(props))
    Node(row::SQLite.Row) = 
        Node(row.sha1, JSON3.read(row.labels), row.d_sha1, row.card, JSON3.read(row.dataset, Vector{Int}), 
            JSON3.read(row.props, Config))

    function Base.show(io::IO, o::Node)
        print(io, "Node($(o.sha1)")
        print(io, "; ") 
        print(io, o.labels)
        print(io, "; ")
        !isempty(o.props) && print(io, "props: "); print_props(io, o.props)
        print(io, ')')
    end
    args(n::Node) = (n.sha1, JSON3.write(n.labels), n.d_sha1, n.card, JSON3.write(n.dataset), 
                    JSON3.write(n.props))

    #-----------------------------------------------------------------------------# Edge
    struct Edge <: AbstractGraphType
        source::String
        target::String
        r_type::String
        props::Config
    end
    Edge(src::String, tgt::String, r_type::String; props...) = Edge(src, tgt, r_type, Config(props))
    Edge(row::SQLite.Row) = Edge(row.source, row.target, row.r_type, JSON3.read(row.props, Config))

    function Base.show(io::IO, o::Edge)
        print(io, "Edge($(o.source), $(o.target), ", repr(o.r_type))
        !isempty(o.props) && print(io, "; "); print_props(io, o.props)
        print(io, ')')
    end
    args(e::Edge) = (e.source, e.target, e.r_type, JSON3.write(e.props))

    #-----------------------------------------------------------------------------# Base methods
    Base.:(==)(a::Node, b::Node) = all(getfield(a,f) == getfield(b,f) for f in fieldnames(Node))
    Base.:(==)(a::Edge, b::Edge) = all(getfield(a,f) == getfield(b,f) for f in fieldnames(Edge))

    Base.pairs(o::T) where {T<: Union{Node, Edge}} = (f => getfield(o,f) for f in fieldnames(T))

    Base.NamedTuple(o::Union{Node,Edge}) = NamedTuple(pairs(o))

    #=============================================================================#
    # DB
    #=============================================================================#
    struct DB <: AbstractGraphType
        sqlitedb::SQLite.DB

        function DB(file::String = ":memory:")
            db = SQLite.DB(file)
            foreach(x -> execute(db, x), [
                "PRAGMA foreign_keys = ON;",
                # assignments
                "CREATE TABLE IF NOT EXISTS assignments (
                    id TEXT NOT NULL UNIQUE PRIMARY KEY,
                    parent TEXT NOT NULL,
                    item TEXT NOT NULL,
                    a_type TEXT NOT NULL,
                    processor_id TEXT NOT NULL,
                    lock_uuid TEXT NOT NULL,
                    status TEXT NOT NULL
                );",
                # commits
                "CREATE TABLE IF NOT EXISTS commits (
                    id  TEXT NOT NULL UNIQUE PRIMARY KEY,
                    committer_name TEXT NOT NULL,
                    committer_email TEXT NOT NULL,
                    message TEXT NOT NULL,
                    props TEXT NOT NULL
                );",
                # tokens
                "CREATE TABLE IF NOT EXISTS tokens (
                    id INTEGER NOT NULL UNIQUE PRIMARY KEY,
                    bin INTEGER NOT NULL,
                    zeros INTEGER NOT NULL,
                    token TEXT NOT NULL,
                    tf INTEGER DEFAULT 0 NOT NULL,
                    refs TEXT NOT NULL
                );",
                "CREATE INDEX IF NOT EXISTS token_idx ON tokens(token);",
                # nodes
                "CREATE TABLE IF NOT EXISTS nodes (
                    sha1 TEXT NOT NULL UNIQUE,
                    labels TEXT DEFAULT '[]',
                    d_sha1 DEFAULT '',
                    card INTEGER DEFAULT 0,
                    dataset TEXT DEFAULT '[]',
                    props TEXT DEFAULT '{}'
                );",
                "CREATE INDEX IF NOT EXISTS sha1_idx ON nodes(sha1);",
                # edges
                "CREATE TABLE IF NOT EXISTS edges (
                    source TEXT NOT NULL,
                    target TEXT NOT NULL,
                    r_type TEXT NOT NULL,
                    props TEXT NOT NULL,
                    PRIMARY KEY (source, target, r_type)
                );",
                "CREATE INDEX IF NOT EXISTS source_idx ON edges(source);",
                "CREATE INDEX IF NOT EXISTS target_idx ON edges(target);",
                "CREATE INDEX IF NOT EXISTS r_type_idx ON edges(r_type);",
                # t_nodes - nodes table in TRANSIT
                "CREATE TABLE IF NOT EXISTS t_nodes (
                    sha1 TEXT NOT NULL UNIQUE,
                    labels TEXT DEFAULT '[]',
                    d_sha1 TEXT DEFAULT '',
                    card INTEGER DEFAULT 0,
                    dataset TEXT DEFAULT '[]',
                    props TEXT DEFAULT '{}'
                );",
                "CREATE INDEX IF NOT EXISTS t_sha1_idx ON t_nodes(sha1);",
                # t_edges - edges table in TRANSIT
                "CREATE TABLE IF NOT EXISTS t_edges (
                    source TEXT NOT NULL,
                    target TEXT NOT NULL,
                    r_type TEXT NOT NULL,
                    props TEXT NOT NULL,
                    PRIMARY KEY (source, target, r_type)
                );",
                "CREATE INDEX IF NOT EXISTS t_source_idx ON t_edges(source);",
                "CREATE INDEX IF NOT EXISTS t_target_idx ON t_edges(target);",
                "CREATE INDEX IF NOT EXISTS t_r_type_idx ON t_edges(r_type);",
            ])
            new(db)
        end
    end

    #=============================================================================#
    # DB Base functions
    #=============================================================================#
    function Base.show(io::IO, db::DB)
        print(io, "Graph.DB(\"$(db.sqlitedb.file)\") " *
                    "($(n_assignments(db)) assignments, " *
                    "$(n_commits(db)) commits, " *
                    "$(n_tokens(db)) tokens, " *
                    "$(n_nodes(db)) nodes, " * 
                    "$(n_edges(db)) edges" *
                    "$(n_tnodes(db)) t_nodes, " * 
                    "$(n_tedges(db)) t_edges)")
    end

    execute(db::DB, args...; kw...) = execute(db.sqlitedb, args...; kw...)

    n_assignments(db::DB) = single_result_execute(db, "SELECT Count(*) FROM assignments")
    n_commits(db::DB) = single_result_execute(db, "SELECT Count(*) FROM commits")
    n_tokens(db::DB) = single_result_execute(db, "SELECT Count(*) FROM tokens")
    n_nodes(db::DB) = single_result_execute(db, "SELECT Count(*) FROM nodes")
    n_edges(db::DB) = single_result_execute(db, "SELECT Count(*) FROM edges")
    n_tnodes(db::DB) = single_result_execute(db, "SELECT Count(*) FROM t_nodes")
    n_tedges(db::DB) = single_result_execute(db, "SELECT Count(*) FROM t_edges")

    Base.length(db::DB) = n_nodes(db)
    Base.size(db::DB) = (nodes=n_nodes(db), edges=n_edges(db))
    Base.lastindex(db::DB) = length(db)
    Base.axes(db::DB, i) = size(db)[i]

    Broadcast.broadcastable(db::DB) = Ref(db)

    #=============================================================================#
    # DB set_lock, unset_lock
    #=============================================================================#
    function set_lock!(db::DB, id::String, lock_uuid::String; result::Bool=false)  
        DBInterface.execute(db, """UPDATE assignments 
            SET lock_uuid='$lock_uuid'
            WHERE (lock_uuid IS NULL OR lock_uuid = '')
            AND id='$id';
            """)
        if result
            DBInterface.execute(db, """SELECT * FROM assignments WHERE lock_uuid = '$lock_uuid';""") |> DataFrame
        end
    end

    function set_lock!(db::DB, 
            parent::String, 
            a_type::String, 
            processor_id::String, 
            new_processor_id::String,
            status::String, 
            new_status::String,
            lock_uuid::String; result::Bool=false) 
        p_parent = parent * "%"
        DBInterface.execute(db, """UPDATE assignments 
            SET lock_uuid='$lock_uuid',
                processor_id='$new_processor_id',
                status='$new_status'
            WHERE (lock_uuid IS NULL OR lock_uuid = '')
            AND parent LIKE '$p_parent'
            AND a_type = '$a_type'
            AND processor_id = '$processor_id'
            AND status = '$status';
        """)
        if result
            DBInterface.execute(db, """SELECT * FROM assignments WHERE lock_uuid = '$lock_uuid';""") |> DataFrame
        end
        
    end

    function set_lock!(db::DB, parent::String, ::Colon, 
            processor_id::String, 
            new_processor_id::String,
            status::String, 
            new_status::String,
            lock_uuid::String; result::Bool=false) 
        p_parent = parent * "%"
        DBInterface.execute(db, """UPDATE assignments 
            SET lock_uuid='$lock_uuid',
                processor_id='$new_processor_id',
                status='$new_status'
            WHERE (lock_uuid IS NULL OR lock_uuid = '')
            AND parent LIKE '$p_parent'
            AND processor_id = '$processor_id'
            AND status = '$status';
        """)
        if result
            DBInterface.execute(db, """SELECT * FROM assignments WHERE lock_uuid = '$lock_uuid';""") |> DataFrame
        end
        
    end

    function unset_lock!(db::DB, id::String, ::Colon) 
        DBInterface.execute(db, "UPDATE assignments SET lock_uuid='' WHERE id='$id'")
    end

    function unset_lock!(db::DB, ::Colon, lock_uuid::String) 
        DBInterface.execute(db, "UPDATE assignments SET lock_uuid='' WHERE lock_uuid='$lock_uuid'")
    end

    # This will unset (unlock) all assignments 
    function unset_lock!(db::DB, ::Colon, ::Colon) 
        DBInterface.execute(db, "UPDATE assignments SET lock_uuid=''")
    end

    function unset_lock!(db::DB, id::String, ::Colon, status::String) 
        DBInterface.execute(db, "UPDATE assignments SET lock_uuid='', status = '$status' WHERE id='$id'")
    end

    function unset_lock!(db::DB, id::String, processor_id::String, status::String) 
        DBInterface.execute(db, "UPDATE assignments SET lock_uuid='', processor_id='$processor_id', status='$status' WHERE id='$id'")
    end

    #=============================================================================#
    # DB insert, replace
    #=============================================================================#
    function Base.insert!(db::DB, assignment::Assignment)
        execute(db.sqlitedb, "INSERT OR IGNORE INTO assignments VALUES(?, ?, ?, ?, ?, ?, ?)", args(assignment))
        db
    end

    function Base.insert!(db::DB, commit::Commit)
        execute(db.sqlitedb, "INSERT OR IGNORE INTO commits (id, committer_name, committer_email, message, props)" *
            " VALUES(?, ?, ?, ?, json(?))", args(commit))
        db
    end

    function Base.insert!(db::DB, token::Token)
        execute(db.sqlitedb, "INSERT OR IGNORE INTO tokens VALUES(?, ?, ?, ?, ?, ?)", args(token))
        db
    end

    function Base.insert!(db::DB, node::Node; table_name::String="nodes")
        execute(db.sqlitedb, "INSERT OR IGNORE INTO $table_name (sha1, labels, d_sha1, card, dataset, props)" *
            " VALUES(?, ?, ?, ?, ?, json(?))", args(node))
        db
    end
    function Base.insert!(db::DB, edge::Edge; table_name::String="edges")
        execute(db.sqlitedb, "INSERT OR IGNORE INTO $table_name VALUES(?, ?, ?, json(?))", args(edge))
        db
    end

    #-----------------------------------------------------------------------------# replace!
    function Base.replace!(db::DB, assignment::Assignment)
        execute(db.sqlitedb, "INSERT INTO assignments VALUES(?, ?, ?, ?, ?, ?, ?)" * 
            " ON CONFLICT(id) DO UPDATE SET" *
            " parent=excluded.parent," *
            " item=excluded.item," *
            " a_type=excluded.a_type," *
            " processor_id=excluded.processor_id," *
            " lock_uuid=excluded.lock_uuid," *
            " status=excluded.status", args(assignment))
        db
    end

    function Base.replace!(db::DB, commit::Commit)
        execute(db.sqlitedb, "INSERT INTO commits VALUES(?, ?, ?, ?, json(?))" *
            " ON CONFLICT(id) DO UPDATE SET" *
            " committer_name=excluded.committer_name," *
            " committer_email=excluded.committer_email," *
            " message=excluded.message," *
            " props=excluded.props", args(commit))
        db
    end

    function Base.replace!(db::DB, token::Token)
        execute(db.sqlitedb, "INSERT INTO tokens VALUES(?, ?, ?, ?, ?, ?)" *
            " ON CONFLICT(id) DO UPDATE SET token=excluded.token, tf=excluded.tf, refs=excluded.refs", args(token))
        db
    end
    # (sha1, labels, d_sha1, card, dataset, props)
    function Base.replace!(db::DB, node::Node; table_name::String="nodes")
        sha1 = node.sha1
        labels = JSON3.write(node.labels)
        d_sha1 = node.d_sha1
        card = node.card
        dataset = JSON3.write(node.dataset)
        props = JSON3.write(node.props)
        execute(db.sqlitedb, "INSERT INTO $table_name (sha1, labels, d_sha1, card, dataset, props)" *
            " VALUES('$sha1', json('$labels'), '$d_sha1', $card, json('$dataset'), json('$props'))" *
            " ON CONFLICT(sha1) DO UPDATE SET" *
            " labels=excluded.labels," *
            " d_sha1=excluded.d_sha1," *
            " card=excluded.card," *
            " dataset=excluded.dataset," *
            " props=excluded.props;") 
        db
    end
    function Base.replace!(db::DB, edge::Edge; table_name::String="edges")
        execute(db.sqlitedb, "INSERT INTO $table_name (source, target, r_type, props) VALUES(?, ?, ?, json(?))" *
            " ON CONFLICT(source,target,r_type) DO UPDATE SET props=excluded.props", args(edge))
        db
    end

    #=============================================================================#
    # DB query
    #=============================================================================#
    function query(db::DB, select::String, from::String, whr::String, args=nothing)
        stmt = "SELECT $select FROM $from WHERE $whr"
        # @info stmt
        res = isnothing(args) ? execute(db, stmt) : execute(db, stmt, args)
        # if isempty(res)
        #     error("No $from found where: $whr")
        # else
        #     return res
        # end
        return res
    end

    #=============================================================================#
    # DB gettoken, getassign, getnode, getedge
    #=============================================================================# 
    function gettokens(db::DB, refs::String, ::Colon) 
        return query(db, "*", "tokens", "refs LIKE '%$refs%'")
    end

    function gettokens(db::DB, ref1::String, ref2::String)
        result = query(db, "*", "tokens", "refs LIKE '%$ref1%' AND refs LIKE '%$ref2%'") 
        if isempty(result)
            return ""
        else
            return result
        end
    end

    function gettokens(db::DB, ::Colon, id::String) 
        return query(db, "*", "tokens", "id LIKE '$id'")
    end

    # function gettokens(db::DB, ::Colon, ::Colon, bin::Int, zeros::Int) 
    #     return query(db, "*", "tokens", "bin=$bin AND zeros=$zeros")
    # end
    #-----------------------------------------------------------------------------# getassign
    function getassign(db::DB, id::String, ::Colon) 
        result = query(db, "*", "assignments", "id LIKE '$id'")
        Assignment(first(result))
    end

    function getassign(db::DB, ::Colon, lock_uuid::String) 
        result = query(db, "*", "assignments", "lock_uuid LIKE '$lock_uuid'")
        return result
    end

    function getassign(db::DB, parent::String, a_type::String, processor_id::String, lock_uuid::String, status::String) 
        result = query(db, "*", "assignments", 
        "parent LIKE '$parent'" *
        " AND a_type LIKE '$a_type'" * 
        " AND processor_id LIKE '$processor_id'" * 
        " AND lock_uuid LIKE '$lock_uuid'" * 
        " AND status LIKE '$status'")
        (Assignment(row) for row in result)
    end

    #-----------------------------------------------------------------------------# getindex (Node)
    function getnode(db::DB, sha1::String, ::Colon; table_name::String="nodes") 
        result = query(db, "*", table_name, "sha1='$sha1'")
        Node(first(result))
    end

    function getnode(db::DB, ::Colon; table_name::String="nodes") 
        result = query(db, "*", table_name, "TRUE")
        (Node(row) for row in result)
    end

    function getnode(db::DB, ::Colon, label::String; table_name::String="nodes") 
        result = query(db, "*", table_name, "labels LIKE '%$label%'")
        (Node(row) for row in result)
    end

    #-----------------------------------------------------------------------------# getindex (Edge)
    # all specified
    function getedge(db::DB, source::String, target::String, r_type::AbstractString; table_name::String="edges")
        result = query(db, "*", table_name, "source LIKE '$source' AND target LIKE '$target' AND r_type LIKE '$r_type'")
        Edge(first(result))
    end

    # one colon
    function getedge(db::DB, source::String, target::String, ::Colon; table_name::String="edges")
        result = query(db, "*", table_name, "source LIKE '$source' AND target LIKE '$target'")
        (Edge(row) for row in result)
    end
    function getedge(db::DB, source::String, ::Colon, r_type::AbstractString; table_name::String="edges")
        result = query(db, "*", table_name, "source LIKE '$source' AND r_type LIKE '$r_type'")
        (Edge(row) for row in result)
    end
    function getedge(db::DB, ::Colon, target::String, r_type::AbstractString; table_name::String="edges")
        result = query(db, "*", table_name, "target LIKE '$target' AND r_type LIKE '$r_type'")
        (Edge(row) for row in result)
    end

    # two colons
    function getedge(db::DB, source::String, ::Colon, ::Colon; table_name::String="edges")
        result = query(db, "*", table_name, "source LIKE '$source'")
        (Edge(row) for row in result)
    end
    function getedge(db::DB, ::Colon, target::String, ::Colon; table_name::String="edges")
        result = query(db, "*", table_name, "target LIKE '$target'")
        (Edge(row) for row in result)
    end
    function getedge(db::DB, ::Colon, ::Colon, r_type::AbstractString; table_name::String="edges")
        result = query(db, "*", table_name, "r_type LIKE '$r_type'")
        (Edge(row) for row in result)
    end

    # all colons
    function getedge(db::DB, ::Colon, ::Colon, ::Colon; table_name::String="edges") 
        result = query(db, "*", table_name, "TRUE")
        (Edge(row) for row in result)
    end

    #=============================================================================#
    # Util functions
    #=============================================================================#    
    function getdict(strct::AbstractGraphType)
        dict = Dict()
        for field in fieldnames(typeof(strct))
            # dict[field] = getfield(strct, field)
            value = getfield(strct, field)
            if value isa Vector || value isa Set
                dict[field] = JSON3.write(value)
            else
                dict[field] = value
            end
        end
        return dict
    end
    
    function single_result_execute(db, stmt, args...)
        ex = execute(db, stmt, args...)
        isempty(ex) ? nothing : first(first(ex))
    end

    function print_props(io::IO, o::Union{Config, Dict})
        for (i,(k,v)) in enumerate(pairs(o))
            if i < 5
                print(io, k, '=', repr(v))
                i == length(o) || print(io, ", ")
            end
        end
        length(o) > 5 && print(io, "…")
    end

    # Function to convert a set to a JSON array string
    function set_to_json(s::Set{String})
        return JSON3.write(collect(s))
    end

    # Function to convert a JSON array string to a set
    function json_to_set(json_str::String)
        return Set(JSON3.read(json_str))
    end

    #-----------------------------------------------------------------------------
    # adjacency_matrix work in progress
    """
    adjacency_matrix(db, type)

    Create the adjacency matrix for a given edge `type`.  If `A[i,j] == true`, there exists an
    edge from node `i` to node `j` with type `type`.
    """
    # function adjacency_matrix(db::DB, type)
    #     n = n_nodes(db)
    #     out = falses(n, n)
    #     for row in execute(db, "SELECT DISTINCT source, target FROM edges WHERE type=?;", (type,))
    #         out[row.source, row.target] = true
    #     end
    #     out
    # end

    # function adjacency_matrix(g::SQLite.DB)
    #     # Create a DataFrame to store the adjacency matrix
    #     adjacency_df = DataFrame()

    #     # Get the number of nodes in the graph
    #     n_nodes = n_nodes(g)

    #     # Initialize the DataFrame with zeros
    #     for i in 1:n_nodes
    #         adjacency_df[!, Symbol("node_$i")] = zeros(n_nodes)
    #     end

    #     # Fill in the DataFrame with the adjacency matrix
    #     for edge in g.edges
    #         source = edge.source
    #         target = edge.target
    #         adjacency_df[source, Symbol("node_$target")] = 1
    #     end

    #     return adjacency_df
    # end

end