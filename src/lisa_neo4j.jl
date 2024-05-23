
# include("lisa_sets.jl")
include("lisa_store.jl")

"""
    This module contains the functions to interact with the Neo4j database.
    It is used only for POC purposes and will be replaced by a more robust solution in the future.
"""
module LisaNeo4j
    using ..SetCore
    using ..Store

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

    export Neo_node, request, cypher, collect_hll_sets, search_by_tokens, 
            select_edges, select_nodes, add_neo4j_node, add_neo4j_edge, dict_to_dfrow

    struct Neo_node
        labels::Vector{String}
        sha1::String
        d_sha1::String
        hll_set::SetCore.HllSet
        props::Config
    end

    Neo_node(sha1::String, labels::String...; d_sha1::String="", card::Int, hll_set::SetCore.HllSet, props::Dict{String, Any}) = 
        Neo_node(sha1, collect(labels), d_sha1, card, hll_set, EasyConfig(props))

    
    function request(url, headers, query)
        response = HTTP.request("POST", url, headers, query)
        return JSON3.read(IOBuffer(String(response.body)))
    end

    function cypher(query)
        return JSON3.write(Dict("statements" => [Dict("statement" => query)]))
    end

    function collect_hll_sets(json::JSON3.Object, hlls::Dict{String, Neo_node}; p::Int=10)
        rows = [data["row"] for data in json.results[1].data]
        for row in rows
            labels = JSON3.read(row[1], Array{String,1})
            sha1 = row[2]
            d_sha1 = row[3]
            # println(row[4])
            dataset = JSON3.read(row[4], Vector{UInt64})
            
            props = JSON3.read(Store.remove_quotes(row[5]), Dict{String, Any})
            # println("props: ", props)

            hll = SetCore.HllSet{p}()
            SetCore.restore(hll, dataset)
            neo_node = Neo_node(labels, sha1, d_sha1, hll, props)
            hlls[sha1] = neo_node
        end
    end

    function search_by_tokens(db::SQLite.DB, query::String...)
        refs = Set()
        for token in query
            results = DBInterface.execute(db, "SELECT refs FROM tokens WHERE token LIKE ?;", ["%" * token * "%"]) |> DataFrame
            for result in eachrow(results)
                # println("result: ", (JSON3.read(result.refs)))
                if length(result) > 0                
                    refs = union(refs, Set(JSON3.read(result.refs)))
                end
            end
        end
        return refs
    end

    function select_nodes(db::SQLite.DB, refs::Set, nodes::Vector) 
        for ref in refs
            # println("ref: ", ref)
            results = DBInterface.execute(db, "SELECT sha1, labels, d_sha1, dataset, card, props FROM nodes WHERE sha1=?;", [ref]) |> DataFrame
            for result in eachrow(results)
                if length(result) > 0
                    push!(nodes, result)
                end
            end
        end
        return nodes
    end

    """
        This function creates a new node in the Neo4j database.
    """
    function add_neo4j_node(labels, data::DataFrameRow)
        sha1 = data.sha1
        d_sha1 =  data.d_sha1
        dataset = data.dataset
        card = data.card
        props = JSON3.read(data.props)
        set_props = join(["\"$key\": \"$value\"" for (key, value) in pairs(props)], ",")
        
        label = join(JSON3.read(labels, Array{String, 1}), ":")  # replace(labels, r"[\"\[\]]" => "")
        stmt = """MERGE (n:$label {sha1: '$sha1', labels: '$labels', d_sha1: '$d_sha1', dataset: '$dataset', card: $card}) 
            SET n.props = '{$set_props}'
        """
        
        return cypher(stmt)
    end

    function add_neo4j_nodes_by_refs(db::SQLite.DB, refs::Set, url, headers)
        nodes = Vector()
        LisaNeo4j.select_nodes(db, refs, nodes)

        # Add nodes to the Neo4j database
        for node in nodes
            labels = replace(string(node.labels), ";" => "")
            query = LisaNeo4j.add_neo4j_node(labels, node)
            data = LisaNeo4j.request(url, headers, query)
        end
    end

    function select_edges(db::SQLite.DB, refs::Set, edges::Vector) 
        edges_refs = Set()
        for ref in refs
            # println("ref: ", ref)
            results = DBInterface.
                execute(db, "SELECT source, target, r_type, props FROM edges WHERE source=? OR target=?;", [ref, ref]) |> DataFrame
            for result in eachrow(results)
                if length(result) > 0
                    push!(edges, result)
                    edges_refs = union(edges_refs, Set([result.source, result.target]))
                end
            end
        end
        return edges_refs
    end

    """
        This function creates a new edge in the Neo4j database.
    """
    function add_neo4j_edge(data::DataFrameRow)
        source = data.source
        target = data.target
        r_type = data.r_type
        # Parse the properties
        row = JSON3.read(data.props)
        
        # Build the Cypher query
        stmt = """
            MATCH (a), (b) WHERE a.sha1 = '$source' AND b.sha1 = '$target'
            MERGE (a)-[r:$r_type]->(b)
        """
        if length(row) > 0
            # Build the SET clause dynamically
            set_clause = join([" r.$(key) = '$(value)'" for (key, value) in pairs(row)], ",")

            stmt *= " SET $set_clause"
        end
        return cypher(stmt)
    end

    function add_neo4j_edges_by_refs(db::SQLite.DB, refs::Set, url, headers)
        edges = Vector()
        LisaNeo4j.select_edges(db, refs, edges)
        # println("edges: ", edges)

        # Add edges to the Neo4j database
        for edge in edges
            query = add_neo4j_edge(edge)
            # println("query: ", query)
            data = request(url, headers, query)
        end
    end

    # Utility functions
    #------------------
    function dict_to_dfrow(dict::Dict{String, Any})
        df = DataFrame(dict)
        df_row = df[1, :]
        return df_row
    end
end