# Pkg.add("Graphs") 
using Graphs


function tie_ranges_dijk(g::GenericGraph)
    e_count = Graphs.num_edges(g)
    e_lens = ones(e_count)
    dist = ones(e_count)
    for pair in enumerate(Graphs.edges(g))
        index, edge = pair
        s_node = Graphs.source(edge,g)
        d_node = Graphs.target(edge,g)
        dist[index] = Graphs.num_vertices(g)
        #println(pair)
        e_lens[index] = Graphs.dijkstra_shortest_paths(g,dist,s_node).dists[d_node]
        dist[index] = 1
    end
    return e_lens
end

# Compute the range of all edges in graph
function tie_ranges(g::GenericGraph)
    e_count = Graphs.num_edges(g)
    e_lens = ones(e_count)
    dist = ones(e_count)
    for pair in enumerate(Graphs.edges(g))
        index, edge = pair        
        e_lens[index] = tie_range(g,edge)
    end
    return e_lens
end

# Compute the range of a single edge, via BFS
function tie_range(g::GenericGraph, null_edge)

    s_node = Graphs.source( null_edge, g )
    d_node = Graphs.target( null_edge, g )
    visited = BitVector( Graphs.num_vertices(g) )

    # Set up inital step, skipping the null_edge
    node_queue = Set{Int64}()
    for v in out_neighbors(s_node, g)
        if !is(v, d_node)
            push!(node_queue, v)
            visited[v] = true
        end
    end

	# BFS through graph until target node is reached
	# if target node is unreachable, return -1
    tie_len = 2
    while !isempty(node_queue)
        next_node_queue = Set{Int64}()
        for nbr in node_queue
            v_set = out_neighbors(nbr, g)
            for v in v_set
                if is(v,d_node)
                	# We found target
                    return tie_len
                end
                if !visited[v]
                    push!(next_node_queue, v)
                    visited[v] = true
                end
            end
        end
        node_queue = next_node_queue
        tie_len += 1
    end
    return -1
end

################################################################
##
## Main
##
################################################################

#adj_array = readdlm("/Users/Shared/Vesta/facebook100/simmons81.edge",' ', Int64)
#adj_array = readdlm("/Users/Shared/Vesta/facebook100/Reed98.edge",' ', Int64)
#adj_array = readdlm("/Users/Shared/Vesta/facebook100/Northeastern19.edge",' ', Int64)
adj_array = readdlm("/Users/Shared/Vesta/facebook100/Texas84.edge",' ', Int64)


node_count = maximum(adj_array)+1
e_count = size(adj_array)[1]

g = Graphs.simple_graph(node_count, is_directed=false)
for i=1:e_count
    Graphs.add_edge!(g, adj_array[i,1]+1, adj_array[i,2]+1)
end

tic(); tr = tie_ranges(g); toc()


