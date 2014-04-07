using NetworkJ

adj_array = readdlm("/Users/g/Documents/tie_range/data/Simmons81.edgelist",' ')

adj_array = adj_array[:,1:2]

e_count = size(adj_array)[1]

g = empty_graph(1.)
for i=1:e_count
    edge = (adj_array[i,1]+1, adj_array[i,2]+1)
    add_edge!(g, edge)
end
@time e, d = tie_range(g)