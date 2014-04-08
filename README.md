Compute Tie Range for each tie in a graph. 

For speed, make sure graph is a single component and remove nodes with degree < 2. 
In the future, the code could handle this automatically. 

The C code requires igraph to build. The compile command is included as a comment. 

    Timings on Vesta                                 
    |---------------------------------------------------|
    | Network  | #nodes |  #edges  | c_times  | j_times |
    |---------------------------------------------------|
    |       1  |   962  |   18812  |   0.17   |   0.58  |
    |       2  |  1510  |   32984  |   0.67   |   1.24  |
    |       3  | 13868  |  381919  |  11.31   |  18.00  |
    |       4  | 36364  | 1590651  | 129.60   | 504.60  |
    |---------------------------------------------------|
    
  
+This code is licensed under a [GPL-3 license](https://www.gnu.org/copyleft/gpl.html).

+The data is not coverd by this license. Get the data from https://archive.org/details/oxford-2005-facebook-matrix
