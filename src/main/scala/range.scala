import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Set //all sets here are mutuable for convenience
import scala.collection.mutable.Map

/*
There are three views in GraphX:
    1) Vertex, which is like a list of tuples (VertexId, Attr) 
    2) Edge, which returns Edge[Src, Dst, Attr]
    3) Triplet, which returns EdgeTriplet[Src, Dst, srcAttr, dstAttr, attr], where attr is the edge attr

Tie range approach:
    1) Token passing
    2) Global found edge range hash
    3) Post-process after each wave by popping all tokens in global found hash

Dual start: each node should issue a token for each of its neighbors

At each step, a node should pass each token to all of its neighbors.
    The neighbor should reject the token if: the token already exists
    The token contains the neighbor as the destination and range = 1

We are done if:
    A node has two tokens where: token1.src = token2.dst, and token token1.dst = token2.src
    Put (src, dst), (dst, src) in the found hash


An edge triplet should be thought of as an outlink
    Function should be written to deal with outlinks
    

*/

object TieRange{

    //override equals to care only about the first two elements
    case class Token( srcId: Long, dstId: Long, originId: Long, var range: Int ) {
        def companion(t: Token): Boolean = {
            if (t.srcId == srcId & t.dstId == dstId){
                if ((originId == srcId && t.originId == t.dstId) || (originId == dstId && t.originId == t.srcId)) {
                    true
                }
                else {
                    false
                }
            }
            else {
                false
            }
        }
        override def equals( arg: Any ) = arg match {
            case Token(s, t, u, _) => (s, t, u) == (srcId, dstId, originId)
            case _ => false
        }
        override def hashCode() = (srcId, dstId, originId).hashCode
    }



    //map function should have ego pass all tokens to alter 


    //this method takes an edge triplet and a set of known edge ranges
    //passes all tokens on src to dst if
    //    1. range not found
    //    2. token.dst != triplet.dst
    //"returns" a triplet.sendToDst(Tokens To Add)

    //maybe rangeKnown should be an array of length n
    //check if rangeKnown will be shared in memory
    //double check whether triplet is a pointer or a copied object

    //we want to keep the range of the tokens already at the destination
    //to do this:
    //1) put all destination tokens in a set (except those that are found)
    //2) THEN add all source tokens (except those that are found), with range += 1
    //this works because we don't replace an object already in a set
    def token_pass_map(triplet: EdgeContext[Set[Token], Int, Set[Token]]): Unit = {
        val newTokenSet = scala.collection.mutable.Set[Token]()
        val dstTokens = triplet.dstAttr
        val srcTokens = triplet.srcAttr
        for (token <- dstTokens){
            //range unknown, token destination not triplet destination
            //rangeKnown.get((token.srcId, token.dstId)).get != -1
            if (token.dstId != triplet.dstId & token.range != 0){ //&& !rangeKnown.contains(token)){
                newTokenSet += token.copy()
            }
        }
        for (token <- srcTokens){
            //range unknown, token destination not triplet destination
            if (token.dstId != triplet.dstId){ //&& !rangeKnown.contains(token)){
                //these are being passed so we increment the range
                //recall that these will not be added to newTokenSet if the src-dst pair is already there
                //this prevents incrementing the range of a token from the destination node
                val newToken = token.copy()
                newToken.range += 1
                newTokenSet += newToken
            }
        }
        //send the message!
        triplet.sendToDst(newTokenSet)
    }

    def token_pass_reduce(a: Set[Token], b: Set[Token]): Set[Token] = {
        a.union(b)
    }


    //keeps all tokens with range greater than or equal to the cutoff
    def keep_tokens(id: Long, attr: Set[Token], cutoff: Int) = {
        val output = Set[Token]()
        for (token <- attr) {
            if (token.range >= cutoff) {
                output += token
            }
        }
        output
    }



    //want to get this faster than n^2
    //strategy: make a map
    //make one pass through both forwards and backwards, the condition we want is equality
    //create a map of form: firstElem -> secondElem -> (count, range)
    //this should be on order 2n
    def introspection_map(id: Long, attr: Set[Token]): Set[Token] = {
        //this is still a set
        val foundMap = scala.collection.mutable.Map[Long, Map[Long, Token]]()
        val output = Set[Token]()

        for (t <- attr) {
            if (!foundMap.contains(t.srcId)) {
                foundMap += (t.srcId -> Map[Long, Token]())
                foundMap(t.srcId) += (t.dstId -> t)
            }
            else if (!foundMap(t.srcId).contains(t.dstId)) {
                foundMap(t.srcId) += (t.dstId -> t)
            }
            else {
                val u = foundMap(t.srcId)(t.dstId)
                //handle the check at the case class level
                if (t.companion(u)){
                    output += Token(t.srcId, t.dstId, 0L, t.range + u.range)
                }
            }
        }
        output
    }


    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Tie range")
        val sc = new SparkContext(conf)

        val path = "/Users/g/Google Drive/independent-work/range/Reed98_gc.ncol"

        val edgeData = sc.textFile(path).map(s => s.split(" ").map(x => x.toInt))
        val edgeArray = edgeData.map(s => Edge(s(0), s(1), 0)).collect

        //-1: haven't found or infinite
        val rangeKnown = Set[Token]()

        val edges: RDD[Edge[Int]] = sc.parallelize(edgeArray)

        //Where am I? What am I doing? Where am I going?
        val users: VertexRDD[Int] = VertexRDD(edgeData.flatMap(x => x).distinct().map(id => (id.toLong, 1)))

        val defaultUserOne = -1

        val graphOne = Graph(users, edges, defaultUserOne)

        //initize tokens
        //send a src-dst token to source
        val withTokens: VertexRDD[Set[Token]] = graphOne.aggregateMessages[Set[Token]](
            triplet => {
                triplet.sendToSrc(Set( Token(triplet.srcId, triplet.dstId, triplet.srcId, 0 ) ) )
                triplet.sendToDst(Set( Token(triplet.srcId, triplet.dstId, triplet.dstId, 0 ) ) )
            },
            (a, b) => a.union(b)
        )

        val defaultUserTwo = Set[Token]()
        val graphTwo = Graph(withTokens, edges, defaultUserTwo)



        val newVertices: VertexRDD[Set[Token]] = graphTwo.aggregateMessages[Set[Token]](token_pass_map, token_pass_reduce)

        val newGraph = Graph(newVertices, edges, defaultUserTwo)

        //delete the zeroes, have already been passed
        val newGraphPruned: Graph[Set[Token], Int] = newGraph.mapVertices((id, attr) => keep_tokens(id, attr, 1))

        val found = newGraph.mapVertices((id, attr) => introspection_map(id, attr)).vertices.flatMap(x => x._2).map(x => ((x.srcId, x.dstId), x.range)).reduceByKey((a, b) => math.min(a, b))

        println(found.collect)

        //val newNewVertices: VertexRDD[Set[Token]] = newGraphPruned.aggregateMessages[Set[Token]](token_pass_map, token_pass_reduce)

        //val newNewGraph = Graph(newNewVertices, edges, defaultUser)

        //val newNewGraphPruned = newNewGraph.mapVertices((id, attr) => keep_tokens(id, attr, 2))

        //val found = newNewGraph.mapVertices((id, attr) => introspection_map(id, attr)).vertices.flatMap(x => x._2).map(x => ((x.srcId, x.dstId), x.range)).reduceByKey((a, b) => math.min(a, b))
    }
}