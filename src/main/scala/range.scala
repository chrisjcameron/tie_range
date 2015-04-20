import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._
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
    case class Token( srcId: Long, dstId: Long, var range: Int ) {
        override def equals( arg: Any ) = arg match {
            case Token(s, t, _) => (s, t) == (srcId, dstId)
            case _ => false
        }
        override def hashCode() = (srcId, dstId).hashCode
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


    //idea: if i --> j, and j --> k, i --> k then the i --> k link is range two
    //      note that if i --> k deleted, i --> j --> k still exists
    //      the i --> k triplet will have i's tokens and k's tokens
    //      i will have Token(i, j, 0)
    //      k will have Token(j, k, 0)
    //we really just need to match the first two elements
    //condition:
    //      Token(i, k, 0) exists for i (i has an outlink to k)
    //      There is some j such that Token(i, j, 0) exists and Token(j, k, 0) exists
    //alg:
    //      for each of i's tokens (i, j):
    //          check (j, k) in Set(k's tokens)
    //      if yes, return true
    //
    //on one tie, we just check if that tie is range two! forget the rest       
    def range_two_map(triplet: EdgeTriplet[Set[Token], Int]): Token = {
        val rangeTwoSet = Set[Token]()
        val srcTokens = triplet.srcAttr
        val dstTokens = triplet.dstAttr
        val dstId = triplet.dstId

        for (t <- srcTokens) {
            //(j, k), for candidate j and fixed k
            val tempToken = Token(t.dstId, dstId, 0)
            if (dstTokens.contains(tempToken)) {
                return tempToken
            }
        }
        return Token(-1L, -1L, -1)
    }

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Tie range")
        val sc = new SparkContext(conf)

        val path = "/Users/g/Google Drive/independent-work/range/release-youtube-links_gc.ncol"

        val edgeData = sc.textFile(path).map(s => s.split(" ").map(x => x.toInt))
        val edgeArray = edgeData.map(s => Edge(s(0), s(1), 0)).collect

        //-1: haven't found or infinite
        val rangeKnown = Map[(Long, Long), Int]()

        val edges: RDD[Edge[Int]] = sc.parallelize(edgeArray)

        val users: VertexRDD[Int] = VertexRDD(edgeData.flatMap(x => x).distinct().map(id => (id.toLong, 1)))

        val defaultUserOne = -1

        val graphOne = Graph(users, edges, defaultUserOne)

        //initize tokens
        //send a src-dst token to source
        val withTokens: VertexRDD[Set[Token]] = graphOne.aggregateMessages[Set[Token]](
            triplet => {
                triplet.sendToSrc(Set( Token(triplet.srcId, triplet.dstId, 0 ) ) )
                triplet.sendToDst(Set( Token(triplet.srcId, triplet.dstId, 0 ) ) )
            },
            (a, b) => a.union(b)
        )



        val defaultUserTwo = Set[Token]()
        val graphTwo = Graph(withTokens, edges, defaultUserTwo)

        val rangeTwo = graphTwo.triplets.map(triplet => range_two_map(triplet))

        println(rangeTwo.collect.toSet.size)



    }
}