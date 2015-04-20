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
    //originId is the spot where the TOKEN started
    //i.e. Token(src, dst, dst, 1) represnts the (src, dst) edge but the token starts at dst
    case class Token( srcId: Long, dstId: Long, originId: Long, var range: Int ) {
        def companion: Token = {
            if (originId == srcId) {
                Token(srcId, dstId, dstId, range)
            }
            else if (originId == dstId) {
                Token(srcId, dstId, srcId, range)
            }
            else {
                Token(srcId, dstId, originId, range)
            }
        }
        override def equals( arg: Any ) = arg match {
            case Token(s, t, u, _) => (s, t, u) == (srcId, dstId, originId)
            case _ => false
        }
        override def hashCode() = (srcId, dstId, originId).hashCode
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
        val srcId = triplet.srcId
        val dstId = triplet.dstId

        for (t <- srcTokens) {
            //(j, k), for candidate j and fixed k
            val tempToken = Token(t.dstId, dstId, dstId, 2)
            if (dstTokens.contains(tempToken)) {
                return Token(srcId, dstId, -1L, 2)
            }
        }
        return Token(-1L, -1L, -1L, -1)
    }


    //strategy here is a little different
    //for each edge, we cycle through the source tokens
    //remember that Token(1,2,_) will begin at both 1 and 2
    //so for some third edge, after a step of passing, if Token(1,2,2) in both src and dst
    //we have found a range 3 path

    //assume we only have range=2 tokens
    def range_three_map(triplet: EdgeTriplet[Set[Token], Int]): Set[Token] = {
        val rangeThreeSet = Set[Token]()
        val srcSet = triplet.srcAttr
        val dstSet = triplet.dstAttr
        val srcId = triplet.srcId
        val dstId = triplet.dstId

        for (t <- srcSet){
            if (dstSet.contains(t)){
                val newToken = t.copy()
                newToken.range = 3
                rangeThreeSet += newToken
            }
        }

        rangeThreeSet
    }


    //if we haven't found, pass to neighbors
    def pass_messages(triplet: EdgeContext[Set[Token], Int, Set[Token]], rangeKnown: Set[Token]): Unit = {
        val toSend = Set[Token]()
        val tokens = triplet.srcAttr.union(triplet.dstAttr)

        for (t <- tokens){
            val foundToken = Token(t.srcId, t.dstId, -1, 2)
            if (!rangeKnown.contains(foundToken)){
                val newMsg = t.copy()
                newMsg.range += 1
                toSend += newMsg
            }
        }
        triplet.sendToDst(toSend)
        triplet.sendToSrc(toSend)
    }

    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Tie range")
        val sc = new SparkContext(conf)

        val path = "/Users/g/Google Drive/independent-work/range/Reed98_gc.ncol"

        val edgeData = sc.textFile(path).map(s => s.split(" ").map(x => x.toInt))
        val edgeArray = edgeData.map(s => Edge(s(0), s(1), 0)).collect

        val edges: RDD[Edge[Int]] = sc.parallelize(edgeArray)

        val users: VertexRDD[Int] = VertexRDD(edgeData.flatMap(x => x).distinct().map(id => (id.toLong, 1)))

        val defaultUserOne = -1

        val graphOne = Graph(users, edges, defaultUserOne)

        //initize tokens
        //send a src-dst token to source
        val withTokens: VertexRDD[Set[Token]] = graphOne.aggregateMessages[Set[Token]](
            triplet => {
                triplet.sendToSrc(Set( Token(triplet.srcId, triplet.dstId, triplet.srcId, 1 ) ) )
                triplet.sendToDst(Set( Token(triplet.srcId, triplet.dstId, triplet.dstId, 1 ) ) )
            },
            (a, b) => a.union(b)
        )

        val defaultUserTwo = Set[Token]()
        val graphTwo = Graph(withTokens, edges, defaultUserTwo)

        val rangeTwoKnown = Set[Token]() ++= graphTwo.triplets.map(triplet => range_two_map(triplet)).collect.toSet

        println(rangeTwoKnown)

        val passMessages: VertexRDD[Set[Token]] = graphTwo.aggregateMessages[Set[Token]](
            triplet => pass_messages(triplet, rangeTwoKnown),
            (a, b) => a.union(b)
        )

        val graphThree = Graph(passMessages, edges, defaultUserTwo)

        val rangeThreeFound = graphThree.triplets.map(triplet => range_three_map(triplet)).reduce((a, b) => a.union(b))

        println(rangeTwoKnown.size)
        println(rangeThreeFound.size)



    }
}