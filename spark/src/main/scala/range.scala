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
  Compute tie-range for undirected graphs

  We employ token passing

  Each node issues a token for each neighbor

  Algo:
    Generate (src, dst) tokens for each node based on links

    Map: node passes all tokens to all neighbors with range + 1
    Reduce: node only accepts tokens if it does not possess (src, dst) already
        node rejects if it is the destination and range = 1

    If a node has 2 tokens of form (src, dst) and (dst, src) then we are done for that pair

  An edge triplet should be thought of as an outlink
    Function should be written to deal with outlinks


*/

case class Token(
    srcId: Long,
    dstId: Long,
    var range: Int
) {
    override def hashCode() = (srcId, dstId).hashCode
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
        val srcTokens = triplet.srcAttr
        val dstTokens = triplet.dstAttr
        val srcId = triplet.srcId
        val dstId = triplet.dstId

        for (t <- srcTokens) {
            //needs to be (i, j), cannot be (i, k)
            if (t.srcId == triplet.srcId && t.dstId != triplet.dstId){
                val tempToken = Token(t.dstId, dstId, dstId, true, 2)
                if (dstTokens.contains(tempToken)) {
                    return Token(srcId, dstId, -1L, true, 2)
                }
            }
            //(j, k), for candidate j and fixed k
        }
        return Token(-1L, -1L, -1L, true, -1)
    }

    //if we haven't found, pass to neighbors

    //pass tokens to a node if:
    //      1) they are not the destiantion
    //      2) they are not the source
    //if we pass to
    def pass_messages(triplet: EdgeContext[Set[Token], Int, Set[Token]]): Unit = {

        val srcTokens = triplet.srcAttr
        val dstTokens = triplet.dstAttr
        val toSrc = Set[Token]()
        val toDst = Set[Token]()

        //downstream
        for (t <- srcTokens){
            val foundToken = Token(t.srcId, t.dstId, -1L, true, 2)
            //don't pass to destination
            if (t.dstId != triplet.dstId){
                val dstPass = t.copy()
                dstPass.range += 1
                dstPass.downstream = true
                toDst += dstPass
            }
        }

        //upstream
        for (t <- dstTokens){
            val foundToken = Token(t.srcId, t.dstId, -1L, true, 2)
            //don't pass to destination
            if (t.srcId != triplet.srcId){
                val srcPass = t.copy()
                srcPass.range += 1
                srcPass.downstream = false
                toSrc += srcPass
            }

        }

        triplet.sendToDst(toDst)
        triplet.sendToSrc(toSrc)
    }


    //strategy:
    //for each src, go through all inlinks
    //for each dst, go through all outlinks
    //
    def range_three_map(triplet: EdgeTriplet[Set[Token], Int], edgeSet: collection.immutable.Set[(Long, Long)], rangeTwoKnown: collection.immutable.Set[Token]): Token = {
        val rangeThreeSet = Set[Token]()
        val srcSet = triplet.srcAttr
        val dstSet = triplet.dstAttr
        val srcId = triplet.srcId
        val dstId = triplet.dstId

        for (t <- srcSet){
            //inlink to src
            if (t.dstId == srcId){
                for (u <- dstSet) {
                    //outlink from dst
                    if (u.srcId == dstId){
                        val candidate = (t.srcId, u.dstId)
                        val candidateToken = Token(t.srcId, u.dstId, -1L, true, 3)
                        if (edgeSet.contains(candidate) && !rangeTwoKnown.contains(candidateToken)){
                            return candidateToken
                        }
                    }
                }
            }
        }
        return Token(-1L, -1L, -1L, true, -1)
    }


object TieRange{

    // initialize tokens
    def createToken(
        triplet: EdgeContext[Long, Int, Token]
    ): Unit = {
        val t = Token(triplet.srcId, triplet.dstId, 1)
        triplet.sendToSrc(t)
    }
    // very simple, for each pair, pass from dst to src
    def initializeTokens(g: Graph): Graph = {
        val tokenGraph: Graph = g.aggregateMessages[Token](
            triplet => createToken(triplet),
            (a, b) => a.union(b)
        )
        return tokenGraph
    }

    // pass tokens
    def passToken(g: )


    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Tie range")
        val sc = new SparkContext(conf)

        val path = "/Users/g/Google Drive/independent-work/range/Texas84_gc.ncol"

        val edgeData = sc.textFile(path).map(s => s.split(" ").map(x => x.toInt))
        val edgeArray = edgeData.map(s => Edge(s(0), s(1), 0)).collect
        val edgeSet = edgeData.map(s => (s(0).toLong, s(1).toLong)).collect.toSet

        val edges: RDD[Edge[Int]] = sc.parallelize(edgeArray)

        val users: VertexRDD[Int] = VertexRDD(edgeData.flatMap(x => x).distinct().map(id => (id.toLong, 1)))

        val defaultUser = -1

        val g = Graph(users, edges, defaultUserOne)

        val tokenGraph = initializeTokens(g)

        passTokens(tokenGraph)

        //initize tokens
        //send a src-dst token to source
        val withTokens: VertexRDD[Set[Token]] = graphOne.aggregateMessages[Set[Token]](
            triplet => {
                triplet.sendToSrc(Set( Token(triplet.srcId, triplet.dstId, triplet.srcId, true, 1 ) ) )
                triplet.sendToDst(Set( Token(triplet.srcId, triplet.dstId, triplet.dstId, true, 1 ) ) )
            },
            (a, b) => a.union(b)
        )
    }
}
