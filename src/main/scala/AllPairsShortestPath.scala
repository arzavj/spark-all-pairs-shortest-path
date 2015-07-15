import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.concurrent.duration._


private object AllPairsShortestPath {
  def main(args: Array[String]) {
    //println(args.mkString(", "))
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("AllPairsShortestPath").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir("checkpoint/")
    val n = 6
    //val m = args(1).toInt
    //val stepSize = args(2).toInt
    //val interval = args(3).toInt
    val graph = generateGraph(n, sc)
    val apsp = new DistributedBlockFW

    //val matA = generateInput(graph, n, sc, m, m)
    //val ApspPartitioner = GridPartitioner(matA.numRowBlocks, matA.numColBlocks, matA.blocks.partitions.length)

    //val localMat = matA.toLocalMatrix()
    //val resultMat = time{distributedApsp(matA, stepSize, ApspPartitioner, interval)}


    val result = time {
      apsp.compute(graph, 3)
    }
    //val resultMat1 = time{distributedApsp(matA, 1, ApspPartitioner, interval)}
    val resultLocalMat = result.toLocal()
    //val resultLocalMat1 = resultMat1.toLocalMatrix()
    // println(fromBreeze(localMinPlus(toBreeze(localMat), toBreeze(localMat.transpose))).toString())
    //println(matA.toLocalMatrix().toString())
    //println(localMat.toString)

    println(resultLocalMat.toString)
    println()
    println(result.lookupDist(1, 2))
    //println()
    //println(resultLocalMat1.toString)
    // val collectedValues = blockMin(matA.blocks, matA.transpose.blocks, ApspPartitioner).foreach(println)
    // blockMinPlus(matA.blocks, matA.transpose.blocks, matA.numRowBlocks, matA.numColBlocks, ApspPartitioner).foreach(println)
    System.exit(0)
  }



  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val duration = Duration(t1 - t0, NANOSECONDS)
    println("Elapsed time: " + duration.toSeconds + "s")
    result
  }


  def generateGraph(n: Int, sc: SparkContext): Graph[Long, Double] = {
    val graph = GraphGenerators.logNormalGraph(sc, n).mapEdges(e => e.attr.toDouble)
    graph
  }
}








