import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.scalatest.{Outcome, FlatSpec}
import AllPairsShortestPath._
import breeze.linalg.{DenseMatrix => BDM}

class APSPSpec extends FlatSpec {

  val conf = new SparkConf().setAppName("AllPairsShortestPath").setMaster("local[4]").set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext(conf)

  override def withFixture(test: NoArgTest) : Outcome = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    try {
      test() // invoke the test function
    }
  }

  def fourByFourBlockMatrx = {
    val entries = sc.parallelize(Array(
      (0, 1, 20), (0, 2, 4), (0, 3, 2),
      (1, 0, 2), (1, 2, 1), (1, 3, 3), (2, 0, 1),
      (2, 1, 6), (2, 3, 5), (3, 0, 4), (3, 1, 2), (3, 2, 2))).map { case (i, j, v) => MatrixEntry(i, j, v) }
    val coordMat = new CoordinateMatrix(entries)
    val matA = coordMat.toBlockMatrix(2, 2).cache()
    matA
  }

  def ApspPartitioner = {
    GridPartitioner(fourByFourBlockMatrx.numRowBlocks, fourByFourBlockMatrx.numColBlocks, fourByFourBlockMatrx.blocks.partitions.length)
  }

  "The sample 4x4 Block Matrix" should "be valid" in {
    fourByFourBlockMatrx.validate()
  }

  it should "match our APSP matrix" in {
    println(fourByFourBlockMatrx.toLocalMatrix())
    val observed = toBreeze(distributedApsp(fourByFourBlockMatrx, 1, ApspPartitioner, sc, 2).toLocalMatrix())
    val expected = BDM(
      (0.0, 4.0, 4.0, 2.0),
      (2.0, 0.0, 1.0, 3.0),
      (1.0, 5.0, 0.0, 3.0),
      (3.0, 2.0, 2.0, 0.0)
    )
    assert(observed === expected)
  }
}
