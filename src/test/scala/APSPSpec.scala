import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.scalatest.{FlatSpec}

/**
 * Created by arzav on 5/26/15.
 */
class APSPSpec extends FlatSpec {

  def fixture = {
    new {
      val conf = new SparkConf().setAppName("AllPairsShortestPath").setMaster("local[4]")
      val sc = new SparkContext(conf)
    }
  }

  def fourByFourBlockMatrx = {
    val f = fixture
    val entries = f.sc.parallelize(Array(
      (0, 1, 20), (0, 2, 4), (0, 3, 2),
      (1, 0, 2), (1, 2, 1), (1, 3, 3), (2, 0, 1),
      (2, 1, 6), (2, 3, 5), (3, 0, 4), (3, 1, 2), (3, 2, 2))).map { case (i, j, v) => MatrixEntry(i, j, v) }
    val coordMat = new CoordinateMatrix(entries)
    val matA = coordMat.toBlockMatrix(2, 2).cache()
    val ApspPartitioner = GridPartitioner(matA.numRowBlocks, matA.numColBlocks, matA.blocks.partitions.length)
    matA
  }

  "The sample 4x4 Block Matrix" should "be valid" in {
    fourByFourBlockMatrx.validate()
  }
}
