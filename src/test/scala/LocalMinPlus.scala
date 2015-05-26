import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.scalatest.{FlatSpec}
import breeze.linalg.{DenseMatrix => BDM, DenseVector, min, Matrix =>BM}
import AllPairsShortestPath._

/**
 * Created by arzav on 5/26/15.
 */
class LocalMinPlus extends FlatSpec {


  def fourByFourBlockMatrx = {
    BDM(
      (0.0, 20.0, 4.0, 2.0),
      (2.0, 0.0, 1.0, 3.0),
      (1.0, 6.0, 0.0, 5.0),
      (4.0, 2.0, 2.0, 0.0)
    )
  }

  def fourByFourMinPlusProduct = {
    BDM(
      (0.0,  2.0,  1.0,  2.0),
      (2.0,  0.0,  1.0,  2.0),
      (1.0,  1.0,  0.0,  2.0),
      (2.0,  2.0,  2.0,  0.0)
    )
  }

  "The minPlus product of the sample 4x4 matrix with itself" should "be correct" in {
    assert(localMinPlus(fourByFourBlockMatrx, fourByFourBlockMatrx.t) === fourByFourMinPlusProduct)
  }
}
