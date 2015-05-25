import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{SparseMatrix, DenseMatrix, Matrix}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseMatrix => BDM, DenseVector, min, Matrix =>BM}

/**
 * Created by arzav on 5/23/15.
 */
object AllPairsShortestPath {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AllPairsShortestPath").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val entries = sc.parallelize(Array((0, 1, 20), (0, 2, 4), (0, 3, 2),
      (1, 0, 2), (1, 2, 1), (1, 3, 3),(2, 0, 1),
      (2,1, 6), (2, 3, 5), (3, 0, 4), (3, 1, 2), (3, 2, 2))).map{case (i, j, v) =>MatrixEntry(i, j, v)}
    val coordMat = new CoordinateMatrix(entries)
    val matA = coordMat.toBlockMatrix(2, 2).cache()
    val n1 = matA.rowsPerBlock
    val p1 = matA.numRowBlocks
    val n = matA.numRows.toInt
    val ApspPartitioner = GridPartitioner(matA.numRowBlocks, matA.numColBlocks, matA.blocks.partitions.length)
    matA.validate()
//    val collectedValues = blockMin(matA.blocks, matA.transpose.blocks, ApspPartitioner).foreach(println)
    blockMinPlus(matA.blocks, matA.transpose.blocks, matA.numRowBlocks, matA.numColBlocks, ApspPartitioner).foreach(println)
    System.exit(0)
  }

  /** changed the toBreeze() function in BlockMatrix class, convert to a dense breeze matrix*/
  def toBreeze(A: Matrix): BDM[Double] = {
    val Anew = A match{
      case dense: DenseMatrix => dense
      case sparse: SparseMatrix => sparse.toDense
    }
    new BDM[Double](Anew.numRows, Anew.numCols, Anew.toArray)
  }

  /** change the fromBreeze() function in Matrices class, can only convert from dense breeze matrix to dense matrix*/
  def fromBreeze(dm: BDM[Double]): Matrix = {
    val newMatrix = new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
    return newMatrix
  }

  def localMinPlus(A: BDM[Double], B: BDM[Double]): BDM[Double] = {
    require(A.cols == B.rows, "The Plus operation doesn't match dimension")
    val k = A.cols
    val onesA = DenseVector.ones[Double](A.rows)
    val onesB = DenseVector.ones[Double](B.cols)
    var AMinPlusB = A(::, 0) * onesA.t + onesB * B(0, ::)
    for (i <- 1 to (k-1)) {
      val a = A(::, i)
      val b = B(i, ::)
      val aPlusb = a * onesA.t + onesB * b
      AMinPlusB = min(aPlusb, AMinPlusB)
    }
    return AMinPlusB
  }

//  // Why mapValues doesn't work???
  def blockMin(Ablocks: RDD[((Int, Int), Matrix)], Bblocks: RDD[((Int, Int), Matrix)], ApspPartitioner: GridPartitioner): RDD[((Int, Int), Matrix)] = {
    val addedBlocks = Ablocks.join(Bblocks, ApspPartitioner)
      .mapValues {
      case (a, b) => fromBreeze(min(toBreeze(a), toBreeze(b)))
    }
    return addedBlocks
  }


  def blockMinPlus(Ablocks: RDD[((Int, Int), Matrix)], Bblocks: RDD[((Int, Int), Matrix)],
                   numRowBlocks: Int, numColBlocks: Int, ApspPartitioner: GridPartitioner): RDD[((Int, Int), Matrix)] = {
    // Each block of A must do cross plus with the corresponding blocks in each column of B.
    // TODO: Optimize to send block to a partition once, similar to ALS
    val flatA = Ablocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
      Iterator.tabulate(numColBlocks)(j => ((blockRowIndex, j, blockColIndex), block))
    }
    // Each block of B must do cross plus with the corresponding blocks in each row of A.
    val flatB = Bblocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
      Iterator.tabulate(numRowBlocks)(i => ((i, blockColIndex, blockRowIndex), block))
    }
    val newBlocks = flatA.join(flatB, ApspPartitioner)
      .map { case ((blockRowIndex, blockColIndex, _), (a, b)) =>
      val C = localMinPlus(toBreeze(a), toBreeze(b))
      ((blockRowIndex, blockColIndex), C)
    }.reduceByKey(ApspPartitioner, (a, b) => min(a, b))
      .mapValues(C => fromBreeze(C))
    return newBlocks
  }
}
