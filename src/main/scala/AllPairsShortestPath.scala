import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{SparseMatrix, DenseMatrix, Matrix}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, BlockMatrix}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseMatrix => BDM, DenseVector, min, Matrix =>BM}


object AllPairsShortestPath {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AllPairsShortestPath").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val entries = sc.parallelize(Array(
      (0, 1, 20), (0, 2, 4), (0, 3, 2),
      (1, 0, 2), (1, 2, 1), (1, 3, 3),(2, 0, 1),
      (2, 1, 6), (2, 3, 5), (3, 0, 4), (3, 1, 2), (3, 2, 2))).map{case (i, j, v) => MatrixEntry(i, j, v)}
    val coordMat = new CoordinateMatrix(entries)
    val matA = coordMat.toBlockMatrix(2, 2).cache()
    val ApspPartitioner = GridPartitioner(matA.numRowBlocks, matA.numColBlocks, matA.blocks.partitions.length)
    matA.validate()
    val localMat = matA.toLocalMatrix()
    println(localMat.toString())
    println(distributedApsp(matA, 1, ApspPartitioner).toLocalMatrix().toString())
//    println(fromBreeze(localMinPlus(toBreeze(localMat), toBreeze(localMat.transpose))).toString())
//    val collectedValues = blockMin(matA.blocks, matA.transpose.blocks, ApspPartitioner).foreach(println)
//    blockMinPlus(matA.blocks, matA.transpose.blocks, matA.numRowBlocks, matA.numColBlocks, ApspPartitioner).foreach(println)
    System.exit(0)
  }

  /**
   * Convert a local matrix into a dense breeze matrix.
   * TODO: use breeze sparse matrix if local matrix is sparse
   */
  def toBreeze(A: Matrix): BDM[Double] = {
    val denseMat = A match {
      case dense: DenseMatrix => dense
      case sparse: SparseMatrix => sparse.toDense
    }
    new BDM[Double](denseMat.numRows, denseMat.numCols, denseMat.toArray)
  }

  /**
   * Convert from dense breeze matrix to local dense matrix.
   */
  def fromBreeze(dm: BDM[Double]): Matrix = {
    new DenseMatrix(dm.rows, dm.cols, dm.toArray, dm.isTranspose)
  }

  def localMinPlus(A: BDM[Double], B: BDM[Double]): BDM[Double] = {
    require(A.cols == B.rows, "Num rows of A does not match the num rows of B")
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
    AMinPlusB
  }

  def blockMin(Ablocks: RDD[((Int, Int), Matrix)], Bblocks: RDD[((Int, Int), Matrix)],
               ApspPartitioner: GridPartitioner): RDD[((Int, Int), Matrix)] = {
    val addedBlocks = Ablocks.join(Bblocks, ApspPartitioner).mapValues {
      case (a, b) => fromBreeze(min(toBreeze(a), toBreeze(b)))
    }
    addedBlocks
  }


  def blockMinPlus(Ablocks: RDD[((Int, Int), Matrix)], Bblocks: RDD[((Int, Int), Matrix)],
                   numRowBlocks: Int, numColBlocks: Int, 
                   ApspPartitioner: GridPartitioner): RDD[((Int, Int), Matrix)] = {

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

  /**
   *
   * @param A nxn adjacency matrix represented as a BlockMatrix
   * @param stepSize
   * @param ApspPartitioner
   * @return
   */
  def distributedApsp(A: BlockMatrix, stepSize: Int, ApspPartitioner: GridPartitioner): BlockMatrix = {
    require(A.numRows() == A.numCols(), "The adjacency matrix must be square.")
    require(A.rowsPerBlock == A.colsPerBlock, "The blocks making up the adjacency matrix must be square.")
    require(stepSize < A.rowsPerBlock, "Step size must be less than number of rows in a block.")
    val n = A.numRows()
    val niter = math.ceil(n * 1.0 / stepSize).toInt
    var apspRDD = A.blocks
    var rowRDD : RDD[((Int, Int), Matrix)] = null
    var colRDD : RDD[((Int, Int), Matrix)] = null
    // TODO: shuffle the data first if stepSize > 1
    for (i <- 0 to (niter - 1)) {
      val StartBlock = i * stepSize / A.rowsPerBlock
      val EndBlock = math.min((i + 1) * stepSize - 1, n - 1) / A.rowsPerBlock
      val startIndex = i * stepSize - StartBlock * A.rowsPerBlock
      val endIndex =  (math.min((i + 1) * stepSize - 1, n - 1) - EndBlock * A.rowsPerBlock).toInt
      if (StartBlock == EndBlock) {
        rowRDD = apspRDD.filter(_._1._1 == StartBlock)
          .mapValues(localMat => fromBreeze(toBreeze(localMat)(startIndex to endIndex, ::)))
        colRDD  = apspRDD.filter(_._1._2 == StartBlock)
          .mapValues(localMat => fromBreeze(toBreeze(localMat)(::, startIndex to endIndex)))
      } else {
        // we required StartBlock >= EndBlock - 1 at the beginning
        rowRDD = apspRDD.filter(kv => kv._1._1 == StartBlock || kv._1._1 == EndBlock)
          .map { case ((i, j), localMat) =>
            i match {
              case StartBlock =>
                ((i, j), fromBreeze(toBreeze(localMat)(startIndex to localMat.numRows, ::)))
              case EndBlock =>
                ((i, j), fromBreeze(toBreeze(localMat)(0 to endIndex, ::)))
            }
          }
        colRDD = apspRDD.filter(kv => kv._1._2 == StartBlock || kv._1._2 == EndBlock)
          .map { case ((i, j), localMat) =>
            j match {
              case StartBlock =>
                ((i, j), fromBreeze(toBreeze(localMat)(::, startIndex to localMat.numRows)))
              case EndBlock =>
                ((i, j), fromBreeze(toBreeze(localMat)(::, 0 to endIndex)))
            }
          }
      }

      apspRDD = blockMin(apspRDD, blockMinPlus(colRDD, rowRDD, A.numRowBlocks, A.numColBlocks, ApspPartitioner),
        ApspPartitioner)
    }
    return new BlockMatrix(apspRDD, A.rowsPerBlock, A.colsPerBlock, n, n)
  }
}

