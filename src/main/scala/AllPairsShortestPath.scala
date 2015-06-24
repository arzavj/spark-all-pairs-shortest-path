import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{SparseMatrix, DenseMatrix, Matrix}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, BlockMatrix}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseMatrix => BDM, sum, DenseVector, min}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.concurrent.duration._


object AllPairsShortestPath {

  def main(args: Array[String]): Unit = {
    println(args.mkString(", "))
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("AllPairsShortestPath").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint/")
    val n = args(0).toInt
    val m = args(1).toInt
    val stepSize = args(2).toInt
    val interval = args(3).toInt
    val graph = generateGraph(n, sc)
    val matA = generateInput(graph, n, sc, m, m)
    val ApspPartitioner = GridPartitioner(matA.numRowBlocks, matA.numColBlocks, matA.blocks.partitions.length)

    val localMat = matA.toLocalMatrix()
    val resultMat = time{distributedApsp(matA, stepSize, ApspPartitioner, sc, interval)}
    //val resultMat1 = time{distributedApsp(matA, 1, ApspPartitioner, sc, interval)}
    val resultLocalMat = resultMat.toLocalMatrix()
    //val resultLocalMat1 = resultMat1.toLocalMatrix()
   // println(fromBreeze(localMinPlus(toBreeze(localMat), toBreeze(localMat.transpose))).toString())
    //println(matA.toLocalMatrix().toString())
    //println(localMat.toString)

    //println(resultLocalMat.toString)
    //println()
    //println(resultLocalMat1.toString)
   // val collectedValues = blockMin(matA.blocks, matA.transpose.blocks, ApspPartitioner).foreach(println)
   // blockMinPlus(matA.blocks, matA.transpose.blocks, matA.numRowBlocks, matA.numColBlocks, ApspPartitioner).foreach(println)
    System.exit(0)
  }


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val duration = Duration(t1 - t0, NANOSECONDS)
    println("Elapsed time: " + duration.toSeconds + "s")
    result
  }


  def generateGraph(n: Int, sc: SparkContext): Graph[Long, Double] = {
    val graph = GraphGenerators.logNormalGraph(sc, n).mapEdges(e => e.attr.toDouble)
    graph
  }


  /**
   *  add infinity to missing off diagonal elements and 0 to diagonal elements
   */
  def addInfinity(A: SparseMatrix, rowBlockID: Int, colBlockID: Int): Matrix = {
    val inf = scala.Double.PositiveInfinity
    val result: BDM[Double] = BDM.tabulate(A.numRows, A.numCols){case (i, j) => inf}
    for (j <- 0 until A.values.length)
        for (i <- 0 until A.numCols) {
          if (j >= A.colPtrs(i) & j < A.colPtrs(i + 1))
            result(A.rowIndices(j), i) = A.values(j)
        }
    if (rowBlockID == colBlockID) {
      require(A.numCols == A.numRows, "Diagonal block should have a square matrix")
      for (i <- 0 until A.numCols)
        result(i, i) = 0.0
    }
    fromBreeze(result)
  }


  def generateInput(graph: Graph[Long,Double], n: Int, sc:SparkContext,
                    RowsPerBlock: Int, ColsPerBlock: Int): BlockMatrix = {
    require(RowsPerBlock == ColsPerBlock, "need a square grid partition")
    val entries = graph.edges.map { case edge => MatrixEntry(edge.srcId.toInt, edge.dstId.toInt, edge.attr) }
    val coordMat = new CoordinateMatrix(entries, n, n)
    val matA = coordMat.toBlockMatrix(RowsPerBlock, ColsPerBlock)
    val ApspPartitioner = GridPartitioner(matA.numRowBlocks, matA.numColBlocks, matA.blocks.partitions.length)
    require(matA.numColBlocks == matA.numRowBlocks)

    // make sure that all block indices appears in the matrix blocks
    // add the blocks that are not represented
    val activeBlocks: BDM[Int] = BDM.zeros[Int](matA.numRowBlocks, matA.numColBlocks)
    //matA.blocks.foreach{case ((i, j), v) => activeBlocks(i, j)= 1}
    val activeIdx = matA.blocks.map { case ((i, j), v) => (i, j) }.collect()
    activeIdx.foreach { case (i, j) => activeBlocks(i, j) = 1 }
    val nAddedBlocks = matA.numRowBlocks * matA.numColBlocks - sum(activeBlocks)
    // recognize which blocks need to be added
    val addedBlocksIdx = Array.range(0, nAddedBlocks).map(i => (0, i))
    var index = 0
    for (i <- 0 until matA.numRowBlocks)
      for (j <- 0 until matA.numColBlocks) {
        if (activeBlocks(i, j) == 0) {
          addedBlocksIdx(index) = (i, j)
          index = index + 1
        }
      }
    // Create empty blocks with just the non-represented block indices
    val addedBlocks = sc.parallelize(addedBlocksIdx).map { case (i, j) => {
      var nRows = matA.rowsPerBlock
      var nCols = matA.colsPerBlock
      if (i == matA.numRowBlocks - 1) nRows = matA.numRows().toInt - nRows * (matA.numRowBlocks - 1)
      if (j == matA.numColBlocks - 1) nCols = matA.numCols().toInt - nCols * (matA.numColBlocks - 1)
      val newMat: Matrix = new SparseMatrix(nRows, nCols, BDM.zeros[Int](1, nCols + 1).toArray,
        Array[Int](), Array[Double]())
      ((i, j), newMat)
    }
    }
    val initialBlocks = addedBlocks.union(matA.blocks).partitionBy(ApspPartitioner)


    val blocks: RDD[((Int, Int), Matrix)] = initialBlocks.map { case ((i, j), v) => {
      val converted = v match {
        case dense: DenseMatrix => dense
        case sparse: SparseMatrix => addInfinity(sparse, i, j)
      }
      ((i, j), converted)
    }
    }
    new BlockMatrix(blocks, matA.rowsPerBlock, matA.colsPerBlock, n, n)
  }



  /**
   * Convert a local matrix into a dense breeze matrix.
   * TODO: use breeze sparse matrix if local matrix is sparse
   */
  def toBreeze(A: Matrix): BDM[Double] = {
    new BDM[Double](A.numRows, A.numCols, A.toArray)
  }

  /**
   * Convert from dense breeze matrix to local dense matrix.
   */
  def fromBreeze(dm: BDM[Double]): Matrix = {
    new DenseMatrix(dm.rows, dm.cols, dm.toArray, dm.isTranspose)
  }



  def localMinPlus(A: BDM[Double], B: BDM[Double], positions:Range): (BDM[Double], BDM[Double]) = {
    require(A.cols == B.rows, " Num cols of A does not match the num rows of B")
    val updatedPath = BDM.zeros[Double](A.rows, A.cols)
    val onesA = DenseVector.ones[Double](B.cols)
    val onesB = DenseVector.ones[Double](A.rows)
    val AMinPlusB = A(::, 0) * onesA.t + onesB * B(0, ::)
    if (A.cols > 1) {
      for (i <- 1 until A.cols) {
        val a = A(::, i)
        val b = B(i, ::)
       // val aPlusb = a * onesA.t + onesB * b
       // AMinPlusB = min(aPlusb, AMinPlusB)
        for (j <- 0 until A.rows)
          for (k <- 0 until A.cols)
            if (a(j) + b(k) < AMinPlusB(j, k)) {
              AMinPlusB(j, k) = a(j) + b(k)
              updatedPath(j, k) = positions(i) * 1.0
            }
      }
    }
    (AMinPlusB, updatedPath)
  }

  /**
   * Calculate APSP for a local square matrix and update the path midpoint
   */
  def localFW(A: BDM[Double], positions:Range): (BDM[Double], BDM[Double]) = {
    require(A.rows == A.cols, "Matrix for localFW should be square!")
    val B = A

    // initialize the path matrix
    val updatedPath = BDM.zeros[Double](A.rows, A.cols) :-= 1.0

    //val onesA = DenseVector.ones[Double](A.rows)
    for (i <- 0 until A.rows) {
      val a = B(::, i)
      val b = B(i, ::)
      //B = min(B, a * onesA.t + onesA * b)
      for (j <- 0 until B.rows)
        for (k <- 0 until B.cols)
          if (a(j) + b(k) < B(j, k)) {
            B(j, k) = a(j) + b(k)
            updatedPath(j, k) = positions(i) * 1.0
          }
    }
    (B, updatedPath)
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
  def distributedApsp(A: BlockMatrix, stepSize: Int, ApspPartitioner: GridPartitioner,
                      sc: SparkContext, interval: Int): BlockMatrix = {
    require(A.numRows() == A.numCols(), "The adjacency matrix must be square.")
    //require(A.rowsPerBlock == A.colsPerBlock, "The matrix must be square.")
    require(A.numRowBlocks == A.numColBlocks, "The blocks making up the adjacency matrix must be square.")
    require(A.rowsPerBlock == A.colsPerBlock, "The matrix in each block should be square")
    require(stepSize <= A.rowsPerBlock, "Step size must be less than number of rows in a block.")

    val blockNInter = math.ceil(A.rowsPerBlock * 1.0 / stepSize).toInt
    val niter = blockNInter * (A.numRowBlocks - 1) +
      math.ceil((A.numRows - A.rowsPerBlock * (A.numRowBlocks - 1))  * 1.0 / stepSize).toInt

    //var apspRDD = A.blocks
    //apspRDD has two elements: the distance matrix and the path midpoint matrix
    var apspRDD = A.blocks
      .mapValues(localMat => (localMat, fromBreeze(BDM.zeros[Double](localMat.numRows, localMat.numCols) - 1.0)))
    //var rowRDD : RDD[((Int, Int), Matrix)] = null
    //var colRDD : RDD[((Int, Int), Matrix)] = null

    for (i <- 0 until niter) {
      if ((i + 1) % interval == 0) {
        apspRDD.checkpoint()
        apspRDD.count()
      }

      val blockIndex = i / blockNInter
      val posInBlock = i - blockIndex * blockNInter
      val BlocknRows = (blockIndex + 1) match {
        case A.numRowBlocks => A.numRows.toInt - (A.numRowBlocks - 1) * A.rowsPerBlock
        case _ => A.rowsPerBlock
      }
      val startIndex = math.min(BlocknRows - 1, posInBlock * stepSize)
      val endIndex = math.min(BlocknRows, (posInBlock + 1) * stepSize)
      val positions = sc.broadcast((blockIndex * A.rowsPerBlock + startIndex) until (blockIndex * A.rowsPerBlock + endIndex))

      // Calculate the APSP of the square matrix and update the path midpoint
      // squareMat is an RDD of length 1 storing the updated square matrix and the midpoints of that matrix
      val squareMat = apspRDD.filter(kv => (kv._1._1 == blockIndex) && (kv._1._2 == blockIndex))
          .mapValues { case (distMat, pathMat) => {
        val updated = localFW(toBreeze(distMat)(startIndex to endIndex, startIndex to endIndex), positions.value)
        (fromBreeze(updated._1), fromBreeze(updated._2))
      }}

      val x = sc.broadcast(squareMat.first._2._1)

        // the rowRDD updated by squareMat, an RDD storing both the distance and the path midpoints
      val rowRDD = apspRDD.filter(_._1._1 == blockIndex)
        .mapValues{ case (distMat, pathMat) => {
          val updated = localMinPlus(toBreeze(x.value), toBreeze(distMat)(startIndex until endIndex, ::), positions.value)
          (fromBreeze(updated._1), fromBreeze((updated._2)))
        }}
      // the colRDD updated by squareMat
      val colRDD  = apspRDD.filter(_._1._2 == blockIndex)
        .mapValues{ case (distMat, pathMat) => {
        val updated = localMinPlus(toBreeze(distMat)(::, startIndex until endIndex), toBreeze(x.value), positions.value)
        (fromBreeze(updated._1), fromBreeze((updated._2)))
      }}

      // need updates
      apspRDD = blockMin(apspRDD, blockMinPlus(colRDD, rowRDD, A.numRowBlocks, A.numColBlocks, ApspPartitioner),
        ApspPartitioner)
    }
    new BlockMatrix(apspRDD, A.rowsPerBlock, A.colsPerBlock, A.numRows, A.numCols)
  }
}



