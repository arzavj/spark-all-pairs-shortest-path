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
    val resultMat = time{distributedApsp(matA, stepSize, ApspPartitioner, interval)}
    val resultMat1 = time{distributedApsp(matA, 1, ApspPartitioner, interval)}
    val resultLocalMat = resultMat.toLocalMatrix()
    val resultLocalMat1 = resultMat1.toLocalMatrix()
   // println(fromBreeze(localMinPlus(toBreeze(localMat), toBreeze(localMat.transpose))).toString())
    //println(matA.toLocalMatrix().toString())
    //println(localMat.toString)

    println(resultLocalMat.toString)
    println()
    println(resultLocalMat1.toString)
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




  def localMinPlus(A: BDM[Double], B: BDM[Double]): BDM[Double] = {
    require(A.cols == B.rows, " Num cols of A does not match the num rows of B")
    val k = A.cols
    val onesA = DenseVector.ones[Double](B.cols)
    val onesB = DenseVector.ones[Double](A.rows)
    var AMinPlusB = A(::, 0) * onesA.t + onesB * B(0, ::)
    if (k > 1) {
      for (i <- 1 until k) {
        val a = A(::, i)
        val b = B(i, ::)
        val aPlusb = a * onesA.t + onesB * b
        AMinPlusB = min(aPlusb, AMinPlusB)
      }
    }
    AMinPlusB
  }

  /**
   * Calculate APSP for a local square matrix
   */
  def localFW(A: BDM[Double]): BDM[Double] = {
    require(A.rows == A.cols, "Matrix for localFW should be square!")
    var B = A
    val onesA = DenseVector.ones[Double](A.rows)
    for (i <- 0 until A.rows) {
      val a = B(::, i)
      val b = B(i, ::)
      B = min(B, a * onesA.t + onesA * b)
    }
    B
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
                      interval: Int): BlockMatrix = {
    require(A.numRows() == A.numCols(), "The adjacency matrix must be square.")
    //require(A.rowsPerBlock == A.colsPerBlock, "The matrix must be square.")
    require(A.numRowBlocks == A.numColBlocks, "The blocks making up the adjacency matrix must be square.")
    require(A.rowsPerBlock == A.colsPerBlock, "The matrix in each block should be square")
    require(stepSize <= A.rowsPerBlock, "Step size must be less than number of rows in a block.")
	  val sc = A.blocks.sparkContext
    val n = A.numRows()
    //val niter = math.ceil(n * 1.0 / stepSize).toInt
    val blockNInter = math.ceil(A.rowsPerBlock * 1.0 / stepSize).toInt
    val niter = blockNInter * (A.numRowBlocks - 1) +
      math.ceil((A.numRows - A.rowsPerBlock * (A.numRowBlocks - 1))  * 1.0 / stepSize).toInt
    var apspRDD = A.blocks
    var rowRDD : RDD[((Int, Int), Matrix)] = null
    var colRDD : RDD[((Int, Int), Matrix)] = null
    for (i <- 0 to (niter - 1)) {
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
      // Calculate the APSP of the square matrix
      val squareMat = apspRDD.filter(kv => (kv._1._1 == blockIndex) && (kv._1._2 == blockIndex))
          .mapValues(localMat =>
        fromBreeze(localFW(toBreeze(localMat)(startIndex until endIndex, startIndex until endIndex))))
          .first._2
      val x = sc.broadcast(squareMat)
        // the rowRDD updated by squareMat
      rowRDD = apspRDD.filter(_._1._1 == blockIndex)
        .mapValues(localMat => fromBreeze(localMinPlus(toBreeze(x.value),
        toBreeze(localMat)(startIndex until endIndex, ::))))
      // the colRDD updated by squareMat
      colRDD  = apspRDD.filter(_._1._2 == blockIndex)
        .mapValues(localMat => fromBreeze(localMinPlus(toBreeze(localMat)(::, startIndex until endIndex),
        toBreeze(x.value))))


      apspRDD = blockMin(apspRDD, blockMinPlus(colRDD, rowRDD, A.numRowBlocks, A.numColBlocks, ApspPartitioner),
        ApspPartitioner)
    }
    new BlockMatrix(apspRDD, A.rowsPerBlock, A.colsPerBlock, n, n)
  }
}



