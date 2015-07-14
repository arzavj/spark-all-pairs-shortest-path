import java.io.Serializable
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{SparseMatrix, DenseMatrix, Matrix}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, BlockMatrix}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseMatrix => BDM, sum, DenseVector, min}
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel


import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.concurrent.duration._


object AllPairsShortestPath {
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
    val result = new APSP

    //val matA = generateInput(graph, n, sc, m, m)
    //val ApspPartitioner = GridPartitioner(matA.numRowBlocks, matA.numColBlocks, matA.blocks.partitions.length)

    //val localMat = matA.toLocalMatrix()
    //val resultMat = time{distributedApsp(matA, stepSize, ApspPartitioner, interval)}


    val resultMat = time {
      result.compute(graph, 3)
    }
    //val resultMat1 = time{distributedApsp(matA, 1, ApspPartitioner, interval)}
    val resultLocalMat = resultMat.toLocalMatrix()
    //val resultLocalMat1 = resultMat1.toLocalMatrix()
    // println(fromBreeze(localMinPlus(toBreeze(localMat), toBreeze(localMat.transpose))).toString())
    //println(matA.toLocalMatrix().toString())
    //println(localMat.toString)

    println(resultLocalMat.toString)
    println()
    println(result.lookupDist(resultMat, 1, 2))
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



class APSP (
             var stepSize: Int = 250,
             var checkpointInterval: Int = 2,
             var checkpointDir: String = "checkpoint/"
             ) extends Serializable with Logging {


  /** storage level for user/product in/out links */
  private var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  private var finalRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  /**
   * :: DeveloperApi ::
   * Sets storage level for intermediate RDDs (user/product in/out links). The default value is
   * `MEMORY_AND_DISK`. Users can change it to a serialized storage, e.g., `MEMORY_AND_DISK_SER` and
   * set `spark.rdd.compress` to `true` to reduce the space requirement, at the cost of speed.
   */
  @DeveloperApi
  def setIntermediateRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    require(storageLevel != StorageLevel.NONE,
      "ALS is not designed to run without persisting intermediate RDDs.")
    this.intermediateRDDStorageLevel = storageLevel
    this
  }

  /**
   * :: DeveloperApi ::
   * Sets storage level for final RDDs (user/product used in MatrixFactorizationModel). The default
   * value is `MEMORY_AND_DISK`. Users can change it to a serialized storage, e.g.
   * `MEMORY_AND_DISK_SER` and set `spark.rdd.compress` to `true` to reduce the space requirement,
   * at the cost of speed.
   */
  @DeveloperApi
  def setFinalRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    this.finalRDDStorageLevel = storageLevel
    this
  }

  /**
   *  add infinity to missing off diagonal elements and 0 to diagonal elements
   */
  private def addInfinity(A: SparseMatrix, isDiagonalBlock: Boolean): Matrix = {
    val inf = scala.Double.PositiveInfinity
    val result: BDM[Double] = BDM.tabulate(A.numRows, A.numCols){case (i, j) => inf}
    for (j <- 0 until A.values.length)
      for (i <- 0 until A.numCols) {
        if (j >= A.colPtrs(i) & j < A.colPtrs(i + 1))
          result(A.rowIndices(j), i) = A.values(j)
      }
    if (isDiagonalBlock) {
      require(A.numCols == A.numRows, "Diagonal block should have a square matrix")
      for (i <- 0 until A.numCols)
        result(i, i) = 0.0
    }
    fromBreeze(result)
  }

    /**
     * Convert a graph to a dense BlockMatrix (adjacency) with missing edges being infinity
     * Assume that a single machine can hold an nBlocks * nBlocks matrix
     */

   private def graphToAdjacencyMat(graph: Graph[Long,Double], sizePerBlock: Int): BlockMatrix = {
      val n = graph.vertices.count
      val entries = graph.edges.map { case edge => MatrixEntry(edge.srcId, edge.dstId, edge.attr) }
      val sc = entries.sparkContext
      val coordMat = new CoordinateMatrix(entries, n, n)
      val NBlocks = math.ceil(n / (sizePerBlock * 1.0)).toInt
      val matA = coordMat.toBlockMatrix(sizePerBlock, sizePerBlock)
      val apspPartitioner = GridPartitioner(NBlocks, NBlocks, matA.blocks.partitions.length)
      // This check should be unnecessary, just to double check
      require((matA.numColBlocks == matA.numRowBlocks) & (matA.numColBlocks == NBlocks))

      // add a block of infinity if the whole block is not represented
      val activeBlocks: BDM[Int] = BDM.zeros[Int](NBlocks, NBlocks)
      val activeIdx = matA.blocks.map { case ((i, j), v) => (i, j) }.collect()
      // find out where the whole block is missing
      activeIdx.foreach { case (i, j) => activeBlocks(i, j) = 1 }
      val nAddedBlocks = NBlocks * NBlocks - sum(activeBlocks)
      val addedBlocksIdx = new Array[(Int, Int)](nAddedBlocks)
      var index = 0
      for (i <- 0 until NBlocks)
        for (j <- 0 until NBlocks) {
          if (activeBlocks(i, j) == 0) {
            addedBlocksIdx(index) = (i, j)
            index = index + 1
          }
        }
      // Create empty blocks with just the non-represented block indices
      val addedBlocks = sc.parallelize(addedBlocksIdx).map { case (i, j) => {
        val nRows = (i + 1) match {
          case NBlocks => (n - sizePerBlock * (NBlocks - 1)).toInt
          case _ => sizePerBlock
        }
        val nCols = (j + 1) match {
          case NBlocks => (n - sizePerBlock * (NBlocks - 1)).toInt
          case _ => sizePerBlock
        }
        val newMat: Matrix = new SparseMatrix(nRows, nCols, Array.fill(nCols + 1)(0),
          Array[Int](), Array[Double]())
        ((i, j), newMat)
      }}
      val initialBlocks = addedBlocks.union(matA.blocks).partitionBy(apspPartitioner)

      val blocks: RDD[((Int, Int), Matrix)] = initialBlocks.map { case ((i, j), v) => {
        val converted = v match {
          case dense: DenseMatrix => dense
          case sparse: SparseMatrix => addInfinity(sparse, i == j)
        }
        ((i, j), converted)
      }}
      new BlockMatrix(blocks, sizePerBlock, sizePerBlock, n, n)
    }

  /**
   * Convert a local matrix into a dense breeze matrix.
   * TODO: use breeze sparse matrix if local matrix is sparse
   */
  private def toBreeze(A: Matrix): BDM[Double] = {
    new BDM[Double](A.numRows, A.numCols, A.toArray)
  }

  /**
   * Convert from dense breeze matrix to local dense matrix.
   */
  private def fromBreeze(dm: BDM[Double]): Matrix = {
    new DenseMatrix(dm.rows, dm.cols, dm.toArray, dm.isTranspose)
  }

  private def localMinPlus(A: BDM[Double], B: BDM[Double]): BDM[Double] = {
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
  private def localFW(A: BDM[Double]): BDM[Double] = {
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

  private def blockMin(Ablocks: RDD[((Int, Int), Matrix)], Bblocks: RDD[((Int, Int), Matrix)],
                       ApspPartitioner: GridPartitioner): RDD[((Int, Int), Matrix)] = {
    val addedBlocks = Ablocks.join(Bblocks, ApspPartitioner).mapValues {
      case (a, b) => fromBreeze(min(toBreeze(a), toBreeze(b)))
    }
    addedBlocks
  }

  private def blockMinPlus(Ablocks: RDD[((Int, Int), Matrix)], Bblocks: RDD[((Int, Int), Matrix)],
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
   *          requires the blocks to be square (m * m)
   *          also requires that for the (i, j)th block, if i < m-1 and j < m-1 then the matrix
   *          in the block must have size A.rowsPerBlock * A.colsPerBlock
   */
  def compute(A: BlockMatrix): BlockMatrix = {
    require(A.numRows() == A.numCols(), "The adjacency matrix must be square.")
    //require(A.rowsPerBlock == A.colsPerBlock, "The matrix must be square.")
    require(A.numRowBlocks == A.numColBlocks, "The blocks making up the adjacency matrix must be square.")
    require(A.rowsPerBlock == A.colsPerBlock, "The matrix in each block should be square")
    //require(stepSize <= A.rowsPerBlock, "Step size must be less than number of rows in a block.")
    if (stepSize > A.rowsPerBlock) {
      stepSize = A.rowsPerBlock
    }
    val apspPartitioner = GridPartitioner(A.numRowBlocks, A.numColBlocks, A.blocks.partitions.length)
    val sc = A.blocks.sparkContext
    sc.setCheckpointDir(checkpointDir)
    val n = A.numRows()
    //val niter = math.ceil(n * 1.0 / stepSize).toInt
    val blockNInter = math.ceil(A.rowsPerBlock * 1.0 / stepSize).toInt
    val niter = blockNInter * (A.numRowBlocks - 1) +
      math.ceil((A.numRows - A.rowsPerBlock * (A.numRowBlocks - 1))  * 1.0 / stepSize).toInt
    var apspRDD = A.blocks
    var rowRDD : RDD[((Int, Int), Matrix)] = null
    var colRDD : RDD[((Int, Int), Matrix)] = null
    for (i <- 0 to (niter - 1)) {
      if ((i + 1) % checkpointInterval == 0) {
        apspRDD.checkpoint()
        apspRDD.count()
      }
      val blockIndex = i / blockNInter
      val posInBlock = i - blockIndex * blockNInter
      val BlockNRows = (blockIndex + 1) match {
        case A.numRowBlocks => A.numRows.toInt - (A.numRowBlocks - 1) * A.rowsPerBlock
        case _ => A.rowsPerBlock
      }
      val startIndex = math.min(BlockNRows - 1, posInBlock * stepSize)
      val endIndex = math.min(BlockNRows, (posInBlock + 1) * stepSize)
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


      apspRDD = blockMin(apspRDD, blockMinPlus(colRDD, rowRDD, A.numRowBlocks, A.numColBlocks, apspPartitioner),
        apspPartitioner)
    }
    new BlockMatrix(apspRDD, A.rowsPerBlock, A.colsPerBlock, n, n)
  }

  def compute(graph: Graph[Long, Double], sizePerBlock: Int): BlockMatrix = {
    val A = graphToAdjacencyMat(graph, sizePerBlock)
    compute(A)
  }

  def lookupDist(apspResult: BlockMatrix, srcId: Long, dstId: Long): Double = {
    val sizePerBlock = apspResult.rowsPerBlock
    val rowBlockId = (srcId/sizePerBlock).toInt
    val colBlockId = (dstId/sizePerBlock).toInt
    val block = apspResult.blocks.filter{case ((i, j), _) => ( i == rowBlockId) & (j == colBlockId)}
    .first._2
    block.toArray((dstId % sizePerBlock).toInt * block.numRows + (srcId % sizePerBlock).toInt)
  }
}




