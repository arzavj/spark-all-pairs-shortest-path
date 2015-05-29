import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{SparseMatrix, DenseMatrix, Matrix}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, BlockMatrix}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseMatrix => BDM, sum, DenseVector, min}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators


object AllPairsShortestPath {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AllPairsShortestPath").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val graph = generateGraph(12, sc)
    val matA = generateInput(graph, 12, sc, 6, 6)
    val ApspPartitioner = GridPartitioner(matA.numRowBlocks, matA.numColBlocks, matA.blocks.partitions.length)




    val localMat = matA.toLocalMatrix()
    val resultMat = distributedApsp(matA, 4, ApspPartitioner).toLocalMatrix
   // println(fromBreeze(localMinPlus(toBreeze(localMat), toBreeze(localMat.transpose))).toString())
    println(matA.rowsPerBlock)
    println(localMat.toString)
    println(resultMat.toString)
   // val collectedValues = blockMin(matA.blocks, matA.transpose.blocks, ApspPartitioner).foreach(println)
   // blockMinPlus(matA.blocks, matA.transpose.blocks, matA.numRowBlocks, matA.numColBlocks, ApspPartitioner).foreach(println)
    System.exit(0)
  }


  /**
   *  add infinity to missing off diagonal elements and 0 to diagonal elements
   */
  def addInfinity(A: SparseMatrix, rowBlockID: Int, colBlockID: Int): Matrix = {
    val inf = scala.Double.PositiveInfinity
    val result: BDM[Double] = BDM.tabulate(A.numRows, A.numCols){case (i, j) => inf}
    for (j <- 0 to A.values.length)
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

  def generateGraph(n: Int, sc: SparkContext): Graph[Long, Double] = {
    val graph = GraphGenerators.logNormalGraph(sc, n).mapEdges(e => e.attr.toDouble)
    graph
  }

  def generateInput(graph: Graph[Long,Double], n: Int, sc:SparkContext,
                    numRowBlocks: Int, numColBlocks: Int): BlockMatrix = {
    require(numRowBlocks == numColBlocks, "need a square grid partition")
    val entries = graph.edges.map{case edge => MatrixEntry(edge.srcId.toInt, edge.dstId.toInt, edge.attr)}
    val coordMat = new CoordinateMatrix(entries, n, n)
    val matA = coordMat.toBlockMatrix(numRowBlocks, numRowBlocks)
    val ApspPartitioner = GridPartitioner(matA.numRowBlocks, matA.numColBlocks, matA.blocks.partitions.length)
    require(matA.numColBlocks == matA.numRowBlocks)

    // make sure that all block indices appears in the matrix blocks
    // add the blocks that are not represented
    val activeBlocks: BDM[Int] = BDM.tabulate(matA.numRowBlocks, matA.numColBlocks){case (i, j) => 0}
    matA.blocks.foreach{case ((i, j), v) => activeBlocks(i, j)= 1}
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
    val addedBlocks = sc.parallelize(addedBlocksIdx).map{case (i, j) => {
      var nRows = matA.rowsPerBlock
      var nCols = matA.colsPerBlock
      if (i == matA.numRowBlocks - 1) nRows = matA.numRows().toInt - nRows * (matA.numRowBlocks - 1)
      if (j == matA.numColBlocks - 1) nCols = matA.numCols().toInt - nCols * (matA.numColBlocks - 1)
      val newMat: Matrix = new SparseMatrix(nRows, nCols, BDM.tabulate(1, nCols + 1){case (i, j) => 0}.toArray,
                                    Array[Int](), Array[Double]())
      ((i, j), newMat)
    }}
    val initialBlocks = addedBlocks.union(matA.blocks).partitionBy(ApspPartitioner)


    val blocks: RDD[((Int, Int), Matrix)] = initialBlocks.map{case ((i, j), v) => {
      val converted = v match {
        case dense: DenseMatrix => dense
        case sparse: SparseMatrix => addInfinity(sparse, i, j)
      }
      ((i, j),converted)
    }}
    new BlockMatrix(blocks, matA.rowsPerBlock, matA.colsPerBlock, n, n)
  }


  def localMinPlus(A: BDM[Double], B: BDM[Double]): BDM[Double] = {
    require(A.cols == B.rows, "Num rows of A does not match the num rows of B")
    val k = A.cols
    val onesA = DenseVector.ones[Double](B.cols)
    val onesB = DenseVector.ones[Double](A.rows)
    var AMinPlusB = A(::, 0) * onesA.t + onesB * B(0, ::)
    if (k > 1)
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
    require(stepSize <= A.rowsPerBlock, "Step size must be less than number of rows in a block.")
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
                ((i, j), fromBreeze(toBreeze(localMat)(startIndex until localMat.numRows, ::)))
              case EndBlock =>
                ((i, j), fromBreeze(toBreeze(localMat)(0 to endIndex, ::)))
            }
          }
        colRDD = apspRDD.filter(kv => kv._1._2 == StartBlock || kv._1._2 == EndBlock)
          .map { case ((i, j), localMat) =>
            j match {
              case StartBlock =>
                ((i, j), fromBreeze(toBreeze(localMat)(::, startIndex until localMat.numRows)))
              case EndBlock =>
                ((i, j), fromBreeze(toBreeze(localMat)(::, 0 to endIndex)))
            }
          }
      }

      apspRDD = blockMin(apspRDD, blockMinPlus(colRDD, rowRDD, A.numRowBlocks, A.numColBlocks, ApspPartitioner),
        ApspPartitioner)
    }
    new BlockMatrix(apspRDD, A.rowsPerBlock, A.colsPerBlock, n, n)
  }
}


