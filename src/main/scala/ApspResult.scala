import java.io.Serializable
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel


/**
 * Created by jingshuw on 7/14/15.
 */
class ApspResult (
                 var size: Long,
                 var distMatrix: BlockMatrix)
  extends Serializable with Logging{

  validateResult(distMatrix)

  private def validateResult(result: BlockMatrix): Unit = {
    require(result.numRows == result.numCols,
      "The shortest distance matrix is not square.")
    require(size == result.numRows,
      s"The size of the shortest distance matrix does not match $size.")
    if (result.blocks.getStorageLevel == StorageLevel.NONE) {
      logWarning("The APSP result is not cached. Lookup could be slow")
    }
  }

  def lookupDist(srcId: Long, dstId: Long): Double = {
    val sizePerBlock = distMatrix.rowsPerBlock
    val rowBlockId = (srcId/sizePerBlock).toInt
    val colBlockId = (dstId/sizePerBlock).toInt
    val block = distMatrix.blocks.filter{case ((i, j), _) => ( i == rowBlockId) & (j == colBlockId)}
      .first._2
    block.toArray((dstId % sizePerBlock).toInt * block.numRows + (srcId % sizePerBlock).toInt)
  }

  def toLocal(): Matrix = {
    distMatrix.toLocalMatrix()
  }
}
