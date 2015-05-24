import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{Matrix, Matrices, DenseMatrix, SparseMatrix}
import breeze.linalg.{DenseMatrix => BDM, DenseVector, min, Matrix =>BM}
import org.apache.spark.Partitioner
import org.apache.spark.rdd._

/*
 * the definition of GridPartitioner is copied from the BlockMatrix code
 */
class GridPartitioner(
		val rows: Int,
		val cols: Int,
		val rowsPerPart: Int,
		val colsPerPart: Int) extends Partitioner {
		require(rows > 0)
		require(cols > 0)
		require(rowsPerPart > 0)
		require(colsPerPart > 0)
		private val rowPartitions = math.ceil(rows * 1.0 / rowsPerPart).toInt
		private val colPartitions = math.ceil(cols * 1.0 / colsPerPart).toInt

		override val numPartitions: Int = rowPartitions * colPartitions

		/**
		 * Returns the index of the partition the input coordinate belongs to.
		 *
		 * @param key The coordinate (i, j) or a tuple (i, j, k), where k is the inner index used in
		 *            multiplication. k is ignored in computing partitions.
		 * @return The index of the partition, which the coordinate belongs to.
		 */
		override def getPartition(key: Any): Int = {
			key match {
				case (i: Int, j: Int) =>
						 getPartitionId(i, j)
				case (i: Int, j: Int, _: Int) =>
						 getPartitionId(i, j)
				case _ =>
						 throw new IllegalArgumentException(s"Unrecognized key: $key.")
			}
		}

	/** Partitions sub-matrices as blocks with neighboring sub-matrices. */
	private def getPartitionId(i: Int, j: Int): Int = {
		require(0 <= i && i < rows, s"Row index $i out of range [0, $rows).")
		require(0 <= j && j < cols, s"Column index $j out of range [0, $cols).")
		i / rowsPerPart + j / colsPerPart * rowPartitions
	}

	override def equals(obj: Any): Boolean = {
		obj match {
			case r: GridPartitioner =>
					(this.rows == r.rows) && (this.cols == r.cols) &&
						(this.rowsPerPart == r.rowsPerPart) && (this.colsPerPart == r.colsPerPart)
						case _ =>
						false
		}
	}

// This doesn't work!!
	override def hashCode: Int = {
		com.google.common.base.Objects.hashCode(
				rows: java.lang.Integer,
				cols: java.lang.Integer,
				rowsPerPart: java.lang.Integer,
				colsPerPart: java.lang.Integer)
	}
}


/* warning: previously defined class GridPartitioner is not a companion 
 * to object GridPartitioner. 
 * Companions must be defined together; you may wish to use :paste mode for this.
 */
object GridPartitioner {

	/** Creates a new [[GridPartitioner]] instance. */
	def apply(rows: Int, cols: Int, rowsPerPart: Int, colsPerPart: Int): GridPartitioner = {
		new GridPartitioner(rows, cols, rowsPerPart, colsPerPart)
	}

	/** Creates a new [[GridPartitioner]] instance with the input suggested number of partitions. */
	def apply(rows: Int, cols: Int, suggestedNumPartitions: Int): GridPartitioner = {
		require(suggestedNumPartitions > 0)
			val scale = 1.0 / math.sqrt(suggestedNumPartitions)
			val rowsPerPart = math.round(math.max(scale * rows, 1.0)).toInt
			val colsPerPart = math.round(math.max(scale * cols, 1.0)).toInt
			new GridPartitioner(rows, cols, rowsPerPart, colsPerPart)
	}
}





