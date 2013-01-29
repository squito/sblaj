package org.sblaj.spark

import org.sblaj.featurization.DictionaryCache
import org.sblaj._
import _root_.spark.{SparkContext, RDD}
import _root_.spark.storage.StorageLevel
import collection._
import org.sblaj.MatrixDims

trait RowSparseVectorRDD[G] {
  def colDictionary: DictionaryCache[G]
  def dims: RowMatrixPartitionDims
  //TODO add min feature counts
  def toEnumeratedVectorRDD(sc: SparkContext, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): EnumeratedRowSparseVectorRDD[G]
}

trait EnumeratedRowSparseVectorRDD[G] extends RowSparseVectorRDD[G] {
  /**
   * collects the RDD into an in-memory matrix on the driver
   */
  def toSparseMatrix(sc: SparkContext) : SparseBinaryRowMatrix

  /**
   * Applies a function `f` to all elements of this.
   *
   * Note that the function will be applied by spark on RDDs.  So sparks rules on serialization
   * apply.  Also, sparks utilities like accumulators and broadcast variables are available.
   */
  def foreach(f: SparseBinaryVector => Unit)
}

case class RowMatrixPartitionDims(val totalDims: MatrixDims, val partitionDims: Map[Int, (Long,Long)])

class LongRowSparseVectorRDD[G](val vectorRdd: RDD[LongSparseBinaryVectorWithRowId],
                                val matrixDims: RowMatrixPartitionDims,
                                val colDictionary: DictionaryCache[G]) extends RowSparseVectorRDD[G] {

  override def dims = matrixDims

  override def toEnumeratedVectorRDD(sc: SparkContext, storageLevel: StorageLevel) = {
    val enumeration = sc.broadcast(colDictionary.getEnumeration())
    //TODO dictionary needs to be upated w/ enumeration
    val enumVectorsRdd = vectorRdd.map {
      v =>
        val enumeratedFeatures = v.colIds.map{enumeration.value.getEnumeratedId(_)}
        java.util.Arrays.sort(enumeratedFeatures)
        new BaseSparseBinaryVector(enumeratedFeatures, 0, enumeratedFeatures.length)  //TODO keep rowId
    }
    enumVectorsRdd.persist(storageLevel)
    enumVectorsRdd.count  //just to force the calculation
    new EnumeratedSparseVectorRDD[G](enumVectorsRdd, matrixDims, colDictionary)
  }
}

//TODO this should be templated
class EnumeratedSparseVectorRDD[G](val vectorRDD: RDD[BaseSparseBinaryVector], val matrixDims: RowMatrixPartitionDims,
                                   val colDictionary: DictionaryCache[G]) extends EnumeratedRowSparseVectorRDD[G] {
  override def toSparseMatrix(sc: SparkContext) = {
    val vectors = vectorRDD.collect()
    val matrix = new SparseBinaryRowMatrix(nMaxRows = matrixDims.totalDims.nRows.toInt,
      nMaxNonZeros = matrixDims.totalDims.nnz.toInt,
      nColumns = matrixDims.totalDims.nCols.toInt)
    var rowIdx = 0  //zipWithIndex actually creates a new array ...
    var colIdx = 0
    vectors.foreach {
      v =>
        matrix.rowStartIdx(rowIdx) = colIdx
        System.arraycopy(v.theColIds, 0, matrix.colIds, colIdx, v.theColIds.length)
        colIdx += v.theColIds.length
        rowIdx += 1
    }
    matrix.rowStartIdx(rowIdx) = colIdx
    matrix
  }

  override def toEnumeratedVectorRDD(sc: SparkContext, storageLevel: StorageLevel) = this
  override def dims = matrixDims

  override def foreach(f: SparseBinaryVector => Unit) {
    vectorRDD.foreach(f)
  }
}

