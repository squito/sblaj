package org.sblaj.spark

import org.sblaj.featurization.DictionaryCache
import org.sblaj.{SparseBinaryRowMatrix, BaseSparseBinaryVector, LongSparseBinaryVectorWithRowId, MatrixDims}
import spark.{SparkContext, RDD}
import collection._

trait RowSparseVectorRDD[G] {
  def colDictionary: DictionaryCache[G]
  def dims: RowMatrixPartitionDims
  def toEnumeratedVectorRDD(sc: SparkContext): EnumeratedRowSparseVectorRDD[G]
}

trait EnumeratedRowSparseVectorRDD[G] extends RowSparseVectorRDD[G] {
  def toSparseMatrix(sc: SparkContext) : SparseBinaryRowMatrix
}

case class RowMatrixPartitionDims(val totalDims: MatrixDims, val partitionDims: Map[Int, (Long,Long)])

class LongRowSparseVectorRDD[G](val vectorRdd: RDD[LongSparseBinaryVectorWithRowId],
                                val matrixDims: RowMatrixPartitionDims,
                                val colDictionary: DictionaryCache[G]) extends RowSparseVectorRDD[G] {

  def dims = matrixDims

  def toEnumeratedVectorRDD(sc: SparkContext) = {
    val enumeration = sc.broadcast(colDictionary.getEnumeration())
    val enumVectorsRdd = vectorRdd.map {
      v =>
        val enumeratedFeatures = v.colIds.map{enumeration.value.getEnumeratedId(_)}
        java.util.Arrays.sort(enumeratedFeatures)
        new BaseSparseBinaryVector(enumeratedFeatures, 0, enumeratedFeatures.length)  //TODO keep rowId
    }
    new EnumeratedSparseVectorRDD[G](enumVectorsRdd, matrixDims, colDictionary)
  }
}

//TODO this should be templated
class EnumeratedSparseVectorRDD[G](val vectorRDD: RDD[BaseSparseBinaryVector], val matrixDims: RowMatrixPartitionDims,
                                   val colDictionary: DictionaryCache[G]) extends EnumeratedRowSparseVectorRDD[G] {
  def toSparseMatrix(sc: SparkContext) = {
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

  def toEnumeratedVectorRDD(sc: SparkContext) = this
  def dims = matrixDims
}

