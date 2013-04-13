package org.sblaj.spark

import org.sblaj.featurization.{FeatureEnumeration, DictionaryCache}
import org.sblaj._
import _root_.spark.{SparkContext, RDD}
import _root_.spark.storage.StorageLevel
import collection._
import featurization.FeatureEnumeration
import org.sblaj.MatrixDims
import java.util
import _root_.spark.SparkContext._

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

  def colEnumeration: FeatureEnumeration

  /**
   * Applies a function `f` to all elements of this.
   *
   * Note that the function will be applied by spark on RDDs.  So sparks rules on serialization
   * apply.  Also, sparks utilities like accumulators and broadcast variables are available.
   */
  def foreach(f: SparseBinaryVector => Unit)

  def subsetColumnsByFeature(sc: SparkContext)(f: G => Boolean) : EnumeratedRowSparseVectorRDD[G] = {
    val enumeration = colEnumeration
    val validIds = colDictionary.flatMap{case (feature,id) => if (f(feature)) Some(id) else None}.toArray.map{
      longId => enumeration.getEnumeratedId(longId).get
    }
    subsetColumnById(sc, validIds)
  }

  def subsetColumnById(sc: SparkContext, ids: Array[Int]) : EnumeratedRowSparseVectorRDD[G]

  def subsetRows(sc: SparkContext)(f: SparseBinaryVector => Boolean) : EnumeratedRowSparseVectorRDD[G]
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
        val enumeratedFeatures = v.colIds.map{enumeration.value.getEnumeratedId(_).get}
        java.util.Arrays.sort(enumeratedFeatures)
        new BaseSparseBinaryVector(enumeratedFeatures, 0, enumeratedFeatures.length)  //TODO keep rowId
    }
    enumVectorsRdd.persist(storageLevel)
    enumVectorsRdd.count  //just to force the calculation
    new EnumeratedSparseVectorRDD[G](enumVectorsRdd, matrixDims, colDictionary,enumeration.value)
  }
}

//TODO this should be templated
class EnumeratedSparseVectorRDD[G](
    val vectorRDD: RDD[BaseSparseBinaryVector],
    val matrixDims: RowMatrixPartitionDims,
    val colDictionary: DictionaryCache[G],
    val colEnumeration: FeatureEnumeration
) extends EnumeratedRowSparseVectorRDD[G] {
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
        System.arraycopy(v.colIds, 0, matrix.colIds, colIdx, v.colIds.length)
        colIdx += v.colIds.length
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

  override def subsetColumnById(sc: SparkContext, ids: Array[Int]) = {
    val subsetEnumeration = colEnumeration.subset(ids)
    val bcSubsetEnumeration = sc.broadcast(subsetEnumeration)
    val bcOrigEnumeration = sc.broadcast(colEnumeration)
    // we need to keep track of how the number of non-zeros changes in each partition

    val totalNnz = sc.accumulator(0l)
    class TransformIter(val itr: Iterator[BaseSparseBinaryVector])
      extends FinalValueIterator[BaseSparseBinaryVector, (Long,Long)] {
      var nrows = 0
      var nnz = 0
      val sub = itr.map{vector =>
        val newRow : BaseSparseBinaryVector = vector.subset(ids)
        // not the most efficient way to remap the ids, but it works for now
        newRow.colIds = newRow.colIds.flatMap{oldId =>
          val longId = bcOrigEnumeration.value.getLongId(oldId)
          bcSubsetEnumeration.value.getEnumeratedId(longId)
        }.sorted

        nnz += newRow.nnz
        totalNnz += nnz
        nrows += 1
        newRow
      }
      def next = sub.next
      def hasNext = sub.hasNext
      def finalValue = (nrows, nnz)
    }

    val (subRdd, partitionDims) = SparkFeaturizer.mapWithPartitionDims(vectorRDD, sc)(itr => new TransformIter(itr))

    val subDims = new RowMatrixPartitionDims(
      new MatrixDims(nRows = matrixDims.totalDims.nRows, nCols = ids.length, nnz = totalNnz.value),
      partitionDims = partitionDims.value
    )
    new EnumeratedSparseVectorRDD[G](subRdd, subDims, colDictionary, subsetEnumeration)
  }

  override def subsetRows(sc: SparkContext)(f: SparseBinaryVector => Boolean) = {
    //filter the rdd, but also keep track of nrows & nnz per partition
    val totalRows = sc.accumulator(0l)
    val totalNnz = sc.accumulator(0l)
    class TransformIter(val itr: Iterator[BaseSparseBinaryVector])
      extends FinalValueIterator[BaseSparseBinaryVector, (Long,Long)] {
      var nrows = 0
      var nnz = 0
      val sub = itr.filter{vector =>
        val ok = f(vector)
        if (ok) {
          nrows += 1
          nnz += vector.nnz
          totalRows += 1
          totalNnz += vector.nnz
        }
        ok
      }
      def next = sub.next
      def hasNext = sub.hasNext
      def finalValue = (nrows, nnz)
    }

    val (subRdd, partitionDims) = SparkFeaturizer.mapWithPartitionDims(vectorRDD, sc)(itr => new TransformIter(itr))
    val subDims = new RowMatrixPartitionDims(
      new MatrixDims(nRows = totalRows.value, nCols =  matrixDims.totalDims.nCols, nnz = totalNnz.value),
      partitionDims = partitionDims.value
    )
    new EnumeratedSparseVectorRDD[G](subRdd, subDims, colDictionary, colEnumeration)
  }
}

