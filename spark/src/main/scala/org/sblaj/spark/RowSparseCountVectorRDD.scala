package org.sblaj.spark

import org.sblaj.featurization.DictionaryCache
import org.sblaj._
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import collection._
import featurization.FeatureEnumeration
import org.sblaj.MatrixDims
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast

trait RowSparseCountVectorRDD[G] {
  def colDictionary: DictionaryCache[G]
  def dims: RowMatrixPartitionDims
  //TODO add min feature counts
  def toEnumeratedVectorRDD(sc: SparkContext, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): EnumeratedRowSparseCountVectorRDD[G]
}

trait EnumeratedRowSparseCountVectorRDD[G] extends RowSparseCountVectorRDD[G] {
  def toSparseMatrix(sc: SparkContext) : SparseCountRowMatrix
  def colEnumeration: FeatureEnumeration
  def foreach(f: SparseCountVector => Unit)

  def subsetColumnsByFeature(sc: SparkContext)(f: G => Boolean) : EnumeratedRowSparseCountVectorRDD[G] = {
    val enumeration = colEnumeration
    val validIds = colDictionary.flatMap{case (feature,id) => if (f(feature)) Some(id) else None}.toArray.map{
      longId => enumeration.getEnumeratedId(longId).get
    }
    subsetColumnById(sc, validIds)
  }

  def subsetColumnById(sc: SparkContext, ids: Array[Int]) : EnumeratedRowSparseCountVectorRDD[G]

  def subsetRows(sc: SparkContext)(f: SparseCountVector => Boolean) : EnumeratedRowSparseCountVectorRDD[G]
}

class LongRowSparseCountVectorRDD[G](val vectorRdd: RDD[LongSparseCountVectorWithRowId],
                                val matrixDims: RowMatrixPartitionDims,
                                val colDictionary: DictionaryCache[G]) extends RowSparseCountVectorRDD[G] {

  override def dims = matrixDims

  override def toEnumeratedVectorRDD(sc: SparkContext, storageLevel: StorageLevel): EnumeratedSparseCountVectorRDD[G] = {
    val enumeration = sc.broadcast(colDictionary.getEnumeration())
    println("enumerating vector RDD of size " + matrixDims.totalDims)
    //TODO dictionary needs to be updated w/ enumeration
    val enumVectorsRdd = vectorRdd.map {
      v =>
        val enumeratedFeatures = v.colIds.map{enumeration.value.getEnumeratedId(_).get}
        val enumeratedCounts = v.cnts.clone()
        Sorting.sortParallel(enumeratedFeatures, enumeratedCounts)
        new BaseSparseCountVector(enumeratedFeatures, v.cnts, 0, enumeratedFeatures.length)  //TODO keep rowId
    }
    enumVectorsRdd.persist(storageLevel)
    val c = enumVectorsRdd.count  //just to force the calculation
    println(s"$c enumerated vectors")
    new EnumeratedSparseCountVectorRDD[G](enumVectorsRdd, matrixDims, colDictionary,enumeration.value)
  }
}

//TODO this should be templated
class EnumeratedSparseCountVectorRDD[G](
    val vectorRDD: RDD[BaseSparseCountVector],
    val matrixDims: RowMatrixPartitionDims,
    val colDictionary: DictionaryCache[G],
    val colEnumeration: FeatureEnumeration
) extends EnumeratedRowSparseCountVectorRDD[G] {
  override def toSparseMatrix(sc: SparkContext) = {
    val vectors = vectorRDD.collect()
    println("collected " + vectors.size + " vectors")
    val matrix = new SparseCountRowMatrix(nMaxRows = matrixDims.totalDims.nRows.toInt,
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
    matrix.setSize(rowIdx, colIdx)
    matrix
  }

  override def toEnumeratedVectorRDD(sc: SparkContext, storageLevel: StorageLevel) = this
  override def dims = matrixDims

  override def foreach(f: SparseCountVector => Unit) {
    vectorRDD.foreach(f)
  }

  override def subsetColumnById(sc: SparkContext, ids: Array[Int]) = {
    val subsetEnumeration = colEnumeration.subset(ids)
    val bcSubsetEnumeration = sc.broadcast(subsetEnumeration)
    val bcOrigEnumeration = sc.broadcast(colEnumeration)
    // we need to keep track of how the number of non-zeros changes in each partition
    val totalNnz = sc.accumulator(0l)

    val transformer = new CountColumnSubsetter(ids, bcOrigEnumeration, bcSubsetEnumeration,totalNnz)

    val (subRdd, partitionDims) = SparkCountFeaturizer.mapWithPartitionDims(vectorRDD, sc)(transformer)

    //b/c map with partitions is lazy, we don't actually have the new dims yet unless we force it to run.
    // perhaps we should change the "dim" abstraction to be lazy as well.
    subRdd.cache()
    subRdd.count()

    val subDims = new RowMatrixPartitionDims(
      new MatrixDims(nRows = matrixDims.totalDims.nRows, nCols = ids.length, nnz = totalNnz.value),
      partitionDims = partitionDims.value
    )
    new EnumeratedSparseCountVectorRDD[G](subRdd, subDims, colDictionary, subsetEnumeration)
  }

  override def subsetRows(sc: SparkContext)(f: SparseCountVector => Boolean) = {
    //filter the rdd, but also keep track of nrows & nnz per partition
    val totalRows = sc.accumulator(0l)
    val totalNnz = sc.accumulator(0l)
    class TransformIter(val itr: Iterator[BaseSparseCountVector])
      extends FinalValueIterator[BaseSparseCountVector, (Long,Long)] {
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

    val (subRdd, partitionDims) = SparkCountFeaturizer.mapWithPartitionDims(vectorRDD, sc)(itr => new TransformIter(itr))
    val subDims = new RowMatrixPartitionDims(
      new MatrixDims(nRows = totalRows.value, nCols =  matrixDims.totalDims.nCols, nnz = totalNnz.value),
      partitionDims = partitionDims.value
    )
    new EnumeratedSparseCountVectorRDD[G](subRdd, subDims, colDictionary, colEnumeration)
  }
}


private[spark] class CountColumnSubsetter(
  val ids: Array[Int],
  val bcOrigEnumeration: Broadcast[FeatureEnumeration],
  val bcSubsetEnumeration: Broadcast[FeatureEnumeration],
  val totalNnz: Accumulator[Long]
)
  extends Function[Iterator[BaseSparseCountVector],FinalValueIterator[BaseSparseCountVector, (Long,Long)]]
  with Serializable
{

  def apply(itr: Iterator[BaseSparseCountVector]) = new FinalValueIterator[BaseSparseCountVector, (Long,Long)]{
    var nrows = 0
    var nnz = 0
    val sub = itr.map{vector =>
      val newRow : BaseSparseCountVector = vector.subset(ids)
      // not the most efficient way to remap the ids, but it works for now
      newRow.colIds = newRow.colIds.flatMap{oldId =>
        val longId = bcOrigEnumeration.value.getLongId(oldId)
        bcSubsetEnumeration.value.getEnumeratedId(longId)
      }.sorted

      nnz += newRow.nnz
      totalNnz += newRow.nnz
      nrows += 1
      newRow
    }
    def next = sub.next
    def hasNext = sub.hasNext
    def finalValue = (nrows, nnz)

  }
}
