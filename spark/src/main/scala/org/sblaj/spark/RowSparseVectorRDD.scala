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

  override def toEnumeratedVectorRDD(sc: SparkContext, storageLevel: StorageLevel): EnumeratedSparseVectorRDD[G] = {
    val enumeration = sc.broadcast(colDictionary.getEnumeration())
    println("enumerating vector RDD of size " + matrixDims.totalDims)
    //TODO dictionary needs to be upated w/ enumeration
    val enumVectorsRdd = vectorRdd.map {
      v =>
        val enumeratedFeatures = v.colIds.map{enumeration.value.getEnumeratedId(_).get}
        java.util.Arrays.sort(enumeratedFeatures)
        new BaseSparseBinaryVector(enumeratedFeatures, 0, enumeratedFeatures.length)  //TODO keep rowId
    }
    enumVectorsRdd.persist(storageLevel)
    val c = enumVectorsRdd.count  //just to force the calculation
    println(s"$c enumerated vectors")
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
    println("collected " + vectors.size + " vectors")
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

    val transformer = new ColumnSubsetter(ids, bcOrigEnumeration, bcSubsetEnumeration,totalNnz)

    val (subRdd, partitionDims) = SparkFeaturizer.mapWithPartitionDims(vectorRDD, sc)(transformer)

    //b/c map with partitions is lazy, we don't actually have the new dims yet unless we force it to run.
    // perhaps we should change the "dim" abstraction to be lazy as well.
    subRdd.cache()
    subRdd.count()

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


private[spark] class ColumnSubsetter(
  val ids: Array[Int],
  val bcOrigEnumeration: Broadcast[FeatureEnumeration],
  val bcSubsetEnumeration: Broadcast[FeatureEnumeration],
  val totalNnz: Accumulator[Long]
)
  extends Function[Iterator[BaseSparseBinaryVector],FinalValueIterator[BaseSparseBinaryVector, (Long,Long)]]
  with Serializable
{

  def apply(itr: Iterator[BaseSparseBinaryVector]) = new FinalValueIterator[BaseSparseBinaryVector, (Long,Long)]{
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
      totalNnz += newRow.nnz
      nrows += 1
      newRow
    }
    def next = sub.next
    def hasNext = sub.hasNext
    def finalValue = (nrows, nnz)

  }
}
