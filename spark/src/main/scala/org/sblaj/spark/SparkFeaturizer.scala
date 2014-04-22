package org.sblaj.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.sblaj.featurization.{Murmur64, HashMapDictionaryCache}
import org.apache.spark.storage.StorageLevel
import org.sblaj._
import collection._
import org.sblaj.MatrixDims
import scala.reflect.ClassTag

object SparkFeaturizer {


  class HashMapDictionaryCacheAcc[G] extends AccumulableParam[HashMapDictionaryCache[G], (G,Long)] {
    def addAccumulator(m: HashMapDictionaryCache[G], kv: (G,Long)): HashMapDictionaryCache[G] = {
      m.addMapping(kv._1, kv._2)
      m
    }

    def addInPlace(a: HashMapDictionaryCache[G], b: HashMapDictionaryCache[G]): HashMapDictionaryCache[G] = {
      a ++= b
      a
    }

    def zero(initialValue: HashMapDictionaryCache[G]) = new HashMapDictionaryCache[G]
  }

  /**
   * Convert an RDD into a SparseBinaryMatrix.
   *
   * This assumes that each record in the RDD corresponds to one row in matrix, and that the entire dictionary
   * will fit in memory.
   *
   */
  def rowPerRecord[U: ClassTag,G](
    data: RDD[U],
    sc: SparkContext,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    rddName: Option[String] = None)
  (rowIdAssigner: U => Long)
  (featureExtractor: U => Traversable[G]) = {
    val dictionary = sc.accumulable(new HashMapDictionaryCache[G]())(new HashMapDictionaryCacheAcc[G])
    val nnzAcc = sc.accumulator(0l)

    val (vectorRdd, partitionDims) = mapWithPartitionDims(data, sc){itr => new TransformIter(itr, rowIdAssigner, featureExtractor, nnzAcc, dictionary)}
    rddName.foreach{n => vectorRdd.setName(n)}
    vectorRdd.persist(storageLevel)
    val nRows = vectorRdd.count
    val nnz = nnzAcc.value
    val colDictionary = dictionary.value
    val nCols = colDictionary.size
    val matrixDims = new MatrixDims(nRows, nCols, nnz)
    val fullDims = RowMatrixPartitionDims(totalDims = matrixDims, partitionDims = partitionDims.value)
    println("creating rdd matrix w/ dims = " + fullDims.totalDims)
    new LongRowSparseVectorRDD[G](vectorRdd, fullDims, colDictionary)
  }


  def mapWithPartitionDims[A : ClassTag, B : ClassTag](
    rdd: RDD[A],
    sc: SparkContext
  )(
    transform: Iterator[A] => (FinalValueIterator[B,(Long,Long)])
  ) : (RDD[B], Accumulable[mutable.HashMap[Int, (Long,Long)], _]) = {
    val partitionDims = sc.accumulableCollection(mutable.HashMap[Int, (Long, Long)]())
    val transformedRDD = rdd.mapPartitionsWithIndex{
      case (partIdx, itr) =>
        val finalValIterator = transform(itr)
        new Iterator[B]() {
          def next = finalValIterator.next
          def hasNext = {
            val r = finalValIterator.hasNext
            if (!r) {
              //now store our accumulator updates
              partitionDims.localValue += (partIdx -> finalValIterator.finalValue)
            }
            r
          }
        }
    }

    (transformedRDD, partitionDims)
  }
}

trait FinalValueIterator[A,B] extends Iterator[A] {
  def finalValue : B
}

private class TransformIter[U,G](
    val itr: Iterator[U],
    val rowIdAssigner: U => Long,
    val featureExtractor: U => Traversable[G],
    val nnz: Accumulator[Long],
    val dictionary: Accumulable[HashMapDictionaryCache[G], (G, Long)]
) extends FinalValueIterator[LongSparseBinaryVectorWithRowId, (Long,Long)] {
  var partitionsRows = 0l
  var partitionNnz = 0l
  val sub = itr.map{
    u =>
    //TODO save rowId in a dictionary also??
      val rowId = rowIdAssigner(u)
      val features = featureExtractor(u)
      val featureIds = new Array[Long](features.size)
      nnz += features.size
      partitionsRows += 1
      partitionNnz += features.size
      var idx = 0
      features.foreach{ f =>
        val hash = Murmur64.hash64(f.toString)
        dictionary.localValue.addMapping(f, hash)
        featureIds(idx) = hash
        idx += 1
      }
      java.util.Arrays.sort(featureIds)
      new LongSparseBinaryVectorWithRowId(rowId, featureIds, 0, featureIds.length)
  }
  override def next = sub.next
  override def hasNext = sub.hasNext
  override def finalValue = (partitionsRows, partitionNnz)
}


