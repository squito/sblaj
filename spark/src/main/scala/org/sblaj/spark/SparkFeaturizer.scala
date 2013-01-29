package org.sblaj.spark

import spark.{AccumulatorParam, RDD, SparkContext}
import org.sblaj.featurization.{Murmur64, HashMapDictionaryCache}
import _root_.spark.storage.StorageLevel
import org.sblaj._
import collection._

object SparkFeaturizer {

  //TODO remove this when its released in spark
  implicit object LongAccumulatorParam extends AccumulatorParam[Long] {
    def addInPlace(t1: Long, t2: Long): Long = t1 + t2
    def zero(initialValue: Long) = 0l
  }

  /**
   * Convert an RDD into a SparseBinaryMatrix.
   *
   * This assumes that each record in the RDD corresponds to one row in matrix, and that the entire dictionary
   * will fit in memory.
   *
   */
  def rowPerRecord[U,G](data: RDD[U], sc: SparkContext, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)(rowIdAssigner: U => Long)(featureExtractor: U => Traversable[G]) = {
    val dictionary = sc.accumulableCollection(new HashMapDictionaryCache[G]())
    val nnzAcc = sc.accumulator(0l)
    val partitionDims = sc.accumulableCollection(mutable.HashMap[Int, (Long, Long)]())
    val vectorRdd = data.mapPartitionsWithSplit{
      case (split, itr) =>
        /* We want to process the data in an iterative fashion, BUT, when the iterator
         * is done, then we want to store the size of the partition in the accumulator.  So, wrap with a special
         * iterator that gets to "cleanup" when the inner iterator is done
         */
        var partitionsRows = 0l
        var partitionNnz = 0l
        new Iterator[LongSparseBinaryVectorWithRowId](){
          val sub = itr.map{
            u =>
            //TODO save rowId in a dictionary also??
              val rowId = rowIdAssigner(u)
              val features = featureExtractor(u)
              val featureIds = new Array[Long](features.size)
              nnzAcc += features.size
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
          def next = sub.next
          def hasNext = {
            val r = sub.hasNext
            if (!r) {
              //now store our accumulator updates
              partitionDims.localValue += (split -> (partitionsRows, partitionNnz))
            }
            r
          }
        }
    }
    val nRows = vectorRdd.count
    val nnz = nnzAcc.value
    val colDictionary = dictionary.value
    val nCols = colDictionary.size
    val matrixDims = new MatrixDims(nRows, nCols, nnz)
    val fullDims = RowMatrixPartitionDims(totalDims = matrixDims, partitionDims = partitionDims.value)
    new LongRowSparseVectorRDD[G](vectorRdd, fullDims, colDictionary)
  }
}
