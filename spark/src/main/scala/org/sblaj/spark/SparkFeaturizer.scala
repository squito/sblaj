package org.sblaj.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.sblaj.featurization._
import org.apache.spark.storage.StorageLevel
import org.sblaj._
import collection._
import scala.reflect.ClassTag
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import org.sblaj.MatrixDims

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


  def dictionarySample[U: ClassTag](
    data: RDD[U],
    sc: SparkContext,
    dictionarySampleRate: Double = 0.1,
    minCount: Int = 10,
    topFeatures: Int = 1e6.toInt)
  (rowIdAssigner: U => Long)
  (featureExtractor: U => Traversable[String]): GeneralCompleteDictionary[String] = {

    val tooLowAcc = sc.accumulator(0l)
    val okAcc = sc.accumulator(0l)

    // we could probably do the map-side combine more efficiently ourselves, since we can use a specialized map
    val featureCountsRdd = data.flatMap{in =>
      if (SampleUtils.toUnit(Murmur64.hash64(rowIdAssigner(in).toString)) < dictionarySampleRate) {
        featureExtractor(in).map{f => (Murmur64.hash64(f.toString), (Vector(f), 1))}
      } else {
        Seq()
      }
    }.reduceByKey{case ((features1,count1), (features2, count2)) => (features1 ++ features2, count1 + count2)}.
      filter{case (fHash, (features, counts)) =>
        if (counts >= minCount) {
          okAcc += 1
          true
        } else {
          tooLowAcc += 1
          false
        }
    }
    val features = featureCountsRdd.collect()
    println("features below minCount = " + tooLowAcc.value)
    println("ok features = " + okAcc.value)

    val limitedFeatures = features.sortBy{-_._2._2}.take(topFeatures)

    val hashToECode = new Long2IntOpenHashMap()
    val reverseEnum = new Array[Long](limitedFeatures.length)
    (0 until limitedFeatures.length).foreach{idx =>
      val hash = limitedFeatures(idx)._1
      hashToECode.put(hash, idx)
      reverseEnum(idx) = hash
    }

    //TODO wrap this up into a dictionary and return it
    new GeneralCompleteDictionary[String](
      elems = null, //TODO
      hashToECode,
      reverseEnum
    )
  }

  def scalableRowPerRecord[U: ClassTag](
    data: RDD[U],
    sc: SparkContext,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    rddName: Option[String] = None,
    dictionarySampleRate: Double = 0.1,
    minCount: Int = 10,
    topFeatures: Int = 1e6.toInt)
  (rowIdAssigner: U => Long)
  (featureExtractor: U => Traversable[String]): EnumeratedSparseVectorRDD[String] = {


    val dictionary = dictionarySample(data, sc, dictionarySampleRate, minCount, topFeatures)(rowIdAssigner)(featureExtractor)

    val bcHashToECode = sc.broadcast(new HashToEnum(dictionary.enumerated))
    val nnzAcc = sc.accumulator(0l)
    val (vectorRdd, partitionDims) = mapWithPartitionDims(data, sc){itr => new SubsetTransformIter[U](itr,featureExtractor,nnzAcc, bcHashToECode.value)}
    rddName.foreach{vectorRdd.setName}
    vectorRdd.persist(storageLevel)
    val nRows = vectorRdd.count
    val nnz = nnzAcc.value

    val dims = RowMatrixPartitionDims(totalDims = new MatrixDims(nRows, dictionary.reverseEnum.length, nnz), partitionDims = partitionDims.value)
    new EnumeratedSparseVectorRDD[String](vectorRdd, dims, dictionary, dictionary)
  }




    /**
   * Convert an RDD into a SparseBinaryMatrix.
   *
   * This assumes that each record in the RDD corresponds to one row in matrix, and that the entire dictionary
   * will fit in memory.
   *
   */
  def accumulatorRowPerRecord[U: ClassTag,G](
    data: RDD[U],
    sc: SparkContext,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
    rddName: Option[String] = None)
  (rowIdAssigner: U => Long)
  (featureExtractor: U => Traversable[G]): LongRowSparseVectorRDD[G] = {
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

private class SubsetTransformIter[U](
  val itr: Iterator[U],
  val featureExtractor: U => Traversable[String],
  val nnz: Accumulator[Long],
  val codes: Long => Option[Int]
) extends FinalValueIterator[BaseSparseBinaryVector, (Long,Long)] {
  var partitionRows = 0l
  var partitionNnz = 0l

  val sub = itr.map{ u =>
    //TODO rowId?
    val features = featureExtractor(u).flatMap{f => codes(Murmur64.hash64(f))}.toArray.sorted
    partitionRows += 1
    partitionNnz += features.length
    nnz += features.length
    new BaseSparseBinaryVector(features, 0, features.length)
  }

  override def next = sub.next
  override def hasNext = sub.hasNext
  override def finalValue = (partitionRows, partitionNnz)
}


