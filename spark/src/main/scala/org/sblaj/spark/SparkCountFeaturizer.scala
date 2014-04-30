package org.sblaj.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.sblaj.featurization._
import org.apache.spark.storage.StorageLevel
import org.sblaj._
import collection._
import scala.reflect.ClassTag
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, Long2IntOpenHashMap}
import org.sblaj.MatrixDims

object SparkCountFeaturizer {


  class HashMapDictionaryCacheAcc[G] extends AccumulableParam[HashMapDictionaryCache[G], (G, Long)] {
    def addAccumulator(m: HashMapDictionaryCache[G], kv: (G, Long)): HashMapDictionaryCache[G] = {
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
                                   (featureExtractor: U => Traversable[(String, Int)]): (RDD[DictionaryRow], DictionaryBuildingAccumulatorBundle) = {

    val tooLowAcc = sc.accumulator(0l)
    val okAcc = sc.accumulator(0l)
    val collisionsInFinalFeatures = sc.accumulator(0l)

    // we could probably do the map-side combine more efficiently ourselves, since we can use a specialized map
    val featureCountsRdd: RDD[(String, Int)] = data.flatMap {
      in =>
        if (SampleUtils.toUnit(Murmur64.hash64(rowIdAssigner(in).toString)) < dictionarySampleRate) {
          featureExtractor(in)
        } else {
          Seq()
        }
    }.reduceByKey {
      _ + _
    }
      .filter {
      case (feature, counts) =>
        if (counts >= minCount) {
          true
        } else {
          tooLowAcc += 1
          false
        }
    }.setName("feature-counts")
      .persist(StorageLevel.MEMORY_ONLY)

    //we could probably do this approximately, with larger buckets, but this should still scale pretty well
    val featureCountsHistogram = featureCountsRdd.map {
      case (feature, counts) => (counts, 1)
    }.
      reduceByKey {
      _ + _
    }.collect().sortBy {
      -_._1
    }

    val preciseMinCount = getHistogramCutoff(featureCountsHistogram, topFeatures)
    println("precise min count = " + preciseMinCount)

    val dictionaryRdd: RDD[DictionaryRow] = featureCountsRdd.filter {
      case (feature, counts) =>
        if (counts > preciseMinCount) {
          okAcc += 1
          true
        } else {
          tooLowAcc += 1
          false
        }
    }.map {
      case (feature, counts) =>
        (Murmur64.hash64(feature.toString), (feature, counts))
    }.groupByKey().map {
      case (fHash, featuresAndCounts) =>
        if (featuresAndCounts.size > 1) {
          collisionsInFinalFeatures += 1
        }
        DictionaryRow(fHash, featuresAndCounts)
    }
    (dictionaryRdd, DictionaryBuildingAccumulatorBundle(okAcc, tooLowAcc, collisionsInFinalFeatures))
  }

  def dictRddToInMem(rdd: RDD[DictionaryRow]): GeneralCompleteDictionary[String] = {

    /* amazingly, even after filtering down to the top 1M features, we can still run out of memory
       when we try to collect, with 16GB.  Pretty insane if you think about it.
     */
    val limitedFeatures = rdd.map {
      d =>
        val joinedName = d.namesAndCounts.map {
          _._1
        }.map {
          truncate(_, 30)
        }.mkString("|OR|")
        val totalCount = d.namesAndCounts.map {
          _._2
        }.sum
        (d.hash, joinedName, totalCount)
    }.collect()
    println("collected limited features, building in memory dictionary")

    val hashToECode = new Long2IntOpenHashMap()
    val reverseEnum = new Array[Long](limitedFeatures.length)
    (0 until limitedFeatures.length).foreach {
      idx =>
        val hash = limitedFeatures(idx)._1
        hashToECode.put(hash, idx)
        reverseEnum(idx) = hash
    }

    val long2String = new Long2ObjectOpenHashMap[Seq[String]]()
    limitedFeatures.foreach {
      case (hash, names, count) =>
        long2String.put(hash, Seq(names)) //TODO I've already flattened the strings, I should remove the Seq
    }

    new GeneralCompleteDictionary[String](
      elems = long2String,
      hashToECode,
      reverseEnum
    )
  }

  def scalableRowPerRecord[U: ClassTag](
                                         data: RDD[U],
                                         sc: SparkContext,
                                         rddDir: String,
                                         storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                         rddName: Option[String] = None,
                                         dictionarySampleRate: Double = 0.1,
                                         minCount: Int = 10,
                                         topFeatures: Int = 1e6.toInt)
                                       (rowIdAssigner: U => Long)
                                       (featureExtractor: U => Traversable[(String,Int)]): EnumeratedSparseCountVectorRDD[String] = {


    val (dictionaryRdd, dictionaryAccs) = dictionarySample(data, sc, dictionarySampleRate, minCount, topFeatures)(rowIdAssigner)(featureExtractor)
    println("sample dictionaryRows:")
    dictionaryRdd.setName("limited dictionary RDD")
    dictionaryRdd.persist(storageLevel)
    dictionaryRdd.saveAsObjectFile(rddDir + "/dictionary")
    dictionaryRdd.take(10).foreach {
      println
    }

    println("features below minCount = " + dictionaryAccs.tooLow.value)
    println("ok features = " + dictionaryAccs.ok.value)
    println("collisions = " + dictionaryAccs.collisions.value)

    val dictionary = dictRddToInMem(dictionaryRdd)
    println("unpersisting dictionary")
    dictionaryRdd.unpersist()

    val bcHashToECode = sc.broadcast(new HashToEnum(dictionary.enumerated))
    val nnzAcc = sc.accumulator(0l)
    val (vectorRdd, partitionDims) = mapWithPartitionDims(data, sc) {
      itr => new SubsetTransformCntIter[U](itr, featureExtractor, nnzAcc, bcHashToECode.value)
    }
    rddName.foreach {
      vectorRdd.setName
    }
    vectorRdd.persist(storageLevel)
    vectorRdd.saveAsObjectFile(rddDir + "/enumerated_vectors")
    val nRows = vectorRdd.count
    val nnz = nnzAcc.value

    val dims = RowMatrixPartitionDims(totalDims = new MatrixDims(nRows, dictionary.reverseEnum.length, nnz), partitionDims = partitionDims.value)
    new EnumeratedSparseCountVectorRDD[String](vectorRdd, dims, dictionary, dictionary)
  }


  /**
   * Convert an RDD into a SparseCountMatrix.
   *
   * This assumes that each record in the RDD corresponds to one row in matrix, and that the entire dictionary
   * will fit in memory.
   *
   */
  def accumulatorRowPerRecord[U: ClassTag, G](data: RDD[U],
                                               sc: SparkContext,
                                               storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                               rddName: Option[String] = None)
                                             (rowIdAssigner: U => Long)
                                             (featureExtractor: U => Traversable[(G,Int)]): LongRowSparseCountVectorRDD[G] = {
    val dictionary = sc.accumulable(new HashMapDictionaryCache[G]())(new HashMapDictionaryCacheAcc[G])
    val nnzAcc = sc.accumulator(0l)

    val (vectorRdd, partitionDims) = mapWithPartitionDims(data, sc) {
      itr => new TransformCntIter(itr, rowIdAssigner, featureExtractor, nnzAcc, dictionary)
    }
    rddName.foreach {
      n => vectorRdd.setName(n)
    }
    vectorRdd.persist(storageLevel)
    val nRows = vectorRdd.count
    val nnz = nnzAcc.value
    val colDictionary = dictionary.value
    val nCols = colDictionary.size
    val matrixDims = new MatrixDims(nRows, nCols, nnz)
    val fullDims = RowMatrixPartitionDims(totalDims = matrixDims, partitionDims = partitionDims.value)
    println("creating rdd matrix w/ dims = " + fullDims.totalDims)
    new LongRowSparseCountVectorRDD[G](vectorRdd, fullDims, colDictionary)
  }


  def mapWithPartitionDims[A: ClassTag, B: ClassTag](rdd: RDD[A],
                                                     sc: SparkContext)(
                                                      transform: Iterator[A] => (FinalValueIterator[B, (Long, Long)])
                                                      ): (RDD[B], Accumulable[mutable.HashMap[Int, (Long, Long)], _]) = {
    val partitionDims = sc.accumulableCollection(mutable.HashMap[Int, (Long, Long)]())
    val transformedRDD = rdd.mapPartitionsWithIndex {
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

  /**
   * find the cutoff to get the top *max* elements.
   * returns the first value that should *not* be included
   */
  def getHistogramCutoff(histo: Array[(Int, Int)], max: Int): Int = {
    //could potentially be refactored into a general histogram
    var total = 0
    var cutoff = histo(0)._1
    val itr = histo.iterator
    while (itr.hasNext && total <= max) {
      val (v, counts) = itr.next()
      total += counts
      cutoff = v
    }
    if (total <= max) //we've read everything, still haven't gone over the limit.  take everything
      cutoff -= 1
    cutoff
  }

  def truncate(s: String, l: Int): String = {
    if (s.length > l) s.substring(0, l)
    else s
  }
}

private class TransformCntIter[U, G](
                                   val itr: Iterator[U],
                                   val rowIdAssigner: U => Long,
                                   val featureExtractor: U => Traversable[(G, Int)],
                                   val nnz: Accumulator[Long],
                                   val dictionary: Accumulable[HashMapDictionaryCache[G], (G, Long)]
                                   ) extends FinalValueIterator[LongSparseCountVectorWithRowId, (Long, Long)] {
  var partitionsRows = 0l
  var partitionNnz = 0l
  val sub = itr.map {
    u =>
    //TODO save rowId in a dictionary also??
      val rowId = rowIdAssigner(u)
      val featureCnts = featureExtractor(u)
      val featureIds = new Array[Long](featureCnts.size)
      val cnts = new Array[Int](featureCnts.size)
      nnz += featureCnts.size
      partitionsRows += 1
      partitionNnz += featureCnts.size
      var idx = 0
      featureCnts.foreach {
        case(f,cnt) =>
          val hash = Murmur64.hash64(f.toString)
          dictionary.localValue.addMapping(f, hash)
          featureIds(idx) = hash
          cnts(idx) = cnt
          idx += 1
      }
      Sorting.sortParallel(featureIds, cnts)
      new LongSparseCountVectorWithRowId(rowId, featureIds, cnts, 0, featureIds.length)
  }

  override def next = sub.next

  override def hasNext = sub.hasNext

  override def finalValue = (partitionsRows, partitionNnz)
}

private class SubsetTransformCntIter[U](
                                      val itr: Iterator[U],
                                      val featureExtractor: U => Traversable[(String, Int)],
                                      val nnz: Accumulator[Long],
                                      val codes: Long => Option[Int]
                                      ) extends FinalValueIterator[BaseSparseCountVector, (Long, Long)] {
  var partitionRows = 0l
  var partitionNnz = 0l

  val sub = itr.map {
    u =>
    //TODO rowId?
      val (featureSeq, cntSeq) = featureExtractor(u).flatMap {
        case (f, cnt) => codes(Murmur64.hash64(f)).map(_ -> cnt)
      }.unzip
      val features = featureSeq.toArray
      val cnts = cntSeq.toArray
      Sorting.sortParallel(features, cnts)

      partitionRows += 1
      partitionNnz += features.length
      nnz += features.length
      new BaseSparseCountVector(features, cnts, 0, features.length)
  }

  override def next = sub.next

  override def hasNext = sub.hasNext

  override def finalValue = (partitionRows, partitionNnz)
}
