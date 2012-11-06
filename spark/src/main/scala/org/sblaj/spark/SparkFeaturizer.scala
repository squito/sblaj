package org.sblaj.spark

import _root_.spark.{AccumulatorParam, RDD, SparkContext}
import org.sblaj.featurization.{DictionaryCache, Murmur64, HashMapDictionaryCache}
import org.sblaj._

object SparkFeaturizer {


  //seriously spark??
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
   * @param data
   * @param featureExtractor
   * @tparam U
   * @tparam G
   * @return
   */
  def rowPerRecord[U,G](data: RDD[U], sc: SparkContext)(rowIdAssigner: U => Long)(featureExtractor: U => Traversable[G]) = {
    val dictionary = sc.accumulableCollection(new HashMapDictionaryCache[G]())
    val nnzAcc = sc.accumulator(0l)
    val vectorRdd = data.map {
      u =>
        //TODO save rowId in a dictionary also??
        val rowId = rowIdAssigner(u)
        val features = featureExtractor(u)
        val featureIds = new Array[Long](features.size)
        nnzAcc += features.size
        var idx = 0
        features.foreach{ f =>
          val hash = Murmur64.hash64(f.toString)
          dictionary.value.addMapping(f, hash)
          featureIds(idx) = hash
          idx += 1
        }
        java.util.Arrays.sort(featureIds)
        new LongSparseBinaryVectorWithRowId(rowId, featureIds, 0, featureIds.length)
    }
    val nRows = vectorRdd.count
    val nnz = nnzAcc.value
    val colDictionary = dictionary.value
    val nCols = colDictionary.size
    val matrixDims = new MatrixDims(nRows, nCols, nnz)
    new SparseVectorRDD[G](vectorRdd, matrixDims, colDictionary)
  }
}

class SparseVectorRDD[G](val vectorRdd: RDD[LongSparseBinaryVectorWithRowId], val matrixDims: MatrixDims,
                                                         val colDictionary: DictionaryCache[G]) {
  def toEnumeratedVectors(sc: SparkContext) = {
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
class EnumeratedSparseVectorRDD[G](val vectorRDD: RDD[BaseSparseBinaryVector], val matrixDims: MatrixDims,
                                   val colDictionary: DictionaryCache[G]) {
  def toSparseMatrix() = {
    val vectors = vectorRDD.collect()
    val matrix = new SparseBinaryRowMatrix(nMaxRows = matrixDims.nRows.toInt,
      nMaxNonZeros = matrixDims.nnz.toInt,
      nColumns = matrixDims.nCols.toInt)
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
}

//TODO some container that knows the matrix sizes of each partition, and so can convert each partition into a matrix