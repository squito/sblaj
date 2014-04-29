package org.sblaj.featurization

import scala.collection.{TraversableOnce, Traversable}
import org.sblaj._

/**
 * Created by matt on 4/28/14.
 */

trait CountFeaturizer[T] {
  //TODO just like binary featurizer, this interface needs some work
  def featurize(t: T, dictionary: DictionaryCache[String]): (Long, Array[Long], Array[Int], Int, Int)
}

trait MurmurCountFeaturizer[T] extends CountFeaturizer[T] {

  val buffer = new Array[Long](100000)
  val counts = new Array[Int](100000)

  def extractor(t: T): Map[String, Int]

  def getId(t: T): Long

  def featurize(t: T, dictionary: DictionaryCache[String]) = {
    val id = getId(t)
    val featureCounts = extractor(t)
    var idx = 0
    featureCounts.foreach {
      case (f, cnt) =>
        val code = Murmur64.hash64(f)
        buffer(idx) = code
        counts(idx) = cnt
        dictionary.addMapping(f, code)
        idx += 1
    }

    Sorting.sortParallel(buffer, counts, 0, idx)

    (id, buffer, counts, 0, idx)
  }
}


/**
 * Does some of the book-keeping required to put a dataset into a SparseMatrix.
 *
 * The current implementation is NOT really optimized -- it mostly just expects things to
 * fit into memory.  Eventually there should be other implementations that are smarter
 */
object CountFeaturizerHelper {
  def applyFeaturizer[T](t: T, featurizer: CountFeaturizer[T],
                         dictionary: DictionaryCache[String], matrixCounts: RowMatrixCountBuilder) = {
    val (id, cols, cnts, startIdx, endIdx) = featurizer.featurize(t, dictionary)
    val n = endIdx - startIdx
    val theCols = new Array[Long](n)
    val theCnts = new Array[Int](n)
    System.arraycopy(cols, startIdx, theCols, 0, n)
    System.arraycopy(cnts, startIdx, theCnts, 0, n)
    matrixCounts +=(theCols, 0, n)
    println("Created rowId " + id)
    new LongSparseCountVectorWithRowId(id, theCols, theCnts, 0, n)
  }
  def mapFeaturize[T](ts: TraversableOnce[T], featurizer: CountFeaturizer[T]) = {
    val matrixCounts = new RowMatrixCountBuilder()
    val dictionary = new HashMapDictionaryCache[String]()
    val matrix = new Array[LongSparseCountVectorWithRowId](ts.size)
    var idx = 0
    ts.foreach{ t =>
      matrix(idx) = applyFeaturizer(t, featurizer, dictionary, matrixCounts)
      idx += 1
    }
    LongCountRowMatrix(matrixCounts, matrix, dictionary)

  }



}

case class LongRowSparseCountVector(val id: Long, val colIds: Array[Long], val startIdx: Int, val endIdx: Int) {
  def enumerateInto(into: Array[Int], pos: Int, enumeration: FeatureEnumeration) = {
    var idx = startIdx
    while (idx < endIdx) {
      into(pos + idx) = enumeration.getEnumeratedId(colIds(idx)).get
      idx += 1
    }
    pos + idx
  }
}

case class LongCountRowMatrix(val dims: RowMatrixCountBuilder, val matrix: Traversable[LongSparseCountVectorWithRowId],
                         val dictionary: DictionaryCache[String]) {

  def toSparseCountRowMatrix():SparseCountRowMatrix = {

    // translate longs into dense ints
    val enumeration = dictionary.getEnumeration()
    val mat = new SparseCountRowMatrix(dims.nRows.asInstanceOf[Int], dims.nnz.asInstanceOf[Int], dims.colIds.size())
    var pos = 0
    var rowIdx = 0
    // iterate through our vectors
    matrix.foreach {
      vector =>
        mat.rowStartIdx(rowIdx) = pos
        pos = vector.enumerateInto(mat.colIds, mat.cnts, pos, enumeration)
        rowIdx += 1
    }
    mat.rowStartIdx(rowIdx) = pos
    mat.setSize(rowIdx, pos)
    mat
  }

}