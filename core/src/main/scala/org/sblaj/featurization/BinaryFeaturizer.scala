package org.sblaj.featurization

import org.sblaj.SparseBinaryRowMatrix
import util.MurmurHash

trait BinaryFeaturizer[T] {
  //TODO this interface needs some work
  // you should be able to featurize into a buffer, so you don't have to size your own arrays,
  // and it should take care of duplicates for you
  def featurize(t: T, dictionary: DictionaryCache[String]) : (Long, Array[Long])
}

trait MurmurFeaturizer[T] extends BinaryFeaturizer[T] {

  def extractor : T => TraversableOnce[String]

  def getId(t:T): Long

  def featurize(t : T, dictionary: DictionaryCache[String]) = {
    val id = getId(t)
    val featureNames = extractor(t)
    val features = new Array[Long](featureNames.size)
    var idx = 0
    featureNames.foreach{ f =>
      val code = Murmur64.hash64(f)
      features(idx) = code
      dictionary.addMapping(f, code)
      idx += 1
    }
    (id, features.sorted)
  }
}

/**
 * Does some of the book-keeping required to put a dataset into a SparseMatrix.
 *
 * The current implementation is NOT really optimized -- it mostly just expects things to
 * fit into memory.  Eventually there should be other implementations that are smarter
 */
object FeaturizerHelper {

  def applyFeaturizer[T](t: T, featurizer: BinaryFeaturizer[T],
                       dictionary: DictionaryCache[String], matrixCounts: RowMatrixCounts) = {
    val (id, cols) = featurizer.featurize(t, dictionary)
    matrixCounts += cols
    LongRowSparseBinaryVector(id, cols)
  }


  def mapFeaturize[T](ts: TraversableOnce[T], featurizer: BinaryFeaturizer[T]) = {
    val matrixCounts = new RowMatrixCounts()
    val dictionary = new HashMapDictionaryCache[String]()
    //Q: Is there any way to do this w/ map? to force it to map from TraversableOnce to Traversable?
    val matrix = new Array[LongRowSparseBinaryVector](ts.size)
    var idx = 0
    ts.foreach{ t =>
      matrix(idx) = applyFeaturizer(t, featurizer, dictionary, matrixCounts)
      idx += 1
    }
    LongRowMatrix(matrixCounts, matrix, dictionary)
  }
}


class RowMatrixCounts(var nRows: Long = 0, var nnz: Long = 0, val colIds: java.util.HashSet[Long] = new java.util.HashSet[Long]())
  extends Serializable {
  def += (cols: Array[Long]) {
    cols.foreach {
      colIds.add(_)
    }
    nRows += 1
    nnz += cols.length
  }

  override def toString() = {
    "(" + nRows + "," + colIds.size() + "," + nnz + ")"
  }
}

case class LongRowSparseBinaryVector(val id: Long, val colIds: Array[Long]) {
  def enumerateInto(into: Array[Int], pos: Int, enumeration: FeatureEnumeration) = {
    var idx = 0
    while (idx < colIds.length) {
      into(pos + idx) = enumeration.getEnumeratedId(colIds(idx))
      idx += 1
    }
    pos + idx
  }
}

case class LongRowMatrix(val dims: RowMatrixCounts, val matrix: Traversable[LongRowSparseBinaryVector],
                            val dictionary: DictionaryCache[String]) {

  def toSparseBinaryRowMatrix() = {

    val enumeration = dictionary.getEnumeration()
    val mat = new SparseBinaryRowMatrix(dims.nRows.asInstanceOf[Int], dims.nnz.asInstanceOf[Int], dims.colIds.size())
    var pos = 0
    var rowIdx = 0
    matrix.foreach{ vector =>
      mat.rowStartIdx(rowIdx) = pos
      pos = vector.enumerateInto(mat.colIds, pos, enumeration)
      rowIdx += 1
    }
    mat.rowStartIdx(rowIdx) = pos
    mat.setSize(rowIdx, pos)
    mat
  }

}


object Murmur64 {
  //scala's murmurhash is only 32-bit ... interface makes it hard to get 64-bits out.
  // this is a stupid way to get 64 bits

  val highBitMask: Long = 0xffffffffl

  def hash64(s: String) : Long = {
    val murmur = new MurmurHash[String](0xf7ca7fd2)
    val hash = MurmurHash.stringHash(s)
    murmur.append(hash)
    val low = murmur.hash.asInstanceOf[Long]
    murmur.append(hash)
    val high = murmur.hash.asInstanceOf[Long]
    (high << 32) | (low & highBitMask)
  }
}