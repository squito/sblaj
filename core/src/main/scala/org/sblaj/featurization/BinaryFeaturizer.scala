package org.sblaj.featurization

import org.sblaj.{LongSparseBinaryVectorWithRowId, LongSparseBinaryVector, SparseBinaryRowMatrix}
import util.MurmurHash
import org.sblaj.io.{DictionaryIO, VectorFileSet, VectorIO}
import java.io._
import scala.Serializable
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream
import collection._
import scala.util.hashing.MurmurHash3

trait BinaryFeaturizer[T] {
  //TODO this interface needs some work
  // you should be able to featurize into a buffer, so you don't have to size your own arrays,
  // and it should take care of duplicates for you
  def featurize(t: T, dictionary: DictionaryCache[String]) : (Long, Array[Long], Int, Int)
}

trait MurmurFeaturizer[T] extends BinaryFeaturizer[T] {

  val buffer = new Array[Long](100000)

  def extractor(t: T): Set[String]

  def getId(t:T): Long

  def featurize(t : T, dictionary: DictionaryCache[String]) = {
    val id = getId(t)
    val featureNames = extractor(t)
    var idx = 0
    featureNames.foreach{ f =>
      val code = Murmur64.hash64(f)
      buffer(idx) = code
      dictionary.addMapping(f, code)
      idx += 1
    }
    java.util.Arrays.sort(buffer, 0, idx)
    (id, buffer, 0, idx)
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
                       dictionary: DictionaryCache[String], matrixCounts: RowMatrixCountBuilder) = {
    val (id, cols, startIdx, endIdx) = featurizer.featurize(t, dictionary)
    val n = endIdx - startIdx
    val theCols = new Array[Long](n)
    System.arraycopy(cols, startIdx, theCols, 0, n)
    matrixCounts += (theCols, 0, n)
    new LongSparseBinaryVectorWithRowId(id, theCols, 0, n)
  }

  def mapFeaturize[T](ts: TraversableOnce[T], featurizer: BinaryFeaturizer[T]) = {
    val matrixCounts = new RowMatrixCountBuilder()
    val dictionary = new HashMapDictionaryCache[String]()
    //Q: Is there any way to do this w/ map? to force it to map from TraversableOnce to Traversable?
    val matrix = new Array[LongSparseBinaryVectorWithRowId](ts.size)
    var idx = 0
    ts.foreach{ t =>
      matrix(idx) = applyFeaturizer(t, featurizer, dictionary, matrixCounts)
      idx += 1
    }
    LongRowMatrix(matrixCounts, matrix, dictionary)
  }

  def featurizeToFiles[T](ts: Iterator[T], featurizer: BinaryFeaturizer[T], files: VectorFileSet, maxPartSize: Int) = {
    files.mkdirs()
    var countsBuilder : RowMatrixCountBuilder = null
    var dictionary: HashMapDictionaryCache[String] = null
    var out : DataOutputStream = null
    def openPart(partNum: Int) = {
      println("beginning part " + partNum)
      countsBuilder = new RowMatrixCountBuilder()
      dictionary = new HashMapDictionaryCache[String]()
      out = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(files.getOneFileSet(partNum).vectorFile)))
    }

    def finishPart(partNum: Int) = {
      println("finishing part " + partNum)
      out.close()
      DictionaryIO.writeDictionary(dictionary, files.getOneFileSet(partNum).dictionaryFile)
      VectorIO.writeMatrixCounts(countsBuilder.toRowMatrixCounts, files.getOneFileSet(partNum).dimensionFile)
    }
    var idx = 0
    var partNum = 0
    openPart(partNum)
    ts.foreach{ t =>
      if (idx > maxPartSize) {
        finishPart(partNum)
        partNum += 1
        idx = 0
        openPart(partNum)
      }
      val vector = applyFeaturizer(t, featurizer, dictionary, countsBuilder)
      val v = new LongSparseBinaryVector(vector.colIds, vector.startIdx, vector.endIdx)
      VectorIO.append(v, out)
      idx += 1
    }
    finishPart(partNum)
  }
}


class RowMatrixCountBuilder(var nRows: Long = 0, var nnz: Long = 0, val colIds: java.util.HashSet[Long] = new java.util.HashSet[Long]())
  extends Serializable {
  def += (cols: Array[Long], startIdx: Int, endIdx: Int) {
    (startIdx until endIdx).foreach{idx =>
      colIds.add(cols(idx))
    }
    nRows += 1
    nnz += cols.length
  }

  override def toString() = {
    "(" + nRows + "," + colIds.size() + "," + nnz + ")"
  }

  def toRowMatrixCounts: RowMatrixCounts = RowMatrixCounts(nRows, colIds.size(), nnz)
}
case class RowMatrixCounts(val nRows: Long, val nCols: Long, val nnz: Long)

case class LongRowSparseBinaryVector(val id: Long, val colIds: Array[Long], val startIdx: Int, val endIdx: Int) {
  def enumerateInto(into: Array[Int], pos: Int, enumeration: FeatureEnumeration) = {
    var idx = startIdx
    while (idx < endIdx) {
      into(pos + idx) = enumeration.getEnumeratedId(colIds(idx)).get
      idx += 1
    }
    pos + idx
  }
}

case class LongRowMatrix(val dims: RowMatrixCountBuilder, val matrix: Traversable[LongSparseBinaryVectorWithRowId],
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
    val hash = MurmurHash3.stringHash(s)
    val low = hash.asInstanceOf[Long]
    val high = MurmurHash3.mix(hash, hash).asInstanceOf[Long]
    (high << 32) | (low & highBitMask)
  }
}

object SampleUtils {
  def toUnit(long: Long): Double = {
    long.toDouble / Long.MaxValue
  }
}