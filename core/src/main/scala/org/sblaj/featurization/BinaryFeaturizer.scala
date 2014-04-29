package org.sblaj.featurization

import org.sblaj.{LongSparseBinaryVectorWithRowId, SparseBinaryRowMatrix}
import scala.Serializable
import scala.collection._
import scala.util.hashing.MurmurHash3


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

  def hash64(cs: CharSequence) : Long = {
    val s = cs.toString
    val low = MurmurHash3.stringHash(s)
    val high = MurmurHash3.stringHash(s, low)
    (high.toLong << 32) | (low & highBitMask)
  }
}

object SampleUtils {
  def toUnit(long: Long): Double = {
    long.toDouble / Long.MaxValue
  }
}