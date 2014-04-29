package org.sblaj

import org.sblaj.featurization.FeatureEnumeration
import java.util.Arrays

/**
 * Created by matt on 4/28/14.
 */
trait SparseCountVector extends Traversable[MTuple2[Int,Int]] {
  def nnz : Int
  def get(col:Int): Int
}

class LongSparseCountVector(val colIds: Array[Long], val cnts: Array[Int], val startIdx: Int, val endIdx: Int) extends Serializable {

  // the vector occupies a portion of a single array of Long features [293L, 16L] & [11, 2)
  // we can map Long to enumerated ints                               [3, 1]
  //
  def enumerateInto(into: Array[Int], intoCnts: Array[Int], pos: Int, enumeration: FeatureEnumeration) = {
    var sourceIdx = startIdx
    var targetIdx = pos
    while (sourceIdx < endIdx) {
      enumeration.getEnumeratedId(colIds(sourceIdx)).foreach{intCode =>
        into(targetIdx) = intCode
        intoCnts(targetIdx) = cnts(sourceIdx)
        targetIdx += 1
      }
      sourceIdx += 1
    }
    Sorting.sortParallel(into, intoCnts, pos, targetIdx-pos)
    targetIdx

  }
}

class LongSparseCountVectorWithRowId(val rowId: Long, override val colIds: Array[Long], override val cnts: Array[Int],
                                     override val startIdx: Int, override val endIdx: Int)
  extends LongSparseCountVector(colIds, cnts, startIdx, endIdx) with RowId


class BaseSparseCountVector(var colIds: Array[Int], var cnts: Array[Int], var startIdx: Int, var endIdx: Int)
  extends SparseCountVector with Serializable {

  def nnz = endIdx - startIdx

  def get(col : Int) : Int =
    Arrays.binarySearch(colIds, startIdx, endIdx, col) match {
      case no if no < 0 => 0
      case idx => cnts(idx)
    }

  def reset(colIds: Array[Int], cnts: Array[Int], startIdx: Int, endIdx: Int) {
    this.colIds = colIds
    this.cnts = cnts
    this.startIdx = startIdx
    this.endIdx = endIdx
  }

  override def foreach[U](f: MTuple2[Int,Int] => U) {
    val pair = new MTuple2[Int,Int](0,0)
    var idx = startIdx
    while (idx < endIdx) {
      pair._1 = colIds(idx)
      pair._2 = cnts(idx)
      f(pair)
      idx += 2
    }
  }
}
