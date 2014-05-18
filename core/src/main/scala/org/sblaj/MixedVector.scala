package org.sblaj

/**
 *
 */
class MixedVector(
  denseCols: Array[Float],
  var denseStartIdx: Int,
  var denseEndIdx: Int,
  sparseColIds: Array[Int],
  sparseColVals: Array[Float],
  var sparseStartIdx: Int,
  var sparseEndIdx: Int,
  nDenseCols: Int,
  nSparseCols: Int
) {

  def get(col: Int): Float = {
    if (col >= nDenseCols) {
      val idx = java.util.Arrays.binarySearch(sparseColIds, sparseStartIdx, sparseEndIdx, col)
      if (idx >= 0) {
        sparseColVals(idx)
      } else {
        0f
      }
    } else {
      denseCols(denseStartIdx + col)
    }
  }

  def resetPosition(
    denseStartIdx: Int,
    denseEndIdx: Int,
    sparseStartIdx: Int,
    sparseEndIdx: Int
  ) {
    this.denseStartIdx = denseStartIdx
    this.denseEndIdx = denseEndIdx
    this.sparseStartIdx = sparseStartIdx
    this.sparseEndIdx = sparseEndIdx
  }

  override def toString(): String = {
    (0 until (nDenseCols + nSparseCols)).map{colIdx => colIdx  + ":" + get(colIdx)}.mkString("{", ",", "}")
  }
}
