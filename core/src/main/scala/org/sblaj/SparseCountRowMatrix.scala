package org.sblaj

import scala.collection.Traversable

/**
 * Created by matt on 4/28/14.
 */
class SparseCountRowMatrix private(nMaxRows: Int, nMaxNonZeros: Int, nColumns: Int, columnIds: Array[Int], counts: Array[Int], rowStartIdxs: Array[Int])
  extends SparseMatrix
  with Traversable[SparseCountVector]
  with Serializable {

  val maxRows: Int = nMaxRows
  val maxNnz: Int = nMaxNonZeros
  val nCols: Int = nColumns

  val colIds: Array[Int] = columnIds
  val cnts: Array[Int] = counts
  val rowStartIdx: Array[Int] = rowStartIdxs

  var nRows: Int = 0
  var nnz: Int = 0

  def this(nMaxRows: Int, nMaxNonZeros: Int, nColumns: Int) {
    this(nMaxRows, nMaxNonZeros, nColumns, new Array[Int](nMaxNonZeros), new Array[Int](nMaxNonZeros), new Array[Int](nMaxRows + 1))
  }

  override def multInto(y: Array[Float], r: Array[Float]): Unit = ???

  override def get(x: Int, y: Int) : Float = {
    val p = java.util.Arrays.binarySearch(colIds, y, rowStartIdx(x), rowStartIdx(x + 1))
    if (p < 0)
      return 0
    else
      return counts(p)
  }

  override def foreach[U](f: (SparseCountVector) => U): Unit = {
    var rowIdx = 0
    val v = new BaseSparseCountVector(colIds, counts, 0, 0)
    while (rowIdx < nRows) {
      v.reset(colIds, cnts, rowStartIdx(rowIdx), rowStartIdx(rowIdx + 1))
      f(v)
      rowIdx += 1
    }
  }

  def setSize(nRows: Int, nnz: Int) {
    this.nRows = nRows
    this.nnz = nnz
  }
}