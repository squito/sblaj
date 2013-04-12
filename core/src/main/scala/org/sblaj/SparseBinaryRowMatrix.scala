package org.sblaj

/**
 *
 * foreach doesn't pass new object to each function
 */

class SparseBinaryRowMatrix private (nMaxRows: Int, nMaxNonZeros:Int, nColumns: Int, columnIds: Array[Int], rowStartIdxs: Array[Int])
  extends SparseMatrix
  with Traversable[SparseBinaryVector]  //maybe this should be part of SparseMatrix?
  {

  val maxRows: Int = nMaxRows
  val maxNnz: Int = nMaxNonZeros
  val nCols : Int = nColumns

  val colIds : Array[Int] = columnIds
  val rowStartIdx : Array[Int] = rowStartIdxs

  var nRows : Int = 0
  var nnz : Int = 0

  def this(nMaxRows: Int, nMaxNonZeros:Int, nColumns: Int) {
    this(nMaxRows, nMaxNonZeros, nColumns, new Array[Int](nMaxNonZeros), new Array[Int](nMaxRows + 1))
  }

  /**
   * create a sparsebinaryrowmatrix with the given arrays to hold the data.
   * <p>
   * stores a reference to the given arrays, does *not* make a copy
   * @param columnIds
   * @param rowStartIdxs
   * @param nColumns
   */
  def this(columnIds: Array[Int], rowStartIdxs: Array[Int], nColumns: Int) {
    this(rowStartIdxs.length - 1, columnIds.length, nColumns, columnIds, rowStartIdxs)
    setSize(maxRows, maxNnz)
  }

  def setSize(nRows: Int, nnz: Int) {
    this.nRows = nRows
    this.nnz = nnz
  }

  /**
   * note that this doesn't necessarily pass a different object to each function call -- it may reuse the same
   * object multiple times
   * @param f
   * @tparam U
   */
  def foreach[U](f: SparseBinaryVector => U) : Unit = {
    var rowIdx = 0
    val v = new BaseSparseBinaryVector(colIds, 0,0)
    while (rowIdx < nRows) {
      v.reset(colIds, rowStartIdx(rowIdx), rowStartIdx(rowIdx + 1))
      f(v)
      rowIdx += 1
    }
  }

  def get(x: Int, y: Int) : Float = {
    val p = java.util.Arrays.binarySearch(colIds, y, rowStartIdx(x), rowStartIdx(x + 1))
    if (p < 0)
      return 0
    else
      return 1
  }

  def multInto(y : Array[Float], r: Array[Float]) {
    //TODO
  }


  def getColSums() : Array[Int] = {
    val colSums = new Array[Int](nCols)
    var idx = 0
    while (idx < nnz) {
      val col = colIds(idx)
      colSums(col) += 1
      idx += 1
    }
    colSums
  }

  override def toString() = {
    //mostly needed b/c the REPL insists on calling toString when you create matrix
    getClass.getSimpleName + " with nRows = " + nRows + ", nCols = " + nCols + ", nnz = " + nnz
  }

}