package org.sblaj

import collection._
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

  def cooccurrenceCountsMap(topN: Int): (Array[Int], Map[Int,Map[Int, Int]]) = {
    val colSums = getColSums()
    val cols = colSums.zipWithIndex.sortBy{- _._1}.slice(0,topN).map{_._2}.sorted
    (cols,cooccurrenceCountsMap(cols))
  }

  def cooccurrenceCounts(topN: Int): (Array[Int], Array[Int]) = {
    val colSums = getColSums()
    val cols = colSums.zipWithIndex.sortBy{- _._1}.slice(0,topN).map{_._2}.sorted
    (cols,cooccurrenceCounts(cols))
  }

  def cooccurrenceCountsMap(cols: Array[Int]): Map[Int,Map[Int, Int]] = {
    val arr = cooccurrenceCounts(cols)
    //map it back to original id space, so its easier to work with
    ArrayUtils.matrixAsMap(cols, arr)
  }

  def cooccurrenceCounts(cols:Array[Int]): Array[Int] = {
    val n = cols.length
    val revMapping = new Array[Int](nCols)
    (0 until nCols).foreach{idx => revMapping(idx) = -1}
    (0 until cols.length).foreach{idx => revMapping(cols(idx)) = idx}
    val result = new Array[Int](n * n)
    (0 until nRows).foreach{rowNum =>
      var outerIdx = rowStartIdx(rowNum)
      val rowEndIdx = rowStartIdx(rowNum + 1)
      var outerColIdx = 0
      while( outerIdx < rowEndIdx && outerColIdx < cols.length) {
        var outerFound = false
        // find the next column in this vector that is in the target set of columns
        while( !outerFound && outerIdx < rowEndIdx && outerColIdx < cols.length) {
          //take advantage of the fact that both arrays are sorted
          if (colIds(outerIdx) == cols(outerColIdx)) {
            outerFound = true
          }
          else if (colIds(outerIdx) < cols(outerColIdx))
            outerIdx += 1
          else
            outerColIdx += 1
        }
        if (outerFound) {
          val c1 = revMapping(colIds(outerIdx))
          result(c1 * n + c1) += 1  //this feature cooccurs w/ itself
          //pair this column w/ all other columns in this vector
          var innerIdx = outerIdx + 1
          var innerColIdx = outerColIdx + 1
          while (innerIdx < rowEndIdx && innerColIdx < cols.length) {
            var found = false
            //find the next column in the target set
            while (!found && innerIdx < rowEndIdx && innerColIdx < cols.length) {
              if (colIds(innerIdx) == cols(innerColIdx)) {
                //finally, we've got two cols that co-occur in this row, both of which are in the target set
                // colIds(outerIdx) and colids(innerIdx)
                val c2 = revMapping(colIds(innerIdx))
                result(c1 * n + c2) += 1
                innerIdx += 1
                innerColIdx += 1
              } else if (colIds(innerIdx) < cols(innerColIdx)) {
                innerIdx += 1
              } else {
                innerColIdx += 1
              }
            }
          }
          outerIdx += 1
          outerColIdx += 1
        }
      }
    }
    //copy from upper diagonal into lower diagonal
    (1 until n).foreach{ rowIdx =>
      (rowIdx until n).foreach{colIdx =>
        result(colIdx * n + rowIdx) = result(rowIdx * n + colIdx)
      }
    }
    result
  }

  def getRow(row: Int, into: BaseSparseBinaryVector) {
    into.reset(colIds, rowStartIdx(row), rowStartIdx(row + 1))
  }

  def getRow(row: Int) = {
    new BaseSparseBinaryVector(colIds, rowStartIdx(row), rowStartIdx(row + 1))
  }

}
