package org.sblaj

class StdMixedRowMatrix(
  val nDenseCols: Int,
  val nSparseCols: Int,
  val maxNnz: Int,
  val maxRows: Int,
  val denseCols: Array[Float],
  val sparseColIds: Array[Int],
  val sparseColVals: Array[Float],
  val sparseRowStartIdx: Array[Int]
) extends MixedRowMatrix {
  
  
  def this(
    nDenseCols: Int,
    nSparseCols: Int,
    maxNnz: Int,
    maxRows: Int
  ) {
    this(
      nDenseCols,
      nSparseCols,
      maxNnz,
      maxRows,
      new Array[Float](nDenseCols * maxRows),
      new Array[Int](maxNnz),
      new Array[Float](maxNnz),
      new Array[Int](maxRows + 1)
    )
  }

  var nRows: Int = 0
  var nnz: Int = 0
  def nCols = nDenseCols + nSparseCols


  def foreach[T](f: MixedVector => T) {
    var row = 0
    val vector = getVector
    while (row < nRows) {
      val nextRow = row + 1
      vector.resetPosition(
        denseStartIdx = row * nDenseCols,
        denseEndIdx = nextRow * nDenseCols,
        sparseStartIdx = sparseRowStartIdx(row),
        sparseEndIdx = sparseRowStartIdx(nextRow)
      )
      f(vector)
      row = nextRow
    }
  }

  def setSize(nRows: Int, nnz: Int) {
    this.nRows = nRows
    this.nnz = nnz
  }

  def setRowVector(vector: MixedVector, rowIdx: Int) {
    val nextRow = rowIdx + 1
    vector.resetPosition(
      denseStartIdx = rowIdx * nDenseCols,
      denseEndIdx = nextRow * nDenseCols,
      sparseStartIdx = sparseRowStartIdx(rowIdx),
      sparseEndIdx =  sparseRowStartIdx(nextRow)
    )
  }

  def getVector: MixedVector = {
    new MixedVector(
      denseCols = denseCols,
      denseStartIdx = 0,
      denseEndIdx = 0,
      sparseColIds = sparseColIds,
      sparseColVals = sparseColVals,
      sparseStartIdx = 0,
      sparseEndIdx = 0,
      nDenseCols = nDenseCols,
      nSparseCols = nSparseCols
    )
  }

  /**
   * returns the row indexes for the rows which match the predicate
   */
  def rowFilter(predicate: MixedVector => Boolean): Array[Int] = {
    var matchingIdxs = IndexedSeq[Int]()
    var rowIdx = 0
    foreach{v =>
      if (predicate(v)) {
        matchingIdxs :+= rowIdx
      }
      rowIdx += 1
    }
    matchingIdxs.toArray
  }

  def rowSubset(rowIdxs: Array[Int]): SubsetMixedRowMatrix = {
    StdMixedRowMatrix.checkRowSubsetIdxs(rowIdxs, nRows)
    new SubsetMixedRowMatrix(rowIdxs, this)
  }

  def rowSubset(predicate: MixedVector => Boolean): SubsetMixedRowMatrix = {
    rowSubset(rowFilter(predicate))
  }

  def getColSums: Array[Float] = {
    val cs = new Array[Float](nCols)
    (0 until nRows).foreach{rowIdx =>
      val rowOffset = rowIdx * nDenseCols
      (0 until nDenseCols).foreach{colIdx =>
        cs(colIdx) += denseCols(rowOffset + colIdx)
      }
    }

    (0 until nnz).foreach{sparseIdx =>
      val colId = sparseColIds(sparseIdx)
      cs(colId) += sparseColVals(sparseIdx)
    }
    cs
  }

  def getDenseSumSq: Array[Float] = {
    val ss = new Array[Float](nDenseCols)
    (0 until nRows).foreach{rowIdx =>
      val rowOffset = rowIdx * nDenseCols
      (0 until nDenseCols).foreach{colIdx =>
        val p = denseCols(rowOffset + colIdx)
        val p2 = p * p
        ss(colIdx) += p2
      }
    }
    ss
  }

  def sizeString = s"$nRows x ($nDenseCols , $nSparseCols): $nnz.  maxRows = $maxRows, maxNnz = $maxNnz"
}

object StdMixedRowMatrix {
  def checkRowSubsetIdxs(rowIdxs: Array[Int], nRows: Int) {
    (0 until rowIdxs.length).foreach{idx =>
      val v = rowIdxs(idx)
      if (v < 0 || v >= nRows) require(false, s"tried to subset with out of range row id ($v)")
      if (idx > 0 && v < rowIdxs(idx - 1)) require(false, s"subset row idxs must be sorted (unsorted at idx $idx)")
    }
  }
}
