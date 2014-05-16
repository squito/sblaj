package org.sblaj

/**
 *
 */
class MixedRowMatrix(
  val nDenseCols: Int,
  val nSparseCols: Int,
  val maxNnz: Int,
  val maxRows: Int,
  val denseCols: Array[Float],
  val sparseColIds: Array[Int],
  val sparseColVals: Array[Float],
  val sparseRowStartIdx: Array[Int]
) extends Traversable[MixedVector] {
  
  
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


  def foreach[T](f: MixedVector => T) {
    val vector = new MixedVector(
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
    var row = 0
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
  }
}
