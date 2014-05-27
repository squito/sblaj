package org.sblaj

class SubsetMixedRowMatrix(val rows: Array[Int], val parent: StdMixedRowMatrix) extends MixedRowMatrix {
  def nRows = rows.length
  def nCols = parent.nCols

  def foreach[T](f: MixedVector => T) {
    val vector = parent.getVector
    (0 until rows.length).foreach{rowSubsetIdx => //remember, foreach is only fast on a Range (not fast on an Array)
      val row = rows(rowSubsetIdx)
      parent.setRowVector(vector, row)
      f(vector)
    }
  }

  def rowFilter(f: MixedVector => Boolean): Array[Int] = {
    ???
  }

  /**
   * As of right now, rowIdxs reference numbering in THIS matrix.  eg., if the first row in this subset is row 50 of
   * the parent matrix, then if you pass in idx 0, you get row 50 of the original matrix
   *
   *
   * @param rowIdxs
   * @return
   */
  def rowSubset(rowIdxs: Array[Int]): SubsetMixedRowMatrix = {
    StdMixedRowMatrix.checkRowSubsetIdxs(rowIdxs, nRows)
    val originalIdx = rowIdxs.map{subIdx => rows(subIdx)}
    new SubsetMixedRowMatrix(originalIdx, parent)
  }

  def rowSubset(f: MixedVector => Boolean): SubsetMixedRowMatrix = {
    rowSubset(rowFilter(f))
  }

  def getColSums: Array[Float] = {
    val cs = new Array[Float](nCols)
    (0 until rows.length).foreach{rowSubIdx =>
        val rowIdx = rows(rowSubIdx)
        val rowOffset = rowIdx * parent.nDenseCols
        (0 until parent.nDenseCols).foreach{colIdx =>
          try {
            cs(colIdx) += parent.denseCols(rowOffset + colIdx)
          } catch {
            case ex: Throwable =>
              println("exception on (row, col) = " + (rowIdx, colIdx))
              println("exception on (rowOffset) = " + rowOffset)
              println("exception on (denseCols.size, nDenseCols) = " + (parent.denseCols.size, parent.nDenseCols))
              throw ex
          }
        }
    }

    (0 until rows.length).foreach{rowSubIdx =>
      val rowIdx = rows(rowSubIdx)
      (parent.sparseRowStartIdx(rowIdx) until parent.sparseRowStartIdx(rowIdx + 1)).foreach{sparseIdx =>
        val colId = parent.sparseColIds(sparseIdx)
        cs(colId) += parent.sparseColVals(sparseIdx)
      }
    }
    cs

  }

  def getDenseSumSq: Array[Float] = {
    val ss = new Array[Float](parent.nDenseCols)
    (0 until rows.length).foreach{rowSubIdx =>
      val rowIdx = rows(rowSubIdx)
      val rowOffset = rowIdx * parent.nDenseCols
      (0 until parent.nDenseCols).foreach{colIdx =>
        try {
          val p = parent.denseCols(rowOffset + colIdx)
          val p2 = p * p
          ss(colIdx) += p2
        } catch {
          case ex: Throwable =>
            println("exception on (row, col) = " + (rowIdx, colIdx))
            println("exception on (rowOffset) = " + rowOffset)
            println("exception on (denseCols.size, nDenseCols) = " + (parent.denseCols.size, parent.nDenseCols))
            throw ex
        }
      }
    }
    ss
  }

  def getColumn(colIdx: Int, into: Array[Float], pos: Int) {
    if (colIdx > parent.nDenseCols) {
      (0 until rows.length).foreach{rowSubIdx =>
        val rowIdx = rows(rowSubIdx)
        val rowOffset = rowIdx * parent.nDenseCols
        into(rowSubIdx) = parent.denseCols(rowOffset + colIdx)
      }
    } else {
      (0 until rows.length).foreach{rowSubIdx =>
        val rowIdx = rows(rowSubIdx)
        (parent.sparseRowStartIdx(rowIdx) until parent.sparseRowStartIdx(rowIdx + 1)).foreach{sparseIdx =>
          val colId = parent.sparseColIds(sparseIdx)
          if (colId == colIdx) {
            into(rowSubIdx + pos) = parent.sparseColVals(sparseIdx)
          }
        }
      }
    }

  }

  def getVector: MixedVector = {
    parent.getVector
  }

  def setRowVector(vector: MixedVector, rowIdx: Int) {
    parent.setRowVector(vector, rows(rowIdx))
  }

  def sizeString = s"$nRows x (${parent.nDenseCols} , ${parent.nSparseCols}).  parentSize = ${parent.sizeString}"

}
