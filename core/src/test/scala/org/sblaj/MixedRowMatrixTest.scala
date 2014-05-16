package org.sblaj

import org.scalatest.{Matchers, FunSuite}

class MixedRowMatrixTest extends FunSuite with Matchers {

  def genMatrix: MixedRowMatrix = {
    val nRows = 4
    val nDense = 5
    val denseData = new Array[Float](nRows * nDense)
    val sparseIds = new Array[Int](nRows * nRows)  //extra space
    val sparseVals = new Array[Float](nRows * nRows)
    val rowStartIdx = new Array[Int](nRows + 1)
    val matrix = new MixedRowMatrix(
      nDenseCols = nDense,
      nSparseCols = nRows,
      denseCols = denseData,
      sparseColIds = sparseIds,
      sparseColVals = sparseVals,
      sparseRowStartIdx = rowStartIdx,
      maxNnz = nRows * nRows,
      maxRows = nRows
    )

    (0 until nRows).foreach{rowIdx =>
      //fill in all the dense values
      (0 until nDense).foreach{denseIdx =>
        denseData(rowIdx * nDense + denseIdx) = (rowIdx * nDense + denseIdx + 1) * 0.5f
      }

      //now add (rowIdx + 1) sparseValues to this row (eg., row 0 gets 1 sparse val, row 1 gets 2, row 2 gets 3, ...
      val rowStartPos = rowStartIdx(rowIdx)
      (0 until (rowIdx + 1)).foreach{sparseColId =>
        sparseIds(rowStartPos + sparseColId) = sparseColId + nDense
        sparseVals(rowStartPos + sparseColId) = sparseColId + 0.1f
      }
      rowStartIdx(rowIdx + 1) = rowStartPos + rowIdx + 1
    }
    matrix.setSize(4, rowStartIdx(nRows))


    matrix

  }

  test("foreach access") {
    val matrix = genMatrix

    import matrix._ //Because I'm lazy! this is important if you're looking at this code, its where all the vals live


    var rowIdx = 0
    matrix.foreach{v =>
      (0 until nDenseCols).foreach { denseIdx =>
        v.get(denseIdx) should be ((rowIdx * nDenseCols + denseIdx + 1) * 0.5f)
      }

      (0 until nRows).foreach {sparseIdx =>
        val sparseVal = v.get(sparseIdx + nDenseCols)
        withClue(s"row = $rowIdx, col = $sparseIdx, sparseVal = $sparseVal\n" +
          s"sparseIds = ${sparseColIds.mkString(",")}\n" +
          s"sparseVals = ${sparseColVals.mkString(",")}\n" +
          s"rowStartIdx = ${sparseRowStartIdx.mkString(",")}"){
          if (sparseIdx <= rowIdx)
            sparseVal should be (sparseIdx + 0.1f)
          else
            sparseVal should be (0f)
        }
      }
      rowIdx += 1
    }
    rowIdx should be (nRows)

  }

  test("rowFilter") {
    val matrix = genMatrix

    matrix.rowFilter{v =>
      v.get(1) > 1.5
    } should be (Array(1,2,3))


    matrix.rowFilter{v =>
      v.get(0) > 0
    } should be (Array(0,1,2,3))

    matrix.rowFilter{v =>
      v.get(20) > 1.0
    } should be (Array())

    matrix.rowFilter{v =>
      v.get(7) > 0.1
    } should be (Array(2,3))
  }
}
