package org.sblaj

import org.scalatest.{Matchers, FunSuite}

class MixedRowMatrixTest extends FunSuite with Matchers {

  test("foreach access") {

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
      (0 until nDense).foreach{denseIdx =>
        denseData(rowIdx * nDense + denseIdx) = (rowIdx * nDense + denseIdx) * 0.5f
      }

      //now add (rowIdx) sparseValues to this row
      val rowStartPos = rowStartIdx(rowIdx)
      (0 until (rowIdx + 1)).foreach{sparseColId =>
        sparseIds(rowStartPos + sparseColId) = sparseColId + nDense
        sparseVals(rowStartPos + sparseColId) = sparseColId + 0.1f
      }
      rowStartIdx(rowIdx + 1) = rowStartPos + rowIdx + 1
    }
    matrix.setSize(4, rowStartIdx(nRows))

    var rowIdx = 0
    matrix.foreach{v =>
      (0 until nDense).foreach { denseIdx =>
        v.get(denseIdx) should be ((rowIdx * nDense + denseIdx) * 0.5f)
      }

      (0 until nRows).foreach {sparseIdx =>
        val sparseVal = v.get(sparseIdx + nDense)
        withClue(s"row = $rowIdx, col = $sparseIdx, sparseVal = $sparseVal\n" +
          s"sparseIds = ${sparseIds.mkString(",")}\n" +
          s"sparseVals = ${sparseVals.mkString(",")}\n" +
          s"rowStartIdx = ${rowStartIdx.mkString(",")}"){
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
}
