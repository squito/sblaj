package org.sblaj

import org.scalatest.{Matchers, FunSuite}

class StdMixedRowMatrixTest extends FunSuite with Matchers {

  def genMatrix: StdMixedRowMatrix = {
    val nRows = 4
    val nDense = 5
    val denseData = new Array[Float](nRows * nDense)
    val sparseIds = new Array[Int](nRows * nRows)  //extra space
    val sparseVals = new Array[Float](nRows * nRows)
    val rowStartIdx = new Array[Int](nRows + 1)
    val matrix = new StdMixedRowMatrix(
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

    var rowIdx = 0
    matrix.foreach{v =>
      validateRow(matrix, v, rowIdx)
      rowIdx += 1
    }
    rowIdx should be (matrix.nRows)

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

  test("rowSubset") {
    val matrix = genMatrix

    val subset = matrix.rowSubset(Array(1,3))
    subset.nRows should be (2)
    validateSubset(subset)

    val subsub = subset.rowSubset(Array(1))
    subsub.nRows should be (1)
    subsub.rows should be (Array(3))
    validateSubset(subsub)


    val emptySubset = matrix.rowSubset(Array())
    emptySubset.nRows should be (0)
    validateSubset(emptySubset)

    val completeSubset = matrix.rowSubset(Array(0,1,2,3))
    completeSubset.nRows should be (4)
    validateSubset(completeSubset)

    val badRowIdExc = the [Exception] thrownBy (matrix.rowSubset(Array(1,3,4)))
    badRowIdExc.getMessage should include ("tried to subset with out of range row id (4)")


    val badRowIdExc2 = the [Exception] thrownBy (matrix.rowSubset(Array(-1,1,3)))
    badRowIdExc2.getMessage should include ("tried to subset with out of range row id (-1)")

    //or ... should this be OK?
    val nonSortedExc = the [Exception] thrownBy(matrix.rowSubset(Array(2,1)))
    nonSortedExc.getMessage should include ("subset row idxs must be sorted (unsorted at idx 1)")


    val subsubExc = the [Exception] thrownBy(subset.rowSubset(Array(1,3)))
    subsubExc.getMessage should include ("tried to subset with out of range row id (3)")
  }

  test("col sums") {
    pending //std & subset
  }

  test("sparseMatrix") {
    pending
  }

  test("sparseMatrix w/ rowSubset") {
    pending
  }

  /** test that a row matches what we think we've created w/ genMatrix */
  def validateRow(matrix: StdMixedRowMatrix, v: MixedVector, rowIdx: Int) {
    (0 until matrix.nDenseCols).foreach { denseIdx =>
      v.get(denseIdx) should be ((rowIdx * matrix.nDenseCols + denseIdx + 1) * 0.5f)
    }

    (0 until matrix.nRows).foreach {sparseIdx =>
      val sparseVal = v.get(sparseIdx + matrix.nDenseCols)
      withClue(s"row = $rowIdx, col = $sparseIdx, sparseVal = $sparseVal\n" +
        s"sparseIds = ${matrix.sparseColIds.mkString(",")}\n" +
        s"sparseVals = ${matrix.sparseColVals.mkString(",")}\n" +
        s"rowStartIdx = ${matrix.sparseRowStartIdx.mkString(",")}"){
        if (sparseIdx <= rowIdx)
          sparseVal should be (sparseIdx + 0.1f)
        else
          sparseVal should be (0f)
      }
    }
  }

  def validateSubset(subset: SubsetMixedRowMatrix) {
    subset.nRows should be (subset.rows.length)
    var subsetRowIdx = 0
    subset.foreach{v =>
      val rowIdx = subset.rows(subsetRowIdx)
      validateRow(subset.parent, v, rowIdx)
      subsetRowIdx += 1
    }
    subsetRowIdx should be (subset.nRows)
  }
}
