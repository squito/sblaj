package org.sblaj

import org.scalatest.{FlatSpec, Suite}
import org.scalatest.matchers.ShouldMatchers

/**
 *
 */

class SparseBinaryRowMatrixTest extends FlatSpec with ShouldMatchers {

  def fixture() = {
    new {
      val m = new SparseBinaryRowMatrix(columnIds = Array[Int](0,1,2,0,5,8,3,4), rowStartIdxs = Array[Int](0,3,3,6,8), nColumns = 10)
    }
  }

  "Matrix Constructor" should "have correct size fields" in {
    val m = fixture().m
    m.nRows should be (4)
    m.nCols should be (10)
    m.nnz should be (8)
    m.maxRows should be (m.nRows)
    m.maxNnz should be (m.nnz)
  }

  "For each" should "loop over all rows" in {
    val m = fixture().m
    var rowIdx = 0
    m.foreach { v => {
      rowIdx match {
        case 0 => {
          v.nnz should be (3)
        }
        case 1 => {
          v.nnz should be (0)
        }
        case 2 => {
          v.nnz should be (3)
        }
        case 3 => {
          v.nnz should be (2)
        }
        case _ => fail()
      }
      rowIdx += 1
    }}
    rowIdx should be (4)
  }
}
