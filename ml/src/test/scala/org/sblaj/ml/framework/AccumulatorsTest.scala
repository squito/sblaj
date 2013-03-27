package org.sblaj.ml.framework

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

/**
 *
 */

class AccumulatorsTest extends FunSuite with ShouldMatchers {
  test("DenseFloatAccumulators") {
    val a = new DenseFloatArrayAccumulator(5)
    a.length should be (5)
    a.arr.length should be (5)
    a.arr(2) = 6.9f
    a.arr(3) = 2.1f
    val b = a.zero
    a.arr(2) should be (6.9f)
    b.arr(2) should be (0.0f)

    b.arr(2) = 1.1f
    b.arr(4) = 3.8f

    a ++= b
    a.arr(0) should be (0.0f)
    a.arr(1) should be (0.0f)
    a.arr(2) should be (8.0f)
    a.arr(3) should be (2.1f)
    a.arr(4) should be (3.8f)
  }

  test("Dense2DFloatAccumulators") {
    val a = new Dense2DFloatArrayAccumulator(5,4)
    a.nRow should be (5)
    a.nCol should be (4)
    a.arr.length should be (5)
    a.arr.foreach{_ .length should be (4)}
    def fill(arr: Array[Array[Float]])(rc: (Int,Int) => Float) {
      (0 until 5).foreach{row =>
        (0 until 4).foreach{col =>
          arr(row)(col) = rc(row,col)
        }
      }
    }
    val b = a.zero
    fill(a.arr){(r:Int,c:Int) => r * 2.5f}
    fill(b.arr){(r:Int,c:Int) => c * 0.8f}
    a ++= b
    def check(arr: Array[Array[Float]])(rc:(Int,Int, Float) => Boolean) {
      (0 until 5).foreach{row =>
        (0 until 4).foreach{col =>
          if (! rc(row, col, arr(row)(col))) fail("failed check on " + row + "," + col)
        }
      }
    }
    check(a.arr){(r:Int,c:Int, v: Float) => v == r * 2.5f + c * 0.8f}
    check(b.arr){(r:Int, c:Int, v: Float) => v == c * 0.8f}
  }
}
