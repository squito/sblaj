package org.sblaj

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class BaseSparseBinaryVectorTest extends FlatSpec with ShouldMatchers {

  def fixture() = {
    new {
      val theta = new Array[Array[Float]](50)
      for (i <- 0 until 50) {
        val arr = new Array[Float](3)
        for (j <- 0 until 3) {
          arr(j) = i + j
        }
        theta(i) = arr
      }

      val v = new BaseSparseBinaryVector(colIds = Array[Int](0,1,2,0,5,8,3,4), startIdx = 3, endIdx = 6)
    }
  }

  "dot" should "compute correct answer" in {
    val f = fixture()
    val denseVector = (1 to 10).map(_.toFloat / 10).toArray
    val result = f.v.dot(denseVector)
    result.toDouble should be ( (0.1 + 0.6 + 0.9) plusOrMinus 0.00001)
  }

  "tMultC" should "compute correct answer" in {
    val f = fixture()
    val result = new Array[Float](3)
    f.v.tMultC(f.theta, result)
    for (j <- 0 until 3) {
      val v = result(j).toDouble
      v should be ( (5.0 + 8) + (3 * j) plusOrMinus 0.000001)
    }
  }
}