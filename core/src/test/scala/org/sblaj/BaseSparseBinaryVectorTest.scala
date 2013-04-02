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

  "mult" should "compute correct answer" in {
    val f = fixture()
    val result = new Array[Float](3)
    f.v.mult(f.theta, result)
    for (j <- 0 until 3) {
      val v = result(j).toDouble
      v should be ( (5.0 + 8) + (3 * j) plusOrMinus 0.000001)
    }

    //should add to whatever is already there
    val initial = Array[Float](1.0f, -5.0f, 0.25f)
    Array.copy(initial, 0, result, 0, 3)
    f.v.mult(f.theta, result)
    for (j <- 0 until 3) {
      val v = result(j).toDouble
      v should be ( (5.0 + 8) + (3 * j) + initial(j) plusOrMinus 0.000001)
    }

  }

  "indexAddInto" should "compute correct answer" in {
    val f = fixture()
    val thetaMod = fixture().theta
    val denseVector = (1 to 3).map(_.toFloat / 10).toArray
    f.v.indexAddInto(denseVector, thetaMod)

    for (i <- (0 until thetaMod.length)) {
      for (j <- (0 until thetaMod(i).length)) {
        val incr = if (i == 0 || i == 5 || i == 8) denseVector(j) else 0
        thetaMod(i)(j) should be (f.theta(i)(j) + incr)
      }
    }
  }

  "asCountVector" should "have correct iteration" in {
    val f = fixture()
    val v = f.v
    v.size should be (3)
    v.toList should be (List(0,5,8))
    val cv = v.asCountVector
    cv.foreach{ case MTuple2(id, count) => count should be (1)}
    cv.size should be (v.size)
    v.reset(v.theColIds, 0, 3)
    v.toList should be (List(0,1,2))
    cv.foreach{ case MTuple2(id,count) => count should be (1)}
    cv.map{_._1}.toList should be (v.toList)
    cv.size should be (v.size)

    val v2 = new BaseSparseBinaryVector(colIds = Array[Int](56,3,4,5,19,42,3), startIdx = 2, endIdx = 4)
    v2.toList should be (List(4,5))
    cv.reset(v2)
    cv.foreach{case x@MTuple2(id,count) => count should be (1)}
    cv.map{_._1}.toList should be (v2.toList)
  }
}