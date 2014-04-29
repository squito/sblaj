package org.sblaj

import org.scalatest.{Matchers, FunSuite}
import org.scalatest.matchers.{MatchResult,Matcher}
import ArrayUtils._

class ArrayUtilsTest extends FunSuite with Matchers {

  val beNan = new Matcher[Float] {
    def apply(v: Float) = {
      MatchResult(Float.NaN.equals(v), v + " was supposed to be NaN", "b")
    }
  }

  test("stableLogNormalize") {
    val scaledLogProbs = Array[Float](0, -3, -3, -1, -5e6f, -5e6f)
    val scaledProbs = scaledLogProbs.map(math.exp(_).asInstanceOf[Float])
    val scaledSum = scaledProbs.sum
    val scaledNorm = scaledProbs.map(_ / scaledSum)


    var oneHardTest = false
    for (offset <- List(0f, -10f, -5e4f)) {
      val logProbs = scaledLogProbs.map(_ + offset)

      //check our test is reasonably hard -- naive normalization should fail
      val exp = logProbs.map(math.exp(_).asInstanceOf[Float])
      val sum = exp.sum.asInstanceOf[Float]
      val naiveNorm = exp.map(_ / sum)
      if (sum == 0 && beNan.apply(naiveNorm(0)).matches)
        oneHardTest = true

      //check normalized, and check we get the right normalization constant
      val normalizingFactor = stableLogNormalize(logProbs, 0, logProbs.length)

      var idx = 0
      while (idx < scaledNorm.length) {
        logProbs(idx) should be(scaledNorm(idx) +- 0.0001f)
        idx += 1
      }
    }
    oneHardTest should be (true)
  }

  test("+=") {
    val eps = 0.00001f

    val into = Array[Float](0.1f, 0.2f, 0.3f)
    val add = Array[Float](0.5f, 0.3f, 0.1f)
    +=(add, into)
    into(0) should be (0.6f +- eps)
    into(1) should be (0.5f +- eps)
    into(2) should be (0.4f +- eps)


    +=(add, 3, into)
    into(0) should be (2.1f +- eps)
    into(1) should be (1.4f +- eps)
    into(2) should be (0.7f +- eps)
  }
}
