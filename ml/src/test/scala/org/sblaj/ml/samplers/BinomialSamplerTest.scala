package org.sblaj.ml.samplers

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

/**
 *
 */

class BinomialSamplerTest extends FunSuite with ShouldMatchers {
  test("genPowerLawRates") {
    val into = new Array[Float](100)
    List( (0.5f, 0.1f, 0, 100), (0.5f, 1e-5f, 0, 100), (0.1f, 1e-3f, 50, 20)).map {
      case (high, low, start, n) =>
      BinomialSampler.genPowerLawRates(high, low, into, start, n)
      into(start) should be (high)
      into(start + n - 1) should be (low)
      //half way point should be geometric mean
      val exp = math.sqrt(high * low).asInstanceOf[Float]
      into(start + n / 2) should be (exp plusOrMinus exp * 0.15f)   // huge slop, oh well
    }
  }
}
