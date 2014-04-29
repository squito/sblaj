package org.sblaj.featurization

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

/**
 *
 */

class BinaryFeaturizerTest extends FunSuite with ShouldMatchers {


  test("murmur64") {
    val strings = List("oogabooga", "abcdefg", "a;lkdjfapdf", "", "a", "b", "ba", "ab", "AB")
    val codes = strings.flatMap{ s =>
      val hash64 = Murmur64.hash64(s)
      val low = hash64.asInstanceOf[Int]
      val high = (hash64 >> 32).asInstanceOf[Int]
      low should not be (high)
      high should not be (-1) //this will happen if you don't mask the bits properly
      List(low, high)
    }.toSet

    // if its any good, all codes should be distinct, across both low & high bits
    codes.size should be (2 * strings.size)
  }
}
