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

  test("featurization") {
    //generate some random strings
    val nRows = 100
    val rawData = (0 until nRows).map { idx =>
      List("abc", "xyz")
    }
    val featurizer = new MurmurFeaturizer[List[String]] {
      def getId(t: List[String]) = Murmur64.hash64(t.head)
      def extractor(l: List[String]) = l.toSet
    }

    val featurized = FeaturizerHelper.mapFeaturize(rawData, featurizer)

    val binMat = featurized.toSparseBinaryRowMatrix()
    //check basic contract
    binMat.nRows should be (nRows)
    binMat.nCols should be (2)
    binMat.nnz should be (200)
    binMat.foreach {vector =>
      vector.nnz should be (2)
      vector.get(0) should be (1)
      vector.get(1) should be (1)
    }
    //also check details of underlying data structure
    val expColIds = (0 until nRows).flatMap{idx => List(0,1)}.toArray
    binMat.colIds should be (expColIds)
  }

}
