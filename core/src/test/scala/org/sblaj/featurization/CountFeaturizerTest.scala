package org.sblaj.featurization

import org.scalatest.{ShouldMatchers, FunSuite}
import org.sblaj.SparseCountVector

/**
 * Created by matt on 4/28/14.
 */

class CountFeaturizerTest extends FunSuite with ShouldMatchers {


  test("featurization") {
    //generate some random strings
    val nRows = 20
    val rawData = (0 until nRows).map {
      idx =>
        List(
          "abc", "abc", "abc", "abc", "abc",
          "def", "def", "def",
          "xyz", "xyz", "xyz", "xyz", "xyz", "xyz", "xyz")
    }

    val featurizer = new MurmurCountFeaturizer[List[String]] {
        def getId(t: List[String]) = Murmur64.hash64(t.head)

      def extractor(l: List[String]) = l.groupBy(s => s).map {
        case (k, vs) => (k, vs.size)
      }
    }

    println("mapFeaturize\n\n\n\n")
    val featurized = CountFeaturizerHelper.mapFeaturize(rawData, featurizer)

    println("toSparseCountRowMatrix\n\n\n\n")
    val binMat = featurized.toSparseCountRowMatrix()

    println("check basic contract\n\n\n\n")

    //check basic contract
    binMat.nRows should be(nRows)
    binMat.nCols should be(3)
    binMat.nnz should be(nRows * 3)

    val enumz = featurized.dictionary.getEnumeration()


    binMat.foreach {
      vector: SparseCountVector =>
        vector.nnz should be(3)
        vector.get(0) should be(1)
        vector.get(1) should be(1)
    }
    //also check details of underlying data structure
    val expColIds = (0 until nRows).flatMap {
      idx => List(0, 1)
    }.toArray
    binMat.colIds should be(expColIds)
  }

}
