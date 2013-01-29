package org.sblaj.spark

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import spark.SparkContext
import org.sblaj.MatrixDims
import org.sblaj.featurization.{Murmur64, HashMapDictionaryCache}

class SparkFeaturizerTest extends FunSuite with ShouldMatchers {

  test("featurization") {
    val sc = new SparkContext("local[4]", "featurization test")
    try {
      val origData = sc.parallelize(1 to 1e3.toInt, 20)
      val matrixRDD = SparkFeaturizer.rowPerRecord(origData, sc) {_.toLong} { i =>
        var l = List[String]()
        l +:=  i.toString
        if (i % 2 == 0)
          l +:= "wakka"
        if (i % 5 == 0)
          l +:= "ooga booga"
        if (i % 13 == 0)
          l +:= "foobar"
        l
      }

      //check dims
      matrixRDD.dims.totalDims should be (MatrixDims(1000, 1003, 1000 + 1000 / 2 + 1000 / 5 + 1000 / 13))
      matrixRDD.dims.partitionDims.values.foreach { case(nRows, nnz) =>
        nRows should be (1000 / 20)
        nnz should (be (88) or be (89))
      }

      //check dictionary
      val colDictionary = matrixRDD.colDictionary.asInstanceOf[HashMapDictionaryCache[String]]
      colDictionary should have size (1003)
      (1 to 1e3.toInt).foreach{i => colDictionary.contains(i.toString)}

      //check data
      val vectors = matrixRDD.vectorRdd.collect
      vectors.size should be (1e3.toInt)
      vectors.foreach {
        row =>
          val id = row.rowId
          java.util.Arrays.binarySearch(row.colIds, Murmur64.hash64(id.toString)) should be >= (0)
      }
    }
    finally {
      sc.stop()
      System.clearProperty("spark.master.port")
    }
  }

}
