package org.sblaj.spark

import org.scalatest.{Matchers, BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext

/**
*
*/

class RowSparseVectorRDDTest extends FunSuite  with Matchers with BeforeAndAfter {

  var sc : SparkContext = null

  before {
    SparkBinaryFeaturizerTest.silenceSparkLogging
    sc = new SparkContext("local[4]", "row sparse vector rdd test")
  }

  after {
    sc.stop
    sc = null
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  test("enumerated") {
    SparkBinaryFeaturizerTest.withTestLock{
      println("beginnging enumerated in RowSparseVector")
      val matrixRdd = SparkBinaryFeaturizerTest.makeSimpleBinaryMatrixRDD(sc)
      val enumerated = matrixRdd.toEnumeratedVectorRDD(sc, org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
      enumerated.colDictionary should be (matrixRdd.colDictionary)
      enumerated.matrixDims should be (matrixRdd.matrixDims)
      val enumeration = enumerated.colEnumeration
      val nFeatures = matrixRdd.matrixDims.totalDims.nCols.toInt
      //check enumerated ids are in valid range, and are all distinct
      val enumeratedIds = enumerated.colDictionary.map{ case (_,longId) =>
        val enumId = enumeration.getEnumeratedId(longId)
        enumId should be ('defined)
        enumId.get should be  < (nFeatures)
        enumId.get should be >= (0)
        enumId.get
      }.toSet
      enumeratedIds.size should be (nFeatures)
      println("finishing enumerated")
      sc.stop
      println("finished enumerated")
    }
  }

  test("subset columns") {
    val matrixRdd = SparkBinaryFeaturizerTest.makeSimpleBinaryMatrixRDD(sc)
    val enumerated = matrixRdd.toEnumeratedVectorRDD(sc, org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
    val subset = enumerated.subsetColumnsByFeature(sc){
      name => name.equals("wakka") || name.equals("ooga booga") || name.equals("foobar")
    }
    //TODO get handle on RDD w/out casting
    val rdd = subset.asInstanceOf[EnumeratedSparseBinaryVectorRDD[String]].vectorRDD
    rdd.count() should be (1000)
    rdd.map{v => v.nnz}.reduce{_ + _} should be (776)

    val dims = subset.dims.totalDims
    dims.nRows should be (matrixRdd.matrixDims.totalDims.nRows)
    dims.nCols should be (3)
    dims.nnz should be (1000 / 2 + 1000 / 5 + 1000 / 13)
  }

}
