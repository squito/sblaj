package org.sblaj.spark

import org.scalatest.{Matchers, BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext
import org.sblaj.MatrixDims
import org.sblaj.featurization.{Murmur64, HashMapDictionaryCache}
import org.apache.log4j.{Logger, Level}

class SparkBinaryFeaturizerTest extends FunSuite with Matchers with BeforeAndAfter {

  var sc : SparkContext = null
  before {
    SparkBinaryFeaturizerTest.silenceSparkLogging
    sc = new SparkContext("local[4]", "featurization test")
  }

  after {
    sc.stop()
    sc = null
    System.clearProperty("spark.driver.port")
  }

  test("featurization") { SparkBinaryFeaturizerTest.withTestLock{
    println("beginning featurization in SparkFeaturizer")
    val matrixRDD = SparkBinaryFeaturizerTest.makeSimpleBinaryMatrixRDD(sc)

    //check dims
//    println(matrixRDD.dims.totalDims)
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

  }}

  test ("multi-featurize") { SparkBinaryFeaturizerTest.withTestLock{

    val m1 = SparkBinaryFeaturizerTest.makeSimpleBinaryMatrixRDD(sc)
    val m2 = SparkBinaryFeaturizerTest.makeSimpleBinaryMatrixRDD(sc)

    println(m1.dims.totalDims)
    println(m2.dims.totalDims)
  }}


  test ("getHistogramCutoff") {
    val histo = Array((20,1),(19,4), (10, 100), (9,500))
    SparkBinaryFeaturizer.getHistogramCutoff(histo, 1) should be (19)
    SparkBinaryFeaturizer.getHistogramCutoff(histo, 2) should be (19)
    SparkBinaryFeaturizer.getHistogramCutoff(histo, 4) should be (19)
    SparkBinaryFeaturizer.getHistogramCutoff(histo, 5) should be (10)
    SparkBinaryFeaturizer.getHistogramCutoff(histo, 104) should be (10)
    SparkBinaryFeaturizer.getHistogramCutoff(histo, 105) should be (9)
    SparkBinaryFeaturizer.getHistogramCutoff(histo, 605) should be (8)
    SparkBinaryFeaturizer.getHistogramCutoff(histo, 700) should be (8)
  }

}

object SparkBinaryFeaturizerTest {

  val testLock = new Object()
  def withTestLock(body:  => Unit) {
    testLock.synchronized(body)
  }

  def makeSimpleBinaryMatrixRDD(sc: SparkContext) : LongRowSparseBinaryVectorRDD[String] = {
    val origData = sc.parallelize(1 to 1e3.toInt, 20)
    val matrixRDD = SparkBinaryFeaturizer.accumulatorRowPerRecord(origData, sc) {_.toLong} { i =>
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
    matrixRDD
  }

  def silenceSparkLogging {
    Seq("spark", "org.eclipse.jetty", "akka").map{
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel()
        logger.setLevel(Level.WARN)
        loggerName -> prevLevel
    }
  }
}
