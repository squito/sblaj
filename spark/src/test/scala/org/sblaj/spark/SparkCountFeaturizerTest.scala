package org.sblaj.spark

import org.scalatest.{Matchers, BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext
import org.sblaj.MatrixDims
import org.sblaj.featurization.{Murmur64, HashMapDictionaryCache}
import org.apache.log4j.{Logger, Level}

class SparkCountFeaturizerTest extends FunSuite with Matchers with BeforeAndAfter {

  var sc : SparkContext = null
  before {
    SparkCountFeaturizerTest.silenceSparkLogging
    sc = new SparkContext("local[4]", "featurization test")
  }

  after {
    sc.stop()
    sc = null
    System.clearProperty("spark.driver.port")
  }

  test("featurization") { SparkCountFeaturizerTest.withTestLock{
    println("beginning featurization in SparkFeaturizer")
    val matrixRDD = SparkCountFeaturizerTest.makeSimpleCountMatrixRDD(sc)

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
        val idx = java.util.Arrays.binarySearch(row.colIds, Murmur64.hash64(id.toString))
        idx should be >= (0)
        row.cnts(idx) should be (1)

        if (id % 2 == 0) {
          val idx2 = java.util.Arrays.binarySearch(row.colIds, Murmur64.hash64("wakka"))
          idx2 should be >= (0)
          row.cnts(idx2) should be (2)
        }
        if (id % 5 == 0) {
          val idx3 = java.util.Arrays.binarySearch(row.colIds, Murmur64.hash64("ooga booga"))
          idx3 should be >= (0)
          row.cnts(idx3) should be (5)
        }
        if (id % 13 == 0) {
          val idx4 = java.util.Arrays.binarySearch(row.colIds, Murmur64.hash64("foobar"))
          idx4 should be >= (0)
          row.cnts(idx4) should be (13)
        }
    }

  }}

  test ("multi-featurize") { SparkCountFeaturizerTest.withTestLock{

    val m1 = SparkCountFeaturizerTest.makeSimpleCountMatrixRDD(sc)
    val m2 = SparkCountFeaturizerTest.makeSimpleCountMatrixRDD(sc)

    println(m1.dims.totalDims)
    println(m2.dims.totalDims)
  }}


  test ("getHistogramCutoff") {
    val histo = Array((20,1),(19,4), (10, 100), (9,500))
    SparkCountFeaturizer.getHistogramCutoff(histo, 1) should be (19)
    SparkCountFeaturizer.getHistogramCutoff(histo, 2) should be (19)
    SparkCountFeaturizer.getHistogramCutoff(histo, 4) should be (19)
    SparkCountFeaturizer.getHistogramCutoff(histo, 5) should be (10)
    SparkCountFeaturizer.getHistogramCutoff(histo, 104) should be (10)
    SparkCountFeaturizer.getHistogramCutoff(histo, 105) should be (9)
    SparkCountFeaturizer.getHistogramCutoff(histo, 605) should be (8)
    SparkCountFeaturizer.getHistogramCutoff(histo, 700) should be (8)
  }

}

object SparkCountFeaturizerTest {

  val testLock = new Object()
  def withTestLock(body:  => Unit) {
    testLock.synchronized(body)
  }

  def makeSimpleCountMatrixRDD(sc: SparkContext) : LongRowSparseCountVectorRDD[String] = {
    val origData = sc.parallelize(1 to 1e3.toInt, 20)
    val matrixRDD = SparkCountFeaturizer.accumulatorRowPerRecord(origData, sc) {_.toLong} { i =>
      var l = List[(String, Int)]()
      l +:=  i.toString -> 1
      if (i % 2 == 0)
        l +:= "wakka" -> 2
      if (i % 5 == 0)
        l +:= "ooga booga" -> 5
      if (i % 13 == 0)
        l +:= "foobar" -> 13
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
