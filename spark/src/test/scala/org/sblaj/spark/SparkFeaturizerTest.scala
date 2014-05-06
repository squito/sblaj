package org.sblaj.spark

import org.scalatest.{Matchers, BeforeAndAfter, FunSuite}
import org.apache.spark.SparkContext
import org.sblaj.MatrixDims
import org.sblaj.featurization.{GeneralCompleteDictionary, Murmur64, HashMapDictionaryCache}
import org.apache.log4j.{Logger, Level}
import java.io.File
import org.sblaj.io.EnumVectorIO

class SparkFeaturizerTest extends FunSuite with Matchers with BeforeAndAfter {

  var sc : SparkContext = null
  before {
    SparkFeaturizerTest.silenceSparkLogging
    sc = new SparkContext("local[4]", "featurization test")
  }

  val testDir = new File("test_output/" + getClass.getSimpleName)

  after {
    sc.stop()
    sc = null
    System.clearProperty("spark.driver.port")
  }

  test("featurization") {
    println("beginning featurization in SparkFeaturizer")
    val matrixRDD = SparkFeaturizerTest.makeSimpleMatrixRDD(sc)

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

  }

  test("enum featurization") {

    val origData = sc.parallelize(0 until 1e3.toInt, 20)
    val d = testDir.getAbsolutePath + "/enum_featurization"
    val matrixRDD = SparkFeaturizer.scalableRowPerRecord(
      origData,
      sc,
      rddDir = d + "/init",
      minCount = 1,
      dictionarySampleRate = 1.0
    ) {_.toLong} { i =>
      var l = List[String]()
      l +:=  i.toString
      if (i % 2 == 0)
        l +:= "wakka"
      if (i % 5 == 0)
        l +:= "ooga booga"
      if (i % 13 == 0)
        l +:= "foobar"
//      println(i -> l)
      l
    }

//    val dict = matrixRDD.colEnumeration.asInstanceOf[GeneralCompleteDictionary[String]]
//    println("************* matrix RDD ******************")
//    matrixRDD.vectorRDD.foreach{x =>
//      println(x.colIds.map{id =>
//        val longId = dict.reverseEnum(id)
//        val name = dict.elems.get(longId)
//        (id, longId, name)
//      }.mkString(","))
//    }
//
//
//    println("*************** saving RDD ***************")


    SparkIO.saveEnumeratedSparseVectorRDD(matrixRDD, d + "/final", d + "/final")

    val (dictionary, loaded) = EnumVectorIO.loadLimitedMatrix(new File(d,"final"))
//    println("********* Dictionary ************")
//    dictionary.zipWithIndex.foreach{println}
//    println("******** loaded matrix ***********")
//
    (0 until loaded.nRows).foreach{row =>
      val (s,e) = (loaded.rowStartIdx(row), loaded.rowStartIdx(row + 1))
      var exp = List[String]()
      exp +:=  row.toString
      if (row % 2 == 0)
        exp +:= "wakka"
      if (row % 5 == 0)
        exp +:= "ooga booga"
      if (row % 13 == 0)
        exp +:= "foobar"

      val rowExpanded: Set[String] = loaded.colIds.slice(s,e).map{x => dictionary(x)}.toSet
      rowExpanded should be (exp.toSet)
//      println("row = " + row)
//      println(s"($s, $e)\t ${loaded.colIds.slice(s,e).mkString(",")} \t ${loaded.colIds.slice(s,e).map{x => dictionary(x)}.mkString(",")}")
    }
  }

  test ("multi-featurize") {

    val m1 = SparkFeaturizerTest.makeSimpleMatrixRDD(sc)
    val m2 = SparkFeaturizerTest.makeSimpleMatrixRDD(sc)

    println(m1.dims.totalDims)
    println(m2.dims.totalDims)
  }


  test ("getHistogramCutoff") {
    val histo = Array((20,1),(19,4), (10, 100), (9,500))
    SparkFeaturizer.getHistogramCutoff(histo, 1) should be (19)
    SparkFeaturizer.getHistogramCutoff(histo, 2) should be (19)
    SparkFeaturizer.getHistogramCutoff(histo, 4) should be (19)
    SparkFeaturizer.getHistogramCutoff(histo, 5) should be (10)
    SparkFeaturizer.getHistogramCutoff(histo, 104) should be (10)
    SparkFeaturizer.getHistogramCutoff(histo, 105) should be (9)
    SparkFeaturizer.getHistogramCutoff(histo, 605) should be (8)
    SparkFeaturizer.getHistogramCutoff(histo, 700) should be (8)
  }

}

object SparkFeaturizerTest {

  val testLock = new Object()
  def withTestLock(body:  => Unit) {
    testLock.synchronized(body)
  }

  def makeSimpleMatrixRDD(sc: SparkContext) : LongRowSparseVectorRDD[String] = {
    val origData = sc.parallelize(1 to 1e3.toInt, 20)
    val matrixRDD = SparkFeaturizer.accumulatorRowPerRecord(origData, sc) {_.toLong} { i =>
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
    Seq("org.apache.spark", "org.eclipse.jetty", "akka", "org.apache.hadoop").map{
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel()
        logger.setLevel(Level.WARN)
        loggerName -> prevLevel
    }
  }
}
