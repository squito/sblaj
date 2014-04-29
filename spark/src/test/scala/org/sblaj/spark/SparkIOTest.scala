package org.sblaj.spark

import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}
import org.apache.spark.SparkContext
import scala.util.Random

/**
 *
 */
class SparkIOTest extends FunSuite with Matchers with BeforeAndAfter {

  var sc : SparkContext = null
  before {
    SparkFeaturizerTest.silenceSparkLogging
    sc = new SparkContext("local[4]", "spark io test")
  }

  after {
    sc.stop()
    sc = null
    System.clearProperty("spark.driver.port")
  }


  test("io round trip") {

    val rawRdd = sc.parallelize(1 to 1e4.toInt)
    val featurized = SparkFeaturizer.scalableRowPerRecord(rawRdd, sc, "test/output/spark_io_test/init", minCount = 2){x =>
      x
    }{ x =>
      val rng = new Random()
      val count = rng.nextInt(50)
      def genChar: String = ('a'.toInt + rng.nextInt(26)).toChar.toString
      (0 until count).map{_ => genChar + genChar}
    }

    SparkIO.saveEnumeratedSparseVectorRDD(featurized, "test/output/spark_io_test/rdd", "test/output/spark_io_test/local")


    val loadedRdd = SparkIO.loadSparseBinaryVectorRdd(sc, path = "test/output/spark_io_test/rdd/vectors")

    loadedRdd.count should be (1e4.toInt)
    loadedRdd.first.size should be > 0
  }
}
