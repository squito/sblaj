package org.sblaj.spark

import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}
import org.apache.spark.SparkContext
import scala.util.Random
import org.sblaj.BaseSparseBinaryVector
import java.io._
import java.nio.ByteBuffer

/**
 *
 */
class SparkIOTest extends FunSuite with Matchers with BeforeAndAfter {

  var sc : SparkContext = null
  before {
    SparkBinaryFeaturizerTest.silenceSparkLogging
    sc = new SparkContext("local[4]", "spark io test")
  }

  after {
    sc.stop()
    sc = null
    System.clearProperty("spark.driver.port")
  }


  test("io round trip") {

    val rawRdd = sc.parallelize(1 to 1e4.toInt)
    val featurized = SparkBinaryFeaturizer.scalableRowPerRecord(rawRdd, sc, "test/output/spark_io_test/init", minCount = 2){x =>
      x
    }{ x =>
      val rng = new Random()
      val count = rng.nextInt(50)
      def genChar: String = ('a'.toInt + rng.nextInt(26)).toChar.toString
      (0 until count).map{_ => genChar + genChar}
    }

    SparkIO.saveEnumeratedSparseBinaryVectorRDD(featurized, "test/output/spark_io_test/rdd", "test/output/spark_io_test/local")


    val loadedRdd = SparkIO.loadSparseBinaryVectorRdd(sc, path = "test/output/spark_io_test/rdd/vectors")

    loadedRdd.count should be (1e4.toInt)
    loadedRdd.first.size should be > 0
    val first10 = loadedRdd.take(10)
    //this is a really important check -- the way the record reading works, its easy to just blast the old records
    first10(0) should not be theSameInstanceAs(first10(1))
  }

  test("spark io to normal io") {
    val data = Seq(
      new BaseSparseBinaryVector(Array(0,5,8,10), 0, 4),
      new BaseSparseBinaryVector(Array(1,2,3,4,5,6,7), 0, 7)
    )
    val vectorRDD = sc.parallelize(data, 1)

    val path = "test/output/spark_io_test/normal_io"
    SparkIO.saveSparseBinaryVectorRdd(vectorRDD, path)

    val partFiles = new File(path).listFiles().filter{_.getName().startsWith("part-")}
    partFiles.size should be (1)
    val in = new DataInputStream(new FileInputStream(partFiles.head))
    val buffer = new Array[Byte](200) //more than enough
    val nRead = in.read(buffer)
    nRead should be ((4 + 7 + 2) * 4)
    val intBuf = ByteBuffer.wrap(buffer, 0, nRead).asIntBuffer()
    data.foreach{v =>
      intBuf.get() should be (v.size)
      (0 until v.size).foreach{idx =>
        intBuf.get() should be (v.colIds(idx))
      }
    }

  }
}
