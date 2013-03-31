package org.sblaj.ml

import org.scalatest.{Tag, FunSuite}
import java.io.{FileOutputStream, ObjectOutputStream, FileInputStream, ObjectInputStream}

/**
 * Util for writing tests that generate their own data (eg., via some random process).
 * Though you want to keep the code that generates the data, sometimes its nice to save
 * one copy of the generated data (eg to a file), to get a bit more repeatability, &
 * to save time. This provides ways to check if the data exists, rebuild if necessary, etc.
 *
 * TODO move to a standalone place
 */
trait GeneratedDataSet extends FunSuite {

  def testDataFromFile[A <: Serializable](
    testName: String,
    file: String,
    regenerate: Boolean = false,
    tags: Seq[Tag] = Seq.empty)(
    dataGen: => A)(
    testFun: A => Unit
    ) {
    val f = new java.io.File(file)
    val dataset =
      if (f.exists() && !regenerate) {
        try {
          println("loading dataset from " + file + " for test " + testName)
          val in = new ObjectInputStream(new FileInputStream(f))
          val d = in.readObject().asInstanceOf[A]
          in.close()
          d
        } catch {
          case ex: Exception =>
            println("exception loading dataset from file, will regenerate data instead")
            ex.printStackTrace()
            genDataAndSaveToFile(f)(dataGen)
        }
      }
      else {
        println("generating data set for test " + testName)
        genDataAndSaveToFile(f)(dataGen)
      }
    test(testName, tags: _*)(testFun(dataset))
  }

  def genDataAndSaveToFile[A <: Serializable](f: java.io.File)(dataGen: => A): A = {
    val d = dataGen
    try {
      f.getParentFile.mkdirs()
      val out = new ObjectOutputStream(new FileOutputStream(f))
      out.writeObject(d)
      out.close()
    } catch {
      case ex: Exception =>
        println("exception saving dataset to file, returning dataset anyway")
        ex.printStackTrace()
    }
    d
  }
}
