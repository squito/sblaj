package org.sblaj.io

import org.scalatest.{Matchers, FunSuite}
import org.sblaj.StdMixedRowMatrix
import java.io.File

class EnumVectorIOTest extends FunSuite with Matchers {

  val dir = new File("test/generated/EnumVectorIOTest")
  dir.mkdirs()

  test("lots of stuff") {
    pending
  }

  test("mixed matrix io") {
    val mixedMat = new StdMixedRowMatrix(
      nDenseCols = 10,
      nSparseCols = 100,
      maxNnz = 1000,
      maxRows = 30
    )
    mixedMat.setSize(30, 1000)
    val file = new File(dir, "mixed_io")
    EnumVectorIO.writeMixedMatrix(mixedMat, file)

    val deserMat = EnumVectorIO.readMixedMatrix(file)
    deserMat.nRows should be (30)
    deserMat.nDenseCols should be (10)
    deserMat.nSparseCols should be (100)
    deserMat.nnz should be (1000)
  }

}
