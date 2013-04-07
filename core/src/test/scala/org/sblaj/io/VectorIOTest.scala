package org.sblaj.io

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import java.io._
import org.sblaj.LongSparseBinaryVector

class VectorIOTest extends FunSuite with ShouldMatchers {

  val testRootFile = new File("test/generated/VectorIOTest")
  testRootFile.mkdirs()
  test("basic LongSparseVector io") {
    val v = new LongSparseBinaryVector(Array(0l,1l,2l), 0, 3)
    val file = new File(testRootFile,"LongSparseVectorTest.bin")
    val out = new DataOutputStream(new FileOutputStream(file))
    VectorIO.append(v, out)
    out.close

    val in = new DataInputStream(new FileInputStream(file))
    val itr = new LongSparseRowVectorDataInputIterator(in, 1)
    itr.hasNext should be (true)
    val v2 = itr.next
    itr.hasNext should be (false)
    v2.colIds should be (Array(0l, 1l, 2l))
    v2.startIdx should be (0)
    v2.endIdx should be (3)
    in.close()
  }
}
