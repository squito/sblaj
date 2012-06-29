package org.sblaj

import org.scalatest.Suite

/**
 *
 */

class SparseBinaryRowMatrixTest extends Suite {

  def testMatrixConstructor() {
    val m = new SparseBinaryRowMatrix(5, 10, 3)
    assert(m.maxNnz == 10)
  }
}
