package org.sblaj

import java.util

/**
 *
 */

trait SparseBinaryVector {
  //TODO can the dense matrix class also be generified, and still performant?

  def dot(x:Array[Float]) : Float

  /**
   * return (theta)^T (this)^T.
   * <p>
   * eg., take the transpose of theta, and multiply it by this
   * as a column vector.
   *
   * @param theta a dense matrix
   * @param into store the result in this array
   */
  def tMultC(theta:Array[Array[Float]], into: Array[Float]) : Unit
}


class BaseSparseBinaryVector (val colIds: Array[Int], val startIdx: Int, val endIdx: Int)
extends SparseBinaryVector {
  def dot(x: Array[Float]) : Float = {
    var f : Float = 0
    var idx = startIdx
    while (idx < endIdx) {
      f += x(colIds(idx))
      idx += 1
    }
    f
  }

  def tMultC(theta: Array[Array[Float]], into: Array[Float]) : Unit = {
    util.Arrays.fill(into, 0f)
    var idx = startIdx
    while (idx < endIdx) {
      val colId = colIds(idx)
      val theta_c = theta(colId)
      var j = 0
      while (j < into.length) {
        into(j) += theta_c(j)
        j += 1
      }
      idx += 1
    }
  }
}