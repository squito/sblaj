package org.sblaj

/**
 *
 */

object ArrayUtils {

  def matrixString(theta: Array[Array[Float]]) : String = {
    theta.map { _.mkString("[", ",", "]")}.mkString("\n")
  }

  def arraySum(arr: Array[Float], startIdx : Int, endIdx : Int) = {
    var sum = 0f
    var idx = startIdx
    while (idx < endIdx) {
      sum += arr(idx)
      idx += 1
    }
    sum
  }

  def stableLogNormalize(arr: Array[Float], startIdx: Int, endIdx: Int) : Float = {
    //first find approximate normalizing value
    var max = Float.MinValue
    var idx = startIdx
    while (idx < endIdx) {
      if (arr(idx) > max)
        max = arr(idx)
      idx += 1
    }

    //scale by approx normalizer, compute remaining normalization
    idx = startIdx
    var sum = 0f
    while (idx < endIdx) {
      arr(idx) = math.exp(arr(idx) - max).asInstanceOf[Float]
      sum += arr(idx)
      idx += 1
    }

    //do final normalization
    idx = startIdx
    while (idx  < endIdx) {
      arr(idx) = arr(idx) / sum
      idx += 1
    }

    //return log(overall normalizing constant)
    (max + math.log(sum).asInstanceOf[Float])
  }


  def createSquareArray(nRows: Int, nCols: Int) : Array[Array[Float]] = {
    val result: Array[Array[Float]] = new Array[Array[Float]](nRows)
    var idx = 0
    while (idx < result.length) {
      result(idx) = new Array[Float](nCols)
      idx += 1
    }
    result
  }

  def zeroSquareArray(arr: Array[Array[Float]]) {
    var idx = 0
    while (idx < arr.length) {
      java.util.Arrays.fill(arr(idx), 0f)
      idx += 1
    }
  }

  def +=(into: Array[Float], add: Array[Float]) {
    var idx = 0
    while (idx < into.length) {
      into(idx) += add(idx)
      idx += 1
    }
  }

}
