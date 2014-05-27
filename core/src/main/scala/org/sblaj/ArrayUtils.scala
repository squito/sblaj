package org.sblaj

import collection._

/**
 *
 */

object ArrayUtils {

  def matrixString[T](theta: Array[Array[T]]) : String = {
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

  def arrayNormalize(arr: Array[Float], startIdx: Int, endIdx: Int) {
    val sum = arraySum(arr, startIdx, endIdx)
    var idx = startIdx
    while (idx < endIdx) {
      arr(idx) = arr(idx) / sum
      idx += 1
    }
  }

  /**
   * Given a set of probabilities on the log scale, normalizes them so that they sum to one.
   * The given array is modified to have the scaled probabilities (on *linear* scale, no longer log)
   * and log of the normalizing constant is returned (often this is the log-likelihood).
   *
   * The method used here is robust to work even when the un-normalized values are very small
   **/
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


  def createRectArray(nRows: Int, nCols: Int) : Array[Array[Float]] = {
    val result: Array[Array[Float]] = new Array[Array[Float]](nRows)
    var idx = 0
    while (idx < result.length) {
      result(idx) = new Array[Float](nCols)
      idx += 1
    }
    result
  }

  def zeroRectArray(arr: Array[Array[Float]]) {
    var idx = 0
    while (idx < arr.length) {
      java.util.Arrays.fill(arr(idx), 0f)
      idx += 1
    }
  }

  def +=(add: Array[Float], into: Array[Float]) {
    var idx = 0
    while (idx < into.length) {
      into(idx) += add(idx)
      idx += 1
    }
  }

  def +=(add: Array[Float], mult: Float, into: Array[Float]) {
    var idx = 0
    while (idx < into.length) {
      into(idx) += add(idx) * mult
      idx += 1
    }
  }

  def cumSum(arr: Array[Float]) = {
    val cs = new Array[Float](arr.length)
    var idx = 0
    var sum = 0f
    while (idx < arr.length) {
      sum += arr(idx)
      cs(idx) = sum
      idx += 1
    }
    cs
  }

  def topK(arr: Array[Float], startIdx: Int, endIdx: Int, k: Int): IndexedSeq[(Int,Float)] = {
    //terrible implementation ... really should use a bounded priority queue.
    val l = endIdx - startIdx
    val copy = new Array[Float](l)
    System.arraycopy(arr, startIdx, copy, 0, l)
    val sorted = copy.zipWithIndex.sortBy{- _._1}
    sorted.slice(0, k).map{_.swap}
  }

  def matrixAsMap[T](idRemap:Array[Int], matrix: Array[T]) : Map[Int,Map[Int, T]] = {
    val n = idRemap.length
    val map = mutable.HashMap[Int,mutable.HashMap[Int, T]]()
    (0 until n).foreach{outerIdx =>
      (0 until n).foreach{innerIdx =>
        map.getOrElseUpdate(idRemap(outerIdx), mutable.HashMap[Int, T]()) += idRemap(innerIdx) -> matrix(outerIdx * n + innerIdx)
      }
    }
    map
  }


  def quantiles(data: Array[Float], quantiles: Array[Float], isSorted: Boolean = false): Array[Float] = {
    val sData = if (!isSorted) {
      val t = new Array[Float](data.length)
      System.arraycopy(data, 0, t, 0, data.length)
      java.util.Arrays.sort(t)
      t
    } else {
      data
    }

    val qs = new Array[Float](quantiles.length)
    (0 until quantiles.length).foreach{qIdx =>
      val q = quantiles(qIdx)
      //TODO interpolation
      val idx = math.min((q * sData.length), sData.length -1).toInt
      qs(qIdx) = sData(idx)
    }
    qs
  }


}
