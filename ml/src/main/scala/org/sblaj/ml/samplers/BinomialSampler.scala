package org.sblaj.ml.samplers

import org.sblaj.SparseBinaryRowMatrix

/**
 *
 */

class BinomialSampler(val rates:Array[Float]) {
  /**
   * return number of non-zero features generated
   * @param matrix
   */
  def genSampleInto(matrix: SparseBinaryRowMatrix) = {
    assert(matrix.nCols == rates.length)
    var nnz = matrix.nnz
    var idx = 0
    val rng = new java.util.Random()
    while (idx < rates.length) {
      //TODO faster random number generators
      if (rng.nextFloat() < rates(idx)) {
        matrix.colIds(nnz) = idx
        nnz += 1
      }
      idx += 1
    }
    matrix.rowStartIdx(matrix.nRows + 1) =  nnz
    matrix.setSize(matrix.nRows + 1, nnz)
  }
}

object BinomialSampler {
  def genPowerLawRates(highRate: Float, lowRate: Float, n: Int) : Array[Float] = {
    val rates = new Array[Float](n)
    genPowerLawRates(highRate, lowRate, rates, 0, n)
    rates
  }

  def genPowerLawRates(highRate: Float, lowRate: Float, into: Array[Float], startPos: Int, n: Int) {
    val scale = highRate
    val base = math.exp( math.log(lowRate / highRate) / (n -1))
    var offset = 0
    while (offset < n) {
      into(startPos + offset) = (scale * math.pow(base, offset)).asInstanceOf[Float]
      offset += 1
    }
  }
}