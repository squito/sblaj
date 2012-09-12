package org.sblaj.examples.samplers

import org.sblaj.SparseBinaryRowMatrix

/**
 *
 */

object MultinomialSampler {
  val rng = new java.util.Random()

  def sampleFromCumProbs(cumProbs: Array[Float]) : Int = {
    val p = rng.nextFloat()
    var idx = 0
    while (idx < cumProbs.length) {
      if (p < cumProbs(idx))
        return idx
      idx += 1
    }
    return idx
  }
}
