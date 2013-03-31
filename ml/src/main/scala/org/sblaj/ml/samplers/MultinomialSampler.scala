package org.sblaj.ml.samplers

/**
 *
 */

object MultinomialSampler {
  val rng = new java.util.Random()

  def sampleFromCumProbs(cumProbs: Array[Float]) : Int = {
    val p = rng.nextFloat()
    var idx = 0
    while (idx < cumProbs.length) {
      if (p <= cumProbs(idx))
        return idx
      idx += 1
    }
    //ack, this actually happens often ... this is bad
    //throw new RuntimeException("rng = " + p + " but last elem of cumProbs = " + cumProbs.last)
    return idx - 1
  }
}
