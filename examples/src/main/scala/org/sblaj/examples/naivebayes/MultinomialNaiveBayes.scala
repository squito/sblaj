package org.sblaj.examples.naivebayes

import org.sblaj.SparseBinaryVector
import org.sblaj.ArrayUtils._

/**
 *
 */
class MultinomialNaiveBayes(val nFeatures: Int, val nClasses: Int, val classPriors: Array[Float]) {
  val logTheta: Array[Array[Float]] = new Array[Array[Float]](nFeatures)
  var idx = 0
  while (idx < logTheta.length) {
    logTheta(idx) = new Array[Float](nClasses)
  }
}

object MultinomialNaiveBayes {
  def classPosteriors(data: SparseBinaryVector, theta: Array[Array[Float]],
                      logPriors: Array[Float], posteriors: Array[Float], posteriorPos: Int) {
    Array.copy(logPriors, 0, posteriors, posteriorPos, logPriors.length)
    data.mult(theta, posteriors) //TODO include posteriorPos
    stableLogNormalize(posteriors, 0, logPriors.length)
  }


}


