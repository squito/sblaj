package org.sblaj.examples.naivebayes

import org.sblaj.{ArrayUtils, SparseBinaryRowMatrix, SparseBinaryVector}
import org.sblaj.ArrayUtils._
import org.sblaj.examples.samplers.MultinomialSampler

/**
 *
 */
class MultinomialNaiveBayes(val nFeatures: Int, val nClasses: Int, val classPriors: Array[Float]) {
  val logTheta: Array[Array[Float]] = createSquareArray(nFeatures, nClasses)
  val numCounts: Array[Array[Float]] = createSquareArray(nFeatures, nClasses)
  val denomCounts : Array[Float] = new Array[Float](nClasses)
  val logClassPriors = classPriors.map{math.log(_).asInstanceOf[Float]}

  /**
   * find the posterior probability of the class for each sample, given the current feature emission
   * rates, and update numCounts & denomCounts
   * @param data
   */
  def addToFeatureEmissionCounts(data: SparseBinaryRowMatrix) {
    val posteriors = new Array[Float](nClasses)
    data.foreach{ vector =>
      val logLikelihood = MultinomialNaiveBayes.classPosteriors(vector, logTheta, logClassPriors, posteriors)
      //update the num & denom counts

      //for each feature that was emitted, increment the emission counts by the posterior probability
      vector.indexAddInto(posteriors, numCounts)

      //update the total number of emissions per class by (posterior * numEmittedFeatures)
      +=(posteriors, vector.nnz, denomCounts)
    }
  }

  def oneEMRound(data: SparseBinaryRowMatrix) {
    resetEmissionCounts()
    addToFeatureEmissionCounts(data)
    updateTheta()
  }

  def updateTheta() {
    //straight ML estimate for each logTheta
    import math.log
    var featureIdx = 0
    while (featureIdx < nFeatures) {
      var classIdx = 0
      while (classIdx < nClasses) {
        logTheta(featureIdx)(classIdx) = log(numCounts(featureIdx)(classIdx) / denomCounts(classIdx)).asInstanceOf[Float]
        classIdx += 1
      }
      featureIdx += 1
    }
  }

  def resetEmissionCounts() {
    zeroSquareArray(numCounts)
    java.util.Arrays.fill(denomCounts, 0f)
  }

  def randomInit(data: SparseBinaryRowMatrix) {
    val cumPriors = ArrayUtils.cumSum(classPriors)
    val posterior = new Array[Float](nClasses)
    data.foreach { vector =>
      val klass = MultinomialSampler.sampleFromCumProbs(cumPriors)
      posterior(klass) = 1
      vector.indexAddInto(posterior, numCounts)
      +=(posterior, vector.nnz, denomCounts)
      posterior(klass) = 0
    }
    updateTheta()
  }

}

object MultinomialNaiveBayes {
  /**
   *
   * @param logTheta nFeatures x nClasses
   */
  def classPosteriors(data: SparseBinaryVector, logTheta: Array[Array[Float]],
                      logPriors: Array[Float], posteriors: Array[Float]) : Float = {
    Array.copy(logPriors, 0, posteriors, 0, logPriors.length)
    data.mult(logTheta, posteriors)
    stableLogNormalize(posteriors, 0, logPriors.length)
  }
}


