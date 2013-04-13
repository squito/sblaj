package org.sblaj.ml.naivebayes

import org.sblaj.{ArrayUtils, SparseBinaryRowMatrix, SparseBinaryVector}
import org.sblaj.ArrayUtils._
import org.sblaj.ml.samplers.MultinomialSampler

/**
 *
 */
class MultinomialNaiveBayes(val nFeatures: Int, val nClasses: Int) {
  val logTheta: Array[Array[Float]] = createSquareArray(nFeatures, nClasses)
  val numCounts: Array[Array[Float]] = createSquareArray(nFeatures, nClasses)
  val denomCounts : Array[Float] = new Array[Float](nClasses)

  /**
   * find the posterior probability of the class for each sample, given the current feature emission
   * rates, and update numCounts & denomCounts
   * @param data
   * @return the log-likelihood of the observed data
   */
  def addToFeatureEmissionCounts(data: SparseBinaryRowMatrix, logClassPriors: Array[Array[Float]]) = {
    val posteriors = new Array[Float](nClasses)
    var totalLL = 0f
    var idx = 0
    data.foreach{ vector =>
      val logLikelihood = MultinomialNaiveBayes.classPosteriors(vector, logTheta, logClassPriors(idx), posteriors)
      //update the num & denom counts

      //for each feature that was emitted, increment the emission counts by the posterior probability
      vector.outerPlus(posteriors, numCounts)

      //update the total number of emissions per class by (posterior * numEmittedFeatures)
      +=(posteriors, vector.nnz, denomCounts)
      totalLL += logLikelihood
      idx += 1
    }
    totalLL
  }

  def oneEMRound(data: SparseBinaryRowMatrix, logClassPriors : Array[Array[Float]]) = {
    resetEmissionCounts()
    val ll = addToFeatureEmissionCounts(data, logClassPriors)
    updateTheta()
    ll
  }

  def emTillConvergence(data: SparseBinaryRowMatrix, logClassPriors: Array[Array[Float]],
                        maxRounds: Int = 100, minLogLikelihoodDelta : Float = 1e-10f) {
    randomInit(data, logClassPriors)
    var prevLL = oneEMRound(data, logClassPriors)
    var round = 1
    while (round < maxRounds) {
      val nextLL = oneEMRound(data, logClassPriors)
      round += 1
      println("LL(" + round + ") = " + nextLL + "\t Delta = " + (nextLL - prevLL))
      if (nextLL - prevLL < minLogLikelihoodDelta)
        return
      prevLL = nextLL
    }
  }

  def updateTheta() {
    //TODO add way to switch between ML & MAP
    mlThetaUpdate()
  }

  def mlThetaUpdate() {
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

  var overallFeatureRates : Array[Float] = null
  def mapThetaUpdate(priorWeight: Float, data: SparseBinaryRowMatrix) {
    //estimate theta using MAP, where each feature has a prior centered around its overall rate
    if (overallFeatureRates == null)
      overallFeatureRates = MultinomialNaiveBayes.getOverallFeatureRates(data)
    import math.log
    var featureIdx = 0
    while (featureIdx < nFeatures) {
      var classIdx = 0
      while (classIdx < nClasses) {
        //TODO correct map estimate here
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

  def randomInit(data: SparseBinaryRowMatrix, logClassPriors: Array[Array[Float]]) {
    val posterior = new Array[Float](nClasses)
    var idx = 0
    data.foreach { vector =>
      val cumPriors = ArrayUtils.cumSum(logClassPriors(idx).map{math.exp(_).asInstanceOf[Float]})
      val klass = MultinomialSampler.sampleFromCumProbs(cumPriors)
      posterior(klass) = 1
      vector.outerPlus(posterior, numCounts)
      +=(posterior, vector.nnz, denomCounts)
      posterior(klass) = 0
      idx += 1
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

  def getOverallFeatureRates(data: SparseBinaryRowMatrix) = {
    data.getColSums.map{_.asInstanceOf[Float] / data.nnz}
  }


}


