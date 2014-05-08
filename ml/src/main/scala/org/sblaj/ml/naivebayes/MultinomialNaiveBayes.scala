package org.sblaj.ml.naivebayes

import org.sblaj.{ArrayUtils, SparseBinaryRowMatrix, SparseBinaryVector}
import org.sblaj.ArrayUtils._
import org.sblaj.ml.samplers.MultinomialSampler

/**
 *
 */
class MultinomialNaiveBayes(val nFeatures: Int, val nClasses: Int, val logTheta: Array[Array[Float]]) {
  def this(nFeatures: Int, nClasses: Int) {
    this(nFeatures, nClasses, createRectArray(nFeatures, nClasses))
  }
  val numCounts: Array[Array[Float]] = createRectArray(nFeatures, nClasses)
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

  def oneEMRound(data: SparseBinaryRowMatrix, logClassPriors : Array[Array[Float]], priorWeight: Option[Float]) = {
    resetEmissionCounts()
    val ll = addToFeatureEmissionCounts(data, logClassPriors)
    updateTheta(priorWeight, data)
    ll
  }

  def emTillConvergence(
    data: SparseBinaryRowMatrix,
    logClassPriors: Array[Array[Float]],
    priorWeight: Option[Float],
    maxRounds: Int = 100,
    minLogLikelihoodDelta : Float = 1e-10f
  ) {
    randomInit(data, logClassPriors, priorWeight)
    var prevLL = oneEMRound(data, logClassPriors, priorWeight)
    var round = 1
    while (round < maxRounds) {
      val nextLL = oneEMRound(data, logClassPriors, priorWeight)
      round += 1
      println("LL(" + round + ") = " + nextLL + "\t Delta = " + (nextLL - prevLL))
      if (nextLL - prevLL < minLogLikelihoodDelta)
        return
      prevLL = nextLL
    }
  }

  def updateTheta(priorWeight: Option[Float], data: SparseBinaryRowMatrix) {
    priorWeight match {
      case None =>
        mlThetaUpdate()
      case Some(pw) =>
        mapThetaUpdate(pw, data)
    }
  }

  def mlThetaUpdate() {
    //straight ML estimate for each logTheta
    import math.log
    var featureIdx = 0
    while (featureIdx < nFeatures) {
      var classIdx = 0
      while (classIdx < nClasses) {
        logTheta(featureIdx)(classIdx) = log(numCounts(featureIdx)(classIdx) / denomCounts(classIdx)).toFloat
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
        val num: Float  = numCounts(featureIdx)(classIdx) + priorWeight
        val denom: Float = denomCounts(classIdx) + (priorWeight / overallFeatureRates(featureIdx))
        logTheta(featureIdx)(classIdx) = log(num / denom).asInstanceOf[Float]
        classIdx += 1
      }
      featureIdx += 1
    }

  }

  def resetEmissionCounts() {
    zeroRectArray(numCounts)
    java.util.Arrays.fill(denomCounts, 0f)
  }

  def randomInit(data: SparseBinaryRowMatrix, logClassPriors: Array[Array[Float]], priorWeight: Option[Float]) {
    val posterior = new Array[Float](nClasses)
    var idx = 0
    data.foreach { vector =>
      val cumPriors = ArrayUtils.cumSum(logClassPriors(idx).map{math.exp(_).toFloat})
      val klass = MultinomialSampler.sampleFromCumProbs(cumPriors)
      posterior(klass) = 1
      vector.outerPlus(posterior, numCounts)
      +=(posterior, vector.nnz, denomCounts)
      posterior(klass) = 0
      idx += 1
    }
    updateTheta(priorWeight, data)
  }


  def showTopFeatures(data: SparseBinaryRowMatrix, k: Int, classIdxToLabel: Option[Int => String], dictionary: Int => String) {
    if (overallFeatureRates == null)
      overallFeatureRates = MultinomialNaiveBayes.getOverallFeatureRates(data)
    val rateRatio = new Array[Float](nFeatures)
    (0 until nClasses).foreach{classIdx =>
      println("class " + classIdx + " " + classIdxToLabel.map{f => f(classIdx)}.getOrElse(""))
      (0 until nFeatures).foreach{featureIdx =>
        rateRatio(featureIdx) = math.exp(logTheta(featureIdx)(classIdx)).toFloat / overallFeatureRates(featureIdx)
      }
      ArrayUtils.topK(rateRatio, 0, nFeatures, k).map{case(featureIdx, ratio) =>
        println(dictionary(featureIdx) + "\t" + ratio + "\t" + featureIdx)
      }
      println()
    }
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


