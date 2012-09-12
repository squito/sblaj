package org.sblaj.examples.naivebayes

import org.scalatest.FunSuite
import org.sblaj.{ArrayUtils, SparseBinaryRowMatrix, BaseSparseBinaryVector, SparseBinaryVector}
import org.scalatest.matchers.ShouldMatchers
import org.sblaj.ArrayUtils._
import org.sblaj.examples.samplers.{BinomialSampler, MultinomialSampler}

/**
 *
 */

class MultinomialNaiveBayesTest extends FunSuite with ShouldMatchers {
  test("class posterior") {
    //come up with rates
    val nClasses = 3
    val nDims = 20
    val theta = makeGiveawayTheta(nDims, nClasses)
    val logTheta = theta.map(_.map(math.log(_).asInstanceOf[Float]))

    //generate some data
    val vector : SparseBinaryVector = new BaseSparseBinaryVector(Array[Int](0,3,6), startIdx = 0, endIdx = 3)
    val logPriors = Array[Float](0.3f, 0.3f, 0.4f).map(math.log(_).asInstanceOf[Float])

    val posterior = new Array[Float](3)
    MultinomialNaiveBayes.classPosteriors(vector, logTheta, logPriors, posterior)

    arraySum(posterior, 0, 3) should be (1.0f plusOrMinus 0.00001f)
    posterior(0) should be (1.0f plusOrMinus 0.00001f)
  }

  //TODO move to more general location
  def normalizeColumns(theta: Array[Array[Float]]) {
    val nCols = theta(0).length
    val colSums = new Array[Float](nCols)
    for (feature <- (0 until theta.length)) {
      for (klass <- (0 until nCols)) {
        colSums(klass) += theta(feature)(klass)
      }
    }

    //this isn't a stable normalization, but OK for the moment
    for (feature <- (0 until theta.length)) {
      for (klass <- (0 until nCols)) {
        theta(feature)(klass) /= colSums(klass)
      }
    }
  }


  def makeGiveawayTheta(nDims: Int, nClasses: Int) = {
    val theta = new Array[Array[Float]](nDims)
    for (feature <- (0 until nDims))
    {
      val ratePerClass = new Array[Float](nClasses)
      for (klass <- (0 until nClasses)) {
        ratePerClass(klass) = math.random.asInstanceOf[Float]
        if (feature % nClasses == klass) {
          ratePerClass(klass) = 500   //"give away" features for each class
        }
      }
      theta(feature) = ratePerClass
    }
    normalizeColumns(theta)
    theta
  }


  test("real EM") {
    val classPriors = Array(0.6f, 0.2f, 0.2f)
    val dataSet = generatePowerLawTestData(3, classPriors, 10000, 10, 0.9f, 0.1f, 10, 0.1f, 1e-5f)
    val nb = new MultinomialNaiveBayes(20, 3, classPriors)
    println(dataSet.data.nRows + "," + dataSet.data.nCols + "," + dataSet.data.nnz)
    nb.randomInit(dataSet.data)
    println(ArrayUtils.matrixString(nb.numCounts))
    println("denom counts = " + nb.denomCounts.mkString(","))
    println(ArrayUtils.matrixString(nb.logTheta))
    println()

    nb.oneEMRound(dataSet.data)
    println(ArrayUtils.matrixString(nb.logTheta))
  }


  def generatePowerLawTestData(nClasses:Int, priors: Array[Float], nSamples:Int, nGiveaway: Int, giveawayHigh: Float, giveawayLow: Float,
                               nNoise: Int, noiseHigh: Float, noiseLow: Float) : BinomialTestData = {
    val rates = new Array[Array[Float]](nClasses)
    var klass = 0
    while (klass < nClasses) {
      rates(klass) = new Array[Float](nGiveaway + nNoise)
      klass += 1
    }
    var featureIdx = 0
    while (featureIdx < nGiveaway) {
      klass = 0
      while (klass < nClasses) {
        rates(klass)(featureIdx) = if (featureIdx % nClasses == klass) giveawayHigh else giveawayLow
        klass += 1
      }
      featureIdx += 1
    }
    val noiseRates = BinomialSampler.genPowerLawRates(noiseHigh, noiseLow, nNoise)
    while(featureIdx < nNoise + nGiveaway) {
      klass = 0
      while (klass < nClasses) {
        rates(klass)(featureIdx) = noiseRates(featureIdx - nGiveaway)
        klass += 1
      }
      featureIdx += 1
    }
    BinomialTestData.generate(nSamples, priors, rates)
  }


}

class BinomialTestData(val rates: Array[Array[Float]], val priors: Array[Float], val data: SparseBinaryRowMatrix, val trueLabels: Array[Int])

object BinomialTestData {
  def generate(nSamples: Int, priors: Array[Float], rates: Array[Array[Float]]) = {
    val trueLabels = new Array[Int](nSamples)
    var sampleIdx = 0
    val priorCumSum = ArrayUtils.cumSum(priors)
    while (sampleIdx < nSamples) {
      trueLabels(sampleIdx) = MultinomialSampler.sampleFromCumProbs(priorCumSum)
      sampleIdx += 1
    }
    val avgRate = rates.map{arr => ArrayUtils.arraySum(arr, 0, arr.length)}.max
    //gross over-estimate of how much room we need
    val maxNnz = (avgRate * 2 * nSamples).asInstanceOf[Int]
    val matrix = new SparseBinaryRowMatrix(nSamples, maxNnz, rates(0).length)

    val classSamplers = rates.map{new BinomialSampler(_)}

    //go through row by row and generate the data
    sampleIdx = 0
    while (sampleIdx < nSamples) {
      classSamplers(trueLabels(sampleIdx)).genSampleInto(matrix)
      sampleIdx += 1
    }

    new BinomialTestData(rates, priors, matrix, trueLabels)
  }
}

