package org.sblaj.ml.naivebayes

import org.scalatest.FunSuite
import org.sblaj.{ArrayUtils, SparseBinaryRowMatrix, BaseSparseBinaryVector, SparseBinaryVector}
import org.scalatest.matchers.ShouldMatchers
import org.sblaj.ArrayUtils._
import org.sblaj.ml.samplers.{BinomialSampler, MultinomialSampler}

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
    val nNoise = 5000
    val nGiveaway = 20
    val nb = new MultinomialNaiveBayes(nNoise + nGiveaway, 3)
    var goodFit = false
    var itr = 0
    while (!goodFit && itr < 100) {
      val dataSet = BinomialTestData.generatePowerLawTestData(3, classPriors, 100000, nGiveaway, 0.9f, 0.1f, nNoise, 0.1f, 1e-5f)
      val logSoftLabels = dataSet.makeLogSoftLabels(0.7f)
      println(dataSet.data.nRows + "," + dataSet.data.nCols + "," + dataSet.data.nnz)
      nb.randomInit(dataSet.data, logSoftLabels)

      nb.emTillConvergence(dataSet.data, logSoftLabels)



      val perf = dataSet.trueClassMetrics(nb.logTheta)
      println("performance : " + perf)
      if (perf._1 > 0.95) {
        goodFit = true
        if (nNoise + nGiveaway < 50) {
          println("est theta:")
          println(ArrayUtils.matrixString(nb.logTheta))
          println("true theta:")
          println(ArrayUtils.matrixString(dataSet.rates))
        }
      }
      itr += 1
    }
    goodFit should be (true)
  }




}

class BinomialTestData(val rates: Array[Array[Float]], val priors: Array[Float], val data: SparseBinaryRowMatrix,
                       val trueLabels: Array[Int]) {
  def trueClassMetrics(logTheta: Array[Array[Float]]) = {
    var ll = 0f
    var idx = 0
    var avgLikelihood = 0f
    var avgLikelihoodByClass = new Array[Float](priors.length)
    var countsPerClass = new Array[Float](priors.length)
    val logPriors = priors.map{math.log(_).asInstanceOf[Float]}
    val posteriors = new Array[Float](priors.length)
    data.foreach {vector =>
      MultinomialNaiveBayes.classPosteriors(vector, logTheta, logPriors, posteriors)
      val klass = trueLabels(idx)
      avgLikelihood += posteriors(klass)
      avgLikelihoodByClass(klass) += posteriors(klass)
      countsPerClass(klass) += 1
      ll += math.log(posteriors(trueLabels(idx))).asInstanceOf[Float]
      if (idx % 100 == 0) {
        println(klass + "\t" + posteriors.mkString(","))
      }
      idx += 1
    }
    avgLikelihood /= trueLabels.length
    (avgLikelihood, ll, avgLikelihoodByClass.zipWithIndex.map{x => x._1 / countsPerClass(x._2)}.mkString("{", ",", "}"), countsPerClass.mkString("{", ",", "}"))
  }

  def makeLogSoftLabels(strength: Float) = {
    val logSoftLabels = new Array[Array[Float]](data.nRows)
    var rowIdx = 0
    val strong = math.log(strength).asInstanceOf[Float]
    val weak = math.log( (1 - strength) / (priors.length - 1)).asInstanceOf[Float]
    while (rowIdx < data.nRows) {
      logSoftLabels(rowIdx) = new Array[Float](priors.length)
      var klassIdx = 0
      while (klassIdx < priors.length) {
        logSoftLabels(rowIdx)(klassIdx) = if (trueLabels(rowIdx) == klassIdx) strong else weak
        klassIdx += 1
      }
      rowIdx += 1
    }
    logSoftLabels
  }
}

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
    generate(nSamples, priors, rates)
  }

}

