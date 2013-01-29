package org.sblaj.ml.sgd

import org.scalatest.FunSuite
import org.sblaj.{SparseBinaryRowMatrix, ArrayUtils}
import org.sblaj.ml.samplers.BinomialSampler
import org.scalatest.matchers.ShouldMatchers

/**
 *
 */

class LogisticRegressionTest extends FunSuite with ShouldMatchers {

  def makeData(nSamples: Int, weights: Array[Float], featureRates: Array[Float]) = {
    //first, make the data

    val sampler = new BinomialSampler(featureRates)
    val avgRate = ArrayUtils.arraySum(featureRates, 0, featureRates.length)
    //gross over-estimate of how much room we need
    val maxNnz = (avgRate * 2 * nSamples).asInstanceOf[Int]
    val matrix = new SparseBinaryRowMatrix(nSamples, maxNnz, featureRates.length)
    (0 until nSamples).foreach{
      idx => sampler.genSampleInto(matrix)
    }
    //now we've got the data, assign the labels based on the true function
    val labels = new Array[Float](nSamples)
    var idx = 0
    matrix.foreach {
      vector =>
        val z = vector.dot(weights)
        val posProb = LogisticFunction.logistic(vector.dot(weights))
        labels(idx) = if (math.random < posProb) 1f else -1f
        idx += 1
    }

    (matrix, labels, weights)
  }

  test("easy logistic regression") {
    //really easy test case
    val nSamples = 1e6.toInt
    val weights = List[Float](0.5f, -1.2f, 0.3f).toArray
    val featureRates = List[Float](0.5f, 0.5f, 0.1f).toArray
    val testSet = makeData(nSamples, weights, featureRates)

    val regressor = new LogisticRegression(weights.length, LogisticUpdate)
    (0 until 20).foreach{idx =>
      println("iteration : " + idx)
      val eta = (.1 / (1 + idx)).toFloat
      regressor.matrixUpdate(testSet._1,testSet._2,eta)
      println("fitted weights = " + regressor.weights.mkString(","))
      println("true weights = " + testSet._3.mkString(","))
    }

    weights.zipWithIndex.foreach { case (w,idx) =>
      regressor.weights(idx) should be (w plusOrMinus 0.2f)
    }
  }

  test("easy ridge regression") {
    val nSamples = 1e6.toInt
    val weights = List[Float](0.5f, -1.2f, 0.3f).toArray
    val featureRates = List[Float](0.5f, 0.5f, 0.1f).toArray
    val testSet = makeData(nSamples, weights, featureRates)

    val ridgeRegressor = new LogisticRegression(weights.length, LogisticUpdate, new RidgePenalty(15f))
    (0 until 20).foreach{idx =>
      println("iteration : " + idx)
      val eta = (.1 / (1 + idx)).toFloat
      ridgeRegressor.matrixUpdate(testSet._1,testSet._2,eta)
    }
    println("fitted weights = " + ridgeRegressor.weights.mkString(","))
    println("true weights = " + testSet._3.mkString(","))


    weights.zipWithIndex.foreach { case (w,idx) =>
      math.signum(ridgeRegressor.weights(idx)) should be (math.signum(w))
      val absW = math.abs(w)
      val absWHat = math.abs(ridgeRegressor.weights(idx))
      // the bounds are basically reverse-engineered, like a regression test, but the idea is to
      // make sure the fit follows basic ridge regression properties
      absWHat should be < absW + 0.05f
      absWHat should be > absW - 0.3f
    }
  }


  lazy val nSamples = 1e4.toInt
  lazy val nFeatures = 2e2.toInt
  lazy val highDimData = {
    val weights = new Array[Float](nFeatures)
    val rng = new java.util.Random()
    (0 until nFeatures).foreach {
      idx => weights(idx) = rng.nextGaussian().toFloat
    }
    val featureRates = BinomialSampler.genPowerLawRates(0.8f, 1e-5f, nFeatures)
    val testData = makeData(nSamples, weights, featureRates)
    (testData._1, testData._2, testData._3, featureRates)
  }

  test("high dim Ridge LR") {
    //high dimensions, not that many sample points, and some very rare features
    val testSet = highDimData

    val regressor = new LogisticRegression(nFeatures, LogisticUpdate, new RidgePenalty(20f))
    (0 until 20).foreach{idx =>
      println("iteration : " + idx)
      val eta = (.1 / (1 + idx)).toFloat
      regressor.matrixUpdate(testSet._1,testSet._2,eta)
    }
    compareWeights(testSet._3, regressor.weights, testSet._4)

  }

  def compareWeights(trueWeights: Array[Float], estWeights: Array[Float], rates: Array[Float]) {
    println("idx\trate\tBeta\tBetahat\tSignMatch?\tMagnitudeDelta")
    (0 until trueWeights.length).foreach{idx =>
      import math._
      val t = trueWeights(idx)
      val h = estWeights(idx)
      val s = signum(t) == signum(h)
      val d = abs(t) - abs(h)
      println(idx + ":\t" + rates(idx) + "\t" + t + "\t" + h + "\t" + s + "\t" + d)
    }
  }

  test("high dim Lasso LR") {
    //high dimensions, not that many sample points, and some very rare features
    val testSet = highDimData

    val regressor = new LogisticRegression(nFeatures, LogisticUpdate, new LassoPenalty(20f))
    (0 until 20).foreach{idx =>
      println("iteration : " + idx)
      val eta = (.1 / (1 + idx)).toFloat
      regressor.matrixUpdate(testSet._1,testSet._2,eta)
    }

    compareWeights(testSet._3, regressor.weights, testSet._4)
  }


}
