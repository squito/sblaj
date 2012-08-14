package org.sblaj.examples.naivebayes

import org.scalatest.FunSuite
import org.sblaj.{BaseSparseBinaryVector, SparseBinaryVector}
import org.scalatest.matchers.ShouldMatchers
import org.sblaj.ArrayUtils._

/**
 *
 */

class MultinomialNaiveBayesTest extends FunSuite with ShouldMatchers {
  test("class posterior") {
    //come up with rates
    val nClasses = 3
    val nDims = 20
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
    val logTheta = theta.map(_.map(math.log(_).asInstanceOf[Float]))

    //generate some data
    val vector : SparseBinaryVector = new BaseSparseBinaryVector(Array[Int](0,3,6), startIdx = 0, endIdx = 3)
    val logPriors = Array[Float](0.3f, 0.3f, 0.4f).map(math.log(_).asInstanceOf[Float])

    val posterior = new Array[Float](3)
    MultinomialNaiveBayes.classPosteriors(vector, logTheta, logPriors, posterior, 0)

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

}
