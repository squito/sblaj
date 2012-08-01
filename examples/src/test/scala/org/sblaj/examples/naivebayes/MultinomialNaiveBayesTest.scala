package org.sblaj.examples.naivebayes

import org.scalatest.FunSuite

/**
 *
 */

class MultinomialNaiveBayesTest extends FunSuite {
  test("class posterior") {
    val nClasses = 3
    val nDims = 20
    val theta = new Array[Array[Float]](nDims)
    for (feature <- (0 until nDims))
    {
      val ratePerClass = new Array[Float](nClasses)
      for (klass <- (0 until nClasses)) {
        ratePerClass(klass) = math.random.asInstanceOf[Float]
        if (feature % nClasses == klass) {
          ratePerClass(klass) *= 20   //"give away" features for each class
        }
      }
      theta(feature) = ratePerClass
    }
    normalizeColumns(theta)
    println(matrixString(theta))




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


  def matrixString(theta: Array[Array[Float]]) : String = {
    theta.map { _.mkString("[", ",", "]")}.mkString("\n")
  }
}
