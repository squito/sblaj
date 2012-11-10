package org.sblaj.ml.sgd

import org.sblaj.{SparseBinaryRowMatrix, SparseBinaryVector}

class LogisticRegression(val weights: Array[Float], val update: GradientUpdate, val regularizer: Regularizer) {
  def this(nFeatures: Int, update: GradientUpdate) =
    this(new Array[Float](nFeatures), update, NoopRegularizer)
  def this(nFeatures: Int, update: GradientUpdate, regularizer: Regularizer) =
    this(new Array[Float](nFeatures), update, regularizer)
  def pointUpdate(vector: SparseBinaryVector, label: Float, eta: Float) {
    update.gradient(vector, label, weights, weights, eta)
  }

  def matrixUpdate(matrix: SparseBinaryRowMatrix, labels: IndexedSeq[Float], eta: Float) {
    var idx = 0;
    matrix.foreach{
      vector =>
        pointUpdate(vector, labels(idx), eta)
        idx += 1
    }
    //regularizing inside the per-point update is insanely slow, b/c we lose sparsity speedup
    // probably should actually be more often than this, though ...
    regularizer.regularize(weights, eta)
  }
}


trait GradientUpdate {
  /**
   * compute the gradient based on one vector and a current set of weights.  Multiply the gradient
   * by the "step size" eta, and add it to gradientStore.  label is +/- 1
   */
  def gradient(point: SparseBinaryVector, label: Float, weights: Array[Float], gradientStore: Array[Float], eta: Float)
}

trait Regularizer {
  def regularize(weights: Array[Float], eta: Float)
}

object LogisticUpdate extends GradientUpdate {
  def gradient(point: SparseBinaryVector, label: Float, weights: Array[Float], gradientStore: Array[Float], eta: Float) {
    val z = point.dot(weights)
//    println("z = " + z)
    val p = (LogisticFunction.logistic(-label * z) * label * eta).toFloat
//    println("p = " + p)
    //is this a common enough operation that we should put it directly in SparseBinaryVector, to eliminate the need for the closure?
    point.foreach{ colIdx =>
      gradientStore(colIdx) += p
    }
  }
}

object NoopRegularizer extends Regularizer {
  def regularize(weights: Array[Float], eta: Float) {}
}

class RidgePenalty(val penalty: Float) extends Regularizer {
  def regularize(weights: Array[Float], eta: Float) {
    var idx = 0
    while (idx < weights.size) {
      val l = penalty * weights(idx) * eta
      if ((weights(idx) > 0 && l > weights(idx)) ||
        (weights(idx) < 0 && l < weights(idx)))
        weights(idx) = 0
      else
        weights(idx) -= l
      idx += 1
    }
  }
}

class LassoPenalty(val penalty: Float) extends Regularizer {
  def regularize(weights: Array[Float], eta: Float) {
    var idx = 0
    while (idx < weights.size) {
      val l = penalty * eta * math.signum(weights(idx))
      if ((weights(idx) > 0 && l > weights(idx)) ||
        (weights(idx) < 0 && l < weights(idx)))
        weights(idx) = 0
      else
        weights(idx) -= l
      idx += 1
    }

  }
}

class LogisticRidgeUpdate(val penalty: Float) extends GradientUpdate {
  def gradient(point: SparseBinaryVector, label: Float, weights: Array[Float], gradientStore: Array[Float], eta: Float) {
    //same general term
    LogisticUpdate.gradient(point, label, weights, gradientStore, eta)
    //now add in the ridge penalty
    var idx = 0
    while (idx < weights.size) {
      gradientStore(idx) -= penalty * weights(idx) * eta
      idx += 1
    }
  }
}

object LogisticFunction {
  def logistic(z: Float) = 1.0 / (1 + math.exp(-z))
}