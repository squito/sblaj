package org.sblaj.ml.lda

import org.sblaj.{ArrayUtils, SparseBinaryVector}
import org.sblaj.ml.utils.Dirichlet.digamma

/**
 * Online Variational Bayes LDA, as described in
 *
 * Hoffman. Online Learning for Latent Dirichlet Allocation. 2010
 *
 * This implementation focuses on optimizing:
 * 1) cache-locality, by using tight arrays & choice of array / loop ordering.  (Could take this further still)
 * 2) minimal memory use.  In particular, we don't store phi over all words in the document batch, b/c that could
 *  require a lot more memory, even though we end up recomputing it for the lambda update
 * 3) iterative inner loops.  I know they're ugly, but using functional-style in the iteration has a monstrous
 *  performance penalty.  Maybe some cases could be switched to range.foreach, since that is roughly comparable.
 *
 */
class OnlineVBLDA(
  val nTopics: Int,
  val nWords: Int,
  val lambda: Array[Float], //nTopics * nWords
  val alpha: Float,
  val eta: Float,
  val tau0: Float,
  val kappa: Float
) {
  private[lda] var nextGamma: Array[Float] = new Array[Float](nTopics)
  private[lda] var prevGamma: Array[Float] = new Array[Float](nTopics)
  private[lda] var phiOneWord: Array[Float] = new Array[Float](nTopics)

  val maxNuUpdate = 0.00001f

  //exp for "expectation", not exponential
  private[lda] val expLogTheta: Array[Float] = new Array[Float](nTopics)
  private[lda] val expLogBeta: Array[Float] = new Array[Float](nTopics * nWords)


  /**
   * infer the posterior topic mixture of one document.  This is the "E step" as described in the paper.
   */
  def inferGamma(wordsInDocument: SparseBinaryVector) {
    java.util.Arrays.fill(prevGamma, 1.0f)
    //TODO copute expLogBeta
    var itr = 0
    var nuDelta = Float.MaxValue
    while (nuDelta >= maxNuUpdate) {
      java.util.Arrays.fill(nextGamma, alpha)  //TODO alpha really should be a vector, so you can have per-topic priors
      gammaUpdate(wordsInDocument)
      nuDelta = 0f
      var idx = 0
      while (idx < nTopics) {
        nuDelta += math.abs(nextGamma(idx) - prevGamma(idx))
        idx += 1
      }
      nuDelta = nuDelta / nTopics
      println("nu update itr " + itr + " nuDelta = " + nuDelta)
      val tmp = nextGamma
      nextGamma = prevGamma
      prevGamma = tmp
    }
    //now prevGamma holds the final assignment
  }


  private[lda] def gammaUpdate(wordsInDocument: SparseBinaryVector) {
    computeExpLogTheta()
    wordsInDocument.foreach{word => // hopefully this foreach isn't too big a performance hit ...
      var topic = 0
      while (topic < nTopics) {
        phiOneWord(topic) = expLogTheta(topic) + expLogBeta(topic * nWords + word)  //maybe expLogBeta should have topic on inside?
        topic += 1
      }
      //TODO double check this is really doing the right scaling & normalization
      ArrayUtils.stableLogNormalize(phiOneWord, 0, nTopics)
      topic = 0
      while (topic < nTopics) {
        nextGamma(topic) += phiOneWord(topic)
        topic += 1
      }
    }
  }


  private[lda] def computeExpLogTheta() {
    var nuSum = 0f
    var topic = 0
    while (topic < nTopics) {
      nuSum += prevGamma(topic)
      topic += 1
    }
    val diGammaSum = digamma(nuSum)
    topic = 0
    while (topic < nTopics) {
      expLogTheta(topic) = (digamma(prevGamma(topic)) - diGammaSum).toFloat
      topic += 1
    }
  }

  private[lda] def computeExpLogBeta() {
    var topic = 0
    while (topic < nTopics) {
      var lambdaSum = 0f
      var word = 0
      while (word < nWords) {
        lambdaSum += lambda(topic * nWords + word)
        word += 1
      }
      val digammaLambdaSum = digamma(lambdaSum)
      word = 0
      while (word < nWords) {
        expLogBeta(topic * nWords + word) = (digamma(lambda(topic * nWords + word)) - digammaLambdaSum).toFloat
        word += 1
      }
      topic += 1
    }
  }

  private[lda] def computeRho(t: Int): Float = math.pow((tau0 + t), -kappa).toFloat

  /**
   * update our estimate of lambda given the topic assignments for this document
   */
  private[lda] def lambdaUpdate(wordsInDocument: SparseBinaryVector, numDocuments: Int, itr: Int) {
    val rho = computeRho(itr)
    val oneMinusRho = 1 - rho
    wordsInDocument.foreach{word =>
      //we recompute phi here, to save memory of storing it
      var topic = 0
      while (topic < nTopics) {
        phiOneWord(topic) = expLogTheta(topic) + expLogBeta(topic * nWords + word)  //maybe expLogBeta should have topic on inside?
        topic += 1
      }
      //TODO double check this is really doing the right scaling & normalization
      ArrayUtils.stableLogNormalize(phiOneWord, 0, nTopics)
      topic = 0
      while (topic < nTopics) {
        val oldLambda = lambda(topic * nWords + word)
        val newLambda = eta + numDocuments * phiOneWord(topic)
        lambda(topic * nWords + word) = oldLambda * oneMinusRho + rho * newLambda
        topic += 1
      }

    }
  }
}
