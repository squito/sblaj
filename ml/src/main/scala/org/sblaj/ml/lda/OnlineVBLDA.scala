package org.sblaj.ml.lda

import org.sblaj.{ArrayUtils, SparseBinaryVector}
import cc.mallet.types.Dirichlet.{digamma,logGamma}

/**
 * Online Variational Bayes LDA, as described in
 *
 * Hoffman. Online Learning for Latent Dirichlet Allocation. 2010
 *
 * This implementation focuses on optimizing:
 * 1) cache-locality, by using tight arrays & choice of array / loop ordering.  (Could take this further still)
 * 2) minimal memory use.  In particular, we don't store phi over all words in the document batch, b/c that could
 *  require a lot more memory.  This means we end up recomputing it for the lambda update
 * 3) iterative inner loops.  I know they're ugly, but using functional-style in the iteration has a monstrous
 *  performance penalty.  Maybe some cases could be switched to range.foreach, since that is not much worse.
 *
 * memory-used : O(nTopics * maxBatchSize + nTopics * nWords)
 */
class OnlineVBLDA(
  val nTopics: Int,
  val nWords: Int,
  val lambda: Array[Float], //nTopics * nWords
  val alpha: Float,
  val eta: Float,
  val tau0: Float,
  val kappa: Float,
  val corpusSize: Int,
  val maxBatchSize: Int
) {

  private[lda] var nextGamma: Array[Float] = new Array[Float](nTopics)
  private[lda] var prevGamma: Array[Float] = new Array[Float](nTopics)
  private[lda] var phiOneWord: Array[Float] = new Array[Float](nTopics)

  val maxNuUpdate = 0.00001f

  private[lda] val batchGammas: Array[Float] = new Array[Float](maxBatchSize * nTopics)
  //exp for "expectation", not exponential
  private[lda] val batchExpLogTheta: Array[Float] = new Array[Float](maxBatchSize * nTopics)
  private[lda] val expLogBeta: Array[Float] = new Array[Float](nTopics * nWords)

  private[lda] val lambdaUpdate: Array[Float] = new Array[Float](nTopics * nWords)

  var numUpdates = 0

  /**
   * Infer the posterior for the topic assignments for the given batch of documents, update the internal
   * set of topic-word weights.
   */
  def learnFromDocumentBatch(documentBatch: IndexedSeq[SparseBinaryVector]) {
    computeExpLogBeta()
    (0 until documentBatch.size).foreach { documentIdx =>
      inferGamma(documentBatch(documentIdx), documentIdx)
      //if I'm smart I could get rid of this arraycopy
      System.arraycopy(prevGamma, 0, batchGammas, documentIdx * nTopics, nTopics)
    }
    lambdaBatchUpdate(documentBatch)
    numUpdates += 1
  }

  /**
   * use this method to infer the topic posteriors for one document when you do NOT want to update
   * the model itself.
   *
   * Returned array is shared internal buffer
   */
  def inferDocumentTopicPosteriors(document: SparseBinaryVector): Array[Float] = {
    //assumption here is that expLogBeta already holds correct value, from last model update
    inferGamma(document, 0)
    prevGamma
  }

  /**
   * infer the posterior topic mixture of one document.  This is the "E step" as described in the paper.
   */
  private[lda] def inferGamma(wordsInDocument: SparseBinaryVector, documentBatchIdx: Int) {
    java.util.Arrays.fill(prevGamma, 1.0f)
    var itr = 0
    var nuDelta = Float.MaxValue
    while (nuDelta >= maxNuUpdate) {
      java.util.Arrays.fill(nextGamma, alpha)  //TODO alpha really should be a vector, so you can have per-topic priors
      gammaUpdate(wordsInDocument, documentBatchIdx)
      nuDelta = 0f
      var idx = 0
      while (idx < nTopics) {
        nuDelta += math.abs(nextGamma(idx) - prevGamma(idx))
        idx += 1
      }
      nuDelta = nuDelta / nTopics
//      println("nu update itr " + itr + " nuDelta = " + nuDelta)
      val tmp = nextGamma
      nextGamma = prevGamma
      prevGamma = tmp
      itr += 1
    }
    //now prevGamma holds the final assignment
  }


  private[lda] def gammaUpdate(wordsInDocument: SparseBinaryVector, documentBatchIdx: Int) {
    computeExpLogTheta(documentBatchIdx)
    wordsInDocument.foreach{word => // hopefully this foreach isn't too big a performance hit ...
      var topic = 0
      while (topic < nTopics) {
        phiOneWord(topic) = batchExpLogTheta(documentBatchIdx * nTopics + topic) + expLogBeta(topic * nWords + word)  //maybe expLogBeta should have topic on inside?
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


  private[lda] def computeExpLogTheta(documentBatchIdx: Int) {
    var nuSum = 0f
    var topic = 0
    while (topic < nTopics) {
      nuSum += prevGamma(topic)
      topic += 1
    }
    val diGammaSum = digamma(nuSum)
    topic = 0
    while (topic < nTopics) {
      batchExpLogTheta(documentBatchIdx * nTopics + topic) = (digamma(prevGamma(topic)) - diGammaSum).toFloat
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

//  /**
//   * update our estimate of lambda given the topic assignments for this document.  The "M Step" from the paper
//   */
//  private[lda] def lambdaUpdate(wordsInDocument: SparseBinaryVector, itr: Int) {
//    val rho = computeRho(itr)
//    val oneMinusRho = 1 - rho
//    wordsInDocument.foreach{word =>
//      //we recompute phi here, to save memory of storing it
//      var topic = 0
//      while (topic < nTopics) {
//        phiOneWord(topic) = expLogTheta(topic) + expLogBeta(topic * nWords + word)  //maybe expLogBeta should have topic on inside?
//        topic += 1
//      }
//      //TODO double check this is really doing the right scaling & normalization
//      ArrayUtils.stableLogNormalize(phiOneWord, 0, nTopics)
//      topic = 0
//      while (topic < nTopics) {
//        val oldLambda = lambda(topic * nWords + word)
//        val newLambda = eta + corpusSize * phiOneWord(topic)
//        lambda(topic * nWords + word) = oldLambda * oneMinusRho + rho * newLambda
//        topic += 1
//      }
//
//    }
//  }

  /**
   * update our estimate of lambda from this batch of documents.  The "M Step" from the paper.
   * assumes assumes expLogBeta & batchExpLogTheta already store correct values for this batch
   */
  private[lda] def lambdaBatchUpdate(documentBatch: IndexedSeq[SparseBinaryVector]) {
    zeroLambdaUpdate()
    computeLambdaUpdate(documentBatch)
    updateLambda(documentBatch.size)
  }

  private[lda] def zeroLambdaUpdate() {
    var idx = 0
    while (idx < nTopics * nWords) {
      lambdaUpdate(idx) = 0f
      idx += 1
    }
  }

  private[lda] def computeLambdaUpdate(documentBatch: IndexedSeq[SparseBinaryVector]) {
    (0 until documentBatch.size).foreach{documentBatchIdx =>
      documentBatch(documentBatchIdx).foreach{word =>
      //we recompute phi here, to save memory of storing it
        var topic = 0
        while (topic < nTopics) {
          phiOneWord(topic) = batchExpLogTheta(documentBatchIdx * nTopics + topic) + expLogBeta(topic * nWords + word)  //maybe expLogBeta should have topic on inside?
          topic += 1
        }
        //TODO double check this is really doing the right scaling & normalization
        ArrayUtils.stableLogNormalize(phiOneWord, 0, nTopics)
        topic = 0
        while (topic < nTopics) {
          lambdaUpdate(topic * nWords + word) += phiOneWord(topic)
          topic += 1
        }
      }
    }
  }

  private[lda] def updateLambda(batchSize: Int) {
    val rho = computeRho(numUpdates)
    val oneMinusRho = 1 - rho
    val batchSizeMultiplier = corpusSize.toFloat / batchSize
    var idx = 0
    //note that this loop needs to update *every* value of lambda, even for those words that weren't in the batch.
    while (idx < nTopics * nWords) {
      val oneLambdaUpdate = eta + batchSizeMultiplier * lambdaUpdate(idx)
      lambda(idx) = oneMinusRho * lambda(idx) + rho * oneLambdaUpdate
      idx += 1
    }
  }

  def estimateEvidenceLowerBound(documents: Iterable[SparseBinaryVector]): Float = {
    /*ELBO =
      sum_d {
        E[log p(doc_d | theta_d, beta)] +
        E[log p(theta_d | alpha) - log q(theta | gamma_d)]
      } +
      E[log p(beta | eta) - log q (beta | lambda)]

      As we aren't computing over *all* documents, we scale up the sum from the
      partial set of documents
    */

    //TODO make a version of this that will use the values for gamma that have already been computed,
    // eg. so that we can get a running estimate for each mini-batch

    var score = 0f
    var nDocs = 0
    documents.foreach{ doc =>
      score += documentELBOTerm(doc)
      nDocs += 1
    }

    //compensate for sub-sampling of population of documents
    score = score * corpusSize / nDocs

    score += modelELBOTerm()

    score
  }


  private val gammaLnAlpha = logGamma(alpha)
  private val gammaLnAlphaK = logGamma(alpha * nTopics)

  private[lda] def documentELBOTerm(document: SparseBinaryVector): Float = {
    //E[log p(doc | theta, beta)]
    //this will redo work if we've already got this stuff computed somewhere
    inferDocumentTopicPosteriors(document)
    val documentBatchIdx = 0
    var score = 0f
    document.foreach{word =>
      var phiMax = 0f
      forTopics{topic =>
        val p = batchExpLogTheta(documentBatchIdx * nTopics + topic) + expLogBeta(topic * nWords + word)  //maybe expLogBeta should have topic on inside?
        phiOneWord(topic) = p
        if (phiMax < p) phiMax = p
      }
      var phiNorm = 0f
      forTopics { topic =>
        phiNorm += math.exp(phiOneWord(topic) - phiMax).toFloat
      }
      phiNorm = (math.log(phiNorm) + phiMax).toFloat
      score += phiNorm
    }

    //E[log p(theta | alpha) - log q(theta | gamma)]
    var gammaSum = 0f
    forTopics{topic =>
      score += (alpha - prevGamma(topic)) * batchExpLogTheta(documentBatchIdx * nTopics + topic)
      score += (logGamma(prevGamma(topic)) - gammaLnAlpha).toFloat
      gammaSum += prevGamma(topic)
    }
    score += (gammaLnAlphaK - logGamma(gammaSum)).toFloat
    score
  }

  private val gammaLnEta = logGamma(eta)
  private val gammaLnEtaW = logGamma(eta * nWords)
  private[lda] def modelELBOTerm(): Float = {
    // E[log p(beta | eta) - log q (beta | lambda)]
    var score = 0f
    forTopics{topic =>
      var lambdaSum = 0f
      forWords{word =>
        val idx = topic * nWords + word
        score += (eta - lambda(idx)) * expLogBeta(idx)
        score += (logGamma(lambda(idx)) - gammaLnEta).toFloat
        lambdaSum += lambda(idx)
      }
      score += (gammaLnEtaW - logGamma(lambdaSum)).toFloat
    }
    score
  }

  //when you don't need these loops to be super optimized ...
  private def forTopics(f: Int => Unit) {(0 until nTopics).foreach{f}}
  private def forWords(f: Int => Unit) {(0 until nWords).foreach{f}}
}

object OnlineVBLDA {
  val defaultAlpha = 0.001f
  val defaultEta = 0.001f
  val defaultTau0 = 256f
  val defaultKappa = 0.5f
  val defaultMaxBatchSize = 256

  def randomLambda(nTopics: Int, nWords: Int): Array[Float] = {
    val lambda = new Array[Float](nTopics * nWords)
    (0 until nTopics).foreach{topic =>
      (0 until nWords).foreach{word =>
        lambda(topic * nWords + word) = math.random.toFloat
      }
      ArrayUtils.arrayNormalize(lambda, topic * nWords, (topic + 1) * nWords)
    }
    lambda
  }

  def apply(nTopics: Int, nWords: Int, corpusSize: Int) = {
    new OnlineVBLDA(nTopics, nWords, randomLambda(nTopics, nWords), alpha = defaultAlpha,
      eta = defaultEta, tau0 = defaultTau0, kappa = defaultKappa, corpusSize = corpusSize,
      maxBatchSize = defaultMaxBatchSize)
  }

  def showTopWordsPerTopic(lda: OnlineVBLDA, k: Int) {
    val nWords = lda.nWords
    val negLambda = lda.lambda.map{-_}
    (0 until lda.nTopics).foreach{topic =>
      val sum = ArrayUtils.arraySum(lda.lambda, topic * nWords, (topic + 1) * nWords)
      val topk = ArrayUtils.topK(lda.lambda, topic * nWords, (topic + 1) * nWords, k)
      val bottomk = ArrayUtils.topK(negLambda, topic * nWords, (topic + 1) * nWords, k)
      println("topic " + topic)
      topk.foreach{case (id, v) => println("\t" + id + ": " + v / sum)}
      println()
      bottomk.foreach{case (id, v) => println("\t" + id + ": " + (-v / sum))}
    }
  }
}
