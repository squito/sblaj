package org.sblaj.ml.lda

import org.scalatest.matchers.ShouldMatchers
import org.sblaj.{SparseBinaryVectorBuilder, SparseBinaryVector}
import collection._
import org.sblaj.ml.samplers.MultinomialSampler
import org.sblaj.ml.GeneratedDataSet

/**
 *
 */

class OnlineVBLDATest extends GeneratedDataSet with ShouldMatchers {

  testDataFromFile("basic lda", "test/gendata/basic_lda") {
    // really easy test case, but should confirm basics of whether algo works correctly or not
    val nTopics = 10
    val nWords = 1000
    val nDocs = 500
    val nTopicsPerDocument = 3
    val wordsPerDocument = 100

    generateEasyDataSet(nTopics, nWords, nDocs, nTopicsPerDocument, wordsPerDocument)
  } { dataset =>
    dataset.show
  }


  def generateEasyDataSet(nTopics: Int, nWords: Int, nDocuments: Int, topicsPerDocument: Int, wordsPerDocument: Int)
  : LdaDataSet = {
    val cumTopicWordProbs = Array.ofDim[Float](nTopics, nWords)
    //easy data set, words are pretty distinct in each topic
    //first make topics
    (0 until nTopics).foreach{topic =>
      val l = topic * nWords.toDouble / nTopics
      val u = (topic + 1) * nWords.toDouble / nTopics
      (0 until nWords).foreach{word =>
        val r = math.random.toFloat
        cumTopicWordProbs(topic)(word) = if (word >= l && word < u) r else r / 10
      }
      val sum = cumTopicWordProbs(topic).sum
      var cumsum = 0.0f
      (0 until nWords).foreach{word =>
        val t = cumsum
        cumsum += cumTopicWordProbs(topic)(word) / sum
        cumTopicWordProbs(topic)(word) = t
      }
    }
    //now generate documents
    val documentTopics = new Array[IndexedSeq[Int]](nDocuments)
    val documents = new Array[SparseBinaryVector](nDocuments)
    (0 until nDocuments).foreach{document =>
      val topics = chooseTopics(nTopics, topicsPerDocument)
      documentTopics(document) = topics
      val wordsInDoc = mutable.Set[Int]()
      (0 until wordsPerDocument).foreach{ w => //we might get duplicate words, thats ok
        //1. choose topic
        val topic = topics(math.floor(math.random * topicsPerDocument).toInt)
        //2. choose a word for that topic
        val word = MultinomialSampler.sampleFromCumProbs(cumTopicWordProbs(topic))
        wordsInDoc += word
      }
      documents(document) = SparseBinaryVectorBuilder.featureSetToVector(wordsInDoc)
    }
    new LdaDataSet(cumTopicWordProbs, documentTopics, documents)
  }

  private def chooseTopics(nChoices: Int, k: Int): IndexedSeq[Int] = {
    //super inefficient, but works
    require(k < nChoices)
    val r = mutable.Set[Int]()
    while (r.size < k) {
      r += math.floor(math.random * nChoices).toInt
    }
    r.toArray.sorted
  }
}

private[lda] class LdaDataSet(
  val topicWordProbs: Array[Array[Float]], //aka lambda
  val documentTopics: IndexedSeq[IndexedSeq[Int]],
  val documents: IndexedSeq[SparseBinaryVector]
) extends Serializable {
  def show {
    (0 until documents.length).foreach{doc =>
      println("**** " + doc + " : " + documentTopics(doc).mkString(","))
      println(documents(doc).mkString(","))
      //crude way of checking if words were realy sampled from right topics
      println(documents(doc).map{_ / 100}.mkString(","))
    }
  }
}
