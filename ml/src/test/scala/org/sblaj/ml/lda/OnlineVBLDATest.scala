package org.sblaj.ml.lda

import org.scalatest.matchers.ShouldMatchers
import org.sblaj.{StreamToBatch, SparseBinaryVectorBuilder, SparseBinaryVector}
import collection._
import org.sblaj.ml.samplers.MultinomialSampler
import org.sblaj.ml.GeneratedDataSet
import org.apache.log4j.Logger

/**
 *
 */

class OnlineVBLDATest extends GeneratedDataSet with ShouldMatchers {

  val logger = Logger.getLogger(classOf[OnlineVBLDATest])

  testDataFromFile("basic lda", "test/gendata/basic_lda") {
    // really easy test case, but should confirm basics of whether algo works correctly or not
    val nTopics = 10
    val nWords = 1000
    val nDocs = 5000
    val nTopicsPerDocument = 3
    val wordsPerDocument = 100

    generateEasyDataSet(nTopics, nWords, nDocs, nTopicsPerDocument, wordsPerDocument)
  } { dataset =>
    val lda = OnlineVBLDA(dataset.nTopics, dataset.nWords, dataset.nDocuments)
    val nIterations = 100
    (0 to nIterations).foreach{ itr =>
      val batches = new StreamToBatch(lda.maxBatchSize, dataset.documents.iterator)
      batches.iterator.foreach{documentBatch =>
        lda.learnFromDocumentBatch(documentBatch)
      }
      if (itr % 10 == 0)
        OnlineVBLDA.showTopWordsPerTopic(lda, 10)
      logger.info("finished iteration " + itr)
    }
    (0 until 10).foreach{doc =>
      lda.inferDocumentTopicPosteriors(dataset.documents(doc))
      dataset.showOneDoc(doc)
      val s = lda.prevGamma.sum
      println("\t" + lda.prevGamma.zipWithIndex.map{case(alpha, idx) => idx -> alpha / s}.mkString(","))
    }
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
//        val r = math.random.toFloat
        cumTopicWordProbs(topic)(word) = if (word >= l && word < u) 20 else 1
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
  val topicWordProbs: Array[Array[Float]],
  val documentTopics: IndexedSeq[IndexedSeq[Int]],
  val documents: IndexedSeq[SparseBinaryVector]
) extends Serializable {
  def show {
    (0 until documents.length).foreach{showOneDoc}
  }
  def showOneDoc(doc:Int){
    println("**** " + doc + " : " + documentTopics(doc).mkString(","))
    println(documents(doc).mkString(","))
    //crude way of checking if words were realy sampled from right topics
    println(documents(doc).map{_ / 100}.groupBy{t => t}.map{case(k,l) => (k,l.size)}.toArray.sortBy{_._1}.mkString(","))
  }

  def nTopics: Int = topicWordProbs.length
  def nWords: Int = topicWordProbs(0).length
  def nDocuments: Int = documents.length
}
