package org.sblaj.featurization

/**
 *
 */

trait DictionaryCache {
  def addMapping(name: String, code: Long)

  def getEnumeration() : FeatureEnumeration
}

trait FeatureEnumeration {
  def getEnumeratedId(code:Long) : Int
}

class HashMapDictionaryCache extends java.util.HashMap[String, Long] with DictionaryCache {
  def addMapping(name: String, code: Long) {
    put(name, code)
  }

  def getEnumeration() = {
    val ids = new Array[Long](size())
    import scala.collection.JavaConverters._
    values().asScala.zipWithIndex.foreach{
      case (code, idx) =>
        ids(idx) = code
    }

    java.util.Arrays.sort(ids)
    new SortEnumeration(ids)
  }
}

class SortEnumeration(val ids: Array[Long]) extends FeatureEnumeration {
  def getEnumeratedId(code:Long) = {
    java.util.Arrays.binarySearch(ids, code)
  }
}