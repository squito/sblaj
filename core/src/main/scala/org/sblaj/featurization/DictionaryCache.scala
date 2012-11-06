package org.sblaj.featurization

import collection._

/**
 *
 */

trait DictionaryCache[G] {
  def addMapping(name: G, code: Long)

  def getEnumeration() : FeatureEnumeration
}

trait FeatureEnumeration {
  def getEnumeratedId(code:Long) : Int
}

class HashMapDictionaryCache[G] extends mutable.HashMap[G, Long] with DictionaryCache[G] {
  def addMapping(name: G, code: Long) {
    put(name, code)
  }

  def getEnumeration() = {
    val ids = new Array[Long](size)
    values.zipWithIndex.foreach{
      case (code, idx) =>
        ids(idx) = code
    }

    java.util.Arrays.sort(ids)
    new SortEnumeration(ids)
  }
}

class SortEnumeration(val ids: Array[Long]) extends FeatureEnumeration with Serializable {
  def getEnumeratedId(code:Long) = {
    java.util.Arrays.binarySearch(ids, code)
  }
}