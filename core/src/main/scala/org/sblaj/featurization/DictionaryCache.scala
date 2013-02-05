package org.sblaj.featurization

import collection._

/**
 *
 */

trait DictionaryCache[G] extends Traversable[(G,Long)] {
  def addMapping(name: G, code: Long)

  def getEnumeration() : FeatureEnumeration
}

trait FeatureEnumeration {
  def getEnumeratedId(code:Long) : Option[Int]
  def getLongId(code: Int) : Long
  def subset(ids: Array[Int]) : FeatureEnumeration
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
  override def getEnumeratedId(code:Long) = {
    val idx = java.util.Arrays.binarySearch(ids, code)
    if (idx >= 0)
      Some(idx)
    else
      None
  }

  override def getLongId(code: Int) = ids(code)

  override def subset(ids: Array[Int]) = {
    //TODO
    null
  }
}