package org.sblaj.featurization

import collection._
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap
import java.io.PrintWriter
import io.Source
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

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
  def size : Int
}

class HashMapDictionaryCache[G] extends DictionaryCache[G] {
  private val map = new Object2LongOpenHashMap[G]()
  def addMapping(name: G, code: Long) {
    map.put(name, code)
  }

  def getEnumeration() = {
    val ids = new Array[Long](map.size)
    var idx = 0
    val itr = map.values().iterator()
    while (itr.hasNext) {
      ids(idx) = itr.next()
      idx += 1
    }

    java.util.Arrays.sort(ids)
    new SortEnumeration(ids)
  }

  override def foreach[U](f: ((G, Long)) => U) {
    val itr = map.object2LongEntrySet().iterator()
    while (itr.hasNext) {
      val n = itr.next()
      f((n.getKey, n.getLongValue))
    }
  }

  def reverseMapping: Long2ObjectOpenHashMap[G] = {
    val m = new Long2ObjectOpenHashMap[G](map.size())
    foreach{case(key,code) => m.put(code, key)}
    m
  }
}

class SortEnumeration(val ids: Array[Long]) extends FeatureEnumeration with Serializable{
  override def getEnumeratedId(code:Long) = {
    val idx = java.util.Arrays.binarySearch(ids, code)
    if (idx >= 0)
      Some(idx)
    else
      None
  }

  override def getLongId(code: Int) = ids(code)

  override def subset(ids: Array[Int]) = {
    throw new UnsupportedOperationException()
  }

  def size = ids.size
}

trait CodeLookup[G] {
  def apply(code: Long): G
}

class ArrayCodeLookup[G](val arr: Array[G]) extends CodeLookup[G] {
  def apply(code: Long) = arr(code.toInt)

  def saveAsText(out: PrintWriter) {
    (0 until arr.size).foreach{idx => out.println(idx + "\t" + arr(idx))}
  }
}

object ArrayCodeLookup {
  def loadFromText(size: Int, in: Source): ArrayCodeLookup[String] = {
    val arr = new Array[String](size)
    val lines = in.getLines()
    (0 until size).foreach{idx => arr(idx) = lines.next()}
    new ArrayCodeLookup[String](arr)
  }
}