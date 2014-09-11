package org.sblaj.io

import java.io.PrintWriter

import it.unimi.dsi.fastutil.longs._
import org.sblaj.featurization.{DictionaryCache, FeatureEnumeration, HashMapDictionaryCache, SortEnumeration}
import org.sblaj.util.Logging

import scala.io.{BufferedSource, Source}

object DictionaryIO extends Logging {

  def writeDictionary(dictionary: DictionaryCache[String], file: String) {
    val out = new PrintWriter(file)
    writeDictionary(dictionary, out)
    out.close()
  }

  def writeDictionary(dictionary: DictionaryCache[String], out: PrintWriter): Unit = {
    dictionary.foreach{case(key, code) => out.println(key + "\t" + code)}
  }

  def writeEnumeration(enum: Array[Long], long2Name: Function[Long, Seq[String]], file: String) {
    val out = new PrintWriter(file)
    writeEnumeration(enum, long2Name, out)
    out.close()
  }

  def writeEnumeration(enum: Array[Long], long2Name: Function[Long, Seq[String]], out: PrintWriter) {
    enum.foreach{ longCode =>
      val names = long2Name(longCode)
      out.println(names.mkString("|OR|"))
    }
  }

  def entryIterator(file:String):Iterator[(String,Long)] = {
    entryIterator(Source.fromFile(file))
  }

  def entryIterator(source:BufferedSource):Iterator[(String,Long)] = {
    source.getLines().map{ line=>
      val p = line.lastIndexOf("\t")
      (line.substring(0,p), line.substring(p+1, line.length).toLong)
    }
  }

  def readOneDictionary(file: String): HashMapDictionaryCache[String] = {
    readOneDictionary(Source.fromFile(file), mergeInto = new HashMapDictionaryCache[String])
  }

  def readOneDictionary(file: String, mergeInto: HashMapDictionaryCache[String]): HashMapDictionaryCache[String] = {
    readOneDictionary(Source.fromFile(file), mergeInto = mergeInto)
  }


  def readOneDictionary(source: BufferedSource, mergeInto: HashMapDictionaryCache[String] = new HashMapDictionaryCache[String]): HashMapDictionaryCache[String] = {
    entryIterator(source).zipWithIndex.foreach{ case((key,code),idx) =>
      if (idx % 1e6.toInt == 0)
        info("reading line " + idx)
      mergeInto.addMapping(key,code)
    }
    mergeInto
  }

  def buildMergedDictionary(fileSet:VectorFileSet): HashMapDictionaryCache[String] = {
    val merged = new HashMapDictionaryCache[String]()
    fileSet.foreach { onePart =>
      val f = onePart.dictionaryFile
      println("merging: " + f)
      readOneDictionary(f, merged)
      println("merged dictionary size = " + merged.size)
    }
    writeDictionary(merged, fileSet.getMergedDictionaryFile)
    merged
  }

  def readMergeDictionaries(fileSet: VectorFileSet): HashMapDictionaryCache[String] = {
    fileSet.getMergedDictionaryOption.map{readOneDictionary(_)}.getOrElse{
      buildMergedDictionary(fileSet)
    }
  }

  def readRevDictionary(fileSet: VectorFileSet): Long2ObjectOpenHashMap[String] = {
    if (fileSet.getMergedDictionaryOption.isEmpty) buildMergedDictionary(fileSet)
    val m = new Long2ObjectOpenHashMap[String]()
    Source.fromFile(fileSet.getMergedDictionaryOption.get).getLines().zipWithIndex.foreach{case(line,idx) =>
      if (idx % 1e6.toInt == 0) info("reading line " + idx)
      val p = line.lastIndexOf("\t")
      m.put(line.substring(p + 1, line.length).toLong, line.substring(0,p))
    }
    m
  }

  def idIterator(file: String) : LongIterator = {
    val lines = Source.fromFile(file).getLines()
    new LongIterator {
      def next() = nextLong()

      def skip(p1: Int) = 0

      def remove() {}

      def nextLong() = {
        val line = lines.next()
        val p = line.lastIndexOf("\t")
        line.substring(p + 1, line.length).toLong
      }

      def hasNext = lines.hasNext
    }
  }

  def idIterator(fileSet: VectorFileSet): LongIterator = {
    val files = fileSet.getMergedDictionaryOption.map{Seq(_)}.getOrElse {
      fileSet.iterator.map{_.dictionaryFile}.toSeq
    }
    val subItrs = files.map{idIterator(_)}
    new LongIterator {
      var itrItr: Iterator[LongIterator] = subItrs.iterator
      var curItr: LongIterator = itrItr.next()
      def next() = nextLong()

      def skip(p1: Int) = 0

      def remove() {}

      def nextLong() = {
        hasNext
        curItr.nextLong()
      }

      def hasNext = {
        while(!curItr.hasNext && itrItr.hasNext) {
          curItr = itrItr.next
        }
        curItr.hasNext
      }
    }
  }

  def idSet(fileSet: VectorFileSet): LongAVLTreeSet = {
    val idSet = new LongAVLTreeSet()
    val itr = idIterator(fileSet)
    while (itr.hasNext) {
      idSet.add(itr.nextLong())
    }
    idSet
  }

  def idEnumeration(fileSet: VectorFileSet): FeatureEnumeration = {
    new SortEnumeration(idSet(fileSet).toLongArray)
  }

  /**
   * build a set of ids that match the given predicate.  Generally, the idea
   * behind this is that you are going to keep a pretty small set of entries
   */
  def matchingIds(fileSet: VectorFileSet)(predicate: ((String,Long)) => Boolean): LongSet = {
    matchingIds(fileSet, new LongOpenHashSet())(predicate)
  }
  def matchingIds(fileSet: VectorFileSet, idSet: LongSet)(predicate: ((String,Long)) => Boolean): LongSet = {
    fileSet.foreach{part =>
      entryIterator(part.dictionaryFile).filter(predicate).foreach{case(_,code) => idSet.add(code)}
    }
    idSet
  }
}
