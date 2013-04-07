package org.sblaj.io

import org.sblaj.featurization.{SortEnumeration, FeatureEnumeration, HashMapDictionaryCache}
import java.io.PrintWriter
import io.Source
import it.unimi.dsi.fastutil.longs.{LongIterator, LongAVLTreeSet}

object DictionaryIO {

  def writeDictionary(dictionary: HashMapDictionaryCache[String], file: String) {
    val out = new PrintWriter(file)
    dictionary.foreach{case(key, code) => out.println(key + "\t" + code)}
    out.close()
  }

  def readOneDictionary(file: String, mergeInto: HashMapDictionaryCache[String] = new HashMapDictionaryCache[String]): HashMapDictionaryCache[String] = {
    Source.fromFile(file).getLines().foreach{ line =>
      val p = line.lastIndexOf("\t")
      mergeInto.addMapping(line.substring(0, p), line.substring(p + 1, line.length).toLong)
    }
    mergeInto
  }

  def readMergeDictionaries(fileSet: VectorFileSet): HashMapDictionaryCache[String] = {
    fileSet.getMergedDictionaryOption.map{readOneDictionary(_)}.getOrElse{
      val numParts = fileSet.numParts
      val merged = (0 until numParts).foldLeft(new HashMapDictionaryCache[String]()){ case(dictionary,partNum) =>
        readOneDictionary(fileSet.getOneFileSet(partNum).dictionaryFile, dictionary)
      }
      writeDictionary(merged, fileSet.getMergedDictionaryFile)
      merged
    }
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
      val numParts = fileSet.numParts
      (0 until numParts).map{fileSet.getOneFileSet(_).dictionaryFile}
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
}
