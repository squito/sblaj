package org.sblaj.io

import org.sblaj.featurization.HashMapDictionaryCache
import java.io.PrintWriter

object DictionaryIO {

  def writeDictionary(dictionary: HashMapDictionaryCache[String], file: String) {
    val out = new PrintWriter(file)
    dictionary.foreach{case(key, code) => out.println(key + "\t" + code)}
    out.close()
  }
}
