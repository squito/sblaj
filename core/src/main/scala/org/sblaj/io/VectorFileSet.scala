package org.sblaj.io
import java.io.File

class VectorFileSet(val dir: String) {
  def getOneFileSet(partNum: Int): OneVectorFileSet = {
    //hadoop friendly naming
    val p = "%05d".format(partNum)
    val o = OneVectorFileSet(
      dir + "/vectors.bin.parts/part-" + p,
      dir + "/dictionary.txt.parts/part-" + p,
      dir + "/dims.txt.parts/part-" + p
    )
    o
  }

  def mkdirs() {
    val f = getOneFileSet(0)
    List(f.vectorFile, f.dictionaryFile, f.dimensionFile).foreach{
      new java.io.File(_).getParentFile.mkdirs()
    }
  }

  def getMergedDictionaryFile: String = dir + "/mergedDictionary.txt"

  def getMergedDictionaryOption: Option[String] = {
    val p = getMergedDictionaryFile
    if (new File(p).exists()) Some(p) else None
  }

  def numParts: Int = new java.io.File(dir + "/vectors.bin.parts/").list().length
  def oneFileSetItr: Iterator[OneVectorFileSet] = {
    (0 until numParts).toIterator.map{getOneFileSet}
  }
}

case class OneVectorFileSet(
  val vectorFile: String,
  val dictionaryFile: String,
  val dimensionFile: String
)