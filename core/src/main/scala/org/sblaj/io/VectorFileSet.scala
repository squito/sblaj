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

  def getMergedDictionary: Option[String] = {
    val p = dir + "/mergedDictionary.txt"
    if (new File(p).exists()) Some(p) else None
  }
}

case class OneVectorFileSet(
  val vectorFile: String,
  val dictionaryFile: String,
  val dimensionFile: String
)