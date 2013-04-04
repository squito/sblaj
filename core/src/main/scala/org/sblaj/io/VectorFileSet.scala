package org.sblaj.io

/**
 *
 */

case class VectorFileSet(
  val vectorFile: String,
  val dictionaryFile: String,
  val dimensionFile: String
)

object VectorFileSet {
  def forDir(dir: String) = {
    VectorFileSet(dir + "/vectors.bin", dir + "/dictionary.txt", dir + "/dims.txt")
  }
}
