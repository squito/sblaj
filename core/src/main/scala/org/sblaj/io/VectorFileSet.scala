package org.sblaj.io
import java.io.File
import org.sblaj.featurization.RowMatrixCounts
import io.Source

class VectorFileSet(val dir: String) extends Iterable[OneVectorFileSet] {
  def getOneFileSet(partNum: Int): OneVectorFileSet = {
    //hadoop friendly naming
    val p = "%05d".format(partNum)
    val o = OneVectorFileSet(
      dir + "/vectors.bin.parts/part-" + p,
      dir + "/dictionary.txt.parts/part-" + p,
      dir + "/dims.txt.parts/part-" + p,
      partNum
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

  private def numParts: Int = new java.io.File(dir + "/vectors.bin.parts/").list().length
  def iterator: Iterator[OneVectorFileSet] = {
    (0 until numParts).toIterator.map{getOneFileSet}
  }

  def getMergedDimFile: String = dir + "/mergedDims.txt"

  def sampleFirstParts(nParts: Int): VectorFileSet = {
    new SampledVectorFileSet(dir, (0 until nParts).toSet)
  }

  def sampleSizeLimitIntVectors(maxBytes: Long): VectorFileSet = {
    var bytesSoFar = 0l
    val parts = takeWhile{part =>
      val partCounts = VectorIO.loadMatrixCounts(part)
      val partSize = (partCounts.nnz + partCounts.nRows) * 4l
      if (bytesSoFar + partSize < maxBytes) {
        bytesSoFar += partSize
        true
      }
      else
        false
    }.map{_.partNum}.toSet
    new SampledVectorFileSet(dir, parts)
  }

  def loadCounts: RowMatrixCounts = {
    VectorIO.loadMatrixCounts(Source.fromFile(getMergedDimFile))
  }
}

class SampledVectorFileSet(dir: String, val sampledParts: Set[Int]) extends VectorFileSet(dir) {
  override def iterator: Iterator[OneVectorFileSet] = {
    sampledParts.toIterator.map{getOneFileSet}
  }
  override def loadCounts: RowMatrixCounts = {
    var nRows = 0
    var nnz = 0
    var nCols = 0
    iterator.foreach{part =>
      val partCounts = VectorIO.loadMatrixCounts(part)
      nRows += partCounts.nRows.toInt
      nnz += partCounts.nnz.toInt
      nCols = partCounts.nCols.toInt
    }
    RowMatrixCounts(nRows, nCols, nnz)
  }

}

case class OneVectorFileSet(
  val vectorFile: String,
  val dictionaryFile: String,
  val dimensionFile: String,
  val partNum: Int
)