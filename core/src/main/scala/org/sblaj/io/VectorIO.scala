package org.sblaj.io

import org.sblaj.{BaseSparseBinaryVector, LongSparseBinaryVector}
import java.io._
import io.Source
import org.sblaj.featurization.{RowMatrixCounts}
import it.unimi.dsi.fastutil.io.BinIO

/**
 *
 */
object VectorIO {
  def append(v: LongSparseBinaryVector, out: DataOutputStream) {
    out.writeInt(v.endIdx - v.startIdx)
    (v.startIdx until v.endIdx).foreach{idx => out.writeLong(v.colIds(idx))}
  }

  def rowIterator(fileSet: VectorFileSet): Iterator[LongSparseBinaryVector] = {
    fileSet.oneFileSetItr.map{
      onePart => rowIterator(onePart)
    }.reduce{_ ++ _}
  }

  /**
   * return an iterator which returns one vector at a time.  Doesn't try to build all vectors together into a big
   * matrix
   */
  def rowIterator(onePart: OneVectorFileSet): Iterator[LongSparseBinaryVector] = {
    val matrixCounts = loadMatrixCounts(onePart)
    val in = new DataInputStream(new BufferedInputStream(new FileInputStream(onePart.vectorFile)))
    new LongSparseRowVectorDataInputIterator(in, matrixCounts.nRows.toInt)
  }

  def loadMatrixCounts(oneFileSet: OneVectorFileSet): RowMatrixCounts = {
    loadMatrixCounts(Source.fromFile(oneFileSet.dimensionFile))
  }

  def loadMatrixCounts(in: Source): RowMatrixCounts = {
    val raw = in.getLines().toArray
    RowMatrixCounts(raw(0).toLong, raw(1).toLong, raw(2).toLong)
  }


  def convertToIntVectors(longVectors: VectorFileSet, intVectors: VectorFileSet) {
    val idEnum = DictionaryIO.idEnumeration(longVectors)
    (0 until longVectors.numParts).foreach{partNum =>
      val buffer = new Array[Int](idEnum.size)  //max possible, probably much bigger than needed, but should be OK
      val f = new java.io.File(intVectors.getOneFileSet(partNum).vectorFile)
      f.getParentFile.mkdirs()
      println("beginning to enumerate into " + f)
      val out = new DataOutputStream(new FileOutputStream(f))
      val longs = rowIterator(longVectors.getOneFileSet(partNum))
      longs.foreach{lv =>
        val endIdx = lv.enumerateInto(buffer, 0, idEnum)
        val iv = new BaseSparseBinaryVector(buffer, 0, endIdx)
        append(iv, out)
      }
      out.close
    }
  }

  def append(v: BaseSparseBinaryVector, out: DataOutputStream) {
    out.write(v.theEndIdx - v.theStartIdx)
    (v.theStartIdx until v.theEndIdx).foreach{idx => out.write(v.theColIds(idx))}
  }


}

private[io] class LongSparseRowVectorDataInputIterator(
  in: DataInput,
  val nRecords: Int
) extends Iterator[LongSparseBinaryVector]{
  var nSoFar = 0
  def next = {
    val length = in.readInt()
    nSoFar += 1
    val arr = new Array[Long](length)
    (0 until length).foreach{idx => arr(idx) = in.readLong()}
    new LongSparseBinaryVector(arr, 0, length)
  }
  def hasNext = nSoFar < nRecords

}