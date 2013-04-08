package org.sblaj.io

import org.sblaj.{SparseBinaryRowMatrix, BaseSparseBinaryVector, LongSparseBinaryVector}
import java.io._
import io.Source
import org.sblaj.featurization.{RowMatrixCounts}
import it.unimi.dsi.fastutil.io.{FastBufferedOutputStream, FastBufferedInputStream}

/**
 *
 */
object VectorIO {
  def append(v: LongSparseBinaryVector, out: DataOutputStream) {
    out.writeInt(v.endIdx - v.startIdx)
    (v.startIdx until v.endIdx).foreach{idx => out.writeLong(v.colIds(idx))}
  }

  def longBinaryRowIterator(fileSet: VectorFileSet): Iterator[LongSparseBinaryVector] = {
    fileSet.oneFileSetItr.map{
      onePart => longBinaryRowIterator(onePart)
    }.reduce{_ ++ _}
  }

  /**
   * return an iterator which returns one vector at a time.  Doesn't try to build all vectors together into a big
   * matrix
   */
  def longBinaryRowIterator(onePart: OneVectorFileSet): Iterator[LongSparseBinaryVector] = {
    val matrixCounts = loadMatrixCounts(onePart)
    val in = new DataInputStream(new FastBufferedInputStream(new FileInputStream(onePart.vectorFile)))
    new LongSparseRowVectorDataInputIterator(in, matrixCounts.nRows.toInt)
  }

  def writeMatrixCounts(counts:RowMatrixCounts, file: String) {
    val dimsOut = new PrintWriter(file)
    dimsOut.println(counts.nRows)
    dimsOut.println(counts.nCols)
    dimsOut.println(counts.nnz)
    dimsOut.close()
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
    val partCounts = (0 until longVectors.numParts).map{partNum =>
      val buffer = new Array[Int](idEnum.size)  //max possible, probably much bigger than needed, but should be OK
      val f = new java.io.File(intVectors.getOneFileSet(partNum).vectorFile)
      f.getParentFile.mkdirs()
      println("beginning to enumerate into " + f)
      val out = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(f)))
      val longs = longBinaryRowIterator(longVectors.getOneFileSet(partNum))
      var nnz = 0
      var nRows = 0
      longs.foreach{lv =>
        val endIdx = lv.enumerateInto(buffer, 0, idEnum)
        val iv = new BaseSparseBinaryVector(buffer, 0, endIdx)
        nRows += 1
        nnz += endIdx
        append(iv, out)
      }
      out.close
      //write out dims that include the full range of colIds
      new java.io.File(intVectors.getOneFileSet(partNum).dimensionFile).getParentFile.mkdirs()
      val counts = new RowMatrixCounts(nRows, idEnum.size, nnz)
      writeMatrixCounts(counts, intVectors.getOneFileSet(partNum).dimensionFile)
      counts
    }
    val totalCounts = partCounts.reduce{(a,b) => RowMatrixCounts(a.nRows + b.nRows, a.nCols, a.nnz + b.nnz)}
    writeMatrixCounts(totalCounts, intVectors.getMergedDimFile)
  }

  def append(v: BaseSparseBinaryVector, out: DataOutputStream) {
    out.writeInt(v.theEndIdx - v.theStartIdx)
    (v.theStartIdx until v.theEndIdx).foreach{idx => out.writeInt(v.theColIds(idx))}
  }


  def loadMatrix(intVectors: VectorFileSet): SparseBinaryRowMatrix = {
    val totalCounts = loadMatrixCounts(Source.fromFile(intVectors.getMergedDimFile))
    val mat = new SparseBinaryRowMatrix(
      nMaxRows =  totalCounts.nRows.toInt,
      nColumns = totalCounts.nCols.toInt,
      nMaxNonZeros =  totalCounts.nnz.toInt
    )
    var colIdx = 0
    var rowIdx = 0
    (0 until intVectors.numParts).foreach{part =>
      val newIdxs = readSparseBinaryVectors(
        intVectors.getOneFileSet(part),
        cols = mat.colIds,
        startColIdx = colIdx,
        rowStarts = mat.rowStartIdx,
        startRowIdx = rowIdx
      )
      rowIdx = newIdxs._1
      colIdx = newIdxs._2
    }
    mat.setSize(rowIdx, colIdx)
    mat
  }

  def readOneSparseBinaryVector(in: DataInput, into: Array[Int], pos: Int): Int = {
    val len = in.readInt()
    var p = pos
    while (p < pos + len) {
      into(p) = in.readInt()
      p += 1
    }
    p
  }

  def readSparseBinaryVectors(
    onePart: OneVectorFileSet,
    cols: Array[Int],
    startColIdx: Int,
    rowStarts: Array[Int],
    startRowIdx: Int
  ): (Int,Int) = {
    val counts = loadMatrixCounts(onePart)
    var rowIdx = startRowIdx
    var colIdx = startColIdx
    val in = new DataInputStream(new FastBufferedInputStream(new FileInputStream(onePart.vectorFile)))
    while (rowIdx < counts.nRows + startRowIdx) {
      rowStarts(rowIdx) = colIdx
      colIdx = readOneSparseBinaryVector(in, cols, colIdx)
      rowIdx += 1
    }
    in.close()
    (rowIdx, colIdx)
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
