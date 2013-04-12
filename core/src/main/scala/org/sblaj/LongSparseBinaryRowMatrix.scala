package org.sblaj

import featurization.RowMatrixCounts
import io.{VectorIO, VectorFileSet}
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, Long2IntMap}
import scala.io.Source

trait LongSparseBinaryRowMatrix extends Traversable[LongSparseBinaryVector] {
  def dims: RowMatrixCounts
  def getColSums: Long2IntMap
}

class FileLongSparseBinaryRowMatrix(
  val fileSet: VectorFileSet
) extends LongSparseBinaryRowMatrix {
  def foreach[U](f: LongSparseBinaryVector => U) {
    VectorIO.longBinaryRowIterator(fileSet).foreach(f)
  }
  lazy val dims = {
    //TODO the nCols in this is totally incorrect ... should there be another type?
    fileSet.foldLeft(new RowMatrixCounts(0,0,0)){case(countsSoFar, part) =>
      val partCounts = VectorIO.loadMatrixCounts(part)
      //cols is totally inaccurate
      new RowMatrixCounts(countsSoFar.nRows + partCounts.nRows, 0, countsSoFar.nnz + partCounts.nnz)
    }
  }
  def getColSums = {
    val m = new Long2IntOpenHashMap()
    foreach{vector =>
      (vector.startIdx until vector.endIdx).foreach{idx =>
        val col = vector.colIds(idx)
        val prev = if (m.containsKey(col)) m.get(col) else 0
        m.put(col, prev + 1)
      }
    }
    m
  }
}