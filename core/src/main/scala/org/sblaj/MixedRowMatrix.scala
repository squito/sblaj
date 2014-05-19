package org.sblaj

trait  MixedRowMatrix extends Traversable[MixedVector] {
  def nRows: Int
  def nCols: Int
  def foreach[T](f: MixedVector => T)
  def rowFilter(f: MixedVector => Boolean): Array[Int]
  def rowSubset(rowIdxs: Array[Int]): MixedRowMatrix
  def getColSums: Array[Float]
}
