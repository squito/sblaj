package org.sblaj

trait  MixedRowMatrix extends Traversable[MixedVector] with Serializable {
  def nRows: Int
  def nCols: Int
  def foreach[T](f: MixedVector => T)
  def rowFilter(f: MixedVector => Boolean): Array[Int]
  def rowSubset(rowIdxs: Array[Int]): MixedRowMatrix
  def rowSubset(f: MixedVector => Boolean): MixedRowMatrix
  def getColSums: Array[Float]

  def getVector: MixedVector
  def setRowVector(v: MixedVector, rowIdx: Int)

  def sizeString: String

  override def toString(): String = getClass().getSimpleName + " " + sizeString
}
