package org.sblaj

trait  MixedRowMatrix extends Traversable[MixedVector] with Serializable {
  def nRows: Int
  def nCols: Int
  def foreach[T](f: MixedVector => T)
  def rowFilter(f: MixedVector => Boolean): Array[Int]
  def rowSubset(rowIdxs: Array[Int]): MixedRowMatrix
  def rowSubset(f: MixedVector => Boolean): MixedRowMatrix
  def getColSums: Array[Float]
  def getDenseSumSq: Array[Float]

  /**
   * get a column vector.  *always* returns a dense vector, even if you request a sparse column
   * @param colIdx
   * @return
   */
  def getColumn(colIdx: Int): Array[Float] = {
    val into = new Array[Float](nRows)
    getColumn(colIdx, into, 0)
    into
  }

  def getColumn(colIdx: Int, into: Array[Float], pos: Int)

  def getVector: MixedVector
  def setRowVector(v: MixedVector, rowIdx: Int)

  def sizeString: String

  override def toString(): String = getClass().getSimpleName + " " + sizeString
}
