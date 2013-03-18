package org.sblaj.ml.framework

trait Accumulators[T <: Accumulators[T]] extends Serializable {
  def zero: T
  def ++=(other: T)
}


class DenseFloatArrayAccumulator(
  val arr: Array[Float],
  val length: Int
) extends Accumulators[DenseFloatArrayAccumulator] {
  def this(length: Int) = this(new Array[Float](length), length)

  def zero = new DenseFloatArrayAccumulator(new Array[Float](length), length)
  def ++=(other: DenseFloatArrayAccumulator) {
    var idx = 0
    while (idx < length) {
      arr(idx) += other.arr(idx)
      idx += 1
    }
  }
}

class Dense2DFloatArrayAccumulator(
  val arr: Array[Array[Float]],
  val nRow: Int,
  val nCol: Int
) extends Accumulators[Dense2DFloatArrayAccumulator] {
  def zero = new Dense2DFloatArrayAccumulator(Array.ofDim[Float](nRow,nCol), nRow, nCol)
  def ++=(other: Dense2DFloatArrayAccumulator) {
    var rowIdx = 0
    while (rowIdx < nRow) {
      var colIdx = 0
      while (colIdx < nCol) {
        arr(rowIdx)(colIdx) += other.arr(rowIdx)(colIdx)
        colIdx += 1
      }
      rowIdx += 1
    }
  }
}