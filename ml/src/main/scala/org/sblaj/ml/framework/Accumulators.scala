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