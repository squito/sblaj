package org.sblaj.ml.framework

import org.sblaj.SparseBinaryVector

trait IterativeAccumulatingAlgo[A <: Accumulators[A]] {
  def initializeAccumulators: A
  //TODO template for data types
  def oneIteration(data: Traversable[SparseBinaryVector], acc: A)

  /**
   * called at the end of each iteration with the iteration number and the fully merged
   * accumulator values.  Return true if we should continue with another iteration, else false
   */
  def iterationEnd(itr: Int, acc:A) : Boolean
}
