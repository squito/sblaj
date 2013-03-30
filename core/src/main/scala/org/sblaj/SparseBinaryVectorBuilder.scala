package org.sblaj

import collection._

/**
 * Utils to help create a SparseBinaryVector.  Some of these aren't necessarily efficient,
 * and are just provided for convenience.
 */
object SparseBinaryVectorBuilder {
  def featureSetToVector(features: Set[Int]): SparseBinaryVector = {
    val arr = features.toArray.sorted
    new BaseSparseBinaryVector(arr, 0, arr.length)
  }
}
