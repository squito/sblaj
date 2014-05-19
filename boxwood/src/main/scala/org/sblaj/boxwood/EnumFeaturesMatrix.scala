package org.sblaj.boxwood

import org.sblaj.{MixedVector, StdMixedRowMatrix}
import com.quantifind.boxwood.{EnumUnionFeatureSet, EnumUnion}

class EnumFeaturesMatrix[U <: EnumUnion[Enum[_]], +T <: EnumUnionFeatureSet[U]](
  val matrix: StdMixedRowMatrix,
  val featureSet: T
) {

  /**
   * filter rows, making use of the enum over the features.
   *
   * Not as efficient as first
   * translating your filter function to an operation over idxs, but in most cases
   * this probably doesn't matter, since in both cases you're paying the cost of the closure
   * anyway
   */
  def eRowFilter(f: BEMVector[U,T] => Boolean): Array[Int] = {
    rowFilter{v =>
      //TODO come up with a version of this which doesn't require so much object creation
      f(BEMVector.wrap(v, featureSet))
    }
  }

  def rowFilter(f: MixedVector => Boolean): Array[Int] = {
    matrix.rowFilter(f)
  }
}
