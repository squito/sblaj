package org.sblaj.boxwood

import com.quantifind.boxwood.{EnumUnionFeatureSet, EnumUnion}
import org.sblaj.MixedVector

trait BigEnumMatrix[U <: EnumUnion[Enum[_]], T <: EnumUnionFeatureSet[U]] {
  def featureSet: T
  //TODO parallel versions of this?
  def foreachBlock[X](f: (Int,EnumFeaturesMatrix[U,T]) => X)
  def nBlocks: Int
  def nRows: Long
  def getColSums: Array[Float]

  def eRowFilter(f: BEMVector[U,T] => Boolean): Array[Array[Int]] = {
    rowFilter{v =>
      //TODO come up with a version of this which doesn't require so much object creation
      f(BEMVector.wrap(v, featureSet))
    }
  }

  def rowFilter(f: MixedVector => Boolean): Array[Array[Int]]

  def rowSubset(idxs: Array[Array[Int]]): BigEnumMatrix[U,T]

  def rowSubset(f: MixedVector => Boolean): BigEnumMatrix[U,T] = {
    rowSubset(rowFilter(f))
  }

  def eRowSubset(f: BEMVector[U,T] => Boolean): BigEnumMatrix[U,T] = {
    rowSubset{v =>
      //TODO come up with a version of this which doesn't require so much object creation
      f(BEMVector.wrap(v, featureSet))
    }
  }

}
