package org.sblaj.boxwood

import org.sblaj.{StdMixedRowMatrix, MixedRowMatrix, MixedVector}
import com.quantifind.boxwood.{EnumUnionFeatureSet, EnumUnion}

class EnumFeaturesMatrix[U <: EnumUnion[Enum[_]], +T <: EnumUnionFeatureSet[U]](
  val matrix: MixedRowMatrix,
  val featureSet: T
) extends MixedRowMatrix with Serializable {


  def this(
    nSparseCols: Int,
    maxNnz: Int,
    maxRows: Int,
    featureSet: T
  ) {
    this(
      new StdMixedRowMatrix(
        featureSet.nFeatures,
        nSparseCols,
        maxNnz,
        maxRows
      ),
      featureSet
    )
  }


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

  def rowSubset(idxs: Array[Int]): EnumFeaturesMatrix[U,T] = {
    new EnumFeaturesMatrix[U,T](
      matrix.rowSubset(idxs),
      featureSet
    )
  }

  def rowSubset(f: MixedVector => Boolean): EnumFeaturesMatrix[U,T] = {
    rowSubset(rowFilter(f))
  }

  def eRowSubset(f: BEMVector[U,T] => Boolean): EnumFeaturesMatrix[U,T] = {
    rowSubset(eRowFilter(f))
  }


  def nCols: Int = matrix.nCols
  def nRows: Int = matrix.nRows

  def foreach[T](f: MixedVector => T) {
    matrix.foreach(f)
  }

  def getColSums: Array[Float] = matrix.getColSums

  def getVector: MixedVector = {
    matrix.getVector
  }

  def setRowVector(vector: MixedVector, rowIdx: Int) {
    //TODO this doesn't really make sense -- we need to template on the row type
    matrix.setRowVector(vector, rowIdx)
  }

  def getVector(rowIdx: Int): BEMVector[U,T] = {
    val v = matrix.getVector
    matrix.setRowVector(v, rowIdx)
    BEMVector.wrap[U,T](v, featureSet)
  }

  def sizeString = matrix.sizeString

}
