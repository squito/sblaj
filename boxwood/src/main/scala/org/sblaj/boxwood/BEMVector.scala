package org.sblaj.boxwood

import org.sblaj.MixedVector
import com.quantifind.boxwood.{EnumUnionFeatureSet, EnumUnion, FeatureSet}

/**
 * Boxwood
 * Enum
 * Mixed
 * Vector
 */
class BEMVector[U <: EnumUnion[Enum[_]], +T <: EnumUnionFeatureSet[U]](
  denseCols: Array[Float],
  denseStartIdx: Int,
  denseEndIdx: Int,
  sparseColIds: Array[Int],
  sparseColVals: Array[Float],
  sparseStartIdx: Int,
  sparseEndIdx: Int,
  nSparseCols: Int,
  val featureSet: T
  ) extends MixedVector(
  denseCols = denseCols,
  denseStartIdx = denseStartIdx,
  denseEndIdx = denseEndIdx,
  sparseColIds = sparseColIds,
  sparseColVals = sparseColVals,
  sparseStartIdx = sparseStartIdx,
  sparseEndIdx = sparseEndIdx,
  nSparseCols = nSparseCols,
  nDenseCols = featureSet.nFeatures
) {

  def this(nSparseCols: Int, nnz: Int, featureSet: T) {
    this(
      new Array[Float](featureSet.nFeatures),
      0,
      featureSet.nFeatures,
      new Array[Int](nSparseCols),
      new Array[Float](nSparseCols),
      0,
      nnz,
      nSparseCols,
      featureSet
    )
  }

  //just for testing ... in general this is wasteful of space for the sparse data
  private[boxwood] def this(nSparseCols: Int, featureSet: T) {
    this(nSparseCols, nSparseCols, featureSet)
  }

  def apply[E <: Enum[E]](e: E)(implicit ev: U with EnumUnion[E]): Float = {
    val idx: Int = featureSet.get(e)
    get(idx)
  }

  def update[E <: Enum[E]](e: E, v: Float)(implicit ev: U with EnumUnion[E]) {
    val idx: Int = featureSet.get(e)
    denseCols(idx + denseStartIdx) = v
  }
}

object BEMVector {
  def wrap[U <: EnumUnion[Enum[_]], T <: EnumUnionFeatureSet[U]](v: MixedVector, f: T):
    BEMVector[U,T] =
  {
    new BEMVector(
      denseCols = v.denseCols,
      denseStartIdx = v.denseStartIdx,
      denseEndIdx = v.denseEndIdx,
      sparseColIds = v.sparseColIds,
      sparseColVals = v.sparseColVals,
      sparseStartIdx = v.sparseStartIdx,
      sparseEndIdx =  v.sparseEndIdx,
      nSparseCols =  v.nSparseCols,
      featureSet = f
    )
  }

}
