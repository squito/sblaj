package org.sblaj.boxwood

import org.sblaj.MixedVector
import com.quantifind.boxwood.FeatureSet

class BMVector[+T <: FeatureSet](
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

  //just for testing ... in general this is wasteful of space for the sparse data
  private[boxwood] def this(nSparseCols: Int, featureSet: T) {
    this(
      new Array[Float](featureSet.nFeatures),
      0,
      featureSet.nFeatures,
      new Array[Int](nSparseCols),
      new Array[Float](nSparseCols),
      0,
      nSparseCols,
      nSparseCols,
      featureSet
    )
  }


}
