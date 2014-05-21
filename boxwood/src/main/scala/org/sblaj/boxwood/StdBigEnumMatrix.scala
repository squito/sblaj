package org.sblaj.boxwood

import com.quantifind.boxwood.{EnumUnionFeatureSet, EnumUnion}
import org.sblaj.{ArrayUtils, MixedVector}

class StdBigEnumMatrix[U <: EnumUnion[Enum[_]], T <: EnumUnionFeatureSet[U]](
    val featureSet: T,
    val parts: Array[EnumFeaturesMatrix[U,T]]
) extends BigEnumMatrix[U,T] {

  def nBlocks = parts.size
  def nRows = parts.map{_.nRows}.sum

  def rowFilter(f: MixedVector => Boolean): Array[Array[Int]] = {
    parts.map{_.rowFilter(f)}
  }

  def foreachBlock[X](f: (Int,EnumFeaturesMatrix[U,T]) => X) = {
    (0 until parts.length).foreach{idx =>
      f(idx, parts(idx))
    }
  }

  def getColSums: Array[Float] = {
    val colSums = parts(0).getColSums
    (1 until colSums.length).foreach{idx =>
      val nextCs = parts(idx).getColSums
      ArrayUtils.+=(nextCs, colSums)
    }
    colSums
  }

  def rowSubset(rowIdxs: Array[Array[Int]]): StdBigEnumMatrix[U,T] = {
    require(rowIdxs.length == parts.length)
    val newParts = new Array[EnumFeaturesMatrix[U,T]](parts.length)
    (0 until rowIdxs.length).foreach{partIdx =>
      newParts(partIdx) = parts(partIdx).rowSubset(rowIdxs(partIdx))
    }
    new StdBigEnumMatrix[U,T](featureSet, newParts)
  }
}
