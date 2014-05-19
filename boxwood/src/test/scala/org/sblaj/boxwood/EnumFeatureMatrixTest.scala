package org.sblaj.boxwood

import org.scalatest.{Matchers, FunSuite}
import com.quantifind.boxwood.{BaseFeatureSet, EnumUnionFeatureSet, EnumUnionCompanion, EnumUnion}
import org.sblaj.StdMixedRowMatrix


object enums {
  class Union_LunchOrder[T <: Enum[T]] extends EnumUnion[T]
  object Union_LunchOrder extends EnumUnionCompanion[Union_LunchOrder[_ <: Enum[_]]] {
    implicit object EntreeWitness extends Union_LunchOrder[EntreeOrder]
    implicit object SodaWitness extends Union_LunchOrder[SodaOrder]
    implicit object SatisfactionWitness extends Union_LunchOrder[CustomerSatisfaction]

    val enumClasses = Seq(classOf[EntreeOrder], classOf[SodaOrder], classOf[CustomerSatisfaction])
  }

  trait LunchOrder_FeatureSet extends EnumUnionFeatureSet[Union_LunchOrder[_ <: Enum[_]]] {
    def enumUnion = Union_LunchOrder
  }
  object LunchOrder_FeatureSet extends BaseFeatureSet with LunchOrder_FeatureSet



  class Union_EntreeSatisfaction[T <: Enum[T]] extends EnumUnion[T]
  object Union_EntreeSatisfaction extends EnumUnionCompanion[Union_EntreeSatisfaction[_ <: Enum[_]]] {
    implicit object EntreeWitness extends Union_EntreeSatisfaction[EntreeOrder]
    implicit object SatisfactionWitness extends Union_EntreeSatisfaction[CustomerSatisfaction]

    val enumClasses = Seq(classOf[EntreeOrder], classOf[CustomerSatisfaction])
  }

  trait EntreeSatisfaction_FeatureSet extends EnumUnionFeatureSet[Union_EntreeSatisfaction[_ <: Enum[_]]] {
    def enumUnion = Union_EntreeSatisfaction
  }
  object EntreeSatisfaction_FeatureSet extends BaseFeatureSet with EntreeSatisfaction_FeatureSet

}




class EnumFeatureMatrixTest extends FunSuite with Matchers {
  import enums._
  type U = Union_LunchOrder[_ <: Enum[_]]
  type F = LunchOrder_FeatureSet

  def genData: EnumFeaturesMatrix[U,F] = {
    val matrix = new StdMixedRowMatrix(
      nDenseCols = LunchOrder_FeatureSet.nFeatures,
      nSparseCols = 100,
      maxNnz = 100000,
      maxRows = 1000
    )

    //TODO fill data


    new EnumFeaturesMatrix[U,F](matrix, LunchOrder_FeatureSet)
  }

}


object EntreeSatisfactionFilter {

  //NOTE: none of these functions know *anything* about soda whatsoever.  they don't reference the "complete"
  // enum union at all, so we can add whatever we want to it, and these will be just fine

  def withHamburgers[U <: EnumUnion[Enum[_]]](v: BEMVector[U,_])
    (implicit ev: U with EnumUnion[EntreeOrder]): Boolean = {
    v(EntreeOrder.Hamburger) >= 2
  }

  def withGoodTaste[U <: EnumUnion[Enum[_]]](v: BEMVector[U,_])
    (implicit ev: U with EnumUnion[CustomerSatisfaction]): Boolean = {
    v(CustomerSatisfaction.Taste) >= 4
  }

  def withBadTaste[U <: EnumUnion[Enum[_]]](v: BEMVector[U,_])
    (implicit ev: U with EnumUnion[CustomerSatisfaction]): Boolean = {
    v(CustomerSatisfaction.Taste) <= 2
  }

  def hamburgerGoodOrBadTaste[U <: EnumUnion[Enum[_]]](v: BEMVector[U,_])
    (implicit ev: U with EnumUnion[EntreeOrder] with EnumUnion[CustomerSatisfaction]): Boolean = {
    withHamburgers(v) && (withGoodTaste(v) || withBadTaste(v))
  }

}
