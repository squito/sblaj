package org.sblaj.boxwood

import org.scalatest.{Matchers, FunSuite}
import com.quantifind.boxwood._


class FeatureSetMatrixTest extends FunSuite with Matchers {

  import FeatureSets._
  object LunchOrder extends BaseFeatureSet with SodaSize with Entree
  object EntreeOnly extends BaseFeatureSet with Entree


  test("vector type safety") {
    val v1 = new BMVector(10, LunchOrder)
    v1.denseCols(v1.denseStartIdx + LunchOrder.hamburger) = 7
    v1.denseCols(v1.denseStartIdx + LunchOrder.small) = 13

    def getSmallSodas(v: BMVector[SodaSize]): Float = {
      v.denseCols(v.denseStartIdx + v.featureSet.small)
    }

    getSmallSodas(v1) should be (13)

    val v2 = new BMVector(3, EntreeOnly)
    //this vector doesn't contain the right columns, compiler catches it
    "getSmallSodas(v2)" shouldNot compile
  }


  class Union_LunchOrder[T <: Enum[T]] extends EnumUnion[T]
  object Union_LunchOrder extends EnumUnionCompanion[Union_LunchOrder[_ <: Enum[_]]] {
    //for these implicits to work, they must be defined *above* their use (if in the same source file)
    implicit object EntreeWitness extends Union_LunchOrder[EntreeOrder]
    implicit object SodaWitness extends Union_LunchOrder[SodaOrder]

    val enumClasses = Seq(classOf[EntreeOrder], classOf[SodaOrder])
  }

  trait LunchOrder_FeatureSet extends EnumUnionFeatureSet[Union_LunchOrder[_ <: Enum[_]]] {
    def enumUnion = Union_LunchOrder
  }
  object LunchOrder_FeatureSet extends BaseFeatureSet with LunchOrder_FeatureSet


  class Union_SodaOnly[T <: Enum[T]] extends EnumUnion[T]
  object Union_SodaOnly extends EnumUnionCompanion[Union_SodaOnly[_ <: Enum[_]]] {
    implicit object SodaWitness extends Union_SodaOnly[SodaOrder]
    val enumClasses = Seq(classOf[SodaOrder])
  }

  trait SodaOnly_FeatureSet extends EnumUnionFeatureSet[Union_SodaOnly[_ <: Enum[_]]] {
    def enumUnion = Union_SodaOnly
  }
  object SodaOnly_FeatureSet extends BaseFeatureSet with SodaOnly_FeatureSet



  test("enum vector type safety") {
    val v2 = new BEMVector[Union_LunchOrder[_ <: Enum[_]], LunchOrder_FeatureSet](10, LunchOrder_FeatureSet)
    v2(EntreeOrder.Hamburger) = 5f
    v2(SodaOrder.Large) = 3f

    //this is the awesome part -- this method *only* knows that the vector contains a soda order.  It has no idea
    // what else is in there
    def getSodaOrder[T <: EnumUnion[Enum[_]]](v: BEMVector[T, _])
      (implicit ev: T with EnumUnion[SodaOrder]): Array[Float] = {
      SodaOrder.values().map{size => v(size)}
    }
    def getHamburgerOrder[T <: EnumUnion[Enum[_]]](v: BEMVector[T,_])
      (implicit ev: T with EnumUnion[EntreeOrder]): Float = {
      v(EntreeOrder.Hamburger)
    }

    getSodaOrder(v2) should be (Array(0f,0f,3f))
    getHamburgerOrder(v2) should be (5f)


    val v3 = new BEMVector[Union_SodaOnly[_ <: Enum[_]], SodaOnly_FeatureSet](10, SodaOnly_FeatureSet)
    v3(SodaOrder.Small) = 2f

    getSodaOrder(v3) should be (Array(2f,0f,0f))
    "getHamburgerOrder(v3)" shouldNot compile

  }

}
