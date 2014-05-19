package org.sblaj.boxwood

import org.scalatest.{Matchers, FunSuite}
import com.quantifind.boxwood.{BaseFeatureSet, EnumUnionFeatureSet, EnumUnionCompanion, EnumUnion}
import org.sblaj.StdMixedRowMatrix
import scala.util.Random


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

  val nRows = 1000
  val nSparse = 100

  def genData: EnumFeaturesMatrix[U,F] = {

    //TODO need an easier way to create dictionary, even if just for testing ...
    val goodWords = Set(
      "bbq",
      "mmmm"
    )
    val badWords = Set(
      "soggy",
      "cold"
    )
    val otherWords = Set(
      "cheese",
      "buns"
    )
    val sparseDictionary: Map[String, Int] = (goodWords ++ badWords ++ otherWords).
      foldLeft((Map[String,Int](), LunchOrder_FeatureSet.nFeatures)){case ((m,n), s) =>
        val nextMap = (m + (s -> n))
      (nextMap, n + 1)
    }._1

    val matrix = new StdMixedRowMatrix(
      nDenseCols = LunchOrder_FeatureSet.nFeatures,
      nSparseCols = nSparse,
      maxNnz = nSparse * nRows,  //way more than actually necessary
      maxRows = nRows
    )

    val v = matrix.getVector
    val rng = new Random()
    (0 until nRows).foreach{rowIdx =>
      matrix.setRowVector(v, rowIdx)
      //not sure why I have to put in type parameters here ... type inference works in other cases
      val ev = BEMVector.wrap[Union_LunchOrder[_ <: Enum[_]], LunchOrder_FeatureSet](v, LunchOrder_FeatureSet)
      SodaOrder.values().foreach{sodaSize => ev(sodaSize) = math.random.toFloat}
      val wordIds = if (rowIdx % 2 == 0) {
        ev(EntreeOrder.Hamburger) = 2.0f
        var words = Set[String]()
        if (rowIdx % 3 == 0) {
          ev(CustomerSatisfaction.Taste) = 5.0f
          words += goodWords.toIndexedSeq(rng.nextInt(goodWords.size))
        } else {
          ev(CustomerSatisfaction.Taste) = 1.0f
          words += badWords.toIndexedSeq(rng.nextInt(badWords.size))
        }
        words += otherWords.toIndexedSeq(rng.nextInt(otherWords.size))
        words.map{sparseDictionary}.toIndexedSeq.sorted
      } else {
        IndexedSeq[Int]()
      }

      val start = matrix.sparseRowStartIdx(rowIdx)
      val end = start + wordIds.length
      (0 until wordIds.length).foreach{sparseIdx =>
        val t = sparseIdx + start
        matrix.sparseColIds(t) = wordIds(sparseIdx)
        matrix.sparseColVals(t) = 1.0f
      }
      matrix.sparseRowStartIdx(rowIdx + 1) = end
    }
    matrix.setSize(nRows, matrix.sparseRowStartIdx(nRows))

    new EnumFeaturesMatrix[U,F](matrix, LunchOrder_FeatureSet)
  }


  test("basic matrix api") {
    val m = genData
    m.nRows should be (nRows)
    m.nCols should be (nSparse + LunchOrder_FeatureSet.nFeatures)
    m.featureSet should be (LunchOrder_FeatureSet)
    m.eRowFilter{v =>
      v(EntreeOrder.Hamburger) > 1
    }.foreach{rowIdx => (rowIdx % 2) should be (0)}
  }


  test("demo type-safe slice & dice w/ general math") {
    val data = genData
    import EntreeSatisfactionFilter._

    val happyHamburger = data.rowSubset(data.eRowFilter{v => withGoodTaste(v) && withHamburgers(v)})
    //note that if you pass in a closure directly, somehow or the other, the implicit magic is all hidden from you
    val happy2 = data.eRowFilter{v =>
      v(EntreeOrder.Hamburger) >= 2 && v(CustomerSatisfaction.Taste) >= 4
    }
    val unHappyHamburger = data.rowSubset(data.eRowFilter{v => withBadTaste(v) && withHamburgers(v)})

    """data.eRowFilter{v =>
      v(IceCreamOrder.Vanilla) >= 1.0f
    }""" shouldNot compile

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
