package org.sblaj.compare

import org.sblaj.{SubsetMixedRowMatrix, StdMixedRowMatrix, MixedRowMatrix}
import org.sblaj.util.BoundedPriorityQueue

/**
 * simple class for getting a quick comparison of two slices of data.
 *
 * This is making a *lot* of arbitrary choices -- the point is that its a convenient starting point, not that
 * it completely solves the problem, since ultimately that involves a lot of modeling decisions
 */
class MatrixCompare(comparisons: Array[ColumnCompare]) {


  def compare(target: MixedRowMatrix, ref: MixedRowMatrix): MatrixComparison = {
    require(comparisons.length == target.nCols)
    require(comparisons.length == ref.nCols)

    val ts = target.getColSums
    val tsSq = target.getDenseSumSq
    val tN = target.nRows

    val rs = ref.getColSums
    val rsSq = ref.getDenseSumSq
    val rN = ref.nRows


    val ratioComparisons = new TopTests(50, "ratio tests")(CompareIndexAndValueByValue)
    val tComparisons = new TopTests(50, "tstat")(CompareIndexAndValueByAbsValue)
    (0 until comparisons.length).foreach{colIdx =>
      val cm = comparisons(colIdx)
      cm match {
        case SmoothedRatioOfRates(priorStrength) =>
          val rOpt = smoothedRatioOfRates(ts(colIdx), tN, rs(colIdx), rN, priorStrength)
          rOpt.foreach{r =>
            ratioComparisons += (colIdx -> r.toFloat)
          }
        case TTest =>
          val tOpt = tstat(ts(colIdx), tsSq(colIdx), tN, rs(colIdx), rsSq(colIdx), rN)
          tOpt.foreach{t =>
            tComparisons += (colIdx -> t.toFloat)
          }
      }
    }

    // might be nice if we also included some *really* simple notion of how separable the two classes were, eg
    // w/ naive bayes or something.  of course, that can be misleading, b/c most likely you've still got the columns
    // that define the actual differences in there, so maybe its not even worth the trouble

    MatrixComparison(
      target,
      ref,
      comparisons,
      Seq(tComparisons, ratioComparisons) //TODO more usable format ...
    )

  }



  def tstat(
    targetSum: Double,
    targetSumSq: Double,
    targetN: Double,
    refSum: Double,
    refSumSq: Double,
    refN: Double
  ): Option[Double] = {
    //welch's t-test, unequal variances
    //https://en.wikipedia.org/wiki/Welch%27s_t_test
    val tr = targetSum / targetN
    val rr = refSum / refN
    val num = tr - rr
    val s1sq = (targetSumSq/targetN) - (tr * tr)
    val s2sq = (refSumSq / refN) - (rr * rr)
    val denom = math.sqrt((s1sq / targetN) + (s2sq / refN))
    if (denom == 0 && num == 0) //probably should be < eps
      None
    else
      Some(num / denom)
  }

  def smoothedRatioOfRates(
    targetCounts: Double,
    targetN: Double,
    refCounts: Double,
    refN: Double,
    priorStrength: Double
  ): Option[Double] = {
    if (targetN > 0 && refN > 0 && refCounts > 0) {
      val denom = refCounts / refN
      val smoothedNum = (targetCounts + denom * priorStrength) / (targetN + priorStrength)
      Some(smoothedNum / denom)
    } else {
      None
    }
  }

}

object MatrixCompare {
  def guessMeta(matrix: MixedRowMatrix): MatrixMeta = {
    matrix match {
      case s:StdMixedRowMatrix =>
        val colMeta = new Array[ColumnMeta](s.nCols)
        (0 until s.nDenseCols).foreach{colIdx =>
          colMeta(colIdx) = Proportion
        }
        (s.nDenseCols until s.nCols).foreach{colIdx =>
          colMeta(colIdx) = Binary  //this is a terrible guess, since the column can actual have arbitrary value ...
        }
        MatrixMeta(colMeta)
      case sub: SubsetMixedRowMatrix =>
        guessMeta(sub.parent)
      case _ =>
        throw new RuntimeException("sorry, don't know how to guess column meta for matrix: " + matrix)
    }
  }

  def guessMetaToCompare(meta: ColumnMeta, ratioOfRatesPrior: Float = 5): ColumnCompare = {
    meta match {
      case Binary => SmoothedRatioOfRates(ratioOfRatesPrior)
      case Proportion => TTest
    }
  }

  def guessColumnCompare(matrix: MixedRowMatrix): Array[ColumnCompare] = {
    guessMeta(matrix).colMeta.map{guessMetaToCompare(_, 5)}
  }


  def compare(target: MixedRowMatrix, reference: MixedRowMatrix): MatrixComparison = {
    val meta = guessColumnCompare(reference)
    val comparator = new MatrixCompare(meta)
    comparator.compare(target, reference)
  }
}

case class MatrixComparison(
  targetMatrix: MixedRowMatrix,
  referenceMatrix: MixedRowMatrix,
  colComparisons: Array[ColumnCompare],
  comparisons: Seq[Any]
)


object CompareIndexAndValueByValue extends Ordering[(Int, Float)] {
  def compare(l: (Int, Float), r: (Int, Float)): Int = {
    l._2 compareTo r._2
  }
}

object CompareIndexAndValueByAbsValue extends Ordering[(Int, Float)] {
  def compare(l: (Int, Float), r: (Int, Float)): Int = {
    math.abs(l._2) compareTo math.abs(r._2)
  }
}


case class MatrixMeta(colMeta: Array[ColumnMeta])


sealed trait ColumnMeta
case object Binary extends ColumnMeta
case object Proportion extends ColumnMeta
//counts, normal, etc.

sealed trait ColumnCompare
case class SmoothedRatioOfRates(priorStrength: Float) extends ColumnCompare
case object TTest extends ColumnCompare
//quantiles at values (sampled), chi-sq over quantiles, index99, conf interval for diff of normal means, etc



class TopTests(val maxSize: Int, val name: String)(ordering: Ordering[(Int,Float)]) extends BoundedPriorityQueue[(Int,Float)] {
  implicit override val ord = ordering
  val revOrd = ord.reverse
  override def toString: String = {
    "Top Comparisons, by " + name + ": \n" +
      toIndexedSeq.sorted(revOrd).zipWithIndex.map{case((colIdx, v), sortIdx) =>
        s"${sortIdx + 1}. $colIdx ($v)"
      }.mkString("\n")
  }
}
