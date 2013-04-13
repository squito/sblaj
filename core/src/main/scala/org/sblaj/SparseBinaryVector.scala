package org.sblaj

import featurization.FeatureEnumeration
import java.util.Arrays

/**
 *
 */

trait SparseBinaryVector extends Traversable[Int]{
  //TODO can the dense matrix class also be generified, and still performant?

  def nnz : Int

  /**
   * return the value of the given column (either 0 or 1)
   * <p>
   * Note that though this method is provided for convenience, in general you should *NOT*
   * use it, because it will be slow
   * @param col
   * @return
   */
  def get(col: Int) : Int

  def dot(x:Array[Float]) : Float

  /**
   * compute (this)x(theta), and *add* the result to <code>into</code>
   *
   * eg., take this as a row vector, and multiply it by the dense matrix
   * theta, and add the result to what is in the given array
   *
   * @param theta a dense matrix
   * @param into add the result into this array
   */
  def mult(theta:Array[Array[Float]], into: Array[Float]) : Unit

  /**
   * Compute the outer product of this vector with a dense vector, and add it into the given 2d matrix
   *
   * into(i)(j) += this(i) * vals(j)
   *
   * @param vals
   * @param into
   */
  def outerPlus(vals: Array[Float], into: Array[Array[Float]]) : Unit

  def asCountVector: BinaryVectorAsCountVector[SparseBinaryVector] = new GenericSparseBinaryVectorToSparseCountVector(this)

  //I can't figure out how to get more correct bounds here, but this is probably good enough
  def asCountVector(into: BinaryVectorAsCountVector[SparseBinaryVector]) {
    into.reset(this)
  }
}

class BaseSparseBinaryVector(var colIds: Array[Int], var startIdx: Int, var endIdx: Int)
extends SparseBinaryVector with Serializable {

  /**
   * reset this object to point to a different vector
   * <p>
   * Note that this method should be used very sparingly -- it violates a lot of the tenants
   * of functional programming.  However, the performance benefits of avoiding recreating
   * new objects and forcing more GC are worth it sometimes
   * @param colIds
   * @param startIdx
   * @param endIdx
   */
  def reset(colIds: Array[Int], startIdx: Int, endIdx: Int) {
    this.colIds = colIds
    this.startIdx = startIdx
    this.endIdx = endIdx
  }

  def dot(x: Array[Float]) : Float = {
    var f : Float = 0
    var idx = startIdx
    while (idx < endIdx) {
      f += x(colIds(idx))
      idx += 1
    }
    f
  }

  def mult(theta: Array[Array[Float]], into: Array[Float]) : Unit = {
    var idx = startIdx
    while (idx < endIdx) {
      val colId = colIds(idx)
      val theta_c = theta(colId)
      var j = 0
      while (j < into.length) {
        into(j) += theta_c(j)
        j += 1
      }
      idx += 1
    }
  }

  def outerPlus(vals: Array[Float], into: Array[Array[Float]]) {
    var rowIdx = startIdx
    while (rowIdx < endIdx) {
      val row = colIds(rowIdx)
      var col = 0
      while (col < vals.length) {
        into(row)(col) += vals(col)
        col += 1
      }
      rowIdx += 1
    }
  }

  def nnz = endIdx - startIdx
  def get(col : Int) : Int = {
    if (Arrays.binarySearch(colIds, startIdx, endIdx, col) < 0)
      return 0
    else
      return 1
  }

  override def toString() = {
    val sb = new StringBuilder()
    sb.append("[")
    var idx = startIdx
    while (idx < endIdx - 1) {
      sb.append(colIds(idx))
      sb.append(",")
      idx += 1
    }
    if (idx < endIdx)
      sb.append(colIds(idx))
    sb.append("]")
    sb.toString()
  }

  def foreach[U](f:Int => U) {
    var idx = startIdx
    while (idx < endIdx) {
      f(colIds(idx))
      idx += 1
    }
  }

  /**
   * return a new vector which only contains the given columns.
   *
   * @param colsToKeep *sorted* array of columns to keep
   * @return
   */
  def subset(colsToKeep: Array[Int]) : BaseSparseBinaryVector = {
    //slow, but works
    val subsetColIds = colsToKeep.filter{Arrays.binarySearch(colIds, startIdx, endIdx, _) >= 0}
    new BaseSparseBinaryVector(subsetColIds, 0, subsetColIds.length)
  }

  def dot(other: BaseSparseBinaryVector): Int = {
    //ugh, wish I could move this up higher in interfaces, but I want to know the concrete type of other for impl ...
    var thisIdx = startIdx
    var otherIdx = other.startIdx
    var sum = 0
    while (thisIdx < endIdx && otherIdx < other.endIdx) {
      if (colIds(thisIdx) == other.colIds(otherIdx)) {
        sum += 1
        thisIdx += 1
        otherIdx += 1
      } else if (colIds(thisIdx) < other.colIds(otherIdx)) {
        thisIdx += 1
      } else {
        otherIdx += 1
      }
    }
    sum
  }
}


//TODO figure out to get rid of the Int / Long types, and use @specialized.
// however, not all of the methods make sense w/ Long col ids (eg., dot(Array[Float]), so probably need another base type
class LongSparseBinaryVector(val colIds: Array[Long], val startIdx: Int, val endIdx: Int) extends Serializable {
  def enumerateInto(into: Array[Int], pos: Int, enumeration: FeatureEnumeration) = {
    var sourceIdx = startIdx
    var targetIdx = pos
    while (sourceIdx < endIdx) {
      enumeration.getEnumeratedId(colIds(sourceIdx)).foreach{intCode =>
        into(targetIdx) = intCode
        targetIdx += 1
      }
      sourceIdx += 1
    }
    Arrays.sort(into, pos, targetIdx)
    targetIdx
  }
}

class LongSparseBinaryVectorWithRowId(val rowId: Long, override val colIds: Array[Long],override val startIdx: Int,
                                      override val endIdx: Int)
  extends LongSparseBinaryVector(colIds, startIdx, endIdx) with RowId


trait RowId {
  val rowId: Long
}

trait BinaryVectorAsCountVector[-T <: SparseBinaryVector] extends SparseCountVector {
  def reset(vector: T)
}

/**
 * simple way of converting any SparseBinaryVector to a SparseCountVector.  Could probably be improved
 * for specific implementations
 */
class GenericSparseBinaryVectorToSparseCountVector(var binaryVector: SparseBinaryVector)
  extends SparseCountVector
  with BinaryVectorAsCountVector[SparseBinaryVector]
{
  def foreach[U](f: MTuple2[Int,Int] => U) {
    val pair = new MTuple2[Int,Int](0,0)
    binaryVector.foreach{colId =>
      pair._1 = colId
      pair._2 = 1
      f(pair)
    }
  }
  def reset(vector: SparseBinaryVector) {binaryVector = vector}
}