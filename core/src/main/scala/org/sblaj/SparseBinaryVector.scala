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
   * into(i)(j) += this(i) * vals(j)
   * @param vals
   * @param into
   */
  def indexAddInto(vals: Array[Float], into: Array[Array[Float]]) : Unit

  def asCountVector: BinaryVectorAsCountVector[SparseBinaryVector] = new GenericSparseBinaryVectorToSparseCountVector(this)

  //I can't figure out how to get more correct bounds here, but this is probably good enough
  def asCountVector(into: BinaryVectorAsCountVector[SparseBinaryVector]) {
    into.reset(this)
  }
}

class BaseSparseBinaryVector(colIds: Array[Int], startIdx: Int, endIdx: Int)
extends SparseBinaryVector with Serializable {

  var theColIds = colIds
  var theStartIdx = startIdx
  var theEndIdx = endIdx

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
    theColIds = colIds
    theStartIdx = startIdx
    theEndIdx = endIdx
  }

  def dot(x: Array[Float]) : Float = {
    var f : Float = 0
    var idx = theStartIdx
    while (idx < theEndIdx) {
      f += x(theColIds(idx))
      idx += 1
    }
    f
  }

  def mult(theta: Array[Array[Float]], into: Array[Float]) : Unit = {
    var idx = theStartIdx
    while (idx < theEndIdx) {
      val colId = theColIds(idx)
      val theta_c = theta(colId)
      var j = 0
      while (j < into.length) {
        into(j) += theta_c(j)
        j += 1
      }
      idx += 1
    }
  }

  def indexAddInto(vals: Array[Float], into: Array[Array[Float]]) {
    var rowIdx = theStartIdx
    while (rowIdx < theEndIdx) {
      val row = theColIds(rowIdx)
      var col = 0
      while (col < vals.length) {
        into(row)(col) += vals(col)
        col += 1
      }
      rowIdx += 1
    }
  }

  def nnz = theEndIdx - theStartIdx
  def get(col : Int) : Int = {
    if (Arrays.binarySearch(theColIds, theStartIdx, theEndIdx, col) < 0)
      return 0
    else
      return 1
  }

  override def toString() = {
    val sb = new StringBuilder()
    sb.append("[")
    var idx = theStartIdx
    while (idx < theEndIdx - 1) {
      sb.append(colIds(idx))
      sb.append(",")
      idx += 1
    }
    if (idx < theEndIdx)
      sb.append(colIds(idx))
    sb.append("]")
    sb.toString()
  }

  def foreach[U](f:Int => U) {
    var idx = theStartIdx
    while (idx < theEndIdx) {
      f(colIds(idx))
      idx += 1
    }
  }

  /**
   * return a new vector which only contains the given columns.
   *
   * @param colIds *sorted* array of columns to keep
   * @return
   */
  def subset(colIds: Array[Int]) : BaseSparseBinaryVector = {
    //slow, but works
    val subsetColIds = theColIds.filter{Arrays.binarySearch(colIds, _) >= 0}
    new BaseSparseBinaryVector(subsetColIds, 0, subsetColIds.length)
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