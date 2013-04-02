package org.sblaj

/**
 * a vector of features, where each feature has an integer count associated with it
 */
trait SparseCountVector extends Traversable[MTuple2[Int,Int]]{
  //TODO define these functions
//  def nnz: Int
//  def get(col:Int): Int
//  def dot(x: Array[Float]): Float

  //should I also define a two-arg version:
  // foreachPair[U](f: (Int,Int) => U)
  //?? would make it easier for user to write functions w/ out calling unapply
  //behind the scenes ... dunno how expensive that is
}

class BaseSparseCountVector(
  var colIdsAndCounts: Array[Int],
  var startIdx: Int,
  var endIdx: Int
) extends SparseCountVector {
  def foreach[U](f: MTuple2[Int,Int] => U) {
    val pair = new MTuple2[Int,Int](0,0)
    var idx = startIdx
    while (idx < endIdx) {
      pair._1 = colIdsAndCounts(idx)
      pair._2 = colIdsAndCounts(idx + 1)
      f(pair)
      idx += 2
    }
  }
}
