package org.sblaj

/**
 *
 */

trait SparseMatrix {

  /**
   * multiply the matrix by the dense column vector y, and store the result in r
   * @param y
   * @return
   */
  def multInto(y: Array[Float], r: Array[Float]);


  /**
   * get the value of the matrix in position (x,y).
   * <p>
   * This method is NOT intended to be used by most algorithms -- its mostly for debugging
   * purposes.  In general, accessing data via this method will result in very slow
   * code
   *
   * @param x
   * @param y
   * @return
   */
  def get(x: Int, y: Int) : Float;
}
