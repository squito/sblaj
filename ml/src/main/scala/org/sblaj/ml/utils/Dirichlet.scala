package org.sblaj.ml.utils

/**
 * shamelessly copied from Mallet
 * http://www.cs.umass.edu/~mccallum/mallet
 *
 * maybe I should move this to a standalone project ...
 */
object Dirichlet {

  val DIGAMMA_COEF_1 = 1/12.0
  val DIGAMMA_COEF_2 = 1/120.0
  val DIGAMMA_COEF_3 = 1/252.0
  val DIGAMMA_COEF_4 = 1/240.0
  val DIGAMMA_COEF_5 = 1/132.0
  val DIGAMMA_COEF_6 = 691/32760.0
  val DIGAMMA_COEF_7 = 1/12.0;
  val DIGAMMA_COEF_8 = 3617/8160.0;
  val DIGAMMA_COEF_9 = 43867/14364.0;
  val DIGAMMA_COEF_10 = 174611/6600.0;

  val DIGAMMA_LARGE = 9.5;
  val DIGAMMA_SMALL = .000001;

  val EULER_MASCHERONI = -0.5772156649015328606065121;

  /** Calculate digamma using an asymptotic expansion involving Bernoulli numbers. */
  def digamma(v: Double): Double = {
    var z = v
    //This is based on matlab code by Tom Minka
    var psi = 0.0

    if (z < DIGAMMA_SMALL) {
      psi = EULER_MASCHERONI - (1 / z); // + (PI_SQUARED_OVER_SIX * z);
      /*for (int n=1; n<100000; n++) {
      psi += z / (n * (n + z));
      }*/
      return psi;
    }

    while (z < DIGAMMA_LARGE) {
      psi -= 1 / z;
      z += 1;
    }

    val invZ = 1/z;
    val invZSquared = invZ * invZ;

    psi += math.log(z) - .5 * invZ
    - invZSquared * (DIGAMMA_COEF_1 - invZSquared *
      (DIGAMMA_COEF_2 - invZSquared *
        (DIGAMMA_COEF_3 - invZSquared *
          (DIGAMMA_COEF_4 - invZSquared *
            (DIGAMMA_COEF_5 - invZSquared *
              (DIGAMMA_COEF_6 - invZSquared *
                DIGAMMA_COEF_7))))));

    return psi;
  }
}
