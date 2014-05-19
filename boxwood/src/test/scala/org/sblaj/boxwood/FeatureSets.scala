package org.sblaj.boxwood


import com.quantifind.boxwood._

object FeatureSets {

  trait SodaSize extends FeatureSet {
    var sodaSizeStartIdx: Int = _
    abstract override def setOffsetIndex(idx: Int) = {
      sodaSizeStartIdx = idx
      super.setOffsetIndex(idx + 3)
    }
    def small = sodaSizeStartIdx
    def medium = sodaSizeStartIdx + 1
    def large = sodaSizeStartIdx + 2
  }

  trait Entree extends FeatureSet {
    var entreeStartIdx: Int = _
    abstract override def setOffsetIndex(idx: Int) = {
      entreeStartIdx = idx
      super.setOffsetIndex(idx + 2)
    }
    def hotdog = entreeStartIdx
    def hamburger = entreeStartIdx + 1
  }

}
