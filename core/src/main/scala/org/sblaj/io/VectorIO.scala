package org.sblaj.io

import org.sblaj.LongSparseBinaryVector
import java.io.DataOutputStream

/**
 *
 */
object VectorIO {
  def append(v: LongSparseBinaryVector, out: DataOutputStream) {
    out.write(v.endIdx - v.startIdx)
    (v.startIdx until v.endIdx).foreach{idx => out.writeLong(v.colIds(idx))}
  }
}
