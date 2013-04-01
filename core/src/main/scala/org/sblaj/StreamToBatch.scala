package org.sblaj

import collection._

/**
 * Util to take a stream and convert it into "mini-batches".  That is, given a read-once interface to data,
 * it will buffer up some amount of the data into memory, into a convenient format for reading many times.
 *
 * Looks a lot scala's GroupedIterator?  it is, except it promises to be backed by an IndexedSeq, not just a Seq.
 * lame
 */
class StreamToBatch[A](
  val maxBatchSize: Int,
  val stream: Iterator[A]
) extends Iterable[IndexedSeq[A]] {
  val buffer = new mutable.ArrayBuffer[A](maxBatchSize)
  def nextBatch: IndexedSeq[A] = {
    buffer.clear()
    while(buffer.size < maxBatchSize && stream.hasNext) {
      buffer += stream.next()
    }
    buffer
  }

  def iterator: Iterator[IndexedSeq[A]] = new BatchIterator

  private class BatchIterator extends Iterator[IndexedSeq[A]] {
    override
    def hasNext = stream.hasNext

    override
    def next = nextBatch
  }
}
