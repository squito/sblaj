package org.sblaj.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.sblaj.BaseSparseBinaryVector
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.sblaj.io.DictionaryIO
import java.io.{File, PrintWriter}


/**
 * save vectors & dictionaries to hdfs using spark
 */
object SparkIO {

  def saveEnumeratedSparseVectorRDD(rdd: EnumeratedSparseVectorRDD[String], rddDir: String, localDir: String) {
    val vectorPath = rddDir + "/vectors"
    saveSparseBinaryVectorRdd(rdd.vectorRDD, vectorPath)

    new File(localDir).mkdirs()

    val dictionaryPath = localDir + "/dictionary"
    DictionaryIO.writeDictionary(rdd.colDictionary, dictionaryPath)

    val dimsPath = localDir + "/dims"
    saveMatrixDims(rdd.dims, dimsPath)
  }

  def saveSparseBinaryVectorRdd(rdd: RDD[BaseSparseBinaryVector], path: String) {
    rdd.map(x => (NullWritable.get(), IntArrayWritable(x))).saveAsSequenceFile(path)
  }

  def loadSparseBinaryVectorRdd(sc: SparkContext, path: String): RDD[BaseSparseBinaryVector] = {
    val inputFormatClass = classOf[SequenceFileInputFormat[NullWritable, IntArrayWritable]]
    //TODO min splits
    sc.hadoopFile(
      path = path,
      inputFormatClass = inputFormatClass,
      keyClass = classOf[NullWritable],
      valueClass = classOf[IntArrayWritable])
      .map{pair =>
        val arr = pair._2.get().map{_.asInstanceOf[IntWritable].get()}
        new BaseSparseBinaryVector(arr, 0, arr.length)
    }
  }

  def saveMatrixDims(dims: RowMatrixPartitionDims, path: String) {
    val out = new PrintWriter(path)

    out.println(dims.totalDims.nRows)
    out.println(dims.totalDims.nCols)
    out.println(dims.totalDims.nnz)
    out.println()

    dims.partitionDims.toIndexedSeq.sortBy{_._1}.foreach{case (part, (nRows, nnz)) =>
      out.println(part)
      out.println(nRows)
      out.println(nnz)
      out.println()
    }

    out.close()
  }

}

class IntArrayWritable extends ArrayWritable(classOf[IntWritable])
object IntArrayWritable {
  def apply(arr: Array[Int], startIdx: Int, endIdx: Int): IntArrayWritable = {
    val r = new Array[Writable](endIdx - startIdx)
    (startIdx until endIdx).foreach{idx =>
      r(idx - startIdx) = new IntWritable(arr(idx))
    }
    val w = new IntArrayWritable()
    w.set(r)
    w
  }
  def apply(v: BaseSparseBinaryVector): IntArrayWritable = {
    apply(v.colIds, v.startIdx, v.endIdx)
  }
}
