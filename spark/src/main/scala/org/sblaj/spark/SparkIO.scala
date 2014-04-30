package org.sblaj.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.sblaj.BaseSparseBinaryVector
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.sblaj.io.DictionaryIO
import java.io.{DataOutputStream, File, PrintWriter}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.Progressable


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
    rdd.map(x => (NullWritable.get(), IntArrayWritable(x)))
      .saveAsHadoopFile(
        path = path,
        keyClass = classOf[NullWritable],
        valueClass = classOf[IntArrayWritable],
        outputFormatClass = classOf[SparseBinaryArrayOutputFormat]
      )
  }

  def loadSparseBinaryVectorRdd(sc: SparkContext, path: String): RDD[BaseSparseBinaryVector] = {
    val inputFormatClass = classOf[SparseBinaryArrayInputFormat]
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


class SparseBinaryArrayOutputFormat extends FileOutputFormat[NullWritable, IntArrayWritable] {

  def getRecordWriter(ignored: FileSystem,
    job: JobConf,
    name: String,
    progress: Progressable
  ): RecordWriter[NullWritable, IntArrayWritable] = {
      val file = FileOutputFormat.getTaskOutputPath(job, name)
      val fs = file.getFileSystem(job)
      val fileOut = fs.create(file, progress)
      new BytesRecordWriter(fileOut)
    }

  class BytesRecordWriter(out: DataOutputStream) extends RecordWriter[NullWritable, IntArrayWritable] {
    def close(rep: Reporter) {
      out.close()
    }
    def write(key: NullWritable,value: IntArrayWritable) {
      val arr = value.get()
      out.writeInt(arr.length)
      arr.foreach{w =>
        out.writeInt(w.asInstanceOf[IntWritable].get())
      }
    }
  }
}

class SparseBinaryArrayInputFormat extends FileInputFormat[NullWritable, IntArrayWritable] {
  def getRecordReader(
    split: InputSplit,
    job: JobConf,
    reporter:Reporter
  ): RecordReader[NullWritable,IntArrayWritable] = {
    reporter.setStatus(split.toString())
    new BytesRecordReader(job, split.asInstanceOf[FileSplit])
  }

  class BytesRecordReader(job: JobConf, split: FileSplit) extends RecordReader[NullWritable, IntArrayWritable] {
    //TODO won't work w/ a codec
    val start = split.getStart()
    val end = start + split.getLength()
    val file = split.getPath()
    // open the file and seek to the start of the split
    val fs = file.getFileSystem(job)
    val fileIn = fs.open(split.getPath())
    if (start != 0)
      fileIn.seek(start)

    var pos = start

    def close() { fileIn.close()}
    def createKey() = {NullWritable.get()}
    def createValue() = {new IntArrayWritable()}
    def getPos(): Long = pos
    def getProgress(): Float = {(pos - start) / (end - start.toFloat)}
    def next(key: NullWritable, value: IntArrayWritable): Boolean = {
      if (pos != end) {
        val length = fileIn.readInt()
        val arr = new Array[Writable](length)
        (0 until length).foreach{idx =>
          arr(idx) = new IntWritable(fileIn.readInt())
        }
        value.set(arr)
        true
      } else {
        false
      }
    }


  }
}