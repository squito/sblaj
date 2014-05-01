package org.sblaj.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.sblaj.{BaseSparseCountVector, BaseSparseBinaryVector}
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

  def saveEnumeratedSparseCountVectorRDD(rdd: EnumeratedSparseCountVectorRDD[String], rddDir: String, localDir: String) {
    val vectorPath = rddDir + "/vectors"
    saveSparseCountVectorRdd(rdd.vectorRDD, vectorPath)

    new File(localDir).mkdirs()

    val dictionaryPath = localDir + "/dictionary"
    DictionaryIO.writeDictionary(rdd.colDictionary, dictionaryPath)

    val dimsPath = localDir + "/dims"
    saveMatrixDims(rdd.dims, dimsPath)
  }

  def saveEnumeratedSparseBinaryVectorRDD(rdd: EnumeratedSparseBinaryVectorRDD[String], rddDir: String, localDir: String) {
    val vectorPath = rddDir + "/vectors"
    saveSparseBinaryVectorRdd(rdd.vectorRDD, vectorPath)

    new File(localDir).mkdirs()

    val dictionaryPath = localDir + "/dictionary"
    DictionaryIO.writeDictionary(rdd.colDictionary, dictionaryPath)

    val dimsPath = localDir + "/dims"
    saveMatrixDims(rdd.dims, dimsPath)
  }

  /**
   * this saves the rdd in a *binary* format, that can also be read in *outside* of hadoop.  The idea is that you can
   * then grab some of these files and read them in in a simple, single-node program.  Can be read back in with
   * loadSparseBinaryVectorRdd, and also outside of hadoop by XXX THIS METHOD NEEDS TO BE WRITTEN
   */
  def saveSparseBinaryVectorRdd(rdd: RDD[BaseSparseBinaryVector], path: String) {
    rdd.map(x => (NullWritable.get(), x))
      .saveAsHadoopFile(
        path = path,
        keyClass = classOf[NullWritable],
        //valueClass is irrelevant for this use case -- probably matters if we want to write a sequence file which we
        // read in later w/ hadoop?
        valueClass = classOf[BaseSparseBinaryVector],
        outputFormatClass = classOf[SparseBinaryArrayOutputFormat]
      )
  }

  def saveSparseCountVectorRdd(rdd: RDD[BaseSparseCountVector], path: String) {
    rdd.map(x => (NullWritable.get(), x))
      .saveAsHadoopFile(
        path = path,
        keyClass = classOf[NullWritable],
        //valueClass is irrelevant for this use case -- probably matters if we want to write a sequence file which we
        // read in later w/ hadoop?
        valueClass = classOf[BaseSparseCountVector],
        outputFormatClass = classOf[SparseCountArrayOutputFormat]
      )
  }

  /**
   * load a set of binary vectors that have been saved with saveSparseBinaryVectorRdd
   */
  def loadSparseBinaryVectorRdd(sc: SparkContext, path: String): RDD[BaseSparseBinaryVector] = {
    val inputFormatClass = classOf[SparseBinaryArrayInputFormat]
    //TODO min splits
    sc.hadoopFile(
      path = path,
      inputFormatClass = inputFormatClass,
      keyClass = classOf[NullWritable],
      valueClass = classOf[BaseSparseBinaryVector]).map{x => x._2.clone()}
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

class SparseBinaryArrayOutputFormat extends FileOutputFormat[NullWritable, BaseSparseBinaryVector] {

  def getRecordWriter(ignored: FileSystem,
    job: JobConf,
    name: String,
    progress: Progressable
  ): RecordWriter[NullWritable, BaseSparseBinaryVector] = {
      val file = FileOutputFormat.getTaskOutputPath(job, name)
      val fs = file.getFileSystem(job)
      val fileOut = fs.create(file, progress)
      new BytesRecordWriter(fileOut)
    }

  class BytesRecordWriter(out: DataOutputStream) extends RecordWriter[NullWritable, BaseSparseBinaryVector] {
    def close(rep: Reporter) {
      out.close()
    }
    def write(key: NullWritable,value: BaseSparseBinaryVector) {
      out.writeInt(value.size)
      (value.startIdx until value.endIdx).foreach{idx =>
        out.writeInt(value.colIds(idx))
      }
    }
  }
}

class SparseBinaryArrayInputFormat extends FileInputFormat[NullWritable, BaseSparseBinaryVector] {
  def getRecordReader(
    split: InputSplit,
    job: JobConf,
    reporter:Reporter
  ): RecordReader[NullWritable,BaseSparseBinaryVector] = {
    reporter.setStatus(split.toString())
    new BytesRecordReader(job, split.asInstanceOf[FileSplit])
  }

  class BytesRecordReader(job: JobConf, split: FileSplit) extends RecordReader[NullWritable, BaseSparseBinaryVector] {
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
    def createValue() = {new BaseSparseBinaryVector(new Array[Int](100), 0, 0)}
    def getPos(): Long = pos
    def getProgress(): Float = {(pos - start) / (end - start.toFloat)}
    def next(key: NullWritable, value: BaseSparseBinaryVector): Boolean = {
      if (pos != end) {
        val length = fileIn.readInt()
        // we are forced to reuse the same record at this stage in any case, so only allocate a new array if we need to
        val arr = {
          if(value.colIds.length >= length)
            value.colIds
          else
            new Array[Int](length)
        }
        (0 until length).foreach{idx =>
          arr(idx) = fileIn.readInt()
        }
        value.reset(arr, 0, length)
        true
      } else {
        false
      }
    }


  }
}


class SparseCountArrayOutputFormat extends FileOutputFormat[NullWritable, BaseSparseCountVector] {

  def getRecordWriter(ignored: FileSystem,
                      job: JobConf,
                      name: String,
                      progress: Progressable
                       ): RecordWriter[NullWritable, BaseSparseCountVector] = {
    val file = FileOutputFormat.getTaskOutputPath(job, name)
    val fs = file.getFileSystem(job)
    val fileOut = fs.create(file, progress)
    new BytesRecordWriter(fileOut)
  }

  class BytesRecordWriter(out: DataOutputStream) extends RecordWriter[NullWritable, BaseSparseCountVector] {
    def close(rep: Reporter) {
      out.close()
    }
    def write(key: NullWritable,value: BaseSparseCountVector) {
      out.writeInt(value.size)
      (value.startIdx until value.endIdx).foreach{idx =>
        out.writeInt(value.colIds(idx))
        out.writeInt(value.cnts(idx))
      }
    }
  }
}

class SparseCountArrayInputFormat extends FileInputFormat[NullWritable, BaseSparseCountVector] {
  def getRecordReader(
                       split: InputSplit,
                       job: JobConf,
                       reporter:Reporter
                       ): RecordReader[NullWritable,BaseSparseCountVector] = {
    reporter.setStatus(split.toString())
    new BytesRecordReader(job, split.asInstanceOf[FileSplit])
  }

  class BytesRecordReader(job: JobConf, split: FileSplit) extends RecordReader[NullWritable, BaseSparseCountVector] {
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

    def close() { fileIn.close() }
    def createKey() = {NullWritable.get()}
    def createValue() = {new BaseSparseCountVector(new Array[Int](100), new Array[Int](100), 0, 0)}
    def getPos(): Long = pos
    def getProgress(): Float = {(pos - start) / (end - start.toFloat)}
    def next(key: NullWritable, value: BaseSparseCountVector): Boolean = {
      if (pos != end) {
        val length = fileIn.readInt()
        // we are forced to reuse the same record at this stage in any case, so only allocate a new array if we need to
        val arr = {
          if(value.colIds.length >= length)
            value.colIds
          else
            new Array[Int](length)
        }
        val cnts = {
          if(value.colIds.length >= length)
            value.colIds
          else
            new Array[Int](length)
        }
        (0 until length).foreach{idx =>
          arr(idx) = fileIn.readInt()
          cnts(idx) = fileIn.readInt()
        }
        value.reset(arr, cnts, 0, length)
        true
      } else {
        false
      }
    }


  }
}
