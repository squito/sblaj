package org.sblaj.io

import java.io.{FileInputStream, DataInputStream, File}
import org.sblaj.{SparseBinaryRowMatrix, MatrixDims}
import scala.collection.Map
import scala.io.Source
import it.unimi.dsi.fastutil.io.FastBufferedInputStream

object EnumVectorIO {

  def loadMatrixDims(file: File): RowMatrixPartitionDims = {
    val in = Source.fromFile(file).getLines()

    //first 3 lines are overall dims
    val nRows = in.next().toInt
    val nCols = in.next().toInt
    val nnz = in.next().toInt

    require(in.next()=="")

    //next its repeating sets of (nrows, nnz) per file
    var partitionDims = Map[Int,(Long,Long)]()
    in.grouped(4).foreach{partitionLines =>
      val partNum = partitionLines(0).toInt
      val partRows = partitionLines(1).toInt
      val partNnz = partitionLines(2).toInt
      partitionDims += partNum -> (partRows, partNnz)
    }
    new RowMatrixPartitionDims(MatrixDims(nRows, nCols, nnz), partitionDims)
  }

  def loadDictionary(file: File, nCols: Int): Array[String] = {
    val r = new Array[String](nCols)
    val in = Source.fromFile(file).getLines()
    in.zipWithIndex.foreach{case(line,idx) =>
      val name = line.split("\t")(0)  //ignore the long code
      r(idx) = name
    }
    r
  }

  def loadLimitedMatrix(dir: File, maxSizeBytes: Long = 2e9.toLong): (Array[String], SparseBinaryRowMatrix) = {

    //TODO TONS of duplication w/ VectorIO ... need to refactor

    val dims = loadMatrixDims(new File(dir, "dims"))
    val nCols = dims.totalDims.nCols.toInt
    val dictionary = loadDictionary(new File(dir, "dictionary"), nCols)
    val nChars = dictionary.map{_.length}.sum
    val approxDictSpaceBytes = nChars * 2 + dictionary.length * 16  //very very approximate
    var usedSpace = approxDictSpaceBytes.toLong
    val partsToLoad = dims.partitionDims.toIndexedSeq.sortBy{_._1}.takeWhile{case(part,(nRows, nnz)) =>
      val nextSpace = usedSpace + (nRows + nnz) * 4
      if (nextSpace < maxSizeBytes) {
        usedSpace = nextSpace
        true
      } else {
        false
      }
    }

    println(s"will use up to partition ${partsToLoad.last._1}, using $usedSpace memory.  $approxDictSpaceBytes for dictionary")

    val (totalRows, totalNnz) = partsToLoad.foldLeft((0l,0l)){case(prev, (_,(nRows, nnz))) => (prev._1 + nRows, prev._2 + nnz)}
    println("creating matrix of size " + totalRows.toInt + "," + totalNnz.toInt + "," + nCols)
    val matrix = new SparseBinaryRowMatrix(totalRows.toInt, totalNnz.toInt, nCols)
    var nextRow = 0
    var nextNnz = 0
    val vectorDir = new File(dir, "vectors")
    partsToLoad.foreach{case(part, (nRows, nnz)) =>
      val f = new File(vectorDir, "part-%05d".format(part))
      println("reading " + f + ", expecting " + nRows + " rows.  Starting from " + nextNnz + ", expecting to read " + nnz)
      val initialNnzPos = nextNnz
      nextNnz = appendOneVectorFile(f, matrix, nextRow, nextNnz, nRows.toInt)
      val nnzRead = nextNnz - initialNnzPos
      assert(nnzRead == nnz.toInt, s"$nnzRead read entries was not equal to expected ${nnz.toInt}")
      nextRow += nRows.toInt
    }
    matrix.setSize(nextRow, nextNnz)
    (dictionary, matrix)
  }

  def appendOneVectorFile(file: File, matrix: SparseBinaryRowMatrix, rowPos: Int, nnzPos: Int, rowsToRead: Int): Int = {
    val in = new DataInputStream(new FastBufferedInputStream(new FileInputStream(file)))
    var nextRowPos = rowPos
    var nextNnzPos = nnzPos
    (0 until rowsToRead).foreach{idx =>
      matrix.rowStartIdx(nextRowPos) = nextNnzPos
      nextNnzPos = VectorIO.readOneSparseBinaryVector(in, matrix.colIds, nextNnzPos)
      nextRowPos += 1
    }
    matrix.rowStartIdx(nextRowPos) = nextNnzPos
    in.close()
    nextNnzPos
  }
}

case class RowMatrixPartitionDims(val totalDims: MatrixDims, val partitionDims: Map[Int, (Long,Long)])

