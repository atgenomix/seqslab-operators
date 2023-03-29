package com.atgenomix.seqslab.piper.plugin.atgenomix.udf

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import org.apache.spark.sql.api.java.{UDF1, UDF4}

import java.net.URL
import scala.util.{Failure, Success, Try}

class genomeBedPartFunc(bed: URL, dict: URL) extends UDF1[String, Array[Long]] {

  val partitioner: GenomicPartitioner = GenomicPartitioner(Array(bed), dict)

  override def call(line: String): Array[Long] = {
    val content = line.split("\t", 4)
    val chr = content(0)
    val start = content(1).toInt
    val end = content(2).toInt

    Try((start, end)) match {
      case Success((pos, matePos)) =>
        val k1 = partitioner.getKeyValOrNoneInterval(chr, pos)
        val k2 = partitioner.getKeyValOrNoneInterval(chr, end)
        (k1 ++ k2).distinct
          // remove duplicated keys from same partition (just keep Read1)
          .foldLeft(Map.empty[Int, Long]) { case (r, k) =>
            val part = partitioner.getPartition(k)
            if (r.contains(part)) {
              r
            } else {
              r + (part -> k)
            }
          }
          .values
          .toArray
      case Failure(_) =>
        throw new Exception()
    }
  }
}