package com.atgenomix.seqslab.udf

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import org.apache.spark.sql.api.java.UDF6

import java.net.URL
import scala.util.{Failure, Success, Try}

class GenomeConsensusPartFunc(bed: URL, dict: URL) extends UDF6[String, Int, Int, String, Int, Int, Array[Int]] {

  val partitioner: GenomicPartitioner = GenomicPartitioner(Array(bed), dict)

  override def call(chr: String, start: Int, end: Int, mateChr: String, mateStart: Int, mateEnd: Int): Array[Int] = {
    Try((start, end, mateStart, mateEnd)) match {
      case Success((start, end, mateStart, mateEnd)) =>
        val k1 = partitioner.getKeyValOrNoneInterval(chr, start, end)
        val k2 = partitioner.getKeyValOrNoneInterval(mateChr, mateStart, mateEnd)
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
          .keys
          .toArray
      case Failure(_) =>
        throw new Exception()
    }
  }
}