package com.atgenomix.seqslab.piper.plugin.atgenomix.udf

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import org.apache.spark.sql.api.java.UDF4

import java.net.URL
import scala.util.{Failure, Success, Try}

class genomeConsensusPartFunc(bed: URL, dict: URL) extends UDF4[String, Int, String, Int, Array[Long]] {

  val partitioner: GenomicPartitioner = GenomicPartitioner(Array(bed), dict)

  override def call(chr: String, pos: Int, mateChr: String, matePos: Int): Array[Long] = {
    Try((pos, matePos)) match {
      case Success((pos, matePos)) =>
        val k1 = partitioner.getKeyValOrNoneInterval(chr, pos)
        val k2 = partitioner.getKeyValOrNoneInterval(mateChr, matePos)
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