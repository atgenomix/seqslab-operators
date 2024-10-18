package com.atgenomix.seqslab.piper.plugin.atgenomix.udf

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import org.apache.spark.sql.api.java.{UDF2, UDF3}

import java.net.URL
import scala.util.{Failure, Success, Try}

/**
 * Models a full-closed interval [start, end]
 */
class GenomePartFunc(bed: URL, dict: URL) extends UDF2[String, Int, Array[Long]] with UDF3[String, Int, Int, Array[Int]] {

  val partitioner: GenomicPartitioner = GenomicPartitioner(Array(bed), dict)

  override def call(t1: String, t2: Int): Array[Long] = {
    if (t2.equals(Int.MinValue)) {
      // branch for VcfPartitionFactory where column("row") and Int.MinValue are passed into the udf, i.e.
      // val key_column = functions.call_udf(this.udfName, t1.col("row"), lit(Int.MinValue))
      val item = t1.split("\t")
      Try(item(1).toInt) match {
        case Success(pos) => partitioner.getKeyValOrNoneInterval(item(0), pos)
        case Failure(_) => throw new Exception()
      }
    } else
      // branch for BamPartitionFactory where referenceName and alignmentStart are passed into this udf, i.e.
      // val keyColumn = functions.call_udf(this.udfName, t1.col("referenceName"), t1.col("alignmentStart"))
      partitioner.getKeyValOrNoneInterval(t1, t2)
  }

  override def call(t1: String, t2: Int, t3: Int): Array[Int] = {
    // for BamPartitionFactory where referenceName, alignmentStart and alignmentEnd are passed into this udf, i.e.
    // val keyColumn = functions.call_udf(this.udfName, t1.col("referenceName"), t1.col("alignmentStart"), t1.col("alignmentEnd"))
    partitioner.getKeyValOrNoneInterval(t1, t2, t3)
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
  }
}