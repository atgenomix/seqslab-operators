package com.atgenomix.seqslab.piper.plugin.atgenomix.udf

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import org.apache.spark.sql.api.java.UDF2

import java.net.URL
import scala.util.{Failure, Success, Try}


class Hg19Part1 extends UDF2[String, Int, Array[Long]] {

    val bed: URL = getClass.getResource("/bed/19/single_node_workflow")
    val dict: URL = getClass.getResource("/reference/19/GRCH/ref.dict")
    val partitioner: GenomicPartitioner = GenomicPartitioner(Array(bed), dict)

    override def call(t1: String, t2: Int): Array[Long] = {
        if (t2.equals(Int.MinValue)) {
            val item = t1.split("\t")
            Try(item(1).toInt) match {
                case Success(pos) => partitioner.getKeyValOrNoneInterval(item(0), pos)
                case Failure(_) => throw new Exception()
            }
        } else partitioner.getKeyValOrNoneInterval(t1, t2)
    }
}