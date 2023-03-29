package com.atgenomix.seqslab.udf

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import org.apache.spark.sql.api.java.UDF2

import java.net.URL
import scala.util.{Failure, Success, Try}


class Hg19Part77 extends UDF2[String, Int, Array[Long]] {

    val bed: URL = getClass.getResource("/bed/19/chromosomes")
    val dict: URL = getClass.getResource("/reference/19/GRCH/ref.dict")
    val partitioner: GenomicPartitioner = GenomicPartitioner(Array(bed), dict)

    override def call(t1: String, t2: Int): Array[Long] = {
        Try(t2) match {
            case Success(pos) => partitioner.getKeyValOrNoneInterval(t1, pos)
            case Failure(_) => throw new Exception()
        }
    }
}