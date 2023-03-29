package com.atgenomix.seqslab.udf

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import org.apache.spark.sql.api.java.UDF2

import java.net.URL
import scala.util.{Failure, Success, Try}

/**
 * # 3109 genome regions overlapped with 1000 base pairs; each region contains average 20 million base pairs. This data-parallel view is recommended for generally-parallelized SNP/INDEL analysis pipelines, such as GATK whole genome analysis.
    *chr1	1	1000000	0	5000
    *chr1	1000001	2000000	1	5000
    *chr1	2000001	3000000	2	5000
    *chr1	3000001	4000000	3	5000
    *chr1	4000001	5000000	4	5000
    *chr1	5000001	6000000	5	5000
    *chr1	6000001	7000000	6	5000
    *chr1	7000001	8000000	7	5000
    *chr1	8000001	9000000	8	5000
  */

class Hg19Part3109Unpadded extends UDF2[String, Int, Array[Long]] {

    val bed: URL = getClass.getResource("/bed/19/contiguous_unmasked_regions_3109_parts_unpadded")
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