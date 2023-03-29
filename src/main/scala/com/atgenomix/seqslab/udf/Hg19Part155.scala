package com.atgenomix.seqslab.udf

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import org.apache.spark.sql.api.java.UDF2

import java.net.URL
import scala.util.{Failure, Success, Try}

/**
  * # 155 genome regions overlapped with 1000 base pairs; each region contains average 20 million base pairs. This data-parallel view is recommended for generally-parallelized SNP/INDEL analysis pipelines, such as GATK whole genome analysis.
    chr1	1	13090000	0	1000
    chr1	13090001	29900000	1	1000
    chr1	29900001	49900000	2	1000
    chr1	49900001	69900000	3	1000
    chr1	69900001	89900000	4	1000
    chr1	89900001	103890000	5	1000
    chr1	103890001	123890000	6	1000
    chr1	123890001	148500000	7	1000
    chr1	148500001	168500000	8	1000
    chr1	168500001	188500000	9	1000
    chr1	188500001	206000000	10	1000
    chr1	206000001	223770000	11	1000
    chr1	223770001	235220000	12	1000
    chr1	235220001	249250621	13	1000
    chr2	1	21175000	14	1000
    chr2	21175001	41175000	15	1000
    chr2	41175001	61175000	16	1000
    chr2	61175001	87690000	17	1000
    chr2	87690001	110130000	18	1000
    chr2	110130001	130130000	19	1000
    chr2	130130001	149720000	20	1000
    chr2	149720001	169720000	21	1000
    chr2	169720001	189720000	22	1000
    chr2	189720001	209720000	23	1000
    chr2	209720001	234030000	24	1000
    chr2	234030001	243199373	25	1000
  */

class Hg19Part155 extends UDF2[String, Int, Array[Long]] {

    val bed: URL = getClass.getResource("/bed/19/contiguous_unmasked_regions_155_parts")
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