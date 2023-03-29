package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.BamPartitionerHg19Part155ConsensusFactory.BamPartitionerHg19Part155Consensus
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, functions}

import java.net.URL
import scala.jdk.CollectionConverters.asScalaBufferConverter

object BamPartitionerHg19Part155ConsensusFactory {
  class BamPartitionerHg19Part155Consensus(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = 155

    override def call(t1: Dataset[Row]): Dataset[Row] = {

      val bed: URL = getClass.getResource("/bed/19/contiguous_unmasked_regions_155_parts")
      val dict: URL = getClass.getResource("/reference/19/GRCH/ref.dict")
      val partitioner = GenomicPartitioner(Array(bed), dict)

      val keyColumn = functions.call_udf("hg19part155consensus",
        t1.col("referenceName"),
        t1.col("alignmentStart"),
        t1.col("mateReferenceName"),
        t1.col("mateAlignmentStart")
      )
      val result = t1
        .withColumn("key", keyColumn)
        .select("key","raw")
        .rdd
        .flatMap(r => r.getList[Long](r.fieldIndex("key")).asScala.map(k => (k, r)))
        .mapPartitions { _.map { case (k, v) =>
          val r = v.get(v.fieldIndex("raw"))
          k -> new GenericRow(Array(r)).asInstanceOf[Row]
        }
        }
        .repartitionAndSortWithinPartitions(partitioner)
        .mapPartitions(_.map(_._2))
      val schema = StructType(Seq(StructField("raw", BinaryType)))
      t1.sparkSession
        .createDataFrame(result, schema)
        .sortWithinPartitions(col("referenceName"), col("alignmentStart"))
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class BamPartitionerHg19Part155ConsensusFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new BamPartitionerHg19Part155Consensus(pluginContext, operatorContext)
  }
}