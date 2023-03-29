package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.VcfPartitionerHg19Part155Factory.VcfPartitionerHg19Part155
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, functions}

import scala.jdk.CollectionConverters.asScalaBufferConverter


object VcfPartitionerHg19Part155Factory {
  class VcfPartitionerHg19Part155(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = 155

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      val schema = t1.schema
      val parallelism = getClass.getResource("/bed/19/contiguous_unmasked_regions_155_parts")
      val ref = getClass.getResource("/reference/19/GRCH/ref.dict")
      val key_column = functions.call_udf("hg19part155", t1.col("row"), lit(Int.MinValue))
      val partitioner = GenomicPartitioner(Array(parallelism), ref)
      val result = t1
        .withColumn("key", key_column)
        .rdd
        .flatMap(r => r.getList[Long](r.fieldIndex("key")).asScala.map(k => (k, r)))
        .repartitionAndSortWithinPartitions(partitioner)
        .map(_._2)
      t1.sparkSession.createDataFrame(result, schema)
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}


class VcfPartitionerHg19Part155Factory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new VcfPartitionerHg19Part155(pluginContext, operatorContext)
  }
}
