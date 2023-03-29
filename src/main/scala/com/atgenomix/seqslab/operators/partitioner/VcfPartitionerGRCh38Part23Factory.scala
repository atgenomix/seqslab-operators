package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.VcfPartitionerGRCh38Part23Factory.VcfPartitionerGRCh38Part23
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, functions}

import scala.jdk.CollectionConverters.asScalaBufferConverter


object VcfPartitionerGRCh38Part23Factory {
  class VcfPartitionerGRCh38Part23(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = 23

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      val schema = t1.schema
      val parallelism = getClass.getResource("/bed/38/chromosomes")
      val ref = getClass.getResource("/reference/38/GRCH/ref.dict")
      val key_column = functions.call_udf("grch38part23", t1.col("row"), lit(Int.MinValue))
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


class VcfPartitionerGRCh38Part23Factory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new VcfPartitionerGRCh38Part23(pluginContext, operatorContext)
  }
}
