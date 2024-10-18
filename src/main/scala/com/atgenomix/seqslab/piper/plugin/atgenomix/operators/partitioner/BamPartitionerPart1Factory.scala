package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.BamPartitionerPart1Factory.BamPartitionerPart1
import htsjdk.samtools.SAMFileHeader
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row}


object BamPartitionerPart1Factory {
  class BamPartitionerPart1(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = 1

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      val header = operatorCtx.getProperties.get("bamHeader").asInstanceOf[SAMFileHeader]
      val dict = header.getSequenceDictionary
      val start2Null: String => String = (ref: String) => if (ref == "*") null else "%07d".format(dict.getSequenceIndex(ref))
      val toNull = udf(start2Null)

      t1.withColumn("refNameIdx", toNull(col("referenceName")))
        .dropDuplicates(Seq("raw"))
        .coalesce(1)    // use coalesce to avoid too many & too large shuffle data
        .sortWithinPartitions(col("refNameIdx").asc_nulls_last, col("alignmentStart"))
        .drop("refNameIdx")
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class BamPartitionerPart1Factory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new BamPartitionerPart1(pluginContext, operatorContext)
  }
}
