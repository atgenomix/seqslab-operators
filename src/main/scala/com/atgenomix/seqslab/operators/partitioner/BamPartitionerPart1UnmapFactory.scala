package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.common.genomics.Bam.ByteArrayUtil
import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.BamPartitionerPart1UnmapFactory.BamPartitionerPart1Unmap
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row}

case class RawBAM(
  raw: Array[Byte]
) {
  def getReadUnmappedFlag: Boolean = {
    val READ_UNMAPPED = 0x4
    val flags = ByteArrayUtil.getUShort(raw.slice(18, 20))
    (flags & READ_UNMAPPED) != 0
  }
}

object BamPartitionerPart1UnmapFactory {
  class BamPartitionerPart1Unmap(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = 1

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      implicit val enc: Encoder[RawBAM] = Encoders.product[RawBAM]
      t1.select("raw")
        .as[RawBAM]
        .filter(_.getReadUnmappedFlag)
        .repartition(1)
        .toDF
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class BamPartitionerPart1UnmapFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new BamPartitionerPart1Unmap(pluginContext, operatorContext)
  }
}
