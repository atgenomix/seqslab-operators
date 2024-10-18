package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.plugin.api.transformer.{SupportsOrdering, Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.VcfPartitionerPart1Factory.VcfPartitionerPart1
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, Dataset, Row}


object VcfPartitionerPart1Factory {
  class VcfPartitionerPart1(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer with SupportsOrdering{

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = 1

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      t1.coalesce(1)
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def getSortExprs(dataset: Dataset[Row]): Array[Column] = {
      val chrToIntUdf = udf((chr: String) => {
        val lowerChr = chr.toLowerCase.stripPrefix("chr")
        if (lowerChr(0) == 'x')
          23
        else if (lowerChr(0) == 'y')
          24
        else if (lowerChr(0) == 'm')
          25
        else {
          // for case like chr3_34
          lowerChr.split('_')(0)(0).toInt
        }
      })

      val strToLongUdf = udf((pos: String) => pos.toLong)

      val (chr, pos) = if (dataset.columns.contains("vcf")) {
        // for MafToVcfFormatter
        chrToIntUdf(dataset.col("vcf.CHROM")) -> strToLongUdf(dataset.col("vcf.POS"))
      } else {
        chrToIntUdf(dataset.col("CHROM")) -> dataset.col("POS")
      }

      Array(chr, pos)
    }

    override def close(): Unit = ()
  }
}


class VcfPartitionerPart1Factory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new VcfPartitionerPart1(pluginContext, operatorContext)
  }
}
