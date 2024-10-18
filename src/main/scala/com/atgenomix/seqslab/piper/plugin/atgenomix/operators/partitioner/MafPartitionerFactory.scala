package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.plugin.api.transformer.{SupportsOrdering, SupportsPartitioner, Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.MafPartitionerFactory.MafPartitioner
import org.apache.spark.sql.delta.implicits.stringEncoder
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}


object MafPartitionerFactory {

  // Partition MAF per sample
  class MafPartitioner(pluginCtx: PluginContext, operatorCtx: OperatorContext)
    extends Transformer with SupportsOrdering with SupportsPartitioner {

    private var barcodeToPartId: Map[String, Int] = Map.empty
    private var numPart: Int = 0

    override def getOperatorContext: OperatorContext = operatorCtx

    override def init(cpuCores: Int, memPerCore: Int): Transformer = this

    override def numPartitions(): Int = numPart

    override def call(df: DataFrame): DataFrame = {
      val samples = getSampleId(df)
      this.numPart = samples.length
      barcodeToPartId = samples.zipWithIndex.toMap

      df
    }

    override def getSortExprs(dataset: Dataset[Row]): Array[Column] = {
      val chrToIntUdf = udf((chr: String) =>
        if (chr.substring(3).toLowerCase == "x")
          23
        else if (chr.substring(3).toLowerCase == "y")
          24
        else if (chr.substring(3).toLowerCase == "m")
          25
        else
          chr.split('_')(0).substring(3).toInt
      )

      val chr = dataset.col("Chromosome")
      Array(chrToIntUdf(chr), col("Start_Position"))
    }

    override def expr(dataset: Dataset[Row]): Column = {
      col("Tumor_Sample_Barcode")
    }

    override def partitionId(objects: AnyRef*): Integer = {
      val barcode = objects.head.asInstanceOf[String]
      barcodeToPartId.getOrElse(barcode, throw new RuntimeException(s"MafPartitioner: unknown barcode $barcode"))
    }

    override def close(): Unit = ()

    private def getSampleId(df: DataFrame): Array[String] = {
      df.select("Tumor_Sample_Barcode")
        .mapPartitions(_.map(_.getString(0)))
        .distinct()
        .collect()
    }
  }
}

class MafPartitionerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new MafPartitioner(pluginContext, operatorContext)
  }
}
