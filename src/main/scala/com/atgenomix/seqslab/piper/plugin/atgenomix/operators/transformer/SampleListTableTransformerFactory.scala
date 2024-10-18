package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer

import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer.SampleListTableTransformerFactory.SampleListTableTransformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, expr}


object SampleListTableTransformerFactory {

  class SampleListTableTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    var _df: Option[DataFrame] = None
    override def call(t1: DataFrame): DataFrame = {
      val className = "com.atgenomix.seqslab.piper.plugin.atgenomix.udf.hive.RetrievePositiveGenotype"
      t1.sparkSession.sql(s"CREATE OR REPLACE TEMPORARY FUNCTION retrieve_positive AS '$className'")
      _df = Option(
        t1.withColumn("alternateAlleles", concat_ws(",", col("alternateAlleles")))
          .where("alternateAlleles != '*'")
          .withColumn("genotypes", expr("retrieve_positive_genotype(genotypes)"))
          .select(col("contigName"), col("start"), col("end"), col("names"),
            col("referenceAllele"), col("alternateAlleles"), col("genotypes.sampleId").alias("sampleIdList"))
      )
      _df.get
    }

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = _df.map(_.rdd.getNumPartitions).getOrElse(1)

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class SampleListTableTransformerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new SampleListTableTransformer(pluginContext, operatorContext)
  }
}
