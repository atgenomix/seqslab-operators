package com.atgenomix.seqslab.operators.transformer

import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.operators.transformer.GenotypeTableTransformerFactory.GenotypeTableTransformer
import org.apache.spark.sql.functions.{col, concat_ws, explode, lit}
import org.apache.spark.sql.DataFrame


object GenotypeTableTransformerFactory {

  class GenotypeTableTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    var _df: Option[DataFrame] = None

    override def call(t1: DataFrame): DataFrame = {
      _df = Option(
        t1.withColumn("alternateAlleles", concat_ws(",", col("alternateAlleles")))
          .where("alternateAlleles != '*'")
          .withColumn("genotypes", explode(col("genotypes")))
          .withColumn("runName", lit(operatorCtx.get("~runName").toString: String))
          .select("runName", "contigName", "start", "end", "names", "referenceAllele", "alternateAlleles", "genotypes.*"))
      _df.get
    }

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = _df.map(_.rdd.getNumPartitions).getOrElse(1)

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }

}

class GenotypeTableTransformerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new GenotypeTableTransformer(pluginContext, operatorContext)
  }
}
