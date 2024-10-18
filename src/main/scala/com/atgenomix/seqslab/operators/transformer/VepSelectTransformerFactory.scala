package com.atgenomix.seqslab.operators.transformer

import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.operators.transformer.VepSelectTransformerFactory.VepSelectTransformer
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, concat_ws, explode, expr, lit}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object VepSelectTransformerFactory {

  class VepSelectTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    var _df: Option[DataFrame] = None

    override def call(t1: DataFrame): DataFrame = {
      val className = "com.atgenomix.seqslab.piper.plugin.atgenomix.udf.hive.com.atgenomix.seqslab.udf.hive.RetrievePattern"
      t1.sparkSession.sql(s"CREATE OR REPLACE TEMPORARY FUNCTION retrieve_pattern AS '$className'")
      val df = t1.flatMap(v => {
        val index = v.fieldIndex("INFO_CSQ")
        val info = v.getList[Row](index).asScala
        val altAllelesIndex = v.fieldIndex("alternateAlleles")
        val altAlleles = v.getList[String](altAllelesIndex).asScala
        if (altAlleles.head == "*") {
          Seq.empty
        } else {
          val llr = info.groupBy(r2 => r2.getAs[String]("SYMBOL")).values
          val newInfo = llr.flatMap(lr => {
            val refSeqResult = lr.filter(_.getAs[String]("SOURCE") == "RefSeq")
            val result = refSeqResult.filter(r3 => {
              val maneSelect = r3.getAs[String]("MANE_SELECT")
              val manePlusClinical = r3.getAs[String]("MANE_PLUS_CLINICAL")
              (maneSelect != null) || ( manePlusClinical != null)
            })
            if (result.isEmpty) {
              if (refSeqResult.isEmpty) Seq(lr.head) else Seq(refSeqResult.head)
            } else result
          })
          val oSeq = v.toSeq
          Seq(Row.fromSeq(oSeq.slice(0, index) ++ Seq(newInfo) ++ oSeq.slice(index + 1, oSeq.size)))
        }
      })(RowEncoder.apply(t1.schema))
      _df = Option(
        df.withColumn("comment", lit(null).cast(StringType))
          .withColumn("alternateAlleles", concat_ws(",", col("alternateAlleles")))
          .withColumn("INFO_CSQ", explode(col("INFO_CSQ")))
          .select("*", "INFO_CSQ.*")
          .withColumn("rsID", expr("retrieve_pattern('rs.*', Existing_variation)"))
          .withColumn("HGMD ID", expr("retrieve_pattern('C[A-Z][0-9].*', Existing_variation)"))
          .withColumn("COSMIC ID", expr("retrieve_pattern('COSV.*', Existing_variation)"))
          .drop(col("INFO_CSQ"))
          .drop(col("genotypes")))
      _df.get
    }

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = _df.map(_.rdd.getNumPartitions).getOrElse(1)

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }

}

class VepSelectTransformerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new VepSelectTransformer(pluginContext, operatorContext)
  }
}
