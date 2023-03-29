package com.atgenomix.seqslab.operators.transformer

import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer.VcfDataFrameTransformerFactory.VcfDataFrameTransformer
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object VcfDataFrameTransformerFactory {
  class VcfDataFrameTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    private var df: Option[DataFrame] = None
    private var schema: StructType = new StructType()
    implicit private var encoder: ExpressionEncoder[Row] = RowEncoder(schema)

    override def init(var1: Int, var2: Int): Transformer = {
      val header = operatorCtx.get("vcfHeader").asInstanceOf[ArrayBuffer[String]]
      val chrom :: pos :: others = header.last.drop(1).split('\t').toList
      val o = others.take(7) // take until FORMAT column
      schema = StructType(Array(StructField(chrom, StringType, true), StructField(pos, IntegerType, true)) ++
        o.map(StructField(_, StringType, true)) ++ Array(StructField("SAMPLES", StringType, true))
      )
      encoder = RowEncoder(schema)
      this
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()

    override def numPartitions(): Int = df.get.rdd.getNumPartitions

    override def call(t1: DataFrame): DataFrame = {
      df = Some(t1.map(row => {
        val c :: p :: o = row.getAs[String]("row").split("\t", 10).toList
        new GenericRowWithSchema(Array(c, p.toInt) ++ o, schema)
      }))
      df.get
    }
  }
}

class VcfDataFrameTransformerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new VcfDataFrameTransformer(pluginContext, operatorContext)
  }
}
