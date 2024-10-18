package com.atgenomix.seqslab.operators.transformer

import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.operators.transformer.IndexTransformerFactory.IndexTransformer
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters.mapAsScalaMapConverter


object IndexTransformerFactory {
  class IndexTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    private var _df: Option[DataFrame] = None
    private var _schema: StructType = new StructType()

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = _df.get.rdd.getNumPartitions

    override def close(): Unit = ()

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      val key = f"${this.getClass.getSimpleName}:startsFrom"
      val rdd = operatorCtx.getProperties.asScala.get(key).map(
        x => {
          if (x.isInstanceOf[Number]) x.asInstanceOf[Double].toInt
          else x.asInstanceOf[String].toDouble.toInt
        }) match {
        case Some(starts) => t1.rdd.zipWithIndex.map {
          case (row, index) => Row.fromSeq(row.toSeq :+ (index + starts))
        }
        case None => t1.rdd.zipWithIndex.map {
          case (row, index) => Row.fromSeq(row.toSeq :+ index)
        }
      }
      _schema = StructType(t1.schema.fields :+ StructField("index", LongType, nullable=false))
      _df = Some(t1.sparkSession.createDataFrame(rdd, _schema).orderBy("index"))
      _df.get
    }

    override def getOperatorContext: OperatorContext = operatorCtx
  }
}


class IndexTransformerFactory extends TransformerSupport{
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new IndexTransformer(pluginContext, operatorContext)
  }
}
