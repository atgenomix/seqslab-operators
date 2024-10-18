package com.atgenomix.seqslab.operators.writer

import com.atgenomix.seqslab.piper.common.SeqslabOperatorContext
import com.atgenomix.seqslab.piper.common.utils.MiscUtil.{getLastName, getOutputSourceInfo}
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, PiperValue, PluginContext}
import com.atgenomix.seqslab.operators.writer.JsonWriterFactory.JsonWriter
import org.apache.spark.sql.{Dataset, Row}

import java.lang
import scala.collection.JavaConverters.mapAsScalaMapConverter

object JsonWriterFactory {
  private class JsonWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Writer {

    val fqn: String = operatorCtx.asInstanceOf[SeqslabOperatorContext].fqn
    val piperValue: PiperValue = operatorCtx.asInstanceOf[SeqslabOperatorContext].outputs.get(fqn)
    assert(piperValue != null, "JsonWriter: piperValue is null")
    val dataSource: DataSource = getOutputSourceInfo(getLastName(fqn), piperValue).headOption
      .map(_._2)
      .getOrElse(throw new RuntimeException(s"JsonWriter: no DataSource for $fqn"))

    override def init(): Writer = this
    override def getDataSource: DataSource = dataSource
    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val className = this.getClass.getSimpleName
      val properties = operatorCtx.getProperties.asScala
      val partitionNum = properties.get(s"$className:partitionNum").map(_.asInstanceOf[String].toInt).getOrElse(1)
      t1.repartition(partitionNum).write.json(dataSource.getUrl)
      null
    }
    override def getOperatorContext: OperatorContext = operatorCtx
    override def close(): Unit = ()
  }
}

class JsonWriterFactory extends WriterSupport {
  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer = {
    new JsonWriter(pluginContext, operatorContext)
  }
}
