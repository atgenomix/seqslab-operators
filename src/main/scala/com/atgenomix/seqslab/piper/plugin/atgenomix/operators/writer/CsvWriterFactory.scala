package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer

import com.atgenomix.seqslab.piper.common.SeqslabOperatorContext
import com.atgenomix.seqslab.piper.common.utils.MiscUtil.{getLastName, getOutputSourceInfo}
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, PiperValue, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer.CsvWriterFactory.CsvWriter
import org.apache.spark.sql.{Dataset, Row}

import java.lang
import scala.collection.JavaConverters.mapAsScalaMapConverter

object CsvWriterFactory {
  private class CsvWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Writer {

    val fqn: String = operatorCtx.asInstanceOf[SeqslabOperatorContext].fqn
    val piperValue: PiperValue = operatorCtx.asInstanceOf[SeqslabOperatorContext].outputs.get(fqn)
    assert(piperValue != null, "CsvWriter: piperValue is null")
    val dataSource: DataSource = getOutputSourceInfo(getLastName(fqn), piperValue).headOption
      .map(_._2)
      .getOrElse(throw new RuntimeException(s"CsvWriter: no DataSource for $fqn"))

    override def init(): Writer = this
    override def getDataSource: DataSource = dataSource
    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val className = this.getClass.getSimpleName
      val properties = operatorCtx.getProperties.asScala
      val delimiter = properties.get(s"$className:delimiter").map(_.asInstanceOf[String]).getOrElse(",")
      val header = properties.get(s"$className:header").map(_.asInstanceOf[String]).getOrElse("true")
      val partitionNum = properties.get(s"$className:partitionNum").map(_.asInstanceOf[String].toInt).getOrElse(1)
      t1.repartition(partitionNum).write.options(Map("sep" -> delimiter, "header" -> header)).csv(dataSource.getUrl)
      null
    }
    override def getOperatorContext: OperatorContext = operatorCtx
    override def close(): Unit = ()
  }
}

class CsvWriterFactory extends WriterSupport {
  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer = {
    new CsvWriter(pluginContext, operatorContext)
  }
}
