package com.atgenomix.seqslab.operators.writer

import com.atgenomix.seqslab.operators.writer.CsvWriterFactory.CsvWriter
import com.atgenomix.seqslab.piper.engine
import com.atgenomix.seqslab.piper.engine.Type.FQN
import com.atgenomix.seqslab.piper.engine.actor.Utils.{getLastName, getOutputSourceInfo}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, PiperValue, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import org.apache.spark.sql.{Dataset, Row}

import java.lang
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

object CsvWriterFactory {
  private class CsvWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Writer {

    val fqn: FQN = operatorCtx.asInstanceOf[engine.SeqslabOperatorContext].fqn
    val piperValue: PiperValue = operatorCtx.asInstanceOf[engine.SeqslabOperatorContext].outputs.get(fqn)
    val dataSource: DataSource = getOutputSourceInfo(getLastName(fqn), piperValue).head._2

    override def init(): Writer = this
    override def getDataSource: DataSource = dataSource
    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val className = this.getClass.getSimpleName
      val delimiter = operatorCtx.getProperties.asScala.get(s"$className:delimiter").map(_.asInstanceOf[String]).getOrElse(",")
      val header = operatorCtx.getProperties.asScala.get(s"$className:header").forall(_.asInstanceOf[Boolean])
      t1.write.option("sep", delimiter).option("header", header).csv(dataSource.getUrl)
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
