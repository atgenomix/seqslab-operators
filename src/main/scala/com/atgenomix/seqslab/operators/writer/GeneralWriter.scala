package com.atgenomix.seqslab.operators.writer

import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import com.atgenomix.seqslab.operators.writer.GeneralWriterFactory.GeneralWriter
import org.apache.spark.sql.{Dataset, Row}

import java.lang

object GeneralWriterFactory {
  private class GeneralWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Writer {
    override def init(): Writer = ???

    override def getDataSource: DataSource = ???

    override def getOperatorContext: OperatorContext = ???

    override def close(): Unit = ???

    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = ???
  }
}

class GeneralWriterFactory extends OperatorPipelineV3 with WriterSupport with Serializable {
  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer = {
    new GeneralWriter(pluginContext, operatorContext)
  }
}