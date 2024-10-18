package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector

import com.atgenomix.seqslab.piper.plugin.api.collector.{Collector, CollectorSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector.SqlDefaultCollectorFactory.SqlDefaultCollector
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import java.io.FileInputStream
import java.nio.file.{DirectoryStream, Path}
import java.util

object SqlDefaultCollectorFactory {
  private class SqlDefaultCollector(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Collector {
    override def init(b: Boolean): Collector = this
    override def setInputStream(fileInputStream: FileInputStream): Unit = ()
    override def setDirectoryStream(directoryStream: DirectoryStream[Path]): Unit = ()
    override def schema(): StructType = StructType(Seq())
    override def getOperatorContext: OperatorContext = operatorCtx
    override def close(): Unit = ()
    override def call(): util.Iterator[Row] = new util.ArrayList[Row]().iterator()
  }
}

class SqlDefaultCollectorFactory extends OperatorPipelineV3 with CollectorSupport with Serializable {
  override def createCollector(pluginContext: PluginContext, operatorContext: OperatorContext): Collector = {
    new SqlDefaultCollector(pluginContext, operatorContext)
  }
}
