package com.atgenomix.seqslab.operators.collector

import com.atgenomix.seqslab.piper.plugin.api.collector.{Collector, CollectorSupport, SupportsRepartitioning}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector.BamCollectorFactory.BamCollector
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.io.{BufferedReader, FileInputStream, FileReader}
import java.nio.file.{DirectoryStream, Path}
import java.util
import scala.jdk.CollectionConverters.asScalaIteratorConverter

object PhenopacketCollectorFactory {
  private class PhenopacketCollector(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Collector
    with SupportsRepartitioning {

    private var isDir: Boolean = _
    private var fis: FileInputStream = _

    override def init(b: Boolean): Collector = {
      isDir = b
      this
    }

    override def setInputStream(fileInputStream: FileInputStream): Unit = {
      fis = fileInputStream
    }

    override def setDirectoryStream(directoryStream: DirectoryStream[Path]): Unit = ???

    override def schema(): StructType = {
    }

    override def call(): util.Iterator[Row] = {
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def numPartitions(): Int = ???

    override def close(): Unit = ()
  }
}

class PhenopacketCollectorFactory extends OperatorPipelineV3 with CollectorSupport with Serializable {
  override def createCollector(pluginContext: PluginContext, operatorContext: OperatorContext): Collector = {
    new PhenopacketCollector(pluginContext, operatorContext)
  }
}
