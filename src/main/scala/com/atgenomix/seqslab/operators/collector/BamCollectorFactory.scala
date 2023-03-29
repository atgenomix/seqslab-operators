package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector

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

object BamCollectorFactory {
  private class BamCollector(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Collector
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
      val field = DataTypes.createStructField("result", DataTypes.StringType, true)
      DataTypes.createStructType(util.Arrays.asList(field))
    }

    override def call(): util.Iterator[Row] = {
      var reader: BufferedReader = null
      try {
        val list = new util.ArrayList[Row]()
        reader = new BufferedReader(new FileReader(fis.getFD))
        val iter = reader.lines().iterator().asScala
        val s = new StringBuilder()
        while (iter.hasNext) {
          s.append(iter.next)
        }
        val a = s.mkString
        list.add(new GenericRowWithSchema(Array(a), schema()))
        //if (iter.nonEmpty) {
        //  val r = iter.reduce(_ ++ _)
        //  list.add(new GenericRowWithSchema(Array(r), schema()))
        //}
        list.iterator()
      } finally {
        reader.close()
      }
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def numPartitions(): Int = ???

    override def close(): Unit = ()
  }
}

// Collect text file into DataFrame
class BamCollectorFactory extends OperatorPipelineV3 with CollectorSupport with Serializable {
  override def createCollector(pluginContext: PluginContext, operatorContext: OperatorContext): Collector = {
    new BamCollector(pluginContext, operatorContext)
  }
}
