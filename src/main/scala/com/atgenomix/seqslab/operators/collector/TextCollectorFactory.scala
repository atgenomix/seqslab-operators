package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector

import com.atgenomix.seqslab.piper.plugin.api.collector.{Collector, CollectorSupport, SupportsAggregation}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector.TextCollectorFactory.TextCollector
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.io.{BufferedReader, FileInputStream, FileReader}
import java.nio.file.{DirectoryStream, Path}
import java.util
import scala.jdk.CollectionConverters.asScalaIteratorConverter

object TextCollectorFactory {
  private class TextCollector(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Collector
    with SupportsAggregation {

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
        if (iter.nonEmpty) {
          val r = iter.reduce(_ ++ _)
          list.add(new GenericRowWithSchema(Array(r), schema()))
        }
        list.iterator()
      } finally {
        reader.close()
      }
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def agg(dataset: Dataset[Row]): Dataset[Row] = {
      val spark = dataset.sparkSession
      import spark.implicits._
      import org.apache.spark.sql.functions._

      dataset.agg(collect_list($"result")).toDF("result")
    }

    override def close(): Unit = ()
  }
}

// Collect text file into DataFrame
class TextCollectorFactory extends OperatorPipelineV3 with CollectorSupport with Serializable {
  override def createCollector(pluginContext: PluginContext, operatorContext: OperatorContext): Collector = {
    new TextCollector(pluginContext, operatorContext)
  }
}
