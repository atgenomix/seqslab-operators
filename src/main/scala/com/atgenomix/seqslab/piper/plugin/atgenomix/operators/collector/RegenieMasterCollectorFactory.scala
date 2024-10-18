package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector

import com.atgenomix.seqslab.piper.plugin.api.collector.{Collector, CollectorSupport, SupportsRepartitioning}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector.RegenieMasterCollectorFactory.RegenieMasterCollector
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.io.{BufferedReader, FileInputStream, FileReader}
import java.nio.file.{DirectoryStream, Path}
import java.util
import scala.collection.JavaConverters.{asJavaIteratorConverter, asScalaIteratorConverter}
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object RegenieMasterCollectorFactory {

  private class RegenieMasterCollector(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Collector
    with SupportsRepartitioning {

    private var isDir: Boolean = _
    private var path: String = _
    private val content = new ListBuffer[String]()
    private val partitionIdRegex: Regex = "(.*)-part-([0-9]*)-(.*)".r
    private var partitionId: Int = _

    override def init(b: Boolean): Collector = {
      isDir = b
      this
    }
    
    override def setInputStream(fileInputStream: FileInputStream): Unit = {
      content.clear()
      val field = fileInputStream.getClass.getDeclaredField("path")
      field.setAccessible(true)
      path = field.get(fileInputStream).toString
      Try(partitionIdRegex.findAllMatchIn(path).toSeq.head.group(2).toInt) match {
        case Success(partitionId) => this.partitionId = partitionId
        case Failure(t) => throw t
      }
      var reader: BufferedReader = null
      try {
        reader = new BufferedReader(new FileReader(fileInputStream.getFD))
        val it = reader.lines().iterator().asScala
        while (it.hasNext) {
          content.append(it.next)
        }
      } finally {
        reader.close()
      }
    }

    override def setDirectoryStream(directoryStream: DirectoryStream[Path]): Unit = ???

    override def schema(): StructType = DataTypes.createStructType(util.Arrays.asList(
      DataTypes.createStructField("header", DataTypes.StringType, true),
      DataTypes.createStructField("snplist", DataTypes.StringType, true),
      DataTypes.createStructField("partitionId", DataTypes.IntegerType, true),
      DataTypes.createStructField("path", DataTypes.StringType, true)
    ))

    override def getOperatorContext: OperatorContext = operatorCtx

    override def call(): util.Iterator[Row] = {
      val header = content.head
      content.tail.map { s =>
        val newS = s.replace("fit_parallel_job1", s"fit_parallel_job${partitionId + 1}")
        Row.fromSeq(Seq(header, newS, partitionId + 1, path))
      }.toIterator.asJava
    }

    override def close(): Unit = ()

    override def numPartitions(): Int = 1
  }
}

class RegenieMasterCollectorFactory extends OperatorPipelineV3 with CollectorSupport with Serializable {
  override def createCollector(pluginContext: PluginContext, operatorContext: OperatorContext): Collector = {
    new RegenieMasterCollector(pluginContext, operatorContext)
  }
}

