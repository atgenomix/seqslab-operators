package com.atgenomix.seqslab.operators.loader

import com.atgenomix.seqslab.piper.common.utils.{AzureUtil, HDFSUtil}
import com.atgenomix.seqslab.piper.plugin.api.loader._
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader.SingleNodeDataSource.SingleNodeDataLoader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.net.URI
import java.nio.file.{Files, Paths}
import java.util
import scala.jdk.CollectionConverters.{asJavaIteratorConverter, mapAsScalaMapConverter}

object SingleNodeDataSource {
  class SingleNodeDataLoader(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Loader
    with SupportsCopyToLocal with SupportsReadPartitions with SupportsHadoopDFS {

    private var srcInfo: DataSource = _
    private var path: String = _
    private var partId: Int = 0
    private var hadoopConfMap: Map[String, String] = Map.empty
    private var isFolder: Boolean = false
    private lazy val numPart: Int = 1

    override def init(dataSource: DataSource): Loader = {
      srcInfo = dataSource
      this
    }

    override def readSchema(): StructType = {
      StructType(Seq(StructField("~partId", IntegerType), StructField("~fqn", StringType)))
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()

    override def call(): util.Iterator[Row] = {
      val uri = new URI(srcInfo.getUrl)
      val p = Paths.get(path)

      srcInfo.getType match {
        case "abfs" | "abfss" =>
          val fs = HDFSUtil.getHadoopFileSystem(uri, hadoopConfMap)
          val isDir = HDFSUtil.isDir(uri)(fs)
          val token = Option(srcInfo.get("Authorization")).map(_.asInstanceOf[String])
          AzureUtil.downloader(uri, p, token, isDir)
        case "file" =>
          Files.createSymbolicLink(Paths.get(uri), p)
        case "jdbc" =>
          ???
        case _ =>
          HDFSUtil.downloader(uri, p)(hadoopConfMap)
      }
      Iterator.empty.asJava
    }

    override def setLocalPath(p: String): String =  {
      path = p
      path
    }

    override def numPartitions(): Int = {
      numPart
    }

    override def setPartitionId(i: Int): Unit = {
      partId = 0
    }

    override def setConfiguration(map: util.Map[String, String]): Unit = {
      hadoopConfMap = map.asScala.toMap
    }
  }
}

class SingleNodeDataSource extends OperatorPipelineV3 with LoaderSupport {
  override def createLoader(pluginContext: PluginContext, operatorContext: OperatorContext): Loader = {
    new SingleNodeDataLoader(pluginContext, operatorContext)
  }
}
