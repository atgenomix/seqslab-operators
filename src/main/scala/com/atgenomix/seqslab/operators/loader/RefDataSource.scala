package com.atgenomix.seqslab.operators.loader

import com.atgenomix.seqslab.piper.common.utils.{AzureUtil, HDFSUtil}
import com.atgenomix.seqslab.piper.plugin.api.loader.{Loader, LoaderSupport, SupportsCopyToLocal, SupportsHadoopDFS}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader.RefDataSource.RefLoader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import java.net.URI
import java.nio.file.Paths
import java.util
import scala.jdk.CollectionConverters.{asJavaIteratorConverter, mapAsScalaMapConverter}

object RefDataSource {
  class RefLoader(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Loader
    with SupportsCopyToLocal with SupportsHadoopDFS {

    private var srcInfo: DataSource = _
    private var path: String = _
    private var hadoopConfMap: Map[String, String] = Map.empty

    override def getOperatorContext: OperatorContext = operatorCtx

    override def init(src: DataSource): Loader = {
      srcInfo = src
      this
    }

    override def readSchema(): StructType = ???

    override def setLocalPath(s: String): String = {
      path = s
      path
    }

    override def call(): util.Iterator[Row] = {
      val uri = new URI(srcInfo.getUrl)
      val p = Paths.get(path)

      srcInfo.getType match {
        case "abfs" | "abfss" =>
          val fs = HDFSUtil.getHadoopFileSystem(uri, hadoopConfMap)
          val isDir = HDFSUtil.isDir(uri)(fs)
          val token = Option(srcInfo.get("Authorization")).map(_.asInstanceOf[String])
          AzureUtil.downloader(uri, p, token, isDir)
        case "jdbc" =>
          ???
        case "file" | _ =>
          HDFSUtil.downloader(uri, p)(hadoopConfMap)
      }
      Iterator.empty.asJava
    }

    override def close(): Unit = ()

    override def setConfiguration(map: util.Map[String, String]): Unit = {
      hadoopConfMap = map.asScala.toMap
    }
  }
}

class RefDataSource extends OperatorPipelineV3 with LoaderSupport {
  override def createLoader(pluginContext: PluginContext, operatorContext: OperatorContext): Loader = {
    new RefLoader(pluginContext, operatorContext)
  }
}
