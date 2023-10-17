package com.atgenomix.seqslab.operators.loader

import com.atgenomix.seqslab.operators.loader.RefDataSource.RefLoader
import com.atgenomix.seqslab.piper.common.utils.{AzureUtil, HDFSUtil, HttpUtil}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.loader.{Loader, LoaderSupport, SupportsCopyToLocal, SupportsHadoopDFS}
import models.SeqslabAny
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
      val auth = srcInfo.asScala.get("headers") match {
        case Some(headers) =>
          headers.asInstanceOf[Map[String, SeqslabAny]].get("Authorization") match {
            case Some(any) => any.o.map(f => f.toString)
            case None => None
          }
        case None => None
      }
      srcInfo.getType match {
        case "abfs" | "abfss" if auth.isDefined =>
          val fs = HDFSUtil.getHadoopFileSystem(uri, hadoopConfMap)
          val isDir = HDFSUtil.isDir(uri)(fs)
          AzureUtil.download(uri, p, auth, isDir)
        case "http" | "https" if uri.getHost.endsWith("core.windows.net") =>
          AzureUtil.download(uri, p, None, isDir = false)
        case "http" | "https" =>
          HttpUtil.download(srcInfo.getUrl, path, auth)
        case "file" | _ =>
          HDFSUtil.download(uri, p)(hadoopConfMap)
        case "jdbc" =>
          ???
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
