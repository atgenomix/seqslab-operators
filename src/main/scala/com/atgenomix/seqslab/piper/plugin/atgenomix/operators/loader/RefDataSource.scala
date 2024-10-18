package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader

import com.atgenomix.seqslab.piper.common.utils.{AzureUtil, HDFSUtil, HttpUtil, RetryUtil, S3Util}
import com.atgenomix.seqslab.piper.plugin.api.loader.{Loader, LoaderSupport, SupportsCopyToLocal, SupportsHadoopDFS}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader.RefDataSource.RefLoader
import models.SeqslabAny
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import software.amazon.awssdk.regions.Region

import java.net.URI
import java.nio.file.{Files, Paths}
import java.util
import scala.collection.JavaConverters.{asJavaIteratorConverter, mapAsScalaMapConverter}

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
      val auth = Option(srcInfo.get("headers")) match {
        case Some(headers) =>
          headers.asInstanceOf[Map[String, SeqslabAny]].get("Authorization") match {
            case Some(any) => any.o.map(f => f.toString)
            case None => None
          }
        case None => None
      }
      // ensure all intermediate dir are created
      Files.createDirectories(p.getParent)
      srcInfo.getType match {
        case "abfs" | "abfss" if auth.isDefined =>
          val fs = HDFSUtil.getHadoopFileSystem(uri, hadoopConfMap)
          val isDir = HDFSUtil.isDir(uri)(fs)
          AzureUtil.download(uri, p, auth, isDir)
        case "http" | "https" if uri.getHost.endsWith("core.windows.net") =>
          AzureUtil.download(uri, p, None, isDir = false)
        case "http" | "https" =>
          RetryUtil.retryWithCallback(HttpUtil.download(srcInfo.getUrl, path, auth), Files.deleteIfExists(p))
        case "s3" | "s3a" =>
          val fs = HDFSUtil.getHadoopFileSystem(uri, hadoopConfMap)
          val isDir = HDFSUtil.isDir(uri)(fs)
          val region = Region.of(srcInfo.getRegion)
          val credentialsProvider = S3Util.getCredential(hadoopConfMap)
          S3Util.download(uri, p, isDir, region, credentialsProvider)
        case "file" | _ =>
          RetryUtil.retryWithCallback(HDFSUtil.download(uri, p)(hadoopConfMap), Files.deleteIfExists(p))
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
