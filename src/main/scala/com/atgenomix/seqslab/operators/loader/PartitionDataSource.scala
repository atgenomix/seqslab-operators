package com.atgenomix.seqslab.operators.loader

import com.atgenomix.seqslab.piper.common.utils.HDFSUtil
import com.atgenomix.seqslab.piper.plugin.api.loader._
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader.PartitionDataSource.PartitionDataLoader
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.net.URI
import java.nio.file.Paths
import java.util
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

object PartitionDataSource {
  class PartitionDataLoader(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Loader
    with SupportsCopyToLocal with SupportsReadPartitions with SupportsHadoopDFS {

    private var srcInfo: DataSource = _
    private var path: String = _
    private var partId: Int = -1
    private var hadoopConfMap: Map[String, String] = Map.empty
    private var isFolder: Boolean = false
    private lazy val numPart: Int = getNumPartitions

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
      val fileName = getFileName(hadoopConfMap)
      val name = if (isFolder) s"part-${"%05d".format(partId)}-$fileName" else fileName
      val src: URI = new URI(srcInfo.getUrl).resolve(name)
      HDFSUtil.downloader(src, Paths.get(path))(hadoopConfMap)
      new util.ArrayList[Row]().iterator()
    }

    override def setLocalPath(p: String): String =  {
      path = p
      path
    }

    override def numPartitions(): Int = {
      numPart
    }

    override def setPartitionId(i: Int): Unit = {
      partId = i
    }

    override def setConfiguration(map: util.Map[String, String]): Unit = {
      hadoopConfMap = map.asScala.toMap
    }

    private def getNumPartitions: Int = {
      val p = new org.apache.hadoop.fs.Path(srcInfo.getUrl)
      val fs = HDFSUtil.getHadoopFileSystem(p.toUri, hadoopConfMap)
      if (fs.getFileStatus(p).isDirectory) {
        isFolder = true
        fs.listStatus(p).length
      } else {
        isFolder = false
        1
      }
    }

    private def getFileName(hadoopConfMap: Map[String, String]): String = {
      val p = new Path(srcInfo.getUrl)
      val fs = HDFSUtil.getHadoopFileSystem(p.toUri, hadoopConfMap)
      val status = fs.getFileStatus(p)
      status.getPath.getName
    }
  }
}

class PartitionDataSource extends OperatorPipelineV3 with LoaderSupport {
  override def createLoader(pluginContext: PluginContext, operatorContext: OperatorContext): Loader = {
    new PartitionDataLoader(pluginContext, operatorContext)
  }
}
