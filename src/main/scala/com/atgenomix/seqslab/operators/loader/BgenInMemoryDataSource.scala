package com.atgenomix.seqslab.operators.loader

import com.atgenomix.seqslab.piper.common.operator.AbstractInMemoryDataLoader
import com.atgenomix.seqslab.piper.common.utils.HDFSUtil
import com.atgenomix.seqslab.piper.plugin.api.loader.{Loader, LoaderSupport}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, PluginContext}
import com.atgenomix.seqslab.operators.loader.BgenInMemoryDataSource.BgenInMemoryLoader
import com.atgenomix.seqslab.operators.partitioner.glow.BgenFileFormat
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.mutable


object BgenInMemoryDataSource {
  private class BgenInMemoryLoader(pluginCtx: PluginContext, operatorCtx: OperatorContext)
    extends AbstractInMemoryDataLoader(pluginCtx, operatorCtx) {
    override def getName: String = "BgenInMemoryDataLoader"

    override def init(dataSource: DataSource): Loader = {
      srcInfo = dataSource
      val conf = pluginCtx.piper.spark.sparkContext.hadoopConfiguration
      val p = new org.apache.hadoop.fs.Path(srcInfo.getUrl)
      implicit val fs: FileSystem = HDFSUtil.getHadoopFileSystem(p.toUri, conf)
      val url = if (HDFSUtil.isDir(p.toUri)) {
        HDFSUtil.listAllFiles(srcInfo.getUrl)
          .find(_.endsWith(".bgen"))
          .getOrElse(throw new RuntimeException(s"BgenInMemoryLoader: no .bgen file in ${srcInfo.getUrl}"))
      } else {
        dataSource.getUrl
      }
      val path = new Path(url)
      val options = getOptions()
      val sampleIds = BgenFileFormat.getBgenSampleIds(options, conf, path).map(_.mkString("|"))
      operatorCtx.setProperties(Map("sampleIds" -> sampleIds.asInstanceOf[Object]).asJava)
      this
    }

    private def getOptions(): Map[String, String] = {
      val properties: mutable.Map[String, AnyRef] = operatorCtx.getProperties.asScala
      val className: String = this.getName
      val useBgenIndex: String = properties.get(s"$className:useBgenIndex").map(_.asInstanceOf[String]).getOrElse("true")
      val emitHardCalls: String = properties.get(s"$className:emitHardCalls").map(_.asInstanceOf[String]).getOrElse("true")
      val hardCallThreshold: String = properties.get(s"$className:hardCallThreshold").map(_.asInstanceOf[String]).getOrElse("0.9")
      Map(
        "useBgenIndex" -> useBgenIndex,
        "emitHardCalls" -> emitHardCalls,
        "hardCallThreshold" -> hardCallThreshold
      )
    }
  }
}

class BgenInMemoryDataSource extends LoaderSupport {
  override def createLoader(pluginContext: PluginContext, operatorContext: OperatorContext): Loader = {
    new BgenInMemoryLoader(pluginContext, operatorContext)
  }
}
