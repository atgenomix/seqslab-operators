package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer

import com.atgenomix.seqslab.piper.common.SeqslabOperatorContext
import com.atgenomix.seqslab.piper.common.utils.HDFSUtil
import com.atgenomix.seqslab.piper.common.utils.MiscUtil.{getLastName, getOutputSourceInfo}
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, PiperValue, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer.RegenieMasterWriterFactory.RegenieMasterWriter
import org.apache.spark.sql.{Dataset, Row}

import java.lang
import java.net.URI
import scala.collection.JavaConverters.mapAsScalaMapConverter


object RegenieMasterWriterFactory {
  private class RegenieMasterWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Writer {

    val fqn: String = operatorCtx.asInstanceOf[SeqslabOperatorContext].fqn
    val piperValue: PiperValue = operatorCtx.asInstanceOf[SeqslabOperatorContext].outputs.get(fqn)
    assert(piperValue != null, "RegenieMasterWriter: piperValue is null")
    val dataSource: DataSource = getOutputSourceInfo(getLastName(fqn), piperValue).headOption
      .map(_._2)
      .getOrElse(throw new RuntimeException(s"RegenieMasterWriter: no DataSource for $fqn"))

    override def init(): Writer = this
    override def getDataSource: DataSource = dataSource
    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val header = t1.head.getAs[String]("header")
      t1.sort("partitionId").show(1000, truncate=false)
      val conf = pluginCtx.piper.spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      val fs = HDFSUtil.getHadoopFileSystem(new URI(dataSource.getUrl), conf)
      val os = HDFSUtil.getOutputStream(new URI(s"${dataSource.getUrl}fit_parallel.master"))(fs)
      os.write(s"$header\n".getBytes)
      t1.sort("partitionId")
        .drop("partitionId", "path")
        .collect()
        .foreach(row => os.write(s"${row.getAs[String]("snplist")}\n".getBytes))
      os.close()
      null
    }
    override def getOperatorContext: OperatorContext = operatorCtx
    override def close(): Unit = ()
  }
}

class RegenieMasterWriterFactory extends WriterSupport {
  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer = {
    new RegenieMasterWriter(pluginContext, operatorContext)
  }
}
