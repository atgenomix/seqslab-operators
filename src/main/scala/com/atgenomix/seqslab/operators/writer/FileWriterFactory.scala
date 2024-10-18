package com.atgenomix.seqslab.operators.writer

import com.atgenomix.seqslab.piper.common.SeqslabOperatorContext
import com.atgenomix.seqslab.piper.common.utils.MiscUtil.{getLastName, getOutputSourceInfo}
import com.atgenomix.seqslab.piper.common.utils.{HDFSUtil, HadoopUtil}
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, PiperValue, PluginContext}
import com.atgenomix.seqslab.operators.writer.FileWriterFactory.FileWriter
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Row}

import java.lang
import java.net.URI

object FileWriterFactory {
  private class FileWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Writer {

    val fqn: String = operatorCtx.asInstanceOf[SeqslabOperatorContext].fqn
    val piperValue: PiperValue = operatorCtx.asInstanceOf[SeqslabOperatorContext].outputs.get(fqn)
    assert(piperValue != null, "FileWriter: piperValue is null")

    val dataSource: DataSource = getOutputSourceInfo(getLastName(fqn), piperValue).headOption
      .map(_._2)
      .getOrElse(throw new RuntimeException(s"FileWriter: no DataSource for $fqn"))

    override def init(): Writer = this
    override def getDataSource: DataSource = dataSource
    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val spark = pluginCtx.piper.spark
      val sc = spark.sparkContext
      val url = dataSource.getUrl
      val confMapBc = sc.broadcast(HadoopUtil.getHadoopConfMap(spark))

      t1.foreach { row =>
        val bytes = row.getAs[Array[Byte]](0)
        val fs = HDFSUtil.getHadoopFileSystem(new URI(url), confMapBc.value)
        val os = fs.create(new Path(url))
        os.write(bytes)
        os.flush()
        os.close()
      }
      null
    }
    override def getOperatorContext: OperatorContext = operatorCtx
    override def close(): Unit = ()
  }
}

class FileWriterFactory extends WriterSupport {
  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer = {
    new FileWriter(pluginContext, operatorContext)
  }
}
