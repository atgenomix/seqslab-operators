package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer

import com.atgenomix.seqslab.piper.common.SeqslabOperatorContext
import com.atgenomix.seqslab.piper.common.utils.MiscUtil.{getLastName, getOutputSourceInfo}
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, PiperValue, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer.VcfGlowWriterFactory.VcfGlowWriter
import org.apache.spark.sql.{Dataset, Row}

import java.lang
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer

object VcfGlowWriterFactory {
  private class VcfGlowWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Writer {

    val opName: String = this.getClass.getSimpleName
    val fqn: String = operatorCtx.asInstanceOf[SeqslabOperatorContext].fqn
    val piperValue: PiperValue = operatorCtx.asInstanceOf[SeqslabOperatorContext].outputs.get(fqn)
    assert(piperValue != null, s"$opName: piperValue is null")
    val dataSource: DataSource = getOutputSourceInfo(getLastName(fqn), piperValue).headOption
      .map(_._2)
      .getOrElse(throw new RuntimeException(s"$opName: no DataSource for $fqn"))

    override def init(): Writer = this
    override def getDataSource: DataSource = dataSource
    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val props = operatorCtx.getProperties.asScala
      val cPartition = t1.rdd.getNumPartitions
      val eH = props.get(s"$opName:vcfHeader").map(_.asInstanceOf[String])
      // Configure partition and format
      var writer = props.get(s"$opName:partitionNum").map(_.asInstanceOf[String].toInt).getOrElse(0) match {
        case 0 if cPartition == 1 => t1.write.format("bigvcf")
        case 0 =>
          val compression = props.get(s"$opName:compression").map(_.asInstanceOf[String]).getOrElse("bgzf")
          t1.write.option("compression", compression).format("vcf")
        case dPartition if dPartition == 1 => t1.write.format("bigvcf")
        case dPartition =>
          val compression = props.get(s"$opName:compression").map(_.asInstanceOf[String]).getOrElse("bgzf")
          val df = dPartition match {
            case d if d < cPartition => t1.coalesce(dPartition)
            case d if d > cPartition => t1.repartition(dPartition)
            case _ => t1
          }
          df.write.option("compression", compression).format("vcf")
      }
      // Configure VcfHeader
      writer = eH match {
        case Some(header) if !header.equals("infer") => writer.option("vcfHeader", header)
        case _ =>
          props.get("vcfHeader").map(_.asInstanceOf[ArrayBuffer[String]].foldLeft("")((o, l) => o + l + "\n")) match {
          case Some(header) => writer.option("vcfHeader", header)
          case _ => writer
        }
      }
      writer.save(dataSource.getUrl)
      null
    }
    override def getOperatorContext: OperatorContext = operatorCtx
    override def close(): Unit = ()
  }
}

class VcfGlowWriterFactory extends WriterSupport {
  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer = {
    new VcfGlowWriter(pluginContext, operatorContext)
  }
}
