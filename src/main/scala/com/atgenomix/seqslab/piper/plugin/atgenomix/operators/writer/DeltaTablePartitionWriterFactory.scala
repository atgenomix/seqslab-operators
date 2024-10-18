package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer

import com.atgenomix.seqslab.piper.common.operator.AbstractDeltaTableWriter
import com.atgenomix.seqslab.piper.common.utils.DeltaTableUtil.createDeltaTable
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer.DeltaTablePartitionWriterFactory.DeltaTablePartitionWriter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}

import java.lang
import scala.collection.JavaConverters.mapAsScalaMapConverter


object DeltaTablePartitionWriterFactory {
  private class DeltaTablePartitionWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends AbstractDeltaTableWriter(pluginCtx, operatorCtx) {

   override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val properties = operatorCtx.getProperties.asScala
      val partitionByKey = s"${this.getClass.getSimpleName}:partitionBy"
      val partitionNumKey = s"${this.getClass.getSimpleName}:partitionNum"
      val partitionBy = properties.get(partitionByKey).map(_.asInstanceOf[String].split(',')).getOrElse(Array.empty)
      val partitionNum = properties.get(partitionNumKey).map(_.asInstanceOf[String].toInt).getOrElse(1)
      val tableName = createDeltaTable(fqn, db, dataSource, partitionBy, Option(t1.schema))(t1.sparkSession)
      (partitionBy.toList match {
        case Nil => t1
        case _ if partitionNum == 1 => t1.repartition(partitionBy.map(col): _*)
        case _ => t1.repartition(partitionNum)
      }).write.format("delta").mode("overwrite").saveAsTable(tableName)
      null
    }
  }
}

class DeltaTablePartitionWriterFactory extends OperatorPipelineV3 with WriterSupport with Serializable {
  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer = {
    new DeltaTablePartitionWriter(pluginContext, operatorContext)
  }
}