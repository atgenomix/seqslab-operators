package com.atgenomix.seqslab.operators.writer

import com.atgenomix.seqslab.operators.writer.DeltaTablePartitionWriterFactory.DeltaTablePartitionWriter
import com.atgenomix.seqslab.piper.engine.actor.Utils
import com.atgenomix.seqslab.piper.engine.actor.Utils.DeltaTableWriter
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import org.apache.spark.sql.{Dataset, Row}

import java.lang


object DeltaTablePartitionWriterFactory {
  private class DeltaTablePartitionWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext)
    extends DeltaTableWriter(pluginCtx, operatorCtx) {

    override def call(t1: Dataset[Row], t2: lang.Boolean): Void = {
      val key = s"${this.getClass.getSimpleName}:partitionBy"
      val partitionBy = operatorCtx.getProperties.get(key).asInstanceOf[Seq[String]]
      val tableName = Utils.createDeltaTable(fqn, db, runId, dataSource, partitionBy, Option(t1.schema))(t1.sparkSession)
      t1.write.format("delta").mode("overwrite").saveAsTable(tableName)
      null
    }
  }
}

class DeltaTablePartitionWriterFactory extends OperatorPipelineV3 with WriterSupport with Serializable {
  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer = {
    new DeltaTablePartitionWriter(pluginContext, operatorContext)
  }
}