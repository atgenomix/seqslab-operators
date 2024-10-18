package com.atgenomix.seqslab.operators.writer

import com.atgenomix.seqslab.piper.common.operator.AbstractDeltaTableWriter
import com.atgenomix.seqslab.piper.common.utils.DeltaTableUtil.createDeltaTable
import com.atgenomix.seqslab.piper.plugin.api.writer.{Writer, WriterSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.operators.writer.DeltaTableWriterFactory.DeltaTableWriter
import org.apache.spark.sql
import org.apache.spark.sql.Row

import java.lang

object DeltaTableWriterFactory{
  private class DeltaTableWriter(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends AbstractDeltaTableWriter(pluginCtx, operatorCtx) {

    override def call(t1: sql.Dataset[Row], t2: lang.Boolean): Void = {
      val tableName = createDeltaTable(fqn, db, dataSource)(t1.sparkSession)
      t1.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(tableName)
      null
    }
  }
}

class DeltaTableWriterFactory extends OperatorPipelineV3 with WriterSupport with Serializable {
  override def createWriter(pluginContext: PluginContext, operatorContext: OperatorContext): Writer = {
    new DeltaTableWriter(pluginContext, operatorContext)
  }
}
