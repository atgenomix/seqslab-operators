package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor

import com.atgenomix.seqslab.piper.plugin.api._
import com.atgenomix.seqslab.piper.plugin.api.executor._
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor.SqlExecutorFactory.TableLocalizationExecutor
import org.apache.spark.sql.{Dataset, Row}

object SqlExecutorFactory {
  private class TableLocalizationExecutor(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Executor
    with SupportsTableLocalization {

    private var namespace: String = _

    private var tableName: String = _

    override def getOperatorContext: OperatorContext = operatorCtx

    override def init(): Executor = this

    override def setTableName(name: String): Unit = {
      val nameParts = name.split('.')
      assert(nameParts.length == 2)
      namespace = nameParts(0)
      tableName = nameParts(1)
    }

    override def call(src: Dataset[Row]): Dataset[Row] = {
      src.sparkSession.sql(s"CREATE SCHEMA IF NOT EXISTS $namespace")
      src.createOrReplaceGlobalTempView(s"${namespace}_$tableName")
      val tableCmd = s"CREATE TABLE IF NOT EXISTS $namespace.$tableName AS SELECT * FROM global_temp.${namespace}_$tableName"
      src.sparkSession.sql(tableCmd)
      src
    }

    override def close(): Unit = ()
  }}

class SqlExecutorFactory extends ExecutorSupport {
  override def createExecutor(pluginCtx: PluginContext, operatorCtx: OperatorContext): Executor = {
    new TableLocalizationExecutor(pluginCtx, operatorCtx)
  }
}
