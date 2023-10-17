package com.atgenomix.seqslab.operators.executor

import com.atgenomix.seqslab.operators.executor.CsvExecutorFactory.CsvExecutor
import com.atgenomix.seqslab.piper.common.utils.FileUtil
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.executor.{Executor, ExecutorSupport, SupportsFileLocalization}
import org.apache.spark.sql.Row

import java.util

object CsvExecutorFactory {
  private class CsvExecutor(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Executor
    with SupportsFileLocalization {
    private var path: String = _

    override def init(): Executor = this

    override def setLocalPath(s: String): String = {
      path = s
      path
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def call(iter: util.Iterator[Row]): Integer = {
      FileUtil.write(path) { bw =>
        if (iter.hasNext) {
          val row = iter.next()
          // write header with # prefix
          val header = row.schema.fields.map(_.name).mkString("" ,"\t", "\n")
          bw.write(header)
          // write first row
          val first = row.mkString("" ,"\t", "\n")
          bw.write(first)
        }
        // write others
        while (iter.hasNext) {
          val row = iter.next()
          bw.write(row.mkString("" ,"\t", "\n"))
        }
      }
    }

    override def close(): Unit = ()
  }}

class CsvExecutorFactory extends ExecutorSupport {
  override def createExecutor(pluginContext: PluginContext, operatorContext: OperatorContext): Executor = {
    new CsvExecutor(pluginContext, operatorContext)
  }
}
