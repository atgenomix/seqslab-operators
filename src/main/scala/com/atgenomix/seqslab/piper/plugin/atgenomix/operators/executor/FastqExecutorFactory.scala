package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor

import com.atgenomix.seqslab.piper.common.genomics.Fastq
import com.atgenomix.seqslab.piper.common.utils.FileUtil
import com.atgenomix.seqslab.piper.plugin.api.executor.{Executor, ExecutorSupport, SupportsFileLocalization}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor.FastqExecutorFactory.FastqExecutor
import org.apache.spark.sql.Row

import java.util

object FastqExecutorFactory {
  private class FastqExecutor(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Executor
    with SupportsFileLocalization {

    private var path: String = _

    override def init(): Executor = this

    override def setLocalPath(s: String): String = {
      path = s
      path
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def call(iter: util.Iterator[Row]): Integer = {
      FileUtil.writeGz(path){ bw =>
        while (iter.hasNext) {
          val row = iter.next()
          val fq = Fastq(row)
          bw.write(s"${fq.toStringContent}\n")
        }
      }
    }

    override def close(): Unit = ()
  }
}

class FastqExecutorFactory extends ExecutorSupport {
  override def createExecutor(pluginContext: PluginContext, operatorContext: OperatorContext): Executor = {
    new FastqExecutor(pluginContext, operatorContext)
  }
}
