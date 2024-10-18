package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor

import com.atgenomix.seqslab.piper.common.utils.FileUtil
import com.atgenomix.seqslab.piper.plugin.api.executor.{Executor, ExecutorSupport, SupportsFileLocalization}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor.BedExecutorFactory.BedExecutor
import org.apache.spark.sql.Row

import java.util
import scala.collection.mutable.ArrayBuffer


object BedExecutorFactory {
  class BedExecutor(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Executor
    with SupportsFileLocalization {

    private var path: String = _

    override def init(): Executor = this

    override def getOperatorContext: OperatorContext = operatorCtx

    override def setLocalPath(s: String): String = {
      path = s
      path
    }

    override def call(t1: util.Iterator[Row]): Integer = {
      val r = FileUtil.write(path){ bw =>
        val h = operatorCtx.get("bedHeader")
        assert(h != null, "BedExecutor: bedHeader is null")

        val header = h.asInstanceOf[ArrayBuffer[String]]
        if (header.nonEmpty) {
          bw.write(header.mkString("\n"))
          bw.write("\n")
        }

        while (t1.hasNext) {
          val row = t1.next()
          val bed = row.schema.fields
            // remove field generated from BedPartitioner
            .filter(f => f.name != "partId")
            .map { field => s"${row.getAs[String](field.name)}" }.mkString("\t")
          bw.write(bed)
          bw.write("\n")
        }
        // make sure write back to destination
        bw.flush()
      }
      r
    }

    override def close(): Unit = ()
  }
}

class BedExecutorFactory extends ExecutorSupport {
  override def createExecutor(pluginContext: PluginContext, operatorContext: OperatorContext): Executor = {
    new BedExecutor(pluginContext, operatorContext)
  }
}
