package com.atgenomix.seqslab.operators.executor

import com.atgenomix.seqslab.piper.common.utils.{FileUtil, ProcessUtil}
import com.atgenomix.seqslab.piper.plugin.api.executor.{Executor, ExecutorSupport, SupportsFileLocalization}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.operators.executor.VcfExecutorFactory.VcfExecutor
import org.apache.spark.sql.Row

import java.io.BufferedOutputStream
import java.util
import scala.collection.mutable.ArrayBuffer


object VcfExecutorFactory {
  class VcfExecutor(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Executor
    with SupportsFileLocalization {

    private var path: String = _

    override def init(): Executor = this

    override def getOperatorContext: OperatorContext = operatorCtx

    override def setLocalPath(s: String): String = {
      path = s
      path
    }

    override def call(t1: util.Iterator[Row]): Integer = {
      val r = FileUtil.writeBgz(path){ bos =>
        val h = operatorCtx.get("vcfHeader")
        assert(h != null, "VcfExecutor: vcfHeader is null")

        val header = h.asInstanceOf[ArrayBuffer[String]]
        val bufferedOS = new BufferedOutputStream(bos)
        bufferedOS.write(header.mkString("\n").getBytes)
        bufferedOS.write("\n".getBytes)

        while (t1.hasNext) {
          val row = t1.next()
          val vcf = row.schema
            .fields
            // remove fields generated from VcfPartitioner
            .filter(f => f.name != "partId" && f.name != "col")
            .map { field => s"${row.getAs[String](field.name)}" }
            .mkString("\t")

          bufferedOS.write(s"$vcf\n".getBytes)
        }
        // make sure write back to destination
        bufferedOS.flush()
      }

      if (r == 0) {
        val (code, _, stderr) = ProcessUtil.executeAndGetStdout(List("tabix", "-f", path))
        if (code != 0) println("Generate VCF index failed: " + stderr)
      }

      r
    }

    override def close(): Unit = ()
  }
}

class VcfExecutorFactory extends ExecutorSupport {
  override def createExecutor(pluginContext: PluginContext, operatorContext: OperatorContext): Executor = {
    new VcfExecutor(pluginContext, operatorContext)
  }
}
