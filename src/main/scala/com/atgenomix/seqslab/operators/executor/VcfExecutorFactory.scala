package com.atgenomix.seqslab.operators.executor

import com.atgenomix.seqslab.operators.executor.VcfExecutorFactory.VcfExecutor
import com.atgenomix.seqslab.piper.common.utils.{FileUtil, ProcessUtil}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.executor.{Executor, ExecutorSupport, SupportsFileLocalization}
import org.apache.spark.sql.Row

import java.io.{BufferedWriter, OutputStreamWriter}
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
        val bw = new BufferedWriter(new OutputStreamWriter(bos))
        val h = operatorCtx.get("vcfHeader").asInstanceOf[ArrayBuffer[String]]
        bw.write(h.mkString("\n"))
        bw.write("\n")
        while (t1.hasNext) {
          val row = t1.next()
          val vcf = row.schema.fields
            // remove field generated from VcfPartitioner
            .filter(f => f.name != "partId")
            .map { field => s"${row.getAs[String](field.name)}" }.mkString("\t")
          bw.write(vcf)
          bw.write("\n")
        }
        // make sure write back to destination
        bw.flush()
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
