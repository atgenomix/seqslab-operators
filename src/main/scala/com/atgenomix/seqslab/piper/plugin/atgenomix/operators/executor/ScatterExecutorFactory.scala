package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor

import com.atgenomix.seqslab.piper.common.utils.ProcessUtil.execute
import com.atgenomix.seqslab.piper.plugin.api.executor.{Executor, ExecutorSupport, SupportsFileLocalization}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor.ScatterExecutorFactory.ScatterExecutor
import org.apache.spark.sql.Row

import java.nio.file.{Path, Paths}
import java.util
import scala.annotation.tailrec
import scala.util.{Failure, Random, Success, Try}

object ScatterExecutorFactory {
  class ScatterExecutor(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Executor with SupportsFileLocalization {

    private var inputsDir: Path = _

    override def init(): Executor = this

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()

    override def setLocalPath(p: String): String =  {
      inputsDir = Paths.get(p)
      p
    }

    override def call(t1: util.Iterator[Row]): Integer = {
      @tailrec
      def download(ip: String, src: String, dst: Path, count: Int): Unit = {
        Try {
          // random sleep to prevent scp storm
          Thread.sleep(Random.nextInt(30) * 1000)
          execute(List("scp", "-P", "2022", "-r", s"root@$ip:$src/", inputsDir.toString))
          // execute(List("cp", "-r", s"$src/", inputsDir.toString))
        } match {
          case Success(_) =>
            execute(List("ssh", s"root@$ip", "-p", "2022", "rm", "-rf", src))
            // execute(List("rm", "-rf", src))
            ()
          case Failure(_) if count > 0 =>
            download(ip, src, dst, count - 1)
          case Failure(exception) =>
            throw exception
        }
      }

      while (t1.hasNext) {
        val row = t1.next()
        val ip = new String(row.getAs[Array[Byte]]("ip"))
        val src = new String(row.getAs[Array[Byte]]("path"))
        val fileName = Paths.get(src).getFileName
        val dst = inputsDir.resolve(fileName)

        download(ip, src, dst, 10)
      }

      0
    }
  }
}

class ScatterExecutorFactory extends ExecutorSupport {
  override def createExecutor(pluginContext: PluginContext, operatorContext: OperatorContext): Executor = {
    new ScatterExecutor(pluginContext, operatorContext)
  }
}
