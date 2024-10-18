package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor

import com.atgenomix.seqslab.piper.common.utils.{FileUtil, ProcessUtil}
import com.atgenomix.seqslab.piper.plugin.api.executor.{Executor, ExecutorSupport, SupportsFileLocalization}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor.BamExecutorFactory.BamExecutor
import htsjdk.samtools.util.BinaryCodec
import htsjdk.samtools.{SAMFileHeader, SAMTextHeaderCodec}
import org.apache.spark.sql.Row

import java.io.{BufferedOutputStream, OutputStream, StringWriter}
import java.nio.charset.Charset
import java.util
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object BamExecutorFactory {
  class BamExecutor(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Executor
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
        val header = operatorCtx.get("bamHeader")
        assert(header != null, "BamExecutor: bamHeader is null")

        writeHeader(bos, header.asInstanceOf[SAMFileHeader])

        val bufferedOS = new BufferedOutputStream(bos)
        while (t1.hasNext) {
          val row = t1.next()
          val bam = row.getAs[Array[Byte]]("raw")
          bufferedOS.write(bam)
        }

        // make sure write back to destination
        bufferedOS.flush()
      }

      if (r == 0) {
        val (code, _, stderr) = ProcessUtil.executeAndGetStdout(List("samtools", "index", path))
        if (code != 0) println("Generate BAM index failed: " + stderr)
      }

      r
    }

    override def close(): Unit = ()

    private def writeHeader(os: OutputStream, header: SAMFileHeader): Unit = {
      val binaryCodec = new BinaryCodec(os)
      binaryCodec.writeBytes("BAM\001".getBytes(Charset.forName("UTF8")))
      val sw = new StringWriter
      new SAMTextHeaderCodec().encode(sw, header)
      binaryCodec.writeString(sw.toString, true, false)
      val dict = header.getSequenceDictionary
      binaryCodec.writeInt(dict.size)
      for (rec <- dict.getSequences.asScala) {
        binaryCodec.writeString(rec.getSequenceName, true, true)
        binaryCodec.writeInt(rec.getSequenceLength)
      }
    }
  }
}

class BamExecutorFactory extends ExecutorSupport {
  override def createExecutor(pluginContext: PluginContext, operatorContext: OperatorContext): Executor = {
    new BamExecutor(pluginContext, operatorContext)
  }
}
