package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor

import com.atgenomix.seqslab.piper.common.utils.FileUtil
import com.atgenomix.seqslab.piper.plugin.api.executor.{Executor, ExecutorSupport, SupportsFileLocalization}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor.RegenieBgenExecutorFactory.RegenieBgenExecutor
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.glow.BgenRecordWriter
import io.projectglow.common.BgenOptions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.Paths
import java.util
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object RegenieBgenExecutorFactory {

  class RegenieBgenExecutor(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Executor
    with SupportsFileLocalization {

    private var path: String = _
    private var masterPath: String = _
    private var snpListPath: String = _
    private val snpListPrefix: String = "fit_parallel"
    private val writeHeader = true
    private val properties = operatorCtx.getProperties.asScala
    private val withMasterSnpList = properties.get(s"${this.getName}:masterSnpList").forall(_.asInstanceOf[String].toBoolean)
    private val withNumVariants = properties.get(s"${this.getName}:numVariants").forall(_.asInstanceOf[String].toBoolean)
    private val bitsPerProb = properties.getOrElse(BITS_PER_PROB_KEY, "8").asInstanceOf[String].toInt
    private val maxPloidy = properties.getOrElse(MAX_PLOIDY_KEY, MAX_PLOIDY_VALUE).asInstanceOf[String].toInt
    private val defaultPloidy = properties.getOrElse(DEFAULT_PLOIDY_KEY, DEFAULT_PLOIDY_VALUE).asInstanceOf[String].toInt
    private val defaultPhasing = properties.getOrElse(DEFAULT_PHASING_KEY, DEFAULT_PHASING_VALUE).asInstanceOf[String].toBoolean
    private var sampleIds: Option[String] = None

    override def init(): Executor = this

    override def setLocalPath(s: String): String = {
      val inputDir = Paths.get(s).getParent
      val inputDirString = inputDir.toString
      val workingDir = Paths.get(inputDirString.substring(0, inputDirString.length-7))
      snpListPath = workingDir.resolve(s"${snpListPrefix}_job1.snplist").toString
      masterPath = workingDir.resolve(s"$snpListPrefix.master").toString
      sampleIds = operatorCtx.getProperties.asScala.getOrElse("sampleIds", None).asInstanceOf[Option[String]]
      path = s
      path
    }

    override def call(t1: util.Iterator[Row]): Integer = {

      val insideSampleIds = sampleIds.map(_.split('|'))
      val snpList = new ListBuffer[String]()
      def row2InternalRow(value: Any): Any = {
        value match {
          case s: String => UTF8String.fromString(s)
          case a: mutable.WrappedArray[String] => ArrayData.toArrayData(a.map(row2InternalRow))
          case r: GenericRowWithSchema => InternalRow.fromSeq(r.toSeq.map(row2InternalRow))
          case any => any
        }
      }

      var numVariants = 0
      val partIds = scala.collection.mutable.Set[Int]()
      // Create .bgen file
      val r0 = if (withNumVariants) {
        val t2 = t1.toSeq
        numVariants = t2.length
        FileUtil.writeByteArray(path) { baos =>
          if (t2.nonEmpty) {
            val writer = new BgenRecordWriter(
              baos,
              t2.head.schema,
              writeHeader,
              t2.length,
              bitsPerProb,
              maxPloidy,
              defaultPloidy,
              defaultPhasing,
              insideSampleIds
            )
            t2.foreach { row =>
              if (withMasterSnpList) {
                val names = row.getList[String](row.fieldIndex("names")).asScala
                snpList.append(names(1))
                Try(row.getAs[Int]("partId")) match {
                  case Success(partId) => partIds += partId
                  case Failure(_) => partIds += 1
                }
              }
              val internalRow = InternalRow.fromSeq(row.toSeq.map(row2InternalRow))
              writer.write(internalRow)
            }
            // make sure write back to destination
            writer.close()
          }
          baos.flush()
        }
      } else {
        FileUtil.writeByteArray(path) { baos =>
          if (t1.hasNext) {
            numVariants += 1
            var row = t1.next()
            val writer = new BgenRecordWriter(
              baos,
              row.schema,
              writeHeader,
              Int.MaxValue,
              bitsPerProb,
              maxPloidy,
              defaultPloidy,
              defaultPhasing,
              insideSampleIds
            )
            do {
              if (withMasterSnpList) {
                val names = row.getList[String](row.fieldIndex("names")).asScala
                snpList.append(names(1))
                Try(row.getAs[Int]("partId")) match {
                  case Success(partId) => partIds += partId
                  case Failure(_) => partIds += 1
                }
              }
              val internalRow = InternalRow.fromSeq(row.toSeq.map(row2InternalRow))
              writer.write(internalRow)
              row = if (t1.hasNext) {
                numVariants += 1
                t1.next()
              } else null
            } while (row != null)
            // make sure write back to destination
            writer.close()
          }
          baos.flush()
        }
      }
      // Create .bgi file
      /*if (r == 0) {
        val (code, _, stderr) = ProcessUtil.executeAndGetStdout(List("samtools", "index", path))
        if (code != 0) println("Generate BAM index failed: " + stderr)
      }*/
      // Create master file
      val r1 = if (withMasterSnpList) {
        FileUtil.write(masterPath){ bw =>
          val output = new mutable.StringBuilder(operatorCtx.getProperties.asScala("master").toString)
          output.append(s"${snpListPrefix}_job1 ${partIds.size} $numVariants")
          bw.write(output.mkString)
        }
      } else 0
      // Create snp list file
      val r2 = if (withMasterSnpList) {
        FileUtil.write(snpListPath){ bw =>
          snpList.foreach { snp =>
            bw.write(s"$snp\n")
          }
          bw.flush()
        }
      } else 0
      r0 + r1 + r2
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}


class RegenieBgenExecutorFactory extends ExecutorSupport {
  override def createExecutor(pluginContext: PluginContext, operatorContext: OperatorContext): Executor = {
    new RegenieBgenExecutor(pluginContext, operatorContext)
  }
}
