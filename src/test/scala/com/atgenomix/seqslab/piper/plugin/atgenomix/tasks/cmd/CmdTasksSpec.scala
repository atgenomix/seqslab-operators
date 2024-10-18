package com.atgenomix.seqslab.piper.plugin.atgenomix.tasks.cmd


import com.atgenomix.seqslab.piper.common.utils.HDFSUtil.getHadoopFileSystem
import com.atgenomix.seqslab.piper.engine.sql.{Args4j, PiedPiperArgs, PiperMain}
import com.atgenomix.seqslab.piper.plugin.atgenomix.SparkHadoopSessionBuilder
import com.atgenomix.seqslab.piper.plugin.atgenomix.SparkHadoopSessionBuilder.{jedis, server}
import models.{SeqslabPipeline, SeqslabWomArray, SeqslabWomFile, SeqslabWomInteger, SeqslabWomString, SingleDataset}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import play.api.libs.json.Json

import java.io.{File, FileInputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Date
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.io.Source
import scala.util.{Failure, Success, Try}


class CmdTasksSpec extends AnyFlatSpec with SparkHadoopSessionBuilder {

  override def hdfsPort: Int = 9000

  override def beforeAll(): Unit = {
    SparkHadoopSessionBuilder.updateAllTests(this.getClass.getSimpleName)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    SparkHadoopSessionBuilder.updateFinishTests(this.getClass.getSimpleName)
    super.afterAll()
  }

  it should "successfully run task without file localization" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_localization_free.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "Demo.CreateInterpretation:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutorLocalizationFree.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test1",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/cmd_executor_localization_free/",
      "--task-root", "hdfs://localhost:9000/plugin_test/cmd_executor_localization_free/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-1",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully run task without file localization $time")
  }

  /** Case to partition vcf file with provided hdfs Path
   * task vcfAnnotationTask {
   *   input {
   *     File vcf
   *     File ref
   *   }
   *   command <<<
   *     ls -l ~{ref}
   *     ./vep -i ~{vcf} --cache --force_overwrite --af_gnomade --vcf
   *   >>>
   *   output {
   *     File vcfOutput = "variant_effect_output.txt"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_cmd_executor_1.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_1.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "vcfDemo.vcfAnnotationTask:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutor1.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test1",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/cmd_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-1",
      "--redis-db", "0",
      "--dbg"
    )

    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully execute with input_mapping_cmd_executor_1.json and script.sh $time")

    assert(_hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/variant_effect_output1.txt")).length == 1)
    assert(_hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/variant_effect_output1.txt.tbi")).length == 1)
    assert(_hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/variant_effect_output2.txt")).length == 1)
  }

  it should "successfully execute with input_mapping_cmd_executor_bed.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_bed.json").getPath
    val scriptPath = getClass.getResource("/script/testCmdExecutorBed.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "vcfDemo.vcfAnnotationTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test1",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/cmd_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-1",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully execute with input_mapping_cmd_executor_bed.json and script.sh $time")
    assert(_hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/variant_effect_output155.bed")).length == 23)
    assert(_hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/variant_effect_output3101.bed")).length == 23)
  }

  it should "Repartition BAM file to 23 partitions where paired reads will be presented each partitions" in {
    val partNum = 23
    val udfName = "GRCh38Part23"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutor2.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_$udfName",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_$udfName/",
      "--task-root", s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-3",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Repartition BAM file to 23 partitions where paired reads will be presented each partitions $time")
    val files = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_${udfName}/outputs/output.bam"))
    assert(files.length == partNum)
  }

  it should "Repartition BAM file to 50Consensus partitions where paired reads will be presented each partitions" in {
    val partNum = 50
    val udfName = "GRCh38Part50Consensus"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutor2.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_$udfName",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_$udfName/",
      "--task-root", s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-6",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Repartition BAM file to 50Consensus partitions where paired reads will be presented each partitions $time")
    val files = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_${udfName}/outputs/output.bam"))
    assert(files.length == partNum)
  }

  it should "Repartition BAM file into 2 partitions with 2 reads spanning the boundary and thus should be duplicated in both partitions" in {
    val partNum = 2
    val udfName = "CrossPart"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutorSamtoolsView.sh").getPath
    val outputPath = s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/"
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_$udfName",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_$udfName/",
      "--task-root", outputPath,
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-6",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Repartition BAM file to 2 partitions where paired reads will be presented each partitions $time")
    val files = _hadoopFS.listStatus(new Path(s"${outputPath}/output.sam"))
    assert(files.length == partNum)

    Seq("00000", "00001").map(
      x => {
        val src = s"${outputPath}/output.sam/part-${x}-output.sam"
        val dst = s"/tmp/res/part-${x}-result.sam"

        _hadoopFS.copyToLocalFile(new Path(src), new Path(dst))
        Try(Source.fromFile(dst)) match {
          case Success(f) =>
            assert(f.getLines().length == 2)
            f.close()
          case Failure(exception) =>
            throw exception
        }
      }
    )
  }

  it should "Repartition BAM file filtered with reads discordantly align to 2 chromosomal regions chr1 and chrX" in {
    val partNum = 2
    val udfName = "chr1NXConsensus"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutorSamtoolsView.sh").getPath
    val outputPath = s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/"
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_$udfName",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_$udfName/",
      "--task-root", outputPath,
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-6",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Repartition BAM file to 2 partitions where paired reads will be presented each partitions $time")
    val files = _hadoopFS.listStatus(new Path(s"${outputPath}/output.sam"))
    assert(files.length == partNum)

    Seq("00000", "00001").map(
      x => {
        val src = s"${outputPath}/output.sam/part-${x}-output.sam"
        val dst = s"/tmp/res/part-${x}-result.sam"

        _hadoopFS.copyToLocalFile(new Path(src), new Path(dst))
        Try(Source.fromFile(dst)) match {
          case Success(f) =>
            if (x == "00000") assert(f.getLines().length == 28)
            else assert(f.getLines().length == 12)
            f.close()
          case Failure(exception) =>
            throw exception
        }
      }
    )
  }

  it should "Repartition BAM file with alternative contigs not in bed intervals should be sorted based on seqDict" in {
    val partNum = 1
    val udfName = "alt_contig_sort"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutorSamtoolsView.sh").getPath
    val outputPath = s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/"
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_$udfName",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_$udfName/",
      "--task-root", outputPath,
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-6",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Repartition BAM file to 2 partitions where paired reads will be presented each partitions $time")
    val src = s"$outputPath/output.sam"
    val files = _hadoopFS.listStatus(new Path(src))
    assert(files.length == partNum)
    val dst = s"/tmp/res/result.sam"
    _hadoopFS.copyToLocalFile(new Path(src), new Path(dst))
    Try(Source.fromFile(dst)) match {
      case Success(f) =>
        assert(f.getLines().length == 6628)
        f.close()
      case Failure(exception) =>
        throw exception
    }
  }

  it should "Repartition single-end BAM file using consensus bam partitioner" in {
    val partNum = 2
    val udfName = "single-end-consensus"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutorSamtoolsView.sh").getPath
    val outputPath = s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/"
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_$udfName",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_$udfName/",
      "--task-root", outputPath,
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-6",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Repartition BAM file to 2 partitions where paired reads will be presented each partitions $time")
    val files = _hadoopFS.listStatus(new Path(s"${outputPath}/output.sam"))
    assert(files.length == partNum)

    Seq("00000", "00001").map(
      x => {
        val src = s"${outputPath}/output.sam/part-${x}-output.sam"
        val dst = s"/tmp/res/part-${x}-result.sam"

        _hadoopFS.copyToLocalFile(new Path(src), new Path(dst))
        Try(Source.fromFile(dst)) match {
          case Success(f) =>
            if ( x == "00000") assert(f.getLines().length == 12)
            else assert(f.getLines().isEmpty)
            f.close()
          case Failure(exception) =>
            throw exception
        }
      }
    )
  }

  it should "Repartition and dropDuplicate records using Bam1 operator to handle ConsensusBam operator generated Bam" in {
    val partNum = 1
    val udfName = "Part1_consensus_inputs"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutorSamtoolsView.sh").getPath
    val outputPath = s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/"
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_$udfName",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_$udfName/",
      "--task-root", s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-7",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Repartition BAM file to 1 partitions where paired reads will be presented each partitions $time")
    val src = s"$outputPath/output.sam"
    val files = _hadoopFS.listStatus(new Path(src))
    assert(files.length == partNum)
    val dst = s"/tmp/res/output.sam"
    _hadoopFS.copyToLocalFile(new Path(src), new Path(dst))
    Try(Source.fromFile(dst)) match {
      case Success(f) =>
        assert(f.getLines().length == 28)
        f.close()
      case Failure(exception) =>
        throw exception
    }
  }

  it should "Repartition BAM file to 1 partitions where paired reads will be presented each partitions" in {
    val partNum = 1
    val udfName = "Part1"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutor2.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_$udfName",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_$udfName/",
      "--task-root", s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-7",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Repartition BAM file to 1 partitions where paired reads will be presented each partitions $time")
    val files = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_${udfName}/outputs/output.bam"))
    assert(files.length == partNum)
  }

  it should "Repartition BAM file to 1Unmap partitions where paired reads will be presented each partitions" in {
    val partNum = 1
    val udfName = "Part1Unmap"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutor2.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_$udfName",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_$udfName/",
      "--task-root", s"hdfs://localhost:9000/plugin_test/cmd_executor_test_$udfName/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-8",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Repartition BAM file to 1Unmap partitions where paired reads will be presented each partitions $time")
    Thread.sleep(1000)
    val files = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_${udfName}/outputs/output.bam"))
    assert(files.length == partNum)
  }

  it should "successfully process Fastq data" in {
    val hadoopConf = new Configuration()
    val outputPrefix = "outputs/cmd_executor_test_fastq/"
    val outputCheck = "outputs/cmd_executor_test_fastq/call-Fastp/NA12878_fastp_R2.fastq.gz/"
    val backends = Seq(
      ("hdfs://localhost:9000/", "hdfs://localhost:9000/", "cloud"),
      //("hdfs://localhost:9000/", "file:///tmp/we2/", "local"),
      //(s"file://${getClass.getResource("/").getPath}", "file:///tmp/we2/", "local"),
    )
    backends.map { case (iSrc, oSrc, cloud) =>
      val properties = Map(
        "inputType" -> s"$iSrc".split(":").head,
        "inputPrefix" -> s"$iSrc",
        "outputType" -> s"$oSrc$outputPrefix".split(":").head,
        "outputPrefix" -> s"$oSrc$outputPrefix"
      )
      val filePath = getInputMappingPath("/inputMappings/input_mapping_cmd_executor_fastq.json", properties)
      val args = Array(
        "--workflow-req-src", "File",
        "--task-fqn", "GermlineSnpsIndelsGatk4Hg19.PreProcessingForVariantDiscovery_GATK4.Fastp:x",
        "--file-path", filePath.toString,
        "--script-path", getClass.getResource("/script/testCmdExecutorFastq.sh").getPath,
        "--run-name", "test-run-name",
        "--workflow-id", "run_cmd_test_fastq",
        "--org", "cus_OqsOKDdinNaWW7s",
        "--tenant", "tenant",
        "--region", "westus2",
        "--cloud", cloud,
        "--user", "usr_gNGAlr1m0EYMbEx",
        "--fs-root", oSrc,
        "--task-root", oSrc,
        "--redis-url", server.getHost,
        "--redis-port", server.getBindPort.toString,
        "--redis-key", "test-9",
        "--redis-db", "0",
        "--dbg"
      )
      val st = new Date().getTime
      try {
        val hadoopConfMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
        hadoopConfMap.foreach { case (k, v) => hadoopConf.set(k, v) }
        new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopConfMap)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        new File(filePath.toString).deleteOnExit()
      }
      val ed = new Date().getTime
      val time = (ed - st) / 1000
      println(s"TIME: successfully process Fastq data $time")
      val files = getHadoopFileSystem(new URI(s"$oSrc$outputCheck"), hadoopConf).listStatus(new Path(s"$oSrc$outputCheck"))
      assert(files.length == 1)
    }
  }

  /** Case to partition vcf file with provided hdfs Path
   * task BaseRecalibrator {
   *   input {
   *     Array[String] recalibration_report_filename
   *     ...
   *   }
   *   command <<<
   *     set -e -o pipefail
   *     ls -l ~{write_lines(recalibration_report_filename)}
   *     echo ~{sep=" " SampleNames}
   *     echo ~{sep='--known-sites' known_indels_sites_indices}
   *     cp ~{input_bam_index[0]} ~{recalibration_report_filename[0]}
   *     cp ~{input_bam_index[1]} ~{recalibration_report_filename[1]}
   *   >>>
   *   output {
   *     ...
   *   }
   * }
   */
  it should "successfully localize Array[File] with RefLoader settings" in {
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_array_file.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutorArrayFile.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"GermlineSnpsIndelsGatk4Hg19.PreProcessingForVariantDiscovery_GATK4.BaseRecalibrator:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_array_file",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_array_file/",
      "--task-root", s"hdfs://localhost:9000/plugin_test/cmd_executor_test_array_file/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-10",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully localize Array[File] with RefLoader settings $time")
    Thread.sleep(1000)
    val files0 = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_array_file/outputs/call-BaseRecalibrator/NA12878.hg19.recal_data0.csv/"))
    val files1 = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_array_file/outputs/call-BaseRecalibrator/NA12878.hg19.recal_data1.csv/"))
    assert(files0.length == 1)
    assert(files1.length == 1)
  }

  it should "successfully localize Array[File] with SingleNodeDataLoader settings" in {
    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_SingleNodeDataLoader.json").getPath
    val scriptPath = getClass.getResource("/script/testCmdExecutorSingleNodeDataLoader.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"GermlineSnpsIndelsGatk4Hg19.PreProcessingForVariantDiscovery_GATK4.BaseRecalibrator:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--run-name", "test-run-name",
      "--workflow-id", s"run_cmd_test_array_file",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_array_file/",
      "--task-root", s"hdfs://localhost:9000/plugin_test/cmd_executor_test_array_file/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-10",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully localize Array[File] with SingleNodeDataLoader settings $time")
    Thread.sleep(1000)
    val dst = "hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/SingleNodeDataLoader/"
    val files = _hadoopFS.listStatus(new Path(dst))
    assert(files.length == 1)
    _hadoopFS.copyToLocalFile(new Path(dst), new Path("/tmp/res/output"))
    val f = Source.fromFile("/tmp/res/output")
    val fileContents = f.getLines.mkString
    f.close()
    assert(fileContents == "part-0.vcfpart-1.vcf" )
  }

  it should "successfully execute with input_mapping_cmd_executor_std_function.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_std_function.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "vcfDemo.vcfAnnotationTask:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutorStdFunc.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test11",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/std_func_cmd_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/std_func_cmd_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-11",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully execute with input_mapping_cmd_executor_std_function.json and script.sh $time")

    val pipeline = Json.parse(jedis.get("test-11:output_mapping:vcfDemo.vcfAnnotationTask:x")).validate[SeqslabPipeline].get
    assert(pipeline.outputs("vcfDemo.vcfAnnotationTask.stdFuncOutput1").getInteger == 23)
  }

  it should "successfully execute PiedPiper with input_mapping_cmd_executor_bgen.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_bgen.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "PlinkBedReading.Task1:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutorBgen.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test12",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/bgen_cmd_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/bgen_cmd_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-12",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully execute PiedPiper with input_mapping_cmd_executor_bgen.json and script.sh $time")
    val pipeline = Json.parse(jedis.get("test-12:output_mapping:PlinkBedReading.Task1:x")).validate[SeqslabPipeline].get
    val master = pipeline.datasets("PlinkBedReading.Task1.masterFile").asInstanceOf[SingleDataset].single.accessMethods.get.head.accessUrl.url
    val snpList = pipeline.datasets("PlinkBedReading.Task1.snpList").asInstanceOf[SingleDataset].single.accessMethods.get.head.accessUrl.url
    val l0_Y1 = pipeline.datasets("PlinkBedReading.Task1.l0_Y1").asInstanceOf[SingleDataset].single.accessMethods.get.head.accessUrl.url
    _hadoopFS.listStatus(new Path(master)).foreach(println)
    _hadoopFS.listStatus(new Path(snpList)).foreach(println)
    _hadoopFS.listStatus(new Path(l0_Y1)).foreach(println)
    assert(_hadoopFS.listStatus(new Path(master)).length == 1)
    assert(_hadoopFS.listStatus(new Path(snpList)).length == 6)
    val masterDir = Files.createTempDirectory("fit_parallel")
    _hadoopFS.listStatus(new Path(master)).foreach(p => {
      _hadoopFS.copyToLocalFile(p.getPath, new Path(masterDir.toAbsolutePath.toString))
    })
    Files.list(masterDir)
      .filter(p => p.toAbsolutePath.toString.endsWith(".master"))
      .forEach(p => {
        val fis = new FileInputStream(p.toFile)
        val body = IOUtils.toString(fis, StandardCharsets.UTF_8.name())
        println(body)
        assert(body.split("\n").length == 7)
      })
  }

  it should "successfully execute PiedPiper with input_mapping_cmd_executor_binary.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_binary.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutor2.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test13",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/binary_cmd_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/binary_cmd_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-13",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully execute PiedPiper with input_mapping_cmd_executor_binary.json and script.sh $time")
    val _ = Json.parse(jedis.get("test-13:output_mapping:bamDemo.bamPartitionTask:x")).validate[SeqslabPipeline].get
  }

  it should "successfully execute PiedPiper with input_mapping_cmd_executor_partition_collect.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_partition_collect.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "vcfDemo.vcfAnnotationTask:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutorPartitionCollect.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test_binary_collect",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/cmd_executor_partition_collect_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/cmd_executor_partition_collect_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-14",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully execute PiedPiper with input_mapping_cmd_executor_partition_collect.json and script.sh $time")
    assert(_hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_partition_collect_test/outputs/variant_effect_output1")).length == 2)
    assert(_hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_partition_collect_test/outputs/variant_effect_output2")).length == 2)

    val txt0 = new Path("hdfs://localhost:9000/plugin_test/cmd_executor_partition_collect_test/outputs/variant_effect_output1/0.txt")
    val is = _hadoopFS.open(txt0)
    val content = new Array[Byte](6)
    val numReads = is.read(content)
    assert(numReads == 6)
    assert(content sameElements "test0\n".toCharArray.map(_.toByte))

    val txt1 = new Path("hdfs://localhost:9000/plugin_test/cmd_executor_partition_collect_test/outputs/variant_effect_output1/1.txt")
    val is1 = _hadoopFS.open(txt1)
    val numReads2 = is1.read(content)
    assert(numReads2 == 6)
    assert(content sameElements "test1\n".toCharArray.map(_.toByte))
  }

  it should "successfully execute PiedPiper with input_mapping_cmd_executor_partition_collect_file.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_partition_collect_file.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "vcfDemo.vcfAnnotationTask:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutorPartitionCollectFile.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test_binary_collect",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/cmd_executor_partition_collect_file_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/cmd_executor_partition_collect_file_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-15",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully execute PiedPiper with input_mapping_cmd_executor_partition_collect_file.json and script.sh $time")
    val txt0 = new Path("hdfs://localhost:9000/plugin_test/cmd_executor_partition_collect_file_test/outputs/variant_effect_output1")
    val is = _hadoopFS.open(txt0)
    val content = new Array[Byte](6)
    val numReads = is.read(content)
    assert(numReads == 6)
    assert(content sameElements "test0\n".toCharArray.map(_.toByte))

    val txt1 = new Path("hdfs://localhost:9000/plugin_test/cmd_executor_partition_collect_file_test/outputs/variant_effect_output2")
    val is1 = _hadoopFS.open(txt1)
    val numReads2 = is1.read(content)
    assert(numReads2 == 6)
    assert(content sameElements "test1\n".toCharArray.map(_.toByte))
  }

  it should "transform MAF json to VCF successfully" in {
    val filePath = getInputMappingPath("/inputMappings/input_mapping_cmd_executor_maf_to_vcf.json")
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "vcfDemo.vcfAnnotationTask:x",
      "--file-path", filePath.toString,
      "--script-path", getClass.getResource("/script/testCmdExecutorMafToVcf.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test16",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/maf_to_vcf_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/maf_to_vcf_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-16",
      "--redis-db", "0",
      "--dbg"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: transform MAF json to VCF successfully $time")

    val src = "hdfs://localhost:9000/plugin_test/maf_to_vcf_test/outputs/final_report.vcf"
    val dst = "/tmp/res/final_report.vcf"
    _hadoopFS.copyToLocalFile(new Path(src), new Path(dst))
    Try(Source.fromFile(dst)) match {
      case Success(f) =>
        assert(f.getLines().length == 522)
        f.close()
      case Failure(exception) =>
        throw exception
    }
  }

  /**
   * task CreateInterpretation{
   *   input{
   *     String phenopacketID
   *     String runDate
   *     String ID
   *     String diseaseID
   *   }
   *
   *   command <<<
   *     set -e -o pipefail
   *   >>>
   *
   *   output{
   *     String diagnosisID = read_string("did.txt")
   *     String interpretationID = read_string("iid.txt")
   *     String runDateStr = read_string("rundate.txt")
   *     Int readInt = read_int("read_int.txt")
   *     Array[String] readLines = read_lines("read_lines.txt")
   *   }
   */
  it should "execute task with dummy loader" in {
    val filePath = getInputMappingPath("/inputMappings/input_mapping_cmd_executor_dummy_loader.json")
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "NIPT1.CreateInterpretation:x",
      "--file-path", filePath.toString,
      "--script-path", getClass.getResource("/script/testCmdExecutorDummy.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test17",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/dummy_loader/",
      "--task-root", "hdfs://localhost:9000/plugin_test/dummy_loader/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-17",
      "--redis-db", "0"
    )
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: execute task with dummy loader $time")
    val s = jedis.get("test-17:output_mapping:NIPT1.CreateInterpretation:x")
    val pipeline = Json.parse(s).validate[SeqslabPipeline].get
    assert(pipeline.outputs("NIPT1.CreateInterpretation.readInt").asInstanceOf[SeqslabWomInteger].getInteger().equals(10))
    assert(pipeline.outputs("NIPT1.CreateInterpretation.readLinesTest").asInstanceOf[SeqslabWomArray].array.get
      .map(a => a.asInstanceOf[SeqslabWomString].string.get).equals(Seq("test", "test1")))
  }

  /** Case to partition vcf file with provided hdfs Path
   * task vcfPhasingPartitioner {
   *   input {
   *     File vcf
   *   }
   *   command <<
   *     echo ~{jsonpath("$.dataset.partition().contigName")}:\
   *     ~{jsonpath("$.dataset.partition().contigName")}.b38.gmap.gz > returnValues.txt
   *   >>>
   *   output {
   *     FILE returnValues = "returnValues.txt"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_cmd_executor_VcfPhasingPartitioner.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_VcfPhasingPartitioner.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "vcfDemo.vcfPhasingPartitioner:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutorVcfPhasingPartitioner.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test18",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/vcfPhasingPartitioner_cmd_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/vcfPhasingPartitioner_cmd_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-18",
      "--redis-db", "0",
      "--dbg"
    )

    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully execute with input_mapping_cmd_executor_VcfPhasingPartitioner.json and script.sh $time")
    val dst = "hdfs://localhost:9000/plugin_test/vcfPhasingPartitioner_cmd_executor_test/outputs/returnValues"
    _hadoopFS.copyToLocalFile(new Path(s"${dst}/part-00000-returnValues.txt"), new Path("/tmp/res/part-00000-returnValues.txt"))
    val f1 = Source.fromFile("/tmp/res/part-00000-returnValues.txt")
    val fileContents = f1.getLines.mkString("\n")
    assert(fileContents.equals("chr1:chr1.b38.gmap.gz"))
    f1.close()
  }

  it should "get job UUID from input mappings" in {
    val url = models.AccessURL("abfss://seqslab@sponsorwus2f40castorage.dfs.core.windows.net/outputs/GermlineAnalysis/c2d5c274-5347-414e-bce9-6816547949e0/call-CallingAndQC/shard-0/GermlineCallingAndQC/b5dfb62b-8c81-4773-98ee-a272469cc9e4/call-mapping/Bwa/886f6bb9-44f6-4a1f-9d70-f593eaac903c/call-Fastp/HG002.novaseq.pcr-free.30x.R1.fastq.gz", Some(Map.empty))
    val methods = Seq(models.AccessMethod("abfss", url, "westus3"))
    val drs = models.DrsObject(
      id = "drs_aM8uEpsvfy4DMhU",
      name = Some("part-00000.bam"),
      self_uri = Some("drs://dev-api.seqslab.net/drs_aM8uEpsvfy4DMhU"),
      file_type = Some("bam"),
      mime_type = Some(""),
      description = None,
      metaData = None,
      size = None,
      tags = Some(Seq.empty),
      createdTime = Some("2022-09-28T03:44:41.506377Z"),
      updatedTime = Some("2022-09-28T03:44:41.506377Z"),
      version = None,
      checksums = Some(Seq(models.Checksum("f2209abe3e73ca628185c668140f37ba819dd51cf5bc8b50a19adfded4dedab7", "sha256"))),
      contents = None,
      accessMethods = Some(methods))
    val outputWomValues = Map(
      "bamOutput" -> SeqslabWomFile(None, Some(drs))
    )
    val uuid = PiperMain.getJobUuid(outputWomValues)
    assert(uuid == "c2d5c274-5347-414e-bce9-6816547949e0")
  }

  /** Case to partition vcf file with provided hdfs Path
   * task vcfImputePartitioner {
   *   input {
   *     File vcf
   *   }
   *   command <<
   *     echo ~{jsonpath("$.dataset.partition().contigName")}:\
   *     ~{jsonpath("$.dataset.partition().start")}-\
   *     ~{jsonpath("$.dataset.partition().end")},\
   *     bufferRegion:~{jsonpath("$.dataset.partition().bufferedRegion")},\
   *     imputationRegion:~{jsonpath("$.dataset.partition().imputationRegion")} > returnValues.txt
   *   >>>
   *   output {
   *     FILE returnValues = "returnValues.txt"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_cmd_executor_VcfImputePartitioner.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_VcfImputePartitioner.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "vcfDemo.vcfImputePartitioner:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutorVcfImputePartitioner.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test20",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/vcfImputePartitioner_cmd_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/vcfImputePartitioner_cmd_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-20",
      "--redis-db", "0",
      "--dbg"
    )

    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: successfully execute with input_mapping_cmd_executor_VcfImputePartitioner.json and script.sh $time")
    val dst = "hdfs://localhost:9000/plugin_test/vcfImputePartitioner_cmd_executor_test/outputs/returnValues"
    _hadoopFS.copyToLocalFile(new Path(s"${dst}/part-00000-returnValues.txt"), new Path("/tmp/res/part-00000-returnValues.txt"))
    val f1 = Source.fromFile("/tmp/res/part-00000-returnValues.txt")
    val file1Contents = f1.getLines.mkString("\n")
    assert(file1Contents.equals("chr1:800000-900000,bufferRegion:chr1:800000-900000,imputationRegion:chr1:805000-895000"))
    f1.close()
    _hadoopFS.copyToLocalFile(new Path(s"${dst}/part-00003-returnValues.txt"), new Path("/tmp/res/part-00003-returnValues.txt"))
    val f2 = Source.fromFile("/tmp/res/part-00003-returnValues.txt")
    val file2Contents = f2.getLines.mkString("\n")
    assert(file2Contents.equals("chr2:1200001-1300000,bufferRegion:chr2:1200001-1300000,imputationRegion:chr2:1205001-1295000"))
    f2.close()
  }

  it should "execute task with Batch loader" in {
    val filePath = getInputMappingPath("/inputMappings/input_mapping_cmd_executor_BatchDataLoader.json")
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "scatterDemo.bedPartitionTask:x",
      "--file-path", filePath.toString,
      "--script-path", getClass.getResource("/script/testCmdExecutorBatchDataLoader.sh").getPath,
      "--run-name", "test-run-name",
      "--workflow-id", "run_cmd_test18",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/scatter/",
      "--task-root", "hdfs://localhost:9000/plugin_test/scatter/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-18",
      "--redis-db", "0",
      "--dbg"
    )

    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: execute task with batch loader $time")
  }
}
