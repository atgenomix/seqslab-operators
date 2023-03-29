package com.atgenomix.seqslab.tasks.cmd


import com.atgenomix.seqslab.piper.common.utils.HDFSUtil.getHadoopFileSystem
import com.atgenomix.seqslab.piper.engine.cli.{Args4j, PiedPiperArgs}
import com.atgenomix.seqslab.piper.engine.sql.PiperMain
import com.atgenomix.seqslab.SparkHadoopSessionBuilder
import com.atgenomix.seqslab.SparkHadoopSessionBuilder.server
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.ParallelTestExecution
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.net.URI
import scala.io.Source
import scala.jdk.CollectionConverters.mapAsScalaMapConverter


class CmdTasksSpec extends AnyFlatSpec with SparkHadoopSessionBuilder with ParallelTestExecution {

  override def hdfsPort: Int = 9000

  override def beforeAll(): Unit = {
    SparkHadoopSessionBuilder.updateAllTests(this.getClass.getSimpleName)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    SparkHadoopSessionBuilder.updateFinishTests(this.getClass.getSimpleName)
    super.afterAll()
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
  it should "Engine should successfully execute with input_mapping_cmd_executor_1.json and script.sh" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_1.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "vcfDemo.vcfAnnotationTask:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testCmdExecutor1.sh").getPath,
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
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    assert(_hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/variant_effect_output1.txt")).length == 23)
    assert(_hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/variant_effect_output2.txt")).length == 23)
  }

  it should "Repartition BAM file to 155 partitions" in {

    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_2.json").getPath
    val scriptPath = getClass.getResource("/script/testCmdExecutor2.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--workflow-id", "run_cmd_test_2",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/cmd_executor_test_2/",
      "--task-root", "hdfs://localhost:9000/plugin_test/cmd_executor_test_2/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "test-2",
      "--redis-db", "0",
      "--dbg"
    )
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val files = _hadoopFS.listStatus(new Path("hdfs://localhost:9000/plugin_test/cmd_executor_test_2/outputs/output.bam"))
    assert(files.length == 155)
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
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val files = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_${udfName}/outputs/output.bam"))
    assert(files.length == partNum)
  }

  it should "Repartition BAM file to 50 partitions where paired reads will be presented each partitions" in {
    val partNum = 50
    val udfName = "GRCh38Part50"
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutor2.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"bamDemo.bamPartitionTask:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
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
      "--redis-key", "test-4",
      "--redis-db", "0",
      "--dbg"
    )
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
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
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val files = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_${udfName}/outputs/output.bam"))
    assert(files.length == partNum)
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
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
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
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
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
      try {
        val hadoopConfMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
        hadoopConfMap.foreach { case (k, v) => hadoopConf.set(k, v) }
        new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopConfMap)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        new File(filePath.toString).deleteOnExit()
      }
      val files = getHadoopFileSystem(new URI(s"$oSrc$outputCheck"), hadoopConf).listStatus(new Path(s"$oSrc$outputCheck"))
      assert(files.length == 1)
    }
  }

  it should "successfully localize Array[File] with RefLoader settings" in {
    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_array_file.json").getPath
    val scriptPath = getClass.getResource(s"/script/testCmdExecutorArrayFile.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"GermlineSnpsIndelsGatk4Hg19.PreProcessingForVariantDiscovery_GATK4.BaseRecalibrator:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
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
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val files0 = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_array_file/outputs/call-BaseRecalibrator/NA12878.hg19.recal_data0.csv/"))
    val files1 = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/cmd_executor_test_array_file/outputs/call-BaseRecalibrator/NA12878.hg19.recal_data1.csv/"))
    assert(files0.length == 1)
    assert(files1.length == 1)
  }

  it should "successfully localize Array[File] with SinleNodeDataLoader settings" in {
    val filePath = getClass.getResource("/inputMappings/input_mapping_cmd_executor_SingleNodeDataLoader.json").getPath
    val scriptPath = getClass.getResource("/script/testCmdExecutorSingleNodeDataLoader.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", s"GermlineSnpsIndelsGatk4Hg19.PreProcessingForVariantDiscovery_GATK4.BaseRecalibrator:x",
      "--file-path", filePath,
      "--script-path", scriptPath,
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
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val dst = "hdfs://localhost:9000/plugin_test/cmd_executor_test/outputs/SingleNodeDataLoader/"
    val files = _hadoopFS.listStatus(new Path(dst))
    assert(files.length == 1)

    _hadoopFS.copyToLocalFile(new Path(s"${dst}/part-00000-output"), new Path("/tmp/res/part-00000-output"))
    val f = Source.fromFile("/tmp/res/part-00000-output")
    val fileContents = f.getLines.mkString
    f.close()
    assert(fileContents == "part-0.vcfpart-1.vcf" )
  }
}
