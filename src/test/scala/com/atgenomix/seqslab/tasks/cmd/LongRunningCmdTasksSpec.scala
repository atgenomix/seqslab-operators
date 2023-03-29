package com.atgenomix.seqslab.piper.plugin.atgenomix.tasks.cmd

import com.atgenomix.seqslab.piper.engine.cli.{Args4j, PiedPiperArgs}
import com.atgenomix.seqslab.piper.engine.sql.PiperMain
import com.atgenomix.seqslab.piper.plugin.atgenomix.SparkHadoopSessionBuilder
import com.atgenomix.seqslab.piper.plugin.atgenomix.SparkHadoopSessionBuilder.server
import org.apache.hadoop.fs.Path
import org.scalatest.ParallelTestExecution
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters.mapAsScalaMapConverter


class LongRunningCmdTasksSpec extends AnyFlatSpec with SparkHadoopSessionBuilder with ParallelTestExecution {

  override def hdfsPort: Int = 9000

  override def beforeAll: Unit = {
    SparkHadoopSessionBuilder.updateAllTests(this.getClass.getSimpleName)
    super.beforeAll()
  }

  override def afterAll: Unit = {
    SparkHadoopSessionBuilder.updateFinishTests(this.getClass.getSimpleName)
    super.afterAll()
  }

//  it should "Repartition BAM file to 3101 partitions where paired reads will be presented each partitions" in {
//    val partNum = 3101
//    val udfName = "GRCh38Part3101"
//    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_${udfName}.json").getPath
//    val scriptPath = getClass.getResource(s"/script/testCmdExecutor2.sh").getPath
//    val args = Array(
//      "--workflow-req-src", "File",
//      "--task-fqn", s"test-${udfName}",
//      "--file-path", filePath,
//      "--script-path", scriptPath,
//      "--workflow-id", s"run_cmd_test_${udfName}",
//      "--org", "cus_OqsOKDdinNaWW7s",
//      "--tenant", "tenant",
//      "--region", "westus2",
//      "--cloud", "azure",
//      "--user", "usr_gNGAlr1m0EYMbEx",
//      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/long_running_cmd_executor_test_${udfName}/",
//      "--task-root", s"hdfs://localhost:9000/plugin_test/long_running_cmd_executor_test_${udfName}/outputs/",
//      "--redis-url", server.getHost,
//      "--redis-port", server.getBindPort.toString,
//      "--redis-key", "test-3101",
//      "--redis-db", "0",
//      "--dbg"
//    )
//    try {
//      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
//      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
//    } catch {
//      case e: Exception => e.printStackTrace()
//    }
//    val files = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/long_running_cmd_executor_test_${udfName}/outputs/output.bam"))
//    assert(files.length == partNum)
//  }

//  it should "Repartition BAM file to 3109 partitions where paired reads will be presented each partitions" in {
//    val partNum = 3109
//    val udfName = "Hg19Part3109"
//    val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_${udfName}.json").getPath
//    val scriptPath = getClass.getResource(s"/script/testCmdExecutor2.sh").getPath
//    val args = Array(
//      "--workflow-req-src", "File",
//      "--task-fqn", s"test-${udfName}",
//      "--file-path", filePath,
//      "--script-path", scriptPath,
//      "--workflow-id", s"run_cmd_test_${udfName}",
//      "--org", "cus_OqsOKDdinNaWW7s",
//      "--tenant", "tenant",
//      "--region", "westus2",
//      "--cloud", "azure",
//      "--user", "usr_gNGAlr1m0EYMbEx",
//      "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/long_running_cmd_executor_test_${udfName}/",
//      "--task-root", s"hdfs://localhost:9000/plugin_test/long_running_cmd_executor_test_${udfName}/outputs/",
//      "--redis-url", server.getHost,
//      "--redis-port", server.getBindPort.toString,
//      "--redis-key", "test-3109",
//      "--redis-db", "0",
//      "--dbg"
//    )
//    try {
//      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
//      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
//    } catch {
//      case e: Exception => e.printStackTrace()
//    }
//    val files = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/long_running_cmd_executor_test_${udfName}/outputs/output.bam"))
//    assert(files.length == partNum)
//  }
  
    it should "Repartition BAM file to 45 partitions where paired reads will be presented each partitions" in {
      val partNum = 45
      val udfName = "Hg19Chr20Part45"
      val filePath = getClass.getResource(s"/inputMappings/input_mapping_cmd_executor_$udfName.json").getPath
      val scriptPath = getClass.getResource("/script/testCmdExecutor2.sh").getPath
      val args = Array(
        "--workflow-req-src", "File",
        "--task-fqn", s"test-$udfName",
        "--file-path", filePath,
        "--script-path", scriptPath,
        "--workflow-id", s"run_cmd_test_$udfName",
        "--org", "cus_OqsOKDdinNaWW7s",
        "--tenant", "tenant",
        "--region", "westus2",
        "--cloud", "azure",
        "--user", "usr_gNGAlr1m0EYMbEx",
        "--fs-root", s"hdfs://localhost:9000/wes/plugin_test/long_running_cmd_executor_test_$udfName/",
        "--task-root", s"hdfs://localhost:9000/plugin_test/long_running_cmd_executor_test_$udfName/outputs/",
        "--redis-url", server.getHost,
        "--redis-port", server.getBindPort.toString,
        "--redis-key", "test-45",
        "--redis-db", "0",
        "--dbg"
      )
      try {
        val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
        new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
      } catch {
        case e: Exception => e.printStackTrace()
      }
      val files = _hadoopFS.listStatus(new Path(s"hdfs://localhost:9000/plugin_test/long_running_cmd_executor_test_$udfName/outputs/output.bam"))
      assert(files.length == partNum)
    }
}
