package com.atgenomix.seqslab.tasks.sql

import com.atgenomix.seqslab.piper.engine.cli.{Args4j, PiedPiperArgs}
import com.atgenomix.seqslab.piper.engine.sql.PiperMain
import com.atgenomix.seqslab.SparkHadoopSessionBuilder
import com.atgenomix.seqslab.SparkHadoopSessionBuilder.{jedis, server}
import io.delta.tables.DeltaTable
import models._
import org.apache.spark.sql.functions.col
import org.scalatest.ParallelTestExecution
import org.scalatest.flatspec.AnyFlatSpec
import play.api.libs.json.Json

import java.sql.Timestamp
import scala.jdk.CollectionConverters.mapAsScalaMapConverter


class SqlTasksSpec extends AnyFlatSpec with SparkHadoopSessionBuilder with ParallelTestExecution {

  val cmd: String = "INSERT INTO output SELECT * FROM table1 LIMIT 10"
  val table0 = "hdfs://localhost:9000/inputs/deltaTableVcf"
  val table1_test12 = "hdfs://localhost:9000/inputs/vcfCsv/test1.vcf"
  val table1_test3 = "hdfs://localhost:9000/inputs/vcfCsv/test.vcf.gz"
  val output2 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test2"
  val output3 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test3"
  val output4 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test4"

  override def hdfsPort: Int = 9000

  override def beforeAll(): Unit = {
    SparkHadoopSessionBuilder.updateAllTests(this.getClass.getSimpleName)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    SparkHadoopSessionBuilder.updateFinishTests(this.getClass.getSimpleName)
    _spark.catalog.dropTempView("table0")
    _spark.catalog.dropTempView("table1")
    _spark.sql("SHOW TABLES").show()
    _spark.sql("SHOW VIEWS").show()
    super.afterAll()
  }

  /** Case to update existing Deltalake Table with provided hdfs Path
    *
    * task deltalakeTask1 {
    *   input {
    *     File table0
    *     File table1
    *   }
    *   command <<<
    *     INSERT INTO table0 SELECT * FROM table1 LIMIT 10;
    *   >>>
    *   output {
    *     File output = table0
    *   }
    *}
    */
  it should "Engine should successfully execute with input_mapping_sql_executor_1.json and script.sh" in {
    val timestamp = DeltaTable.forPath(_spark, table0).history().filter("version == '2'")
    val properties = Map("updatedTime" -> timestamp.first().getAs[Timestamp]("timestamp").toString)
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_1.json", properties)
    val scriptPath = getClass.getResource("/script/testSqlExecutor1.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "main_wdl.tablecreation1:x",
      "--file-path", filePath.toString,
      "--script-path", scriptPath,
      "--workflow-id", "run_test1",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test1",
      "--redis-db", "0",
      "--run-name", "sqltest_1",
      "--dbg"
    )
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val df = _spark.read.format("delta").load(table0)
    val history = DeltaTable.forPath(_spark, table0).history()
    assert(history.count() == 4L)
    assert(df.count() == 3830L + 10L)
    assert(_spark.sql("SELECT * FROM sqltest_1.main_wdl_tablecreation1_x_output").count() == 3840L)
    // Check Redis Record
    val row = DeltaTable.forPath(_spark, table0).history().orderBy(col("version").desc).collect().head
    val updatedTime = row.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test1:output_mapping:main_wdl.tablecreation1:x")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.table0").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }

  /** Case to create a new Deltalake Table with provided hdfs Path
    *
    * task deltalakeTask2 {
    *   input {
    *     File table0
    *     File table1
    *   }
    *   command <<<
    *     INSERT INTO output SELECT * FROM table1 LIMIT 10;
    *   >>>
    *   output {
    *     File output = "test2.delta"
    *   }
    *}
    */
  it should "Engine should successfully execute with input_mapping_sql_executor_2.json and script.sh" in {
    val timestamp = DeltaTable.forPath(_spark, table0).history().filter("version == '2'")
    val properties = Map("updatedTime" -> timestamp.first().getAs[Timestamp]("timestamp").toString)
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_2.json", properties)
    val scriptPath = getClass.getResource("/script/testSqlExecutor2.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "main_wdl.tablecreation2:1",
      "--file-path", filePath.toString,
      "--script-path", scriptPath,
      "--workflow-id", "run_test2",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test2",
      "--redis-db", "0",
      "--run-name", "sqltest_2",
      "--dbg"
    )
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val df = _spark.read.format("delta").load(output2)
    val history = DeltaTable.forPath(_spark, output2).history()
    assert(history.count() == 2L)
    assert(df.count() == 10L)
    assert(_spark.sql("SELECT * FROM sqltest_2.main_wdl_tablecreation2_1_output").count() == 10L)
    // Check updatedTime of output
    val row = DeltaTable.forPath(_spark, output2).history().orderBy(col("version").desc).collect().head
    val updatedTime = row.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test2:output_mapping:main_wdl.tablecreation2:1")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }

  /** Case to create new Deltalake Table with provided vcf hdfs Path
   *
   * task deltalakeTask3 {
   *   input {
   *     File table1
   *   }
   *   command <<<
   *     INSERT INTO output SELECT * FROM table1 LIMIT 10;
   *   >>>
   *   output {
   *     File output = "test3"
   *   }
   * }
   */
  it should "Engine should successfully execute with input_mapping_sql_executor_3.json and script.sh" in {
    val filePath = getClass.getResource("/inputMappings/input_mapping_sql_executor_3.json").getPath
    val scriptPath = getClass.getResource("/script/testSqlExecutor2.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "main_wdl.tablecreation3:10",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--workflow-id", "run_test3",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test3",
      "--redis-db", "0",
      "--run-name", "sqltest_3",
      "--dbg"
    )
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val df = _spark.read.format("delta").load(output3)
    val history = DeltaTable.forPath(_spark, output3).history()
    assert(history.count() == 2L)
    assert(df.count() == 10L)
    assert(_spark.sql("SELECT * FROM sqltest_3.main_wdl_tablecreation3_10_output").count() == 10L)
    // Check updatedTime of output
    val row = DeltaTable.forPath(_spark, output3).history().orderBy(col("version").desc).collect().head
    val updatedTime = row.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test3:output_mapping:main_wdl.tablecreation3:10")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }

  /** Case to create new Deltalake Table from result of SELECT
    *
    * task deltalakeTask4 {
    *   input {
    *     File table0
    *   }
    *   command <<<
    *     SELECT hardy_weinberg(genotypes) AS hardy_weinberg FROM table0 LIMIT 100;
    *   >>>
    *   output {
    *     File output = "test4"
    *   }
    * }
    */
  it should "Engine should successfully execute with input_mapping_sql_executor_4.json and script.sh" in {
    val timestamp = DeltaTable.forPath(_spark, table0).history().filter("version == '2'")
    val properties = Map("updatedTime" -> timestamp.first().getAs[Timestamp]("timestamp").toString)
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_4.json", properties)
    val scriptPath = getClass.getResource("/script/testSqlExecutor4.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "main_wdl.tablecreation4:10",
      "--file-path", filePath.toString,
      "--script-path", scriptPath,
      "--workflow-id", "run_test4",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test4",
      "--redis-db", "0",
      "--run-name", "sqltest_4",
      "--dbg"
    )
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val df = _spark.read.format("delta").load(output4)
    val history = DeltaTable.forPath(_spark, output4).history()
    assert(history.count() == 2L)
    assert(df.count() == 100L)
    assert(_spark.sql("SELECT * FROM sqltest_4.main_wdl_tablecreation4_10_output").count() == 100L)
    // Check updatedTime of output
    val updatedTime = history.head.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test4:output_mapping:main_wdl.tablecreation4:10")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }
}