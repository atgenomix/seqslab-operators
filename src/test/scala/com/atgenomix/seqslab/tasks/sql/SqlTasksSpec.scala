package com.atgenomix.seqslab.tasks.sql

import com.atgenomix.seqslab.piper.engine.sql.{Args4j, PiedPiperArgs, PiperMain}
import com.atgenomix.seqslab.SparkHadoopSessionBuilder
import com.atgenomix.seqslab.SparkHadoopSessionBuilder.{jedis, server}
import io.delta.tables.DeltaTable
import models._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import play.api.libs.json.Json

import java.sql.Timestamp
import java.util.Date
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.io.Source


class SqlTasksSpec extends AnyFlatSpec with SparkHadoopSessionBuilder {

  val table0 = "hdfs://localhost:9000/inputs/deltaTableVcf"
  val output2 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test2"
  val output3 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test3"
  val output4 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test4"
  val output5 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test5"
  val output6 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test6"
  val gcnvVariantsDelta = "hdfs://localhost:9000/inputs/FGS2280080_gcnvVariants.delta"
  val output7 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test7"
  val DB = "hdfs://localhost:9000/inputs/merge/table.delta"
  val output10 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test10"
  val output11 = "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/test11"
  val tableOptimize = "hdfs://localhost:9000/inputs/deltaTableOptimize"
  val tableAlter = "hdfs://localhost:9000/inputs/deltaTable"

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
   * struct Region {
   *   String rank
   * }
   *
   * struct Om {
   *   File inFile
   *   String Gene
   *   String Feature
   *   Array[String] BIOTYPE
   *   Region EXON
   *   Region INTRON
   *   String INFO
   *   String gDNA
   *   String cDNA
   *   String protein
   *   Array[String] Consequence
   *   String Context
   *   Array[String] Source
   *   Array[String] Reference
   * }
   *
   * task deltalakeTask1 {
   *   input {
   *     File table0
   *     File table1
   *     Om om
   *   }
   *   command <<<
   *     INSERT INTO table0 SELECT * FROM table1 LIMIT 10;
   *   >>>
   *   output {
   *     File output = table0
   *   }
   *}
   */
  it should "successfully execute with input_mapping_sql_executor_1.json and script.sh" in {
    val properties = getTimeStampFromDeltaTable(table0, Some("2"))
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
      "--run-name", "sqltest-1"
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
    println(s"TIME: Engine should successfully execute with input_mapping_sql_executor_1.json and script.sh $time")
    val df = _spark.read.format("delta").load(table0)
    val history = DeltaTable.forPath(_spark, table0).history()
    assert(history.count() == 4L)
    assert(df.count() == 3830L + 10L)
    val tables = _spark.sql(s"SHOW TABLES IN sqltest_1").collect().map(_.getAs[String]("tableName"))

    // Check Input/Output View
    val inputView = tables.filter(_.startsWith("main_wdl_tablecreation1_x_table0")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_1.$inputView").count() == 3830L)
    val outputView = tables.filter(_.startsWith("main_wdl_tablecreation1_x_output")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_1.$outputView").count() == 3840L)

    // Check Redis Record
    val row = DeltaTable.forPath(_spark, table0).history().orderBy(col("version").desc).collect().head
    val updatedTime = row.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test1:output_mapping:main_wdl.tablecreation1:x")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.table0").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }

  /** Case to create a new Deltalake Table with provided hdfs Path
   * struct VCF {
   *   File vcf
   *   String CHROM
   *   Int POS
   *   String ID
   *   String REF
   *   String ALT
   *   Int QUAL
   *   String FILTER
   *   String INFO
   *   String FORMAT
   *   String HG001
   * }
   *
   * task deltalakeTask2 {
   *   input {
   *     VCF table0
   *     Om om
   *     Array[File] table1
   *   }
   *   command <<<
   *     INSERT INTO output SELECT * FROM table1 LIMIT 10
   *   >>>
   *   output {
   *     File output = "test2.delta"
   *   }
   *}
   */
  it should "throw an exception as non-select command does not support table creation" in {
    val properties = getTimeStampFromDeltaTable(table0, Some("2"))
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
    var exception = false
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        exception = true
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: throw an exception as non-select command does not support table creation $time")
    assert(exception)
  }

  /** Case to create new Deltalake Table with provided vcf hdfs Path
   *
   * task deltalakeTask3 {
   *   input {
   *     File table1
   *   }
   *   command <<<
   *     SELECT * FROM table1 LIMIT 10;
   *   >>>
   *   output {
   *     File output = "test3"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_3.json and script.sh" in {
    val filePath = getClass.getResource("/inputMappings/input_mapping_sql_executor_3.json").getPath
    val scriptPath = getClass.getResource("/script/testSqlExecutor3.sh").getPath
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
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Engine should successfully execute with input_mapping_sql_executor_3.json and script.sh $time")
    val df = _spark.read.format("delta").load(output3)
    val history = DeltaTable.forPath(_spark, output3).history()
    assert(history.count() == 2L)
    assert(df.count() == 10L)
    val view = _spark.sql(s"SHOW TABLES IN sqltest_3")
      .collect().map(_.getAs[String]("tableName")).filter(_.startsWith("main_wdl_tablecreation3_10_output")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_3.$view").count() == 10L)
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
  it should "successfully execute with input_mapping_sql_executor_4.json and script.sh" in {
    val properties = getTimeStampFromDeltaTable(table0, Some("2"))
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
    val st = new Date().getTime
    try {
      val hadoopMap = _spark.sparkContext.hadoopConfiguration.getPropsWithPrefix("").asScala.toMap
      new PiperMain(Args4j[PiedPiperArgs](args)).run()(_spark, hadoopMap)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val ed = new Date().getTime
    val time = (ed - st) / 1000
    println(s"TIME: Engine should successfully execute with input_mapping_sql_executor_4.json and script.sh $time")
    val df = _spark.read.format("delta").load(output4)
    val history = DeltaTable.forPath(_spark, output4).history()
    assert(history.count() == 2L)
    assert(df.count() == 100L)
    val view = _spark.sql(s"SHOW TABLES IN sqltest_4")
      .collect().map(_.getAs[String]("tableName")).filter(_.startsWith("main_wdl_tablecreation4_10_output")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_4.$view").count() == 100L)
    // Check updatedTime of output
    val updatedTime = history.head.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test4:output_mapping:main_wdl.tablecreation4:10")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }

  /** Case to create new Deltalake Table from result of SELECT
   *
   * task deltalakeTask5 {
   *   input {
   *     File table1
   *   }
   *   command <<<
   *     SELECT * FROM table1 LIMIT 10;
   *   >>>
   *   output {
   *     File output = "test5"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_5.json and script.sh" in {
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_5.json", Map.empty)
    val scriptPath = getClass.getResource("/script/testSqlExecutor5.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "main_wdl.tablecreation5:10",
      "--file-path", filePath.toString,
      "--script-path", scriptPath,
      "--workflow-id", "run_test5",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test5",
      "--redis-db", "0",
      "--run-name", "sqltest_5",
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
    println(s"TIME: Engine should successfully execute with input_mapping_sql_executor_5.json and script.sh $time")
    val df = _spark.read.format("delta").load(output5)
    val history = DeltaTable.forPath(_spark, output5).history()
    assert(history.count() == 2L)
    assert(df.count() == 10L)
    val view = _spark.sql(s"SHOW TABLES IN sqltest_5")
      .collect().map(_.getAs[String]("tableName")).filter(_.startsWith("main_wdl_tablecreation5_10_output")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_5.$view").count() == 10L)
    // Check updatedTime of output
    val updatedTime = history.head.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test5:output_mapping:main_wdl.tablecreation5:10")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }

  /** Case to create new Deltalake Table with PartitionBy from result of SELECT
   *
   * task deltalakeTask6 {
   * input {
   *     File table1
   *   }
   *   command <<<
   *     SELECT * FROM table1 LIMIT 10;
   *   >>>
   *   output {
   *     File output = "test6"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_6.json and script.sh" in {
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_6.json", Map.empty)
    val scriptPath = getClass.getResource("/script/testSqlExecutor5.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "main_wdl.tablecreation6:11",
      "--file-path", filePath.toString,
      "--script-path", scriptPath,
      "--workflow-id", "run_test6",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test6",
      "--redis-db", "0",
      "--run-name", "sqltest_6",
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
    println(s"TIME: Engine should successfully execute with input_mapping_sql_executor_6.json and script.sh $time")
    val df = _spark.read.format("delta").load(output6)
    val history = DeltaTable.forPath(_spark, output6).history()
    assert(history.count() == 2L)
    assert(df.count() == 10L)
    val view = _spark.sql(s"SHOW TABLES IN sqltest_6")
      .collect().map(_.getAs[String]("tableName")).filter(_.startsWith("main_wdl_tablecreation6_11_output")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_6.$view").count() == 10L)
    // Check updatedTime of output
    val updatedTime = history.head.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test6:output_mapping:main_wdl.tablecreation6:11")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
    // Check partition works
    val files = _hadoopFS.listStatus(new Path(output6))
    assert(files.length == 8)
  }

  /** Case to create new Deltalake Table with PartitionBy from result of merge of Dataframe
   *
   * task deltalakeTask7 {
   *   input {
   *     Array[File] gcnvVariantsDelta
   *   }
   *   command <<<
   *     SELECT * FROM gcnvVariantsDelta
   *   >>>
   *   output {
   *     File outFileMergedDelta = "test7"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_7.json and script.sh" in {
    val properties = getTimeStampFromDeltaTable(gcnvVariantsDelta, Some("1"))
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_7.json", properties)
    val scriptPath = getClass.getResource("/script/testSqlExecutor7.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "CNVGermlineCohortWorkflow.MergeDelta:0",
      "--file-path", filePath.toString,
      "--script-path", scriptPath,
      "--workflow-id", "run_test7",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test7",
      "--redis-db", "0",
      "--run-name", "sqltest_7",
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
    println(s"TIME: Engine should successfully execute with input_mapping_sql_executor_7.json and script.sh $time")
    val df = _spark.read.format("delta").load(output7)
    val history = DeltaTable.forPath(_spark, output7).history()
    assert(history.count() == 2L)
    assert(df.count() == 189L)
    val view = _spark.sql(s"SHOW TABLES IN sqltest_7").collect().map(_.getAs[String]("tableName"))
      .filter(_.startsWith("cnvgermlinecohortworkflow_mergedelta_0_outfilemergeddelta")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_7.$view").count() == 189L)
    // Check updatedTime of output
    val updatedTime = history.head.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test7:output_mapping:CNVGermlineCohortWorkflow.MergeDelta:0")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("CNVGermlineCohortWorkflow.MergeDelta.outFileMergedDelta").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
    // Check partition works
    val files = _hadoopFS.listStatus(new Path(output7))
    assert(files.length == 2)
  }

  /** Case to create new Deltalake Table from results of Join of Table
   *
   * task deltalakeTask8 {
   *   input {
   *     File features
   *     Cell barcodes {
   *        File file
   *        String barcode__1
   *     }
   *     File matrix
   *   }
   *   command <<<
   *     SELECT matrix.*, features._c0 as gene_id, features._c1 as gene_name, features._c2 as type, barcodes.barcode from matrix \
   *     JOIN features ON matrix.gene=features.index \
   *     JOIN barcodes ON matrix.cell=barcodes.index ORDER BY cell, gene
   *   >>>
   *   output {
   *     File outFile = "test8.delta"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_8.json and script.sh" in {
    val filePath = getClass.getResource("/inputMappings/input_mapping_sql_executor_8.json").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "RNASingleCellCellRangerScanpy.Compress10XFiles:x",
      "--file-path", filePath,
      "--script-path", getClass.getResource("/script/testSqlExecutor8.sh").getPath,
      "--workflow-id", "run_test8",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test8",
      "--redis-db", "0",
      "--run-name", "sqltest_8",
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
    println(s"TIME: Engine should successfully execute with input_mapping_sql_executor_8.json and script.sh $time")
    val s = jedis.get("run_test8:output_mapping:RNASingleCellCellRangerScanpy.Compress10XFiles:x")
    val pipeline = Json.parse(s).validate[SeqslabPipeline].get
    assert(pipeline.outputs("RNASingleCellCellRangerScanpy.Compress10XFiles.outFile").getString == "test8.delta")
    val view = _spark.sql(s"SHOW TABLES IN sqltest_8").collect().map(_.getAs[String]("tableName"))
      .filter(_.startsWith("rnasinglecellcellrangerscanpy_compress10xfiles_x_outfile")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_8.$view").count() == 6788L)
  }

  /** Case to Merge new data into table with schema evolution
   *
   * task MergeTable {
   *   input {
   *     File inputData
   *     File DB
   *   }
   *   command <<<
   *     MERGE INTO DB t USING inputData s ON t.Country = s.Country
   *     WHEN MATCHED THEN UPDATE SET *
   *     WHEN NOT MATCHED THEN INSERT *
   *   >>>
   *   output {
   *     
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_9.json and script.sh" in {
    val properties = getTimeStampFromDeltaTable(DB, Some("1"))
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_9.json", properties)
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "DeltalakeTask9.MergeTable:x",
      "--file-path", filePath.toString,
      "--script-path", getClass.getResource("/script/testSqlExecutor9.sh").getPath,
      "--workflow-id", "run_test9",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test9",
      "--redis-db", "0",
      "--run-name", "sqltest_9",
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
    println(s"TIME: successfully execute with input_mapping_sql_executor_9.json and script.sh $time")
    assert(DeltaTable.forPath(DB).toDF.schema.fields.length == 3)

    // Check Input/Output View
    val tables = _spark.sql("SHOW TABLES IN sqltest_9").collect().map(_.getAs[String]("tableName"))
    val inputView = tables.filter(_.startsWith("deltalaketask9_mergetable_x_db")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_9.$inputView").count() == 2)
  }

  /** Case to create new Deltalake Table with provided vcf hdfs Path
   *
   * task deltalakeTask10 {
   *   input {
   *     File table1
   *     File table2
   *   }
   *   command <<<
   *     SELECT GenotypeDT.runName, GenotypeDT.sampleId, GenotypeDT.calls, \
   *     VariantsDT.SYMBOL, VariantsDT.SOURCE, VariantsDT.`rsID`, VariantsDT.`HGMD ID` \
   *     FROM table1 AS GenotypeDT \
   *     LEFT JOIN table2 AS VariantsDT \
   *     ON \
   *     VariantsDT.contigName = GenotypeDT.contigName \
   *     AND VariantsDT.start = GenotypeDT.start \
   *     AND VariantsDT.end = GenotypeDT.end \
   *     AND VariantsDT.referenceAllele = GenotypeDT.referenceAllele \
   *     AND VariantsDT.alternateAlleles = GenotypeDT.alternateAlleles \
   *     LIMIT 10
   *   >>>
   *   output {
   *     File output = "test10"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_10.json and script.sh" in {
    val filePath = getClass.getResource("/inputMappings/input_mapping_sql_executor_10.json").getPath
    val scriptPath = getClass.getResource("/script/testSqlExecutor10.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "main_wdl.tablecreation10:10",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--workflow-id", "run_test10",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test10",
      "--redis-db", "0",
      "--run-name", "sqltest_10",
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
    println(s"TIME: successfully execute with input_mapping_sql_executor_10.json and script.sh $time")
    val history = DeltaTable.forPath(_spark, output10).history().orderBy(col("version").desc).collect()
    assert(history.length == 2)
    val view = _spark.sql(s"SHOW TABLES IN sqltest_10").collect().map(_.getAs[String]("tableName"))
      .filter(_.startsWith("main_wdl_tablecreation10_10_output")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_10.$view").count() == 1500L) // (24 SYMBOLs + 1 SYMBOls==null) * 60 samples
    // Ensure that every SYMBOL in the variant has at least one annotation, with RefSeq as the primary priority source
    assert(_spark.sql(s"SELECT * FROM sqltest_10.$view WHERE SYMBOL == 'TRIM46' AND SOURCE == 'RefSeq'").count() == 60L) // rs761613959 MANE_SELECT
    assert(_spark.sql(s"SELECT * FROM sqltest_10.$view WHERE SYMBOL == 'MUC1' AND SOURCE == 'RefSeq'").count() == 60L) // rs761613959 not MANE_SELECT
    // Ensure that MANE_SELECT is selected
    assert(_spark.sql(s"SELECT * FROM sqltest_10.$view WHERE SYMBOL == 'SDF4' AND SOURCE == 'RefSeq'").count() == 60L) // rs11721 MANE_SELECT
    assert(_spark.sql(s"SELECT * FROM sqltest_10.$view WHERE SYMBOL == 'TNFRSF4' AND SOURCE == 'RefSeq'").count() == 60L) // rs11721 MANE_SELECT
    // Ensure that every SYMBOL in the variant has at least one annotation, regardless of whether the source is RefSeq or not
    assert(_spark.sql(s"SELECT * FROM sqltest_10.$view WHERE SYMBOL LIKE 'MT-%' AND SOURCE == 'Ensembl'").count() == 1140L) // chrM:m.2835C>T is annotated by 19 SYMBOLs from Ensembl
    // Ensure that rsID column are created
    assert(_spark.sql(s"SELECT rsID FROM sqltest_10.$view WHERE array_contains(rsID,'rs761613959')").count() == 120L )
    // Check updatedTime of output
    val updatedTime = history.head.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test10:output_mapping:main_wdl.tablecreation10:10")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }

  /** Case to create new Deltalake Table with provided vcf hdfs Path
   *
   * task deltalakeTask11 {
   *   input {
   *     File table1
   *   }
   *   command <<<
   *     SELECT * FROM table1 LIMIT 10
   *   >>>
   *   output {
   *     File output = "test11"
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_11.json and script.sh" in {
    val filePath = getClass.getResource("/inputMappings/input_mapping_sql_executor_11.json").getPath
    val scriptPath = getClass.getResource("/script/testSqlExecutor11.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "main_wdl.tablecreation11:11",
      "--file-path", filePath,
      "--script-path", scriptPath,
      "--workflow-id", "run_test11",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test11",
      "--redis-db", "0",
      "--run-name", "sqltest_11",
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
    println(s"TIME: successfully execute with input_mapping_sql_executor_11.json and script.sh $time")
    val history = DeltaTable.forPath(_spark, output11).history().orderBy(col("version").desc).collect()
    assert(history.length == 2L)
    val view = _spark.sql(s"SHOW TABLES IN sqltest_11").collect().map(_.getAs[String]("tableName"))
      .filter(_.startsWith("main_wdl_tablecreation11_11_output")).head
    assert(_spark.sql(s"SELECT * FROM sqltest_11.$view").count() == 4L)
    // Check updatedTime of output
    val updatedTime = history.head.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test11:output_mapping:main_wdl.tablecreation11:11")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }

  /** Case to optimize a Deltalake Table Z-Ordering
   *
   * task deltalakeTask12 {
   *   input {
   *     File table1
   *   }
   *   command <<<
   *     OPTIMIZE table1 ZORDER BY (contigName, start, end)
   *   >>>
   *   output {
   *     File output = table1
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_12.json and script.sh" in {
    val properties = getTimeStampFromDeltaTable(tableOptimize)
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_12.json", properties)
    val scriptPath = getClass.getResource("/script/testSqlExecutor12.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "deltalake12",
      "--file-path", filePath.toString,
      "--script-path", scriptPath,
      "--workflow-id", "run_test12",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test12",
      "--redis-db", "0",
      "--run-name", "sqltest_12",
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
    println(s"TIME: successfully execute with input_mapping_sql_executor_12.json and script.sh $time")
    val history = DeltaTable.forPath(_spark, tableOptimize).history().orderBy(col("version").desc).collect()
    assert(history.length == 4L)
    val view = _spark.sql(s"SHOW TABLES IN sqltest_12").collect()
      .map(_.getAs[String]("tableName"))
      .find(_.startsWith("deltalake12_output"))
    assert(view.isDefined)
    // Check updatedTime of output
    val updatedTime = history.head.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test12:output_mapping:deltalake12")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake12.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
  }

  /** Case to Alter a Deltalake Table
   *
   * task deltalakeTask13 {
   *   input {
   *     File table
   *   }
   *   command <<<
   *     ALTER TABLE table1 ADD COLUMNS (AddName string)
   *   >>>
   *   output {
   *     File output = table
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_13.json and script.sh" in {
    val properties = getTimeStampFromDeltaTable(tableAlter)
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_13.json", properties)
    val scriptPath = getClass.getResource("/script/testSqlExecutor13.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "deltalake13",
      "--file-path", filePath.toString,
      "--script-path", scriptPath,
      "--workflow-id", "run_test13",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test13",
      "--redis-db", "0",
      "--run-name", "sqltest_13",
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
    println(s"TIME: successfully execute with input_mapping_sql_executor_13.json and script.sh $time")
    val history = DeltaTable.forPath(_spark, tableAlter).history().orderBy(col("version").desc).collect()
    assert(history.length == 3L)
    val view = _spark.sql(s"SHOW TABLES IN sqltest_13").collect()
      .map(_.getAs[String]("tableName"))
      .find(_.startsWith("deltalake13_output"))
    assert(view.isDefined)
    // Check updatedTime of output
    val updatedTime = history.head.getAs[Timestamp]("timestamp").toString.replace(" ", "T").split('.').head
    val pipeline = Json.parse(jedis.get("run_test13:output_mapping:deltalake13")).validate[SeqslabPipeline].get
    assert(pipeline.datasets("deltalake13.output").asInstanceOf[SingleDataset].single.updatedTime.get.startsWith(updatedTime))
    // Check column number increased by 1
    assert(_spark.read.format("delta").load(tableAlter).schema.fields.length == 2)
  }

  /** Case to output vcf file with Glow
   *
   * task deltalake14 {
   *   input {
   *     File vcfInput
   *   }
   *   command <<<
   *     SELECT * FROM vcfInput LIMIT 100
   *   >>>
   *   output {
   *     File output
   *   }
   * }
   */
  it should "successfully execute with input_mapping_sql_executor_14.json and script.sh" in {
    val filePath = getInputMappingPath("/inputMappings/input_mapping_sql_executor_14.json")
    val scriptPath = getClass.getResource("/script/testSqlExecutor14.sh").getPath
    val args = Array(
      "--workflow-req-src", "File",
      "--task-fqn", "deltalake14",
      "--file-path", filePath.toString,
      "--script-path", scriptPath,
      "--workflow-id", "run_test14",
      "--org", "cus_OqsOKDdinNaWW7s",
      "--tenant", "tenant",
      "--region", "westus2",
      "--cloud", "azure",
      "--user", "usr_gNGAlr1m0EYMbEx",
      "--fs-root", "hdfs://localhost:9000/wes/plugin_test/sql_executor_test/",
      "--task-root", "hdfs://localhost:9000/plugin_test/sql_executor_test/outputs/",
      "--redis-url", server.getHost,
      "--redis-port", server.getBindPort.toString,
      "--redis-key", "run_test14",
      "--redis-db", "0",
      "--run-name", "sqltest_14",
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
    println(s"TIME: successfully execute with input_mapping_sql_executor_14.json and script.sh $time")
    // Get headers of output VCF
    val pipeline = Json.parse(jedis.get("run_test14:output_mapping:deltalake14")).validate[SeqslabPipeline].get
    val outputDrs = pipeline.datasets("deltalake14.output").asInstanceOf[SingleDataset].single
    val vcfPath = new Path(outputDrs.accessMethods.get.head.accessUrl.url)
    assert(_hadoopFS.listStatus(vcfPath).length == 1)
    val localPath = "/tmp/res/output.vcf"
    _hadoopFS.copyToLocalFile(vcfPath, new Path(localPath))
    val vcf = Source.fromFile(localPath)
    val outputHeaders = vcf.getLines().filter(_.startsWith("#")).toArray
    // Get headers of input VCF
    val inputPath = "hdfs://localhost:9000/inputs/vcfPartition/P464748.vep.vcf.gz"
    val inputHeaders = _spark.read.textFile(inputPath).filter(s => s.startsWith("#")).collect().toSet
    outputHeaders.foreach(o => assert(inputHeaders.contains(o)))
  }
}