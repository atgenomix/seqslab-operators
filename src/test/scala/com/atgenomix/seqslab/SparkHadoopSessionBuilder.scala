/*
 * Copyright (C) 2022, Atgenomix Incorporated.
 *
 * All Rights Reserved.
 *
 * This program is an unpublished copyrighted work which is proprietary to
 * Atgenomix Incorporated and contains confidential information that is not to
 * be reproduced or disclosed to any other person or entity without prior
 * written consent from Atgenomix, Inc. in each and every instance.
 *
 * Unauthorized reproduction of this program as well as unauthorized
 * preparation of derivative works based upon the program or distribution of
 * copies by sale, rental, lease or lending are violations of federal copyright
 * laws and state trade secret laws, punishable by civil and criminal penalties.
 */

package com.atgenomix.seqslab

import com.atgenomix.seqslab.piper.common.utils.DeltaTableUtil
import com.atgenomix.seqslab.HDFSCluster.startHDFS
import com.atgenomix.seqslab.SparkHadoopSessionBuilder.{startRedis, stopRedis}
import com.github.fppt.jedismock.RedisServer
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.stringtemplate.v4.ST
import redis.clients.jedis.Jedis

import java.io.File
import java.nio.file.{Files, Path}
import java.sql.Timestamp
import scala.util.matching.Regex


object SparkHadoopSessionBuilder {

  var hadoopFS: FileSystem = null
  val localHivePath: Path = Files.createTempDirectory("hiveDataWarehouse")
  var allTests: Seq[String] = Seq.empty
  var finishTests: Seq[String] = Seq.empty
  var url: String = ""

  def updateAllTests(testName: String): Unit = {this.synchronized{allTests = allTests :+ testName}}

  def updateFinishTests(testName: String): Unit = {this.synchronized{finishTests = finishTests :+ testName}}

  def lastTest: Boolean = { allTests.forall(finishTests.contains) }

  def _hadoopFS: FileSystem = {
    this.synchronized {
      if (hadoopFS == null) {
        val conf = new Configuration()
        conf.set("fs.defaultFS", url)
        hadoopFS = FileSystem.get(conf)
        val srcFiles = new org.apache.hadoop.fs.Path("file://" + getClass.getResource("/inputs").getPath)
        val dstFiles = new org.apache.hadoop.fs.Path("hdfs://localhost:9000/inputs")
        if (!hadoopFS.exists(dstFiles)) {
          hadoopFS.copyFromLocalFile(srcFiles, dstFiles)
        }
      }
      hadoopFS
    }
  }

  var spark: SparkSession = _

  def _spark: SparkSession = {
    this.synchronized {
      if (spark == null) {
        _hadoopFS
        spark = SparkSession.builder()
          .enableHiveSupport()
          .master("local[*]")
          .appName(this.getClass.getSimpleName)
          .config("spark.network.timeout", "1800s") // Default timeout for all network interactions.
          .config("spark.hadoop.fs.defaultFS", SparkHadoopSessionBuilder.url)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
          .config("spark.piper.plugins", "com.atgenomix.seqslab.AtgenomixPiperPlugin")
          .config("spark.seqslab.kernel.hive.udf.register.enabled", "true")
          .getOrCreate()
      }
      spark
    }
  }

  var server: RedisServer = _
  var jedis: Jedis = _

  def startRedis(): Unit = {
    this.synchronized {
      if (server == null) {
        server = RedisServer.newRedisServer()
        server.start()
        jedis = new Jedis(server.getHost, server.getBindPort)
      }
    }
  }

  def stopRedis(): Unit = {
    this.synchronized {
      if (server != null) {
        server.stop()
        server = null
        jedis = null
      }
    }
  }
}

trait SparkHadoopSessionBuilder extends BeforeAndAfterAll with BeforeAndAfterEach with HDFSCluster {
  this: Suite =>

  def _hadoopFS: FileSystem = SparkHadoopSessionBuilder._hadoopFS

  var _spark: SparkSession = _

  override def beforeAll(): Unit = {
    startRedis()
    startHDFS(hdfsPort)
    SparkHadoopSessionBuilder.url = getNameNodeURI
  }

  override def afterAll(): Unit = {
    if (SparkHadoopSessionBuilder.lastTest) {
      SparkHadoopSessionBuilder.spark.stop()
      SparkHadoopSessionBuilder.spark = null
      shutdownHDFS()
      SparkHadoopSessionBuilder.hadoopFS = null
      stopRedis()
      deleteLocalFiles()
      deleteHiveMetastore()
    }
  }

  override def beforeEach(): Unit = {
    _spark = SparkHadoopSessionBuilder._spark
  }

  def deleteLocalFiles(): Unit = {
    val workspace = new File(System.getProperty("user.dir"))
    if (workspace.isDirectory) {
      val filePattern: Regex = """local-([0-9]+)-([0-9a-zA-Z-._]+)""".r
      val logPattern: Regex = """([0-9a-zA-Z-.]+).log""".r
      // Delete all local working directory
      workspace.listFiles.filter(f => {
        f.getName match {
          case filePattern(hashId, _) => hashId.length == 13
          case s if s.startsWith("shared-data") => true
          case _ => false
        }
      }).foreach(f => {
        if (f.isDirectory) FileUtils.forceDeleteOnExit(f) else f.deleteOnExit()
        println(s"Delete $f")
      })
      // Delete all log files
      workspace.listFiles
        .flatMap(f => if (f.isDirectory) f.listFiles.filter(!_.isDirectory) else Seq(f))
        .foreach(f => f.getName match {
          case logPattern(_) => f.deleteOnExit()
          case _ =>
        })
    }
    // Delete local backend outputs files
    FileUtils.forceDeleteOnExit(new File("/tmp/we2"))
  }

  def deleteHiveMetastore(): Unit = {
    val workspace = System.getProperty("user.dir")
    val metastore = new File(s"$workspace/metastore_db")
    if (metastore.exists())
      FileUtils.forceDelete(metastore)
  }

  def getInputMappingPath(resourcePath: String, properties: Map[String, String] = Map.empty): java.nio.file.Path = {
    val inputStream = getClass.getResourceAsStream(resourcePath)
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    val fileTemplate = new ST(content)
    properties.foreach { case (key, value) => fileTemplate.add(key, value)}
    val path = Files.createTempFile("", "")
    Files.write(path, fileTemplate.render().getBytes)
    path
  }

  def getTimeStampFromDeltaTable(table: String, version: Option[String] = None): Map[String, String] = {
    val history = DeltaTable.forPath(_spark, table).history()
    val df = version match {
      case Some(version) => history.filter(s"version == '$version'")
      case None => history
    }
    val timestamp = df.first().getAs[Timestamp]("timestamp").toString
    Map("updatedTime" -> DeltaTableUtil.history2DrsZSuffixTimeFormat(timestamp))
  }
}
