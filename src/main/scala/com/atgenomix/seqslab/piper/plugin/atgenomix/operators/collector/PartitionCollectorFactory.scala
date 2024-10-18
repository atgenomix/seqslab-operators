package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector

import com.atgenomix.seqslab.piper.common.SeqslabOperatorContext
import com.atgenomix.seqslab.piper.common.utils.HDFSUtil
import com.atgenomix.seqslab.piper.common.utils.MiscUtil.{getLastName, getOutputSourceInfo}
import com.atgenomix.seqslab.piper.plugin.api._
import com.atgenomix.seqslab.piper.plugin.api.collector.{Collector, CollectorSupport, SupportsAggregation}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector.PartitionCollectorFactory.PartitionCollector
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row}

import java.io.FileInputStream
import java.net.URI
import java.nio.file.{DirectoryStream, Path}
import java.util
import scala.collection.JavaConverters.{asJavaIteratorConverter, asScalaIteratorConverter}

object PartitionCollectorFactory {
  private class PartitionCollector(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Collector with SupportsAggregation {

    val fqn: String = operatorCtx.asInstanceOf[SeqslabOperatorContext].fqn
    val piperValue: PiperValue = operatorCtx.asInstanceOf[SeqslabOperatorContext].outputs.get(fqn)
    val dataSource: DataSource = getOutputSourceInfo(getLastName(fqn), piperValue).head._2
    val upload: Boolean = Option.apply(operatorCtx.get("PartitionCollector:upload")).exists(_.asInstanceOf[Boolean])

    private var isDir: Boolean = _
    private var fis: FileInputStream = _
    private var dis: DirectoryStream[Path] = _

    override def init(b: Boolean): Collector = {
      isDir = b
      this
    }

    override def setInputStream(fileInputStream: FileInputStream): Unit = {
      fis = fileInputStream
    }

    override def setDirectoryStream(directoryStream: DirectoryStream[Path]): Unit = {
      dis = directoryStream
    }

    override def schema(): StructType = {
      if (upload) {
        val field = DataTypes.createStructField("url", DataTypes.StringType, true)
        DataTypes.createStructType(util.Arrays.asList(field))
      } else if (isDir) {
        val name = DataTypes.createStructField("name", DataTypes.StringType, true)
        val result = DataTypes.createStructField("result", DataTypes.BinaryType, true)
        DataTypes.createStructType(util.Arrays.asList(name, result))
      } else {
        val field = DataTypes.createStructField("result", DataTypes.BinaryType, true)
        DataTypes.createStructType(util.Arrays.asList(field))
      }
    }

    override def call(): util.Iterator[Row] = {
      if (upload) {
        implicit val confMap: Map[String, String] = operatorCtx.get("~hadoopConf").asInstanceOf[Map[String, String]]
        val url = dataSource.getUrl
        if (isDir) {
          uploadDir(url)
        } else {
          uploadFile(url)
        }
      } else {
        if (isDir) {
          collectDir()
        } else {
          collectFile()
        }
      }
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()

    private def collectFile(): util.Iterator[Row] = {
      val list = new util.ArrayList[Row]()
      val bytes = Iterator.continually(fis.read).takeWhile(_ != -1).map(_.toByte).toArray
      list.add(new GenericRowWithSchema(Array(bytes), schema()))
      list.iterator()
    }

    private def collectDir(): util.Iterator[Row] = {
      dis.iterator()
        .asScala
        .map { path =>
          val is = new FileInputStream(path.toFile)
          val fileName = path.getFileName.toString
          val bytes = Iterator.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
          new GenericRowWithSchema(Array(fileName, bytes), schema()).asInstanceOf[Row]
        }
        .asJava
    }

    private def uploadFile(url: String)(implicit confMap: Map[String, String]): util.Iterator[Row] = {
      val dst = new URI(url)
      val fs = HDFSUtil.getHadoopFileSystem(dst, confMap)
      val os = fs.create(new org.apache.hadoop.fs.Path(url))

      try {
        val bytes = Iterator.continually(fis.read).takeWhile(_ != -1).map(_.toByte).toArray
        os.write(bytes)

        val list = new util.ArrayList[Row]()
        val row = new GenericRowWithSchema(Array(url), schema()).asInstanceOf[Row]
        list.add(row)
        list.iterator()
      } finally {
        os.close()
      }
    }

    private def uploadDir(url: String)(implicit confMap: Map[String, String]): util.Iterator[Row] = {
      dis.iterator()
        .asScala
        .foreach { path =>
          val dst = new URI(url + "/").resolve(path.getFileName.toString)
          HDFSUtil.uploader(path, dst)
        }

      val list = new util.ArrayList[Row]()
      val row = new GenericRowWithSchema(Array(url), schema()).asInstanceOf[Row]
      list.add(row)
      list.iterator()
    }

    override def agg(dataset: Dataset[Row]): Dataset[Row] = {
      if (upload) {
        dataset.sparkSession.emptyDataFrame
      } else {
        dataset.repartition(1)
      }
    }
  }
}

// Collect binary file into DataFrame
class PartitionCollectorFactory extends OperatorPipelineV3 with CollectorSupport with Serializable {
  override def createCollector(pluginContext: PluginContext, operatorContext: OperatorContext): Collector = {
    new PartitionCollector(pluginContext, operatorContext)
  }
}
