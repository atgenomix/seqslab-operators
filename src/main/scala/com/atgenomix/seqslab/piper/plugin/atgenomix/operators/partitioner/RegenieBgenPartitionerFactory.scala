package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.common.Const
import com.atgenomix.seqslab.piper.plugin.api.transformer.{SupportsPartitioner, Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.RegenieBgenPartitionerFactory.RegenieBgenPartitioner
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, udf}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.mutable

object RegenieBgenPartitionerFactory {
  class RegenieBgenPartitioner(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer with SupportsPartitioner {

    private val opName: String = this.getName
    private val properties: mutable.Map[String, AnyRef] = operatorCtx.getProperties.asScala
    private val bSize: Int = properties.get(s"$opName:bsize") match {
      case Some(n) if n.isInstanceOf[Number] => n.asInstanceOf[Double].toInt
      case Some(s) => s.asInstanceOf[String].toInt
    }
    private val include: String = properties.get(s"$opName:include").map(_.asInstanceOf[String]).getOrElse("")
    private val exclude: String = properties.get(s"$opName:exclude").map(_.asInstanceOf[String]).getOrElse("")
    private val numJobs: Int = properties.get(s"$opName:numJobs") match {
      case Some(n) if n.isInstanceOf[Number] => n.asInstanceOf[Double].toInt
      case Some(s) => s.asInstanceOf[String].toInt
      case None => Int.MaxValue
    }
    private var _numPartitions: Int = 0
    private var _schema: StructType = new StructType()
    private var _df: Option[DataFrame] = None

    override def init(i: Int, i1: Int): Transformer = this

    override def expr(dataset: Dataset[Row]): Column = col("partId")

    override def partitionId(objects: AnyRef*): Integer = objects.head.asInstanceOf[Int]

    override def numPartitions(): Int = _numPartitions

    override def call(t1: DataFrame): DataFrame = {
      val df = if (include != "") {
        val in = pluginCtx.piper.spark.read.text(include).collect().map(r => r.getAs[String](0)).toSet
        t1.filter(row => row.getList[String](row.fieldIndex("names")).asScala.forall(in.contains))
      } else if (exclude != "") {
        val ex = pluginCtx.piper.spark.read.text(exclude).collect().map(r => r.getAs[String](0)).toSet
        t1.filter(row => row.getList[String](row.fieldIndex("names")).asScala.forall(!ex.contains(_)))
      } else {
        t1
      }
      _schema = StructType(t1.schema.fields :+ StructField("rowNumber", IntegerType, nullable=false))
      var accumulatedCount = 0
      val count0 = df.select(col("contigName") :: Const.preservedColumns: _*).groupBy("contigName").count().collect()
        .sortWith((r1, r2) => r1.getAs[Int]("contigName") < r2.getAs[Int]("contigName"))
        .map(r => {
          val count = r.getAs[Long]("count").toInt
          val ret = r.getAs[Int]("contigName") -> (accumulatedCount, _numPartitions)
          _numPartitions = _numPartitions + Math.ceil(count.toDouble / bSize).toInt
          accumulatedCount += count
          ret
        })
      count0.foreach(println(_))
      val count = count0.toMap
      _numPartitions = Math.min(_numPartitions, numJobs)
      val window = Window.partitionBy("contigName").orderBy("start")
      val partitionFunc = udf((contig: Int, rowNumber: Int) => count(contig)._2 + ((rowNumber - 1) / bSize))

      _df = Some(
        df.withColumn("rowNumber", row_number().over(window))
          .withColumn("partId", partitionFunc(col("contigName"), col("rowNumber")))
          .repartition(_numPartitions, col("partId").mod(_numPartitions))
      )
      val builder = new mutable.StringBuilder(s"$accumulatedCount $bSize\n")
      operatorCtx.setProperties(Map("master" -> builder.mkString.asInstanceOf[Object]).asJava)
      _df.get
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class RegenieBgenPartitionerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new RegenieBgenPartitioner(pluginContext, operatorContext)
  }
}
