package com.atgenomix.seqslab.operators.transformer

import com.atgenomix.seqslab.piper.common.SeqslabOperatorContext
import com.atgenomix.seqslab.piper.common.utils.{HDFSUtil, RetryUtil}
import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.operators.transformer.Icd10TransformerFactory.Icd10Transformer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.net.URI
import java.util.UUID


object Icd10TransformerFactory {
  private class Icd10Transformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    private var embeddingsPath: String = _
    private var numPart: Int = 0
    private val tmpURI = operatorCtx.asInstanceOf[SeqslabOperatorContext].tmpURI.get

    override def init(i: Int, i1: Int): Transformer = {
      embeddingsPath = operatorCtx.get("Icd10Transformer:embeddingsPath").asInstanceOf[String]
      this
    }

    override def numPartitions(): Int = numPart

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      this.numPart = t1.rdd.getNumPartitions

      val diagnosis = t1.select("diagnosis").head().getString(0)
      if (diagnosis.isEmpty) {
        t1.withColumn("icd_10", lit("N/A"))
      } else {
        genIcd10(t1, diagnosis)
      }
    }

    private def genIcd10(input: DataFrame, diagnosis: String): DataFrame = {
      val vec = RetryUtil.retry(calculateEmbeddings(diagnosis), 20)
      val dotProductUDF: UserDefinedFunction = udf { i: Array[Double] =>
        val r = i
          .zip(vec)
          .map { case (x: Double, y: Double) =>
            x * y
          }
          .sum
        r
      }
      val url = new URI(embeddingsPath).toURL
      val df = if (url.getProtocol.equals("file") || url.getProtocol.equals("https")) {
        val name = embeddingsPath.split("/").last
        val uuid = UUID.randomUUID().toString
        val dst = tmpURI.resolve(s"$uuid-$name")
        HDFSUtil.save(url, dst, input.sparkSession.sparkContext.hadoopConfiguration)
        input.sparkSession.read.parquet(dst.toString)
      } else input.sparkSession.read.parquet(embeddingsPath)
      val result = df
        .withColumn("distance", dotProductUDF(col("s_embeddings")))
        .select("code", "description", "distance")
        .sort(col("distance").desc)
        .limit(1)
        .rdd
        .flatMap(row => Seq((row.getString(0), row.getString(1))))
        .collect()
        .map(i => s"${i._1} ${i._2}")

      input.withColumn("icd_10", lit(result.headOption.getOrElse("N/A")))
    }

    private def calculateEmbeddings(diagnosis: String): Array[Double] = {
      val dev= operatorCtx.get("Icd10Transformer:device").asInstanceOf[String]
      val modelName = operatorCtx.get("Icd10Transformer:modelName").asInstanceOf[String]

      import sys.process._
      val vecStr = Seq("python3"
        , "-c"
        , s"""
           |from sentence_transformers import SentenceTransformer
           |trans = SentenceTransformer('$modelName', device='$dev')
           |vec = trans.encode("$diagnosis", normalize_embeddings=True).tolist()
           |vec_str = ','.join(map(lambda i: str(i), vec))
           |print(vec_str)""".stripMargin
      ).!!

      vecStr.split(",").map(_.toDouble)
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class Icd10TransformerFactory extends OperatorPipelineV3 with TransformerSupport with Serializable {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new Icd10Transformer(pluginContext, operatorContext)
  }
}