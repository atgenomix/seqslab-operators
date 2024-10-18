package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer

import com.atgenomix.seqslab.piper.common.SeqslabOperatorContext
import com.atgenomix.seqslab.piper.common.utils.{HDFSUtil, RetryUtil}
import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer.IcdOncologyTransformerFactory.IcdOncologyTransformer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.net.URI
import java.util.UUID


object IcdOncologyTransformerFactory {
  private class IcdOncologyTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    private var embeddingsPath: String = _
    private var numPart: Int = 0
    private val tmpURI = operatorCtx.asInstanceOf[SeqslabOperatorContext].tmpURI.get

    override def init(i: Int, i1: Int): Transformer = {
      embeddingsPath = operatorCtx.get("IcdOncologyTransformer:embeddingsPath").asInstanceOf[String]
      this
    }

    override def numPartitions(): Int = numPart

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      this.numPart = t1.rdd.getNumPartitions

      val sampledTissue = t1.select("sampled_tissue").head().getString(0)
      if (sampledTissue.isEmpty) {
        t1.withColumn("icd_O", lit("N/A"))
      } else {
        genIcdOnco(t1, sampledTissue)
      }
    }

    private def genIcdOnco(input: DataFrame, diagnosis: String): DataFrame = {
      val vec = RetryUtil.retryWithCallback(calculateEmbeddings(diagnosis), Thread.sleep(10000), 20)
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

      input.withColumn("icd_O", lit(result.headOption.getOrElse("N/A")))
    }

    private def calculateEmbeddings(diagnosis: String): Array[Double] = {
      val dev = operatorCtx.get("IcdOncologyTransformer:device").asInstanceOf[String]
      val modelName = operatorCtx.get("IcdOncologyTransformer:modelName").asInstanceOf[String]

      import sys.process._
      val vecStr = Seq("python3"
        , "-c"
        , s"""
           |from sentence_transformers import SentenceTransformer
           |trans = SentenceTransformer('$modelName', device='$dev')
           |vec = trans.encode('$diagnosis', normalize_embeddings=True).tolist()
           |vec_str = ','.join(map(lambda i: str(i), vec))
           |print(vec_str)""".stripMargin
      ).!!

      vecStr.split(",").map(_.toDouble)
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class IcdOncologyTransformerFactory extends OperatorPipelineV3 with TransformerSupport with Serializable {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new IcdOncologyTransformer(pluginContext, operatorContext)
  }
}