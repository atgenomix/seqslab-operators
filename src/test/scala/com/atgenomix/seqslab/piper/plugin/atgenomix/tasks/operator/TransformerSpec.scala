package com.atgenomix.seqslab.piper.plugin.atgenomix.tasks.operator

import com.atgenomix.seqslab.piper.common.SeqslabPiperContext
import com.atgenomix.seqslab.piper.engine.sql.{Args4j, PiedPiperArgs, PiperConf}
import com.atgenomix.seqslab.piper.engine.utils.OperatorUtil
import com.atgenomix.seqslab.piper.plugin.api.PluginContext
import com.atgenomix.seqslab.piper.plugin.atgenomix.SparkHadoopSessionBuilder.server
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer.NormalizeSchemaTransformerFactory
import com.atgenomix.seqslab.piper.plugin.atgenomix.{SparkHadoopSessionBuilder, TestUtil}
import models.{SeqslabPipeline, SeqslabWomFile}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec

class TransformerSpec extends AnyFlatSpec with SparkHadoopSessionBuilder {

  override def hdfsPort: Int = 9000

  it should "convert special charactor into underscore" in {
    val args = TestUtil.getCliArgs("NormalizeSchema", "Test.NormalizeSchema:x", "", "", server.getHost, server.getBindPort.toString)
    val piperConf = PiperConf(Args4j[PiedPiperArgs](args))
    val piperValue = SeqslabWomFile(None, None)
    val operator = new models.Operator("NormalizeSchemaTransformer", Map.empty, None)
    val operatorPluginMap = Map("NormalizeSchemaTransformer" -> "com.atgenomix.seqslab.piper.plugin.atgenomix.TestPlugin")
    val piperCtx = new SeqslabPiperContext(_spark, SeqslabPipeline(Map.empty, Map.empty, Map.empty, Map.empty), None)
    val pluginContextMap = Map("com.atgenomix.seqslab.piper.plugin.atgenomix.TestPlugin" ->  new PluginContext(piperCtx))
    val factory = new NormalizeSchemaTransformerFactory()
    val (pluginCtx, opCtx) = OperatorUtil.prepareCtx(operatorPluginMap, pluginContextMap, piperConf)("fqn", operator, factory, piperValue).get
    val transformer = factory.createTransformer(pluginCtx, opCtx)

    transformer.init(1, 100)
    val rdd = _spark.emptyDataFrame.rdd
    val st = StructType(Seq(StructField("sub(float", FloatType), StructField("sub)string", StringType)))
    val fields = Seq(
      StructField("test-array", st),
      StructField("test/int", IntegerType))
    val structType = StructType(fields)
    val input: DataFrame = _spark.sqlContext.createDataFrame(rdd, structType)
    val outputSchema = transformer.call(input).schema
    val sub = outputSchema.fields(0).dataType.asInstanceOf[StructType]

    assert(outputSchema.fields(0).name == "test_array")
    assert(outputSchema.fields(1).name == "test_int")
    assert(sub.fields(0).name == "sub_float")
    assert(sub.fields(1).name == "sub_string")
  }
}
