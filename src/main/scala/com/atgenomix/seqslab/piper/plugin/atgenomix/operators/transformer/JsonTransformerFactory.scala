package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer

import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer.JsonTransformerFactory.JsonTransformer
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.types.DataTypes.createArrayType
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.json4s.JsonAST
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse

object JsonTransformerFactory {
  private class JsonTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    private var numPart: Int = 0

    override def init(i: Int, i1: Int): Transformer = this

    override def numPartitions(): Int = numPart

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      this.numPart = t1.rdd.getNumPartitions

      val df = t1.withColumn("obj", explode(col("result"))).persist()
      val jsonStr = df.select("obj").head().mkString
      val schema = inferSchema(jsonStr)
      val jsonDf = df.withColumn("json", from_json(col("obj"), schema))

      jsonDf.select(col("json.*"))
    }

    private def inferSchema(jsonStr: String): StructType = {
      def getMostComplexType(elementTypes: List[DataType]): DataType = {
        val structTypes = elementTypes.collect { case i: StructType => i }
        if (structTypes.isEmpty) {
          DataTypes.StringType
        } else {
          structTypes.maxBy(_.fields.length)
        }
      }

      def getDataType(jValue: JValue): DataType = {
        jValue match {
          case JsonAST.JNull =>
            DataTypes.NullType
          case JsonAST.JString(_) =>
            DataTypes.StringType
          case JsonAST.JDouble(_) =>
            DataTypes.DoubleType
          case JsonAST.JLong(_) =>
            DataTypes.LongType
          case JsonAST.JInt(_) =>
            DataTypes.IntegerType
          case JsonAST.JBool(_) =>
            DataTypes.BooleanType
          case JsonAST.JObject(obj) =>
            val fields = obj.map { case (name, jv) =>
              val dt = getDataType(jv)
              DataTypes.createStructField(name, dt, true)
            }
            DataTypes.createStructType(fields.toArray)
          case JsonAST.JArray(arr) =>
            val elementTypes = arr.map(getDataType)
            val t = getMostComplexType(elementTypes)
            createArrayType(t)
          case JsonAST.JSet(_) => ???
          case JsonAST.JNothing => ???
          case JsonAST.JDecimal(_) => ???
        }
      }

      val jValue = parse(jsonStr)
      jValue match {
        case JsonAST.JObject(obj) =>
          val fields = obj.map { case (name, jv) =>
            val dt = getDataType(jv)
            DataTypes.createStructField(name, dt, true)
          }
          DataTypes.createStructType(fields.toArray)
      }
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

// transform json column of DataFrame to multiple columns
class JsonTransformerFactory extends OperatorPipelineV3 with TransformerSupport with Serializable {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new JsonTransformer(pluginContext, operatorContext)
  }
}
