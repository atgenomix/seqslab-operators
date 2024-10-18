package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer

import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer.NormalizeSchemaTransformerFactory.NormalizeSchemaTransformer
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.{Dataset, Row}


object NormalizeSchemaTransformerFactory {
  private class NormalizeSchemaTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    private var numPart: Int = 0

    override def init(cpuCores: Int, memPerCore: Int): Transformer = {
      this
    }

    override def numPartitions(): Int = numPart

    override def call(df: Dataset[Row]): Dataset[Row] = {
      this.numPart = df.rdd.getNumPartitions

      val newStructFields = df.schema
        .fields
        .map { field =>
          val newName = rename(field.name)
          val newDataType = recursiveRename(field.dataType)
          field.copy(name = newName, dataType = newDataType)
        }
      val newSchema = StructType(newStructFields)
      df.sqlContext.createDataFrame(df.rdd, newSchema)
    }

    private def rename(name: String): String = {
      if (name.exists(c => c == '(' || c == ')' || c == '/' || c == '-')) {
        name.replaceAll("[()/-]", "_")
      } else {
        name
      }
    }

    private def recursiveRename(dataType: DataType): DataType = {
      dataType match {
        case ArrayType(elementType, containsNull) =>
          ArrayType(recursiveRename(elementType), containsNull)
        case MapType(keyType, valueType, valueContainsNull) =>
          val newKeyType = recursiveRename(keyType)
          val newValType = recursiveRename(valueType)
          MapType(newKeyType, newValType, valueContainsNull)
        case StructType(fields) =>
          val newFields = fields.map { f =>
            val newName = rename(f.name)
            val dt = recursiveRename(f.dataType)
            f.copy(name = newName, dataType = dt)
          }
          StructType(newFields)
        case _ =>
          dataType
      }
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class NormalizeSchemaTransformerFactory extends OperatorPipelineV3 with TransformerSupport with Serializable {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new NormalizeSchemaTransformer(pluginContext, operatorContext)
  }
}
