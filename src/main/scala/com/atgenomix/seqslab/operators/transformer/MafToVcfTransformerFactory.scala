package com.atgenomix.seqslab.operators.transformer

import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.operators.transformer.MafToVcfTransformerFactory.MafToVcfTransformer
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.ArrayBuffer


object MafToVcfTransformerFactory {
  class MafToVcfTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    var numPart: Int = 0
    override def getOperatorContext: OperatorContext = operatorCtx

    override def init(cpuCores: Int, memPerCore: Int): Transformer = {
      createHeader()
      this
    }

    override def numPartitions(): Int = numPart

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      numPart = t1.rdd.getNumPartitions

      implicit val enc: ExpressionEncoder[Row] = RowEncoder(getStructType)
      t1.mapPartitions { iter =>
        iter.map { row =>
          val chrIdx = row.fieldIndex("Chromosome")
          val posIdx = row.fieldIndex("Start_Position")
          val refIdx = row.fieldIndex("Reference_Allele")
          val altIdx = row.fieldIndex("Tumor_Seq_Allele2")
          val chr = row.getString(chrIdx)
          val pos = row.getLong(posIdx).toString
          val ref = row.getString(refIdx)
          val alt = row.getString(altIdx)

          Row(chr, pos, ".", ref, alt)
        }
      }
    }

    private def getStructType : StructType = {
      val commonFields = Array(
        StructField("CHROM", DataTypes.StringType, nullable = false),
        StructField("POS", DataTypes.StringType, nullable = false),
        StructField("ID", DataTypes.StringType, nullable = false),
        StructField("REF", DataTypes.StringType, nullable = false),
        StructField("ALT", DataTypes.StringType, nullable = false))
      DataTypes.createStructType(commonFields)
    }

    private def createHeader(): Unit = {
      val header =
      s"""##fileformat=VCFv4.3
         |##fileDate=20090805
         |##source=myImputationProgramV3.1
         |##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
         |##reference=file:///seq/references/HG19.fasta
         |##phasing=partial
         |#CHROM\tPOS\tID\tREF\tALT""".stripMargin
      val buf = new ArrayBuffer[String]
      buf += header
      operatorCtx.setProperties(Map("vcfHeader" -> buf.asInstanceOf[Object]).asJava)
    }

    override def close(): Unit = ()
  }
}

class MafToVcfTransformerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new MafToVcfTransformer(pluginContext, operatorContext)
  }
}
