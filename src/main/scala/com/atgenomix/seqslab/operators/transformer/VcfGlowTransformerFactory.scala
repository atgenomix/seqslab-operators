package com.atgenomix.seqslab.operators.transformer

import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.operators.transformer.VcfGlowTransformerFactory.VcfGlowTransformer
import com.atgenomix.seqslab.operators.transformer.glow.VCFLineToInternalRowConverter
import htsjdk.samtools.ValidationStringency
import htsjdk.tribble.readers.{AsciiLineReader, AsciiLineReaderIterator}
import htsjdk.variant.vcf.{VCFCodec, VCFFormatHeaderLine, VCFHeader, VCFInfoHeaderLine}
import io.projectglow.vcf.{VCFHeaderUtils, VCFSchemaInferrer}
import org.apache.hadoop.io.Text
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer


object VcfGlowTransformerFactory {
  class VcfGlowTransformer(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {

    private var _df: Option[DataFrame] = None
    private var _schema: StructType = new StructType()
    private var _converter: Option[VCFLineToInternalRowConverter] = None

    override def getOperatorContext: OperatorContext = operatorCtx

    override def init(var1: Int, var2: Int): Transformer = {
      // InferSchema
      val includeSampleIds = true
      val (infoHeaders, formatHeaders, vcfHeader) = readHeaders(pluginCtx.piper.spark)
      _schema = VCFSchemaInferrer.inferSchema(includeSampleIds, true, infoHeaders, formatHeaders)
      _converter = Some(new VCFLineToInternalRowConverter(vcfHeader, _schema, ValidationStringency.valueOf("SILENT"), None))
      this
    }

    override def close(): Unit = ()

    override def numPartitions(): Int = _df.get.rdd.getNumPartitions

    override def call(t1: DataFrame): DataFrame = {
      // Convert Line into InternalRow
      val converter = _converter
      val schema = _schema
      def internalRow2Row(value: Any): Any = {
        value match {
          case s: UTF8String => s.toString
          case a: GenericArrayData => a.array.map(internalRow2Row)
          case r: GenericInternalRow => Row.fromSeq(r.values.map(internalRow2Row).toSeq)
          case any => any
        }
      }
      val partition = operatorCtx.get("partition")
      val numRepartition = if (partition == null) 200 else partition.asInstanceOf[Int]
      _df = Some(t1.map(row => {
        val text = new Text(row.getAs[String]("row"))
        val internalRow = converter.get.convert(text)
        val input = internalRow.toSeq(schema)
        val output = input.map(internalRow2Row)
        Row.fromSeq(output)
      })(RowEncoder.apply(schema)).repartition(numRepartition))
      _df.get
    }

    def readHeaders(sparkSession: SparkSession): (Seq[VCFInfoHeaderLine], Seq[VCFFormatHeaderLine], VCFHeader) = {
      val infoHeaderLines = ArrayBuffer[VCFInfoHeaderLine]()
      val formatHeaderLines = ArrayBuffer[VCFFormatHeaderLine]()
      val header = operatorCtx.get("vcfHeader").asInstanceOf[ArrayBuffer[String]].foldLeft("")((o, line) => o + line + "\n")
      val reader = new AsciiLineReaderIterator(AsciiLineReader.from(new java.io.ByteArrayInputStream(header.getBytes)))
      val vcfHeader = new VCFCodec().readActualHeader(reader).asInstanceOf[VCFHeader]
      val getNonSchemaHeaderLines = false
      VCFHeaderUtils.getUniqueHeaderLines(sparkSession.sparkContext.parallelize(Seq(vcfHeader)), getNonSchemaHeaderLines)
        .foreach {
          case i: VCFInfoHeaderLine => infoHeaderLines += i
          case f: VCFFormatHeaderLine => formatHeaderLines += f
          case _ => // Don't do anything with other header lines
        }
      (infoHeaderLines, formatHeaderLines, vcfHeader)
    }
  }
}

class VcfGlowTransformerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new VcfGlowTransformer(pluginContext, operatorContext)
  }
}
