package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader

import com.atgenomix.seqslab.piper.plugin.api.loader.{Loader, LoaderSupport, SupportsScanPartitions}
import com.atgenomix.seqslab.piper.plugin.api.{DataSource, OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader.MafDataSource.MafLoader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import java.util
import scala.collection.JavaConverters.{asJavaIteratorConverter, asScalaIteratorConverter, collectionAsScalaIterableConverter}
import scala.util.{Failure, Success, Try}


object MafDataSource {

  class MafLoader(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Loader with SupportsScanPartitions {

    private var srcInfo: DataSource = _
    private var partition: Stream[Row] = _

    override def getOperatorContext: OperatorContext = operatorCtx

    override def init(src: DataSource): Loader = {
      srcInfo = src
      this
    }

    override def readSchema(): StructType = {
      val commonFields = Array(
        StructField("Chromosome", DataTypes.StringType),
        StructField("End_Position", DataTypes.LongType),
        StructField("HGVSp_Short", DataTypes.StringType),
        StructField("Hugo_Symbol", DataTypes.StringType),
        StructField("Mutation_Status", DataTypes.StringType),
        StructField("NCBI_Build", DataTypes.StringType),
        StructField("RefSeq", DataTypes.StringType),
        StructField("Reference_Allele", DataTypes.StringType),
        StructField("Start_Position", DataTypes.LongType),
        StructField("Strand", DataTypes.StringType),
        StructField("Tumor_Sample_Barcode", DataTypes.StringType),
        StructField("Tumor_Seq_Allele1", DataTypes.StringType),
        StructField("Tumor_Seq_Allele2", DataTypes.StringType),
        StructField("Variant_Classification", DataTypes.StringType),
        StructField("Variant_Type", DataTypes.StringType))
      DataTypes.createStructType(commonFields)
    }

    override def setPartition(iterator: util.Iterator[Row]): Unit = {
      this.partition = iterator.asScala.toStream
    }

    override def call(): util.Iterator[Row] = {
      partition
        .toIterator
        .flatMap { row =>
          Try {
            val idx = row.fieldIndex("maf")
            // for DataFrames which have a maf column
            val ls = row.getList[Row](idx).asScala
            if (ls != null) {
              ls.filter(r => !r.isNullAt(r.fieldIndex("Start_Position")))
            } else {
              // MAF cannot be generated from Guardant360, thus it's null
              List.empty
            }
          } match {
            case Success(ls) =>
              ls
            case Failure(_: IllegalArgumentException) =>
              // for pure MAF DataFrame
              List(row)
            case Failure(ex) =>
              throw ex
          }
        }
        .asJava
    }

    override def close(): Unit = ()

  }
}

class MafDataSource extends OperatorPipelineV3 with LoaderSupport {
  override def createLoader(pluginContext: PluginContext, operatorContext: OperatorContext): Loader = {
    new MafLoader(pluginContext, operatorContext)
  }
}
