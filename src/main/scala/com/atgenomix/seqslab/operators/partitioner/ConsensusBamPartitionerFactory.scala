package com.atgenomix.seqslab.operators.partitioner

import com.atgenomix.seqslab.operators.partitioner.ConsensusBamPartitionerFactory.ConsensusBamPartitioner
import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import com.atgenomix.seqslab.piper.plugin.api.transformer.{Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.udf.genomeConsensusPartFunc
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, functions}

import java.net.URL
import scala.io.Source
import scala.jdk.CollectionConverters.asScalaBufferConverter

object ConsensusBamPartitionerFactory {
  class ConsensusBamPartitioner(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer {
    private var refSeqDict: java.net.URL = null
    private var partBed: java.net.URL = null
    private val opName: String = "ConsensusGenericBamPartitioner"
    private var udfName: String = null

    override def init(i: Int, i1: Int): Transformer = {
      val ref = operatorCtx.get("BamConsensusPartitioner:refSeqDict")
      val bed = operatorCtx.get("BamConsensusPartitioner:partBed")

      this.refSeqDict = if (ref != null) {
        val rp = getClass.getResource(ref.asInstanceOf[String])
        if (rp != null)
          rp
        else
          new URL(ref.asInstanceOf[String])
      } else getClass.getResource("/reference/38/GRCH/ref.dict")
      this.partBed = if (bed != null) {
        val rp = getClass.getResource(bed.asInstanceOf[String])
        if (rp != null)
          rp
        else
          new URL(bed.asInstanceOf[String])
      } else getClass.getResource("/bed/38/contiguous_unmasked_regions_50_parts")

      val udf = org.apache.spark.sql.functions.udf(new genomeConsensusPartFunc(this.partBed, this.refSeqDict), ArrayType(LongType))
      this.udfName = f"$opName-${refSeqDict.getFile}-${partBed.getFile}"
      pluginCtx.piper.spark.udf.register(this.udfName, udf)

      this
    }

    private def getPartitionNumFromBed(bed: URL): Int = {
      val f = bed.openStream()
      val partNum = Source
        .fromInputStream(f)
        .getLines
        .withFilter(_.nonEmpty)
        .withFilter(!_.startsWith("#"))
        .map(x => x.split("\t")(3))
        .toSet
        .size
      f.close()
      partNum
    }

    override def numPartitions(): Int = this.getPartitionNumFromBed(this.partBed)

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      val partitioner = GenomicPartitioner(Array(this.partBed), this.refSeqDict)

      val keyColumn = functions.call_udf(this.udfName,
        t1.col("referenceName"),
        t1.col("alignmentStart"),
        t1.col("mateReferenceName"),
        t1.col("mateAlignmentStart")
      )
      val result = t1
        .withColumn("key", keyColumn)
        .select("key","raw")
        .rdd
        .flatMap(r => r.getList[Long](r.fieldIndex("key")).asScala.map(k => (k, r)))
        .mapPartitions { _.map { case (k, v) =>
          val r = v.get(v.fieldIndex("raw"))
          k -> new GenericRow(Array(r)).asInstanceOf[Row]
        }
        }
        .repartitionAndSortWithinPartitions(partitioner)
        .mapPartitions(_.map(_._2))
      val schema = StructType(Seq(StructField("raw", BinaryType)))
      t1.sparkSession.createDataFrame(result, schema)
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class ConsensusBamPartitionerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new ConsensusBamPartitioner(pluginContext, operatorContext)
  }
}
