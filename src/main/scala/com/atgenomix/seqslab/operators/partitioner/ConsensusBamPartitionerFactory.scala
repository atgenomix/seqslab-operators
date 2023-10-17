package com.atgenomix.seqslab.operators.partitioner

import com.atgenomix.seqslab.operators.partitioner.ConsensusBamPartitionerFactory.ConsensusBamPartitioner
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.api.transformer.{SupportsPartitioner, Transformer, TransformerSupport}
import com.atgenomix.seqslab.udf.{genomeConsensusPartFunc, genomePartFunc}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{Column, Dataset, Row, functions}

import java.net.URL
import scala.io.Source

object ConsensusBamPartitionerFactory {
  class ConsensusBamPartitioner(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer with SupportsPartitioner {
    private var refSeqDict: java.net.URL = null
    private var partBed: java.net.URL = null
    private val opName: String = "ConsensusGenericBamPartitioner"
    private var udfPartition: String = null
    private var udfOrder: String = null

    override def init(i: Int, i1: Int): Transformer = {
      val ref = operatorCtx.get("ConsensusBamPartitioner:refSeqDict")
      val bed = operatorCtx.get("ConsensusBamPartitioner:partBed")

      this.refSeqDict = if (ref != null) {
        val rp = getClass.getResource(ref.asInstanceOf[String])
        if (rp != null)
          rp
        else
          new URL(ref.asInstanceOf[String])
      } else throw new IllegalArgumentException("ConsensusBamPartitioner:refSeqDict cannot be null")
      this.partBed = if (bed != null) {
        val rp = getClass.getResource(bed.asInstanceOf[String])
        if (rp != null)
          rp
        else
          new URL(bed.asInstanceOf[String])
      } else throw new IllegalArgumentException("ConsensusBamPartitioner:partBed cannot be null")

      val udf = org.apache.spark.sql.functions.udf(new genomeConsensusPartFunc(this.partBed, this.refSeqDict), ArrayType(IntegerType))
      this.udfPartition = f"$opName-${refSeqDict.getFile}-${partBed.getFile}"
      pluginCtx.piper.spark.udf.register(this.udfPartition, udf)

      this.udfOrder = f"$opName-${refSeqDict.getFile}-${partBed.getFile}-sort"
      pluginCtx.piper.spark.udf.register(
        this.udfOrder,
        org.apache.spark.sql.functions.udf(new genomePartFunc(this.partBed, this.refSeqDict), ArrayType(LongType))
      )
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

    override def expr(dataset: Dataset[Row]): Column = {
      col("partId")
    }

    override def partitionId(objects: AnyRef*): Integer = {
      objects.head.asInstanceOf[Int]
    }

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      t1
        .withColumn(
          "parts",
          functions.call_udf(this.udfPartition,
            t1.col("referenceName"),
            t1.col("alignmentStart"),
            t1.col("mateReferenceName"),
            t1.col("mateAlignmentStart")
          )
        )
        .withColumn(
          "order",
          functions.call_udf(this.udfOrder,
            t1.col("referenceName"),
            t1.col("alignmentStart")
          )
        )
        .select(col("raw"), col("order"), functions.explode(col("parts")))
        .withColumn("partId", col("col"))
        .repartition(numPartitions(), col("partId"))
        .sortWithinPartitions(col("order"))
        .drop("parts", "order")
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
