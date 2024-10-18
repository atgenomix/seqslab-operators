package com.atgenomix.seqslab.operators.partitioner

import com.atgenomix.seqslab.piper.common.Const
import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import com.atgenomix.seqslab.piper.plugin.api.transformer.{SupportsPartitioner, Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.operators.partitioner.BedPartitionerFactory.BedPartitioner
import com.atgenomix.seqslab.piper.plugin.atgenomix.udf.GenomeBedPartFunc
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions}

import java.net.URL
import scala.io.Source

object BedPartitionerFactory {
  class BedPartitioner(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer with SupportsPartitioner {
    private var refSeqDict: java.net.URL = null
    private var partBed: java.net.URL = null
    private val opName: String = this.getName
    private var udfName: String = null

    override def init(i: Int, i1: Int): Transformer = {
      val ref = operatorCtx.get(s"$opName:refSeqDict")
      val bed = operatorCtx.get(s"$opName:partBed")

      this.refSeqDict = if (ref != null) {
        val rp = getClass.getResource(ref.asInstanceOf[String])
        if (rp != null) {
          rp
        } else {
          val str = ref.asInstanceOf[String]
          if (str.nonEmpty) {
            new URL(ref.asInstanceOf[String])
          } else {
            throw new IllegalArgumentException(s"$opName:refSeqDict cannot be empty string")
          }
        }
      } else throw new IllegalArgumentException(s"$opName:refSeqDict cannot be null")
      this.partBed = if (bed != null) {
        val rp = getClass.getResource(bed.asInstanceOf[String])
        if (rp != null)
          rp
        else
          new URL(bed.asInstanceOf[String])
      } else throw new IllegalArgumentException(s"$opName:partBed cannot be null")

      val udf = org.apache.spark.sql.functions.udf(new GenomeBedPartFunc(this.partBed, this.refSeqDict), ArrayType(LongType))
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

    override def expr(dataset: Dataset[Row]): Column = {
      col("partId")
    }

    override def partitionId(objects: AnyRef*): Integer = {
      objects.head.asInstanceOf[Int]
    }

    override def call(t1: DataFrame): DataFrame = {
      val partitioner = GenomicPartitioner(Array(this.partBed), this.refSeqDict)
      val keyColumn = functions.call_udf(this.udfName, t1.col("row"))
      val partitionIdUDF = udf((key: Long) => partitioner.getPartition(key))

      t1.withColumn("key", keyColumn)
        .select(explode(col("key")) :: col("row") :: Const.preservedColumns: _*)
        .withColumn("partId", partitionIdUDF(col("col")))
        .repartition(numPartitions(), col("partId"))
        .sortWithinPartitions(col("partId"), col("col"))
        .drop("col")
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class BedPartitionerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new BedPartitioner(pluginContext, operatorContext)
  }
}
