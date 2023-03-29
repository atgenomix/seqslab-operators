/*
 * Copyright (C) 2022, Atgenomix Incorporated.
 *
 * All Rights Reserved.
 *
 * This program is an unpublished copyrighted work which is proprietary to
 * Atgenomix Incorporated and contains confidential information that is not to
 * be reproduced or disclosed to any other person or entity without prior
 * written consent from Atgenomix, Inc. in each and every instance.
 *
 * Unauthorized reproduction of this program as well as unauthorized
 * preparation of derivative works based upon the program or distribution of
 * copies by sale, rental, lease or lending are violations of federal copyright
 * laws and state trade secret laws, punishable by civil and criminal penalties.
 */

package com.atgenomix.seqslab.operators.partitioner

import com.atgenomix.seqslab.operators.partitioner.FastqPartitionerFactory.FastqPartitioner
import com.atgenomix.seqslab.piper.common.genomics.Fastq
import com.atgenomix.seqslab.piper.plugin.api.transformer.{SupportsOrdering, SupportsPartitioner, Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.storage.StorageLevel

object FastqPartitionerFactory {
  private class FastqPartitioner(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends SupportsOrdering with SupportsPartitioner {
    private var cpuCores: Int = 0
    private var memPerCore: Int = 0
    private var partitions: Int = 0
    private var readsPerChunk: Long = 0

    override def init(cpuCores: Int, memPerCore: Int): Transformer = {
      this.cpuCores = cpuCores
      this.memPerCore = memPerCore
      val readsNumOpt = operatorCtx.get("FastqPartitioner:readsPerChunk")
      this.readsPerChunk = if (readsNumOpt != null) readsNumOpt.asInstanceOf[Number].longValue() else 1 * 1024 * 1024
      this
    }

    override def numPartitions(): Int = partitions

    override def getOperatorContext: OperatorContext = operatorCtx

    override def getSortExprs(dataset: Dataset[Row]): Array[Column] = {
      Array(col("id").asc)
    }

    override def expr(dataset: Dataset[Row]): Column = {
      col("partId")
    }

    override def partitionId(objects: AnyRef*): Integer = {
      objects.head.asInstanceOf[Int]
    }

    override def call(src: Dataset[Row]): Dataset[Row] = {
      src.rdd.persist(StorageLevel.DISK_ONLY)

      // recommend reads per chunk = 3 * 1024 * 1024 (will fit max blob size via single write: 256MiB)
      // https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
      // 2022/11/11 updated to 1 * 1024 * 1024, based on the reasons:
      // - default configuration for v2 and v3.1, with ~7min execution time per batch.
      // - rounded from minimum insert size estimation for BWA 100Mbp (~0.67M reads).
      val totalReads = src.rdd.mapPartitions(iter => Iterator(iter.length)).sum.toLong
      println(s"total reads: ${totalReads}")

      partitions = if (totalReads < readsPerChunk || totalReads % readsPerChunk >= readsPerChunk / 2) {
        (totalReads / readsPerChunk).toInt + 1
      } else {
        (totalReads / readsPerChunk).toInt
      }
      val p = partitions  // prevent serialize whole FastqPartitioner
      val spark = src.sparkSession
      val partitionIdUDF = udf((idx: Long) => {
        val p = (idx / readsPerChunk).toInt
        if (p >= partitions) {
          partitions - 1
        } else {
          p
        }
      })

      import spark.implicits._
      val df = src.rdd
        .zipWithIndex()
        .mapPartitions(_.map{ case (row, idx) =>
          val id = row.getString(0)
          val seq = row.getString(1)
          val qual = row.getString(2)
          (id, seq, qual, idx)
        })
        .toDF(Fastq.columns ++ Seq("idx"): _*)
        .withColumn("partId", partitionIdUDF(col("idx")))
        .repartition(p, col("partId"))
        .sortWithinPartitions("partId","id")
      src.unpersist()
      df
    }

    override def close(): Unit = ()
  }
}

// recommend readsPerChunk = 3 * 1024 * 1024 (will fit max blob size via single write: 256MiB)
// https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
class FastqPartitionerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new FastqPartitioner(pluginContext, operatorContext)
  }
}
