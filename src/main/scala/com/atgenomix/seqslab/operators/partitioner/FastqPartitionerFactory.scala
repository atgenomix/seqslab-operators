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

package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.common.genomics.Fastq
import com.atgenomix.seqslab.piper.plugin.api.transformer.{SupportsOrdering, Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.FastqPartitionerFactory.FastqPartitioner
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Row}

object FastqPartitionerFactory {
  private class FastqPartitioner(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends SupportsOrdering {
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

    override def call(src: Dataset[Row]): Dataset[Row] = {
      src.persist()

      // recommend reads per chunk = 3 * 1024 * 1024 (will fit max blob size via single write: 256MiB)
      // https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
      // 2022/11/11 updated to 1 * 1024 * 1024, based on the reasons:
      // - default configuration for v2 and v3.1, with ~7min execution time per batch.
      // - rounded from minimum insert size estimation for BWA 100Mbp (~0.67M reads).
      val totalReads = src.count()
      partitions = if (totalReads < readsPerChunk || totalReads % readsPerChunk >= readsPerChunk / 2) {
        (totalReads / readsPerChunk).toInt + 1
      } else {
        (totalReads / readsPerChunk).toInt
      }
      val p = partitions  // prevent serialize whole FastqPartitioner
      val result = src.rdd
        .map(Fastq.serialize)
        .zipWithIndex()
        .map(i => i._2 -> i._1)
        .repartitionAndSortWithinPartitions(new FqPartitioner(p, readsPerChunk))
        .map(i => Fastq.deserialize(i._2))
      val df = pluginCtx.piper.spark.createDataFrame(result)

      src.unpersist()
      df
    }

    class FqPartitioner(partitions: Int, readsPerChunk: Long) extends org.apache.spark.Partitioner {
      override def numPartitions: Int = partitions

      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[Long]
        val p = (k / readsPerChunk).toInt
        if (p >= partitions) {
          partitions - 1
        } else {
          p
        }
      }
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
