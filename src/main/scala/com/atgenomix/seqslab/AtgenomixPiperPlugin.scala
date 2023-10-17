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

package com.atgenomix.seqslab

import com.atgenomix.seqslab.operators.collector.BamCollectorFactory
import com.atgenomix.seqslab.operators.executor.{BamExecutorFactory, BedExecutorFactory, CsvExecutorFactory, FastqExecutorFactory, SqlExecutorFactory, VcfExecutorFactory}
import com.atgenomix.seqslab.operators.loader.{CsvInMemoryDataSource, PartitionDataSource, RefDataSource, SingleNodeDataSource}
import com.atgenomix.seqslab.operators.partitioner.{BamPartitionerFactory, BamPartitionerPart1Factory, BamPartitionerPart1UnmapFactory, BedPartitionerFactory, ConsensusBamPartitionerFactory, FastqPartitionerFactory, VcfPartitionerFactory}
import com.atgenomix.seqslab.operators.transformer.{IndexTransformerFactory, VcfGlowTransformerFactory}
import com.atgenomix.seqslab.operators.writer.{CsvWriterFactory, DeltaTablePartitionWriterFactory}
import com.atgenomix.seqslab.piper.engine.actor.Utils.SqlDefaultCollectorFactory
import com.atgenomix.seqslab.piper.plugin.api.collector.CollectorSupport
import com.atgenomix.seqslab.piper.plugin.api.executor.ExecutorSupport
import com.atgenomix.seqslab.piper.plugin.api.loader.LoaderSupport
import com.atgenomix.seqslab.piper.plugin.api.transformer.TransformerSupport
import com.atgenomix.seqslab.piper.plugin.api.writer.WriterSupport
import com.atgenomix.seqslab.piper.plugin.api.{PiperContext, PiperPlugin, PluginContext}
import com.atgenomix.seqslab.udf.{GRCh38Part1, GRCh38Part155, GRCh38Part23, GRCh38Part3101, GRCh38Part50, GRCh38Part50Consensus, Hg19Chr20Part45, Hg19Part1, Hg19Part155, Hg19Part155Consensus, Hg19Part23, Hg19Part3109, Hg19Part3109Unpadded, Hg19Part77}
import io.projectglow.Glow
import io.projectglow.sql.optimizer.{ReplaceExpressionsRule, ResolveAggregateFunctionsRule, ResolveExpandStructRule, ResolveGenotypeFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, LongType}

import java.util
import scala.jdk.CollectionConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}


class AtgenomixPiperPlugin extends PiperPlugin {

  override def init(context: PiperContext): PluginContext = {
    Glow.register(context.spark, false)
    new PluginContext(context)
  }

  override def registerLoaders(): java.util.Map[String, LoaderSupport] = {
    Map(
      "CsvInMemoryLoader" -> new CsvInMemoryDataSource().asInstanceOf[LoaderSupport],
      "PartitionDataLoader" -> new PartitionDataSource().asInstanceOf[LoaderSupport],
      "RefLoader" -> new RefDataSource().asInstanceOf[LoaderSupport],
      "SingleNodeDataLoader" -> new SingleNodeDataSource().asInstanceOf[LoaderSupport],
    ).asJava
  }

  override def registerExecutors(): java.util.Map[String, ExecutorSupport] = {
    Map(
      "BamExecutor" -> new BamExecutorFactory().asInstanceOf[ExecutorSupport],
      "CsvExecutor" -> new CsvExecutorFactory().asInstanceOf[ExecutorSupport],
      "FastqExecutor" -> new FastqExecutorFactory().asInstanceOf[ExecutorSupport],
      "VcfExecutor" -> new VcfExecutorFactory().asInstanceOf[ExecutorSupport],
      "BedExecutor" -> new BedExecutorFactory().asInstanceOf[ExecutorSupport],
      "TableLocalizationExecutor" -> new SqlExecutorFactory().asInstanceOf[ExecutorSupport]
    ).asJava
  }

  override def registerTransformers(): java.util.Map[String, TransformerSupport] = {
    Map(
      "FastqPartitioner" -> new FastqPartitionerFactory().asInstanceOf[TransformerSupport],
      "BamPartitionerPart1" -> new BamPartitionerPart1Factory().asInstanceOf[TransformerSupport],
      "BamPartitionerPart1Unmap" -> new BamPartitionerPart1UnmapFactory().asInstanceOf[TransformerSupport],
      "BamPartitioner" -> new BamPartitionerFactory().asInstanceOf[TransformerSupport],
      "ConsensusBamPartitioner" -> new ConsensusBamPartitionerFactory().asInstanceOf[TransformerSupport],
      "VcfPartitioner" -> new VcfPartitionerFactory().asInstanceOf[TransformerSupport],
      "BedPartitioner" -> new BedPartitionerFactory().asInstanceOf[TransformerSupport],
      "VcfGlowTransformer" -> new VcfGlowTransformerFactory().asInstanceOf[TransformerSupport],
      "IndexTransformer" -> new IndexTransformerFactory().asInstanceOf[TransformerSupport]
    ).asJava
  }

  override def registerCollectors(): util.Map[String, CollectorSupport] = {
    Map(
      "BamCollector" -> new BamCollectorFactory().asInstanceOf[CollectorSupport],
      "SqlDefaultCollector" -> new SqlDefaultCollectorFactory().asInstanceOf[CollectorSupport]
    ).asJava
  }

  override def registerWriters(): util.Map[String, WriterSupport] = {
    Map(
      "DeltaTablePartitionWriter" -> new DeltaTablePartitionWriterFactory().asInstanceOf[WriterSupport],
      "CsvWriter" -> new CsvWriterFactory().asInstanceOf[WriterSupport]
    ).asJava
  }

  override def registerUDFs(): java.util.Map[String, UserDefinedFunction] = {
    Map(
      "hg19part1" -> org.apache.spark.sql.functions.udf(new Hg19Part1(), ArrayType(LongType)),
      "hg19part23" -> org.apache.spark.sql.functions.udf(new Hg19Part23(), ArrayType(LongType)),
      "hg19part77" -> org.apache.spark.sql.functions.udf(new Hg19Part77(), ArrayType(LongType)),
      "hg19part155" -> org.apache.spark.sql.functions.udf(new Hg19Part155(), ArrayType(LongType)),
      "hg19part155consensus" -> org.apache.spark.sql.functions.udf(new Hg19Part155Consensus(), ArrayType(LongType)),
      "hg19part3109" -> org.apache.spark.sql.functions.udf(new Hg19Part3109(), ArrayType(LongType)),
      "hg19part3109unpadded" -> org.apache.spark.sql.functions.udf(new Hg19Part3109Unpadded(), ArrayType(LongType)),
      "hg19chr20part45" -> org.apache.spark.sql.functions.udf(new Hg19Chr20Part45(), ArrayType(LongType)),
      "grch38part1" -> org.apache.spark.sql.functions.udf(new GRCh38Part1(), ArrayType(LongType)),
      "grch38part23" -> org.apache.spark.sql.functions.udf(new GRCh38Part23(), ArrayType(LongType)),
      "grch38part50" -> org.apache.spark.sql.functions.udf(new GRCh38Part50(), ArrayType(LongType)),
      "grch38part50consensus" -> org.apache.spark.sql.functions.udf(new GRCh38Part50Consensus(), ArrayType(LongType)),
      "grch38part155" -> org.apache.spark.sql.functions.udf(new GRCh38Part155(), ArrayType(LongType)),
      "grch38part3101" -> org.apache.spark.sql.functions.udf(new GRCh38Part3101(), ArrayType(LongType))
    ).asJava
  }

  override def registerExtensions(): java.util.Map[String, util.List[Rule[LogicalPlan]]] = {
    val resolutionRules = Seq(ReplaceExpressionsRule, ResolveAggregateFunctionsRule, ResolveExpandStructRule, ResolveGenotypeFields)
    Map(
      "ResolutionRule" -> resolutionRules.asJava
    ).asJava
  }
}
