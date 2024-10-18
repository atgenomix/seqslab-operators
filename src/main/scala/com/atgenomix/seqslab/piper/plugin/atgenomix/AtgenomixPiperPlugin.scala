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

package com.atgenomix.seqslab.piper.plugin.atgenomix

import com.atgenomix.seqslab.piper.common.utils.RetryUtil
import com.atgenomix.seqslab.piper.plugin.api._
import com.atgenomix.seqslab.piper.plugin.api.collector.CollectorSupport
import com.atgenomix.seqslab.piper.plugin.api.executor.ExecutorSupport
import com.atgenomix.seqslab.piper.plugin.api.formatter.FormatterSupport
import com.atgenomix.seqslab.piper.plugin.api.loader.LoaderSupport
import com.atgenomix.seqslab.piper.plugin.api.transformer.TransformerSupport
import com.atgenomix.seqslab.piper.plugin.api.writer.WriterSupport
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector._
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor._
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader._
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner._
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer._
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer._
import io.projectglow.Glow
import io.projectglow.sql.optimizer.{ReplaceExpressionsRule, ResolveAggregateFunctionsRule, ResolveExpandStructRule, ResolveGenotypeFields}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}


class AtgenomixPiperPlugin extends PiperPlugin {

  override def init(context: PiperContext): PluginContext = {
    Glow.register(context.spark, false)
    context.spark.conf.getOption("spark.seqslab.kernel.hive.udf.register.enabled") match {
      case Some(r) if r.toLowerCase.equals("true") =>
        val libraryPrefix = "com.atgenomix.seqslab.piper.plugin.atgenomix.udf.hive"
        Map(
          "except_sample_id" -> "ExceptSampleId",
          "gpt" -> "OpenAI",
          "retrieve_pattern" -> "RetrievePattern",
          "retrieve_positive_genotype" -> "RetrievePositiveGenotype",
          "retrieve_rs_id" -> "RetrieveRsID",
        ).foreach { case (k, v) =>
          val sqlTemp = s"CREATE OR REPLACE TEMPORARY FUNCTION $k AS '$libraryPrefix.$v'"
          val sqlPerm = s"CREATE OR REPLACE FUNCTION default.$k AS '$libraryPrefix.$v'"
          RetryUtil.retryWithCallback(context.spark.sql(sqlTemp), Thread.sleep(10000), 10)
          RetryUtil.retryWithCallback(context.spark.sql(sqlPerm), Thread.sleep(10000), 10)
        }
      case None =>
    }
    new PluginContext(context)
  }

  override def registerLoaders(): java.util.Map[String, LoaderSupport] = {
    Map(
      "BgenInMemoryLoader" -> new BgenInMemoryDataSource(),
      "MafLoader" -> new MafDataSource(),
      "PartitionDataLoader" -> new PartitionDataSource(),
      "RefLoader" -> new RefDataSource(),
      "SingleNodeDataLoader" -> new SingleNodeDataSource(),
      "BatchDataLoader" -> new BatchDataSource(),
      "InMemoryDataLoader" -> new InMemoryDataSource()
    ).asJava
  }

  override def registerExecutors(): java.util.Map[String, ExecutorSupport] = {
    Map(
      "BamExecutor" -> new BamExecutorFactory(),
      "BedExecutor" -> new BedExecutorFactory(),
      "CsvExecutor" -> new CsvExecutorFactory(),
      "FastqExecutor" -> new FastqExecutorFactory(),
      "RegenieBgenExecutor" -> new RegenieBgenExecutorFactory(),
      "TableLocalizationExecutor" -> new SqlExecutorFactory(),
      "VcfExecutor" -> new VcfExecutorFactory()
    ).asJava
  }

  override def registerTransformers(): java.util.Map[String, TransformerSupport] = {
    Map(
      // Partitioner
      "BamPartitionerPart1" -> new BamPartitionerPart1Factory(),
      "BamPartitionerPart1Unmap" -> new BamPartitionerPart1UnmapFactory(),
      "BamPartitioner" -> new BamPartitionerFactory(),
      "BedPartitioner" -> new BedPartitionerFactory(),
      "ConsensusBamPartitioner" -> new ConsensusBamPartitionerFactory(),
      "FastqPartitioner" -> new FastqPartitionerFactory(),
      "MafPartitioner" -> new MafPartitionerFactory(),
      "RegenieBgenPartitioner" -> new RegenieBgenPartitionerFactory(),
      "VcfPartitioner" -> new VcfPartitionerFactory(),
      "VcfPartitionerPart1" -> new VcfPartitionerPart1Factory(),
      "VcfPhasingPartitioner" -> new VcfPhasingPartitionerFactory(),
      "VcfImputePartitioner" -> new VcfImputePartitionerFactory(),
      // Transformer
      "GenotypeTableTransformer" -> new GenotypeTableTransformerFactory(),
      "Icd10Transformer" -> new Icd10TransformerFactory(),
      "IcdOncologyTransformer" -> new IcdOncologyTransformerFactory(),
      "IndexTransformer" -> new IndexTransformerFactory(),
      "JsonTransformer" -> new JsonTransformerFactory(),
      "MafToVcfTransformer" -> new MafToVcfTransformerFactory(),
      "SampleListTableTransformer" -> new SampleListTableTransformerFactory(),
      "VcfGlowTransformer" -> new VcfGlowTransformerFactory(),
      "VepSelectTransformer" -> new VepSelectTransformerFactory(),
      "NormalizeSchemaTransformer" -> new NormalizeSchemaTransformerFactory()
    ).asJava
  }

  override def registerFormatters(): util.Map[String, FormatterSupport] = {
    Map.empty[String, FormatterSupport].asJava
  }

  override def registerCollectors(): util.Map[String, CollectorSupport] = {
    Map(
      "BamCollector" -> new BamCollectorFactory().asInstanceOf[CollectorSupport],
      "PartitionCollector" -> new PartitionCollectorFactory().asInstanceOf[CollectorSupport],
      "RegenieMasterCollector" -> new RegenieMasterCollectorFactory().asInstanceOf[CollectorSupport],
      "SqlDefaultCollector" -> new SqlDefaultCollectorFactory().asInstanceOf[CollectorSupport],
      "TextCollector" -> new TextCollectorFactory().asInstanceOf[CollectorSupport]
    ).asJava
  }

  override def registerWriters(): util.Map[String, WriterSupport] = {
    Map(
      "CsvWriter" -> new CsvWriterFactory().asInstanceOf[WriterSupport],
      "DeltaTableWriter" -> new DeltaTableWriterFactory().asInstanceOf[WriterSupport],
      "DeltaTablePartitionWriter" -> new DeltaTablePartitionWriterFactory().asInstanceOf[WriterSupport],
      "DirWriter" -> new DirWriterFactory().asInstanceOf[WriterSupport],
      "FileWriter" -> new FileWriterFactory().asInstanceOf[WriterSupport],
      "JsonWriter" -> new JsonWriterFactory().asInstanceOf[WriterSupport],
      "RegenieMasterWriter" -> new RegenieMasterWriterFactory().asInstanceOf[WriterSupport],
      "VcfGlowWriter" -> new VcfGlowWriterFactory().asInstanceOf[WriterSupport]
    ).asJava
  }

  override def registerUDFs(): java.util.Map[String, UserDefinedFunction] = {
    Map.empty[String, UserDefinedFunction].asJava
  }

  override def registerExtensions(): java.util.Map[String, util.List[Rule[LogicalPlan]]] = {
    val resolutionRules = Seq(ReplaceExpressionsRule, ResolveAggregateFunctionsRule, ResolveExpandStructRule, ResolveGenotypeFields)
    Map(
      "ResolutionRule" -> resolutionRules.asJava
    ).asJava
  }
}
