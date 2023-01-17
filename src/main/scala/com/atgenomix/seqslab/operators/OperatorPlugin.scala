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

package com.atgenomix.seqslab.operators

import com.atgenomix.seqslab.piper.plugin.api._
import com.atgenomix.seqslab.piper.plugin.api.collector.CollectorSupport
import com.atgenomix.seqslab.piper.plugin.api.executor.ExecutorSupport
import com.atgenomix.seqslab.piper.plugin.api.loader.LoaderSupport
import com.atgenomix.seqslab.piper.plugin.api.transformer.TransformerSupport
import com.atgenomix.seqslab.piper.plugin.api.writer.WriterSupport
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.collector.BamCollectorFactory
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.executor._
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.loader._
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner._
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer.{VcfDataFrameTransformerFactory, VcfGlowTransformerFactory}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.writer.GeneralWriterFactory
import com.atgenomix.seqslab.piper.plugin.atgenomix.udf._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, LongType}

import java.util
import scala.jdk.CollectionConverters.mapAsJavaMapConverter


class OperatorPlugin extends PiperPlugin {

  override def init(context: PiperContext): PluginContext = {
    new PluginContext(context)
  }

  override def registerCollectors(): util.Map[String, CollectorSupport] = {
    Map(
      "PhenopacketCollector" -> new PhenopacketCollectorFactory().asInstanceOf[CollectorSupport]
    ).asJava
  }
}
