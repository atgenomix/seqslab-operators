package com.atgenomix.seqslab.operators.loader

import com.atgenomix.seqslab.piper.common.operator.AbstractInMemoryDataLoader
import com.atgenomix.seqslab.piper.plugin.api.loader.{Loader, LoaderSupport, SupportsBatch}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, OperatorPipelineV3, PluginContext}
import com.atgenomix.seqslab.operators.loader.BatchDataSource.BatchDataLoader

object BatchDataSource {

  class BatchDataLoader(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends AbstractInMemoryDataLoader(pluginCtx, operatorCtx) with SupportsBatch {
    override def getName: String = "BatchDataLoader"
  }
}

class BatchDataSource extends OperatorPipelineV3 with LoaderSupport {
  override def createLoader(pluginContext: PluginContext, operatorContext: OperatorContext): Loader = {
    new BatchDataLoader(pluginContext, operatorContext)
  }
}
